import type { Express } from "express";
import { createServer, type Server } from "http";
import type { TickData, Trade, StrategySignal } from "@shared/schema";

// ── In-memory state ──────────────────────────────────────────────
let btcPrices: [number, number][] = []; // [timestamp_ms, price]
let currentMarket: TickData["current_market"] = null;
let lastSettled: TickData["last_settled"] = null;
let trades: Trade[] = [];
let stats = {
  total_trades: 0,
  wins: 0,
  losses: 0,
  win_rate: 0,
  total_pnl: 0,
  daily_pnl: 0,
};
let lastTickData: TickData | null = null;
let previousMarketResult: string | null = null;

// ── API Fetchers ────────────────────────────────────────────────
async function fetchBTCPrice(): Promise<number | null> {
  try {
    const res = await fetch("https://api.coinbase.com/v2/prices/BTC-USD/spot");
    if (!res.ok) return null;
    const data = await res.json();
    return parseFloat(data.data.amount);
  } catch {
    return null;
  }
}

interface KalshiMarket {
  ticker: string;
  status: string;
  close_time: string;
  yes_bid: number;
  yes_ask: number;
  no_bid: number;
  no_ask: number;
  volume: number;
  result?: string;
}

async function fetchKalshiMarkets(status: string, limit: number): Promise<KalshiMarket[]> {
  try {
    const url = `https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXBTC15M&status=${status}&limit=${limit}`;
    const res = await fetch(url, {
      headers: {
        "Accept": "application/json",
        "User-Agent": "KalshiDashboard/1.0",
      },
    });
    if (!res.ok) return [];
    const data = await res.json();
    if (!data.markets) return [];
    return data.markets.map((m: any) => ({
      ticker: m.ticker || "",
      status: m.status || "",
      close_time: m.close_time || "",
      yes_bid: parseDollars(m.yes_bid_dollars) ?? parseDollars(m.yes_bid) ?? 50,
      yes_ask: parseDollars(m.yes_ask_dollars) ?? parseDollars(m.yes_ask) ?? 50,
      no_bid: parseDollars(m.no_bid_dollars) ?? parseDollars(m.no_bid) ?? 50,
      no_ask: parseDollars(m.no_ask_dollars) ?? parseDollars(m.no_ask) ?? 50,
      volume: parseFloat(m.volume_fp || m.volume || "0") || 0,
      result: m.result && m.result !== "" ? m.result : null,
    }));
  } catch {
    return [];
  }
}

function parseDollars(val: any): number | null {
  if (val == null) return null;
  const s = String(val);
  const num = parseFloat(s);
  if (isNaN(num)) return null;
  // If it looks like dollar format (0.56), convert to cents
  if (num > 0 && num <= 1) return Math.round(num * 100);
  return Math.round(num);
}

// ── Momentum Calculation ────────────────────────────────────────
function calcMomentum(minutes: number): number {
  const now = Date.now();
  const cutoff = now - minutes * 60 * 1000;
  const recent = btcPrices.filter(([t]) => t >= cutoff);
  if (recent.length < 2) return 0;
  const first = recent[0][1];
  const last = recent[recent.length - 1][1];
  return ((last - first) / first) * 100;
}

// ── Strategy Evaluators ─────────────────────────────────────────
function evalMomentum(mom1m: number, mom5m: number): StrategySignal {
  if (mom1m > 0.05 && mom5m > 0.02) {
    const conf = Math.min(0.95, 0.5 + Math.abs(mom1m) * 3 + Math.abs(mom5m) * 5);
    return {
      signal: "yes",
      confidence: parseFloat(conf.toFixed(2)),
      reason: `Bullish: 1m=${mom1m > 0 ? "+" : ""}${mom1m.toFixed(3)}% 5m=${mom5m > 0 ? "+" : ""}${mom5m.toFixed(3)}%`,
    };
  }
  if (mom1m < -0.05 && mom5m < -0.02) {
    const conf = Math.min(0.95, 0.5 + Math.abs(mom1m) * 3 + Math.abs(mom5m) * 5);
    return {
      signal: "no",
      confidence: parseFloat(conf.toFixed(2)),
      reason: `Bearish: 1m=${mom1m.toFixed(3)}% 5m=${mom5m.toFixed(3)}%`,
    };
  }
  return { signal: "none", confidence: 0, reason: "No clear momentum" };
}

function evalMeanReversion(
  mom1m: number,
  mom5m: number,
  yesBid: number,
  yesAsk: number
): StrategySignal {
  const spike1m = Math.abs(mom1m);
  const confirmed5m = Math.abs(mom5m) > 0.06;
  const yesAvg = (yesBid + yesAsk) / 2;

  if (spike1m > 0.12 && !confirmed5m) {
    if (mom1m > 0 && yesAvg > 60) {
      const conf = Math.min(0.9, 0.4 + spike1m * 2);
      return {
        signal: "no",
        confidence: parseFloat(conf.toFixed(2)),
        reason: `Fade spike: 1m=+${mom1m.toFixed(3)}% but YES@${yesAvg.toFixed(0)}c overpriced`,
      };
    }
    if (mom1m < 0 && yesAvg < 40) {
      const conf = Math.min(0.9, 0.4 + spike1m * 2);
      return {
        signal: "yes",
        confidence: parseFloat(conf.toFixed(2)),
        reason: `Fade dip: 1m=${mom1m.toFixed(3)}% but YES@${yesAvg.toFixed(0)}c cheap`,
      };
    }
  }
  return { signal: "none", confidence: 0, reason: "No spike detected" };
}

function evalConsensus(
  mom1m: number,
  yesBid: number,
  yesAsk: number
): StrategySignal {
  let bullishVotes = 0;
  let bearishVotes = 0;

  // Vote 1: 1-min momentum direction
  if (mom1m > 0.01) bullishVotes++;
  else if (mom1m < -0.01) bearishVotes++;

  // Vote 2: Previous market result
  if (previousMarketResult === "yes") bullishVotes++;
  else if (previousMarketResult === "no") bearishVotes++;

  // Vote 3: Orderbook skew
  const yesAvg = (yesBid + yesAsk) / 2;
  if (yesBid > 52) bullishVotes++;
  else if (yesBid < 48) bearishVotes++;

  const yesPrice = yesAvg;
  
  if (bullishVotes >= 2) {
    if (yesPrice >= 35 && yesPrice <= 57) {
      const conf = Math.min(0.95, 0.55 + bullishVotes * 0.12);
      return {
        signal: "yes",
        confidence: parseFloat(conf.toFixed(2)),
        reason: `${bullishVotes}/3 agree YES (mom:${mom1m > 0.01 ? "Y" : "N"} prev:${previousMarketResult || "—"} skew:${yesBid > 52 ? "Y" : "N"})`,
      };
    }
    return { signal: "none", confidence: 0, reason: `${bullishVotes}/3 bull but YES@${yesPrice.toFixed(0)}c outside 35-57c range` };
  }

  if (bearishVotes >= 2) {
    const noPrice = 100 - yesPrice;
    if (noPrice >= 35 && noPrice <= 57) {
      const conf = Math.min(0.95, 0.55 + bearishVotes * 0.12);
      return {
        signal: "no",
        confidence: parseFloat(conf.toFixed(2)),
        reason: `${bearishVotes}/3 agree NO (mom:${mom1m < -0.01 ? "Y" : "N"} prev:${previousMarketResult || "—"} skew:${yesBid < 48 ? "Y" : "N"})`,
      };
    }
    return { signal: "none", confidence: 0, reason: `${bearishVotes}/3 bear but NO@${noPrice.toFixed(0)}c outside 35-57c range` };
  }

  return { signal: "none", confidence: 0, reason: `No consensus (bull:${bullishVotes} bear:${bearishVotes})` };
}

// ── Paper Trading ───────────────────────────────────────────────
function simulateTrade(
  signal: StrategySignal,
  market: NonNullable<TickData["current_market"]>
): void {
  if (signal.signal === "none") return;

  const price = signal.signal === "yes" ? market.yes_ask : (100 - market.yes_bid);
  if (price <= 0 || price >= 100) return;
  const contracts = Math.min(10, Math.floor(500 / price));
  if (contracts <= 0) return;
  const stake = (price * contracts) / 100;

  // Simulate outcome based on signal confidence (simplified)
  const win = Math.random() < signal.confidence * 0.85;
  const profit = win ? ((100 - price) * contracts) / 100 : -stake;

  const trade: Trade = {
    time: new Date().toISOString(),
    ticker: market.ticker,
    strategy: "consensus",
    side: signal.signal as "yes" | "no",
    price,
    contracts,
    stake: parseFloat(stake.toFixed(2)),
    outcome: win ? "win" : "loss",
    profit: parseFloat(profit.toFixed(2)),
  };

  trades.unshift(trade);
  if (trades.length > 50) trades.pop();

  stats.total_trades++;
  if (win) stats.wins++;
  else stats.losses++;
  stats.win_rate = stats.total_trades > 0
    ? parseFloat(((stats.wins / stats.total_trades) * 100).toFixed(1))
    : 0;
  stats.total_pnl = parseFloat((stats.total_pnl + profit).toFixed(2));
  stats.daily_pnl = stats.total_pnl;
}

// ── Main Tick ───────────────────────────────────────────────────
async function tick(): Promise<TickData> {
  // Fetch BTC price
  const btcPrice = await fetchBTCPrice();
  const now = Date.now();

  if (btcPrice !== null) {
    btcPrices.push([now, btcPrice]);
    // Keep last 15 minutes
    const cutoff = now - 15 * 60 * 1000;
    btcPrices = btcPrices.filter(([t]) => t >= cutoff);
  }

  const currentPrice = btcPrice ?? (btcPrices.length > 0 ? btcPrices[btcPrices.length - 1][1] : 0);
  const mom1m = calcMomentum(1);
  const mom5m = calcMomentum(5);

  // Fetch open markets
  const openMarkets = await fetchKalshiMarkets("open", 5);
  if (openMarkets.length > 0) {
    const m = openMarkets[0];
    const closeTime = new Date(m.close_time).getTime();
    const secondsRemaining = Math.max(0, Math.floor((closeTime - now) / 1000));
    currentMarket = {
      ticker: m.ticker,
      yes_bid: m.yes_bid,
      yes_ask: m.yes_ask,
      seconds_remaining: secondsRemaining,
      volume: m.volume,
    };
  }

  // Fetch settled markets
  const settledMarkets = await fetchKalshiMarkets("settled", 5);
  if (settledMarkets.length > 0) {
    const s = settledMarkets[0];
    if (s.result) {
      lastSettled = {
        ticker: s.ticker,
        result: s.result as "yes" | "no",
      };
      previousMarketResult = s.result;
    }
  }

  // Evaluate strategies
  const yesBid = currentMarket?.yes_bid ?? 50;
  const yesAsk = currentMarket?.yes_ask ?? 50;

  const momentum = evalMomentum(mom1m, mom5m);
  const meanReversion = evalMeanReversion(mom1m, mom5m, yesBid, yesAsk);
  const consensus = evalConsensus(mom1m, yesBid, yesAsk);

  // Simulate paper trade on consensus signal
  if (consensus.signal !== "none" && currentMarket) {
    // Only trade once per market per signal
    const alreadyTraded = trades.some(
      (t) => t.ticker === currentMarket!.ticker && t.strategy === "consensus"
    );
    if (!alreadyTraded) {
      simulateTrade(consensus, currentMarket);
    }
  }

  const tickData: TickData = {
    timestamp: new Date().toISOString(),
    btc_price: currentPrice,
    btc_momentum_1m: parseFloat(mom1m.toFixed(4)),
    btc_momentum_5m: parseFloat(mom5m.toFixed(4)),
    btc_prices: btcPrices.slice(-180), // ~15min at 5s intervals
    current_market: currentMarket,
    last_settled: lastSettled,
    strategies: {
      momentum,
      mean_reversion: meanReversion,
      consensus,
    },
    trades,
    stats,
  };

  lastTickData = tickData;
  return tickData;
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // SSE endpoint
  app.get("/api/stream", (req, res) => {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });

    // Send initial data immediately if available
    if (lastTickData) {
      res.write(`data: ${JSON.stringify(lastTickData)}\n\n`);
    }

    const interval = setInterval(async () => {
      try {
        const data = await tick();
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      } catch (err) {
        console.error("Tick error:", err);
      }
    }, 5000);

    // Send initial tick immediately
    tick().then((data) => {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    }).catch(console.error);

    req.on("close", () => {
      clearInterval(interval);
    });
  });

  // Health check
  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok", timestamp: new Date().toISOString() });
  });

  return httpServer;
}
