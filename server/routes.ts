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
  pending: 0,
  wins: 0,
  losses: 0,
  win_rate: 0,
  total_pnl: 0,
  daily_pnl: 0,
  bot_paused: false,
};
let lastTickData: TickData | null = null;
let previousMarketResult: string | null = null;
let botPaused = false;
const DAILY_LOSS_LIMIT = -50; // $50

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

// ── Order Flow Imbalance (OFI) from Crypto.com ─────────────────
interface OrderbookLevel {
  price: number;
  quantity: number;
}

let lastOfi: number = 0; // cached OFI value

async function fetchOFI(): Promise<number> {
  try {
    // Crypto.com public orderbook endpoint (no auth needed)
    const res = await fetch("https://api.crypto.com/exchange/v1/public/get-book?instrument_name=BTC_USD&depth=10");
    if (!res.ok) return lastOfi;
    const data = await res.json();
    const book = data?.result?.data?.[0] || data?.result?.data || data?.result;
    
    if (!book) return lastOfi;
    
    const bids: [number, number][] = book.bids || [];
    const asks: [number, number][] = book.asks || [];
    
    // Multi-level OFI: weighted sum across depth levels (closer levels weighted more)
    let bidVolume = 0;
    let askVolume = 0;
    
    for (let i = 0; i < Math.min(bids.length, 5); i++) {
      const weight = 1 / (i + 1); // decreasing weight with depth
      bidVolume += parseFloat(String(bids[i][1])) * weight;
    }
    for (let i = 0; i < Math.min(asks.length, 5); i++) {
      const weight = 1 / (i + 1);
      askVolume += parseFloat(String(asks[i][1])) * weight;
    }
    
    const total = bidVolume + askVolume;
    if (total === 0) return lastOfi;
    
    // OFI ranges from -1 (all sell pressure) to +1 (all buy pressure)
    lastOfi = (bidVolume - askVolume) / total;
    return lastOfi;
  } catch {
    return lastOfi;
  }
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

function evalResolutionRider(
  secondsRemaining: number,
  yesBid: number,
  yesAsk: number,
  mom1m: number
): StrategySignal {
  // Only fires in the last 90 seconds
  if (secondsRemaining > 90 || secondsRemaining < 10) {
    return { signal: "none", confidence: 0, reason: `Not in resolution window (${secondsRemaining}s left)` };
  }
  
  const yesAvg = (yesBid + yesAsk) / 2;
  
  // If market strongly favors YES and momentum confirms
  if (yesAvg > 60 && mom1m > 0) {
    const conf = Math.min(0.95, 0.6 + (yesAvg - 60) / 100);
    return {
      signal: "yes",
      confidence: parseFloat(conf.toFixed(2)),
      reason: `Resolution rider: YES@${yesAvg.toFixed(0)}c with ${secondsRemaining}s left, momentum confirms`,
    };
  }
  
  // If market strongly favors NO
  if (yesAvg < 40 && mom1m < 0) {
    const noAvg = 100 - yesAvg;
    const conf = Math.min(0.95, 0.6 + (noAvg - 60) / 100);
    return {
      signal: "no",
      confidence: parseFloat(conf.toFixed(2)),
      reason: `Resolution rider: NO@${noAvg.toFixed(0)}c with ${secondsRemaining}s left, momentum confirms`,
    };
  }
  
  return { signal: "none", confidence: 0, reason: `Resolution window but no strong lean (YES@${yesAvg.toFixed(0)}c, ${secondsRemaining}s)` };
}

function evalFavoriteBias(
  yesBid: number,
  yesAsk: number,
  secondsRemaining: number
): StrategySignal {
  // Only activates in the first 10 minutes (enough time for the favorite to hold)
  if (secondsRemaining < 180) {
    return { signal: "none", confidence: 0, reason: "Too close to expiry for favorite bias" };
  }
  
  const yesAvg = (yesBid + yesAsk) / 2;
  
  // Strong favorite: YES > 70c. Academic research shows favorites win MORE often than implied.
  if (yesAvg >= 70 && yesAsk <= 80) {
    const conf = Math.min(0.92, yesAvg / 100 + 0.05); // slightly above market-implied
    return {
      signal: "yes",
      confidence: parseFloat(conf.toFixed(2)),
      reason: `Favorite-longshot: YES@${yesAvg.toFixed(0)}c is favorite, bias says it wins more than ${yesAvg.toFixed(0)}% of the time`,
    };
  }
  
  // Strong favorite on NO side: YES < 30c
  if (yesAvg <= 30 && (100 - yesBid) <= 80) {
    const noAvg = 100 - yesAvg;
    const conf = Math.min(0.92, noAvg / 100 + 0.05);
    return {
      signal: "no",
      confidence: parseFloat(conf.toFixed(2)),
      reason: `Favorite-longshot: NO@${noAvg.toFixed(0)}c is favorite, bias says it wins more than ${noAvg.toFixed(0)}% of the time`,
    };
  }
  
  return { signal: "none", confidence: 0, reason: `No strong favorite (YES@${yesAvg.toFixed(0)}c, need >70 or <30)` };
}

function evalConsensus(
  mom1m: number,
  yesBid: number,
  yesAsk: number,
  ofi: number // NEW parameter
): StrategySignal {
  let bullishVotes = 0;
  let bearishVotes = 0;

  // Vote 1: 1-min momentum
  if (mom1m > 0.01) bullishVotes++;
  else if (mom1m < -0.01) bearishVotes++;

  // Vote 2: Previous market result
  if (previousMarketResult === "yes") bullishVotes++;
  else if (previousMarketResult === "no") bearishVotes++;

  // Vote 3: Orderbook skew (Kalshi)
  if (yesBid > 52) bullishVotes++;
  else if (yesBid < 48) bearishVotes++;

  // Vote 4: OFI from crypto exchange (NEW)
  if (ofi > 0.15) bullishVotes++;
  else if (ofi < -0.15) bearishVotes++;

  const yesPrice = (yesBid + yesAsk) / 2;
  const totalVoters = 4; // updated from 3
  
  if (bullishVotes >= 3) { // require 3/4 for higher conviction
    if (yesPrice >= 35 && yesPrice <= 57) {
      const conf = Math.min(0.95, 0.55 + bullishVotes * 0.10);
      return {
        signal: "yes",
        confidence: parseFloat(conf.toFixed(2)),
        reason: `${bullishVotes}/${totalVoters} agree YES (mom:${mom1m > 0.01 ? "Y" : "N"} prev:${previousMarketResult || "\u2014"} skew:${yesBid > 52 ? "Y" : "N"} ofi:${ofi > 0.15 ? "Y" : "N"})`,
      };
    }
    return { signal: "none", confidence: 0, reason: `${bullishVotes}/${totalVoters} bull but YES@${yesPrice.toFixed(0)}c outside 35-57c range` };
  }

  if (bearishVotes >= 3) {
    const noPrice = 100 - yesPrice;
    if (noPrice >= 35 && noPrice <= 57) {
      const conf = Math.min(0.95, 0.55 + bearishVotes * 0.10);
      return {
        signal: "no",
        confidence: parseFloat(conf.toFixed(2)),
        reason: `${bearishVotes}/${totalVoters} agree NO (mom:${mom1m < -0.01 ? "Y" : "N"} prev:${previousMarketResult || "\u2014"} skew:${yesBid < 48 ? "Y" : "N"} ofi:${ofi < -0.15 ? "Y" : "N"})`,
      };
    }
    return { signal: "none", confidence: 0, reason: `${bearishVotes}/${totalVoters} bear but NO@${noPrice.toFixed(0)}c outside 35-57c range` };
  }

  return { signal: "none", confidence: 0, reason: `No consensus (bull:${bullishVotes} bear:${bearishVotes}/${totalVoters})` };
}

// ── Kelly Criterion Position Sizing ─────────────────────────────
function kellySize(confidence: number, priceCents: number, bankroll: number = 500, fraction: number = 0.25): number {
  // confidence = our estimated probability of winning
  // priceCents = what we pay per contract (in cents)
  // Kelly: f* = (b*p - q) / b where b = (100-price)/price, p = confidence, q = 1-p
  const p = confidence;
  const q = 1 - p;
  const b = (100 - priceCents) / priceCents; // odds
  const kelly = (b * p - q) / b;
  if (kelly <= 0) return 0; // no edge, don't bet
  const fractionalKelly = kelly * fraction; // use 25% Kelly for safety
  const dollarBet = bankroll * fractionalKelly;
  const contracts = Math.floor(dollarBet / (priceCents / 100));
  return Math.max(0, Math.min(20, contracts)); // cap at 20 contracts
}

// ── Paper Trading (Real Settlement) ─────────────────────────────
// Trades go in as "pending" and only resolve when Kalshi settles
// the market. This mirrors real trading behavior exactly.

function openPaperTrade(
  signal: StrategySignal,
  market: NonNullable<TickData["current_market"]>,
  strategyName: string
): void {
  if (signal.signal === "none") return;
  if (botPaused) return;
  if (stats.daily_pnl <= DAILY_LOSS_LIMIT) {
    botPaused = true;
    return;
  }

  const price = signal.signal === "yes" ? market.yes_ask : (100 - market.yes_bid);
  if (price <= 0 || price >= 100) return;
  const contracts = kellySize(signal.confidence, price);
  if (contracts <= 0) return;
  const stake = (price * contracts) / 100;

  const trade: Trade = {
    time: new Date().toISOString(),
    ticker: market.ticker,
    strategy: strategyName,
    side: signal.signal as "yes" | "no",
    price,
    contracts,
    stake: parseFloat(stake.toFixed(2)),
    outcome: "pending",  // stays pending until Kalshi settles the market
    profit: 0,
  };

  trades.unshift(trade);
  if (trades.length > 100) trades.pop();
  stats.total_trades++;
  recalcStats();
}

function settlePendingTrades(settledMarkets: KalshiMarket[]): void {
  // Build a map of ticker -> result for quick lookup
  const resultMap = new Map<string, string>();
  for (const m of settledMarkets) {
    if (m.result) {
      resultMap.set(m.ticker, m.result);
    }
  }

  for (const trade of trades) {
    if (trade.outcome !== "pending") continue;

    const result = resultMap.get(trade.ticker);
    if (!result) continue; // market hasn't settled yet

    // Settle the trade based on the actual market result
    if (trade.side === result) {
      // We bet YES and result is YES, or we bet NO and result is NO → win
      trade.outcome = "win";
      trade.profit = parseFloat((((100 - trade.price) * trade.contracts) / 100).toFixed(2));
    } else {
      // Wrong side → lose the stake
      trade.outcome = "loss";
      trade.profit = parseFloat((-trade.stake).toFixed(2));
    }
  }

  recalcStats();
}

function recalcStats(): void {
  const completed = trades.filter((t) => t.outcome !== "pending");
  stats.pending = trades.filter((t) => t.outcome === "pending").length;
  stats.wins = completed.filter((t) => t.outcome === "win").length;
  stats.losses = completed.filter((t) => t.outcome === "loss").length;
  stats.total_trades = trades.length;
  stats.win_rate = completed.length > 0
    ? parseFloat(((stats.wins / completed.length) * 100).toFixed(1))
    : 0;
  stats.total_pnl = parseFloat(
    completed.reduce((sum, t) => sum + t.profit, 0).toFixed(2)
  );
  stats.daily_pnl = stats.total_pnl;
  stats.bot_paused = botPaused;
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

  // Fetch settled markets (enough to cover any pending trades)
  const settledMarkets = await fetchKalshiMarkets("settled", 20);
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

  // Settle any pending paper trades against real Kalshi results
  settlePendingTrades(settledMarkets);

  // Evaluate strategies
  const yesBid = currentMarket?.yes_bid ?? 50;
  const yesAsk = currentMarket?.yes_ask ?? 50;

  const ofi = await fetchOFI();
  const secondsRemaining = currentMarket?.seconds_remaining ?? 999;

  const momentum = evalMomentum(mom1m, mom5m);
  const meanReversion = evalMeanReversion(mom1m, mom5m, yesBid, yesAsk);
  const consensus = evalConsensus(mom1m, yesBid, yesAsk, ofi);
  const resolutionRider = evalResolutionRider(secondsRemaining, yesBid, yesAsk, mom1m);
  const favoriteBias = evalFavoriteBias(yesBid, yesAsk, secondsRemaining);

  // Fire trades for any strategy that signals
  const strategyMap: Record<string, StrategySignal> = {
    consensus,
    resolution_rider: resolutionRider,
    favorite_bias: favoriteBias,
    momentum,
    mean_reversion: meanReversion,
  };

  for (const [name, signal] of Object.entries(strategyMap)) {
    if (signal.signal !== "none" && currentMarket) {
      const alreadyTraded = trades.some(
        (t) => t.ticker === currentMarket!.ticker && t.strategy === name
      );
      if (!alreadyTraded) {
        openPaperTrade(signal, currentMarket, name);
      }
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
      resolution_rider: resolutionRider,
      favorite_bias: favoriteBias,
    },
    ofi,
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
