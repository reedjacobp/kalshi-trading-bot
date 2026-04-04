#!/usr/bin/env python3
"""
Kalshi 15-Minute Crypto Trading Bot

Main entry point. Runs a polling loop that:
1. Discovers the current open 15-min crypto market
2. Feeds real-time BTC/ETH prices into strategy engines
3. Executes trades when strategies generate consensus signals
4. Tracks P&L, settles positions, and enforces risk limits

Supports paper trading (default) and live trading.

Usage:
    # Paper trading (default)
    python bot.py

    # Live trading (requires API keys and PAPER_TRADE=false in .env)
    python bot.py --live

    # Specify strategy
    python bot.py --strategy consensus
    python bot.py --strategy momentum
    python bot.py --strategy mean_reversion
    python bot.py --strategy all
"""

import argparse
import csv
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from kalshi_client import KalshiClient
from market_scanner import MarketScanner
from price_feed import PriceFeed
from risk_manager import RiskConfig, RiskManager, TradeRecord
from strategies import (
    ConsensusStrategy,
    MeanReversionStrategy,
    MomentumStrategy,
    Signal,
)


# ─── Configuration ───────────────────────────────────────────────────────────

def load_config():
    """Load configuration from .env and CLI args."""
    load_dotenv()

    parser = argparse.ArgumentParser(description="Kalshi 15-min Crypto Trading Bot")
    parser.add_argument("--live", action="store_true", help="Enable live trading (overrides .env)")
    parser.add_argument("--strategy", type=str, default=None, help="Strategy: momentum, mean_reversion, consensus, all")
    parser.add_argument("--series", type=str, default=None, help="Market series (e.g. KXBTC15M, KXETH15M)")
    parser.add_argument("--stake", type=float, default=None, help="Stake per trade in USD")
    args = parser.parse_args()

    config = {
        "api_key_id": os.getenv("KALSHI_API_KEY_ID", ""),
        "private_key_path": os.getenv("KALSHI_PRIVATE_KEY_PATH", ""),
        "env": os.getenv("KALSHI_ENV", "demo"),
        "series": args.series or os.getenv("MARKET_SERIES", "KXBTC15M"),
        "stake_usd": args.stake or float(os.getenv("STAKE_USD", "5.00")),
        "max_daily_loss": float(os.getenv("MAX_DAILY_LOSS_USD", "25.00")),
        "max_concurrent": int(os.getenv("MAX_CONCURRENT_POSITIONS", "3")),
        "poll_interval": int(os.getenv("POLL_INTERVAL_SECONDS", "5")),
        "strategy": args.strategy or os.getenv("STRATEGY", "consensus"),
        "paper_trade": not args.live and os.getenv("PAPER_TRADE", "true").lower() == "true",
    }

    # Determine crypto symbol from series
    if "BTC" in config["series"]:
        config["crypto_symbol"] = "BTC-USD"
    elif "ETH" in config["series"]:
        config["crypto_symbol"] = "ETH-USD"
    else:
        config["crypto_symbol"] = "BTC-USD"

    return config


# ─── Trade Logger ────────────────────────────────────────────────────────────

class TradeLogger:
    """Logs trades to CSV and provides a live display."""

    def __init__(self, csv_path: str = "data/trades.csv"):
        self.csv_path = Path(csv_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_csv()

    def _init_csv(self):
        if not self.csv_path.exists():
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "time", "strategy", "ticker", "side", "price_cents",
                    "contracts", "stake_usd", "order_id", "outcome",
                    "payout_usd", "profit_usd", "reason",
                ])

    def log_trade(self, record: TradeRecord, reason: str = ""):
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.fromtimestamp(record.timestamp, tz=timezone.utc).isoformat(),
                record.strategy,
                record.ticker,
                record.side,
                record.price_cents,
                record.contracts,
                f"{record.stake_usd:.2f}",
                record.order_id,
                record.outcome,
                f"{record.payout_usd:.2f}",
                f"{record.profit_usd:.2f}",
                reason,
            ])

    def log_settlement(self, record: TradeRecord):
        """Update the CSV with settlement info (appends a settlement row)."""
        self.log_trade(record, reason=f"SETTLED:{record.outcome}")


# ─── Display ─────────────────────────────────────────────────────────────────

def display_status(
    market, scanner, price_feed, risk_mgr, strategies_status, config
):
    """Print a single-line status update to the terminal."""
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    mode = "PAPER" if config["paper_trade"] else "LIVE"

    if market:
        ticker = market.get("ticker", "???")
        secs = scanner.seconds_until_close(market)
        yes_bid, yes_ask = scanner.parse_yes_price(market)
        bid_str = f"{yes_bid}" if yes_bid is not None else "—"
        ask_str = f"{yes_ask}" if yes_ask is not None else "—"
        market_str = f"{ticker} ({secs:.0f}s) yes={bid_str}/{ask_str}"
    else:
        market_str = "No open market"

    btc_price = price_feed.current_price
    btc_str = f"{config['crypto_symbol'].split('-')[0]}=${btc_price:,.0f}" if btc_price else "—"
    mom_1m = price_feed.momentum_1m()
    mom_str = f"{mom_1m:+.3f}%" if mom_1m is not None else "—"

    stats = risk_mgr.stats_summary()

    # Build strategy signals line
    sig_parts = []
    for name, rec in strategies_status.items():
        if rec.should_trade:
            sig_parts.append(f"{name[0].upper()}:{rec.signal.value}")
        else:
            sig_parts.append(f"{name[0].upper()}:—")
    sigs = " ".join(sig_parts)

    print(
        f"\r[{now}] [{mode}] {market_str} | {btc_str} ({mom_str}) | {sigs} | {stats}",
        end="", flush=True,
    )


# ─── Paper Trade Executor ────────────────────────────────────────────────────

class PaperExecutor:
    """Simulates order execution for paper trading."""

    def place_order(self, ticker, side, count, price_cents, **kwargs):
        order_id = f"PAPER-{uuid.uuid4().hex[:12]}"
        return {
            "order": {
                "order_id": order_id,
                "status": "filled",
                "client_order_id": kwargs.get("client_order_id", ""),
            }
        }


# ─── Main Bot Loop ───────────────────────────────────────────────────────────

class TradingBot:
    """Main trading bot orchestrator."""

    def __init__(self, config: dict):
        self.config = config
        self.running = True

        # Set up components
        if config["paper_trade"]:
            # Paper mode: use unauthenticated client for market data only
            self.client = None
            self.executor = PaperExecutor()
            print(f"[INIT] Paper trading mode — no API keys needed for market data")
        else:
            if not config["api_key_id"] or not config["private_key_path"]:
                print("ERROR: Live trading requires KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH in .env")
                sys.exit(1)
            self.client = KalshiClient(
                key_id=config["api_key_id"],
                private_key_path=config["private_key_path"],
                env=config["env"],
            )
            self.executor = self.client
            print(f"[INIT] LIVE trading on {config['env']} environment")

        # For paper trading, we create a minimal client for public endpoints
        if self.client is None:
            from kalshi_client import DEMO_BASE_URL, PROD_BASE_URL
            import requests

            class PublicOnlyClient:
                """Minimal client that only calls public (unauthenticated) endpoints."""
                def __init__(self, env):
                    self.base_url = DEMO_BASE_URL if env == "demo" else PROD_BASE_URL
                    self.session = requests.Session()

                def _public_get(self, path, params=None):
                    url = f"{self.base_url}{path}"
                    resp = self.session.get(url, params=params)
                    resp.raise_for_status()
                    return resp.json()

                def get_markets(self, **kwargs):
                    params = {k: v for k, v in kwargs.items() if v is not None}
                    return self._public_get("/markets", params=params)

                def get_market(self, ticker):
                    return self._public_get(f"/markets/{ticker}")

                def get_orderbook(self, ticker, depth=10):
                    return self._public_get(f"/markets/{ticker}/orderbook", params={"depth": depth})

                def get_trades(self, **kwargs):
                    params = {k: v for k, v in kwargs.items() if v is not None}
                    return self._public_get("/markets/trades", params=params)

                def get_events(self, **kwargs):
                    params = {k: v for k, v in kwargs.items() if v is not None}
                    return self._public_get("/events", params=params)

            self.client = PublicOnlyClient(config["env"])

        self.scanner = MarketScanner(self.client, series=config["series"])
        self.price_feed = PriceFeed(
            symbol=config["crypto_symbol"], window_seconds=1200
        )

        # Risk manager
        risk_config = RiskConfig(
            stake_usd=config["stake_usd"],
            max_daily_loss_usd=config["max_daily_loss"],
            max_concurrent_positions=config["max_concurrent"],
        )
        self.risk_mgr = RiskManager(risk_config)

        # Strategies
        self.strategies = self._init_strategies(config["strategy"])

        # Logger
        csv_name = "paper_trades.csv" if config["paper_trade"] else "live_trades.csv"
        self.logger = TradeLogger(f"data/{csv_name}")

        # Track which markets we've already traded on
        self._traded_tickers: set = set()
        self._last_settled_ticker: str = ""

    def _init_strategies(self, strategy_name: str) -> dict:
        """Initialize the selected strategy or all strategies."""
        all_strats = {
            "momentum": MomentumStrategy(),
            "mean_reversion": MeanReversionStrategy(),
            "consensus": ConsensusStrategy(),
        }
        if strategy_name == "all":
            return all_strats
        if strategy_name in all_strats:
            return {strategy_name: all_strats[strategy_name]}
        print(f"WARNING: Unknown strategy '{strategy_name}', defaulting to consensus")
        return {"consensus": all_strats["consensus"]}

    def run(self):
        """Main event loop."""
        print(f"[START] Kalshi Trading Bot")
        print(f"  Series:     {self.config['series']}")
        print(f"  Strategies: {', '.join(self.strategies.keys())}")
        print(f"  Stake:      ${self.config['stake_usd']:.2f}/trade")
        print(f"  Max Loss:   ${self.config['max_daily_loss']:.2f}/day")
        print(f"  Mode:       {'PAPER' if self.config['paper_trade'] else 'LIVE'}")
        print(f"  Polling:    every {self.config['poll_interval']}s")
        print(f"{'='*72}")
        print("Warming up price feed (collecting 60s of data)...")

        # Warm up the price feed
        warmup_end = time.time() + 60
        while time.time() < warmup_end and self.running:
            self.price_feed.fetch_price()
            p = self.price_feed.current_price
            if p:
                remaining = warmup_end - time.time()
                print(f"\r  {self.config['crypto_symbol'].split('-')[0]} = ${p:,.2f} | {remaining:.0f}s remaining...", end="", flush=True)
            time.sleep(3)
        print(f"\n{'='*72}")
        print("Bot is now live. Press Ctrl+C to stop.\n")

        try:
            while self.running:
                self._tick()
                time.sleep(self.config["poll_interval"])
        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _tick(self):
        """Single iteration of the main loop."""
        # 1. Fetch latest crypto price
        self.price_feed.fetch_price()

        # 2. Find the current market
        market = self.scanner.get_next_expiring_market()
        last_settled = self.scanner.get_last_settled_market()

        # 3. Check for settlements
        self._check_settlements()

        # 4. Evaluate strategies
        strategies_status = {}
        for name, strategy in self.strategies.items():
            rec = strategy.evaluate(market, last_settled, self.price_feed, self.scanner)
            strategies_status[name] = rec

        # 5. Display status
        display_status(market, self.scanner, self.price_feed, self.risk_mgr, strategies_status, self.config)

        # 6. Execute trades if any strategy fires
        if market:
            ticker = market.get("ticker", "")
            if ticker and ticker not in self._traded_tickers:
                self._maybe_trade(market, strategies_status)

    def _maybe_trade(self, market: dict, strategies_status: dict):
        """Check if any strategy wants to trade and execute if approved."""
        ticker = market.get("ticker", "")

        for name, rec in strategies_status.items():
            if not rec.should_trade:
                continue

            side = rec.signal.value  # "yes" or "no"
            price_cents = rec.max_price_cents

            # Risk check
            approved, reason = self.risk_mgr.approve_trade(
                ticker=ticker,
                strategy_name=name,
                side=side,
                confidence=rec.confidence,
                price_cents=price_cents,
            )
            if not approved:
                continue

            # Calculate position size
            contracts = self.risk_mgr.calculate_contracts(price_cents)
            stake = contracts * (price_cents / 100.0)

            # Execute
            print(f"\n  >>> {name.upper()}: BUY {contracts} {side.upper()} @ {price_cents}c on {ticker} (${stake:.2f})")
            print(f"      Reason: {rec.reason}")

            try:
                client_oid = str(uuid.uuid4())

                if self.config["paper_trade"]:
                    result = self.executor.place_order(
                        ticker=ticker,
                        side=side,
                        count=contracts,
                        price_cents=price_cents,
                        client_order_id=client_oid,
                    )
                else:
                    result = self.executor.place_order(
                        ticker=ticker,
                        action="buy",
                        side=side,
                        count=contracts,
                        order_type="limit",
                        yes_price=price_cents if side == "yes" else (100 - price_cents),
                        client_order_id=client_oid,
                    )

                order_id = result.get("order", {}).get("order_id", client_oid)
                print(f"      Order placed: {order_id}")

                # Record trade
                record = TradeRecord(
                    timestamp=time.time(),
                    ticker=ticker,
                    strategy=name,
                    side=side,
                    price_cents=price_cents,
                    contracts=contracts,
                    stake_usd=stake,
                    order_id=order_id,
                    client_order_id=client_oid,
                )
                self.risk_mgr.record_trade(record)
                self.logger.log_trade(record, reason=rec.reason)
                self._traded_tickers.add(ticker)

            except Exception as e:
                print(f"\n      ERROR placing order: {e}")

    def _check_settlements(self):
        """Check if any open positions have settled."""
        if not self.risk_mgr.open_positions:
            return

        settled_markets = self.scanner.get_settled_markets(limit=20)
        for mkt in settled_markets:
            ticker = mkt.get("ticker", "")
            result = mkt.get("result")
            if ticker in self.risk_mgr.open_positions and result:
                self.risk_mgr.settle_trade(ticker, result)
                record = next(
                    (t for t in self.risk_mgr.trades if t.ticker == ticker and t.outcome != ""),
                    None,
                )
                if record:
                    emoji = "WIN" if record.outcome == "win" else "LOSS"
                    print(f"\n  <<< SETTLED {ticker}: {emoji} (${record.profit_usd:+.2f})")
                    self.logger.log_settlement(record)

    def _shutdown(self):
        """Clean shutdown with final stats."""
        self.running = False
        print(f"\n\n{'='*72}")
        print("BOT STOPPED — Final Statistics")
        print(f"{'='*72}")

        completed = [t for t in self.risk_mgr.trades if t.outcome != ""]
        pending = [t for t in self.risk_mgr.trades if t.outcome == ""]

        if completed:
            wins = sum(1 for t in completed if t.outcome == "win")
            losses = len(completed) - wins
            wr = wins / len(completed) * 100

            total_staked = sum(t.stake_usd for t in completed)
            total_pnl = sum(t.profit_usd for t in completed)

            print(f"  Completed trades: {len(completed)} ({wins}W / {losses}L)")
            print(f"  Win rate:         {wr:.1f}%")
            print(f"  Total staked:     ${total_staked:.2f}")
            print(f"  Total P&L:        ${total_pnl:+.2f}")
            print(f"  ROI:              {(total_pnl/total_staked*100) if total_staked > 0 else 0:+.1f}%")

            # Per-strategy breakdown
            strat_names = set(t.strategy for t in completed)
            for sn in sorted(strat_names):
                strat_trades = [t for t in completed if t.strategy == sn]
                sw = sum(1 for t in strat_trades if t.outcome == "win")
                sl = len(strat_trades) - sw
                sp = sum(t.profit_usd for t in strat_trades)
                print(f"    {sn:20s}: {sw}W/{sl}L  ${sp:+.2f}")
        else:
            print("  No completed trades.")

        if pending:
            print(f"\n  Pending trades: {len(pending)}")
            for t in pending:
                print(f"    {t.ticker} ({t.strategy}) {t.side} @ {t.price_cents}c x{t.contracts}")

        print(f"\n  Trade log: {self.logger.csv_path}")
        print(f"{'='*72}")

    def stop(self):
        """Signal the bot to stop."""
        self.running = False


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    config = load_config()
    bot = TradingBot(config)

    # Handle SIGINT/SIGTERM
    def handle_signal(signum, frame):
        bot.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    bot.run()


if __name__ == "__main__":
    main()
