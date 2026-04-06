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
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

import requests as _requests
from dotenv import load_dotenv

from kalshi_client import KalshiClient
from market_scanner import MarketScanner
from rti_feed import RTIFeed
from risk_manager import RiskConfig, RiskManager, TradeRecord, kalshi_taker_fee
from strategies import (
    ConsensusStrategy,
    FavoriteBiasStrategy,
    MeanReversionStrategy,
    MomentumStrategy,
    ResolutionRiderStrategy,
    Signal,
)
from strategies.early_exit import EarlyExitMonitor
from multi_feed import MultiExchangeFeed
from vol_regime import VolRegimeDetector
from bayesian_updater import BayesianUpdater
from kl_divergence import KLDivergenceSignal


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


# ─── Logging Setup ──────────────────────────────────────────────────────────

def setup_file_logger(log_dir: str = "data/logs") -> logging.Logger:
    """Set up a file logger that persists across terminal sessions."""
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = log_path / f"bot_{date_str}.log"

    logger = logging.getLogger("kalshi_bot")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers on re-init
    if not logger.handlers:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        ))
        logger.addHandler(fh)

    return logger


# ─── Trade Logger ────────────────────────────────────────────────────────────

class TradeLogger:
    """Logs trades to CSV and provides a live display."""

    CSV_COLUMNS = [
        "time", "run_id", "strategy", "ticker", "side", "price_cents",
        "contracts", "stake_usd", "order_id", "outcome",
        "payout_usd", "profit_usd", "reason",
    ]

    def __init__(self, csv_path: str = "data/trades.csv", run_id: str = ""):
        self.csv_path = Path(csv_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self._migrate_csv()
        self._init_csv()

    def _init_csv(self):
        if not self.csv_path.exists():
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(self.CSV_COLUMNS)

    def _migrate_csv(self):
        """Add run_id column to existing CSVs that don't have it."""
        if not self.csv_path.exists():
            return
        with open(self.csv_path, "r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None or "run_id" in header:
                return
            rows = list(reader)
        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_COLUMNS)
            for row in rows:
                # Insert empty run_id after the time column
                row.insert(1, "")
                writer.writerow(row)

    def log_trade(self, record: TradeRecord, reason: str = ""):
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.fromtimestamp(record.timestamp, tz=timezone.utc).isoformat(),
                self.run_id,
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

    def get_unsettled_trades(self) -> list[dict]:
        """Read the CSV and return trades that have no matching settlement row."""
        if not self.csv_path.exists():
            return []
        with open(self.csv_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Collect all tickers that have a SETTLED row
        settled_tickers = set()
        for row in rows:
            reason = row.get("reason", "")
            if reason.startswith("SETTLED:"):
                settled_tickers.add(row["ticker"])

        # Return trade rows (not settlement rows) that haven't been settled
        unsettled = []
        for row in rows:
            reason = row.get("reason", "")
            if not reason.startswith("SETTLED:") and row["ticker"] not in settled_tickers:
                unsettled.append(row)
        return unsettled


# ─── OFI (Order Flow Imbalance) from Crypto.com ─────────────────────────────

_ofi_session = _requests.Session()
_last_ofi: float = 0.0


def fetch_ofi() -> float:
    """Fetch order flow imbalance from Crypto.com BTC orderbook."""
    global _last_ofi
    try:
        resp = _ofi_session.get(
            "https://api.crypto.com/exchange/v1/public/get-book"
            "?instrument_name=BTC_USD&depth=10",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        book = data.get("result", {}).get("data", [{}])[0] if isinstance(
            data.get("result", {}).get("data"), list
        ) else data.get("result", {})

        bids = book.get("bids", [])
        asks = book.get("asks", [])

        bid_vol = sum(float(b[1]) / (i + 1) for i, b in enumerate(bids[:5]))
        ask_vol = sum(float(a[1]) / (i + 1) for i, a in enumerate(asks[:5]))
        total = bid_vol + ask_vol
        if total == 0:
            return _last_ofi
        _last_ofi = (bid_vol - ask_vol) / total
        return _last_ofi
    except Exception:
        return _last_ofi


# ─── SSE Server ─────────────────────────────────────────────────────────────

class _SSEState:
    """Shared mutable state between the bot loop and SSE server."""
    tick_data: str = ""  # JSON string of the latest TickData
    enabled_assets: dict = None  # {"btc": True, "eth": True, "sol": True}
    trading_enabled: bool = True  # Global trading on/off switch
    lock = threading.Lock()

    def __init__(self):
        self.enabled_assets = {"btc": True, "eth": True, "sol": True}
        self.trading_enabled = True


_sse_state = _SSEState()


class SSEHandler(BaseHTTPRequestHandler):
    """Serves the /api/stream SSE endpoint, /api/health, and control endpoints."""

    def do_GET(self):
        if self.path == "/api/stream":
            self._handle_stream()
        elif self.path == "/api/health":
            self._json_response(200, {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})
        else:
            self._json_response(404, {"error": "not found"})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length)) if content_length else {}

        if self.path == "/api/toggle-asset":
            asset = body.get("asset", "").lower()
            enabled = body.get("enabled")
            if asset in ("btc", "eth", "sol") and isinstance(enabled, bool):
                with _sse_state.lock:
                    _sse_state.enabled_assets[asset] = enabled
                self._json_response(200, {"ok": True, "enabled_assets": _sse_state.enabled_assets})
            else:
                self._json_response(400, {"error": "invalid asset or enabled value"})
        elif self.path == "/api/toggle-trading":
            enabled = body.get("enabled")
            if isinstance(enabled, bool):
                with _sse_state.lock:
                    _sse_state.trading_enabled = enabled
                self._json_response(200, {"ok": True, "trading_enabled": _sse_state.trading_enabled})
            else:
                self._json_response(400, {"error": "invalid enabled value"})
        else:
            self._json_response(404, {"error": "not found"})

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _handle_stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        last_sent = ""
        try:
            # Send current state immediately on connect
            with _sse_state.lock:
                data = _sse_state.tick_data
            if data:
                self.wfile.write(f"data: {data}\n\n".encode())
                self.wfile.flush()
                last_sent = data

            while True:
                with _sse_state.lock:
                    data = _sse_state.tick_data
                if data and data != last_sent:
                    self.wfile.write(f"data: {data}\n\n".encode())
                    self.wfile.flush()
                    last_sent = data
                time.sleep(1)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _json_response(self, status: int, body: dict):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        pass  # Suppress default request logging


class _ThreadingSSEServer(ThreadingMixIn, HTTPServer):
    """HTTPServer that handles each connection in its own thread."""
    daemon_threads = True


def start_sse_server(port: int = 5050):
    """Start the SSE HTTP server in a background daemon thread."""
    server = _ThreadingSSEServer(("0.0.0.0", port), SSEHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


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
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # Set up persistent file logger
        self.file_log = setup_file_logger()
        self._log(f"=== Bot starting: run_id={self.run_id} mode={'PAPER' if config['paper_trade'] else 'LIVE'} ===")

        # Set up components
        if config["paper_trade"]:
            # Paper mode: use unauthenticated client for market data only
            self.client = None
            self.executor = PaperExecutor()
            self._log("[INIT] Paper trading mode — no API keys needed for market data")
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
            self._log(f"[INIT] LIVE trading on {config['env']} environment")

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

        # Multi-asset: scanners and price feeds for all three series
        # Use RTIFeed (CF Benchmarks RTI approximation) instead of single-exchange
        # PriceFeed — aggregates Coinbase, Kraken, Bitstamp, Gemini, Binance
        # with volume weighting and outlier filtering to match Kalshi's settlement index
        self.assets = {
            "btc": {
                "series": "KXBTC15M",
                "symbol": "BTC-USD",
                "scanner": MarketScanner(self.client, series="KXBTC15M"),
                "price_feed": RTIFeed(symbol="BTC-USD", window_seconds=1200),
            },
            "eth": {
                "series": "KXETH15M",
                "symbol": "ETH-USD",
                "scanner": MarketScanner(self.client, series="KXETH15M"),
                "price_feed": RTIFeed(symbol="ETH-USD", window_seconds=1200),
            },
            "sol": {
                "series": "KXSOL15M",
                "symbol": "SOL-USD",
                "scanner": MarketScanner(self.client, series="KXSOL15M"),
                "price_feed": RTIFeed(symbol="SOL-USD", window_seconds=1200),
            },
        }

        # Multi-exchange feeds for cross-exchange signals
        self.multi_feeds = {
            key: MultiExchangeFeed(symbol=a["symbol"])
            for key, a in self.assets.items()
        }

        # Keep backwards-compat references for strategies (use configured primary asset)
        primary = "btc" if "BTC" in config["series"] else "eth" if "ETH" in config["series"] else "sol"
        self.scanner = self.assets[primary]["scanner"]
        self.price_feed = self.assets[primary]["price_feed"]
        self.display_feeds = {a["symbol"]: a["price_feed"] for a in self.assets.values()}

        # Risk manager
        risk_config = RiskConfig(
            stake_usd=config["stake_usd"],
            max_daily_loss_usd=config["max_daily_loss"],
            max_concurrent_positions=config["max_concurrent"],
        )
        self.risk_mgr = RiskManager(risk_config)

        # Strategies
        self.strategies = self._init_strategies(config["strategy"])

        # Early exit monitor for open positions
        self.exit_monitor = EarlyExitMonitor(
            stop_loss_cents=15,
            take_profit_cents=10,
        )

        # Volatility regime detector
        self.vol_detector = VolRegimeDetector()
        self._current_regime = None
        self._regime_params = None

        # Bayesian probability updater — dynamically adjusts confidence
        # as new price evidence arrives during each 15-min window
        self.bayesian = BayesianUpdater()

        # KL-divergence cross-asset signal — detects mispricings between
        # BTC/ETH/SOL contract prices based on their correlation
        self.kl_signal = KLDivergenceSignal()

        # Logger
        csv_name = "paper_trades.csv" if config["paper_trade"] else "live_trades.csv"
        self.logger = TradeLogger(f"data/{csv_name}", run_id=self.run_id)

        # Track which markets we've already traded on
        self._traded_tickers: set = set()
        self._last_settled_ticker: str = ""

        # Resolve unsettled trades from previous runs
        self._resolve_unsettled_trades()

        # Dashboard trade history (matches the Trade shape the frontend expects)
        self._dashboard_trades: list[dict] = []

        # Start SSE server for dashboard
        sse_port = int(os.getenv("SSE_PORT", "5050"))
        self._sse_server = start_sse_server(port=sse_port)
        self._log(f"[INIT] SSE server started on port {sse_port}")

    def _log(self, msg: str, level: str = "info"):
        """Print to terminal AND write to persistent log file."""
        print(msg)
        getattr(self.file_log, level, self.file_log.info)(msg)

    def _get_balance(self) -> float:
        """Fetch account balance. Returns USD available, or fallback for paper mode."""
        if self.config["paper_trade"]:
            # Paper mode: simulate a starting balance minus open stakes
            paper_balance = float(os.getenv("PAPER_BALANCE", "100.00"))
            open_stake = sum(t.stake_usd for t in self.risk_mgr.trades if t.outcome == "")
            return max(0, paper_balance + self.risk_mgr.total_pnl - open_stake)
        try:
            resp = self.client.get_balance()
            # Kalshi returns balance in cents
            return resp.get("balance", 0) / 100.0
        except Exception as e:
            self._log(f"  [WARN] Failed to fetch balance: {e}", level="warning")
            return 0.0

    def _init_strategies(self, strategy_name: str) -> dict:
        """Initialize the selected strategy or all strategies."""
        all_strats = {
            "momentum": MomentumStrategy(),
            "mean_reversion": MeanReversionStrategy(),
            "consensus": ConsensusStrategy(),
            "resolution_rider": ResolutionRiderStrategy(),
            "favorite_bias": FavoriteBiasStrategy(
                min_favorite_price=75,  # Raised from 70 — skip soft favorites
                max_entry_price=80,     # Don't overpay for BTC
                asset_overrides={
                    "KXETH": {"min_fav": 80, "max_entry": 84},  # Tightened max_entry from 85
                    "KXSOL": {"min_fav": 85, "max_entry": 89},  # Tightened max_entry from 90
                },
            ),
        }
        if strategy_name == "all":
            return all_strats
        if strategy_name in all_strats:
            return {strategy_name: all_strats[strategy_name]}
        print(f"WARNING: Unknown strategy '{strategy_name}', defaulting to consensus")
        return {"consensus": all_strats["consensus"]}

    def _resolve_unsettled_trades(self):
        """On startup, check if any trades from previous runs can be settled now."""
        unsettled = self.logger.get_unsettled_trades()
        if not unsettled:
            return

        self._log(f"[INIT] Found {len(unsettled)} unsettled trades from previous runs, resolving...")

        settled_markets = self.scanner.get_settled_markets(limit=200)
        settled_by_ticker = {m["ticker"]: m.get("result") for m in settled_markets}

        resolved = 0
        for trade_row in unsettled:
            ticker = trade_row["ticker"]
            result = settled_by_ticker.get(ticker)
            if not result:
                continue

            side = trade_row["side"]
            contracts = int(trade_row["contracts"])
            stake_usd = float(trade_row["stake_usd"])

            record = TradeRecord(
                timestamp=time.time(),
                ticker=ticker,
                strategy=trade_row["strategy"],
                side=side,
                price_cents=int(trade_row["price_cents"]),
                contracts=contracts,
                stake_usd=stake_usd,
                order_id=trade_row["order_id"],
            )

            if side == result:
                record.outcome = "win"
                record.payout_usd = contracts * 1.00
                record.profit_usd = record.payout_usd - stake_usd
            else:
                record.outcome = "loss"
                record.payout_usd = 0.0
                record.profit_usd = -stake_usd

            self.logger.log_settlement(record)
            emoji = "WIN" if record.outcome == "win" else "LOSS"
            self._log(f"  [RESOLVED] {ticker}: {emoji} (${record.profit_usd:+.2f})")
            resolved += 1

        self._log(f"[INIT] Resolved {resolved}/{len(unsettled)} trades")

    def run(self):
        """Main event loop."""
        self._log(f"[START] Kalshi Trading Bot (run_id={self.run_id})")
        self._log(f"  Series:     {self.config['series']}")
        self._log(f"  Strategies: {', '.join(self.strategies.keys())}")
        self._log(f"  Stake:      ${self.config['stake_usd']:.2f}/trade")
        self._log(f"  Max Loss:   ${self.config['max_daily_loss']:.2f}/day")
        self._log(f"  Mode:       {'PAPER' if self.config['paper_trade'] else 'LIVE'}")
        self._log(f"  Polling:    every {self.config['poll_interval']}s")
        print(f"{'='*72}")
        print("Warming up price feed (collecting 60s of data)...")

        # Warm up all price feeds — also publish tick data so the dashboard works during warmup
        warmup_end = time.time() + 60
        while time.time() < warmup_end and self.running:
            for asset in self.assets.values():
                asset["price_feed"].fetch_price()
            p = self.assets["btc"]["price_feed"].current_price
            if p:
                remaining = warmup_end - time.time()
                print(f"\r  BTC=${p:,.0f} | {remaining:.0f}s remaining...", end="", flush=True)
                # Publish a minimal tick so the dashboard shows data during warmup
                no_signals = {
                    name: type("R", (), {
                        "signal": Signal.NO_TRADE, "confidence": 0,
                        "reason": "Warming up...", "should_trade": False,
                    })()
                    for name in self.strategies
                }
                all_markets = {k: a["scanner"].get_next_expiring_market() for k, a in self.assets.items()}
                all_last_settled = {k: None for k in self.assets}
                all_strategies = {k: no_signals for k in self.assets}
                self._publish_tick(all_markets, all_last_settled, all_strategies)
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
        # 1. Fetch latest crypto prices for all assets concurrently
        #    Pull from Coinbase (primary) + Binance/Kraken (multi-feed)
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=6) as pool:
            pool.map(lambda a: a["price_feed"].fetch_price(), self.assets.values())
            pool.map(lambda mf: mf.fetch_all(), self.multi_feeds.values())

        # 2. Detect volatility regime and apply dynamic parameters
        btc_feed = self.assets["btc"]["price_feed"]
        regime_params = self._regime_params = self.vol_detector.get_params(btc_feed)
        if self._current_regime != regime_params.regime:
            self._current_regime = regime_params.regime
            self._log(f"  [VOL] Regime: {regime_params.regime.value.upper()}")

        # Apply regime params to exit monitor
        self.exit_monitor.stop_loss_cents = regime_params.stop_loss_cents
        self.exit_monitor.trailing_distance = regime_params.trailing_distance
        self.exit_monitor.trailing_activation = regime_params.trailing_activation

        # Apply regime params to risk manager
        self.risk_mgr.config.kelly_fraction = regime_params.kelly_fraction
        self.risk_mgr.config.max_position_pct = regime_params.max_position_pct

        # Apply regime params to strategies
        for name, strategy in self.strategies.items():
            if name == "favorite_bias":
                strategy.min_favorite_price = regime_params.fav_min_favorite
                strategy.max_entry_price = regime_params.fav_max_entry
            elif name == "momentum":
                strategy.min_momentum_pct = regime_params.momentum_threshold
            elif name == "consensus":
                strategy.min_edge = regime_params.consensus_min_edge

        # 3. Iterate all assets: scan markets, evaluate strategies, trade
        all_markets = {}
        all_last_settled = {}
        all_strategies = {}

        for i, (key, asset) in enumerate(self.assets.items()):
            scanner = asset["scanner"]
            feed = asset["price_feed"]

            market = scanner.get_next_expiring_market()
            last_settled = scanner.get_last_settled_market()
            all_markets[key] = market
            all_last_settled[key] = last_settled

            # Check settlements for this scanner
            try:
                self._check_settlements_for(scanner)
            except Exception as e:
                self._log(f"  [WARN] Settlement check failed for {key}: {e}", level="warning")

            # Check open positions for early exit
            if market and not self.config["paper_trade"]:
                self._check_early_exits(market, scanner, feed)

            # Feed contract prices into KL-divergence signal
            if market:
                yes_bid_kl, yes_ask_kl = scanner.parse_yes_price(market)
                if yes_bid_kl is not None and yes_ask_kl is not None:
                    yes_mid = (yes_bid_kl + yes_ask_kl) // 2
                    self.kl_signal.update_price(key, yes_mid)

                # Feed Bayesian updater for any open positions on this market
                ticker_bayes = market.get("ticker", "")
                if ticker_bayes in self.risk_mgr.open_positions:
                    btc_price = feed.current_price
                    mom = feed.momentum_1m()
                    secs = scanner.seconds_until_close(market)
                    if btc_price:
                        posterior = self.bayesian.update(
                            ticker_bayes, btc_price,
                            momentum_1m=mom,
                            seconds_remaining=secs,
                            contract_price_cents=yes_mid if yes_bid_kl else None,
                        )
                        # Bayesian early exit signal — if posterior collapses
                        record = self.risk_mgr.open_positions[ticker_bayes]
                        if posterior is not None and self.bayesian.should_exit(ticker_bayes, record.price_cents / 100.0):
                            self._log(f"  [BAYES] Posterior collapsed to {posterior:.0%} for {ticker_bayes}, triggering early exit check")

            # Evaluate strategies against this asset's market + price feed
            # ETH/SOL only run favorite_bias; BTC runs all
            # Regime can disable strategies dynamically
            strats = {}
            for name, strategy in self.strategies.items():
                # Asset filter: ETH/SOL only run favorite_bias
                if key != "btc" and name != "favorite_bias":
                    strats[name] = type("R", (), {
                        "signal": Signal.NO_TRADE, "confidence": 0,
                        "reason": "Disabled for this asset", "should_trade": False,
                    })()
                # Regime filter: skip strategies disabled by volatility regime
                elif name == "favorite_bias" and not regime_params.fav_bias_enabled:
                    strats[name] = type("R", (), {
                        "signal": Signal.NO_TRADE, "confidence": 0,
                        "reason": f"Disabled in {regime_params.regime.value} vol regime",
                        "should_trade": False,
                    })()
                elif name == "momentum" and not regime_params.momentum_enabled:
                    strats[name] = type("R", (), {
                        "signal": Signal.NO_TRADE, "confidence": 0,
                        "reason": f"Disabled in {regime_params.regime.value} vol regime",
                        "should_trade": False,
                    })()
                else:
                    rec = strategy.evaluate(market, last_settled, feed, scanner)
                    strats[name] = rec
            all_strategies[key] = strats

            # Cross-asset signal boost using KL-divergence + BTC settlement
            if key != "btc":
                for name, rec in strats.items():
                    if not rec.should_trade:
                        continue

                    # KL-divergence boost: adjust confidence based on
                    # cross-asset mispricing detection
                    kl_boost = self.kl_signal.get_confidence_boost(
                        key, rec.signal.value
                    )
                    if kl_boost != 0:
                        rec.confidence = max(0.1, min(0.95, rec.confidence + kl_boost))
                        if kl_boost > 0:
                            rec.reason += f" [+KL {kl_boost:+.0%}]"
                        else:
                            rec.reason += f" [KL divergence {kl_boost:+.0%}]"

                    # BTC settlement boost (existing logic)
                    btc_settled = all_last_settled.get("btc")
                    if btc_settled and btc_settled.get("result"):
                        btc_result = btc_settled["result"]
                        if rec.signal.value == btc_result:
                            rec.confidence = min(0.95, rec.confidence + 0.05)
                            rec.reason += f" [+BTC {btc_result} boost]"

            # Execute trades if asset is enabled AND global trading is on
            with _sse_state.lock:
                asset_enabled = _sse_state.enabled_assets.get(key, True)
                trading_on = _sse_state.trading_enabled
            if market and asset_enabled and trading_on:
                ticker = market.get("ticker", "")
                if ticker and ticker not in self._traded_tickers:
                    self._maybe_trade(market, strats)

        # 3. Display status for primary asset
        primary_key = next(iter(self.assets))
        display_status(
            all_markets[primary_key], self.assets[primary_key]["scanner"],
            self.assets[primary_key]["price_feed"], self.risk_mgr,
            all_strategies[primary_key], self.config,
        )

        # 4. Publish tick data to SSE for dashboard
        self._publish_tick(all_markets, all_last_settled, all_strategies)

    def _maybe_trade(self, market: dict, strategies_status: dict):
        """Check if any strategy wants to trade and execute if approved."""
        ticker = market.get("ticker", "")
        balance = self._get_balance()

        # Get fresh book prices for order placement — fetch the LIVE
        # orderbook, not the cached /markets data, so our limit price
        # reflects the current best bid/ask.
        scanner = next(
            (a["scanner"] for a in self.assets.values()
             if a["scanner"].series in ticker),
            self.scanner,
        )
        try:
            book = scanner.client.get_orderbook(ticker, depth=1)
            yes_bids = book.get("orderbook", {}).get("yes", [])
            no_bids = book.get("orderbook", {}).get("no", [])
            # Orderbook prices are in cents: [[price_cents, quantity], ...]
            # Best YES bid = highest yes bid; Best YES ask = 100 - highest no bid
            yes_bid = yes_bids[0][0] if yes_bids else None
            yes_ask = (100 - no_bids[0][0]) if no_bids else None
            if yes_bid is not None and yes_ask is not None:
                self._log(f"      [BOOK] {ticker}: yes_bid={yes_bid} yes_ask={yes_ask}")
        except Exception:
            # Fall back to cached market data
            yes_bid, yes_ask = scanner.parse_yes_price(market)

        for name, rec in strategies_status.items():
            if not rec.should_trade:
                continue

            side = rec.signal.value  # "yes" or "no"
            max_price = rec.max_price_cents  # strategy's ceiling price

            # Determine the actual execution price from the live book.
            # YES buy: pay the yes_ask. NO buy: pay 100 - yes_bid (= NO ask).
            if side == "yes":
                exec_price = yes_ask if yes_ask is not None else max_price
            else:
                exec_price = (100 - yes_bid) if yes_bid is not None else max_price

            # Don't trade if live book price exceeds strategy's max
            if exec_price > max_price:
                continue

            # Add spread-crossing buffer: bump the limit price by a few
            # cents so the order crosses the spread instead of resting at
            # exactly the ask.  Cap at the strategy's max to stay within
            # its risk parameters.
            SPREAD_BUFFER_CENTS = 3
            exec_price = min(exec_price + SPREAD_BUFFER_CENTS, max_price)

            # Risk check using actual execution price
            approved, reason = self.risk_mgr.approve_trade(
                ticker=ticker,
                strategy_name=name,
                side=side,
                confidence=rec.confidence,
                price_cents=exec_price,
                balance_usd=balance,
            )
            if not approved:
                continue

            # Kelly-sized position based on balance and confidence
            contracts = self.risk_mgr.calculate_contracts(
                exec_price, confidence=rec.confidence, balance_usd=balance
            )
            stake = contracts * (exec_price / 100.0)

            # Don't trade if stake exceeds available balance
            if not self.config["paper_trade"] and stake > balance:
                self._log(f"  [SKIP] {name}: stake ${stake:.2f} exceeds balance ${balance:.2f}")
                continue

            # Fee-adjusted EXPECTED VALUE check
            # EV = confidence * (payout - stake - fees) - (1 - confidence) * (stake + entry_fee)
            entry_fee = kalshi_taker_fee(contracts, exec_price)
            settle_fee = kalshi_taker_fee(contracts, 100 - exec_price)
            payout = contracts * 1.00
            ev_win = rec.confidence * (payout - stake - entry_fee - settle_fee)
            ev_loss = (1 - rec.confidence) * (stake + entry_fee)
            expected_value = ev_win - ev_loss
            if expected_value <= 0:
                self._log(f"  [SKIP] {name}: negative EV after fees "
                          f"(EV=${expected_value:.2f}, conf={rec.confidence:.0%}, "
                          f"fees=${entry_fee + settle_fee:.2f})")
                continue

            # Execute — mark ticker BEFORE placing order to prevent re-entry
            self._traded_tickers.add(ticker)

            # For Kalshi API: yes_price is always in YES terms
            api_yes_price = exec_price if side == "yes" else (100 - exec_price)

            self._log(f"\n  >>> {name.upper()}: BUY {contracts} {side.upper()} @ {exec_price}c on {ticker} "
                      f"(${stake:.2f}, fees=${entry_fee:.2f}, bal=${balance:.2f})")
            self._log(f"      Reason: {rec.reason}")

            try:
                client_oid = str(uuid.uuid4())

                if self.config["paper_trade"]:
                    result = self.executor.place_order(
                        ticker=ticker,
                        side=side,
                        count=contracts,
                        price_cents=exec_price,
                        client_order_id=client_oid,
                    )
                else:
                    result = self.executor.place_order(
                        ticker=ticker,
                        action="buy",
                        side=side,
                        count=contracts,
                        order_type="limit",
                        yes_price=api_yes_price,
                        client_order_id=client_oid,
                    )

                order_id = result.get("order", {}).get("order_id", client_oid)

                # Live mode: verify the order was filled
                if not self.config["paper_trade"]:
                    order_status = result.get("order", {}).get("status", "")
                    if order_status not in ("filled", "resting", "executed"):
                        self._log(f"      [WARN] Order status: {order_status} — skipping record")
                        continue
                    if order_status == "resting":
                        # Order is on the book — poll for fill.
                        # Adaptive patience: wait longer when plenty of time remains
                        # in the window. At near-midpoint prices (consensus territory),
                        # orders need time for counterparties to arrive.
                        # Don't cancel and lose queue position prematurely.
                        secs_in_window = scanner.seconds_until_close(market)
                        if secs_in_window > 600:
                            max_wait = 120  # 2 min patience early in window
                        elif secs_in_window > 300:
                            max_wait = 90   # 90s mid-window
                        elif secs_in_window > 120:
                            max_wait = 60   # 60s when time getting short
                        else:
                            max_wait = 30   # 30s near expiry
                        filled = False
                        for wait_round in range(max_wait):  # max_wait × 1s
                            time.sleep(1)
                            try:
                                # Check fills first — most reliable way to detect execution
                                fills_resp = self.client.get_fills(order_id=order_id)
                                fills = fills_resp.get("fills", [])
                                if fills:
                                    fill_count = sum(f.get("count", 0) for f in fills)
                                    if fill_count >= contracts:
                                        self._log(f"      [FILLED] Order filled ({fill_count} contracts via fills endpoint)")
                                        filled = True
                                        break
                                    elif fill_count > 0:
                                        secs_left = scanner.seconds_until_close(market)
                                        if secs_left < 120:
                                            contracts = fill_count
                                            stake = contracts * (exec_price / 100.0)
                                            entry_fee = kalshi_taker_fee(contracts, exec_price)
                                            self._log(f"      [PARTIAL] Filled {fill_count}/{contracts}, keeping partial (market closing soon)")
                                            filled = True
                                            break
                                        self._log(f"      [WAITING] Partial fill {fill_count}/{contracts}, {secs_left:.0f}s left...")
                                        continue

                                # Also check if order left the resting state
                                order_resp = self.client.get_orders(ticker=ticker)
                                orders = order_resp.get("orders", [])
                                our_order = next(
                                    (o for o in orders if o.get("order_id") == order_id), None
                                )
                                if our_order is None:
                                    # No longer in open orders — likely filled
                                    self._log(f"      [FILLED] Order no longer resting (assumed filled)")
                                    filled = True
                                    break
                                cur_status = our_order.get("status", "")
                                if cur_status in ("executed", "filled"):
                                    filled = True
                                    break
                            except Exception as e:
                                self._log(f"      [WARN] Fill check failed: {e}", level="warning")

                        if not filled:
                            # Check fills one final time before cancelling —
                            # the order may have filled between our last check and now
                            try:
                                fills_resp = self.client.get_fills(order_id=order_id)
                                fills = fills_resp.get("fills", [])
                                fill_count = sum(f.get("count", 0) for f in fills)
                                if fill_count > 0:
                                    self._log(f"      [FILLED] Order filled on final check ({fill_count} contracts)")
                                    if fill_count < contracts:
                                        contracts = fill_count
                                        stake = contracts * (exec_price / 100.0)
                                        entry_fee = kalshi_taker_fee(contracts, exec_price)
                                    filled = True
                            except Exception:
                                pass

                        if not filled:
                            self._log(f"      [CANCEL] Order {order_id} not filled after {max_wait}s, cancelling")
                            try:
                                self.client.cancel_order(order_id)
                            except Exception:
                                pass
                            continue

                self._log(f"      Order placed: {order_id}")

                # Record trade
                record = TradeRecord(
                    timestamp=time.time(),
                    ticker=ticker,
                    strategy=name,
                    side=side,
                    price_cents=exec_price,
                    contracts=contracts,
                    stake_usd=stake,
                    order_id=order_id,
                    client_order_id=client_oid,
                )
                self.risk_mgr.record_trade(record)
                self.logger.log_trade(record, reason=rec.reason)

                # Register with Bayesian updater for dynamic confidence tracking
                btc_feed = self.assets.get("btc", {}).get("price_feed")
                btc_px = btc_feed.current_price if btc_feed else None
                if btc_px:
                    self.bayesian.register(
                        ticker=ticker,
                        prior=rec.confidence,
                        direction=side,
                        entry_btc_price=btc_px,
                    )

                # Add to dashboard trade list
                self._dashboard_trades.insert(0, {
                    "time": datetime.fromtimestamp(record.timestamp, tz=timezone.utc).isoformat(),
                    "ticker": ticker,
                    "strategy": name,
                    "side": side,
                    "price": exec_price,
                    "contracts": contracts,
                    "stake": round(stake, 2),
                    "outcome": "pending",
                    "profit": 0,
                    "fees": round(record.entry_fee_usd, 2),
                    "profit_after_fees": 0,
                })
                if len(self._dashboard_trades) > 100:
                    self._dashboard_trades.pop()

            except Exception as e:
                self._log(f"\n      ERROR placing order: {e}", level="error")

    def _check_early_exits(self, market: dict, scanner, price_feed):
        """Check open positions on this market for early exit signals."""
        ticker = market.get("ticker", "")
        if ticker not in self.risk_mgr.open_positions:
            return

        record = self.risk_mgr.open_positions[ticker]
        secs_left = scanner.seconds_until_close(market)

        # Get current book prices
        try:
            book = scanner.client.get_orderbook(ticker, depth=1)
            yes_bids = book.get("orderbook", {}).get("yes", [])
            no_bids = book.get("orderbook", {}).get("no", [])
            yes_bid = yes_bids[0][0] if yes_bids else None
            yes_ask = (100 - no_bids[0][0]) if no_bids else None
        except Exception:
            yes_bid, yes_ask = scanner.parse_yes_price(market)

        if yes_bid is None or yes_ask is None:
            return

        mom_1m = price_feed.momentum_1m()
        rec = self.exit_monitor.check_position(
            ticker=ticker,
            side=record.side,
            entry_price_cents=record.price_cents,
            current_yes_bid=yes_bid,
            current_yes_ask=yes_ask,
            seconds_remaining=secs_left,
            momentum_1m=mom_1m,
        )

        if not rec.should_exit:
            return

        # Execute the sell
        self._log(f"\n  <<< EXIT {ticker}: {rec.reason}")

        try:
            sell_side = record.side  # Sell the same side we hold
            # To sell YES, we sell at yes_bid; to sell NO, we sell at 100-yes_ask
            if sell_side == "yes":
                sell_yes_price = yes_bid
            else:
                sell_yes_price = yes_ask  # selling NO at yes_ask

            result = self.client.place_order(
                ticker=ticker,
                action="sell",
                side=sell_side,
                count=record.contracts,
                order_type="limit",
                yes_price=sell_yes_price,
            )

            order_status = result.get("order", {}).get("status", "")
            if order_status in ("executed", "filled"):
                # Calculate exit P&L
                if sell_side == "yes":
                    exit_price = yes_bid
                else:
                    exit_price = 100 - yes_ask
                exit_proceeds = record.contracts * (exit_price / 100.0)
                record.outcome = "win" if exit_proceeds > record.stake_usd else "loss"
                record.payout_usd = exit_proceeds
                record.profit_usd = exit_proceeds - record.stake_usd
                record.profit_after_fees = record.profit_usd - record.entry_fee_usd
                self.risk_mgr.open_positions.pop(ticker, None)
                if record.profit_usd < 0:
                    self.risk_mgr._last_loss_ts = time.time()

                self._log(f"      Sold {record.contracts} {sell_side} @ {exit_price}c (P&L: ${record.profit_usd:+.2f})")
                self.logger.log_settlement(record)

                # Update dashboard
                for dt in self._dashboard_trades:
                    if dt["ticker"] == ticker and dt["outcome"] == "pending":
                        dt["outcome"] = record.outcome
                        dt["profit"] = round(record.profit_usd, 2)
                        dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                        break
            else:
                self._log(f"      [WARN] Exit order status: {order_status}")
        except Exception as e:
            self._log(f"      [WARN] Exit failed: {e}", level="warning")

    def _check_settlements(self):
        """Check settlements across all asset scanners."""
        for asset in self.assets.values():
            self._check_settlements_for(asset["scanner"])

    def _check_settlements_for(self, scanner):
        """Check if any open positions have settled (in-memory + CSV)."""
        settled_markets = scanner.get_settled_markets(limit=20)
        settled_by_ticker = {m["ticker"]: m.get("result") for m in settled_markets if m.get("result")}

        # 1. Settle in-memory positions (current session trades)
        for ticker in list(self.risk_mgr.open_positions.keys()):
            result = settled_by_ticker.get(ticker)
            if not result:
                continue
            self.risk_mgr.settle_trade(ticker, result)
            self.bayesian.clear(ticker)  # Stop Bayesian tracking
            record = next(
                (t for t in self.risk_mgr.trades if t.ticker == ticker and t.outcome != ""),
                None,
            )
            if record:
                emoji = "WIN" if record.outcome == "win" else "LOSS"
                self._log(f"\n  <<< SETTLED {ticker}: {emoji} (${record.profit_usd:+.2f})")
                self.logger.log_settlement(record)
                # Update dashboard trade
                for dt in self._dashboard_trades:
                    if dt["ticker"] == ticker and dt["outcome"] == "pending":
                        dt["outcome"] = record.outcome
                        dt["profit"] = round(record.profit_usd, 2)
                        dt["fees"] = round(record.entry_fee_usd + record.settle_fee_usd, 2)
                        dt["profit_after_fees"] = round(record.profit_after_fees, 2)

        # 2. Also resolve any unsettled CSV trades from previous runs (throttled to ~1/min)
        now = time.time()
        if now - getattr(self, '_last_csv_settle_check', 0) < 60:
            return
        self._last_csv_settle_check = now
        unsettled = self.logger.get_unsettled_trades()
        for trade_row in unsettled:
            ticker = trade_row["ticker"]
            result = settled_by_ticker.get(ticker)
            if not result:
                continue

            side = trade_row["side"]
            contracts = int(trade_row["contracts"])
            stake_usd = float(trade_row["stake_usd"])

            record = TradeRecord(
                timestamp=time.time(),
                ticker=ticker,
                strategy=trade_row["strategy"],
                side=side,
                price_cents=int(trade_row["price_cents"]),
                contracts=contracts,
                stake_usd=stake_usd,
                order_id=trade_row["order_id"],
            )

            if side == result:
                record.outcome = "win"
                record.payout_usd = contracts * 1.00
                record.profit_usd = record.payout_usd - stake_usd
            else:
                record.outcome = "loss"
                record.payout_usd = 0.0
                record.profit_usd = -stake_usd

            self.logger.log_settlement(record)
            emoji = "WIN" if record.outcome == "win" else "LOSS"
            self._log(f"\n  <<< SETTLED (prev run) {ticker}: {emoji} (${record.profit_usd:+.2f})")

            # Update dashboard if it's there
            for dt in self._dashboard_trades:
                if dt["ticker"] == ticker and dt["outcome"] == "pending":
                    dt["outcome"] = record.outcome
                    dt["profit"] = round(record.profit_usd, 2)
                    dt["fees"] = round(record.entry_fee_usd + record.settle_fee_usd, 2)
                    dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                    break

    def _publish_tick(self, all_markets, all_last_settled, all_strategies):
        """Build TickData JSON and push to the SSE server."""

        # Helper: build price data for an asset
        def _price_data(key):
            feed = self.assets[key]["price_feed"]
            return {
                "price": feed.current_price or 0,
                "mom_1m": feed.momentum_1m() or 0,
                "mom_5m": feed.momentum_5m() or 0,
                "prices": [[int(ts * 1000), px] for ts, px in feed.prices][-180:],
            }

        btc = _price_data("btc")
        eth = _price_data("eth")
        sol = _price_data("sol")

        def _parse_strike(val):
            """Convert strike price from API (string/int/float/None) to float or None."""
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def _extract_strike_from_title(market):
            """Extract target price from market title/subtitle (e.g. '$66,838.62 target')."""
            import re
            for field in ("subtitle", "title", "yes_sub_title", "no_sub_title"):
                text = market.get(field, "") or ""
                # Match dollar amounts like $66,838.62 or $2,038.86
                match = re.search(r'\$([0-9,]+\.?\d*)', text)
                if match:
                    try:
                        return float(match.group(1).replace(",", ""))
                    except ValueError:
                        continue
            return None

        # Helper: build market data for an asset
        def _market_data(key):
            market = all_markets.get(key)
            if not market:
                return None
            scanner = self.assets[key]["scanner"]
            yes_bid, yes_ask = scanner.parse_yes_price(market)
            return {
                "ticker": market.get("ticker", ""),
                "yes_bid": yes_bid or 50,
                "yes_ask": yes_ask or 50,
                "seconds_remaining": int(scanner.seconds_until_close(market)),
                "volume": float(market.get("volume", 0) or market.get("volume_fp", 0) or 0),
                "floor_strike": _parse_strike(market.get("floor_strike")) or _extract_strike_from_title(market),
                "cap_strike": _parse_strike(market.get("cap_strike")),
            }

        # Helper: build last-settled data
        def _settled_data(key):
            ls = all_last_settled.get(key)
            if not ls or not ls.get("result"):
                return None
            return {"ticker": ls.get("ticker", ""), "result": ls["result"]}

        # Strategy signals — build per-asset, ensure all 5 are present
        all_strategy_names = [
            "momentum", "mean_reversion", "consensus",
            "resolution_rider", "favorite_bias",
        ]

        def _strat_data(key):
            status = all_strategies.get(key, {})
            data = {}
            for name in all_strategy_names:
                rec = status.get(name)
                if rec:
                    data[name] = {
                        "signal": rec.signal.value if rec.signal.value != "none" else "none",
                        "confidence": round(rec.confidence, 2),
                        "reason": rec.reason,
                    }
                else:
                    data[name] = {"signal": "none", "confidence": 0, "reason": ""}
            return data

        # OFI
        ofi = fetch_ofi()

        # Balance
        balance = self._get_balance()
        is_paper = self.config["paper_trade"]

        # Stats
        completed = [t for t in self.risk_mgr.trades if t.outcome != ""]
        pending = [t for t in self.risk_mgr.trades if t.outcome == ""]
        wins = sum(1 for t in completed if t.outcome == "win")
        total_pnl = round(sum(t.profit_usd for t in completed), 2)
        total_fees = round(sum(t.entry_fee_usd + t.settle_fee_usd for t in completed), 2)
        # Include entry fees for pending trades too
        total_fees += round(sum(t.entry_fee_usd for t in pending), 2)
        total_pnl_after_fees = round(sum(t.profit_after_fees for t in completed), 2)
        # Subtract entry fees for pending trades (already paid)
        total_pnl_after_fees -= round(sum(t.entry_fee_usd for t in pending), 2)

        tick_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "btc_price": btc["price"],
            "btc_momentum_1m": round(btc["mom_1m"], 4),
            "btc_momentum_5m": round(btc["mom_5m"], 4),
            "btc_prices": btc["prices"],
            "eth_price": eth["price"],
            "eth_momentum_1m": round(eth["mom_1m"], 4),
            "eth_momentum_5m": round(eth["mom_5m"], 4),
            "eth_prices": eth["prices"],
            "sol_price": sol["price"],
            "sol_momentum_1m": round(sol["mom_1m"], 4),
            "sol_momentum_5m": round(sol["mom_5m"], 4),
            "sol_prices": sol["prices"],
            "current_market": _market_data("btc"),
            "last_settled": _settled_data("btc"),
            "strategies": _strat_data("btc"),
            # Per-asset markets, settlements, and strategies
            "markets": {
                "btc": _market_data("btc"),
                "eth": _market_data("eth"),
                "sol": _market_data("sol"),
            },
            "settled": {
                "btc": _settled_data("btc"),
                "eth": _settled_data("eth"),
                "sol": _settled_data("sol"),
            },
            "strategies_by_asset": {
                "btc": _strat_data("btc"),
                "eth": _strat_data("eth"),
                "sol": _strat_data("sol"),
            },
            "enabled_assets": dict(_sse_state.enabled_assets),
            "trading_enabled": _sse_state.trading_enabled,
            "vol_regime": self._regime_params.regime.value if hasattr(self, '_regime_params') and self._regime_params else "medium",
            "vol_reading": round(self.assets["btc"]["price_feed"].volatility(300) or 0, 4),
            "ofi": round(ofi, 4),
            "exchange_data": {
                key: {
                    "divergence_pct": round(mf.divergence_pct or 0, 4),
                    "exchange_lead": mf.exchange_lead(),
                    "santiment": mf.fetch_santiment() if key == "btc" else {},
                }
                for key, mf in self.multi_feeds.items()
            },
            "trades": self._dashboard_trades,
            "stats": {
                "total_trades": len(self._dashboard_trades),
                "pending": len(pending),
                "wins": wins,
                "losses": len(completed) - wins,
                "win_rate": round(wins / len(completed) * 100, 1) if completed else 0,
                "total_pnl": total_pnl,
                "total_fees": total_fees,
                "total_pnl_after_fees": total_pnl_after_fees,
                "daily_pnl": total_pnl,
                "daily_pnl_after_fees": total_pnl_after_fees,
                "bot_paused": self.risk_mgr.daily_pnl <= -self.config["max_daily_loss"],
                "paper_balance": round(balance, 2) if is_paper else None,
                "live_balance": round(balance, 2) if not is_paper else None,
                "is_paper": is_paper,
            },
        }

        with _sse_state.lock:
            _sse_state.tick_data = json.dumps(tick_data)

    def _shutdown(self):
        """Clean shutdown with final stats, persisted to log and JSON."""
        self.running = False
        self._log(f"\n\n{'='*72}")
        self._log("BOT STOPPED — Final Statistics")
        self._log(f"{'='*72}")

        completed = [t for t in self.risk_mgr.trades if t.outcome != ""]
        pending = [t for t in self.risk_mgr.trades if t.outcome == ""]

        summary = {
            "run_id": self.run_id,
            "mode": "paper" if self.config["paper_trade"] else "live",
            "series": self.config["series"],
            "strategies": list(self.strategies.keys()),
            "started": self.run_id,
            "stopped": datetime.now(timezone.utc).isoformat(),
            "completed_trades": len(completed),
            "pending_trades": len(pending),
        }

        if completed:
            wins = sum(1 for t in completed if t.outcome == "win")
            losses = len(completed) - wins
            wr = wins / len(completed) * 100

            total_staked = sum(t.stake_usd for t in completed)
            total_pnl = sum(t.profit_usd for t in completed)
            roi = (total_pnl / total_staked * 100) if total_staked > 0 else 0

            self._log(f"  Completed trades: {len(completed)} ({wins}W / {losses}L)")
            self._log(f"  Win rate:         {wr:.1f}%")
            self._log(f"  Total staked:     ${total_staked:.2f}")
            self._log(f"  Total P&L:        ${total_pnl:+.2f}")
            self._log(f"  ROI:              {roi:+.1f}%")

            summary.update({
                "wins": wins,
                "losses": losses,
                "win_rate": round(wr, 1),
                "total_staked_usd": round(total_staked, 2),
                "total_pnl_usd": round(total_pnl, 2),
                "roi_pct": round(roi, 1),
            })

            # Per-strategy breakdown
            strat_names = set(t.strategy for t in completed)
            strategy_breakdown = {}
            for sn in sorted(strat_names):
                strat_trades = [t for t in completed if t.strategy == sn]
                sw = sum(1 for t in strat_trades if t.outcome == "win")
                sl = len(strat_trades) - sw
                sp = sum(t.profit_usd for t in strat_trades)
                self._log(f"    {sn:20s}: {sw}W/{sl}L  ${sp:+.2f}")
                strategy_breakdown[sn] = {"wins": sw, "losses": sl, "pnl_usd": round(sp, 2)}
            summary["strategy_breakdown"] = strategy_breakdown
        else:
            self._log("  No completed trades.")

        if pending:
            self._log(f"\n  Pending trades: {len(pending)}")
            pending_list = []
            for t in pending:
                self._log(f"    {t.ticker} ({t.strategy}) {t.side} @ {t.price_cents}c x{t.contracts}")
                pending_list.append({
                    "ticker": t.ticker, "strategy": t.strategy,
                    "side": t.side, "price_cents": t.price_cents,
                    "contracts": t.contracts, "stake_usd": round(t.stake_usd, 2),
                })
            summary["pending_detail"] = pending_list

        # Persist session summary to JSON
        sessions_dir = Path("data/sessions")
        sessions_dir.mkdir(parents=True, exist_ok=True)
        summary_path = sessions_dir / f"{self.run_id}.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        self._log(f"\n  Trade log:       {self.logger.csv_path}")
        self._log(f"  Session summary: {summary_path}")
        self._log(f"  Full log:        data/logs/bot_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log")
        self._log(f"{'='*72}")

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
