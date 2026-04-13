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
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

import requests as _requests
from dotenv import load_dotenv

from kalshi_client import KalshiClient
from kalshi_ws import KalshiWebSocket
from market_scanner import MarketScanner
from rti_feed import RTIFeed
from strategy_matrix import StrategyMatrix
from risk_manager import RiskConfig, RiskManager, TradeRecord, kalshi_taker_fee, kalshi_maker_fee
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
from calibrator import ConfidenceCalibrator
from performance import PerformanceTracker
from broad_scanner import BroadScanner
from strategy_tracker import StrategyTracker


# ─── Orderbook parsing ───────────────────────────────────────────────────────

def parse_book_top(book: dict) -> tuple:
    """
    Extract top-of-book (yes_bid, yes_ask) in cents from a Kalshi orderbook
    response. Handles both the current `orderbook_fp` schema with dollar
    strings and the legacy `orderbook` schema with cent integers.

    Kalshi returns bid levels sorted ASCENDING (worst→best); the best bid is
    at index [-1]. Only bids are returned (no asks); the YES ask is derived
    from the best NO bid as (100 - best_no_bid) in cents.

    Returns (yes_bid_cents, yes_ask_cents), either may be None if missing.
    """
    yes_levels = []
    no_levels = []

    fp = book.get("orderbook_fp")
    if fp:
        for lvl in fp.get("yes_dollars", []) or []:
            try:
                yes_levels.append(int(round(float(lvl[0]) * 100)))
            except (ValueError, TypeError, IndexError):
                continue
        for lvl in fp.get("no_dollars", []) or []:
            try:
                no_levels.append(int(round(float(lvl[0]) * 100)))
            except (ValueError, TypeError, IndexError):
                continue
    else:
        legacy = book.get("orderbook", {}) or {}
        for lvl in legacy.get("yes", []) or []:
            try:
                yes_levels.append(int(lvl[0]))
            except (ValueError, TypeError, IndexError):
                continue
        for lvl in legacy.get("no", []) or []:
            try:
                no_levels.append(int(lvl[0]))
            except (ValueError, TypeError, IndexError):
                continue

    yes_bid = yes_levels[-1] if yes_levels else None
    yes_ask = (100 - no_levels[-1]) if no_levels else None
    return yes_bid, yes_ask


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
        "poll_interval": 0,  # Legacy — bot runs at full speed via WebSocket
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
        "payout_usd", "profit_usd", "reason", "confidence",
        "order_type", "fees_usd",
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
        """Migrate existing CSVs to add missing columns (run_id, confidence)."""
        if not self.csv_path.exists():
            return
        with open(self.csv_path, "r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return
            rows = list(reader)

        needs_rewrite = False

        # Migration 1: add run_id column
        if "run_id" not in header:
            run_id_idx = 1  # After time column
            header.insert(run_id_idx, "run_id")
            for row in rows:
                row.insert(run_id_idx, "")
            needs_rewrite = True

        # Migration 2: add confidence column
        if "confidence" not in header:
            header.append("confidence")
            for row in rows:
                row.append("")  # Empty confidence for historical rows
            needs_rewrite = True

        # Migration 3: add order_type and fees_usd columns
        if "order_type" not in header:
            header.append("order_type")
            for row in rows:
                row.append("")
            needs_rewrite = True
        if "fees_usd" not in header:
            header.append("fees_usd")
            for row in rows:
                row.append("")
            needs_rewrite = True

        if needs_rewrite:
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                for row in rows:
                    writer.writerow(row)

    def log_trade(self, record: TradeRecord, reason: str = "", confidence: float = None):
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
                f"{confidence:.4f}" if confidence is not None else "",
                "maker" if record.is_maker else "taker",
                f"{record.entry_fee_usd:.4f}",
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

        # Collect order_ids that have a SETTLED row
        settled_order_ids = set()
        for row in rows:
            reason = row.get("reason", "")
            if reason.startswith("SETTLED:"):
                settled_order_ids.add(row["order_id"])

        # Return trade rows (not settlement rows) whose order_id hasn't been settled
        unsettled = []
        for row in rows:
            reason = row.get("reason", "")
            if not reason.startswith("SETTLED:") and row["order_id"] not in settled_order_ids:
                unsettled.append(row)
        return unsettled

    def get_historical_stats(self) -> dict:
        """Load all settled trades from CSV and compute period P&Ls."""
        if not self.csv_path.exists():
            return {"daily_gross": 0, "daily_net": 0, "daily_fees": 0,
                    "weekly_gross": 0, "weekly_net": 0, "weekly_fees": 0,
                    "monthly_gross": 0, "monthly_net": 0, "monthly_fees": 0,
                    "alltime_gross": 0, "alltime_net": 0, "alltime_fees": 0,
                    "total_trades": 0, "wins": 0, "losses": 0, "pending": 0,
                    "trades": []}

        now = datetime.now(timezone.utc)
        # Daily P&L resets at midnight Pacific (UTC-7 PDT)
        # Compute the UTC timestamp of today's midnight Pacific
        pacific_now = now - timedelta(hours=7)
        today_pacific = pacific_now.strftime("%Y-%m-%d")
        daily_cutoff_utc = (datetime.strptime(today_pacific, "%Y-%m-%d").replace(
            tzinfo=timezone.utc) + timedelta(hours=7)).isoformat()
        week_ago = (now - timedelta(days=7)).isoformat()
        month_ago = (now - timedelta(days=30)).isoformat()

        with open(self.csv_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        settlements = [r for r in rows if r.get("reason", "").startswith("SETTLED:")]
        entries = [r for r in rows if not r.get("reason", "").startswith("SETTLED:")]
        settled_ids = {s["order_id"] for s in settlements}
        unsettled = [e for e in entries if e["order_id"] not in settled_ids]

        # Compute gross P&L and fees per period.
        # Uses stored fees_usd if present (from Kalshi reconcile),
        # otherwise falls back to computed taker fee.
        import math
        def _fee(row):
            stored = row.get("fees_usd", "")
            if stored:
                try:
                    return float(stored)
                except ValueError:
                    pass
            p = int(row["price_cents"]) / 100.0
            c = int(row["contracts"])
            return math.ceil(0.07 * c * p * (1 - p) * 100) / 100

        # Map order_id -> entry row for fee/type lookup on settlements
        entry_by_oid = {e["order_id"]: e for e in entries}

        daily_gross = weekly_gross = monthly_gross = alltime_gross = 0.0
        daily_fees = weekly_fees = monthly_fees = alltime_fees = 0.0

        for s in settlements:
            pnl = float(s["profit_usd"])
            # Prefer stored fee on settlement row, fall back to entry row
            entry = entry_by_oid.get(s["order_id"])
            fee = _fee(s) if s.get("fees_usd") else (_fee(entry) if entry else _fee(s))
            alltime_gross += pnl
            alltime_fees += fee
            if s["time"] >= month_ago:
                monthly_gross += pnl
                monthly_fees += fee
            if s["time"] >= week_ago:
                weekly_gross += pnl
                weekly_fees += fee
            if s["time"] >= daily_cutoff_utc:
                daily_gross += pnl
                daily_fees += fee

        wins = sum(1 for s in settlements if s["outcome"] == "win")
        losses = sum(1 for s in settlements if s["outcome"] == "loss")

        # Build full trade list for dashboard
        recent = []
        for e in entries:
            s = next((s for s in settlements if s["order_id"] == e["order_id"]), None)
            fee = _fee(e)
            profit = round(float(s["profit_usd"]), 2) if s else 0
            # Use stored order_type if present, else default to taker
            order_type = e.get("order_type") or "taker"
            recent.append({
                "time": e["time"],
                "ticker": e["ticker"],
                "strategy": e["strategy"],
                "side": e["side"],
                "price": int(e["price_cents"]),
                "contracts": int(e["contracts"]),
                "stake": round(float(e["stake_usd"]), 2),
                "outcome": s["outcome"] if s else "pending",
                "profit": profit,
                "fees": round(fee, 2),
                "profit_after_fees": round(profit - fee, 2),
                "order_type": order_type,
                "order_id": e.get("order_id", ""),
            })

        return {
            "daily_gross": round(daily_gross, 2),
            "daily_net": round(daily_gross - daily_fees, 2),
            "daily_fees": round(daily_fees, 2),
            "weekly_gross": round(weekly_gross, 2),
            "weekly_net": round(weekly_gross - weekly_fees, 2),
            "weekly_fees": round(weekly_fees, 2),
            "monthly_gross": round(monthly_gross, 2),
            "monthly_net": round(monthly_gross - monthly_fees, 2),
            "monthly_fees": round(monthly_fees, 2),
            "alltime_gross": round(alltime_gross, 2),
            "alltime_net": round(alltime_gross - alltime_fees, 2),
            "alltime_fees": round(alltime_fees, 2),
            "total_trades": wins + losses,
            "wins": wins,
            "losses": losses,
            "pending": len(unsettled),
            "trades": list(reversed(recent)),  # newest first
        }


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
    allow_reuse_address = True


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
    """Simulates order execution for paper trading with realistic slippage."""

    def __init__(self, slippage_model=None, client=None):
        self.slippage_model = slippage_model
        self.client = client  # PublicOnlyClient for orderbook access

    def place_order(self, ticker, side, count, price_cents, **kwargs):
        order_id = f"PAPER-{uuid.uuid4().hex[:12]}"

        # If we have a slippage model and orderbook access, simulate realistically
        if self.slippage_model and self.client:
            try:
                book = self.client.get_orderbook(ticker, depth=1)
                yes_bid, yes_ask = parse_book_top(book)

                result = self.slippage_model.simulate_fill(
                    side=side,
                    requested_price_cents=price_cents,
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    contracts=count,
                )

                if not result.filled:
                    return {
                        "order": {
                            "order_id": order_id,
                            "status": "cancelled",
                            "client_order_id": kwargs.get("client_order_id", ""),
                        }
                    }

                return {
                    "order": {
                        "order_id": order_id,
                        "status": "filled",
                        "client_order_id": kwargs.get("client_order_id", ""),
                        "fill_price_cents": result.fill_price_cents,
                        "contracts_filled": result.contracts_filled,
                    }
                }
            except Exception:
                pass  # Fall through to simple fill

        # Fallback: instant fill at requested price
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

        # Now that we have a client, set up slippage model for paper executor
        if config["paper_trade"]:
            from slippage_model import SlippageModel
            self.executor = PaperExecutor(
                slippage_model=SlippageModel(),
                client=self.client,
            )

        # Multi-asset: scanners and price feeds for 15M and daily series
        # Use RTIFeed (CF Benchmarks RTI approximation) instead of single-exchange
        # PriceFeed — aggregates Coinbase, Kraken, Bitstamp, Gemini, Binance
        # with volume weighting and outlier filtering to match Kalshi's settlement index
        daily_stake = float(os.getenv("DAILY_STAKE_USD", "15.00"))
        self.assets = {
            # ── 15-minute markets ──
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
            # ── Additional crypto 15M (resolution_rider only) ──
            "doge": {
                "series": "KXDOGE15M",
                "symbol": "DOGE-USD",
                "scanner": MarketScanner(self.client, series="KXDOGE15M"),
                "price_feed": RTIFeed(symbol="DOGE-USD", window_seconds=1200),
            },
            "xrp": {
                "series": "KXXRP15M",
                "symbol": "XRP-USD",
                "scanner": MarketScanner(self.client, series="KXXRP15M"),
                "price_feed": RTIFeed(symbol="XRP-USD", window_seconds=1200),
            },
            "bnb": {
                "series": "KXBNB15M",
                "symbol": "BNB-USD",
                "scanner": MarketScanner(self.client, series="KXBNB15M"),
                "price_feed": RTIFeed(symbol="BNB-USD", window_seconds=1200),
            },
            "hype": {
                "series": "KXHYPE15M",
                "symbol": "HYPE-USD",
                "scanner": MarketScanner(self.client, series="KXHYPE15M"),
                "price_feed": RTIFeed(symbol="HYPE-USD", window_seconds=1200),
            },
        }
        # ── Daily markets (stronger edge, higher stake) ──
        # Walk-forward backtest: 12/12 months profitable, 89% WR, Sharpe 0.28
        # Share price feeds with 15M counterparts to avoid duplicate API calls
        self.assets.update({
            "btc_daily": {
                "series": "KXBTCD",
                "symbol": "BTC-USD",
                "scanner": MarketScanner(self.client, series="KXBTCD"),
                "price_feed": self.assets["btc"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
            "eth_daily": {
                "series": "KXETHD",
                "symbol": "ETH-USD",
                "scanner": MarketScanner(self.client, series="KXETHD"),
                "price_feed": self.assets["eth"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
            "sol_daily": {
                "series": "KXSOLD",
                "symbol": "SOL-USD",
                "scanner": MarketScanner(self.client, series="KXSOLD"),
                "price_feed": self.assets["sol"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
            "doge_daily": {
                "series": "KXDOGED",
                "symbol": "DOGE-USD",
                "scanner": MarketScanner(self.client, series="KXDOGED"),
                "price_feed": self.assets["doge"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
            "xrp_daily": {
                "series": "KXXRPD",
                "symbol": "XRP-USD",
                "scanner": MarketScanner(self.client, series="KXXRPD"),
                "price_feed": self.assets["xrp"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
            "bnb_daily": {
                "series": "KXBNBD",
                "symbol": "BNB-USD",
                "scanner": MarketScanner(self.client, series="KXBNBD"),
                "price_feed": self.assets["bnb"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
            "hype_daily": {
                "series": "KXHYPED",
                "symbol": "HYPE-USD",
                "scanner": MarketScanner(self.client, series="KXHYPED"),
                "price_feed": self.assets["hype"]["price_feed"],
                "stake_override": daily_stake,
                "is_daily": True,
            },
        })

        # Multi-exchange feeds for cross-exchange signals (one per symbol, shared by 15M and daily)
        self.multi_feeds = {}
        _seen_symbols = set()
        for key, a in self.assets.items():
            sym = a["symbol"]
            if sym not in _seen_symbols:
                self.multi_feeds[key] = MultiExchangeFeed(symbol=sym)
                _seen_symbols.add(sym)

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

        # Early exit monitor for open positions.
        # Disabled 2026-04-13: RR thesis is 98% WR hold-to-settlement; stop
        # losses were firing on normal adverse moves and then the market was
        # recovering before settlement, turning would-be wins into -$13 to
        # -$17 whipsaw losses. Observed 4 such trades on 2026-04-13 totaling
        # ~$60 of unnecessary losses.
        # The monitor instance is still constructed so the rest of the
        # codebase doesn't break, but _check_early_exits and _force_early_exit
        # are gated by self._early_exits_enabled below.
        self._early_exits_enabled = False
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

        # WebSocket feed for real-time Kalshi contract prices
        self.ws_feed = KalshiWebSocket(
            key_id=os.getenv("KALSHI_API_KEY_ID"),
            private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH", "~/.key/kalshi/key.pem"),
            env=os.getenv("KALSHI_ENV", "prod"),
        )
        self.ws_feed.subscribe([
            "KXBTC15M", "KXBTCD", "KXETH15M", "KXETHD", "KXSOL15M", "KXSOLD",
            "KXDOGE15M", "KXDOGED", "KXXRP15M", "KXXRPD",
            "KXBNB15M", "KXBNBD", "KXHYPE15M", "KXHYPED",
        ])

        # Enable recording of live contract prices for future calibration
        data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
        self.ws_feed.enable_recording(data_dir)

        # Inject WebSocket feed into all scanners for real-time prices
        for asset in self.assets.values():
            asset["scanner"].ws_feed = self.ws_feed

        # Broad market scanner — scans ALL Kalshi markets for resolution_rider
        # opportunities (95-99c contracts), not just crypto series.
        # Broad scanner disabled — non-crypto markets lack price feeds for
        # the mandatory buffer check. Can re-enable when we add price feeds.
        self.broad_scanner = None

        # KL-divergence cross-asset signal — detects mispricings between
        # BTC/ETH/SOL contract prices based on their correlation
        self.kl_signal = KLDivergenceSignal()

        # Logger
        csv_name = "paper_trades.csv" if config["paper_trade"] else "live_trades.csv"
        self.logger = TradeLogger(f"data/{csv_name}", run_id=self.run_id)

        # Confidence calibrator — maps raw strategy confidence to empirical
        # win probabilities using historical trade data
        self.calibrator = ConfidenceCalibrator(
            csv_paths=["data/paper_trades.csv", "data/live_trades.csv"],
        )
        cal_stats = self.calibrator.get_strategy_stats()
        if cal_stats:
            self._log(f"[INIT] Calibrator loaded: {', '.join(f'{s}={d['total_trades']}t' for s, d in cal_stats.items())}")
        else:
            self._log("[INIT] Calibrator: no historical data, using cold-start dampening")

        # Performance tracker — computes Sharpe, drawdown, profit factor
        paper_balance = float(os.getenv("PAPER_BALANCE", "100.00")) if config["paper_trade"] else 0.0
        self.perf_tracker = PerformanceTracker(initial_balance=paper_balance)

        # Per-strategy tracker — auto-suspends underperforming strategies
        self.strategy_tracker = StrategyTracker()

        # Adaptive strategy matrix — auto-disables (asset, strategy) combos
        # that underperform, shadow-tracks disabled combos, re-enables on recovery
        self.strategy_matrix = StrategyMatrix(
            window_size=20,
            disable_threshold=-0.05,       # Tightened from -10%: cut losers faster
            first_enable_threshold=0.05,   # Raised from 1%: need real edge, not noise
            enable_threshold=0.05,         # Raised from 2%: must prove recovery
            extended_disable_threshold=0.10,  # Raised from 5%: repeat offenders need strong proof
            first_enable_min_trades=10,    # Raised from 3: need statistically meaningful sample
            min_trades_to_judge=3,         # Lowered from 5: disable faster on bad edge
            cooldown_seconds=900,          # Raised from 300: one full 15M market cycle
            persist_path="data/strategy_matrix_state.json",
            strategy_overrides={
                # Consensus has been a consistent money loser (-$69 over 73 trades,
                # 33% WR at 47c avg entry). Make it very hard to enable and quick
                # to disable. It must prove itself over a large shadow sample.
                "consensus": {
                    "first_enable_min_trades": 30,   # Need 30 shadow trades (vs 10 default)
                    "first_enable_threshold": 0.15,  # 15% shadow edge (vs 5% default)
                    "enable_threshold": 0.15,        # 15% shadow edge to re-enable (vs 5%)
                    "extended_disable_threshold": 0.20,  # 20% if repeatedly disabled
                    "min_trades_to_judge": 3,        # Same as default — disable fast
                    "disable_threshold": -0.03,      # Trigger at -3% edge (vs -5% default)
                },
            },
        )
        # Pre-populate all cells so dashboard shows the full matrix at startup
        active_assets = list(self.assets.keys())
        active_strategies = list(self.strategies.keys())
        self.strategy_matrix.initialize_cells(active_assets, active_strategies)

        # Pre-enable strategies with strong walk-forward evidence.
        # These don't need to prove themselves in shadow mode — they have
        # 12 months of out-of-sample data (11K+ trades for favorite_bias,
        # Load optimized per-cell RR params from Monte Carlo optimizer
        self._rr_cell_params = {}
        rr_params_path = Path("data/rr_params.json")
        if rr_params_path.exists():
            with open(rr_params_path) as f:
                all_rr_params = json.load(f)
            # Enable cells that were profitable in cross-validation
            # (or have no CV data but profitable training)
            safe_cells = {k: v for k, v in all_rr_params.items()
                          if v.get("cv_val_profit", 0) > 0 or
                          (v.get("cv_folds", 0) == 0 and v.get("training_profit", 0) > 0)}
            self._rr_cell_params = safe_cells
            self._log(f"[INIT] Loaded optimized RR params: {len(safe_cells)} safe cells "
                      f"({', '.join(sorted(safe_cells.keys()))})")
            unsafe = set(all_rr_params.keys()) - set(safe_cells.keys())
            if unsafe:
                self._log(f"[INIT] Disabled RR cells (CV losses): {', '.join(sorted(unsafe))}")

        # Enable safe RR cells, disable unsafe ones
        for asset in active_assets:
            # Map asset key to cell name: btc -> btc_15m, btc_daily -> btc_hourly
            if asset.endswith("_daily"):
                cell = asset.replace("_daily", "_hourly")
            else:
                cell = f"{asset}_15m"

            if cell in self._rr_cell_params:
                self.strategy_matrix.force_enable(asset, "resolution_rider", clear_history=True)
            else:
                self.strategy_matrix.force_disable(asset, "resolution_rider", hard=True)

        # Hard-disable non-RR strategies
        for asset in active_assets:
            self.strategy_matrix.force_disable(asset, "consensus", hard=True)
            self.strategy_matrix.force_disable(asset, "favorite_bias", hard=True)

        # Track which markets we've already traded on
        self._traded_tickers: set = set()
        self._last_settled_ticker: str = ""

        # Resolve unsettled trades from previous runs
        self._resolve_unsettled_trades()

        # Dashboard trade history (matches the Trade shape the frontend expects)
        # Load historical trades from CSV for persistent dashboard + P&L tracking
        hist = self.logger.get_historical_stats()
        self._dashboard_trades: list[dict] = hist["trades"]
        self._hist_stats = hist
        self._log(f"[INIT] Loaded {hist['total_trades']} historical trades "
                  f"(daily=${hist['daily_gross']:+.2f} weekly=${hist['weekly_gross']:+.2f} "
                  f"monthly=${hist['monthly_gross']:+.2f} alltime=${hist['alltime_gross']:+.2f} "
                  f"fees=${hist['alltime_fees']:.2f})")

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
            # mean_reversion removed: historical backtest over 44K markets shows
            # Sharpe -0.001, 28.8% win rate, zero edge. Confirmed money loser.
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

        # Query ALL asset scanners (not just primary) so daily contracts
        # (KXBTCD, KXETHD, KXSOLD) are found alongside 15M contracts
        settled_by_ticker = {}
        for key, asset in self.assets.items():
            try:
                settled_markets = asset["scanner"].get_settled_markets(limit=200)
                for m in settled_markets:
                    if m.get("result"):
                        settled_by_ticker[m["ticker"]] = m["result"]
            except Exception as e:
                self._log(f"  [WARN] Settlement query failed for {key}: {e}", level="warning")

        resolved = 0
        for trade_row in unsettled:
            ticker = trade_row["ticker"]
            result = settled_by_ticker.get(ticker)

            # Direct API fallback for tickers not found in bulk queries
            # (daily markets can have 100+ strikes, so bulk may miss some)
            if not result:
                for asset in self.assets.values():
                    scanner = asset["scanner"]
                    if scanner.series in ticker:
                        try:
                            market_data = scanner.client.get_market(ticker)
                            m = market_data.get("market", market_data)
                            if m.get("status") in ("settled", "finalized") and m.get("result"):
                                result = m["result"]
                                self._log(f"  [SETTLE] Found via direct lookup: {ticker} -> {result}")
                        except Exception:
                            pass
                        break

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
            record.profit_after_fees = record.profit_usd  # No entry fee data from CSV

            self.logger.log_settlement(record)
            self.perf_tracker.record(record.profit_usd, record.timestamp)
            self._record_outcome(record)
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
        self._log(f"  Polling:    full speed (WebSocket)")
        self._log(
            f"  Early exits: {'ENABLED' if self._early_exits_enabled else 'DISABLED'} "
            f"(hold to settlement)"
        )
        # Start WebSocket feeds for real-time data
        self.ws_feed.start()
        RTIFeed.start_shared_ws()
        for asset in self.assets.values():
            asset["price_feed"].attach_ws()
        self._log("[INIT] WebSocket feeds started (Kalshi contracts + crypto exchanges)")

        # Start fast RR scanner in its own thread (decoupled from REST I/O)
        self._start_fast_rr_thread()

        # Start reconciliation thread — keeps CSV in sync with Kalshi API
        self._start_reconcile_thread()

        print(f"{'='*72}")
        print("Warming up price feed (collecting 60s of data)...")

        # Warm up price feeds — only fetch unique feeds (dailies share with 15M)
        unique_feeds = list({id(a["price_feed"]): a["price_feed"] for a in self.assets.values()}.values())
        warmup_end = time.time() + 60
        while time.time() < warmup_end and self.running:
            for feed in unique_feeds:
                feed.fetch_price()
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
            consecutive_errors = 0
            while self.running:
                try:
                    self._tick()
                    consecutive_errors = 0
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    consecutive_errors += 1
                    if consecutive_errors <= 3 or consecutive_errors % 10 == 0:
                        self._log(f"  [ERROR] Tick failed ({consecutive_errors}x): {e}", level="error")
                    # Exponential backoff: 1s, 2s, 4s, 8s, 15s, 15s, 15s...
                    # Never give up — Kalshi outages are transient
                    time.sleep(min(15, 2 ** min(consecutive_errors - 1, 4)))
                # Minimal yield — WebSocket feeds provide data continuously,
                # so we run as fast as possible with just enough sleep to
                # avoid CPU spin and let background threads breathe
                time.sleep(0.05)
        except KeyboardInterrupt:
            pass
        finally:
            RTIFeed.stop_shared_ws()
            self._shutdown()

    def _refresh_rr_cache(self):
        """Refresh the cached active market per asset (called from slow path)."""
        now = time.time()
        if now - getattr(self, '_last_rr_cache_refresh', 0) < 10:
            return
        self._last_rr_cache_refresh = now
        for key, asset in self.assets.items():
            try:
                scanner = asset["scanner"]
                market = scanner.get_next_expiring_market()
                if market:
                    asset["_rr_market"] = {
                        "ticker": market.get("ticker", ""),
                        "close_time": market.get("close_time", ""),
                        "floor_strike": market.get("floor_strike"),
                    }
                    # Also cache near-certain markets for hourly/daily
                    if asset.get("is_daily"):
                        rr_markets = scanner.get_near_certain_markets(max_hours=8/60)
                        asset["_rr_daily_markets"] = [
                            {"ticker": m.get("ticker",""), "close_time": m.get("close_time",""),
                             "floor_strike": m.get("floor_strike")}
                            for m in rr_markets
                        ]
            except Exception:
                pass

    def _fast_rr_scan(self):
        """
        Lightning-fast resolution rider check. ZERO REST calls.
        Uses only WebSocket prices + cached market metadata.
        Runs at the top of every tick before any I/O.
        """
        rr = self.strategies.get("resolution_rider")
        if not rr:
            return

        for key, asset in self.assets.items():
            cache = asset.get("_rr_market")
            if not cache:
                continue

            # Get per-cell optimized params (or skip if cell is disabled)
            is_daily = asset.get("is_daily", False)
            cell_name = key.replace("_daily", "_hourly") if is_daily else f"{key}_15m"
            cell_params = self._rr_cell_params.get(cell_name)
            if not cell_params:
                continue  # Cell not in safe list

            # Override RR params for this cell
            cell_max_secs = cell_params.get("max_seconds", rr.max_seconds)
            cell_buffer = cell_params.get("min_price_buffer_pct", rr.min_price_buffer_pct)

            # Build list of markets to check (main + hourly/daily extras)
            markets_to_check = [cache]
            if is_daily:
                markets_to_check = asset.get("_rr_daily_markets", [cache])

            for market_cache in markets_to_check:
                ticker = market_cache.get("ticker", "")
                if not ticker:
                    continue
                if ticker in self._traded_tickers or ticker in self.risk_mgr.open_positions:
                    continue

                # Time check from cached close_time (no I/O)
                close_str = market_cache.get("close_time", "").replace("Z", "+00:00")
                try:
                    close_dt = datetime.fromisoformat(close_str)
                    secs_left = max(0, (close_dt - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError):
                    continue
                if secs_left < rr.min_seconds or secs_left > cell_max_secs:
                    continue

                # WS price check (real-time, zero I/O)
                tick = self.ws_feed.get_tick(ticker)
                if not tick or not tick.yes_bid or not tick.yes_ask:
                    continue

                cell_min_cp = cell_params.get("min_contract_price", rr.min_contract_price)
                cell_max_ep = cell_params.get("max_entry_price", rr.max_entry_price)
                yes_mid = (tick.yes_bid + tick.yes_ask) / 2
                fav = max(yes_mid, 100 - yes_mid)
                if fav < cell_min_cp or fav > cell_max_ep:
                    continue

                # Price buffer check (WS-cached crypto price, zero I/O)
                feed = asset["price_feed"]
                if not feed.current_price or not market_cache.get("floor_strike"):
                    continue
                try:
                    strike = float(market_cache["floor_strike"])
                except (ValueError, TypeError):
                    continue
                if strike <= 0:
                    continue
                buffer_pct = (feed.current_price - strike) / strike * 100

                # Determine which side we'd trade, then check buffer for that side
                no_mid = 100 - yes_mid
                if yes_mid >= no_mid:
                    # YES side favored — price must be ABOVE strike by buffer
                    if buffer_pct < cell_buffer:
                        continue
                else:
                    # NO side favored — price must be BELOW strike by buffer
                    if buffer_pct > -cell_buffer:
                        continue

                # Passed all fast checks — do full evaluation and trade
                market_dict = {
                    "ticker": ticker,
                    "close_time": market_cache["close_time"],
                    "floor_strike": market_cache["floor_strike"],
                    "yes_bid": tick.yes_bid,
                    "yes_ask": tick.yes_ask,
                }

                # Use a lightweight shim scanner (WS data only)
                class _FastShim:
                    def __init__(shim, ws, client, mkt):
                        shim._ws = ws
                        shim.client = client
                        shim._mkt = mkt
                    def seconds_until_close(shim, market):
                        return secs_left
                    def parse_yes_price(shim, market):
                        return (tick.yes_bid, tick.yes_ask)

                shim = _FastShim(self.ws_feed, self.client, market_dict)
                rec = rr.evaluate(market_dict, None, feed, shim,
                                  min_buffer_override=cell_buffer)
                if not rec.should_trade:
                    continue

                # Check matrix
                if not self.strategy_matrix.is_enabled(key, "resolution_rider"):
                    continue

                self._log(f"  [FAST-RR] Hit: {ticker} {rec.reason}")
                strats = {"resolution_rider": rec}
                self._traded_tickers.add(ticker)
                stake_override = asset.get("stake_override")
                self._maybe_trade(market_dict, strats, stake_override=stake_override, asset_key=key)

    def _start_fast_rr_thread(self):
        """Start the fast RR scanner in a dedicated high-frequency thread."""
        def _rr_loop():
            while self.running:
                try:
                    self._fast_rr_scan()
                except Exception as e:
                    if not hasattr(self, '_rr_thread_errs'):
                        self._rr_thread_errs = 0
                    self._rr_thread_errs += 1
                    if self._rr_thread_errs <= 3:
                        self._log(f"  [FAST-RR] Thread error: {e}", level="warning")
                time.sleep(0.05)  # 20Hz, never blocked by REST

        t = threading.Thread(target=_rr_loop, daemon=True, name="fast-rr")
        t.start()
        self._log("[INIT] Fast RR thread started (20Hz, zero I/O)")

    def _start_reconcile_thread(self):
        """Periodically reconcile our CSV with Kalshi's API to keep data accurate."""
        if self.config["paper_trade"]:
            return  # No need in paper mode

        try:
            from reconcile_kalshi_api import reconcile
        except ImportError as e:
            self._log(f"[INIT] Reconcile module not available: {e}", level="warning")
            return

        def _reconcile_loop():
            # Initial 30s delay to let the bot warm up
            time.sleep(30)
            last_parallel_count = 0
            while self.running:
                try:
                    stats = reconcile(self.client, verbose=False, backup=False)
                    changes = (stats["contracts_fixed"] + stats["price_fixed"] +
                               stats["pnl_fixed"] + stats["missing_added"])
                    if changes > 0:
                        self._log(
                            f"[RECONCILE] {stats['matched']} matched, "
                            f"{stats['pnl_fixed']} pnl fixed, "
                            f"{stats['missing_added']} missing added"
                        )
                    # Alert on new parallel-position settlements. Historical
                    # ones (from before the reduce_only fix) are recorded but
                    # only the delta is loud-warned — otherwise we'd spam.
                    parallel = stats.get("parallel_positions", 0)
                    if parallel > last_parallel_count:
                        new = parallel - last_parallel_count
                        self._log(
                            f"[RECONCILE] WARN: {new} new settlement(s) with parallel "
                            f"YES+NO positions detected — exit path may have skipped "
                            f"reduce_only. Total affected: {parallel}",
                            level="warning",
                        )
                    last_parallel_count = parallel
                except Exception as e:
                    self._log(f"[RECONCILE] Error: {e}", level="warning")
                time.sleep(60)  # Run every 60 seconds

        t = threading.Thread(target=_reconcile_loop, daemon=True, name="reconcile")
        t.start()
        self._log("[INIT] Reconcile thread started (60s interval)")

    def _tick(self):
        """Single iteration of the main loop."""

        # 1. Update RTI composite from WebSocket-cached exchange prices (no HTTP)
        from concurrent.futures import ThreadPoolExecutor
        unique_feeds = list({id(a["price_feed"]): a["price_feed"] for a in self.assets.values()}.values())
        for f in unique_feeds:
            f.fetch_price()  # In WS mode: just reads cached data, ~0ms

        # Multi-exchange feeds still use REST — throttle to every 3 seconds
        now = time.time()
        if now - getattr(self, '_last_multi_fetch', 0) >= 3:
            self._last_multi_fetch = now
            with ThreadPoolExecutor(max_workers=3) as pool:
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

            # Check open positions for early exit (both paper and live)
            if market:
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
                        # Bayesian early exit signal — log for monitoring.
                        # Don't force exit based on Bayesian alone — the model
                        # tracks BTC price change from entry, NOT distance from
                        # strike. It can signal "exit" on a winning position if
                        # BTC moved slightly toward the strike but is still
                        # comfortably on the right side.
                        record = self.risk_mgr.open_positions[ticker_bayes]
                        if (posterior is not None
                            and self.bayesian.should_exit(ticker_bayes, record.price_cents / 100.0)
                            and self._early_exits_enabled):
                            # Only log and attempt exit when the early-exit
                            # mechanism is enabled. Otherwise the log spammed
                            # every tick with no corresponding action.
                            self._log(f"  [BAYES] Posterior collapsed to {posterior:.0%} for {ticker_bayes}, triggering early exit check")
                            self._check_early_exits(market, scanner, feed)

            # Evaluate strategies against this asset's market + price feed
            # All (asset, strategy) combos start in shadow mode and must prove
            # edge before getting real money. The matrix handles enable/disable.
            is_daily = asset.get("is_daily", False)
            strats = {}

            # For hourly/daily series, find ALL 95-99c strikes across every
            # event window (hourly AND daily). The main market is chosen for
            # favorite_bias (70-90c range), so resolution_rider needs its own.
            # Only enter hourly/daily resolution_rider in the last 5 minutes.
            # 15M markets are naturally constrained; hourly/daily at 95c with
            # 12+ min left can still flip (lost 2 trades at 755s left).
            rr_markets = scanner.get_near_certain_markets(max_hours=8/60) if is_daily else []

            for name, strategy in self.strategies.items():
                # Regime filter: skip strategies disabled by volatility regime
                if name == "favorite_bias" and not regime_params.fav_bias_enabled:
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
                elif name == "consensus" and not regime_params.consensus_enabled:
                    strats[name] = type("R", (), {
                        "signal": Signal.NO_TRADE, "confidence": 0,
                        "reason": f"Disabled in {regime_params.regime.value} vol regime",
                        "should_trade": False,
                    })()
                else:
                    if name == "resolution_rider" and rr_markets:
                        # Evaluate RR against the best 95c+ strike (hourly/daily: 0.5% buffer)
                        rec = strategy.evaluate(rr_markets[0], last_settled, feed, scanner, min_buffer_override=0.5)
                        strats[name] = rec
                    elif market is not None:
                        rec = strategy.evaluate(market, last_settled, feed, scanner)
                        strats[name] = rec
                    else:
                        strats[name] = type("R", (), {
                            "signal": Signal.NO_TRADE, "confidence": 0,
                            "reason": "No active market", "should_trade": False,
                        })()
            all_strategies[key] = strats

            # Also try additional 95c+ strikes from other event windows
            # (e.g., daily 5pm market while the hourly is also running)
            if is_daily and len(rr_markets) > 1:
                rr_strategy = self.strategies.get("resolution_rider")
                for extra_rr in rr_markets[1:]:
                    extra_ticker = extra_rr.get("ticker", "")
                    if extra_ticker in self._traded_tickers:
                        continue
                    if extra_ticker in self.risk_mgr.open_positions:
                        continue
                    rec = rr_strategy.evaluate(extra_rr, last_settled, feed, scanner, min_buffer_override=0.5)
                    if rec.should_trade:
                        extra_strats = {"resolution_rider": rec}
                        stake_override = asset.get("stake_override")
                        self._maybe_trade(extra_rr, extra_strats, stake_override=stake_override, asset_key=key)

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
                # Map daily asset keys to their base for the toggle
                toggle_key = key.replace("_daily", "")
                asset_enabled = _sse_state.enabled_assets.get(toggle_key, True)
                trading_on = _sse_state.trading_enabled
            if market and asset_enabled and trading_on:
                ticker = market.get("ticker", "")
                # For daily markets with multiple strikes (KXBTCD-26APR0702-T68899.99),
                # use the event prefix (KXBTCD-26APR0702) to prevent trading multiple
                # strikes in the same event window.
                if asset.get("is_daily"):
                    parts = ticker.rsplit("-", 1)
                    event_key = parts[0] if len(parts) > 1 else ticker
                else:
                    event_key = ticker
                if ticker and event_key not in self._traded_tickers:
                    stake_override = asset.get("stake_override")
                    self._maybe_trade(market, strats, stake_override=stake_override, asset_key=key)

        # 2a. Refresh RR market cache for the fast path (every 10s)
        self._refresh_rr_cache()

        # 2b. Broad scanner disabled — can't trade non-crypto without price
        # feeds for the buffer check, and the REST sweep adds API load.
        # self._tick_broad_scanner()

        # 3. Display status for primary asset
        primary_key = next(iter(self.assets))
        display_status(
            all_markets[primary_key], self.assets[primary_key]["scanner"],
            self.assets[primary_key]["price_feed"], self.risk_mgr,
            all_strategies[primary_key], self.config,
        )

        # 4. Publish tick data to SSE for dashboard (~4 updates/sec)
        now_pub = time.time()
        if now_pub - getattr(self, '_last_sse_publish', 0) >= 0.25:
            self._last_sse_publish = now_pub
            self._publish_tick(all_markets, all_last_settled, all_strategies)

        # 5. Write heartbeat for external monitoring (~every 5s)
        if now_pub - getattr(self, '_last_heartbeat', 0) >= 5:
            self._last_heartbeat = now_pub
            self._write_heartbeat("running")

        # 6. Save price snapshots for future backtesting (throttled to ~1/min)
        now = time.time()
        if now - getattr(self, '_last_price_save', 0) >= 60:
            self._last_price_save = now
            self._save_price_snapshot()

        # 7. Log strategy matrix summary (~every 5 min)
        if now - getattr(self, '_last_matrix_log', 0) >= 300:
            self._last_matrix_log = now
            summary = self.strategy_matrix.get_summary()
            if "edge=" in summary:
                self._log(f"\n{summary}")

    def _save_price_snapshot(self):
        """Save 1-minute price candles for future backtesting."""
        try:
            price_dir = Path("data/prices")
            price_dir.mkdir(parents=True, exist_ok=True)
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            price_file = price_dir / f"{date_str}.csv"

            # Write header if new file
            if not price_file.exists():
                with open(price_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp", "btc", "eth", "sol"])

            # Append current prices
            row = [
                datetime.now(timezone.utc).isoformat(),
                self.assets["btc"]["price_feed"].current_price or "",
                self.assets["eth"]["price_feed"].current_price or "",
                self.assets["sol"]["price_feed"].current_price or "",
            ]
            with open(price_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except Exception:
            pass  # Don't let price saving crash the bot

    def _tick_broad_scanner(self):
        """Evaluate resolution_rider against broad market scanner candidates."""
        try:
            candidates = self.broad_scanner.get_candidates()
        except Exception as e:
            if not hasattr(self, '_broad_err_count'):
                self._broad_err_count = 0
            self._broad_err_count += 1
            if self._broad_err_count <= 3 or self._broad_err_count % 50 == 0:
                self._log(f"  [BROAD] Scanner error ({self._broad_err_count}x): {e}", level="warning")
            return

        if not candidates:
            return

        balance = self._get_balance()
        rr_strategy = self.strategies.get("resolution_rider")
        if not rr_strategy:
            return

        for candidate in candidates:
            ticker = candidate.get("ticker", "")
            if not ticker or ticker in self._traded_tickers:
                continue

            # Already have a position on this market
            if ticker in self.risk_mgr.open_positions:
                continue

            # Build a lightweight market dict compatible with resolution_rider.evaluate()
            # The strategy only needs: ticker, close_time, and bid/ask from scanner
            market_dict = {
                "ticker": ticker,
                "close_time": candidate.get("close_time", ""),
                "yes_bid": candidate["_yes_bid"],
                "yes_ask": candidate["_yes_ask"],
            }

            # Use a shim scanner that returns the WS-sourced bid/ask
            class _BroadShim:
                def __init__(self, ws_feed, client, mkt):
                    self._ws = ws_feed
                    self.client = client
                    self._mkt = mkt
                def seconds_until_close(self, market):
                    close_str = market.get("close_time", "").replace("Z", "+00:00")
                    try:
                        close_dt = datetime.fromisoformat(close_str)
                        return max(0, (close_dt - datetime.now(timezone.utc)).total_seconds())
                    except (ValueError, TypeError):
                        return 0
                def parse_yes_price(self, market):
                    return (self._mkt["yes_bid"], self._mkt["yes_ask"])
                def get_orderbook(self, ticker, depth=1):
                    # Needed by _maybe_trade for order placement
                    return self.client.get_orderbook(ticker, depth=depth)

            shim = _BroadShim(self.ws_feed, self.client, market_dict)

            rec = rr_strategy.evaluate(market_dict, None, None, shim)
            if not rec.should_trade:
                continue

            # Check strategy matrix — use "broad" as the asset key
            series = candidate.get("series_ticker", "broad")
            if not self.strategy_matrix.is_enabled("broad", "resolution_rider"):
                # Shadow-track the trade for the matrix
                # (We'll record the outcome when it settles)
                pass

            # Build strategy dict for _maybe_trade
            strats = {"resolution_rider": rec}
            self._traded_tickers.add(ticker)
            self.broad_scanner.mark_traded(ticker)

            self._log(f"  [BROAD] Candidate: {ticker} ({candidate['rr_side'].upper()} "
                      f"@{candidate['rr_price']}c, {candidate['rr_secs_left']:.0f}s left, "
                      f"vol={candidate['rr_volume']:.0f})")

            self._maybe_trade(market_dict, strats, asset_key="broad")

    def _maybe_trade(self, market: dict, strategies_status: dict, stake_override: float = None, asset_key: str = ""):
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
            yes_bid, yes_ask = parse_book_top(book)
            if yes_bid is not None and yes_ask is not None:
                self._log(f"      [BOOK] {ticker}: yes_bid={yes_bid} yes_ask={yes_ask}")
        except Exception:
            # Fall back to cached market data
            yes_bid, yes_ask = scanner.parse_yes_price(market)

        # Sanity check: cross-verify book-derived prices against the
        # market's own yes_bid/yes_ask fields. Divergence on EITHER side
        # means the book parse is stale or the market has flipped — skip
        # the trade rather than act on fantasy prices.
        #
        # Checks BOTH bid and ask independently: a one-sided book collapse
        # (e.g., all NO bids pulled after YES becomes favorite) leaves
        # yes_ask=None but yes_bid=99, which can produce a bogus NO-side
        # ask of 1¢ in _maybe_trade's exec_price calculation. The previous
        # `if yes_bid is not None AND yes_ask is not None` gate silently
        # bypassed this case.
        #
        # Threshold 10¢: observed normal divergence on near-the-money
        # markets is 0-4¢; the 2026-04-13 incidents had ~80-95¢ divergence.
        market_bid, market_ask = scanner.parse_yes_price(market)
        if yes_ask is not None and market_ask is not None and abs(yes_ask - market_ask) > 10:
            self._log(
                f"      [SANITY] {ticker}: book yes_ask={yes_ask} diverges from "
                f"market yes_ask={market_ask} — skipping trade",
                level="warning",
            )
            return
        if yes_bid is not None and market_bid is not None and abs(yes_bid - market_bid) > 10:
            self._log(
                f"      [SANITY] {ticker}: book yes_bid={yes_bid} diverges from "
                f"market yes_bid={market_bid} — skipping trade",
                level="warning",
            )
            return

        # ── Shadow tracking: record what disabled strategies WOULD do,
        # so the matrix can evaluate them for future enablement.
        for name, rec in strategies_status.items():
            if rec.should_trade and not self.strategy_matrix.is_enabled(asset_key, name):
                # This strategy wants to trade but is matrix-disabled.
                # We'll track the outcome when the market settles.
                side = "yes" if rec.signal == Signal.BUY_YES else "no"
                shadow_price = rec.max_price_cents
                if not hasattr(self, '_shadow_pending'):
                    self._shadow_pending = {}
                shadow_key = f"{ticker}|{name}|{asset_key}"
                self._shadow_pending[shadow_key] = {
                    "ticker": ticker,
                    "strategy": name,
                    "asset_key": asset_key,
                    "side": side,
                    "price_cents": shadow_price,
                    "contracts": max(1, int(5.0 / (shadow_price / 100.0))) if shadow_price > 0 else 1,
                    "timestamp": time.time(),
                }

        # ── Conflict resolution: if multiple strategies want to trade,
        # pick the best one. Don't allow opposing signals on the same market.
        candidates = [
            (name, rec) for name, rec in strategies_status.items()
            if rec.should_trade
            and not self.strategy_tracker.is_suspended(name)[0]
            and self.strategy_matrix.is_enabled(asset_key, name)
        ]

        if len(candidates) > 1:
            yes_cands = [(n, r) for n, r in candidates if r.signal == Signal.BUY_YES]
            no_cands = [(n, r) for n, r in candidates if r.signal == Signal.BUY_NO]

            if yes_cands and no_cands:
                # Opposing signals — pick the side with the best calibrated edge
                self._log(f"  [CONFLICT] {len(yes_cands)} YES vs {len(no_cands)} NO on {ticker}")

                def _score(name, rec):
                    cal_p = self.calibrator.calibrate(name, rec.confidence)
                    edge = cal_p - rec.max_price_cents / 100.0
                    return cal_p * max(0, edge)

                best = max(candidates, key=lambda x: _score(x[0], x[1]))
                candidates = [best]
            else:
                # All agree on direction — pick highest calibrated confidence
                best = max(candidates, key=lambda x: self.calibrator.calibrate(x[0], x[1].confidence))
                candidates = [best]

        for name, rec in candidates:
            if not rec.should_trade:
                continue

            side = rec.signal.value  # "yes" or "no"
            max_price = rec.max_price_cents  # strategy's ceiling price

            # Calibrate confidence: map raw heuristic to empirical win probability
            calibrated_p = self.calibrator.calibrate(name, rec.confidence)

            # Walk-forward validated bypasses: don't let the calibrator's
            # small-sample shrinkage override backtest-proven probabilities.
            # Evidence: 12-month out-of-sample walk-forward (Oct 2024 - Nov 2025)
            # - Favorite bias: 11,820 trades, 89% WR, 12/12 months profitable
            # - Resolution rider: 18,316 trades, 98% WR, 12/12 months profitable
            # - Consensus: 4,204 trades, 56% WR, 10/11 months profitable
            if name == "favorite_bias" and rec.confidence >= 0.75:
                calibrated_p = max(calibrated_p, rec.confidence)
            elif name == "resolution_rider" and rec.confidence >= 0.60:
                calibrated_p = max(calibrated_p, rec.confidence)
            elif name == "consensus" and rec.confidence >= 0.80:
                calibrated_p = max(calibrated_p, rec.confidence)

            # Determine execution price using time-aware maker/taker logic.
            # With enough time, post passive limit orders (maker, lower fees).
            # Near expiry, cross the spread for guaranteed fills (taker).
            if side == "yes":
                ask_price = yes_ask if yes_ask is not None else max_price
                bid_price = yes_bid if yes_bid is not None else (ask_price - 4)
            else:
                ask_price = (100 - yes_bid) if yes_bid is not None else max_price
                bid_price = (100 - yes_ask) if yes_ask is not None else (ask_price - 4)

            # Don't trade if the ask exceeds strategy's max
            if ask_price > max_price:
                continue

            secs_remaining = scanner.seconds_until_close(market)
            if name == "resolution_rider":
                # Resolution rider: always cross the ask (taker). The edge is
                # only 1-5c so getting filled at the right price matters more
                # than saving fees. Maker midpoint can drop below 95c minimum.
                exec_price = min(ask_price, max_price)
                is_maker = False
            elif secs_remaining > 600:
                # >10 min: passive maker — post at bid+1c
                exec_price = min(bid_price + 1, max_price)
                is_maker = True
            elif secs_remaining > 300:
                # 5-10 min: post at midpoint (still likely maker)
                exec_price = min((bid_price + ask_price) // 2, max_price)
                is_maker = True
            else:
                # <5 min: cross at the ask (taker, guaranteed fill)
                exec_price = min(ask_price, max_price)
                is_maker = False

            # Hard floor: never place an RR order outside the strategy's
            # [min_contract_price, max_entry_price] band. Prevents the
            # 2026-04-13 "NO@1c" incident where a market flipped between
            # RR evaluation and order submission, causing exec_price to
            # collapse while the sanity check was bypassed by a one-sided
            # book (no_bids empty → yes_ask None → sanity check skipped).
            # This guard catches the failure unconditionally at the site
            # where exec_price is known to be final.
            if name == "resolution_rider":
                strat = self.strategies.get("resolution_rider")
                strat_min = strat.min_contract_price if strat else 95
                strat_max = strat.max_entry_price if strat else 98
                if exec_price < strat_min or exec_price > strat_max:
                    self._log(
                        f"  [SKIP] {name}: exec_price {exec_price}c outside "
                        f"[{strat_min}, {strat_max}]c band "
                        f"(book may have moved between signal and submission)",
                        level="warning",
                    )
                    continue

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

            # Position sizing — different approach per strategy
            if name == "resolution_rider":
                # Fixed stake from .env, no Kelly sizing
                MAX_RESOLUTION_RIDER_STAKE = float(os.getenv("RESOLUTION_RIDER_STAKE_USD", "10.00"))
                price_frac = exec_price / 100.0
                if price_frac > 0:
                    contracts = max(1, int(MAX_RESOLUTION_RIDER_STAKE / price_frac))
                    stake = contracts * price_frac
                else:
                    contracts = 1
                    stake = price_frac
            else:
                # Other strategies: standard sizing with $3 cap
                orig_stake = self.risk_mgr.config.stake_usd
                effective_override = stake_override
                if effective_override:
                    self.risk_mgr.config.stake_usd = effective_override
                contracts = self.risk_mgr.calculate_contracts(
                    exec_price, confidence=rec.confidence, balance_usd=balance,
                    calibrated_probability=calibrated_p,
                )
                self.risk_mgr.config.stake_usd = orig_stake
                stake = contracts * (exec_price / 100.0)

                # Cap max loss per trade (optimized: $3 gives best Sortino)
                MAX_LOSS_PER_TRADE = 3.00
                if stake > MAX_LOSS_PER_TRADE:
                    contracts = max(1, int(MAX_LOSS_PER_TRADE / (exec_price / 100.0)))
                    stake = contracts * (exec_price / 100.0)

            # If stake exceeds available balance, downsize to fit remaining cash
            # rather than skipping entirely. Only skip if even 1 contract won't fit.
            if not self.config["paper_trade"] and stake > balance:
                price_frac = exec_price / 100.0
                if price_frac <= 0 or balance < price_frac:
                    self._log(f"  [SKIP] {name}: balance ${balance:.2f} < 1 contract @ {exec_price}c")
                    continue
                new_contracts = int(balance / price_frac)
                new_stake = new_contracts * price_frac
                self._log(f"  [DOWNSIZE] {name}: ${stake:.2f} → ${new_stake:.2f} ({contracts}→{new_contracts} contracts) to fit balance ${balance:.2f}")
                contracts = new_contracts
                stake = new_stake

            # Fee-adjusted EXPECTED VALUE check
            entry_fee_fn = kalshi_maker_fee if is_maker else kalshi_taker_fee
            entry_fee = entry_fee_fn(contracts, exec_price)
            payout = contracts * 1.00

            # Resolution rider uses empirical win rate (99%) for EV, not market price
            # The edge IS the gap between price (95c) and actual win rate (99%)
            ev_prob = 0.99 if name == "resolution_rider" else calibrated_p
            ev_win = ev_prob * (payout - stake - entry_fee)
            ev_loss = (1 - ev_prob) * (stake + entry_fee)
            expected_value = ev_win - ev_loss
            if expected_value <= 0:
                self._log(f"  [SKIP] {name}: negative EV after fees "
                          f"(EV=${expected_value:.2f}, raw_conf={rec.confidence:.0%}, "
                          f"cal_p={calibrated_p:.0%}, fees=${entry_fee:.2f})")
                continue

            # Execute — mark ticker BEFORE placing order to prevent re-entry
            # For daily markets, mark the event prefix to block all strikes in this event
            self._traded_tickers.add(ticker)
            parts = ticker.rsplit("-", 1)
            if len(parts) > 1 and parts[0] != ticker:
                self._traded_tickers.add(parts[0])  # e.g. KXBTCD-26APR0702

            # For Kalshi API: yes_price is always in YES terms
            api_yes_price = exec_price if side == "yes" else (100 - exec_price)

            order_mode = "MAKER" if is_maker else "TAKER"

            # Log orderbook depth for resolution_rider to gauge available liquidity
            if name == "resolution_rider":
                try:
                    _depth_book = scanner.client.get_orderbook(ticker, depth=10)
                    _ob = _depth_book.get("orderbook", {})
                    _yes_bids = _ob.get("yes", [])
                    _no_bids = _ob.get("no", [])
                    # Available to buy YES at 95-99c = NO bids where 100-price is 95-99
                    _yes_available = [(100 - p, q) for p, q in _no_bids if 95 <= (100 - p) <= 99]
                    # Available to buy NO at 95-99c = YES bids where 100-price is 95-99
                    _no_available = [(100 - p, q) for p, q in _yes_bids if 95 <= (100 - p) <= 99]
                    _our_depth = _yes_available if side == "yes" else _no_available
                    _total_cts = sum(q for _, q in _our_depth)
                    _depth_str = ", ".join(f"{p}c:{q:.0f}" for p, q in sorted(_our_depth))
                    self._log(f"      [DEPTH] {side.upper()} 95-99c: {_total_cts:.0f} contracts available [{_depth_str}]")
                except Exception:
                    self._log(f"      [DEPTH] Could not fetch orderbook")

            self._log(f"\n  >>> {name.upper()}: BUY {contracts} {side.upper()} @ {exec_price}c on {ticker} "
                      f"(${stake:.2f}, fees=${entry_fee:.2f}, bal=${balance:.2f}, {order_mode})")
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
                    # Handle slippage model non-fill
                    paper_status = result.get("order", {}).get("status", "filled")
                    if paper_status == "cancelled":
                        self._log(f"  [PAPER] Order not filled (slippage model)")
                        continue
                    # Handle slippage-adjusted fill price and partial fills
                    fill_price = result.get("order", {}).get("fill_price_cents")
                    if fill_price is not None and fill_price != exec_price:
                        exec_price = fill_price
                        stake = contracts * (exec_price / 100.0)
                        entry_fee = entry_fee_fn(contracts, exec_price)
                    filled_count = result.get("order", {}).get("contracts_filled")
                    if filled_count is not None and filled_count < contracts:
                        contracts = filled_count
                        stake = contracts * (exec_price / 100.0)
                        entry_fee = entry_fee_fn(contracts, exec_price)
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
                        # Maker orders get more patience since we're earning
                        # better pricing and lower fees by waiting.
                        secs_in_window = scanner.seconds_until_close(market)
                        if is_maker:
                            # Maker: more patience — the whole point is to wait
                            if secs_in_window > 600:
                                max_wait = 240  # 4 min early in window
                            elif secs_in_window > 300:
                                max_wait = 150  # 2.5 min mid-window
                            else:
                                max_wait = 60   # 1 min (shouldn't hit this often)
                        else:
                            # Taker: shorter patience, we expect immediate fills
                            if secs_in_window > 300:
                                max_wait = 60
                            elif secs_in_window > 120:
                                max_wait = 30
                            else:
                                max_wait = 15
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
                                        # Accept partial fill immediately, cancel the rest
                                        try:
                                            self.client.cancel_order(order_id)
                                        except Exception:
                                            pass
                                        contracts = fill_count
                                        stake = contracts * (exec_price / 100.0)
                                        entry_fee = entry_fee_fn(contracts, exec_price)
                                        self._log(f"      [PARTIAL] Filled {fill_count} contracts, cancelled remainder")
                                        filled = True
                                        break

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
                                        entry_fee = entry_fee_fn(contracts, exec_price)
                                    filled = True
                            except Exception:
                                pass

                        if not filled:
                            # Check for partial fills before cancelling
                            try:
                                fills_resp = self.client.get_fills(order_id=order_id)
                                fills = fills_resp.get("fills", [])
                                fill_count = sum(f.get("count", 0) for f in fills)
                            except Exception:
                                fill_count = 0

                            self._log(f"      [CANCEL] Order {order_id} not fully filled after {max_wait}s, cancelling ({order_mode})")
                            try:
                                self.client.cancel_order(order_id)
                            except Exception:
                                pass

                            if fill_count > 0:
                                # Keep the partial fill — we own these contracts
                                contracts = fill_count
                                stake = contracts * (exec_price / 100.0)
                                entry_fee = entry_fee_fn(contracts, exec_price)
                                self._log(f"      [PARTIAL] Keeping {fill_count} filled contracts after cancel")
                                filled = True
                            else:
                                continue

                self._log(f"      Order placed: {order_id}")

                # Reconcile with actual fills from Kalshi —
                # limit orders can fill at much better prices than requested,
                # and some "taker" orders actually fill as maker ($0 fees).
                if not self.config["paper_trade"]:
                    try:
                        fills_resp = self.client.get_fills(order_id=order_id)
                        fills = fills_resp.get("fills", [])
                        if fills:
                            total_qty = 0
                            total_cost = 0.0
                            total_fees = 0.0
                            any_taker = False
                            for f in fills:
                                qty = int(float(f.get("count_fp", 0) or f.get("count", 0) or 0))
                                if side == "yes":
                                    fill_price = float(f.get("yes_price_dollars", 0) or 0) * 100
                                else:
                                    fill_price = float(f.get("no_price_dollars", 0) or 0) * 100
                                total_qty += qty
                                total_cost += qty * fill_price
                                total_fees += float(f.get("fee_cost", 0) or 0)
                                if f.get("is_taker", True):
                                    any_taker = True
                            if total_qty > 0:
                                avg_fill_price = total_cost / total_qty
                                actual_price = int(round(avg_fill_price))
                                if actual_price != exec_price:
                                    self._log(f"      [FILL] Actual fill: {total_qty} @ {actual_price}c (requested {exec_price}c)")
                                    exec_price = actual_price
                                contracts = total_qty
                                stake = contracts * (exec_price / 100.0)
                                # Use actual fees from Kalshi instead of recomputing
                                is_maker = not any_taker
                                entry_fee = round(total_fees, 2)
                                if is_maker:
                                    self._log(f"      [FILL] Classified as MAKER (fees=${entry_fee:.2f})")
                    except Exception as e:
                        self._log(f"      [WARN] Fill reconciliation failed: {e}", level="warning")

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
                    is_maker=is_maker,
                )
                self.risk_mgr.record_trade(record)
                self.logger.log_trade(record, reason=rec.reason, confidence=rec.confidence)

                # Register with Bayesian updater using calibrated probability as prior
                btc_feed = self.assets.get("btc", {}).get("price_feed")
                btc_px = btc_feed.current_price if btc_feed else None
                if btc_px:
                    self.bayesian.register(
                        ticker=ticker,
                        prior=calibrated_p,
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
                    "profit_after_fees": 0,  # Updated on settlement
                    "order_type": "maker" if is_maker else "taker",
                    "order_id": order_id,
                })
                if len(self._dashboard_trades) > 500:
                    self._dashboard_trades.pop()

            except Exception as e:
                self._log(f"\n      ERROR placing order: {e}", level="error")

    def _check_early_exits(self, market: dict, scanner, price_feed):
        """Check open positions on this market for early exit signals."""
        # Early exits disabled 2026-04-13 — hold every RR position to
        # settlement. See __init__ for the rationale (stop-loss whipsaws).
        if not self._early_exits_enabled:
            return
        ticker = market.get("ticker", "")
        if ticker not in self.risk_mgr.open_positions:
            return
        # Cross-path dedup: if any exit path already submitted a sell on this
        # ticker (even if it's still resting on the book, not yet filled),
        # don't fire another one. Prevents the 2026-04-13 repeated-sell bug
        # where a pending limit order caused 5 sells of the same 102 contracts.
        if not hasattr(self, '_exit_pending'):
            self._exit_pending = set()
        if ticker in self._exit_pending:
            return

        record = self.risk_mgr.open_positions[ticker]
        secs_left = scanner.seconds_until_close(market)

        # Get current book prices
        try:
            book = scanner.client.get_orderbook(ticker, depth=1)
            yes_bid, yes_ask = parse_book_top(book)
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

        sell_side = record.side  # Sell the same side we hold
        # Exit price: YES sells at yes_bid; NO sells at 100-yes_ask
        if sell_side == "yes":
            exit_price = yes_bid
        else:
            exit_price = 100 - yes_ask

        if self.config["paper_trade"]:
            # Paper mode: simulate the sell at current book price
            exit_proceeds = record.contracts * (exit_price / 100.0)
            record.outcome = "win" if exit_proceeds > record.stake_usd else "loss"
            record.payout_usd = exit_proceeds
            record.profit_usd = exit_proceeds - record.stake_usd
            record.profit_after_fees = record.profit_usd - record.entry_fee_usd
            self.risk_mgr.open_positions.pop(ticker, None)
            if record.profit_usd < 0:
                self.risk_mgr._last_loss_ts = time.time()

            self._log(f"      [PAPER] Sold {record.contracts} {sell_side} @ {exit_price}c (P&L: ${record.profit_usd:+.2f})")
            self.logger.log_settlement(record)
            self.perf_tracker.record(record.profit_usd, record.timestamp)
            self._record_outcome(record)

            # Update dashboard
            for dt in self._dashboard_trades:
                if dt["ticker"] == ticker and dt["outcome"] == "pending":
                    dt["outcome"] = record.outcome
                    dt["profit"] = round(record.profit_usd, 2)
                    dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                    break
        else:
            # Live mode: place a real sell order
            try:
                if sell_side == "yes":
                    sell_yes_price = yes_bid
                else:
                    sell_yes_price = yes_ask  # selling NO at yes_ask

                # Mark as exit-pending BEFORE submitting, so concurrent exit
                # paths can't fire a second sell while this one is in flight.
                self._exit_pending.add(ticker)

                # reduce_only is CRITICAL: without it, Kalshi treats "sell yes"
                # as opening a new short (equivalent to a new NO long) rather
                # than closing the existing long YES. That's what caused the
                # 2026-04-13 XRP "buy NO at 20c" incident.
                result = self.client.place_order(
                    ticker=ticker,
                    action="sell",
                    side=sell_side,
                    count=record.contracts,
                    order_type="limit",
                    yes_price=sell_yes_price,
                    reduce_only=True,
                )

                order_status = result.get("order", {}).get("status", "")
                if order_status in ("executed", "filled"):
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
                    self.perf_tracker.record(record.profit_usd, record.timestamp)
                    self._record_outcome(record)

                    # Update dashboard
                    for dt in self._dashboard_trades:
                        if dt["ticker"] == ticker and dt["outcome"] == "pending":
                            dt["outcome"] = record.outcome
                            dt["profit"] = round(record.profit_usd, 2)
                            dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                            break
                elif order_status == "canceled":
                    # IoC + reduce_only: order was cancelled because nothing
                    # filled (price moved, or no match). Clear the guard so
                    # the next tick can retry at a fresh price.
                    self._exit_pending.discard(ticker)
                    self._log(
                        f"      [WARN] Exit canceled (IoC unfilled at {sell_yes_price}c); "
                        f"will retry next tick."
                    )
                else:
                    self._log(
                        f"      [WARN] Exit order resting (status={order_status}); "
                        f"keeping ticker in _exit_pending to block re-fire. "
                        f"Position will reconcile on fill/settlement."
                    )
            except Exception as e:
                # Placement failed — clear the guard so a later tick can retry.
                self._exit_pending.discard(ticker)
                self._log(f"      [WARN] Exit failed: {e}", level="warning")

    def _force_early_exit(self, ticker: str, record, scanner, reason: str):
        """Force an early exit when Bayesian posterior collapses.

        Bypasses the exit_monitor's stop loss threshold — the Bayesian signal
        is faster and more accurate than stale orderbook prices.
        """
        # Early exits disabled 2026-04-13 — hold every RR position to
        # settlement. The bayesian-collapse exit was also whipsawing on the
        # same trades as the stop loss.
        if not self._early_exits_enabled:
            return
        if ticker not in self.risk_mgr.open_positions:
            return

        # Cross-path dedup shared with _check_early_exits.
        if not hasattr(self, '_exit_pending'):
            self._exit_pending = set()
        if ticker in self._exit_pending:
            return

        # Legacy per-path guard (kept for belt-and-suspenders)
        if not hasattr(self, '_bayes_exit_attempted'):
            self._bayes_exit_attempted = set()
        if ticker in self._bayes_exit_attempted:
            return
        self._bayes_exit_attempted.add(ticker)

        # Get current book prices for the exit
        try:
            book = scanner.client.get_orderbook(ticker, depth=1)
            yes_bid, yes_ask = parse_book_top(book)
        except Exception:
            self._log(f"      [WARN] Bayesian exit: couldn't get orderbook for {ticker}")
            return

        if yes_bid is None or yes_ask is None:
            return

        sell_side = record.side
        if sell_side == "yes":
            exit_price = yes_bid
        else:
            exit_price = 100 - yes_ask

        self._log(f"\n  <<< BAYES EXIT {ticker}: {reason}")

        if self.config["paper_trade"]:
            exit_proceeds = record.contracts * (exit_price / 100.0)
            record.outcome = "win" if exit_proceeds > record.stake_usd else "loss"
            record.payout_usd = exit_proceeds
            record.profit_usd = exit_proceeds - record.stake_usd
            record.profit_after_fees = record.profit_usd - record.entry_fee_usd
            self.risk_mgr.open_positions.pop(ticker, None)
            if record.profit_usd < 0:
                self.risk_mgr._last_loss_ts = time.time()

            self._log(f"      [PAPER] Sold {record.contracts} {sell_side} @ {exit_price}c (P&L: ${record.profit_usd:+.2f})")
            self.logger.log_settlement(record)
            self.perf_tracker.record(record.profit_usd, record.timestamp)
            self._record_outcome(record)
        else:
            try:
                sell_yes_price = yes_bid if sell_side == "yes" else yes_ask
                # Mark pending BEFORE submitting (cross-path dedup).
                self._exit_pending.add(ticker)
                # reduce_only=True: close the existing position, don't open a new one.
                result = self.client.place_order(
                    ticker=ticker,
                    action="sell",
                    side=sell_side,
                    count=record.contracts,
                    order_type="limit",
                    yes_price=sell_yes_price,
                    reduce_only=True,
                )

                order_status = result.get("order", {}).get("status", "")
                if order_status in ("executed", "filled"):
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
                    self.perf_tracker.record(record.profit_usd, record.timestamp)
                    self._record_outcome(record)
                elif order_status == "canceled":
                    # IoC + reduce_only: cancelled unfilled. Allow retry.
                    self._exit_pending.discard(ticker)
                    self._bayes_exit_attempted.discard(ticker)
                    self._log(
                        f"      [WARN] Bayes exit canceled (IoC unfilled at {sell_yes_price}c); "
                        f"will retry next tick."
                    )
                else:
                    self._log(
                        f"      [WARN] Bayes exit order resting (status={order_status}); "
                        f"keeping ticker in _exit_pending to block re-fire. "
                        f"Position will reconcile on fill/settlement."
                    )
            except Exception as e:
                # Placement failed — clear both guards so a later tick can retry.
                self._exit_pending.discard(ticker)
                self._bayes_exit_attempted.discard(ticker)
                self._log(f"      [WARN] Bayes exit failed: {e}", level="warning")

        self.bayesian.clear(ticker)
        self.exit_monitor.clear_ticker(ticker)

        # Update dashboard
        for dt in self._dashboard_trades:
            if dt["ticker"] == ticker and dt["outcome"] == "pending":
                dt["outcome"] = record.outcome
                dt["profit"] = round(record.profit_usd, 2)
                dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                break

    def _check_settlements(self):
        """Check settlements across all asset scanners + broad positions."""
        for asset in self.assets.values():
            self._check_settlements_for(asset["scanner"])
        self._check_broad_settlements()

    def _check_broad_settlements(self):
        """Check settlement for positions from the broad scanner (non-series markets)."""
        now = time.time()
        if now - getattr(self, '_last_broad_settle_check', 0) < 30:
            return
        self._last_broad_settle_check = now

        known_series = {a["series"] for a in self.assets.values()}
        for ticker in list(self.risk_mgr.open_positions.keys()):
            # Skip positions that belong to a known series scanner
            if any(s in ticker for s in known_series):
                continue
            try:
                market_data = self.client.get_market(ticker)
                m = market_data.get("market", market_data)
                if m.get("status") not in ("settled", "finalized") or not m.get("result"):
                    continue
                result = m["result"]
                self._log(f"  [BROAD-SETTLE] {ticker} -> {result}")
                self.risk_mgr.settle_trade(ticker, result)
                record = next(
                    (t for t in self.risk_mgr.trades if t.ticker == ticker and t.outcome != ""),
                    None,
                )
                if record:
                    emoji = "WIN" if record.outcome == "win" else "LOSS"
                    self._log(f"\n  <<< SETTLED (broad) {ticker}: {emoji} (${record.profit_usd:+.2f})")
                    self.logger.log_settlement(record)
                    self.perf_tracker.record(record.profit_usd, record.timestamp)
                    self._record_outcome(record)
                    if self.broad_scanner:
                        self.broad_scanner.clear_traded(ticker)
                    for dt in self._dashboard_trades:
                        if dt["ticker"] == ticker and dt["outcome"] == "pending":
                            dt["outcome"] = record.outcome
                            dt["profit"] = round(record.profit_usd, 2)
                            dt["fees"] = round(record.entry_fee_usd + record.settle_fee_usd, 2)
                            dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                            break
            except Exception as e:
                pass  # Will retry in 30s

    def _check_settlements_for(self, scanner):
        """Check if any open positions have settled (in-memory + CSV)."""
        settled_markets = scanner.get_settled_markets(limit=50)
        settled_by_ticker = {m["ticker"]: m.get("result") for m in settled_markets if m.get("result")}

        # For positions not found in the bulk query (common with daily markets
        # that have 100+ strikes per event), check each ticker directly via API
        for ticker in list(self.risk_mgr.open_positions.keys()):
            if ticker in settled_by_ticker:
                continue
            if scanner.series not in ticker:
                continue
            try:
                market_data = scanner.client.get_market(ticker)
                m = market_data.get("market", market_data)
                if m.get("status") in ("settled", "finalized") and m.get("result"):
                    settled_by_ticker[ticker] = m["result"]
                    self._log(f"  [SETTLE] Found settlement via direct lookup: {ticker} -> {m['result']}")
            except Exception:
                pass

        # 1. Settle in-memory positions (current session trades)
        for ticker in list(self.risk_mgr.open_positions.keys()):
            result = settled_by_ticker.get(ticker)
            if not result:
                continue
            self.risk_mgr.settle_trade(ticker, result)
            self.bayesian.clear(ticker)
            self._resolve_shadow_trades(ticker, result)  # Settle shadow trades too
            record = next(
                (t for t in self.risk_mgr.trades if t.ticker == ticker and t.outcome != ""),
                None,
            )
            if record:
                emoji = "WIN" if record.outcome == "win" else "LOSS"
                self._log(f"\n  <<< SETTLED {ticker}: {emoji} (${record.profit_usd:+.2f})")
                self.logger.log_settlement(record)
                self.perf_tracker.record(record.profit_usd, record.timestamp)
                self._record_outcome(record)
                # Update dashboard trade
                for dt in self._dashboard_trades:
                    if dt["ticker"] == ticker and dt["outcome"] == "pending":
                        dt["outcome"] = record.outcome
                        dt["profit"] = round(record.profit_usd, 2)
                        dt["fees"] = round(record.entry_fee_usd + record.settle_fee_usd, 2)
                        dt["profit_after_fees"] = round(record.profit_after_fees, 2)
                        break

        # 1b. Resolve shadow trades for ALL settled markets (not just our positions)
        for ticker, result in settled_by_ticker.items():
            self._resolve_shadow_trades(ticker, result)

        # 2. Also resolve any unsettled CSV trades from previous runs
        # Throttle per-scanner (not global) so each asset type gets checked
        now = time.time()
        if not hasattr(self, '_csv_settle_checks'):
            self._csv_settle_checks = {}
        if now - self._csv_settle_checks.get(scanner.series, 0) < 60:
            return
        self._csv_settle_checks[scanner.series] = now
        unsettled = self.logger.get_unsettled_trades()
        lookups_this_cycle = 0
        max_lookups_per_cycle = 3  # Cap individual API calls to avoid rate-limiting
        for trade_row in unsettled:
            ticker = trade_row["ticker"]

            # Only handle trades belonging to THIS scanner's series
            if scanner.series not in ticker:
                continue

            result = settled_by_ticker.get(ticker)

            # Direct API fallback (matches in-memory position handling)
            if not result:
                if lookups_this_cycle >= max_lookups_per_cycle:
                    continue  # Skip — will retry next cycle
                lookups_this_cycle += 1
                try:
                    market_data = scanner.client.get_market(ticker)
                    m = market_data.get("market", market_data)
                    if m.get("status") in ("settled", "finalized") and m.get("result"):
                        result = m["result"]
                        self._log(f"  [SETTLE] CSV trade found via direct lookup: {ticker} -> {result}")
                except Exception:
                    pass

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
                record.settle_fee_usd = 0.0
            else:
                record.outcome = "loss"
                record.payout_usd = 0.0
                record.profit_usd = -stake_usd
                record.settle_fee_usd = 0.0
            # Entry fee: compute from price (taker fee on entry)
            record.entry_fee_usd = kalshi_taker_fee(contracts, record.price_cents)
            record.profit_after_fees = record.profit_usd - record.entry_fee_usd

            self.logger.log_settlement(record)
            self.perf_tracker.record(record.profit_usd, record.timestamp)
            self._record_outcome(record)
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

        # Stats — reload from CSV periodically (every 30s)
        # The CSV is the single source of truth, kept in sync with Kalshi by
        # the reconcile thread. Both stats AND trade list get refreshed here.
        if not hasattr(self, '_stats_refresh_ts') or time.time() - self._stats_refresh_ts > 30:
            self._hist_stats = self.logger.get_historical_stats()
            self._hist_stats["pending"] = len([t for t in self.risk_mgr.trades if t.outcome == ""])
            # Also refresh the dashboard trade list from CSV
            self._dashboard_trades = self._hist_stats["trades"]
            self._stats_refresh_ts = time.time()

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
                "total_trades": self._hist_stats["total_trades"],
                "pending": self._hist_stats["pending"],
                "wins": self._hist_stats["wins"],
                "losses": self._hist_stats["losses"],
                "win_rate": round(self._hist_stats["wins"] / max(1, self._hist_stats["total_trades"]) * 100, 1),
                "total_pnl": self._hist_stats["alltime_gross"],
                "total_fees": self._hist_stats["alltime_fees"],
                "total_pnl_after_fees": self._hist_stats["alltime_net"],
                "daily_pnl": self._hist_stats["daily_gross"],
                "daily_pnl_after_fees": self._hist_stats["daily_net"],
                "daily_fees": self._hist_stats["daily_fees"],
                "weekly_pnl": self._hist_stats["weekly_gross"],
                "weekly_pnl_net": self._hist_stats["weekly_net"],
                "weekly_fees": self._hist_stats["weekly_fees"],
                "monthly_pnl": self._hist_stats["monthly_gross"],
                "monthly_pnl_net": self._hist_stats["monthly_net"],
                "monthly_fees": self._hist_stats["monthly_fees"],
                "alltime_pnl": self._hist_stats["alltime_gross"],
                "alltime_pnl_net": self._hist_stats["alltime_net"],
                "alltime_fees": self._hist_stats["alltime_fees"],
                "bot_paused": self.risk_mgr.daily_pnl <= -self.config["max_daily_loss"],
                "paper_balance": round(balance, 2) if is_paper else None,
                "live_balance": round(balance, 2) if not is_paper else None,
                "is_paper": is_paper,
            },
            "strategy_matrix": self.strategy_matrix.get_matrix_snapshot(),
            "rr_config": {
                "defaults": {
                    "min_contract_price": rr.min_contract_price,
                    "max_entry_price": rr.max_entry_price,
                    "min_seconds": rr.min_seconds,
                    "max_seconds": rr.max_seconds,
                    "min_price_buffer_pct": rr.min_price_buffer_pct,
                    "max_adverse_momentum": rr.max_adverse_momentum,
                    "max_stake_usd": 10.0,
                },
                "per_cell": {k: {
                    "price": f"{v['min_contract_price']}-{v['max_entry_price']}c",
                    "max_secs": v["max_seconds"],
                    "buffer": f"{v['min_price_buffer_pct']}%",
                    "cv_wr": v.get("cv_mean_win_rate"),
                    "cv_trades": v.get("cv_total_val_trades", 0),
                } for k, v in self._rr_cell_params.items()},
            } if (rr := self.strategies.get("resolution_rider")) else {},
        }

        with _sse_state.lock:
            _sse_state.tick_data = json.dumps(tick_data)

    def _asset_key_from_ticker(self, ticker: str) -> str:
        """Derive the asset key from a market ticker for matrix tracking."""
        if "KXBTC15M" in ticker: return "btc"
        if "KXBTCD" in ticker: return "btc_daily"
        if "KXETH15M" in ticker: return "eth"
        if "KXETHD" in ticker: return "eth_daily"
        if "KXSOL15M" in ticker: return "sol"
        if "KXSOLD" in ticker: return "sol_daily"
        return "unknown"

    def _resolve_shadow_trades(self, ticker: str, result: str):
        """Resolve pending shadow trades when a market settles."""
        if not hasattr(self, '_shadow_pending'):
            return

        resolved = []
        for key, shadow in list(self._shadow_pending.items()):
            if shadow["ticker"] != ticker:
                continue

            side = shadow["side"]
            price = shadow["price_cents"]
            contracts = shadow["contracts"]
            stake = contracts * (price / 100.0)

            if side == result:
                pnl = contracts * 1.00 - stake
                outcome = "win"
            else:
                pnl = -stake
                outcome = "loss"

            self.strategy_matrix.record_shadow_trade(
                asset=shadow["asset_key"],
                strategy=shadow["strategy"],
                pnl=pnl,
                stake=stake,
                outcome=outcome,
            )
            resolved.append(key)
            self._log(
                f"  [SHADOW] {shadow['asset_key']}/{shadow['strategy']}: "
                f"{outcome.upper()} ${pnl:+.2f} on {ticker}"
            )

        for key in resolved:
            del self._shadow_pending[key]

    def _record_outcome(self, record):
        """Record a trade outcome to both strategy_tracker and strategy_matrix."""
        self.strategy_tracker.record_outcome(record.strategy, record.profit_usd)
        # Refresh historical stats so dashboard P&L stays current
        self._hist_stats = self.logger.get_historical_stats()
        asset_key = self._asset_key_from_ticker(record.ticker)
        self.strategy_matrix.record_trade(
            asset=asset_key,
            strategy=record.strategy,
            pnl=record.profit_usd,
            stake=record.stake_usd,
            outcome=record.outcome,
        )

    def _write_heartbeat(self, status: str = "running"):
        """Write heartbeat file for external monitoring."""
        try:
            heartbeat_path = Path("data/heartbeat.json")
            heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            heartbeat_path.write_text(json.dumps({
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "open_positions": len(self.risk_mgr.open_positions),
                "run_id": self.run_id,
                "mode": "paper" if self.config["paper_trade"] else "live",
            }))
        except Exception:
            pass  # Don't let heartbeat failure crash the bot

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

            # Performance metrics
            perf = self.perf_tracker.compute()
            self._log(f"\n  Risk-Adjusted Metrics:")
            self._log(f"    Sharpe:         {perf.sharpe_ratio:.2f}")
            self._log(f"    Sortino:        {perf.sortino_ratio:.2f}")
            self._log(f"    Profit Factor:  {perf.profit_factor:.2f}")
            self._log(f"    Max Drawdown:   ${perf.max_drawdown_usd:.2f} ({perf.max_drawdown_pct:.1f}%)")
            self._log(f"    Calmar:         {perf.calmar_ratio:.2f}")
            self._log(f"    Expectancy:     ${perf.expectancy:+.2f}/trade")
            summary["performance"] = {
                "sharpe_ratio": round(perf.sharpe_ratio, 3),
                "sortino_ratio": round(perf.sortino_ratio, 3),
                "profit_factor": round(perf.profit_factor, 3),
                "max_drawdown_usd": round(perf.max_drawdown_usd, 2),
                "max_drawdown_pct": round(perf.max_drawdown_pct, 1),
                "calmar_ratio": round(perf.calmar_ratio, 3),
                "expectancy": round(perf.expectancy, 3),
            }
        else:
            self._log("  No completed trades.")

        if pending:
            self._log(f"\n  Pending trades: {len(pending)}")
            pending_list = []
            for t in pending:
                self._log(f"    {t.ticker} ({t.strategy}) {t.side} @ {t.price_cents}c x{t.contracts}")

                # Fetch current market price and compute unrealized P&L
                unrealized_pnl = None
                try:
                    scanner = next(
                        (a["scanner"] for a in self.assets.values()
                         if a["scanner"].series in t.ticker),
                        self.scanner,
                    )
                    book = scanner.client.get_orderbook(t.ticker, depth=1)
                    yes_bid, yes_ask = parse_book_top(book)

                    if t.side == "yes" and yes_bid is not None:
                        current_value = t.contracts * (yes_bid / 100.0)
                        unrealized_pnl = current_value - t.stake_usd
                    elif t.side == "no" and yes_ask is not None:
                        current_value = t.contracts * ((100 - yes_ask) / 100.0)
                        unrealized_pnl = current_value - t.stake_usd

                    if unrealized_pnl is not None:
                        self._log(f"      Unrealized P&L: ${unrealized_pnl:+.2f}")

                    # In live mode, attempt to unwind the position
                    if not self.config["paper_trade"] and yes_bid is not None:
                        # Respect cross-path exit dedup: don't stack an unwind sell
                        # on top of an already-in-flight exit order.
                        if hasattr(self, '_exit_pending') and t.ticker in self._exit_pending:
                            self._log(f"      [UNWIND] Skipping {t.ticker} — exit already in flight")
                        else:
                            sell_price = yes_bid if t.side == "yes" else yes_ask
                            if sell_price is not None:
                                self._log(f"      [UNWIND] Placing sell order for {t.ticker}...")
                                try:
                                    # reduce_only=True: close the position, don't open a new one.
                                    result = self.client.place_order(
                                        ticker=t.ticker, action="sell", side=t.side,
                                        count=t.contracts, order_type="limit",
                                        yes_price=sell_price,
                                        reduce_only=True,
                                    )
                                    oid = result.get("order", {}).get("order_id", "unknown")
                                    self._log(f"      [UNWIND] Sell order placed: {oid}")
                                except Exception as e:
                                    self._log(f"      [UNWIND] FAILED to sell {t.ticker}: {e}")
                except Exception as e:
                    self._log(f"      [WARN] Could not fetch market data for {t.ticker}: {e}")

                pending_list.append({
                    "ticker": t.ticker, "strategy": t.strategy,
                    "side": t.side, "price_cents": t.price_cents,
                    "contracts": t.contracts, "stake_usd": round(t.stake_usd, 2),
                    "unrealized_pnl": round(unrealized_pnl, 2) if unrealized_pnl is not None else None,
                })
            summary["pending_detail"] = pending_list

            if not self.config["paper_trade"] and pending:
                self._log(f"\n  WARNING: {len(pending)} positions may still be open. Check Kalshi dashboard.")

        # Persist session summary to JSON
        sessions_dir = Path("data/sessions")
        sessions_dir.mkdir(parents=True, exist_ok=True)
        summary_path = sessions_dir / f"{self.run_id}.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        # Write final heartbeat (stopped status)
        self._write_heartbeat("stopped")

        self._log(f"\n  Trade log:       {self.logger.csv_path}")
        self._log(f"  Session summary: {summary_path}")
        self._log(f"  Full log:        data/logs/bot_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log")
        self._log(f"{'='*72}")

    def stop(self):
        """Signal the bot to stop."""
        self.running = False
        self.ws_feed.stop()


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
