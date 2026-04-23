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
import math
import os
import re
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path
from typing import Optional

import requests as _requests
from dotenv import load_dotenv

from kalshi_client import KalshiClient
from kalshi_ws import KalshiWebSocket
from market_scanner import MarketScanner
from rti_feed import RTIFeed
from strategy_matrix import StrategyMatrix
from risk_manager import RiskConfig, RiskManager, TradeRecord, kalshi_taker_fee, kalshi_maker_fee
from strategies import ResolutionRiderStrategy, Signal
from multi_feed import MultiExchangeFeed
from performance import PerformanceTracker


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


_STRIKE_TITLE_RE = re.compile(r'\$([0-9,]+\.?\d*)')


# Per-cell optimizer output from rr_params.json is the source of truth
# for gate values. The previous HEURISTIC_PARAMS overlay was removed on
# 2026-04-22 after analysis showed it was uniformly loosening safety on
# the bot's best cells (btc_15m, btc_hourly, eth_15m — trained buffers
# 0.46-0.68% forced down to 0.15%) while tightening the noisy ones — a
# bad tradeoff when cells that pass the optimizer's CV gate already
# encode per-cell-specific safety in their trained thresholds.
#
# Defense in depth is still in place at the strategy level
# (resolution_rider.py): max_entry_price clamped ≤ 97c (98c trap) and
# min_price_buffer_pct floored at 0.15% (the specific incident that
# triggered the overlay). Those are surgical — they bite only on the
# two known failure modes, not across every dimension of every cell.


def _classify_ticker_cell(ticker: str) -> Optional[str]:
    """Map a Kalshi ticker to its cell name (e.g. 'btc_15m', 'eth_hourly').
    Returns None for non-crypto or unrecognized tickers. Mirrors
    optimize_rr.classify_ticker but kept inline to avoid the optimizer
    import pulling in numpy/pandas at bot startup.
    """
    if not ticker:
        return None
    series = ticker.upper().split("-", 1)[0]
    if series == "KXSHIBAD":
        return "shiba_daily"
    for coin in ("BTC", "ETH", "SOL", "DOGE", "XRP", "BNB", "HYPE"):
        if series == f"KX{coin}15M":
            return f"{coin.lower()}_15m"
        if series == f"KX{coin}D":
            return f"{coin.lower()}_hourly"
    return None


def load_recent_cell_pnl(days: int = 7,
                         csv_path: Optional[Path] = None) -> dict[str, dict]:
    """Compute per-cell settled P&L from live_trades.csv over the last N days.

    Returns {cell_name: {"n_trades": int, "profit_usd": float, "wins": int,
                          "worst_loss_usd": float}}.
    `worst_loss_usd` is the absolute value of the largest single losing
    trade in the window (0.0 if no losses). Cancels and pending orders
    contribute nothing. Only resolution_rider strategy rows are counted.

    This is the ground-truth reality check used by evaluate_cell_safety —
    no CV, no simulator, just what we actually made.
    """
    path = csv_path or Path("data/live_trades.csv")
    if not path.exists():
        return {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result: dict[str, dict] = {}
    try:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                if (row.get("strategy") or "") != "resolution_rider":
                    continue
                outcome = (row.get("outcome") or "").strip()
                if outcome not in ("win", "loss"):
                    continue
                t_str = (row.get("time") or "").replace("Z", "+00:00")
                try:
                    t = datetime.fromisoformat(t_str)
                except ValueError:
                    continue
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                if t < cutoff:
                    continue
                cell = _classify_ticker_cell(row.get("ticker", ""))
                if not cell:
                    continue
                slot = result.setdefault(
                    cell, {"n_trades": 0, "profit_usd": 0.0,
                           "wins": 0, "worst_loss_usd": 0.0})
                slot["n_trades"] += 1
                if outcome == "win":
                    slot["wins"] += 1
                try:
                    p = float(row.get("profit_usd") or 0)
                except ValueError:
                    p = 0.0
                slot["profit_usd"] += p
                if outcome == "loss" and p < 0:
                    loss_abs = -p
                    if loss_abs > slot["worst_loss_usd"]:
                        slot["worst_loss_usd"] = loss_abs
    except OSError:
        return {}
    return result


# Minimum settled trades required in the 7d window before we'll disable
# a cell on the basis of cumulative P&L. Under this, there's not enough
# evidence to veto on the cumulative rule — let the cell gather data.
# The loss-count veto (MAX_7D_LOSSES_VETO) still applies at any n.
MIN_CELL_VETO_TRADES = int(os.environ.get("MIN_CELL_VETO_TRADES", "5"))

# Loss-count veto: if a cell logs ≥ this many settled losses in the
# rolling 7-day window, disable it regardless of cumulative P&L or
# trade count. Previously this was a dollar-amount threshold
# (MAX_SINGLE_LOSS_VETO_USD=$50), but that fired on every ordinary 97c
# loss at $100 stakes — disabling profitable cells on a single bad
# fill. Counts are stake-insensitive: 3+ losses in 7d is a signal
# regardless of whether stakes were $10 or $500.
#
# Default 3: at 95% WR a cell should see ≤2 losses/7d on realistic
# volume; 3+ is outside the noise distribution. Env-tunable.
MAX_7D_LOSSES_VETO = int(os.environ.get("MAX_7D_LOSSES_VETO", "3"))


def evaluate_cell_safety(cell_name: str, v: dict,
                         pnl_by_cell: Optional[dict] = None,
                         enable_all: bool = False,
                         safety_margin: float = 0.0) -> tuple[bool, str]:
    """Decide whether a cell should be allowed to trade, from live P&L.

    Returns (enabled, reason). reason is a short human string when
    disabled, or "" when enabled.

    Rules, in order:
      1. RR_ENABLE_ALL=1 → always enable (validation mode).
      2. ≥ MAX_7D_LOSSES_VETO settled losses in the 7d window → DISABLE
         (pattern-of-losses veto; applies at any trade count).
      3. Fewer than MIN_CELL_VETO_TRADES settled trades in the last 7
         days → enable (insufficient evidence for cumulative veto).
      4. Non-negative 7-day cumulative P&L → enable.
      5. Otherwise → disable with the numbers in the reason string.

    `v` and `safety_margin` are accepted for backward compatibility
    with callers that still pass them; the new gate ignores them.
    """
    if enable_all:
        return True, ""
    stats = (pnl_by_cell or {}).get(
        cell_name,
        {"n_trades": 0, "profit_usd": 0.0, "wins": 0, "worst_loss_usd": 0.0})
    n = stats["n_trades"]
    w = stats.get("wins", 0)
    n_losses = max(0, n - w)
    if n_losses >= MAX_7D_LOSSES_VETO:
        return False, (
            f"loss-count veto: {n_losses} losses in 7d "
            f"(≥ {MAX_7D_LOSSES_VETO} threshold; {n} trades, "
            f"worst ${stats.get('worst_loss_usd', 0.0):.2f})")
    p = stats["profit_usd"]
    if n < MIN_CELL_VETO_TRADES:
        return True, ""
    if p >= 0:
        return True, ""
    return False, (f"7d live P&L ${p:+.2f} over {n} trades "
                   f"({w}W/{n_losses}L); needs ≥$0")


def best_strike_for_market(market: dict):
    """Return the best-available strike (float) for a Kalshi market, or None.

    Kalshi's daily/hourly markets populate `floor_strike` directly. The 15M
    markets do not — the strike is embedded in the `subtitle`/`title` text
    like "$66,838.62 target", so we regex it out as a fallback. This helper
    is the single source of truth used by both live trading (for buffer
    computation) and the tick recorder (for persisting strike alongside
    bid/ask so the optimizer can reconstruct buffer during CV).
    """
    fs = market.get("floor_strike")
    if fs is not None:
        try:
            return float(fs)
        except (ValueError, TypeError):
            pass
    for field in ("subtitle", "title", "yes_sub_title", "no_sub_title"):
        text = market.get(field, "") or ""
        match = _STRIKE_TITLE_RE.search(text)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


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
        "stake_usd": args.stake or float(os.getenv("STAKE_USD", "10.00")),
        "max_daily_loss": float(os.getenv("MAX_DAILY_LOSS_USD", "25.00")),
        "max_concurrent": int(os.getenv("MAX_CONCURRENT_POSITIONS", "3")),
        "poll_interval": 0,  # Legacy — bot runs at full speed via WebSocket
        "strategy": args.strategy or os.getenv("STRATEGY", "resolution_rider"),
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
        # Single lock for all CSV mutations. upsert_entry does a
        # read-modify-write cycle, so concurrent writers without this
        # would silently lose rows.
        self._lock = threading.Lock()
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

    def _row_for(self, record: TradeRecord, reason: str, confidence: Optional[float]) -> list:
        return [
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
        ]

    def log_trade(self, record: TradeRecord, reason: str = "", confidence: float = None):
        with self._lock:
            with open(self.csv_path, "a", newline="") as f:
                csv.writer(f).writerow(self._row_for(record, reason, confidence))

    def upsert_entry(self, record: TradeRecord, reason: str = "", confidence: float = None):
        """Write or update a NON-settlement row for `record.order_id`.

        Exists to close the reconcile race: right after the bot places an
        order and receives an `order_id`, we immediately upsert a pending
        row so the reconcile thread finds the oid in `all_oids` and won't
        write a competing `kalshi_api_import` row. As the fill-wait loop
        learns more (partial fill, full fill, cancellation), it calls this
        again to rewrite the same row in place with the final shape.
        """
        if not record.order_id:
            return self.log_trade(record, reason=reason, confidence=confidence)
        with self._lock:
            new_row = self._row_for(record, reason, confidence)
            if not self.csv_path.exists():
                with open(self.csv_path, "a", newline="") as f:
                    csv.writer(f).writerow(new_row)
                return
            with open(self.csv_path, "r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if not rows:
                with open(self.csv_path, "a", newline="") as f:
                    csv.writer(f).writerow(new_row)
                return
            header = rows[0]
            try:
                oid_idx = header.index("order_id")
                reason_idx = header.index("reason")
            except ValueError:
                with open(self.csv_path, "a", newline="") as f:
                    csv.writer(f).writerow(new_row)
                return
            # Match the LAST non-settlement row with this oid so partial-fill
            # updates land on the entry row rather than overwriting a settlement.
            target = None
            for i in range(len(rows) - 1, 0, -1):
                r = rows[i]
                if len(r) <= oid_idx:
                    continue
                if r[oid_idx] != record.order_id:
                    continue
                if r[reason_idx].startswith("SETTLED:"):
                    continue
                target = i
                break
            if target is None:
                with open(self.csv_path, "a", newline="") as f:
                    csv.writer(f).writerow(new_row)
                return
            rows[target] = new_row
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(rows)

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

        # Return trade rows (not settlement rows) whose order_id hasn't been settled.
        # Skip CANCELLED/REJECTED rows and any zero-contract row: those represent
        # orders that never filled, so there is nothing to settle. Without this
        # guard the settlement loop would write a phantom SETTLED row with
        # contracts=0, stake=0 for every cancelled order.
        unsettled = []
        for row in rows:
            reason = row.get("reason", "")
            if reason.startswith("SETTLED:"):
                continue
            if reason.startswith("CANCELLED") or reason.startswith("REJECTED"):
                continue
            try:
                if int(row.get("contracts") or 0) <= 0:
                    continue
            except (ValueError, TypeError):
                continue
            if row["order_id"] in settled_order_ids:
                continue
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

        # Exclude zero-contract rows: these are cancelled/rejected orders that
        # never filled (plus any phantom SETTLED rows produced before the
        # get_unsettled_trades fix). They shouldn't count as trades.
        def _has_contracts(r):
            try:
                return int(r.get("contracts") or 0) > 0
            except (ValueError, TypeError):
                return False

        rows = [r for r in rows if _has_contracts(r)]
        settlements = [r for r in rows if r.get("reason", "").startswith("SETTLED:")]
        entries = [
            r for r in rows
            if not r.get("reason", "").startswith("SETTLED:")
            and not r.get("reason", "").startswith("CANCELLED")
            and not r.get("reason", "").startswith("REJECTED")
        ]
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
        seen_settled_oids: set[str] = set()
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
            if s is not None:
                seen_settled_oids.add(e["order_id"])

        # Orphan settlements: the maker-timeout path rewrites the entry row
        # with reason="CANCELLED: unfilled after Ns" when the bot gives up,
        # but Kalshi may still fill the order afterward. That leaves a
        # SETTLED row whose only companion is a CANCELLED row — filtered out
        # of `entries` above, so the trade never lands on the dashboard.
        # Recover these by using the CANCELLED row (or the settlement row
        # itself) as the entry source.
        all_settled_oids = {s["order_id"] for s in settlements}
        orphan_oids = all_settled_oids - seen_settled_oids
        if orphan_oids:
            cancelled_by_oid = {
                r["order_id"]: r for r in rows
                if r.get("reason", "").startswith("CANCELLED")
            }
            for s in settlements:
                oid = s["order_id"]
                if oid not in orphan_oids:
                    continue
                src = cancelled_by_oid.get(oid, s)
                fee = _fee(s) if s.get("fees_usd") else _fee(src)
                profit = round(float(s["profit_usd"]), 2)
                try:
                    price = int(src.get("price_cents") or s["price_cents"])
                    contracts = int(src.get("contracts") or s["contracts"])
                    stake = round(float(src.get("stake_usd") or s["stake_usd"]), 2)
                except (ValueError, TypeError):
                    continue
                recent.append({
                    "time": src.get("time") or s["time"],
                    "ticker": src.get("ticker") or s["ticker"],
                    "strategy": src.get("strategy") or s.get("strategy", ""),
                    "side": src.get("side") or s["side"],
                    "price": price,
                    "contracts": contracts,
                    "stake": stake,
                    "outcome": s["outcome"],
                    "profit": profit,
                    "fees": round(fee, 2),
                    "profit_after_fees": round(profit - fee, 2),
                    "order_type": src.get("order_type") or s.get("order_type") or "taker",
                    "order_id": oid,
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
            # Sort by time so the orphan-recovery path (which appends at
            # the end of the list regardless of when the settlement
            # happened) doesn't corrupt chronological order.
            "trades": sorted(recent, key=lambda r: r.get("time", ""), reverse=True),
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
    """Shared mutable state between the bot loop and SSE server.

    The only runtime control surface is `trading_enabled` — a global
    pause. Per-asset toggles were removed because RR is already scoped
    to safe cells by the optimizer, and partial toggles caused the bot
    to keep collecting data while silently ignoring half the markets.
    """
    tick_data: str = ""  # JSON string of the latest TickData
    trading_enabled: bool = True  # Global trading on/off switch
    lock = threading.Lock()

    def __init__(self):
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

        if self.path == "/api/toggle-trading":
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
        # ── Daily markets (share price feeds with 15M counterparts) ──
        # RR uses a single flat stake (STAKE_USD) for both 15M and daily
        # cells; per-asset sizing is expressed through cell params, not
        # through a separate daily-stake override.
        self.assets.update({
            "btc_daily": {
                "series": "KXBTCD",
                "symbol": "BTC-USD",
                "scanner": MarketScanner(self.client, series="KXBTCD"),
                "price_feed": self.assets["btc"]["price_feed"],
                "is_daily": True,
            },
            "eth_daily": {
                "series": "KXETHD",
                "symbol": "ETH-USD",
                "scanner": MarketScanner(self.client, series="KXETHD"),
                "price_feed": self.assets["eth"]["price_feed"],
                "is_daily": True,
            },
            "sol_daily": {
                "series": "KXSOLD",
                "symbol": "SOL-USD",
                "scanner": MarketScanner(self.client, series="KXSOLD"),
                "price_feed": self.assets["sol"]["price_feed"],
                "is_daily": True,
            },
            "doge_daily": {
                "series": "KXDOGED",
                "symbol": "DOGE-USD",
                "scanner": MarketScanner(self.client, series="KXDOGED"),
                "price_feed": self.assets["doge"]["price_feed"],
                "is_daily": True,
            },
            "xrp_daily": {
                "series": "KXXRPD",
                "symbol": "XRP-USD",
                "scanner": MarketScanner(self.client, series="KXXRPD"),
                "price_feed": self.assets["xrp"]["price_feed"],
                "is_daily": True,
            },
            "bnb_daily": {
                "series": "KXBNBD",
                "symbol": "BNB-USD",
                "scanner": MarketScanner(self.client, series="KXBNBD"),
                "price_feed": self.assets["bnb"]["price_feed"],
                "is_daily": True,
            },
            "hype_daily": {
                "series": "KXHYPED",
                "symbol": "HYPE-USD",
                "scanner": MarketScanner(self.client, series="KXHYPED"),
                "price_feed": self.assets["hype"]["price_feed"],
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

        # RR is hold-to-settlement: no early exits, no mid-trade Bayesian
        # collapse, no vol-regime gating of strategies that no longer exist.
        # Live results confirmed exits whipsawed normal adverse moves into
        # unnecessary losses (~$60 on 2026-04-13).

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

        # Strike cache populated from scanner/Kalshi API as markets are
        # discovered. Lookups return the last-known strike for a ticker,
        # or None if we haven't seen its metadata yet. The tick recorder
        # uses this to attach floor_strike to each tick row so the
        # optimizer can compute buffer_pct for 15M cells, whose ticker
        # format (KXBTC15M-<yymmdd><hhmm>-<mm>) doesn't encode it.
        self._ticker_strikes: dict = {}

        # Enable recording of live contract prices for future calibration
        data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
        self.ws_feed.enable_recording(data_dir, strike_lookup=self._ticker_strikes.get)

        # Inject WebSocket feed into all scanners for real-time prices
        for asset in self.assets.values():
            asset["scanner"].ws_feed = self.ws_feed

        # Logger
        csv_name = "paper_trades.csv" if config["paper_trade"] else "live_trades.csv"
        self.logger = TradeLogger(f"data/{csv_name}", run_id=self.run_id)

        # Performance tracker — computes Sharpe, drawdown, profit factor
        paper_balance = float(os.getenv("PAPER_BALANCE", "100.00")) if config["paper_trade"] else 0.0
        self.perf_tracker = PerformanceTracker(initial_balance=paper_balance)

        # Adaptive strategy matrix — auto-disables (asset, RR) cells that
        # underperform, shadow-tracks disabled cells, re-enables on recovery.
        # The whitelists guarantee the matrix can never accumulate stale
        # cells from deleted strategies, broad-scan tickers, or "unknown"
        # asset keys — anything outside the lists is silently dropped.
        active_assets = list(self.assets.keys())
        active_strategies = list(self.strategies.keys())
        self.strategy_matrix = StrategyMatrix(
            window_size=20,
            disable_threshold=-0.05,
            first_enable_threshold=0.05,
            enable_threshold=0.05,
            extended_disable_threshold=0.10,
            first_enable_min_trades=10,
            min_trades_to_judge=3,
            cooldown_seconds=900,
            persist_path="data/strategy_matrix_state.json",
            allowed_assets=active_assets,
            allowed_strategies=active_strategies,
        )
        # Pre-populate all cells so dashboard shows the full matrix at startup
        self.strategy_matrix.initialize_cells(active_assets, active_strategies)

        # Load optimized per-cell RR params from Monte Carlo optimizer.
        # Each cell (e.g. "xrp_15m") has its own max_seconds, buffer, momentum
        # gate, etc. Only cells that are cross-validation-profitable are kept.
        self._rr_cell_params = {}
        # Full cell set (enabled + disabled) with the gate verdict annotated
        # on each — published to the dashboard so users can see *why* a
        # cell is off. self._rr_cell_params stays safe-only for the
        # scanner's fast path.
        self._rr_cell_params_all = {}
        rr_params_path = Path("data/rr_params.json")
        if rr_params_path.exists():
            with open(rr_params_path) as f:
                all_rr_params = json.load(f)
            # Cell enable/disable gate. Set RR_ENABLE_ALL=1 to bypass
            # and let every known cell trade (validation mode).
            enable_all = os.environ.get("RR_ENABLE_ALL", "0") == "1"

            # Load live P&L over the last 7 days. This is the sole
            # signal used by evaluate_cell_safety — a cell with
            # non-negative 7d P&L (or too few trades to veto) is
            # enabled; a cell losing money over 7 days is disabled.
            pnl_by_cell = load_recent_cell_pnl(days=7)
            if pnl_by_cell:
                self._log(
                    f"[INIT] 7d live P&L loaded: "
                    + ", ".join(
                        f"{c}=${s['profit_usd']:+.2f}({s['n_trades']}t)"
                        for c, s in sorted(pnl_by_cell.items())
                    )
                )

            safe_cells = {}
            annotated = {}
            for k, v in all_rr_params.items():
                ok, reason = evaluate_cell_safety(
                    k, v, pnl_by_cell=pnl_by_cell, enable_all=enable_all)
                # Pass per-cell optimizer params through directly. The
                # strategy-level clamps (max_ep ≤ 97, min_buf ≥ 0.15)
                # still apply at evaluate() time — those are the only
                # hard safety invariants.
                merged = dict(v)
                annotated[k] = {**merged, "enabled": ok,
                                "disabled_reason": reason or None}
                if ok:
                    safe_cells[k] = merged
            self._rr_cell_params = safe_cells
            self._rr_cell_params_all = annotated
            self._log(f"[INIT] RR cells enabled: {len(safe_cells)} "
                      f"({', '.join(sorted(safe_cells.keys()))})")
            unsafe = set(all_rr_params.keys()) - set(safe_cells.keys())
            if unsafe:
                reasons = [
                    f"{c}: {annotated[c]['disabled_reason']}"
                    for c in sorted(unsafe)
                ]
                self._log(f"[INIT] Disabled RR cells:\n    "
                          + "\n    ".join(reasons))

        # Enable safe RR cells, disable unsafe ones
        for asset in active_assets:
            if asset.endswith("_daily"):
                cell = asset.replace("_daily", "_hourly")
            else:
                cell = f"{asset}_15m"

            if cell in self._rr_cell_params:
                self.strategy_matrix.force_enable(asset, "resolution_rider", clear_history=True)
            else:
                self.strategy_matrix.force_disable(asset, "resolution_rider", hard=True)

        # Track which markets we've already traded on
        self._traded_tickers: set = set()
        self._last_settled_ticker: str = ""

        # Rolling buffer of recent [SKIP] events (book moved between RR
        # signal and order submission). Surfaced on the dashboard so dry
        # spells are diagnosable without tailing logs.
        self._recent_skips: list[dict] = []

        # Per-hit outcome telemetry — one row per [FAST-RR] Hit in
        # data/hit_outcomes.csv. Lets us diagnose conversion rate
        # (Hit→submit) and see which gates are dropping trades.
        # Outcomes: submitted, skip_book_moved, skip_outside_band,
        # skip_risk_rejected, skip_balance_low, skip_negative_ev.
        self._hit_outcomes_csv = Path("data/hit_outcomes.csv")
        self._hit_outcomes_csv.parent.mkdir(parents=True, exist_ok=True)
        if not self._hit_outcomes_csv.exists():
            with open(self._hit_outcomes_csv, "w", newline="") as f:
                csv.writer(f).writerow([
                    "time", "ticker", "strategy", "side", "max_price_c",
                    "yes_bid_c", "yes_ask_c", "outcome", "reason",
                    "ask_price_c", "exec_price_c", "stake_usd",
                    "secs_left", "cell",
                ])
        self._hit_ctx: Optional[dict] = None
        # Lock for the hit_outcomes.csv write path. Separate from the
        # TradeLogger lock because they write different files; making
        # them independent reduces contention when a parallel submission
        # fan-out writes both in flight.
        self._hit_outcomes_lock = threading.Lock()
        # Rolling 24h outcome counts for dashboard summary.
        self._hit_outcome_counts: dict = {}
        self._hit_outcome_window_start: float = time.time()

        # Per-ticker gate state for the dashboard matrix. Keyed by ticker
        # → {cell, blocked_at, detail, last_seen}. blocked_at="passed"
        # means the ticker cleared every gate and hit _maybe_trade. This
        # lets the dashboard render a live "why is this ticker not firing"
        # table without the user tailing logs.
        self._ticker_gate_state: dict = {}

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

    # Balance cache: _get_balance is called from _maybe_trade (per trade)
    # AND _publish_tick (4× per second for SSE). Without caching that was
    # ~4 REST calls/sec doing nothing useful. Cache for 10s — balance only
    # changes when we trade or a position settles, and both paths are
    # already handled by invalidate_balance_cache() below, so 10s of
    # staleness on rare unsynced balance moves is acceptable.
    _BALANCE_CACHE_TTL_S = 10.0

    def invalidate_balance_cache(self) -> None:
        """Call after submitting an order or recording a settlement so the
        next _get_balance() fetches fresh rather than serving a stale value.
        Safe to call from any thread — pure attribute write."""
        self._balance_cache_ts = 0.0

    def _get_balance(self) -> float:
        """Fetch account balance with a short TTL cache. Returns USD
        available. In paper mode, returns a simulated balance (no caching
        since it's already a cheap local computation)."""
        if self.config["paper_trade"]:
            paper_balance = float(os.getenv("PAPER_BALANCE", "100.00"))
            open_stake = sum(t.stake_usd for t in self.risk_mgr.trades if t.outcome == "")
            return max(0, paper_balance + self.risk_mgr.total_pnl - open_stake)

        now = time.time()
        cache_ts = getattr(self, '_balance_cache_ts', 0.0)
        if now - cache_ts < self._BALANCE_CACHE_TTL_S:
            return getattr(self, '_balance_cache_value', 0.0)

        try:
            resp = self.client.get_balance()
            value = resp.get("balance", 0) / 100.0  # Kalshi returns cents
            self._balance_cache_value = value
            self._balance_cache_ts = now
            return value
        except Exception as e:
            self._log(f"  [WARN] Failed to fetch balance: {e}", level="warning")
            # Return cached value if we have one — better than 0 which
            # would falsely trip the balance gate.
            return getattr(self, '_balance_cache_value', 0.0)

    def _init_strategies(self, strategy_name: str) -> dict:
        """Initialize the trading strategies. Only Resolution Rider remains;
        momentum/mean_reversion/consensus/favorite_bias were removed after
        backtests showed zero edge and live results confirmed it.
        """
        return {"resolution_rider": ResolutionRiderStrategy()}

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
        self._log("  Exits:      hold to settlement (RR thesis)")
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

        # Start orderbook depth snapshot thread — collects depth data for
        # future optimizer features (book imbalance, depth-at-level, etc).
        self._start_orderbook_thread()

        # Start missed-trades watchdog — periodically flags settled
        # markets that reached 94c+ on the winning side without us
        # trading them.
        self._start_missed_trades_thread()

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
        """Refresh the cached active market per asset (called from slow path).

        Interval: 2s on 2026-04-21 (was 5s). The underlying scanner
        REST cache is also 3s for "open" status, so 2s polling means
        we hit the fresh edge of that cache. Combined with paginated
        `_fresh_markets` (no more 200-market truncation) this closes
        the "missed trades due to discovery latency" gap the 4/21
        analysis identified.
        """
        now = time.time()
        if now - getattr(self, '_last_rr_cache_refresh', 0) < 2:
            return
        self._last_rr_cache_refresh = now
        for key, asset in self.assets.items():
            try:
                scanner = asset["scanner"]
                market = scanner.get_next_expiring_market()
                if market:
                    yes_bid, yes_ask = scanner.parse_yes_price(market)
                    ticker = market.get("ticker", "")
                    strike = best_strike_for_market(market)
                    if ticker and strike is not None:
                        self._ticker_strikes[ticker] = strike
                    asset["_rr_market"] = {
                        "ticker": ticker,
                        "close_time": market.get("close_time", ""),
                        "floor_strike": strike if strike is not None else market.get("floor_strike"),
                        "yes_bid": yes_bid,
                        "yes_ask": yes_ask,
                    }
                    # Also cache near-certain markets for hourly/daily.
                    # Scanner fetches via REST and runs its own WS-first /
                    # REST-fallback parse. We store the bid/ask alongside
                    # ticker metadata so the fast path has a fallback when
                    # the live WS hasn't received a tick for a quiet daily
                    # strike (which turned out to be ~70% of btc_hourly
                    # cached strikes — the "no_ws_tick" debug counter).
                    if asset.get("is_daily"):
                        # 1.0 → 3.0 on 2026-04-21. Analysis of missed
                        # winners on 4/21 showed many daily strikes
                        # closing 1–3 hours out were skipped by the
                        # 1-hour filter, even though their favorite
                        # side was already at 94c+ with massive buffers.
                        # 3 hours captures those while still excluding
                        # far-out strikes where price can still move
                        # meaningfully before close.
                        rr_markets = scanner.get_near_certain_markets(max_hours=3.0)
                        cached = []
                        for m in rr_markets:
                            yb, ya = scanner.parse_yes_price(m)
                            m_ticker = m.get("ticker", "")
                            m_strike = best_strike_for_market(m)
                            if m_ticker and m_strike is not None:
                                self._ticker_strikes[m_ticker] = m_strike
                            cached.append({
                                "ticker": m_ticker,
                                "close_time": m.get("close_time", ""),
                                "floor_strike": m_strike if m_strike is not None else m.get("floor_strike"),
                                "yes_bid": yb,
                                "yes_ask": ya,
                            })
                        asset["_rr_daily_markets"] = cached
            except Exception as e:
                if not hasattr(self, '_rr_cache_err_ts') or time.time() - self._rr_cache_err_ts > 60:
                    self._log(f"  [RR-CACHE] {key}: refresh error: {e}", level="warning")
                    self._rr_cache_err_ts = time.time()

        # Market-coverage heartbeat: every 60s, log how many markets
        # each daily asset is watching. Lets us spot coverage gaps
        # (e.g., pagination truncated, max_hours too tight, scanner
        # error hiding markets) without tailing debug counters. The
        # count should be roughly stable; a sudden drop is a signal.
        if (not hasattr(self, '_rr_coverage_log_ts')
                or time.time() - self._rr_coverage_log_ts > 60):
            self._rr_coverage_log_ts = time.time()
            parts = []
            for key, asset in self.assets.items():
                if not asset.get("is_daily"):
                    continue
                ml = asset.get("_rr_daily_markets") or []
                parts.append(f"{key}={len(ml)}")
            if parts:
                self._log(f"  [RR-COVERAGE] daily strikes watched: {', '.join(parts)}")

    def _fast_rr_scan(self):
        """
        Lightning-fast resolution rider check. ZERO REST calls.
        Uses only WebSocket prices + cached market metadata.
        Runs at the top of every tick before any I/O.
        """
        rr = self.strategies.get("resolution_rider")
        if not rr:
            return

        # Debug counters (aggregate per-cell, flushed every 60s).
        # Added 2026-04-14 to diagnose the "no trades" issue — if this
        # hasn't been needed in a while you can delete it.
        if not hasattr(self, "_frr_debug"):
            from collections import Counter
            self._frr_debug = {
                "counts": Counter(),
                "last_flush": time.time(),
            }
        dbg = self._frr_debug
        if time.time() - dbg["last_flush"] >= 60:
            dbg["last_flush"] = time.time()
            if dbg["counts"]:
                summary = ", ".join(
                    f"{k}={v}" for k, v in sorted(dbg["counts"].items())
                )
                self._log(f"  [FRR-DBG] last 60s: {summary}")
            dbg["counts"].clear()

        def bump(cell, reason, ticker: str = "", detail: Optional[dict] = None):
            dbg["counts"][f"{cell}.{reason}"] += 1
            # Per-ticker state for the dashboard gate-matrix visualization.
            # Only update when we know which ticker was being evaluated —
            # cell-level failures (like no_markets) don't tie to a ticker.
            if ticker:
                self._ticker_gate_state[ticker] = {
                    "ticker": ticker,
                    "cell": cell,
                    "blocked_at": reason,
                    "detail": detail or {},
                    "last_seen": time.time(),
                }

        # Candidates that pass every gate in this sweep. Dispatched after
        # the gate loop so bursts of simultaneous 94c+ setups are handled
        # in parallel (bounded thread pool) rather than serialized through
        # network latency.
        pending_trades: list = []

        for key, asset in self.assets.items():
            cache = asset.get("_rr_market")
            if not cache:
                bump(key, "no_cache")
                continue

            # Get per-cell optimized params (or skip if cell is disabled)
            is_daily = asset.get("is_daily", False)
            cell_name = key.replace("_daily", "_hourly") if is_daily else f"{key}_15m"
            cell_params = self._rr_cell_params.get(cell_name)
            if not cell_params:
                bump(cell_name, "no_cell")
                continue  # Cell not in safe list

            # Override RR params for this cell
            cell_max_secs = cell_params.get("max_seconds", rr.max_seconds)
            cell_buffer = cell_params.get("min_price_buffer_pct", rr.min_price_buffer_pct)

            # Build list of markets to check (main + hourly/daily extras)
            # For hourly/daily: prefer the filtered list, but always
            # include the primary cache as fallback — _rr_daily_markets
            # can be empty if no strikes pass the price filter yet.
            if is_daily:
                markets_to_check = list(asset.get("_rr_daily_markets") or [])
                if cache and cache.get("ticker") and cache not in markets_to_check:
                    markets_to_check.append(cache)
            else:
                markets_to_check = [cache]

            if not markets_to_check:
                bump(cell_name, "no_markets")
                continue

            for market_cache in markets_to_check:
                ticker = market_cache.get("ticker", "")
                if not ticker:
                    bump(cell_name, "no_ticker")
                    continue
                if ticker in self._traded_tickers or ticker in self.risk_mgr.open_positions:
                    bump(cell_name, "already_traded", ticker=ticker)
                    continue

                # Time check from cached close_time (no I/O)
                close_str = market_cache.get("close_time", "").replace("Z", "+00:00")
                try:
                    close_dt = datetime.fromisoformat(close_str)
                    secs_left = max(0, (close_dt - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError):
                    bump(cell_name, "bad_close_time", ticker=ticker)
                    continue
                if secs_left < rr.min_seconds:
                    bump(cell_name, "secs_too_low", ticker=ticker,
                         detail={"secs_left": round(secs_left, 1),
                                 "min_required": rr.min_seconds})
                    continue
                if secs_left > cell_max_secs:
                    bump(cell_name, "secs_too_high", ticker=ticker,
                         detail={"secs_left": round(secs_left, 1),
                                 "max_allowed": cell_max_secs})
                    continue

                # Price check — prefer live WS, fall back to cached REST
                # bid/ask from _refresh_rr_cache. Fallback is important for
                # daily strikes that aren't actively quoted on WS but do
                # have REST-cached prices (scanner's own lookup).
                #
                # CRITICAL: ws_feed.get_tick() returns the last-stored tick
                # with no freshness check. A quiet market (no WS update
                # for minutes) will still return its stale tick, which
                # would then make gate decisions against a price the book
                # has long since left. We require ts-freshness matching
                # the REST cache cadence; stale ticks fall through to REST.
                WS_TICK_MAX_AGE_S = 5.0
                tick = self.ws_feed.get_tick(ticker)
                tick_age = time.time() - tick.ts if tick else None
                if (tick and tick.yes_bid and tick.yes_ask
                        and tick_age is not None and tick_age <= WS_TICK_MAX_AGE_S):
                    bid, ask = tick.yes_bid, tick.yes_ask
                else:
                    bid = market_cache.get("yes_bid")
                    ask = market_cache.get("yes_ask")
                    if not bid or not ask:
                        bump(cell_name, "no_price", ticker=ticker,
                             detail={"secs_left": round(secs_left, 1),
                                     "ws_tick_age_s": (round(tick_age, 1)
                                                       if tick_age is not None
                                                       else None)})
                        continue

                # Record which source provided bid/ask (WS or REST cache)
                # so stale-data-driven skips are visible on the dashboard.
                price_source = ("ws" if tick_age is not None
                                and tick_age <= WS_TICK_MAX_AGE_S else "rest")

                cell_min_cp = cell_params.get("min_contract_price", rr.min_contract_price)
                cell_max_ep = cell_params.get("max_entry_price", rr.max_entry_price)
                yes_mid = (bid + ask) / 2
                fav = max(yes_mid, 100 - yes_mid)
                if fav < cell_min_cp:
                    bump(cell_name, "fav_too_low", ticker=ticker,
                         detail={"secs_left": round(secs_left, 1),
                                 "yes_bid": bid, "yes_ask": ask,
                                 "fav": round(fav, 1), "min_required": cell_min_cp,
                                 "src": price_source,
                                 "ws_tick_age_s": (round(tick_age, 1)
                                                   if tick_age is not None
                                                   else None)})
                    continue
                if fav > cell_max_ep:
                    bump(cell_name, "fav_too_high", ticker=ticker,
                         detail={"secs_left": round(secs_left, 1),
                                 "yes_bid": bid, "yes_ask": ask,
                                 "fav": round(fav, 1), "max_allowed": cell_max_ep,
                                 "src": price_source,
                                 "ws_tick_age_s": (round(tick_age, 1)
                                                   if tick_age is not None
                                                   else None)})
                    continue

                # Price buffer check (WS-cached crypto price, zero I/O)
                feed = asset["price_feed"]
                if not feed.current_price:
                    bump(cell_name, "no_spot", ticker=ticker,
                         detail={"secs_left": round(secs_left, 1)})
                    continue
                if not market_cache.get("floor_strike"):
                    bump(cell_name, "no_strike", ticker=ticker)
                    continue
                try:
                    strike = float(market_cache["floor_strike"])
                except (ValueError, TypeError):
                    bump(cell_name, "bad_strike", ticker=ticker)
                    continue
                if strike <= 0:
                    bump(cell_name, "bad_strike", ticker=ticker)
                    continue
                buffer_pct = (feed.current_price - strike) / strike * 100

                # Time-scaled buffer requirement (matches optimizer +
                # resolution_rider.required_buffer). cell_buffer is the
                # base threshold at 60s remaining; scales with sqrt(t/60).
                cell_buf_required = cell_buffer * math.sqrt(max(1.0, secs_left) / 60.0)

                # Determine which side we'd trade, then check buffer for that side
                no_mid = 100 - yes_mid
                if yes_mid >= no_mid:
                    # YES side favored — price must be ABOVE strike by buffer
                    if buffer_pct < cell_buf_required:
                        bump(cell_name, "yes_buf_low", ticker=ticker,
                             detail={"secs_left": round(secs_left, 1),
                                     "buffer_pct": round(buffer_pct, 3),
                                     "required_pct": round(cell_buf_required, 3)})
                        continue
                else:
                    # NO side favored — price must be BELOW strike by buffer
                    if buffer_pct > -cell_buf_required:
                        bump(cell_name, "no_buf_high", ticker=ticker,
                             detail={"secs_left": round(secs_left, 1),
                                     "buffer_pct": round(buffer_pct, 3),
                                     "required_pct": round(cell_buf_required, 3)})
                        continue

                # Passed all fast checks — do full evaluation and trade
                market_dict = {
                    "ticker": ticker,
                    "close_time": market_cache["close_time"],
                    "floor_strike": market_cache["floor_strike"],
                    "yes_bid": bid,
                    "yes_ask": ask,
                }

                # Use a lightweight shim scanner — close over the resolved
                # bid/ask (not the raw WS tick) so evaluate() sees what we
                # just gate-checked, including the REST fallback case.
                class _FastShim:
                    def __init__(shim, ws, client, mkt, _bid, _ask, _secs):
                        shim._ws = ws
                        shim.client = client
                        shim._mkt = mkt
                        shim._bid = _bid
                        shim._ask = _ask
                        shim._secs = _secs
                    def seconds_until_close(shim, market):
                        return shim._secs
                    def parse_yes_price(shim, market):
                        return (shim._bid, shim._ask)

                shim = _FastShim(self.ws_feed, self.client, market_dict, bid, ask, secs_left)
                rec = rr.evaluate(market_dict, None, feed, shim,
                                  cell_params=cell_params)
                if not rec.should_trade:
                    bump(cell_name, "eval_no_trade", ticker=ticker,
                         detail={"secs_left": round(secs_left, 1),
                                 "yes_bid": bid, "yes_ask": ask,
                                 "buffer_pct": round(buffer_pct, 3),
                                 "rec_reason": rec.reason[:80]})
                    continue

                # Check matrix
                if not self.strategy_matrix.is_enabled(key, "resolution_rider"):
                    bump(cell_name, "matrix_disabled", ticker=ticker)
                    continue

                # Ticker passed every gate — record for the dashboard so
                # the matrix shows a green row (vs red for any blocked gate).
                self._ticker_gate_state[ticker] = {
                    "ticker": ticker,
                    "cell": cell_name,
                    "blocked_at": "passed",
                    "detail": {"secs_left": round(secs_left, 1),
                               "yes_bid": bid, "yes_ask": ask,
                               "buffer_pct": round(buffer_pct, 3)},
                    "last_seen": time.time(),
                }

                self._log(f"  [FAST-RR] Hit: {ticker} {rec.reason}")
                # Build a per-candidate hit_ctx. Must be a local dict —
                # parallel dispatch below would race on a shared
                # instance attribute.
                hit_ctx = {
                    "time": datetime.now(timezone.utc).isoformat(),
                    "ticker": ticker,
                    "strategy": "resolution_rider",
                    "side": rec.signal.value,
                    "max_price_c": rec.max_price_cents,
                    "secs_left": round(secs_left, 1),
                    "cell": cell_name,
                }
                strats = {"resolution_rider": rec}
                # Dedupe against the slow path: on daily markets the slow
                # path checks an event_key = ticker without the strike
                # suffix (e.g. KXSOLD-26APR1412 from KXSOLD-26APR1412-
                # T85.9999), so adding just `ticker` leaves the slow
                # path unaware and it fires a second order on the same
                # market. Add both. Done BEFORE dispatch so parallel
                # workers can't double-trade a ticker already claimed.
                self._traded_tickers.add(ticker)
                if is_daily:
                    parts = ticker.rsplit("-", 1)
                    if len(parts) > 1:
                        self._traded_tickers.add(parts[0])
                pending_trades.append((market_dict, strats, key, cell_params, hit_ctx))

        # Dispatch pending submissions. Single candidate stays on the
        # calling thread — no pool overhead. Multiple candidates fan
        # out to a bounded pool so network latency (place_order +
        # fill-wait) doesn't serialize across opportunities. Cap at 3
        # workers: 3 concurrent order flows is plenty for Kalshi's
        # write budget, and higher would start starving writes across
        # parallel fill-wait polls.
        if len(pending_trades) == 1:
            m, s, k, cp, hc = pending_trades[0]
            self._maybe_trade(m, s, asset_key=k, cell_params=cp, hit_ctx=hc)
        elif len(pending_trades) > 1:
            self._log(
                f"  [FAST-RR] Dispatching {len(pending_trades)} "
                f"parallel submissions")
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(
                    max_workers=min(3, len(pending_trades)),
                    thread_name_prefix="submit") as ex:
                futures = [
                    ex.submit(self._maybe_trade, m, s,
                              asset_key=k, cell_params=cp, hit_ctx=hc)
                    for m, s, k, cp, hc in pending_trades
                ]
                for fut in futures:
                    try:
                        fut.result()
                    except Exception as e:
                        self._log(
                            f"  [FAST-RR] Parallel submission raised: {e}",
                            level="error")

    def _start_fast_rr_thread(self):
        """Start the fast RR scanner in a dedicated high-frequency thread.

        The scan makes zero REST calls — pure local dict + WS cache reads
        — so the rate limit here is practical CPU overhead, not API quota.
        Default 50Hz (20ms between scans). Env override via RR_SCAN_HZ.

        Above ~100Hz returns diminish: WS updates themselves arrive at
        ~100ms intervals on most markets, so extra scan cycles just
        re-evaluate the same data. Below 10Hz risks missing short-lived
        94c+ windows that open/close in 200ms.
        """
        hz = float(os.environ.get("RR_SCAN_HZ", "50"))
        # Clamp so a typo (e.g. RR_SCAN_HZ=500000) doesn't spin the CPU
        # to 100% doing nothing useful.
        hz = max(1.0, min(200.0, hz))
        sleep_s = 1.0 / hz

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
                time.sleep(sleep_s)

        t = threading.Thread(target=_rr_loop, daemon=True, name="fast-rr")
        t.start()
        self._log(f"[INIT] Fast RR thread started ({hz:.0f}Hz, zero I/O)")

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

    def _start_orderbook_thread(self):
        """Periodically snapshot orderbook depth for each active market.

        Writes to {DATA_DIR}/orderbooks/YYYY-MM-DD.csv with schema:
          timestamp, ticker, yes_bids, yes_asks, no_bids, no_asks
        Each depth column is a compact JSON array of [price_cents, qty]
        pairs, top-to-bottom of book. This gives the optimizer a full
        picture of book imbalance at entry time without storing it per
        tick (which would be overkill).

        The thread staggers snapshots so we don't hammer the API with 14
        simultaneous get_orderbook calls. Target cadence: each market is
        snapshotted every ~60s, which at ~14 markets means one call every
        ~4s on average — well within Kalshi rate limits.
        """
        from pathlib import Path
        import data_paths

        ob_dir = data_paths.ensure("orderbooks")

        def _snapshot_market(ticker: str):
            try:
                book = self.client.get_orderbook(ticker, depth=10)
            except Exception:
                return None

            yes_bids: list = []
            yes_asks: list = []
            no_bids: list = []
            no_asks: list = []
            fp = book.get("orderbook_fp") or {}
            for lvl in fp.get("yes_dollars", []) or []:
                try:
                    yes_bids.append([int(round(float(lvl[0]) * 100)), float(lvl[1])])
                except (ValueError, TypeError, IndexError):
                    pass
            for lvl in fp.get("no_dollars", []) or []:
                try:
                    no_bids.append([int(round(float(lvl[0]) * 100)), float(lvl[1])])
                except (ValueError, TypeError, IndexError):
                    pass
            # YES asks are derived from NO bids: someone willing to SELL YES
            # at price p is equivalent to someone bidding to BUY NO at (100-p).
            for p_cents, qty in no_bids:
                yes_asks.append([100 - p_cents, qty])
            for p_cents, qty in yes_bids:
                no_asks.append([100 - p_cents, qty])
            return {
                "yes_bids": yes_bids, "yes_asks": yes_asks,
                "no_bids": no_bids,   "no_asks": no_asks,
            }

        def _write_row(ticker: str, book: dict):
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            path = ob_dir / f"{date_str}.csv"
            new_file = not path.exists()
            with open(path, "a", newline="") as f:
                writer = csv.writer(f)
                if new_file:
                    writer.writerow(["timestamp", "ticker",
                                     "yes_bids", "yes_asks",
                                     "no_bids", "no_asks"])
                writer.writerow([
                    datetime.now(timezone.utc).isoformat(),
                    ticker,
                    json.dumps(book["yes_bids"]),
                    json.dumps(book["yes_asks"]),
                    json.dumps(book["no_bids"]),
                    json.dumps(book["no_asks"]),
                ])

        def _orderbook_loop():
            time.sleep(15)  # Let startup settle
            per_market_interval = 60.0  # Target cadence per ticker
            while self.running:
                # Collect the current set of tickers that matter —
                # whichever markets the bot's scanners are actively
                # tracking. Prefer _rr_market (single next-expiring)
                # and _rr_daily_markets (near-certain strikes).
                tickers: list = []
                for asset in self.assets.values():
                    rrm = asset.get("_rr_market")
                    if rrm and rrm.get("ticker"):
                        tickers.append(rrm["ticker"])
                    for m in asset.get("_rr_daily_markets") or []:
                        if m.get("ticker"):
                            tickers.append(m["ticker"])
                tickers = list(dict.fromkeys(tickers))  # de-dupe, keep order

                if not tickers:
                    time.sleep(5)
                    continue

                # Stagger per-market calls to spread load over the interval
                delay = max(0.5, per_market_interval / max(1, len(tickers)))
                for ticker in tickers:
                    if not self.running:
                        break
                    book = _snapshot_market(ticker)
                    if book is not None:
                        try:
                            _write_row(ticker, book)
                        except Exception as e:
                            if not hasattr(self, "_ob_errs"):
                                self._ob_errs = 0
                            self._ob_errs += 1
                            if self._ob_errs <= 3:
                                self._log(f"[OB] Write error: {e}", level="warning")
                    time.sleep(delay)

        t = threading.Thread(target=_orderbook_loop, daemon=True, name="orderbooks")
        t.start()
        self._log("[INIT] Orderbook snapshot thread started (~60s/market)")

    def _start_missed_trades_thread(self):
        """Start the missed-trades watchdog. Every 5 min it scans
        recently-settled markets and flags any ticker that reached
        94c+ on the eventually-winning side without an RR entry.

        This is pure diagnostics — the tracker writes to
        data/missed_trades.csv and publishes a recent-misses list
        for the dashboard. It never touches the trading path.
        """
        import data_paths
        from missed_trades import MissedTradeTracker

        ticks_dir = data_paths.resolve("ticks")
        output_csv = Path("data/missed_trades.csv")
        scanners = {a["scanner"].series: a["scanner"]
                    for a in self.assets.values()}
        self.missed_tracker = MissedTradeTracker(
            scanners=scanners,
            trades_csv=Path("data/live_trades.csv"),
            ticks_dir=ticks_dir,
            output_csv=output_csv,
        )
        self.missed_tracker.start(interval_seconds=300)
        self._log(
            f"[INIT] Missed-trades watchdog started "
            f"(ticks={ticks_dir}, out={output_csv})")

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

            # Resolution Rider is the only strategy. It runs entirely in the
            # fast-RR thread (bot.py:_fast_rr_scan) at 20Hz with per-cell
            # optimized params. The slow tick path only builds a strategies
            # dict for the dashboard — it does NOT trade RR, since doing so
            # with defaults would bypass cell_params gating (e.g. let a 96c
            # YES fire with 8 minutes left when cell says max_seconds=60).
            is_daily = asset.get("is_daily", False)
            strats = {}
            rr_strategy = self.strategies.get("resolution_rider")
            if rr_strategy is not None and market is not None:
                cell_name = key.replace("_daily", "_hourly") if is_daily else f"{key}_15m"
                cell_params = self._rr_cell_params.get(cell_name)
                # Display-only evaluation: shows the current RR assessment on
                # the dashboard but never triggers a trade from this path.
                rec = rr_strategy.evaluate(market, last_settled, feed, scanner,
                                            cell_params=cell_params)
                strats["resolution_rider"] = rec
            all_strategies[key] = strats

            # Execute trades only if global trading is on.
            with _sse_state.lock:
                trading_on = _sse_state.trading_enabled
            if market and trading_on:
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
                    self._maybe_trade(market, strats, asset_key=key,
                                      cell_params=cell_params)

        # 2a. Refresh RR market cache for the fast path (internal 5s guard)
        self._refresh_rr_cache()

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

        # 6. Save price snapshots for future backtesting.
        # High-frequency (5s) → {DATA_DIR}/prices_hf/YYYY-MM-DD.csv for
        # strike reconstruction on 15M markets. Coarse (1-min) →
        # {DATA_DIR}/prices/... for the existing optimizer paths and
        # backward compat. See data_paths.py for resolution rules.
        now = time.time()
        if now - getattr(self, '_last_price_save_hf', 0) >= 5:
            self._last_price_save_hf = now
            self._save_price_snapshot(sub_dir="prices_hf")
        if now - getattr(self, '_last_price_save', 0) >= 60:
            self._last_price_save = now
            self._save_price_snapshot(sub_dir="prices")

        # 7. Log strategy matrix summary (~every 5 min)
        if now - getattr(self, '_last_matrix_log', 0) >= 300:
            self._last_matrix_log = now
            summary = self.strategy_matrix.get_summary()
            if "edge=" in summary:
                self._log(f"\n{summary}")

    # Coins whose spot prices we persist to {DATA_DIR}/prices/YYYY-MM-DD.csv.
    # Must match the 15M series the bot trades so optimize_rr can compute
    # per-coin momentum features for every RR cell.
    PRICE_SNAPSHOT_COINS = ("btc", "eth", "sol", "doge", "xrp", "bnb", "hype")

    def _save_price_snapshot(self, sub_dir: str = "prices"):
        """Save a price snapshot row for future backtesting.

        Two cadences run in parallel (called from _tick on different
        throttles):
          - sub_dir="prices"    → 1-minute candles (legacy, preserved
                                  for the existing optimizer paths that
                                  expect one row per minute).
          - sub_dir="prices_hf" → 5-second candles, used by the optimizer
                                  to reconstruct 15M market strikes (60s
                                  BRTI average at market open) with far
                                  less approximation noise than the
                                  1-minute snapshot allows.

        Header mismatch handling: if a pre-existing file's header doesn't
        match `PRICE_SNAPSHOT_COINS`, the old file is rotated aside and a
        new one is started with the current schema. This prevents the
        mixed-schema corruption that happened on 2026-04-14 when the coin
        list was extended from 3 to 7 while a partial file already existed.
        """
        try:
            import data_paths
            price_dir = data_paths.ensure(sub_dir)
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            price_file = price_dir / f"{date_str}.csv"

            header = ["timestamp", *self.PRICE_SNAPSHOT_COINS]

            if price_file.exists():
                # Check the existing header. If it doesn't match, rotate.
                with open(price_file, "r", newline="") as f:
                    first = f.readline().rstrip("\r\n")
                existing_header = first.split(",") if first else []
                if existing_header != header:
                    ts = datetime.now(timezone.utc).strftime("%H%M%S")
                    rotated = price_file.with_suffix(f".csv.schema_mismatch_{ts}")
                    price_file.rename(rotated)
                    self._log(
                        f"  [PRICE-SNAPSHOT] Header mismatch in {price_file.name}, "
                        f"rotated to {rotated.name} and starting fresh",
                        level="warning",
                    )

            if not price_file.exists():
                with open(price_file, "w", newline="") as f:
                    csv.writer(f).writerow(header)

            row = [datetime.now(timezone.utc).isoformat()]
            for coin in self.PRICE_SNAPSHOT_COINS:
                feed = self.assets.get(coin, {}).get("price_feed")
                row.append(feed.current_price if feed and feed.current_price else "")
            with open(price_file, "a", newline="") as f:
                csv.writer(f).writerow(row)
        except Exception:
            pass  # Don't let price saving crash the bot

    def _log_submission_timing(self, ticker: str, t0: float, stages: dict) -> None:
        """Emit a single [TIMING] line showing ms deltas through the
        submission flow. Stages is an insertion-ordered dict of
        {step_name: monotonic_ts}. A zero-ms stage is usually just a
        branch that took the local path — still worth logging to show
        the shape of the flow."""
        if not stages:
            return
        parts = []
        prev = t0
        last_ts = t0
        for name, ts in stages.items():
            parts.append(f"{name}={int((ts - prev) * 1000)}ms")
            prev = ts
            last_ts = ts
        parts.append(f"total={int((last_ts - t0) * 1000)}ms")
        self._log(f"  [TIMING] {ticker}: " + " ".join(parts))

    def _maybe_trade(self, market: dict, strategies_status: dict, asset_key: str = "",
                     cell_params: dict = None, hit_ctx: Optional[dict] = None):
        """Check if any strategy wants to trade and execute if approved.

        `hit_ctx` is the per-candidate telemetry dict created at the
        [FAST-RR] Hit site. Always pass it explicitly when dispatching
        from a worker thread; the instance-attribute fallback is only
        safe under single-threaded (sequential) execution.
        """
        _trade_t0 = time.monotonic()
        _trade_stages: dict = {}
        if hit_ctx is None:
            hit_ctx = self._hit_ctx
        ticker = market.get("ticker", "")
        balance = self._get_balance()
        _trade_stages["balance"] = time.monotonic()

        # Get fresh book prices for order placement — fetch the LIVE
        # orderbook, not the cached /markets data, so our limit price
        # reflects the current best bid/ask.
        scanner = next(
            (a["scanner"] for a in self.assets.values()
             if a["scanner"].series in ticker),
            self.scanner,
        )
        # Maker mode (default) skips the fresh orderbook fetch — the
        # fast-RR scanner already gave us current WS-backed bid/ask
        # (with staleness check) on the `market` dict, and the ~100-500ms
        # REST call here was the #1 source of "book moved" races. For
        # maker, stale WS data just means our resting bid ends up at a
        # now-unfavorable price; the order expires unfilled at no cost.
        # For taker, we still need the fresh book because a stale ask
        # can mean paying a bad price on immediate execution.
        is_taker_mode = os.getenv("RR_TAKER", "0") == "1"
        if is_taker_mode:
            try:
                book = scanner.client.get_orderbook(ticker, depth=1)
                yes_bid, yes_ask = parse_book_top(book)
                if yes_bid is not None and yes_ask is not None:
                    self._log(f"      [BOOK] {ticker}: yes_bid={yes_bid} yes_ask={yes_ask}")
            except Exception:
                yes_bid, yes_ask = scanner.parse_yes_price(market)
        else:
            # Use the bid/ask the fast-RR scanner already validated.
            yes_bid = market.get("yes_bid")
            yes_ask = market.get("yes_ask")
            if yes_bid is None or yes_ask is None:
                # Fast-RR shouldn't have reached here without valid
                # prices, but guard against a bad market dict anyway.
                yes_bid, yes_ask = scanner.parse_yes_price(market)
        _trade_stages["book"] = time.monotonic()

        # Latch fresh book values onto the hit context so every downstream
        # outcome row captures the book the decision was made against.
        if hit_ctx is not None:
            hit_ctx["yes_bid_c"] = yes_bid
            hit_ctx["yes_ask_c"] = yes_ask

        # No sanity check — the fast-RR scanner already validated prices
        # via WS-cached bid/ask before reaching _maybe_trade. The old
        # cross-verification against REST-cached market data was blocking
        # hundreds of legitimate trades per session because the cache goes
        # stale near settlement. If the live orderbook fetch above failed,
        # we already fell back to cached prices at line 1884.
        # The hard floor check below (cell min/max band) is sufficient
        # protection against bogus prices.

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

        # Only resolution_rider remains; the only filter is the strategy matrix.
        candidates = [
            (name, rec) for name, rec in strategies_status.items()
            if rec.should_trade and self.strategy_matrix.is_enabled(asset_key, name)
        ]

        for name, rec in candidates:
            if not rec.should_trade:
                continue

            side = rec.signal.value  # "yes" or "no"
            max_price = rec.max_price_cents  # strategy's ceiling price

            if side == "yes":
                ask_price = yes_ask if yes_ask is not None else max_price
                bid_price = yes_bid if yes_bid is not None else (ask_price - 4)
            else:
                ask_price = (100 - yes_bid) if yes_bid is not None else max_price
                bid_price = (100 - yes_ask) if yes_ask is not None else (ask_price - 4)

            # Book-moved check. Prior to 2026-04-21 this skipped
            # unconditionally whenever the fresh orderbook's ask
            # exceeded the strategy's max_price. Analysis of 4/21
            # showed this was throwing away ~50% of our trade decisions
            # — the fast-RR scanner saw a valid 94-97c setup, but by
            # the time _maybe_trade fetched a fresh orderbook the
            # book had moved to 99-100c and we bailed.
            #
            # For TAKER mode this check is correct (we'd immediately
            # fill at the new bad price). For MAKER mode (our default)
            # it is overcautious: a maker bid at max_price just sits
            # unfilled if the book doesn't come back, then expires —
            # no loss, no fee. The 15s maker timeout already bounds
            # our downside to wasted time.
            is_taker = os.getenv("RR_TAKER", "0") == "1"
            if is_taker and ask_price > max_price:
                self._log(
                    f"  [SKIP-TAKER] {name}: {ticker} ask {ask_price}c > max {max_price}c "
                    f"(book moved; yes_bid={yes_bid} yes_ask={yes_ask})"
                )
                self._record_skip(ticker, name, side, ask_price, max_price,
                                  yes_bid, yes_ask, hit_ctx=hit_ctx)
                continue
            if not is_taker and ask_price > max_price:
                # Maker path: log so the behavior is visible on the
                # dashboard + hit_outcomes.csv, but proceed to post
                # the resting bid at max_price. It will either fill
                # on a book-reversal or expire at 15s.
                self._log(
                    f"  [BOOK-MOVED-MAKER] {name}: {ticker} ask {ask_price}c > max "
                    f"{max_price}c — posting resting maker bid at max_price"
                )

            # RR order mode: default is MAKER (post at bid, no fees).
            # Kalshi maker fees are $0, and maker fills are cheaper by the
            # whole spread (1-2c) — which at RR's ~2c-per-win economics is
            # ~doubling the per-trade margin when you do fill. The tradeoff
            # is that some trades will time out without a fill, but those
            # trades just get cancelled (no loss).
            #
            # Flip to taker by setting RR_TAKER=1 if you need guaranteed
            # fills on a given cell.
            if is_taker:
                exec_price = min(ask_price, max_price)
                is_maker = False
            else:
                # Post at bid side, bounded by the cell's contract-price band.
                # On a book with yes_bid=96, yes_ask=97 this posts a resting
                # buy at 96c that fills only when a counterparty crosses.
                #
                # When the book has moved above max_price (bid_price >
                # max_price), clamp to max_price so the resting order
                # still conforms to the strategy's ceiling — waits for
                # the book to reverse to our price or expires. This is
                # the behavior we want in the "book-moved" case.
                cp = cell_params or {}
                cell_min_cp = cp.get("min_contract_price", 95)
                maker_price = max(bid_price, cell_min_cp)
                exec_price = min(maker_price, max_price)
                is_maker = True

            # Hard floor: never submit an RR order outside the cell's
            # [min_contract_price, max_entry_price] band. Guards against the
            # 2026-04-13 "NO@1c" incident where a market flipped between the
            # RR signal and order submission (one-sided book → yes_ask None
            # → earlier sanity check bypassed).
            # Uses per-cell params so cells with wider bands (e.g. 90-98c)
            # aren't blocked by the strategy-level defaults.
            cp = cell_params or {}
            strat_min = cp.get("min_contract_price", 95)
            strat_max = cp.get("max_entry_price", 98)
            if exec_price < strat_min or exec_price > strat_max:
                self._log(
                    f"  [SKIP] {name}: exec_price {exec_price}c outside "
                    f"[{strat_min}, {strat_max}]c band "
                    f"(book moved between signal and submission)",
                    level="warning",
                )
                self._log_hit_outcome(
                    "skip_outside_band",
                    reason=f"exec {exec_price}c not in [{strat_min},{strat_max}]",
                    exec_price_c=exec_price,
                    hit_ctx=hit_ctx,
                )
                continue

            calibrated_p = rec.confidence

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
                # This was silent before — we fixed the visibility gap
                # after seeing 72/122 hits over 3 days end up with no
                # observable outcome. risk_mgr rejects (daily-loss cap,
                # open-position limit, confidence floor) now land in
                # hit_outcomes.csv with the reason string attached.
                self._log(f"  [SKIP] {name}: {ticker} risk_mgr rejected ({reason})")
                self._log_hit_outcome(
                    "skip_risk_rejected",
                    reason=reason,
                    exec_price_c=exec_price,
                    hit_ctx=hit_ctx,
                )
                continue
            _trade_stages["risk"] = time.monotonic()

            # Safety margin vs settlement. A place_order + 15s fill-wait +
            # cleanup (cancel, taker-fallback) needs ~15-25s. If there
            # isn't enough runway left, skip the entry — the order would
            # otherwise time out with the market already closed and the
            # bot charged for a cancelled order it never actually filled.
            # MIN_FLOW_SECS is the smallest window that can complete a
            # sane order flow; at secs < this we were logging [CANCEL]
            # every time and nothing ever filled.
            MIN_FLOW_SECS = 15
            secs_remaining = scanner.seconds_until_close(market)
            if secs_remaining < MIN_FLOW_SECS:
                self._log(
                    f"  [SKIP] {name}: {ticker} too close to settlement "
                    f"({secs_remaining:.1f}s < {MIN_FLOW_SECS}s flow budget)")
                self._log_hit_outcome(
                    "skip_too_close",
                    reason=f"{secs_remaining:.1f}s < {MIN_FLOW_SECS}s",
                    exec_price_c=exec_price,
                    hit_ctx=hit_ctx,
                )
                continue

            # RR position sizing: single flat STAKE_USD (read once in
            # load_config, piped via self.config). No Kelly — the edge
            # is 1-5c with ~98% WR, so variance is low enough that flat
            # sizing beats Kelly's aggressiveness and the single knob is
            # easier to reason about than the old 3-way stake soup.
            flat_stake = self.config["stake_usd"]
            price_frac = exec_price / 100.0
            if price_frac > 0:
                contracts = max(1, int(flat_stake / price_frac))
                stake = contracts * price_frac
            else:
                contracts = 1
                stake = price_frac

            # If stake exceeds available balance, downsize to fit remaining cash
            # rather than skipping entirely. Only skip if even 1 contract won't fit.
            if not self.config["paper_trade"] and stake > balance:
                price_frac = exec_price / 100.0
                if price_frac <= 0 or balance < price_frac:
                    self._log(f"  [SKIP] {name}: balance ${balance:.2f} < 1 contract @ {exec_price}c")
                    self._log_hit_outcome(
                        "skip_balance_low",
                        reason=f"balance ${balance:.2f} < 1 contract @ {exec_price}c",
                        exec_price_c=exec_price,
                        hit_ctx=hit_ctx,
                    )
                    continue
                new_contracts = int(balance / price_frac)
                new_stake = new_contracts * price_frac
                self._log(f"  [DOWNSIZE] {name}: ${stake:.2f} → ${new_stake:.2f} ({contracts}→{new_contracts} contracts) to fit balance ${balance:.2f}")
                contracts = new_contracts
                stake = new_stake

            # Fee-adjusted EV check. RR uses empirical win rate (~99%) for
            # EV, not market-implied price: the edge IS the gap between the
            # 95-98c entry and the ~99% hold-to-settlement win rate.
            # Maker orders pay $0 fees on Kalshi.
            entry_fee = 0.0 if is_maker else kalshi_taker_fee(contracts, exec_price)
            payout = contracts * 1.00
            ev_prob = 0.99
            ev_win = ev_prob * (payout - stake - entry_fee)
            ev_loss = (1 - ev_prob) * (stake + entry_fee)
            expected_value = ev_win - ev_loss
            if expected_value <= 0:
                self._log(f"  [SKIP] {name}: negative EV after fees "
                          f"(EV=${expected_value:.2f}, fees=${entry_fee:.2f})")
                self._log_hit_outcome(
                    "skip_negative_ev",
                    reason=f"EV=${expected_value:.2f} fees=${entry_fee:.2f}",
                    exec_price_c=exec_price,
                    stake_usd=round(stake, 2),
                    hit_ctx=hit_ctx,
                )
                continue
            _trade_stages["ev"] = time.monotonic()

            # Execute — mark ticker BEFORE placing order to prevent re-entry
            # For daily markets, mark the event prefix to block all strikes in this event
            self._traded_tickers.add(ticker)
            parts = ticker.rsplit("-", 1)
            if len(parts) > 1 and parts[0] != ticker:
                self._traded_tickers.add(parts[0])  # e.g. KXBTCD-26APR0702

            # For Kalshi API: yes_price is always in YES terms
            api_yes_price = exec_price if side == "yes" else (100 - exec_price)

            order_mode = "MAKER" if is_maker else "TAKER"

            # Log orderbook depth at 95-99c. Kalshi's modern API returns
            # `orderbook_fp` (dollar-string prices) not `orderbook` (cent
            # integers), so we parse both schemas — the legacy .get("orderbook")
            # path always returned empty and the log was useless.
            try:
                _depth_book = scanner.client.get_orderbook(ticker, depth=10)
                _yes_levels = []  # (cents, qty) pairs, each level
                _no_levels = []
                _fp = _depth_book.get("orderbook_fp")
                if _fp:
                    for lvl in _fp.get("yes_dollars", []) or []:
                        try:
                            _yes_levels.append((int(round(float(lvl[0]) * 100)), float(lvl[1])))
                        except (ValueError, TypeError, IndexError):
                            continue
                    for lvl in _fp.get("no_dollars", []) or []:
                        try:
                            _no_levels.append((int(round(float(lvl[0]) * 100)), float(lvl[1])))
                        except (ValueError, TypeError, IndexError):
                            continue
                else:
                    _legacy = _depth_book.get("orderbook", {}) or {}
                    for lvl in _legacy.get("yes", []) or []:
                        try:
                            _yes_levels.append((int(lvl[0]), float(lvl[1])))
                        except (ValueError, TypeError, IndexError):
                            continue
                    for lvl in _legacy.get("no", []) or []:
                        try:
                            _no_levels.append((int(lvl[0]), float(lvl[1])))
                        except (ValueError, TypeError, IndexError):
                            continue
                # Available to BUY YES at 95-99c = NO bids whose (100-p)
                # lands in 95-99; same for NO.
                _yes_available = [(100 - p, q) for p, q in _no_levels if 95 <= (100 - p) <= 99]
                _no_available = [(100 - p, q) for p, q in _yes_levels if 95 <= (100 - p) <= 99]
                _our_depth = _yes_available if side == "yes" else _no_available
                _total_cts = sum(q for _, q in _our_depth)
                _depth_str = ", ".join(f"{p}c:{q:.0f}" for p, q in sorted(_our_depth))
                self._log(f"      [DEPTH] {side.upper()} 95-99c: {_total_cts:.0f} contracts available [{_depth_str}]")
            except Exception as e:
                self._log(f"      [DEPTH] Could not fetch orderbook: {e}")

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
                        entry_fee = kalshi_taker_fee(contracts, exec_price)
                    filled_count = result.get("order", {}).get("contracts_filled")
                    if filled_count is not None and filled_count < contracts:
                        contracts = filled_count
                        stake = contracts * (exec_price / 100.0)
                        entry_fee = kalshi_taker_fee(contracts, exec_price)
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
                _trade_stages["submit"] = time.monotonic()

                # Balance just changed (locked stake). Invalidate the
                # cache so the next call fetches fresh — otherwise the
                # balance gate could approve a stacking order against
                # stale pre-trade balance.
                self.invalidate_balance_cache()

                # Reserve the order_id in the CSV immediately. Closes the
                # reconcile race: the reconcile thread scans `all_oids` from
                # the CSV and imports any Kalshi fill whose oid isn't in the
                # set as `kalshi_api_import`. Without this preliminary row,
                # a partially-filled order can be snatched by reconcile
                # before the fill-wait loop finishes, which is what produced
                # the phantom "no trade from current run_id" situation.
                pending_record = TradeRecord(
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
                self.logger.upsert_entry(
                    pending_record, reason=f"PLACED: {rec.reason}", confidence=rec.confidence,
                )

                # Live mode: verify the order was filled
                if not self.config["paper_trade"]:
                    order_status = result.get("order", {}).get("status", "")
                    if order_status not in ("filled", "resting", "executed"):
                        self._log(f"      [WARN] Order status: {order_status} — skipping record")
                        # Mark the reserved row as rejected so reconcile
                        # doesn't try to fill it with actual Kalshi state.
                        pending_record.contracts = 0
                        pending_record.stake_usd = 0
                        self.logger.upsert_entry(
                            pending_record, reason=f"REJECTED: status={order_status}",
                            confidence=rec.confidence,
                        )
                        continue
                    if order_status == "resting":
                        # Order is on the book — poll for fill.
                        # Cap maker wait to leave time for taker fallback
                        # (need ~10s for cancel + resubmit + settle).
                        secs_in_window = scanner.seconds_until_close(market)
                        cp = cell_params or {}
                        cell_min_secs = cp.get("min_seconds", 10)
                        # Reserve enough time for taker fallback
                        taker_reserve = cell_min_secs + 10
                        if is_maker:
                            # Maker wait: bounded by 30s. Longer waits
                            # rarely convert (a resting bid unfilled
                            # after 30s is usually getting passed by)
                            # and block iteration through newer setups.
                            # Unfilled orders cancel at no P&L cost; we
                            # then try taker fallback if there's room.
                            max_wait = max(5, min(30, int(secs_in_window - taker_reserve)))
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
                                        entry_fee = kalshi_taker_fee(contracts, exec_price)
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

                                # Drift check: if the book's current ask has
                                # moved ≥2c above our resting maker bid, the
                                # market has walked away from us. Cancel and
                                # go to taker fallback rather than waste the
                                # rest of the wait window. Only checked after
                                # a short settle period so a one-tick flicker
                                # doesn't trip it. WS-backed, zero REST cost.
                                if (is_maker and wait_round >= 5
                                        and wait_round % 3 == 0):
                                    cur_bid, cur_ask = scanner.parse_yes_price(market)
                                    if cur_bid is not None and cur_ask is not None:
                                        ref_ask = (cur_ask if side == "yes"
                                                   else 100 - cur_bid)
                                        drift = ref_ask - exec_price
                                        if drift >= 2:
                                            self._log(
                                                f"      [DRIFT] Book moved "
                                                f"{drift}c away from maker bid "
                                                f"@ {exec_price}c (ref ask "
                                                f"{ref_ask}c) — abandoning "
                                                f"maker at {wait_round + 1}s")
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
                                entry_fee = kalshi_taker_fee(contracts, exec_price)
                                self._log(f"      [PARTIAL] Keeping {fill_count} filled contracts after cancel")
                                filled = True
                            elif is_maker:
                                # Maker didn't fill — try taker fallback if
                                # we're still within the cell's time window.
                                remaining = scanner.seconds_until_close(market)
                                cp = cell_params or {}
                                min_secs = cp.get("min_seconds", 10)
                                if remaining > min_secs + 5:
                                    taker_price = min(ask_price, max_price)
                                    if strat_min <= taker_price <= strat_max:
                                        taker_fee = kalshi_taker_fee(contracts, taker_price)
                                        api_yes_taker = taker_price if side == "yes" else (100 - taker_price)
                                        self._log(
                                            f"      [TAKER-FALLBACK] Resubmitting as taker @ "
                                            f"{taker_price}c ({remaining:.0f}s left)")
                                        try:
                                            taker_oid = str(uuid.uuid4())
                                            taker_result = self.executor.place_order(
                                                ticker=ticker,
                                                action="buy",
                                                side=side,
                                                count=contracts,
                                                order_type="limit",
                                                yes_price=api_yes_taker,
                                                client_order_id=taker_oid,
                                            )
                                            taker_status = taker_result.get("order", {}).get("status", "")
                                            if taker_status in ("filled", "executed"):
                                                exec_price = taker_price
                                                stake = contracts * (exec_price / 100.0)
                                                entry_fee = taker_fee
                                                is_maker = False
                                                order_id = taker_result.get("order", {}).get("order_id", taker_oid)
                                                pending_record.order_id = order_id
                                                pending_record.client_order_id = taker_oid
                                                pending_record.price_cents = exec_price
                                                pending_record.stake_usd = stake
                                                pending_record.is_maker = False
                                                self._log(f"      [TAKER-FILLED] Filled at {exec_price}c (fees=${taker_fee:.2f})")
                                                filled = True
                                            else:
                                                self._log(f"      [TAKER-FALLBACK] Status: {taker_status}, abandoning")
                                        except Exception as e:
                                            self._log(f"      [TAKER-FALLBACK] Failed: {e}", level="warning")

                            if not filled:
                                # Mark the reserved row as cancelled so reconcile
                                # doesn't resurrect it or import a phantom row.
                                pending_record.contracts = 0
                                pending_record.stake_usd = 0
                                self.logger.upsert_entry(
                                    pending_record,
                                    reason=f"CANCELLED: unfilled after {max_wait}s",
                                    confidence=rec.confidence,
                                )
                                _trade_stages["wait"] = time.monotonic()
                                self._log_submission_timing(
                                    ticker, _trade_t0, _trade_stages)
                                continue

                self._log(f"      Order placed: {order_id}")
                self._log_hit_outcome(
                    "submitted",
                    reason=order_id,
                    exec_price_c=exec_price,
                    stake_usd=round(stake, 2),
                    hit_ctx=hit_ctx,
                )

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

                _trade_stages["wait"] = time.monotonic()
                self._log_submission_timing(ticker, _trade_t0, _trade_stages)

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
                # upsert_entry rewrites the PLACED row we wrote earlier with
                # the final filled shape (actual contracts, actual fees, etc.).
                # Falls through to append-only if no matching oid row exists
                # (paper mode or unexpected state).
                self.logger.upsert_entry(record, reason=rec.reason, confidence=rec.confidence)

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

    # Early-exit and Bayesian-exit methods were removed 2026-04-14 after
    # live results showed they whipsawed ~$60 of profits on 2026-04-13. RR
    # holds every position to settlement — no mid-trade exit logic remains.

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

        # Helper: build price data for an asset, including the exact
        # momentum the RR cell uses for entry gating (cell window/periods)
        # plus the cell's max_adverse_momentum gate for context.
        def _price_data(key):
            feed = self.assets[key]["price_feed"]
            # Match the runtime cell — 15M for primary, hourly for _daily.
            if key.endswith("_daily"):
                cell_name = key.replace("_daily", "_hourly")
            else:
                cell_name = f"{key}_15m"
            cell = self._rr_cell_params.get(cell_name, {})
            cell_window = cell.get("momentum_window", 60)
            cell_periods = cell.get("momentum_periods", 5)
            vol_lookback = cell.get("vol_lookback", 300)
            cell_mom = None
            if hasattr(feed, "momentum_smoothed"):
                cell_mom = feed.momentum_smoothed(window=cell_window, periods=cell_periods)
            realized_vol = None
            if hasattr(feed, "volatility"):
                realized_vol = feed.volatility(lookback_seconds=vol_lookback)
            return {
                "price": feed.current_price or 0,
                "mom_1m": feed.momentum_1m() or 0,
                "mom_5m": feed.momentum_5m() or 0,
                "mom_cell": round(cell_mom, 4) if cell_mom is not None else None,
                "mom_window": cell_window,
                "mom_periods": cell_periods,
                "mom_gate": cell.get("max_adverse_momentum"),
                "realized_vol": round(realized_vol, 4) if realized_vol is not None else None,
                "vol_gate": cell.get("max_realized_vol_pct"),
                "vol_lookback": vol_lookback,
                "prices": [[int(ts * 1000), px] for ts, px in feed.prices][-180:],
            }

        btc = _price_data("btc")
        eth = _price_data("eth")
        sol = _price_data("sol")

        # Gather momentum for every tradeable asset key so the dashboard
        # can show the exact values the bot uses for RR gating. Includes
        # both 15M and _daily entries — same price feed but different
        # cell windows, so each gets its own gate context.
        asset_momentum = {}
        for key in self.assets.keys():
            pd = _price_data(key)
            asset_momentum[key] = {
                "price": round(pd["price"], 4) if pd["price"] else 0,
                "mom_1m": round(pd["mom_1m"], 4),
                "mom_5m": round(pd["mom_5m"], 4),
                "mom_cell": pd["mom_cell"],
                "mom_window": pd["mom_window"],
                "mom_periods": pd["mom_periods"],
                "mom_gate": pd["mom_gate"],
                "realized_vol": pd["realized_vol"],
                "vol_gate": pd["vol_gate"],
                "vol_lookback": pd["vol_lookback"],
            }

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

        def _strat_data(key):
            status = all_strategies.get(key, {})
            rec = status.get("resolution_rider")
            if rec:
                return {"resolution_rider": {
                    "signal": rec.signal.value if rec.signal.value != "none" else "none",
                    "confidence": round(rec.confidence, 2),
                    "reason": rec.reason,
                }}
            return {"resolution_rider": {"signal": "none", "confidence": 0, "reason": ""}}

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
            # Use the authoritative open-positions dict. Counting
            # risk_mgr.trades entries by outcome=="" is wrong: that list is
            # append-only and keeps orphaned records when a position is
            # settled via the CSV-based fallback path (which does not call
            # settle_trade). open_positions is popped correctly on
            # settle_trade and matches heartbeat.json.
            self._hist_stats["pending"] = len(self.risk_mgr.open_positions)
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
            # Per-asset markets, settlements, and strategies — all 14
            # asset keys (7 coins × {15M, daily}) so the dashboard can
            # surface every market the bot is actually trading.
            "markets": {key: _market_data(key) for key in self.assets.keys()},
            "settled": {key: _settled_data(key) for key in self.assets.keys()},
            "strategies_by_asset": {key: _strat_data(key) for key in self.assets.keys()},
            "asset_momentum": asset_momentum,
            "trading_enabled": _sse_state.trading_enabled,
            "vol_regime": "medium",  # Vol-regime gating removed; RR uses per-cell optimized params.
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
            "recent_skips": self._recent_skips[-20:],
            "hit_outcomes_summary": {
                "window_hours": 24,
                "counts": dict(self._hit_outcome_counts),
                "total": sum(self._hit_outcome_counts.values()),
                "fills": self._hit_outcome_counts.get("submitted", 0),
            },
            "gate_matrix": self._build_gate_matrix_snapshot(),
            "rr_config": {
                "defaults": {
                    "min_contract_price": rr.min_contract_price,
                    "max_entry_price": rr.max_entry_price,
                    "min_seconds": rr.min_seconds,
                    "max_seconds": rr.max_seconds,
                    "min_price_buffer_pct": rr.min_price_buffer_pct,
                    "max_adverse_momentum": rr.max_adverse_momentum,
                    "max_stake_usd": self.config["stake_usd"],
                },
                # Publish ALL cells (safe + disabled) so the dashboard can
                # show *why* a cell isn't trading instead of falling back to
                # defaults silently. `enabled` = cleared the safety gate.
                "per_cell": {k: {
                    # Raw numeric fields — dashboard formats these. Units:
                    #   *_contract_price / *_entry_price → cents (int)
                    #   min_seconds / max_seconds       → seconds (int)
                    #   min_price_buffer_pct            → percent (float, e.g. 0.68 = 0.68%)
                    #   mom_gate (max_adverse_momentum) → percent, negative (float)
                    #   vol_gate (max_realized_vol_pct) → percent (float) or null when disabled
                    "min_contract_price": v["min_contract_price"],
                    "max_entry_price": v["max_entry_price"],
                    "min_seconds": v.get("min_seconds"),
                    "max_seconds": v["max_seconds"],
                    "min_price_buffer_pct": v["min_price_buffer_pct"],
                    # Display-formatted convenience strings (kept for back-compat)
                    "price": f"{v['min_contract_price']}-{v['max_entry_price']}c",
                    "max_secs": v["max_seconds"],
                    "buffer": f"{v['min_price_buffer_pct']}%",
                    "mom_gate": v.get("max_adverse_momentum"),
                    "mom_window": v.get("momentum_window"),
                    "mom_periods": v.get("momentum_periods"),
                    "vol_gate": v.get("max_realized_vol_pct"),
                    "cv_wr": v.get("cv_mean_win_rate"),
                    "cv_trades": v.get("cv_total_val_trades", 0),
                    "enabled": bool(v.get("enabled", False)),
                    "disabled_reason": v.get("disabled_reason"),
                } for k, v in self._rr_cell_params_all.items()},
            } if (rr := self.strategies.get("resolution_rider")) else {},
        }

        with _sse_state.lock:
            _sse_state.tick_data = json.dumps(tick_data)

    def _asset_key_from_ticker(self, ticker: str) -> str:
        """Derive the asset key from a market ticker for matrix tracking."""
        for coin in ("btc", "eth", "sol", "doge", "xrp", "bnb", "hype"):
            uc = coin.upper()
            if f"KX{uc}15M" in ticker:
                return coin
            if f"KX{uc}D" in ticker:
                return f"{coin}_daily"
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
        """Record a trade outcome to the strategy matrix and refresh stats."""
        self._hist_stats = self.logger.get_historical_stats()
        asset_key = self._asset_key_from_ticker(record.ticker)
        self.strategy_matrix.record_trade(
            asset=asset_key,
            strategy=record.strategy,
            pnl=record.profit_usd,
            stake=record.stake_usd,
            outcome=record.outcome,
        )

    # Canonical gate order — MUST match the order of `bump(cell, reason)`
    # calls inside _fast_rr_scan so the dashboard can render columns
    # green (passed) / red (blocked here) / gray (not reached).
    GATE_ORDER: list[str] = [
        "already_traded", "bad_close_time",
        "secs_too_low", "secs_too_high",
        "no_price", "fav_too_low", "fav_too_high",
        "no_spot", "no_strike", "bad_strike",
        "yes_buf_low", "no_buf_high",
        "eval_no_trade", "matrix_disabled",
    ]

    def _build_gate_matrix_snapshot(self) -> list:
        """Per-ticker gate state for the dashboard matrix. Emits one row
        per active ticker (seen in the last 120s), sorted by cell then
        ticker. Each row has enough context to explain WHY a particular
        market isn't firing right now."""
        cutoff = time.time() - 120
        rows = []
        for ticker, st in self._ticker_gate_state.items():
            if st.get("last_seen", 0) < cutoff:
                continue
            rows.append({
                "ticker": ticker,
                "cell": st.get("cell", ""),
                "blocked_at": st.get("blocked_at", ""),
                "detail": st.get("detail", {}),
                "age_s": round(time.time() - st.get("last_seen", 0), 1),
            })
        rows.sort(key=lambda r: (r["cell"], r["ticker"]))
        return {"gates": self.GATE_ORDER, "rows": rows}

    def _record_skip(self, ticker: str, strategy: str, side: str,
                     ask_price: int, max_price: int,
                     yes_bid: Optional[int], yes_ask: Optional[int],
                     hit_ctx: Optional[dict] = None):
        """Compatibility shim — the book-moved path was the first
        instrumented skip. Now routed through _log_hit_outcome so every
        skip reason lands in hit_outcomes.csv uniformly."""
        self._log_hit_outcome(
            "skip_book_moved",
            reason="ask_gt_max",
            ask_price_c=ask_price,
            yes_bid_c=yes_bid, yes_ask_c=yes_ask,
            hit_ctx=hit_ctx,
        )

    def _log_hit_outcome(self, outcome: str, reason: str = "",
                         hit_ctx: Optional[dict] = None, **extras):
        """Write one row to hit_outcomes.csv describing what happened to
        a [FAST-RR] Hit. Callers pass their candidate's hit_ctx dict
        (required for correct attribution under parallel dispatch); if
        omitted, falls back to the instance attribute for backward compat.

        Falls back to a minimal row if ctx is absent — keeps auditing
        honest even if a code path bypasses the Hit→ctx setup."""
        ctx = hit_ctx if hit_ctx is not None else (self._hit_ctx or {})
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "time": ctx.get("time", now_iso),
            "ticker": ctx.get("ticker", extras.get("ticker", "")),
            "strategy": ctx.get("strategy", extras.get("strategy", "")),
            "side": ctx.get("side", extras.get("side", "")),
            "max_price_c": ctx.get("max_price_c", extras.get("max_price_c", "")),
            "yes_bid_c": extras.get("yes_bid_c", ctx.get("yes_bid_c", "")),
            "yes_ask_c": extras.get("yes_ask_c", ctx.get("yes_ask_c", "")),
            "outcome": outcome,
            "reason": reason,
            "ask_price_c": extras.get("ask_price_c", ""),
            "exec_price_c": extras.get("exec_price_c", ""),
            "stake_usd": extras.get("stake_usd", ""),
            "secs_left": ctx.get("secs_left", ""),
            "cell": ctx.get("cell", ""),
        }

        # Append to CSV under a dedicated lock — multiple parallel
        # submissions can land here at once.
        try:
            with self._hit_outcomes_lock:
                with open(self._hit_outcomes_csv, "a", newline="") as f:
                    csv.writer(f).writerow([
                        row["time"], row["ticker"], row["strategy"], row["side"],
                        row["max_price_c"], row["yes_bid_c"], row["yes_ask_c"],
                        row["outcome"], row["reason"],
                        row["ask_price_c"], row["exec_price_c"], row["stake_usd"],
                        row["secs_left"], row["cell"],
                    ])
        except Exception:
            pass

        # Rolling 24h counter for dashboard summary. Dict mutations are
        # GIL-atomic for simple assignment/increment.
        if time.time() - self._hit_outcome_window_start > 86400:
            self._hit_outcome_counts = {}
            self._hit_outcome_window_start = time.time()
        self._hit_outcome_counts[outcome] = self._hit_outcome_counts.get(outcome, 0) + 1

        # Mirror skip-kind outcomes to the _recent_skips dashboard feed so
        # the existing RecentSkipsPanel shows them with the right reason
        # (was showing only book_moved before).
        if outcome.startswith("skip_"):
            self._recent_skips.append({
                "timestamp": row["time"],
                "ticker": row["ticker"],
                "strategy": row["strategy"],
                "side": row["side"],
                "ask_price": row["ask_price_c"] if row["ask_price_c"] != "" else 0,
                "max_price": row["max_price_c"] if row["max_price_c"] != "" else 0,
                "yes_bid": row["yes_bid_c"] if row["yes_bid_c"] != "" else None,
                "yes_ask": row["yes_ask_c"] if row["yes_ask_c"] != "" else None,
                "reason": outcome.replace("skip_", "") + (f":{reason}" if reason else ""),
            })
            if len(self._recent_skips) > 50:
                self._recent_skips = self._recent_skips[-50:]

        # Clear the instance-level ctx only when we were using it.
        # Callers that passed an explicit hit_ctx manage their own
        # state; clearing the instance attr would cross-wire threads.
        if hit_ctx is None:
            self._hit_ctx = None

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

    def reload_rr_params(self) -> dict:
        """Re-read data/rr_params.json and re-apply the cell safety gate +
        strategy-matrix enable/disable. Called from the SIGHUP handler so
        the nightly auto-reoptimizer can deploy new params without a full
        bot restart.

        Returns a summary dict with {"loaded": n, "disabled": [cells...]}."""
        rr_params_path = Path("data/rr_params.json")
        if not rr_params_path.exists():
            self._log("[RELOAD] rr_params.json missing — keeping current params", level="warning")
            return {"loaded": 0, "disabled": []}
        with open(rr_params_path) as f:
            all_rr_params = json.load(f)

        enable_all = os.environ.get("RR_ENABLE_ALL", "0") == "1"

        # Refresh 7-day live P&L on reload so the gate reflects the
        # latest trade results (the nightly cron, a manual SIGHUP, or
        # the bot starting up after a bad run all see current reality).
        pnl_by_cell = load_recent_cell_pnl(days=7)

        safe_cells = {}
        annotated = {}
        for k, v in all_rr_params.items():
            ok, reason = evaluate_cell_safety(
                k, v, pnl_by_cell=pnl_by_cell, enable_all=enable_all)
            # Per-cell optimizer params pass through directly; strategy-
            # level clamps at max_ep≤97 and min_buf≥0.15 guard the two
            # known failure modes without neutering the per-cell tuning.
            merged = dict(v)
            annotated[k] = {**merged, "enabled": ok,
                            "disabled_reason": reason or None}
            if ok:
                safe_cells[k] = merged
        prev_cells = set(self._rr_cell_params.keys())
        new_cells = set(safe_cells.keys())
        added = sorted(new_cells - prev_cells)
        removed = sorted(prev_cells - new_cells)

        # Atomic swap — never leave the bot in a half-updated state.
        self._rr_cell_params = safe_cells
        self._rr_cell_params_all = annotated
        self._log(f"[RELOAD] RR params reloaded: {len(safe_cells)} safe cells")
        if added:
            self._log(f"[RELOAD] newly enabled: {', '.join(added)}")
        if removed:
            self._log(f"[RELOAD] newly disabled: {', '.join(removed)}")

        # Re-sync the strategy matrix to mirror cell state.
        for asset in self.assets.keys():
            if asset.endswith("_daily"):
                cell = asset.replace("_daily", "_hourly")
            else:
                cell = f"{asset}_15m"
            if cell in self._rr_cell_params:
                self.strategy_matrix.force_enable(asset, "resolution_rider", clear_history=False)
            else:
                self.strategy_matrix.force_disable(asset, "resolution_rider", hard=True)

        unsafe = set(all_rr_params.keys()) - new_cells
        return {"loaded": len(safe_cells), "disabled": sorted(unsafe)}


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    config = load_config()
    bot = TradingBot(config)

    # Handle SIGINT/SIGTERM
    def handle_signal(signum, frame):
        bot.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # SIGHUP: hot-reload rr_params.json. The nightly auto-reoptimizer
    # uses this so deploys don't require a full restart (and don't
    # interrupt any open position).
    def handle_sighup(signum, frame):
        try:
            bot.reload_rr_params()
        except Exception as e:
            bot._log(f"[RELOAD] reload_rr_params failed: {e}", level="error")
    signal.signal(signal.SIGHUP, handle_sighup)

    bot.run()


if __name__ == "__main__":
    main()
