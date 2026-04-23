"""Missed-trade tracker for Resolution Rider.

Periodically scans recently-settled 15M / daily crypto markets and flags
tickers that reached 94c+ on the winning side during their life but have
no RR entry row in live_trades.csv. Writes a row per miss to
data/missed_trades.csv and keeps the last N in memory for the dashboard.

This is a diagnostic feed, not a strategy: the value is in driving the
number of misses to zero by tuning gates / fixing execution bugs.
"""

from __future__ import annotations

import csv
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


logger = logging.getLogger("kalshi_bot")


class MissedTradeTracker:
    """Background scanner that flags missed RR-eligible setups.

    A "miss" is a ticker that:
      1. Settled within `lookback_minutes`
      2. Reached `threshold_cents`+ on the eventually-winning side during
         its life (visible in the tick log)
      3. Does not appear as an RR entry row in live_trades.csv
    """

    def __init__(
        self,
        scanners: dict,
        trades_csv: Path,
        ticks_dir: Path,
        output_csv: Path,
        lookback_minutes: int = 30,
        threshold_cents: int = 94,
    ):
        self.scanners = scanners
        self.trades_csv = Path(trades_csv)
        self.ticks_dir = Path(ticks_dir)
        self.output_csv = Path(output_csv)
        self.lookback_minutes = lookback_minutes
        self.threshold_cents = threshold_cents

        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._recent_misses: list = []
        self._seen_tickers: set = set()  # dedup across scan passes
        self._lock = threading.Lock()

        self._init_output_csv()

    def _init_output_csv(self) -> None:
        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        if not self.output_csv.exists():
            with open(self.output_csv, "w", newline="") as f:
                csv.writer(f).writerow([
                    "detected_at", "ticker", "series",
                    "close_time", "settle_result",
                    "peak_side", "peak_price_c", "peak_timestamp",
                ])

    def start(self, interval_seconds: int = 300) -> None:
        """Start the background scan thread."""
        if self._thread and self._thread.is_alive():
            return
        self.running = True
        self._thread = threading.Thread(
            target=self._loop, args=(interval_seconds,),
            daemon=True, name="missed-trades")
        self._thread.start()

    def stop(self) -> None:
        self.running = False

    def _loop(self, interval_s: int) -> None:
        time.sleep(60)  # let WS + scanners bootstrap
        while self.running:
            try:
                new_misses = self.scan()
                if new_misses:
                    self._log_misses(new_misses)
            except Exception as e:
                logger.warning(f"[MISSED] scan error: {e}")
            for _ in range(interval_s):
                if not self.running:
                    break
                time.sleep(1)

    def _traded_tickers(self) -> set:
        """Tickers we placed a non-cancelled RR entry on (any time)."""
        if not self.trades_csv.exists():
            return set()
        tickers: set = set()
        try:
            with open(self.trades_csv, newline="") as f:
                for row in csv.DictReader(f):
                    if (row.get("strategy") or "") != "resolution_rider":
                        continue
                    reason = row.get("reason") or ""
                    if reason.startswith("SETTLED:"):
                        continue
                    if reason.startswith(("CANCELLED", "REJECTED")):
                        continue
                    try:
                        if int(row.get("contracts") or 0) <= 0:
                            continue
                    except (ValueError, TypeError):
                        continue
                    t = row.get("ticker") or ""
                    if t:
                        tickers.add(t)
        except OSError:
            pass
        return tickers

    def _ticks_for_market(self, ticker: str,
                          window_start: datetime,
                          window_end: datetime) -> list:
        """Load ticks for `ticker` within the UTC window. Reads the
        date-partitioned files that overlap the window."""
        ticks: list = []
        day = window_start.date()
        end_day = window_end.date()
        while day <= end_day:
            path = self.ticks_dir / f"{day.isoformat()}.csv"
            day += timedelta(days=1)
            if not path.exists():
                continue
            try:
                with open(path, newline="") as f:
                    for row in csv.DictReader(f):
                        if (row.get("ticker") or "") != ticker:
                            continue
                        ts_str = (row.get("timestamp") or "").replace("Z", "+00:00")
                        try:
                            ts = datetime.fromisoformat(ts_str)
                        except ValueError:
                            continue
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        if ts < window_start or ts > window_end:
                            continue
                        ticks.append((ts, row))
            except OSError:
                continue
        ticks.sort(key=lambda t: t[0])
        return ticks

    def scan(self) -> list:
        """Run one scan pass. Returns newly-detected misses (excluding
        tickers already reported in prior passes)."""
        if not self.scanners:
            return []

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=self.lookback_minutes)
        traded = self._traded_tickers()
        detected: list = []

        for series, scanner in self.scanners.items():
            try:
                settled = scanner.get_settled_markets(limit=50)
            except Exception:
                continue

            for m in settled:
                ticker = m.get("ticker") or ""
                if not ticker:
                    continue
                if ticker in self._seen_tickers or ticker in traded:
                    continue

                close_str = (m.get("close_time") or "").replace("Z", "+00:00")
                try:
                    close_dt = datetime.fromisoformat(close_str)
                except ValueError:
                    continue
                if close_dt.tzinfo is None:
                    close_dt = close_dt.replace(tzinfo=timezone.utc)
                if close_dt < cutoff:
                    continue

                settle = (m.get("result") or "").lower()
                if settle not in ("yes", "no"):
                    continue

                # 15M markets live ~15 min; daily events can live hours.
                # 2h window covers both without pulling noise.
                ticks = self._ticks_for_market(
                    ticker, close_dt - timedelta(hours=2), close_dt)
                if not ticks:
                    continue

                peak_price = 0
                peak_ts = ""
                for _ts, row in ticks:
                    try:
                        yb = int(row.get("yes_bid") or 0)
                        ya = int(row.get("yes_ask") or 0)
                    except (ValueError, TypeError):
                        continue
                    # Ask of the winning side — that's the price we would
                    # have paid to enter. If yes settled, yes_ask is our
                    # entry price. If no settled, no_ask = 100 - yes_bid.
                    if settle == "yes":
                        price = ya
                    else:
                        price = 100 - yb
                    if self.threshold_cents <= price <= 99 and price > peak_price:
                        peak_price = price
                        peak_ts = row.get("timestamp", "")

                if peak_price >= self.threshold_cents:
                    detected.append({
                        "detected_at": now.isoformat(),
                        "ticker": ticker,
                        "series": series,
                        "close_time": close_dt.isoformat(),
                        "settle_result": settle,
                        "peak_side": settle,
                        "peak_price_c": peak_price,
                        "peak_timestamp": peak_ts,
                    })
                    self._seen_tickers.add(ticker)

        return detected

    def _log_misses(self, misses: list) -> None:
        with self._lock:
            with open(self.output_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for m in misses:
                    writer.writerow([
                        m["detected_at"], m["ticker"], m["series"],
                        m["close_time"], m["settle_result"],
                        m["peak_side"], m["peak_price_c"], m["peak_timestamp"],
                    ])
            self._recent_misses.extend(misses)
            if len(self._recent_misses) > 50:
                self._recent_misses = self._recent_misses[-50:]

        by_series: dict = defaultdict(int)
        for m in misses:
            by_series[m["series"]] += 1
        summary = ", ".join(f"{s}={n}" for s, n in sorted(by_series.items()))
        logger.info(f"[MISSED] {len(misses)} new missed setup(s): {summary}")

    def recent_misses(self, max_items: int = 20) -> list:
        """Snapshot of the most recent misses (for dashboard / SSE)."""
        with self._lock:
            return list(self._recent_misses[-max_items:])
