#!/usr/bin/env python3
"""
Monte Carlo Parameter Optimizer for Resolution Rider

Phase 1: Explore parameter space against historical data
Phase 2: Walk-forward validate top candidates against recent tick data
Output: Per-cell optimal parameters in data/rr_params.json

Usage:
    python optimize_rr.py
"""

import csv
import json
import os
import multiprocessing
import random
import re
import sys
import time
from collections import defaultdict

# Unbuffered output so we see progress in real-time
sys.stdout.reconfigure(line_buffering=True)
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ── Slippage Model (from Kalshi fill data) ───────────────────────

def load_slippage_model(kalshi_csv: str) -> dict[int, float]:
    """
    Compute average slippage penalty per entry price level from Kalshi fills.

    Slippage is estimated as the average taker fees per contract at each
    entry price level (94-99c range). This captures the real cost of
    crossing the spread that perfect-fill simulation misses.

    Returns: dict mapping entry_price_cents -> slippage_cents_per_contract
    """
    if not os.path.exists(kalshi_csv):
        print(f"  [slippage] Kalshi CSV not found at {kalshi_csv}, using defaults")
        return {}

    # Accumulate per-price-level stats from RR-range fills
    by_price = defaultdict(lambda: {"total_qty": 0, "total_fees": 0, "count": 0})
    with open(kalshi_csv, newline="") as f:
        for row in csv.DictReader(f):
            if row["type"] != "trade":
                continue
            ep = int(row["entry_price_cents"])
            if 94 <= ep <= 99:
                qty = int(row["quantity"])
                fees = int(row["open_fees_cents"]) + int(row["close_fees_cents"])
                d = by_price[ep]
                d["total_qty"] += qty
                d["total_fees"] += fees
                d["count"] += 1

    slippage = {}
    for price, d in sorted(by_price.items()):
        if d["total_qty"] > 0:
            # Fee-based slippage: actual fees paid per contract
            fee_per_contract = d["total_fees"] / d["total_qty"]
            # Add a fixed spread-crossing estimate (empirical: ~0.3c avg)
            # This accounts for the ask being slightly above mid when we buy
            spread_penalty = 0.3
            slippage[price] = fee_per_contract + spread_penalty
        else:
            slippage[price] = 0.5  # Default if no data

    return slippage


# Global slippage model, loaded once at startup
SLIPPAGE_MODEL: dict[int, float] = {}


def get_slippage_cents(entry_price_cents: int) -> float:
    """Get slippage penalty in cents for a given entry price."""
    if not SLIPPAGE_MODEL:
        return 0.5  # Conservative default if no model loaded
    return SLIPPAGE_MODEL.get(entry_price_cents, 0.5)


# ── Parameter Space ───────────────────────────────────────────────

PARAM_RANGES = {
    "min_contract_price": (95, 98),
    "max_entry_price": (96, 98),
    "max_seconds": (60, 480),
    "min_price_buffer_pct": (0.05, 0.50),
    "max_adverse_momentum": (-0.10, 0.0),
    "momentum_window": (30, 300),
    "momentum_periods": (1, 10),
}

ALPHA = 0.3  # Win rate weight in blended score (training phase only)


# ── RR parameter space ───────────────────────────────────────────
# Every runtime-gating parameter in resolution_rider.py must appear
# below. Audit done 2026-04-14 after the XRP/SOL losses:
#   min_contract_price, max_entry_price, min_seconds, max_seconds,
#   min_price_buffer_pct, max_adverse_momentum, momentum_window,
#   momentum_periods, max_realized_vol_pct — all searched.
#   kelly_fraction / max_bankroll_pct are intentionally NOT searched
#   (RR uses fixed STAKE_USD).
#
# Design note: max_entry_price is PINNED at 98. Prior runs collapsed
# cells to min==max (e.g. 96-96c, 97-97c) because the 7-day training
# set only had a handful of tradeable ticks per cell, and those ticks
# happened to land on specific cents. In live trading the book blows
# through those cents in 1-2 seconds, so a min==max cell catches
# almost nothing. We pin the top at 98 (the highest price where
# risk/reward still makes sense after fees — breakeven WR is 98.1%)
# and let the optimizer pick the floor only.

# vol_lookback is fixed at 300s for now; making it a search dimension
# would explode the candidate count without much benefit since a
# 5-min realized-vol window is the standard for crypto microstructure.
VOL_LOOKBACK = 300

# Cell price band: always 95..98 at the widest, but the optimizer can
# tighten the floor on a per-cell basis (e.g. "this cell only wins at
# 97c+"). Band is guaranteed to be at least 1 cent wide.
MAX_ENTRY_PRICE = 98


def sample_params() -> dict:
    p = {
        # Floor of the entry band. max is pinned (see design note above).
        "min_contract_price": random.randint(95, 97),
        "max_entry_price": MAX_ENTRY_PRICE,
        "min_seconds": random.choice([10, 15, 30, 45, 60]),
        "max_seconds": random.choice([60, 90, 120, 180, 240, 300, 360, 420, 480, 600]),
        "min_price_buffer_pct": round(random.uniform(0.05, 0.50), 3),
        "max_adverse_momentum": round(random.uniform(-0.10, 0.0), 4),
        "momentum_window": random.choice([30, 60, 90, 120, 180, 300]),
        "momentum_periods": random.randint(1, 10),
        # None = vol filter disabled. Values span calm (~0.02%) to
        # quite-volatile (~0.20%) per-tick stddev. A None choice gives
        # the optimizer the option to keep the filter off if it doesn't
        # add edge for that cell.
        "max_realized_vol_pct": random.choice(
            [None, 0.02, 0.04, 0.06, 0.08, 0.10, 0.15, 0.20]
        ),
        "vol_lookback": VOL_LOOKBACK,
    }
    if p["min_seconds"] >= p["max_seconds"]:
        p["min_seconds"] = 10
    return p


def grid_params() -> list[dict]:
    combos = []
    # max is pinned at 98 (see MAX_ENTRY_PRICE), only the floor varies
    for mcp in [95, 96, 97]:
        for mins in [10, 30, 60]:
            for ms in [120, 240, 360, 480, 600]:
                if mins >= ms:
                    continue
                for buf in [0.08, 0.12, 0.15, 0.20, 0.30]:
                    for mom in [-0.08, -0.05, -0.03, 0.0]:
                        for vol in [None, 0.05, 0.10]:
                            combos.append({
                                "min_contract_price": mcp,
                                "max_entry_price": MAX_ENTRY_PRICE,
                                "min_seconds": mins,
                                "max_seconds": ms,
                                "min_price_buffer_pct": buf,
                                "max_adverse_momentum": mom,
                                "momentum_window": 60,
                                "momentum_periods": 5,
                                "max_realized_vol_pct": vol,
                                "vol_lookback": VOL_LOOKBACK,
                            })
    return combos


# ── Data Helpers ──────────────────────────────────────────────────

def parse_strike(ticker: str) -> Optional[float]:
    m = re.search(r'-T([\d.]+)$', ticker)
    return float(m.group(1)) if m else None


def classify_ticker(ticker: str) -> tuple[str, str]:
    """Map a Kalshi ticker to (coin, market_type) for cell grouping.

    Kalshi's naming is inconsistent about what "D" means:
      - KXBTCD, KXETHD, KXSOLD, KXDOGED, KXXRPD, KXBNBD, KXHYPED
        are HOURLY markets that close on the hour (confirmed by
        live ticker close_time patterns like 04:00:00Z, 05:00:00Z, etc.)
      - KXSHIBAD is a true DAILY market that closes once per day
        at 5pm EDT (confirmed by user 2026-04-14)
    We treat SHIBA as a separate "daily" market_type so the cell
    name reflects reality (shiba_daily, not shiba_hourly) and so any
    future market-type-specific tuning is clean.

    Uses exact-match on the series component (everything before the
    first "-") to avoid prefix collisions like "KXSOLDATHOLD-..." being
    mis-classified as KXSOLD.
    """
    t = ticker.upper()
    series = t.split("-", 1)[0]  # "KXBTCD-26APR..." → "KXBTCD"
    if series == "KXSHIBAD":
        return "shiba", "daily"
    for coin in ["HYPE", "DOGE", "BTC", "ETH", "SOL", "XRP", "BNB"]:
        if series == f"KX{coin}15M":
            return coin.lower(), "15m"
        if series == f"KX{coin}D":
            return coin.lower(), "hourly"
    return "unknown", "unknown"


def get_price_at(prices: list[tuple[float, float]], ts: float) -> Optional[float]:
    if not prices:
        return None
    if ts <= prices[0][0]:
        return prices[0][1]
    if ts >= prices[-1][0]:
        return prices[-1][1]
    lo, hi = 0, len(prices) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if prices[mid][0] <= ts:
            lo = mid
        else:
            hi = mid - 1
    return prices[lo][1]


def compute_momentum(prices: list[tuple[float, float]], ts: float,
                     window: int = 60, periods: int = 5) -> Optional[float]:
    readings = []
    for i in range(periods):
        offset = i * window
        cur = get_price_at(prices, ts - offset)
        prev = get_price_at(prices, ts - offset - window)
        if cur and prev and prev > 0:
            readings.append(((cur - prev) / prev) * 100)
    return sum(readings) / len(readings) if readings else None


def compute_realized_vol(prices: list[tuple[float, float]], ts: float,
                         lookback: float = 300) -> Optional[float]:
    """Stddev of per-sample returns inside [ts - lookback, ts], in percent.

    Mirrors RTIFeed.volatility() so the optimizer's view of realized
    vol matches what the live bot sees at decision time.
    """
    if not prices:
        return None
    cutoff = ts - lookback
    # Binary-search left edge, then linear scan to ts (the lists are sorted)
    lo, hi = 0, len(prices)
    while lo < hi:
        mid = (lo + hi) // 2
        if prices[mid][0] < cutoff:
            lo = mid + 1
        else:
            hi = mid
    window = []
    for i in range(lo, len(prices)):
        if prices[i][0] > ts:
            break
        window.append(prices[i][1])
    if len(window) < 5:
        return None
    returns = []
    for i in range(1, len(window)):
        if window[i - 1] != 0:
            returns.append((window[i] - window[i - 1]) / window[i - 1])
    if len(returns) < 3:
        return None
    import statistics
    return statistics.stdev(returns) * 100


# ── Data Loading ──────────────────────────────────────────────────

def load_tick_windows(tick_dir: str) -> list[dict]:
    """Build market windows from tick CSVs (real-time bid/ask data).

    Files can be written with either space-separated or T-separated ISO
    timestamps depending on whoever wrote them; force-coerce both to a
    tz-aware datetime so the downstream sort doesn't blow up on mixed types.
    """
    print("  Loading tick data...")
    frames = []
    for f in sorted(Path(tick_dir).glob("*.csv")):
        print(f"    {f.name}...")
        df_one = pd.read_csv(f)
        df_one["timestamp"] = pd.to_datetime(df_one["timestamp"], utc=True, format="mixed")
        frames.append(df_one)
    if not frames:
        return []
    df = pd.concat(frames, ignore_index=True).sort_values("timestamp")

    windows = []
    for ticker, group in df.groupby("ticker"):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            continue

        group = group.sort_values("timestamp")
        rows = group.to_dict("records")
        if len(rows) < 2:
            continue

        # Settlement from final tick
        last_bid = rows[-1].get("yes_bid", 50)
        if last_bid >= 90:
            result = "yes"
        elif last_bid <= 10:
            result = "no"
        else:
            continue

        close_time = rows[-1]["timestamp"]
        if isinstance(close_time, str):
            close_time = pd.Timestamp(close_time)
        close_time += pd.Timedelta(seconds=5)

        windows.append({
            "ticker": ticker, "coin": coin, "market_type": mtype,
            "strike": parse_strike(ticker), "result": result,
            "close_time": close_time, "ticks": rows,
        })

    return windows


def load_hf_trade_windows(hf_dir: str, max_markets: int = 10000) -> list[dict]:
    """Build market windows from the HuggingFace TrevorJS/kalshi-trades dataset.

    Layout (16 trade shards + 4 markets shards in `hf_dir`):
      trades-NNNN.parquet: [trade_id, ticker, count, yes_price, no_price,
        taker_side, created_time] — 154.5M rows total across 16 shards.
      markets-NNNN.parquet: [ticker, event_ticker, ..., yes_bid, yes_ask,
        no_bid, no_ask, last_price, ..., result, created_time, open_time,
        close_time] — 17.5M rows total across 4 shards.

    Unlike the misterrPink1 parquet we had on disk, this one has meaningful
    15M crypto coverage (~1.4M KXBTC15M, 184K KXETH15M, 49K KXSOL15M trades
    through Jan 2026) which our older parquet completely lacked.

    We use DuckDB to do the filtering + markets→trades join in-SQL rather
    than loading 5+ GB into pandas. After DuckDB returns the subsampled
    joined rows (~10K markets × ~100 trades avg = ~1M rows, ~100MB), we
    materialize into the same window dict format `preprocess_window`
    expects so the rest of the pipeline is unchanged.

    Limitations:
      - Trade prints have no separate bid/ask. We use yes_price for both
        bid and ask, so mid == trade-price. This matches how our existing
        `load_parquet_trade_windows` treats the misterrPink1 data.
      - Buffer/momentum features require historical crypto spot, which we
        don't have for 2021-2025. preprocess_window sets those to None and
        simulate_fast silently skips the checks. Price/secs/side gates
        still apply, which is enough to find real edge for hourly cells.
    """
    import duckdb

    trades_glob = os.path.join(hf_dir, "trades-*.parquet")
    markets_glob = os.path.join(hf_dir, "markets-*.parquet")

    shard_files = list(Path(hf_dir).glob("trades-*.parquet"))
    if not shard_files:
        print(f"  HF: no trades parquet found in {hf_dir}")
        return []

    print(f"  Loading HF dataset from {hf_dir}...")
    print(f"    trade shards: {len(shard_files)}")

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB';")

    # Step 1: find resolved crypto markets that ALSO have trades.
    #
    # Key gotcha: the HF markets table has ~435K KXBTCD tickers (every
    # strike at every hour) but only ~58K of them ever actually traded.
    # If we sample from all resolved markets, only ~8% of our sample
    # lands on a ticker with trades — we get tiny samples. Instead we
    # inner-join against the set of tickers that HAVE trade prints,
    # then subsample from that.
    #
    # KXBTCVSGOLD and KXSOLNASD are cross-asset markets (BTC vs gold,
    # SOL vs Nasdaq) — excluded. We also use exact series-prefix
    # matching instead of LIKE 'KXBTC%' to avoid pulling in future
    # Kalshi series that happen to start the same way.
    crypto_series_sql = """
        (
          starts_with(ticker, 'KXBTC15M-') OR starts_with(ticker, 'KXBTCD-')
          OR starts_with(ticker, 'KXETH15M-') OR starts_with(ticker, 'KXETHD-')
          OR starts_with(ticker, 'KXSOL15M-') OR starts_with(ticker, 'KXSOLD-')
          OR starts_with(ticker, 'KXDOGE15M-') OR starts_with(ticker, 'KXDOGED-')
          OR starts_with(ticker, 'KXXRP15M-') OR starts_with(ticker, 'KXXRPD-')
          OR starts_with(ticker, 'KXBNB15M-') OR starts_with(ticker, 'KXBNBD-')
          OR starts_with(ticker, 'KXHYPE15M-') OR starts_with(ticker, 'KXHYPED-')
          OR starts_with(ticker, 'KXSHIBAD-')
        )
    """

    print(f"    discovering tickers with trades...")
    t0 = time.time()
    con.execute(f"""
        CREATE TEMP TABLE traded_tickers AS
        SELECT DISTINCT ticker
        FROM read_parquet('{trades_glob}')
        WHERE {crypto_series_sql}
    """)
    n_traded = con.execute("SELECT COUNT(*) FROM traded_tickers").fetchone()[0]
    print(f"    {n_traded:,} unique crypto tickers have trades "
          f"({time.time() - t0:.1f}s)")

    print(f"    joining against resolved markets + subsampling...")
    t0 = time.time()
    con.execute(f"""
        CREATE TEMP TABLE sampled_markets AS
        WITH resolved AS (
            SELECT DISTINCT m.ticker, m.close_time, m.result
            FROM read_parquet('{markets_glob}') m
            INNER JOIN traded_tickers t ON m.ticker = t.ticker
            WHERE m.result IN ('yes', 'no')
              AND m.close_time IS NOT NULL
        )
        SELECT * FROM resolved
        ORDER BY hash(ticker)
        LIMIT {max_markets}
    """)
    n_markets = con.execute("SELECT COUNT(*) FROM sampled_markets").fetchone()[0]
    print(f"    sampled {n_markets:,} traded+resolved markets "
          f"(of {n_traded:,} with trades, max={max_markets:,}) "
          f"in {time.time() - t0:.1f}s")

    # Step 2: pull trades for those markets only, joined with close_time/result
    print(f"    loading trades for sampled markets...")
    t0 = time.time()
    rows = con.execute(f"""
        SELECT
          t.ticker,
          t.created_time,
          t.yes_price,
          t.count,
          s.close_time,
          s.result
        FROM read_parquet('{trades_glob}') t
        INNER JOIN sampled_markets s ON t.ticker = s.ticker
        ORDER BY t.ticker, t.created_time
    """).df()
    print(f"    loaded {len(rows):,} trades in {time.time() - t0:.1f}s")

    # Step 3: build windows grouped by ticker
    print(f"    building windows...")
    t0 = time.time()
    windows = []
    skipped_unknown = 0
    skipped_empty = 0

    for ticker, group in rows.groupby("ticker", sort=False):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            skipped_unknown += 1
            continue

        close_time = group.iloc[0]["close_time"]
        result = group.iloc[0]["result"]
        if close_time is None or pd.isna(close_time) or result not in ("yes", "no"):
            skipped_empty += 1
            continue
        if isinstance(close_time, str):
            close_time = pd.Timestamp(close_time)

        ticks = [{
            "timestamp": row["created_time"],
            "yes_bid": int(row["yes_price"]),
            "yes_ask": int(row["yes_price"]),  # no spread in trade prints
            "volume": int(row["count"]) if pd.notna(row["count"]) else 0,
        } for _, row in group.iterrows()]

        windows.append({
            "ticker": ticker,
            "coin": coin,
            "market_type": mtype,
            "strike": parse_strike(ticker),
            "result": result,
            "close_time": close_time,
            "ticks": ticks,
        })

    print(f"    built {len(windows):,} windows in {time.time() - t0:.1f}s "
          f"(skipped: {skipped_unknown} unknown-series, "
          f"{skipped_empty} empty/invalid)")
    return windows


def load_parquet_trade_windows(
    trades_path: str,
    markets_path: str,
    max_markets: int = 30000,
) -> list[dict]:
    """Build market windows from the historical trades + markets parquets.

    Data layout (from misterrPink1/prediction-market-analysis dataset):
      trades_path: crypto_trades_filtered.parquet — 6.8M trade prints with
        columns [trade_id, ticker, count, yes_price, no_price, taker_side,
        created_time, _fetched_at, asset]
      markets_path: crypto_markets_filtered.parquet — 1.85M market snapshots
        with [ticker, result, close_time, ...]. Exactly one row per ticker
        (scraped once post-settlement), which is why we use it only for
        settlement + close_time lookup, not as a source of ticks.

    For each ticker, we build a window whose "ticks" are the trade prints:
      - timestamp = trade created_time
      - yes_bid = yes_ask = trade yes_price (no spread info from a trade print)
      - volume = trade count

    Since trade prints have no bid/ask spread, the preprocess_window logic
    that treats yes_mid = (bid + ask) / 2 still works — the mid just equals
    the execution price. That means every trade in the 95-99c range counts
    as a candidate "we could have been the taker at this price" entry.

    Limitations:
      - No crypto spot coverage for 2024-2025 in our data/prices/ → buffer
        and momentum checks skip silently for these windows (preprocess_window
        sets buffer_pct=None when spot data is absent).
      - Only daily-series tickers (KX*D) are present; no 15M data.
    """
    print("  Loading parquet trades...")
    trades = pd.read_parquet(trades_path)
    print(f"    trades: {len(trades):,} rows")

    print("  Loading parquet markets (for close_time + result)...")
    markets = pd.read_parquet(markets_path, columns=["ticker", "close_time", "result"])
    # Dedupe on ticker (one row per market, but just in case)
    markets = markets.drop_duplicates(subset=["ticker"], keep="last")
    print(f"    markets: {len(markets):,} unique tickers")

    # Join trades with market metadata
    merged = trades.merge(markets, on="ticker", how="inner")
    merged = merged[merged["result"].isin(["yes", "no"])]
    print(f"    after join + result filter: {len(merged):,} trades across "
          f"{merged['ticker'].nunique():,} markets")

    # Subsample if we have too many markets (keeps runtime manageable)
    tickers = merged["ticker"].unique()
    if len(tickers) > max_markets:
        rng = np.random.default_rng(42)
        tickers = rng.choice(tickers, max_markets, replace=False)
        merged = merged[merged["ticker"].isin(tickers)]
        print(f"    subsampled to {max_markets:,} markets")

    windows = []
    skipped_unknown = 0
    for ticker, group in merged.groupby("ticker"):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            skipped_unknown += 1
            continue

        group = group.sort_values("created_time")
        close_time = group.iloc[0]["close_time"]
        if close_time is None or pd.isna(close_time):
            continue
        if isinstance(close_time, str):
            close_time = pd.Timestamp(close_time)

        ticks = [{
            "timestamp": row["created_time"],
            "yes_bid": row["yes_price"],
            "yes_ask": row["yes_price"],  # trade prints have no spread; mid==price
            "volume": row["count"],
        } for _, row in group.iterrows()]

        windows.append({
            "ticker": ticker,
            "coin": coin,
            "market_type": mtype,
            "strike": parse_strike(ticker),
            "result": group.iloc[0]["result"],
            "close_time": close_time,
            "ticks": ticks,
        })
    print(f"    built {len(windows):,} windows "
          f"(skipped {skipped_unknown} unknown-series tickers)")
    return windows


def load_parquet_windows(path: str, max_markets: int = 30000) -> list[dict]:
    """Build market windows from historical parquet snapshots."""
    print("  Loading parquet...")
    df = pd.read_parquet(path)

    prefixes = ("KXBTCD", "KXETHD", "KXSOLD", "KXDOGED", "KXXRPD",
                "KXBNBD", "KXHYPED", "KXSHIBAD")
    mask = df["ticker"].str.startswith(prefixes) & df["result"].isin(["yes", "no"])
    df = df[mask].copy()
    print(f"  Filtered: {len(df):,} rows")

    tickers = df["ticker"].unique()
    if len(tickers) > max_markets:
        tickers = np.random.choice(tickers, max_markets, replace=False)
        df = df[df["ticker"].isin(tickers)]

    windows = []
    for ticker, group in df.groupby("ticker"):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            continue

        group = group.sort_values("_fetched_at")
        result = group.iloc[0]["result"]
        close_time = group.iloc[0].get("close_time")
        if close_time is not None:
            close_time = pd.Timestamp(close_time)

        ticks = [{
            "timestamp": row["_fetched_at"],
            "yes_bid": row.get("yes_bid", 50),
            "yes_ask": row.get("yes_ask", 50),
            "volume": row.get("volume", 0),
        } for _, row in group.iterrows()]

        windows.append({
            "ticker": ticker, "coin": coin, "market_type": mtype,
            "strike": parse_strike(ticker), "result": result,
            "close_time": close_time, "ticks": ticks,
        })

    return windows


SUPPORTED_COINS = ["btc", "eth", "sol", "doge", "xrp", "bnb", "hype"]


def load_crypto_prices(price_dir: str) -> dict[str, list[tuple[float, float]]]:
    """Load per-coin spot price history from data/prices/*.csv.

    Handles two schemas seamlessly:
      - legacy: timestamp,btc,eth,sol (files before the 7-coin rollout)
      - current: timestamp,btc,eth,sol,doge,xrp,bnb,hype

    Any coin column present in the row is picked up, so optimization for
    DOGE/XRP/BNB/HYPE works as soon as data has been collected for them.
    """
    prices = defaultdict(list)
    for f in sorted(Path(price_dir).glob("*.csv")):
        with open(f) as fh:
            for row in csv.DictReader(fh):
                try:
                    ts = datetime.fromisoformat(
                        row["timestamp"].replace("Z", "+00:00")).timestamp()
                except (ValueError, KeyError):
                    continue
                for coin in SUPPORTED_COINS:
                    val = row.get(coin)
                    if val in (None, ""):
                        continue
                    try:
                        prices[coin].append((ts, float(val)))
                    except ValueError:
                        pass
    for coin in prices:
        prices[coin].sort()
    return dict(prices)


# ── Simulation ────────────────────────────────────────────────────

def preprocess_window(window: dict, crypto_prices: dict) -> Optional[dict]:
    """
    Pre-extract all tradeable ticks from a window ONCE.
    Returns a compact dict with only the data the MC sweep needs.
    This avoids re-parsing timestamps and re-walking ticks for every candidate.
    """
    ticks = window["ticks"]
    result = window["result"]
    close_time = window["close_time"]
    strike = window.get("strike")
    coin = window["coin"]

    if close_time is None or not ticks:
        return None

    if isinstance(close_time, str):
        close_time = pd.Timestamp(close_time)
    if close_time.tzinfo is None:
        close_time = close_time.tz_localize("UTC")

    coin_prices = crypto_prices.get(coin, [])

    # Extract all potentially tradeable ticks (broadly: 94-99c, 10-500s)
    entries = []
    for tick in ticks:
        ts = tick["timestamp"]
        if isinstance(ts, str):
            ts = pd.Timestamp(ts)
        if hasattr(ts, 'tzinfo') and ts.tzinfo is None:
            ts = ts.tz_localize("UTC")

        secs_left = (close_time - ts).total_seconds()
        if secs_left < 10 or secs_left > 500:
            continue

        yes_bid = tick.get("yes_bid", 50)
        yes_ask = tick.get("yes_ask", 50)
        if not yes_bid or not yes_ask:
            continue

        yes_mid = (yes_bid + yes_ask) / 2
        no_mid = 100 - yes_mid

        if yes_mid >= 94:
            side, entry_price = "yes", yes_ask
        elif no_mid >= 94:
            side, entry_price = "no", 100 - yes_bid
        else:
            continue

        if entry_price < 94 or entry_price > 99:
            continue

        # Pre-compute buffer, momentum, and realized vol
        buffer_pct = None
        momentum = None
        realized_vol = None
        if strike and coin_prices:
            ts_epoch = ts.timestamp() if hasattr(ts, 'timestamp') else float(ts)
            cp = get_price_at(coin_prices, ts_epoch)
            if cp and strike > 0:
                buffer_pct = (cp - strike) / strike * 100
                # Pre-compute momentum for several windows
                momentum = {}
                for mw in [30, 60, 90, 120, 180, 300]:
                    for mp in [1, 3, 5, 7, 10]:
                        m = compute_momentum(coin_prices, ts_epoch, mw, mp)
                        if m is not None:
                            momentum[(mw, mp)] = m
                realized_vol = compute_realized_vol(coin_prices, ts_epoch, VOL_LOOKBACK)

        entries.append({
            "secs_left": secs_left,
            "side": side,
            "entry_price": entry_price,
            "fav_price": max(yes_mid, no_mid),
            "buffer_pct": buffer_pct,
            "momentum": momentum,
            "realized_vol": realized_vol,
        })

    if not entries:
        return None

    return {
        "result": result,
        "entries": entries,
    }


def simulate_fast(preprocessed: dict, params: dict) -> Optional[dict]:
    """Fast simulation against pre-processed entries."""
    min_cp = params["min_contract_price"]
    max_ep = params["max_entry_price"]
    min_secs = params.get("min_seconds", 10)
    max_secs = params["max_seconds"]
    min_buffer = params["min_price_buffer_pct"]
    max_mom = params["max_adverse_momentum"]
    max_vol = params.get("max_realized_vol_pct")
    mom_key = (params.get("momentum_window", 60), params.get("momentum_periods", 5))
    result = preprocessed["result"]

    for e in preprocessed["entries"]:
        if e["secs_left"] < min_secs or e["secs_left"] > max_secs:
            continue

        # Realized-volatility filter (None = disabled for this candidate).
        # Mirrors resolution_rider.evaluate exactly.
        if max_vol is not None and e["realized_vol"] is not None:
            if e["realized_vol"] > max_vol:
                continue
        if e["fav_price"] < min_cp:
            continue
        if e["entry_price"] < min_cp or e["entry_price"] > max_ep:
            continue

        # Buffer
        if e["buffer_pct"] is not None:
            if e["side"] == "yes" and e["buffer_pct"] < min_buffer:
                continue
            if e["side"] == "no" and e["buffer_pct"] > -min_buffer:
                continue

        # Momentum
        if max_mom < 0 and e["momentum"]:
            mom = e["momentum"].get(mom_key)
            if mom is not None:
                if e["side"] == "yes" and mom < max_mom:
                    continue
                if e["side"] == "no" and mom > -max_mom:
                    continue

        won = (e["side"] == result)
        contracts = max(1, int(10.0 / (e["entry_price"] / 100.0)))
        # Apply slippage penalty from empirical Kalshi fill data
        slippage_per_contract = get_slippage_cents(int(e["entry_price"])) / 100.0
        slippage_cost = contracts * slippage_per_contract
        if won:
            profit = contracts * (100 - e["entry_price"]) / 100.0 - slippage_cost
        else:
            profit = -contracts * e["entry_price"] / 100.0 - slippage_cost
        return {"won": won, "profit": profit}

    return None


def evaluate_params(preprocessed_windows: list[dict], params: dict) -> dict:
    wins = losses = 0
    total_profit = 0.0
    for pw in preprocessed_windows:
        r = simulate_fast(pw, params)
        if r is None:
            continue
        if r["won"]:
            wins += 1
        else:
            losses += 1
        total_profit += r["profit"]
    trades = wins + losses
    if trades == 0:
        return {"score": 0, "win_rate": 0, "trades": 0, "profit": 0, "wins": 0, "losses": 0}
    wr = wins / trades
    ppt = total_profit / trades
    norm_p = max(0, min(1, (ppt + 10) / 10.5))
    return {
        "score": round(ALPHA * wr + (1 - ALPHA) * norm_p, 6),
        "win_rate": round(wr, 4),
        "trades": trades, "profit": round(total_profit, 2),
        "wins": wins, "losses": losses,
    }


# ── Worker-pool machinery for parallelized candidate scoring ─────
#
# The candidate loop is embarrassingly parallel — each of the 5,700+
# candidates evaluates independently against the same preprocessed
# windows. We use multiprocessing.Pool with per-worker initializer to
# copy the (potentially MB-sized) preprocessed data ONCE per worker
# rather than pickling it per task. This gives ~4-8× speedup on a
# typical 8-core machine with zero behavioral change vs the single-
# threaded path.
_WORKER_PP_BY_BUCKET: list = []
_WORKER_TRAIN_SETS: list = []


def _cv_worker_init(pp_by_bucket, train_sets):
    """Pool initializer — stash the preprocessed data in module globals
    so each worker doesn't have to pickle/unpickle it for every task."""
    global _WORKER_PP_BY_BUCKET, _WORKER_TRAIN_SETS
    _WORKER_PP_BY_BUCKET = pp_by_bucket
    _WORKER_TRAIN_SETS = train_sets


def _cv_score_candidate(params: dict):
    """Evaluate one candidate across all CV buckets. Returns the tuple
    the main loop uses for ranking, or None if it doesn't meet the
    minimum-trade threshold.
    """
    fold_results = []
    for b in range(len(_WORKER_TRAIN_SETS)):
        train_r = evaluate_params(_WORKER_TRAIN_SETS[b], params)
        val_r = evaluate_params(_WORKER_PP_BY_BUCKET[b], params)
        fold_results.append({
            "train": train_r, "val": val_r, "val_date": f"bucket_{b}",
        })

    total_val_trades = sum(f["val"]["trades"] for f in fold_results)
    if total_val_trades < 2:
        return None

    total_val_wins = sum(f["val"]["wins"] for f in fold_results)
    total_val_losses = sum(f["val"]["losses"] for f in fold_results)
    total_train_trades = sum(f["train"]["trades"] for f in fold_results)
    val_wr = total_val_wins / total_val_trades if total_val_trades > 0 else 0

    train_wr_num = sum(
        f["train"]["win_rate"] * f["train"]["trades"]
        for f in fold_results if f["train"]["trades"] > 0
    )
    train_total = sum(f["train"]["trades"] for f in fold_results)
    train_wr = train_wr_num / train_total if train_total > 0 else 0

    total_val_profit = sum(f["val"]["profit"] for f in fold_results)
    score = total_val_profit

    return (
        score, val_wr, total_val_losses, total_val_trades,
        train_wr, total_train_trades, params, fold_results,
    )


# ── Optimization ──────────────────────────────────────────────────

def optimize_cell(name: str, train: list[dict], val: list[dict],
                  crypto_prices: dict, n_random: int = 3000) -> Optional[dict]:
    if not train:
        print(f"  {name}: no training data, skipping")
        return None
    print(f"  {name}: {len(train)} train, {len(val)} validation windows")

    # Pre-process: extract tradeable ticks ONCE (the expensive step)
    print(f"    Pre-processing training windows...")
    train_pp = [pw for w in train if (pw := preprocess_window(w, crypto_prices)) is not None]
    print(f"    {len(train_pp)} windows have tradeable ticks (95c+)")
    if not train_pp:
        print(f"  {name}: no tradeable ticks in training data")
        return None

    val_pp = [pw for w in val if (pw := preprocess_window(w, crypto_prices)) is not None]
    print(f"    {len(val_pp)} validation windows have tradeable ticks")

    candidates = grid_params()
    for _ in range(n_random):
        candidates.append(sample_params())

    # Phase 1: Training (fast — uses pre-processed data)
    scored = []
    for i, p in enumerate(candidates):
        if i > 0 and i % 1000 == 0:
            print(f"    {i}/{len(candidates)}...")
        r = evaluate_params(train_pp, p)
        if r["trades"] >= 3:
            scored.append((r["score"], r, p))
    scored.sort(key=lambda x: (-x[0], -x[1]["trades"]))

    if not scored:
        print(f"  {name}: no viable candidates")
        return None

    print(f"    {len(scored)} viable candidates (of {len(candidates)} tested)")

    # Phase 2: Walk-forward validation
    top_n = min(50, len(scored))
    print(f"    Validating top {top_n} on {len(val_pp)} windows...")

    best = None
    for _, train_r, p in scored[:top_n]:
        if not val_pp:
            best = {**p, "training_win_rate": train_r["win_rate"],
                    "training_trades": train_r["trades"],
                    "training_profit": train_r["profit"],
                    "validation_win_rate": None, "validation_trades": 0}
            break
        vr = evaluate_params(val_pp, p)
        if vr["trades"] == 0:
            continue
        if vr["losses"] == 0 and (best is None or vr["trades"] > best.get("validation_trades", 0)):
            best = {**p, "training_win_rate": train_r["win_rate"],
                    "training_trades": train_r["trades"],
                    "training_profit": train_r["profit"],
                    "validation_win_rate": vr["win_rate"],
                    "validation_trades": vr["trades"],
                    "validation_profit": vr["profit"]}

    # Fallback: best training score even if validation has losses
    if best is None and scored:
        _, train_r, p = scored[0]
        vr = evaluate_params(val_pp, p) if val_pp else {}
        best = {**p, "training_win_rate": train_r["win_rate"],
                "training_trades": train_r["trades"],
                "training_profit": train_r["profit"],
                "validation_win_rate": vr.get("win_rate"),
                "validation_trades": vr.get("trades", 0),
                "validation_profit": vr.get("profit", 0)}

    if best:
        print(f"    BEST: train WR={best['training_win_rate']:.1%} ({best['training_trades']}t), "
              f"val WR={best.get('validation_win_rate', 'N/A')} ({best.get('validation_trades', 0)}t)")
        print(f"    buffer={best['min_price_buffer_pct']}%, max_secs={best['max_seconds']}, "
              f"price={best['min_contract_price']}-{best['max_entry_price']}c, "
              f"momentum={best['max_adverse_momentum']}")
    return best


def main():
    t0 = time.time()
    print("=" * 60)
    print("Resolution Rider Parameter Optimizer")
    print("=" * 60)

    print("\n[1/4] Loading data...")

    # Use tick data for everything — parquet snapshots are too sparse for RR simulation
    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")

    # Load slippage model from Kalshi fill data
    global SLIPPAGE_MODEL
    kalshi_csv = os.path.join(data_dir, "from_kalshi", "Kalshi-Transactions-2026.csv")
    SLIPPAGE_MODEL = load_slippage_model(kalshi_csv)
    if SLIPPAGE_MODEL:
        print("  Slippage model (cents/contract):")
        for price, slip in sorted(SLIPPAGE_MODEL.items()):
            print(f"    {price}c entry -> {slip:.2f}c slippage")
    else:
        print("  No slippage model loaded, using 0.5c default")
    tick_dir = os.path.join(data_dir, "ticks")
    all_tick_windows = load_tick_windows(tick_dir) if Path(tick_dir).exists() else []
    print(f"  Ticks: {len(all_tick_windows)} market windows from live recorder")

    # Load historical training data from disk. Preference order:
    #   1. HuggingFace TrevorJS/kalshi-trades (superset: 15M coverage + newer dates)
    #   2. misterrPink1/prediction-market-analysis parquet (legacy, hourly only)
    # Either contributes "historical" windows that get combined with the live
    # ticks. Control knobs:
    #   USE_HF=1/0           — prefer HF over legacy parquet (default 1)
    #   INCLUDE_HISTORICAL=0 — disable historical entirely, live-only run
    #   HF_DIR               — override HF dataset path
    #   PARQUET_MAX_MARKETS  — subsample cap for both HF and legacy loaders
    historical_windows: list = []
    if os.getenv("INCLUDE_HISTORICAL", "1") != "0":
        max_markets = int(os.getenv("PARQUET_MAX_MARKETS", "10000"))
        hf_dir = os.getenv("HF_DIR", "/mnt/d/datasets/hf_kalshi_trades")
        use_hf = os.getenv("USE_HF", "1") != "0"

        if use_hf and Path(hf_dir).exists() and any(Path(hf_dir).glob("trades-*.parquet")):
            try:
                historical_windows = load_hf_trade_windows(
                    hf_dir, max_markets=max_markets,
                )
                print(f"  HF: {len(historical_windows)} historical market windows "
                      f"from {hf_dir}")
            except Exception as e:
                print(f"  HF load FAILED: {e}")
                import traceback
                traceback.print_exc()
        else:
            # Fall back to misterrPink1 parquet (daily-only, older)
            trades_parquet = os.path.join(data_dir, "crypto_trades_filtered.parquet")
            markets_parquet = os.path.join(data_dir, "crypto_markets_filtered.parquet")
            if Path(trades_parquet).exists() and Path(markets_parquet).exists():
                try:
                    historical_windows = load_parquet_trade_windows(
                        trades_parquet, markets_parquet, max_markets=max_markets,
                    )
                    print(f"  Legacy parquet: {len(historical_windows)} windows")
                except Exception as e:
                    print(f"  Legacy parquet load FAILED: {e}")
            else:
                print(f"  No historical dataset found (looked at {hf_dir} and {trades_parquet})")

    all_windows = all_tick_windows + historical_windows
    print(f"  TOTAL: {len(all_windows)} market windows "
          f"({len(all_tick_windows)} live + {len(historical_windows)} historical)")

    # Use local data/prices if available, fall back to other paths
    price_candidates = [
        "data/prices",
        "/home/jake/workspaces/kalshi-trading-bot/python-bot/data/prices",
    ]
    price_dir = next((p for p in price_candidates if Path(p).exists()), "data/prices")
    crypto_prices = load_crypto_prices(price_dir)
    for coin, p in crypto_prices.items():
        print(f"  {coin.upper()}: {len(p)} price points")

    # Group windows by cell for k-fold
    print("\n[2/4] Organizing by cell and date...")
    windows_by_cell = defaultdict(list)
    for w in all_windows:
        cell = f"{w['coin']}_{w['market_type']}"
        windows_by_cell[cell].append(w)

    all_cells = sorted(windows_by_cell.keys())
    print(f"  {len(all_cells)} cells: {', '.join(all_cells)}")

    # Get unique dates for k-fold (use the combined window set)
    all_dates = sorted(set(
        w["close_time"].strftime("%Y-%m-%d") if hasattr(w["close_time"], "strftime")
        else str(w["close_time"])[:10]
        for w in all_windows
    ))
    print(f"  {len(all_dates)} distinct dates (showing first/last 3): "
          f"{', '.join(all_dates[:3])} ... {', '.join(all_dates[-3:])}")

    print("\n[3/4] Optimizing (leave-one-day-out cross-validation)...")
    results = {}
    for cell in all_cells:
        print(f"\n--- {cell} ---")
        cell_windows = windows_by_cell[cell]

        # Tag each window with its date
        dated = []
        for w in cell_windows:
            d = (w["close_time"].strftime("%Y-%m-%d") if hasattr(w["close_time"], "strftime")
                 else str(w["close_time"])[:10])
            dated.append((d, w))

        # Pre-process ALL windows once
        print(f"  Pre-processing {len(dated)} windows...")
        preprocessed = []
        for d, w in dated:
            pp = preprocess_window(w, crypto_prices)
            if pp is not None:
                pp["_date"] = d
                preprocessed.append(pp)
        print(f"  {len(preprocessed)} have tradeable ticks")

        if len(preprocessed) < 3:
            print(f"  {cell}: not enough data, skipping")
            continue

        # Generate candidates
        candidates = grid_params()
        for _ in range(3000):
            candidates.append(sample_params())

        # Bucketed CV: group unique dates into at most MAX_FOLDS buckets, then
        # leave-one-bucket-out. With 7 days of live data this still gives us
        # 7 folds (one per day). With 300+ days of historical data we cap at
        # MAX_FOLDS to keep runtime bounded (otherwise CV = 300+ × candidates,
        # which runs for hours on btc_hourly). Each bucket has roughly equal
        # window counts so the folds are balanced.
        MAX_FOLDS = int(os.getenv("MAX_CV_FOLDS", "10"))
        unique_dates = sorted(set(pp["_date"] for pp in preprocessed))
        if len(unique_dates) < 2:
            print(f"  {cell}: only 1 date, can't cross-validate")
            # Use all data as training, no validation
            scored = []
            for i, p in enumerate(candidates):
                if i > 0 and i % 1000 == 0:
                    print(f"    {i}/{len(candidates)}...")
                r = evaluate_params(preprocessed, p)
                if r["trades"] >= 3:
                    scored.append((r["score"], r, p))
            if scored:
                scored.sort(key=lambda x: (-x[0], -x[1]["trades"]))
                _, best_r, best_p = scored[0]
                results[cell] = {
                    **best_p,
                    "training_win_rate": best_r["win_rate"],
                    "training_trades": best_r["trades"],
                    "training_profit": best_r["profit"],
                    "cv_folds": 0,
                    "cv_mean_win_rate": None,
                    "cv_total_val_trades": 0,
                }
                print(f"    BEST (no CV): WR={best_r['win_rate']:.1%} ({best_r['trades']}t)")
            continue

        # Assign each date to a bucket 0..MAX_FOLDS-1 by position
        n_buckets = min(MAX_FOLDS, len(unique_dates))
        date_to_bucket = {}
        for idx, d in enumerate(unique_dates):
            bucket = (idx * n_buckets) // len(unique_dates)
            date_to_bucket[d] = bucket
        # Pre-partition preprocessed windows by bucket ONCE, not per candidate
        pp_by_bucket: list[list[dict]] = [[] for _ in range(n_buckets)]
        for pp in preprocessed:
            pp_by_bucket[date_to_bucket[pp["_date"]]].append(pp)

        print(f"  Running {n_buckets}-fold CV ({len(unique_dates)} dates, "
              f"{[len(b) for b in pp_by_bucket]} windows/bucket)...")

        # Pre-compute per-bucket "not this bucket" slices for training
        # (list comprehension per candidate was O(candidates * windows * folds))
        train_sets = []
        for b in range(n_buckets):
            train_sets.append([pp for i, pp in enumerate(preprocessed)
                               if date_to_bucket[pp["_date"]] != b])

        # Parallel candidate scoring across CPU cores. Each worker gets
        # one pickled copy of pp_by_bucket + train_sets at startup (via
        # the initializer), then just receives lightweight params dicts
        # per task. Chunksize=50 balances IPC overhead against worker
        # starvation; on 5700 candidates × 10 folds, this gives ~4-8×
        # speedup over the serial loop.
        n_workers = max(1, (os.cpu_count() or 4) - 1)
        chunksize = max(10, len(candidates) // (n_workers * 20))
        print(f"    parallelizing {len(candidates)} candidates across {n_workers} workers "
              f"(chunksize={chunksize})...")

        candidate_scores = []
        t_cv_start = time.time()
        with multiprocessing.Pool(
            n_workers,
            initializer=_cv_worker_init,
            initargs=(pp_by_bucket, train_sets),
        ) as pool:
            for i, result in enumerate(pool.imap_unordered(
                _cv_score_candidate, candidates, chunksize=chunksize,
            )):
                if result is not None:
                    candidate_scores.append(result)
                if (i + 1) % 1000 == 0:
                    elapsed = time.time() - t_cv_start
                    rate = (i + 1) / elapsed
                    eta = (len(candidates) - (i + 1)) / rate if rate > 0 else 0
                    print(f"    {i + 1}/{len(candidates)}  ({rate:.0f}/s, ETA {eta:.0f}s)")
        print(f"    CV done in {time.time() - t_cv_start:.0f}s "
              f"({len(candidate_scores)} viable candidates)")

        if not candidate_scores:
            print(f"  {cell}: no candidates with validation trades")
            continue

        # Sort: by total validation profit (highest first), then trade count
        candidate_scores.sort(key=lambda x: (-x[0], -x[3]))

        best = candidate_scores[0]
        score, val_wr, val_losses, val_trades, train_wr, train_trades, p, folds = best

        val_profit = sum(f["val"]["profit"] for f in folds)
        train_profit = sum(f["train"]["profit"] for f in folds)

        results[cell] = {
            **p,
            "training_win_rate": round(train_wr, 4),
            "training_trades": train_trades,
            "training_profit": round(train_profit, 2),
            "cv_folds": n_buckets,
            "cv_mean_win_rate": round(val_wr, 4),
            "cv_total_val_trades": val_trades,
            "cv_val_losses": val_losses,
            "cv_val_profit": round(val_profit, 2),
        }

        vol_str = (f"vol≤{p['max_realized_vol_pct']}%" if p.get("max_realized_vol_pct") is not None
                   else "vol=off")
        print(f"    BEST: CV val WR={val_wr:.1%} ({val_trades}t, {val_losses}L), "
              f"train WR={train_wr:.1%} ({train_trades}t)")
        print(f"    buffer={p['min_price_buffer_pct']}%, "
              f"secs={p.get('min_seconds', 10)}-{p['max_seconds']}, "
              f"price={p['min_contract_price']}-{p['max_entry_price']}c, "
              f"momentum={p['max_adverse_momentum']} (w={p.get('momentum_window', 60)}s, "
              f"p={p.get('momentum_periods', 5)}), {vol_str}")

    print("\n[4/4] Saving...")
    out = Path("data/rr_params.json")
    out.parent.mkdir(parents=True, exist_ok=True)

    # Back up previous params with timestamp
    if out.exists():
        from datetime import datetime
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup = out.parent / f"rr_params_{ts}.json"
        import shutil
        shutil.copy2(out, backup)
        print(f"  Backed up previous params to {backup}")

    with open(out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  Saved to {out}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"{'=' * 60}")
    for cell, r in sorted(results.items()):
        print(f"  {cell:15s}: train={r['training_win_rate']:.0%} ({r['training_trades']}t) "
              f"val={r.get('validation_win_rate', 'N/A')} ({r.get('validation_trades', 0)}t) "
              f"buf={r['min_price_buffer_pct']}% max_s={r['max_seconds']}")


if __name__ == "__main__":
    main()
