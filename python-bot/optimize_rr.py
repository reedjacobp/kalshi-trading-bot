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


def sample_params() -> dict:
    p = {
        "min_contract_price": random.randint(95, 98),
        "max_entry_price": random.randint(96, 98),
        "max_seconds": random.choice([60, 120, 180, 240, 300, 360, 420, 480]),
        "min_price_buffer_pct": round(random.uniform(0.05, 0.50), 3),
        "max_adverse_momentum": round(random.uniform(-0.10, 0.0), 4),
        "momentum_window": random.choice([30, 60, 90, 120, 180, 300]),
        "momentum_periods": random.randint(1, 10),
    }
    if p["min_contract_price"] > p["max_entry_price"]:
        p["min_contract_price"], p["max_entry_price"] = p["max_entry_price"], p["min_contract_price"]
    return p


def grid_params() -> list[dict]:
    combos = []
    for mcp in [95, 96, 97]:
        for mep in [97, 98]:
            if mcp > mep:
                continue
            for ms in [120, 240, 360, 480]:
                for buf in [0.08, 0.12, 0.15, 0.20, 0.30]:
                    for mom in [-0.08, -0.05, -0.03, 0.0]:
                        combos.append({
                            "min_contract_price": mcp,
                            "max_entry_price": mep,
                            "max_seconds": ms,
                            "min_price_buffer_pct": buf,
                            "max_adverse_momentum": mom,
                            "momentum_window": 60,
                            "momentum_periods": 5,
                        })
    return combos


# ── Data Helpers ──────────────────────────────────────────────────

def parse_strike(ticker: str) -> Optional[float]:
    m = re.search(r'-T([\d.]+)$', ticker)
    return float(m.group(1)) if m else None


def classify_ticker(ticker: str) -> tuple[str, str]:
    t = ticker.upper()
    for coin in ["HYPE", "DOGE", "SHIBA", "BTC", "ETH", "SOL", "XRP", "BNB"]:
        if f"KX{coin}15M" in t:
            return coin.lower(), "15m"
        if f"KX{coin}D" in t:
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


# ── Data Loading ──────────────────────────────────────────────────

def load_tick_windows(tick_dir: str) -> list[dict]:
    """Build market windows from tick CSVs (real-time bid/ask data)."""
    print("  Loading tick data...")
    frames = []
    for f in sorted(Path(tick_dir).glob("*.csv")):
        print(f"    {f.name}...")
        frames.append(pd.read_csv(f, parse_dates=["timestamp"]))
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


def load_crypto_prices(price_dir: str) -> dict[str, list[tuple[float, float]]]:
    prices = defaultdict(list)
    for f in sorted(Path(price_dir).glob("*.csv")):
        with open(f) as fh:
            for row in csv.DictReader(fh):
                try:
                    ts = datetime.fromisoformat(
                        row["timestamp"].replace("Z", "+00:00")).timestamp()
                except (ValueError, KeyError):
                    continue
                for coin in ["btc", "eth", "sol"]:
                    val = row.get(coin)
                    if val:
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

        # Pre-compute buffer and momentum
        buffer_pct = None
        momentum = None
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

        entries.append({
            "secs_left": secs_left,
            "side": side,
            "entry_price": entry_price,
            "fav_price": max(yes_mid, no_mid),
            "buffer_pct": buffer_pct,
            "momentum": momentum,
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
    max_secs = params["max_seconds"]
    min_buffer = params["min_price_buffer_pct"]
    max_mom = params["max_adverse_momentum"]
    mom_key = (params.get("momentum_window", 60), params.get("momentum_periods", 5))
    result = preprocessed["result"]

    for e in preprocessed["entries"]:
        if e["secs_left"] > max_secs:
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
    print(f"  Ticks: {len(all_tick_windows)} market windows")

    # Use local data/prices if available, fall back to other paths
    price_candidates = [
        "data/prices",
        "/home/jake/workspaces/kalshi-trading-bot/python-bot/data/prices",
    ]
    price_dir = next((p for p in price_candidates if Path(p).exists()), "data/prices")
    crypto_prices = load_crypto_prices(price_dir)
    for coin, p in crypto_prices.items():
        print(f"  {coin.upper()}: {len(p)} price points")

    # Split tick windows by date: Apr 8-9 train, Apr 10-11 validate
    # Group windows by cell and by date for k-fold
    print("\n[2/4] Organizing by cell and date...")
    windows_by_cell = defaultdict(list)
    for w in all_tick_windows:
        cell = f"{w['coin']}_{w['market_type']}"
        windows_by_cell[cell].append(w)

    all_cells = sorted(windows_by_cell.keys())
    print(f"  {len(all_cells)} cells: {', '.join(all_cells)}")

    # Get unique dates for k-fold
    all_dates = sorted(set(
        w["close_time"].strftime("%Y-%m-%d") if hasattr(w["close_time"], "strftime")
        else str(w["close_time"])[:10]
        for w in all_tick_windows
    ))
    print(f"  {len(all_dates)} dates for k-fold: {', '.join(all_dates)}")

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

        # Leave-one-day-out: for each fold, train on N-1 days, validate on 1
        fold_dates = sorted(set(pp["_date"] for pp in preprocessed))
        if len(fold_dates) < 2:
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

        print(f"  Running {len(fold_dates)}-fold CV ({', '.join(fold_dates)})...")

        # Score each candidate across all folds
        candidate_scores = []
        for i, p in enumerate(candidates):
            if i > 0 and i % 1000 == 0:
                print(f"    {i}/{len(candidates)}...")

            fold_results = []
            for val_date in fold_dates:
                train_pp = [pp for pp in preprocessed if pp["_date"] != val_date]
                val_pp = [pp for pp in preprocessed if pp["_date"] == val_date]
                train_r = evaluate_params(train_pp, p)
                val_r = evaluate_params(val_pp, p)
                fold_results.append({
                    "train": train_r, "val": val_r, "val_date": val_date,
                })

            # Aggregate: mean validation win rate, total val trades, any val losses
            total_val_trades = sum(f["val"]["trades"] for f in fold_results)
            total_val_wins = sum(f["val"]["wins"] for f in fold_results)
            total_val_losses = sum(f["val"]["losses"] for f in fold_results)
            total_train_trades = sum(f["train"]["trades"] for f in fold_results)

            if total_val_trades < 2:
                continue

            val_wr = total_val_wins / total_val_trades if total_val_trades > 0 else 0
            train_wr = sum(f["train"]["win_rate"] * f["train"]["trades"]
                          for f in fold_results if f["train"]["trades"] > 0)
            train_total = sum(f["train"]["trades"] for f in fold_results)
            train_wr = train_wr / train_total if train_total > 0 else 0

            # Score: total profit across all validation folds
            # This naturally rewards both high WR AND high volume
            total_val_profit = sum(f["val"]["profit"] for f in fold_results)
            score = total_val_profit

            candidate_scores.append((
                score, val_wr, total_val_losses, total_val_trades,
                train_wr, total_train_trades, p, fold_results,
            ))

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
            "cv_folds": len(fold_dates),
            "cv_mean_win_rate": round(val_wr, 4),
            "cv_total_val_trades": val_trades,
            "cv_val_losses": val_losses,
            "cv_val_profit": round(val_profit, 2),
        }

        print(f"    BEST: CV val WR={val_wr:.1%} ({val_trades}t, {val_losses}L), "
              f"train WR={train_wr:.1%} ({train_trades}t)")
        print(f"    buffer={p['min_price_buffer_pct']}%, max_secs={p['max_seconds']}, "
              f"price={p['min_contract_price']}-{p['max_entry_price']}c, "
              f"momentum={p['max_adverse_momentum']}")

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
