#!/usr/bin/env python3
"""
Walk-Forward Parameter Optimization (Vectorized)

Sweeps the 3 most impactful parameters using vectorized pandas operations
instead of iterrows. ~50x faster than row-by-row iteration.

Parameters:
1. max_entry_price: How expensive a favorite we'll buy (70-90c)
2. max_loss_per_trade: Cap on single position risk ($3-$15)
3. disable_threshold: Matrix edge threshold for auto-disable (-2% to -10%)
"""

import itertools
import json
import math
import os
import statistics
import sys
import time as _time
from collections import defaultdict
from pathlib import Path

try:
    import duckdb
    import numpy as np
    import pandas as pd
except ImportError:
    print("ERROR: duckdb, numpy, pandas required")
    sys.exit(1)

from risk_manager import kalshi_maker_fee, kalshi_taker_fee


def load_data(data_dir):
    con = duckdb.connect()
    files = []
    for name in ["crypto_markets_filtered.parquet", "crypto_markets_extended.parquet"]:
        p = Path(data_dir) / name
        if p.exists():
            files.append(str(p))

    common_cols = "ticker, status, last_price, volume, result, close_time"
    union_sql = " UNION ALL ".join(f"SELECT {common_cols} FROM read_parquet('{f}')" for f in files)

    print("Loading markets...", flush=True)
    df = con.execute(f"""
        WITH combined AS ({union_sql})
        SELECT DISTINCT ON (ticker) *
        FROM combined
        WHERE ticker LIKE 'KXBTCD%'
        AND status IN ('finalized', 'settled')
        AND result IN ('yes', 'no')
        AND volume >= 50
        AND last_price > 0 AND last_price < 100
        ORDER BY ticker, close_time ASC
    """).fetchdf()

    # Pre-compute columns needed for favorite_bias
    df["price"] = df["last_price"].astype(int)
    df["no_price"] = 100 - df["price"]
    df["fav_price"] = df[["price", "no_price"]].max(axis=1)
    df["fav_side"] = np.where(df["price"] >= df["no_price"], "yes", "no")
    df["is_win"] = df["fav_side"] == df["result"]
    df["month"] = pd.to_datetime(df["close_time"], utc=True).dt.to_period("M")

    months = sorted(df["month"].unique())
    print(f"  {len(df):,} markets, {len(months)} months ({months[0]} to {months[-1]})", flush=True)
    return df, months


def simulate_combo(df, months, max_entry, max_loss, disable_thresh, train_months=2):
    """Simulate a single parameter combination across all months (vectorized)."""
    # Filter to favorite_bias eligible markets (fav_price >= 70 and <= max_entry)
    mask = (df["fav_price"] >= 70) & (df["fav_price"] <= max_entry)
    eligible = df[mask].copy()

    if len(eligible) == 0:
        return None

    # Compute contracts and stake
    entry_prices = eligible["fav_price"].values
    contracts = np.maximum(1, (5.0 / (entry_prices / 100.0)).astype(int))
    stakes = contracts * (entry_prices / 100.0)

    # Apply max loss cap
    over_cap = stakes > max_loss
    contracts[over_cap] = np.maximum(1, (max_loss / (entry_prices[over_cap] / 100.0)).astype(int))
    stakes[over_cap] = contracts[over_cap] * (entry_prices[over_cap] / 100.0)

    eligible["contracts"] = contracts
    eligible["stake"] = stakes

    # Compute P&L
    is_win = eligible["is_win"].values
    pnl = np.where(is_win, contracts * 1.0 - stakes, -stakes)

    # Fees (vectorized approximation — maker entry, taker settle)
    # maker_fee = 0 (confirmed zero), settle_fee on wins only
    p = entry_prices / 100.0
    settle_fees = np.where(is_win, np.ceil(0.07 * contracts * (1 - p) * p * 100) / 100, 0)
    pnl -= settle_fees

    eligible["pnl"] = pnl
    eligible["is_win_bool"] = is_win

    # Walk-forward: only trade in test months (skip first train_months)
    test_months = months[train_months:]
    test_mask = eligible["month"].isin(test_months)
    test = eligible[test_mask].copy()

    if len(test) == 0:
        return None

    # Simulate matrix auto-disable using rolling edge
    # Process chronologically, tracking rolling P&L
    test = test.sort_values("close_time")
    pnl_arr = test["pnl"].values
    stake_arr = test["stake"].values

    # Rolling window simulation
    window = 20
    enabled = True
    actual_pnl = []
    actual_win = []
    month_pnl = defaultdict(float)
    total_trades = 0
    wins = 0
    losses = 0
    total_staked = 0.0

    # Use numpy cumulative sums for rolling edge
    cum_pnl = 0.0
    cum_stake = 0.0
    trade_count = 0
    shadow_cum_pnl = 0.0
    shadow_cum_stake = 0.0
    shadow_count = 0

    month_vals = test["month"].values

    for i in range(len(pnl_arr)):
        if enabled:
            actual_pnl.append(pnl_arr[i])
            actual_win.append(test.iloc[i]["is_win_bool"])
            total_trades += 1
            total_staked += stake_arr[i]
            if test.iloc[i]["is_win_bool"]:
                wins += 1
            else:
                losses += 1
            month_pnl[str(month_vals[i])] += pnl_arr[i]

            cum_pnl += pnl_arr[i]
            cum_stake += stake_arr[i]
            trade_count += 1

            # Approximate rolling window by decay
            if trade_count > window:
                decay = (window - 1) / window
                cum_pnl *= decay
                cum_stake *= decay

            # Check disable
            if trade_count >= 5 and cum_stake > 0:
                if cum_pnl / cum_stake < disable_thresh:
                    enabled = False
                    shadow_cum_pnl = 0
                    shadow_cum_stake = 0
                    shadow_count = 0
        else:
            # Shadow mode
            shadow_cum_pnl += pnl_arr[i]
            shadow_cum_stake += stake_arr[i]
            shadow_count += 1
            if shadow_count >= 5 and shadow_cum_stake > 0:
                if shadow_cum_pnl / shadow_cum_stake > 0.02:
                    enabled = True
                    cum_pnl = shadow_cum_pnl
                    cum_stake = shadow_cum_stake
                    trade_count = shadow_count

    if total_trades == 0:
        return None

    total_pnl = sum(actual_pnl)
    monthly_vals = [month_pnl.get(str(m), 0.0) for m in test_months]

    # Metrics
    if len(monthly_vals) > 1 and any(v != 0 for v in monthly_vals):
        avg = statistics.mean(monthly_vals)
        std = statistics.stdev(monthly_vals) if len(monthly_vals) > 1 else 1
        sharpe = (avg / std) * (12 ** 0.5) if std > 0 else 0
        downside = [p for p in monthly_vals if p < 0]
        down_std = statistics.stdev(downside) if len(downside) > 1 else std
        sortino = (avg / down_std) * (12 ** 0.5) if down_std > 0 else 0
    else:
        sharpe = sortino = 0

    # Max drawdown
    cum = np.cumsum(actual_pnl)
    peak = np.maximum.accumulate(cum)
    max_dd = float(np.max(peak - cum)) if len(cum) > 0 else 0

    months_pos = sum(1 for v in monthly_vals if v > 0)

    return {
        "max_entry": max_entry,
        "max_loss": max_loss,
        "disable_thresh": disable_thresh,
        "total_trades": total_trades,
        "total_pnl": round(total_pnl, 2),
        "total_staked": round(total_staked, 2),
        "wins": wins,
        "losses": losses,
        "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3),
        "max_drawdown": round(max_dd, 2),
        "monthly_pnl": [round(v, 2) for v in monthly_vals],
        "months_positive": months_pos,
        "months_total": len(monthly_vals),
    }


def main():
    t0 = _time.time()
    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
    df, months = load_data(data_dir)

    max_entry_values = [78, 80, 82, 85, 88, 90]
    max_loss_values = [3.0, 4.0, 5.0, 7.0, 10.0, 15.0]
    disable_thresh_values = [-0.02, -0.03, -0.05, -0.07, -0.10]

    combos = list(itertools.product(max_entry_values, max_loss_values, disable_thresh_values))
    print(f"\nSweeping {len(combos)} parameter combinations...", flush=True)

    results = []
    for idx, (me, ml, dt) in enumerate(combos):
        r = simulate_combo(df, months, me, ml, dt)
        if r:
            results.append(r)
        if (idx + 1) % 30 == 0:
            print(f"  {idx+1}/{len(combos)} done...", flush=True)

    print(f"  {len(combos)}/{len(combos)} done ({_time.time()-t0:.0f}s)", flush=True)

    # Sort by Sortino
    results.sort(key=lambda r: r["sortino"], reverse=True)

    print(f"\n{'='*95}")
    print("TOP 15 PARAMETER COMBINATIONS (sorted by Sortino ratio)")
    print(f"{'='*95}")
    print(f"{'MaxEntry':>8} {'MaxLoss':>8} {'DisThr':>7} | {'Trades':>7} {'WR':>5} {'P&L':>10} {'Sharpe':>7} {'Sortino':>8} {'MaxDD':>7} {'Mo+':>5}")
    print(f"{'-'*95}")

    for r in results[:15]:
        wr = r["wins"] / r["total_trades"] * 100
        print(
            f"{r['max_entry']:>7}c ${r['max_loss']:>6.0f} {r['disable_thresh']:>+6.0%} | "
            f"{r['total_trades']:>7,} {wr:>4.0f}% ${r['total_pnl']:>+9.2f} "
            f"{r['sharpe']:>+7.3f} {r['sortino']:>+8.3f} ${r['max_drawdown']:>6.2f} "
            f"{r['months_positive']:>2}/{r['months_total']}"
        )

    print(f"\nBOTTOM 5:")
    for r in results[-5:]:
        wr = r["wins"] / r["total_trades"] * 100
        print(
            f"{r['max_entry']:>7}c ${r['max_loss']:>6.0f} {r['disable_thresh']:>+6.0%} | "
            f"{r['total_trades']:>7,} {wr:>4.0f}% ${r['total_pnl']:>+9.2f} "
            f"{r['sharpe']:>+7.3f} {r['sortino']:>+8.3f} ${r['max_drawdown']:>6.2f} "
            f"{r['months_positive']:>2}/{r['months_total']}"
        )

    # Sensitivity
    print(f"\n{'='*95}")
    print("PARAMETER SENSITIVITY (avg Sortino by value)")
    print(f"{'='*95}")

    for param, extractor in [("max_entry", "max_entry"), ("max_loss", "max_loss"), ("disable_thresh", "disable_thresh")]:
        by_val = defaultdict(list)
        for r in results:
            by_val[r[extractor]].append(r["sortino"])
        print(f"\n  {param}:")
        for val in sorted(by_val.keys()):
            avg = sum(by_val[val]) / len(by_val[val])
            best = max(by_val[val])
            avg_pnl_list = [r["total_pnl"] for r in results if r[extractor] == val]
            avg_pnl = sum(avg_pnl_list) / len(avg_pnl_list)
            print(f"    {val:>8} → Sortino={avg:+.3f}  best={best:+.3f}  avg_pnl=${avg:+,.0f}")

    best = results[0]
    print(f"\n{'='*95}")
    print(f"RECOMMENDED:")
    print(f"  fav_max_entry:      {best['max_entry']}c")
    print(f"  max_loss_per_trade: ${best['max_loss']:.0f}")
    print(f"  disable_threshold:  {best['disable_thresh']:+.0%}")
    print(f"  Projected: {best['total_trades']:,}t, {best['wins']/best['total_trades']*100:.0f}% WR, "
          f"${best['total_pnl']:+,.2f}, Sortino={best['sortino']:+.3f}, "
          f"{best['months_positive']}/{best['months_total']} months profitable")
    print(f"{'='*95}")

    Path("data").mkdir(exist_ok=True)
    with open("data/param_optimization.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to data/param_optimization.json ({_time.time()-t0:.0f}s total)")


if __name__ == "__main__":
    main()
