"""
Analyze live tick data to find optimal time windows for each cell.

Takes the current rr_params.json (price band, buffer, momentum, vol)
as fixed, and reports at what secs_left values tradeable entries exist
in the real tick data. This answers: "given the optimizer's price/buffer
params, when do qualifying setups actually appear?"

Usage:
    python analyze_time_windows.py
"""

import json
import math
import sys
import os
from pathlib import Path
from collections import defaultdict

import pandas as pd

# Reuse optimizer's data loading and preprocessing
from optimize_rr import (
    load_tick_windows, classify_ticker, preprocess_window,
    load_crypto_prices, get_price_at, compute_momentum,
    compute_realized_vol, VOL_LOOKBACK, evaluate_params,
)


def main():
    print("=" * 60)
    print("Time Window Analysis (tick data only)")
    print("=" * 60)

    # Load current params
    with open("data/rr_params.json") as f:
        rr_params = json.load(f)

    # Load ONLY tick data (not settled API)
    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
    tick_dir = os.path.join(data_dir, "ticks")
    tick_windows = load_tick_windows(tick_dir) if Path(tick_dir).exists() else []
    print(f"  Loaded {len(tick_windows)} tick windows")

    if not tick_windows:
        print("No tick data found!")
        sys.exit(1)

    # Load crypto prices for buffer computation
    crypto_prices = {}
    for d in ["data/prices_hf", os.path.join(data_dir, "prices_hf"),
              "data/prices", os.path.join(data_dir, "prices")]:
        if Path(d).exists():
            loaded = load_crypto_prices(d)
            for coin, pts in loaded.items():
                crypto_prices.setdefault(coin, []).extend(pts)
    # Dedupe per coin
    for coin in list(crypto_prices):
        seen = set()
        deduped = []
        for ts, px in sorted(crypto_prices[coin]):
            if ts not in seen:
                seen.add(ts)
                deduped.append((ts, px))
        crypto_prices[coin] = deduped
        print(f"  {coin.upper()}: {len(deduped)} price points")

    # Group windows by cell
    by_cell = defaultdict(list)
    for w in tick_windows:
        coin = w["coin"]
        mtype = w["market_type"]
        cell = f"{coin}_{mtype}"
        by_cell[cell].append(w)

    print(f"\n{'=' * 60}")
    print("Per-cell analysis: at what secs_left do qualifying entries appear?")
    print("=" * 60)

    for cell in sorted(rr_params.keys()):
        params = rr_params[cell]
        windows = by_cell.get(cell, [])
        if not windows:
            print(f"\n--- {cell}: no tick data ---")
            continue

        min_cp = params["min_contract_price"]
        max_ep = params["max_entry_price"]
        min_buf = params.get("min_price_buffer_pct", 0.05)
        max_mom = params.get("max_adverse_momentum", 0.0)
        mom_w = params.get("momentum_window", 60)
        mom_p = params.get("momentum_periods", 5)
        max_vol = params.get("max_realized_vol_pct")

        # Preprocess all windows with WIDE time filter (10-900s)
        all_entries = []
        wins = 0
        losses = 0
        for w in windows:
            pp = preprocess_window(w, crypto_prices)
            if not pp:
                continue
            result = pp["result"]
            for e in pp["entries"]:
                sl = e["secs_left"]
                if sl < 10 or sl > 900:
                    continue
                ep = e["entry_price"]
                fav = e["fav_price"]
                side = e["side"]
                buf = e.get("buffer_pct")
                mom = e.get("momentum")
                vol = e.get("realized_vol")

                # Apply price filter
                if fav < min_cp or fav > max_ep:
                    continue

                # Apply buffer filter (time-scaled)
                if buf is not None:
                    req = min_buf * math.sqrt(max(1.0, sl) / 60.0)
                    if side == "yes" and buf < req:
                        continue
                    if side == "no" and buf > -req:
                        continue

                # Apply momentum filter
                if max_mom < 0 and mom:
                    m_val = mom.get((mom_w, mom_p))
                    if m_val is not None:
                        if side == "yes" and m_val < max_mom:
                            continue
                        if side == "no" and m_val > -max_mom:
                            continue

                # Apply vol filter
                if max_vol is not None and vol is not None:
                    if vol > max_vol:
                        continue

                # This entry passes all non-time filters
                won = (side == result)
                all_entries.append({"secs_left": sl, "side": side, "won": won,
                                    "entry_price": ep})
                if won:
                    wins += 1
                else:
                    losses += 1

        if not all_entries:
            print(f"\n--- {cell}: {len(windows)} windows, 0 qualifying entries ---")
            continue

        total = len(all_entries)
        wr = wins / total if total > 0 else 0

        # Bucket by time
        buckets = defaultdict(lambda: {"count": 0, "wins": 0})
        for e in all_entries:
            sl = e["secs_left"]
            if sl <= 30:
                b = "0-30s"
            elif sl <= 60:
                b = "30-60s"
            elif sl <= 90:
                b = "60-90s"
            elif sl <= 120:
                b = "90-120s"
            elif sl <= 180:
                b = "120-180s"
            elif sl <= 300:
                b = "180-300s"
            elif sl <= 600:
                b = "300-600s"
            else:
                b = "600-900s"
            buckets[b]["count"] += 1
            if e["won"]:
                buckets[b]["wins"] += 1

        print(f"\n--- {cell} ---")
        print(f"  Current params: {min_cp}-{max_ep}c, buffer={min_buf}%, "
              f"momentum={max_mom}, vol={max_vol}")
        print(f"  Current window: {params.get('min_seconds', 10)}-{params.get('max_seconds', 60)}s")
        print(f"  Total qualifying entries: {total} ({wins}W/{losses}L, {wr:.1%} WR)")
        print(f"  Time distribution:")
        bucket_order = ["0-30s", "30-60s", "60-90s", "90-120s",
                        "120-180s", "180-300s", "300-600s", "600-900s"]
        for b in bucket_order:
            d = buckets.get(b)
            if d and d["count"] > 0:
                bwr = d["wins"] / d["count"]
                bar = "#" * min(50, d["count"] // 2)
                print(f"    {b:>10s}: {d['count']:5d} entries  {bwr:5.1%} WR  {bar}")

        # Suggest optimal window
        best_window = None
        best_score = -999
        for min_s in [10, 15, 30, 45, 60]:
            for max_s in [60, 90, 120, 180, 240, 300, 480, 600, 900]:
                if min_s >= max_s:
                    continue
                w_entries = [e for e in all_entries
                             if min_s <= e["secs_left"] <= max_s]
                if len(w_entries) < 5:
                    continue
                w_wins = sum(1 for e in w_entries if e["won"])
                w_wr = w_wins / len(w_entries)
                # Score: profit-first (same as optimizer)
                w_profit = sum(
                    (100 - e["entry_price"]) / 100 if e["won"]
                    else -(e["entry_price"] / 100)
                    for e in w_entries
                )
                score = w_profit
                if w_wr > 0.90:
                    score *= 1 + (w_wr - 0.90) * 5
                if score > best_score:
                    best_score = score
                    best_window = (min_s, max_s, len(w_entries), w_wins,
                                   len(w_entries) - w_wins, w_wr, w_profit)

        if best_window:
            ms, mxs, n, w, l, wr, pft = best_window
            print(f"  SUGGESTED: min_seconds={ms}, max_seconds={mxs} "
                  f"({n} trades, {w}W/{l}L, {wr:.1%} WR, ${pft:.2f} profit)")


if __name__ == "__main__":
    main()
