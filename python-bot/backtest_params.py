"""Replay candidate RR params against the 7 days of recorded tick data and print
per-cell WR/PnL. Uses optimize_rr's own simulate_fast/preprocess_window so the
results are exactly what the runtime bot would produce.

Usage:
    python backtest_params.py
"""
import json
import os
from collections import defaultdict
from pathlib import Path

import optimize_rr


def run_backtest(label: str, params_fn, train_and_val_windows, crypto_prices):
    print(f"\n{'=' * 78}")
    print(f"  {label}")
    print(f"{'=' * 78}")
    print(f"  {'cell':<14} {'params':<38} {'trades':>7} {'wins':>6} {'WR':>6} {'pnl':>9}")
    print(f"  {'-' * 14} {'-' * 38} {'-' * 7} {'-' * 6} {'-' * 6} {'-' * 9}")

    totals = defaultdict(lambda: {"t": 0, "w": 0, "l": 0, "pnl": 0.0})

    for cell, windows in train_and_val_windows.items():
        # Pre-process all windows for this cell
        preprocessed = [
            pw for w in windows
            if (pw := optimize_rr.preprocess_window(w, crypto_prices)) is not None
        ]
        if not preprocessed:
            print(f"  {cell:<14} (no tradeable ticks)")
            continue

        params = params_fn(cell)
        result = optimize_rr.evaluate_params(preprocessed, params)
        if result["trades"] == 0:
            param_str = (
                f"{params['min_contract_price']}-{params['max_entry_price']}c "
                f"{params.get('min_seconds', 10)}-{params['max_seconds']}s "
                f"buf={params['min_price_buffer_pct']} "
                f"mom={params['max_adverse_momentum']}"
            )
            print(f"  {cell:<14} {param_str:<38} {0:>7}  —        —")
            continue

        wr = result["win_rate"]
        param_str = (
            f"{params['min_contract_price']}-{params['max_entry_price']}c "
            f"{params.get('min_seconds', 10)}-{params['max_seconds']}s "
            f"buf={params['min_price_buffer_pct']} "
            f"mom={params['max_adverse_momentum']}"
        )
        print(f"  {cell:<14} {param_str:<38} {result['trades']:>7} {result['wins']:>6}  {wr:>5.1%}  ${result['profit']:>+7.2f}")
        totals[label]["t"] += result["trades"]
        totals[label]["w"] += result["wins"]
        totals[label]["l"] += result["losses"]
        totals[label]["pnl"] += result["profit"]

    t = totals[label]
    if t["t"]:
        overall_wr = t["w"] / t["t"]
        print(f"  {'-' * 14} {'-' * 38} {'-' * 7} {'-' * 6} {'-' * 6} {'-' * 9}")
        print(f"  {'TOTAL':<14} {'':>38} {t['t']:>7} {t['w']:>6}  {overall_wr:>5.1%}  ${t['pnl']:>+7.2f}")
    return totals[label]


def main():
    print("Loading Kalshi tick data + crypto spot prices...")

    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")

    # Slippage model (shared across all runs)
    kalshi_csv = os.path.join(data_dir, "from_kalshi", "Kalshi-Transactions-2026.csv")
    optimize_rr.SLIPPAGE_MODEL = optimize_rr.load_slippage_model(kalshi_csv)

    tick_dir = os.path.join(data_dir, "ticks")
    all_tick_windows = optimize_rr.load_tick_windows(tick_dir)
    print(f"  Loaded {len(all_tick_windows)} tick windows")

    import data_paths
    price_dir = str(data_paths.resolve("prices"))
    crypto_prices = optimize_rr.load_crypto_prices(price_dir)
    print(f"  Loaded spot prices for: {sorted(crypto_prices.keys())}")

    # Group by cell name
    by_cell = defaultdict(list)
    for w in all_tick_windows:
        cell = f"{w['coin']}_{w['market_type']}"
        by_cell[cell].append(w)
    for cell in sorted(by_cell):
        print(f"  {cell}: {len(by_cell[cell])} windows")

    # Load current params from rr_params.json
    current = json.load(open("data/rr_params.json"))

    # Params function: pull from rr_params.json
    def current_params_for(cell):
        return current.get(cell, {}) or {
            "min_contract_price": 95, "max_entry_price": 98,
            "min_seconds": 10, "max_seconds": 60,
            "min_price_buffer_pct": 0.15,
            "max_adverse_momentum": -0.05,
            "momentum_window": 60, "momentum_periods": 5,
            "max_realized_vol_pct": None,
        }

    # Proposed loosened: buffer=0.15% + momentum=-0.20% + 60sx5 smoothing
    # Keep price band at current 95-98c and time windows as optimized.
    def proposed_params_for(cell):
        base = current_params_for(cell)
        loosened = dict(base)
        loosened["min_price_buffer_pct"] = 0.15
        loosened["max_adverse_momentum"] = -0.20
        loosened["momentum_window"] = 60
        loosened["momentum_periods"] = 5
        return loosened

    # Also test a middle-ground: buffer=0.15% + mom=-0.10% (5 min smoothed)
    def middle_params_for(cell):
        base = current_params_for(cell)
        mid = dict(base)
        mid["min_price_buffer_pct"] = 0.15
        mid["max_adverse_momentum"] = -0.10
        mid["momentum_window"] = 60
        mid["momentum_periods"] = 5
        return mid

    # Run three scenarios
    current_tot = run_backtest("CURRENT PARAMS (from rr_params.json)", current_params_for, by_cell, crypto_prices)
    middle_tot = run_backtest("MIDDLE: buf=0.15%, mom=-0.10%, 60sx5", middle_params_for, by_cell, crypto_prices)
    proposed_tot = run_backtest("PROPOSED: buf=0.15%, mom=-0.20%, 60sx5", proposed_params_for, by_cell, crypto_prices)

    print(f"\n{'=' * 78}")
    print("  SUMMARY")
    print(f"{'=' * 78}")
    print(f"  {'scenario':<50} {'trades':>7} {'WR':>6} {'pnl':>9}")
    for label in ["CURRENT PARAMS (from rr_params.json)", "MIDDLE: buf=0.15%, mom=-0.10%, 60sx5", "PROPOSED: buf=0.15%, mom=-0.20%, 60sx5"]:
        t = {"CURRENT PARAMS (from rr_params.json)": current_tot,
             "MIDDLE: buf=0.15%, mom=-0.10%, 60sx5": middle_tot,
             "PROPOSED: buf=0.15%, mom=-0.20%, 60sx5": proposed_tot}[label]
        if t["t"]:
            wr = t["w"] / t["t"]
            print(f"  {label:<50} {t['t']:>7} {wr:>5.1%}  ${t['pnl']:>+7.2f}")
        else:
            print(f"  {label:<50} {0:>7}  —       —")


if __name__ == "__main__":
    main()
