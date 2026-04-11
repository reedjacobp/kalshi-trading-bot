#!/usr/bin/env python3
"""
Walk-Forward Out-of-Sample Backtest

Splits historical data into rolling train/test windows to validate
that strategy edge persists out-of-sample, not just in-sample.

Approach:
  For each month M in the dataset:
    - Train window: months [M-2, M-1] (compute calibration & win rates)
    - Test window:  month M (trade using train-period parameters)
    - Record test-period P&L as out-of-sample result

This answers: "If I had calibrated my strategies on the last 2 months
of data, would they have been profitable in the following month?"

Usage:
    python backtest_walkforward.py
    python backtest_walkforward.py --data-dir /mnt/d/datasets/prediction-market-analysis
"""

import argparse
import json
import math
import os
import sys
import time as _time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

try:
    import duckdb
    import pandas as pd
except ImportError:
    print("ERROR: duckdb and pandas required. pip install duckdb pandas")
    sys.exit(1)

from performance import PerformanceTracker, PerformanceMetrics
from risk_manager import kalshi_taker_fee

# Reuse signal computation and strategy evaluators from the full backtest
from backtest_historical import (
    BacktestConfig,
    SimulatedTrade,
    compute_signals,
    evaluate_favorite_bias,
    evaluate_consensus,
)


@dataclass
class WindowResult:
    """Results for a single walk-forward test window."""
    window_start: str
    window_end: str
    train_markets: int
    test_markets: int
    # Per-strategy in this window
    strategy_results: dict  # strategy -> {trades, wins, pnl, win_rate, ...}


class WalkForwardBacktest:
    """
    Walk-forward out-of-sample validation.

    Slides a window through time: train on N months, test on the next month.
    """

    def __init__(self, data_dir: str, config: BacktestConfig = None):
        self.data_dir = Path(data_dir)
        self.config = config or BacktestConfig()
        self.con = duckdb.connect()

        markets_file = self.data_dir / "crypto_markets_filtered.parquet"
        trades_file = self.data_dir / "crypto_trades_filtered.parquet"
        if not markets_file.exists():
            markets_file = self.data_dir / "crypto_markets.parquet"

        self.markets_file = str(markets_file)
        self.trades_file = str(trades_file)

    def run(self, train_months: int = 2, strategies: list[str] = None) -> list[WindowResult]:
        """
        Run walk-forward backtest.

        Args:
            train_months: Number of months in the training window.
            strategies: Which strategies to test.

        Returns:
            List of WindowResult, one per test month.
        """
        if strategies is None:
            strategies = ["favorite_bias", "consensus"]

        strategy_funcs = {
            "favorite_bias": evaluate_favorite_bias,
            "consensus": evaluate_consensus,
        }

        # Load all markets
        print("Loading markets...")
        markets_df = self.con.execute(f"""
            SELECT ticker, result, close_time, volume, last_price
            FROM read_parquet('{self.markets_file}')
            WHERE ticker LIKE 'KXBTCD%'
            AND status = 'finalized'
            AND result IN ('yes', 'no')
            AND volume >= {self.config.min_volume}
            AND last_price IS NOT NULL
            AND last_price > 0 AND last_price < 100
            ORDER BY close_time ASC
        """).fetchdf()
        print(f"  {len(markets_df):,} markets")

        # Add month column
        markets_df["month"] = pd.to_datetime(markets_df["close_time"], utc=True).dt.to_period("M")
        months = sorted(markets_df["month"].unique())
        print(f"  Spanning {months[0]} to {months[-1]} ({len(months)} months)")

        # Bulk-load trades
        print("Loading trades...")
        t0 = _time.time()
        all_trades = self.con.execute(f"""
            SELECT ticker, yes_price, taker_side, count, created_time
            FROM read_parquet('{self.trades_file}')
            WHERE ticker LIKE 'KXBTCD%'
            ORDER BY ticker, created_time ASC
        """).fetchdf()
        trades_by_ticker = {
            ticker: group.reset_index(drop=True)
            for ticker, group in all_trades.groupby("ticker")
        }
        del all_trades
        print(f"  Loaded in {_time.time() - t0:.1f}s")

        # Walk forward
        results: list[WindowResult] = []

        for i in range(train_months, len(months)):
            test_month = months[i]
            train_start = months[i - train_months]
            train_end = months[i - 1]

            # Split data
            train_mask = (markets_df["month"] >= train_start) & (markets_df["month"] <= train_end)
            test_mask = markets_df["month"] == test_month

            train_df = markets_df[train_mask]
            test_df = markets_df[test_mask]

            if len(test_df) == 0:
                continue

            # ── TRAIN PHASE: compute win rates by price bucket per strategy ──
            # This simulates what the calibrator would learn from historical data
            train_stats = self._compute_train_stats(
                train_df, trades_by_ticker, strategies, strategy_funcs
            )

            # ── TEST PHASE: trade using train-period knowledge ──
            window_result = self._run_test_window(
                test_df, trades_by_ticker, strategies, strategy_funcs, train_stats,
                window_start=str(test_month.start_time.date()),
                window_end=str(test_month.end_time.date()),
                train_markets=len(train_df),
            )
            results.append(window_result)

            # Progress
            strat_summary = []
            for s, r in window_result.strategy_results.items():
                if r["trades"] > 0:
                    strat_summary.append(f"{s}: {r['trades']}t {r['win_rate']:.0%} ${r['pnl']:+.0f}")
            print(f"  {test_month}: {' | '.join(strat_summary) or 'no trades'}")

        return results

    def _compute_train_stats(self, train_df, trades_by_ticker, strategies, strategy_funcs):
        """Compute win rates from the training window (what calibrator would learn)."""
        stats = {}
        for strat_name in strategies:
            func = strategy_funcs[strat_name]
            wins_by_bucket = defaultdict(lambda: {"wins": 0, "total": 0})
            prev_result = None

            for _, row in train_df.iterrows():
                ticker = row["ticker"]
                result = row["result"]
                t_df = trades_by_ticker.get(ticker)
                if t_df is None or len(t_df) < 5:
                    prev_result = result
                    continue

                signals = compute_signals(t_df)
                if signals is None:
                    prev_result = result
                    continue

                rec = func(signals, self.config, prev_result)
                if rec is not None:
                    side, confidence, _ = rec
                    entry_price = signals["entry_price"] if side == "yes" else 100 - signals["entry_price"]
                    bucket = (entry_price // 10) * 10
                    is_win = (side == result)
                    wins_by_bucket[bucket]["total"] += 1
                    if is_win:
                        wins_by_bucket[bucket]["wins"] += 1

                prev_result = result

            # Compute win rates per bucket
            bucket_wr = {}
            total_trades = 0
            total_wins = 0
            for bucket, data in wins_by_bucket.items():
                if data["total"] > 0:
                    bucket_wr[bucket] = data["wins"] / data["total"]
                    total_trades += data["total"]
                    total_wins += data["wins"]

            stats[strat_name] = {
                "bucket_wr": bucket_wr,
                "overall_wr": total_wins / total_trades if total_trades > 0 else 0,
                "total_trades": total_trades,
            }
        return stats

    def _run_test_window(self, test_df, trades_by_ticker, strategies, strategy_funcs,
                         train_stats, window_start, window_end, train_markets):
        """Run strategies on the test window using train-period calibration."""
        strat_results = {}

        for strat_name in strategies:
            func = strategy_funcs[strat_name]
            train_info = train_stats.get(strat_name, {})
            train_wr = train_info.get("overall_wr", 0)

            # Skip strategy if it had negative edge in training
            # (This simulates the auto-suspension logic)
            if train_info.get("total_trades", 0) > 20 and train_wr < 0.52:
                strat_results[strat_name] = {
                    "trades": 0, "wins": 0, "pnl": 0.0, "win_rate": 0.0,
                    "skipped": True, "train_wr": train_wr,
                }
                continue

            trades = []
            prev_result = None

            for _, row in test_df.iterrows():
                ticker = row["ticker"]
                result = row["result"]
                t_df = trades_by_ticker.get(ticker)
                if t_df is None or len(t_df) < 5:
                    prev_result = result
                    continue

                signals = compute_signals(t_df)
                if signals is None:
                    prev_result = result
                    continue

                rec = func(signals, self.config, prev_result)
                if rec is not None:
                    side, confidence, reason = rec
                    entry_price = signals["entry_price"] if side == "yes" else 100 - signals["entry_price"]
                    contracts = max(1, int(self.config.stake_usd / (entry_price / 100.0)))
                    stake = contracts * (entry_price / 100.0)

                    is_win = (side == result)
                    profit = (contracts * 1.00 - stake) if is_win else -stake
                    entry_fee = kalshi_taker_fee(contracts, entry_price)
                    settle_fee = kalshi_taker_fee(contracts, 100 - entry_price) if is_win else 0
                    profit_after_fees = profit - entry_fee - settle_fee

                    trades.append({
                        "outcome": "win" if is_win else "loss",
                        "pnl": profit_after_fees,
                        "entry_price": entry_price,
                    })

                prev_result = result

            wins = sum(1 for t in trades if t["outcome"] == "win")
            total_pnl = sum(t["pnl"] for t in trades)

            strat_results[strat_name] = {
                "trades": len(trades),
                "wins": wins,
                "pnl": round(total_pnl, 2),
                "win_rate": wins / len(trades) if trades else 0.0,
                "skipped": False,
                "train_wr": train_wr,
            }

        return WindowResult(
            window_start=window_start,
            window_end=window_end,
            train_markets=train_markets,
            test_markets=len(test_df),
            strategy_results=strat_results,
        )


def print_walk_forward_report(results: list[WindowResult]):
    print(f"\n{'='*80}")
    print("WALK-FORWARD OUT-OF-SAMPLE REPORT")
    print(f"{'='*80}")

    if not results:
        print("No results.")
        return

    # Collect all strategy names
    all_strats = set()
    for r in results:
        all_strats.update(r.strategy_results.keys())

    for strat_name in sorted(all_strats):
        print(f"\n─── {strat_name} ───")
        print(f"  {'Window':<22} | {'Train WR':>8} | {'Test Trades':>11} | {'Test WR':>7} | {'Test P&L':>10} | {'Cum P&L':>10}")
        print(f"  {'-'*80}")

        cum_pnl = 0.0
        total_trades = 0
        total_wins = 0
        profitable_months = 0
        total_months = 0

        for r in results:
            sr = r.strategy_results.get(strat_name, {})
            trades = sr.get("trades", 0)
            wins = sr.get("wins", 0)
            pnl = sr.get("pnl", 0.0)
            wr = sr.get("win_rate", 0.0)
            train_wr = sr.get("train_wr", 0.0)
            skipped = sr.get("skipped", False)
            cum_pnl += pnl

            total_trades += trades
            total_wins += wins
            if trades > 0:
                total_months += 1
                if pnl > 0:
                    profitable_months += 1

            skip_marker = " [SKIP]" if skipped else ""
            if trades > 0:
                print(f"  {r.window_start:>10} to {r.window_end:<10} | {train_wr:>7.0%} | {trades:>11,} | {wr:>6.0%} | ${pnl:>+9,.2f} | ${cum_pnl:>+9,.2f}{skip_marker}")
            else:
                reason = f"train_wr={train_wr:.0%}" if skipped else "no signals"
                print(f"  {r.window_start:>10} to {r.window_end:<10} | {train_wr:>7.0%} | {'—':>11} | {'—':>7} | {'—':>10} | ${cum_pnl:>+9,.2f} ({reason})")

        overall_wr = total_wins / total_trades if total_trades > 0 else 0
        print(f"  {'-'*80}")
        print(f"  TOTAL: {total_trades:,} trades | {overall_wr:.1%} WR | ${cum_pnl:+,.2f} P&L | {profitable_months}/{total_months} months profitable")

    # Summary comparison
    print(f"\n{'='*80}")
    print("STRATEGY COMPARISON (out-of-sample only)")
    print(f"{'='*80}")
    print(f"  {'Strategy':<20} | {'Trades':>7} | {'WR':>6} | {'Total P&L':>10} | {'$/Trade':>8} | {'Months+':>8}")
    print(f"  {'-'*70}")

    for strat_name in sorted(all_strats):
        total_trades = 0
        total_wins = 0
        total_pnl = 0.0
        profitable_months = 0
        active_months = 0
        for r in results:
            sr = r.strategy_results.get(strat_name, {})
            t = sr.get("trades", 0)
            total_trades += t
            total_wins += sr.get("wins", 0)
            total_pnl += sr.get("pnl", 0.0)
            if t > 0:
                active_months += 1
                if sr.get("pnl", 0) > 0:
                    profitable_months += 1

        wr = total_wins / total_trades if total_trades > 0 else 0
        per_trade = total_pnl / total_trades if total_trades > 0 else 0
        months_str = f"{profitable_months}/{active_months}"
        print(f"  {strat_name:<20} | {total_trades:>7,} | {wr:>5.1%} | ${total_pnl:>+9,.2f} | ${per_trade:>+7.2f} | {months_str:>8}")

    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(description="Walk-forward out-of-sample backtest")
    parser.add_argument("--data-dir", type=str,
                        default=os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis"))
    parser.add_argument("--train-months", type=int, default=2,
                        help="Months in training window")
    parser.add_argument("--strategy", type=str, nargs="+", default=None)
    parser.add_argument("--min-volume", type=int, default=100)
    parser.add_argument("--output", type=str, default="data/backtest_walkforward.json")
    args = parser.parse_args()

    config = BacktestConfig(min_volume=args.min_volume)
    bt = WalkForwardBacktest(args.data_dir, config)
    results = bt.run(train_months=args.train_months, strategies=args.strategy)
    print_walk_forward_report(results)

    # Save
    serializable = []
    for r in results:
        serializable.append({
            "window_start": r.window_start,
            "window_end": r.window_end,
            "train_markets": r.train_markets,
            "test_markets": r.test_markets,
            "strategy_results": r.strategy_results,
        })
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(serializable, f, indent=2)
    print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
