#!/usr/bin/env python3
"""
Extended Walk-Forward Backtest

Builds on backtest_walkforward.py to:
1. Support all crypto series (15M + daily), not just KXBTCD
2. Work with market metadata alone (no trade data required for favorite_bias)
3. Add resolution_rider strategy evaluation
4. Load both original + extended (API-pulled) parquet datasets
5. Cover Dec 2024 through Apr 2026

Usage:
    python backtest_walkforward_extended.py
    python backtest_walkforward_extended.py --data-dir /mnt/d/datasets/prediction-market-analysis
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

from performance import PerformanceTracker
from risk_manager import kalshi_taker_fee, kalshi_maker_fee

# Reuse from existing backtest
from backtest_historical import (
    BacktestConfig,
    compute_signals,
    evaluate_favorite_bias,
    evaluate_consensus,
)


# ─── New Strategy Evaluators ────────────────────────────────────────────────

def evaluate_resolution_rider(signals: dict, config: BacktestConfig, prev_result: str = None):
    """
    Resolution Rider: trade heavily-favored markets near resolution.

    In live trading, this fires when a market is >90c or <10c near expiry.
    In backtest, we use last_price as proxy for near-expiry price.

    Returns (side, confidence, reason) or None.
    """
    price = signals["entry_price"]

    # YES is extreme favorite (>90c)
    if price >= 90:
        confidence = min(0.98, price / 100 + 0.03)
        return ("yes", confidence, f"RR YES@{price}c near resolution")

    # NO is extreme favorite (<10c → NO price is >90c)
    no_price = 100 - price
    if no_price >= 90:
        confidence = min(0.98, no_price / 100 + 0.03)
        return ("no", confidence, f"RR NO@{no_price}c near resolution")

    return None


def signals_from_metadata(last_price: int, volume: int):
    """
    Create minimal signals dict from market metadata (no trade data needed).

    This enables favorite_bias and resolution_rider backtesting on markets
    where we only have market-level data (no individual trade tape).
    """
    if last_price is None or last_price <= 0 or last_price >= 100:
        return None
    return {
        "entry_price": int(last_price),
        "flow_imbalance": 0.0,  # unknown without trade data
        "momentum": 0,
        "recent_momentum": 0,
        "n_trades": 0,
        "volume": int(volume) if volume else 0,
    }


# ─── Walk-Forward Engine ────────────────────────────────────────────────────

@dataclass
class WindowResult:
    window_start: str
    window_end: str
    train_markets: int
    test_markets: int
    strategy_results: dict


class ExtendedWalkForward:
    def __init__(self, data_dir: str, config: BacktestConfig = None):
        self.data_dir = Path(data_dir)
        self.config = config or BacktestConfig()
        self.con = duckdb.connect()

    def _load_markets(self, series_filter: str = "KXBTCD%") -> pd.DataFrame:
        """Load markets from both original and extended parquet files."""
        files = []

        # Original dataset
        for name in ["crypto_markets_filtered.parquet", "crypto_markets.parquet"]:
            p = self.data_dir / name
            if p.exists():
                files.append(str(p))
                break

        # Extended dataset (from pull_historical.py)
        ext = self.data_dir / "crypto_markets_extended.parquet"
        if ext.exists():
            files.append(str(ext))

        if not files:
            print("ERROR: No parquet files found")
            sys.exit(1)

        # Union all files using common columns, dedup by ticker
        common_cols = "ticker, event_ticker, status, last_price, volume, result, close_time"
        union_sql = " UNION ALL ".join(
            f"SELECT {common_cols} FROM read_parquet('{f}')" for f in files
        )

        df = self.con.execute(f"""
            WITH combined AS ({union_sql})
            SELECT DISTINCT ON (ticker) *
            FROM combined
            WHERE ticker LIKE '{series_filter}'
            AND status IN ('finalized', 'settled')
            AND result IN ('yes', 'no')
            AND volume >= {self.config.min_volume}
            AND last_price IS NOT NULL
            AND last_price > 0 AND last_price < 100
            ORDER BY ticker, close_time ASC
        """).fetchdf()

        return df

    def _load_trades(self, series_filter: str = "KXBTCD%") -> dict:
        """Load trade data if available. Returns ticker -> DataFrame."""
        trade_files = []
        for name in ["crypto_trades_filtered.parquet", "crypto_trades_extended.parquet"]:
            p = self.data_dir / name
            if p.exists():
                trade_files.append(str(p))

        if not trade_files:
            return {}

        union_sql = " UNION ALL ".join(
            f"SELECT * FROM read_parquet('{f}')" for f in trade_files
        )

        all_trades = self.con.execute(f"""
            SELECT ticker, yes_price, taker_side, count, created_time
            FROM ({union_sql})
            WHERE ticker LIKE '{series_filter}'
            ORDER BY ticker, created_time ASC
        """).fetchdf()

        if len(all_trades) == 0:
            return {}

        return {
            ticker: group.reset_index(drop=True)
            for ticker, group in all_trades.groupby("ticker")
        }

    def run(self, train_months: int = 2,
            strategies: list[str] = None,
            series_patterns: list[str] = None) -> list[WindowResult]:

        if strategies is None:
            strategies = ["favorite_bias", "consensus", "resolution_rider"]

        if series_patterns is None:
            series_patterns = ["KXBTCD%"]

        strategy_funcs = {
            "favorite_bias": evaluate_favorite_bias,
            "consensus": evaluate_consensus,
            "resolution_rider": evaluate_resolution_rider,
        }
        # consensus requires trade data; these don't
        metadata_only_strats = {"favorite_bias", "resolution_rider"}

        # Load data for all series
        print("Loading markets...", flush=True)
        all_markets = []
        for pattern in series_patterns:
            df = self._load_markets(pattern)
            print(f"  {pattern}: {len(df):,} markets", flush=True)
            all_markets.append(df)

        markets_df = pd.concat(all_markets, ignore_index=True)
        markets_df = markets_df.drop_duplicates(subset="ticker")
        print(f"  Total: {len(markets_df):,} unique markets", flush=True)

        # Load trade data where available
        print("Loading trades...", flush=True)
        trades_by_ticker = {}
        for pattern in series_patterns:
            t = self._load_trades(pattern)
            trades_by_ticker.update(t)
        print(f"  Trade data for {len(trades_by_ticker):,} markets", flush=True)

        # Add month column
        markets_df["month"] = pd.to_datetime(
            markets_df["close_time"], utc=True
        ).dt.to_period("M")
        months = sorted(markets_df["month"].unique())
        print(f"  Spanning {months[0]} to {months[-1]} ({len(months)} months)", flush=True)

        # Walk forward
        results = []
        for i in range(train_months, len(months)):
            test_month = months[i]
            train_start = months[i - train_months]
            train_end = months[i - 1]

            train_mask = (markets_df["month"] >= train_start) & (markets_df["month"] <= train_end)
            test_mask = markets_df["month"] == test_month

            train_df = markets_df[train_mask]
            test_df = markets_df[test_mask]

            if len(test_df) == 0:
                continue

            # Train phase
            train_stats = self._compute_train_stats(
                train_df, trades_by_ticker, strategies, strategy_funcs, metadata_only_strats
            )

            # Test phase
            window_result = self._run_test_window(
                test_df, trades_by_ticker, strategies, strategy_funcs,
                metadata_only_strats, train_stats,
                window_start=str(test_month.start_time.date()),
                window_end=str(test_month.end_time.date()),
                train_markets=len(train_df),
            )
            results.append(window_result)

            # Progress
            parts = []
            for s, r in window_result.strategy_results.items():
                if r["trades"] > 0:
                    parts.append(f"{s}: {r['trades']}t {r['win_rate']:.0%} ${r['pnl']:+.0f}")
            print(f"  {test_month}: {' | '.join(parts) or 'no trades'}", flush=True)

        return results

    def _get_signals(self, row, trades_by_ticker, metadata_only_strats, strat_name):
        """Get signals for a market, falling back to metadata if no trade data."""
        ticker = row["ticker"]
        t_df = trades_by_ticker.get(ticker)

        # Try full signal computation from trade data
        if t_df is not None and len(t_df) >= 5:
            signals = compute_signals(t_df)
            if signals is not None:
                return signals

        # Fall back to metadata-only signals for strategies that support it
        if strat_name in metadata_only_strats:
            return signals_from_metadata(row["last_price"], row["volume"])

        return None

    def _compute_train_stats(self, train_df, trades_by_ticker, strategies,
                              strategy_funcs, metadata_only_strats):
        stats = {}
        for strat_name in strategies:
            func = strategy_funcs[strat_name]
            wins_by_bucket = defaultdict(lambda: {"wins": 0, "total": 0})
            prev_result = None

            for _, row in train_df.iterrows():
                result = row["result"]
                signals = self._get_signals(row, trades_by_ticker, metadata_only_strats, strat_name)
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

    def _run_test_window(self, test_df, trades_by_ticker, strategies,
                          strategy_funcs, metadata_only_strats, train_stats,
                          window_start, window_end, train_markets):
        strat_results = {}

        for strat_name in strategies:
            func = strategy_funcs[strat_name]
            train_info = train_stats.get(strat_name, {})
            train_wr = train_info.get("overall_wr", 0)

            if train_info.get("total_trades", 0) > 20 and train_wr < 0.52:
                strat_results[strat_name] = {
                    "trades": 0, "wins": 0, "pnl": 0.0, "win_rate": 0.0,
                    "skipped": True, "train_wr": train_wr,
                }
                continue

            trades = []
            prev_result = None

            for _, row in test_df.iterrows():
                result = row["result"]
                signals = self._get_signals(row, trades_by_ticker, metadata_only_strats, strat_name)
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
                    # Use maker fees for entry (our new approach), taker for settlement
                    entry_fee = kalshi_maker_fee(contracts, entry_price)
                    settle_fee = kalshi_taker_fee(contracts, 100 - entry_price) if is_win else 0
                    profit_after_fees = profit - entry_fee - settle_fee

                    trades.append({
                        "outcome": "win" if is_win else "loss",
                        "pnl": profit_after_fees,
                        "entry_price": entry_price,
                        "confidence": confidence,
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


# ─── Reporting ──────────────────────────────────────────────────────────────

def print_report(results: list[WindowResult]):
    from backtest_walkforward import print_walk_forward_report
    print_walk_forward_report(results)


def main():
    parser = argparse.ArgumentParser(description="Extended walk-forward backtest")
    parser.add_argument("--data-dir", type=str,
                        default=os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis"))
    parser.add_argument("--train-months", type=int, default=2)
    parser.add_argument("--strategy", type=str, nargs="+", default=None,
                        help="Strategies to test (default: favorite_bias consensus resolution_rider)")
    parser.add_argument("--series", type=str, nargs="+", default=None,
                        help="SQL LIKE patterns for series (default: KXBTCD%%)")
    parser.add_argument("--min-volume", type=int, default=50,
                        help="Minimum market volume (default: 50, lower for 15M markets)")
    parser.add_argument("--output", type=str, default="data/backtest_walkforward_extended.json")
    args = parser.parse_args()

    config = BacktestConfig(min_volume=args.min_volume)
    bt = ExtendedWalkForward(args.data_dir, config)

    series = args.series or ["KXBTCD%"]
    strategies = args.strategy or ["favorite_bias", "consensus", "resolution_rider"]

    results = bt.run(
        train_months=args.train_months,
        strategies=strategies,
        series_patterns=series,
    )
    print_report(results)

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
    print(f"\nResults saved to {args.output}", flush=True)


if __name__ == "__main__":
    main()
