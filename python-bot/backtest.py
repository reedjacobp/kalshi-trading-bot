"""
Backtest Harness for Kalshi Trading Bot (Foundation)

Validates strategy signals against historical trade data. Computes
what calibrated confidence would have predicted and generates
per-strategy performance metrics.

This is the foundation layer — it works with existing CSV trade logs
and session data. Full market simulation (replaying price data through
strategies) requires saved price feed data, which is added as a
side-effect of the bot's _tick() method.

Usage:
    python backtest.py                    # Analyze all trade CSVs
    python backtest.py --csv data/live_trades.csv  # Specific file
"""

import argparse
import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from calibrator import ConfidenceCalibrator
from performance import PerformanceTracker, PerformanceMetrics


@dataclass
class BacktestResults:
    """Results from a backtest run."""
    total_trades: int = 0
    aggregate: PerformanceMetrics = field(default_factory=PerformanceMetrics)
    per_strategy: dict[str, PerformanceMetrics] = field(default_factory=dict)
    calibration_accuracy: dict[str, dict] = field(default_factory=dict)


class BacktestEngine:
    """
    Analyzes historical trades to validate strategy performance.

    Phase 1: Signal validation from existing CSV data.
    - Parses settled trades from CSV
    - Computes per-strategy performance metrics
    - Validates calibration accuracy (predicted vs actual win rates)
    - Identifies which strategies have true edge
    """

    def __init__(
        self,
        csv_paths: list[str] = None,
    ):
        self.csv_paths = csv_paths or [
            "data/paper_trades.csv",
            "data/live_trades.csv",
        ]
        self.calibrator = ConfidenceCalibrator(csv_paths=self.csv_paths)

    def run(self) -> BacktestResults:
        """Run the backtest analysis on historical trade data."""
        trades = self._load_trades()
        if not trades:
            return BacktestResults()

        results = BacktestResults(total_trades=len(trades))

        # Aggregate metrics
        agg_tracker = PerformanceTracker()
        for t in trades:
            agg_tracker.record(t["profit_usd"])
        results.aggregate = agg_tracker.compute()

        # Per-strategy metrics
        strategies = set(t["strategy"] for t in trades)
        for strat in sorted(strategies):
            strat_trades = [t for t in trades if t["strategy"] == strat]
            tracker = PerformanceTracker()
            for t in strat_trades:
                tracker.record(t["profit_usd"])
            results.per_strategy[strat] = tracker.compute()

        # Calibration accuracy: for each strategy, compare predicted
        # win rate (from confidence/price) against actual
        for strat in sorted(strategies):
            strat_trades = [t for t in trades if t["strategy"] == strat]
            results.calibration_accuracy[strat] = self._calibration_analysis(strat, strat_trades)

        return results

    def _load_trades(self) -> list[dict]:
        """Load all settled trades from CSVs.

        The CSV has two row types per trade:
        1. Entry row: outcome="", reason="signal reason"
        2. Settlement row: outcome="win"/"loss", reason="SETTLED:win"

        We use the SETTLEMENT rows for outcome/P&L data, and join back
        to entry rows by ticker to get the signal reason and confidence.
        """
        trades = []
        for csv_path in self.csv_paths:
            path = Path(csv_path)
            if not path.exists():
                continue
            with open(path, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Index entry rows by ticker for confidence/reason lookup
            entry_by_ticker: dict[str, dict] = {}
            for row in rows:
                reason = row.get("reason", "")
                if not reason.startswith("SETTLED:") and row.get("outcome", "").strip() == "":
                    ticker = row.get("ticker", "")
                    if ticker:
                        entry_by_ticker[ticker] = row

            # Use settlement rows as the source of truth for outcomes
            for row in rows:
                outcome = row.get("outcome", "").strip()
                if outcome not in ("win", "loss"):
                    continue
                reason = row.get("reason", "")
                if not reason.startswith("SETTLED:"):
                    continue

                # Join with entry row for confidence data
                ticker = row.get("ticker", "")
                entry = entry_by_ticker.get(ticker, {})
                confidence_str = entry.get("confidence", "") or row.get("confidence", "")

                try:
                    trades.append({
                        "time": row.get("time", ""),
                        "strategy": row.get("strategy", "unknown"),
                        "ticker": ticker,
                        "side": row.get("side", ""),
                        "price_cents": int(row.get("price_cents", 0)),
                        "contracts": int(row.get("contracts", 0)),
                        "stake_usd": float(row.get("stake_usd", 0)),
                        "outcome": outcome,
                        "profit_usd": float(row.get("profit_usd", 0)),
                        "confidence": self._parse_float(confidence_str),
                    })
                except (ValueError, TypeError):
                    continue
        return trades

    @staticmethod
    def _parse_float(s: str) -> Optional[float]:
        try:
            return float(s) if s.strip() else None
        except (ValueError, TypeError):
            return None

    def _calibration_analysis(self, strategy: str, trades: list[dict]) -> dict:
        """
        Compare predicted probability against actual win rate.

        Bins trades by confidence (or price proxy) and computes the
        difference between predicted and actual win rates per bin.
        """
        if not trades:
            return {"error": "no trades"}

        # Group by confidence bucket (or price bucket as proxy)
        bins: dict[int, list[bool]] = {}
        for t in trades:
            conf = t.get("confidence")
            if conf is not None:
                bucket = min(9, int(conf * 10))
            else:
                # Use price as proxy
                bucket = min(9, t["price_cents"] // 10)

            if bucket not in bins:
                bins[bucket] = []
            bins[bucket].append(t["outcome"] == "win")

        analysis = {}
        for bucket in sorted(bins.keys()):
            outcomes = bins[bucket]
            n = len(outcomes)
            actual_wr = sum(outcomes) / n
            # What the calibrator would predict for the midpoint
            midpoint_conf = (bucket + 0.5) / 10
            predicted_wr = self.calibrator.calibrate(strategy, midpoint_conf)
            analysis[f"bin_{bucket}"] = {
                "n": n,
                "actual_wr": round(actual_wr, 3),
                "predicted_wr": round(predicted_wr, 3),
                "gap": round(actual_wr - predicted_wr, 3),
                "midpoint": round(midpoint_conf, 2),
            }

        # Overall calibration error (Brier-like)
        total_gap = 0
        total_n = 0
        for bucket_data in analysis.values():
            if isinstance(bucket_data, dict) and "gap" in bucket_data:
                total_gap += abs(bucket_data["gap"]) * bucket_data["n"]
                total_n += bucket_data["n"]
        avg_gap = total_gap / total_n if total_n > 0 else 0

        return {
            "bins": analysis,
            "avg_calibration_gap": round(avg_gap, 3),
            "total_trades": len(trades),
        }

    def generate_report(self, results: BacktestResults) -> str:
        """Generate a human-readable backtest report."""
        lines = [
            "=" * 60,
            "BACKTEST REPORT",
            "=" * 60,
            "",
            f"Total trades analyzed: {results.total_trades}",
            "",
            "─── Aggregate Performance ───",
            f"  Win Rate:      {results.aggregate.win_rate:.0%}",
            f"  Total P&L:     ${results.aggregate.total_pnl:+.2f}",
            f"  Sharpe:        {results.aggregate.sharpe_ratio:.2f}",
            f"  Profit Factor: {results.aggregate.profit_factor:.2f}",
            f"  Max Drawdown:  ${results.aggregate.max_drawdown_usd:.2f}",
            f"  Expectancy:    ${results.aggregate.expectancy:+.2f}/trade",
            "",
        ]

        for strat, metrics in results.per_strategy.items():
            lines.extend([
                f"─── {strat} ({metrics.total_trades} trades) ───",
                f"  Win Rate:      {metrics.win_rate:.0%}",
                f"  P&L:           ${metrics.total_pnl:+.2f}",
                f"  Sharpe:        {metrics.sharpe_ratio:.2f}",
                f"  Profit Factor: {metrics.profit_factor:.2f}",
                f"  Avg Win:       ${metrics.avg_win:+.2f}",
                f"  Avg Loss:      ${metrics.avg_loss:+.2f}",
                f"  Max Drawdown:  ${metrics.max_drawdown_usd:.2f}",
            ])

            cal = results.calibration_accuracy.get(strat, {})
            gap = cal.get("avg_calibration_gap", "N/A")
            lines.append(f"  Calibration Gap: {gap}")
            lines.append("")

        lines.append("=" * 60)
        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Backtest historical trades")
    parser.add_argument("--csv", type=str, nargs="+", default=None,
                        help="CSV file paths to analyze")
    args = parser.parse_args()

    csv_paths = args.csv or ["data/paper_trades.csv", "data/live_trades.csv"]
    engine = BacktestEngine(csv_paths=csv_paths)
    results = engine.run()
    report = engine.generate_report(results)
    print(report)

    # Save results to JSON
    output_path = Path("data/backtest_results.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({
            "total_trades": results.total_trades,
            "aggregate": {
                "win_rate": results.aggregate.win_rate,
                "total_pnl": results.aggregate.total_pnl,
                "sharpe": results.aggregate.sharpe_ratio,
                "profit_factor": results.aggregate.profit_factor,
                "max_drawdown": results.aggregate.max_drawdown_usd,
            },
            "per_strategy": {
                name: {
                    "win_rate": m.win_rate,
                    "total_pnl": m.total_pnl,
                    "sharpe": m.sharpe_ratio,
                    "profit_factor": m.profit_factor,
                    "trades": m.total_trades,
                }
                for name, m in results.per_strategy.items()
            },
            "calibration": results.calibration_accuracy,
        }, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
