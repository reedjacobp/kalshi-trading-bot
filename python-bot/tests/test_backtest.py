"""Tests for the backtest harness."""

import csv
import os
import tempfile
import unittest

from backtest import BacktestEngine, BacktestResults


class TestBacktestWithNoData(unittest.TestCase):
    def test_empty_results(self):
        engine = BacktestEngine(csv_paths=["/nonexistent/path.csv"])
        results = engine.run()
        self.assertEqual(results.total_trades, 0)

    def test_report_with_no_trades(self):
        engine = BacktestEngine(csv_paths=["/nonexistent/path.csv"])
        results = engine.run()
        report = engine.generate_report(results)
        self.assertIn("Total trades analyzed: 0", report)


class TestBacktestWithData(unittest.TestCase):
    def _make_csv(self, rows, has_confidence=False):
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        columns = [
            "time", "run_id", "strategy", "ticker", "side", "price_cents",
            "contracts", "stake_usd", "order_id", "outcome",
            "payout_usd", "profit_usd", "reason",
        ]
        if has_confidence:
            columns.append("confidence")

        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(row)
        return path

    def test_basic_analysis(self):
        rows = []
        # 10 consensus wins: entry row + SETTLED row
        for i in range(10):
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"TICK-W{i}", "yes", 45, 10, "4.50", f"O-W{i}",
                "", "0.00", "0.00", "signal reason",
            ])
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"TICK-W{i}", "yes", 45, 10, "4.50", f"O-W{i}",
                "win", "10.00", "5.50", "SETTLED:win",
            ])
        # 5 consensus losses
        for i in range(5):
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"TICK-L{i}", "yes", 45, 10, "4.50", f"O-L{i}",
                "", "0.00", "0.00", "signal reason",
            ])
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"TICK-L{i}", "yes", 45, 10, "4.50", f"O-L{i}",
                "loss", "0.00", "-4.50", "SETTLED:loss",
            ])
        path = self._make_csv(rows)
        try:
            engine = BacktestEngine(csv_paths=[path])
            results = engine.run()
            self.assertEqual(results.total_trades, 15)
            self.assertIn("consensus", results.per_strategy)
            self.assertAlmostEqual(results.per_strategy["consensus"].win_rate, 10/15)
            self.assertGreater(results.aggregate.sharpe_ratio, 0)
        finally:
            os.unlink(path)

    def test_multi_strategy_analysis(self):
        rows = []
        # 5 consensus wins
        for i in range(5):
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"C-W{i}", "yes", 50, 5, "2.50", f"CO-W{i}",
                "", "0.00", "0.00", "signal",
            ])
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"C-W{i}", "yes", 50, 5, "2.50", f"CO-W{i}",
                "win", "5.00", "2.50", "SETTLED:win",
            ])
        # 5 momentum losses
        for i in range(5):
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "momentum",
                f"M-L{i}", "yes", 50, 5, "2.50", f"MO-L{i}",
                "", "0.00", "0.00", "signal",
            ])
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "momentum",
                f"M-L{i}", "yes", 50, 5, "2.50", f"MO-L{i}",
                "loss", "0.00", "-2.50", "SETTLED:loss",
            ])
        path = self._make_csv(rows)
        try:
            engine = BacktestEngine(csv_paths=[path])
            results = engine.run()
            self.assertEqual(results.total_trades, 10)
            self.assertIn("consensus", results.per_strategy)
            self.assertIn("momentum", results.per_strategy)
            # Consensus: all wins, momentum: all losses
            self.assertAlmostEqual(results.per_strategy["consensus"].win_rate, 1.0)
            self.assertAlmostEqual(results.per_strategy["momentum"].win_rate, 0.0)
        finally:
            os.unlink(path)

    def test_no_double_counting(self):
        """Entry row with outcome + SETTLED row should count as 1 trade."""
        rows = [
            ["2026-04-05T00:00:00Z", "run1", "consensus", "T1", "yes", 50, 5, "2.50", "O1",
             "", "0.00", "0.00", "signal reason"],
            ["2026-04-05T00:00:00Z", "run1", "consensus", "T1", "yes", 50, 5, "2.50", "O1",
             "win", "5.00", "2.50", "SETTLED:win"],
        ]
        path = self._make_csv(rows)
        try:
            engine = BacktestEngine(csv_paths=[path])
            results = engine.run()
            self.assertEqual(results.total_trades, 1)
        finally:
            os.unlink(path)

    def test_report_generation(self):
        rows = []
        for i in range(3):
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"T-{i}", "yes", 50, 5, "2.50", f"O-{i}",
                "", "0.00", "0.00", "signal",
            ])
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"T-{i}", "yes", 50, 5, "2.50", f"O-{i}",
                "win", "5.00", "2.50", "SETTLED:win",
            ])
        path = self._make_csv(rows)
        try:
            engine = BacktestEngine(csv_paths=[path])
            results = engine.run()
            report = engine.generate_report(results)
            self.assertIn("BACKTEST REPORT", report)
            self.assertIn("consensus", report)
            self.assertIn("Sharpe", report)
        finally:
            os.unlink(path)

    def test_calibration_accuracy(self):
        rows = []
        # All trades at confidence 0.75, some win some lose
        for i in range(20):
            outcome = "win" if i < 12 else "loss"
            profit = 2.50 if outcome == "win" else -2.50
            # Entry row with confidence
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"T-{i}", "yes", 50, 5, "2.50", f"O-{i}",
                "", "0.00", "0.00", "signal", "0.75",
            ])
            # Settlement row
            rows.append([
                "2026-04-05T00:00:00Z", "run1", "consensus",
                f"T-{i}", "yes", 50, 5, "2.50", f"O-{i}",
                outcome, "5.00" if outcome == "win" else "0.00",
                str(profit), f"SETTLED:{outcome}", "",
            ])
        path = self._make_csv(rows, has_confidence=True)
        try:
            engine = BacktestEngine(csv_paths=[path])
            results = engine.run()
            cal = results.calibration_accuracy.get("consensus", {})
            self.assertIn("total_trades", cal)
            self.assertEqual(cal["total_trades"], 20)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
