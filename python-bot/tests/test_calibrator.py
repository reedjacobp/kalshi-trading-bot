"""Tests for the confidence calibrator: shrinkage, interpolation, CSV parsing."""

import csv
import os
import tempfile
import unittest

from calibrator import ConfidenceCalibrator


class TestColdStart(unittest.TestCase):
    """Test behavior with no historical data."""

    def test_no_data_dampens_toward_half(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        # With no data, calibrated = 0.5 + (raw - 0.5) * 0.6
        result = cal.calibrate("consensus", 0.80)
        expected = 0.5 + (0.80 - 0.5) * 0.6  # 0.68
        self.assertAlmostEqual(result, expected, places=2)

    def test_no_data_at_half_stays_at_half(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("momentum", 0.50)
        self.assertAlmostEqual(result, 0.50, places=2)

    def test_no_data_high_confidence_capped(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("consensus", 1.0)
        # 0.5 + 0.5 * 0.6 = 0.80
        self.assertAlmostEqual(result, 0.80, places=2)
        self.assertLessEqual(result, 0.95)

    def test_no_data_low_confidence_floored(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("consensus", 0.0)
        # 0.5 + (-0.5) * 0.6 = 0.20
        self.assertAlmostEqual(result, 0.20, places=2)
        self.assertGreaterEqual(result, 0.05)

    def test_unknown_strategy_uses_cold_start(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("nonexistent_strategy", 0.70)
        expected = 0.5 + (0.70 - 0.5) * 0.6
        self.assertAlmostEqual(result, expected, places=2)


class TestOutputBounds(unittest.TestCase):
    """Calibrated probability must always be in [0.05, 0.95]."""

    def test_extreme_high(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("test", 1.0)
        self.assertLessEqual(result, 0.95)
        self.assertGreaterEqual(result, 0.05)

    def test_extreme_low(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("test", 0.0)
        self.assertLessEqual(result, 0.95)
        self.assertGreaterEqual(result, 0.05)

    def test_negative_clamped(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("test", -0.5)
        self.assertGreaterEqual(result, 0.05)

    def test_over_one_clamped(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        result = cal.calibrate("test", 1.5)
        self.assertLessEqual(result, 0.95)


class TestCSVParsing(unittest.TestCase):
    """Test loading calibration data from CSV files."""

    def _write_csv(self, path, rows, has_confidence=True):
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

    def test_load_with_confidence_column(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            rows = []
            # 30 trades for favorite_bias with confidence ~0.80, all wins
            for i in range(30):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "favorite_bias",
                    f"TICK-{i}", "yes", 75, 5, "3.75", f"ORD-{i}",
                    "win", "5.00", "1.25", "test", "0.80",
                ])
            self._write_csv(path, rows, has_confidence=True)

            cal = ConfidenceCalibrator(csv_paths=[path], min_strategy_trades=20)
            result = cal.calibrate("favorite_bias", 0.80)
            # Should be close to 1.0 (all wins) with some shrinkage
            self.assertGreater(result, 0.80)
        finally:
            os.unlink(path)

    def test_load_without_confidence_uses_price_proxy(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            rows = []
            # 25 trades at price 75c, 20 wins, 5 losses
            for i in range(20):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "consensus",
                    f"TICK-W{i}", "yes", 75, 5, "3.75", f"ORD-W{i}",
                    "win", "5.00", "1.25", "test",
                ])
            for i in range(5):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "consensus",
                    f"TICK-L{i}", "yes", 75, 5, "3.75", f"ORD-L{i}",
                    "loss", "0.00", "-3.75", "test",
                ])
            self._write_csv(path, rows, has_confidence=False)

            cal = ConfidenceCalibrator(csv_paths=[path], min_strategy_trades=20)
            result = cal.calibrate("consensus", 0.75)
            # Should be pulled from raw empirical (80%) toward something reasonable
            self.assertGreater(result, 0.50)
            self.assertLess(result, 0.95)
        finally:
            os.unlink(path)

    def test_settlement_rows_excluded(self):
        """Rows with reason starting with SETTLED: should be skipped."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            rows = []
            # 25 entry rows (wins)
            for i in range(25):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "consensus",
                    f"TICK-{i}", "yes", 50, 5, "2.50", f"ORD-{i}",
                    "win", "5.00", "2.50", "signal reason", "0.65",
                ])
            # 25 settlement echo rows (should be excluded)
            for i in range(25):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "consensus",
                    f"TICK-{i}", "yes", 50, 5, "2.50", f"ORD-{i}",
                    "win", "5.00", "2.50", "SETTLED:win", "0.65",
                ])
            self._write_csv(path, rows, has_confidence=True)

            cal = ConfidenceCalibrator(csv_paths=[path], min_strategy_trades=20)
            stats = cal.get_strategy_stats()
            # Should count 25 trades, not 50
            self.assertEqual(stats["consensus"]["total_trades"], 25)
        finally:
            os.unlink(path)

    def test_mixed_outcomes_calibration(self):
        """Test calibration with mixed win/loss results."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            rows = []
            # 15 wins at confidence 0.70
            for i in range(15):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "momentum",
                    f"TICK-W{i}", "yes", 45, 10, "4.50", f"ORD-W{i}",
                    "win", "10.00", "5.50", "test", "0.70",
                ])
            # 10 losses at confidence 0.70
            for i in range(10):
                rows.append([
                    "2026-04-05T00:00:00Z", "run1", "momentum",
                    f"TICK-L{i}", "yes", 45, 10, "4.50", f"ORD-L{i}",
                    "loss", "0.00", "-4.50", "test", "0.70",
                ])
            self._write_csv(path, rows, has_confidence=True)

            cal = ConfidenceCalibrator(csv_paths=[path], min_strategy_trades=20)
            # Empirical win rate at 0.70 confidence = 15/25 = 60%
            result = cal.calibrate("momentum", 0.70)
            # Should be near 60% (pulled from 70% toward empirical)
            self.assertGreater(result, 0.50)
            self.assertLess(result, 0.75)
        finally:
            os.unlink(path)

    def test_nonexistent_csv_path(self):
        """Should handle missing files gracefully."""
        cal = ConfidenceCalibrator(csv_paths=["/nonexistent/path.csv"])
        result = cal.calibrate("test", 0.70)
        # Falls back to cold start
        self.assertIsNotNone(result)

    def test_empty_csv(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
            f.write("")

        try:
            cal = ConfidenceCalibrator(csv_paths=[path])
            result = cal.calibrate("test", 0.70)
            self.assertIsNotNone(result)
        finally:
            os.unlink(path)


class TestStrategyStats(unittest.TestCase):
    """Test the get_strategy_stats reporting."""

    def test_empty_stats(self):
        cal = ConfidenceCalibrator(csv_paths=[])
        stats = cal.get_strategy_stats()
        self.assertEqual(stats, {})

    def test_stats_report_total_trades(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            columns = [
                "time", "run_id", "strategy", "ticker", "side", "price_cents",
                "contracts", "stake_usd", "order_id", "outcome",
                "payout_usd", "profit_usd", "reason", "confidence",
            ]
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for i in range(5):
                    writer.writerow([
                        "2026-04-05T00:00:00Z", "run1", "consensus",
                        f"T-{i}", "yes", 50, 1, "0.50", f"O-{i}",
                        "win", "1.00", "0.50", "test", "0.60",
                    ])
            cal = ConfidenceCalibrator(csv_paths=[path])
            stats = cal.get_strategy_stats()
            self.assertEqual(stats["consensus"]["total_trades"], 5)
            self.assertFalse(stats["consensus"]["calibration_ready"])  # < 20
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
