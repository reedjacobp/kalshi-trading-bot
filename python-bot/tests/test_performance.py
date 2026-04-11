"""Tests for performance metrics: Sharpe, Sortino, drawdown, profit factor."""

import unittest
import math

from performance import PerformanceTracker, PerformanceMetrics


class TestEmptyTracker(unittest.TestCase):
    def test_no_trades(self):
        tracker = PerformanceTracker()
        m = tracker.compute()
        self.assertEqual(m.total_trades, 0)
        self.assertEqual(m.sharpe_ratio, 0.0)
        self.assertEqual(m.max_drawdown_usd, 0.0)


class TestBasicMetrics(unittest.TestCase):
    def test_all_wins(self):
        tracker = PerformanceTracker()
        for _ in range(5):
            tracker.record(1.0)
        m = tracker.compute()
        self.assertEqual(m.total_trades, 5)
        self.assertEqual(m.wins, 5)
        self.assertEqual(m.losses, 0)
        self.assertAlmostEqual(m.win_rate, 1.0)
        self.assertAlmostEqual(m.avg_win, 1.0)
        self.assertAlmostEqual(m.total_pnl, 5.0)
        self.assertEqual(m.max_drawdown_usd, 0.0)

    def test_all_losses(self):
        tracker = PerformanceTracker()
        for _ in range(5):
            tracker.record(-1.0)
        m = tracker.compute()
        self.assertEqual(m.wins, 0)
        self.assertEqual(m.losses, 5)
        self.assertAlmostEqual(m.win_rate, 0.0)
        self.assertAlmostEqual(m.avg_loss, -1.0)
        self.assertAlmostEqual(m.total_pnl, -5.0)
        self.assertAlmostEqual(m.profit_factor, 0.0)

    def test_mixed_results(self):
        tracker = PerformanceTracker()
        tracker.record(3.0)   # win
        tracker.record(-1.0)  # loss
        tracker.record(2.0)   # win
        tracker.record(-1.0)  # loss
        m = tracker.compute()
        self.assertEqual(m.total_trades, 4)
        self.assertEqual(m.wins, 2)
        self.assertEqual(m.losses, 2)
        self.assertAlmostEqual(m.total_pnl, 3.0)
        self.assertAlmostEqual(m.avg_win, 2.5)
        self.assertAlmostEqual(m.avg_loss, -1.0)
        self.assertAlmostEqual(m.profit_factor, 5.0 / 2.0)
        self.assertAlmostEqual(m.expectancy, 0.75)


class TestSharpeRatio(unittest.TestCase):
    def test_known_sharpe(self):
        # Returns: [1, 1, 1, 1] → mean=1, std=0 → sharpe = 0 (no variance)
        tracker = PerformanceTracker()
        for _ in range(4):
            tracker.record(1.0)
        m = tracker.compute()
        self.assertAlmostEqual(m.sharpe_ratio, 0.0)  # Zero std

    def test_sharpe_with_variance(self):
        # Returns: [2, -1, 2, -1]
        returns = [2.0, -1.0, 2.0, -1.0]
        tracker = PerformanceTracker()
        for r in returns:
            tracker.record(r)
        m = tracker.compute()
        # mean = 0.5, std = sqrt(sum((r-0.5)^2)/3)
        mean_r = 0.5
        var = sum((r - mean_r)**2 for r in returns) / 3
        expected_sharpe = mean_r / math.sqrt(var)
        self.assertAlmostEqual(m.sharpe_ratio, expected_sharpe, places=4)

    def test_negative_sharpe(self):
        tracker = PerformanceTracker()
        tracker.record(-2.0)
        tracker.record(1.0)
        tracker.record(-2.0)
        m = tracker.compute()
        self.assertLess(m.sharpe_ratio, 0)


class TestSortinoRatio(unittest.TestCase):
    def test_sortino_all_wins(self):
        tracker = PerformanceTracker()
        tracker.record(1.0)
        tracker.record(2.0)
        m = tracker.compute()
        # No downside → inf
        self.assertEqual(m.sortino_ratio, float("inf"))

    def test_sortino_with_losses(self):
        tracker = PerformanceTracker()
        tracker.record(3.0)
        tracker.record(-1.0)
        tracker.record(2.0)
        tracker.record(-2.0)
        m = tracker.compute()
        self.assertGreater(m.sortino_ratio, 0)


class TestMaxDrawdown(unittest.TestCase):
    def test_simple_drawdown(self):
        tracker = PerformanceTracker(initial_balance=100.0)
        tracker.record(10.0)   # equity: 110
        tracker.record(-30.0)  # equity: 80
        tracker.record(5.0)    # equity: 85
        m = tracker.compute()
        # Peak was 110, trough was 80 → drawdown = 30
        self.assertAlmostEqual(m.max_drawdown_usd, 30.0)

    def test_no_drawdown_monotonic_wins(self):
        tracker = PerformanceTracker()
        for _ in range(5):
            tracker.record(1.0)
        m = tracker.compute()
        self.assertAlmostEqual(m.max_drawdown_usd, 0.0)

    def test_drawdown_percentage(self):
        tracker = PerformanceTracker(initial_balance=100.0)
        tracker.record(100.0)  # equity: 200 (peak)
        tracker.record(-50.0)  # equity: 150
        m = tracker.compute()
        # drawdown = 50, peak = 200, pct = 25%
        self.assertAlmostEqual(m.max_drawdown_usd, 50.0)
        self.assertAlmostEqual(m.max_drawdown_pct, 25.0)

    def test_calmar_ratio(self):
        tracker = PerformanceTracker(initial_balance=100.0)
        tracker.record(20.0)   # +20
        tracker.record(-10.0)  # -10
        m = tracker.compute()
        # total_pnl = 10, max_dd = 10, calmar = 1.0
        self.assertAlmostEqual(m.total_pnl, 10.0)
        self.assertAlmostEqual(m.max_drawdown_usd, 10.0)
        self.assertAlmostEqual(m.calmar_ratio, 1.0)


class TestComputeFromReturns(unittest.TestCase):
    def test_static_method(self):
        returns = [1.0, -0.5, 1.0, -0.5]
        m = PerformanceTracker.compute_from_returns(returns)
        self.assertEqual(m.total_trades, 4)
        self.assertAlmostEqual(m.total_pnl, 1.0)


class TestSummaryStr(unittest.TestCase):
    def test_no_trades_summary(self):
        tracker = PerformanceTracker()
        self.assertEqual(tracker.summary_str(), "No completed trades")

    def test_with_trades(self):
        tracker = PerformanceTracker()
        tracker.record(1.0)
        tracker.record(-0.5)
        s = tracker.summary_str()
        self.assertIn("Sharpe", s)
        self.assertIn("Drawdown", s)


if __name__ == "__main__":
    unittest.main()
