"""Tests for per-strategy P&L tracking and auto-suspension."""

import time
import unittest

from strategy_tracker import StrategyTracker


class TestBasicTracking(unittest.TestCase):
    def test_record_win(self):
        tracker = StrategyTracker()
        tracker.record_outcome("consensus", 1.50)
        stats = tracker.get_stats("consensus")
        self.assertEqual(stats.total_trades, 1)
        self.assertEqual(stats.wins, 1)
        self.assertAlmostEqual(stats.total_pnl, 1.50)

    def test_record_loss(self):
        tracker = StrategyTracker()
        tracker.record_outcome("momentum", -2.00)
        stats = tracker.get_stats("momentum")
        self.assertEqual(stats.losses, 1)
        self.assertEqual(stats.consecutive_losses, 1)

    def test_consecutive_losses_reset_on_win(self):
        tracker = StrategyTracker()
        tracker.record_outcome("test", -1.0)
        tracker.record_outcome("test", -1.0)
        tracker.record_outcome("test", 2.0)  # win resets
        stats = tracker.get_stats("test")
        self.assertEqual(stats.consecutive_losses, 0)

    def test_win_rate(self):
        tracker = StrategyTracker()
        tracker.record_outcome("test", 1.0)
        tracker.record_outcome("test", -1.0)
        tracker.record_outcome("test", 1.0)
        stats = tracker.get_stats("test")
        self.assertAlmostEqual(stats.win_rate, 2/3)

    def test_unknown_strategy_not_suspended(self):
        tracker = StrategyTracker()
        suspended, reason = tracker.is_suspended("nonexistent")
        self.assertFalse(suspended)


class TestSuspensionTriggers(unittest.TestCase):
    def test_suspend_on_rolling_pnl(self):
        tracker = StrategyTracker(
            rolling_window=5,
            suspension_threshold=-3.0,
            cooldown_seconds=60,
            max_consecutive_losses=100,  # Disable this trigger
            max_drawdown=100.0,          # Disable this trigger
        )
        # 5 losses of -1.0 each → rolling = -5.0 < -3.0
        for _ in range(5):
            tracker.record_outcome("bad_strat", -1.0)

        suspended, reason = tracker.is_suspended("bad_strat")
        self.assertTrue(suspended)
        self.assertIn("Rolling P&L", reason)

    def test_suspend_on_consecutive_losses(self):
        tracker = StrategyTracker(
            max_consecutive_losses=3,
            cooldown_seconds=60,
        )
        for _ in range(3):
            tracker.record_outcome("streaky", -0.50)

        suspended, reason = tracker.is_suspended("streaky")
        self.assertTrue(suspended)
        self.assertIn("consecutive losses", reason)

    def test_suspend_on_drawdown(self):
        tracker = StrategyTracker(
            max_drawdown=5.0,
            cooldown_seconds=60,
        )
        # Build up peak, then lose
        tracker.record_outcome("dd_strat", 10.0)
        tracker.record_outcome("dd_strat", -6.0)  # drawdown = 6 > 5

        suspended, reason = tracker.is_suspended("dd_strat")
        self.assertTrue(suspended)
        self.assertIn("Drawdown", reason)

    def test_no_suspension_within_limits(self):
        tracker = StrategyTracker(
            rolling_window=10,
            suspension_threshold=-10.0,
            max_consecutive_losses=10,
            max_drawdown=20.0,
        )
        # Small losses within all limits
        for _ in range(3):
            tracker.record_outcome("ok_strat", -0.50)

        suspended, _ = tracker.is_suspended("ok_strat")
        self.assertFalse(suspended)


class TestCooldownExpiry(unittest.TestCase):
    def test_suspension_expires(self):
        tracker = StrategyTracker(
            max_consecutive_losses=2,
            cooldown_seconds=1,  # 1 second cooldown for testing
        )
        tracker.record_outcome("temp", -1.0)
        tracker.record_outcome("temp", -1.0)

        # Should be suspended now
        suspended, _ = tracker.is_suspended("temp")
        self.assertTrue(suspended)

        # Wait for cooldown
        time.sleep(1.1)
        suspended, _ = tracker.is_suspended("temp")
        self.assertFalse(suspended)

    def test_suspension_count_increments(self):
        tracker = StrategyTracker(
            max_consecutive_losses=2,
            cooldown_seconds=0,  # Instant cooldown for testing
            max_drawdown=100.0,  # Disable drawdown trigger
        )
        tracker.record_outcome("multi", -1.0)
        tracker.record_outcome("multi", -1.0)
        stats = tracker.get_stats("multi")
        self.assertEqual(stats.suspension_count, 1)

        # After cooldown expires, losses continue → triggers again
        # 3rd and 4th loss both trigger (consecutive_losses is 3 and 4)
        tracker.record_outcome("multi", -1.0)
        self.assertGreaterEqual(stats.suspension_count, 2)


class TestSummaryDict(unittest.TestCase):
    def test_serializable_output(self):
        tracker = StrategyTracker()
        tracker.record_outcome("consensus", 1.0)
        tracker.record_outcome("consensus", -0.5)
        tracker.record_outcome("momentum", -1.0)

        summary = tracker.summary_dict()
        self.assertIn("consensus", summary)
        self.assertIn("momentum", summary)
        self.assertEqual(summary["consensus"]["total_trades"], 2)
        self.assertEqual(summary["consensus"]["wins"], 1)
        self.assertAlmostEqual(summary["consensus"]["total_pnl"], 0.5)


class TestDrawdownTracking(unittest.TestCase):
    def test_peak_and_drawdown(self):
        tracker = StrategyTracker()
        tracker.record_outcome("test", 5.0)   # peak = 5
        tracker.record_outcome("test", -3.0)  # pnl = 2, dd = 3
        tracker.record_outcome("test", 2.0)   # pnl = 4, dd still 3
        tracker.record_outcome("test", -4.0)  # pnl = 0, dd = 5

        stats = tracker.get_stats("test")
        self.assertAlmostEqual(stats.peak_pnl, 5.0)
        self.assertAlmostEqual(stats.max_drawdown, 5.0)
        self.assertAlmostEqual(stats.current_drawdown, 5.0)


if __name__ == "__main__":
    unittest.main()
