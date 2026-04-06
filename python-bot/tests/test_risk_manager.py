"""Tests for the risk manager: fee calculations, position sizing, trade approval, settlement."""

import time
import unittest

from risk_manager import RiskConfig, RiskManager, TradeRecord, kalshi_taker_fee


class TestKalshiTakerFee(unittest.TestCase):
    """Test the Kalshi taker fee formula: 0.07 * contracts * P * (1-P), rounded up."""

    def test_fee_at_50_cents(self):
        # P=0.50, 1-P=0.50 → 0.07 * 1 * 0.25 = 0.0175 → ceil to $0.02
        self.assertEqual(kalshi_taker_fee(1, 50), 0.02)

    def test_fee_at_50_cents_10_contracts(self):
        # 0.07 * 10 * 0.25 = 0.175 → ceil to $0.18
        self.assertEqual(kalshi_taker_fee(10, 50), 0.18)

    def test_fee_at_extreme_price(self):
        # P=0.95 → 0.07 * 1 * 0.95 * 0.05 = 0.003325 → ceil to $0.01
        self.assertEqual(kalshi_taker_fee(1, 95), 0.01)

    def test_fee_at_low_price(self):
        # P=0.10 → 0.07 * 1 * 0.10 * 0.90 = 0.0063 → ceil to $0.01
        self.assertEqual(kalshi_taker_fee(1, 10), 0.01)

    def test_fee_symmetry(self):
        # Fee at 30c should equal fee at 70c (same P*(1-P))
        self.assertEqual(kalshi_taker_fee(1, 30), kalshi_taker_fee(1, 70))

    def test_fee_zero_contracts(self):
        self.assertEqual(kalshi_taker_fee(0, 50), 0.0)

    def test_fee_scales_with_contracts(self):
        fee_1 = kalshi_taker_fee(1, 40)
        fee_5 = kalshi_taker_fee(5, 40)
        # 5 contracts should cost more than 1
        self.assertGreater(fee_5, fee_1)


class TestCalculateContracts(unittest.TestCase):
    """Test Kelly-criterion position sizing."""

    def setUp(self):
        self.config = RiskConfig(stake_usd=5.00, kelly_fraction=0.25, max_position_pct=0.05)
        self.rm = RiskManager(self.config)

    def test_minimum_one_contract(self):
        # Even with tiny balance, should get at least 1 contract
        contracts = self.rm.calculate_contracts(50, confidence=0.6, balance_usd=1.0)
        self.assertGreaterEqual(contracts, 1)

    def test_maximum_100_contracts(self):
        contracts = self.rm.calculate_contracts(1, confidence=0.99, balance_usd=100000.0)
        self.assertLessEqual(contracts, 100)

    def test_zero_at_boundary_prices(self):
        self.assertEqual(self.rm.calculate_contracts(0), 0)
        self.assertEqual(self.rm.calculate_contracts(100), 0)

    def test_fixed_stake_fallback(self):
        # No confidence or balance → falls back to fixed stake
        contracts = self.rm.calculate_contracts(50)
        expected = int(5.00 / 0.50)  # 10
        self.assertEqual(contracts, expected)

    def test_kelly_reduces_with_low_confidence(self):
        # Low confidence should size smaller than high confidence
        low = self.rm.calculate_contracts(50, confidence=0.55, balance_usd=100.0)
        high = self.rm.calculate_contracts(50, confidence=0.90, balance_usd=100.0)
        self.assertLessEqual(low, high)

    def test_capped_at_max_position_pct(self):
        # With huge confidence, stake is capped at 5% of balance
        contracts = self.rm.calculate_contracts(10, confidence=0.99, balance_usd=100.0)
        max_stake = 100.0 * 0.05  # $5
        actual_stake = contracts * 0.10
        self.assertLessEqual(actual_stake, max_stake + 0.10)  # allow 1 contract rounding

    def test_capped_at_fixed_stake_ceiling(self):
        # Kelly can't exceed the fixed stake_usd ceiling
        contracts = self.rm.calculate_contracts(10, confidence=0.99, balance_usd=10000.0)
        actual_stake = contracts * 0.10
        self.assertLessEqual(actual_stake, self.config.stake_usd + 0.10)


class TestApproveTrade(unittest.TestCase):
    """Test trade approval logic with all risk filters."""

    def setUp(self):
        self.config = RiskConfig(
            stake_usd=5.00,
            max_daily_loss_usd=25.00,
            max_weekly_loss_usd=75.00,
            max_concurrent_positions=3,
            min_confidence=0.3,
            cooldown_after_loss_secs=60,
            max_trades_per_hour=20,
        )
        self.rm = RiskManager(self.config)

    def test_approve_valid_trade(self):
        approved, reason = self.rm.approve_trade("TICKER-1", "consensus", "yes", 0.6, 50)
        self.assertTrue(approved)
        self.assertEqual(reason, "Approved")

    def test_approve_kelly_sized_within_balance(self):
        # Reproduces bug: $5 fixed stake > 5% of $69.92 ($3.50), but Kelly
        # sizing would produce a ~$2.80 position which IS within limits.
        # The old code rejected this because it compared the fixed ceiling
        # against the balance cap instead of the actual Kelly-sized stake.
        approved, reason = self.rm.approve_trade(
            "TICKER-FAV", "favorite_bias", "yes",
            confidence=0.80, price_cents=75, balance_usd=69.92,
        )
        self.assertTrue(approved, f"Should approve Kelly-sized trade, got: {reason}")

    def test_reject_low_confidence(self):
        approved, reason = self.rm.approve_trade("TICKER-1", "consensus", "yes", 0.1, 50)
        self.assertFalse(approved)
        self.assertIn("Confidence", reason)

    def test_reject_max_positions(self):
        # Fill up positions
        for i in range(3):
            record = TradeRecord(
                timestamp=time.time(), ticker=f"TICK-{i}", strategy="test",
                side="yes", price_cents=50, contracts=1, stake_usd=0.50,
            )
            self.rm.record_trade(record)

        approved, reason = self.rm.approve_trade("TICK-NEW", "consensus", "yes", 0.6, 50)
        self.assertFalse(approved)
        self.assertIn("Max positions", reason)

    def test_reject_duplicate_ticker(self):
        record = TradeRecord(
            timestamp=time.time(), ticker="TICK-DUP", strategy="test",
            side="yes", price_cents=50, contracts=1, stake_usd=0.50,
        )
        self.rm.record_trade(record)

        approved, reason = self.rm.approve_trade("TICK-DUP", "consensus", "yes", 0.6, 50)
        self.assertFalse(approved)
        self.assertIn("Already have position", reason)

    def test_reject_daily_loss_limit(self):
        # Simulate losses exceeding daily limit
        for i in range(6):
            record = TradeRecord(
                timestamp=time.time(), ticker=f"LOSS-{i}", strategy="test",
                side="yes", price_cents=50, contracts=10, stake_usd=5.00,
            )
            self.rm.record_trade(record)
            self.rm.settle_trade(f"LOSS-{i}", "no")  # all losses

        approved, reason = self.rm.approve_trade("TICK-NEW", "consensus", "yes", 0.6, 50)
        self.assertFalse(approved)
        self.assertIn("loss limit", reason.lower())

    def test_reject_loss_cooldown(self):
        record = TradeRecord(
            timestamp=time.time(), ticker="LOSS-1", strategy="test",
            side="yes", price_cents=50, contracts=1, stake_usd=0.50,
        )
        self.rm.record_trade(record)
        self.rm.settle_trade("LOSS-1", "no")

        approved, reason = self.rm.approve_trade("TICK-NEW", "consensus", "yes", 0.6, 50)
        self.assertFalse(approved)
        self.assertIn("cooldown", reason.lower())

    def test_approve_after_cooldown_expires(self):
        record = TradeRecord(
            timestamp=time.time(), ticker="LOSS-1", strategy="test",
            side="yes", price_cents=50, contracts=1, stake_usd=0.50,
        )
        self.rm.record_trade(record)
        self.rm.settle_trade("LOSS-1", "no")
        # Manually expire the cooldown
        self.rm._last_loss_ts = time.time() - 61

        approved, _ = self.rm.approve_trade("TICK-NEW", "consensus", "yes", 0.6, 50)
        self.assertTrue(approved)


class TestSettlement(unittest.TestCase):
    """Test trade settlement and P&L tracking."""

    def setUp(self):
        self.rm = RiskManager(RiskConfig())

    def test_win_settlement(self):
        record = TradeRecord(
            timestamp=time.time(), ticker="WIN-1", strategy="test",
            side="yes", price_cents=40, contracts=5, stake_usd=2.00,
        )
        self.rm.record_trade(record)
        self.rm.settle_trade("WIN-1", "yes")

        self.assertEqual(record.outcome, "win")
        self.assertEqual(record.payout_usd, 5.00)
        self.assertEqual(record.profit_usd, 3.00)
        self.assertGreater(record.entry_fee_usd, 0)
        self.assertGreater(record.settle_fee_usd, 0)
        self.assertAlmostEqual(
            record.profit_after_fees,
            record.profit_usd - record.entry_fee_usd - record.settle_fee_usd,
        )

    def test_loss_settlement(self):
        record = TradeRecord(
            timestamp=time.time(), ticker="LOSS-1", strategy="test",
            side="yes", price_cents=40, contracts=5, stake_usd=2.00,
        )
        self.rm.record_trade(record)
        self.rm.settle_trade("LOSS-1", "no")

        self.assertEqual(record.outcome, "loss")
        self.assertEqual(record.payout_usd, 0.0)
        self.assertEqual(record.profit_usd, -2.00)
        self.assertEqual(record.settle_fee_usd, 0.0)

    def test_settle_removes_from_open_positions(self):
        record = TradeRecord(
            timestamp=time.time(), ticker="POS-1", strategy="test",
            side="yes", price_cents=50, contracts=1, stake_usd=0.50,
        )
        self.rm.record_trade(record)
        self.assertIn("POS-1", self.rm.open_positions)

        self.rm.settle_trade("POS-1", "yes")
        self.assertNotIn("POS-1", self.rm.open_positions)

    def test_settle_unknown_ticker_is_noop(self):
        self.rm.settle_trade("UNKNOWN", "yes")  # should not raise

    def test_pnl_tracking(self):
        # Win a trade
        r1 = TradeRecord(
            timestamp=time.time(), ticker="T-1", strategy="test",
            side="yes", price_cents=40, contracts=2, stake_usd=0.80,
        )
        self.rm.record_trade(r1)
        self.rm.settle_trade("T-1", "yes")

        # Lose a trade
        r2 = TradeRecord(
            timestamp=time.time(), ticker="T-2", strategy="test",
            side="yes", price_cents=50, contracts=2, stake_usd=1.00,
        )
        self.rm.record_trade(r2)
        self.rm.settle_trade("T-2", "no")

        # Net P&L should be win profit + loss
        expected = r1.profit_usd + r2.profit_usd
        self.assertAlmostEqual(self.rm.total_pnl, expected)


class TestWinRate(unittest.TestCase):
    """Test win rate calculation."""

    def test_no_trades(self):
        rm = RiskManager()
        self.assertIsNone(rm.win_rate)

    def test_all_wins(self):
        rm = RiskManager()
        for i in range(3):
            r = TradeRecord(
                timestamp=time.time(), ticker=f"W-{i}", strategy="test",
                side="yes", price_cents=50, contracts=1, stake_usd=0.50,
            )
            rm.record_trade(r)
            rm.settle_trade(f"W-{i}", "yes")
        self.assertAlmostEqual(rm.win_rate, 1.0)

    def test_mixed_results(self):
        rm = RiskManager()
        # 2 wins, 1 loss
        for i, result in enumerate(["yes", "yes", "no"]):
            r = TradeRecord(
                timestamp=time.time(), ticker=f"M-{i}", strategy="test",
                side="yes", price_cents=50, contracts=1, stake_usd=0.50,
            )
            rm.record_trade(r)
            rm.settle_trade(f"M-{i}", result)
        self.assertAlmostEqual(rm.win_rate, 2 / 3)

    def test_pending_trades_excluded(self):
        rm = RiskManager()
        r = TradeRecord(
            timestamp=time.time(), ticker="P-1", strategy="test",
            side="yes", price_cents=50, contracts=1, stake_usd=0.50,
        )
        rm.record_trade(r)
        # Trade is pending (not settled) — win_rate should be None
        self.assertIsNone(rm.win_rate)


class TestRateLimit(unittest.TestCase):
    """Test hourly trade rate limiting."""

    def test_rate_limit_rejects(self):
        config = RiskConfig(max_trades_per_hour=2, cooldown_after_loss_secs=0)
        rm = RiskManager(config)

        for i in range(2):
            r = TradeRecord(
                timestamp=time.time(), ticker=f"RL-{i}", strategy="test",
                side="yes", price_cents=50, contracts=1, stake_usd=0.50,
            )
            rm.record_trade(r)

        approved, reason = rm.approve_trade("RL-NEW", "test", "yes", 0.6, 50)
        self.assertFalse(approved)
        self.assertIn("trade limit", reason.lower())


if __name__ == "__main__":
    unittest.main()
