"""Tests for the paper trading slippage model."""

import unittest

from slippage_model import SlippageModel, FillResult


class TestFillProbability(unittest.TestCase):
    def test_aggressive_order_usually_fills(self):
        """Order well above the ask should almost always fill."""
        model = SlippageModel(seed=42)
        fills = 0
        for _ in range(100):
            result = model.simulate_fill("yes", 55, yes_bid=48, yes_ask=52, contracts=5)
            if result.filled:
                fills += 1
        # Crossing spread by 3c should fill >80% of the time
        self.assertGreater(fills, 80)

    def test_passive_order_rarely_fills(self):
        """Order well below the ask should rarely fill."""
        model = SlippageModel(seed=42)
        fills = 0
        for _ in range(100):
            result = model.simulate_fill("yes", 42, yes_bid=48, yes_ask=52, contracts=5)
            if result.filled:
                fills += 1
        # 10c below ask should fill <30% of the time
        self.assertLess(fills, 30)

    def test_at_ask_has_base_fill_rate(self):
        """Order exactly at the ask should fill at ~base_fill_rate."""
        model = SlippageModel(base_fill_rate=0.85, seed=42)
        fills = 0
        n = 1000
        for _ in range(n):
            result = model.simulate_fill("yes", 52, yes_bid=48, yes_ask=52, contracts=5)
            if result.filled:
                fills += 1
        rate = fills / n
        # Should be close to 85% ± margin
        self.assertGreater(rate, 0.70)
        self.assertLess(rate, 0.95)


class TestSlippage(unittest.TestCase):
    def test_fill_price_at_least_requested(self):
        """Fill price should be >= requested (worse for buyer)."""
        model = SlippageModel(spread_slippage_max=3, seed=42)
        for _ in range(50):
            result = model.simulate_fill("yes", 50, yes_bid=45, yes_ask=50, contracts=5)
            if result.filled:
                self.assertGreaterEqual(result.fill_price_cents, 50)

    def test_fill_price_capped_at_99(self):
        model = SlippageModel(spread_slippage_max=5, seed=42)
        result = model.simulate_fill("yes", 98, yes_bid=95, yes_ask=98, contracts=1)
        if result.filled:
            self.assertLessEqual(result.fill_price_cents, 99)


class TestPartialFills(unittest.TestCase):
    def test_partial_fills_occur(self):
        """With enough trials, some partial fills should occur."""
        model = SlippageModel(partial_fill_rate=0.50, seed=42)  # 50% partial for testing
        partials = 0
        for _ in range(100):
            result = model.simulate_fill("yes", 55, yes_bid=48, yes_ask=52, contracts=10)
            if result.filled and result.contracts_filled < 10:
                partials += 1
        self.assertGreater(partials, 5)

    def test_partial_fill_at_least_one_contract(self):
        model = SlippageModel(partial_fill_rate=1.0, seed=42)  # Always partial
        for _ in range(20):
            result = model.simulate_fill("yes", 55, yes_bid=48, yes_ask=52, contracts=10)
            if result.filled:
                self.assertGreaterEqual(result.contracts_filled, 1)

    def test_single_contract_never_partial(self):
        """With 1 contract, partial fill is impossible."""
        model = SlippageModel(partial_fill_rate=1.0, seed=42)
        for _ in range(20):
            result = model.simulate_fill("yes", 55, yes_bid=48, yes_ask=52, contracts=1)
            if result.filled:
                self.assertEqual(result.contracts_filled, 1)


class TestEdgeCases(unittest.TestCase):
    def test_no_orderbook(self):
        model = SlippageModel()
        result = model.simulate_fill("yes", 50, yes_bid=None, yes_ask=None, contracts=5)
        self.assertFalse(result.filled)
        self.assertEqual(result.status, "cancelled")

    def test_zero_contracts(self):
        model = SlippageModel()
        result = model.simulate_fill("yes", 50, yes_bid=45, yes_ask=50, contracts=0)
        self.assertFalse(result.filled)

    def test_no_side(self):
        """Buying NO uses 100 - yes_bid as the effective ask."""
        model = SlippageModel(seed=42, base_fill_rate=1.0)
        result = model.simulate_fill("no", 55, yes_bid=45, yes_ask=50, contracts=5)
        # NO ask = 100 - 45 = 55, our price is 55 (at ask)
        self.assertTrue(result.filled)

    def test_deterministic_with_seed(self):
        """Same seed should produce same results."""
        results1 = []
        results2 = []
        for _ in range(10):
            m1 = SlippageModel(seed=123)
            m2 = SlippageModel(seed=123)
            r1 = m1.simulate_fill("yes", 50, yes_bid=45, yes_ask=50, contracts=5)
            r2 = m2.simulate_fill("yes", 50, yes_bid=45, yes_ask=50, contracts=5)
            self.assertEqual(r1.filled, r2.filled)
            if r1.filled:
                self.assertEqual(r1.fill_price_cents, r2.fill_price_cents)


if __name__ == "__main__":
    unittest.main()
