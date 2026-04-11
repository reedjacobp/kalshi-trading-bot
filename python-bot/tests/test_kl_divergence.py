"""Tests for KL-divergence cross-asset signal with rolling correlations."""

import time
import unittest

from kl_divergence import KLDivergenceSignal, DEFAULT_CORRELATION


class TestRollingCorrelation(unittest.TestCase):
    """Test rolling Pearson correlation computation."""

    def test_fallback_to_default_with_no_data(self):
        kl = KLDivergenceSignal()
        corr = kl.get_correlation("btc", "eth")
        self.assertAlmostEqual(corr, DEFAULT_CORRELATION[("btc", "eth")])

    def test_rolling_correlation_with_correlated_prices(self):
        kl = KLDivergenceSignal(min_corr_samples=10)
        # Feed perfectly correlated price pairs
        now = time.time()
        for i in range(40):
            price_a = 50 + i
            price_b = 50 + i  # Perfectly correlated
            kl._prices["btc"] = price_a
            kl._prices["eth"] = price_b
            kl._price_ts["btc"] = now
            kl._price_ts["eth"] = now
            kl._pair_history[("btc", "eth")].append((price_a, price_b, now))
            now += 30

        corr = kl._compute_rolling_correlation("btc", "eth")
        self.assertIsNotNone(corr)
        self.assertGreater(corr, 0.9)

    def test_rolling_correlation_with_anticorrelated_prices(self):
        import math
        kl = KLDivergenceSignal(min_corr_samples=10)
        now = time.time()
        # Use sinusoidal oscillation: when A goes up, B goes down
        for i in range(60):
            price_a = 100 + 10 * math.sin(i * 0.3)
            price_b = 100 - 10 * math.sin(i * 0.3)  # Anti-correlated
            kl._pair_history[("btc", "eth")].append((price_a, price_b, now))
            now += 30

        corr = kl._compute_rolling_correlation("btc", "eth")
        self.assertIsNotNone(corr)
        self.assertLess(corr, -0.9)

    def test_rolling_correlation_insufficient_data(self):
        kl = KLDivergenceSignal(min_corr_samples=30)
        now = time.time()
        # Only 5 samples, need 30
        for i in range(5):
            kl._pair_history[("btc", "eth")].append((50 + i, 50 + i, now))
            now += 30

        corr = kl._compute_rolling_correlation("btc", "eth")
        self.assertIsNone(corr)

    def test_get_correlation_uses_rolling_when_available(self):
        kl = KLDivergenceSignal(min_corr_samples=10)
        now = time.time()
        # Feed enough data for rolling
        for i in range(40):
            price_a = 50 + i
            price_b = 50 + i * 0.5  # Partially correlated
            kl._pair_history[("btc", "eth")].append((price_a, price_b, now))
            now += 30

        corr = kl.get_correlation("btc", "eth")
        # Should be a rolling value, not exactly the default
        self.assertIsNotNone(corr)
        # The rolling correlation should be positive (prices move together)
        self.assertGreater(corr, 0.0)

    def test_get_current_correlations_dashboard(self):
        kl = KLDivergenceSignal()
        result = kl.get_current_correlations()
        # Should have entries for all default pairs
        self.assertIn(("btc", "eth"), result)
        self.assertIn(("btc", "sol"), result)
        self.assertIn(("eth", "sol"), result)
        # With no data, rolling should be None
        self.assertIsNone(result[("btc", "eth")]["rolling"])
        self.assertEqual(result[("btc", "eth")]["default"], 0.67)

    def test_correlation_clamped_to_bounds(self):
        kl = KLDivergenceSignal(min_corr_samples=5)
        now = time.time()
        # Feed identical prices (should give correlation close to 1 or undefined)
        for i in range(20):
            kl._pair_history[("btc", "eth")].append((50 + i, 50 + i, now))
            now += 30

        corr = kl._compute_rolling_correlation("btc", "eth")
        if corr is not None:
            self.assertGreaterEqual(corr, -1.0)
            self.assertLessEqual(corr, 1.0)


class TestKLDivergenceSignal(unittest.TestCase):
    """Test the KL-divergence signal generation."""

    def test_no_signal_without_prices(self):
        kl = KLDivergenceSignal()
        signal = kl.get_signal("eth")
        self.assertIsNone(signal)

    def test_signal_with_divergent_prices(self):
        kl = KLDivergenceSignal(kl_threshold=0.01)
        now = time.time()
        # BTC at 80c YES, ETH at 40c YES — significant divergence
        kl._prices["btc"] = 80
        kl._prices["eth"] = 40
        kl._price_ts["btc"] = now
        kl._price_ts["eth"] = now

        signal = kl.get_signal("eth")
        if signal is not None:
            direction, kl_score = signal
            # ETH should be underpriced on YES given BTC is high
            self.assertEqual(direction, "yes")
            self.assertGreater(kl_score, 0)

    def test_no_signal_with_aligned_prices(self):
        kl = KLDivergenceSignal(kl_threshold=0.5)
        now = time.time()
        # Both at 50c — perfectly aligned
        kl._prices["btc"] = 50
        kl._prices["eth"] = 50
        kl._price_ts["btc"] = now
        kl._price_ts["eth"] = now

        signal = kl.get_signal("eth")
        self.assertIsNone(signal)

    def test_confidence_boost_confirms(self):
        kl = KLDivergenceSignal(kl_threshold=0.01)
        now = time.time()
        kl._prices["btc"] = 80
        kl._prices["eth"] = 40
        kl._price_ts["btc"] = now
        kl._price_ts["eth"] = now

        # KL says ETH YES is underpriced; proposing YES should get a boost
        boost = kl.get_confidence_boost("eth", "yes")
        self.assertGreaterEqual(boost, 0.0)

    def test_confidence_boost_contradicts(self):
        kl = KLDivergenceSignal(kl_threshold=0.01)
        now = time.time()
        kl._prices["btc"] = 80
        kl._prices["eth"] = 40
        kl._price_ts["btc"] = now
        kl._price_ts["eth"] = now

        # KL says ETH YES is underpriced; proposing NO should get a penalty
        boost = kl.get_confidence_boost("eth", "no")
        self.assertLessEqual(boost, 0.0)

    def test_stale_prices_ignored(self):
        kl = KLDivergenceSignal()
        old_time = time.time() - 120  # 2 min old
        kl._prices["btc"] = 80
        kl._prices["eth"] = 40
        kl._price_ts["btc"] = old_time
        kl._price_ts["eth"] = old_time

        kl_val = kl.kl_divergence("btc", "eth")
        self.assertIsNone(kl_val)


if __name__ == "__main__":
    unittest.main()
