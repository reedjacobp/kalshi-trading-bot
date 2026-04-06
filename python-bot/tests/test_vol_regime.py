"""Tests for the volatility regime detector."""

import time
import unittest
from unittest.mock import MagicMock

from vol_regime import VolRegime, VolRegimeDetector


class TestVolRegimeDetector(unittest.TestCase):

    def _mock_feed(self, vol_value):
        feed = MagicMock()
        feed.volatility.return_value = vol_value
        return feed

    def test_low_vol_regime(self):
        detector = VolRegimeDetector()
        feed = self._mock_feed(0.01)
        # Need 3 consecutive readings to switch
        for _ in range(4):
            regime = detector.detect(feed)
        self.assertEqual(regime, VolRegime.LOW)

    def test_high_vol_regime(self):
        detector = VolRegimeDetector()
        feed = self._mock_feed(0.15)
        for _ in range(4):
            regime = detector.detect(feed)
        self.assertEqual(regime, VolRegime.HIGH)

    def test_medium_vol_regime(self):
        detector = VolRegimeDetector()
        feed = self._mock_feed(0.05)
        for _ in range(4):
            regime = detector.detect(feed)
        self.assertEqual(regime, VolRegime.MEDIUM)

    def test_hysteresis_prevents_flipping(self):
        detector = VolRegimeDetector()
        # Start in medium
        feed_med = self._mock_feed(0.05)
        for _ in range(4):
            detector.detect(feed_med)

        # One high reading shouldn't switch
        feed_high = self._mock_feed(0.15)
        regime = detector.detect(feed_high)
        self.assertEqual(regime, VolRegime.MEDIUM)

        # Two high readings shouldn't switch
        regime = detector.detect(feed_high)
        self.assertEqual(regime, VolRegime.MEDIUM)

        # Third switches
        regime = detector.detect(feed_high)
        self.assertEqual(regime, VolRegime.HIGH)

    def test_none_vol_keeps_last_regime(self):
        detector = VolRegimeDetector()
        feed = self._mock_feed(None)
        regime = detector.detect(feed)
        self.assertEqual(regime, VolRegime.MEDIUM)  # default

    def test_params_low_disables_momentum(self):
        detector = VolRegimeDetector()
        feed = self._mock_feed(0.01)
        for _ in range(4):
            detector.detect(feed)
        params = detector.get_params(feed)
        self.assertFalse(params.momentum_enabled)
        self.assertTrue(params.fav_bias_enabled)

    def test_params_high_disables_fav_bias(self):
        detector = VolRegimeDetector()
        feed = self._mock_feed(0.15)
        for _ in range(4):
            detector.detect(feed)
        params = detector.get_params(feed)
        self.assertFalse(params.fav_bias_enabled)
        self.assertTrue(params.momentum_enabled)

    def test_params_high_widens_stops(self):
        detector = VolRegimeDetector()
        feed_med = self._mock_feed(0.05)
        for _ in range(4):
            detector.detect(feed_med)
        med_params = detector.get_params(feed_med)

        detector2 = VolRegimeDetector()
        feed_high = self._mock_feed(0.15)
        for _ in range(4):
            detector2.detect(feed_high)
        high_params = detector2.get_params(feed_high)

        self.assertGreater(high_params.stop_loss_cents, med_params.stop_loss_cents)
        self.assertLess(high_params.kelly_fraction, med_params.kelly_fraction)


if __name__ == "__main__":
    unittest.main()
