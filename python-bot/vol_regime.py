"""
Volatility Regime Detector for Kalshi 15-Minute Crypto Markets

Classifies the current market into one of three regimes based on
recent price volatility, then provides parameter overrides for
strategies and risk management.

Regimes:
- LOW:  Quiet market. Favorites hold reliably. Tighten stops,
        enable favorite bias, skip momentum (no moves to ride).
- MED:  Normal conditions. Run all strategies with default params.
- HIGH: Volatile market. Widen stops (avoid noise-triggered exits),
        skip favorite bias (favorites blow up), enable momentum
        (trends are stronger and more persistent).

The detector uses a rolling 5-minute volatility reading from the
PriceFeed and classifies it against calibrated thresholds.
"""

import logging
from enum import Enum
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("kalshi_bot")


class VolRegime(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class RegimeParams:
    """Strategy parameter overrides for the current regime."""
    regime: VolRegime

    # Favorite bias
    fav_bias_enabled: bool
    fav_min_favorite: int       # min_favorite_price override
    fav_max_entry: int          # max_entry_price override

    # Momentum
    momentum_enabled: bool
    momentum_threshold: float   # min_momentum_pct override

    # Consensus
    consensus_enabled: bool
    consensus_min_edge: float   # min_edge override

    # Early exit
    stop_loss_cents: int
    trailing_distance: int
    trailing_activation: int

    # Risk
    kelly_fraction: float       # Kelly multiplier override
    max_position_pct: float     # Max % of balance per trade


# Calibrated thresholds for BTC 5-min vol (std of returns * 100)
# These should be tuned over time based on observed distributions.
# Typical crypto 5-min vol: 0.01-0.03% = low, 0.03-0.08% = med, >0.08% = high
VOL_LOW_THRESHOLD = 0.03   # Below this = low vol
VOL_HIGH_THRESHOLD = 0.08  # Above this = high vol


class VolRegimeDetector:
    """
    Detects the current volatility regime from PriceFeed data and
    returns parameter overrides for all strategies.
    """

    def __init__(
        self,
        low_threshold: float = VOL_LOW_THRESHOLD,
        high_threshold: float = VOL_HIGH_THRESHOLD,
    ):
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        self._last_regime = VolRegime.MEDIUM
        self._regime_hold_count = 0  # Prevent rapid flipping

    def detect(self, price_feed) -> VolRegime:
        """
        Classify the current volatility regime.

        Uses 5-minute volatility from the price feed. Requires a minimum
        hold of 3 ticks before switching regimes (hysteresis).
        """
        vol = price_feed.volatility(lookback_seconds=300)
        if vol is None:
            return self._last_regime

        if vol < self.low_threshold:
            new_regime = VolRegime.LOW
        elif vol > self.high_threshold:
            new_regime = VolRegime.HIGH
        else:
            new_regime = VolRegime.MEDIUM

        # Hysteresis: require 3 consecutive readings in the new regime
        if new_regime != self._last_regime:
            self._regime_hold_count += 1
            if self._regime_hold_count >= 3:
                logger.info(f"[VOL] Regime change: {self._last_regime.value} -> {new_regime.value} (vol={vol:.4f}%)")
                self._last_regime = new_regime
                self._regime_hold_count = 0
        else:
            self._regime_hold_count = 0

        return self._last_regime

    def get_params(self, price_feed) -> RegimeParams:
        """
        Detect regime and return the full set of parameter overrides.
        """
        regime = self.detect(price_feed)

        if regime == VolRegime.LOW:
            return RegimeParams(
                regime=regime,
                # Low vol: favorites hold well, bias works
                fav_bias_enabled=True,
                fav_min_favorite=72,      # Can enter slightly softer favorites
                fav_max_entry=82,
                # Low vol: momentum doesn't work (no moves)
                momentum_enabled=False,
                momentum_threshold=0.05,
                # Consensus: works well in quiet markets
                consensus_enabled=True,
                consensus_min_edge=0.10,
                # Tighter stops (less noise to dodge)
                stop_loss_cents=10,
                trailing_distance=4,
                trailing_activation=8,
                # Slightly more aggressive sizing (higher hit rate expected)
                kelly_fraction=0.30,
                max_position_pct=0.05,
            )

        elif regime == VolRegime.HIGH:
            return RegimeParams(
                regime=regime,
                # High vol: favorites blow up, disable bias
                fav_bias_enabled=False,
                fav_min_favorite=85,      # Only extreme favorites if re-enabled
                fav_max_entry=88,
                # High vol: momentum works well (trends persist)
                momentum_enabled=True,
                momentum_threshold=0.03,  # Lower threshold — moves are real
                # High vol: consensus edge evaporates (84% WR in low activity → 61% in high)
                consensus_enabled=False,
                consensus_min_edge=0.12,
                # Wider stops (avoid noise-triggered exits)
                stop_loss_cents=22,
                trailing_distance=8,
                trailing_activation=14,
                # More conservative sizing (bigger swings)
                kelly_fraction=0.15,
                max_position_pct=0.03,
            )

        else:  # MEDIUM
            return RegimeParams(
                regime=regime,
                fav_bias_enabled=True,
                fav_min_favorite=75,
                fav_max_entry=80,
                momentum_enabled=True,
                momentum_threshold=0.05,
                consensus_enabled=True,
                consensus_min_edge=0.10,
                stop_loss_cents=15,
                trailing_distance=5,
                trailing_activation=10,
                kelly_fraction=0.25,
                max_position_pct=0.05,
            )
