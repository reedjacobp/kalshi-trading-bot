"""
KL-Divergence Cross-Asset Signal for Kalshi 15-Minute Crypto Markets

Detects mispricings between correlated assets (BTC, ETH, SOL) by
measuring how much their Kalshi contract price distributions diverge
from their historical correlation.

Core idea: BTC, ETH, and SOL are ~60-70% correlated. If BTC's 15-min
contract is priced at 80c YES but ETH's equivalent is at 50c YES,
that's a divergence from the norm — either ETH is underpriced or BTC
is overpriced. KL-divergence quantifies this gap.

Formula: D_KL(P||Q) = Σ P_i * log(P_i / Q_i)

Where P and Q are the implied probability distributions from contract
prices. High KL-divergence signals a potential mispricing to exploit.
"""

import math
import time
from collections import deque
from typing import Optional


# Default correlation coefficients (15-min windows)
# Used as fallback when insufficient data for rolling computation
DEFAULT_CORRELATION = {
    ("btc", "eth"): 0.67,
    ("btc", "sol"): 0.58,
    ("eth", "sol"): 0.62,
}

# Keep backward compat reference
CORRELATION = DEFAULT_CORRELATION


class KLDivergenceSignal:
    """
    Computes KL-divergence between correlated Kalshi contract prices
    to detect cross-asset mispricings.

    Usage:
        kl = KLDivergenceSignal()
        kl.update_price("btc", 75)  # BTC YES at 75c
        kl.update_price("eth", 45)  # ETH YES at 45c
        signal = kl.get_signal("eth")
        # -> ("yes", 0.12) meaning ETH YES is underpriced, KL=0.12
    """

    def __init__(
        self,
        kl_threshold: float = 0.08,       # Min KL to signal (lowered from typical 0.2)
        history_size: int = 480,           # ~4 hours at 30s intervals
        min_samples: int = 5,              # Need this many samples before signalling
        min_corr_samples: int = 30,        # Need this many for rolling correlation
        correlation_decay: float = 0.95,   # EMA decay for running correlation
    ):
        self.kl_threshold = kl_threshold
        self.history_size = history_size
        self.min_samples = min_samples
        self.min_corr_samples = min_corr_samples
        self.correlation_decay = correlation_decay

        # Current contract prices (YES side, in cents)
        self._prices: dict[str, int] = {}
        self._price_ts: dict[str, float] = {}

        # History of price pairs for running correlation
        self._pair_history: dict[tuple, deque] = {}
        for pair in DEFAULT_CORRELATION:
            self._pair_history[pair] = deque(maxlen=history_size)

        # Cache for rolling correlations
        self._rolling_correlations: dict[tuple, Optional[float]] = {}

    def update_price(self, asset: str, yes_price_cents: int):
        """Update the current contract price for an asset."""
        self._prices[asset] = yes_price_cents
        self._price_ts[asset] = time.time()

        # Record pair snapshots when both assets have recent prices
        now = time.time()
        for pair in CORRELATION:
            a, b = pair
            if a in self._prices and b in self._prices:
                if now - self._price_ts.get(a, 0) < 30 and \
                   now - self._price_ts.get(b, 0) < 30:
                    self._pair_history[pair].append((
                        self._prices[a],
                        self._prices[b],
                        now,
                    ))

    def _compute_rolling_correlation(self, asset_a: str, asset_b: str) -> Optional[float]:
        """
        Compute rolling Pearson correlation from price pair history.

        Uses return series (not price levels) to avoid spurious correlation
        from trending prices. Requires at least min_corr_samples data points.
        """
        pair = tuple(sorted([asset_a, asset_b]))
        history = self._pair_history.get(pair)
        if history is None or len(history) < self.min_corr_samples:
            return None

        # Extract price series
        prices_a = [h[0] for h in history]
        prices_b = [h[1] for h in history]

        if len(prices_a) < 2:
            return None

        # Compute returns
        returns_a = []
        returns_b = []
        for i in range(1, len(prices_a)):
            if prices_a[i - 1] > 0 and prices_b[i - 1] > 0:
                returns_a.append((prices_a[i] - prices_a[i - 1]) / prices_a[i - 1])
                returns_b.append((prices_b[i] - prices_b[i - 1]) / prices_b[i - 1])

        if len(returns_a) < self.min_corr_samples - 1:
            return None

        n = len(returns_a)
        mean_a = sum(returns_a) / n
        mean_b = sum(returns_b) / n

        cov = sum((returns_a[i] - mean_a) * (returns_b[i] - mean_b) for i in range(n)) / n
        var_a = sum((r - mean_a) ** 2 for r in returns_a) / n
        var_b = sum((r - mean_b) ** 2 for r in returns_b) / n

        if var_a <= 0 or var_b <= 0:
            return None

        corr = cov / (math.sqrt(var_a) * math.sqrt(var_b))
        # Clamp to [-1, 1] for numerical safety
        return max(-1.0, min(1.0, corr))

    def get_correlation(self, asset_a: str, asset_b: str) -> float:
        """
        Get the current correlation estimate between two assets.
        Uses rolling correlation if available, falls back to defaults.
        """
        pair = tuple(sorted([asset_a, asset_b]))
        rolling = self._compute_rolling_correlation(asset_a, asset_b)
        if rolling is not None:
            self._rolling_correlations[pair] = rolling
            return rolling
        # Fallback to cached rolling or default
        if pair in self._rolling_correlations and self._rolling_correlations[pair] is not None:
            return self._rolling_correlations[pair]
        return DEFAULT_CORRELATION.get(pair, DEFAULT_CORRELATION.get((pair[1], pair[0]), 0.5))

    def get_current_correlations(self) -> dict[tuple, dict]:
        """Return current correlation estimates for all pairs (for dashboard)."""
        result = {}
        for pair in DEFAULT_CORRELATION:
            rolling = self._compute_rolling_correlation(pair[0], pair[1])
            result[pair] = {
                "rolling": rolling,
                "default": DEFAULT_CORRELATION[pair],
                "active": rolling if rolling is not None else DEFAULT_CORRELATION[pair],
                "samples": len(self._pair_history.get(pair, [])),
            }
        return result

    def kl_divergence(self, asset_a: str, asset_b: str) -> Optional[float]:
        """
        Compute KL-divergence between two assets' implied distributions.

        Each asset's contract price implies P(yes), P(no) = [p, 1-p].
        Given their correlation, we'd expect similar distributions.
        KL measures how different they actually are.
        """
        if asset_a not in self._prices or asset_b not in self._prices:
            return None

        # Check freshness
        now = time.time()
        if now - self._price_ts.get(asset_a, 0) > 60:
            return None
        if now - self._price_ts.get(asset_b, 0) > 60:
            return None

        p_a = self._prices[asset_a] / 100.0  # Asset A's implied YES prob
        p_b = self._prices[asset_b] / 100.0  # Asset B's implied YES prob

        # Use rolling correlation with fallback to defaults
        corr = self.get_correlation(asset_a, asset_b)

        # Expected ETH YES prob given BTC YES prob and their correlation
        # Simplified model: E[p_b | p_a] = 0.5 + corr * (p_a - 0.5)
        expected_b = 0.5 + corr * (p_a - 0.5)
        expected_b = max(0.05, min(0.95, expected_b))

        # KL(actual_b || expected_b) — how surprising is actual_b given expected_b?
        actual_dist = [p_b, 1 - p_b]
        expected_dist = [expected_b, 1 - expected_b]

        kl = _kl_div(actual_dist, expected_dist)
        return kl

    def get_signal(self, target_asset: str) -> Optional[tuple[str, float]]:
        """
        Check if the target asset is mispriced relative to correlated assets.

        Returns:
            (direction, kl_score) where direction is "yes" or "no" and
            kl_score is the divergence magnitude.
            Returns None if no mispricing detected.
        """
        if target_asset not in self._prices:
            return None

        # Compare target against all correlated assets
        max_kl = 0.0
        best_direction = None

        for pair in DEFAULT_CORRELATION:
            if target_asset not in pair:
                continue

            other = pair[0] if pair[1] == target_asset else pair[1]
            if other not in self._prices:
                continue

            kl = self.kl_divergence(other, target_asset)
            if kl is None:
                continue

            if kl > max_kl:
                max_kl = kl

                # Determine direction of mispricing using rolling correlation
                corr = self.get_correlation(target_asset, other)
                p_other = self._prices[other] / 100.0
                p_target = self._prices[target_asset] / 100.0
                expected_target = 0.5 + corr * (p_other - 0.5)

                if p_target < expected_target:
                    # Target is underpriced on YES (should be higher)
                    best_direction = "yes"
                else:
                    # Target is overpriced on YES (NO is underpriced)
                    best_direction = "no"

        if max_kl >= self.kl_threshold and best_direction is not None:
            return (best_direction, max_kl)

        return None

    def get_confidence_boost(self, target_asset: str, proposed_direction: str) -> float:
        """
        Get a confidence boost (or penalty) for a proposed trade direction
        based on cross-asset KL-divergence.

        Returns a value to ADD to the strategy's confidence:
        - Positive if KL supports the proposed direction
        - Negative if KL contradicts it
        - 0 if no signal or insufficient data
        """
        signal = self.get_signal(target_asset)
        if signal is None:
            return 0.0

        direction, kl_score = signal

        # Scale the boost by KL magnitude (capped at 0.10)
        boost = min(0.10, kl_score * 0.5)

        if direction == proposed_direction:
            return boost   # Confirming — boost confidence
        else:
            return -boost  # Contradicting — reduce confidence


def _kl_div(p: list[float], q: list[float]) -> float:
    """
    Compute KL-divergence D_KL(P || Q).
    Clamps values to avoid log(0).
    """
    eps = 1e-10
    kl = 0.0
    for pi, qi in zip(p, q):
        pi = max(eps, min(1 - eps, pi))
        qi = max(eps, min(1 - eps, qi))
        kl += pi * math.log(pi / qi)
    return max(0.0, kl)
