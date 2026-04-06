"""
Bayesian Probability Updater for Kalshi 15-Minute Crypto Markets

Instead of estimating probability once at entry and never updating,
this module continuously updates our belief about a market's outcome
using incoming price evidence (BTC ticks, momentum shifts, vol changes).

Based on Bayes' theorem:  P(H|E) = P(E|H) * P(H) / P(E)

Applied to our 15-min markets:
- H = "BTC will be above the strike at expiry"
- E = new price tick, momentum reading, or vol change
- Prior = our initial confidence estimate from the strategy
- Posterior = updated confidence after seeing new evidence

This feeds into:
1. Better entry timing — wait for posterior to peak before entering
2. Dynamic position sizing — adjust Kelly fraction as confidence shifts
3. Smarter exits — if posterior drops below breakeven, exit early
"""

import math
import time
from collections import deque
from typing import Optional


class BayesianUpdater:
    """
    Maintains a running posterior probability for each tracked market.

    Uses a simplified Bayesian model where each new price tick provides
    evidence about the likely outcome. The strength of evidence depends
    on how far price has moved relative to the strike and how much time
    remains.
    """

    def __init__(
        self,
        evidence_weight: float = 0.15,   # How much each tick shifts the posterior
        momentum_weight: float = 0.10,   # Extra weight for momentum confirmation
        decay_rate: float = 0.02,        # Posterior decays toward 0.5 without new evidence
        min_ticks_for_update: int = 3,   # Need at least this many ticks before updating
    ):
        self.evidence_weight = evidence_weight
        self.momentum_weight = momentum_weight
        self.decay_rate = decay_rate
        self.min_ticks_for_update = min_ticks_for_update
        # ticker -> tracking state
        self._states: dict[str, dict] = {}

    def register(
        self,
        ticker: str,
        prior: float,
        direction: str,
        entry_btc_price: float,
        strike_implied_move_pct: float = 0.0,
    ):
        """
        Start tracking a market with an initial prior probability.

        Args:
            ticker: Market ticker
            prior: Initial probability estimate (from strategy confidence)
            direction: "yes" or "no" — which side we're considering/holding
            entry_btc_price: BTC price when we started tracking
            strike_implied_move_pct: How much BTC needs to move for the
                strike to be hit (0 = at-the-money)
        """
        self._states[ticker] = {
            "prior": prior,
            "posterior": prior,
            "direction": direction,
            "entry_price": entry_btc_price,
            "strike_move_pct": strike_implied_move_pct,
            "ticks": deque(maxlen=60),  # Recent evidence ticks
            "last_update": time.time(),
            "peak_posterior": prior,
        }

    def update(
        self,
        ticker: str,
        btc_price: float,
        momentum_1m: Optional[float] = None,
        seconds_remaining: float = 900,
        contract_price_cents: Optional[int] = None,
    ) -> Optional[float]:
        """
        Update the posterior probability given new evidence.

        Returns the updated posterior, or None if ticker isn't tracked.
        """
        state = self._states.get(ticker)
        if state is None:
            return None

        now = time.time()
        dt = now - state["last_update"]
        state["last_update"] = now

        # Evidence 1: Price movement relative to entry
        # If we're YES and BTC is rising, that's confirming evidence
        price_change_pct = (btc_price - state["entry_price"]) / state["entry_price"] * 100
        if state["direction"] == "no":
            price_change_pct = -price_change_pct  # Flip for NO positions

        state["ticks"].append((now, price_change_pct))

        if len(state["ticks"]) < self.min_ticks_for_update:
            return state["posterior"]

        # Kalman gain scheduling: how much to trust live observation vs prior model.
        # Inspired by weather prediction systems that blend forecast with real-time
        # measurement, increasing observation weight as more data arrives.
        #
        # At window open (900s left):  kalman_gain = 0.15 (trust prior model 85%)
        # At mid-window (450s left):   kalman_gain = 0.50 (equal blend)
        # At 2 min left (120s left):   kalman_gain = 0.80 (trust observation 80%)
        # At 30s left:                 kalman_gain = 0.95 (nearly all observation)
        #
        # This is better than a linear time_factor because it controls the
        # blend ratio directly rather than scaling evidence magnitude.
        elapsed_frac = max(0, min(1, 1 - seconds_remaining / 900))
        kalman_gain = 0.15 + 0.80 * (elapsed_frac ** 1.5)  # Convex curve — ramps faster near end
        kalman_gain = min(0.95, kalman_gain)

        # Evidence 1: Price movement (live observation)
        observation_log_odds = price_change_pct * self.evidence_weight

        # Evidence 2: Momentum confirmation (live observation)
        if momentum_1m is not None:
            mom_confirms = (momentum_1m > 0 and state["direction"] == "yes") or \
                          (momentum_1m < 0 and state["direction"] == "no")
            mom_strength = abs(momentum_1m)
            if mom_confirms:
                observation_log_odds += mom_strength * self.momentum_weight * 10
            else:
                observation_log_odds -= mom_strength * self.momentum_weight * 10

        # Evidence 3: Contract price (market's own assessment — blend separately)
        if contract_price_cents is not None:
            market_prob = contract_price_cents / 100.0
            if state["direction"] == "no":
                market_prob = 1 - market_prob
            market_log_odds = _prob_to_log_odds(market_prob)
            our_log_odds = _prob_to_log_odds(state["posterior"])
            # Market weight also increases with Kalman gain (market knows more near expiry)
            market_blend = kalman_gain * 0.3  # Up to 28.5% market weight near expiry
            observation_log_odds += (market_log_odds - our_log_odds) * market_blend

        # Apply Kalman-weighted update: blend prior model with live observation
        evidence_log_odds = observation_log_odds * kalman_gain

        # Apply Bayesian update via log-odds
        prior_log_odds = _prob_to_log_odds(state["posterior"])
        posterior_log_odds = prior_log_odds + evidence_log_odds

        # Decay toward prior when evidence is weak (prevents runaway drift)
        if abs(evidence_log_odds) < 0.01 and dt > 5:
            prior_log_odds_orig = _prob_to_log_odds(state["prior"])
            posterior_log_odds = (
                posterior_log_odds * (1 - self.decay_rate) +
                prior_log_odds_orig * self.decay_rate
            )

        # Convert back to probability, clamped to [0.05, 0.95]
        posterior = _log_odds_to_prob(posterior_log_odds)
        posterior = max(0.05, min(0.95, posterior))

        state["posterior"] = posterior
        state["peak_posterior"] = max(state["peak_posterior"], posterior)

        return posterior

    def get_posterior(self, ticker: str) -> Optional[float]:
        """Get the current posterior probability for a ticker."""
        state = self._states.get(ticker)
        return state["posterior"] if state else None

    def get_peak_posterior(self, ticker: str) -> Optional[float]:
        """Get the highest posterior seen for a ticker."""
        state = self._states.get(ticker)
        return state["peak_posterior"] if state else None

    def should_enter(
        self,
        ticker: str,
        min_posterior: float = 0.55,
        min_ticks: int = 5,
    ) -> bool:
        """
        Check if the posterior supports entering a trade now.

        Requires the posterior to be above min_posterior and stable
        (not declining from peak).
        """
        state = self._states.get(ticker)
        if state is None:
            return False

        if len(state["ticks"]) < min_ticks:
            return False

        posterior = state["posterior"]
        peak = state["peak_posterior"]

        # Posterior must be above threshold
        if posterior < min_posterior:
            return False

        # Posterior must not have dropped significantly from peak
        # (wait for it to stabilize or re-rise)
        if peak - posterior > 0.08:
            return False

        return True

    def should_exit(self, ticker: str, entry_confidence: float) -> bool:
        """
        Check if updated posterior suggests exiting an open position.

        Exit if posterior has dropped below breakeven probability.
        """
        state = self._states.get(ticker)
        if state is None:
            return False

        # Exit if posterior dropped well below entry confidence
        if state["posterior"] < entry_confidence * 0.7:
            return True

        # Exit if posterior dropped below 50% (we no longer think we'll win)
        if state["posterior"] < 0.45:
            return True

        return False

    def clear(self, ticker: str):
        """Stop tracking a ticker."""
        self._states.pop(ticker, None)


def _prob_to_log_odds(p: float) -> float:
    """Convert probability to log-odds."""
    p = max(0.001, min(0.999, p))
    return math.log(p / (1 - p))


def _log_odds_to_prob(lo: float) -> float:
    """Convert log-odds back to probability."""
    lo = max(-10, min(10, lo))  # Clamp to avoid overflow
    return 1.0 / (1.0 + math.exp(-lo))
