"""
Early Exit Strategy for Kalshi 15-Minute Crypto Markets

Not a new-entry strategy — this monitors OPEN positions and recommends
selling them before settlement when:
1. The thesis has broken (price moved against us significantly)
2. The position is profitable and momentum is reversing (lock in gains)
3. Trailing stop: as price moves in our favor, the stop tightens

Why this matters:
- Favorite bias trades have terrible loss/win ratios ($3.68 avg loss
  vs $0.86 avg win). Cutting losses early dramatically improves P&L.
- A YES contract bought at 80c that drops to 60c is -20c per contract
  if sold now, vs -80c if it settles NO. That's 4x less damage.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ExitRecommendation:
    """Recommendation to exit (sell) an existing position."""
    should_exit: bool
    reason: str
    urgency: str = "normal"  # "normal" or "urgent"


class EarlyExitMonitor:
    """
    Monitors open positions and recommends exits.

    Checks three conditions:
    1. STOP LOSS: If our side's price has dropped significantly from
       entry, cut the loss before it gets worse.
    2. TRAILING STOP: As the position becomes profitable, the stop
       tightens to lock in gains. Once up 10c+, stop trails at -5c
       from the peak price seen.
    3. TAKE PROFIT: If we're profitable but momentum is reversing,
       lock in the gain.

    This is called from the main bot loop, NOT as a strategy.evaluate().
    """

    def __init__(
        self,
        stop_loss_cents: int = 15,         # Exit if price moved 15c against us
        take_profit_cents: int = 10,       # Consider taking profit at +10c
        trailing_activation: int = 10,     # Activate trailing stop after +10c
        trailing_distance: int = 5,        # Trail at 5c below peak
        min_seconds_for_exit: int = 30,    # Don't try to exit in final 30s
    ):
        self.stop_loss_cents = stop_loss_cents
        self.take_profit_cents = take_profit_cents
        self.trailing_activation = trailing_activation
        self.trailing_distance = trailing_distance
        self.min_seconds_for_exit = min_seconds_for_exit
        # Track peak price per ticker for trailing stop
        self._peak_values: dict[str, int] = {}  # ticker -> peak current_value

    def check_position(
        self,
        ticker: str,
        side: str,
        entry_price_cents: int,
        current_yes_bid: int,
        current_yes_ask: int,
        seconds_remaining: float,
        momentum_1m: Optional[float] = None,
    ) -> ExitRecommendation:
        """
        Check if an open position should be exited early.

        Args:
            ticker: Market ticker (for tracking peak prices)
            side: "yes" or "no" — which side we're holding
            entry_price_cents: Price we paid (on our side)
            current_yes_bid: Current YES bid from orderbook
            current_yes_ask: Current YES ask from orderbook
            seconds_remaining: Seconds until market close
            momentum_1m: 1-minute price momentum (positive = bullish)

        Returns:
            ExitRecommendation with should_exit and reason.
        """
        no_exit = ExitRecommendation(should_exit=False, reason="")

        if seconds_remaining < self.min_seconds_for_exit:
            no_exit.reason = "Too close to settlement for exit"
            return no_exit

        # Current market value of our position
        if side == "yes":
            current_value = current_yes_bid
            momentum_against = momentum_1m is not None and momentum_1m < -0.02
        else:
            current_value = 100 - current_yes_ask
            momentum_against = momentum_1m is not None and momentum_1m > 0.02

        price_change = current_value - entry_price_cents

        # Update peak value for trailing stop
        prev_peak = self._peak_values.get(ticker, current_value)
        peak = max(prev_peak, current_value)
        self._peak_values[ticker] = peak

        # 1. STOP LOSS: Price moved significantly against us from entry
        if price_change <= -self.stop_loss_cents:
            self._peak_values.pop(ticker, None)
            return ExitRecommendation(
                should_exit=True,
                reason=f"Stop loss: position down {price_change}c (entry={entry_price_cents}c, now={current_value}c)",
                urgency="urgent",
            )

        # 2. TRAILING STOP: Once profitable enough, trail from peak
        if peak - entry_price_cents >= self.trailing_activation:
            drop_from_peak = peak - current_value
            if drop_from_peak >= self.trailing_distance:
                self._peak_values.pop(ticker, None)
                return ExitRecommendation(
                    should_exit=True,
                    reason=(
                        f"Trailing stop: peaked at {peak}c, now {current_value}c "
                        f"(dropped {drop_from_peak}c from peak, entry={entry_price_cents}c, "
                        f"locking in {price_change:+d}c)"
                    ),
                    urgency="normal",
                )

        # 3. TAKE PROFIT: Profitable + momentum reversing
        if price_change >= self.take_profit_cents and momentum_against:
            self._peak_values.pop(ticker, None)
            return ExitRecommendation(
                should_exit=True,
                reason=f"Take profit: position up {price_change}c but momentum reversing",
                urgency="normal",
            )

        no_exit.reason = f"Position at {price_change:+d}c (peak {peak}c), holding"
        return no_exit

    def clear_ticker(self, ticker: str):
        """Clear tracking data for a settled/exited ticker."""
        self._peak_values.pop(ticker, None)
