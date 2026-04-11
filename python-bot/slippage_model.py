"""
Slippage Model for Paper Trading

Simulates realistic order execution instead of instant fills at
the requested price. Models:
1. Fill probability based on price aggressiveness
2. Slippage (worse fill price than requested)
3. Partial fills
4. Non-fills for passive orders
"""

import random
from dataclasses import dataclass
from typing import Optional


@dataclass
class FillResult:
    """Result of a simulated order fill."""
    filled: bool
    fill_price_cents: int = 0
    contracts_filled: int = 0
    status: str = "cancelled"  # "filled", "partial", "cancelled"
    reason: str = ""


class SlippageModel:
    """
    Realistic paper-trade fill simulation.

    Models the probability and price of order fills based on the
    relationship between the limit price and current orderbook.
    """

    def __init__(
        self,
        base_fill_rate: float = 0.85,
        spread_slippage_max: int = 2,
        partial_fill_rate: float = 0.10,
        partial_fill_min_pct: float = 0.50,
        seed: Optional[int] = None,
    ):
        """
        Args:
            base_fill_rate: Base probability of fill when crossing spread.
            spread_slippage_max: Max slippage in cents (0 to N).
            partial_fill_rate: Probability of a partial fill (vs full).
            partial_fill_min_pct: Minimum fraction filled on partial.
            seed: RNG seed for deterministic testing.
        """
        self.base_fill_rate = base_fill_rate
        self.spread_slippage_max = spread_slippage_max
        self.partial_fill_rate = partial_fill_rate
        self.partial_fill_min_pct = partial_fill_min_pct
        self._rng = random.Random(seed)

    def simulate_fill(
        self,
        side: str,
        requested_price_cents: int,
        yes_bid: Optional[int],
        yes_ask: Optional[int],
        contracts: int,
    ) -> FillResult:
        """
        Simulate whether an order fills and at what price.

        Args:
            side: "yes" or "no"
            requested_price_cents: Limit price on our side (in cents)
            yes_bid: Current best YES bid from orderbook
            yes_ask: Current best YES ask from orderbook
            contracts: Number of contracts requested

        Returns:
            FillResult with fill details.
        """
        if yes_bid is None or yes_ask is None:
            return FillResult(
                filled=False, status="cancelled",
                reason="No orderbook data available",
            )

        if contracts <= 0:
            return FillResult(
                filled=False, status="cancelled",
                reason="Zero contracts",
            )

        # Determine the market price on our side
        if side == "yes":
            market_ask = yes_ask  # We're buying YES, paying the ask
        else:
            market_ask = 100 - yes_bid  # We're buying NO, paying 100 - yes_bid

        # How aggressive is our order?
        # Positive distance = our price is ABOVE the ask (aggressive, crossing)
        # Zero = at the ask
        # Negative = below the ask (passive, may not fill)
        distance = requested_price_cents - market_ask

        # Fill probability
        if distance >= 0:
            # At or above ask — likely to fill
            fill_prob = self.base_fill_rate + (1 - self.base_fill_rate) * min(1.0, distance / 5.0)
        else:
            # Below ask — much less likely
            fill_prob = max(0.05, self.base_fill_rate - abs(distance) * 0.15)

        # Roll the dice
        if self._rng.random() > fill_prob:
            return FillResult(
                filled=False, status="cancelled",
                reason=f"No fill (prob={fill_prob:.0%}, distance={distance}c from ask)",
            )

        # Fill price: apply slippage (always worse for us)
        slippage = self._rng.randint(0, self.spread_slippage_max)
        fill_price = requested_price_cents + slippage  # Higher = worse for buyer

        # Cap at 99 (can't pay more than 99c for a binary)
        fill_price = min(fill_price, 99)

        # Partial fill check
        contracts_filled = contracts
        status = "filled"
        if contracts > 1 and self._rng.random() < self.partial_fill_rate:
            pct = self._rng.uniform(self.partial_fill_min_pct, 0.90)
            contracts_filled = max(1, int(contracts * pct))
            status = "partial"

        return FillResult(
            filled=True,
            fill_price_cents=fill_price,
            contracts_filled=contracts_filled,
            status=status,
            reason=f"Fill at {fill_price}c ({contracts_filled}/{contracts} contracts, slippage={slippage}c)",
        )
