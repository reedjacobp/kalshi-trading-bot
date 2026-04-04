"""
Base classes for trading strategies.

Each strategy produces a Signal indicating whether to buy YES, buy NO,
or do nothing on the current market.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Signal(Enum):
    """Trading signal produced by a strategy."""
    BUY_YES = "yes"
    BUY_NO = "no"
    NO_TRADE = "none"


@dataclass
class TradeRecommendation:
    """Full recommendation from a strategy."""
    signal: Signal
    confidence: float       # 0.0 to 1.0
    strategy_name: str
    reason: str
    max_price_cents: int    # Maximum price willing to pay (1-99)

    @property
    def should_trade(self) -> bool:
        return self.signal != Signal.NO_TRADE


class Strategy:
    """
    Abstract base class for trading strategies.

    Subclasses must implement `evaluate()` which takes the current
    market state and returns a TradeRecommendation.
    """

    name: str = "base"

    def evaluate(
        self,
        market: dict,
        last_settled: Optional[dict],
        price_feed,
        scanner,
    ) -> TradeRecommendation:
        """
        Evaluate the current market and return a trade recommendation.

        Args:
            market: Current open market dict from Kalshi API
            last_settled: Most recently settled market (with result)
            price_feed: PriceFeed instance with buffered crypto prices
            scanner: MarketScanner instance

        Returns:
            TradeRecommendation with signal, confidence, and max price.
        """
        raise NotImplementedError
