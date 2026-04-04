"""
Momentum Strategy for Kalshi 15-Minute Crypto Markets

Core idea: If BTC/ETH spot price is trending in a direction over the
short term, bet that the trend continues into the next 15-minute window.

The strategy uses multiple momentum timeframes and price velocity to
confirm the signal, only trading when there's a clear directional move.

Why this works on Kalshi:
- 15-minute crypto markets are binary: "Will BTC be higher or lower?"
- Crypto prices exhibit short-term momentum (autocorrelation in returns)
- Sharp moves tend to continue for at least a few more minutes
- We filter for high-confidence moves to avoid choppy, mean-reverting noise
"""

from typing import Optional

from strategies.base import Signal, Strategy, TradeRecommendation


class MomentumStrategy(Strategy):
    """
    Multi-timeframe momentum strategy.

    Combines 1-minute momentum (fast signal) with 5-minute momentum
    (confirmation) and price velocity (acceleration filter).

    Only trades when:
    1. 1-min and 5-min momentum agree on direction
    2. The move exceeds a minimum threshold (filters noise)
    3. Price velocity confirms acceleration (not decelerating)
    4. The Kalshi contract price offers edge (not already priced in)
    """

    name = "momentum"

    def __init__(
        self,
        min_momentum_pct: float = 0.05,     # Minimum 1-min momentum to trigger
        min_5m_momentum_pct: float = 0.02,  # Minimum 5-min momentum for confirmation
        max_entry_price: int = 60,           # Won't buy YES above 60 cents
        min_entry_price: int = 30,           # Won't buy YES below 30 cents (too cheap = too uncertain)
        velocity_threshold: float = 0.0,     # Price velocity must be positive for YES
    ):
        self.min_momentum_pct = min_momentum_pct
        self.min_5m_momentum_pct = min_5m_momentum_pct
        self.max_entry_price = max_entry_price
        self.min_entry_price = min_entry_price
        self.velocity_threshold = velocity_threshold

    def evaluate(self, market, last_settled, price_feed, scanner) -> TradeRecommendation:
        no_trade = TradeRecommendation(
            signal=Signal.NO_TRADE,
            confidence=0.0,
            strategy_name=self.name,
            reason="",
            max_price_cents=0,
        )

        # Get momentum readings
        mom_1m = price_feed.momentum_1m()
        mom_5m = price_feed.momentum_5m()
        velocity = price_feed.price_velocity(lookback_seconds=30)

        if mom_1m is None or mom_5m is None:
            no_trade.reason = "Insufficient price data for momentum calculation"
            return no_trade

        # Check current market pricing
        yes_bid, yes_ask = scanner.parse_yes_price(market)
        if yes_ask is None:
            no_trade.reason = "No ask price available"
            return no_trade

        # ── BULLISH SIGNAL (BTC going up → buy YES) ──────────────────────
        if mom_1m > self.min_momentum_pct and mom_5m > self.min_5m_momentum_pct:
            # Both timeframes agree: bullish
            if velocity is not None and velocity <= self.velocity_threshold:
                no_trade.reason = f"Bullish momentum but velocity decelerating ({velocity:.2f})"
                return no_trade

            # Check if the contract price offers edge
            if yes_ask > self.max_entry_price:
                no_trade.reason = f"YES ask {yes_ask}c too expensive (max {self.max_entry_price}c)"
                return no_trade

            if yes_ask < self.min_entry_price:
                no_trade.reason = f"YES ask {yes_ask}c too cheap / uncertain"
                return no_trade

            # Confidence scales with momentum strength
            confidence = min(1.0, (abs(mom_1m) / 0.3) * 0.6 + (abs(mom_5m) / 0.2) * 0.4)

            return TradeRecommendation(
                signal=Signal.BUY_YES,
                confidence=confidence,
                strategy_name=self.name,
                reason=f"Bullish: 1m={mom_1m:+.3f}% 5m={mom_5m:+.3f}% vel={velocity or 0:.1f}",
                max_price_cents=min(yes_ask + 2, self.max_entry_price),
            )

        # ── BEARISH SIGNAL (BTC going down → buy NO) ─────────────────────
        if mom_1m < -self.min_momentum_pct and mom_5m < -self.min_5m_momentum_pct:
            if velocity is not None and velocity >= -self.velocity_threshold:
                no_trade.reason = f"Bearish momentum but velocity decelerating ({velocity:.2f})"
                return no_trade

            # For NO, the price is (100 - yes_ask)
            no_price = 100 - (yes_bid or yes_ask)
            if no_price > self.max_entry_price:
                no_trade.reason = f"NO effective price {no_price}c too expensive"
                return no_trade

            if no_price < self.min_entry_price:
                no_trade.reason = f"NO effective price {no_price}c too cheap / uncertain"
                return no_trade

            confidence = min(1.0, (abs(mom_1m) / 0.3) * 0.6 + (abs(mom_5m) / 0.2) * 0.4)

            return TradeRecommendation(
                signal=Signal.BUY_NO,
                confidence=confidence,
                strategy_name=self.name,
                reason=f"Bearish: 1m={mom_1m:+.3f}% 5m={mom_5m:+.3f}% vel={velocity or 0:.1f}",
                max_price_cents=min(no_price + 2, self.max_entry_price),
            )

        # ── NO SIGNAL ────────────────────────────────────────────────────
        no_trade.reason = f"No clear momentum: 1m={mom_1m:+.3f}% 5m={mom_5m:+.3f}%"
        return no_trade
