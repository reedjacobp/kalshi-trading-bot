"""
Resolution Rider Strategy for Kalshi 15-Minute Crypto Markets

Core idea: In the final 90 seconds before a market closes, the
contract price strongly reflects the actual outcome. If the market
is heavily favoring one side AND short-term momentum confirms it,
ride the resolution into expiry.

Why this works on Kalshi:
- Near expiry, contract prices converge toward 0 or 100
- A contract at 70c YES with 60 seconds left has a very high hit rate
- Momentum confirmation filters out stale prices / thin books
- Payoff is small per contract but win rate is very high
"""

from typing import Optional

from strategies.base import Signal, Strategy, TradeRecommendation


class ResolutionRiderStrategy(Strategy):
    """
    Trades in the final window before market resolution.

    Only fires when:
    1. Less than 90 seconds remain (but more than 10)
    2. The contract strongly favors one side (YES > 60 or NO > 60)
    3. 1-minute momentum confirms the direction
    """

    name = "resolution_rider"

    def __init__(
        self,
        max_seconds: int = 90,          # Only trade in last 90s
        min_seconds: int = 10,          # Don't trade in final 10s (settlement risk)
        min_contract_skew: int = 60,    # Contract must be at least 60c on favored side
        max_entry_price: int = 80,      # Won't pay more than 80c (near-certainty bets ok here)
    ):
        self.max_seconds = max_seconds
        self.min_seconds = min_seconds
        self.min_contract_skew = min_contract_skew
        self.max_entry_price = max_entry_price

    def evaluate(self, market, last_settled, price_feed, scanner) -> TradeRecommendation:
        no_trade = TradeRecommendation(
            signal=Signal.NO_TRADE,
            confidence=0.0,
            strategy_name=self.name,
            reason="",
            max_price_cents=0,
        )

        secs_left = scanner.seconds_until_close(market)

        if secs_left > self.max_seconds or secs_left < self.min_seconds:
            no_trade.reason = f"Not in resolution window ({secs_left:.0f}s left)"
            return no_trade

        yes_bid, yes_ask = scanner.parse_yes_price(market)
        if yes_bid is None or yes_ask is None:
            no_trade.reason = "No bid/ask available"
            return no_trade

        mom_1m = price_feed.momentum_1m()
        yes_avg = (yes_bid + yes_ask) / 2

        # YES is the favorite and momentum confirms
        if yes_avg > self.min_contract_skew and mom_1m is not None and mom_1m > 0:
            our_price = yes_ask
            if our_price > self.max_entry_price:
                no_trade.reason = f"YES ask {our_price}c too expensive for resolution rider"
                return no_trade

            confidence = min(0.95, 0.6 + (yes_avg - self.min_contract_skew) / 100)
            return TradeRecommendation(
                signal=Signal.BUY_YES,
                confidence=confidence,
                strategy_name=self.name,
                reason=f"Resolution rider: YES@{yes_avg:.0f}c with {secs_left:.0f}s left, momentum confirms",
                max_price_cents=our_price,
            )

        # NO is the favorite and momentum confirms
        no_avg = 100 - yes_avg
        if no_avg > self.min_contract_skew and mom_1m is not None and mom_1m < 0:
            our_price = 100 - yes_bid
            if our_price > self.max_entry_price:
                no_trade.reason = f"NO price {our_price}c too expensive for resolution rider"
                return no_trade

            confidence = min(0.95, 0.6 + (no_avg - self.min_contract_skew) / 100)
            return TradeRecommendation(
                signal=Signal.BUY_NO,
                confidence=confidence,
                strategy_name=self.name,
                reason=f"Resolution rider: NO@{no_avg:.0f}c with {secs_left:.0f}s left, momentum confirms",
                max_price_cents=our_price,
            )

        no_trade.reason = (
            f"Resolution window but no strong lean "
            f"(YES@{yes_avg:.0f}c, mom={mom_1m or 0:+.3f}%, {secs_left:.0f}s)"
        )
        return no_trade
