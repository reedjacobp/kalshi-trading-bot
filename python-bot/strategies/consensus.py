"""
Consensus Strategy for Kalshi 15-Minute Crypto Markets

The flagship strategy. Only trades when multiple independent signals
agree, dramatically improving win rate at the cost of fewer trades.

Combines:
1. Momentum (are prices trending?)
2. Previous market result (did the last 15-min window go the same way?)
3. Orderbook skew (is there more buying or selling pressure?)
4. Volatility filter (is the market in a tradeable regime?)

This is the recommended strategy for live trading because it has the
highest expected Sharpe ratio — it trades less but wins more often.
"""

from typing import Optional

from strategies.base import Signal, Strategy, TradeRecommendation


class ConsensusStrategy(Strategy):
    """
    Multi-signal consensus strategy.

    Requires at least 2 out of 3 independent signals to agree before
    entering a trade. The three signal sources:

    1. MOMENTUM: 1-minute BTC price direction
    2. PREVIOUS: Result of the last settled 15-min market
    3. ORDERBOOK: Kalshi contract price skew (where smart money is)

    Entry rules:
    - At least 2/3 signals agree on YES or NO
    - Contract price must be in the sweet spot (35-50 cents on our side)
    - At least 3 minutes remaining in the window
    - Daily loss cap not reached
    - Estimated edge must exceed minimum threshold

    Positive expectancy requires entry prices well below the win rate.
    At 50c max entry, breakeven is 50% — a 60%+ win rate gives real edge.
    """

    name = "consensus"

    def __init__(
        self,
        momentum_threshold: float = 0.03,   # Min 1m momentum for signal
        max_entry_price: int = 55,           # Won't pay more than 55c (raised from 50 for spread crossing)
        min_entry_price: int = 38,           # Raised from 35 — avoid deep coin-flip zone
        min_seconds_remaining: int = 180,    # Need at least 3 min left
        min_agreement: int = 2,              # Min signals that must agree (out of 3)
        min_edge: float = 0.10,              # Raised from 5% to 10% — require more conviction
    ):
        self.momentum_threshold = momentum_threshold
        self.max_entry_price = max_entry_price
        self.min_entry_price = min_entry_price
        self.min_seconds_remaining = min_seconds_remaining
        self.min_agreement = min_agreement
        self.min_edge = min_edge

    def _momentum_signal(self, price_feed) -> Optional[str]:
        """Returns 'yes' (bullish), 'no' (bearish), or None."""
        mom = price_feed.momentum_1m()
        if mom is None:
            return None
        if mom > self.momentum_threshold:
            return "yes"
        if mom < -self.momentum_threshold:
            return "no"
        return None

    def _previous_signal(self, last_settled: Optional[dict]) -> Optional[str]:
        """
        Returns the result of the previous market.
        Trend-following: if last market settled YES, signal YES.
        """
        if last_settled is None:
            return None
        result = last_settled.get("result")
        if result in ("yes", "no"):
            return result
        return None

    def _orderbook_signal(self, market: dict, scanner) -> Optional[str]:
        """
        Infer direction from orderbook imbalance, NOT from the mid-price.

        Fetches the full orderbook and compares total YES bid depth vs
        total NO bid depth (i.e., YES ask depth). A lopsided book
        suggests directional pressure that isn't yet reflected in price.
        """
        try:
            book = scanner.client.get_orderbook(market["ticker"], depth=10)
        except Exception:
            return None

        yes_bids = book.get("orderbook", {}).get("yes", [])
        no_bids = book.get("orderbook", {}).get("no", [])

        # Sum up resting quantity on each side
        yes_depth = sum(level[1] for level in yes_bids) if yes_bids else 0
        no_depth = sum(level[1] for level in no_bids) if no_bids else 0

        total = yes_depth + no_depth
        if total < 10:
            return None  # Too thin to read

        imbalance = (yes_depth - no_depth) / total
        if imbalance > 0.15:
            return "yes"
        if imbalance < -0.15:
            return "no"
        return None

    def evaluate(self, market, last_settled, price_feed, scanner) -> TradeRecommendation:
        no_trade = TradeRecommendation(
            signal=Signal.NO_TRADE,
            confidence=0.0,
            strategy_name=self.name,
            reason="",
            max_price_cents=0,
        )

        # Time filter
        secs_left = scanner.seconds_until_close(market)
        if secs_left < self.min_seconds_remaining:
            no_trade.reason = f"Only {secs_left:.0f}s remaining (need {self.min_seconds_remaining}s)"
            return no_trade

        # Collect signals
        sig_momentum = self._momentum_signal(price_feed)
        sig_previous = self._previous_signal(last_settled)
        sig_orderbook = self._orderbook_signal(market, scanner)

        signals = [sig_momentum, sig_previous, sig_orderbook]
        signal_names = ["momentum", "previous", "orderbook"]

        # Count votes
        yes_votes = sum(1 for s in signals if s == "yes")
        no_votes = sum(1 for s in signals if s == "no")
        valid_votes = sum(1 for s in signals if s is not None)

        signal_summary = ", ".join(
            f"{name}={sig or '—'}" for name, sig in zip(signal_names, signals)
        )

        # Need minimum agreement
        if yes_votes >= self.min_agreement:
            direction = "yes"
        elif no_votes >= self.min_agreement:
            direction = "no"
        else:
            no_trade.reason = f"No consensus ({yes_votes}Y/{no_votes}N): {signal_summary}"
            return no_trade

        # Price filter
        yes_bid, yes_ask = scanner.parse_yes_price(market)
        if yes_ask is None:
            no_trade.reason = "No ask price available"
            return no_trade

        if direction == "yes":
            our_price = yes_ask
        else:
            our_price = 100 - (yes_bid or yes_ask)

        if our_price > self.max_entry_price:
            no_trade.reason = f"Price {our_price}c too high (max {self.max_entry_price}c): {signal_summary}"
            return no_trade

        if our_price < self.min_entry_price:
            no_trade.reason = f"Price {our_price}c too low (min {self.min_entry_price}c): {signal_summary}"
            return no_trade

        # Confidence based on agreement strength
        agreement_ratio = max(yes_votes, no_votes) / max(valid_votes, 1)
        confidence = agreement_ratio * 0.7 + (1.0 - our_price / 100.0) * 0.3

        # Expected value check: at this price, does our estimated win
        # probability clear breakeven by at least min_edge?
        # Breakeven win rate at price P cents = P / 100
        breakeven_wr = our_price / 100.0
        estimated_wr = confidence  # Use confidence as our probability estimate
        edge = estimated_wr - breakeven_wr

        if edge < self.min_edge:
            no_trade.reason = (
                f"Insufficient edge: est_WR={estimated_wr:.0%} vs "
                f"breakeven={breakeven_wr:.0%} (edge={edge:.0%}, need {self.min_edge:.0%}): {signal_summary}"
            )
            return no_trade

        signal = Signal.BUY_YES if direction == "yes" else Signal.BUY_NO
        return TradeRecommendation(
            signal=signal,
            confidence=confidence,
            strategy_name=self.name,
            reason=f"Consensus {direction.upper()} ({max(yes_votes, no_votes)}/{valid_votes}): {signal_summary}",
            max_price_cents=our_price,
        )
