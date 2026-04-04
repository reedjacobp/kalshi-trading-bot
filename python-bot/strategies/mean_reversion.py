"""
Mean Reversion Strategy for Kalshi 15-Minute Crypto Markets

Core idea: After a sharp move in one direction, prices tend to
snap back. If the contract is mispriced relative to a "fair" 50/50
baseline adjusted for recent volatility, bet on reversion.

This is the counter-trend complement to the Momentum strategy.

Why this works on Kalshi:
- After large spikes, crypto tends to partially retrace
- Kalshi contracts near extremes (very high YES or very low YES)
  offer outsized payoffs if you correctly predict reversion
- The 15-minute window is short enough that most moves are noise,
  not structural trend changes
- We look for overextended moves where the market has overreacted
"""

from typing import Optional

from strategies.base import Signal, Strategy, TradeRecommendation


class MeanReversionStrategy(Strategy):
    """
    Mean reversion strategy that bets against overextended moves.

    Key mechanics:
    1. Detect when 1-min momentum is sharply up but 5-min or 15-min
       is flat or opposite (spike likely to fade)
    2. Look for contracts priced at extremes (YES > 65 or YES < 35)
       where a reversion gives a good payout
    3. Confirm with volatility: high vol = bigger reversion potential
    4. Time filter: prefer trading in the first 8 minutes of a
       15-min window (more time for reversion to play out)
    """

    name = "mean_reversion"

    def __init__(
        self,
        spike_threshold_pct: float = 0.12,   # 1-min move must exceed this
        confirm_window_pct: float = 0.05,     # 5-min move should be LESS than this (divergence)
        min_contract_skew: int = 60,          # Contract must be at least this skewed
        max_entry_price: int = 55,            # Max price on our side
        min_seconds_remaining: int = 300,     # Need at least 5 min remaining
    ):
        self.spike_threshold_pct = spike_threshold_pct
        self.confirm_window_pct = confirm_window_pct
        self.min_contract_skew = min_contract_skew
        self.max_entry_price = max_entry_price
        self.min_seconds_remaining = min_seconds_remaining

    def evaluate(self, market, last_settled, price_feed, scanner) -> TradeRecommendation:
        no_trade = TradeRecommendation(
            signal=Signal.NO_TRADE,
            confidence=0.0,
            strategy_name=self.name,
            reason="",
            max_price_cents=0,
        )

        # Need enough time for reversion to play out
        secs_left = scanner.seconds_until_close(market)
        if secs_left < self.min_seconds_remaining:
            no_trade.reason = f"Only {secs_left:.0f}s remaining (need {self.min_seconds_remaining}s)"
            return no_trade

        mom_1m = price_feed.momentum_1m()
        mom_5m = price_feed.momentum_5m()
        vol = price_feed.volatility(lookback_seconds=300)

        if mom_1m is None:
            no_trade.reason = "Insufficient price data"
            return no_trade

        yes_bid, yes_ask = scanner.parse_yes_price(market)
        if yes_ask is None or yes_bid is None:
            no_trade.reason = "No bid/ask available"
            return no_trade

        # ── SPIKE UP → BET ON REVERSION (buy NO) ────────────────────────
        # BTC spiked up sharply in the last minute, but the broader trend
        # doesn't support it. Contract is skewed high. Bet it comes back.
        if mom_1m > self.spike_threshold_pct:
            # Check for divergence: 5m should NOT be strongly bullish
            if mom_5m is not None and mom_5m > self.confirm_window_pct:
                no_trade.reason = f"Spike up but 5m trend supports it ({mom_5m:+.3f}%)"
                return no_trade

            # Contract should be skewed toward YES (high price)
            if yes_ask < self.min_contract_skew:
                no_trade.reason = f"YES ask {yes_ask}c not skewed enough (need >{self.min_contract_skew}c)"
                return no_trade

            # Our NO price
            no_price = 100 - yes_bid
            if no_price > self.max_entry_price:
                no_trade.reason = f"NO price {no_price}c too expensive"
                return no_trade

            # Confidence based on spike magnitude and divergence
            spike_factor = min(1.0, abs(mom_1m) / 0.3)
            div_factor = 0.5 if mom_5m is None else max(0, 1.0 - abs(mom_5m) / self.spike_threshold_pct)
            confidence = spike_factor * 0.6 + div_factor * 0.4

            return TradeRecommendation(
                signal=Signal.BUY_NO,
                confidence=confidence,
                strategy_name=self.name,
                reason=f"Spike up reversion: 1m={mom_1m:+.3f}% 5m={mom_5m or 0:+.3f}% ask={yes_ask}c",
                max_price_cents=min(no_price + 2, self.max_entry_price),
            )

        # ── SPIKE DOWN → BET ON REVERSION (buy YES) ─────────────────────
        if mom_1m < -self.spike_threshold_pct:
            if mom_5m is not None and mom_5m < -self.confirm_window_pct:
                no_trade.reason = f"Spike down but 5m trend supports it ({mom_5m:+.3f}%)"
                return no_trade

            # Contract should be skewed toward NO (low YES price)
            if yes_ask > (100 - self.min_contract_skew):
                no_trade.reason = f"YES ask {yes_ask}c not skewed low enough"
                return no_trade

            if yes_ask > self.max_entry_price:
                no_trade.reason = f"YES ask {yes_ask}c too expensive"
                return no_trade

            spike_factor = min(1.0, abs(mom_1m) / 0.3)
            div_factor = 0.5 if mom_5m is None else max(0, 1.0 - abs(mom_5m) / self.spike_threshold_pct)
            confidence = spike_factor * 0.6 + div_factor * 0.4

            return TradeRecommendation(
                signal=Signal.BUY_YES,
                confidence=confidence,
                strategy_name=self.name,
                reason=f"Spike down reversion: 1m={mom_1m:+.3f}% 5m={mom_5m or 0:+.3f}% ask={yes_ask}c",
                max_price_cents=min(yes_ask + 2, self.max_entry_price),
            )

        no_trade.reason = f"No spike detected: 1m={mom_1m:+.3f}% (threshold ±{self.spike_threshold_pct}%)"
        return no_trade
