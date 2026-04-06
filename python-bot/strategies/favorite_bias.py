"""
Favorite-Longshot Bias Strategy for Kalshi 15-Minute Crypto Markets

Core idea: Academic research consistently finds that in prediction
markets, favorites (contracts priced >70c) win MORE often than their
price implies, while longshots (<30c) win LESS often. This is the
"favorite-longshot bias."

Why this works on Kalshi:
- Retail bettors overweight longshots (cheap tickets to big payoffs)
- This pushes favorite prices slightly below true probability
- Buying the favorite captures a small but persistent edge
- Works best with enough time remaining for the favorite to hold
"""

from typing import Optional

from strategies.base import Signal, Strategy, TradeRecommendation


class FavoriteBiasStrategy(Strategy):
    """
    Exploits the favorite-longshot bias in prediction markets.

    Only fires when:
    1. A clear favorite exists (one side priced >= threshold)
    2. The entry price is not too extreme (<=max_entry, to preserve payoff)
    3. Enough time remains (>= 3 min) for the favorite to hold

    Supports per-asset thresholds via asset_overrides dict, keyed by
    series prefix (e.g. "KXETH", "KXSOL").
    """

    name = "favorite_bias"

    def __init__(
        self,
        min_favorite_price: int = 70,   # Need at least 70c on one side
        max_entry_price: int = 80,      # Won't pay more than 80c
        min_seconds_remaining: int = 180,  # Need at least 3 min
        asset_overrides: dict = None,    # e.g. {"KXETH": {"min_fav": 80}, "KXSOL": {"min_fav": 85}}
    ):
        self.min_favorite_price = min_favorite_price
        self.max_entry_price = max_entry_price
        self.min_seconds_remaining = min_seconds_remaining
        self.asset_overrides = asset_overrides or {}

    def evaluate(self, market, last_settled, price_feed, scanner) -> TradeRecommendation:
        no_trade = TradeRecommendation(
            signal=Signal.NO_TRADE,
            confidence=0.0,
            strategy_name=self.name,
            reason="",
            max_price_cents=0,
        )

        secs_left = scanner.seconds_until_close(market)
        if secs_left < self.min_seconds_remaining:
            no_trade.reason = "Too close to expiry for favorite bias"
            return no_trade

        yes_bid, yes_ask = scanner.parse_yes_price(market)
        if yes_bid is None or yes_ask is None:
            no_trade.reason = "No bid/ask available"
            return no_trade

        # Resolve per-asset thresholds
        ticker = market.get("ticker", "")
        min_fav = self.min_favorite_price
        max_entry = self.max_entry_price
        for prefix, overrides in self.asset_overrides.items():
            if ticker.startswith(prefix):
                min_fav = overrides.get("min_fav", min_fav)
                max_entry = overrides.get("max_entry", max_entry)
                break

        yes_avg = (yes_bid + yes_ask) / 2

        # YES is the strong favorite
        if yes_avg >= min_fav and yes_ask <= max_entry:
            # Confidence slightly above market-implied probability
            confidence = min(0.92, yes_avg / 100 + 0.05)
            return TradeRecommendation(
                signal=Signal.BUY_YES,
                confidence=confidence,
                strategy_name=self.name,
                reason=(
                    f"Favorite-longshot: YES@{yes_avg:.0f}c is favorite, "
                    f"bias says it wins more than {yes_avg:.0f}% of the time"
                ),
                max_price_cents=yes_ask,
            )

        # NO is the strong favorite
        no_avg = 100 - yes_avg
        no_price = 100 - yes_bid
        if no_avg >= min_fav and no_price <= max_entry:
            confidence = min(0.92, no_avg / 100 + 0.05)
            return TradeRecommendation(
                signal=Signal.BUY_NO,
                confidence=confidence,
                strategy_name=self.name,
                reason=(
                    f"Favorite-longshot: NO@{no_avg:.0f}c is favorite, "
                    f"bias says it wins more than {no_avg:.0f}% of the time"
                ),
                max_price_cents=no_price,
            )

        no_trade.reason = f"No strong favorite (YES@{yes_avg:.0f}c, need >{min_fav})"
        return no_trade
