"""
Market Scanner for Kalshi 15-Minute Crypto Markets

Discovers and tracks KXBTC15M / KXETH15M markets.
Provides helpers to find the current open market, the most recently
settled market, and upcoming markets.
"""

import time
from datetime import datetime, timezone
from typing import Optional

from kalshi_client import KalshiClient


class MarketScanner:
    """Finds and tracks 15-minute crypto markets on Kalshi."""

    def __init__(self, client: KalshiClient, series: str = "KXBTC15M"):
        self.client = client
        self.series = series
        self._cache = {}
        self._cache_ts = {}   # per-key timestamps
        self._cache_ttl = 3  # seconds — keep fresh for fast-moving 15-min markets

    def _fresh_markets(self, status: str = None, limit: int = 200) -> list:
        """Fetch markets from the API, with a per-key TTL cache."""
        cache_key = f"{status}_{limit}"
        now = time.time()
        cached_ts = self._cache_ts.get(cache_key, 0)
        if cache_key in self._cache and (now - cached_ts) < self._cache_ttl:
            return self._cache[cache_key]

        result = self.client.get_markets(
            series_ticker=self.series, status=status, limit=limit
        )
        markets = result.get("markets", [])
        self._cache[cache_key] = markets
        self._cache_ts[cache_key] = now
        return markets

    def get_open_markets(self) -> list:
        """Get all currently open (active/tradeable) markets in this series."""
        return self._fresh_markets(status="open")

    def get_settled_markets(self, limit: int = 50) -> list:
        """Get recently settled markets, sorted by close time descending."""
        markets = self._fresh_markets(status="settled", limit=limit)
        # Sort by close_time descending (most recent first)
        markets.sort(
            key=lambda m: m.get("close_time", ""), reverse=True
        )
        return markets

    def get_next_expiring_market(self) -> Optional[dict]:
        """
        Get the open market closest to expiration.
        This is typically the one we want to trade on.
        """
        open_markets = self.get_open_markets()
        if not open_markets:
            return None

        # Sort by close_time ascending — the soonest closing is the active one
        open_markets.sort(key=lambda m: m.get("close_time", ""))
        return open_markets[0]

    def get_last_settled_market(self) -> Optional[dict]:
        """Get the most recently settled market and its result."""
        settled = self.get_settled_markets(limit=10)
        if not settled:
            return None
        return settled[0]

    def seconds_until_close(self, market: dict) -> float:
        """How many seconds until a market's close_time."""
        close_str = market.get("close_time", "")
        if not close_str:
            return float("inf")
        # Parse ISO 8601 timestamp
        close_str = close_str.replace("Z", "+00:00")
        close_dt = datetime.fromisoformat(close_str)
        now = datetime.now(timezone.utc)
        return max(0, (close_dt - now).total_seconds())

    def parse_yes_price(self, market: dict) -> tuple:
        """
        Extract best yes bid/ask from a market dict.
        Returns (yes_bid_cents, yes_ask_cents).
        Handles both dollar-string and cent-integer formats.
        """
        # The API returns prices as dollar strings like "0.5600"
        yes_bid = market.get("yes_bid_dollars") or market.get("yes_bid")
        yes_ask = market.get("yes_ask_dollars") or market.get("yes_ask")

        def to_cents(val):
            if val is None:
                return None
            if isinstance(val, str):
                try:
                    return int(round(float(val) * 100))
                except ValueError:
                    return None
            return int(val)

        return to_cents(yes_bid), to_cents(yes_ask)

    def market_summary(self, market: dict) -> str:
        """One-line summary of a market for logging."""
        ticker = market.get("ticker", "???")
        status = market.get("status", "???")
        yes_bid, yes_ask = self.parse_yes_price(market)
        secs = self.seconds_until_close(market)
        result = market.get("result", "")
        bid_str = f"{yes_bid}" if yes_bid is not None else "—"
        ask_str = f"{yes_ask}" if yes_ask is not None else "—"
        res_str = f" → {result}" if result else ""
        return f"{ticker} ({status}, {secs:.0f}s) yes={bid_str}/{ask_str}{res_str}"
