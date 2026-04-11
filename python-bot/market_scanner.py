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

    def __init__(self, client: KalshiClient, series: str = "KXBTC15M",
                 ws_feed=None):
        self.client = client
        self.series = series
        self.ws_feed = ws_feed  # KalshiWebSocket for real-time prices
        self._cache = {}
        self._cache_ts = {}   # per-key timestamps
        self._cache_ttl = 60  # seconds — fast RR uses WS, REST is just for discovery/settlement

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
        For 15M series: returns the single active market.
        For daily series (KXBTCD etc): returns the best-favorite strike.
        """
        open_markets = self.get_open_markets()
        if not open_markets:
            return None

        # Sort by close_time ascending — the soonest closing is the active one
        open_markets.sort(key=lambda m: m.get("close_time", ""))

        # For daily series, many strikes share the same close_time.
        # Pick the one with the strongest favorite (best for favorite-bias).
        earliest_close = open_markets[0].get("close_time", "")
        same_window = [m for m in open_markets if m.get("close_time", "") == earliest_close]

        if len(same_window) <= 1:
            return open_markets[0]

        # Find the strike with the best tradeable favorite.
        # Prefer strikes in the 70-90c range (sweet spot for favorite bias)
        # over extreme 95-100c strikes that have no payoff.
        best = None
        best_score = 0
        for m in same_window:
            yes_bid, yes_ask = self.parse_yes_price(m)
            if yes_bid is None or yes_ask is None:
                continue
            if yes_bid <= 0 and yes_ask >= 100:
                continue  # No real prices
            yes_mid = (yes_bid + yes_ask) / 2
            fav_price = max(yes_mid, 100 - yes_mid)
            vol = float(m.get("volume", 0) or m.get("volume_fp", 0) or 0)

            # Best range for favorite bias: 70-90c on the favorite side
            # Score higher for being in this range, penalize extremes
            if fav_price >= 70 and fav_price <= 90:
                range_score = 20  # Strong bonus for tradeable range
            elif fav_price > 90:
                range_score = 1   # Near-certain, poor payoff
            elif fav_price >= 60:
                range_score = 5   # Moderate favorite
            else:
                continue  # Skip coin-flip strikes

            vol_weight = min(1.0, vol / 100) if vol > 0 else 0.1
            score = range_score * vol_weight * fav_price
            if score > best_score:
                best_score = score
                best = m

        return best or open_markets[0]

    def get_near_certain_markets(self, max_hours: float = 2.0) -> list[dict]:
        """
        Get ALL 95-99c strikes for resolution_rider across every open
        event window in this series (not just the soonest one).

        This finds candidates in both hourly AND daily markets within
        the same series (e.g., KXBTCD has hourly events and a 5pm daily).

        Args:
            max_hours: Only consider markets closing within this many hours.

        Returns:
            List of market dicts, sorted by edge (most edge first).
        """
        open_markets = self.get_open_markets()
        if not open_markets:
            return []

        now_secs = time.time()
        candidates = []

        for m in open_markets:
            # Time filter: skip markets too far out or too close
            secs_left = self.seconds_until_close(m)
            if secs_left > max_hours * 3600 or secs_left < 10:
                continue

            yes_bid, yes_ask = self.parse_yes_price(m)
            if yes_bid is None or yes_ask is None:
                continue
            yes_mid = (yes_bid + yes_ask) / 2
            fav_price = max(yes_mid, 100 - yes_mid)
            if 95 <= fav_price <= 99:
                edge = 100 - fav_price
                candidates.append((edge, secs_left, m))

        # Sort by edge descending (most edge first), then by soonest closing
        candidates.sort(key=lambda c: (-c[0], c[1]))
        return [c[2] for c in candidates]

    def get_near_certain_market(self) -> Optional[dict]:
        """Get the single best 95-99c strike (convenience wrapper)."""
        results = self.get_near_certain_markets()
        return results[0] if results else None

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

        Prefers real-time WebSocket data when available, falls back
        to REST API market data.
        """
        ticker = market.get("ticker", "")

        # Prefer WebSocket real-time prices
        if self.ws_feed and ticker:
            ws_bid, ws_ask = self.ws_feed.get_yes_prices(ticker)
            if ws_bid is not None and ws_ask is not None and ws_bid > 0:
                return ws_bid, ws_ask

        # Fallback: REST API data
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
