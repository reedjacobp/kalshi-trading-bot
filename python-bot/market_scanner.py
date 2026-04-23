"""
Market Scanner for Kalshi 15-Minute Crypto Markets

Discovers and tracks KXBTC15M / KXETH15M markets.
Provides helpers to find the current open market, the most recently
settled market, and upcoming markets.
"""

import os
import time
from datetime import datetime, timezone
from typing import Optional

from kalshi_client import KalshiClient


class MarketScanner:
    """Finds and tracks 15-minute crypto markets on Kalshi."""

    _scanner_count = 0  # Class-level counter for staggering cache expiry

    # Per-status cache TTL. "open" markets move fast — need fresh data
    # for the RR gate to fire on emergent 94c+ setups. "settled" and
    # other statuses rarely change post-close, so a longer TTL keeps
    # reconcile / settlement-tracking REST load low.
    #
    # 2026-04-22: _CACHE_TTL_OPEN_S bumped 3 → 8 → 20 → 60. Empirical
    # Kalshi cap on the /markets endpoint for our key is ≤ 0.3 req/s
    # (tighter than the documented 20/s reads cap). At 14 scanners,
    # 60s TTL gives ~0.23/s, under the observed cap. WS continues to
    # feed live book updates per-ticker at sub-second latency, so
    # reaction speed for already-tracked markets is unchanged — only
    # the latency to discover a newly-opened market grows from 20s
    # to 60s (still <7% of a 15M market cycle).
    # Env override: set SCANNER_OPEN_TTL_S to change without a rebuild.
    _CACHE_TTL_OPEN_S = int(os.getenv("SCANNER_OPEN_TTL_S", "60"))
    # Settled-status TTL: settled markets are immutable, and `_tick` was
    # calling `get_last_settled_market()` on every scanner every tick.
    # With 14 scanners and 30s TTL that was 14/30 = 0.47/s on /markets
    # ?status=settled — combined with 0.23/s for status=open this put
    # us over Kalshi's endpoint cap and drove the 35/min 429 rate
    # that the OPEN TTL bump alone couldn't fix. 300s = 5min is fine;
    # a new settlement only matters for reconcile/P&L which has other
    # paths. Env override: SCANNER_SETTLED_TTL_S.
    _CACHE_TTL_DEFAULT_S = int(os.getenv("SCANNER_SETTLED_TTL_S", "300"))

    def __init__(self, client: KalshiClient, series: str = "KXBTC15M",
                 ws_feed=None):
        self.client = client
        self.series = series
        self.ws_feed = ws_feed  # KalshiWebSocket for real-time prices
        self._cache = {}
        self._cache_ts = {}   # per-key timestamps
        self._offset_applied: set = set()
        # Assign a stable per-instance index so per-cache-key offsets
        # can be computed later using the right TTL. A single offset
        # is wrong because open (TTL=60) and settled (TTL=300) need
        # different spacing — 4.3s between open refreshes but 21.4s
        # between settled, otherwise settled bunches into a 60s window
        # within a 300s TTL and bursts every 5 min.
        MarketScanner._scanner_count += 1
        self._scanner_idx = MarketScanner._scanner_count

    def _fresh_markets(self, status: str = None, limit: int = 500) -> list:
        """Fetch ALL markets matching the filter, paginating via cursor.

        Previous behavior truncated at the first `limit` markets because
        the response cursor was ignored. For series like KXBTCD that
        have many strikes open simultaneously (multiple event windows ×
        multiple strikes each), truncation silently hid entries from
        the RR gate — the exact "missing trades due to code issues"
        category we cannot tolerate.

        TTL is status-aware: 3s for "open" (fast-moving), 30s otherwise.
        Pagination uses the API's cursor continuation to walk through
        all pages until no cursor is returned. Worst case: a series with
        N markets does ceil(N / limit) REST calls on cache miss.
        """
        cache_key = f"{status}_{limit}"
        now = time.time()
        cached_ts = self._cache_ts.get(cache_key, 0)
        ttl = (self._CACHE_TTL_OPEN_S if status == "open"
               else self._CACHE_TTL_DEFAULT_S)
        if cache_key in self._cache and cached_ts > 0 and (now - cached_ts) < ttl:
            return self._cache[cache_key]

        all_markets: list = []
        cursor: Optional[str] = None
        page_count = 0
        max_pages = 20  # safety ceiling — 20 × 500 = 10k markets
        while page_count < max_pages:
            result = self.client.get_markets(
                series_ticker=self.series, status=status, limit=limit,
                cursor=cursor,
            )
            page = result.get("markets", [])
            all_markets.extend(page)
            page_count += 1
            cursor = result.get("cursor")
            if not cursor or not page:
                break
        self._cache[cache_key] = all_markets
        # On the first fetch per cache_key, apply a per-TTL stagger so
        # 14 scanners' refreshes spread evenly across the full TTL
        # window for this cache_key — open over 60s, settled over 300s.
        # The prior code used a single 0-60s offset for both, which
        # bunched settled refreshes into a 60s window inside the 300s
        # TTL and produced a 35 × 429/min burst every 5 minutes.
        if cache_key not in self._offset_applied:
            step = max(1.0, ttl / 14.0)
            offset = (self._scanner_idx * step) % ttl
            self._cache_ts[cache_key] = now - offset
            self._offset_applied.add(cache_key)
        else:
            self._cache_ts[cache_key] = now
        return all_markets

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
            # Include anything from 85c+ so per-cell params (which can
            # go as low as 89c) aren't filtered out before the fast-RR
            # scanner gets to apply its own cell-specific thresholds.
            if 85 <= fav_price <= 99:
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
        # Use limit=50 to share cache with _check_settlements_for() and
        # avoid a separate REST call for the same data
        settled = self.get_settled_markets(limit=50)
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
