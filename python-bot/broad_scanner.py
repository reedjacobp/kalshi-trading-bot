"""
Broad Market Scanner — Resolution Rider Across All Kalshi Markets

Scans the entire Kalshi marketplace for contracts priced 95-99c
(near-certain outcomes) with sufficient liquidity. Feeds candidates
to the resolution_rider strategy regardless of market category.

Architecture:
  - REST: one paginated sweep of all open markets every ~2 minutes
    for metadata (close_time, event_ticker, series_ticker)
  - WebSocket: real-time prices for ALL markets (already streaming,
    just stopped filtering them out)
  - No extra API calls for price data — it's free from the WS feed

Nevada restrictions: excludes sports, elections, and entertainment.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("kalshi_bot")


# Series prefixes and event keywords for restricted categories (Nevada).
# These are filtered out before any trading logic runs.
RESTRICTED_SERIES_PREFIXES = (
    # Elections / Politics
    "PRES", "SENATE", "HOUSE", "GOV", "CONGRESS", "SCOTUS",
    "KXPOLITICAL", "KXELECTION",
    # Sports
    "NFL", "NBA", "MLB", "NHL", "UFC", "MMA", "NCAA", "PGA", "FIFA",
    "SOCCER", "TENNIS", "BOXING", "WNBA", "MLS", "F1",
    # Entertainment
    "OSCAR", "EMMY", "GRAMMY", "GOLDEN", "TONY", "SAG",
    "KXENTERTAINMENT",
)

RESTRICTED_KEYWORDS = (
    # Lowercase keywords checked against event_ticker and series_ticker
    "election", "electoral", "inaug", "president", "senat", "congress",
    "governor", "mayor", "politic", "scotus", "nominee", "primarywinner",
    "nfl", "nba", "mlb", "nhl", "ufc", "ncaa", "pga", "wnba",
    "oscar", "emmy", "grammy", "goldglob", "tony", "sag",
    "bachelor", "survivor", "idol", "bigbrother",
)


class BroadScanner:
    """
    Scans all open Kalshi markets for resolution_rider candidates.

    Uses cached REST metadata + live WebSocket prices to find
    contracts at 95-99c with enough liquidity and time constraints
    appropriate for the grind strategy.
    """

    def __init__(
        self,
        client,
        ws_feed,
        refresh_interval: float = 120,      # seconds between full market sweeps
        min_price: int = 95,                 # minimum contract price (cents)
        max_price: int = 99,                 # maximum (need SOME edge)
        min_volume: float = 10,              # minimum contracts traded
        max_seconds_to_close: int = 3600,    # only markets closing within 1 hour
        min_seconds_to_close: int = 30,      # skip if too close to settlement
        max_spread: int = 5,                 # max bid-ask spread in cents
    ):
        self.client = client
        self.ws_feed = ws_feed
        self.refresh_interval = refresh_interval
        self.min_price = min_price
        self.max_price = max_price
        self.min_volume = min_volume
        self.max_seconds_to_close = max_seconds_to_close
        self.min_seconds_to_close = min_seconds_to_close
        self.max_spread = max_spread

        # Cached market metadata from REST: ticker -> market dict
        self._markets: dict[str, dict] = {}
        self._last_refresh: float = 0

        # Track tickers we've already traded to avoid duplicates
        self._traded_tickers: set[str] = set()

    def _is_restricted(self, market: dict) -> bool:
        """Check if a market falls into a restricted category (Nevada)."""
        series = (market.get("series_ticker") or "").lower()
        event = (market.get("event_ticker") or "").lower()
        ticker = (market.get("ticker") or "").lower()
        combined = f"{series} {event} {ticker}"

        for prefix in RESTRICTED_SERIES_PREFIXES:
            if series.startswith(prefix.lower()) or event.startswith(prefix.lower()):
                return True

        for keyword in RESTRICTED_KEYWORDS:
            if keyword in combined:
                return True

        return False

    def _seconds_until_close(self, market: dict) -> float:
        """Parse close_time and return seconds remaining."""
        close_str = market.get("close_time", "")
        if not close_str:
            return float("inf")
        try:
            close_str = close_str.replace("Z", "+00:00")
            close_dt = datetime.fromisoformat(close_str)
            now = datetime.now(timezone.utc)
            return max(0, (close_dt - now).total_seconds())
        except (ValueError, TypeError):
            return float("inf")

    def refresh_markets(self):
        """Fetch all open markets from REST API (paginated)."""
        now = time.time()
        if now - self._last_refresh < self.refresh_interval:
            return

        self._last_refresh = now
        new_markets = {}
        cursor = None
        pages = 0
        max_pages = 10  # Safety limit: 200 markets/page × 10 = 2000 markets

        try:
            while pages < max_pages:
                resp = self.client.get_markets(
                    status="open",
                    limit=200,
                    cursor=cursor,
                )
                markets = resp.get("markets", [])
                if not markets:
                    break

                for m in markets:
                    ticker = m.get("ticker", "")
                    if ticker and not self._is_restricted(m):
                        new_markets[ticker] = m

                cursor = resp.get("cursor")
                if not cursor:
                    break
                pages += 1

            self._markets = new_markets
            logger.info(f"[BROAD] Refreshed: {len(new_markets)} open markets "
                        f"({pages + 1} pages, excluded restricted)")
        except Exception as e:
            logger.warning(f"[BROAD] Market refresh failed: {e}")

    def get_candidates(self) -> list[dict]:
        """
        Find resolution_rider candidates: markets at 95-99c with
        sufficient liquidity, closing soon, in allowed categories.

        Returns a list of market dicts augmented with:
          - 'rr_side': "yes" or "no" (which side is 95-99c)
          - 'rr_price': the entry price in cents
          - 'rr_secs_left': seconds until close
          - 'rr_volume': volume from WS
        """
        self.refresh_markets()
        candidates = []
        now = time.time()

        for ticker, market in self._markets.items():
            # Skip already-traded
            if ticker in self._traded_tickers:
                continue

            # Time filter
            secs_left = self._seconds_until_close(market)
            if secs_left > self.max_seconds_to_close:
                continue
            if secs_left < self.min_seconds_to_close:
                continue

            # Get live price from WebSocket
            tick = self.ws_feed.get_tick(ticker)
            if not tick:
                continue

            # Stale tick check (>60s old = probably no activity)
            if now - tick.ts > 60:
                continue

            yes_bid = tick.yes_bid
            yes_ask = tick.yes_ask
            if not yes_bid or not yes_ask:
                continue

            # Spread filter
            spread = yes_ask - yes_bid
            if spread > self.max_spread:
                continue

            # Volume filter
            if tick.volume < self.min_volume:
                continue

            yes_mid = (yes_bid + yes_ask) / 2
            no_mid = 100 - yes_mid

            # Check YES side: 95-99c
            if yes_mid >= self.min_price and yes_ask <= self.max_price:
                candidates.append({
                    **market,
                    "rr_side": "yes",
                    "rr_price": yes_ask,
                    "rr_secs_left": secs_left,
                    "rr_volume": tick.volume,
                    # Provide bid/ask for the scanner interface
                    "_yes_bid": yes_bid,
                    "_yes_ask": yes_ask,
                })

            # Check NO side: 95-99c
            elif no_mid >= self.min_price:
                no_ask = 100 - yes_bid
                if no_ask <= self.max_price:
                    candidates.append({
                        **market,
                        "rr_side": "no",
                        "rr_price": no_ask,
                        "rr_secs_left": secs_left,
                        "rr_volume": tick.volume,
                        "_yes_bid": yes_bid,
                        "_yes_ask": yes_ask,
                    })

        # Sort by edge (lower price = more edge), then by time remaining
        candidates.sort(key=lambda c: (c["rr_price"], c["rr_secs_left"]))
        return candidates

    def mark_traded(self, ticker: str):
        """Mark a ticker as traded so we don't re-evaluate it."""
        self._traded_tickers.add(ticker)

    def clear_traded(self, ticker: str):
        """Remove a ticker from the traded set (after settlement)."""
        self._traded_tickers.discard(ticker)

    def stats(self) -> dict:
        """Summary stats for dashboard/logging."""
        return {
            "total_markets": len(self._markets),
            "last_refresh": self._last_refresh,
            "traded_tickers": len(self._traded_tickers),
        }
