"""
Real-Time Crypto Price Feed

Pulls BTC/ETH spot prices from Coinbase's public API.
Maintains a rolling window of prices for momentum / mean-reversion signals.
"""

import time
from collections import deque
from typing import Optional

import requests


COINBASE_URL = "https://api.coinbase.com/v2/prices"


class PriceFeed:
    """
    Fetches and buffers crypto spot prices from Coinbase.

    Maintains a rolling window of (timestamp, price) tuples
    for computing momentum, volatility, and mean-reversion signals.
    """

    def __init__(self, symbol: str = "BTC-USD", window_seconds: int = 900):
        """
        Args:
            symbol: Coinbase trading pair (e.g. "BTC-USD", "ETH-USD")
            window_seconds: How much history to keep (default 900 = 15 min)
        """
        self.symbol = symbol
        self.window_seconds = window_seconds
        self.prices: deque = deque()  # (timestamp, price) tuples
        self._session = requests.Session()

    def fetch_price(self) -> Optional[float]:
        """Fetch the current spot price from Coinbase."""
        try:
            resp = self._session.get(
                f"{COINBASE_URL}/{self.symbol}/spot", timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            price = float(data["data"]["amount"])
            now = time.time()
            self.prices.append((now, price))
            self._prune()
            return price
        except Exception:
            return None

    def _prune(self):
        """Remove prices older than the window."""
        cutoff = time.time() - self.window_seconds
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()

    @property
    def current_price(self) -> Optional[float]:
        """Most recent price in the buffer."""
        if not self.prices:
            return None
        return self.prices[-1][1]

    @property
    def current_timestamp(self) -> Optional[float]:
        """Timestamp of most recent price."""
        if not self.prices:
            return None
        return self.prices[-1][0]

    def price_at(self, seconds_ago: float) -> Optional[float]:
        """
        Get the price closest to `seconds_ago` seconds in the past.
        Returns None if we don't have data that far back.
        """
        if not self.prices:
            return None
        target = time.time() - seconds_ago
        best = None
        best_diff = float("inf")
        for ts, px in self.prices:
            diff = abs(ts - target)
            if diff < best_diff:
                best_diff = diff
                best = px
        # Only return if we found something within 30s of the target
        if best_diff > 30:
            return None
        return best

    def momentum(self, lookback_seconds: float = 60) -> Optional[float]:
        """
        Compute price momentum as percentage change over lookback period.

        Returns:
            Percentage change (e.g. 0.15 means +0.15%), or None if
            insufficient data.
        """
        current = self.current_price
        past = self.price_at(lookback_seconds)
        if current is None or past is None or past == 0:
            return None
        return ((current - past) / past) * 100

    def momentum_15m(self) -> Optional[float]:
        """15-minute momentum (percentage change)."""
        return self.momentum(lookback_seconds=900)

    def momentum_5m(self) -> Optional[float]:
        """5-minute momentum."""
        return self.momentum(lookback_seconds=300)

    def momentum_1m(self) -> Optional[float]:
        """1-minute momentum."""
        return self.momentum(lookback_seconds=60)

    def volatility(self, lookback_seconds: float = 300) -> Optional[float]:
        """
        Compute recent price volatility as the standard deviation of
        returns over the lookback window.

        Returns:
            Annualized-ish vol estimate, or None if insufficient data.
        """
        cutoff = time.time() - lookback_seconds
        window_prices = [px for ts, px in self.prices if ts >= cutoff]
        if len(window_prices) < 5:
            return None

        returns = []
        for i in range(1, len(window_prices)):
            if window_prices[i - 1] != 0:
                r = (window_prices[i] - window_prices[i - 1]) / window_prices[i - 1]
                returns.append(r)

        if len(returns) < 3:
            return None

        import numpy as np
        return float(np.std(returns)) * 100  # as percentage

    def price_velocity(self, lookback_seconds: float = 30) -> Optional[float]:
        """
        Price velocity: rate of change in dollars per second.
        Useful for detecting sharp moves.
        """
        cutoff = time.time() - lookback_seconds
        window = [(ts, px) for ts, px in self.prices if ts >= cutoff]
        if len(window) < 2:
            return None
        dt = window[-1][0] - window[0][0]
        dp = window[-1][1] - window[0][1]
        if dt == 0:
            return None
        return dp / dt

    def ema(self, span_seconds: float = 60) -> Optional[float]:
        """
        Exponential moving average over the price buffer.
        Uses a decay factor based on the span.
        """
        if not self.prices:
            return None
        import numpy as np

        alpha = 2.0 / (1 + max(1, len([
            px for ts, px in self.prices
            if ts >= time.time() - span_seconds
        ])))

        ema_val = self.prices[0][1]
        for _, px in self.prices:
            ema_val = alpha * px + (1 - alpha) * ema_val
        return ema_val
