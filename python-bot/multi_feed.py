"""
Multi-Exchange Price Feed & Sentiment Data

Pulls prices from Coinbase, Binance, and Kraken simultaneously.
Detects cross-exchange price divergences as a trading signal.
Optionally integrates Santiment social/on-chain data.
"""

import os
import time
import logging
from collections import deque
from typing import Optional

import requests

logger = logging.getLogger("kalshi_bot")

# Symbol mappings per exchange
SYMBOL_MAP = {
    "BTC-USD": {"binance": "BTCUSDT", "kraken": "XBTUSD"},
    "ETH-USD": {"binance": "ETHUSDT", "kraken": "ETHUSD"},
    "SOL-USD": {"binance": "SOLUSDT", "kraken": "SOLUSD"},
}

SANTIMENT_SLUG = {
    "BTC-USD": "bitcoin",
    "ETH-USD": "ethereum",
    "SOL-USD": "solana",
}


class ExchangePrice:
    """Price from a single exchange at a point in time."""
    def __init__(self, exchange: str, price: float, timestamp: float):
        self.exchange = exchange
        self.price = price
        self.timestamp = timestamp


class MultiExchangeFeed:
    """
    Aggregates prices from Coinbase, Binance, and Kraken.

    Provides:
    - Median price across exchanges (more robust than single source)
    - Cross-exchange divergence signal (when one exchange leads)
    - Santiment social volume / sentiment data
    """

    def __init__(self, symbol: str = "BTC-USD"):
        self.symbol = symbol
        self._session = requests.Session()
        self._last_prices: dict[str, ExchangePrice] = {}
        self._divergence_history: deque = deque(maxlen=60)  # 1 min of divergence readings
        # Santiment
        self._santiment_key = os.getenv("SANTIMENT_API_KEY", "")
        self._santiment_cache: dict = {}
        self._santiment_cache_ts: float = 0
        self._santiment_cache_ttl: float = 300  # 5 min

    def fetch_all(self) -> dict[str, Optional[float]]:
        """Fetch prices from all exchanges. Returns {exchange: price}."""
        results = {}
        now = time.time()

        # Coinbase
        try:
            resp = self._session.get(
                f"https://api.coinbase.com/v2/prices/{self.symbol}/spot",
                timeout=3,
            )
            resp.raise_for_status()
            price = float(resp.json()["data"]["amount"])
            results["coinbase"] = price
            self._last_prices["coinbase"] = ExchangePrice("coinbase", price, now)
        except Exception:
            pass

        # Binance
        binance_sym = SYMBOL_MAP.get(self.symbol, {}).get("binance")
        if binance_sym:
            try:
                resp = self._session.get(
                    f"https://api.binance.com/api/v3/ticker/price?symbol={binance_sym}",
                    timeout=3,
                )
                resp.raise_for_status()
                price = float(resp.json()["price"])
                results["binance"] = price
                self._last_prices["binance"] = ExchangePrice("binance", price, now)
            except Exception:
                pass

        # Kraken
        kraken_sym = SYMBOL_MAP.get(self.symbol, {}).get("kraken")
        if kraken_sym:
            try:
                resp = self._session.get(
                    f"https://api.kraken.com/0/public/Ticker?pair={kraken_sym}",
                    timeout=3,
                )
                resp.raise_for_status()
                data = resp.json().get("result", {})
                # Kraken returns nested dict with pair as key
                for pair_data in data.values():
                    price = float(pair_data["c"][0])  # last trade price
                    results["kraken"] = price
                    self._last_prices["kraken"] = ExchangePrice("kraken", price, now)
                    break
            except Exception:
                pass

        # Record divergence
        if len(results) >= 2:
            prices = list(results.values())
            mid = sorted(prices)[len(prices) // 2]
            max_div = max(abs(p - mid) / mid * 100 for p in prices)
            self._divergence_history.append((now, max_div))

        return results

    @property
    def median_price(self) -> Optional[float]:
        """Median price across all exchanges with recent data."""
        recent = [
            ep.price for ep in self._last_prices.values()
            if time.time() - ep.timestamp < 30
        ]
        if not recent:
            return None
        return sorted(recent)[len(recent) // 2]

    @property
    def divergence_pct(self) -> Optional[float]:
        """Current max divergence between exchanges as a percentage."""
        recent = [
            ep for ep in self._last_prices.values()
            if time.time() - ep.timestamp < 10
        ]
        if len(recent) < 2:
            return None
        prices = [ep.price for ep in recent]
        mid = sorted(prices)[len(prices) // 2]
        if mid == 0:
            return None
        return max(abs(p - mid) / mid * 100 for p in prices)

    def exchange_lead(self) -> Optional[str]:
        """
        Detect which exchange is leading (price higher/lower first).

        If one exchange's price is >0.05% above the median, it may be
        leading a move. Returns 'bullish' or 'bearish' or None.
        """
        recent = [
            ep for ep in self._last_prices.values()
            if time.time() - ep.timestamp < 10
        ]
        if len(recent) < 2:
            return None

        median = self.median_price
        if median is None or median == 0:
            return None

        # Check Binance (usually fastest) for divergence from median
        binance = self._last_prices.get("binance")
        if binance and time.time() - binance.timestamp < 10:
            div = (binance.price - median) / median * 100
            if div > 0.05:
                return "bullish"
            elif div < -0.05:
                return "bearish"
        return None

    def fetch_santiment(self) -> dict:
        """
        Fetch social volume and sentiment from Santiment (free tier).

        Returns dict with keys: social_volume, sentiment, social_dominance
        Cached for 5 minutes (free tier rate limits).
        """
        if not self._santiment_key:
            return {}

        now = time.time()
        if now - self._santiment_cache_ts < self._santiment_cache_ttl:
            return self._santiment_cache

        slug = SANTIMENT_SLUG.get(self.symbol, "bitcoin")
        result = {}

        try:
            # Social volume (last 1 hour)
            resp = self._session.get(
                "https://api.santiment.net/graphql",
                params={"query": f'''{{
                    getMetric(metric: "social_volume_total") {{
                        timeseriesData(
                            slug: "{slug}"
                            from: "utc_now-1h"
                            to: "utc_now"
                            interval: "1h"
                        ) {{ datetime value }}
                    }}
                }}'''},
                headers={"Authorization": f"Apikey {self._santiment_key}"},
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            ts_data = data.get("data", {}).get("getMetric", {}).get("timeseriesData", [])
            if ts_data:
                result["social_volume"] = ts_data[-1].get("value", 0)
        except Exception as e:
            logger.debug(f"Santiment social_volume fetch failed: {e}")

        try:
            # Weighted sentiment
            resp = self._session.get(
                "https://api.santiment.net/graphql",
                params={"query": f'''{{
                    getMetric(metric: "sentiment_volume_consumed_total") {{
                        timeseriesData(
                            slug: "{slug}"
                            from: "utc_now-1h"
                            to: "utc_now"
                            interval: "1h"
                        ) {{ datetime value }}
                    }}
                }}'''},
                headers={"Authorization": f"Apikey {self._santiment_key}"},
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            ts_data = data.get("data", {}).get("getMetric", {}).get("timeseriesData", [])
            if ts_data:
                result["sentiment"] = ts_data[-1].get("value", 0)
        except Exception as e:
            logger.debug(f"Santiment sentiment fetch failed: {e}")

        self._santiment_cache = result
        self._santiment_cache_ts = now
        return result
