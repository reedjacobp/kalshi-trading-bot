"""
CF Benchmarks RTI (Real-Time Index) Approximation

Kalshi's KXBTC15M / KXETH15M / KXSOL15M markets settle against
composite reference indices, not a single exchange's spot price.
The CF BRTI methodology aggregates prices across constituent exchanges
using volume-weighted mid-prices with outlier filtering.

This module approximates that methodology using public APIs:
1. Pulls prices from constituent exchanges (Coinbase, Kraken, Bitstamp,
   Gemini, Binance) — the same exchanges CF Benchmarks uses
2. Applies volume-weighted averaging (heavier weight to high-volume venues)
3. Filters outlier prices (>2 std devs from median)
4. Maintains a rolling buffer for momentum/vol calculations

Why this matters:
- Our strategies compute momentum from Coinbase alone
- If Coinbase leads/lags the composite by even 0.05%, our momentum
  signal fires early or late relative to what Kalshi settles against
- Using the RTI approximation aligns our signals with the settlement price
"""

import time
import logging
import statistics
from collections import deque
from typing import Optional

import requests

logger = logging.getLogger("kalshi_bot")


# Exchange endpoints and volume weights (approximate, based on typical
# BTC spot volume shares across CF Benchmarks constituent exchanges)
EXCHANGE_CONFIG = {
    "BTC-USD": {
        "coinbase":  {"weight": 0.30, "url": "https://api.coinbase.com/v2/prices/BTC-USD/spot", "parser": "coinbase"},
        "kraken":    {"weight": 0.20, "url": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD", "parser": "kraken"},
        "bitstamp":  {"weight": 0.15, "url": "https://www.bitstamp.net/api/v2/ticker/btcusd/", "parser": "bitstamp"},
        "gemini":    {"weight": 0.15, "url": "https://api.gemini.com/v1/pubticker/btcusd", "parser": "gemini"},
        "binance":   {"weight": 0.20, "url": "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", "parser": "binance"},
    },
    "ETH-USD": {
        "coinbase":  {"weight": 0.30, "url": "https://api.coinbase.com/v2/prices/ETH-USD/spot", "parser": "coinbase"},
        "kraken":    {"weight": 0.20, "url": "https://api.kraken.com/0/public/Ticker?pair=ETHUSD", "parser": "kraken"},
        "bitstamp":  {"weight": 0.15, "url": "https://www.bitstamp.net/api/v2/ticker/ethusd/", "parser": "bitstamp"},
        "gemini":    {"weight": 0.15, "url": "https://api.gemini.com/v1/pubticker/ethusd", "parser": "gemini"},
        "binance":   {"weight": 0.20, "url": "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT", "parser": "binance"},
    },
    "SOL-USD": {
        "coinbase":  {"weight": 0.30, "url": "https://api.coinbase.com/v2/prices/SOL-USD/spot", "parser": "coinbase"},
        "kraken":    {"weight": 0.20, "url": "https://api.kraken.com/0/public/Ticker?pair=SOLUSD", "parser": "kraken"},
        "bitstamp":  {"weight": 0.20, "url": "https://www.bitstamp.net/api/v2/ticker/solusd/", "parser": "bitstamp"},
        "binance":   {"weight": 0.30, "url": "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT", "parser": "binance"},
    },
}


def _parse_coinbase(resp_json: dict) -> Optional[float]:
    try:
        return float(resp_json["data"]["amount"])
    except (KeyError, ValueError, TypeError):
        return None


def _parse_kraken(resp_json: dict) -> Optional[float]:
    try:
        for pair_data in resp_json.get("result", {}).values():
            return float(pair_data["c"][0])  # last trade price
    except (KeyError, ValueError, TypeError, StopIteration):
        return None


def _parse_bitstamp(resp_json: dict) -> Optional[float]:
    try:
        return float(resp_json["last"])
    except (KeyError, ValueError, TypeError):
        return None


def _parse_gemini(resp_json: dict) -> Optional[float]:
    try:
        return float(resp_json["last"])
    except (KeyError, ValueError, TypeError):
        return None


def _parse_binance(resp_json: dict) -> Optional[float]:
    try:
        return float(resp_json["price"])
    except (KeyError, ValueError, TypeError):
        return None


PARSERS = {
    "coinbase": _parse_coinbase,
    "kraken": _parse_kraken,
    "bitstamp": _parse_bitstamp,
    "gemini": _parse_gemini,
    "binance": _parse_binance,
}


class RTIFeed:
    """
    Approximates the CF Benchmarks Real-Time Index for a given crypto pair.

    Drop-in replacement for PriceFeed — provides the same interface
    (current_price, momentum_1m, volatility, etc.) but backed by a
    volume-weighted composite price instead of a single exchange.
    """

    def __init__(self, symbol: str = "BTC-USD", window_seconds: int = 1200):
        self.symbol = symbol
        self.window_seconds = window_seconds
        self.prices: deque = deque()  # (timestamp, rti_price) tuples
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "kalshi-bot/1.0"
        self._exchange_config = EXCHANGE_CONFIG.get(symbol, {})
        # Track per-exchange prices for diagnostics
        self._last_exchange_prices: dict[str, float] = {}
        self._last_exchange_ts: dict[str, float] = {}
        # Fallback: if all exchanges fail, use last known price
        self._last_rti: Optional[float] = None

    def fetch_price(self) -> Optional[float]:
        """
        Fetch prices from all constituent exchanges and compute the
        volume-weighted RTI composite price.
        """
        now = time.time()
        raw_prices: dict[str, float] = {}

        for exchange, config in self._exchange_config.items():
            try:
                resp = self._session.get(config["url"], timeout=3)
                resp.raise_for_status()
                parser = PARSERS[config["parser"]]
                price = parser(resp.json())
                if price and price > 0:
                    raw_prices[exchange] = price
                    self._last_exchange_prices[exchange] = price
                    self._last_exchange_ts[exchange] = now
            except Exception:
                # Use last known price if fresh enough (< 30s old)
                if exchange in self._last_exchange_prices:
                    age = now - self._last_exchange_ts.get(exchange, 0)
                    if age < 30:
                        raw_prices[exchange] = self._last_exchange_prices[exchange]

        if not raw_prices:
            return self._last_rti

        # Step 1: Outlier filtering — remove prices >2 std devs from median
        filtered = self._filter_outliers(raw_prices)

        # Step 2: Volume-weighted average of remaining prices
        rti_price = self._weighted_average(filtered)

        if rti_price is not None:
            self.prices.append((now, rti_price))
            self._prune()
            self._last_rti = rti_price

        return rti_price

    def _filter_outliers(self, prices: dict[str, float]) -> dict[str, float]:
        """Remove prices that are >2 standard deviations from the median."""
        if len(prices) < 3:
            return prices  # Can't meaningfully filter with < 3 sources

        values = list(prices.values())
        med = statistics.median(values)
        std = statistics.stdev(values) if len(values) > 1 else 0

        if std == 0:
            return prices

        filtered = {}
        for exchange, price in prices.items():
            if abs(price - med) <= 2 * std:
                filtered[exchange] = price
            else:
                logger.debug(
                    f"[RTI] Filtered outlier: {exchange}={price:.2f} "
                    f"(median={med:.2f}, std={std:.2f})"
                )

        return filtered if filtered else prices  # Never return empty

    def _weighted_average(self, prices: dict[str, float]) -> Optional[float]:
        """Compute volume-weighted average using configured weights."""
        if not prices:
            return None

        total_weight = 0.0
        weighted_sum = 0.0

        for exchange, price in prices.items():
            weight = self._exchange_config.get(exchange, {}).get("weight", 0.1)
            weighted_sum += price * weight
            total_weight += weight

        if total_weight == 0:
            return None

        return weighted_sum / total_weight

    def _prune(self):
        """Remove prices older than the window."""
        cutoff = time.time() - self.window_seconds
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()

    # ── PriceFeed-compatible interface ──────────────────────────────

    @property
    def current_price(self) -> Optional[float]:
        if not self.prices:
            return None
        return self.prices[-1][1]

    @property
    def current_timestamp(self) -> Optional[float]:
        if not self.prices:
            return None
        return self.prices[-1][0]

    def price_at(self, seconds_ago: float) -> Optional[float]:
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
        if best_diff > 30:
            return None
        return best

    def momentum(self, lookback_seconds: float = 60) -> Optional[float]:
        current = self.current_price
        past = self.price_at(lookback_seconds)
        if current is None or past is None or past == 0:
            return None
        return ((current - past) / past) * 100

    def momentum_15m(self) -> Optional[float]:
        return self.momentum(lookback_seconds=900)

    def momentum_5m(self) -> Optional[float]:
        return self.momentum(lookback_seconds=300)

    def momentum_1m(self) -> Optional[float]:
        return self.momentum(lookback_seconds=60)

    def volatility(self, lookback_seconds: float = 300) -> Optional[float]:
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

        return statistics.stdev(returns) * 100  # as percentage

    def price_velocity(self, lookback_seconds: float = 30) -> Optional[float]:
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
        if not self.prices:
            return None

        span_prices = [
            px for ts, px in self.prices
            if ts >= time.time() - span_seconds
        ]
        alpha = 2.0 / (1 + max(1, len(span_prices)))

        ema_val = self.prices[0][1]
        for _, px in self.prices:
            ema_val = alpha * px + (1 - alpha) * ema_val
        return ema_val

    # ── RTI-specific diagnostics ────────────────────────────────────

    @property
    def exchange_spread_bps(self) -> Optional[float]:
        """Max spread between exchange prices in basis points."""
        now = time.time()
        recent = {
            ex: px for ex, px in self._last_exchange_prices.items()
            if now - self._last_exchange_ts.get(ex, 0) < 30
        }
        if len(recent) < 2:
            return None
        prices = list(recent.values())
        mid = statistics.median(prices)
        if mid == 0:
            return None
        return (max(prices) - min(prices)) / mid * 10000  # basis points

    @property
    def num_active_exchanges(self) -> int:
        """Number of exchanges providing recent data."""
        now = time.time()
        return sum(
            1 for ex in self._last_exchange_ts
            if now - self._last_exchange_ts[ex] < 30
        )
