"""
Real-Time Crypto Price WebSocket Feeds

Streams live trade prices from Binance and Coinbase via WebSocket,
providing sub-second price updates for the RTI composite calculation.

Binance (20-30% RTI weight): single connection for BTC/ETH/SOL
Coinbase (30% RTI weight): single connection for BTC/ETH/SOL

Together these cover ~50-60% of RTI weight with real-time data.
Remaining exchanges (Kraken, Bitstamp, Gemini) use REST polling
in a background thread at ~5s intervals.
"""

import asyncio
import json
import logging
import threading
import time
from collections import defaultdict
from typing import Callable, Optional

import websockets
import requests

logger = logging.getLogger("kalshi_bot")


class CryptoWSFeed:
    """
    Aggregates real-time crypto prices from multiple exchange WebSockets.

    Provides per-exchange latest prices that RTIFeed reads to compute
    the volume-weighted composite.

    Usage:
        feed = CryptoWSFeed()
        feed.start()
        price = feed.get_price("binance", "BTC-USD")  # latest Binance BTC
    """

    # Binance US WebSocket (binance.com blocks US IPs with HTTP 451)
    BINANCE_WS = ("wss://stream.binance.us:9443/stream?streams="
                   "btcusd@trade/ethusd@trade/solusd@trade/"
                   "dogeusd@trade/xrpusd@trade/bnbusd@trade")

    # Coinbase Advanced Trade WebSocket
    COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

    # Symbol mapping: our symbol -> exchange symbol
    BINANCE_SYMBOLS = {
        "btcusd": "BTC-USD", "ethusd": "ETH-USD", "solusd": "SOL-USD",
        "dogeusd": "DOGE-USD", "xrpusd": "XRP-USD", "bnbusd": "BNB-USD",
    }
    COINBASE_SYMBOLS = [
        "BTC-USD", "ETH-USD", "SOL-USD",
        "DOGE-USD", "XRP-USD",
        # BNB and HYPE not on Coinbase — Binance only
    ]

    # REST fallback exchanges (polled in background)
    REST_EXCHANGES = {
        "kraken": {
            "BTC-USD": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
            "ETH-USD": "https://api.kraken.com/0/public/Ticker?pair=ETHUSD",
            "SOL-USD": "https://api.kraken.com/0/public/Ticker?pair=SOLUSD",
            "DOGE-USD": "https://api.kraken.com/0/public/Ticker?pair=DOGEUSD",
            "XRP-USD": "https://api.kraken.com/0/public/Ticker?pair=XRPUSD",
        },
        "bitstamp": {
            "BTC-USD": "https://www.bitstamp.net/api/v2/ticker/btcusd/",
            "ETH-USD": "https://www.bitstamp.net/api/v2/ticker/ethusd/",
            "SOL-USD": "https://www.bitstamp.net/api/v2/ticker/solusd/",
            "DOGE-USD": "https://www.bitstamp.net/api/v2/ticker/dogeusd/",
            "XRP-USD": "https://www.bitstamp.net/api/v2/ticker/xrpusd/",
        },
        "gemini": {
            "BTC-USD": "https://api.gemini.com/v1/pubticker/btcusd",
            "ETH-USD": "https://api.gemini.com/v1/pubticker/ethusd",
            "SOL-USD": "https://api.gemini.com/v1/pubticker/solusd",
        },
    }

    def __init__(self):
        # exchange -> symbol -> (price, timestamp)
        self._prices: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
        self._lock = threading.Lock()
        self._running = False
        self._threads: list[threading.Thread] = []
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "kalshi-bot/1.0"
        self._on_update: Optional[Callable] = None

    def on_update(self, callback: Callable):
        """Set callback for when any price updates. Called with (exchange, symbol, price)."""
        self._on_update = callback

    def start(self):
        """Start all WebSocket and REST polling threads."""
        self._running = True

        # Binance WebSocket thread
        t1 = threading.Thread(target=self._run_binance, daemon=True, name="binance-ws")
        t1.start()
        self._threads.append(t1)

        # Coinbase WebSocket thread
        t2 = threading.Thread(target=self._run_coinbase, daemon=True, name="coinbase-ws")
        t2.start()
        self._threads.append(t2)

        # REST polling thread for remaining exchanges
        t3 = threading.Thread(target=self._run_rest_poll, daemon=True, name="rest-poll")
        t3.start()
        self._threads.append(t3)

        logger.info("[CRYPTO-WS] Started Binance + Coinbase WebSocket feeds + REST polling")

    def stop(self):
        self._running = False
        for t in self._threads:
            t.join(timeout=3)

    def get_price(self, exchange: str, symbol: str) -> Optional[tuple[float, float]]:
        """Get (price, timestamp) for an exchange+symbol, or None."""
        with self._lock:
            return self._prices.get(exchange, {}).get(symbol)

    def get_all_prices(self, symbol: str) -> dict[str, tuple[float, float]]:
        """Get all exchange prices for a symbol: {exchange: (price, ts)}."""
        with self._lock:
            result = {}
            for exchange, symbols in self._prices.items():
                if symbol in symbols:
                    result[exchange] = symbols[symbol]
            return result

    def _set_price(self, exchange: str, symbol: str, price: float):
        now = time.time()
        with self._lock:
            self._prices[exchange][symbol] = (price, now)
        if self._on_update:
            try:
                self._on_update(exchange, symbol, price)
            except Exception:
                pass

    # ── Binance WebSocket ──────────────────────────────────────────

    def _run_binance(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._binance_loop())
        except Exception as e:
            logger.error(f"[BINANCE-WS] Loop crashed: {e}")
        finally:
            loop.close()

    async def _binance_loop(self):
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(
                    self.BINANCE_WS, ping_interval=20, ping_timeout=10
                ) as ws:
                    backoff = 1
                    logger.info("[BINANCE-WS] Connected")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", {})
                            stream = msg.get("stream", "")
                            # Trade stream: {"p":"71234.56", "s":"BTCUSDT", ...}
                            price_str = data.get("p")
                            sym_raw = stream.split("@")[0] if "@" in stream else ""
                            symbol = self.BINANCE_SYMBOLS.get(sym_raw)
                            if price_str and symbol:
                                self._set_price("binance", symbol, float(price_str))
                        except (json.JSONDecodeError, ValueError):
                            pass
            except Exception as e:
                if self._running:
                    logger.warning(f"[BINANCE-WS] Disconnected: {e}, reconnecting in {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)

    # ── Coinbase WebSocket ─────────────────────────────────────────

    def _run_coinbase(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._coinbase_loop())
        except Exception as e:
            logger.error(f"[COINBASE-WS] Loop crashed: {e}")
        finally:
            loop.close()

    async def _coinbase_loop(self):
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(
                    self.COINBASE_WS, ping_interval=20, ping_timeout=10
                ) as ws:
                    backoff = 1
                    # Subscribe to ticker for all crypto symbols
                    sub = {
                        "type": "subscribe",
                        "channels": [
                            {"name": "ticker", "product_ids": self.COINBASE_SYMBOLS}
                        ],
                    }
                    await ws.send(json.dumps(sub))
                    logger.info("[COINBASE-WS] Connected and subscribed")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            if msg.get("type") == "ticker":
                                symbol = msg.get("product_id", "")
                                price_str = msg.get("price")
                                if symbol in self.COINBASE_SYMBOLS and price_str:
                                    self._set_price("coinbase", symbol, float(price_str))
                        except (json.JSONDecodeError, ValueError):
                            pass
            except Exception as e:
                if self._running:
                    logger.warning(f"[COINBASE-WS] Disconnected: {e}, reconnecting in {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)

    # ── REST Polling (Kraken, Bitstamp, Gemini) ────────────────────

    def _run_rest_poll(self):
        """Poll remaining exchanges every 3 seconds."""
        while self._running:
            for exchange, symbols in self.REST_EXCHANGES.items():
                for symbol, url in symbols.items():
                    try:
                        resp = self._session.get(url, timeout=3)
                        resp.raise_for_status()
                        data = resp.json()
                        price = self._parse_rest(exchange, data)
                        if price and price > 0:
                            self._set_price(exchange, symbol, price)
                    except Exception:
                        pass

                    if not self._running:
                        return

            time.sleep(3)

    @staticmethod
    def _parse_rest(exchange: str, data: dict) -> Optional[float]:
        try:
            if exchange == "kraken":
                for pair_data in data.get("result", {}).values():
                    return float(pair_data["c"][0])
            elif exchange == "bitstamp":
                return float(data["last"])
            elif exchange == "gemini":
                return float(data["last"])
        except (KeyError, ValueError, TypeError):
            return None
        return None
