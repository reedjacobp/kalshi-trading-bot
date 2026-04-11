"""
Kalshi WebSocket Client for Real-Time Market Data

Streams live ticker updates (yes_bid, yes_ask, last_price, volume)
for all subscribed crypto series via Kalshi's WebSocket API.

Runs in a background thread with auto-reconnect. Thread-safe
read access from the bot's synchronous main loop.

Optionally records price data to parquet for future calibration.
"""

import asyncio
import base64
import csv
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websockets
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


logger = logging.getLogger("kalshi_bot")

DEMO_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"
PROD_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


class MarketTick:
    """Thread-safe snapshot of a market's latest prices."""
    __slots__ = ("yes_bid", "yes_ask", "last_price", "volume", "ts")

    def __init__(self, yes_bid: int, yes_ask: int, last_price: int,
                 volume: float, ts: float):
        self.yes_bid = yes_bid
        self.yes_ask = yes_ask
        self.last_price = last_price
        self.volume = volume
        self.ts = ts


class KalshiWebSocket:
    """
    Background WebSocket client that streams live Kalshi market data.

    Usage:
        ws = KalshiWebSocket(key_id, private_key_path, env="prod")
        ws.subscribe(["KXBTC15M", "KXBTCD", "KXETH15M", ...])
        ws.start()

        # From any thread:
        tick = ws.get_tick("KXBTC15M-26APR080230-30")
        if tick:
            print(f"yes={tick.yes_bid}/{tick.yes_ask}")
    """

    def __init__(self, key_id: str, private_key_path: str, env: str = "prod"):
        self.key_id = key_id
        self.env = env
        self.ws_url = DEMO_WS_URL if env == "demo" else PROD_WS_URL

        # Load RSA key (same as KalshiClient)
        key_path = Path(private_key_path).expanduser()
        with open(key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

        # State
        self._series: list[str] = []
        self._ticks: dict[str, MarketTick] = {}  # ticker -> latest tick
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._cmd_id = 0
        self._stats = {"messages": 0, "reconnects": 0, "last_msg_ts": 0.0}
        self._recorder: Optional[TickRecorder] = None

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """RSA-PSS signature (same as KalshiClient)."""
        message = f"{timestamp_ms}{method}{path}".encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_headers(self) -> dict:
        """Generate auth headers for WebSocket handshake."""
        timestamp = str(int(time.time() * 1000))
        signature = self._sign(timestamp, "GET", WS_PATH)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }

    def subscribe(self, series_prefixes: list[str]):
        """Set the series prefixes to track (client-side filter)."""
        self._series = list(series_prefixes)

    def enable_recording(self, data_dir: str = "/mnt/d/datasets/prediction-market-analysis"):
        """Enable persistent recording of price data for calibration."""
        self._recorder = TickRecorder(data_dir)
        logger.info(f"[WS] Recording enabled: {data_dir}")

    def start(self):
        """Start the WebSocket in a background daemon thread."""
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="kalshi-ws")
        self._thread.start()
        logger.info("[WS] WebSocket thread started")

    def stop(self):
        """Stop the WebSocket thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def get_tick(self, ticker: str) -> Optional[MarketTick]:
        """Get the latest tick for a market (thread-safe)."""
        with self._lock:
            return self._ticks.get(ticker)

    def get_all_ticks(self) -> dict[str, MarketTick]:
        """Get a snapshot of all stored ticks (thread-safe)."""
        with self._lock:
            return dict(self._ticks)

    def get_yes_prices(self, ticker: str) -> tuple[Optional[int], Optional[int]]:
        """Get (yes_bid, yes_ask) in cents, or (None, None) if no data."""
        tick = self.get_tick(ticker)
        if tick is None or time.time() - tick.ts > 30:
            return None, None
        return tick.yes_bid, tick.yes_ask

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def _next_cmd_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    def _run_loop(self):
        """Entry point for the background thread — runs asyncio event loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._connect_loop())
        except Exception as e:
            logger.error(f"[WS] Event loop crashed: {e}")
        finally:
            loop.close()

    async def _connect_loop(self):
        """Reconnect loop — keeps trying to connect/reconnect."""
        backoff = 1
        while self._running:
            try:
                await self._session()
                backoff = 1  # reset on clean disconnect
            except websockets.exceptions.ConnectionClosedError as e:
                logger.warning(f"[WS] Connection closed: {e}")
            except Exception as e:
                logger.warning(f"[WS] Connection error: {e}")

            self._connected = False
            if not self._running:
                break

            self._stats["reconnects"] += 1
            wait = min(backoff, 30)
            logger.info(f"[WS] Reconnecting in {wait}s...")
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, 30)

    async def _session(self):
        """Single WebSocket session — connect, subscribe, stream."""
        headers = self._auth_headers()
        logger.info(f"[WS] Connecting to {self.ws_url}...")

        async with websockets.connect(
            self.ws_url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._connected = True
            logger.info("[WS] Connected, subscribing to ticker channel...")

            # Subscribe to all tickers (filter client-side by series prefix)
            sub_msg = {
                "id": self._next_cmd_id(),
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker"],
                },
            }
            await ws.send(json.dumps(sub_msg))

            # Process messages
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    self._handle_message(msg)
                except json.JSONDecodeError:
                    logger.warning(f"[WS] Invalid JSON: {raw[:100]}")

    def _handle_message(self, msg: dict):
        """Process an incoming WebSocket message."""
        msg_type = msg.get("type", "")

        if msg_type == "ticker":
            self._handle_ticker(msg.get("msg", {}))
        elif msg_type == "error":
            logger.error(f"[WS] Error: {msg}")
        elif msg_type == "ok":
            subs = msg.get("msg", [])
            for s in subs:
                logger.info(f"[WS] Subscribed to {s.get('channel')} (sid={s.get('sid')})")

        self._stats["messages"] += 1
        self._stats["last_msg_ts"] = time.time()

    def _handle_ticker(self, data: dict):
        """Process a ticker update — store latest prices for ALL markets."""
        ticker = data.get("market_ticker", "")
        if not ticker:
            return

        is_tracked_series = not self._series or any(ticker.startswith(s) for s in self._series)

        # Parse prices — API sends dollars, we store cents
        def to_cents(val):
            if val is None:
                return 0
            if isinstance(val, (int, float)):
                return int(round(val * 100))
            try:
                return int(round(float(val) * 100))
            except (ValueError, TypeError):
                return 0

        yes_bid = to_cents(data.get("yes_bid_dollars"))
        yes_ask = to_cents(data.get("yes_ask_dollars"))
        last_price = to_cents(data.get("price_dollars"))
        volume = float(data.get("volume_fp", 0) or 0)

        tick = MarketTick(
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            last_price=last_price,
            volume=volume,
            ts=time.time(),
        )

        with self._lock:
            prev = self._ticks.get(ticker)
            self._ticks[ticker] = tick

        # Record on price change only (deduped) for calibration data
        # Only record tracked series to avoid filling disk with all of Kalshi
        if self._recorder and is_tracked_series:
            if prev is None or prev.yes_bid != yes_bid or prev.yes_ask != yes_ask:
                self._recorder.record(ticker, yes_bid, yes_ask, last_price, volume)


class TickRecorder:
    """
    Records contract price changes to daily CSV files on disk.

    Saves only when bid/ask changes (deduped), keeping ~5 MB/day
    across all crypto markets. Data is stored as CSV for simplicity
    and append-friendliness, with periodic parquet conversion available.

    File layout: {data_dir}/ticks/YYYY-MM-DD.csv
    """

    CSV_COLUMNS = ["timestamp", "ticker", "yes_bid", "yes_ask", "last_price", "volume"]

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir) / "ticks"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: str = ""
        self._file = None
        self._writer = None
        self._rows_written = 0

    def record(self, ticker: str, yes_bid: int, yes_ask: int,
               last_price: int, volume: float):
        """Append a tick to today's CSV file."""
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")

        # Roll to new file at midnight UTC
        if date_str != self._current_date:
            self._roll_file(date_str)

        self._writer.writerow([
            now.isoformat(),
            ticker,
            yes_bid,
            yes_ask,
            last_price,
            f"{volume:.0f}",
        ])
        self._file.flush()
        self._rows_written += 1

    def _roll_file(self, date_str: str):
        """Open a new daily CSV file."""
        if self._file:
            self._file.close()

        self._current_date = date_str
        filepath = self.data_dir / f"{date_str}.csv"
        is_new = not filepath.exists()

        self._file = open(filepath, "a", newline="")
        self._writer = csv.writer(self._file)

        if is_new:
            self._writer.writerow(self.CSV_COLUMNS)
            self._file.flush()

        logger.info(f"[REC] Recording to {filepath}")

    def close(self):
        """Close the current file."""
        if self._file:
            self._file.close()
            self._file = None

    @property
    def rows_today(self) -> int:
        return self._rows_written
