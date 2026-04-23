"""
Kalshi API Client with RSA-PSS Authentication

Handles all communication with the Kalshi Trading API v2.
Supports both demo and production environments.
"""

import base64
import datetime
import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError, Timeout
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


logger = logging.getLogger("kalshi_bot")


DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class _TokenBucket:
    """Thread-safe token-bucket rate limiter with adaptive backoff.

    acquire() blocks until one token is available; up to `burst` tokens
    can be consumed at once, then refills at `rate` tokens/sec. penalize(s)
    drains the bucket so every subsequent acquire() waits at least `s`
    seconds — this is how a 429 is propagated to every concurrent caller,
    not just the one that got throttled.
    """

    __slots__ = ("_rate", "_burst", "_tokens", "_last", "_lock", "_waits")

    def __init__(self, rate: float, burst: float):
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._last = time.monotonic()
        self._lock = threading.Lock()
        self._waits = 0

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self._burst,
                    self._tokens + (now - self._last) * self._rate,
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
                self._waits += 1
            time.sleep(wait)

    def penalize(self, seconds: float) -> None:
        """Drain the bucket so next acquire() waits at least `seconds`."""
        if seconds <= 0:
            return
        with self._lock:
            floor = 1.0 - seconds * self._rate
            self._tokens = min(self._tokens, floor)
            self._last = time.monotonic()

    def drain_waits(self) -> int:
        """Return the number of blocking waits since last call, reset counter."""
        with self._lock:
            w = self._waits
            self._waits = 0
            return w


def _parse_retry_after(value, attempt: int) -> float:
    """Parse the server's Retry-After header, falling back to bounded
    exponential backoff if the value is missing or malformed."""
    fallback = float(min(1 << max(0, attempt), 30))
    if value is None:
        return fallback
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return fallback


class KalshiClient:
    """Authenticated client for the Kalshi Trading API v2."""

    # Kalshi's documented per-key caps are 20 reads/sec and 10 writes/sec.
    # We run at 75% of each to leave headroom for bursts from scanner
    # refresh cycles. Env-tunable — lower if you see recurring 429s.
    _READ_RATE = float(os.environ.get("KALSHI_READ_RATE", "15"))
    _WRITE_RATE = float(os.environ.get("KALSHI_WRITE_RATE", "8"))

    # Shared across all instances (Kalshi limits are per API key).
    _read_bucket = _TokenBucket(_READ_RATE, _READ_RATE)
    _write_bucket = _TokenBucket(_WRITE_RATE, _WRITE_RATE)

    # Per-minute call stats for the [KALSHI-API] summary line.
    _stats_lock = threading.Lock()
    _stats = {"reads": 0, "writes": 0, "r429": 0, "w429": 0,
              "last_report": time.monotonic()}

    def __init__(self, key_id: str, private_key_path: str, env: str = "demo"):
        self.key_id = key_id
        self.base_url = DEMO_BASE_URL if env == "demo" else PROD_BASE_URL
        self.env = env
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # Load RSA private key
        key_path = Path(private_key_path).expanduser()
        with open(key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

    def _record_call(self, is_read: bool, is_429: bool = False) -> None:
        """Bump per-minute call counters; emit a summary line every 60s.

        One summary per minute across all instances so the journal
        shows true aggregate throughput vs Kalshi's per-key limit.
        """
        cls = type(self)
        with cls._stats_lock:
            s = cls._stats
            if is_429:
                s["r429" if is_read else "w429"] += 1
            elif is_read:
                s["reads"] += 1
            else:
                s["writes"] += 1
            now = time.monotonic()
            if now - s["last_report"] < 60.0:
                return
            elapsed = now - s["last_report"]
            r_per_s = s["reads"] / elapsed
            w_per_s = s["writes"] / elapsed
            r429, w429 = s["r429"], s["w429"]
            s["reads"] = s["writes"] = s["r429"] = s["w429"] = 0
            s["last_report"] = now
        rw = cls._read_bucket.drain_waits()
        ww = cls._write_bucket.drain_waits()
        print(
            f"[KALSHI-API] last 60s: "
            f"reads={r_per_s:.1f}/s ({r429} × 429, {rw} waits), "
            f"writes={w_per_s:.1f}/s ({w429} × 429, {ww} waits)",
            flush=True,
        )

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """Create RSA-PSS signature for request authentication."""
        # Strip query parameters before signing
        path_clean = path.split("?")[0]
        message = f"{timestamp_ms}{method}{path_clean}".encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_headers(self, method: str, path: str) -> dict:
        """Generate the three required auth headers."""
        timestamp = str(int(time.time() * 1000))
        signature = self._sign(timestamp, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }

    _MAX_ATTEMPTS = 3
    _TRANSIENT_BACKOFF_CAP = 4.0  # seconds, for network/5xx retries

    def _do_http(self, method: str, path: str, *,
                 params: dict = None, data: dict = None,
                 authenticate: bool) -> dict:
        """Execute an HTTP call with token-bucket rate limit and bounded retry.

        Reads (GET) and writes (POST/DELETE/PUT) consume from separate
        buckets so a read burst cannot starve writes (or vice versa) —
        Kalshi's per-key limits are 20 reads/sec and 10 writes/sec. A
        429 drains the matching bucket by Retry-After seconds, which
        naturally paces every concurrent caller's next acquire(); we do
        not sleep in the caller thread on 429 because the next retry's
        bucket.acquire() will wait precisely as long as needed.

        Non-429 4xx responses are treated as client errors and raise
        immediately. 5xx and network errors get bounded exponential
        backoff, capped at _TRANSIENT_BACKOFF_CAP seconds.
        """
        from urllib.parse import urlparse

        method_u = method.upper()
        is_read = method_u == "GET"
        bucket = type(self)._read_bucket if is_read else type(self)._write_bucket
        url = f"{self.base_url}{path}"

        last_exc: Optional[Exception] = None
        for attempt in range(self._MAX_ATTEMPTS):
            bucket.acquire()
            try:
                kwargs: dict = {"params": params, "json": data, "timeout": 10}
                if authenticate:
                    sign_path = urlparse(url).path
                    # Re-sign each attempt — timestamp must be fresh.
                    kwargs["headers"] = self._auth_headers(method_u, sign_path)

                resp = self.session.request(method_u, url, **kwargs)

                if resp.status_code == 429:
                    retry_after = _parse_retry_after(
                        resp.headers.get("Retry-After"), attempt)
                    bucket.penalize(retry_after)
                    self._record_call(is_read, is_429=True)
                    last_exc = HTTPError(
                        f"429 Too Many Requests on {method_u} {path}",
                        response=resp)
                    if attempt + 1 < self._MAX_ATTEMPTS:
                        logger.warning(
                            f"Rate limited (429) on {method_u} {path}; "
                            f"bucket backing off {retry_after:.1f}s")
                        continue
                    break  # give up after max attempts

                resp.raise_for_status()
                self._record_call(is_read)
                return resp.json()

            except (ConnectionError, Timeout, ChunkedEncodingError) as e:
                last_exc = e
                if attempt + 1 >= self._MAX_ATTEMPTS:
                    break
                wait = min(float(1 << attempt), self._TRANSIENT_BACKOFF_CAP)
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self._MAX_ATTEMPTS}) "
                    f"on {method_u} {path}: {e}; retrying in {wait:.1f}s")
                time.sleep(wait)

            except HTTPError as e:
                status = (e.response.status_code
                          if e.response is not None else 500)
                # 4xx (except 429, handled above) is a client error — don't retry.
                if 400 <= status < 500:
                    raise
                last_exc = e
                if attempt + 1 >= self._MAX_ATTEMPTS:
                    break
                wait = min(float(1 << attempt), self._TRANSIENT_BACKOFF_CAP)
                logger.warning(
                    f"Server error (attempt {attempt + 1}/{self._MAX_ATTEMPTS}) "
                    f"on {method_u} {path}: {e}; retrying in {wait:.1f}s")
                time.sleep(wait)

        raise last_exc if last_exc else RuntimeError(
            f"Request to {method_u} {path} failed without an exception")

    def _request(self, method: str, path: str, params: dict = None, data: dict = None):
        """Authenticated request to the Kalshi API."""
        return self._do_http(method, path, params=params, data=data,
                             authenticate=True)

    def _public_get(self, path: str, params: dict = None):
        """Unauthenticated GET for public market data."""
        return self._do_http("GET", path, params=params, authenticate=False)

    # ─── Market Data (Public) ────────────────────────────────────────────

    def get_markets(
        self,
        series_ticker: str = None,
        event_ticker: str = None,
        status: str = None,
        limit: int = 100,
        min_close_ts: int = None,
        max_close_ts: int = None,
        cursor: str = None,
    ) -> dict:
        """List markets with optional filters."""
        params = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if status:
            params["status"] = status
        if min_close_ts:
            params["min_close_ts"] = min_close_ts
        if max_close_ts:
            params["max_close_ts"] = max_close_ts
        if cursor:
            params["cursor"] = cursor
        return self._public_get("/markets", params=params)

    def get_market(self, ticker: str) -> dict:
        """Get a single market by ticker."""
        return self._public_get(f"/markets/{ticker}")

    def get_orderbook(self, ticker: str, depth: int = 10) -> dict:
        """Get the order book for a market."""
        return self._public_get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    def get_trades(self, ticker: str = None, limit: int = 100, min_ts: int = None) -> dict:
        """Get recent trades, optionally filtered by market ticker."""
        params = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if min_ts:
            params["min_ts"] = min_ts
        return self._public_get("/markets/trades", params=params)

    def get_events(self, series_ticker: str = None, status: str = None, limit: int = 100) -> dict:
        """List events with optional filters."""
        params = {"limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if status:
            params["status"] = status
        return self._public_get("/events", params=params)

    # ─── Trading (Authenticated) ─────────────────────────────────────────

    def get_balance(self) -> dict:
        """Get account balance."""
        return self._request("GET", "/portfolio/balance")

    def get_positions(self, **kwargs) -> dict:
        """Get current positions."""
        return self._request("GET", "/portfolio/positions", params=kwargs)

    def get_fills(self, **kwargs) -> dict:
        """Get trade fill history."""
        return self._request("GET", "/portfolio/fills", params=kwargs)

    def get_orders(self, **kwargs) -> dict:
        """Get open orders."""
        return self._request("GET", "/portfolio/orders", params=kwargs)

    def place_order(
        self,
        ticker: str,
        action: str,
        side: str,
        count: int,
        order_type: str = "limit",
        yes_price: int = None,
        client_order_id: str = None,
        expiration_ts: int = None,
        reduce_only: bool = False,
        time_in_force: str = None,
    ) -> dict:
        """
        Place an order on a market.

        Args:
            ticker: Market ticker (e.g. "KXBTC15M-26APR03-T1600")
            action: "buy" or "sell"
            side: "yes" or "no"
            count: Number of contracts
            order_type: "limit" or "market"
            yes_price: Price in cents (1-99), required for limit orders
            client_order_id: Unique ID for deduplication (auto-generated if not provided)
            expiration_ts: Unix timestamp for order expiration (optional)
            reduce_only: If True, the order can ONLY reduce an existing
                position (never open a new one). Required on exit/sell
                orders — without it, Kalshi creates a new offsetting
                position instead of closing the existing long. Note that
                reduce_only orders MUST use time_in_force="immediate_or_cancel".
            time_in_force: Order duration. One of "fill_or_kill",
                "good_till_canceled", or "immediate_or_cancel". Required
                to be "immediate_or_cancel" when reduce_only is True.
        """
        if client_order_id is None:
            client_order_id = str(uuid.uuid4())

        # Kalshi requires IoC time-in-force when using reduce_only.
        if reduce_only and time_in_force is None:
            time_in_force = "immediate_or_cancel"

        order_data = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": order_type,
            "client_order_id": client_order_id,
        }
        if yes_price is not None:
            order_data["yes_price"] = yes_price
        if expiration_ts is not None:
            order_data["expiration_ts"] = expiration_ts
        if reduce_only:
            order_data["reduce_only"] = True
        if time_in_force is not None:
            order_data["time_in_force"] = time_in_force

        return self._request("POST", "/portfolio/orders", data=order_data)

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        return self._request("DELETE", f"/portfolio/orders/{order_id}")

    def amend_order(self, order_id: str, data: dict) -> dict:
        """Amend an existing order."""
        return self._request("PUT", f"/portfolio/orders/{order_id}", data=data)
