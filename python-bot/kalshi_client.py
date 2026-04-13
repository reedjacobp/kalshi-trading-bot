"""
Kalshi API Client with RSA-PSS Authentication

Handles all communication with the Kalshi Trading API v2.
Supports both demo and production environments.
"""

import base64
import datetime
import json
import logging
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


class KalshiClient:
    """Authenticated client for the Kalshi Trading API v2."""

    # Rate limiter shared across all instances (Kalshi rate-limits per API key)
    _rate_lock = threading.Lock()
    _request_times: list[float] = []
    _max_requests_per_second = 8  # Stay under Kalshi's limit with headroom for orders

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

    def _wait_for_rate_limit(self):
        """Block until we're under the per-second request cap."""
        with self._rate_lock:
            now = time.monotonic()
            # Prune timestamps older than 1 second
            self._request_times = [t for t in self._request_times if now - t < 1.0]
            if len(self._request_times) >= self._max_requests_per_second:
                sleep_until = self._request_times[0] + 1.0
                wait = sleep_until - now
                if wait > 0:
                    time.sleep(wait)
                # Prune again after sleeping
                now = time.monotonic()
                self._request_times = [t for t in self._request_times if now - t < 1.0]
            self._request_times.append(time.monotonic())

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

    def _request(self, method: str, path: str, params: dict = None, data: dict = None):
        """Make an authenticated request to the Kalshi API with retry/backoff."""
        from urllib.parse import urlparse

        last_exc = None
        for attempt in range(3):
            self._wait_for_rate_limit()
            try:
                url = f"{self.base_url}{path}"
                sign_path = urlparse(url).path
                # Re-sign on each attempt (timestamp must be fresh)
                headers = self._auth_headers(method.upper(), sign_path)

                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=data,
                    timeout=10,
                )
                # Don't retry client errors (4xx) except 429
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                    logger.warning(f"Rate limited (429), retrying in {retry_after}s")
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (ConnectionError, Timeout, ChunkedEncodingError) as e:
                last_exc = e
                wait = 2 ** attempt
                logger.warning(f"Request failed (attempt {attempt + 1}/3): {e}, retrying in {wait}s")
                time.sleep(wait)
            except HTTPError as e:
                if e.response is not None and e.response.status_code < 500:
                    raise  # Don't retry client errors
                last_exc = e
                wait = 2 ** attempt
                logger.warning(f"Server error (attempt {attempt + 1}/3): {e}, retrying in {wait}s")
                time.sleep(wait)
        raise last_exc

    def _public_get(self, path: str, params: dict = None):
        """Make an unauthenticated GET (public market data) with retry/backoff."""
        last_exc = None
        for attempt in range(3):
            self._wait_for_rate_limit()
            try:
                url = f"{self.base_url}{path}"
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                    logger.warning(f"Rate limited (429), retrying in {retry_after}s")
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (ConnectionError, Timeout, ChunkedEncodingError) as e:
                last_exc = e
                wait = 2 ** attempt
                logger.warning(f"Public GET failed (attempt {attempt + 1}/3): {e}, retrying in {wait}s")
                time.sleep(wait)
            except HTTPError as e:
                if e.response is not None and e.response.status_code < 500:
                    raise
                last_exc = e
                wait = 2 ** attempt
                logger.warning(f"Server error (attempt {attempt + 1}/3): {e}, retrying in {wait}s")
                time.sleep(wait)
        raise last_exc

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
