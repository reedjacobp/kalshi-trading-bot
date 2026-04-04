"""
Kalshi API Client with RSA-PSS Authentication

Handles all communication with the Kalshi Trading API v2.
Supports both demo and production environments.
"""

import base64
import datetime
import json
import time
import uuid
from pathlib import Path
from typing import Optional

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiClient:
    """Authenticated client for the Kalshi Trading API v2."""

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
        """Make an authenticated request to the Kalshi API."""
        url = f"{self.base_url}{path}"
        # Build the full path for signing (includes /trade-api/v2 prefix)
        from urllib.parse import urlparse
        sign_path = urlparse(url).path
        headers = self._auth_headers(method.upper(), sign_path)

        resp = self.session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=data,
        )
        resp.raise_for_status()
        return resp.json()

    def _public_get(self, path: str, params: dict = None):
        """Make an unauthenticated GET (public market data)."""
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

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
        """
        if client_order_id is None:
            client_order_id = str(uuid.uuid4())

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

        return self._request("POST", "/portfolio/orders", data=order_data)

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        return self._request("DELETE", f"/portfolio/orders/{order_id}")

    def amend_order(self, order_id: str, data: dict) -> dict:
        """Amend an existing order."""
        return self._request("PUT", f"/portfolio/orders/{order_id}", data=data)
