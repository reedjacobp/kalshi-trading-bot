"""Tests for Kalshi API client retry/backoff logic."""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock

import requests
from requests.exceptions import ConnectionError, Timeout

from kalshi_client import KalshiClient


class TestRetryLogic(unittest.TestCase):
    """Test that API calls retry on transient failures."""

    def _make_client(self):
        """Create a client with a mocked private key (skip file I/O)."""
        with patch.object(KalshiClient, "__init__", lambda self, *a, **kw: None):
            client = KalshiClient.__new__(KalshiClient)
            client.key_id = "test-key"
            client.base_url = "https://demo-api.kalshi.co/trade-api/v2"
            client.env = "demo"
            client.session = MagicMock()
            client.private_key = MagicMock()
            # Mock the sign method
            client._sign = MagicMock(return_value="fake-sig")
            return client

    @patch("kalshi_client.time.sleep")  # Don't actually sleep in tests
    def test_public_get_retries_on_connection_error(self, mock_sleep):
        client = self._make_client()

        # Fail twice, succeed third time
        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"markets": []}
        success_resp.raise_for_status = MagicMock()

        client.session.get.side_effect = [
            ConnectionError("Connection reset"),
            ConnectionError("Connection reset"),
            success_resp,
        ]

        result = client._public_get("/markets")
        self.assertEqual(result, {"markets": []})
        self.assertEqual(client.session.get.call_count, 3)

    @patch("kalshi_client.time.sleep")
    def test_public_get_raises_after_3_failures(self, mock_sleep):
        client = self._make_client()
        client.session.get.side_effect = Timeout("Timed out")

        with self.assertRaises(Timeout):
            client._public_get("/markets")
        self.assertEqual(client.session.get.call_count, 3)

    @patch("kalshi_client.time.sleep")
    def test_public_get_no_retry_on_client_error(self, mock_sleep):
        client = self._make_client()

        error_resp = MagicMock()
        error_resp.status_code = 400
        error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=error_resp
        )

        client.session.get.return_value = error_resp

        with self.assertRaises(requests.exceptions.HTTPError):
            client._public_get("/markets")
        self.assertEqual(client.session.get.call_count, 1)

    @patch("kalshi_client.time.sleep")
    def test_request_retries_on_server_error(self, mock_sleep):
        client = self._make_client()

        # First call: 500, second call: success
        error_resp = MagicMock()
        error_resp.status_code = 500
        http_error = requests.exceptions.HTTPError(response=error_resp)
        error_resp.raise_for_status.side_effect = http_error

        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"balance": 10000}
        success_resp.raise_for_status = MagicMock()

        client.session.request.side_effect = [error_resp, success_resp]

        result = client._request("GET", "/portfolio/balance")
        self.assertEqual(result, {"balance": 10000})
        self.assertEqual(client.session.request.call_count, 2)

    @patch("kalshi_client.time.sleep")
    def test_rate_limit_429_retries(self, mock_sleep):
        client = self._make_client()

        rate_limited_resp = MagicMock()
        rate_limited_resp.status_code = 429
        rate_limited_resp.headers = {"Retry-After": "1"}

        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"markets": []}
        success_resp.raise_for_status = MagicMock()

        client.session.get.side_effect = [rate_limited_resp, success_resp]

        result = client._public_get("/markets")
        self.assertEqual(result, {"markets": []})
        self.assertEqual(client.session.get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
