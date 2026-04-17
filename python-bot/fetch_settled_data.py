"""
Fetch settled Kalshi crypto markets and convert to optimizer-compatible
tick windows. This produces "best case" training data: real settlement
outcomes with last_price as the entry proxy (no spread, no timing noise).

Usage:
    python fetch_settled_data.py [--series KXBTCD KXBTC15M ...] [--max-pages 300]

Output:
    data/settled_windows.pkl — list of tick windows ready for optimize_rr.py
"""

import os
import sys
import json
import time
import pickle
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

import pandas as pd

# Use our existing authenticated client
from kalshi_client import KalshiClient


# All crypto series we trade
ALL_SERIES = [
    "KXBTC15M", "KXBTCD",
    "KXETH15M", "KXETHD",
    "KXSOL15M", "KXSOLD",
    "KXDOGE15M", "KXDOGED",
    "KXXRP15M",  "KXXRPD",
    "KXBNB15M",  "KXBNBD",
    "KXHYPE15M", "KXHYPED",
    "KXSHIBAD",
]

# Map series ticker to (coin, market_type) — mirrors classify_ticker
SERIES_MAP = {}
for _coin in ["BTC", "ETH", "SOL", "DOGE", "XRP", "BNB", "HYPE"]:
    SERIES_MAP[f"KX{_coin}15M"] = (_coin.lower(), "15m")
    SERIES_MAP[f"KX{_coin}D"] = (_coin.lower(), "hourly")
SERIES_MAP["KXSHIBAD"] = ("shiba", "daily")


def fetch_settled_markets(client: KalshiClient, series: str,
                          max_pages: int = 300) -> list[dict]:
    """Paginate through all settled markets for a series.

    Uses authenticated _request to get higher rate limits and adds
    generous spacing to avoid 429s (the bot may be running too).
    """
    all_markets = []
    cursor = None

    for page in range(max_pages):
        params = {
            "series_ticker": series,
            "status": "settled",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            result = client._request("GET", "/markets", params=params)
        except Exception as e:
            print(f"    page {page + 1}: error {e}, stopping")
            break

        markets = result.get("markets", [])
        if not markets:
            break

        all_markets.extend(markets)
        cursor = result.get("cursor")
        if not cursor:
            break

        if (page + 1) % 50 == 0:
            print(f"    page {page + 1}: {len(all_markets)} markets so far")
        time.sleep(0.05)  # ~8 req/s — safe when bot isn't running

    return all_markets


def market_to_window(market: dict, coin: str, mtype: str) -> dict | None:
    """Convert a settled Kalshi market to a tick window for the optimizer.

    Best-case assumptions:
    - Entry at last_price (no spread)
    - Single tick at 60s before close
    - Real settlement outcome
    """
    ticker = market.get("ticker", "")
    close_time_str = market.get("close_time", "")
    result = market.get("result", "")
    last_price = market.get("last_price_dollars")
    volume = market.get("volume_fp", 0)
    strike = market.get("floor_strike")
    settlement_price = market.get("expiration_value")

    # Need valid result and price
    if result not in ("yes", "no"):
        return None
    try:
        last_price = float(last_price)
    except (TypeError, ValueError):
        return None
    if last_price <= 0:
        return None

    # Convert last_price to cents
    price_cents = int(round(last_price * 100))
    if price_cents < 1 or price_cents > 99:
        return None

    # Parse close time
    try:
        close_time = pd.Timestamp(close_time_str)
        if close_time.tzinfo is None:
            close_time = close_time.tz_localize("UTC")
    except Exception:
        return None

    # Parse strike
    strike_val = None
    if strike is not None:
        try:
            strike_val = float(strike)
            if strike_val <= 0:
                strike_val = None
        except (ValueError, TypeError):
            pass

    # Create a synthetic tick at 60s before close (best case entry point)
    tick_time = close_time - pd.Timedelta(seconds=60)

    # Best case: no spread, bid = ask = last_price
    tick = {
        "timestamp": tick_time,
        "ticker": ticker,
        "yes_bid": price_cents,
        "yes_ask": price_cents,
        "last_price": price_cents,
        "volume": float(volume or 0),
        "floor_strike": strike_val,
    }

    return {
        "ticker": ticker,
        "coin": coin,
        "market_type": mtype,
        "strike": strike_val,
        "result": result,
        "close_time": close_time,
        "ticks": [tick],
        "source": "settled_api",
        "settlement_price": float(settlement_price) if settlement_price else None,
        "last_price_dollars": float(last_price),
        "volume": float(volume or 0),
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch settled Kalshi markets")
    parser.add_argument("--series", nargs="+", default=None,
                        help="Series to fetch (default: all crypto)")
    parser.add_argument("--max-pages", type=int, default=300,
                        help="Max API pages per series (200 markets/page)")
    parser.add_argument("--output", default="data/settled_windows.pkl",
                        help="Output pickle path")
    args = parser.parse_args()

    series_list = args.series or ALL_SERIES

    # Init client from .env
    from dotenv import load_dotenv
    load_dotenv()

    key_id = os.environ.get("KALSHI_API_KEY_ID")
    key_path = os.path.expanduser(os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""))
    env = os.environ.get("KALSHI_ENV", "prod")

    if not key_id or not key_path:
        print("Error: KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH must be set in .env")
        sys.exit(1)

    client = KalshiClient(key_id=key_id, private_key_path=key_path, env=env)

    print("=" * 60)
    print("Fetching settled Kalshi crypto markets")
    print("=" * 60)

    all_windows = []
    stats = defaultdict(lambda: {"markets": 0, "windows": 0, "yes": 0, "no": 0})

    for series in series_list:
        if series not in SERIES_MAP:
            print(f"\n  {series}: unknown series, skipping")
            continue

        coin, mtype = SERIES_MAP[series]
        cell = f"{coin}_{mtype}"

        print(f"\n  {series} ({cell})...")
        markets = fetch_settled_markets(client, series, max_pages=args.max_pages)
        print(f"    fetched {len(markets)} settled markets")

        stats[cell]["markets"] += len(markets)

        for m in markets:
            w = market_to_window(m, coin, mtype)
            if w:
                all_windows.append(w)
                stats[cell]["windows"] += 1
                stats[cell][w["result"]] += 1

    # Save
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(all_windows, f)

    print(f"\n{'=' * 60}")
    print(f"Saved {len(all_windows)} windows to {out_path}")
    print(f"{'=' * 60}")
    for cell, s in sorted(stats.items()):
        print(f"  {cell:15s}: {s['markets']:6d} markets -> {s['windows']:5d} windows "
              f"({s['yes']} yes, {s['no']} no)")


if __name__ == "__main__":
    main()
