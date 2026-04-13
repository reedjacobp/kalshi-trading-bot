"""
One-off: fetch a live Kalshi orderbook and print its raw shape so we can
verify whether bids are returned ascending (worst→best, best is [-1]) or
descending (best→worst, best is [0]).

Usage:
    python dump_orderbook.py                  # uses a default active ticker
    python dump_orderbook.py KXBTCD-26APR1310
"""

import json
import os
import sys

from dotenv import load_dotenv
load_dotenv()

from kalshi_client import KalshiClient


def main():
    client = KalshiClient(
        key_id=os.environ["KALSHI_API_KEY_ID"],
        private_key_path=os.environ["KALSHI_PRIVATE_KEY_PATH"],
        env=os.environ.get("KALSHI_ENV", "demo"),
    )

    if len(sys.argv) > 1:
        ticker = sys.argv[1]
    else:
        # Pick any currently-open market — use /markets with small limit
        resp = client._public_get("/markets", params={"status": "open", "limit": 20})
        markets = resp.get("markets", [])
        if not markets:
            print("No open markets found.")
            return
        # Prefer one with visible depth
        ticker = markets[0]["ticker"]
        print(f"No ticker given — using first open market: {ticker}")

    print(f"\nFetching orderbook for {ticker}...\n")
    book = client.get_orderbook(ticker, depth=10)

    print("=== RAW RESPONSE ===")
    print(json.dumps(book, indent=2))

    print("\n=== INTERPRETATION ===")
    ob = book.get("orderbook", {})
    yes_bids = ob.get("yes", [])
    no_bids = ob.get("no", [])

    print(f"\nyes bids ({len(yes_bids)} levels):")
    for i, level in enumerate(yes_bids):
        print(f"  [{i}] price={level[0]}¢ qty={level[1]}")

    print(f"\nno bids ({len(no_bids)} levels):")
    for i, level in enumerate(no_bids):
        print(f"  [{i}] price={level[0]}¢ qty={level[1]}")

    if yes_bids:
        first_price = yes_bids[0][0]
        last_price = yes_bids[-1][0]
        if first_price < last_price:
            order = "ASCENDING (worst→best). Best yes bid is [-1]."
        elif first_price > last_price:
            order = "DESCENDING (best→worst). Best yes bid is [0]."
        else:
            order = "Single level or tied — can't determine."
        print(f"\nyes_bids ordering: {order}")

    if no_bids:
        first_price = no_bids[0][0]
        last_price = no_bids[-1][0]
        if first_price < last_price:
            order = "ASCENDING (worst→best). Best no bid is [-1]."
        elif first_price > last_price:
            order = "DESCENDING (best→worst). Best no bid is [0]."
        else:
            order = "Single level or tied — can't determine."
        print(f"no_bids ordering: {order}")

    # Also show orderbook_fp if present (newer schema)
    if "orderbook_fp" in book:
        print("\n=== orderbook_fp (newer schema) ===")
        print(json.dumps(book["orderbook_fp"], indent=2))


if __name__ == "__main__":
    main()
