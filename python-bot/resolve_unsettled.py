#!/usr/bin/env python3
"""
One-time script to batch-resolve unsettled CSV trades.

Looks up each unsettled trade via the Kalshi API (with rate-limit-safe
pacing) and appends settlement rows to live_trades.csv.

Usage:
    python resolve_unsettled.py          # dry run (shows what would be resolved)
    python resolve_unsettled.py --apply  # actually write settlements to CSV
"""

import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from kalshi_client import KalshiClient

CSV_PATH = Path("data/live_trades.csv")

DRY_RUN = "--apply" not in sys.argv


def get_unsettled_trades():
    with open(CSV_PATH, "r", newline="") as f:
        rows = list(csv.DictReader(f))

    settled_order_ids = set()
    for row in rows:
        if row.get("reason", "").startswith("SETTLED:"):
            settled_order_ids.add(row["order_id"])

    return [
        row for row in rows
        if not row.get("reason", "").startswith("SETTLED:")
        and row["order_id"] not in settled_order_ids
    ]


def kalshi_taker_fee(contracts, profit_cents):
    """Mirror the bot's fee calculation."""
    fee_per = min(0.07, profit_cents / 100 * 0.15)
    return round(contracts * fee_per, 2)


def main():
    client = KalshiClient(
        key_id=os.environ["KALSHI_API_KEY_ID"],
        private_key_path=os.environ["KALSHI_PRIVATE_KEY_PATH"],
        env=os.environ.get("KALSHI_ENV", "prod"),
    )

    unsettled = get_unsettled_trades()
    print(f"Found {len(unsettled)} unsettled trades")
    if not unsettled:
        return

    # De-duplicate by ticker (multiple trades on same market share a result)
    unique_tickers = list(dict.fromkeys(row["ticker"] for row in unsettled))
    print(f"Unique tickers to look up: {len(unique_tickers)}")

    if DRY_RUN:
        print("\n[DRY RUN] Pass --apply to write settlements. Preview:\n")

    # Look up each ticker with 0.5s pacing to stay under rate limits
    results = {}
    for i, ticker in enumerate(unique_tickers):
        try:
            data = client.get_market(ticker)
            m = data.get("market", data)
            status = m.get("status", "")
            result = m.get("result", "")
            if status in ("settled", "finalized") and result:
                results[ticker] = result
                print(f"  [{i+1}/{len(unique_tickers)}] {ticker} -> {result}")
            else:
                print(f"  [{i+1}/{len(unique_tickers)}] {ticker} -> {status} (no result yet)")
        except Exception as e:
            print(f"  [{i+1}/{len(unique_tickers)}] {ticker} -> ERROR: {e}")
        time.sleep(0.5)  # ~2 req/s, well under rate limit

    resolved = 0
    skipped = 0
    for row in unsettled:
        ticker = row["ticker"]
        result = results.get(ticker)
        if not result:
            skipped += 1
            continue

        side = row["side"]
        contracts = int(row["contracts"])
        stake_usd = float(row["stake_usd"])
        price_cents = int(row["price_cents"])

        if side == result:
            outcome = "win"
            payout = contracts * 1.00
            profit = payout - stake_usd
            settle_fee = kalshi_taker_fee(contracts, 100 - price_cents)
        else:
            outcome = "loss"
            payout = 0.0
            profit = -stake_usd
            settle_fee = 0.0

        if not DRY_RUN:
            with open(CSV_PATH, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    row["time"],
                    row["run_id"],
                    row["strategy"],
                    ticker,
                    side,
                    price_cents,
                    contracts,
                    f"{stake_usd:.2f}",
                    row["order_id"],
                    outcome,
                    f"{payout:.2f}",
                    f"{profit:.2f}",
                    f"SETTLED:{outcome}",
                    "",
                ])
        resolved += 1

    print(f"\n{'Would resolve' if DRY_RUN else 'Resolved'}: {resolved}")
    print(f"Skipped (not yet settled): {skipped}")
    if DRY_RUN and resolved > 0:
        print("\nRun with --apply to write these settlements.")


if __name__ == "__main__":
    main()
