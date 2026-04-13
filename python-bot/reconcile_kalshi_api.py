#!/usr/bin/env python3
"""
Reconcile live_trades.csv with Kalshi via the API.

Uses /portfolio/fills for entry data (price, maker/taker, fees) and
/portfolio/settlements for settlement outcomes. Much more accurate than
the CSV exports which lag by days or weeks.

For every trade in our CSV, we:
  1. Look up the fills for that order_id
  2. Compute actual weighted-avg fill price and fees
  3. Look up the settlement for that ticker
  4. Update profit_usd, stake_usd, price_cents, contracts

Usage:
    python reconcile_kalshi_api.py
"""

import csv
import os
import shutil
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(".env")

from kalshi_client import KalshiClient


OUR_CSV = "data/live_trades.csv"
COLUMNS = [
    "time", "run_id", "strategy", "ticker", "side", "price_cents",
    "contracts", "stake_usd", "order_id", "outcome", "payout_usd",
    "profit_usd", "reason", "confidence", "order_type", "fees_usd",
]


def fetch_all_fills(client: KalshiClient) -> dict:
    """Fetch all fills via pagination, grouped by order_id."""
    by_order = defaultdict(list)
    cursor = None
    total = 0
    while True:
        params = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = client._request("GET", "/portfolio/fills", params=params)
        fills = resp.get("fills", [])
        for f in fills:
            by_order[f["order_id"]].append(f)
            total += 1
        cursor = resp.get("cursor")
        if not cursor or not fills:
            break
    print(f"  Fetched {total} fills across {len(by_order)} orders")
    return by_order


def fetch_all_settlements(client: KalshiClient) -> dict:
    """Fetch all settlements via pagination, keyed by ticker."""
    by_ticker = {}
    cursor = None
    total = 0
    while True:
        params = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = client._request("GET", "/portfolio/settlements", params=params)
        settlements = resp.get("settlements", [])
        for s in settlements:
            by_ticker[s["ticker"]] = s
            total += 1
        cursor = resp.get("cursor")
        if not cursor or not settlements:
            break
    print(f"  Fetched {total} settlements")
    return by_ticker


def aggregate_fills(fills: list) -> dict:
    """Aggregate multiple fills for one order into totals."""
    total_qty = 0
    total_cost = 0.0
    total_fees = 0.0
    any_taker = False
    side = None
    for f in fills:
        qty = int(float(f.get("count_fp", 0) or 0))
        side = f.get("side", side)
        if side == "yes":
            price = float(f.get("yes_price_dollars", 0) or 0) * 100
        else:
            price = float(f.get("no_price_dollars", 0) or 0) * 100
        total_qty += qty
        total_cost += qty * price
        total_fees += float(f.get("fee_cost", 0) or 0)
        if f.get("is_taker", True):
            any_taker = True
    avg_price = round(total_cost / total_qty) if total_qty > 0 else 0
    return {
        "contracts": total_qty,
        "avg_price_cents": avg_price,
        "fees_usd": round(total_fees, 4),
        "order_type": "taker" if any_taker else "maker",
        "side": side,
    }


def reconcile(client: KalshiClient, verbose: bool = True, backup: bool = True) -> dict:
    """
    Reconcile live_trades.csv with Kalshi API data.

    Returns a stats dict with counts of changes made.
    Can be called from the bot to keep state in sync with Kalshi.
    """
    def log(msg):
        if verbose:
            print(msg)

    log("\n[1/4] Fetching Kalshi data...")
    fills_by_order = fetch_all_fills(client) if verbose else _silent_fetch_fills(client)
    settlements_by_ticker = fetch_all_settlements(client) if verbose else _silent_fetch_settlements(client)

    log("\n[2/4] Loading our trades...")
    with open(OUR_CSV, newline="") as f:
        our_rows = list(csv.DictReader(f))
    log(f"  {len(our_rows)} rows")

    # Group our rows by order_id
    entries_by_oid = {}
    settlements_by_oid = {}
    for i, row in enumerate(our_rows):
        oid = row.get("order_id", "")
        if not oid:
            continue
        if row.get("reason", "").startswith("SETTLED:"):
            settlements_by_oid[oid] = i
        else:
            entries_by_oid[oid] = i

    all_oids = set(entries_by_oid.keys()) | set(settlements_by_oid.keys())
    log(f"  {len(all_oids)} unique orders in our CSV")

    log("\n[3/4] Reconciling...")
    stats = {
        "matched": 0, "price_fixed": 0, "contracts_fixed": 0,
        "pnl_fixed": 0, "unmatched": 0, "no_settlement": 0,
        "missing_added": 0,
    }

    for oid in all_oids:
        if oid not in fills_by_order:
            stats["unmatched"] += 1
            continue

        agg = aggregate_fills(fills_by_order[oid])
        if agg["contracts"] == 0:
            continue

        stats["matched"] += 1

        ref_idx = entries_by_oid.get(oid) or settlements_by_oid.get(oid)
        ref_row = our_rows[ref_idx]
        ticker = ref_row["ticker"]

        kalshi_side = agg["side"]
        our_price = agg["avg_price_cents"]
        stake = agg["contracts"] * our_price / 100.0
        settlement = settlements_by_ticker.get(ticker)

        if oid in entries_by_oid:
            row = our_rows[entries_by_oid[oid]]
            old_contracts = int(row["contracts"]) if row["contracts"] else 0
            old_price = int(row["price_cents"]) if row["price_cents"] else 0
            if old_contracts != agg["contracts"]:
                stats["contracts_fixed"] += 1
            if old_price != our_price:
                stats["price_fixed"] += 1
            row["contracts"] = str(agg["contracts"])
            row["price_cents"] = str(our_price)
            row["stake_usd"] = f"{stake:.2f}"
            row["side"] = kalshi_side
            row["order_type"] = agg["order_type"]
            row["fees_usd"] = f"{agg['fees_usd']:.4f}"

        if oid in settlements_by_oid:
            row = our_rows[settlements_by_oid[oid]]
            row["contracts"] = str(agg["contracts"])
            row["price_cents"] = str(our_price)
            row["stake_usd"] = f"{stake:.2f}"
            row["side"] = kalshi_side
            row["order_type"] = agg["order_type"]
            row["fees_usd"] = f"{agg['fees_usd']:.4f}"

            if settlement:
                market_result = settlement.get("market_result", "")
                if kalshi_side == market_result:
                    outcome = "win"
                    payout = agg["contracts"] * 1.00
                    profit = payout - stake
                else:
                    outcome = "loss"
                    payout = 0.0
                    profit = -stake

                old_profit = float(row["profit_usd"]) if row["profit_usd"] else 0.0
                if abs(old_profit - profit) > 0.005:
                    stats["pnl_fixed"] += 1

                row["outcome"] = outcome
                row["payout_usd"] = f"{payout:.2f}"
                row["profit_usd"] = f"{profit:.2f}"
                row["reason"] = f"SETTLED:{outcome}"
            else:
                stats["no_settlement"] += 1

    # Also promote any entry-only rows to settled if settlement now exists
    for oid, entry_idx in entries_by_oid.items():
        if oid in settlements_by_oid:
            continue  # Already has a settlement row
        if oid not in fills_by_order:
            continue
        entry_row = our_rows[entry_idx]
        ticker = entry_row["ticker"]
        settlement = settlements_by_ticker.get(ticker)
        if not settlement:
            continue
        agg = aggregate_fills(fills_by_order[oid])
        if agg["contracts"] == 0:
            continue
        kalshi_side = agg["side"]
        our_price = agg["avg_price_cents"]
        stake = agg["contracts"] * our_price / 100.0

        market_result = settlement.get("market_result", "")
        if kalshi_side == market_result:
            outcome = "win"
            payout = agg["contracts"] * 1.00
            profit = payout - stake
        else:
            outcome = "loss"
            payout = 0.0
            profit = -stake

        # Add a settlement row
        settle_row = dict(entry_row)
        settle_row["time"] = settlement.get("settled_time", entry_row["time"])
        settle_row["contracts"] = str(agg["contracts"])
        settle_row["price_cents"] = str(our_price)
        settle_row["stake_usd"] = f"{stake:.2f}"
        settle_row["outcome"] = outcome
        settle_row["payout_usd"] = f"{payout:.2f}"
        settle_row["profit_usd"] = f"{profit:.2f}"
        settle_row["reason"] = f"SETTLED:{outcome}"
        settle_row["order_type"] = agg["order_type"]
        settle_row["fees_usd"] = f"{agg['fees_usd']:.4f}"
        our_rows.append(settle_row)
        stats["pnl_fixed"] += 1

    # Find missing orders (in Kalshi but not in our log)
    missing_oids = set(fills_by_order.keys()) - all_oids
    log(f"\n[4/4] Missing orders: {len(missing_oids)}")

    missing_rows = []
    for oid in missing_oids:
        agg = aggregate_fills(fills_by_order[oid])
        if agg["contracts"] == 0:
            continue
        ticker = fills_by_order[oid][0]["ticker"]
        our_price = agg["avg_price_cents"]
        stake = agg["contracts"] * our_price / 100.0
        settlement = settlements_by_ticker.get(ticker)
        first_fill_time = min(f["created_time"] for f in fills_by_order[oid])
        # Guess strategy: RR if price >= 95c (standard case), or if it's a crypto
        # market and the fill looks like it may have crashed from a high price
        # (e.g. filled at 1c instead of 98c due to mid-order market move)
        is_crypto = any(ticker.startswith(p) for p in [
            "KXBTC", "KXETH", "KXSOL", "KXDOGE", "KXBNB", "KXHYPE", "KXXRP"
        ])
        if our_price >= 95:
            strategy = "resolution_rider"
        elif is_crypto and our_price <= 5:
            # Likely an RR trade that got filled at a much better price after market crashed
            strategy = "resolution_rider"
        else:
            strategy = "unknown"

        entry = {
            "time": first_fill_time,
            "run_id": "kalshi_api_import",
            "strategy": strategy,
            "ticker": ticker,
            "side": agg["side"],
            "price_cents": str(our_price),
            "contracts": str(agg["contracts"]),
            "stake_usd": f"{stake:.2f}",
            "order_id": oid,
            "outcome": "",
            "payout_usd": "0.00",
            "profit_usd": "0.00",
            "reason": f"Imported from Kalshi API ({strategy})",
            "confidence": "",
            "order_type": agg["order_type"],
            "fees_usd": f"{agg['fees_usd']:.4f}",
        }
        missing_rows.append(entry)

        if settlement:
            market_result = settlement.get("market_result", "")
            if agg["side"] == market_result:
                outcome = "win"
                payout = agg["contracts"] * 1.00
                profit = payout - stake
            else:
                outcome = "loss"
                payout = 0.0
                profit = -stake
            settle = dict(entry)
            settle["time"] = settlement.get("settled_time", first_fill_time)
            settle["outcome"] = outcome
            settle["payout_usd"] = f"{payout:.2f}"
            settle["profit_usd"] = f"{profit:.2f}"
            settle["reason"] = f"SETTLED:{outcome}"
            settle["order_type"] = agg["order_type"]
            settle["fees_usd"] = f"{agg['fees_usd']:.4f}"
            missing_rows.append(settle)
        stats["missing_added"] += 1

    our_rows.extend(missing_rows)
    our_rows.sort(key=lambda r: r.get("time", ""))

    if backup:
        backup_path = OUR_CSV + f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(OUR_CSV, backup_path)
        if verbose:
            print(f"\nBacked up to {backup_path}")

    with open(OUR_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(our_rows)

    stats["total_rows"] = len(our_rows)
    return stats


def _silent_fetch_fills(client: KalshiClient) -> dict:
    by_order = defaultdict(list)
    cursor = None
    while True:
        params = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = client._request("GET", "/portfolio/fills", params=params)
        fills = resp.get("fills", [])
        for f in fills:
            by_order[f["order_id"]].append(f)
        cursor = resp.get("cursor")
        if not cursor or not fills:
            break
    return by_order


def _silent_fetch_settlements(client: KalshiClient) -> dict:
    by_ticker = {}
    cursor = None
    while True:
        params = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = client._request("GET", "/portfolio/settlements", params=params)
        settlements = resp.get("settlements", [])
        for s in settlements:
            by_ticker[s["ticker"]] = s
        cursor = resp.get("cursor")
        if not cursor or not settlements:
            break
    return by_ticker


def main():
    print("=" * 60)
    print("Kalshi API Reconciliation")
    print("=" * 60)

    client = KalshiClient(
        key_id=os.getenv("KALSHI_API_KEY_ID"),
        private_key_path=os.path.expanduser(os.getenv("KALSHI_PRIVATE_KEY_PATH")),
        env=os.getenv("KALSHI_ENV", "prod"),
    )

    stats = reconcile(client, verbose=True, backup=True)

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Matched orders:       {stats['matched']}")
    print(f"  Contracts corrected:  {stats['contracts_fixed']}")
    print(f"  Prices corrected:     {stats['price_fixed']}")
    print(f"  P&L corrected:        {stats['pnl_fixed']}")
    print(f"  Missing orders added: {stats['missing_added']}")
    print(f"  No settlement (open): {stats['no_settlement']}")
    print(f"  Total rows now:       {stats['total_rows']}")


if __name__ == "__main__":
    main()
