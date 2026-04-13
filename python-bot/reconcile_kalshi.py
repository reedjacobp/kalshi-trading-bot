#!/usr/bin/env python3
"""
Reconcile live_trades.csv with Kalshi's gold-standard transaction export.

Kalshi normalizes all prices to yes-side, while our bot logs the actual
side's price (e.g. NO@98c). This script handles the conversion.

Usage:
    python reconcile_kalshi.py
"""

import csv
import math
import os
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


DATA_DIR = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
KALSHI_CSV = os.path.join(DATA_DIR, "from_kalshi", "Kalshi-Transactions-2026.csv")
OUR_CSV = "data/live_trades.csv"
COLUMNS = [
    "time", "run_id", "strategy", "ticker", "side", "price_cents",
    "contracts", "stake_usd", "order_id", "outcome", "payout_usd",
    "profit_usd", "reason", "confidence",
]


def parse_ts(ts_str: str) -> datetime:
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_kalshi(path: str) -> list[dict]:
    """Load Kalshi trades, converting prices to our bot's convention."""
    trades = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["type"] != "trade":
                continue
            trades.append(row)
    return trades


def aggregate_kalshi(trades: list[dict]) -> dict:
    """
    Aggregate Kalshi fills into orders grouped by (ticker, side, time_bucket).

    Returns dict[ticker] -> list of order dicts.
    Each order has: side, contracts, entry_price_yes, pnl_cents, open_ts, close_ts
    """
    # Group by ticker
    by_ticker = defaultdict(list)
    for t in trades:
        by_ticker[t["market_ticker"]].append(t)

    result = {}
    for ticker, fills in by_ticker.items():
        fills.sort(key=lambda f: f["open_timestamp"])

        # Sub-group by time proximity (60s window)
        orders = []
        current = [fills[0]]
        for fill in fills[1:]:
            prev_ts = parse_ts(current[-1]["open_timestamp"])
            this_ts = parse_ts(fill["open_timestamp"])
            if abs((this_ts - prev_ts).total_seconds()) <= 120:
                current.append(fill)
            else:
                orders.append(current)
                current = [fill]
        orders.append(current)

        order_list = []
        for order_fills in orders:
            qty = sum(int(f["quantity"]) for f in order_fills)
            pnl = sum(int(f["realized_pnl_without_fees_cents"]) for f in order_fills)
            pnl_net = sum(int(f["realized_pnl_with_fees_cents"]) for f in order_fills)
            fees = sum(int(f["open_fees_cents"]) + int(f["close_fees_cents"]) for f in order_fills)

            # Kalshi side (yes/no)
            side = order_fills[0]["side"]

            # Kalshi entry_price is always yes-side
            total_cost = sum(int(f["quantity"]) * int(f["entry_price_cents"]) for f in order_fills)
            avg_entry_yes = round(total_cost / qty) if qty > 0 else 0

            open_ts = min(parse_ts(f["open_timestamp"]) for f in order_fills)
            close_ts = max(parse_ts(f["close_timestamp"]) for f in order_fills)

            order_list.append({
                "ticker": ticker,
                "side": side,
                "contracts": qty,
                "entry_price_yes": avg_entry_yes,
                "pnl_cents": pnl,
                "pnl_net_cents": pnl_net,
                "fees_cents": fees,
                "open_ts": open_ts,
                "close_ts": close_ts,
                "matched": False,
            })

        result[ticker] = order_list

    return result


def our_price_to_yes(side: str, price_cents: int) -> int:
    """Convert our bot's price (which is the side's price) to yes-side."""
    if side == "yes":
        return price_cents
    else:
        return 100 - price_cents


def main():
    print("=" * 60)
    print("Kalshi Trade Reconciliation")
    print("=" * 60)

    # Load Kalshi
    print("\n[1/5] Loading Kalshi data...")
    kalshi_trades = load_kalshi(KALSHI_CSV)
    kalshi_orders = aggregate_kalshi(kalshi_trades)
    total_k_orders = sum(len(v) for v in kalshi_orders.values())
    print(f"  {len(kalshi_trades)} fills -> {total_k_orders} orders across {len(kalshi_orders)} tickers")

    # Load ours
    print("\n[2/5] Loading our trades...")
    with open(OUR_CSV, newline="") as f:
        our_rows = list(csv.DictReader(f))
    print(f"  {len(our_rows)} rows")

    # Index our rows by order_id
    entries_by_oid = {}  # order_id -> row index
    settlements_by_oid = {}
    for i, row in enumerate(our_rows):
        oid = row.get("order_id", "")
        if not oid:
            continue
        reason = row.get("reason", "")
        if reason.startswith("SETTLED:"):
            settlements_by_oid[oid] = i
        else:
            entries_by_oid[oid] = i

    # Build our order list
    our_orders = []
    all_oids = set(entries_by_oid.keys()) | set(settlements_by_oid.keys())
    for oid in all_oids:
        entry_idx = entries_by_oid.get(oid)
        settle_idx = settlements_by_oid.get(oid)
        ref = our_rows[entry_idx] if entry_idx is not None else our_rows[settle_idx]
        our_orders.append({
            "order_id": oid,
            "entry_idx": entry_idx,
            "settle_idx": settle_idx,
            "ticker": ref["ticker"],
            "side": ref["side"],
            "price_cents": int(ref["price_cents"]) if ref["price_cents"] else 0,
            "contracts": int(ref["contracts"]) if ref["contracts"] else 0,
            "time": parse_ts(ref["time"]),
        })

    print(f"  {len(our_orders)} unique orders")

    # Match
    print("\n[3/5] Matching...")
    stats = {"matched": 0, "pnl_updated": 0, "contracts_updated": 0,
             "price_updated": 0, "missing_added": 0, "unmatched_ours": 0}

    for our_order in our_orders:
        ticker = our_order["ticker"]
        if ticker not in kalshi_orders:
            stats["unmatched_ours"] += 1
            continue

        # Convert our price to yes-side for comparison
        our_yes_price = our_price_to_yes(our_order["side"], our_order["price_cents"])

        # Find best match by time + side + price
        best_k = None
        best_score = float("inf")
        for k_order in kalshi_orders[ticker]:
            if k_order["matched"]:
                continue
            # Must match side
            if k_order["side"] != our_order["side"]:
                continue
            time_diff = abs((our_order["time"] - k_order["open_ts"]).total_seconds())
            price_diff = abs(our_yes_price - k_order["entry_price_yes"])
            score = time_diff + price_diff * 5
            if score < best_score:
                best_score = score
                best_k = k_order

        if best_k is None or best_score > 600:
            stats["unmatched_ours"] += 1
            continue

        best_k["matched"] = True
        stats["matched"] += 1

        kalshi_pnl = best_k["pnl_cents"] / 100.0
        kalshi_contracts = best_k["contracts"]
        # Convert Kalshi yes-price back to our side's price
        if our_order["side"] == "yes":
            kalshi_our_price = best_k["entry_price_yes"]
        else:
            kalshi_our_price = 100 - best_k["entry_price_yes"]

        # Update entry row
        if our_order["entry_idx"] is not None:
            row = our_rows[our_order["entry_idx"]]
            if int(row["contracts"]) != kalshi_contracts:
                row["contracts"] = str(kalshi_contracts)
                row["stake_usd"] = f"{kalshi_contracts * kalshi_our_price / 100:.2f}"
                stats["contracts_updated"] += 1
            if int(row["price_cents"]) != kalshi_our_price:
                row["price_cents"] = str(kalshi_our_price)
                row["stake_usd"] = f"{kalshi_contracts * kalshi_our_price / 100:.2f}"
                stats["price_updated"] += 1

        # Update settlement row
        if our_order["settle_idx"] is not None:
            row = our_rows[our_order["settle_idx"]]
            old_profit = float(row["profit_usd"]) if row["profit_usd"] else 0.0

            row["contracts"] = str(kalshi_contracts)
            row["price_cents"] = str(kalshi_our_price)
            row["stake_usd"] = f"{kalshi_contracts * kalshi_our_price / 100:.2f}"

            if abs(old_profit - kalshi_pnl) > 0.005:
                row["profit_usd"] = f"{kalshi_pnl:.2f}"
                stats["pnl_updated"] += 1

            # Fix payout and outcome
            if kalshi_pnl >= 0:
                stake = kalshi_contracts * kalshi_our_price / 100.0
                row["payout_usd"] = f"{stake + kalshi_pnl:.2f}"
                row["outcome"] = "win"
                row["reason"] = "SETTLED:win"
            else:
                row["payout_usd"] = "0.00"
                row["outcome"] = "loss"
                row["reason"] = "SETTLED:loss"

    print(f"  Matched: {stats['matched']}")
    print(f"  P&L updated: {stats['pnl_updated']}")
    print(f"  Contracts updated: {stats['contracts_updated']}")
    print(f"  Price updated: {stats['price_updated']}")
    print(f"  Unmatched (ours only): {stats['unmatched_ours']}")

    # Find missing trades (on Kalshi but not in our log)
    print("\n[4/5] Finding missing Kalshi trades...")
    missing_rows = []
    for ticker, orders in kalshi_orders.items():
        for k_order in orders:
            if k_order["matched"]:
                continue

            side = k_order["side"]
            contracts = k_order["contracts"]
            pnl = k_order["pnl_cents"] / 100.0
            entry_yes = k_order["entry_price_yes"]

            # Convert to our convention
            if side == "yes":
                our_price = entry_yes
            else:
                our_price = 100 - entry_yes

            stake = contracts * our_price / 100.0
            outcome = "win" if pnl >= 0 else "loss"
            payout = stake + pnl if pnl >= 0 else 0.0

            # Guess strategy
            if our_price >= 95:
                strategy = "resolution_rider"
            else:
                strategy = "unknown"

            oid = f"kalshi_{ticker}_{side}_{k_order['open_ts'].strftime('%Y%m%dT%H%M%S')}"

            # Entry row
            missing_rows.append({
                "time": k_order["open_ts"].isoformat(),
                "run_id": "kalshi_import",
                "strategy": strategy,
                "ticker": ticker,
                "side": side,
                "price_cents": str(our_price),
                "contracts": str(contracts),
                "stake_usd": f"{stake:.2f}",
                "order_id": oid,
                "outcome": "",
                "payout_usd": "0.00",
                "profit_usd": "0.00",
                "reason": f"Imported from Kalshi ({strategy})",
                "confidence": "",
            })

            # Settlement row
            missing_rows.append({
                "time": k_order["close_ts"].isoformat(),
                "run_id": "kalshi_import",
                "strategy": strategy,
                "ticker": ticker,
                "side": side,
                "price_cents": str(our_price),
                "contracts": str(contracts),
                "stake_usd": f"{stake:.2f}",
                "order_id": oid,
                "outcome": outcome,
                "payout_usd": f"{payout:.2f}",
                "profit_usd": f"{pnl:.2f}",
                "reason": f"SETTLED:{outcome}",
                "confidence": "",
            })
            stats["missing_added"] += 1

    print(f"  Added {stats['missing_added']} missing orders ({len(missing_rows)} rows)")

    if missing_rows:
        added_pnl = sum(float(r["profit_usd"]) for r in missing_rows if r["outcome"])
        print(f"  Net P&L from added trades: ${added_pnl:+.2f}")

    our_rows.extend(missing_rows)
    our_rows.sort(key=lambda r: r.get("time", ""))

    # Write
    print("\n[5/5] Writing...")
    backup = OUR_CSV + f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(OUR_CSV, backup)
    print(f"  Backed up to {backup}")

    with open(OUR_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(our_rows)
    print(f"  Wrote {len(our_rows)} rows")

    # Verify
    print(f"\n{'=' * 60}")
    print("VERIFICATION")
    print(f"{'=' * 60}")
    settled = [r for r in our_rows if r.get("reason", "").startswith("SETTLED:")]
    our_total = sum(float(r["profit_usd"]) for r in settled)

    k_total = sum(int(r["realized_pnl_without_fees_cents"]) for r in kalshi_trades) / 100

    print(f"  Kalshi gross P&L:  ${k_total:+.2f}")
    print(f"  Our gross P&L:     ${our_total:+.2f}")
    print(f"  Gap:               ${our_total - k_total:+.2f}")


if __name__ == "__main__":
    main()
