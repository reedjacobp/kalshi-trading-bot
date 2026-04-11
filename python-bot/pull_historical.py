#!/usr/bin/env python3
"""
Pull Historical Market & Trade Data from Kalshi API

Fetches settled markets and their trade tapes for all crypto series,
then saves as parquet files compatible with the existing backtest dataset.

Usage:
    python pull_historical.py
    python pull_historical.py --since 2025-12-01 --until 2026-04-01
    python pull_historical.py --series KXBTCD --series KXBTC15M
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from kalshi_client import KalshiClient

# All crypto series we trade
ALL_SERIES = ["KXBTCD", "KXETHD", "KXSOLD", "KXBTC15M", "KXETH15M", "KXSOL15M"]

# Rate limit: bot consumes most of the budget; go slow to share
REQUEST_DELAY = 1.0


def init_client() -> KalshiClient:
    """Initialize Kalshi client from environment."""
    from dotenv import load_dotenv
    load_dotenv()

    key_id = os.getenv("KALSHI_API_KEY_ID")
    key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "~/.key/kalshi/key.pem")
    env = os.getenv("KALSHI_ENV", "prod")

    if not key_id:
        log("ERROR: KALSHI_API_KEY_ID not set in .env")
        sys.exit(1)

    return KalshiClient(key_id=key_id, private_key_path=key_path, env=env)


def _auth_get_markets(client: KalshiClient, **params) -> dict:
    """Use authenticated endpoint for markets (separate rate limit from public)."""
    query_params = {k: v for k, v in params.items() if v is not None}
    return client._request("GET", "/markets", params=query_params)


def fetch_settled_markets(client: KalshiClient, series: str,
                          min_ts: int, max_ts: int) -> list[dict]:
    """Fetch all settled markets for a series within a time range."""
    markets = []
    cursor = None
    page = 0
    consecutive_errors = 0

    while True:
        try:
            resp = _auth_get_markets(
                client,
                series_ticker=series,
                status="settled",
                limit=200,
                min_close_ts=min_ts,
                max_close_ts=max_ts,
                cursor=cursor,
            )
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            backoff = min(60, 2 ** consecutive_errors)
            log(f"    {series}: Error ({e}), backing off {backoff}s...")
            time.sleep(backoff)
            if consecutive_errors > 10:
                log(f"    {series}: Too many errors, stopping at {len(markets)} markets")
                break
            continue

        batch = resp.get("markets", [])
        if not batch:
            break

        markets.extend(batch)
        page += 1

        if page % 10 == 0:
            log(f"    {series}: {len(markets)} markets ({page} pages)...")

        cursor = resp.get("cursor")
        if not cursor:
            break

        time.sleep(REQUEST_DELAY)

    return markets


def fetch_trades_for_markets(client: KalshiClient, tickers: list[str],
                             batch_size: int = 50) -> dict[str, list[dict]]:
    """Fetch trade tapes for a list of market tickers.

    Returns dict of ticker -> list of trade dicts.
    Uses the bulk trades endpoint with ticker filter for efficiency.
    """
    trades_by_ticker = {}
    total = len(tickers)

    consecutive_errors = 0
    for i, ticker in enumerate(tickers):
        try:
            resp = client.get_trades(ticker=ticker, limit=100)
            ticker_trades = resp.get("trades", [])
            if ticker_trades:
                trades_by_ticker[ticker] = ticker_trades
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors > 5:
                backoff = min(30, 2 ** (consecutive_errors - 5))
                time.sleep(backoff)
            if consecutive_errors <= 3:
                pass  # silent
            else:
                log(f"    WARN: {consecutive_errors} errors in a row ({e}), backing off...")

        if (i + 1) % 500 == 0:
            log(f"    Trades: {i + 1}/{total} tickers processed "
                  f"({len(trades_by_ticker)} with data)...")

        time.sleep(REQUEST_DELAY)

    return trades_by_ticker


def markets_to_df(markets: list[dict], series: str) -> pd.DataFrame:
    """Convert API market responses to a DataFrame matching the existing parquet schema."""
    rows = []
    for m in markets:
        # Skip markets without a result
        result = m.get("result", "")
        if result not in ("yes", "no"):
            continue

        rows.append({
            "ticker": m.get("ticker", ""),
            "event_ticker": m.get("event_ticker", ""),
            "market_type": m.get("market_type", "binary"),
            "title": m.get("title", ""),
            "yes_sub_title": m.get("yes_sub_title", ""),
            "no_sub_title": m.get("no_sub_title", ""),
            "status": m.get("status", "finalized"),
            "yes_bid": m.get("yes_bid", 0),
            "yes_ask": m.get("yes_ask", 0),
            "no_bid": m.get("no_bid", 0),
            "no_ask": m.get("no_ask", 0),
            "last_price": int(float(m.get("last_price_dollars", 0) or m.get("last_price", 0) or 0) * 100),
            "expiration_value": float(m.get("expiration_value", 0) or 0),
            "floor_strike": float(m.get("floor_strike", 0) or 0),
            "volume": int(float(m.get("volume_fp", 0) or m.get("volume", 0) or 0)),
            "volume_24h": int(float(m.get("volume_24h_fp", 0) or m.get("volume_24h", 0) or 0)),
            "open_interest": int(float(m.get("open_interest_fp", 0) or m.get("open_interest", 0) or 0)),
            "result": result,
            "created_time": pd.Timestamp(m.get("created_time"), tz="UTC") if m.get("created_time") else pd.NaT,
            "open_time": pd.Timestamp(m.get("open_time"), tz="UTC") if m.get("open_time") else pd.NaT,
            "close_time": pd.Timestamp(m.get("close_time"), tz="UTC") if m.get("close_time") else pd.NaT,
            "_fetched_at": pd.Timestamp.now(),
            "asset": series.rstrip("D").rstrip("15M"),  # KXBTCD -> KXBTC, KXBTC15M -> KXBTC
        })

    df = pd.DataFrame(rows)
    # Match existing dtypes
    for col in ["yes_bid", "yes_ask", "no_bid", "no_ask", "last_price",
                "volume", "volume_24h", "open_interest"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")
    return df


def trades_to_df(trades_by_ticker: dict[str, list[dict]], asset: str) -> pd.DataFrame:
    """Convert API trade responses to a DataFrame matching the existing trades parquet schema."""
    rows = []
    for ticker, trades in trades_by_ticker.items():
        for t in trades:
            rows.append({
                "trade_id": t.get("trade_id", ""),
                "ticker": ticker,
                "count": t.get("count", 0),
                "yes_price": t.get("yes_price", 0),
                "no_price": t.get("no_price", 0),
                "taker_side": t.get("taker_side", ""),
                "created_time": pd.Timestamp(t.get("created_time"), tz="UTC") if t.get("created_time") else pd.NaT,
                "_fetched_at": pd.Timestamp.now(),
                "asset": asset,
            })

    df = pd.DataFrame(rows)
    for col in ["count", "yes_price", "no_price"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")
    return df


def log(msg=""):
    """Flush-safe print for background execution."""
    print(msg, flush=True)


def main():
    parser = argparse.ArgumentParser(description="Pull historical Kalshi data")
    parser.add_argument("--since", default="2025-12-01",
                        help="Start date (YYYY-MM-DD), default: 2025-12-01")
    parser.add_argument("--until", default="2026-04-07",
                        help="End date (YYYY-MM-DD), default: 2026-04-07")
    parser.add_argument("--series", action="append", default=None,
                        help="Series to fetch (can specify multiple). Default: all crypto series")
    parser.add_argument("--skip-trades", action="store_true",
                        help="Skip fetching individual trade data (faster, markets only)")
    parser.add_argument("--trades-for", action="append", default=None,
                        help="Only fetch trades for these series (e.g. --trades-for KXBTCD)")
    parser.add_argument("--out-dir", default=os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis"),
                        help="Output directory for parquet files")
    args = parser.parse_args()

    series_list = args.series or ALL_SERIES
    trades_series = args.trades_for or ([] if args.skip_trades else ["KXBTCD"])

    since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    until_dt = datetime.strptime(args.until, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    min_ts = int(since_dt.timestamp())
    max_ts = int(until_dt.timestamp())

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log(f"Pulling Kalshi data: {args.since} to {args.until}")
    log(f"Series: {series_list}")
    log(f"Trade data for: {trades_series or 'none (--skip-trades)'}")
    log(f"Output: {out_dir}")
    log()

    client = init_client()

    # ── Fetch Markets ──────────────────────────────────────────────────
    all_market_dfs = []
    for series in series_list:
        log(f"Fetching {series} markets...")
        t0 = time.time()
        markets = fetch_settled_markets(client, series, min_ts, max_ts)
        df = markets_to_df(markets, series)
        elapsed = time.time() - t0
        log(f"  {series}: {len(df)} settled markets ({elapsed:.1f}s)")
        if len(df) > 0:
            all_market_dfs.append(df)

    if not all_market_dfs:
        log("No markets found!")
        return

    markets_df = pd.concat(all_market_dfs, ignore_index=True)
    markets_file = out_dir / "crypto_markets_extended.parquet"
    markets_df.to_parquet(markets_file, index=False)
    log(f"\nSaved {len(markets_df)} markets to {markets_file}")

    # Print summary by series
    log("\nMarket counts by series:")
    for series in series_list:
        prefix = series
        count = len(markets_df[markets_df["ticker"].str.startswith(prefix)])
        if count > 0:
            first = markets_df[markets_df["ticker"].str.startswith(prefix)]["close_time"].min()
            last = markets_df[markets_df["ticker"].str.startswith(prefix)]["close_time"].max()
            log(f"  {series:12s}: {count:>7,} markets  ({str(first)[:10]} to {str(last)[:10]})")

    # ── Fetch Trades ───────────────────────────────────────────────────
    if trades_series:
        all_trade_dfs = []
        for series in trades_series:
            tickers = markets_df[markets_df["ticker"].str.startswith(series)]["ticker"].tolist()
            if not tickers:
                log(f"\nNo {series} markets to fetch trades for")
                continue

            log(f"\nFetching trades for {len(tickers)} {series} markets...")
            t0 = time.time()
            asset = series.rstrip("D").rstrip("15M")
            trades_map = fetch_trades_for_markets(client, tickers)
            df = trades_to_df(trades_map, asset)
            elapsed = time.time() - t0
            log(f"  {series}: {len(df)} trades across {len(trades_map)} markets ({elapsed:.1f}s)")
            if len(df) > 0:
                all_trade_dfs.append(df)

        if all_trade_dfs:
            trades_df = pd.concat(all_trade_dfs, ignore_index=True)
            trades_file = out_dir / "crypto_trades_extended.parquet"
            trades_df.to_parquet(trades_file, index=False)
            log(f"\nSaved {len(trades_df)} trades to {trades_file}")

    log("\nDone! Use these files with the walk-forward backtest:")
    log(f"  python backtest_walkforward.py --data-dir {out_dir} --extended")


if __name__ == "__main__":
    main()
