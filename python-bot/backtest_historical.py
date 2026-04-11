#!/usr/bin/env python3
"""
Historical Backtest against Kalshi Prediction Market Dataset

Uses the Jon-Becker prediction-market-analysis dataset (KXBTCD daily
crypto markets) to validate trading strategies with real settlement data.

Strategies tested:
1. Favorite-Longshot Bias — buy the favorite when priced 70c+
2. Consensus (adapted) — combine trade flow imbalance, momentum,
   and previous result for multi-signal agreement
3. Mean Reversion — fade sharp price spikes

Data: ~4.5M trades across ~390K settled KXBTCD markets (Oct 2024 - Nov 2025)

Usage:
    python backtest_historical.py
    python backtest_historical.py --data-dir /mnt/d/datasets/prediction-market-analysis
    python backtest_historical.py --strategy favorite_bias
"""

import argparse
import json
import math
import os
import sys
import time as _time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb required. Install with: pip install duckdb")
    sys.exit(1)

from performance import PerformanceTracker, PerformanceMetrics
from risk_manager import kalshi_taker_fee


# ─── Data Types ──────────────────────────────────────────────────────────────

@dataclass
class MarketSnapshot:
    """A settled market with trade-derived signals."""
    ticker: str
    result: str              # "yes" or "no"
    close_time: str
    volume: int
    last_price: int          # Last trade price (yes side, cents)
    # Signals derived from trade tape
    trade_flow_imbalance: float  # +1 = all yes takers, -1 = all no takers
    price_momentum: float        # price change over last N trades (cents)
    entry_price: int             # price at our simulated entry point
    n_trades: int                # trades in our analysis window


@dataclass
class SimulatedTrade:
    """A simulated trade from the backtest."""
    ticker: str
    strategy: str
    side: str           # "yes" or "no"
    entry_price: int    # cents
    contracts: int
    stake_usd: float
    result: str         # market result
    outcome: str        # "win" or "loss"
    profit_usd: float
    profit_after_fees: float


@dataclass
class BacktestConfig:
    """Configuration for the backtest."""
    # Entry criteria
    min_volume: int = 100           # Skip illiquid markets
    # Favorite bias params
    fav_min_price: int = 70         # Minimum favorite price
    fav_max_entry: int = 85         # Max entry price for favorites
    # Consensus params
    consensus_min_agreement: int = 2  # 2 of 3 signals must agree
    consensus_min_price: int = 35
    consensus_max_price: int = 60
    consensus_min_edge: float = 0.10
    # Mean reversion params
    mr_spike_threshold: float = 10   # cents of recent move to trigger
    mr_max_entry: int = 55
    # Position sizing
    stake_usd: float = 5.00
    kelly_fraction: float = 0.25
    # Fees
    include_fees: bool = True


# ─── Signal Computation ──────────────────────────────────────────────────────

def compute_signals(trades_df, entry_window_pct: float = 0.5):
    """
    Compute trading signals from a market's trade tape.

    Args:
        trades_df: DataFrame with columns [yes_price, taker_side, count, created_time]
        entry_window_pct: What fraction of the trading period to use as the
            "entry window" (0.5 = we enter at the midpoint of trading activity)

    Returns:
        dict with signal values, or None if insufficient data
    """
    if len(trades_df) < 5:
        return None

    # Split trades into "before entry" and "after entry"
    n = len(trades_df)
    entry_idx = max(3, int(n * entry_window_pct))

    pre_entry = trades_df.iloc[:entry_idx]
    at_entry = trades_df.iloc[entry_idx - 1]  # Last trade before our entry

    # Entry price
    entry_price = int(at_entry["yes_price"])

    # Trade flow imbalance: weighted by volume
    yes_vol = sum(
        r["count"] for _, r in pre_entry.iterrows()
        if r["taker_side"] == "yes"
    )
    no_vol = sum(
        r["count"] for _, r in pre_entry.iterrows()
        if r["taker_side"] == "no"
    )
    total_vol = yes_vol + no_vol
    if total_vol == 0:
        return None
    flow_imbalance = (yes_vol - no_vol) / total_vol

    # Price momentum: price change over the pre-entry window
    first_price = int(pre_entry.iloc[0]["yes_price"])
    momentum = entry_price - first_price  # in cents

    # Recent momentum (last 20% of pre-entry trades)
    recent_start = max(0, len(pre_entry) - max(3, len(pre_entry) // 5))
    recent_prices = [int(r["yes_price"]) for _, r in pre_entry.iloc[recent_start:].iterrows()]
    recent_momentum = recent_prices[-1] - recent_prices[0] if len(recent_prices) >= 2 else 0

    return {
        "entry_price": entry_price,
        "flow_imbalance": flow_imbalance,
        "momentum": momentum,
        "recent_momentum": recent_momentum,
        "n_trades": len(pre_entry),
        "volume": total_vol,
    }


# ─── Strategy Evaluators ────────────────────────────────────────────────────

def evaluate_favorite_bias(signals: dict, config: BacktestConfig, prev_result: str = None):
    """
    Favorite-Longshot Bias: buy the favorite when priced above threshold.

    Returns (side, confidence, reason) or None.
    """
    price = signals["entry_price"]

    # YES is the favorite
    if price >= config.fav_min_price and price <= config.fav_max_entry:
        confidence = min(0.92, price / 100 + 0.05)
        return ("yes", confidence, f"Fav YES@{price}c")

    # NO is the favorite
    no_price = 100 - price
    if no_price >= config.fav_min_price and no_price <= config.fav_max_entry:
        confidence = min(0.92, no_price / 100 + 0.05)
        return ("no", confidence, f"Fav NO@{no_price}c")

    return None


def evaluate_consensus(signals: dict, config: BacktestConfig, prev_result: str = None):
    """
    Adapted Consensus: 3 signals must agree.
    1. Trade flow imbalance (>15% = directional signal)
    2. Price momentum (direction of recent price movement)
    3. Previous market result (trend-following)

    Returns (side, confidence, reason) or None.
    """
    price = signals["entry_price"]
    flow = signals["flow_imbalance"]
    mom = signals["momentum"]

    # Price filter
    our_price_yes = price
    our_price_no = 100 - price
    if our_price_yes > config.consensus_max_price and our_price_no > config.consensus_max_price:
        return None
    if our_price_yes < config.consensus_min_price and our_price_no < config.consensus_min_price:
        return None

    # Signal 1: Trade flow imbalance
    sig_flow = None
    if flow > 0.15:
        sig_flow = "yes"
    elif flow < -0.15:
        sig_flow = "no"

    # Signal 2: Price momentum
    sig_mom = None
    if mom > 2:  # >2c move
        sig_mom = "yes"
    elif mom < -2:
        sig_mom = "no"

    # Signal 3: Previous market result
    sig_prev = prev_result  # "yes", "no", or None

    signals_list = [sig_flow, sig_mom, sig_prev]
    yes_votes = sum(1 for s in signals_list if s == "yes")
    no_votes = sum(1 for s in signals_list if s == "no")
    valid_votes = sum(1 for s in signals_list if s is not None)

    if yes_votes >= config.consensus_min_agreement:
        direction = "yes"
        our_price = our_price_yes
    elif no_votes >= config.consensus_min_agreement:
        direction = "no"
        our_price = our_price_no
    else:
        return None

    if our_price > config.consensus_max_price or our_price < config.consensus_min_price:
        return None

    agreement = max(yes_votes, no_votes) / max(valid_votes, 1)
    confidence = agreement * 0.7 + (1.0 - our_price / 100.0) * 0.3

    # Edge check
    breakeven = our_price / 100.0
    if confidence - breakeven < config.consensus_min_edge:
        return None

    reason = f"Cons {direction.upper()} ({max(yes_votes,no_votes)}/{valid_votes}): flow={flow:+.2f} mom={mom:+d} prev={sig_prev or '-'}"
    return (direction, confidence, reason)


def evaluate_mean_reversion(signals: dict, config: BacktestConfig, prev_result: str = None):
    """
    Mean Reversion: fade sharp recent price spikes.

    Returns (side, confidence, reason) or None.
    """
    price = signals["entry_price"]
    recent_mom = signals["recent_momentum"]
    overall_mom = signals["momentum"]

    # Spike up: recent momentum strongly positive but overall is moderate
    if recent_mom > config.mr_spike_threshold:
        if abs(overall_mom) < recent_mom * 0.7:  # Divergence
            no_price = 100 - price
            if no_price <= config.mr_max_entry:
                confidence = min(0.70, 0.5 + abs(recent_mom) / 50)
                return ("no", confidence, f"MR spike up: recent={recent_mom:+d}c overall={overall_mom:+d}c")

    # Spike down
    if recent_mom < -config.mr_spike_threshold:
        if abs(overall_mom) < abs(recent_mom) * 0.7:
            if price <= config.mr_max_entry:
                confidence = min(0.70, 0.5 + abs(recent_mom) / 50)
                return ("yes", confidence, f"MR spike down: recent={recent_mom:+d}c overall={overall_mom:+d}c")

    return None


# ─── Backtest Engine ─────────────────────────────────────────────────────────

class HistoricalBacktest:
    """
    Runs strategies against historical Kalshi settlement data.
    """

    def __init__(self, data_dir: str, config: BacktestConfig = None):
        self.data_dir = Path(data_dir)
        self.config = config or BacktestConfig()
        self.con = duckdb.connect()

        # Verify data exists
        markets_file = self.data_dir / "crypto_markets_filtered.parquet"
        trades_file = self.data_dir / "crypto_trades_filtered.parquet"
        if not markets_file.exists():
            # Try the unfiltered data
            markets_file = self.data_dir / "crypto_markets.parquet"
        if not markets_file.exists():
            raise FileNotFoundError(f"No market data found in {self.data_dir}")
        if not trades_file.exists():
            raise FileNotFoundError(f"No trade data found in {self.data_dir}")

        self.markets_file = str(markets_file)
        self.trades_file = str(trades_file)

    def run(self, strategies: list[str] = None, max_markets: int = None) -> dict:
        """
        Run the backtest.

        Args:
            strategies: List of strategy names to test. Default: all.
            max_markets: Limit number of markets (for quick testing).

        Returns:
            Dict with results per strategy and aggregate.
        """
        if strategies is None:
            strategies = ["favorite_bias", "consensus", "mean_reversion"]

        strategy_funcs = {
            "favorite_bias": evaluate_favorite_bias,
            "consensus": evaluate_consensus,
            "mean_reversion": evaluate_mean_reversion,
        }

        # Load settled markets with volume
        print("Loading markets...")
        markets_df = self.con.execute(f"""
            SELECT ticker, result, close_time, volume, last_price
            FROM read_parquet('{self.markets_file}')
            WHERE ticker LIKE 'KXBTCD%'
            AND status = 'finalized'
            AND result IN ('yes', 'no')
            AND volume >= {self.config.min_volume}
            AND last_price IS NOT NULL
            AND last_price > 0 AND last_price < 100
            ORDER BY close_time ASC
        """).fetchdf()

        if max_markets:
            markets_df = markets_df.tail(max_markets)

        print(f"  {len(markets_df):,} settled markets with volume >= {self.config.min_volume}")

        # Bulk-load all trades for KXBTCD and index by ticker
        print("Loading all KXBTCD trades (one-time bulk load)...")
        t_load = _time.time()
        all_kxbtcd_trades = self.con.execute(f"""
            SELECT ticker, yes_price, taker_side, count, created_time
            FROM read_parquet('{self.trades_file}')
            WHERE ticker LIKE 'KXBTCD%'
            ORDER BY ticker, created_time ASC
        """).fetchdf()
        print(f"  Loaded {len(all_kxbtcd_trades):,} trades in {_time.time() - t_load:.1f}s")

        # Index by ticker for fast lookup
        print("Indexing trades by ticker...")
        trades_by_ticker = {
            ticker: group.reset_index(drop=True)
            for ticker, group in all_kxbtcd_trades.groupby("ticker")
        }
        del all_kxbtcd_trades  # Free memory
        print(f"  Indexed {len(trades_by_ticker):,} tickers")

        # Results tracking
        all_trades: dict[str, list[SimulatedTrade]] = {s: [] for s in strategies}
        prev_result: str = None
        skipped = 0
        processed = 0
        t0 = _time.time()

        # Process markets chronologically
        for idx, market_row in markets_df.iterrows():
            ticker = market_row["ticker"]
            result = market_row["result"]

            # Look up trades from pre-loaded index
            trades_df = trades_by_ticker.get(ticker)
            if trades_df is None or len(trades_df) < 5:
                skipped += 1
                prev_result = result
                continue

            # Compute signals
            signals = compute_signals(trades_df)
            if signals is None:
                skipped += 1
                prev_result = result
                continue

            # Evaluate each strategy
            for strat_name in strategies:
                func = strategy_funcs[strat_name]
                rec = func(signals, self.config, prev_result)
                if rec is None:
                    continue

                side, confidence, reason = rec
                entry_price = signals["entry_price"] if side == "yes" else 100 - signals["entry_price"]

                # Simulate the trade
                contracts = max(1, int(self.config.stake_usd / (entry_price / 100.0)))
                stake = contracts * (entry_price / 100.0)

                is_win = (side == result)
                if is_win:
                    payout = contracts * 1.00
                    profit = payout - stake
                else:
                    profit = -stake

                # Fees
                entry_fee = kalshi_taker_fee(contracts, entry_price) if self.config.include_fees else 0
                settle_fee = kalshi_taker_fee(contracts, 100 - entry_price) if (self.config.include_fees and is_win) else 0
                profit_after_fees = profit - entry_fee - settle_fee

                all_trades[strat_name].append(SimulatedTrade(
                    ticker=ticker,
                    strategy=strat_name,
                    side=side,
                    entry_price=entry_price,
                    contracts=contracts,
                    stake_usd=round(stake, 2),
                    result=result,
                    outcome="win" if is_win else "loss",
                    profit_usd=round(profit, 2),
                    profit_after_fees=round(profit_after_fees, 2),
                ))

            prev_result = result
            processed += 1

            if processed % 1000 == 0:
                elapsed = _time.time() - t0
                rate = processed / elapsed
                remaining = (len(markets_df) - processed) / rate if rate > 0 else 0
                print(f"  Processed {processed:,}/{len(markets_df):,} "
                      f"({elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)")

        print(f"\nDone. Processed {processed:,} markets, skipped {skipped:,}")

        # Compute results
        return self._compute_results(all_trades, strategies)

    def _compute_results(self, all_trades: dict, strategies: list[str]) -> dict:
        results = {}

        for strat_name in strategies:
            trades = all_trades[strat_name]
            if not trades:
                results[strat_name] = {
                    "trades": 0,
                    "metrics": PerformanceMetrics(),
                    "price_buckets": {},
                }
                continue

            # Performance metrics
            tracker = PerformanceTracker()
            for t in trades:
                tracker.record(t.profit_after_fees)
            metrics = tracker.compute()

            # Win rate by entry price bucket
            price_buckets = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0})
            for t in trades:
                bucket = (t.entry_price // 10) * 10
                price_buckets[bucket]["n"] += 1
                if t.outcome == "win":
                    price_buckets[bucket]["wins"] += 1
                price_buckets[bucket]["pnl"] += t.profit_after_fees

            results[strat_name] = {
                "trades": len(trades),
                "metrics": metrics,
                "price_buckets": dict(price_buckets),
                "sample_trades": trades[:5],
            }

        return results


# ─── Report ──────────────────────────────────────────────────────────────────

def print_report(results: dict):
    print(f"\n{'='*70}")
    print("HISTORICAL BACKTEST REPORT (KXBTCD Daily Crypto Markets)")
    print(f"{'='*70}")

    for strat_name, data in results.items():
        m = data["metrics"]
        n = data["trades"]
        if n == 0:
            print(f"\n─── {strat_name}: NO TRADES ───")
            continue

        print(f"\n─── {strat_name} ({n:,} trades) ───")
        print(f"  Win Rate:        {m.win_rate:.1%}")
        print(f"  Total P&L:       ${m.total_pnl:+,.2f} (after fees)")
        print(f"  Sharpe Ratio:    {m.sharpe_ratio:.3f}")
        print(f"  Sortino Ratio:   {m.sortino_ratio:.3f}")
        print(f"  Profit Factor:   {m.profit_factor:.2f}")
        print(f"  Avg Win:         ${m.avg_win:+.2f}")
        print(f"  Avg Loss:        ${m.avg_loss:+.2f}")
        print(f"  Max Drawdown:    ${m.max_drawdown_usd:,.2f}")
        print(f"  Calmar Ratio:    {m.calmar_ratio:.3f}")
        print(f"  Expectancy:      ${m.expectancy:+.3f}/trade")

        # Price bucket analysis
        buckets = data.get("price_buckets", {})
        if buckets:
            print(f"\n  Entry Price | Trades |  WR%  |   P&L")
            print(f"  {'-'*45}")
            for bucket in sorted(buckets.keys()):
                b = buckets[bucket]
                wr = b["wins"] / b["n"] * 100 if b["n"] > 0 else 0
                print(f"   {bucket:>2}-{bucket+9}c   | {b['n']:>6,} | {wr:>5.1f} | ${b['pnl']:>+10,.2f}")

    print(f"\n{'='*70}")


def save_results(results: dict, output_path: str):
    """Save results to JSON."""
    serializable = {}
    for strat_name, data in results.items():
        m = data["metrics"]
        serializable[strat_name] = {
            "trades": data["trades"],
            "win_rate": round(m.win_rate, 4),
            "total_pnl": round(m.total_pnl, 2),
            "sharpe_ratio": round(m.sharpe_ratio, 4),
            "sortino_ratio": round(m.sortino_ratio, 4),
            "profit_factor": round(m.profit_factor, 4),
            "max_drawdown": round(m.max_drawdown_usd, 2),
            "calmar_ratio": round(m.calmar_ratio, 4),
            "expectancy": round(m.expectancy, 4),
            "avg_win": round(m.avg_win, 4),
            "avg_loss": round(m.avg_loss, 4),
            "price_buckets": {
                str(k): {
                    "n": v["n"],
                    "wins": v["wins"],
                    "win_rate": round(v["wins"] / v["n"], 4) if v["n"] > 0 else 0,
                    "pnl": round(v["pnl"], 2),
                }
                for k, v in data.get("price_buckets", {}).items()
            },
        }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(serializable, f, indent=2)
    print(f"\nResults saved to {output_path}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Historical backtest on Kalshi crypto data")
    parser.add_argument("--data-dir", type=str,
                        default=os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis"),
                        help="Path to prediction-market-analysis data")
    parser.add_argument("--strategy", type=str, nargs="+", default=None,
                        help="Strategies to test (favorite_bias, consensus, mean_reversion)")
    parser.add_argument("--max-markets", type=int, default=None,
                        help="Limit markets for quick testing")
    parser.add_argument("--min-volume", type=int, default=100,
                        help="Minimum market volume")
    parser.add_argument("--output", type=str, default="data/backtest_historical.json",
                        help="Output JSON path")
    args = parser.parse_args()

    config = BacktestConfig(min_volume=args.min_volume)
    bt = HistoricalBacktest(args.data_dir, config)

    results = bt.run(
        strategies=args.strategy,
        max_markets=args.max_markets,
    )

    print_report(results)
    save_results(results, args.output)


if __name__ == "__main__":
    main()
