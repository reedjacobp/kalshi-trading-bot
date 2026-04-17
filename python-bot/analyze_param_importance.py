"""
Parameter Importance Analysis for Resolution Rider

Analyzes which parameters most strongly predict win vs loss outcomes
in real tick data. This informs the optimizer about what to focus on.

Features analyzed:
  - buffer_pct (distance from strike, time-scaled)
  - momentum (smoothed price trend)
  - realized_vol (recent price chop)
  - secs_left (time to settlement)
  - entry_price (contract price in cents)
  - fav_price (favorite-side mid price)

Methods:
  1. Univariate WR curves (WR at each threshold)
  2. Point-biserial correlation with win/loss
  3. Random forest feature importance
  4. Per-feature "value of information" (WR spread between best/worst quintile)

Usage:
    python analyze_param_importance.py
"""

import json
import math
import os
import sys
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

from optimize_rr import (
    load_tick_windows, load_hf_trade_windows, classify_ticker,
    preprocess_window, load_crypto_prices, VOL_LOOKBACK,
)


def collect_entries(tick_windows, crypto_prices):
    """Extract all tradeable entries with full feature vectors."""
    entries = []
    for w in tick_windows:
        pp = preprocess_window(w, crypto_prices)
        if not pp:
            continue
        result = pp["result"]
        for e in pp["entries"]:
            sl = e["secs_left"]
            if sl < 10 or sl > 900:
                continue
            ep = e["entry_price"]
            fav = e["fav_price"]
            side = e["side"]
            buf = e.get("buffer_pct")
            mom_dict = e.get("momentum") or {}
            vol = e.get("realized_vol")
            won = 1 if (side == result) else 0

            # Use a representative momentum value (60s window, 5 periods)
            mom = mom_dict.get((60, 5))
            # Also grab a few other combos for comparison
            mom_30_3 = mom_dict.get((30, 3))
            mom_120_5 = mom_dict.get((120, 5))
            mom_300_2 = mom_dict.get((300, 2))

            # Signed buffer: positive = price on winning side of strike
            signed_buf = buf if buf is not None else None
            if signed_buf is not None and side == "no":
                signed_buf = -signed_buf  # flip so positive = favorable

            # Time-scaled buffer (what the strategy actually uses)
            scaled_buf = None
            if buf is not None:
                raw_req = abs(buf) / math.sqrt(max(1.0, sl) / 60.0)
                scaled_buf = raw_req  # how much buffer relative to requirement

            entries.append({
                "won": won,
                "secs_left": sl,
                "entry_price": ep,
                "fav_price": fav,
                "buffer_pct": abs(buf) if buf is not None else None,
                "signed_buffer": signed_buf,
                "scaled_buffer": scaled_buf,
                "momentum_60_5": mom,
                "momentum_30_3": mom_30_3,
                "momentum_120_5": mom_120_5,
                "momentum_300_2": mom_300_2,
                "realized_vol": vol,
                "side": side,
                "cell": f"{classify_ticker('KX' + w.get('coin','').upper() + ('15M' if w.get('market_type')=='15m' else 'D') + '-X')[0]}_{w.get('market_type','')}",
            })
    return entries


def univariate_wr_analysis(df, feature, n_bins=10):
    """Compute WR at each quantile bin of a feature."""
    valid = df[[feature, "won"]].dropna()
    if len(valid) < 50:
        return None
    try:
        valid["bin"] = pd.qcut(valid[feature], n_bins, duplicates="drop")
    except ValueError:
        return None
    grouped = valid.groupby("bin", observed=True)["won"].agg(["mean", "count"])
    return grouped


def compute_importance(df):
    """Compute multiple importance metrics for each feature."""
    features = ["buffer_pct", "signed_buffer", "secs_left", "entry_price",
                 "momentum_60_5", "momentum_300_2", "realized_vol"]

    results = {}
    for feat in features:
        valid = df[[feat, "won"]].dropna()
        if len(valid) < 100:
            results[feat] = {"correlation": None, "wr_spread": None,
                             "n": len(valid)}
            continue

        # Point-biserial correlation (manual — avoids scipy dependency)
        y = valid["won"].values.astype(float)
        x = valid[feat].values.astype(float)
        n = len(y)
        n1 = y.sum()
        n0 = n - n1
        if n0 == 0 or n1 == 0:
            corr, pval = 0.0, 1.0
        else:
            m1 = x[y == 1].mean()
            m0 = x[y == 0].mean()
            s = x.std()
            if s > 0:
                corr = (m1 - m0) / s * np.sqrt(n1 * n0 / n**2)
            else:
                corr = 0.0
            pval = 0.0  # skip exact p-value without scipy

        # WR spread: difference between top and bottom quintile WR
        try:
            valid["q"] = pd.qcut(valid[feat], 5, labels=False, duplicates="drop")
            q_wr = valid.groupby("q")["won"].mean()
            wr_spread = q_wr.max() - q_wr.min()
            best_q = q_wr.idxmax()
            worst_q = q_wr.idxmin()
            best_wr = q_wr.max()
            worst_wr = q_wr.min()
        except (ValueError, KeyError):
            wr_spread = 0
            best_wr = worst_wr = valid["won"].mean()

        results[feat] = {
            "correlation": abs(corr),
            "corr_sign": "+" if corr > 0 else "-",
            "pval": pval,
            "wr_spread": wr_spread,
            "best_quintile_wr": best_wr,
            "worst_quintile_wr": worst_wr,
            "n": len(valid),
        }

    return results


def random_forest_importance(df):
    """Train a simple RF and extract feature importances."""
    features = ["buffer_pct", "secs_left", "entry_price",
                 "momentum_60_5", "momentum_300_2", "realized_vol"]
    valid = df[features + ["won"]].dropna()
    if len(valid) < 200:
        return None

    try:
        from sklearn.ensemble import RandomForestClassifier
    except ImportError:
        # Fallback: use numpy-based permutation importance
        return _numpy_importance(valid, features)

    X = valid[features].values
    y = valid["won"].values

    rf = RandomForestClassifier(n_estimators=200, max_depth=6,
                                 random_state=42, n_jobs=-1)
    rf.fit(X, y)

    importances = dict(zip(features, rf.feature_importances_))
    return importances


def _numpy_importance(valid, features):
    """Cheap feature importance: WR variance explained by each feature.

    For each feature, bin into quintiles and measure how much the
    per-bin WR varies. More variance = more predictive.
    """
    importances = {}
    for feat in features:
        col = valid[feat].values
        won = valid["won"].values
        try:
            bins = pd.qcut(col, 5, labels=False, duplicates="drop")
            wr_by_bin = [won[bins == b].mean() for b in sorted(set(bins))]
            importances[feat] = np.std(wr_by_bin)
        except (ValueError, IndexError):
            importances[feat] = 0.0
    # Normalize
    total = sum(importances.values()) or 1
    return {k: v / total for k, v in importances.items()}


def main():
    print("=" * 65)
    print("Parameter Importance Analysis — Resolution Rider")
    print("=" * 65)

    # Load tick data only (real timestamps)
    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
    tick_dir = os.path.join(data_dir, "ticks")
    tick_windows = load_tick_windows(tick_dir) if Path(tick_dir).exists() else []

    # Also load HF trade data for more coverage
    hf_dir = os.getenv("HF_DIR", "/mnt/d/datasets/hf_kalshi_trades")
    hf_windows = []
    if Path(hf_dir).exists():
        try:
            hf_windows = load_hf_trade_windows(hf_dir, max_markets=10000)
        except Exception as e:
            print(f"  HF load failed: {e}")

    all_windows = tick_windows + hf_windows
    print(f"  {len(tick_windows)} tick + {len(hf_windows)} HF = {len(all_windows)} total windows")

    # Load prices
    crypto_prices = {}
    for d in ["data/prices_hf", os.path.join(data_dir, "prices_hf"),
              "data/prices", os.path.join(data_dir, "prices")]:
        if Path(d).exists():
            loaded = load_crypto_prices(d)
            for coin, pts in loaded.items():
                crypto_prices.setdefault(coin, []).extend(pts)
    for coin in list(crypto_prices):
        seen = set()
        deduped = [(ts, px) for ts, px in sorted(crypto_prices[coin])
                   if ts not in seen and not seen.add(ts)]
        crypto_prices[coin] = deduped

    print(f"  Prices: {', '.join(f'{c.upper()}={len(v)}' for c, v in crypto_prices.items())}")

    # Collect all entries
    print("\nCollecting entries...")
    entries = collect_entries(all_windows, crypto_prices)
    print(f"  {len(entries)} total entries")

    df = pd.DataFrame(entries)
    wins = df["won"].sum()
    total = len(df)
    print(f"  Overall: {wins:.0f}W / {total - wins:.0f}L ({wins/total:.1%} WR)")

    # Filter to RR-relevant range (85c+ favorites)
    rr = df[df["fav_price"] >= 85].copy()
    rr_wins = rr["won"].sum()
    rr_total = len(rr)
    print(f"  RR range (85c+): {rr_wins:.0f}W / {rr_total - rr_wins:.0f}L ({rr_wins/rr_total:.1%} WR)")

    # === 1. Univariate WR Curves ===
    print(f"\n{'=' * 65}")
    print("1. Univariate WR Curves (how WR changes with each parameter)")
    print("=" * 65)

    for feat in ["buffer_pct", "secs_left", "entry_price", "momentum_60_5",
                  "realized_vol"]:
        result = univariate_wr_analysis(rr, feat, n_bins=8)
        if result is not None:
            print(f"\n  {feat}:")
            for idx, row in result.iterrows():
                wr = row["mean"]
                n = int(row["count"])
                bar = "#" * int(wr * 40)
                print(f"    {str(idx):>30s}: {wr:5.1%} WR ({n:5d}n) {bar}")

    # === 2. Correlation & WR Spread ===
    print(f"\n{'=' * 65}")
    print("2. Parameter Importance Ranking")
    print("=" * 65)

    importance = compute_importance(rr)
    # Sort by WR spread (most discriminating first)
    ranked = sorted(importance.items(),
                    key=lambda x: x[1].get("wr_spread", 0) or 0,
                    reverse=True)

    print(f"\n  {'Parameter':<20s} {'WR Spread':>10s} {'|r|':>8s} {'Best Q':>8s} {'Worst Q':>9s} {'Direction':>10s} {'N':>7s}")
    print(f"  {'-'*20} {'-'*10} {'-'*8} {'-'*8} {'-'*9} {'-'*10} {'-'*7}")
    for feat, m in ranked:
        if m.get("wr_spread") is None:
            print(f"  {feat:<20s} {'N/A':>10s} {'N/A':>8s} (insufficient data, n={m['n']})")
            continue
        direction = f"{m['corr_sign']} ({'higher=better' if m['corr_sign']=='+' else 'lower=better'})"
        print(f"  {feat:<20s} {m['wr_spread']:>9.1%} {m['correlation']:>8.3f} "
              f"{m['best_quintile_wr']:>7.1%} {m['worst_quintile_wr']:>8.1%} "
              f"{direction:>10s} {m['n']:>7d}")

    # === 3. Random Forest Importance ===
    print(f"\n{'=' * 65}")
    print("3. Random Forest Feature Importance")
    print("=" * 65)

    rf_imp = random_forest_importance(rr)
    if rf_imp:
        rf_ranked = sorted(rf_imp.items(), key=lambda x: x[1], reverse=True)
        total_imp = sum(v for v in rf_imp.values())
        for feat, imp in rf_ranked:
            bar = "#" * int(imp / total_imp * 50)
            print(f"  {feat:<20s}: {imp/total_imp:>5.1%}  {bar}")
    else:
        print("  Insufficient data for RF analysis")

    # === 4. Practical Recommendation ===
    print(f"\n{'=' * 65}")
    print("4. Practical Takeaways")
    print("=" * 65)

    if ranked:
        top = ranked[0][0]
        print(f"\n  Most important parameter: {top}")
        print(f"  WR spread between best and worst quintile: {ranked[0][1].get('wr_spread', 0):.1%}")
        print(f"\n  Recommendation for optimizer:")
        print(f"  - Focus search budget on top parameters")
        print(f"  - Use wider ranges for less important parameters")
        print(f"  - Consider fixing the least important params at safe defaults")


if __name__ == "__main__":
    main()
