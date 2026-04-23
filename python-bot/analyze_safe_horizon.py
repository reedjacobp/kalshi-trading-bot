"""Compute the empirical safe-entry horizon per (coin, market_type) cell.

Instead of letting the optimizer search over `max_seconds` as a free
parameter — which blows up the search space and overfits on ~50 CV
trades per 15M cell — derive it from data. For each cell we ask:

    "If I allow entries at any secs_left ≤ X (given price ≥ MIN_PRICE
    and a healthy buffer on the favored side), what fraction of those
    entries end up on the correct side at settlement?"

As X grows, that fraction drops (more time = more chance for an
adverse move). We pick the largest X where the cumulative correctness
is still ≥ CORRECTNESS_THRESHOLD. That becomes the cell's `max_seconds`
ceiling, freeing the optimizer to focus on the parameters that
actually encode edge: price range, buffer, momentum.

Normally invoked inline by `optimize_rr.py` (so horizons are recomputed
from the same data the optimizer is about to train on — no drift).
Can also be run standalone for debugging/exploration:

    python analyze_safe_horizon.py
    MIN_PRICE=90 python analyze_safe_horizon.py
    THRESHOLD=0.95 python analyze_safe_horizon.py

Standalone output: `data/safe_horizons.json` plus a per-cell table on
stdout. The optimizer writes the same file as part of its normal run.

Design note — no top-level import from optimize_rr: keeps the
dependency arrow one-way (optimize_rr imports us), so `optimize_rr`
can call into this module without circular-import fragility.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path

from data_paths import resolve

# Ascending, non-overlapping. Finer near zero (where most gate-passing
# entries land today) and coarser at the tail (where samples are thin
# and all we care about is whether any of them hold up).
DEFAULT_BUCKETS: list[tuple[int, int]] = [
    (0, 30), (30, 45), (45, 60), (60, 90), (90, 120),
    (120, 180), (180, 240), (240, 360), (360, 500),
]


def _bucket_index(secs: float, buckets: list[tuple[int, int]]) -> int:
    for i, (lo, hi) in enumerate(buckets):
        if lo <= secs < hi:
            return i
    return -1


def compute_horizons(
    windows_by_cell: dict[str, list[dict]],
    *,
    min_price: int = 95,
    min_buffer_pct: float = 0.10,
    threshold: float = 0.85,
    buckets: list[tuple[int, int]] | None = None,
    default_max_seconds: int = 120,
    min_bucket_samples: int = 50,
) -> dict[str, dict]:
    """Compute safe-entry horizons from already-preprocessed windows.

    Each window in `windows_by_cell[cell]` must have `_pp` attached
    (the output of `optimize_rr.preprocess_window`). We inspect every
    entry in every window, filter to those at `entry_price ≥ min_price`
    with a favorable buffer (when buffer is available), bucket by
    `secs_left`, and find the largest bucket whose own correctness
    clears `threshold`. That bucket's upper bound becomes the cell's
    `max_seconds` ceiling.

    Per-bucket (not cumulative): an early noisy bucket with low
    correctness on few samples shouldn't veto the cell. A bucket must
    have `n ≥ min_bucket_samples` before its verdict counts, so
    sparse early or late tails don't dominate. Once a sufficiently-
    sampled bucket falls below `threshold`, we stop extending — later
    buckets that recover are usually just noise.

    When a cell has no usable buckets (new market, not enough data)
    we fall back to `default_max_seconds` so the optimizer still gets
    a tradeable ceiling. Callers can detect this via
    `horizons[cell]["from_default"]`.
    """
    if buckets is None:
        buckets = DEFAULT_BUCKETS

    horizons: dict[str, dict] = {}
    for cell, windows in windows_by_cell.items():
        # bucket -> [n_total, n_correct, n_buffer_used]
        bins = [[0, 0, 0] for _ in buckets]
        for w in windows:
            pp = w.get("_pp")
            if pp is None:
                continue
            result = pp["result"]
            for e in pp["entries"]:
                if e["entry_price"] < min_price:
                    continue
                used_buf = False
                if e.get("buffer_pct") is not None:
                    buf = e["buffer_pct"]
                    if e["side"] == "yes" and buf < min_buffer_pct:
                        continue
                    if e["side"] == "no" and buf > -min_buffer_pct:
                        continue
                    used_buf = True
                bi = _bucket_index(e["secs_left"], buckets)
                if bi < 0:
                    continue
                slot = bins[bi]
                slot[0] += 1
                if e["side"] == result:
                    slot[1] += 1
                if used_buf:
                    slot[2] += 1

        # Walk buckets in ascending order. Extension rule is
        # per-bucket (bucket_correctness >= threshold, well-sampled);
        # stop rule is a joint check (bucket AND cumulative both fail).
        # The joint stop rule avoids two pathologies:
        #   1. A sparse, anomalous later bucket (e.g. 87 entries at 7%
        #      correct on bnb_hourly's 240-360s window) doesn't veto a
        #      strong 60-90s horizon just because the optimizer might
        #      theoretically search into that range.
        #   2. A sustained drop (multiple consecutive failing buckets
        #      that push cum below threshold) correctly terminates
        #      further extension.
        # Under-sampled buckets (n < min_bucket_samples) are recorded
        # but ignored for both extension and stop decisions.
        total_n = 0
        total_c = 0
        total_buf = 0
        max_secs = 0
        any_committed = False
        stopped = False
        bucket_rows: list[dict] = []
        for (lo, hi), (n, c, bu) in zip(buckets, bins):
            if n == 0:
                bucket_rows.append({
                    "range": [lo, hi], "n": 0,
                    "bucket_correct": None, "cum_correct": None,
                    "buffer_used_frac": None,
                })
                continue
            b_frac = c / n
            if n >= min_bucket_samples:
                total_n += n
                total_c += c
                total_buf += bu
            cum_frac = total_c / total_n if total_n else 0
            bucket_rows.append({
                "range": [lo, hi], "n": n,
                "bucket_correct": round(b_frac, 4),
                "cum_correct": round(cum_frac, 4),
                "buffer_used_frac": round(bu / n, 4),
            })
            if stopped or n < min_bucket_samples:
                continue
            if b_frac >= threshold:
                max_secs = hi
                any_committed = True
            elif cum_frac < threshold and any_committed:
                stopped = True

        # Two fallback cases for `max_secs == 0`:
        #   1. No samples at all — truly unknown horizon.
        #   2. Samples exist but no bucket cleared threshold — the cell
        #      is either genuinely unprofitable under these filters OR
        #      our threshold is too strict for the price regime. Either
        #      way, locking max_seconds to 0 kills the cell in CV.
        #      Fall back to the default ceiling and let the optimizer's
        #      own CV gate decide whether the cell is viable.
        used_default = max_secs == 0
        if used_default:
            max_secs = default_max_seconds

        horizons[cell] = {
            "max_seconds": max_secs,
            "threshold": threshold,
            "min_price": min_price,
            "min_buffer_pct": min_buffer_pct,
            "n_samples": total_n,
            "n_correct": total_c,
            "overall_correctness": round(total_c / total_n, 4) if total_n else None,
            "buffer_coverage": round(total_buf / total_n, 4) if total_n else None,
            "buckets": bucket_rows,
            "from_default": used_default,
        }
    return horizons


def format_horizons_table(horizons: dict[str, dict]) -> str:
    """Pretty-print per-cell bucket tables. Returns one long string."""
    lines: list[str] = []
    for cell in sorted(horizons.keys()):
        h = horizons[cell]
        lines.append(f"--- {cell} ---")
        if h["from_default"]:
            lines.append(f"  (no samples — falling back to max_seconds={h['max_seconds']}s)")
            lines.append("")
            continue
        header = f"  {'bucket':>11s}  {'n':>6s}  {'bucket %':>9s}  {'cum %':>7s}  {'buf%':>5s}"
        lines.append(header)
        for row in h["buckets"]:
            lo, hi = row["range"]
            if row["n"] == 0:
                lines.append(
                    f"  {lo:>4}-{hi:>3}s  {'0':>6}  {'—':>9}  {'—':>7}  {'—':>5}"
                )
                continue
            mark = " "  # correctness mark computed on reconstruction
            cum = row["cum_correct"] * 100
            if cum >= h["threshold"] * 100:
                mark = "✓"
            lines.append(
                f"  {lo:>4}-{hi:>3}s  {row['n']:>6d}  "
                f"{row['bucket_correct'] * 100:>7.1f}%  "
                f"{cum:>6.1f}% {mark} "
                f"{row['buffer_used_frac'] * 100:>4.0f}%"
            )
        overall = (h['overall_correctness'] or 0) * 100
        lines.append(
            f"  → max_seconds = {h['max_seconds']}s  "
            f"(n={h['n_samples']}, cum correctness = {overall:.1f}%)"
        )
        lines.append("")
    return "\n".join(lines)


def save_horizons(horizons: dict, path: str | None = None) -> str:
    """Write horizons to JSON. Returns the absolute path used."""
    out_path = path or resolve("safe_horizons.json")
    Path(out_path).write_text(json.dumps(horizons, indent=2))
    return str(out_path)


# ─── Standalone entry point ──────────────────────────────────────────

def _standalone_main() -> None:
    """Load windows + preprocess, then compute and report horizons.

    Only used when running this file directly. Lazy-imports from
    optimize_rr to avoid a top-level circular dependency.
    """
    import pickle
    from optimize_rr import (
        load_tick_windows,
        load_hf_trade_windows,
        load_crypto_prices,
        preprocess_window,
    )
    from data_paths import all_candidates

    min_price = int(os.getenv("MIN_PRICE", "94"))
    min_buffer_pct = float(os.getenv("MIN_BUFFER_PCT", "0.05"))
    threshold = float(os.getenv("THRESHOLD", "0.97"))

    print("Safe-entry-horizon analysis")
    print(f"  MIN_PRICE={min_price}¢  MIN_BUFFER={min_buffer_pct}%  "
          f"THRESHOLD={threshold}")
    print()

    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")
    print("[1/3] Loading market windows...")
    tick_dir = os.path.join(data_dir, "ticks")
    tick_windows = load_tick_windows(tick_dir) if Path(tick_dir).exists() else []
    print(f"  Ticks:   {len(tick_windows):>7,}")

    hf_windows: list = []
    if os.getenv("SKIP_HF", "0") != "1":
        hf_dir = os.getenv("HF_DIR", "/mnt/d/datasets/hf_kalshi_trades")
        max_markets = int(os.getenv("PARQUET_MAX_MARKETS", "10000"))
        if Path(hf_dir).exists():
            hf_windows = load_hf_trade_windows(hf_dir, max_markets=max_markets)
            print(f"  HF:      {len(hf_windows):>7,}")

    settled: list = []
    if os.getenv("SKIP_SETTLED", "0") != "1":
        p = Path("data/settled_windows.pkl")
        if p.exists():
            with open(p, "rb") as f:
                settled = pickle.load(f)
            print(f"  Settled: {len(settled):>7,}")

    all_w = tick_windows + hf_windows + settled
    print(f"  TOTAL:   {len(all_w):>7,}")

    print("\n[2/3] Loading crypto spot prices...")
    crypto_prices: dict = defaultdict(list)
    for d in list(all_candidates("prices_hf")) + list(all_candidates("prices")):
        for coin, pts in load_crypto_prices(str(d)).items():
            crypto_prices[coin].extend(pts)
    # De-dupe by second.
    for coin, pts in list(crypto_prices.items()):
        seen = set()
        deduped = []
        for ts, px in sorted(pts):
            k = round(ts)
            if k in seen:
                continue
            seen.add(k)
            deduped.append((ts, px))
        crypto_prices[coin] = deduped

    print("\n[3/3] Preprocessing + bucketing entries...")
    windows_by_cell: dict = defaultdict(list)
    n_pp = 0
    for w in all_w:
        pp = preprocess_window(w, crypto_prices)
        if pp is None:
            continue
        w["_pp"] = pp
        windows_by_cell[f"{w['coin']}_{w['market_type']}"].append(w)
        n_pp += 1
    print(f"  preprocessed {n_pp:,} windows")

    horizons = compute_horizons(
        windows_by_cell,
        min_price=min_price,
        min_buffer_pct=min_buffer_pct,
        threshold=threshold,
    )
    print()
    print(format_horizons_table(horizons))
    path = save_horizons(horizons)
    print(f"Wrote {path}")


if __name__ == "__main__":
    _standalone_main()
