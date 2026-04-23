#!/usr/bin/env python3
"""
Monte Carlo Parameter Optimizer for Resolution Rider

Phase 1: Explore parameter space against historical data
Phase 2: Walk-forward validate top candidates against recent tick data
Output: Per-cell optimal parameters in data/rr_params.json

Usage:
    python optimize_rr.py
"""

import csv
import json
import math
import os
import multiprocessing
import random
import re
import sys
import time
from collections import defaultdict

# Unbuffered output so we see progress in real-time
sys.stdout.reconfigure(line_buffering=True)
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ── Slippage Model (from Kalshi fill data) ───────────────────────

def load_slippage_model(kalshi_csv: str) -> dict[int, float]:
    """
    Compute average slippage penalty per entry price level from Kalshi fills.

    Slippage is estimated as the average taker fees per contract at each
    entry price level (94-99c range). This captures the real cost of
    crossing the spread that perfect-fill simulation misses.

    Returns: dict mapping entry_price_cents -> slippage_cents_per_contract
    """
    if not os.path.exists(kalshi_csv):
        print(f"  [slippage] Kalshi CSV not found at {kalshi_csv}, using defaults")
        return {}

    # Accumulate per-price-level stats from RR-range fills
    by_price = defaultdict(lambda: {"total_qty": 0, "total_fees": 0, "count": 0})
    with open(kalshi_csv, newline="") as f:
        for row in csv.DictReader(f):
            if row["type"] != "trade":
                continue
            ep = int(row["entry_price_cents"])
            if 94 <= ep <= 99:
                qty = int(row["quantity"])
                fees = int(row["open_fees_cents"]) + int(row["close_fees_cents"])
                d = by_price[ep]
                d["total_qty"] += qty
                d["total_fees"] += fees
                d["count"] += 1

    slippage = {}
    for price, d in sorted(by_price.items()):
        if d["total_qty"] > 0:
            # Fee-based slippage: actual fees paid per contract
            fee_per_contract = d["total_fees"] / d["total_qty"]
            # Add a fixed spread-crossing estimate (empirical: ~0.3c avg)
            # This accounts for the ask being slightly above mid when we buy
            spread_penalty = 0.3
            slippage[price] = fee_per_contract + spread_penalty
        else:
            slippage[price] = 0.5  # Default if no data

    return slippage


# Global slippage model, loaded once at startup
SLIPPAGE_MODEL: dict[int, float] = {}


def get_slippage_cents(entry_price_cents: int) -> float:
    """Get slippage penalty in cents for a given entry price.

    Set ZERO_SLIPPAGE=1 to return 0 for every level — this is the right
    simulation mode when the bot runs as maker/post-only on Kalshi, where
    maker fees are 0 and fills happen at the posted price without paying
    the spread-crossing cost captured by the empirical SLIPPAGE_MODEL.
    """
    if os.getenv("ZERO_SLIPPAGE", "1") == "1":
        return 0.0
    if not SLIPPAGE_MODEL:
        return 0.5  # Conservative default if no model loaded
    return SLIPPAGE_MODEL.get(entry_price_cents, 0.5)


# Global fill-rate model (fit from live_trades.csv by fit_fill_rate.py).
# Loaded once at optimizer startup so simulate_fast can weight each
# simulated trade's profit by realistic fill probability instead of
# assuming every gate-pass becomes a fill at the posted price. Without
# this the optimizer converges on absurdly tight windows (45-60s,
# max_seconds=60) because the simulator doesn't pay the IOC-cancel cost
# those tight windows incur live.
FILL_RATE_MODEL: dict = {}


def get_fill_probability(entry_price_cents: int, secs_left: float) -> float:
    """P(fill | price, secs_left) from the fitted logistic model.

    Returns 1.0 when no model is loaded or when DISABLE_FILL_MODEL=1 —
    that's the legacy behavior and a useful ablation knob. Otherwise
    returns sigmoid(a + b*price + c*secs) clipped to [fill_min, fill_max]
    so the optimizer can't exploit made-up regions far outside the
    training distribution.
    """
    if os.getenv("DISABLE_FILL_MODEL", "0") == "1" or not FILL_RATE_MODEL:
        return 1.0
    a = FILL_RATE_MODEL["a"]
    b = FILL_RATE_MODEL["b"]
    c = FILL_RATE_MODEL["c"]
    logit = a + b * entry_price_cents + c * secs_left
    # Clamp the logit before the exp to avoid overflow at extreme
    # values; ±50 maps to probabilities indistinguishable from 0/1.
    if logit > 50:
        p = 1.0
    elif logit < -50:
        p = 0.0
    else:
        p = 1.0 / (1.0 + math.exp(-logit))
    lo = FILL_RATE_MODEL.get("fill_min", 0.3)
    hi = FILL_RATE_MODEL.get("fill_max", 1.0)
    return max(lo, min(hi, p))


def load_fill_rate_model(data_dir: str) -> dict:
    """Load data/fill_rate_model.json if it exists. Idempotent — called
    from main() at startup and safe to call again in workers that were
    spawned before the global was populated."""
    path = Path(data_dir) / "fill_rate_model.json"
    if not path.exists():
        # Also try the bot's default data dir for the dev workflow
        alt = Path(__file__).parent / "data" / "fill_rate_model.json"
        if alt.exists():
            path = alt
        else:
            return {}
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


# ── Parameter Space ───────────────────────────────────────────────

PARAM_RANGES = {
    "min_contract_price": (95, 98),
    "max_entry_price": (96, 98),
    "max_seconds": (60, 480),
    "min_price_buffer_pct": (0.05, 0.50),
    "max_adverse_momentum": (-0.10, 0.0),
    "momentum_window": (30, 300),
    "momentum_periods": (1, 10),
}

ALPHA = 0.3  # Win rate weight in blended score (training phase only)

# Minimum validation trades required IN EVERY FOLD for a candidate to be
# considered viable during CV. Set to 0 by default: Wilson LB on the
# aggregate sample + the per-fold recency weighting already penalizes
# candidates that concentrate all their trades in one slice. The per-fold
# floor was blocking 15M cells whose buffered trades cluster in volatile
# periods, even when the overall sample was statistically solid.
MIN_VAL_TRADES_PER_FOLD = int(os.getenv("MIN_VAL_TRADES_PER_FOLD", "0"))

# Minimum aggregate validation trades across all folds. With walk-forward,
# a cell needs at least this many total validated samples to even attempt
# a fit. Lowered to 15 on 2026-04-22 — 30 was ruling out viable-but-rare
# setups on tight-gate cells and was a meaningful contributor to thin
# trade volume. 15 still gives Wilson LB a big enough sample to punish
# the noisy candidates on its own.
MIN_TOTAL_VAL_TRADES = int(os.getenv("MIN_TOTAL_VAL_TRADES", "15"))

# Wilson score z for the CV lower bound. 1.96 = 95% one-sided, 2.58 = 99%,
# 1.0 ≈ 68%. Higher z = more conservative (prefers larger samples more).
WILSON_Z = float(os.getenv("WILSON_Z", "1.96"))

# Walk-forward CV settings. The optimizer builds train/validate splits
# that respect temporal ordering (no peeking at future data when
# "validating" past slices). Because you re-optimize every 3 days, each
# validation window should roughly represent that cadence.
WF_MAX_FOLDS = int(os.getenv("WF_MAX_FOLDS", "10"))
WF_MIN_DATES = int(os.getenv("WF_MIN_DATES", "6"))  # below this, skip CV
# Recency weighting: each walk-forward fold carries a weight that decays
# with age. The most recent fold gets weight 1.0; earlier folds halve
# every RECENCY_HALFLIFE_FOLDS steps back. Set to a large number to
# effectively disable recency weighting.
RECENCY_HALFLIFE_FOLDS = float(os.getenv("RECENCY_HALFLIFE_FOLDS", "3.0"))

# Risk tolerance — global knob that reshapes the optimizer's score to
# reward trade volume alongside win rate + PPT. The existing score,
# ALPHA*WR + (1-ALPHA)*norm_PPT, is volume-agnostic because PPT
# normalizes profit per trade. That's why the optimizer converges on
# narrow-window strategies (45-60s) — they produce few high-WR trades
# that dominate PPT even when a wider-window strategy would earn far
# more total dollars.
#
# With RISK_TOLERANCE>0, the score becomes:
#   ALPHA*WR + (1-ALPHA)*norm_PPT + RISK_TOLERANCE * norm_TRADES
# where norm_TRADES = min(1, trades / TRADE_VOL_NORM). The optimizer
# still cannot pick money-losing params — CV's Wilson LB gate still
# filters those out — but when ranking *viable* candidates it prefers
# higher-volume ones proportional to RISK_TOLERANCE.
#
# Guide:
#   0.0   — legacy behavior, pure WR+PPT (default)
#   0.1   — mild preference for volume
#   0.3   — noticeably prefers higher-volume strategies
#   0.5+  — aggressive, volume-dominant
RISK_TOLERANCE = float(os.getenv("RISK_TOLERANCE", "0.0"))
TRADE_VOL_NORM = float(os.getenv("TRADE_VOL_NORM", "500.0"))

# ── Maker-order simulation ───────────────────────────────────────
# The simulator defaults to taker semantics (fill at observed ask,
# pay a fee). Set ORDER_MODE=maker to simulate resting limit orders:
# the candidate posts a bid `maker_bid_offset` cents below the current
# ask, and the trade fills only if the market's side-appropriate ask
# walks down to the bid within `maker_timeout` seconds (otherwise the
# order expires with zero P&L).
#
# Maker flips the edge math: entry price drops by the offset, fees go
# to zero (Kalshi charges $0 for maker fills), but fills are adverse-
# selected (the only way to get filled is for the price to move against
# the typical RR thesis) and rarer (the market must actually come back).
# Whether maker beats taker is cell-dependent — run the optimizer in
# each mode and compare.
ORDER_MODE = os.getenv("ORDER_MODE", "taker").strip().lower()
# Maker and taker are both CUDA-backed (see optimize_rr_cuda.py
# score_candidates — maker_fill mask at ~line 290 and bid-priced
# execution at ~line 355). Narrow defaults keep the maker candidate
# grid tractable on CPU-only boxes, but they can be widened freely
# via env vars when CUDA is available.
MAKER_OFFSETS = tuple(
    int(x) for x in os.getenv("MAKER_OFFSETS", "0,1,2,3").split(",") if x.strip()
)
MAKER_TIMEOUTS = tuple(
    int(x) for x in os.getenv("MAKER_TIMEOUTS", "60,120").split(",") if x.strip()
)

# Restrict the per-cell CV loop to a subset of cells. Useful for
# focused maker-mode runs: `CELL_FILTER=btc_hourly,bnb_hourly` runs
# just those two, letting CPU-only maker finish in minutes instead
# of hours. Empty = run every cell.
CELL_FILTER = tuple(
    c.strip() for c in os.getenv("CELL_FILTER", "").split(",") if c.strip()
)


def wilson_lower_bound(wins: int, n: int, z: float = WILSON_Z) -> float:
    """Wilson score lower bound for a binomial proportion.

    Penalizes small samples: 5/5 wins → LB ≈ 0.48, 200/205 wins → LB ≈ 0.93.
    Used to rescore CV candidates so a profit-maxing optimizer cannot
    prefer a 100%-WR-on-5-trades candidate over a 97%-WR-on-200-trades
    one just because the former has zero observed losses.
    """
    if n <= 0:
        return 0.0
    p = wins / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = (z / denom) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, center - margin)


# ── RR parameter space ───────────────────────────────────────────
# Every runtime-gating parameter in resolution_rider.py must appear
# below. Audit done 2026-04-14 after the XRP/SOL losses:
#   min_contract_price, max_entry_price, min_seconds, max_seconds,
#   min_price_buffer_pct, max_adverse_momentum, momentum_window,
#   momentum_periods, max_realized_vol_pct — all searched.
#   kelly_fraction / max_bankroll_pct are intentionally NOT searched
#   (RR uses fixed STAKE_USD).
#
# Design note: entry price band is now swept on BOTH ends. Historically
# `max_entry_price` was pinned at 98 because early runs collapsed cells
# to degenerate 1c bands (e.g. 96-96c, 97-97c) where live execution
# catches almost nothing because the book blows through those cents in
# 1-2 seconds. We mitigate that with a minimum band width (MIN_BAND_WIDTH)
# instead of pinning the max, so the optimizer can discover cells whose
# natural breakeven is below 98c — e.g. sol_15m currently runs ~89% WR
# which is catastrophic at 98c entry (needs 98%) but profitable at 89c.
# This matters mostly for the high-frequency 15M cells; hourly cells
# should keep converging on the classic 95-98c band anyway.

# vol_lookback is fixed at 300s for now; making it a search dimension
# would explode the candidate count without much benefit since a
# 5-min realized-vol window is the standard for crypto microstructure.
VOL_LOOKBACK = 300

# Entry-band constraints. Absolute floor = 88c (anything below is a
# coin-flip, not RR). Absolute ceiling = 98c (99c requires 99% WR
# to break even even with $0 fees, and empirical WR is ~97-98% —
# 99c entries are consistent money losers in tick-level data).
# Minimum band width = 3c so the band is tradeable in live.
MIN_CONTRACT_PRICE_FLOOR = int(os.getenv("MIN_CONTRACT_PRICE_FLOOR", "88"))
MAX_ENTRY_PRICE_CEIL = 98
MIN_BAND_WIDTH = 3


# Floor for min_seconds across all cells. The strategy holds to
# settlement, but the final few seconds are dominated by Kalshi's
# settlement-processing latency and not safe to enter. 10s is the
# same floor the live bot uses.
MIN_SECONDS_FLOOR = 10
# Safe fallback when horizon analysis has no samples for a cell (new
# market families, first-run case). Conservative middle ground — the
# optimizer can still trade, but won't reach into noisy long-horizon
# territory until the data justifies it.
DEFAULT_MAX_SECONDS = 120


def sample_params(max_seconds_cap: int = DEFAULT_MAX_SECONDS) -> dict:
    # Parameter importance ranking (from analyze_param_importance.py):
    #   1. momentum  (40.1% WR spread) — search aggressively
    #   2. buffer    (38.9% WR spread) — search aggressively
    #   3. secs_left (14.4% WR spread) — searched within an empirical
    #      per-cell ceiling from analyze_safe_horizon.compute_horizons.
    #      Cap, not a fixed value: the optimizer still picks the sweet
    #      spot inside the data-supported range.
    #   4. entry_price (4.7%) — set wide, don't over-constrain
    #   5. realized_vol (4.3%) — disabled (not predictive)
    mcp = random.choice([88, 89, 90, 91, 92, 93, 94, 95])
    mep = random.choice([96, 97, 98])
    if mep - mcp < MIN_BAND_WIDTH:
        mcp = mep - MIN_BAND_WIDTH
    # Max-seconds choices limited to the cell's horizon ceiling. At
    # horizon=90, options are {60, 90}; at 120, {60, 90, 120}; etc.
    ms_choices = [ms for ms in (60, 90, 120, 180, 240, 300, 480, 600)
                  if ms <= max_seconds_cap]
    if not ms_choices:
        ms_choices = [max_seconds_cap]
    ms = random.choice(ms_choices)
    # Min-seconds is hardcoded to MIN_SECONDS_FLOOR. Previously
    # searched over {10, 15, 30, 45}, but the optimizer was picking
    # values like 45 on cells whose own horizon data showed >93%
    # correctness in the 0-30s bucket — leaving real trades on the
    # table. Pinning to 10 removes that over-restriction; the buffer
    # and momentum gates still block unsafe entries.
    maker_offset = random.choice(MAKER_OFFSETS) if ORDER_MODE == "maker" else 0
    maker_timeout = (random.choice(MAKER_TIMEOUTS)
                     if ORDER_MODE == "maker" else MAKER_TIMEOUTS[0])
    return {
        "min_contract_price": mcp,
        "max_entry_price": mep,
        "min_seconds": MIN_SECONDS_FLOOR,
        "max_seconds": ms,
        "order_mode": ORDER_MODE,
        "maker_bid_offset": maker_offset,
        "maker_timeout": maker_timeout,
        # Buffer: fine-grained search (rank #2)
        "min_price_buffer_pct": round(random.uniform(0.03, 0.60), 3),
        # Momentum: fine-grained search (rank #1)
        "max_adverse_momentum": round(random.uniform(-0.10, 0.0), 4),
        "momentum_window": random.choice([30, 60, 90, 120, 180, 300]),
        "momentum_periods": random.randint(1, 10),
        # Vol: disabled — rank #5, only 4.3% WR spread, kills trade count
        "max_realized_vol_pct": None,
        "vol_lookback": VOL_LOOKBACK,
    }


def perturb_around(anchor: dict, n: int) -> list[dict]:
    """Generate `n` local-search candidates around an anchor param dict.
    Used by PHASE=refine to refit continuous dims (buffer, momentum)
    near a known-good point rather than searching the whole space.

    Strategy: each dim gets its own noise scale tuned to how much the
    space tolerates wiggling. Continuous dims (buffer, momentum) get
    gaussian perturbations; discrete ones get local neighbor picks."""
    out = []
    a_mcp = anchor["min_contract_price"]
    a_mep = anchor["max_entry_price"]
    a_mins = anchor.get("min_seconds", 10)
    a_maxs = anchor["max_seconds"]
    a_buf = anchor["min_price_buffer_pct"]
    a_mom = anchor["max_adverse_momentum"]
    a_mw = anchor.get("momentum_window", 60)
    a_mp = anchor.get("momentum_periods", 5)

    mw_choices = [30, 60, 90, 120, 180, 300]
    mp_choices = [3, 4, 5, 6, 7, 8, 9, 10]
    mins_choices = [10, 15, 30, 45]
    maxs_choices = [60, 90, 120, 180, 240, 300, 480, 600]

    def _nearby(choices, val, spread):
        """Pick one of the `spread` closest values in `choices` to `val`."""
        sorted_by_dist = sorted(choices, key=lambda c: abs(c - val))
        return random.choice(sorted_by_dist[:spread])

    for _ in range(n):
        # 80% tight noise, 20% wider exploration — prevents getting
        # stuck in a narrow basin if the anchor was a local only.
        wide = random.random() < 0.2
        buf_sigma = 0.08 if wide else 0.02
        mom_sigma = 0.03 if wide else 0.008

        mcp = max(MIN_CONTRACT_PRICE_FLOOR,
                  min(MAX_ENTRY_PRICE_CEIL - MIN_BAND_WIDTH,
                      a_mcp + random.choice([-1, 0, 0, 0, 1])))
        mep = max(mcp + MIN_BAND_WIDTH,
                  min(MAX_ENTRY_PRICE_CEIL,
                      a_mep + random.choice([-1, 0, 0, 0, 1])))
        buf = max(0.01, min(0.80, a_buf + random.gauss(0, buf_sigma)))
        mom = max(-0.15, min(0.0, a_mom + random.gauss(0, mom_sigma)))
        out.append({
            "min_contract_price": mcp,
            "max_entry_price": mep,
            "min_seconds": _nearby(mins_choices, a_mins, 2 if not wide else 4),
            "max_seconds": _nearby(maxs_choices, a_maxs, 2 if not wide else 4),
            "min_price_buffer_pct": round(buf, 4),
            "max_adverse_momentum": round(mom, 5),
            "momentum_window": _nearby(mw_choices, a_mw, 2 if not wide else 4),
            "momentum_periods": _nearby(mp_choices, a_mp, 2 if not wide else 4),
            "max_realized_vol_pct": None,
            "vol_lookback": VOL_LOOKBACK,
        })
    # Always include the anchor itself so we can see if any perturbation
    # actually beat the starting point.
    out.append({
        "min_contract_price": a_mcp,
        "max_entry_price": a_mep,
        "min_seconds": a_mins,
        "max_seconds": a_maxs,
        "min_price_buffer_pct": a_buf,
        "max_adverse_momentum": a_mom,
        "momentum_window": a_mw,
        "momentum_periods": a_mp,
        "max_realized_vol_pct": None,
        "vol_lookback": VOL_LOOKBACK,
    })
    return out


def grid_params(max_seconds_cap: int = DEFAULT_MAX_SECONDS) -> list[dict]:
    """Exhaustive sweep over the parameters that actually encode edge.

    Time bounds (`min_seconds`, `max_seconds`) are still searched, but
    `max_seconds` is clipped to `max_seconds_cap` — the cell's empirical
    horizon from `analyze_safe_horizon.compute_horizons`. At horizon=90,
    candidates sweep max_seconds over {60, 90} only; at horizon=120,
    over {60, 90, 120}; etc. This is meaningfully smaller than the
    unclipped 8-value search but still lets the optimizer find tighter
    windows when CV prefers them.

    `GRID_MODE=small` reverts to a coarse price-band set for fast CPU runs.
    """
    combos = []
    mode = os.environ.get("GRID_MODE", "full").lower()
    if mode == "small":
        band_choices = [
            (90, 98), (92, 98), (93, 98), (95, 98),
            (90, 96), (92, 96), (93, 97),
        ]
    else:
        band_choices = [
            (mcp, mep)
            for mep in (96, 97, 98)
            for mcp in range(MIN_CONTRACT_PRICE_FLOOR, mep - MIN_BAND_WIDTH + 1)
        ]
    # min_seconds is hardcoded to MIN_SECONDS_FLOOR (not searched).
    # See sample_params() for rationale — the old search over
    # {10, 15, 30, 45} was over-restricting cells whose data showed
    # 0-30s entries were safe. Grid only varies max_seconds within
    # the cell's horizon cap.
    time_choices = [
        (MIN_SECONDS_FLOOR, ms)
        for ms in (60, 90, 120, 180, 240, 300, 480, 600)
        if MIN_SECONDS_FLOOR < ms and ms <= max_seconds_cap
    ]
    if not time_choices:
        time_choices = [(MIN_SECONDS_FLOOR, max_seconds_cap)]
    # Maker dims — only searched when ORDER_MODE=maker; otherwise
    # fixed so the grid doesn't multiply out uselessly.
    if ORDER_MODE == "maker":
        maker_choices = [(o, t) for o in MAKER_OFFSETS for t in MAKER_TIMEOUTS]
    else:
        maker_choices = [(0, MAKER_TIMEOUTS[0])]
    for mcp, mep in band_choices:
        for mins, ms in time_choices:
            for buf in [0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]:
                for mom in [-0.10, -0.08, -0.06, -0.04, -0.03,
                            -0.02, -0.01, -0.005, 0.0]:
                    for mw in (30, 60, 90, 120, 180, 300):
                        for mp in (3, 4, 5, 6, 7, 8, 9, 10):
                            for m_off, m_to in maker_choices:
                                combos.append({
                                    "min_contract_price": mcp,
                                    "max_entry_price": mep,
                                    "min_seconds": mins,
                                    "max_seconds": ms,
                                    "min_price_buffer_pct": buf,
                                    "max_adverse_momentum": mom,
                                    "momentum_window": mw,
                                    "momentum_periods": mp,
                                    "max_realized_vol_pct": None,
                                    "vol_lookback": VOL_LOOKBACK,
                                    "order_mode": ORDER_MODE,
                                    "maker_bid_offset": m_off,
                                    "maker_timeout": m_to,
                                })
    return combos


# ── Data Helpers ──────────────────────────────────────────────────

def parse_strike(ticker: str) -> Optional[float]:
    m = re.search(r'-T([\d.]+)$', ticker)
    return float(m.group(1)) if m else None


def classify_ticker(ticker: str) -> tuple[str, str]:
    """Map a Kalshi ticker to (coin, market_type) for cell grouping.

    Kalshi's naming is inconsistent about what "D" means:
      - KXBTCD, KXETHD, KXSOLD, KXDOGED, KXXRPD, KXBNBD, KXHYPED
        are HOURLY markets that close on the hour (confirmed by
        live ticker close_time patterns like 04:00:00Z, 05:00:00Z, etc.)
      - KXSHIBAD is a true DAILY market that closes once per day
        at 5pm EDT (confirmed by user 2026-04-14)
    We treat SHIBA as a separate "daily" market_type so the cell
    name reflects reality (shiba_daily, not shiba_hourly) and so any
    future market-type-specific tuning is clean.

    Uses exact-match on the series component (everything before the
    first "-") to avoid prefix collisions like "KXSOLDATHOLD-..." being
    mis-classified as KXSOLD.
    """
    t = ticker.upper()
    series = t.split("-", 1)[0]  # "KXBTCD-26APR..." → "KXBTCD"
    if series == "KXSHIBAD":
        return "shiba", "daily"
    for coin in ["HYPE", "DOGE", "BTC", "ETH", "SOL", "XRP", "BNB"]:
        if series == f"KX{coin}15M":
            return coin.lower(), "15m"
        if series == f"KX{coin}D":
            return coin.lower(), "hourly"
    return "unknown", "unknown"


def get_price_at(prices: list[tuple[float, float]], ts: float) -> Optional[float]:
    if not prices:
        return None
    if ts <= prices[0][0]:
        return prices[0][1]
    if ts >= prices[-1][0]:
        return prices[-1][1]
    lo, hi = 0, len(prices) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if prices[mid][0] <= ts:
            lo = mid
        else:
            hi = mid - 1
    return prices[lo][1]


def compute_momentum(prices: list[tuple[float, float]], ts: float,
                     window: int = 60, periods: int = 5) -> Optional[float]:
    readings = []
    for i in range(periods):
        offset = i * window
        cur = get_price_at(prices, ts - offset)
        prev = get_price_at(prices, ts - offset - window)
        if cur and prev and prev > 0:
            readings.append(((cur - prev) / prev) * 100)
    return sum(readings) / len(readings) if readings else None


def compute_realized_vol(prices: list[tuple[float, float]], ts: float,
                         lookback: float = 300) -> Optional[float]:
    """Stddev of per-sample returns inside [ts - lookback, ts], in percent.

    Mirrors RTIFeed.volatility() so the optimizer's view of realized
    vol matches what the live bot sees at decision time.
    """
    if not prices:
        return None
    cutoff = ts - lookback
    # Binary-search left edge, then linear scan to ts (the lists are sorted)
    lo, hi = 0, len(prices)
    while lo < hi:
        mid = (lo + hi) // 2
        if prices[mid][0] < cutoff:
            lo = mid + 1
        else:
            hi = mid
    window = []
    for i in range(lo, len(prices)):
        if prices[i][0] > ts:
            break
        window.append(prices[i][1])
    if len(window) < 5:
        return None
    returns = []
    for i in range(1, len(window)):
        if window[i - 1] != 0:
            returns.append((window[i] - window[i - 1]) / window[i - 1])
    if len(returns) < 3:
        return None
    import statistics
    return statistics.stdev(returns) * 100


# ── Data Loading ──────────────────────────────────────────────────

def load_tick_windows(tick_dir: str) -> list[dict]:
    """Build market windows from tick CSVs (real-time bid/ask data).

    Files can be written with either space-separated or T-separated ISO
    timestamps depending on whoever wrote them; force-coerce both to a
    tz-aware datetime so the downstream sort doesn't blow up on mixed types.
    """
    print("  Loading tick data...")
    frames = []
    for f in sorted(Path(tick_dir).glob("*.csv")):
        print(f"    {f.name}...")
        # Tolerate schema drift (e.g. a file that was written first with
        # the 6-column header and later appended with the 7-column
        # `floor_strike` schema after a bot restart). Python engine +
        # on_bad_lines='skip' silently drops rows that don't match the
        # header, losing some recent ticks but preserving everything
        # before the schema change.
        try:
            df_one = pd.read_csv(f, engine="python", on_bad_lines="skip")
        except Exception as e:
            print(f"      skip {f.name}: {e}")
            continue
        if "timestamp" not in df_one.columns:
            continue
        df_one["timestamp"] = pd.to_datetime(df_one["timestamp"], utc=True, format="mixed")
        frames.append(df_one)
    if not frames:
        return []
    df = pd.concat(frames, ignore_index=True).sort_values("timestamp")

    windows = []
    for ticker, group in df.groupby("ticker"):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            continue

        group = group.sort_values("timestamp")
        rows = group.to_dict("records")
        if len(rows) < 2:
            continue

        # Settlement from final tick
        last_bid = rows[-1].get("yes_bid", 50)
        if last_bid >= 90:
            result = "yes"
        elif last_bid <= 10:
            result = "no"
        else:
            continue

        close_time = rows[-1]["timestamp"]
        if isinstance(close_time, str):
            close_time = pd.Timestamp(close_time)
        close_time += pd.Timedelta(seconds=5)

        # Strike resolution: prefer the tick-level floor_strike column
        # (present in CSVs written by the updated tick recorder), fall
        # back to parsing from the ticker suffix (works for daily/hourly
        # series but not 15M, which don't encode the strike in-ticker).
        # Older CSVs without the column land in the fallback path.
        strike = None
        for row in rows:
            rs = row.get("floor_strike")
            if rs is None or rs == "":
                continue
            try:
                rs_f = float(rs)
                if rs_f > 0:
                    strike = rs_f
                    break
            except (ValueError, TypeError):
                continue
        if strike is None:
            strike = parse_strike(ticker)

        windows.append({
            "ticker": ticker, "coin": coin, "market_type": mtype,
            "strike": strike, "result": result,
            "close_time": close_time, "ticks": rows,
        })

    return windows


def load_hf_trade_windows(hf_dir: str, max_markets: int = 10000) -> list[dict]:
    """Build market windows from the HuggingFace TrevorJS/kalshi-trades dataset.

    Layout (16 trade shards + 4 markets shards in `hf_dir`):
      trades-NNNN.parquet: [trade_id, ticker, count, yes_price, no_price,
        taker_side, created_time] — 154.5M rows total across 16 shards.
      markets-NNNN.parquet: [ticker, event_ticker, ..., yes_bid, yes_ask,
        no_bid, no_ask, last_price, ..., result, created_time, open_time,
        close_time] — 17.5M rows total across 4 shards.

    Unlike the misterrPink1 parquet we had on disk, this one has meaningful
    15M crypto coverage (~1.4M KXBTC15M, 184K KXETH15M, 49K KXSOL15M trades
    through Jan 2026) which our older parquet completely lacked.

    We use DuckDB to do the filtering + markets→trades join in-SQL rather
    than loading 5+ GB into pandas. After DuckDB returns the subsampled
    joined rows (~10K markets × ~100 trades avg = ~1M rows, ~100MB), we
    materialize into the same window dict format `preprocess_window`
    expects so the rest of the pipeline is unchanged.

    Limitations:
      - Trade prints have no separate bid/ask. We use yes_price for both
        bid and ask, so mid == trade-price. This matches how our existing
        `load_parquet_trade_windows` treats the misterrPink1 data.
      - Buffer/momentum features require historical crypto spot, which we
        don't have for 2021-2025. preprocess_window sets those to None and
        simulate_fast silently skips the checks. Price/secs/side gates
        still apply, which is enough to find real edge for hourly cells.
    """
    import duckdb

    trades_glob = os.path.join(hf_dir, "trades-*.parquet")
    markets_glob = os.path.join(hf_dir, "markets-*.parquet")

    shard_files = list(Path(hf_dir).glob("trades-*.parquet"))
    if not shard_files:
        print(f"  HF: no trades parquet found in {hf_dir}")
        return []

    print(f"  Loading HF dataset from {hf_dir}...")
    print(f"    trade shards: {len(shard_files)}")

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB';")

    # Step 1: find resolved crypto markets that ALSO have trades.
    #
    # Key gotcha: the HF markets table has ~435K KXBTCD tickers (every
    # strike at every hour) but only ~58K of them ever actually traded.
    # If we sample from all resolved markets, only ~8% of our sample
    # lands on a ticker with trades — we get tiny samples. Instead we
    # inner-join against the set of tickers that HAVE trade prints,
    # then subsample from that.
    #
    # KXBTCVSGOLD and KXSOLNASD are cross-asset markets (BTC vs gold,
    # SOL vs Nasdaq) — excluded. We also use exact series-prefix
    # matching instead of LIKE 'KXBTC%' to avoid pulling in future
    # Kalshi series that happen to start the same way.
    crypto_series_sql = """
        (
          starts_with(ticker, 'KXBTC15M-') OR starts_with(ticker, 'KXBTCD-')
          OR starts_with(ticker, 'KXETH15M-') OR starts_with(ticker, 'KXETHD-')
          OR starts_with(ticker, 'KXSOL15M-') OR starts_with(ticker, 'KXSOLD-')
          OR starts_with(ticker, 'KXDOGE15M-') OR starts_with(ticker, 'KXDOGED-')
          OR starts_with(ticker, 'KXXRP15M-') OR starts_with(ticker, 'KXXRPD-')
          OR starts_with(ticker, 'KXBNB15M-') OR starts_with(ticker, 'KXBNBD-')
          OR starts_with(ticker, 'KXHYPE15M-') OR starts_with(ticker, 'KXHYPED-')
          OR starts_with(ticker, 'KXSHIBAD-')
        )
    """

    print(f"    discovering tickers with trades...")
    t0 = time.time()
    con.execute(f"""
        CREATE TEMP TABLE traded_tickers AS
        SELECT DISTINCT ticker
        FROM read_parquet('{trades_glob}')
        WHERE {crypto_series_sql}
    """)
    n_traded = con.execute("SELECT COUNT(*) FROM traded_tickers").fetchone()[0]
    print(f"    {n_traded:,} unique crypto tickers have trades "
          f"({time.time() - t0:.1f}s)")

    print(f"    joining against resolved markets + subsampling...")
    t0 = time.time()
    con.execute(f"""
        CREATE TEMP TABLE sampled_markets AS
        WITH resolved AS (
            SELECT DISTINCT m.ticker, m.close_time, m.result
            FROM read_parquet('{markets_glob}') m
            INNER JOIN traded_tickers t ON m.ticker = t.ticker
            WHERE m.result IN ('yes', 'no')
              AND m.close_time IS NOT NULL
        )
        SELECT * FROM resolved
        ORDER BY hash(ticker)
        LIMIT {max_markets}
    """)
    n_markets = con.execute("SELECT COUNT(*) FROM sampled_markets").fetchone()[0]
    print(f"    sampled {n_markets:,} traded+resolved markets "
          f"(of {n_traded:,} with trades, max={max_markets:,}) "
          f"in {time.time() - t0:.1f}s")

    # Step 2: pull trades for those markets only, joined with close_time/result
    print(f"    loading trades for sampled markets...")
    t0 = time.time()
    rows = con.execute(f"""
        SELECT
          t.ticker,
          t.created_time,
          t.yes_price,
          t.count,
          s.close_time,
          s.result
        FROM read_parquet('{trades_glob}') t
        INNER JOIN sampled_markets s ON t.ticker = s.ticker
        ORDER BY t.ticker, t.created_time
    """).df()
    print(f"    loaded {len(rows):,} trades in {time.time() - t0:.1f}s")

    # Step 3: build windows grouped by ticker
    print(f"    building windows...")
    t0 = time.time()
    windows = []
    skipped_unknown = 0
    skipped_empty = 0

    for ticker, group in rows.groupby("ticker", sort=False):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            skipped_unknown += 1
            continue

        close_time = group.iloc[0]["close_time"]
        result = group.iloc[0]["result"]
        if close_time is None or pd.isna(close_time) or result not in ("yes", "no"):
            skipped_empty += 1
            continue
        if isinstance(close_time, str):
            close_time = pd.Timestamp(close_time)

        ticks = [{
            "timestamp": row["created_time"],
            "yes_bid": int(row["yes_price"]),
            "yes_ask": int(row["yes_price"]),  # no spread in trade prints
            "volume": int(row["count"]) if pd.notna(row["count"]) else 0,
        } for _, row in group.iterrows()]

        windows.append({
            "ticker": ticker,
            "coin": coin,
            "market_type": mtype,
            "strike": parse_strike(ticker),
            "result": result,
            "close_time": close_time,
            "ticks": ticks,
        })

    print(f"    built {len(windows):,} windows in {time.time() - t0:.1f}s "
          f"(skipped: {skipped_unknown} unknown-series, "
          f"{skipped_empty} empty/invalid)")
    return windows


def load_parquet_trade_windows(
    trades_path: str,
    markets_path: str,
    max_markets: int = 30000,
) -> list[dict]:
    """Build market windows from the historical trades + markets parquets.

    Data layout (from misterrPink1/prediction-market-analysis dataset):
      trades_path: crypto_trades_filtered.parquet — 6.8M trade prints with
        columns [trade_id, ticker, count, yes_price, no_price, taker_side,
        created_time, _fetched_at, asset]
      markets_path: crypto_markets_filtered.parquet — 1.85M market snapshots
        with [ticker, result, close_time, ...]. Exactly one row per ticker
        (scraped once post-settlement), which is why we use it only for
        settlement + close_time lookup, not as a source of ticks.

    For each ticker, we build a window whose "ticks" are the trade prints:
      - timestamp = trade created_time
      - yes_bid = yes_ask = trade yes_price (no spread info from a trade print)
      - volume = trade count

    Since trade prints have no bid/ask spread, the preprocess_window logic
    that treats yes_mid = (bid + ask) / 2 still works — the mid just equals
    the execution price. That means every trade in the 95-99c range counts
    as a candidate "we could have been the taker at this price" entry.

    Limitations:
      - No crypto spot coverage for 2024-2025 in our data/prices/ → buffer
        and momentum checks skip silently for these windows (preprocess_window
        sets buffer_pct=None when spot data is absent).
      - Only daily-series tickers (KX*D) are present; no 15M data.
    """
    print("  Loading parquet trades...")
    trades = pd.read_parquet(trades_path)
    print(f"    trades: {len(trades):,} rows")

    print("  Loading parquet markets (for close_time + result)...")
    markets = pd.read_parquet(markets_path, columns=["ticker", "close_time", "result"])
    # Dedupe on ticker (one row per market, but just in case)
    markets = markets.drop_duplicates(subset=["ticker"], keep="last")
    print(f"    markets: {len(markets):,} unique tickers")

    # Join trades with market metadata
    merged = trades.merge(markets, on="ticker", how="inner")
    merged = merged[merged["result"].isin(["yes", "no"])]
    print(f"    after join + result filter: {len(merged):,} trades across "
          f"{merged['ticker'].nunique():,} markets")

    # Subsample if we have too many markets (keeps runtime manageable)
    tickers = merged["ticker"].unique()
    if len(tickers) > max_markets:
        rng = np.random.default_rng(42)
        tickers = rng.choice(tickers, max_markets, replace=False)
        merged = merged[merged["ticker"].isin(tickers)]
        print(f"    subsampled to {max_markets:,} markets")

    windows = []
    skipped_unknown = 0
    for ticker, group in merged.groupby("ticker"):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            skipped_unknown += 1
            continue

        group = group.sort_values("created_time")
        close_time = group.iloc[0]["close_time"]
        if close_time is None or pd.isna(close_time):
            continue
        if isinstance(close_time, str):
            close_time = pd.Timestamp(close_time)

        ticks = [{
            "timestamp": row["created_time"],
            "yes_bid": row["yes_price"],
            "yes_ask": row["yes_price"],  # trade prints have no spread; mid==price
            "volume": row["count"],
        } for _, row in group.iterrows()]

        windows.append({
            "ticker": ticker,
            "coin": coin,
            "market_type": mtype,
            "strike": parse_strike(ticker),
            "result": group.iloc[0]["result"],
            "close_time": close_time,
            "ticks": ticks,
        })
    print(f"    built {len(windows):,} windows "
          f"(skipped {skipped_unknown} unknown-series tickers)")
    return windows


def load_parquet_windows(path: str, max_markets: int = 30000) -> list[dict]:
    """Build market windows from historical parquet snapshots."""
    print("  Loading parquet...")
    df = pd.read_parquet(path)

    prefixes = ("KXBTCD", "KXETHD", "KXSOLD", "KXDOGED", "KXXRPD",
                "KXBNBD", "KXHYPED", "KXSHIBAD")
    mask = df["ticker"].str.startswith(prefixes) & df["result"].isin(["yes", "no"])
    df = df[mask].copy()
    print(f"  Filtered: {len(df):,} rows")

    tickers = df["ticker"].unique()
    if len(tickers) > max_markets:
        tickers = np.random.choice(tickers, max_markets, replace=False)
        df = df[df["ticker"].isin(tickers)]

    windows = []
    for ticker, group in df.groupby("ticker"):
        coin, mtype = classify_ticker(ticker)
        if coin == "unknown":
            continue

        group = group.sort_values("_fetched_at")
        result = group.iloc[0]["result"]
        close_time = group.iloc[0].get("close_time")
        if close_time is not None:
            close_time = pd.Timestamp(close_time)

        ticks = [{
            "timestamp": row["_fetched_at"],
            "yes_bid": row.get("yes_bid", 50),
            "yes_ask": row.get("yes_ask", 50),
            "volume": row.get("volume", 0),
        } for _, row in group.iterrows()]

        windows.append({
            "ticker": ticker, "coin": coin, "market_type": mtype,
            "strike": parse_strike(ticker), "result": result,
            "close_time": close_time, "ticks": ticks,
        })

    return windows


SUPPORTED_COINS = ["btc", "eth", "sol", "doge", "xrp", "bnb", "hype"]


def load_crypto_prices(price_dir: str) -> dict[str, list[tuple[float, float]]]:
    """Load per-coin spot price history from data/prices/*.csv.

    Handles two schemas seamlessly:
      - legacy: timestamp,btc,eth,sol (files before the 7-coin rollout)
      - current: timestamp,btc,eth,sol,doge,xrp,bnb,hype

    Any coin column present in the row is picked up, so optimization for
    DOGE/XRP/BNB/HYPE works as soon as data has been collected for them.
    """
    prices = defaultdict(list)
    for f in sorted(Path(price_dir).glob("*.csv")):
        with open(f) as fh:
            for row in csv.DictReader(fh):
                try:
                    ts = datetime.fromisoformat(
                        row["timestamp"].replace("Z", "+00:00")).timestamp()
                except (ValueError, KeyError):
                    continue
                for coin in SUPPORTED_COINS:
                    val = row.get(coin)
                    if val in (None, ""):
                        continue
                    try:
                        prices[coin].append((ts, float(val)))
                    except ValueError:
                        pass
    for coin in prices:
        prices[coin].sort()
    return dict(prices)


# ── Simulation ────────────────────────────────────────────────────

def preprocess_window(window: dict, crypto_prices: dict) -> Optional[dict]:
    """
    Pre-extract all tradeable ticks from a window ONCE.
    Returns a compact dict with only the data the MC sweep needs.
    This avoids re-parsing timestamps and re-walking ticks for every candidate.
    """
    ticks = window["ticks"]
    result = window["result"]
    close_time = window["close_time"]
    strike = window.get("strike")
    coin = window["coin"]

    if close_time is None or not ticks:
        return None

    if isinstance(close_time, str):
        close_time = pd.Timestamp(close_time)
    if close_time.tzinfo is None:
        close_time = close_time.tz_localize("UTC")

    coin_prices = crypto_prices.get(coin, [])

    # Strike approximation for 15M markets. Kalshi's 15M binary contracts
    # ("BTC price up in next 15 mins?") don't expose the strike in the
    # ticker name, and the API wipes the floor_strike field after
    # settlement — so historical tick replay has no direct strike value.
    # The rules text clarifies the contract: "is the 60s average BRTI at
    # close >= the 60s average BRTI at open?". That means the effective
    # strike IS the spot price at market open (close_time - 15 min).
    # We approximate it from our 1-minute crypto spot history, accepting
    # ~30s of timing noise against the true 60s BRTI average. This
    # restores the buffer filter for 15M cells in CV, which was the
    # single biggest reason 15M windows couldn't be optimized.
    market_type = window.get("market_type", "")
    if strike is None and market_type == "15m" and coin_prices:
        open_ts = (close_time - pd.Timedelta(minutes=15)).timestamp()
        approx_strike = get_price_at(coin_prices, open_ts)
        if approx_strike and approx_strike > 0:
            strike = approx_strike

    # First pass: build a full (ts_epoch, yes_bid, yes_ask) tick stream,
    # sorted by time. Needed downstream for maker-order fill simulation —
    # a maker bid posted at tick i fills only if some later tick j (within
    # the maker timeout) has an ask ≤ our bid. We precompute the forward-
    # window min/max once per window so simulate_fast does O(1) lookups.
    raw_stream = []
    for tick in ticks:
        ts = tick["timestamp"]
        if isinstance(ts, str):
            ts = pd.Timestamp(ts)
        if hasattr(ts, 'tzinfo') and ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        yb = tick.get("yes_bid", 0)
        ya = tick.get("yes_ask", 0)
        if not yb or not ya:
            continue
        raw_stream.append((ts.timestamp(), int(yb), int(ya), ts))
    raw_stream.sort(key=lambda x: x[0])

    # Precompute forward-window min(yes_ask) and max(yes_bid) for each
    # maker timeout. For tick i, min_ask[t][i] = cheapest ask any time
    # in (ts[i], ts[i]+t]. Used by simulate_fast: maker YES bid at P
    # fills iff min_ask[t][i] ≤ P; maker NO bid at P fills iff
    # 100 - max_bid[t][i] ≤ P, i.e., max_bid[t][i] ≥ 100 - P.
    n_stream = len(raw_stream)
    maker_ask_by_timeout: dict[int, np.ndarray] = {}
    maker_bid_by_timeout: dict[int, np.ndarray] = {}
    if n_stream > 0:
        times = np.fromiter((r[0] for r in raw_stream), dtype=np.float64,
                            count=n_stream)
        bids_arr = np.fromiter((r[1] for r in raw_stream), dtype=np.int16,
                                count=n_stream)
        asks_arr = np.fromiter((r[2] for r in raw_stream), dtype=np.int16,
                                count=n_stream)
        for to in MAKER_TIMEOUTS:
            end_idx = np.searchsorted(times, times + to, side='right')
            ma = np.empty(n_stream, dtype=np.int16)
            mb = np.empty(n_stream, dtype=np.int16)
            for i in range(n_stream):
                j = int(end_idx[i])
                if j > i + 1:
                    ma[i] = asks_arr[i + 1:j].min()
                    mb[i] = bids_arr[i + 1:j].max()
                else:
                    # No future ticks in window — only an instant fill
                    # at the current price would count; we store the
                    # current-tick values so simulate_fast's "bid ≥ min
                    # ask" check behaves like "bid ≥ current ask" (i.e.
                    # only a taker-equivalent bid fills, no maker edge).
                    ma[i] = asks_arr[i]
                    mb[i] = bids_arr[i]
            maker_ask_by_timeout[to] = ma
            maker_bid_by_timeout[to] = mb

    # Second pass: build per-entry dicts, only keeping ticks that
    # plausibly look tradeable (94-99c, 10-500s left). Each entry
    # carries an index back into raw_stream so simulate_fast can
    # reach into the precomputed future windows.
    entries = []
    for i, (ts_epoch, yes_bid, yes_ask, ts) in enumerate(raw_stream):
        secs_left = (close_time - ts).total_seconds()
        if secs_left < 10 or secs_left > 500:
            continue

        yes_mid = (yes_bid + yes_ask) / 2
        no_mid = 100 - yes_mid

        if yes_mid >= 94:
            side, entry_price = "yes", yes_ask
        elif no_mid >= 94:
            side, entry_price = "no", 100 - yes_bid
        else:
            continue

        if entry_price < 94 or entry_price > 99:
            continue

        # Pre-compute buffer, momentum, and realized vol.
        # Buffer requires the strike, which only daily/hourly tickers
        # encode (via the -T<price> suffix). 15M tickers don't, so
        # buffer_pct stays None for those cells. Momentum and vol are
        # pure functions of the crypto spot time-series and don't need
        # the strike — we compute them whenever spot history is available,
        # which makes them usable features for 15M cells.
        buffer_pct = None
        momentum = None
        realized_vol = None
        if coin_prices:
            cp = get_price_at(coin_prices, ts_epoch)
            if cp:
                if strike and strike > 0:
                    buffer_pct = (cp - strike) / strike * 100
                # Pre-compute momentum for every (window, periods) combo
                # that the search space might ask for.
                momentum = {}
                for mw in [30, 60, 90, 120, 180, 300]:
                    for mp in [1, 3, 5, 7, 10]:
                        m = compute_momentum(coin_prices, ts_epoch, mw, mp)
                        if m is not None:
                            momentum[(mw, mp)] = m
                realized_vol = compute_realized_vol(coin_prices, ts_epoch, VOL_LOOKBACK)

        entries.append({
            "secs_left": secs_left,
            "side": side,
            "entry_price": entry_price,
            "fav_price": max(yes_mid, no_mid),
            "buffer_pct": buffer_pct,
            "momentum": momentum,
            "realized_vol": realized_vol,
            # Indexes into the window-level maker_{ask,bid}_by_timeout arrays.
            # Cheaper than copying 8 ints per entry.
            "stream_idx": i,
        })

    if not entries:
        return None

    return {
        "result": result,
        "entries": entries,
        "maker_ask_by_timeout": maker_ask_by_timeout,
        "maker_bid_by_timeout": maker_bid_by_timeout,
    }


def simulate_fast(preprocessed: dict, params: dict) -> Optional[dict]:
    """Fast simulation against pre-processed entries.
    Python loop with early-exit is net faster than numpy here — each
    window has only ~30-50 entries, and numpy's per-call overhead
    exceeds the cost of a Python short-circuiting scan at that size.

    Taker semantics (default) vs maker semantics (ORDER_MODE=maker) is
    selected by the `order_mode` param — taker fills at the observed
    ask, pays a fee, and the fill probability is scaled by the logistic
    model from fit_fill_rate. Maker fills at a bid posted
    `maker_bid_offset` below the ask, pays no fee, and fills only if
    the precomputed forward-window min-ask (for the candidate's
    `maker_timeout`) is at-or-below the bid — binary fill, no P(fill)
    scaling.
    """
    min_cp = params["min_contract_price"]
    max_ep = params["max_entry_price"]
    min_secs = params.get("min_seconds", 10)
    max_secs = params["max_seconds"]
    min_buffer = params["min_price_buffer_pct"]
    max_mom = params["max_adverse_momentum"]
    max_vol = params.get("max_realized_vol_pct")
    mom_key = (params.get("momentum_window", 60), params.get("momentum_periods", 5))
    order_mode = params.get("order_mode", "taker")
    maker_offset = params.get("maker_bid_offset", 0)
    maker_timeout = params.get("maker_timeout", 60)
    result = preprocessed["result"]

    for e in preprocessed["entries"]:
        if e["secs_left"] < min_secs or e["secs_left"] > max_secs:
            continue

        if max_vol is not None and e["realized_vol"] is not None:
            if e["realized_vol"] > max_vol:
                continue
        if e["fav_price"] < min_cp:
            continue
        if e["entry_price"] < min_cp or e["entry_price"] > max_ep:
            continue

        if e["buffer_pct"] is not None:
            time_scale = math.sqrt(max(1.0, e["secs_left"]) / 60.0)
            required = min_buffer * time_scale
            if e["side"] == "yes" and e["buffer_pct"] < required:
                continue
            if e["side"] == "no" and e["buffer_pct"] > -required:
                continue

        if max_mom < 0 and e["momentum"]:
            mom = e["momentum"].get(mom_key)
            if mom is not None:
                if e["side"] == "yes" and mom < max_mom:
                    continue
                if e["side"] == "no" and mom > -max_mom:
                    continue

        won = (e["side"] == result)

        if order_mode == "maker":
            # Post a bid `maker_offset` cents below the observed ask.
            # Fill only if the forward-window min-ask (YES side) or
            # min-no-ask (NO side, via max yes_bid) reaches our bid.
            bid_price = int(e["entry_price"]) - maker_offset
            if bid_price < 1:
                continue
            si = e["stream_idx"]
            if e["side"] == "yes":
                future_min_ask = preprocessed["maker_ask_by_timeout"][maker_timeout][si]
                filled = future_min_ask <= bid_price
            else:
                # NO side: our NO bid at P sits at YES-ask = 100-P.
                # Filled when someone posts a YES-bid ≥ 100-P, i.e.
                # max future yes_bid ≥ 100 - bid_price.
                future_max_bid = preprocessed["maker_bid_by_timeout"][maker_timeout][si]
                filled = future_max_bid >= (100 - bid_price)
            if not filled:
                return None  # order expired unfilled
            # Maker fills at OUR posted price, not the observed ask.
            exec_price = bid_price
            contracts = max(1, int(10.0 / (exec_price / 100.0)))
            stake = contracts * exec_price / 100.0
            # Zero fee + zero slippage on maker (we're the price-setter).
            if won:
                profit = contracts * (100 - exec_price) / 100.0
            else:
                profit = -contracts * exec_price / 100.0
            return {"won": won, "profit": profit, "stake": stake,
                    "p_fill": 1.0, "exec_price": exec_price}

        # --- Taker path (legacy) ---
        contracts = max(1, int(10.0 / (e["entry_price"] / 100.0)))
        stake = contracts * e["entry_price"] / 100.0
        slippage_per_contract = get_slippage_cents(int(e["entry_price"])) / 100.0
        slippage_cost = contracts * slippage_per_contract
        if won:
            profit = contracts * (100 - e["entry_price"]) / 100.0 - slippage_cost
        else:
            profit = -contracts * e["entry_price"] / 100.0 - slippage_cost
        # Expected-value scaling by fill probability. Narrow time
        # windows (low secs_left) get heavily penalized, which is the
        # whole point: the optimizer currently prefers max_seconds=60
        # because it can't see that half those orders die in IOC. A
        # cancelled order earns $0, not the simulated profit, so the
        # expected contribution is profit * P(fill). Unit stays the
        # same, so downstream aggregation (PPT, score) doesn't need
        # to know about the weighting.
        p_fill = get_fill_probability(int(e["entry_price"]), e["secs_left"])
        return {"won": won, "profit": profit * p_fill, "stake": stake,
                "p_fill": p_fill}

    return None


def evaluate_params(preprocessed_windows: list[dict], params: dict) -> dict:
    wins = losses = 0
    win_profit = 0.0
    loss_profit = 0.0
    total_stake = 0.0
    for pw in preprocessed_windows:
        r = simulate_fast(pw, params)
        if r is None:
            continue
        if r["won"]:
            wins += 1
            win_profit += r["profit"]
        else:
            losses += 1
            loss_profit += r["profit"]
        total_stake += r.get("stake", 0.0)
    trades = wins + losses
    if trades == 0:
        return {"score": 0, "win_rate": 0, "trades": 0, "profit": 0,
                "wins": 0, "losses": 0,
                "win_profit": 0.0, "loss_profit": 0.0, "total_stake": 0.0}
    wr = wins / trades
    total_profit = win_profit + loss_profit
    ppt = total_profit / trades
    norm_p = max(0, min(1, (ppt + 10) / 10.5))
    # Volume bonus — RISK_TOLERANCE>0 rewards higher trade counts to
    # counteract PPT's inherent volume-blindness. Kept separate from
    # norm_p so a cell with strong PPT isn't over-rewarded for also
    # having high volume (the scoring still caps at 1 via min()).
    norm_t = min(1.0, trades / TRADE_VOL_NORM) if TRADE_VOL_NORM > 0 else 0.0
    score = ALPHA * wr + (1 - ALPHA) * norm_p + RISK_TOLERANCE * norm_t
    return {
        "score": round(score, 6),
        "win_rate": round(wr, 4),
        "trades": trades, "profit": round(total_profit, 2),
        "wins": wins, "losses": losses,
        "win_profit": round(win_profit, 4),
        "loss_profit": round(loss_profit, 4),
        "total_stake": round(total_stake, 4),
    }


# ── Worker-pool machinery for parallelized candidate scoring ─────
#
# The candidate loop is embarrassingly parallel — each of the 5,700+
# candidates evaluates independently against the same preprocessed
# windows. We use multiprocessing.Pool with per-worker initializer to
# copy the (potentially MB-sized) preprocessed data ONCE per worker
# rather than pickling it per task. This gives ~4-8× speedup on a
# typical 8-core machine with zero behavioral change vs the single-
# threaded path.
_WORKER_PP_BY_BUCKET: list = []
_WORKER_TRAIN_SETS: list = []


def _cv_worker_init(pp_by_bucket, train_sets):
    """Pool initializer — stash the preprocessed data in module globals
    so each worker doesn't have to pickle/unpickle it for every task."""
    global _WORKER_PP_BY_BUCKET, _WORKER_TRAIN_SETS
    _WORKER_PP_BY_BUCKET = pp_by_bucket
    _WORKER_TRAIN_SETS = train_sets


def _cv_score_candidate(params: dict):
    """Evaluate one candidate across all walk-forward folds.

    Folds are ordered oldest-to-newest. Each fold trains on everything
    earlier than its validation window, which means the validation
    signal reflects "would this param have worked on the next chunk?"
    exactly as the live re-optimize workflow uses the data.

    Ranking rules:

    1. Per-fold minimum trade count. Every fold must have at least
       MIN_VAL_TRADES_PER_FOLD validation trades. Prevents candidates
       from "passing" CV by firing only in the richest fold while
       0-trading the rest.

    2. Aggregate minimum trade count. Across all folds, the candidate
       must have at least MIN_TOTAL_VAL_TRADES validation samples.
       Protects against "1 per fold × 2 folds" looking viable.

    3. Recency-weighted Wilson lower bound on win rate. Newer folds
       count more: the most recent fold has weight 1.0, earlier folds
       halve every RECENCY_HALFLIFE_FOLDS steps back. The Wilson LB is
       computed against the weighted win/total counts, which means
       a candidate that does well on stale data but poorly on recent
       folds is naturally downranked.

    4. Trade count (unweighted) is the tiebreaker — among candidates
       with similar Wilson LB, prefer the ones that fire more.

    The asymmetric-payoff "safe profit" is still computed and saved on
    the result for visibility; it does not affect ranking.
    """
    fold_results = []
    for b in range(len(_WORKER_TRAIN_SETS)):
        train_r = evaluate_params(_WORKER_TRAIN_SETS[b], params)
        val_r = evaluate_params(_WORKER_PP_BY_BUCKET[b], params)
        fold_results.append({
            "train": train_r, "val": val_r, "val_date": f"fold_{b}",
        })

    # Rule 1: require each fold to hit a minimum trade count.
    if any(f["val"]["trades"] < MIN_VAL_TRADES_PER_FOLD for f in fold_results):
        return None

    total_val_trades = sum(f["val"]["trades"] for f in fold_results)

    # Rule 2: aggregate sample-size floor.
    if total_val_trades < MIN_TOTAL_VAL_TRADES:
        return None

    total_val_wins = sum(f["val"]["wins"] for f in fold_results)
    total_val_losses = sum(f["val"]["losses"] for f in fold_results)
    total_val_win_profit = sum(f["val"]["win_profit"] for f in fold_results)
    total_val_loss_profit = sum(f["val"]["loss_profit"] for f in fold_results)
    total_val_stake = sum(f["val"]["total_stake"] for f in fold_results)
    total_train_trades = sum(f["train"]["trades"] for f in fold_results)
    val_wr = total_val_wins / total_val_trades if total_val_trades > 0 else 0

    train_wr_num = sum(
        f["train"]["win_rate"] * f["train"]["trades"]
        for f in fold_results if f["train"]["trades"] > 0
    )
    train_total = sum(f["train"]["trades"] for f in fold_results)
    train_wr = train_wr_num / train_total if train_total > 0 else 0

    total_val_profit = total_val_win_profit + total_val_loss_profit

    # Rule 3: recency-weighted Wilson lower bound on validation WR.
    # The last fold (index n-1) is the most recent and gets weight 1.0.
    # Folds farther in the past decay exponentially with half-life
    # RECENCY_HALFLIFE_FOLDS. This means a param that worked on days
    # 1-3 but not on days 7-9 will get a strong WR penalty.
    n_folds = len(fold_results)
    weights = [
        0.5 ** ((n_folds - 1 - i) / RECENCY_HALFLIFE_FOLDS)
        for i in range(n_folds)
    ]
    weighted_wins = sum(f["val"]["wins"] * w for f, w in zip(fold_results, weights))
    weighted_total = sum(f["val"]["trades"] * w for f, w in zip(fold_results, weights))
    # Wilson LB accepts floats; this becomes a weighted approximation
    # rather than a strict binomial bound, which is fine for ranking.
    wr_lb = wilson_lower_bound(int(round(weighted_wins)),
                               max(1, int(round(weighted_total))))

    # Bookkeeping: safe per-trade profit estimate using observed economics
    # and the Wilson LB WR. Not used for ranking — shown in rr_params.json
    # for transparency.
    avg_win = total_val_win_profit / total_val_wins if total_val_wins > 0 else 0.0
    if total_val_losses > 0:
        avg_loss = total_val_loss_profit / total_val_losses
    else:
        avg_stake = total_val_stake / total_val_trades if total_val_trades > 0 else 0.0
        avg_loss = -avg_stake
    safe_ppt = wr_lb * avg_win + (1 - wr_lb) * avg_loss

    # Compact fold summary for downstream code: flat numpy array,
    # columns [val_trades, val_profit, train_profit]. Matches the format
    # returned by optimize_rr_cuda.score_candidates — main() reads these
    # via numpy indexing, avoiding per-fold dict allocations that OOM'd
    # the GPU path at multi-million-candidate scale.
    compact_folds = np.empty((len(fold_results), 3), dtype=np.float64)
    for i, f in enumerate(fold_results):
        compact_folds[i, 0] = f["val"]["trades"]
        compact_folds[i, 1] = f["val"]["profit"]
        compact_folds[i, 2] = f["train"]["profit"]
    return (
        wr_lb, val_wr, total_val_losses, total_val_trades,
        train_wr, total_train_trades, params, compact_folds,
        safe_ppt, total_val_profit,
    )


# ── Optimization ──────────────────────────────────────────────────

def optimize_cell(name: str, train: list[dict], val: list[dict],
                  crypto_prices: dict, n_random: int = 3000) -> Optional[dict]:
    if not train:
        print(f"  {name}: no training data, skipping")
        return None
    print(f"  {name}: {len(train)} train, {len(val)} validation windows")

    # Pre-process: extract tradeable ticks ONCE (the expensive step)
    print(f"    Pre-processing training windows...")
    train_pp = [pw for w in train if (pw := preprocess_window(w, crypto_prices)) is not None]
    print(f"    {len(train_pp)} windows have tradeable ticks (95c+)")
    if not train_pp:
        print(f"  {name}: no tradeable ticks in training data")
        return None

    val_pp = [pw for w in val if (pw := preprocess_window(w, crypto_prices)) is not None]
    print(f"    {len(val_pp)} validation windows have tradeable ticks")

    candidates = grid_params()
    for _ in range(n_random):
        candidates.append(sample_params())

    # Phase 1: Training (fast — uses pre-processed data)
    scored = []
    for i, p in enumerate(candidates):
        if i > 0 and i % 1000 == 0:
            print(f"    {i}/{len(candidates)}...")
        r = evaluate_params(train_pp, p)
        if r["trades"] >= 3:
            scored.append((r["score"], r, p))
    scored.sort(key=lambda x: (-x[0], -x[1]["trades"]))

    if not scored:
        print(f"  {name}: no viable candidates")
        return None

    print(f"    {len(scored)} viable candidates (of {len(candidates)} tested)")

    # Phase 2: Walk-forward validation
    top_n = min(50, len(scored))
    print(f"    Validating top {top_n} on {len(val_pp)} windows...")

    best = None
    for _, train_r, p in scored[:top_n]:
        if not val_pp:
            best = {**p, "training_win_rate": train_r["win_rate"],
                    "training_trades": train_r["trades"],
                    "training_profit": train_r["profit"],
                    "validation_win_rate": None, "validation_trades": 0}
            break
        vr = evaluate_params(val_pp, p)
        if vr["trades"] == 0:
            continue
        if vr["losses"] == 0 and (best is None or vr["trades"] > best.get("validation_trades", 0)):
            best = {**p, "training_win_rate": train_r["win_rate"],
                    "training_trades": train_r["trades"],
                    "training_profit": train_r["profit"],
                    "validation_win_rate": vr["win_rate"],
                    "validation_trades": vr["trades"],
                    "validation_profit": vr["profit"]}

    # Fallback: best training score even if validation has losses
    if best is None and scored:
        _, train_r, p = scored[0]
        vr = evaluate_params(val_pp, p) if val_pp else {}
        best = {**p, "training_win_rate": train_r["win_rate"],
                "training_trades": train_r["trades"],
                "training_profit": train_r["profit"],
                "validation_win_rate": vr.get("win_rate"),
                "validation_trades": vr.get("trades", 0),
                "validation_profit": vr.get("profit", 0)}

    if best:
        print(f"    BEST: train WR={best['training_win_rate']:.1%} ({best['training_trades']}t), "
              f"val WR={best.get('validation_win_rate', 'N/A')} ({best.get('validation_trades', 0)}t)")
        print(f"    buffer={best['min_price_buffer_pct']}%, max_secs={best['max_seconds']}, "
              f"price={best['min_contract_price']}-{best['max_entry_price']}c, "
              f"momentum={best['max_adverse_momentum']}")
    return best


def main():
    t0 = time.time()
    # Python 3.14 defaults multiprocessing start method to 'forkserver',
    # which pickles init args through a pipe and can fail with
    # `_pickle.UnpicklingError: pickle data was truncated` when the
    # per-worker payload (pp_by_bucket + train_sets) gets large — we
    # hit this at ~4000 preprocessed windows × 10 folds on btc_hourly.
    # 'fork' copies memory on write, no pickling at init, no size limit.
    # Safe on Linux since this script doesn't share threads with the
    # children.
    try:
        multiprocessing.set_start_method("fork", force=True)
    except (RuntimeError, ValueError):
        pass  # already set, or not supported on this platform

    print("=" * 60)
    print("Resolution Rider Parameter Optimizer")
    print("=" * 60)

    print("\n[1/4] Loading data...")

    # Use tick data for everything — parquet snapshots are too sparse for RR simulation
    data_dir = os.getenv("DATA_DIR", "/mnt/d/datasets/prediction-market-analysis")

    # Load slippage model from Kalshi fill data
    global SLIPPAGE_MODEL
    kalshi_csv = os.path.join(data_dir, "from_kalshi", "Kalshi-Transactions-2026.csv")
    SLIPPAGE_MODEL = load_slippage_model(kalshi_csv)
    if SLIPPAGE_MODEL:
        print("  Slippage model (cents/contract):")
        for price, slip in sorted(SLIPPAGE_MODEL.items()):
            print(f"    {price}c entry -> {slip:.2f}c slippage")
    else:
        print("  No slippage model loaded, using 0.5c default")

    print(f"  Order mode: {ORDER_MODE.upper()}"
          + (f" (search: bid_offset {MAKER_OFFSETS}, timeout {MAKER_TIMEOUTS}s)"
             if ORDER_MODE == "maker" else ""))

    # Load fill-rate model (fit by fit_fill_rate.py from live_trades.csv).
    # When present, simulate_fast scales each candidate trade's profit by
    # P(fill) so tight-window strategies aren't rewarded for fills they
    # wouldn't actually get. Set DISABLE_FILL_MODEL=1 to ignore the
    # model and recover legacy perfect-fill behavior.
    global FILL_RATE_MODEL
    FILL_RATE_MODEL = load_fill_rate_model(data_dir)
    if FILL_RATE_MODEL and os.getenv("DISABLE_FILL_MODEL", "0") != "1":
        m = FILL_RATE_MODEL
        print(f"  Fill-rate model: n_fills={m.get('n_fills')} "
              f"n_cancels={m.get('n_cancels')} "
              f"clip=[{m.get('fill_min')}, {m.get('fill_max')}]")
        print(f"    P(fill) grid (clipped):")
        for pc in (92, 95, 97, 98):
            row = [f"    {pc}c:"]
            for s in (15, 30, 60, 120, 240):
                row.append(f"{s}s={get_fill_probability(pc, s):.2f}")
            print("  ".join(row))
    elif os.getenv("DISABLE_FILL_MODEL", "0") == "1":
        print("  Fill-rate model: DISABLED via env (using perfect-fill sim)")
    else:
        print("  No fill-rate model found — using perfect-fill sim "
              "(run fit_fill_rate.py to generate one)")
    tick_dir = os.path.join(data_dir, "ticks")
    all_tick_windows = load_tick_windows(tick_dir) if Path(tick_dir).exists() else []
    print(f"  Ticks: {len(all_tick_windows)} market windows from live recorder")

    # Load historical training data from disk. Preference order:
    #   1. HuggingFace TrevorJS/kalshi-trades (superset: 15M coverage + newer dates)
    #   2. misterrPink1/prediction-market-analysis parquet (legacy, hourly only)
    # Either contributes "historical" windows that get combined with the live
    # ticks. Control knobs:
    #   USE_HF=1/0           — prefer HF over legacy parquet (default 1)
    #   INCLUDE_HISTORICAL=0 — disable historical entirely, live-only run
    #   HF_DIR               — override HF dataset path
    #   PARQUET_MAX_MARKETS  — subsample cap for both HF and legacy loaders
    historical_windows: list = []
    if os.getenv("INCLUDE_HISTORICAL", "1") != "0":
        max_markets = int(os.getenv("PARQUET_MAX_MARKETS", "10000"))
        hf_dir = os.getenv("HF_DIR", "/mnt/d/datasets/hf_kalshi_trades")
        use_hf = os.getenv("USE_HF", "1") != "0"

        if use_hf and Path(hf_dir).exists() and any(Path(hf_dir).glob("trades-*.parquet")):
            try:
                historical_windows = load_hf_trade_windows(
                    hf_dir, max_markets=max_markets,
                )
                print(f"  HF: {len(historical_windows)} historical market windows "
                      f"from {hf_dir}")
            except Exception as e:
                print(f"  HF load FAILED: {e}")
                import traceback
                traceback.print_exc()
        else:
            # Fall back to misterrPink1 parquet (daily-only, older)
            trades_parquet = os.path.join(data_dir, "crypto_trades_filtered.parquet")
            markets_parquet = os.path.join(data_dir, "crypto_markets_filtered.parquet")
            if Path(trades_parquet).exists() and Path(markets_parquet).exists():
                try:
                    historical_windows = load_parquet_trade_windows(
                        trades_parquet, markets_parquet, max_markets=max_markets,
                    )
                    print(f"  Legacy parquet: {len(historical_windows)} windows")
                except Exception as e:
                    print(f"  Legacy parquet load FAILED: {e}")
            else:
                print(f"  No historical dataset found (looked at {hf_dir} and {trades_parquet})")

    # Load "best case" settled-market windows (from fetch_settled_data.py).
    # SKIP_SETTLED=1 excludes them — their synthetic 60s timestamp biases
    # time window selection. Use tick + HF data for time-aware optimization.
    settled_windows: list = []
    settled_path = Path("data/settled_windows.pkl")
    if os.getenv("SKIP_SETTLED", "0") == "1":
        print(f"  Settled API: skipped (SKIP_SETTLED=1)")
    elif settled_path.exists():
        import pickle
        with open(settled_path, "rb") as f:
            settled_windows = pickle.load(f)
        print(f"  Settled API: {len(settled_windows)} best-case windows from {settled_path}")
    else:
        print(f"  Settled API: not found ({settled_path}), run fetch_settled_data.py to add")

    all_windows = all_tick_windows + historical_windows + settled_windows
    n_settled = len(settled_windows)
    print(f"  TOTAL: {len(all_windows)} market windows "
          f"({len(all_tick_windows)} live + {len(historical_windows)} historical"
          f" + {n_settled} settled)")

    # Prefer high-frequency (5-second) spot data when present; the HF
    # path gives much cleaner strike approximation for 15M markets. Fall
    # back to the 1-minute file for dates where HF isn't recorded yet.
    # We merge both directories: the load_crypto_prices helper de-dupes
    # by (ticker, timestamp), so overlapping days resolve cleanly with
    # the finer samples taking precedence.
    # Resolve via data_paths so we pick up files regardless of whether
    # they still live under python-bot/data/ or have been migrated to
    # $DATA_DIR. all_candidates() returns both when both have files,
    # so dedupe below merges a half-migrated state cleanly.
    import data_paths
    hf_candidates = data_paths.all_candidates("prices_hf")
    coarse_candidates = data_paths.all_candidates("prices")

    crypto_prices: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for hf_dir in hf_candidates:
        print(f"  Loading HF (5s) crypto spot from {hf_dir}...")
        for coin, pts in load_crypto_prices(str(hf_dir)).items():
            crypto_prices[coin].extend(pts)
    for coarse_dir in coarse_candidates:
        print(f"  Loading 1-min crypto spot from {coarse_dir}...")
        for coin, pts in load_crypto_prices(str(coarse_dir)).items():
            crypto_prices[coin].extend(pts)
    # Sort + dedupe per coin
    for coin in crypto_prices:
        seen = set()
        deduped = []
        for ts, px in sorted(crypto_prices[coin]):
            # Keep the first (HF) when timestamps collide at the second
            key = round(ts)
            if key in seen:
                continue
            seen.add(key)
            deduped.append((ts, px))
        crypto_prices[coin] = deduped
    crypto_prices = dict(crypto_prices)
    for coin, p in crypto_prices.items():
        print(f"  {coin.upper()}: {len(p)} price points")

    # Group windows by cell for k-fold
    print("\n[2/4] Organizing by cell and date...")
    windows_by_cell = defaultdict(list)
    for w in all_windows:
        cell = f"{w['coin']}_{w['market_type']}"
        windows_by_cell[cell].append(w)

    all_cells = sorted(windows_by_cell.keys())
    print(f"  {len(all_cells)} cells: {', '.join(all_cells)}")

    # Eager global preprocessing. Raw windows carry a `ticks` list that
    # averages ~100KB/window for live-recorded 15M markets; with 197k
    # windows that's ~20GB of tick data sitting in windows_by_cell while
    # early cells run. Preprocessing up-front + dropping the raw ticks
    # leaves a ~10KB entries list per window (~2GB global), which is
    # the single biggest RAM reduction we can get without streaming the
    # dataset from disk. preprocess_window is ~fast (a few minutes for
    # 200k windows) vs. many hours lost to OOMs. pp is stashed on the
    # same dict so the per-cell loop can pick it up by identity.
    import gc
    print("  Pre-processing all windows globally (frees raw ticks)...")
    t_pp = time.time()
    n_tradeable = 0
    for cell in all_cells:
        for w in windows_by_cell[cell]:
            pp = preprocess_window(w, crypto_prices)
            if pp is not None:
                w["_pp"] = pp
                n_tradeable += 1
            # Raw ticks no longer needed — freed even if pp was None
            # so non-tradeable windows don't hog memory either.
            w.pop("ticks", None)
    # Drop the flat master list; windows_by_cell still holds everything.
    all_windows = None
    gc.collect()
    print(f"  {n_tradeable:,}/{sum(len(v) for v in windows_by_cell.values()):,} "
          f"have tradeable ticks (preprocessed in {time.time()-t_pp:.0f}s)")

    # Empirical safe-entry horizon per cell. Removes min_seconds /
    # max_seconds from the optimizer's search space: each cell gets a
    # data-driven max_seconds based on where historical correctness
    # drops below the configured threshold. Env knobs:
    #   HORIZON_MIN_PRICE   — entry-price floor for the analysis (¢)
    #   HORIZON_MIN_BUFFER  — favored-side buffer floor (%)
    #   HORIZON_THRESHOLD   — correctness bar (default 0.97)
    #   SKIP_HORIZON=1      — fall back to DEFAULT_MAX_SECONDS everywhere
    from analyze_safe_horizon import (
        compute_horizons, format_horizons_table, save_horizons,
    )
    print("\n  Computing per-cell safe-entry horizons...")
    if os.getenv("SKIP_HORIZON", "0") == "1":
        print("  SKIP_HORIZON=1 — using DEFAULT_MAX_SECONDS "
              f"({DEFAULT_MAX_SECONDS}s) for every cell")
        horizons = {cell: {
            "max_seconds": DEFAULT_MAX_SECONDS, "from_default": True,
            "n_samples": 0, "overall_correctness": None,
        } for cell in all_cells}
    else:
        horizons = compute_horizons(
            windows_by_cell,
            min_price=int(os.getenv("HORIZON_MIN_PRICE", "95")),
            min_buffer_pct=float(os.getenv("HORIZON_MIN_BUFFER", "0.10")),
            threshold=float(os.getenv("HORIZON_THRESHOLD", "0.85")),
            default_max_seconds=DEFAULT_MAX_SECONDS,
        )
        print()
        print(format_horizons_table(horizons))
        saved = save_horizons(horizons)
        print(f"  Wrote {saved}")

    # Get unique dates for k-fold (use the combined window set)
    all_dates = sorted(set(
        w["close_time"].strftime("%Y-%m-%d") if hasattr(w["close_time"], "strftime")
        else str(w["close_time"])[:10]
        for windows in windows_by_cell.values()
        for w in windows
    ))
    print(f"  {len(all_dates)} distinct dates (showing first/last 3): "
          f"{', '.join(all_dates[:3])} ... {', '.join(all_dates[-3:])}")

    print("\n[3/4] Optimizing (walk-forward cross-validation)...")
    results = {}
    for cell in all_cells:
        if CELL_FILTER and cell not in CELL_FILTER:
            print(f"\n--- {cell} --- (skipped by CELL_FILTER)")
            continue
        print(f"\n--- {cell} ---")
        # pop() (not []) so once this cell's tensors are on the GPU the
        # raw-tick memory for this cell can be reclaimed by the next gc —
        # otherwise windows_by_cell keeps ~20GB of ticks alive through
        # every cell and OOMs on large sweeps.
        cell_windows = windows_by_cell.pop(cell)

        # Tag each window with its date
        dated = []
        for w in cell_windows:
            d = (w["close_time"].strftime("%Y-%m-%d") if hasattr(w["close_time"], "strftime")
                 else str(w["close_time"])[:10])
            dated.append((d, w))
        # Raw windows list is now redundant with `dated` — drop to free.
        del cell_windows

        # Reuse the globally-preprocessed pp dicts (step 2/4). This skips
        # re-preprocessing and avoids pulling raw ticks back into memory.
        preprocessed = []
        for d, w in dated:
            pp = w.get("_pp")
            if pp is not None:
                pp["_date"] = d
                preprocessed.append(pp)
        print(f"  {len(preprocessed)} have tradeable ticks (cached)")

        if len(preprocessed) < 3:
            print(f"  {cell}: not enough data, skipping")
            continue

        # Generate candidates. Modes, in precedence order:
        #   PHASE=refine    → local search around an anchor (REFINE_FROM
        #                     file, default data/rr_params_preview.json).
        #   ONLY_LIVE=1     → one-shot eval of the live params for this
        #                     cell (apples-to-apples on new data).
        #   default         → full grid + random sweep.
        phase = os.environ.get("PHASE", "").strip().lower()
        if phase == "refine":
            refine_file = os.environ.get("REFINE_FROM", "data/rr_params_preview.json")
            try:
                _anchors = json.load(open(refine_file))
            except Exception as e:
                print(f"  PHASE=refine: couldn't load {refine_file}: {e}")
                continue
            anchor = _anchors.get(cell)
            if anchor is None:
                print(f"  {cell}: no anchor in {refine_file}, skipping")
                continue
            n_refine = int(os.environ.get("N_REFINE", "50000"))
            candidates = perturb_around(anchor, n_refine)
            print(f"  PHASE=refine: {n_refine} perturbations around anchor "
                  f"(buf={anchor['min_price_buffer_pct']}%, "
                  f"mom={anchor['max_adverse_momentum']}, "
                  f"mw{anchor.get('momentum_window')}/mp{anchor.get('momentum_periods')})")
        elif os.environ.get("ONLY_LIVE", "0") == "1":
            try:
                _live_all = json.load(open("data/rr_params.json"))
            except Exception as e:
                print(f"  ONLY_LIVE set but couldn't load rr_params.json: {e}")
                continue
            live_p = _live_all.get(cell)
            if live_p is None:
                print(f"  {cell}: no live entry, skipping")
                continue
            # Strip the non-param bookkeeping fields; keep only the tunable
            # dims the scorer needs.
            keep = {
                "min_contract_price", "max_entry_price",
                "min_seconds", "max_seconds",
                "min_price_buffer_pct", "max_adverse_momentum",
                "momentum_window", "momentum_periods",
                "max_realized_vol_pct", "vol_lookback",
            }
            candidates = [{k: live_p[k] for k in keep if k in live_p}]
            # Defaults for any missing optional fields.
            candidates[0].setdefault("min_seconds", 10)
            candidates[0].setdefault("vol_lookback", VOL_LOOKBACK)
            candidates[0].setdefault("max_realized_vol_pct", None)
        else:
            cell_max_secs = horizons.get(cell, {}).get("max_seconds", DEFAULT_MAX_SECONDS)
            print(f"    max_seconds cap: {cell_max_secs}s "
                  f"(from {'default' if horizons.get(cell, {}).get('from_default') else 'data'})")
            candidates = grid_params(max_seconds_cap=cell_max_secs)
            n_random = int(os.environ.get("N_RANDOM", "50000"))
            for _ in range(n_random):
                candidates.append(sample_params(max_seconds_cap=cell_max_secs))

        unique_dates = sorted(set(pp["_date"] for pp in preprocessed))
        N = len(unique_dates)

        if N < WF_MIN_DATES:
            # Not enough distinct dates for walk-forward. Fall back to a
            # train-only fit on everything, with no out-of-sample validation.
            # This matches the legacy "1 date" fallback behavior.
            print(f"  {cell}: only {N} dates (need ≥{WF_MIN_DATES}), fitting without CV")
            scored = []
            for i, p in enumerate(candidates):
                if i > 0 and i % 1000 == 0:
                    print(f"    {i}/{len(candidates)}...")
                r = evaluate_params(preprocessed, p)
                if r["trades"] >= 3:
                    scored.append((r["score"], r, p))
            if scored:
                scored.sort(key=lambda x: (-x[0], -x[1]["trades"]))
                _, best_r, best_p = scored[0]
                results[cell] = {
                    **best_p,
                    "training_win_rate": best_r["win_rate"],
                    "training_trades": best_r["trades"],
                    "training_profit": best_r["profit"],
                    "cv_folds": 0,
                    "cv_mean_win_rate": None,
                    "cv_total_val_trades": 0,
                }
                print(f"    BEST (no CV): WR={best_r['win_rate']:.1%} ({best_r['trades']}t)")
            continue

        # Walk-forward split. Carve the sorted dates into n_folds+1 contiguous
        # chunks. Fold k trains on chunks [0..k+1) and validates on chunk k+1.
        # Training grows monotonically; validation always comes AFTER training.
        # This is the methodologically correct setup for a re-optimize-every-
        # N-days workflow: it directly measures "if I had optimized at time T
        # using only past data, how well would the params perform on the next
        # chunk?"
        # n_folds scales with N so each chunk has ≥4 dates on average — this
        # keeps the very first fold's training window from being too small.
        n_folds = min(WF_MAX_FOLDS, max(2, N // 4))
        # +1 chunk so the first chunk is training-only; then n_folds validation
        # chunks follow. Last chunk absorbs leftover so no dates are dropped.
        chunk_count = n_folds + 1
        base_chunk_size = max(1, N // chunk_count)
        chunk_boundaries = [i * base_chunk_size for i in range(chunk_count)]
        chunk_boundaries.append(N)  # final boundary

        # Build walk-forward fold pairs.
        pp_by_date: dict[str, list] = defaultdict(list)
        for pp in preprocessed:
            pp_by_date[pp["_date"]].append(pp)

        pp_by_bucket: list[list[dict]] = []  # validation sets per fold
        train_sets: list[list[dict]] = []    # training sets per fold
        fold_val_ranges: list[str] = []
        for i in range(n_folds):
            train_end = chunk_boundaries[i + 1]
            val_end = chunk_boundaries[i + 2]
            train_dates_set = set(unique_dates[:train_end])
            val_dates_list = unique_dates[train_end:val_end]
            val_dates_set = set(val_dates_list)
            if not val_dates_list:
                continue
            train_pps = [pp for pp in preprocessed if pp["_date"] in train_dates_set]
            val_pps = [pp for pp in preprocessed if pp["_date"] in val_dates_set]
            train_sets.append(train_pps)
            pp_by_bucket.append(val_pps)
            fold_val_ranges.append(f"{val_dates_list[0]}..{val_dates_list[-1]}")

        n_buckets = len(pp_by_bucket)
        if n_buckets < 2:
            print(f"  {cell}: walk-forward produced <2 folds (N={N}), skipping")
            continue

        fold_window_counts = [len(b) for b in pp_by_bucket]
        fold_train_counts = [len(t) for t in train_sets]
        print(f"  Walk-forward: {n_buckets} folds from {N} dates")
        print(f"    train sizes:  {fold_train_counts}")
        print(f"    val sizes:    {fold_window_counts}")
        print(f"    val ranges:   {fold_val_ranges[0]} ... {fold_val_ranges[-1]}")

        # Parallel candidate scoring across CPU cores. Each worker gets
        # one pickled copy of pp_by_bucket + train_sets at startup (via
        # the initializer), then just receives lightweight params dicts
        # per task. Chunksize=50 balances IPC overhead against worker
        # starvation; on 5700 candidates × 10 folds, this gives ~4-8×
        # speedup over the serial loop.
        # Worker count: default cpu_count()-1. Smaller grids tolerate more
        # workers without OOM since worker-page drift scales with total
        # candidates processed. Override with N_WORKERS env to force a
        # specific count (useful when testing larger grids).
        backend = os.environ.get("BACKEND", "cpu").strip().lower()
        t_cv_start = time.time()
        candidate_scores = []

        if backend == "cuda":
            # GPU path — evaluate candidates in batches on the GPU. Single
            # process, no multiprocessing.Pool. Tensor build-up amortizes
            # across the whole candidate sweep for this cell.
            import optimize_rr_cuda as orc
            if not orc.is_available():
                print("  [cuda] torch.cuda not available, falling back to CPU")
                backend = "cpu"
            else:
                batch_size = int(os.environ.get("CUDA_BATCH_SIZE", "2048"))
                print(f"    [cuda] building cell tensors for {len(pp_by_bucket)} folds...")
                t_build = time.time()
                # Pass maker_timeouts only in maker mode so the CUDA
                # build materializes the forward-window ask/bid tensors.
                # Taker runs skip that memory + build-time cost.
                _mto = MAKER_TIMEOUTS if ORDER_MODE == "maker" else ()
                cell_tensors = orc.build_cell_tensors(
                    pp_by_bucket, train_sets, maker_timeouts=_mto,
                )
                print(f"    [cuda] built in {time.time() - t_build:.1f}s "
                      f"(E={cell_tensors['E']:,} entries, W={cell_tensors['W']:,} windows)")

                def _progress(done: int, total: int):
                    elapsed = time.time() - t_cv_start
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rate if rate > 0 else 0
                    print(f"    [cuda] {done}/{total}  ({rate:.0f}/s, ETA {eta:.0f}s)")

                # Drop CPU-side preprocessed dicts — the GPU tensors are
                # the source of truth for scoring. Holding 10-60k dicts
                # per cell in RAM was the 2nd biggest contributor to the
                # OOM on the 2.6M-candidate sweep.
                import gc
                pp_by_bucket.clear()
                train_sets.clear()
                preprocessed.clear()
                gc.collect()

                cuda_results = orc.score_candidates(
                    cell_tensors, candidates,
                    get_slippage_cents_fn=get_slippage_cents,
                    wilson_lower_bound_fn=wilson_lower_bound,
                    min_val_trades_per_fold=MIN_VAL_TRADES_PER_FOLD,
                    min_total_val_trades=MIN_TOTAL_VAL_TRADES,
                    recency_halflife_folds=RECENCY_HALFLIFE_FOLDS,
                    batch_size=batch_size,
                    progress_fn=_progress,
                )
                candidate_scores = [r for r in cuda_results if r is not None]
                # Free cell tensors + candidate list before next cell.
                del cell_tensors, cuda_results
                candidates.clear()
                import torch
                torch.cuda.empty_cache()
                gc.collect()

        if backend != "cuda":
            env_workers = os.environ.get("N_WORKERS", "").strip()
            if env_workers.isdigit() and int(env_workers) > 0:
                n_workers = int(env_workers)
            else:
                n_workers = max(1, (os.cpu_count() or 4) - 1)
            chunksize = max(10, len(candidates) // (n_workers * 20))
            print(f"    parallelizing {len(candidates)} candidates across {n_workers} workers "
                  f"(chunksize={chunksize})...")
            with multiprocessing.Pool(
                n_workers,
                initializer=_cv_worker_init,
                initargs=(pp_by_bucket, train_sets),
            ) as pool:
                for i, result in enumerate(pool.imap_unordered(
                    _cv_score_candidate, candidates, chunksize=chunksize,
                )):
                    if result is not None:
                        candidate_scores.append(result)
                    if (i + 1) % 1000 == 0:
                        elapsed = time.time() - t_cv_start
                        rate = (i + 1) / elapsed
                        eta = (len(candidates) - (i + 1)) / rate if rate > 0 else 0
                        print(f"    {i + 1}/{len(candidates)}  ({rate:.0f}/s, ETA {eta:.0f}s)")

        print(f"    CV done in {time.time() - t_cv_start:.0f}s "
              f"({len(candidate_scores)} viable candidates)")

        if not candidate_scores:
            print(f"  {cell}: no candidates with validation trades")
            continue

        # Profit-first scoring: maximize P&L, with WR bonus when
        # profitable and WR dampening when losing (high WR + negative
        # P&L means you're close to breakeven — better than low WR +
        # negative P&L). Low-trade candidates get discounted.
        def _composite(x):
            pnl = x[9]       # raw validation profit
            wr = x[1]        # raw validation win rate
            trades = x[3]    # validation trade count
            score = pnl
            # WR multiplier: amplify profit for high WR; dampen losses
            # for high WR (closer to breakeven = better losing candidate)
            if wr > 0.90:
                m = 1.0 + (wr - 0.90) * 5.0
                score = score * m if score > 0 else score / m
            # Low-trade discount
            if trades < 20:
                score *= trades / 20.0
            # Risk-tolerance volume bonus. RISK_TOLERANCE>0 scales
            # profit-positive candidates by (1 + RISK_TOLERANCE *
            # norm_trades), pushing the optimizer toward higher-volume
            # strategies among viable (positive-pnl) candidates. For
            # losing candidates it leaves `score` unchanged so we don't
            # accidentally reward losing-but-high-volume params.
            if score > 0 and RISK_TOLERANCE > 0 and TRADE_VOL_NORM > 0:
                norm_t = min(1.0, trades / TRADE_VOL_NORM)
                score *= (1.0 + RISK_TOLERANCE * norm_t)
            return score

        candidate_scores.sort(key=lambda x: -_composite(x))

        best = candidate_scores[0]
        (wr_lb, val_wr, val_losses, val_trades, train_wr, train_trades,
         p, folds, safe_ppt, _) = best
        comp_score = _composite(best)

        # `folds` is now a numpy (n_folds, 3) array: [val_trades, val_profit, train_profit]
        val_profit = float(folds[:, 1].sum())
        train_profit = float(folds[:, 2].sum())
        min_fold_trades = int(folds[:, 0].min())
        safe_total = safe_ppt * val_trades

        results[cell] = {
            **p,
            "training_win_rate": round(train_wr, 4),
            "training_trades": train_trades,
            "training_profit": round(train_profit, 2),
            "cv_folds": n_buckets,
            "cv_mean_win_rate": round(val_wr, 4),
            "cv_total_val_trades": val_trades,
            "cv_val_losses": val_losses,
            "cv_val_profit": round(val_profit, 2),
            "cv_wr_lower_bound": round(wr_lb, 4),
            "cv_safe_ppt": round(safe_ppt, 4),
            "cv_safe_total": round(safe_total, 2),
            "cv_min_fold_trades": min_fold_trades,
            "cv_composite_score": round(comp_score, 4),
        }

        vol_str = (f"vol≤{p['max_realized_vol_pct']}%" if p.get("max_realized_vol_pct") is not None
                   else "vol=off")
        print(f"    BEST: CV val WR={val_wr:.1%} (LB={wr_lb:.1%}) "
              f"({val_trades}t, {val_losses}L, min/fold={min_fold_trades}), "
              f"train WR={train_wr:.1%} ({train_trades}t)")
        print(f"    composite={comp_score:.4f}, raw val profit=${val_profit:.2f}, "
              f"safe per-trade=${safe_ppt:.3f}, safe total=${safe_total:.2f}")
        print(f"    buffer={p['min_price_buffer_pct']}%, "
              f"secs={p.get('min_seconds', 10)}-{p['max_seconds']}, "
              f"price={p['min_contract_price']}-{p['max_entry_price']}c, "
              f"momentum={p['max_adverse_momentum']} (w={p.get('momentum_window', 60)}s, "
              f"p={p.get('momentum_periods', 5)}), {vol_str}")

    print("\n[4/4] Saving...")
    phase = os.environ.get("PHASE", "").strip().lower()
    only_live_mode = os.environ.get("ONLY_LIVE", "0") == "1"
    preview_mode = os.environ.get("PREVIEW", "").strip() not in ("", "0", "false", "False")
    if phase == "refine":
        out = Path("data/rr_params_refined.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        print(f"  PHASE=refine — saving to {out} (compare to preview before deploying)")
    elif only_live_mode:
        out = Path("data/rr_params_live_on_new_data.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        print(f"  ONLY_LIVE mode — saving to {out} (not overwriting anything)")
    elif preview_mode:
        out = Path("data/rr_params_preview.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        print(f"  PREVIEW mode — live rr_params.json will NOT be touched")
    else:
        out = Path("data/rr_params.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        # Back up previous params with timestamp
        if out.exists():
            from datetime import datetime
            ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            backup = out.parent / f"rr_params_{ts}.json"
            import shutil
            shutil.copy2(out, backup)
            print(f"  Backed up previous params to {backup}")

    with open(out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  Saved to {out}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"{'=' * 60}")
    for cell, r in sorted(results.items()):
        print(f"  {cell:15s}: train={r['training_win_rate']:.0%} ({r['training_trades']}t) "
              f"val={r.get('validation_win_rate', 'N/A')} ({r.get('validation_trades', 0)}t) "
              f"buf={r['min_price_buffer_pct']}% max_s={r['max_seconds']}")


if __name__ == "__main__":
    main()
