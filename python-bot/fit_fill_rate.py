"""Fit a logistic fill-rate model from live_trades.csv.

The optimizer's simulate_fast currently assumes every gate-pass becomes a
fill at the posted price. In reality, orders placed at 95-98c with
immediate_or_cancel frequently die before filling — especially in the
final seconds of a market where we have the tightest current gates.

This script fits

    P(fill) = sigmoid(a + b*price_cents + c*secs_left)

from the live trade log and writes the coefficients to
data/fill_rate_model.json so simulate_fast can scale per-trade profit by
realistic fill probability instead of assuming 1.0.

Why logistic (not an empirical 2D table): with ~200 fills + ~35 clean
cancels, any 2D bucket grid is mostly empty cells. Logistic gives us a
smooth surface that degrades gracefully at the edges of the training
distribution rather than jumping to 0 or 1 on single observations.

Why two features: price and secs_left are the two knobs the optimizer
is searching over that plausibly affect fill rate. Side (YES vs NO) is
symmetric on Kalshi's unified book. Market type (15M vs daily) could
matter but n=35 cancels across two buckets would overfit.

Output (data/fill_rate_model.json):
  { "a": <intercept>, "b": <price_coef>, "c": <secs_coef>,
    "fill_min": 0.30, "fill_max": 1.00,
    "n_fills": <int>, "n_cancels": <int>,
    "price_range": [min, max], "secs_range": [min, max],
    "fitted_at": <iso timestamp> }

Notes on extrapolation: we clip P(fill) to [0.30, 1.00] so the
optimizer can't exploit made-up high-fill regions outside our training
distribution. The lower bound matches the observed empirical fill rate
at the worst bucket; anything lower in simulation would overstate the
penalty for regions we've never tried.
"""

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from data_paths import resolve

# Kalshi's market tickers encode settlement time in US Eastern, not UTC.
# Verified against live fills on 2026-04-20: KXBNB15M-26APR200415-15
# settled at 08:15Z which is 04:15 ET.
_KALSHI_TZ = ZoneInfo("America/New_York")

LIVE_TRADES_CSV = resolve("live_trades.csv")
OUT_PATH = resolve("fill_rate_model.json")

# Only orders at price >= this are modeled — RR exclusively trades in
# this zone and lower-priced rows are noise from reconciliation imports.
MIN_MODEL_PRICE = 80

# Clip applied to simulate_fast. Lower bound prevents the optimizer
# from treating unseen (low-price, high-secs) regions as total duds.
FILL_MIN = 0.30
FILL_MAX = 1.00

MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Matches the date portion of a Kalshi ticker after the series prefix:
#   15M markets: YYMMMDDHHMM  (11 chars) — e.g. 26APR200415
#   Daily/hrly:  YYMMMDDHH    (9 chars)  — e.g. 26APR2015
_DATE_15M_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})(\d{2})(\d{2})(?:-|$)")
_DATE_DAILY_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})(\d{2})(?:-|$)")
_SECS_LEFT_RE = re.compile(r"\((\d+)\s*s?\s*left\)")
_CANCEL_AFTER_RE = re.compile(r"CANCELLED: unfilled after (\d+)s")


def parse_close_time(ticker: str) -> datetime | None:
    """Extract the market's close_time from the ticker string.

    Tries the 11-char 15M-style date first because its prefix (YYMMMDD
    then HHMM) is a superset of the 9-char daily prefix and the daily
    regex would otherwise match the truncated form.
    """
    if "15M" in ticker:
        m = _DATE_15M_RE.search(ticker)
        if m:
            yy, mmm, dd, hh, mn = m.groups()
            try:
                local = datetime(
                    2000 + int(yy), MONTHS[mmm], int(dd),
                    int(hh), int(mn), tzinfo=_KALSHI_TZ,
                )
                return local.astimezone(timezone.utc)
            except (KeyError, ValueError):
                return None
        return None
    m = _DATE_DAILY_RE.search(ticker)
    if m:
        yy, mmm, dd, hh = m.groups()
        try:
            local = datetime(
                2000 + int(yy), MONTHS[mmm], int(dd),
                int(hh), 0, tzinfo=_KALSHI_TZ,
            )
            return local.astimezone(timezone.utc)
        except (KeyError, ValueError):
            return None
    return None


def parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def extract_samples(path: Path) -> list[dict]:
    """Group rows by order_id and emit (price, secs_left, filled) samples.

    An order counts as FILLED if any of its rows has outcome in
    {win, loss} — this includes the "raced-cancel" case where the bot
    sent a cancel but Kalshi matched first (51 of 86 observed cancels).

    An order counts as NOT FILLED only if every row has empty outcome
    AND at least one row's reason is a CANCELLED message. Orders still
    in flight (no settle, no cancel) are dropped.

    secs_left is preferred from a "(Xs left)" reason in any row (this
    is what the bot logs at submission). For cancel-only rows that
    overwrote the submit reason (TradeLogger.upsert_entry behavior),
    fall back to `close_time(ticker) - (row_time - cancel_timeout)`.
    """
    rows_by_order: dict[str, list[dict]] = {}
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            oid = (row.get("order_id") or "").strip()
            if not oid:
                continue
            rows_by_order.setdefault(oid, []).append(row)

    samples: list[dict] = []
    for oid, rows in rows_by_order.items():
        # Skip reconciliation imports — they don't have a submission-
        # time price gate that maps to a real fill decision.
        if any((r.get("reason") or "").startswith("Imported from Kalshi API")
               for r in rows):
            continue

        try:
            price = int(rows[0].get("price_cents") or 0)
        except ValueError:
            continue
        if price < MIN_MODEL_PRICE:
            continue

        outcomes = [(r.get("outcome") or "").strip() for r in rows]
        reasons = [r.get("reason") or "" for r in rows]
        filled = any(o in ("win", "loss") for o in outcomes)
        had_cancel = any("CANCELLED" in reason for reason in reasons)

        if not filled and not had_cancel:
            continue

        secs_left: int | None = None
        for reason in reasons:
            m = _SECS_LEFT_RE.search(reason)
            if m:
                secs_left = int(m.group(1))
                break

        if secs_left is None:
            # Fallback: reconstruct submit_time from cancel_timeout.
            close_time = parse_close_time(rows[0].get("ticker", ""))
            cancel_row = next((r for r in rows
                               if "CANCELLED" in (r.get("reason") or "")),
                              None)
            if close_time and cancel_row:
                cancel_ts = parse_iso(cancel_row.get("time", ""))
                m = _CANCEL_AFTER_RE.search(cancel_row.get("reason", ""))
                if cancel_ts and m:
                    submit_ts = cancel_ts.timestamp() - int(m.group(1))
                    secs_left = int(close_time.timestamp() - submit_ts)

        if secs_left is None or secs_left < 0 or secs_left > 1800:
            # 1800s cap: wider-window entries exist in theory but not
            # in current live data; a bad parse shouldn't pollute the fit.
            continue

        samples.append({
            "order_id": oid,
            "price": price,
            "secs_left": secs_left,
            "filled": 1 if filled else 0,
        })
    return samples


def fit_logistic(X: np.ndarray, y: np.ndarray, *, l2: float = 1e-3,
                 max_iter: int = 200, tol: float = 1e-8) -> np.ndarray:
    """IRLS-style Newton-Raphson logistic regression with L2 prior.

    Implementing this by hand (no scipy) keeps the optimizer's runtime
    dependency surface tiny. L2 = 1e-3 is a weak prior that keeps
    coefficients finite when a feature perfectly separates a tiny
    corner of the distribution (common at low n).
    """
    n, d = X.shape
    w = np.zeros(d)
    I = np.eye(d) * l2
    for _ in range(max_iter):
        z = X @ w
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -50, 50)))
        W = p * (1 - p)
        grad = X.T @ (p - y) + l2 * w
        H = X.T @ (X * W[:, None]) + I
        try:
            step = np.linalg.solve(H, grad)
        except np.linalg.LinAlgError:
            break
        w_new = w - step
        if np.max(np.abs(w_new - w)) < tol:
            w = w_new
            break
        w = w_new
    return w


def main() -> None:
    path = Path(LIVE_TRADES_CSV)
    if not path.exists():
        raise SystemExit(f"live_trades.csv not found at {path}")

    samples = extract_samples(path)
    if not samples:
        raise SystemExit("no usable samples extracted — check live_trades schema")

    prices = np.array([s["price"] for s in samples], dtype=float)
    secs = np.array([s["secs_left"] for s in samples], dtype=float)
    y = np.array([s["filled"] for s in samples], dtype=float)

    n_fills = int(y.sum())
    n_cancels = int(len(y) - n_fills)
    print(f"Samples: {len(samples)}  (filled={n_fills}, cancelled={n_cancels})")
    print(f"  price range: [{prices.min():.0f}, {prices.max():.0f}]¢")
    print(f"  secs range:  [{secs.min():.0f}, {secs.max():.0f}]s")

    # Feature matrix: intercept + price + secs_left. Scale features so
    # the Newton step conditions nicely even when price ~95 and secs ~45.
    price_scale = 100.0
    secs_scale = 60.0
    X = np.column_stack([
        np.ones_like(prices),
        prices / price_scale,
        secs / secs_scale,
    ])
    w = fit_logistic(X, y)
    a = float(w[0])
    b = float(w[1] / price_scale)
    c = float(w[2] / secs_scale)
    print(f"Coefficients (unscaled): a={a:+.4f}  b={b:+.6f} /c  c={c:+.6f} /s")

    # Sanity probes
    def p_hat(price_c: float, secs_left: float) -> float:
        logit = a + b * price_c + c * secs_left
        return float(1.0 / (1.0 + np.exp(-np.clip(logit, -50, 50))))

    print("Sanity grid (P(fill) before clip):")
    for pc in (92, 95, 97, 98):
        row = [f"{pc}¢:"]
        for s in (15, 30, 60, 120, 240):
            row.append(f"{s}s={p_hat(pc, s):.2f}")
        print("  " + "  ".join(row))

    out = {
        "a": a, "b": b, "c": c,
        "fill_min": FILL_MIN, "fill_max": FILL_MAX,
        "n_fills": n_fills, "n_cancels": n_cancels,
        "price_range": [float(prices.min()), float(prices.max())],
        "secs_range": [float(secs.min()), float(secs.max())],
        "fitted_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(OUT_PATH).write_text(json.dumps(out, indent=2))
    print(f"\nWrote {OUT_PATH}")


if __name__ == "__main__":
    main()
