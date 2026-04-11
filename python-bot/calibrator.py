"""
Confidence Calibrator for Kalshi Trading Bot

Maps raw strategy confidence scores to calibrated win probabilities
using historical trade data. Raw confidence is an arbitrary heuristic
(e.g. agreement_ratio * 0.7 + price_factor * 0.3) — NOT a calibrated
probability. Feeding uncalibrated scores into Kelly sizing causes
systematic over- or under-betting.

This module:
1. Loads settled trades from CSV files
2. Bins them by (strategy, confidence_bucket) or (strategy, price_bucket)
3. Computes empirical win rates per bin
4. Applies shrinkage toward 0.5 when data is sparse
5. Returns calibrated probabilities via linear interpolation
"""

import csv
import math
from pathlib import Path
from typing import Optional


class ConfidenceCalibrator:
    """
    Maps raw strategy confidence scores to empirical win probabilities.

    Uses historical trade outcomes to build per-strategy calibration
    curves. When confidence data is missing (old CSVs), falls back to
    price-based proxy buckets.
    """

    NUM_BINS = 10  # [0.0-0.1), [0.1-0.2), ..., [0.9-1.0]

    def __init__(
        self,
        csv_paths: list[str] = None,
        min_trades_per_bin: int = 10,
        shrinkage_weight: float = 0.5,
        cold_start_dampening: float = 0.6,
        min_strategy_trades: int = 20,
    ):
        """
        Args:
            csv_paths: Paths to trade CSV files to load.
            min_trades_per_bin: Below this, blend toward 0.5.
            shrinkage_weight: How much to pull toward 0.5 for sparse bins.
            cold_start_dampening: For strategies with < min_strategy_trades,
                apply: calibrated = 0.5 + (raw - 0.5) * dampening.
            min_strategy_trades: Threshold for cold-start dampening.
        """
        self.csv_paths = csv_paths or []
        self.min_trades_per_bin = min_trades_per_bin
        self.shrinkage_weight = shrinkage_weight
        self.cold_start_dampening = cold_start_dampening
        self.min_strategy_trades = min_strategy_trades

        # Per-strategy calibration: strategy -> {bin_index -> (wins, total)}
        self._confidence_bins: dict[str, dict[int, tuple[int, int]]] = {}
        # Fallback: price-based proxy bins: strategy -> {price_bucket -> (wins, total)}
        self._price_bins: dict[str, dict[int, tuple[int, int]]] = {}
        # Total trades per strategy
        self._strategy_totals: dict[str, int] = {}
        # Whether we have confidence data
        self._has_confidence_data = False

        self.load_historical()

    def load_historical(self) -> None:
        """Parse CSV trade data into calibration bins."""
        self._confidence_bins.clear()
        self._price_bins.clear()
        self._strategy_totals.clear()
        self._has_confidence_data = False

        for csv_path in self.csv_paths:
            path = Path(csv_path)
            if not path.exists():
                continue
            self._parse_csv(path)

    def _parse_csv(self, path: Path) -> None:
        """Parse a single CSV file into bins."""
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return

            has_confidence_col = "confidence" in reader.fieldnames

            # First pass: index entry rows by ticker for confidence lookup
            rows = list(reader)
            entry_by_ticker: dict[str, dict] = {}
            for row in rows:
                reason = row.get("reason", "")
                if not reason.startswith("SETTLED:") and row.get("outcome", "").strip() == "":
                    ticker = row.get("ticker", "")
                    if ticker:
                        entry_by_ticker[ticker] = row

            # Second pass: use SETTLED rows (they have the outcomes)
            for row in rows:
                outcome = row.get("outcome", "").strip()
                if outcome not in ("win", "loss"):
                    continue

                # Use settlement rows — they're the ones with actual outcomes
                reason = row.get("reason", "")
                if not reason.startswith("SETTLED:"):
                    # This is an entry row that somehow has an outcome
                    # (future format where entry rows get updated).
                    # Skip if we already have a SETTLED row for this ticker
                    # to avoid double-counting.
                    ticker = row.get("ticker", "")
                    settled_exists = any(
                        r.get("ticker") == ticker and r.get("reason", "").startswith("SETTLED:")
                        for r in rows
                    )
                    if settled_exists:
                        continue

                strategy = row.get("strategy", "unknown")
                is_win = outcome == "win"

                # Get confidence from entry row if not on this row
                ticker = row.get("ticker", "")
                entry = entry_by_ticker.get(ticker, {})

                # Track totals
                self._strategy_totals[strategy] = self._strategy_totals.get(strategy, 0) + 1

                # Confidence-based bins (preferred)
                # Check this row first, then fall back to entry row
                if has_confidence_col:
                    conf_str = (row.get("confidence", "") or "").strip()
                    if not conf_str:
                        conf_str = (entry.get("confidence", "") or "").strip()
                    if conf_str:
                        try:
                            conf = float(conf_str)
                            self._has_confidence_data = True
                            bin_idx = self._to_bin(conf)
                            if strategy not in self._confidence_bins:
                                self._confidence_bins[strategy] = {}
                            wins, total = self._confidence_bins[strategy].get(bin_idx, (0, 0))
                            self._confidence_bins[strategy][bin_idx] = (
                                wins + (1 if is_win else 0),
                                total + 1,
                            )
                        except ValueError:
                            pass

                # Price-based proxy bins (always computed as fallback)
                price_str = row.get("price_cents", "").strip()
                if price_str:
                    try:
                        price = int(price_str)
                        # Normalize price to [0, 1] range for binning
                        price_normalized = price / 100.0
                        bin_idx = self._to_bin(price_normalized)
                        if strategy not in self._price_bins:
                            self._price_bins[strategy] = {}
                        wins, total = self._price_bins[strategy].get(bin_idx, (0, 0))
                        self._price_bins[strategy][bin_idx] = (
                            wins + (1 if is_win else 0),
                            total + 1,
                        )
                    except ValueError:
                        pass

    def _to_bin(self, value: float) -> int:
        """Map a [0, 1] value to a bin index [0, NUM_BINS-1]."""
        value = max(0.0, min(1.0, value))
        bin_idx = int(value * self.NUM_BINS)
        return min(bin_idx, self.NUM_BINS - 1)

    def _bin_midpoint(self, bin_idx: int) -> float:
        """Midpoint of a bin."""
        return (bin_idx + 0.5) / self.NUM_BINS

    def calibrate(self, strategy_name: str, raw_confidence: float) -> float:
        """
        Map a raw confidence score to a calibrated win probability.

        Uses confidence-based bins if available, falls back to price-based
        proxy, then to cold-start dampening.

        Returns: Calibrated probability in [0.05, 0.95].
        """
        raw_confidence = max(0.0, min(1.0, raw_confidence))

        # Check if strategy has enough data for calibration
        total_trades = self._strategy_totals.get(strategy_name, 0)
        if total_trades < self.min_strategy_trades:
            # Cold start: dampen toward 0.5
            calibrated = 0.5 + (raw_confidence - 0.5) * self.cold_start_dampening
            return max(0.05, min(0.95, calibrated))

        # Try confidence-based calibration first
        if self._has_confidence_data and strategy_name in self._confidence_bins:
            result = self._interpolate(
                raw_confidence, self._confidence_bins[strategy_name]
            )
            if result is not None:
                return max(0.05, min(0.95, result))

        # Fall back to price-based proxy
        if strategy_name in self._price_bins:
            result = self._interpolate(
                raw_confidence, self._price_bins[strategy_name]
            )
            if result is not None:
                return max(0.05, min(0.95, result))

        # No data at all: dampen
        calibrated = 0.5 + (raw_confidence - 0.5) * self.cold_start_dampening
        return max(0.05, min(0.95, calibrated))

    def _interpolate(
        self, value: float, bins: dict[int, tuple[int, int]]
    ) -> Optional[float]:
        """
        Interpolate calibrated probability from bin data.

        For each bin, compute empirical win rate with shrinkage.
        Then linearly interpolate between the two nearest bin midpoints.
        """
        if not bins:
            return None

        # Build calibrated win rate for each populated bin
        calibrated_points: list[tuple[float, float]] = []
        for bin_idx in sorted(bins.keys()):
            wins, total = bins[bin_idx]
            midpoint = self._bin_midpoint(bin_idx)

            if total >= self.min_trades_per_bin:
                # Enough data: use empirical rate with light shrinkage
                empirical_wr = wins / total
                # Still apply some shrinkage for robustness
                light_shrinkage = 0.1
                calibrated_wr = (1 - light_shrinkage) * empirical_wr + light_shrinkage * 0.5
            else:
                # Sparse: heavier shrinkage toward 0.5
                empirical_wr = wins / total if total > 0 else 0.5
                calibrated_wr = (
                    self.shrinkage_weight * 0.5
                    + (1 - self.shrinkage_weight) * empirical_wr
                )
            calibrated_points.append((midpoint, calibrated_wr))

        if not calibrated_points:
            return None

        if len(calibrated_points) == 1:
            return calibrated_points[0][1]

        # Linear interpolation
        # Find the two bracketing points
        for i in range(len(calibrated_points) - 1):
            x0, y0 = calibrated_points[i]
            x1, y1 = calibrated_points[i + 1]
            if x0 <= value <= x1:
                if x1 == x0:
                    return y0
                t = (value - x0) / (x1 - x0)
                return y0 + t * (y1 - y0)

        # Extrapolate from nearest edge
        if value <= calibrated_points[0][0]:
            return calibrated_points[0][1]
        return calibrated_points[-1][1]

    def get_strategy_stats(self) -> dict[str, dict]:
        """Return summary stats for each strategy's calibration data."""
        stats = {}
        for strategy, total in self._strategy_totals.items():
            bins = self._confidence_bins.get(strategy, self._price_bins.get(strategy, {}))
            populated_bins = sum(1 for _, (w, t) in bins.items() if t > 0)
            stats[strategy] = {
                "total_trades": total,
                "populated_bins": populated_bins,
                "has_confidence_data": strategy in self._confidence_bins,
                "calibration_ready": total >= self.min_strategy_trades,
            }
        return stats
