"""
Performance Metrics for Kalshi Trading Bot

Computes standard risk-adjusted performance metrics from trade history:
Sharpe ratio, Sortino ratio, max drawdown, profit factor, Calmar ratio.

Can be fed trades incrementally during a live session or loaded from
historical CSV data.
"""

import csv
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PerformanceMetrics:
    """Snapshot of performance metrics."""
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    total_pnl: float = 0.0
    profit_factor: float = 0.0       # sum(wins) / abs(sum(losses))
    expectancy: float = 0.0          # average P&L per trade
    sharpe_ratio: float = 0.0        # risk-adjusted return
    sortino_ratio: float = 0.0       # downside-risk-adjusted return
    max_drawdown_usd: float = 0.0    # peak-to-trough drawdown
    max_drawdown_pct: float = 0.0    # drawdown as % of peak equity
    calmar_ratio: float = 0.0        # total return / max drawdown


class PerformanceTracker:
    """
    Tracks trade-by-trade performance and computes risk-adjusted metrics.

    Maintains an equity curve and running statistics. Can compute
    per-strategy metrics when given filtered trade lists.
    """

    def __init__(self, initial_balance: float = 0.0):
        self._returns: list[float] = []          # per-trade P&L
        self._equity_curve: list[float] = [initial_balance]  # cumulative
        self._timestamps: list[float] = []
        self._peak_equity: float = initial_balance
        self._max_drawdown: float = 0.0
        self._initial_balance = initial_balance

    def record(self, profit_usd: float, timestamp: float = 0.0) -> None:
        """Record a single trade's P&L."""
        self._returns.append(profit_usd)
        self._timestamps.append(timestamp)

        new_equity = self._equity_curve[-1] + profit_usd
        self._equity_curve.append(new_equity)

        # Update peak and drawdown
        if new_equity > self._peak_equity:
            self._peak_equity = new_equity
        drawdown = self._peak_equity - new_equity
        if drawdown > self._max_drawdown:
            self._max_drawdown = drawdown

    def compute(self) -> PerformanceMetrics:
        """Compute all performance metrics from recorded trades."""
        if not self._returns:
            return PerformanceMetrics()

        wins = [r for r in self._returns if r > 0]
        losses = [r for r in self._returns if r <= 0]
        n = len(self._returns)

        total_pnl = sum(self._returns)
        win_count = len(wins)
        loss_count = len(losses)
        win_rate = win_count / n if n > 0 else 0.0

        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        sum_wins = sum(wins)
        sum_losses = abs(sum(losses))
        profit_factor = sum_wins / sum_losses if sum_losses > 0 else float("inf") if sum_wins > 0 else 0.0

        expectancy = total_pnl / n if n > 0 else 0.0

        # Sharpe ratio: mean(returns) / std(returns)
        # For trade-based (not time-based), we don't annualize
        sharpe = self._compute_sharpe(self._returns)

        # Sortino: mean(returns) / downside_deviation
        sortino = self._compute_sortino(self._returns)

        # Max drawdown
        max_dd_pct = 0.0
        if self._peak_equity > 0:
            max_dd_pct = (self._max_drawdown / self._peak_equity) * 100

        # Calmar: total return / max drawdown
        calmar = 0.0
        if self._max_drawdown > 0:
            calmar = total_pnl / self._max_drawdown

        return PerformanceMetrics(
            total_trades=n,
            wins=win_count,
            losses=loss_count,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            total_pnl=total_pnl,
            profit_factor=profit_factor,
            expectancy=expectancy,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown_usd=self._max_drawdown,
            max_drawdown_pct=max_dd_pct,
            calmar_ratio=calmar,
        )

    @staticmethod
    def compute_from_returns(returns: list[float]) -> PerformanceMetrics:
        """Compute metrics from a list of per-trade returns."""
        tracker = PerformanceTracker()
        for r in returns:
            tracker.record(r)
        return tracker.compute()

    @staticmethod
    def from_csv(csv_path: str) -> "PerformanceTracker":
        """Load historical trades from a CSV and build a tracker."""
        tracker = PerformanceTracker()
        path = Path(csv_path)
        if not path.exists():
            return tracker

        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                outcome = row.get("outcome", "").strip()
                if outcome not in ("win", "loss"):
                    continue
                reason = row.get("reason", "")
                if reason.startswith("SETTLED:"):
                    continue
                try:
                    profit = float(row.get("profit_usd", 0))
                    tracker.record(profit)
                except (ValueError, TypeError):
                    continue
        return tracker

    @staticmethod
    def _compute_sharpe(returns: list[float]) -> float:
        """Sharpe ratio from a list of returns."""
        if len(returns) < 2:
            return 0.0
        mean_r = sum(returns) / len(returns)
        var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        std = math.sqrt(var) if var > 0 else 0.0
        if std == 0:
            return 0.0
        return mean_r / std

    @staticmethod
    def _compute_sortino(returns: list[float]) -> float:
        """Sortino ratio: mean / downside deviation."""
        if len(returns) < 2:
            return 0.0
        mean_r = sum(returns) / len(returns)
        downside = [r for r in returns if r < 0]
        if not downside:
            return float("inf") if mean_r > 0 else 0.0
        downside_var = sum(r ** 2 for r in downside) / len(downside)
        downside_std = math.sqrt(downside_var) if downside_var > 0 else 0.0
        if downside_std == 0:
            return 0.0
        return mean_r / downside_std

    def summary_str(self) -> str:
        """Human-readable summary."""
        m = self.compute()
        if m.total_trades == 0:
            return "No completed trades"
        lines = [
            f"Trades: {m.total_trades} ({m.wins}W/{m.losses}L, {m.win_rate:.0%} WR)",
            f"P&L: ${m.total_pnl:+.2f} | Avg Win: ${m.avg_win:+.2f} | Avg Loss: ${m.avg_loss:+.2f}",
            f"Profit Factor: {m.profit_factor:.2f} | Expectancy: ${m.expectancy:+.2f}/trade",
            f"Sharpe: {m.sharpe_ratio:.2f} | Sortino: {m.sortino_ratio:.2f}",
            f"Max Drawdown: ${m.max_drawdown_usd:.2f} ({m.max_drawdown_pct:.1f}%) | Calmar: {m.calmar_ratio:.2f}",
        ]
        return "\n".join(lines)
