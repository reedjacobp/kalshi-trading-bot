"""
Per-Strategy P&L Tracker with Auto-Suspension

Maintains live per-strategy statistics and automatically suspends
strategies that are underperforming. Prevents a single bad strategy
from draining the account while others are profitable.

Suspension triggers (any one):
1. Rolling P&L over last N trades < threshold
2. N+ consecutive losses
3. Drawdown from peak strategy P&L > threshold
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StrategyStats:
    """Live statistics for a single strategy."""
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    peak_pnl: float = 0.0
    max_drawdown: float = 0.0
    consecutive_losses: int = 0
    suspended_until: float = 0.0
    suspension_count: int = 0
    last_suspension_reason: str = ""
    rolling_pnl: deque = field(default_factory=lambda: deque(maxlen=10))

    @property
    def win_rate(self) -> Optional[float]:
        if self.total_trades == 0:
            return None
        return self.wins / self.total_trades

    @property
    def current_drawdown(self) -> float:
        return self.peak_pnl - self.total_pnl

    @property
    def rolling_sum(self) -> float:
        return sum(self.rolling_pnl)


class StrategyTracker:
    """
    Tracks per-strategy performance and suspends underperformers.

    Suspension is temporary — strategies are re-enabled after a cooldown
    period. This prevents permanently disabling a strategy that had a
    bad streak but is fundamentally sound.
    """

    def __init__(
        self,
        rolling_window: int = 10,
        suspension_threshold: float = -5.0,
        cooldown_seconds: int = 1800,
        max_consecutive_losses: int = 5,
        max_drawdown: float = 10.0,
    ):
        """
        Args:
            rolling_window: Number of recent trades for rolling P&L.
            suspension_threshold: Suspend if rolling P&L drops below this.
            cooldown_seconds: How long to suspend (default 30 min).
            max_consecutive_losses: Suspend after this many losses in a row.
            max_drawdown: Suspend if drawdown from peak exceeds this.
        """
        self.rolling_window = rolling_window
        self.suspension_threshold = suspension_threshold
        self.cooldown_seconds = cooldown_seconds
        self.max_consecutive_losses = max_consecutive_losses
        self.max_drawdown = max_drawdown
        self._stats: dict[str, StrategyStats] = {}

    def _get_stats(self, strategy_name: str) -> StrategyStats:
        if strategy_name not in self._stats:
            self._stats[strategy_name] = StrategyStats(
                rolling_pnl=deque(maxlen=self.rolling_window)
            )
        return self._stats[strategy_name]

    def record_outcome(self, strategy_name: str, profit_usd: float) -> None:
        """Record a trade outcome and check suspension triggers."""
        stats = self._get_stats(strategy_name)
        stats.total_trades += 1
        stats.total_pnl += profit_usd
        stats.rolling_pnl.append(profit_usd)

        if profit_usd > 0:
            stats.wins += 1
            stats.consecutive_losses = 0
        else:
            stats.losses += 1
            stats.consecutive_losses += 1

        # Update peak and drawdown
        if stats.total_pnl > stats.peak_pnl:
            stats.peak_pnl = stats.total_pnl
        drawdown = stats.peak_pnl - stats.total_pnl
        if drawdown > stats.max_drawdown:
            stats.max_drawdown = drawdown

        # Check suspension triggers
        self._check_suspension(strategy_name, stats)

    def _check_suspension(self, strategy_name: str, stats: StrategyStats) -> None:
        """Check if a strategy should be suspended."""
        now = time.time()

        # Already suspended?
        if stats.suspended_until > now:
            return

        reason = None

        # Trigger 1: Rolling P&L below threshold
        # Check once we have at least 3 trades (don't wait for full window)
        if len(stats.rolling_pnl) >= 3:
            if stats.rolling_sum < self.suspension_threshold:
                reason = f"Rolling P&L ${stats.rolling_sum:.2f} < ${self.suspension_threshold:.2f}"

        # Trigger 2: Too many consecutive losses
        if stats.consecutive_losses >= self.max_consecutive_losses:
            reason = f"{stats.consecutive_losses} consecutive losses"

        # Trigger 3: Drawdown from peak too large
        if stats.current_drawdown > self.max_drawdown:
            reason = f"Drawdown ${stats.current_drawdown:.2f} > ${self.max_drawdown:.2f}"

        if reason:
            stats.suspended_until = now + self.cooldown_seconds
            stats.suspension_count += 1
            stats.last_suspension_reason = reason

    def is_suspended(self, strategy_name: str) -> tuple[bool, str]:
        """
        Check if a strategy is currently suspended.

        Returns:
            (is_suspended: bool, reason: str)
        """
        stats = self._stats.get(strategy_name)
        if stats is None:
            return False, ""

        now = time.time()
        if stats.suspended_until > now:
            remaining = stats.suspended_until - now
            return True, f"{stats.last_suspension_reason} ({remaining:.0f}s remaining)"

        return False, ""

    def get_stats(self, strategy_name: str) -> StrategyStats:
        return self._get_stats(strategy_name)

    def all_stats(self) -> dict[str, StrategyStats]:
        return dict(self._stats)

    def summary_dict(self) -> dict[str, dict]:
        """Return serializable summary for dashboard/JSON."""
        result = {}
        for name, stats in self._stats.items():
            suspended, reason = self.is_suspended(name)
            result[name] = {
                "total_trades": stats.total_trades,
                "wins": stats.wins,
                "losses": stats.losses,
                "win_rate": round(stats.win_rate, 3) if stats.win_rate is not None else None,
                "total_pnl": round(stats.total_pnl, 2),
                "rolling_pnl": round(stats.rolling_sum, 2),
                "max_drawdown": round(stats.max_drawdown, 2),
                "consecutive_losses": stats.consecutive_losses,
                "suspended": suspended,
                "suspension_count": stats.suspension_count,
                "suspension_reason": reason if suspended else "",
            }
        return result
