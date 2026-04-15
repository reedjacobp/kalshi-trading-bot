"""
Adaptive Strategy Matrix — Auto-Enable/Disable per (Asset, Strategy)

Maintains a rolling performance matrix where each cell tracks how well
a specific strategy performs on a specific asset. Cells that underperform
are automatically disabled; cells that recover in shadow mode are
re-enabled.

The matrix replaces manual toggles and static regime filters with a
data-driven switchboard that adapts to market conditions continuously.

Metrics:
  - edge: rolling P&L / rolling staked (return on risk)
  - Computed over a trade-count window (default 20 trades)
  - Disable threshold: edge < -5%
  - Re-enable threshold: edge > +2% (in shadow mode)
  - Extended disable: if disabled 3+ of last 7 days, require higher
    re-enable threshold (+5%) to avoid flickering
"""

import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("kalshi_bot")


@dataclass
class TradeResult:
    """A single trade outcome for the matrix."""
    timestamp: float
    pnl: float
    stake: float
    outcome: str  # "win" or "loss"


@dataclass
class CellState:
    """State of a single (asset, strategy) cell in the matrix."""
    # Rolling window of real trades
    trades: deque = field(default_factory=lambda: deque(maxlen=30))
    # Shadow trades (tracked while disabled, not executed)
    shadow_trades: deque = field(default_factory=lambda: deque(maxlen=30))
    # Status — starts DISABLED, must prove itself in shadow mode first
    enabled: bool = False
    has_ever_been_enabled: bool = False  # First enable has a lower bar
    disabled_at: Optional[float] = None
    enabled_at: Optional[float] = None
    # Track which days this cell was disabled (for extended-disable logic)
    disabled_days: deque = field(default_factory=lambda: deque(maxlen=7))
    # Hard disable — matrix can never re-enable automatically
    hard_disabled: bool = False
    # Counters
    total_trades: int = 0
    total_shadow_trades: int = 0

    @property
    def edge(self) -> Optional[float]:
        """Rolling edge: total P&L / total staked in the window."""
        if len(self.trades) < 3:
            return None
        total_pnl = sum(t.pnl for t in self.trades)
        total_staked = sum(t.stake for t in self.trades)
        if total_staked == 0:
            return None
        return total_pnl / total_staked

    @property
    def shadow_edge(self) -> Optional[float]:
        """Edge computed from shadow (hypothetical) trades."""
        if len(self.shadow_trades) < 3:
            return None
        total_pnl = sum(t.pnl for t in self.shadow_trades)
        total_staked = sum(t.stake for t in self.shadow_trades)
        if total_staked == 0:
            return None
        return total_pnl / total_staked

    @property
    def win_rate(self) -> Optional[float]:
        if not self.trades:
            return None
        wins = sum(1 for t in self.trades if t.outcome == "win")
        return wins / len(self.trades)

    @property
    def recent_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    @property
    def days_disabled_last_week(self) -> int:
        """How many of the last 7 days this cell was disabled."""
        return len(self.disabled_days)


class StrategyMatrix:
    """
    Adaptive switchboard for (asset, strategy) combinations.

    Tracks rolling performance per cell and automatically disables
    underperforming combinations. Disabled cells are shadow-tracked
    and re-enabled when performance recovers.
    """

    def __init__(
        self,
        window_size: int = 20,              # Trades in rolling window
        disable_threshold: float = -0.05,   # Edge below this → disable
        first_enable_threshold: float = 0.01,  # Shadow edge to enable for first time (low bar)
        enable_threshold: float = 0.02,     # Shadow edge to re-enable after disable
        extended_disable_threshold: float = 0.05,  # Higher bar if disabled 3+/7 days
        first_enable_min_trades: int = 8,   # Min shadow trades for first enable
        min_trades_to_judge: int = 5,       # Need this many trades before disabling
        cooldown_seconds: float = 300,      # Min time between enable/disable toggles
        persist_path: Optional[str] = None, # Path to persist state across restarts
        strategy_overrides: Optional[dict] = None,  # Per-strategy threshold overrides
        allowed_assets: Optional[list[str]] = None,  # Whitelist of asset keys
        allowed_strategies: Optional[list[str]] = None,  # Whitelist of strategy names
    ):
        self.window_size = window_size
        self.disable_threshold = disable_threshold
        self.first_enable_threshold = first_enable_threshold
        self.enable_threshold = enable_threshold
        self.extended_disable_threshold = extended_disable_threshold
        self.first_enable_min_trades = first_enable_min_trades
        self.min_trades_to_judge = min_trades_to_judge
        self.cooldown_seconds = cooldown_seconds
        self.persist_path = persist_path
        self.strategy_overrides = strategy_overrides or {}
        # Whitelists prevent the matrix from accumulating stale cells from
        # deleted strategies or "unknown" tickers. Any (asset, strategy)
        # tuple outside the whitelists is silently dropped at every
        # entry point — load, record, force, query.
        self._allowed_assets = set(allowed_assets) if allowed_assets else None
        self._allowed_strategies = set(allowed_strategies) if allowed_strategies else None

        # The matrix: (asset_key, strategy_name) -> CellState
        self._cells: dict[tuple[str, str], CellState] = defaultdict(CellState)

        # Load persisted state if available
        if persist_path:
            self._load_state()

    def _is_allowed(self, asset: str, strategy: str) -> bool:
        if self._allowed_assets is not None and asset not in self._allowed_assets:
            return False
        if self._allowed_strategies is not None and strategy not in self._allowed_strategies:
            return False
        return True

    def _cell_key(self, asset: str, strategy: str) -> tuple[str, str]:
        return (asset, strategy)

    def initialize_cells(self, assets: list[str], strategies: list[str]):
        """Pre-create all (asset, strategy) cells so the matrix shows up
        in the dashboard immediately, even before any trades occur."""
        created = 0
        for asset in assets:
            for strategy in strategies:
                if not self._is_allowed(asset, strategy):
                    continue
                key = self._cell_key(asset, strategy)
                if key not in self._cells:
                    self._cells[key]  # defaultdict creates with enabled=False
                created += 1
        logger.info(f"[MATRIX] Initialized {created} cells (all shadow mode)")

    def force_enable(self, asset: str, strategy: str, clear_history: bool = False):
        """Pre-enable a cell based on external evidence (e.g. walk-forward backtest).
        The cell can still be disabled later if live performance is poor.
        If clear_history=True, wipe the rolling trade window (stale data from old config)."""
        if not self._is_allowed(asset, strategy):
            return
        key = self._cell_key(asset, strategy)
        cell = self._cells[key]
        cell.enabled = True
        cell.has_ever_been_enabled = True
        cell.enabled_at = time.time()
        if clear_history:
            cell.trades.clear()
            cell.shadow_trades.clear()

    def force_disable(self, asset: str, strategy: str, hard: bool = False):
        """Force-disable a cell. It must re-prove itself via shadow trading.
        If hard=True, the matrix can never re-enable it automatically."""
        if not self._is_allowed(asset, strategy):
            return
        key = self._cell_key(asset, strategy)
        cell = self._cells[key]
        cell.enabled = False
        cell.disabled_at = time.time()
        cell.shadow_trades.clear()
        if hard:
            cell.hard_disabled = True
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if not cell.disabled_days or cell.disabled_days[-1] != today:
            cell.disabled_days.append(today)
        mode = "HARD" if hard else "FORCE"
        logger.info(f"  [MATRIX] {mode} DISABLED {asset}/{strategy}")

    def is_enabled(self, asset: str, strategy: str) -> bool:
        """Check if a (asset, strategy) combination is currently enabled."""
        if not self._is_allowed(asset, strategy):
            return False
        key = self._cell_key(asset, strategy)
        if key not in self._cells:
            # First time seeing this cell — create it disabled, start shadow tracking
            self._cells[key]  # defaultdict creates it with enabled=False
        return self._cells[key].enabled

    def record_trade(self, asset: str, strategy: str, pnl: float,
                     stake: float, outcome: str):
        """Record a completed trade outcome for a cell."""
        if not self._is_allowed(asset, strategy):
            return
        key = self._cell_key(asset, strategy)
        cell = self._cells[key]

        result = TradeResult(
            timestamp=time.time(),
            pnl=pnl,
            stake=stake,
            outcome=outcome,
        )

        if cell.enabled:
            cell.trades.append(result)
            cell.total_trades += 1
        else:
            # Cell is disabled — this is a shadow trade
            cell.shadow_trades.append(result)
            cell.total_shadow_trades += 1

        # Check if state should change
        self._evaluate_cell(key, cell)
        self._persist_state()

    def record_shadow_trade(self, asset: str, strategy: str, pnl: float,
                            stake: float, outcome: str):
        """Record a hypothetical trade that would have been taken if enabled."""
        if not self._is_allowed(asset, strategy):
            return
        key = self._cell_key(asset, strategy)
        cell = self._cells[key]

        result = TradeResult(
            timestamp=time.time(),
            pnl=pnl,
            stake=stake,
            outcome=outcome,
        )
        cell.shadow_trades.append(result)
        cell.total_shadow_trades += 1

        # Check if should re-enable
        self._evaluate_cell(key, cell)
        self._persist_state()

    def _get_params(self, strategy: str) -> dict:
        """Get effective thresholds for a strategy, with per-strategy overrides."""
        overrides = self.strategy_overrides.get(strategy, {})
        return {
            "disable_threshold": overrides.get("disable_threshold", self.disable_threshold),
            "enable_threshold": overrides.get("enable_threshold", self.enable_threshold),
            "first_enable_threshold": overrides.get("first_enable_threshold", self.first_enable_threshold),
            "first_enable_min_trades": overrides.get("first_enable_min_trades", self.first_enable_min_trades),
            "min_trades_to_judge": overrides.get("min_trades_to_judge", self.min_trades_to_judge),
            "extended_disable_threshold": overrides.get("extended_disable_threshold", self.extended_disable_threshold),
        }

    def _evaluate_cell(self, key: tuple, cell: CellState):
        """Check if a cell should be disabled or re-enabled."""
        now = time.time()
        params = self._get_params(key[1])  # key[1] = strategy name

        # Cooldown: don't toggle too fast
        last_toggle = max(cell.disabled_at or 0, cell.enabled_at or 0)
        if now - last_toggle < self.cooldown_seconds:
            return

        if cell.enabled:
            # Check if should DISABLE
            edge = cell.edge
            n_trades = len(cell.trades)
            # Fast disable: catastrophic edge (<-50%) after just 3 trades
            # Normal disable: edge below threshold after min_trades_to_judge
            should_disable = False
            if edge is not None and n_trades >= 3 and edge < -0.50:
                should_disable = True  # Obvious failure, don't wait
            elif edge is not None and n_trades >= params["min_trades_to_judge"]:
                if edge < params["disable_threshold"]:
                    should_disable = True
            if should_disable:
                    cell.enabled = False
                    cell.disabled_at = now
                    # Record this day as a disabled day
                    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    if not cell.disabled_days or cell.disabled_days[-1] != today:
                        cell.disabled_days.append(today)
                    # Clear shadow trades for fresh tracking
                    cell.shadow_trades.clear()
                    logger.info(
                        f"  [MATRIX] DISABLED {key[0]}/{key[1]}: "
                        f"edge={edge:+.1%} over {len(cell.trades)} trades"
                    )
                    self._persist_state()
        else:
            # Hard-disabled cells can never be re-enabled automatically
            if cell.hard_disabled:
                return

            # Check if should ENABLE (first time or re-enable)
            shadow_edge = cell.shadow_edge

            if not cell.has_ever_been_enabled:
                # FIRST ENABLE — lower bar, fewer trades needed
                # "Show me you're not random"
                if (shadow_edge is not None
                        and len(cell.shadow_trades) >= params["first_enable_min_trades"]
                        and shadow_edge > params["first_enable_threshold"]):
                    cell.enabled = True
                    cell.has_ever_been_enabled = True
                    cell.enabled_at = now
                    cell.trades.clear()
                    for st in cell.shadow_trades:
                        cell.trades.append(st)
                    cell.shadow_trades.clear()
                    logger.info(
                        f"  [MATRIX] FIRST ENABLE {key[0]}/{key[1]}: "
                        f"shadow_edge={shadow_edge:+.1%} over "
                        f"{len(cell.trades)} shadow trades"
                    )
                    self._persist_state()
            else:
                # RE-ENABLE — higher bar, previously failed
                if shadow_edge is not None:
                    threshold = params["enable_threshold"]
                    if cell.days_disabled_last_week >= 3:
                        threshold = params["extended_disable_threshold"]

                    if shadow_edge > threshold:
                        cell.enabled = True
                        cell.enabled_at = now
                        cell.trades.clear()
                        for st in cell.shadow_trades:
                            cell.trades.append(st)
                        cell.shadow_trades.clear()
                        logger.info(
                            f"  [MATRIX] RE-ENABLED {key[0]}/{key[1]}: "
                            f"shadow_edge={shadow_edge:+.1%} over "
                            f"{cell.total_shadow_trades} shadow trades "
                            f"(threshold={threshold:+.1%})"
                        )
                        self._persist_state()

    def get_matrix_snapshot(self) -> list[dict]:
        """Get the full matrix state for dashboard display."""
        snapshot = []
        for (asset, strategy), cell in sorted(self._cells.items()):
            edge = cell.edge
            shadow_edge = cell.shadow_edge
            wr = cell.win_rate

            snapshot.append({
                "asset": asset,
                "strategy": strategy,
                "enabled": cell.enabled,
                "edge": round(edge * 100, 1) if edge is not None else None,
                "shadow_edge": round(shadow_edge * 100, 1) if shadow_edge is not None else None,
                "win_rate": round(wr * 100, 1) if wr is not None else None,
                "trades": cell.trade_count,
                "total_trades": cell.total_trades,
                "shadow_trades": len(cell.shadow_trades),
                "recent_pnl": round(cell.recent_pnl, 2),
                "days_disabled_7d": cell.days_disabled_last_week,
                "status": "enabled" if cell.enabled else ("shadow" if not cell.has_ever_been_enabled else "disabled"),
            })

        return snapshot

    def get_summary(self) -> str:
        """Human-readable matrix summary for logging."""
        lines = ["Strategy Matrix:"]
        for (asset, strategy), cell in sorted(self._cells.items()):
            edge = cell.edge
            status = "ON " if cell.enabled else "OFF"
            edge_str = f"{edge:+.1%}" if edge is not None else "n/a"
            wr = cell.win_rate
            wr_str = f"{wr:.0%}" if wr is not None else "n/a"
            shadow = ""
            if not cell.enabled and cell.shadow_edge is not None:
                shadow = f" (shadow: {cell.shadow_edge:+.1%})"
            lines.append(
                f"  [{status}] {asset:12s} / {strategy:20s} "
                f"edge={edge_str:>6s}  WR={wr_str:>4s}  "
                f"trades={cell.trade_count:>3d}{shadow}"
            )
        return "\n".join(lines)

    # ── Persistence ────────────────────────────────────────────────

    def _persist_state(self):
        """Save cell enabled/disabled state to disk."""
        if not self.persist_path:
            return
        try:
            state = {}
            for (asset, strategy), cell in self._cells.items():
                state[f"{asset}|{strategy}"] = {
                    "enabled": cell.enabled,
                    "hard_disabled": cell.hard_disabled,
                    "has_ever_been_enabled": cell.has_ever_been_enabled,
                    "disabled_at": cell.disabled_at,
                    "enabled_at": cell.enabled_at,
                    "disabled_days": list(cell.disabled_days),
                    "total_trades": cell.total_trades,
                    "total_shadow_trades": cell.total_shadow_trades,
                    "trades": [
                        {"ts": t.timestamp, "pnl": t.pnl, "stake": t.stake, "outcome": t.outcome}
                        for t in cell.trades
                    ],
                    "shadow_trades": [
                        {"ts": t.timestamp, "pnl": t.pnl, "stake": t.stake, "outcome": t.outcome}
                        for t in cell.shadow_trades
                    ],
                }
            Path(self.persist_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self.persist_path, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"[MATRIX] Failed to persist state: {e}")

    def _load_state(self):
        """Load cell state from disk, skipping any (asset, strategy)
        tuples that are no longer in the whitelist (e.g. deleted
        strategies or 'unknown' tickers from earlier code paths).
        """
        if not self.persist_path:
            return
        path = Path(self.persist_path)
        if not path.exists():
            return
        try:
            with open(path) as f:
                state = json.load(f)
            skipped = 0
            for key_str, data in state.items():
                asset, strategy = key_str.split("|", 1)
                if not self._is_allowed(asset, strategy):
                    skipped += 1
                    continue
                cell = self._cells[self._cell_key(asset, strategy)]
                cell.enabled = data.get("enabled", False)
                cell.hard_disabled = data.get("hard_disabled", False)
                cell.has_ever_been_enabled = data.get("has_ever_been_enabled", False)
                cell.disabled_at = data.get("disabled_at")
                # Restore trade history
                for t in data.get("trades", []):
                    cell.trades.append(TradeResult(
                        timestamp=t["ts"], pnl=t["pnl"],
                        stake=t["stake"], outcome=t["outcome"],
                    ))
                for t in data.get("shadow_trades", []):
                    cell.shadow_trades.append(TradeResult(
                        timestamp=t["ts"], pnl=t["pnl"],
                        stake=t["stake"], outcome=t["outcome"],
                    ))
                cell.enabled_at = data.get("enabled_at")
                cell.total_trades = data.get("total_trades", 0)
                cell.total_shadow_trades = data.get("total_shadow_trades", 0)
                for d in data.get("disabled_days", []):
                    cell.disabled_days.append(d)
            kept = len(self._cells)
            logger.info(f"[MATRIX] Loaded state: {kept} cells (skipped {skipped} stale)")
            disabled = sum(1 for c in self._cells.values() if not c.enabled)
            if disabled:
                logger.info(f"[MATRIX] {disabled} cells currently disabled")
        except Exception as e:
            logger.warning(f"[MATRIX] Failed to load state: {e}")
