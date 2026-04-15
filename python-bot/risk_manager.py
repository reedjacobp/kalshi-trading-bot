"""
Risk Manager for Kalshi Trading Bot

Enforces position sizing, daily loss limits, max concurrent positions,
and per-trade risk controls. No trade gets executed without passing
through the risk manager first.
"""

import math
import time
from dataclasses import dataclass, field
from typing import Optional


def kalshi_taker_fee(contracts: int, price_cents: int) -> float:
    """
    Kalshi taker fee per trade: 0.07 * contracts * P * (1 - P)
    where P = price in dollars (price_cents / 100).
    Total rounded up to the nearest cent.
    """
    p = price_cents / 100.0
    fee = 0.07 * contracts * p * (1 - p)
    return math.ceil(fee * 100) / 100  # round up to nearest cent


def kalshi_maker_fee(contracts: int, price_cents: int) -> float:
    """
    Kalshi maker fee: $0.00 (zero fees for resting limit orders).
    Confirmed via Kalshi portfolio page — all maker fills show $0.00 fees.
    """
    return 0.0


@dataclass
class RiskConfig:
    """Risk management configuration."""
    stake_usd: float = 5.00           # Fixed stake fallback (used when no balance available)
    kelly_fraction: float = 0.25      # Quarter-Kelly (conservative)
    max_daily_loss_usd: float = 25.00  # Stop after this daily loss
    max_weekly_loss_usd: float = 75.00 # Stop after this weekly loss
    max_concurrent_positions: int = 3   # Max open positions
    max_position_pct: float = 0.05     # Max 5% of balance per trade
    min_confidence: float = 0.3        # Minimum strategy confidence to trade
    cooldown_after_loss_secs: int = 60 # Wait this long after a loss before trading again
    max_trades_per_hour: int = 20      # Rate limit


@dataclass
class TradeRecord:
    """Record of a placed trade."""
    timestamp: float
    ticker: str
    strategy: str
    side: str           # "yes" or "no"
    price_cents: int
    contracts: int
    stake_usd: float
    order_id: str = ""
    client_order_id: str = ""
    outcome: str = ""   # "win", "loss", or "" if pending
    payout_usd: float = 0.0
    profit_usd: float = 0.0
    entry_fee_usd: float = 0.0   # Kalshi fee on entry (maker or taker)
    settle_fee_usd: float = 0.0  # Kalshi taker fee on settlement (win payout)
    profit_after_fees: float = 0.0  # profit_usd minus all fees
    is_maker: bool = False  # True if entry was a passive maker order


class RiskManager:
    """
    Enforces risk limits and position sizing.

    All trade requests must pass through `approve_trade()` before
    execution. The risk manager tracks P&L, open positions, and
    enforces daily/weekly limits.
    """

    def __init__(self, config: RiskConfig = None):
        self.config = config or RiskConfig()
        self.trades: list[TradeRecord] = []
        self.open_positions: dict[str, TradeRecord] = {}  # ticker -> record
        self._last_loss_ts: float = 0
        self._daily_reset_ts: float = time.time()

    @property
    def daily_pnl(self) -> float:
        """Sum of realized P&L since last daily reset."""
        return sum(
            t.profit_usd for t in self.trades
            if t.timestamp >= self._daily_reset_ts and t.outcome != ""
        )

    @property
    def weekly_pnl(self) -> float:
        """Sum of realized P&L over the last 7 days."""
        cutoff = time.time() - 7 * 86400
        return sum(
            t.profit_usd for t in self.trades
            if t.timestamp >= cutoff and t.outcome != ""
        )

    @property
    def trades_this_hour(self) -> int:
        """Number of trades placed in the last hour."""
        cutoff = time.time() - 3600
        return sum(1 for t in self.trades if t.timestamp >= cutoff)

    @property
    def win_rate(self) -> Optional[float]:
        """Win rate of all completed trades."""
        completed = [t for t in self.trades if t.outcome != ""]
        if not completed:
            return None
        wins = sum(1 for t in completed if t.outcome == "win")
        return wins / len(completed)

    @property
    def total_pnl(self) -> float:
        """Total realized P&L."""
        return sum(t.profit_usd for t in self.trades if t.outcome != "")

    def reset_daily(self):
        """Reset the daily P&L counter."""
        self._daily_reset_ts = time.time()

    def approve_trade(
        self,
        ticker: str,
        strategy_name: str,
        side: str,
        confidence: float,
        price_cents: int,
        balance_usd: float = None,
    ) -> tuple[bool, str]:
        """
        Check if a proposed trade passes all risk filters.

        Returns:
            (approved: bool, reason: str)
        """
        # Confidence threshold
        if confidence < self.config.min_confidence:
            return False, f"Confidence {confidence:.2f} below threshold {self.config.min_confidence}"

        # Daily loss limit
        if self.daily_pnl <= -self.config.max_daily_loss_usd:
            return False, f"Daily loss limit reached (${self.daily_pnl:.2f})"

        # Weekly loss limit
        if self.weekly_pnl <= -self.config.max_weekly_loss_usd:
            return False, f"Weekly loss limit reached (${self.weekly_pnl:.2f})"

        # Max concurrent positions
        if len(self.open_positions) >= self.config.max_concurrent_positions:
            return False, f"Max positions reached ({len(self.open_positions)})"

        # Already have a position on this market
        if ticker in self.open_positions:
            return False, f"Already have position on {ticker}"

        # Rate limit
        if self.trades_this_hour >= self.config.max_trades_per_hour:
            return False, f"Hourly trade limit reached ({self.trades_this_hour})"

        # Cooldown after loss
        if time.time() - self._last_loss_ts < self.config.cooldown_after_loss_secs:
            remaining = self.config.cooldown_after_loss_secs - (time.time() - self._last_loss_ts)
            return False, f"Loss cooldown ({remaining:.0f}s remaining)"

        # Balance check (if balance known)
        if balance_usd is not None:
            max_stake = balance_usd * self.config.max_position_pct
            # Use the actual Kelly-sized stake, not the fixed ceiling
            actual_contracts = self.calculate_contracts(price_cents, confidence, balance_usd)
            actual_stake = actual_contracts * (price_cents / 100.0)
            if actual_stake > max_stake:
                return False, f"Stake ${actual_stake:.2f} exceeds {self.config.max_position_pct*100:.0f}% of balance (${max_stake:.2f})"

        return True, "Approved"

    def correlated_position_count(self) -> int:
        """Count open positions that share the same 15-min window (correlated)."""
        # Tickers like KXBTC15M-26APR050400-00: the window suffix is the last part
        windows = set()
        for ticker in self.open_positions:
            # Extract the window identifier (e.g., "26APR050400-00")
            parts = ticker.split("-", 1)
            if len(parts) > 1:
                windows.add(parts[1])
        # Count how many open positions share a window with any other
        if not windows:
            return 0
        return len(self.open_positions)

    def calculate_contracts(
        self, price_cents: int, confidence: float = 0.0, balance_usd: float = None,
        calibrated_probability: float = None,
    ) -> int:
        """
        Kelly-sized position: how many contracts to buy.

        Each contract costs `price_cents` cents and pays $1 if correct.
        Kelly fraction f* = (p*b - q) / b, where:
          p = estimated win probability (calibrated_probability or confidence)
          q = 1 - p
          b = net odds = (100 - price_cents) / price_cents

        We then apply kelly_fraction (0.25 = quarter-Kelly) and cap at
        max_position_pct of balance.

        Args:
            calibrated_probability: If provided, use this as the win
                probability instead of raw confidence. Pass an empirical
                win rate here to override the raw strategy confidence.
        """
        if price_cents <= 0 or price_cents >= 100:
            return 0

        price_usd = price_cents / 100.0

        # Determine stake via Kelly sizing if we have confidence + balance
        if confidence > 0 and balance_usd is not None and balance_usd > 0:
            # Use calibrated probability for Kelly if available, else raw confidence
            p = calibrated_probability if calibrated_probability is not None else confidence
            q = 1 - p
            b = (100 - price_cents) / price_cents  # net odds (payout / cost)
            kelly_f = (p * b - q) / b if b > 0 else 0
            kelly_f = max(0, kelly_f)  # never negative
            kelly_f *= self.config.kelly_fraction  # fractional Kelly

            stake = kelly_f * balance_usd
            # Cap at max_position_pct of balance
            stake = min(stake, balance_usd * self.config.max_position_pct)
            # Also cap at the fixed stake_usd as an absolute ceiling
            stake = min(stake, self.config.stake_usd)
            # Correlation discount: reduce size when multiple positions open
            # BTC/ETH/SOL are ~67% correlated, so 2+ positions = overexposed
            open_count = self.correlated_position_count()
            if open_count >= 2:
                stake *= 0.6  # 40% reduction with 2+ correlated positions
            elif open_count == 1:
                stake *= 0.8  # 20% reduction with 1 existing position
        else:
            # Fallback: fixed stake
            stake = self.config.stake_usd

        contracts = int(stake / price_usd)
        return max(1, min(contracts, 100))  # floor 1, cap 100

    def record_trade(self, record: TradeRecord):
        """Record a new trade and add to open positions."""
        fee_fn = kalshi_maker_fee if record.is_maker else kalshi_taker_fee
        record.entry_fee_usd = fee_fn(record.contracts, record.price_cents)
        self.trades.append(record)
        self.open_positions[record.ticker] = record

    def settle_trade(self, ticker: str, result: str):
        """
        Settle an open position.

        Args:
            ticker: Market ticker
            result: The market's result ("yes" or "no")
        """
        if ticker not in self.open_positions:
            return

        record = self.open_positions.pop(ticker)
        if record.side == result:
            record.outcome = "win"
            record.payout_usd = record.contracts * 1.00  # $1 per winning contract
            record.profit_usd = record.payout_usd - record.stake_usd
            record.settle_fee_usd = 0.0  # Kalshi does not charge a settlement fee
        else:
            record.outcome = "loss"
            record.payout_usd = 0.0
            record.profit_usd = -record.stake_usd
            record.settle_fee_usd = 0.0
            self._last_loss_ts = time.time()
        record.profit_after_fees = record.profit_usd - record.entry_fee_usd

    def stats_summary(self) -> str:
        """Human-readable summary of trading stats."""
        completed = [t for t in self.trades if t.outcome != ""]
        if not completed:
            return "No completed trades yet"

        wins = sum(1 for t in completed if t.outcome == "win")
        losses = len(completed) - wins
        wr = self.win_rate or 0

        lines = [
            f"Trades: {len(completed)} ({wins}W/{losses}L, {wr*100:.1f}% WR)",
            f"Total P&L: ${self.total_pnl:+.2f}",
            f"Daily P&L: ${self.daily_pnl:+.2f}",
            f"Open positions: {len(self.open_positions)}",
        ]
        return " | ".join(lines)
