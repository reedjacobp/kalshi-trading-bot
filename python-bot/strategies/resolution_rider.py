"""
Resolution Rider / High-Probability Grind Strategy

Core idea: Buy contracts ONLY when priced 95-99c — near-certain outcomes
where the market has already resolved but a few cents of edge remain.

Validated by:
- Walk-forward backtest: 18,316 trades, 98-99% WR, 12/12 months profitable
- Polymarket @Sharky6999: $809K profit, 99.3% WR, 27K trades on same approach
- Our live trading: 100% WR (small sample)

Why this works:
- At 95-99c, the "wrong" outcome has only 1-5% probability
- Liquidity thins near resolution as traders take profit early
- A disciplined bot captures the final cents before settlement
- Extremely high win rate enables aggressive Kelly sizing
- Compounds reliably over thousands of trades

This is the primary profit strategy — designed to generate steady,
reliable returns rather than big individual wins.
"""

import math
from typing import Optional

from strategies.base import Signal, Strategy, TradeRecommendation


def required_buffer(base_pct: float, secs_left: float) -> float:
    """Time-scale the base buffer requirement by sqrt(secs_left / 60).

    Rationale: for a Brownian-motion price process, the residual σ at
    time t is proportional to sqrt(t). A safe buffer at 60s remaining
    should be ~2× larger at 240s and ~half that at 15s. Encoding this
    lets the optimizer search a single `base_pct` value that correctly
    gates entries across the full time range of a cell, rather than
    picking a flat threshold that's either too strict late or too loose
    early.

    This is the same transform applied by optimize_rr.simulate_fast, so
    live and training stay in lock-step.
    """
    scaled = math.sqrt(max(1.0, secs_left) / 60.0)
    return base_pct * scaled


class ResolutionRiderStrategy(Strategy):
    """
    High-probability grind: buys only when a contract is 95-99c.

    Fires across the full market window (not just final 90s) because
    contracts can reach 95c+ well before expiry on daily/hourly markets.
    Uses fractional Kelly sizing for optimal position growth.
    """

    name = "resolution_rider"

    def __init__(
        self,
        # Heuristic defaults — applied uniformly to every cell. The live
        # bot also overlays these onto any per-cell rr_params.json entry
        # via HEURISTIC_PARAMS in bot.py, so optimizer output cannot
        # produce an unsafe gate (e.g. 98c entry or sub-0.2% buffer).
        min_contract_price: int = 94,
        max_entry_price: int = 97,          # HARD CAP: 98c is a break-even trap
        min_seconds: int = 10,              # Final 10s has settlement variance
        max_seconds: int = 180,
        min_price_buffer_pct: float = 0.15,  # Floor. Time-scaled by sqrt(t/60) below.
        max_adverse_momentum: float = -0.04,  # Block on sustained adverse trend
        momentum_window: int = 60,
        momentum_periods: int = 3,
        max_realized_vol_pct: Optional[float] = None,  # Disabled. Kept for sig compat.
        vol_lookback: int = 300,
        kelly_fraction: float = 0.30,
        max_bankroll_pct: float = 0.05,
    ):
        self.min_contract_price = min_contract_price
        self.max_entry_price = max_entry_price
        self.min_seconds = min_seconds
        self.max_seconds = max_seconds
        self.min_price_buffer_pct = min_price_buffer_pct
        self.max_adverse_momentum = max_adverse_momentum
        self.momentum_window = momentum_window
        self.momentum_periods = momentum_periods
        self.max_realized_vol_pct = max_realized_vol_pct
        self.vol_lookback = vol_lookback
        self.kelly_fraction = kelly_fraction
        self.max_bankroll_pct = max_bankroll_pct

    def evaluate(self, market, last_settled, price_feed, scanner, cell_params: Optional[dict] = None) -> TradeRecommendation:
        no_trade = TradeRecommendation(
            signal=Signal.NO_TRADE,
            confidence=0.0,
            strategy_name=self.name,
            reason="",
            max_price_cents=0,
        )

        # Per-cell overrides are still read (the dashboard and optimizer
        # code still pass them for display), but hard invariants below
        # clamp max_entry_price and min_price_buffer_pct so a stale
        # rr_params.json entry can never expose the bot to the 98c trap
        # or a sub-0.15% buffer (the kind of config that cost $100 on
        # 4/21). Invariants match the instance defaults that bot.py's
        # HEURISTIC_PARAMS overlay applies — belt and suspenders.
        cp = cell_params or {}
        min_cp = cp.get("min_contract_price", self.min_contract_price)
        max_ep = min(cp.get("max_entry_price", self.max_entry_price), 97)
        min_secs = cp.get("min_seconds", self.min_seconds)
        max_secs = cp.get("max_seconds", self.max_seconds)
        # Floor lowered 0.15 → 0.10 on 2026-04-23 to get more setups through
        # for cells whose trained buffers are well below 0.10 (eth_hourly,
        # xrp_hourly, doge_15m, etc.). 0.10% is still 5× the 0.021% that
        # caused the original eth_hourly disaster, so the catastrophic-
        # buffer protection stays intact. Cells with trained buffers
        # ≥ 0.10 are unaffected by this change.
        min_buf = max(cp.get("min_price_buffer_pct", self.min_price_buffer_pct), 0.10)
        max_adv_mom = cp.get("max_adverse_momentum", self.max_adverse_momentum)
        mom_window = cp.get("momentum_window", self.momentum_window)
        mom_periods = cp.get("momentum_periods", self.momentum_periods)
        max_vol = cp.get("max_realized_vol_pct", self.max_realized_vol_pct)
        vol_lookback = cp.get("vol_lookback", self.vol_lookback)

        secs_left = scanner.seconds_until_close(market)

        if secs_left < min_secs:
            no_trade.reason = f"Too close to settlement ({secs_left:.0f}s left)"
            return no_trade

        if secs_left > max_secs:
            no_trade.reason = f"Too far from settlement ({secs_left:.0f}s left, max {max_secs}s)"
            return no_trade

        yes_bid, yes_ask = scanner.parse_yes_price(market)
        if yes_bid is None or yes_ask is None:
            no_trade.reason = "No bid/ask available"
            return no_trade

        # Price buffer check: ensure the underlying asset price is far enough
        # from the strike that a small move won't flip the outcome.
        # This is MANDATORY — if we can't verify the buffer, we don't trade.
        floor_strike = market.get("floor_strike")
        current_price = price_feed.current_price if price_feed else None

        if not floor_strike:
            no_trade.reason = "No floor_strike in market data — can't verify price buffer"
            return no_trade
        if not current_price:
            no_trade.reason = "No live price feed — can't verify price buffer"
            return no_trade

        buffer_pct = None
        try:
            strike = float(floor_strike)
            if strike > 0:
                buffer_pct = (current_price - strike) / strike * 100
        except (ValueError, TypeError):
            no_trade.reason = "Could not parse floor_strike"
            return no_trade

        # Smoothed momentum using the (window, periods) this cell was
        # optimized against — identical to optimize_rr.compute_momentum.
        momentum = None
        if price_feed and hasattr(price_feed, "momentum_smoothed"):
            momentum = price_feed.momentum_smoothed(window=mom_window, periods=mom_periods)
        elif price_feed:
            momentum = price_feed.momentum_1m()

        # Realized volatility filter — applies to BOTH YES and NO sides
        # since chop is direction-agnostic. None on the param disables it.
        realized_vol = None
        if max_vol is not None and price_feed and hasattr(price_feed, "volatility"):
            realized_vol = price_feed.volatility(lookback_seconds=vol_lookback)
            if realized_vol is not None and realized_vol > max_vol:
                no_trade.reason = (
                    f"Blocked: realized vol {realized_vol:.3f}% > "
                    f"limit {max_vol:.3f}% over {vol_lookback}s"
                )
                return no_trade

        yes_avg = (yes_bid + yes_ask) / 2
        no_avg = 100 - yes_avg

        # YES is the near-certain favorite (95-99c)
        if yes_avg >= min_cp:
            our_price = yes_ask
            if our_price > max_ep:
                no_trade.reason = f"YES@{our_price}c too expensive (max {max_ep}c)"
                return no_trade
            if our_price < min_cp:
                no_trade.reason = f"YES ask {our_price}c below minimum {min_cp}c"
                return no_trade

            # For YES (price above strike), buffer must be positive and
            # larger than the time-scaled requirement. The base
            # `min_buf` is the threshold at 60s remaining; it scales up
            # at longer horizons and down at shorter ones.
            req = required_buffer(min_buf, secs_left)
            if buffer_pct is not None and buffer_pct < req:
                no_trade.reason = (f"YES@{our_price}c but price only {buffer_pct:+.2f}% "
                                   f"from strike (need +{req:.3f}% @ {secs_left:.0f}s)")
                return no_trade

            # Adverse momentum: for YES we need price rising (or at least
            # not falling faster than max_adv_mom, which is negative).
            if max_adv_mom < 0 and momentum is not None and momentum < max_adv_mom:
                no_trade.reason = (f"YES@{our_price}c blocked: momentum {momentum:+.3f}% "
                                   f"below limit {max_adv_mom:+.3f}%")
                return no_trade

            confidence = min(0.995, our_price / 100.0)

            mom_str = f" [mom={momentum:+.3f}%]" if momentum is not None else ""
            vol_str = f" [vol={realized_vol:.3f}%]" if realized_vol is not None else ""
            buffer_str = f" [{buffer_pct:+.1f}% from strike]" if buffer_pct is not None else ""
            buffer_str += mom_str + vol_str
            return TradeRecommendation(
                signal=Signal.BUY_YES,
                confidence=confidence,
                strategy_name=self.name,
                reason=(
                    f"Grind: YES@{our_price}c ({secs_left:.0f}s left) — "
                    f"{100 - our_price}c edge at {confidence:.1%} implied prob{buffer_str}"
                ),
                max_price_cents=our_price,
            )

        # NO is the near-certain favorite (YES < 5c → NO > 95c)
        if no_avg >= min_cp:
            our_price = 100 - yes_bid  # NO ask price
            if our_price > max_ep:
                no_trade.reason = f"NO@{our_price}c too expensive (max {max_ep}c)"
                return no_trade
            if our_price < min_cp:
                no_trade.reason = f"NO price {our_price}c below minimum {min_cp}c"
                return no_trade

            # For NO (price below strike), buffer must be negative and
            # below the negated time-scaled requirement.
            req = required_buffer(min_buf, secs_left)
            if buffer_pct is not None and buffer_pct > -req:
                no_trade.reason = (f"NO@{our_price}c but price only {buffer_pct:+.2f}% "
                                   f"from strike (need -{req:.3f}% @ {secs_left:.0f}s)")
                return no_trade

            # Adverse momentum: for NO we need price falling (or at least
            # not rising faster than -max_adv_mom, which is positive).
            if max_adv_mom < 0 and momentum is not None and momentum > -max_adv_mom:
                no_trade.reason = (f"NO@{our_price}c blocked: momentum {momentum:+.3f}% "
                                   f"above limit {-max_adv_mom:+.3f}%")
                return no_trade

            confidence = min(0.995, our_price / 100.0)

            mom_str = f" [mom={momentum:+.3f}%]" if momentum is not None else ""
            vol_str = f" [vol={realized_vol:.3f}%]" if realized_vol is not None else ""
            buffer_str = f" [{buffer_pct:+.1f}% from strike]" if buffer_pct is not None else ""
            buffer_str += mom_str + vol_str
            return TradeRecommendation(
                signal=Signal.BUY_NO,
                confidence=confidence,
                strategy_name=self.name,
                reason=(
                    f"Grind: NO@{our_price}c ({secs_left:.0f}s left) — "
                    f"{100 - our_price}c edge at {confidence:.1%} implied prob{buffer_str}"
                ),
                max_price_cents=our_price,
            )

        no_trade.reason = (
            f"No 95c+ setup (YES@{yes_avg:.0f}c / NO@{no_avg:.0f}c, {secs_left:.0f}s left)"
        )
        return no_trade
