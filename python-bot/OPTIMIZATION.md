# Resolution Rider Optimization Guide

## Overview

The resolution rider strategy uses per-cell optimized parameters for each (coin, market_type) combination. Parameters are found via Monte Carlo search with leave-one-day-out cross-validation, and only cells that achieve 100% CV win rate are enabled for live trading.

## Quick Start

```bash
cd python-bot

# 1. Run the optimizer (~2 minutes)
python optimize_rr.py

# 2. Restart the bot to load new params
# (Ctrl+C the running bot, then restart)
python bot.py --live
```

## How It Works

### Data Sources

| Source | Location | Purpose |
|---|---|---|
| Tick recordings | `/mnt/d/datasets/prediction-market-analysis/ticks/*.csv` | Real-time bid/ask for every contract update (primary training data) |
| Crypto prices | `data/prices/*.csv` | 1-minute BTC/ETH/SOL candles for buffer and momentum calculations |
| Live trades | `data/live_trades.csv` | Ground truth for actual execution and fills |

### Optimization Process

1. **Load tick data** — all available daily tick CSVs (currently Apr 8-11, grows daily)
2. **Reconstruct market windows** — for each contract, build the price trajectory from open to settlement
3. **Pre-process** — extract all ticks where the contract is at 94-99c within 500s of close, pre-compute buffer and momentum at each tick
4. **Monte Carlo search** — 3,480 parameter candidates per cell (480 grid + 3,000 random)
5. **K-fold cross-validation** — leave-one-day-out: train on N-1 days, validate on the held-out day, rotate through all days
6. **Selection** — pick the param set with zero validation losses and the most trades. If none achieve zero losses, pick the highest-scoring set.
7. **Output** — write `data/rr_params.json` with per-cell optimal configs

### Parameters Tuned Per Cell

| Parameter | Range | What It Controls |
|---|---|---|
| `min_contract_price` | 95-98c | Minimum contract price to consider entering |
| `max_entry_price` | 96-98c | Maximum contract price (99c loses money after fees) |
| `max_seconds` | 60-480s | Latest entry point before settlement |
| `min_price_buffer_pct` | 0.05-0.50% | How far the underlying price must be from the strike |
| `max_adverse_momentum` | -0.10 to 0.00 | Block if price is trending toward the strike |
| `momentum_window` | 30-300s | Window size for momentum smoothing |
| `momentum_periods` | 1-10 | Number of periods averaged for momentum |

### Cell Classification

Each cell is one of:
- **Safe (enabled)** — 100% win rate across all CV folds. Gets live trading with its optimized params.
- **Unsafe (hard-disabled)** — has CV losses. Shadow-tracked only. Gets reconsidered on next optimization run.
- **No data** — not enough tick data yet. Stays disabled until data accumulates.

## When to Re-Optimize

- **Every 3-4 days** as new tick data accumulates (~15 trades/day adds meaningful signal)
- **After market regime changes** — if volatility shifts significantly, params may need updating
- **If live win rate drops below 95%** on any cell — something changed, re-optimize immediately

## What Happens on Bot Restart

1. Bot reads `data/rr_params.json`
2. Cells with `cv_mean_win_rate == 1.0` are force-enabled with their optimized params
3. All other RR cells are hard-disabled (matrix cannot re-enable them)
4. Previously disabled cells that now have 100% CV WR in the new params file get automatically promoted

## File Reference

| File | Purpose |
|---|---|
| `optimize_rr.py` | Monte Carlo optimizer — run this to generate new params |
| `data/rr_params.json` | Output: per-cell optimized parameters (read by bot on startup) |
| `bot.py` | Loads params, applies per-cell configs to fast RR thread |
| `strategies/resolution_rider.py` | Strategy implementation with buffer, momentum, time checks |

## Current Cell Status (as of 2026-04-11)

### Enabled
| Cell | Price | Max Secs | Buffer | CV Trades |
|---|---|---|---|---|
| btc_15m | 96c | 60s | 0.34% | 7 |
| btc_hourly | 95-98c | 60s | 0.07% | 26 |
| bnb_15m | 95-97c | 60s | 0.46% | 2 |
| doge_15m | 97-98c | 60s | 0.41% | 8 |
| doge_hourly | 95-97c | 480s | 0.08% | 3 (no CV) |
| eth_hourly | 96-98c | 240s | 0.29% | 11 |
| sol_hourly | 95-97c | 480s | 0.14% | 5 |

### Disabled (need more data)
| Cell | Best CV WR | Issue |
|---|---|---|
| eth_15m | 75% | 1 loss in 4 CV trades |
| sol_15m | 86% | 2 losses in 14 CV trades |
| hype_15m | 93% | 1 loss in 14 CV trades |
| xrp_15m | 86% | 1 loss in 7 CV trades |
| bnb_hourly | 25% | 3 losses in 4 CV trades |
| hype_hourly | — | No validation trades |
| xrp_hourly | — | No validation trades |

## Future Work

- **Live adaptive optimizer** (`rr_adaptive.py`): continuously adjust params based on every settled market, not just periodic batch re-optimization
- **Stake scaling**: once depth data confirms liquidity, scale position sizes above $10 per cell
- **Non-crypto markets**: re-enable broad scanner when price feeds are available for weather/economics
