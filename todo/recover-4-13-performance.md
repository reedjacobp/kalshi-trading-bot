# Recover 4/13-level trading performance

Goal: get daily trade volume and PnL back toward the 4/13 baseline
(~70 clean trades, ~$120 net after stripping buggy outliers), without
re-introducing the layers we've since added for safety.

Not a code rewrite — a sequence of small, measurable loosening steps
gated by live data.

## Context

### What 4/13 actually looked like

- **84 unique settled orders**, 75W / 9L (89.3% WR), net **+$292.09**
- Strip 7 buggy outlier order_ids (the KXSOL15M-26APR131115-15
  multi-retry saga + one $100 stake blowup) → **70W / 7L, +$118.97**
  (90.9% WR, $1.55/trade)
- Mix: 15 maker + 69 taker fills

### Why volume collapsed after 4/14

Not primarily market conditions — it's accumulated conservatism from
successive layers of safety machinery landing on 4/14:

- `cv_wr_lower_bound` / Wilson-LB gate (didn't exist on 4/13)
- `min_seconds` floor (now 10s, was `None`)
- Vol filter (`pre_voloff` backup 2026-04-14_18:04)
- Band narrowing (`narrow_bands` backup 2026-04-14_21:10)
- Cell culling (`pre_cull` backup 2026-04-14_22:24)
- Switch to strict taker-only (~2026-04-18, based on order_type ratios)

Each layer was defensible individually; collectively they've trimmed
the strategy from "wide-and-active" to "narrow-and-sleepy."

### Daily trajectory since 4/12

```
date          W/L       PnL    fees    WR
2026-04-12   3/0    +$4.99   $0.08  100.0%
2026-04-13  75/9  +$292.09  $17.50   89.3%  ← banner day
2026-04-14  33/2   -$86.74   $4.81   94.3%  ← only clearly bad day
2026-04-15   9/0    +$4.61   $0.22  100.0%
2026-04-16  40/0    +$8.21   $0.59  100.0%
2026-04-17  54/1   +$33.84   $5.68   98.2%
2026-04-18  18/3   -$66.39   $1.46   85.7%
2026-04-19   7/0   +$11.22   $0.58  100.0%
2026-04-20   7/0   +$19.68   $0.62  100.0%
```

Aggregate since 4/12: **~+$222 net**. Bot is slower but not broken —
per-trade PnL is still positive.

## Current state (as of 2026-04-20 ~22:30 PDT)

### Live config

- `python-bot/data/rr_params.json` — rolled back to
  `rr_params_2026-04-20_034546_pre_auto_reopt.json` (12 cells active)
- `min_seconds = 10` across all cells (pinned 2026-04-20 15:05)
- `ORDER_MODE = taker` (live). Maker code exists in the optimizer but
  not in the live bot order path.
- Cells ON: `bnb_15m`, `bnb_hourly`, `btc_15m`, `btc_hourly`,
  `doge_hourly`, `eth_15m`, `eth_hourly`, `hype_hourly`, `shiba_daily`,
  `sol_hourly`, `xrp_15m` (at `RR_SAFETY_MARGIN=0.05`), `xrp_hourly`
- Cells OFF: `doge_15m`, `hype_15m`, `sol_15m` (genuine CV losers)

### Instrumentation

- `[KALSHI-API] last 60s: N req/s (N% of 15/s cap), throttle-sleeps=N`
  logged to journal every 60s (from `kalshi_client.py:_wait_for_rate_limit`)
- Rate cap: **18 req/s** (was 8). Observed steady-state ~10 req/s,
  0 throttle-sleeps
- `_refresh_rr_cache` interval: 5s (was 10s)

### Optimizer / nightly deploy

- `auto_reoptimize.sh` tightened gates (2026-04-20):
  - `MIN_IMPROVEMENT`: $50 → **$150**
  - `MAX_CELL_REGRESSION`: -$50 (unchanged)
  - NEW: `MAX_WR_LB_REGRESSION`: **0.02** (any cell dropping more than
    that blocks deploy)
  - NEW: active-cell-count guard — deploy blocked if the preview's
    safety-gate-passing cell count drops vs re-scored live
- Fill-rate model loads from `data/fill_rate_model.json` (fit by
  `fit_fill_rate.py` from `live_trades.csv`)
- Safe-entry horizon analysis integrated via `analyze_safe_horizon.py`
  during optimizer runs — per-cell `max_seconds` ceiling from data, not
  searched
- Maker simulation implemented in both CPU (`simulate_fast`) and CUDA
  (`optimize_rr_cuda.py`). Enable with `ORDER_MODE=maker`.

### Env knobs worth knowing

```
RR_SAFETY_MARGIN=0.05    # relaxes WR-LB thresholds in evaluate_cell_safety
RISK_TOLERANCE=0.3       # reward trade volume in optimizer scoring
ORDER_MODE=maker         # switch optimizer to maker (sim only — live is taker)
MAKER_OFFSETS=0,1,2,3    # maker search dim
MAKER_TIMEOUTS=60,120    # maker search dim
CELL_FILTER=btc_hourly   # restrict optimizer to specific cells
HORIZON_THRESHOLD=0.85   # correctness bar for safe-entry horizon
SKIP_HORIZON=1           # fall back to DEFAULT_MAX_SECONDS=120
MIN_IMPROVEMENT=150      # nightly deploy gate
MAX_WR_LB_REGRESSION=0.02
ALLOW_CELL_COUNT_DROP=1  # bypass active-cell-count guard
```

## The plan

### Phase 0 — measure first (2-3 days of observation)

Before any change, collect baseline data under the current config.

What to look at:
- `live_trades.csv` — daily W/L, PnL, trade count
- `journalctl -u kalshi-bot` — `FRR-DBG` lines show per-cell blocker
  breakdown (`secs_too_high`, `fav_too_high`, `eval_no_trade`, etc.)
- `[KALSHI-API]` rate samples — confirm no throttle pressure

**Exit criteria:**
- ≥ $30/day net, ≥ 25 trades/day green WR → **stop, don't loosen**
- < $15/day or < 15 trades/day → **proceed to Phase 1**

TODO: build a morning-summary script that outputs this in one shot.
Rough shape:
```
#!/usr/bin/env bash
# scripts/daily_summary.sh
# - Count today's settled W/L from live_trades.csv
# - Sum PnL / fees
# - Show per-cell blocker histogram from last 24h journal
# - Show latest [KALSHI-API] rate samples
```

### Phase 1 — relax the cell-safety gate (free, no code)

Action: set `RR_SAFETY_MARGIN=0.05` persistently in the systemd unit.

Add to `/etc/systemd/system/kalshi-bot.service` under `[Service]`:
```
Environment=RR_SAFETY_MARGIN=0.05
```
Then `sudo systemctl daemon-reload && sudo systemctl reload kalshi-bot`.

Immediate effect: enables `xrp_15m` (wr_lb 0.766 with +$4.30 CV
profit). +1-2 trades/day.

If that's stable for a day, try `0.08` to see if any borderline cells
flip ON. Most unlikely candidates still won't (doge/hype/sol_15m have
negative CV profit, which no amount of margin relaxation enables).

**Abort criteria:** any newly-enabled cell nets negative PnL over 30+
trades. Revert by removing the env line and reloading.

### Phase 2 — raise `max_entry_price` on high-skew cells

From 2026-04-20 journal: `fav_too_high` vs `fav_too_low` ratios show
markets blowing past 98¢ without hitting our ceiling.

```
hype_hourly    11,247 : 1,053     11:1
xrp_hourly     25,720 : 2,177     12:1
sol_hourly      5,127 :    53     96:1 (sparse but extreme)
```

These cells are systematically rejecting entries at 99¢. Breakeven WR
at 99¢ is **99.10%** ($10 stake). RR's typical WR is ~98-99%, so this
is a knife-edge test.

Action: hand-edit `data/rr_params.json`:
```python
# per cell, set:
"max_entry_price": 99,
```
Start with `xrp_hourly` only (highest combined volume + skew).
`sudo systemctl reload kalshi-bot`.

**Exit criteria:** after 20+ fills at 99¢ entries:
- If WR ≥ 99.5% and cell is net-positive → keep, propagate to
  `hype_hourly`, `sol_hourly`
- If WR < 99.3% or cell is net-negative → revert

### Phase 3 — re-examine the optimizer's search space (code change)

The CV grid has some edges that may be too narrow:

- **Buffer** search is 0.05-0.50%; some cells converge to 0.60%+ in
  practice (e.g. `btc_hourly` lives at 0.609%). Widening the grid to
  include 0.70%, 0.85%, 1.00% lets the optimizer find its actual
  optimum.
- **Momentum** search caps at -0.10; relaxing to -0.15 or -0.20 might
  uncover cells that need higher adverse-momentum tolerance.
- **`min_price_buffer_pct`** — there's tension with the fill-rate
  model. Tight buffer = fewer fills. Worth running a constrained sweep
  at `buf ≤ 0.30` to see if the optimizer prefers wider when fill rate
  is scored in.

Files to touch:
- `optimize_rr.py:grid_params()` and `sample_params()` — widen the
  discrete choices for buffer / momentum

Deploy path: land the change, let the nightly cron run it under the
tightened `MIN_IMPROVEMENT=150` gate. It'll only deploy if the new
params genuinely beat current on CV.

### Phase 4 — probe-enable disabled cells at low stake (data-driven)

`doge_15m`, `hype_15m`, `sol_15m` are OFF because their CV said so —
but **their CV is from the pre-cull era**. Current market conditions
may differ. Run them at reduced stake alongside normal trading to
gather fresh data.

Implementation:
- Add a `PROBE_CELLS=doge_15m,hype_15m` env + `PROBE_STAKE_USD=2`
- In `bot.py`'s cell safety loader, treat probe cells as enabled but
  with a smaller stake override
- Run for 3-5 days, then check `live_trades.csv` for their actual WR

This is the only way to distinguish "cell is still bad" from "cell
was bad before and current data would vindicate it."

### Phase 5 — (optional) revisit maker once market conditions change

Current CUDA maker pilot showed maker offers no clear edge over taker
at today's market regime (`offset=0` wins on both tested cells, which
effectively replicates taker without fees). When markets are more
volatile and spreads are wider, maker with `offset=1-2` might pay off.

Trigger to revisit:
- Current cancel rate climbs above 30% sustained
- Spreads (ask - bid) consistently ≥ 3c on hourly cells
- A quick maker CV on `btc_hourly` + `bnb_hourly` shows non-zero
  `maker_bid_offset` winning

Live order path work (when greenlit):
- Revive maker-first in `bot.py` order placement
- Place GTC limit at `yes_ask - offset`
- Wait `maker_timeout` seconds
- On timeout: **cancel and walk away** — do NOT fall back to taker
  (that was what broke it before)
- Pre-check `get_fills_for_order(oid)` on any cancel to guard against
  race-cancel double-fills

## How to resume

### Check the rest state
```bash
systemctl status kalshi-bot kalshi-dashboard
journalctl -u kalshi-bot -f | grep -E "KALSHI-API|FRR-DBG|FAST-RR.*Hit"
```

### Check what the nightly optimizer did
```bash
ls -lt python-bot/data/logs/auto_reopt_*.log | head -3
tail -80 $(ls -t python-bot/data/logs/auto_reopt_*.log | head -1)
# Look for DECISION=deploy or DECISION=skip + reason
```

### Roll back if a deploy goes bad
```bash
# Find the pre-reopt backup
ls -lt python-bot/data/rr_params_*_pre_auto_reopt.json | head -3
# Copy over live, reload
cp python-bot/data/rr_params_2026-04-XX_XXXXXX_pre_auto_reopt.json \
   python-bot/data/rr_params.json
sudo systemctl reload kalshi-bot
```

### Start Phase 1 (if needed)
```bash
# 1. Edit the systemd unit
sudo systemctl edit kalshi-bot
# Add:
#   [Service]
#   Environment=RR_SAFETY_MARGIN=0.05

# 2. Apply
sudo systemctl daemon-reload
sudo systemctl reload kalshi-bot

# 3. Verify the load log
journalctl -u kalshi-bot --since "1 min ago" | grep -E "RELOAD|safe cells"
# Should show "12 safe cells" still; xrp_15m reason should show positive profit
```

## Related files

- `todo/hourly-range-markets.md` — bigger volume unlock (new ticker
  family), separate track
- `python-bot/AUTO_REOPTIMIZE.md` — how the nightly deploy works
- `python-bot/fit_fill_rate.py` — fill-rate model fit script
- `python-bot/analyze_safe_horizon.py` — per-cell `max_seconds`
  derivation
- `python-bot/auto_reoptimize.sh` — tightened nightly gate
- `CLAUDE.md` — top-level project instructions including systemd ops

## Open questions

1. **Is buggy multi-retry behavior still possible?** The 4/13 SOL
   saga involved 6+ orders on the same ticker due to `_traded_tickers`
   clearing on restarts. If the bot restarts during a market window,
   could we repeat that? Worth reading `bot.py` order-placement path
   to confirm the check is robust to restarts (could persist
   `_traded_tickers` to disk if not).
2. **The 4/14 -$86.74 day** — was that one bad trade, or distributed?
   Worth a post-mortem if we loosen gates again.
3. **`shiba_daily`** has only ~83 CV trades but passes the safety
   gate at wr_lb 0.906. Thin sample — worth watching whether its live
   performance matches CV.
