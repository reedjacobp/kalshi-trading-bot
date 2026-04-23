# Hourly range markets — implementation plan

Status: **not started**. Saved for later pickup.

## Goal

Add Kalshi's hourly **range** markets (series `KXBTC`, `KXETH`, `KXSOL` — multi-bucket, with both `floor_strike` and `cap_strike` populated on each bucket) as a third cell family alongside the existing `_15m` and `_hourly` binary above/below markets. Cell name: `{coin}_range`.

Example URL: https://kalshi.com/markets/kxbtc/bitcoin-range/kxbtc-26apr2002

The edge hypothesis is identical to current RR: near settlement, the favored bucket's YES trades at 95-98¢ and a disciplined bot captures the last few cents. The plumbing differs because range markets have **two edges** per bucket instead of one.

## Open questions (answer before starting)

1. **Does Kalshi's REST populate `cap_strike`** on these markets, or is only one edge there and the other has to be regex'd from the title? Pull one live event and eyeball the response.
2. **Do buckets hit 95-98¢ in the final minute?** If the favored bucket never reaches RR's entry zone on thin hourlies, this is dead code. Sample a few settled events before writing anything.
3. **Which coins first?** BTC range only (narrow blast radius), or BTC+ETH+SOL together?

## Phase 1 — verify the premise (~30 min, no code)

- Pull one live event via `client.get_markets(event_ticker="KXBTC-26APR2002")` and inspect the per-bucket shape.
- Sample `KXBTC` trades in the final 60s of a few past events (via `pull_historical.py --series KXBTC`) and check how many hit ≥95¢ on YES. If yes-ask rarely reaches 95¢, stop here.

## Phase 2 — strategy math

- `required_buffer()` stays as-is (time-scaling is coin-agnostic).
- `resolution_rider.evaluate()` gets a range-market branch (currently `resolution_rider.py:133-256`): if both edges present, compute
  ```
  edge_dist_pct = min(price - floor, cap - price) / price * 100
  ```
  and require `edge_dist_pct ≥ required_buffer(min_buf, secs)`. YES is "inside, far from both edges"; NO is "outside by a margin" (rarer but symmetric).
- `best_strike_for_market()` either grows to return `(floor, cap)` or we add a sibling. Prefer keeping the existing single-strike return and adding `best_range_for_market() -> (floor, cap) | None` so every existing caller is untouched.

## Phase 3 — data pipeline

- `kalshi_ws.py` TickRecorder: add `cap_strike` column. Backfill-safe (old rows null).
- `pull_historical.py`: add `KXBTC`, `KXETH`, `KXSOL` to `ALL_SERIES` (line 26) and make sure `cap_strike` survives to parquet.
- Optimizer (`optimize_rr.py`): teach `simulate_fast` the range-buffer formula and add `_range` cells to the sweep grid. Biggest diff — ~100 lines of careful change. **Don't refactor the shared code paths**; branch on a per-row flag so `_15m` and `_hourly` behavior is bit-for-bit identical to today.

## Phase 4 — bot wiring (smallest diff)

- New entries in `self.assets` for `btc_range` etc. (`bot.py:887-955`), pointing to `series="KXBTC"`.
- Add `"KXBTC"`, `"KXETH"`, `"KXSOL"` to `ws_feed.subscribe()` (line 1025).
- `_asset_key_from_ticker()` recognizes bare-prefix tickers and returns `{coin}_range`.
- Cell-name mapping for range markets in `bot.py:1128-1131` and `:1480-1482`.
- `rr_params.json`: add the new cells, **disabled by default** until CV passes.

## Phase 5 — dashboard

- Buffer display in `buildGates` (`client/src/pages/dashboard.tsx`) needs a range case: show `dist-to-edge: X.XX% (edge = floor|cap)`. Small.
- `shared/schema.ts` already has `cap_strike` optional — no schema churn.

## Phase 6 — rollout

- Demo env first (`KALSHI_ENV=demo`), one day of paper trading.
- Run `auto_reoptimize.sh` against the new training data; only deploy cells that clear the existing CV zero-loss gate. The gate is the safety net — no manual overrides.
- Live-enable BTC first, let it run a week before adding ETH/SOL.

## Effort and risk

Roughly **10-12 hours of work spread over a week** — most calendar time is waiting for data accumulation and optimizer runs.

Biggest risk is Phase 3: `simulate_fast` is the optimizer's hot loop and we don't want to regress `_15m` / `_hourly`. Mitigate by keeping the range branch opt-in via a row-level flag, not by refactoring shared code.
