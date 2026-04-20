# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A trading system for Kalshi's 15-minute crypto prediction markets (KXBTC15M / KXETH15M / KXSOL15M). Two halves live in one repo:

- **`python-bot/`** — the actual trader. Long-running process that polls/streams Kalshi, runs the Resolution Rider strategy, places real orders, and publishes SSE events on port 5050. This is where all trading logic lives.
- **`server/` + `client/` + `shared/`** — a dashboard (Express + Vite/React) that reads the bot's SSE stream and renders it. **The dashboard is a viewer, not a brain.** `server/routes.ts` only proxies `/api/stream` from the Python bot; there is no trading logic in TypeScript.

Treat the two halves as separately deployed services (see `deploy.sh` — two `systemd` units) that happen to share a repo.

## Commands

### Dashboard (Node/TS)
```bash
npm run dev        # dev server on :5000 (Vite + tsx server)
npm run build      # vite build + esbuild server → dist/
npm run start      # run built server (NODE_ENV=production)
npm run check      # tsc type-check (noEmit)
```

### Python bot
```bash
cd python-bot
pip install -r requirements.txt

python bot.py              # paper trading
python bot.py --live       # live trading (reads KALSHI_* from .env)
python bot.py --series KXETH15M --stake 10

# Tests (stdlib unittest)
python -m unittest discover tests
python -m unittest tests.test_parse_book_top

# RR parameter optimizer (slow; ~40min CUDA / ~2min CPU)
python optimize_rr.py
python optimize_rr_cuda.py
./auto_reoptimize.sh              # nightly cron: sweep + gated deploy + SIGHUP
```

### Reload params without restart
`kill -HUP $(pgrep -f "python bot.py")` — `bot.py` has a SIGHUP handler that re-reads `data/rr_params.json` and re-applies cell gates. **No restart needed for param updates.**

## Service operations (systemd)

Local dev box runs both halves as `systemd` units: `kalshi-bot` (python-bot/bot.py, live) and `kalshi-dashboard` (dist/index.cjs, port 5000). Unit files live in `python-bot/systemd/`. VPS uses `deploy.sh`, which generates equivalent units under the `kalshi` user.

### One-time install (local)
```bash
cd python-bot/systemd
./install.sh                   # copies units, daemon-reload, enables on boot (does NOT start)
sudo systemctl start kalshi-bot kalshi-dashboard
```

### Daily operations
```bash
# Status / logs
systemctl status kalshi-bot kalshi-dashboard
journalctl -u kalshi-bot -f                       # live bot logs
journalctl -u kalshi-dashboard -f                 # live dashboard logs
journalctl -u kalshi-bot --since "1 hour ago"     # backfill

# Start / stop / restart (sudo required)
sudo systemctl start   kalshi-bot
sudo systemctl stop    kalshi-bot
sudo systemctl restart kalshi-bot                 # full restart (drops SSE clients briefly)
sudo systemctl restart kalshi-dashboard
sudo systemctl restart kalshi-bot kalshi-dashboard

# Hot-reload RR params — NO restart, no interrupted positions
sudo systemctl reload kalshi-bot                  # sends SIGHUP → re-reads rr_params.json
# Equivalent: kill -HUP $(pgrep -f "python bot.py")
```

The bot's own log file at `$DATA_DIR/logs/bot_<date>.log` keeps running across restarts (journal is also captured). Prefer the file when tailing across a restart.

### Code change workflow

- **Python bot code / requirements change** → full restart:
  ```bash
  # (pip install -r requirements.txt if deps changed, into python-bot/.venv)
  sudo systemctl restart kalshi-bot
  ```
- **RR params only (`data/rr_params.json`)** → reload, no restart:
  ```bash
  sudo systemctl reload kalshi-bot
  ```
  The nightly `auto_reoptimize.sh` already does this via `SIGHUP`; manual edits need the reload too.
- **Dashboard code (client/ or server/ or shared/)** → rebuild + restart dashboard:
  ```bash
  npm run build
  sudo systemctl restart kalshi-dashboard
  ```
  Dashboard restart does not touch the bot. The bot's SSE server keeps running and the dashboard re-proxies on boot.
- **`shared/schema.ts` change** → restart **both** (bot publishes payload, dashboard consumes it — schema drift is silent):
  ```bash
  npm run build
  sudo systemctl restart kalshi-bot kalshi-dashboard
  ```
- **Unit file itself (`python-bot/systemd/*.service`)** → reinstall + daemon-reload:
  ```bash
  cd python-bot/systemd && ./install.sh
  sudo systemctl daemon-reload
  sudo systemctl restart kalshi-bot kalshi-dashboard
  ```

### Dev loop (without systemd)

Sometimes it's easier to iterate on the bot in a terminal. Stop the service first so two bots don't both try to place orders:
```bash
sudo systemctl stop kalshi-bot
cd python-bot && python bot.py --live          # Ctrl-C to exit
sudo systemctl start kalshi-bot                # hand trading back to the service
```
Dashboard dev mode (`npm run dev`) can run alongside the `kalshi-dashboard` service because Vite dev uses its own process — just hit the dev server's port instead of 5000.

## Architecture

### Data flow
```
Kalshi WS ─┐
Coinbase   ├─→ python-bot/bot.py ─→ SSE :5050 ─→ server/routes.ts ─→ client (React)
CryptoFeed ┘         │
                     └─→ data/ (ticks, prices, trades, rr_params.json)
```

### Python bot key modules
- `bot.py` — main loop, SIGHUP param reload, SSE server, trade logger. `parse_book_top()` is the single source of truth for reading Kalshi's `orderbook_fp` schema (legacy `orderbook` schema is also handled — don't remove). `best_strike_for_market()` regex-extracts strikes from titles for 15M markets (not in `floor_strike`).
- `kalshi_client.py` — Kalshi API v2 with RSA-PSS signed requests.
- `kalshi_ws.py` — WebSocket feed + `TickRecorder` (writes ticks to `$DATA_DIR/ticks/*.csv`).
- `strategies/resolution_rider.py` — the **only live strategy**. Buys YES at 95-98c near settlement. `required_buffer()` scales buffer by `sqrt(secs_left/60)` and must stay in lock-step with `optimize_rr.simulate_fast`.
- `risk_manager.py` — daily/weekly loss caps, position sizing, cooldowns. `kalshi_taker_fee`/`kalshi_maker_fee` helpers.
- `optimize_rr.py` / `optimize_rr_cuda.py` — Monte Carlo + k-fold CV per (coin, market_type) cell → `data/rr_params.json`. `auto_reoptimize.sh` runs nightly and only deploys if new params beat current ones by ≥ `MIN_IMPROVEMENT` with no cell regressing > `MAX_CELL_REGRESSION`.
- `reconcile_kalshi_api.py` — canonical P&L from Kalshi's settlement API. `reconcile_kalshi.py` (CSV-based) has a known phantom "unknown" sell-fill issue — prefer the API version.
- `data_paths.py` — **all data I/O goes through `root() / ensure() / resolve()`**. Respects `$DATA_DIR`, falls back to `python-bot/data/`. Don't hardcode `data/...` paths in new code.

### Frontend
- Wouter + hash routing (`useHashLocation`) because dashboard is served as a static SPA.
- `client/src/lib/sse.ts` — the single `useSSE` hook. All live data flows through this; pages receive `data` as props.
- Path aliases: `@/*` → `client/src/*`, `@shared/*` → `shared/*`.
- `shared/schema.ts` — Zod schemas for SSE payload shape. This is the contract between Python bot (producer) and React (consumer). Schema drift here breaks the dashboard silently.

### Ports and env
- `5000` — dashboard (both dev & prod).
- `5050` — bot's SSE endpoint (`BOT_SSE_PORT`).
- `DASHBOARD_PASSWORD` — if set, dashboard requires HTTP Basic auth (except `/api/health`).
- `DATA_DIR` — where ticks/prices/params live. Local: typically `/mnt/d/...`; VPS: `/home/kalshi/data`.
- `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`, `KALSHI_ENV` (`demo`|`prod`) — live trading auth.

## External APIs
- **Kalshi Trading API v2** — RSA-PSS signed REST + WebSocket. Demo: `https://demo-api.kalshi.co/trade-api/v2`. Prod: `https://api.elections.kalshi.com/trade-api/v2`. `asyncapi.yaml` and `openapi.yaml` are the vendored specs.
- **Coinbase / Crypto.com** — public price + orderbook feeds, no auth. See `price_feed.py`, `crypto_ws.py`, `multi_feed.py`.
- **DeltaBase BigQuery** — `deltabase-public.prediction_markets`, optional training-data source (see `DELTABASE_SETUP.md`).

## Conventions and gotchas

- **`data/rr_params.json` is committed on purpose** (see `.gitignore`: everything under `python-bot/data/` is ignored *except* `rr_params.json`). It's the live RR config; diffs and reverts matter. Dated archives (`rr_params_YYYY-MM-DD_*.json`) stay untracked.
- **Kalshi orderbook quirk**: bid levels are sorted ascending (worst → best), best bid is at `[-1]`. Only bids are returned; YES ask = `100 - best_no_bid`. `parse_book_top` encodes this — don't reimplement inline.
- **Unified YES/NO book**: YES_ask + NO_ask ≥ 100c always. Cross-side arb is impossible on Kalshi; don't propose it.
- **Maker fees are $0, taker fees are not.** Most execution code uses `immediate_or_cancel` with `reduce_only` for exits; changing that is load-bearing.
- **No stop losses on RR**: the strategy holds every position to settlement. Early-exit code paths are intentionally disabled.
- **Cells**: RR params are per-`(coin, market_type)` cell. A cell is "enabled" only if it passed the CV zero-loss gate during optimization.
- **Tests**: stdlib `unittest`, no pytest. Run from `python-bot/` so `sys.path` hacks in tests work.
- **Hash routing**: `wouter/use-hash-location`. URLs look like `#/pnl`. Don't swap in browser routing without verifying the static-file host supports SPA rewrites.

## What not to touch without a clear reason

- `shared/schema.ts` — changing field shapes breaks the live dashboard. If you add a field, add it to Python's SSE payload in the same change.
- `parse_book_top`, `best_strike_for_market`, `required_buffer` — each fixed a specific incident (2026-04-13 bad-fill, 15M title-strike bug, time-scaling). Re-read the docstrings before changing.
- `auto_reoptimize.sh` deploy gate — loosening `MIN_IMPROVEMENT` or `MAX_CELL_REGRESSION` is a live-money decision.
- Reconcile logic — `reconcile_kalshi.py` has known phantom "unknown" CSV rows; use `reconcile_kalshi_api.py` for authoritative P&L.
