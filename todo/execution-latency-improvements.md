# Execution-latency improvements — follow-ups

Shipped 2026-04-21 (execution-latency pass):

- **#1 Balance cache (10s TTL)** — `_get_balance` was hitting Kalshi 4×/sec from the SSE publisher. Now cached with explicit invalidation after order submission.
- **#2 Skip fresh-book fetch for maker orders** — removed the ~100-500ms `get_orderbook` REST call from the maker-submission path. Maker orders now submit on the WS-cached bid/ask the fast-RR scanner already validated. Taker mode still fetches fresh.
- **#3 Fast-RR scan rate configurable** — existing 20Hz thread bumped to 50Hz default, configurable via `RR_SCAN_HZ` env var (clamped to 1–200).

Shipped 2026-04-22 (follow-up pass):

- **#4 Submission timing instrumentation** — `_maybe_trade` now emits a single `[TIMING]` line per submission showing ms deltas through balance / book / risk / ev / submit / wait stages.
- **#5 Parallel submission of concurrent opportunities** — `_fast_rr_scan` now collects pending trades and dispatches via a bounded `ThreadPoolExecutor` (max 3 workers) when >1 candidate passes gates in one sweep. TradeLogger, `_log_hit_outcome` CSV write, and hit-context are all thread-safe; the hit-context flows through `_maybe_trade` / `_record_skip` / `_log_hit_outcome` as an explicit `hit_ctx` parameter instead of the old shared instance attribute.
- **#6 Shorter maker wait** — cap reduced from 120s → 30s.
- **#7 Adaptive maker → taker flip** — the fill-wait loop samples WS bid/ask every 3s after a 5s settle; if the book's ask has drifted ≥2c away from the resting bid, cancel and fall through to the taker-fallback path instead of waiting out the full timeout.
- **#8 Live missed-trades log** — new `missed_trades.py` module plus a background thread started from `bot.py`. Every 5 min it scans settled crypto markets; any ticker that reached 94c+ on the winning side with no RR entry row gets appended to `data/missed_trades.csv` and kept in memory for the dashboard (wiring still TODO — the data is available via `bot.missed_tracker.recent_misses()`).

Open follow-ups (in priority order):

## 4. Submission timing instrumentation

Add millisecond deltas around each step of `_maybe_trade` (fresh book fetch when present, risk check, EV check, order submit, fill wait). Log a one-line summary per trade so we can see where time is spent.

**Why**: makes future optimizations evidence-driven. If the fresh-book fetch (when in taker mode) is 400ms and the fill polling is 900ms, we know which to target.

**Size**: ~20 lines. Purely diagnostic — no behavior change.

## 5. Parallel submission of concurrent opportunities

When 3+ markets reach 94c+ at the same time (common during settlement-minute clusters), we currently submit sequentially. Each trip through `_maybe_trade` takes ~150-300ms after shipping #2. For 3 markets that's 450-900ms of serial latency; the last market sees book data up to ~1 second old.

**Proposal**: when `_fast_rr_scan` finds >1 tradeable setup in a single sweep, dispatch each `_maybe_trade` call to a thread pool (bounded to, say, 3 concurrent submissions). The Kalshi client is already thread-safe via its rate-limit lock.

**Why**: removes the serialization penalty on burst opportunities. Biggest gains during volatility spikes when multiple strikes hit 94c+ simultaneously.

**Caveats**: balance cache must correctly handle concurrent `invalidate_balance_cache()` calls (it already does — attribute writes are atomic in CPython). `self._traded_tickers` add needs to be done BEFORE the thread dispatch to prevent two threads trading the same ticker.

**Size**: ~40-60 lines. Moderate complexity.

## 6. Shorter maker wait + faster iteration

Current maker wait: up to 120s in `_maybe_trade`. For markets near settlement, 120s is often longer than the market has left. A resting maker bid that hasn't filled in 30s is very unlikely to fill in the next 90s.

**Proposal**: cap maker wait at `min(secs_left - 15, 30)`. Unfilled orders expire faster, freeing us to try the next opportunity.

**Why**: higher cancel rate (no P&L cost), but faster iteration through opportunities in the 94-97c window. Net trade count should increase.

**Size**: ~5 lines. Behavior-changing — watch cancel rate after deploy.

## 7. Adaptive maker → taker flip when book moves away

If a maker order hasn't filled after N seconds AND the book has moved further from our price (not toward us), cancel and resubmit as taker at the current ask (if still in the band). The framework already has a partial taker-fallback path; could be more aggressive.

**Why**: captures trades where the book moved past our price and stayed there. Trade-off: adds taker fees (~0.7c/contract) but better than missing.

**Size**: ~20-30 lines. Behavior-changing — watch fee drag.

## 8. Live "missed trades" log

Every 5 min, scan recently-settled markets: did any reach 94c+ and settle on that side without us trading it? If yes, log with current gate state so we can diagnose the miss in near-real-time.

**Why**: continuous feedback loop. Turns post-hoc analysis (like today's 4/21 tick replay) into a live signal. If a cell has >3 missed winners in the last hour, the dashboard would surface it.

**Size**: ~60-80 lines. New module plus dashboard wiring.

---

## When to revisit

- **After shipping #1-3**: observe trade count for 24-48 hours. If latency-driven misses are still >10% of detected opportunities, ship #5 next.
- **If cancel rate spikes**: likely a #6 or #7 candidate.
- **If any single cell shows a pattern of missed winners**: ship #8 to catch it in real-time.
