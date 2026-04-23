#!/bin/bash
# Nightly auto-reoptimize + per-cell deploy gate.
#
# Flow:
#   1. Run full CUDA sweep → data/rr_params_preview.json
#   2. Re-score current live params on the same dataset → data/rr_params_live_on_new_data.json
#   3. Per-cell merge: for EACH cell independently, take the preview
#      when it clears both caps (profit regression ≤ MAX_CELL_REGRESSION,
#      wr_lb regression ≤ MAX_WR_LB_REGRESSION), otherwise keep current
#      live params. Write the merged result → data/rr_params_merged.json.
#   4. If at least one cell was updated: back up rr_params.json,
#      install rr_params_merged.json in its place, SIGHUP the bot.
#
# This replaces the previous all-or-nothing gate that rejected the
# whole deploy if any one cell regressed. Per-cell merge lets us keep
# winners while letting the sweep keep iterating on problem cells
# across subsequent nights.
#
# DEPLOY_ENABLED=0 runs the whole pipeline observation-only — writes
# preview + merged, doesn't touch rr_params.json.
#
# Called from cron. Exits 0 whether we deploy or skip — only hard
# failures (missing GPU, sweep crash) exit nonzero.

DEPLOY_ENABLED=${DEPLOY_ENABLED:-1}
MAX_CELL_REGRESSION=${MAX_CELL_REGRESSION:-50}
# 2026-04-22: raised 0.02 → 0.10. With RISK_TOLERANCE=0.5 the optimizer
# explicitly trades some WR for volume; a 0.02 WR_LB cap undoes that
# decision by blocking every candidate that's better on profit but
# looser on WR. 0.10 still catches a true cell collapse (>10% WR drop
# is not a tradeoff, it's broken) while accepting the intended
# WR/volume swap. The profit-regression cap ($50) remains unchanged —
# a candidate can't regress profit AND WR at once.
MAX_WR_LB_REGRESSION=${MAX_WR_LB_REGRESSION:-0.10}

# Optimizer-side tunables exported to both invocations below.
#
# ORDER_MODE=taker (2026-04-22): reverted from maker after a maker-mode
# sweep produced zero viable CV candidates for most hourly cells — book-
# walk fills are too rare in hourly-market training data for the maker
# simulator to rank anything. Taker simulation with fill_rate_model has
# been producing the live-profitable cells historically. The bot still
# executes as maker; this is just an optimizer-ranking choice.
#
# RISK_TOLERANCE=0.5 biases the composite score toward higher-volume
# candidates (see the knob's docstring above its definition in
# optimize_rr.py). 0.5 is aggressive but still can't pick money-losing
# params — the Wilson LB gate still filters those out.
export ORDER_MODE="${ORDER_MODE:-taker}"
export RISK_TOLERANCE="${RISK_TOLERANCE:-0.7}"

# 2026-04-23: Tier-2 loosening bundle to drive more trade volume.
# Each axis loosens a different kind of strictness; they compound.
# Revert individually by overriding in your environment.
#
#   WILSON_Z: 1.96 (95% CI) → 1.5 (~87% one-sided) — smaller-sample
#     cells can rank. Wilson LB is still applied; the bar is just lower.
#   MIN_TOTAL_VAL_TRADES: 15 → 10 — cells with fewer CV trades qualify.
#     The Wilson LB still punishes low-n candidates that are noisy.
#   HORIZON_THRESHOLD: 0.85 → 0.80 — per-cell horizon accepts buckets
#     with slightly lower historical correctness → wider max_seconds.
#   HORIZON_MIN_BUFFER: 0.10% → 0.07% — horizon analysis considers
#     entries with tighter buffers → more data points per bucket.
#   MIN_CONTRACT_PRICE_FLOOR: 88 → 85 — optimizer considers candidates
#     down to 85c. At 85c the loss asymmetry is 5.7:1 instead of 32:1
#     at 97c, so 85-94c entries are actually *safer* than 97c entries
#     per unit of loss — while firing far more often.
export WILSON_Z="${WILSON_Z:-1.5}"
export MIN_TOTAL_VAL_TRADES="${MIN_TOTAL_VAL_TRADES:-10}"
export HORIZON_THRESHOLD="${HORIZON_THRESHOLD:-0.80}"
export HORIZON_MIN_BUFFER="${HORIZON_MIN_BUFFER:-0.07}"
export MIN_CONTRACT_PRICE_FLOOR="${MIN_CONTRACT_PRICE_FLOOR:-85}"

set -euo pipefail

cd "$(dirname "$0")"

# Prefer the venv's python so cron (which doesn't source shell rc files)
# finds the installed torch + all other deps. Fall back to system python3
# if the venv is gone.
if [ -x .venv/bin/python3 ]; then
    PYTHON=.venv/bin/python3
else
    PYTHON=$(command -v python3)
fi

TS=$(date +%Y%m%d_%H%M%S)
LOG_DIR=data/logs
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/auto_reopt_${TS}.log"

log() { echo "[$(date -Is)] $*" | tee -a "$LOG"; }

log "=== auto_reoptimize starting ==="
log "per-cell caps: MAX_CELL_REGRESSION=\$$MAX_CELL_REGRESSION, MAX_WR_LB_REGRESSION=$MAX_WR_LB_REGRESSION, DEPLOY_ENABLED=$DEPLOY_ENABLED"
log "optimizer: ORDER_MODE=$ORDER_MODE, RISK_TOLERANCE=$RISK_TOLERANCE"
log "loosening: WILSON_Z=$WILSON_Z, MIN_TOTAL_VAL_TRADES=$MIN_TOTAL_VAL_TRADES, HORIZON_THRESHOLD=$HORIZON_THRESHOLD, HORIZON_MIN_BUFFER=$HORIZON_MIN_BUFFER, MIN_CONTRACT_PRICE_FLOOR=$MIN_CONTRACT_PRICE_FLOOR"

# Sanity: GPU must be available, otherwise CPU path OOMs on the 2.6M sweep.
if ! "$PYTHON" -c "import torch; assert torch.cuda.is_available()" >/dev/null 2>&1; then
    log "FATAL: CUDA not available — aborting"
    exit 1
fi

# Step 1: Full sweep → rr_params_preview.json
log "[1/4] Full CUDA sweep..."
BACKEND=cuda PREVIEW=1 CUDA_BATCH_SIZE=2048 \
    "$PYTHON" optimize_rr.py >> "$LOG" 2>&1
log "[1/4] sweep complete"

# Step 2: Re-score live params on same dataset → rr_params_live_on_new_data.json
log "[2/4] Re-scoring current live params on current data..."
BACKEND=cuda ONLY_LIVE=1 MIN_TOTAL_VAL_TRADES=0 \
    "$PYTHON" optimize_rr.py >> "$LOG" 2>&1
log "[2/4] re-score complete"

# Step 3: Per-cell merge gate. Decides for each cell independently
# whether the preview is safely better than current live, then writes
# a merged rr_params_merged.json that takes the preview for cells
# that pass and keeps the current params for cells that don't. This
# replaces the old all-or-nothing gate — one cell regressing used to
# skip the whole deploy even when 14 others improved.
log "[3/4] Running per-cell merge gate..."
DEPLOY_RESULT=$(MAX_CELL_REGRESSION="$MAX_CELL_REGRESSION" \
                MAX_WR_LB_REGRESSION="$MAX_WR_LB_REGRESSION" \
                "$PYTHON" <<'PY'
import json, os, sys

preview = json.load(open("data/rr_params_preview.json"))
live    = json.load(open("data/rr_params_live_on_new_data.json"))

MAX_CELL_REGRESSION  = float(os.environ["MAX_CELL_REGRESSION"])
MAX_WR_LB_REGRESSION = float(os.environ["MAX_WR_LB_REGRESSION"])

all_cells = sorted(set(preview) | set(live))
merged: dict = {}
updates: list = []   # (cell, reason_str)
holds:   list = []   # (cell, reason_str)

def _fmt_delta(p_prof, l_prof, p_wr, l_wr):
    return (f"Δprofit=${p_prof - l_prof:+.2f} "
            f"Δwr_lb={p_wr - l_wr:+.4f}")

for c in all_cells:
    if c in preview and c not in live:
        merged[c] = preview[c]
        updates.append((c, "new cell"))
        continue
    if c in live and c not in preview:
        merged[c] = live[c]
        holds.append((c, "not in preview"))
        continue
    # Both present — per-cell regression check.
    p_prof = preview[c].get("cv_val_profit", 0)
    l_prof = live[c].get("cv_val_profit", 0)
    p_wr   = preview[c].get("cv_wr_lower_bound", 0)
    l_wr   = live[c].get("cv_wr_lower_bound", 0)
    prof_ok = (p_prof - l_prof) >= -MAX_CELL_REGRESSION
    wr_ok   = (p_wr - l_wr)   >= -MAX_WR_LB_REGRESSION
    delta_str = _fmt_delta(p_prof, l_prof, p_wr, l_wr)
    if prof_ok and wr_ok:
        merged[c] = preview[c]
        updates.append((c, delta_str))
    else:
        merged[c] = live[c]
        fail = []
        if not prof_ok:
            fail.append(f"profit regress > ${MAX_CELL_REGRESSION}")
        if not wr_ok:
            fail.append(f"wr_lb regress > {MAX_WR_LB_REGRESSION}")
        holds.append((c, f"{delta_str} [{', '.join(fail)}]"))

# Pretty report
print("per_cell:")
for c in all_cells:
    p_prof = preview.get(c, {}).get("cv_val_profit", 0)
    l_prof = live.get(c, {}).get("cv_val_profit", 0)
    p_wr   = preview.get(c, {}).get("cv_wr_lower_bound", 0)
    l_wr   = live.get(c, {}).get("cv_wr_lower_bound", 0)
    mark = "UPDATE" if any(u[0] == c for u in updates) else "HOLD  "
    print(f"  [{mark}] {c}: live=${l_prof:+.2f}/wr_lb={l_wr:.3f} "
          f"preview=${p_prof:+.2f}/wr_lb={p_wr:.3f} "
          f"Δprofit=${p_prof - l_prof:+.2f} Δwr_lb={p_wr - l_wr:+.4f}")

# Aggregate metrics on the MERGED output — this is what we'd actually deploy.
merged_sum = sum(merged[c].get("cv_val_profit", 0) for c in merged)
live_sum   = sum(live[c].get("cv_val_profit", 0)   for c in live)
print(f"merged_sum=${merged_sum:.2f}")
print(f"live_sum=${live_sum:.2f}")
print(f"delta=${merged_sum - live_sum:+.2f}")
print(f"updated_cells={len(updates)} held_cells={len(holds)}")

if not updates:
    print("DECISION=skip")
    print("reason=no cell passed per-cell gates")
    sys.exit(0)

# Write the merged params for the bash step to copy into place.
with open("data/rr_params_merged.json", "w") as f:
    json.dump(merged, f, indent=2, sort_keys=True)

print("DECISION=deploy")
PY
) || true
echo "$DEPLOY_RESULT" | tee -a "$LOG"

DECISION=$(echo "$DEPLOY_RESULT" | grep "^DECISION=" | head -1 | cut -d= -f2)

# Step 4: Deploy if gated in.
if [ "$DEPLOY_ENABLED" != "1" ]; then
    log "[4/4] Deploy disabled (DEPLOY_ENABLED=$DEPLOY_ENABLED); observation-only run"
elif [ "$DECISION" = "deploy" ]; then
    log "[4/4] Deploying merged params (per-cell winners)..."
    BACKUP="data/rr_params_${TS}_pre_auto_reopt.json"
    cp data/rr_params.json "$BACKUP"
    cp data/rr_params_merged.json data/rr_params.json
    log "backed up previous params → $BACKUP"

    # SIGHUP the bot so it re-reads rr_params.json with no restart.
    # systemctl reload triggers the ExecReload (kill -HUP $MAINPID)
    # wired up in the unit. Outside systemd we fall back to pgrep.
    if systemctl reload kalshi-bot 2>/dev/null; then
        log "SIGHUP sent via systemctl reload kalshi-bot"
    else
        PID=$(pgrep -f "python bot.py" | head -1 || true)
        if [ -n "$PID" ]; then
            kill -HUP "$PID" && log "SIGHUP sent to PID $PID"
        else
            log "WARN: bot process not found; params written but not reloaded"
        fi
    fi
    log "[4/4] deployed"
else
    log "[4/4] skipping deploy (DECISION=$DECISION)"
fi

# Housekeeping: keep logs for 30 days, backups for 14 days
find "$LOG_DIR" -name "auto_reopt_*.log" -mtime +30 -delete 2>/dev/null || true
find data -name "rr_params_*_pre_auto_reopt.json" -mtime +14 -delete 2>/dev/null || true

log "=== auto_reoptimize done ==="
