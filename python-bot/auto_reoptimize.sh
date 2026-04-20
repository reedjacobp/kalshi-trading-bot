#!/bin/bash
# Nightly auto-reoptimize + deploy gate.
#
# Flow:
#   1. Run full CUDA sweep → data/rr_params_preview.json
#   2. Re-score current live params on the same dataset → data/rr_params_live_on_new_data.json
#   3. Compare total CV profit. Deploy only if preview beats live by MIN_IMPROVEMENT,
#      AND no cell regresses by more than MAX_CELL_REGRESSION.
#   4. If deployed, SIGHUP the bot so it picks up new params live (no restart).
#
# Called from cron. Exits 0 on success (even if we choose not to deploy).

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

# Sanity: GPU must be available, otherwise CPU path OOMs on the 2.6M sweep.
if ! "$PYTHON" -c "import torch; assert torch.cuda.is_available()" >/dev/null 2>&1; then
    log "FATAL: CUDA not available — aborting"
    exit 1
fi

# Step 1: Full sweep → rr_params_preview.json
log "[1/3] Full CUDA sweep..."
BACKEND=cuda PREVIEW=1 CUDA_BATCH_SIZE=2048 \
    "$PYTHON" optimize_rr.py >> "$LOG" 2>&1
log "[1/3] sweep complete"

# Step 2: Re-score live params on same dataset → rr_params_live_on_new_data.json
log "[2/3] Re-scoring current live params on current data..."
BACKEND=cuda ONLY_LIVE=1 MIN_TOTAL_VAL_TRADES=0 \
    "$PYTHON" optimize_rr.py >> "$LOG" 2>&1
log "[2/3] re-score complete"

# Step 3: Compare + conditional deploy
log "[3/3] Comparing preview vs live-on-new-data..."
DEPLOY_RESULT=$("$PYTHON" <<'PY'
import json, shutil, sys, datetime as dt
from pathlib import Path

preview = json.load(open("data/rr_params_preview.json"))
live_on_new = json.load(open("data/rr_params_live_on_new_data.json"))

common = sorted(set(preview) & set(live_on_new))
p_sum = sum(preview[c].get("cv_val_profit", 0) for c in common)
l_sum = sum(live_on_new[c].get("cv_val_profit", 0) for c in common)
delta = p_sum - l_sum

print(f"preview_sum={p_sum:.2f}")
print(f"live_sum={l_sum:.2f}")
print(f"delta={delta:.2f}")

# Gates
MIN_IMPROVEMENT = float((__import__("os").environ.get("MIN_IMPROVEMENT", "50")))
MAX_CELL_REGRESSION = float((__import__("os").environ.get("MAX_CELL_REGRESSION", "-50")))

regressions = []
for c in common:
    cell_delta = preview[c].get("cv_val_profit", 0) - live_on_new[c].get("cv_val_profit", 0)
    if cell_delta < MAX_CELL_REGRESSION:
        regressions.append((c, round(cell_delta, 2)))

if delta < MIN_IMPROVEMENT:
    print(f"DECISION=skip")
    print(f"reason=improvement_below_threshold: {delta:.2f} < {MIN_IMPROVEMENT}")
    sys.exit(0)
if regressions:
    print(f"DECISION=skip")
    print(f"reason=cell_regressions: {regressions}")
    sys.exit(0)

# All gates passed — deploy
backup = Path(f"data/rr_params_{dt.datetime.now():%Y-%m-%d_%H%M%S}_pre_auto_reopt.json")
shutil.copy2("data/rr_params.json", backup)
shutil.copy2("data/rr_params_preview.json", "data/rr_params.json")
print(f"DECISION=deploy")
print(f"backup={backup}")
PY
) || true
echo "$DEPLOY_RESULT" | tee -a "$LOG"

if echo "$DEPLOY_RESULT" | grep -q "^DECISION=deploy"; then
    log "Deployed new params. Signaling bot (SIGHUP) to hot-reload..."
    BOT_PID=$(pgrep -f "python bot.py" | head -1 || true)
    if [ -n "${BOT_PID:-}" ]; then
        kill -HUP "$BOT_PID" || log "SIGHUP failed (bot may be dead)"
        log "SIGHUP sent to PID $BOT_PID"
    else
        log "No running bot found — deployment will take effect on next restart"
    fi
else
    log "Not deployed (see DECISION above)"
fi

# Housekeeping: keep logs for 30 days, backups for 14 days
find "$LOG_DIR" -name "auto_reopt_*.log" -mtime +30 -delete 2>/dev/null || true
find data -name "rr_params_*_pre_auto_reopt.json" -mtime +14 -delete 2>/dev/null || true

log "=== auto_reoptimize done ==="
