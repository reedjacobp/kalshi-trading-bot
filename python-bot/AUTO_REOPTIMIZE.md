# Nightly auto-reoptimize

Runs the CUDA parameter sweep nightly and deploys the result only if it's
meaningfully better than what the bot is currently using. No manual step.

## What it does

1. **Full sweep** (~40 min on the 3070): writes `data/rr_params_preview.json`.
2. **Re-scores the current live params** on the same dataset as the sweep:
   writes `data/rr_params_live_on_new_data.json`. This is the apples-to-apples
   control — removes the "live params' stored score is from an older dataset"
   confound that bit us on 2026-04-18.
3. **Deploy gate**:
   - `sum(preview.cv_val_profit) - sum(live_on_new.cv_val_profit) >= MIN_IMPROVEMENT`
     (default: **$50**)
   - No individual cell regresses by more than `MAX_CELL_REGRESSION`
     (default: **-$50**)
4. If gates pass: back up the current `rr_params.json` with a timestamp,
   copy preview over live, send `SIGHUP` to the bot process.
5. Bot's `SIGHUP` handler calls `reload_rr_params()` which re-reads the
   file and re-applies the cell safety gate — **no restart, no interrupted
   positions**.

If the gate fails (most nights it probably will — the current params are
usually already close to optimal), the live file is untouched.

## One-time setup

### 1. Install the cron job

```
crontab -e
```

Add this line (runs at 03:00 PT = 10:00 UTC):

```
0 10 * * * /home/jake/workspaces/kalshi-trading-bot/python-bot/auto_reoptimize.sh > /dev/null 2>&1
```

The script writes its own log under `data/logs/auto_reopt_*.log` — you don't
need cron to capture output.

### 2. Restart the bot ONCE

The `SIGHUP` reload handler was added to `bot.py` in this change. The bot
needs a single restart to pick it up. After that, future deploys hot-reload
without restart.

## Manual operations

### Run the sweep right now (adopts any new ticks since last run)

```
./auto_reoptimize.sh
```

### Force deploy with different thresholds

```
MIN_IMPROVEMENT=100 MAX_CELL_REGRESSION=-100 ./auto_reoptimize.sh
```

### Check the last run

```
ls -lt data/logs/auto_reopt_*.log | head -3
tail -40 $(ls -t data/logs/auto_reopt_*.log | head -1)
```

### Rollback

Backups of the previous live file are kept 14 days as
`rr_params_<timestamp>_pre_auto_reopt.json`. To roll back:

```
cp data/rr_params_YYYY-MM-DD_HHMMSS_pre_auto_reopt.json data/rr_params.json
kill -HUP $(pgrep -f "python bot.py")
```

## Safety notes

- Gates are conservative by default: a run that improves by only $30 won't
  deploy. Tune `MIN_IMPROVEMENT` down if the cadence is too slow.
- The script never touches `rr_params.json` unless both gates pass.
- Backup files accumulate; the script prunes anything older than 14 days.
- Optimizer logs keep 30 days then auto-delete.
- If CUDA is unavailable (driver crash, GPU busy), the script exits
  without touching anything.
