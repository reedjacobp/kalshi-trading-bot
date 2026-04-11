#!/bin/bash
# ============================================================
# Bidirectional data sync between local machine and VPS
# ============================================================
# Usage:
#   ./sync-data.sh <vps-ip>
#
# Syncs the ticks/ directory and key parquet files both ways:
#   - VPS tick data → local (bot-generated live recordings)
#   - Local parquet data → VPS (for running optimizers on VPS)
#
# Safe to run repeatedly — rsync only transfers changes.
# ============================================================

set -euo pipefail

if [ -z "${1:-}" ]; then
    echo "Usage: ./sync-data.sh <vps-ip>"
    echo "  e.g. ./sync-data.sh 123.45.67.89"
    exit 1
fi

VPS_IP="$1"
VPS_USER="root"
VPS_DATA="/home/kalshi/data"
LOCAL_DATA="/mnt/d/datasets/prediction-market-analysis"

echo "=== Pulling tick data from VPS → local ==="
rsync -avz --progress "${VPS_USER}@${VPS_IP}:${VPS_DATA}/ticks/" "${LOCAL_DATA}/ticks/"

echo ""
echo "=== Pushing parquet files from local → VPS ==="
# Only sync the files the optimizer/backtester actually needs (not the 34GB archive)
rsync -avz --progress \
    "${LOCAL_DATA}/crypto_markets_extended.parquet" \
    "${LOCAL_DATA}/crypto_markets_filtered.parquet" \
    "${LOCAL_DATA}/crypto_trades_filtered.parquet" \
    "${VPS_USER}@${VPS_IP}:${VPS_DATA}/"

echo ""
echo "=== Sync complete ==="
echo "Local ticks: $(ls ${LOCAL_DATA}/ticks/*.csv 2>/dev/null | wc -l) days"
echo "VPS ticks:   $(ssh ${VPS_USER}@${VPS_IP} "ls ${VPS_DATA}/ticks/*.csv 2>/dev/null | wc -l") days"
