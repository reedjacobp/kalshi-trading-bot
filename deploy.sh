#!/bin/bash
# ============================================================
# Kalshi Trading Bot — VPS Deployment Script
# ============================================================
# Run this on a fresh Ubuntu/Debian VPS after SSH-ing in:
#   ssh root@<your-vps-ip>
#   bash deploy.sh
#
# Prerequisites (do these BEFORE running this script):
#   1. Copy your Kalshi private key to the VPS:
#      scp ~/.config/kalshiqt/private_key.pem root@<your-vps-ip>:/tmp/kalshi_private_key.pem
#
#   2. Copy your .env file to the VPS:
#      scp python-bot/.env root@<your-vps-ip>:/tmp/kalshi_bot.env
# ============================================================

set -euo pipefail

BOT_USER="kalshi"
BOT_DIR="/home/${BOT_USER}/kalshi-trading-bot"
DATA_DIR="/home/${BOT_USER}/data"
REPO_URL="https://github.com/reedjacobp/kalshi-trading-bot.git"

echo "=== [1/6] Installing system dependencies ==="
apt update && apt install -y python3 python3-pip python3-venv git

echo "=== [2/6] Creating bot user ==="
if id "$BOT_USER" &>/dev/null; then
    echo "User '$BOT_USER' already exists, skipping."
else
    useradd -m -s /bin/bash "$BOT_USER"
    echo "Created user '$BOT_USER'."
fi

echo "=== [3/6] Cloning repo ==="
if [ -d "$BOT_DIR" ]; then
    echo "Repo already exists, pulling latest..."
    sudo -u "$BOT_USER" git -C "$BOT_DIR" pull
else
    sudo -u "$BOT_USER" git clone "$REPO_URL" "$BOT_DIR"
fi

echo "=== [4/6] Setting up Python venv and dependencies ==="
sudo -u "$BOT_USER" python3 -m venv "${BOT_DIR}/python-bot/venv"
sudo -u "$BOT_USER" "${BOT_DIR}/python-bot/venv/bin/pip" install --upgrade pip
sudo -u "$BOT_USER" "${BOT_DIR}/python-bot/venv/bin/pip" install -r "${BOT_DIR}/python-bot/requirements.txt"

echo "=== [5/7] Placing secrets and configuring data directory ==="
# Create data directory for tick recordings
sudo -u "$BOT_USER" mkdir -p "${DATA_DIR}/ticks"
echo "Data directory: ${DATA_DIR}"

# .env file
if [ -f /tmp/kalshi_bot.env ]; then
    cp /tmp/kalshi_bot.env "${BOT_DIR}/python-bot/.env"
    # Update DATA_DIR to VPS path
    if grep -q "^DATA_DIR=" "${BOT_DIR}/python-bot/.env"; then
        sed -i "s|^DATA_DIR=.*|DATA_DIR=${DATA_DIR}|" "${BOT_DIR}/python-bot/.env"
    else
        echo "DATA_DIR=${DATA_DIR}" >> "${BOT_DIR}/python-bot/.env"
    fi
    chown "$BOT_USER":"$BOT_USER" "${BOT_DIR}/python-bot/.env"
    chmod 600 "${BOT_DIR}/python-bot/.env"
    rm /tmp/kalshi_bot.env
    echo ".env installed (DATA_DIR set to ${DATA_DIR})."
else
    echo "WARNING: /tmp/kalshi_bot.env not found. Copy it manually before starting the bot."
fi

echo "=== [6/7] Placing private key ==="
KEY_DIR="/home/${BOT_USER}/.config/kalshiqt"
if [ -f /tmp/kalshi_private_key.pem ]; then
    sudo -u "$BOT_USER" mkdir -p "$KEY_DIR"
    cp /tmp/kalshi_private_key.pem "${KEY_DIR}/private_key.pem"
    chown "$BOT_USER":"$BOT_USER" "${KEY_DIR}/private_key.pem"
    chmod 600 "${KEY_DIR}/private_key.pem"
    rm /tmp/kalshi_private_key.pem
    echo "Private key installed."
else
    echo "WARNING: /tmp/kalshi_private_key.pem not found. Copy it manually before starting the bot."
fi

echo "=== [7/7] Creating systemd service ==="
cat > /etc/systemd/system/kalshi-bot.service << EOF
[Unit]
Description=Kalshi Trading Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${BOT_USER}
WorkingDirectory=${BOT_DIR}/python-bot
ExecStart=${BOT_DIR}/python-bot/venv/bin/python bot.py --live
Restart=always
RestartSec=10
EnvironmentFile=${BOT_DIR}/python-bot/.env

# Safety limits
MemoryMax=512M
CPUQuota=80%

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=kalshi-bot

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kalshi-bot
systemctl start kalshi-bot

echo ""
echo "============================================"
echo "  Deployment complete!"
echo "============================================"
echo ""
echo "Useful commands:"
echo "  systemctl status kalshi-bot    # check if running"
echo "  journalctl -u kalshi-bot -f    # live logs"
echo "  systemctl restart kalshi-bot   # restart after changes"
echo "  systemctl stop kalshi-bot      # stop the bot"
echo ""
echo "To update the bot later:"
echo "  cd ${BOT_DIR} && sudo -u ${BOT_USER} git pull"
echo "  systemctl restart kalshi-bot"
echo ""
echo "To sync data back to your local machine:"
echo "  rsync -avz root@<vps-ip>:${DATA_DIR}/ /mnt/d/datasets/prediction-market-analysis/"
echo ""
