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

echo "=== [1/8] Installing system dependencies ==="
apt update && apt install -y python3 python3-pip python3-venv git curl

# Install Node.js 20 LTS (for dashboard)
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
    apt install -y nodejs
    echo "Node.js $(node --version) installed."
else
    echo "Node.js $(node --version) already installed."
fi

echo "=== [2/8] Creating bot user ==="
if id "$BOT_USER" &>/dev/null; then
    echo "User '$BOT_USER' already exists, skipping."
else
    useradd -m -s /bin/bash "$BOT_USER"
    echo "Created user '$BOT_USER'."
fi

echo "=== [3/8] Cloning repo ==="
if [ -d "$BOT_DIR" ]; then
    echo "Repo already exists, pulling latest..."
    sudo -u "$BOT_USER" git -C "$BOT_DIR" pull
else
    sudo -u "$BOT_USER" git clone "$REPO_URL" "$BOT_DIR"
fi

echo "=== [4/8] Setting up Python venv and dependencies ==="
sudo -u "$BOT_USER" python3 -m venv "${BOT_DIR}/python-bot/venv"
sudo -u "$BOT_USER" "${BOT_DIR}/python-bot/venv/bin/pip" install --upgrade pip
sudo -u "$BOT_USER" "${BOT_DIR}/python-bot/venv/bin/pip" install -r "${BOT_DIR}/python-bot/requirements.txt"

echo "=== [5/8] Building dashboard ==="
cd "$BOT_DIR"
sudo -u "$BOT_USER" npm install
sudo -u "$BOT_USER" npm run build
cd /

echo "=== [6/8] Placing secrets and configuring data directory ==="
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

echo "=== [7/8] Placing private key ==="
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

echo "=== [8/8] Creating systemd services ==="

# --- Trading bot service ---
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

# --- Dashboard service ---
cat > /etc/systemd/system/kalshi-dashboard.service << EOF
[Unit]
Description=Kalshi Dashboard
After=kalshi-bot.service
Wants=kalshi-bot.service

[Service]
Type=simple
User=${BOT_USER}
WorkingDirectory=${BOT_DIR}
ExecStart=/usr/bin/node dist/index.cjs
Restart=always
RestartSec=10
Environment=NODE_ENV=production
Environment=PORT=5000
Environment=BOT_SSE_HOST=127.0.0.1
Environment=BOT_SSE_PORT=5050

# Safety limits
MemoryMax=256M

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=kalshi-dashboard

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kalshi-bot kalshi-dashboard
systemctl start kalshi-bot kalshi-dashboard

echo ""
echo "============================================"
echo "  Deployment complete!"
echo "============================================"
echo ""
echo "Useful commands:"
echo "  systemctl status kalshi-bot        # check bot status"
echo "  systemctl status kalshi-dashboard  # check dashboard status"
echo "  journalctl -u kalshi-bot -f        # live bot logs"
echo "  journalctl -u kalshi-dashboard -f  # live dashboard logs"
echo "  systemctl restart kalshi-bot       # restart bot"
echo "  systemctl restart kalshi-dashboard # restart dashboard"
echo ""
echo "Dashboard: http://<this-server-ip>:5000"
echo ""
echo "To update the bot later:"
echo "  cd ${BOT_DIR} && sudo -u ${BOT_USER} git pull"
echo "  sudo -u ${BOT_USER} npm run build"
echo "  systemctl restart kalshi-bot kalshi-dashboard"
echo ""
echo "To sync data back to your local machine:"
echo "  rsync -avz root@<vps-ip>:${DATA_DIR}/ /mnt/d/datasets/prediction-market-analysis/"
echo ""
