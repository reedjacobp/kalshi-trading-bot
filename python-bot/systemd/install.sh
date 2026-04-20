#!/bin/bash
# One-time install of the kalshi-bot + kalshi-dashboard systemd services.
# Run this once; after that `systemctl start/stop/status` manages both.

set -euo pipefail

cd "$(dirname "$0")"

echo "==> Copying unit files → /etc/systemd/system/ (needs sudo)"
sudo cp kalshi-bot.service       /etc/systemd/system/kalshi-bot.service
sudo cp kalshi-dashboard.service /etc/systemd/system/kalshi-dashboard.service
sudo chmod 644 /etc/systemd/system/kalshi-bot.service
sudo chmod 644 /etc/systemd/system/kalshi-dashboard.service

echo "==> Reloading systemd"
sudo systemctl daemon-reload

echo "==> Enabling both services on boot"
sudo systemctl enable kalshi-bot kalshi-dashboard

echo
echo "Install complete. Services are ENABLED but NOT started yet — you still"
echo "have a bot running from a terminal. Cutover sequence:"
echo
echo "  1) Stop the terminal bot  (Ctrl-C in the pts session running it)"
echo "  2) sudo systemctl start kalshi-bot"
echo "  3) sudo systemctl start kalshi-dashboard"
echo "  4) sudo systemctl status kalshi-bot kalshi-dashboard    # confirm"
echo
echo "Dashboard listens on http://localhost:5000 (LAN-only by default)."
echo "To expose to your whole home network, edit"
echo "  /etc/systemd/system/kalshi-dashboard.service"
echo "and change  Environment=HOST=127.0.0.1  ->  HOST=0.0.0.0"
echo "then:  sudo systemctl daemon-reload && sudo systemctl restart kalshi-dashboard"
echo
echo "Daily cheatsheet:"
echo "  systemctl status kalshi-bot kalshi-dashboard"
echo "  journalctl -u kalshi-bot -f"
echo "  journalctl -u kalshi-dashboard -f"
echo "  sudo systemctl restart kalshi-bot       # full bot restart"
echo "  sudo systemctl reload  kalshi-bot       # hot-reload rr_params.json (no restart)"
echo
echo "If you change dashboard code, rebuild + restart:"
echo "  cd .. && npm run build && sudo systemctl restart kalshi-dashboard"
