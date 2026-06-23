#!/usr/bin/env bash
# Install as user systemd units: a 15-minute pull timer and a Tailscale-bound
# dashboard. No root required.
#
# Prerequisite: the Netlify CLI, authenticated.
#   npm install -g netlify-cli   # or use: npx netlify-cli
#   netlify login
#
# Usage:
#   ./install.sh        install, enable, start
#   ./install.sh stop   stop and disable
set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$REPO/netlify-logs.py"
UNITDIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
PORT="${NETLIFY_LOGS_PORT:-8800}"
WINDOW="${NETLIFY_LOGS_WINDOW:-20}"

if [ "${1:-install}" = "stop" ]; then
  systemctl --user disable --now netlify-logs.service netlify-logs-pull.timer 2>/dev/null || true
  echo "stopped and disabled."
  exit 0
fi

command -v python3 >/dev/null || { echo "python3 required"; exit 1; }
if [ ! -f "$HOME/.config/netlify/config.json" ] && [ -z "${NETLIFY_AUTH_TOKEN:-}" ]; then
  echo "Netlify auth not found. Install the Netlify CLI and run: netlify login"
  exit 1
fi

mkdir -p "$UNITDIR"

cat > "$UNITDIR/netlify-logs-pull.service" <<EOF
[Unit]
Description=Pull Netlify access logs
After=network-online.target

[Service]
Type=oneshot
ExecStart=$SCRIPT pull --minutes $WINDOW --quiet
EOF

cat > "$UNITDIR/netlify-logs-pull.timer" <<EOF
[Unit]
Description=Pull Netlify access logs every 15 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=15min
Persistent=true

[Install]
WantedBy=timers.target
EOF

cat > "$UNITDIR/netlify-logs.service" <<EOF
[Unit]
Description=Netlify access-log dashboard
After=network-online.target tailscaled.service

[Service]
ExecStart=$SCRIPT serve --host tailscale --port $PORT --minutes $WINDOW
Restart=on-failure
RestartSec=10

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now netlify-logs-pull.timer
systemctl --user enable --now netlify-logs.service
loginctl enable-linger "$USER" 2>/dev/null || \
  echo "note: run 'sudo loginctl enable-linger $USER' to keep units running while logged out."

TSIP="$(tailscale ip -4 2>/dev/null | head -1 || true)"
echo
echo "installed."
echo "  timer:     systemctl --user list-timers netlify-logs-pull.timer"
echo "  dashboard: http://${TSIP:-<tailscale-ip>}:$PORT"
echo "  logs:      journalctl --user -u netlify-logs.service -f"
