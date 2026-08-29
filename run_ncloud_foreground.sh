#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

LB_PORT="${LB_PORT:-8088}"
KEEP_404_STUB_AFTER_CTRL_C="${KEEP_404_STUB_AFTER_CTRL_C:-1}"
STUB_PORTS_AFTER_CTRL_C="${STUB_PORTS_AFTER_CTRL_C:-$LB_PORT 8090 8091 8092}"
CLEANED_UP=0

cleanup() {
  if [[ "$CLEANED_UP" == "1" ]]; then
    return
  fi
  CLEANED_UP=1
  trap '' INT TERM HUP  # ignore further signals during cleanup; EXIT stays for subshells
  trap - EXIT
  echo
  echo "[foreground] stopping complete NCloud system..."
  KEEP_404_STUB="$KEEP_404_STUB_AFTER_CTRL_C" \
    STUB_PORT="$LB_PORT" \
    STUB_PORTS="$STUB_PORTS_AFTER_CTRL_C" \
    "$ROOT_DIR/stop_multi_tablet_cluster.sh" || true
  if [[ "$KEEP_404_STUB_AFTER_CTRL_C" != "0" ]]; then
    for port in $STUB_PORTS_AFTER_CTRL_C; do
      echo "[foreground] 404 stub is listening on http://127.0.0.1:$port"
    done
  fi
  echo "[foreground] stopped"
}

trap cleanup INT TERM HUP EXIT

echo "[foreground] starting complete NCloud system..."
"$ROOT_DIR/start_multi_tablet_cluster.sh"

cat <<'EOF'

[foreground] NCloud is running.
[foreground] Main URL:  http://127.0.0.1:8088
[foreground] Admin:     http://127.0.0.1:8088/admin
[foreground]
[foreground] Press Ctrl-C in this terminal to stop the system and leave 404 stubs.
EOF

while true; do
  sleep 1
done
