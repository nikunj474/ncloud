#!/usr/bin/env bash
set -euo pipefail
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_group_demo}"
SMTP_PORT="${SMTP_PORT:-2525}"
LB_PORT="${LB_PORT:-8088}"
PID_DIR="$DATA_ROOT/pids"
for f in coord smtp node1 node2 node3 node4 fe1 fe2 fe3 lb; do
  if [[ -f "$PID_DIR/$f.pid" ]]; then
    kill "$(cat "$PID_DIR/$f.pid")" 2>/dev/null || true
  fi
done
pkill -f '/coordinator/coordinator --port 7110' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7502' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7503' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8090' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8091' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8092' 2>/dev/null || true
pkill -f "frontend_lb/frontend_lb --port ${LB_PORT}" 2>/dev/null || true
pkill -f "smtp_server/smtp_server --port ${SMTP_PORT}" 2>/dev/null || true
echo "[multi-group] stopped cluster"
