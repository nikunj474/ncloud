#!/usr/bin/env bash
set -euo pipefail
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_tablet_demo}"
PID_DIR="$DATA_ROOT/pids"

for f in coord node1 node2 node3; do
  if [[ -f "$PID_DIR/$f.pid" ]]; then
    kill "$(cat "$PID_DIR/$f.pid")" 2>/dev/null || true
  fi
done

pkill -f '/coordinator/coordinator --port 7010' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6502' 2>/dev/null || true

echo "[multi] stopped cluster"
