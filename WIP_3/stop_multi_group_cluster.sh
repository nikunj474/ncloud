#!/usr/bin/env bash
set -euo pipefail
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_group_demo}"
PID_DIR="$DATA_ROOT/pids"
for f in coord node1 node2 node3 node4; do
  if [[ -f "$PID_DIR/$f.pid" ]]; then
    kill "$(cat "$PID_DIR/$f.pid")" 2>/dev/null || true
  fi
done
pkill -f '/coordinator/coordinator --port 7110' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7502' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7503' 2>/dev/null || true
echo "[multi-group] stopped cluster"
