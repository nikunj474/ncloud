#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$ROOT_DIR}"
cd "$PROJECT_DIR"

COORD_PORT="${COORD_PORT:-7110}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_group_demo}"
CFG_SRC="$PROJECT_DIR/coordinator/coordinator_multi_group_demo.conf"
CFG_DST="$PROJECT_DIR/coordinator_multi_group_demo.conf"
PID_DIR="$DATA_ROOT/pids"

mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3" "$DATA_ROOT/node4" "$PID_DIR"

pkill -f '/coordinator/coordinator --port 7110' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7502' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7503' 2>/dev/null || true
rm -f coordinator_multi_group.log mg_node1.log mg_node2.log mg_node3.log mg_node4.log

cp "$CFG_SRC" "$CFG_DST"

echo "[multi-group] starting node1"
./kvstore/kvserver --port 7500 --data "$DATA_ROOT/node1" \
  --tablet tabletA:a:f --tablet tabletC:m:r --tablet tabletD:s:zzzzz \
  --node-id node1 --repl-port 7600 > mg_node1.log 2>&1 &
echo $! > "$PID_DIR/node1.pid"
sleep 1

echo "[multi-group] starting node2"
./kvstore/kvserver --port 7501 --data "$DATA_ROOT/node2" \
  --tablet tabletA:a:f --tablet tabletB:g:l --tablet tabletD:s:zzzzz \
  --node-id node2 --repl-port 7601 > mg_node2.log 2>&1 &
echo $! > "$PID_DIR/node2.pid"
sleep 1

echo "[multi-group] starting node3"
./kvstore/kvserver --port 7502 --data "$DATA_ROOT/node3" \
  --tablet tabletA:a:f --tablet tabletB:g:l --tablet tabletC:m:r \
  --node-id node3 --repl-port 7602 > mg_node3.log 2>&1 &
echo $! > "$PID_DIR/node3.pid"
sleep 1

echo "[multi-group] starting node4"
./kvstore/kvserver --port 7503 --data "$DATA_ROOT/node4" \
  --tablet tabletB:g:l --tablet tabletC:m:r --tablet tabletD:s:zzzzz \
  --node-id node4 --repl-port 7603 > mg_node4.log 2>&1 &
echo $! > "$PID_DIR/node4.pid"
sleep 1

echo "[multi-group] starting coordinator"
./coordinator/coordinator --port "$COORD_PORT" --config "$CFG_DST" > coordinator_multi_group.log 2>&1 &
echo $! > "$PID_DIR/coord.pid"
sleep 4

echo "[multi-group] cluster ready"
echo "  coordinator: $COORD_PORT"
echo "  node1: 7500 / 7600"
echo "  node2: 7501 / 7601"
echo "  node3: 7502 / 7602"
echo "  node4: 7503 / 7603"
echo "  logs: coordinator_multi_group.log mg_node1.log mg_node2.log mg_node3.log mg_node4.log"
