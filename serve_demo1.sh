#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

KV_PORT=5050
FE_PORT=8090
COORD_PORT=6000
DATA_DIR=/tmp/pc_demo1_data

echo "== Killing old PennCloud processes =="
pkill -f "/kvstore/kvserver" 2>/dev/null || true
pkill -f "/frontend/feserver" 2>/dev/null || true
pkill -f "/coordinator/coordinator" 2>/dev/null || true
sleep 1

echo "== Building all components =="
make -C kvstore
make -C frontend
make -C coordinator

echo "== Starting live Demo I services =="
mkdir -p "$DATA_DIR"
./kvstore/kvserver --port $KV_PORT --data "$DATA_DIR" --tablet main > kvstore.log 2>&1 &
KV_PID=$!
sleep 1

./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT --id fe1 > frontend.log 2>&1 &
FE_PID=$!
sleep 1

./coordinator/coordinator --port $COORD_PORT --config ./coordinator/coordinator.conf > coordinator.log 2>&1 &
COORD_PID=$!
sleep 1

echo
echo "== Live demo is up =="
echo "Open: http://127.0.0.1:$FE_PORT"
echo "KV PID: $KV_PID"
echo "Frontend PID: $FE_PID"
echo "Coordinator PID: $COORD_PID"
echo
echo "Logs:"
echo "  tail -f kvstore.log"
echo "  tail -f frontend.log"
echo "  tail -f coordinator.log"
echo
echo "To stop everything:"
echo "  pkill -f '/kvstore/kvserver'"
echo "  pkill -f '/frontend/feserver'"
echo "  pkill -f '/coordinator/coordinator'"
