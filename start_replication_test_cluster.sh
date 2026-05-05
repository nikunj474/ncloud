#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

PRIMARY_CLIENT_PORT="${PRIMARY_CLIENT_PORT:-5500}"
SECONDARY_CLIENT_PORT="${SECONDARY_CLIENT_PORT:-5501}"
PRIMARY_REPL_PORT="${PRIMARY_REPL_PORT:-5600}"
SECONDARY_REPL_PORT="${SECONDARY_REPL_PORT:-5601}"
TABLET_NAME="${TABLET_NAME:-tabletA}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_repl_cluster}"

PRIMARY_DATA="$DATA_ROOT/node1"
SECONDARY_DATA="$DATA_ROOT/node2"
PRIMARY_LOG="$ROOT_DIR/node1.log"
SECONDARY_LOG="$ROOT_DIR/node2.log"
PID_DIR="$DATA_ROOT/pids"
mkdir -p "$PRIMARY_DATA" "$SECONDARY_DATA" "$PID_DIR"

pkill -f "kvstore/kvserver --port ${PRIMARY_CLIENT_PORT}" 2>/dev/null || true
pkill -f "kvstore/kvserver --port ${SECONDARY_CLIENT_PORT}" 2>/dev/null || true
rm -f "$PRIMARY_LOG" "$SECONDARY_LOG"

echo "[cluster] starting secondary on client:${SECONDARY_CLIENT_PORT} repl:${SECONDARY_REPL_PORT}"
./kvstore/kvserver \
  --port "$SECONDARY_CLIENT_PORT" \
  --data "$SECONDARY_DATA" \
  --tablet "$TABLET_NAME" \
  --node-id node2 \
  --repl-port "$SECONDARY_REPL_PORT" \
  >"$SECONDARY_LOG" 2>&1 &
echo $! > "$PID_DIR/node2.pid"

sleep 1

echo "[cluster] starting primary on client:${PRIMARY_CLIENT_PORT} repl:${PRIMARY_REPL_PORT} -> replica 127.0.0.1:${SECONDARY_REPL_PORT}"
./kvstore/kvserver \
  --port "$PRIMARY_CLIENT_PORT" \
  --data "$PRIMARY_DATA" \
  --tablet "$TABLET_NAME" \
  --node-id node1 \
  --repl-port "$PRIMARY_REPL_PORT" \
  --replica node2@127.0.0.1:${SECONDARY_REPL_PORT} \
  >"$PRIMARY_LOG" 2>&1 &
echo $! > "$PID_DIR/node1.pid"

sleep 1

echo "[cluster] cluster started"
echo "  primary client port:   ${PRIMARY_CLIENT_PORT}"
echo "  secondary client port: ${SECONDARY_CLIENT_PORT}"
echo "  primary repl port:     ${PRIMARY_REPL_PORT}"
echo "  secondary repl port:   ${SECONDARY_REPL_PORT}"
echo "  logs: ${PRIMARY_LOG}, ${SECONDARY_LOG}"
echo "  stop with: ./stop_replication_test_cluster.sh"
