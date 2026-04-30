#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

COORD_PORT="${COORD_PORT:-6000}"
PRIMARY_CLIENT_PORT="${PRIMARY_CLIENT_PORT:-5500}"
SECONDARY_CLIENT_PORT="${SECONDARY_CLIENT_PORT:-5501}"
PRIMARY_REPL_PORT="${PRIMARY_REPL_PORT:-5600}"
SECONDARY_REPL_PORT="${SECONDARY_REPL_PORT:-5601}"
TABLET_NAME="${TABLET_NAME:-tablet0}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_ha_cluster}"
CFG="$ROOT_DIR/coordinator/coordinator.conf"
PID_DIR="$DATA_ROOT/pids"
mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$PID_DIR"

pkill -f '/coordinator/coordinator --port ' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5501' 2>/dev/null || true
rm -f coordinator.log node1.log node2.log

cat > "$CFG" <<CFGEOF
# node <id> <host> <kv_port> <repl_port>
node node1 127.0.0.1 ${PRIMARY_CLIENT_PORT} ${PRIMARY_REPL_PORT}
node node2 127.0.0.1 ${SECONDARY_CLIENT_PORT} ${SECONDARY_REPL_PORT}

# tablet <tablet_id> <row_start> <row_end> <replica1> <replica2> ...
tablet ${TABLET_NAME} "" "" node1 node2
CFGEOF

echo "[ha] starting secondary node2"
./kvstore/kvserver --port "$SECONDARY_CLIENT_PORT" --data "$DATA_ROOT/node2" --tablet "$TABLET_NAME" --node-id node2 --repl-port "$SECONDARY_REPL_PORT" > node2.log 2>&1 &
echo $! > "$PID_DIR/node2.pid"
sleep 1

echo "[ha] starting primary node1"
./kvstore/kvserver --port "$PRIMARY_CLIENT_PORT" --data "$DATA_ROOT/node1" --tablet "$TABLET_NAME" --node-id node1 --repl-port "$PRIMARY_REPL_PORT" --replica node2@127.0.0.1:${SECONDARY_REPL_PORT} > node1.log 2>&1 &
echo $! > "$PID_DIR/node1.pid"
sleep 1

echo "[ha] starting coordinator"
./coordinator/coordinator --port "$COORD_PORT" --config "$CFG" > coordinator.log 2>&1 &
echo $! > "$PID_DIR/coord.pid"
sleep 1

echo "[ha] cluster ready"
echo "  coordinator: $COORD_PORT"
echo "  node1 kv/repl: $PRIMARY_CLIENT_PORT / $PRIMARY_REPL_PORT"
echo "  node2 kv/repl: $SECONDARY_CLIENT_PORT / $SECONDARY_REPL_PORT"
echo "  logs: coordinator.log node1.log node2.log"
