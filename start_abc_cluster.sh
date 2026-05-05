#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

COORD_PORT="${COORD_PORT:-6000}"
NODE1_CLIENT_PORT="${NODE1_CLIENT_PORT:-5500}"
NODE2_CLIENT_PORT="${NODE2_CLIENT_PORT:-5501}"
NODE3_CLIENT_PORT="${NODE3_CLIENT_PORT:-5502}"
NODE1_REPL_PORT="${NODE1_REPL_PORT:-5600}"
NODE2_REPL_PORT="${NODE2_REPL_PORT:-5601}"
NODE3_REPL_PORT="${NODE3_REPL_PORT:-5602}"
TABLET_NAME="${TABLET_NAME:-tablet0}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_abc_cluster}"
CFG="$ROOT_DIR/coordinator/coordinator_abc.conf"
PID_DIR="$DATA_ROOT/pids"
mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3" "$PID_DIR"

# Kill old processes more aggressively so stale coordinators do not survive.
pkill -f '/coordinator/coordinator' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5502' 2>/dev/null || true
lsof -tiTCP:${COORD_PORT} -sTCP:LISTEN | xargs kill -9 2>/dev/null || true

rm -f coordinator.log node1.log node2.log node3.log

cat > "$CFG" <<CFGEOF
# node <id> <host> <kv_port> <repl_port>
node node1 127.0.0.1 ${NODE1_CLIENT_PORT} ${NODE1_REPL_PORT}
node node2 127.0.0.1 ${NODE2_CLIENT_PORT} ${NODE2_REPL_PORT}
node node3 127.0.0.1 ${NODE3_CLIENT_PORT} ${NODE3_REPL_PORT}

# tablet <tablet_id> <row_start> <row_end> <replica1> <replica2> ...
tablet ${TABLET_NAME} a zzzzz node1 node2 node3
CFGEOF

echo "[abc] starting secondary node2"
./kvstore/kvserver --port "$NODE2_CLIENT_PORT" --data "$DATA_ROOT/node2" --tablet "$TABLET_NAME" --node-id node2 --repl-port "$NODE2_REPL_PORT" > node2.log 2>&1 &
echo $! > "$PID_DIR/node2.pid"
sleep 1

echo "[abc] starting secondary node3"
./kvstore/kvserver --port "$NODE3_CLIENT_PORT" --data "$DATA_ROOT/node3" --tablet "$TABLET_NAME" --node-id node3 --repl-port "$NODE3_REPL_PORT" > node3.log 2>&1 &
echo $! > "$PID_DIR/node3.pid"
sleep 1

echo "[abc] starting primary node1"
./kvstore/kvserver --port "$NODE1_CLIENT_PORT" --data "$DATA_ROOT/node1" --tablet "$TABLET_NAME" --node-id node1 --repl-port "$NODE1_REPL_PORT" \
  --replica node2@127.0.0.1:${NODE2_REPL_PORT} --replica node3@127.0.0.1:${NODE3_REPL_PORT} > node1.log 2>&1 &
echo $! > "$PID_DIR/node1.pid"
sleep 1

echo "[abc] starting coordinator"
./coordinator/coordinator --port "$COORD_PORT" --config "$CFG" > coordinator.log 2>&1 &
COORD_PID=$!
echo $COORD_PID > "$PID_DIR/coord.pid"
sleep 2
if ! kill -0 "$COORD_PID" 2>/dev/null; then
  echo "[abc] coordinator failed to start; showing coordinator.log" >&2
  cat coordinator.log >&2 || true
  exit 1
fi

echo "[abc] cluster ready"
echo "  coordinator: $COORD_PORT"
echo "  node1 kv/repl: $NODE1_CLIENT_PORT / $NODE1_REPL_PORT"
echo "  node2 kv/repl: $NODE2_CLIENT_PORT / $NODE2_REPL_PORT"
echo "  node3 kv/repl: $NODE3_CLIENT_PORT / $NODE3_REPL_PORT"
echo "  logs: coordinator.log node1.log node2.log node3.log"