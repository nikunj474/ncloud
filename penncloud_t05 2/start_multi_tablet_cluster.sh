#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$ROOT_DIR}"
cd "$PROJECT_DIR"

SMTP_ENV_FILE="${SMTP_ENV_FILE:-$PROJECT_DIR/.smtp.env}"
if [ -f "$SMTP_ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$SMTP_ENV_FILE"
  set +a
  echo "[multi] loaded SMTP environment from $SMTP_ENV_FILE"
fi

COORD_PORT="${COORD_PORT:-7010}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_tablet_demo}"
CFG="$PROJECT_DIR/coordinator/coordinator_multi_tablet_demo.conf"
PID_DIR="$DATA_ROOT/pids"

mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3" "$PID_DIR"

pkill -f '/coordinator/coordinator --port 7010' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6502' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8090' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8091' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8092' 2>/dev/null || true
rm -f coordinator_multi_tablet.log mt_node1.log mt_node2.log mt_node3.log \
      frontend_8090.log frontend_8091.log frontend_8092.log

cat > "$CFG" <<'CFGEOF'
# node <id> <host> <kv_port> <repl_port>
node node1 127.0.0.1 6500 6600
node node2 127.0.0.1 6501 6601
node node3 127.0.0.1 6502 6602

# tablet <tablet_id> <row_start> <row_end> <replica1> <replica2> <replica3>
tablet tabletA a g node1 node2 node3
tablet tabletB h p node2 node3 node1
tablet tabletC q zzzzz node3 node1 node2
CFGEOF

echo "[multi] starting node1"
./kvstore/kvserver --port 6500 --data "$DATA_ROOT/node1" \
  --tablet tabletA:a:g --tablet tabletB:h:p --tablet tabletC:q:zzzzz \
  --node-id node1 --repl-port 6600 > mt_node1.log 2>&1 &
echo $! > "$PID_DIR/node1.pid"
sleep 1

echo "[multi] starting node2"
./kvstore/kvserver --port 6501 --data "$DATA_ROOT/node2" \
  --tablet tabletA:a:g --tablet tabletB:h:p --tablet tabletC:q:zzzzz \
  --node-id node2 --repl-port 6601 > mt_node2.log 2>&1 &
echo $! > "$PID_DIR/node2.pid"
sleep 1

echo "[multi] starting node3"
./kvstore/kvserver --port 6502 --data "$DATA_ROOT/node3" \
  --tablet tabletA:a:g --tablet tabletB:h:p --tablet tabletC:q:zzzzz \
  --node-id node3 --repl-port 6602 > mt_node3.log 2>&1 &
echo $! > "$PID_DIR/node3.pid"
sleep 1

echo "[multi] starting coordinator"
./coordinator/coordinator --port "$COORD_PORT" --config "$CFG" > coordinator_multi_tablet.log 2>&1 &
echo $! > "$PID_DIR/coord.pid"
sleep 2

FE_BIN="$PROJECT_DIR/frontend/feserver"
if [ -x "$FE_BIN" ]; then
  echo "[multi] starting fe1 (port 8090)"
  "$FE_BIN" --port 8090 --id fe1 \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8090.log 2>&1 &
  echo $! > "$PID_DIR/fe1.pid"

  echo "[multi] starting fe2 (port 8091)"
  "$FE_BIN" --port 8091 --id fe2 \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8091.log 2>&1 &
  echo $! > "$PID_DIR/fe2.pid"

  echo "[multi] starting fe3 (port 8092)"
  "$FE_BIN" --port 8092 --id fe3 \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8092.log 2>&1 &
  echo $! > "$PID_DIR/fe3.pid"
  sleep 1
else
  echo "[multi] WARNING: feserver binary not found at $FE_BIN — start frontends manually"
fi

echo "[multi] cluster ready"
echo "  coordinator: $COORD_PORT"
echo "  node1: 6500 / 6600"
echo "  node2: 6501 / 6601"
echo "  node3: 6502 / 6602"
echo "  fe1: http://127.0.0.1:8090"
echo "  fe2: http://127.0.0.1:8091"
echo "  fe3: http://127.0.0.1:8092"
echo "  admin: http://127.0.0.1:8090/admin"
echo "  config: $CFG"
echo "  logs: coordinator_multi_tablet.log mt_node1.log mt_node2.log mt_node3.log"
echo "        frontend_8090.log frontend_8091.log frontend_8092.log"
