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
SMTP_PORT="${SMTP_PORT:-2525}"
export NCLOUD_MAIL_DOMAIN="${NCLOUD_MAIL_DOMAIN:-ncloud.local}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_tablet_demo}"
CFG="$PROJECT_DIR/coordinator/coordinator_multi_tablet_demo.conf"
PID_DIR="$DATA_ROOT/pids"

mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3" "$PID_DIR"

print_log_tail() {
  local log_file="$1"
  if [ -f "$log_file" ]; then
    echo "[multi] last log lines from $log_file:"
    tail -40 "$log_file" || true
  fi
}

tcp_ready() {
  local host="$1"
  local port="$2"
  (: >"/dev/tcp/$host/$port") >/dev/null 2>&1
}

wait_ready() {
  local name="$1"
  local pid="$2"
  local host="$3"
  local port="$4"
  local log_file="$5"
  local attempts="${6:-80}"

  for ((i = 1; i <= attempts; ++i)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[multi] ERROR: $name exited before $host:$port was ready"
      print_log_tail "$log_file"
      exit 1
    fi
    if tcp_ready "$host" "$port"; then
      return 0
    fi
    sleep 0.25
  done

  echo "[multi] ERROR: $name did not become ready on $host:$port"
  print_log_tail "$log_file"
  exit 1
}

pkill -f "coordinator/coordinator --port ${COORD_PORT}" 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 6502' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8090' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8091' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8092' 2>/dev/null || true
pkill -f "smtp_server/smtp_server --port ${SMTP_PORT}" 2>/dev/null || true
rm -f coordinator_multi_tablet.log mt_node1.log mt_node2.log mt_node3.log \
      frontend_8090.log frontend_8091.log frontend_8092.log smtp_server.log

cat > "$CFG" <<'CFGEOF'
# node <id> <host> <kv_port> <repl_port>
node node1 127.0.0.1 6500 6600
node node2 127.0.0.1 6501 6601
node node3 127.0.0.1 6502 6602

# tablet <tablet_id> <row_start> <row_end> <replica1> <replica2> <replica3>
# Ranges must be contiguous: previously [a,g) [h,p) [q,-) had gaps at
# [g,h) and [p,q) -- usernames like "panda"/"goose" routed to no tablet
# and triggered "Storage temporarily unavailable". Fixed by [a,h) [h,q) [q,-).
tablet tabletA a h node1 node2 node3
tablet tabletB h q node2 node3 node1
tablet tabletC q - node3 node1 node2
CFGEOF

echo "[multi] starting node1"
nohup ./kvstore/kvserver --port 6500 --data "$DATA_ROOT/node1" \
  --tablet tabletA:a:h --tablet tabletB:h:q --tablet tabletC:q: \
  --node-id node1 --repl-port 6600 > mt_node1.log 2>&1 &
NODE1_PID=$!
echo "$NODE1_PID" > "$PID_DIR/node1.pid"
wait_ready "node1 kv" "$NODE1_PID" 127.0.0.1 6500 mt_node1.log
wait_ready "node1 replication" "$NODE1_PID" 127.0.0.1 6600 mt_node1.log

echo "[multi] starting node2"
nohup ./kvstore/kvserver --port 6501 --data "$DATA_ROOT/node2" \
  --tablet tabletA:a:h --tablet tabletB:h:q --tablet tabletC:q: \
  --node-id node2 --repl-port 6601 > mt_node2.log 2>&1 &
NODE2_PID=$!
echo "$NODE2_PID" > "$PID_DIR/node2.pid"
wait_ready "node2 kv" "$NODE2_PID" 127.0.0.1 6501 mt_node2.log
wait_ready "node2 replication" "$NODE2_PID" 127.0.0.1 6601 mt_node2.log

echo "[multi] starting node3"
nohup ./kvstore/kvserver --port 6502 --data "$DATA_ROOT/node3" \
  --tablet tabletA:a:h --tablet tabletB:h:q --tablet tabletC:q: \
  --node-id node3 --repl-port 6602 > mt_node3.log 2>&1 &
NODE3_PID=$!
echo "$NODE3_PID" > "$PID_DIR/node3.pid"
wait_ready "node3 kv" "$NODE3_PID" 127.0.0.1 6502 mt_node3.log
wait_ready "node3 replication" "$NODE3_PID" 127.0.0.1 6602 mt_node3.log

echo "[multi] starting coordinator"
nohup ./coordinator/coordinator --port "$COORD_PORT" --config "$CFG" > coordinator_multi_tablet.log 2>&1 &
COORD_PID=$!
echo "$COORD_PID" > "$PID_DIR/coord.pid"
wait_ready "coordinator" "$COORD_PID" 127.0.0.1 "$COORD_PORT" coordinator_multi_tablet.log

SMTP_BIN="$PROJECT_DIR/smtp_server/smtp_server"
if [ -x "$SMTP_BIN" ]; then
  echo "[multi] starting smtp server (port $SMTP_PORT)"
  nohup "$SMTP_BIN" --port "$SMTP_PORT" \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > smtp_server.log 2>&1 &
  SMTP_PID=$!
  echo "$SMTP_PID" > "$PID_DIR/smtp.pid"
  wait_ready "smtp server" "$SMTP_PID" 127.0.0.1 "$SMTP_PORT" smtp_server.log
else
  echo "[multi] ERROR: SMTP server binary not found at $SMTP_BIN"
  exit 1
fi

FE_BIN="$PROJECT_DIR/frontend/feserver"
if [ -x "$FE_BIN" ]; then
  echo "[multi] starting fe1 (port 8090)"
  nohup "$FE_BIN" --port 8090 --id fe1 \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8090.log 2>&1 &
  FE1_PID=$!
  echo "$FE1_PID" > "$PID_DIR/fe1.pid"
  wait_ready "fe1" "$FE1_PID" 127.0.0.1 8090 frontend_8090.log

  echo "[multi] starting fe2 (port 8091)"
  nohup "$FE_BIN" --port 8091 --id fe2 \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8091.log 2>&1 &
  FE2_PID=$!
  echo "$FE2_PID" > "$PID_DIR/fe2.pid"
  wait_ready "fe2" "$FE2_PID" 127.0.0.1 8091 frontend_8091.log

  echo "[multi] starting fe3 (port 8092)"
  nohup "$FE_BIN" --port 8092 --id fe3 \
    --kv-host 127.0.0.1 --kv-port 6500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8092.log 2>&1 &
  FE3_PID=$!
  echo "$FE3_PID" > "$PID_DIR/fe3.pid"
  wait_ready "fe3" "$FE3_PID" 127.0.0.1 8092 frontend_8092.log
else
  echo "[multi] ERROR: feserver binary not found at $FE_BIN"
  exit 1
fi

echo "[multi] cluster ready"
echo "  coordinator: $COORD_PORT"
echo "  smtp: 127.0.0.1:$SMTP_PORT"
echo "  node1: 6500 / 6600"
echo "  node2: 6501 / 6601"
echo "  node3: 6502 / 6602"
echo "  fe1: http://127.0.0.1:8090"
echo "  fe2: http://127.0.0.1:8091"
echo "  fe3: http://127.0.0.1:8092"
echo "  admin: http://127.0.0.1:8090/admin"
echo "  config: $CFG"
echo "  logs: coordinator_multi_tablet.log mt_node1.log mt_node2.log mt_node3.log"
echo "        frontend_8090.log frontend_8091.log frontend_8092.log smtp_server.log"
