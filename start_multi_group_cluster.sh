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
  echo "[multi-group] loaded SMTP environment from $SMTP_ENV_FILE"
fi

COORD_PORT="${COORD_PORT:-7110}"
SMTP_PORT="${SMTP_PORT:-2525}"
LB_PORT="${LB_PORT:-8088}"
export NCLOUD_OPEN_ADMIN="${NCLOUD_OPEN_ADMIN:-1}"
export NCLOUD_MAIL_DOMAIN="${NCLOUD_MAIL_DOMAIN:-ncloud.local}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_group_demo}"
CFG_SRC="$PROJECT_DIR/coordinator/coordinator_multi_group_demo.conf"
CFG_DST="$PROJECT_DIR/coordinator_multi_group_demo.conf"
LB_CFG="$PROJECT_DIR/frontend_lb.conf"
PID_DIR="$DATA_ROOT/pids"

mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3" "$DATA_ROOT/node4" "$PID_DIR"

pkill -f "coordinator/coordinator --port ${COORD_PORT}" 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7502' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 7503' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8090' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8091' 2>/dev/null || true
pkill -f 'frontend/feserver --port 8092' 2>/dev/null || true
pkill -f "frontend_lb/frontend_lb --port ${LB_PORT}" 2>/dev/null || true
pkill -f "smtp_server/smtp_server --port ${SMTP_PORT}" 2>/dev/null || true
rm -f coordinator_multi_group.log mg_node1.log mg_node2.log mg_node3.log mg_node4.log \
      frontend_8090.log frontend_8091.log frontend_8092.log frontend_lb.log smtp_server.log

cp "$CFG_SRC" "$CFG_DST"
cat > "$LB_CFG" <<'LBEOF'
# frontend <id> <host> <port>
frontend fe1 127.0.0.1 8090
frontend fe2 127.0.0.1 8091
frontend fe3 127.0.0.1 8092
LBEOF

echo "[multi-group] starting node1"
./kvstore/kvserver --port 7500 --data "$DATA_ROOT/node1" \
  --tablet tabletA:a:f --tablet tabletC:m:r --tablet tabletD:s: \
  --node-id node1 --repl-port 7600 > mg_node1.log 2>&1 &
echo $! > "$PID_DIR/node1.pid"
sleep 1

echo "[multi-group] starting node2"
./kvstore/kvserver --port 7501 --data "$DATA_ROOT/node2" \
  --tablet tabletA:a:f --tablet tabletB:g:l --tablet tabletD:s: \
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
  --tablet tabletB:g:l --tablet tabletC:m:r --tablet tabletD:s: \
  --node-id node4 --repl-port 7603 > mg_node4.log 2>&1 &
echo $! > "$PID_DIR/node4.pid"
sleep 1

echo "[multi-group] starting coordinator"
./coordinator/coordinator --port "$COORD_PORT" --config "$CFG_DST" > coordinator_multi_group.log 2>&1 &
echo $! > "$PID_DIR/coord.pid"
sleep 4

SMTP_BIN="$PROJECT_DIR/smtp_server/smtp_server"
if [ -x "$SMTP_BIN" ]; then
  echo "[multi-group] starting smtp server (port $SMTP_PORT)"
  "$SMTP_BIN" --port "$SMTP_PORT" \
    --kv-host 127.0.0.1 --kv-port 7500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > smtp_server.log 2>&1 &
  echo $! > "$PID_DIR/smtp.pid"
  sleep 1
fi

FE_BIN="$PROJECT_DIR/frontend/feserver"
if [ -x "$FE_BIN" ]; then
  echo "[multi-group] starting fe1 (port 8090)"
  "$FE_BIN" --port 8090 --id fe1 \
    --kv-host 127.0.0.1 --kv-port 7500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8090.log 2>&1 &
  echo $! > "$PID_DIR/fe1.pid"
  sleep 1

  echo "[multi-group] starting fe2 (port 8091)"
  "$FE_BIN" --port 8091 --id fe2 \
    --kv-host 127.0.0.1 --kv-port 7500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8091.log 2>&1 &
  echo $! > "$PID_DIR/fe2.pid"
  sleep 1

  echo "[multi-group] starting fe3 (port 8092)"
  "$FE_BIN" --port 8092 --id fe3 \
    --kv-host 127.0.0.1 --kv-port 7500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > frontend_8092.log 2>&1 &
  echo $! > "$PID_DIR/fe3.pid"
  sleep 1
fi

LB_BIN="$PROJECT_DIR/frontend_lb/frontend_lb"
if [ -x "$LB_BIN" ]; then
  echo "[multi-group] starting frontend load balancer (port $LB_PORT)"
  "$LB_BIN" --port "$LB_PORT" --config "$LB_CFG" > frontend_lb.log 2>&1 &
  echo $! > "$PID_DIR/lb.pid"
  sleep 1
fi

echo "[multi-group] cluster ready"
echo "  load balancer: http://127.0.0.1:$LB_PORT"
echo "  coordinator: $COORD_PORT"
echo "  smtp: 127.0.0.1:$SMTP_PORT"
echo "  node1: 7500 / 7600"
echo "  node2: 7501 / 7601"
echo "  node3: 7502 / 7602"
echo "  node4: 7503 / 7603"
echo "  fe1: http://127.0.0.1:8090"
echo "  fe2: http://127.0.0.1:8091"
echo "  fe3: http://127.0.0.1:8092"
echo "  admin: http://127.0.0.1:8090/admin"
echo "  admin via lb: http://127.0.0.1:$LB_PORT/admin"
echo "  logs: coordinator_multi_group.log mg_node1.log mg_node2.log mg_node3.log mg_node4.log"
echo "        frontend_8090.log frontend_8091.log frontend_8092.log frontend_lb.log smtp_server.log"
