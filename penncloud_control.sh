#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$ROOT_DIR}"
cd "$PROJECT_DIR"

COORD_PORT="${COORD_PORT:-7110}"
SMTP_PORT="${SMTP_PORT:-2525}"
LB_PORT="${LB_PORT:-8088}"
DATA_ROOT="${DATA_ROOT:-$PROJECT_DIR/.penncloud_data/pc_multi_group_demo}"
PID_DIR="$DATA_ROOT/pids"
CFG="$PROJECT_DIR/coordinator/coordinator_multi_group_demo.conf"
LB_CFG="$PROJECT_DIR/frontend_lb.conf"

export PENNCLOUD_OPEN_ADMIN="${PENNCLOUD_OPEN_ADMIN:-1}"
export PENNCLOUD_MAIL_DOMAIN="${PENNCLOUD_MAIL_DOMAIN:-penncloud.local}"

mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3" "$DATA_ROOT/node4" "$PID_DIR"

usage() {
  cat <<'EOF'
Usage:
  ./penncloud_control.sh backend  start|stop|restart node1|node2|node3|node4
  ./penncloud_control.sh frontend start|stop|restart fe1|fe2|fe3
  ./penncloud_control.sh coordinator start|stop|restart
  ./penncloud_control.sh lb start|stop|restart
  ./penncloud_control.sh smtp start|stop|restart

Examples:
  ./penncloud_control.sh backend restart node2
  ./penncloud_control.sh frontend restart fe1
  ./penncloud_control.sh coordinator restart
EOF
}

tcp_ready() {
  local host="$1"
  local port="$2"
  nc -z "$host" "$port" >/dev/null 2>&1
}

wait_ready() {
  local name="$1"
  local pid="$2"
  local host="$3"
  local port="$4"
  local log_file="$5"
  local attempts="${6:-40}"

  for ((i = 1; i <= attempts; ++i)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[control] ERROR: $name exited before $host:$port was ready"
      [[ -f "$log_file" ]] && tail -40 "$log_file" || true
      exit 1
    fi
    if tcp_ready "$host" "$port"; then
      echo "[control] $name ready on $host:$port"
      return 0
    fi
    sleep 0.25
  done

  echo "[control] ERROR: $name did not become ready on $host:$port"
  [[ -f "$log_file" ]] && tail -40 "$log_file" || true
  exit 1
}

kill_matching_port() {
  local bin_name="$1"
  local port="$2"
  local sig="${3:-TERM}"
  ps -axo pid=,command= 2>/dev/null | awk -v bin="$bin_name" -v port="$port" '
    index($0, bin) &&
    (index($0, "--port " port) || index($0, "--port=" port) ||
     index($0, "--repl-port " port) || index($0, "--repl-port=" port)) {
      print $1
    }' | while read -r pid; do
      [[ -n "$pid" ]] && kill "-$sig" "$pid" 2>/dev/null || true
    done
}

kill_pid_file() {
  local name="$1"
  local sig="${2:-TERM}"
  local file="$PID_DIR/$name.pid"
  if [[ -f "$file" ]]; then
    kill "-$sig" "$(cat "$file")" 2>/dev/null || true
  fi
}

ensure_configs() {
  cat > "$CFG" <<'CFGEOF'
# node <id> <host> <kv_port> <repl_port>
node node1 127.0.0.1 7500 7600
node node2 127.0.0.1 7501 7601
node node3 127.0.0.1 7502 7602
node node4 127.0.0.1 7503 7603

# tablet <tablet_id> <row_start> <row_end> <replica1> <replica2> <replica3>
tablet tabletA a g node1 node2 node3
tablet tabletB g m node2 node3 node4
tablet tabletC m s node3 node4 node1
tablet tabletD s - node4 node1 node2
CFGEOF

  cat > "$LB_CFG" <<'LBEOF'
# frontend <id> <host> <port>
frontend fe1 127.0.0.1 8090
frontend fe2 127.0.0.1 8091
frontend fe3 127.0.0.1 8092
LBEOF
}

backend_spec() {
  local node="$1"
  case "$node" in
    node1) BACKEND_KV=7500; BACKEND_REPL=7600; BACKEND_TABLETS=(--tablet tabletA:a:g --tablet tabletC:m:s --tablet tabletD:s:) ;;
    node2) BACKEND_KV=7501; BACKEND_REPL=7601; BACKEND_TABLETS=(--tablet tabletA:a:g --tablet tabletB:g:m --tablet tabletD:s:) ;;
    node3) BACKEND_KV=7502; BACKEND_REPL=7602; BACKEND_TABLETS=(--tablet tabletA:a:g --tablet tabletB:g:m --tablet tabletC:m:s) ;;
    node4) BACKEND_KV=7503; BACKEND_REPL=7603; BACKEND_TABLETS=(--tablet tabletB:g:m --tablet tabletC:m:s --tablet tabletD:s:) ;;
    *) echo "[control] unknown backend: $node" >&2; exit 2 ;;
  esac
}

frontend_port() {
  case "$1" in
    fe1) echo 8090 ;;
    fe2) echo 8091 ;;
    fe3) echo 8092 ;;
    *) echo "[control] unknown frontend: $1" >&2; exit 2 ;;
  esac
}

stop_backend() {
  local node="$1"
  backend_spec "$node"
  echo "[control] stopping $node"
  kill_pid_file "$node" TERM
  kill_matching_port kvserver "$BACKEND_KV" TERM
  kill_matching_port kvserver "$BACKEND_REPL" TERM
  sleep 0.25
  kill_matching_port kvserver "$BACKEND_KV" KILL
  kill_matching_port kvserver "$BACKEND_REPL" KILL
}

start_backend() {
  local node="$1"
  backend_spec "$node"
  ensure_configs
  if tcp_ready 127.0.0.1 "$BACKEND_KV"; then
    echo "[control] $node already listening on $BACKEND_KV"
    return 0
  fi
  local log="mg_${node}.log"
  echo "[control] starting $node"
  mkdir -p "$DATA_ROOT/$node"
  nohup ./kvstore/kvserver --port "$BACKEND_KV" --data "$DATA_ROOT/$node" \
    "${BACKEND_TABLETS[@]}" --node-id "$node" --repl-port "$BACKEND_REPL" \
    > "$log" 2>&1 < /dev/null &
  local pid="$!"
  echo "$pid" > "$PID_DIR/$node.pid"
  wait_ready "$node kv" "$pid" 127.0.0.1 "$BACKEND_KV" "$log"
  wait_ready "$node replication" "$pid" 127.0.0.1 "$BACKEND_REPL" "$log"
}

stop_frontend() {
  local fe="$1"
  local port
  port="$(frontend_port "$fe")"
  echo "[control] stopping $fe"
  kill_pid_file "$fe" TERM
  kill_matching_port feserver "$port" TERM
  sleep 0.25
  kill_matching_port feserver "$port" KILL
}

start_frontend() {
  local fe="$1"
  local port
  port="$(frontend_port "$fe")"
  ensure_configs
  if tcp_ready 127.0.0.1 "$port"; then
    echo "[control] $fe already listening on $port"
    return 0
  fi
  local log="frontend_${port}.log"
  echo "[control] starting $fe"
  nohup ./frontend/feserver --port "$port" --id "$fe" \
    --kv-host 127.0.0.1 --kv-port 7500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > "$log" 2>&1 < /dev/null &
  local pid="$!"
  echo "$pid" > "$PID_DIR/$fe.pid"
  wait_ready "$fe" "$pid" 127.0.0.1 "$port" "$log"
}

stop_coordinator() {
  echo "[control] stopping coordinator"
  kill_pid_file coord TERM
  kill_matching_port coordinator "$COORD_PORT" TERM
  sleep 0.25
  kill_matching_port coordinator "$COORD_PORT" KILL
}

start_coordinator() {
  ensure_configs
  if tcp_ready 127.0.0.1 "$COORD_PORT"; then
    echo "[control] coordinator already listening on $COORD_PORT"
    return 0
  fi
  echo "[control] starting coordinator"
  nohup ./coordinator/coordinator --port "$COORD_PORT" --config "$CFG" \
    > coordinator_multi_group.log 2>&1 < /dev/null &
  local pid="$!"
  echo "$pid" > "$PID_DIR/coord.pid"
  wait_ready coordinator "$pid" 127.0.0.1 "$COORD_PORT" coordinator_multi_group.log
}

stop_lb() {
  echo "[control] stopping frontend load balancer"
  kill_pid_file lb TERM
  kill_matching_port frontend_lb "$LB_PORT" TERM
  sleep 0.25
  kill_matching_port frontend_lb "$LB_PORT" KILL
}

start_lb() {
  ensure_configs
  if tcp_ready 127.0.0.1 "$LB_PORT"; then
    echo "[control] load balancer already listening on $LB_PORT"
    return 0
  fi
  echo "[control] starting frontend load balancer"
  nohup ./frontend_lb/frontend_lb --port "$LB_PORT" --config "$LB_CFG" \
    > frontend_lb.log 2>&1 < /dev/null &
  local pid="$!"
  echo "$pid" > "$PID_DIR/lb.pid"
  wait_ready "frontend load balancer" "$pid" 127.0.0.1 "$LB_PORT" frontend_lb.log
}

stop_smtp() {
  echo "[control] stopping smtp server"
  kill_pid_file smtp TERM
  kill_matching_port smtp_server "$SMTP_PORT" TERM
  sleep 0.25
  kill_matching_port smtp_server "$SMTP_PORT" KILL
}

start_smtp() {
  if tcp_ready 127.0.0.1 "$SMTP_PORT"; then
    echo "[control] smtp server already listening on $SMTP_PORT"
    return 0
  fi
  echo "[control] starting smtp server"
  nohup ./smtp_server/smtp_server --port "$SMTP_PORT" \
    --kv-host 127.0.0.1 --kv-port 7500 \
    --coord-host 127.0.0.1 --coord-port "$COORD_PORT" \
    > smtp_server.log 2>&1 < /dev/null &
  local pid="$!"
  echo "$pid" > "$PID_DIR/smtp.pid"
  wait_ready "smtp server" "$pid" 127.0.0.1 "$SMTP_PORT" smtp_server.log
}

restart_target() {
  local kind="$1"
  local target="${2:-}"
  case "$kind" in
    backend) stop_backend "$target"; start_backend "$target" ;;
    frontend) stop_frontend "$target"; start_frontend "$target" ;;
    coordinator) stop_coordinator; start_coordinator ;;
    lb) stop_lb; start_lb ;;
    smtp) stop_smtp; start_smtp ;;
    *) usage; exit 2 ;;
  esac
}

kind="${1:-}"
action="${2:-}"
target="${3:-}"

if [[ -z "$kind" || -z "$action" ]]; then
  usage
  exit 2
fi

case "$action" in
  start)
    case "$kind" in
      backend) start_backend "$target" ;;
      frontend) start_frontend "$target" ;;
      coordinator) start_coordinator ;;
      lb) start_lb ;;
      smtp) start_smtp ;;
      *) usage; exit 2 ;;
    esac
    ;;
  stop|kill)
    case "$kind" in
      backend) stop_backend "$target" ;;
      frontend) stop_frontend "$target" ;;
      coordinator) stop_coordinator ;;
      lb) stop_lb ;;
      smtp) stop_smtp ;;
      *) usage; exit 2 ;;
    esac
    ;;
  restart)
    restart_target "$kind" "$target"
    ;;
  *)
    usage
    exit 2
    ;;
esac
