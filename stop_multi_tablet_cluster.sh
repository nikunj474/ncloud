#!/usr/bin/env bash
set -euo pipefail
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_tablet_demo}"
SMTP_PORT="${SMTP_PORT:-2525}"
KEEP_404_STUB="${KEEP_404_STUB:-1}"
PID_DIR="$DATA_ROOT/pids"

mkdir -p "$PID_DIR"

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

for f in coord smtp node1 node2 node3 fe1 fe2 fe3 fe1_404_stub; do
  if [[ -f "$PID_DIR/$f.pid" ]]; then
    kill "$(cat "$PID_DIR/$f.pid")" 2>/dev/null || true
  fi
done

kill_matching_port 'coordinator' 7010
kill_matching_port 'kvserver' 6500
kill_matching_port 'kvserver' 6501
kill_matching_port 'kvserver' 6502
kill_matching_port 'kvserver' 6600
kill_matching_port 'kvserver' 6601
kill_matching_port 'kvserver' 6602
kill_matching_port 'feserver' 8090
kill_matching_port 'feserver' 8091
kill_matching_port 'feserver' 8092
kill_matching_port 'smtp_server' "$SMTP_PORT"

sleep 0.2

kill_matching_port 'coordinator' 7010 KILL
kill_matching_port 'kvserver' 6500 KILL
kill_matching_port 'kvserver' 6501 KILL
kill_matching_port 'kvserver' 6502 KILL
kill_matching_port 'kvserver' 6600 KILL
kill_matching_port 'kvserver' 6601 KILL
kill_matching_port 'kvserver' 6602 KILL
kill_matching_port 'feserver' 8090 KILL
kill_matching_port 'feserver' 8091 KILL
kill_matching_port 'feserver' 8092 KILL
kill_matching_port 'smtp_server' "$SMTP_PORT" KILL

if [[ "$KEEP_404_STUB" != "0" ]]; then
  FE_BIN="$(cd "$(dirname "$0")" && pwd)/frontend/feserver"
  if [[ -x "$FE_BIN" ]]; then
    nohup "$FE_BIN" --port 8090 --page-not-found-stub > frontend_8090.log 2>&1 &
    echo "$!" > "$PID_DIR/fe1_404_stub.pid"
    echo "[multi] stopped cluster; 404 stub listening on http://127.0.0.1:8090"
    exit 0
  fi
fi

echo "[multi] stopped cluster"
