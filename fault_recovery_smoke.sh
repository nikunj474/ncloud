#!/usr/bin/env bash
# Curl-only multi-group failover/recovery smoke test.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

BASE="${BASE:-http://127.0.0.1:8090}"
DATA_ROOT="${DATA_ROOT:-/tmp/pc_multi_group_demo}"
COOKIE_JAR="$DATA_ROOT/fault-cookies.txt"
BODY_FILE="$DATA_ROOT/fault-body.out"
USER_NAME="${USER_NAME:-testuser}"
PASSWORD="${PASSWORD:-testpass}"

export DATA_ROOT
export NCLOUD_OPEN_ADMIN="${NCLOUD_OPEN_ADMIN:-1}"

mkdir -p "$DATA_ROOT"

cleanup() {
  KEEP_404_STUB=0 ./stop_multi_group_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail() {
  echo "[FAIL] $*" >&2
  if [ -s "$BODY_FILE" ]; then
    echo "[FAIL] last response body:" >&2
    body_bytes="$(wc -c < "$BODY_FILE" | tr -d ' ')"
    if [ "${body_bytes:-0}" -le 4096 ]; then
      cat "$BODY_FILE" >&2 || true
      echo >&2
    else
      echo "[FAIL] omitted $body_bytes byte response body" >&2
    fi
  fi
  for log in coordinator_multi_group.log mg_node1.log mg_node2.log mg_node3.log mg_node4.log frontend_8090.log; do
    if [ -f "$log" ]; then
      echo "[FAIL] tail $log:" >&2
      tail -30 "$log" >&2 || true
    fi
  done
  exit 1
}

wait_tcp() {
  local name="$1"
  local port="$2"
  for _ in $(seq 1 60); do
    if (: >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  fail "$name did not listen on port $port"
}

request() {
  local method="$1"
  local path="$2"
  shift 2
  curl -sS -o "$BODY_FILE" -w '%{http_code}' \
    -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
    -X "$method" "$@" "$BASE$path"
}

expect_code() {
  local expected="$1"
  local actual="$2"
  local label="$3"
  if [ "$actual" != "$expected" ]; then
    fail "$label returned HTTP $actual, expected $expected"
  fi
}

expect_body() {
  local needle="$1"
  local label="$2"
  if ! grep -Fq "$needle" "$BODY_FILE"; then
    fail "$label response did not contain: $needle"
  fi
}

wait_admin_node() {
  local node="$1"
  local alive="$2"
  for _ in $(seq 1 60); do
    code="$(request GET "/api/admin/status")"
    if [ "$code" = "200" ] && grep -Eq "\"id\":\"$node\"[^}]*\"alive\":$alive" "$BODY_FILE"; then
      return 0
    fi
    sleep 0.25
  done
  fail "admin status did not show $node alive=$alive"
}

wait_request_ok() {
  local label="$1"
  local method="$2"
  local path="$3"
  shift 3
  for _ in $(seq 1 40); do
    code="$(request "$method" "$path" "$@")"
    if [ "$code" = "200" ] && grep -Fq '"ok":true' "$BODY_FILE"; then
      echo "[PASS] $label"
      return 0
    fi
    sleep 0.25
  done
  fail "$label did not return ok:true"
}

kill_pid_file() {
  local label="$1"
  local pid_file="$DATA_ROOT/pids/$label.pid"
  if [ ! -f "$pid_file" ]; then
    fail "missing pid file for $label: $pid_file"
  fi
  kill "$(cat "$pid_file")" 2>/dev/null || true
}

admin_control() {
  local kind="$1"
  local action="$2"
  local target="$3"
  code="$(request POST "/api/admin/control" \
    -H 'Content-Type: application/json' \
    --data "{\"kind\":\"$kind\",\"action\":\"$action\",\"target\":\"$target\"}")"
  expect_code 200 "$code" "admin control $kind $action $target"
  expect_body '"ok":true' "admin control $kind $action $target"
}

restart_node2() {
  nohup ./kvstore/kvserver --port 7501 --data "$DATA_ROOT/node2" \
    --tablet tabletA:a:f --tablet tabletB:g:l --tablet tabletD:s: \
    --node-id node2 --repl-port 7601 > mg_node2_recovered.log 2>&1 &
  echo "$!" > "$DATA_ROOT/pids/node2.pid"
  wait_tcp "node2 kv after restart" 7501
  wait_tcp "node2 replication after restart" 7601
  wait_admin_node node2 true
  sleep 1
}

echo "=== NCloud multi-group fault/recovery smoke ==="
KEEP_404_STUB=0 ./stop_multi_group_cluster.sh >/dev/null 2>&1 || true
rm -rf "$DATA_ROOT"
mkdir -p "$DATA_ROOT"
: > "$COOKIE_JAR"

if ! ./start_multi_group_cluster.sh >/tmp/ncloud_fault_start.log 2>&1; then
  cat /tmp/ncloud_fault_start.log >&2 || true
  fail "multi-group cluster failed to start"
fi
wait_tcp "frontend" 8090

code="$(request GET "/api/admin/status")"
expect_code 200 "$code" "admin status"
expect_body '"id":"node4"' "admin status includes node4"
expect_body '"tabletD"' "admin status includes tabletD"
echo "[PASS] integrated cluster reports four-node tablet placement"

wait_request_ok "signup on tabletD user row" POST "/api/signup" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "username=$USER_NAME&password=$PASSWORD"

code="$(request GET "/api/admin/raw?node=node4&limit=20")"
expect_code 200 "$code" "raw KV dump node4"
expect_body "\"row\":\"$USER_NAME\"" "raw KV dump node4"
echo "[PASS] raw KV browser endpoint exposes local tabletD data"

kill_pid_file node4
wait_admin_node node4 false
wait_request_ok "send still succeeds after tabletD primary/node4 is down" POST "/api/send" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "to=$USER_NAME&subject=after-node4-down&body=body1"

admin_control backend kill node1
wait_admin_node node1 false
wait_request_ok "send still succeeds with only node2 holding tabletD live" POST "/api/send" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "to=$USER_NAME&subject=after-node1-down&body=body2"

kill_pid_file node2
wait_admin_node node2 false
code="$(request GET "/api/session")"
if [ "$code" = "200" ] && grep -Fq '"authenticated":true' "$BODY_FILE"; then
  fail "session stayed available after all tabletD replicas were down"
fi
echo "[PASS] all tabletD replicas down makes tabletD data unavailable"

restart_node2

code="$(request GET "/api/session")"
expect_code 200 "$code" "session after node2 recovery"
expect_body '"authenticated":true' "session after node2 recovery"
echo "[PASS] restarted node2 recovers tabletD session data"

code="$(request GET "/api/inbox")"
expect_code 200 "$code" "inbox after recovery"
expect_body '"subject":"after-node1-down"' "inbox after recovery"
echo "[PASS] inbox data remains readable after failover and node2 recovery"

echo
echo "=== FAULT/RECOVERY SMOKE PASSED ==="
