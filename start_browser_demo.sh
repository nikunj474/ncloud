#!/bin/bash
# Boots a full PennCloud stack for browser-based admin/drive testing.
#
# Modes (set MODE env var):
#   MODE=abc           1 tablet, 3 KV nodes, all primary on node1.
#   MODE=multi-tablet  3 tablets, 3 KV nodes; tabletA primary=node1,
#                      tabletB primary=node2, tabletC primary=node3.
#                      Use to demo per-tablet primary distribution.
#
# Usage:
#   bash start_browser_demo.sh
#   MODE=multi-tablet bash start_browser_demo.sh
#
# Optional:
#   PRESERVE_DATA=1 keeps the current DATA_ROOT instead of wiping demo KV data.
#   DATA_ROOT=/path/to/kv-data chooses the multi-tablet KV data directory.
#
# When done: bash stop_browser_demo.sh
set -e

cd "$(dirname "$0")"

MODE="${MODE:-multi-tablet}"


FE1_PORT=8090
FE2_PORT=8091
FE3_PORT=8092
SMTP_PORT=2525

SMTP_ENV_FILE=""
if [ -f .smtp.env ]; then
    SMTP_ENV_FILE=".smtp.env"
elif [ -f ../.smtp.env ]; then
    SMTP_ENV_FILE="../.smtp.env"
fi

if [ -n "$SMTP_ENV_FILE" ]; then
    echo "[demo] loading SMTP relay environment from $SMTP_ENV_FILE"
    set -a
    . "$SMTP_ENV_FILE"
    set +a
else
    echo "[demo] .smtp.env not found in WIP_3/ or repo root; remote SMTP relay will not be configured"
fi

echo "[demo] cleaning up any old processes..."
./stop_abc_cluster.sh 2>/dev/null || true
./stop_multi_tablet_cluster.sh 2>/dev/null || true
pkill -KILL -f feserver 2>/dev/null || true
pkill -KILL -f kvserver 2>/dev/null || true
pkill -KILL -f '/coordinator/coordinator' 2>/dev/null || true
pkill -KILL -f "smtp_server --port $SMTP_PORT" 2>/dev/null || true
sleep 1
if [ "${PRESERVE_DATA:-0}" = "1" ]; then
    echo "[demo] preserving existing KV data (PRESERVE_DATA=1)"
else
    rm -rf /tmp/pc_abc_cluster /tmp/pc_multi_tablet_demo
    if [ -n "${DATA_ROOT:-}" ]; then
        rm -rf "$DATA_ROOT"
    fi
fi

echo "[demo] building current sources..."
make -C kvstore clean all
make -C coordinator clean all
make -C frontend clean all
make -C smtp_server clean all

if [ "$MODE" = "multi-tablet" ]; then
    COORD_PORT=7010
    KV_PORT_FOR_FE=6500     # FE talks to one of the KV nodes; primary lookup goes via coordinator
    PID_DIR=/tmp/pc_multi_tablet_demo/pids
    mkdir -p "$PID_DIR"
    echo "[demo] MODE=multi-tablet -> 3 KV nodes, 3 tablets, each tablet has its own primary"
    echo "[demo] starting multi-tablet cluster (3 KV nodes + coordinator on $COORD_PORT)..."
    COORD_PORT=$COORD_PORT ./start_multi_tablet_cluster.sh
    sleep 1
    # start_multi_tablet_cluster.sh already starts fe1/fe2/fe3 — we don't need to.
else
    COORD_PORT=6010
    KV_PORT_FOR_FE=5500
    PID_DIR=/tmp/pc_abc_cluster/pids
    mkdir -p "$PID_DIR"
    echo "[demo] MODE=abc -> 3 KV nodes replicating one tablet"
    echo "[demo] starting ABC cluster (3 KV nodes + coordinator on $COORD_PORT)..."
    COORD_PORT=$COORD_PORT ./start_abc_cluster.sh
    sleep 1

    echo "[demo] starting frontend fe1 on $FE1_PORT..."
    ./frontend/feserver \
        --port $FE1_PORT \
        --kv-host 127.0.0.1 --kv-port $KV_PORT_FOR_FE \
        --coord-host 127.0.0.1 --coord-port $COORD_PORT \
        --id fe1 > fe1.log 2>&1 &
    echo $! > "$PID_DIR/fe1.pid"

    echo "[demo] starting frontend fe2 on $FE2_PORT..."
    ./frontend/feserver \
        --port $FE2_PORT \
        --kv-host 127.0.0.1 --kv-port $KV_PORT_FOR_FE \
        --coord-host 127.0.0.1 --coord-port $COORD_PORT \
        --id fe2 > fe2.log 2>&1 &
    echo $! > "$PID_DIR/fe2.pid"
fi

if [ "$MODE" = "multi-tablet" ]; then
    echo "[demo] inbound SMTP server already started by start_multi_tablet_cluster.sh"
else
    echo "[demo] starting inbound SMTP server on $SMTP_PORT..."
    ./smtp_server/smtp_server \
        --port $SMTP_PORT \
        --kv-host 127.0.0.1 --kv-port $KV_PORT_FOR_FE > smtp_server.log 2>&1 &
    echo $! > "$PID_DIR/smtp.pid"
fi

sleep 1

echo
echo "================================================================"
echo "                       DEMO READY (mode=$MODE)"
echo "================================================================"
echo
if [ "$MODE" = "multi-tablet" ]; then
    echo "  Tablets and primaries (per coordinator_multi_tablet_demo.conf):"
    echo "    tabletA  rows [a, h)       primary=node1  secondaries=node2,node3"
    echo "    tabletB  rows [h, q)       primary=node2  secondaries=node3,node1"
    echo "    tabletC  rows [q, zzzzz)   primary=node3  secondaries=node1,node2"
    echo
    echo "  Open in your browser (Windows side):"
    echo "    http://localhost:$FE1_PORT/        <-- main app"
    echo "    http://localhost:$FE1_PORT/admin   <-- admin console (3 tablets visible)"
    echo "    http://localhost:$FE2_PORT/admin   <-- admin (alt FE)"
    echo "    http://localhost:$FE3_PORT/admin   <-- admin (alt FE)"
    echo "    SMTP localhost:$SMTP_PORT"
    echo
    echo "  Logs:"
    echo "    frontend_8090.log frontend_8091.log frontend_8092.log smtp_server.log"
    echo "    mt_node1.log mt_node2.log mt_node3.log coordinator_multi_tablet.log"
else
    echo "  Single tablet 'tablet0' replicated on node1 (primary), node2, node3 (secondaries)"
    echo
    echo "  Open in your browser (Windows side):"
    echo "    http://localhost:$FE1_PORT/        <-- main app (login/drive/mail)"
    echo "    http://localhost:$FE1_PORT/admin   <-- admin console"
    echo "    http://localhost:$FE2_PORT/admin   <-- admin console (alt FE)"
    echo "    SMTP localhost:$SMTP_PORT          <-- inbound SMTP"
    echo
    echo "  Logs:"
    echo "    fe1.log fe2.log smtp_server.log node1.log node2.log node3.log coordinator.log"
fi
echo
echo "  When done testing, run:  bash stop_browser_demo.sh"
echo
