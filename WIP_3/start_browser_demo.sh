#!/bin/bash
# Boots a full PennCloud stack for browser-based admin/drive testing.
# - ABC cluster: 3 KV nodes + coordinator
# - 2 frontend servers (so admin kill/restart peer FE can be tested)
#
# Usage: bash start_browser_demo.sh
# After it prints "DEMO READY", open http://localhost:8080/admin in your browser.
# When done: bash stop_browser_demo.sh
set -e

cd "$(dirname "$0")"

# IMPORTANT: several ports are hardcoded in fe_server.cc and the admin SPA.
# Don't change them or admin probes/redirects break.
#   - ports 8090/8091 are hardcoded as fe1/fe2 in fe_server.cc:154-156
#   - coord_port 6010 -> ABC cluster spec (probes KV on 5500/5501/5502)
#   - coord_port 6000 -> "single" spec (probes KV on 5000/5001/5002) <-- wrong for ABC
FE1_PORT=8090
FE2_PORT=8091
COORD_PORT=6010

echo "[demo] cleaning up any old processes..."
./stop_abc_cluster.sh 2>/dev/null || true
pkill -f 'feserver --port 8080' 2>/dev/null || true
pkill -f 'feserver --port 8081' 2>/dev/null || true
pkill -f "feserver --port $FE1_PORT" 2>/dev/null || true
pkill -f "feserver --port $FE2_PORT" 2>/dev/null || true
sleep 1
rm -rf /tmp/pc_abc_cluster

echo "[demo] starting ABC cluster (3 KV nodes + coordinator on $COORD_PORT)..."
COORD_PORT=$COORD_PORT ./start_abc_cluster.sh

sleep 1

echo "[demo] starting frontend fe1 on $FE1_PORT..."
./frontend/feserver \
    --port $FE1_PORT \
    --kv-host 127.0.0.1 --kv-port 5500 \
    --coord-host 127.0.0.1 --coord-port $COORD_PORT \
    --id fe1 > fe1.log 2>&1 &
echo $! > /tmp/pc_abc_cluster/pids/fe1.pid

echo "[demo] starting frontend fe2 on $FE2_PORT..."
./frontend/feserver \
    --port $FE2_PORT \
    --kv-host 127.0.0.1 --kv-port 5500 \
    --coord-host 127.0.0.1 --coord-port $COORD_PORT \
    --id fe2 > fe2.log 2>&1 &
echo $! > /tmp/pc_abc_cluster/pids/fe2.pid

sleep 1

echo
echo "================================================================"
echo "                       DEMO READY"
echo "================================================================"
echo
echo "  Open in your browser (Windows side):"
echo "    http://localhost:$FE1_PORT/        <-- main app (login/drive/mail)"
echo "    http://localhost:$FE1_PORT/admin   <-- admin console"
echo "    http://localhost:$FE2_PORT/admin   <-- admin console (alt FE)"
echo
echo "  Logs:"
echo "    fe1.log fe2.log node1.log node2.log node3.log coordinator.log"
echo
echo "  When done testing, run:  bash stop_browser_demo.sh"
echo
