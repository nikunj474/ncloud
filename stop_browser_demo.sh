#!/bin/bash
# Tears down the demo stack started by start_browser_demo.sh.
cd "$(dirname "$0")"

# Kill ALL feserver processes (not just specific ports) — earlier runs left
# multiple instances on the same port and the per-port pkill could not catch them.
pkill -KILL -f feserver 2>/dev/null || true
pkill -KILL -f smtp_server 2>/dev/null || true
pkill -KILL -f kvserver 2>/dev/null || true
pkill -KILL -f '/coordinator/coordinator' 2>/dev/null || true
sleep 0.5
./stop_abc_cluster.sh 2>/dev/null || true
./stop_multi_tablet_cluster.sh 2>/dev/null || true

echo "[demo] stopped."
