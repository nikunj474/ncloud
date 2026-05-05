#!/usr/bin/env bash
set -euo pipefail
pkill -f '/coordinator/coordinator --port ' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5501' 2>/dev/null || true
echo '[ha] stopped HA cluster'
