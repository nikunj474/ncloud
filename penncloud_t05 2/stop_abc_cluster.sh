#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"
pkill -f '/coordinator/coordinator --port ' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5500' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5501' 2>/dev/null || true
pkill -f 'kvstore/kvserver --port 5502' 2>/dev/null || true
echo "[abc] stopped cluster processes"
