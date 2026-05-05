#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"
PRIMARY_CLIENT_PORT="${PRIMARY_CLIENT_PORT:-5500}"
SECONDARY_CLIENT_PORT="${SECONDARY_CLIENT_PORT:-5501}"
pkill -f "kvstore/kvserver --port ${PRIMARY_CLIENT_PORT}" 2>/dev/null || true
pkill -f "kvstore/kvserver --port ${SECONDARY_CLIENT_PORT}" 2>/dev/null || true
echo "[cluster] stopped replication test cluster"
