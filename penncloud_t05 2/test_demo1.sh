#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

echo "== Killing old PennCloud processes =="
pkill -f "/kvstore/kvserver" 2>/dev/null || true
pkill -f "/frontend/feserver" 2>/dev/null || true
pkill -f "/coordinator/coordinator" 2>/dev/null || true
sleep 1

echo "== Cleaning old builds =="
make -C kvstore clean || true
make -C frontend clean || true
make -C coordinator clean || true

echo "== Building all components =="
make -C kvstore
make -C frontend
make -C coordinator

echo "== Running automated smoke test =="
chmod +x smoke_test.sh
./smoke_test.sh

echo
echo "== Done =="
echo "Automated Demo I validation passed."
