#!/usr/bin/env bash
set -euo pipefail

LB_PORT="${LB_PORT:-8088}"

echo "[test] requesting / from load balancer on port $LB_PORT"
curl -i "http://127.0.0.1:${LB_PORT}/"
echo
echo "[test] requesting /mail?x=1 from load balancer on port $LB_PORT"
curl -i "http://127.0.0.1:${LB_PORT}/mail?x=1"
echo
