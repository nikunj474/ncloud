#!/bin/bash
# 10MB upload/download timing + binary correctness check
set -e

cd "$(dirname "$0")"

if [ ! -f tenmb.bin ]; then
    dd if=/dev/urandom of=tenmb.bin bs=1M count=10 status=none
fi

pkill -f kvserver 2>/dev/null || true
pkill -f feserver 2>/dev/null || true
sleep 1

rm -rf /tmp/pc_big2
mkdir -p /tmp/pc_big2

./kvstore/kvserver --port 5050 --data /tmp/pc_big2 --tablet test > /tmp/kv.log 2>&1 &
KV=$!
sleep 0.5
./frontend/feserver --port 8090 --kv-host 127.0.0.1 --kv-port 5050 --id fe1 > /tmp/fe.log 2>&1 &
FE=$!
sleep 1

cleanup() {
    kill $KV $FE 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

SIGNUP=$(curl -s -c /tmp/c.txt -X POST http://127.0.0.1:8090/api/signup \
    -d 'username=biguser&password=bigpassword')
echo "$SIGNUP" | grep -q '"ok":true'

echo "=== upload 10MB ==="
time curl -s -b /tmp/c.txt -X POST http://127.0.0.1:8090/api/upload \
    -F 'path=/' -F 'file=@tenmb.bin' -o /tmp/up.json
cat /tmp/up.json
echo
echo

UID_VAL=$(grep -o '"uid":"[^"]*"' /tmp/up.json | head -1 | sed 's/"uid":"//;s/"$//')
grep -q '"ok":true' /tmp/up.json
test -n "$UID_VAL"
echo "uid=$UID_VAL"
echo

echo "=== download 10MB ==="
time curl -f -s -b /tmp/c.txt "http://127.0.0.1:8090/api/download/$UID_VAL" -o /tmp/dl.bin
ls -la /tmp/dl.bin
cmp tenmb.bin /tmp/dl.bin
echo
echo "=== md5 check (must match) ==="
if command -v md5sum >/dev/null 2>&1; then
    md5sum tenmb.bin /tmp/dl.bin
else
    md5 tenmb.bin /tmp/dl.bin
fi
echo "=== 10MB TEST PASSED ==="
