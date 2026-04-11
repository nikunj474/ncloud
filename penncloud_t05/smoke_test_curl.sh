#!/bin/bash
# PennCloud smoke test (curl version, no Python dependency)
# Usage: bash smoke_test_curl.sh
set -e

KV_PORT=5050
FE_PORT=8090
DATA_DIR=/tmp/pc_smoke_$$
BASE="http://127.0.0.1:${FE_PORT}"
COOKIES=/tmp/pc_cookies_$$
PASS=0
FAIL=0

echo "=== PennCloud Smoke Test (curl) ==="
mkdir -p $DATA_DIR

# Start KV server
./kvstore/kvserver --port $KV_PORT --data $DATA_DIR --tablet test &
KV_PID=$!
sleep 0.5

# Start Frontend
./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT --coord-host 127.0.0.1 --coord-port 6000 --id fe1 &
FE_PID=$!
sleep 0.5

cleanup() {
    kill $KV_PID $FE_PID 2>/dev/null || true
    rm -rf $DATA_DIR $COOKIES
}
trap cleanup EXIT

check() {
    local desc="$1"
    local cond="$2"
    if eval "$cond"; then
        echo "[PASS] $desc"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] $desc"
        FAIL=$((FAIL + 1))
    fi
}

# Simple JSON value extractor (flat keys only)
json_val() {
    echo "$2" | grep -o "\"$1\":[^,}]*" | head -1 | sed "s/\"$1\"://;s/\"//g;s/^ *//;s/ *$//"
}

# 1. SPA shell
RESP=$(curl -s $BASE/)
check "GET /  ->  SPA shell" '[[ "$RESP" == *"PennCloud"* ]]'

# 2. Signup
RESP=$(curl -s -c $COOKIES -X POST $BASE/api/signup -d 'username=testuser&password=testpass')
OK=$(json_val "ok" "$RESP")
check "POST /api/signup  ->  account created" '[[ "$OK" == "true" ]]'

# 3. Inbox (authenticated)
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
OK=$(json_val "ok" "$RESP")
check "GET /api/inbox  ->  empty inbox (authenticated)" '[[ "$OK" == "true" ]]'

# 4. Send email to self
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/send -d 'to=testuser&subject=Hello&body=Test%20body')
OK=$(json_val "ok" "$RESP")
EMAIL_UID=$(json_val "uid" "$RESP")
check "POST /api/send  ->  email sent, uid=${EMAIL_UID:0:16}..." '[[ "$OK" == "true" ]]'

# 5. Inbox now has one unread email
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
OK=$(json_val "ok" "$RESP")
HAS_EMAIL=$(echo "$RESP" | grep -c '"uid"' || true)
HAS_UNREAD=$(echo "$RESP" | grep -c '"read":false' || true)
check "GET /api/inbox  ->  1 unread email in inbox" '[[ "$OK" == "true" && "$HAS_EMAIL" -ge 1 && "$HAS_UNREAD" -ge 1 ]]'

# 5b. Sent box has one email
RESP=$(curl -s -b $COOKIES $BASE/api/sent)
OK=$(json_val "ok" "$RESP")
SENT_COUNT=$(echo "$RESP" | grep -c '"uid"' || true)
check "GET /api/sent  ->  1 email in sent box" '[[ "$OK" == "true" && "$SENT_COUNT" -ge 1 ]]'

# 6. Read email -> marks as read
RESP=$(curl -s -b $COOKIES "$BASE/api/email/$EMAIL_UID")
OK=$(json_val "ok" "$RESP")
SUBJ=$(json_val "subject" "$RESP")
check "GET /api/email/:uid  ->  email body correct" '[[ "$OK" == "true" && "$SUBJ" == "Hello" ]]'

# 6b. Inbox now shows email as read
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
HAS_READ=$(echo "$RESP" | grep -c '"read":true' || true)
check "GET /api/inbox  ->  email marked as read after viewing" '[[ "$HAS_READ" -ge 1 ]]'

# 6c. View sent email via ?box=sent
RESP=$(curl -s -b $COOKIES "$BASE/api/email/$EMAIL_UID?box=sent")
OK=$(json_val "ok" "$RESP")
SUBJ=$(json_val "subject" "$RESP")
check "GET /api/email/:uid?box=sent  ->  sent email viewable" '[[ "$OK" == "true" && "$SUBJ" == "Hello" ]]'

# 7. Delete email
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/delete-email -d "uid=$EMAIL_UID")
OK=$(json_val "ok" "$RESP")
check "POST /api/delete-email  ->  deleted" '[[ "$OK" == "true" ]]'

# 8. Inbox empty again
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
OK=$(json_val "ok" "$RESP")
EMAIL_COUNT=$(echo "$RESP" | grep -c '"uid"' || true)
check "GET /api/inbox  ->  inbox empty after delete" '[[ "$OK" == "true" && "$EMAIL_COUNT" -eq 0 ]]'

# 9. Upload a file to Drive
TMPFILE=/tmp/pc_upload_$$.txt
echo -n 'PennCloud Demo I file' > $TMPFILE
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/upload -F "path=/" -F "file=@${TMPFILE};filename=demo.txt;type=text/plain")
rm -f $TMPFILE
OK=$(json_val "ok" "$RESP")
FILE_UID=$(json_val "uid" "$RESP")
FILE_PATH=$(json_val "path" "$RESP")
check "POST /api/upload  ->  uploaded demo.txt" '[[ "$OK" == "true" ]]'

# 10. Drive list shows uploaded file
RESP=$(curl -s -b $COOKIES "$BASE/api/drive?path=/")
OK=$(json_val "ok" "$RESP")
HAS_FILE=$(echo "$RESP" | grep -c '"demo.txt"' || true)
check "GET /api/drive  ->  1 file listed" '[[ "$OK" == "true" && "$HAS_FILE" -ge 1 ]]'

# 11. Download file
RESP=$(curl -s -b $COOKIES "$BASE/api/download/$FILE_UID")
check "GET /api/download/:uid  ->  file bytes correct" '[[ "$RESP" == "PennCloud Demo I file" ]]'

# 12. Rename file
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/rename --data-urlencode "path=$FILE_PATH" --data-urlencode "name=demo-renamed.txt")
OK=$(json_val "ok" "$RESP")
FILE_PATH=$(json_val "path" "$RESP")
check "POST /api/rename  ->  renamed file" '[[ "$OK" == "true" ]]'

# 13. Delete file
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/delete-path --data-urlencode "path=$FILE_PATH")
OK=$(json_val "ok" "$RESP")
check "POST /api/delete-path  ->  deleted file" '[[ "$OK" == "true" ]]'

# 14. Drive list empty again
RESP=$(curl -s -b $COOKIES "$BASE/api/drive?path=/")
OK=$(json_val "ok" "$RESP")
FILE_COUNT=$(echo "$RESP" | grep -c '"name"' || true)
check "GET /api/drive  ->  drive empty after delete" '[[ "$OK" == "true" && "$FILE_COUNT" -eq 0 ]]'

# 15. Logout
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/logout -d '')
OK=$(json_val "ok" "$RESP")
check "POST /api/logout  ->  session destroyed" '[[ "$OK" == "true" ]]'

# 16. Inbox without auth -> 401
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' $BASE/api/inbox)
check "GET /api/inbox (no auth)  ->  401 Unauthorized" '[[ "$HTTP_CODE" == "401" ]]'

# 17. Admin metrics API
RESP=$(curl -s $BASE/admin/metrics)
HAS_SERVER=$(echo "$RESP" | grep -c '"server_id"' || true)
check "GET /admin/metrics  ->  returns JSON with server_id" '[[ "$HAS_SERVER" -ge 1 ]]'

# 18. Admin page
RESP=$(curl -s $BASE/admin)
HAS_ADMIN=$(echo "$RESP" | grep -c 'Admin Console' || true)
check "GET /admin  ->  admin page loads" '[[ "$HAS_ADMIN" -ge 1 ]]'

echo
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ $FAIL -gt 0 ]; then
    exit 1
fi
echo "=== ALL SMOKE TESTS PASSED ==="
