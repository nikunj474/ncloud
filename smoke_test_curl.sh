#!/bin/bash
# NCloud smoke test (curl version, no Python dependency)
# Adapted for the flattened WIP_3 layout and current routes.
# Usage: bash smoke_test_curl.sh
set -e

KV_PORT=5050
FE_PORT=8090
DATA_DIR=/tmp/pc_smoke_$$
BASE="http://127.0.0.1:${FE_PORT}"
COOKIES=/tmp/pc_cookies_$$
PASS=0
FAIL=0

echo "=== NCloud Smoke Test (curl) ==="
mkdir -p $DATA_DIR

./kvstore/kvserver --port $KV_PORT --data $DATA_DIR --tablet test &
KV_PID=$!
sleep 0.5

./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT --id fe1 &
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
        echo "       last response: $RESP"
        FAIL=$((FAIL + 1))
    fi
}

# Flat-key JSON value extractor
json_val() {
    echo "$2" | grep -o "\"$1\":[^,}]*" | head -1 | sed "s/\"$1\"://;s/\"//g;s/^ *//;s/ *$//"
}

# 1. SPA shell
RESP=$(curl -s $BASE/)
check "GET /  ->  SPA shell" '[[ "$RESP" == *"NCloud"* ]]'

# 2. Signup
RESP=$(curl -s -c $COOKIES -X POST $BASE/api/signup -d 'username=testuser&password=testpass')
OK=$(json_val "ok" "$RESP")
check "POST /api/signup  ->  account created" '[[ "$OK" == "true" ]]'

# 3. Inbox (authenticated, empty)
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
OK=$(json_val "ok" "$RESP")
check "GET /api/inbox  ->  empty inbox (authenticated)" '[[ "$OK" == "true" ]]'

# 4. Send email to self
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/send -d 'to=testuser&subject=Hello&body=Test%20body')
OK=$(json_val "ok" "$RESP")
EMAIL_UID=$(json_val "uid" "$RESP")
check "POST /api/send  ->  email sent, uid=${EMAIL_UID:0:16}..." '[[ "$OK" == "true" ]]'

# 5. Inbox now has one email
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
OK=$(json_val "ok" "$RESP")
HAS_EMAIL=$(echo "$RESP" | grep -c '"uid"' || true)
check "GET /api/inbox  ->  1 email in inbox" '[[ "$OK" == "true" && "$HAS_EMAIL" -ge 1 ]]'

# 5b. Sent folder has one email
RESP=$(curl -s -b $COOKIES "$BASE/api/inbox?folder=sent")
OK=$(json_val "ok" "$RESP")
SENT_COUNT=$(echo "$RESP" | grep -c '"uid"' || true)
check "GET /api/inbox?folder=sent  ->  1 email in sent" '[[ "$OK" == "true" && "$SENT_COUNT" -ge 1 ]]'

# 6. Read email
RESP=$(curl -s -b $COOKIES "$BASE/api/email/$EMAIL_UID")
OK=$(json_val "ok" "$RESP")
SUBJ=$(echo "$RESP" | grep -o '"subject":"[^"]*"' | head -1 | sed 's/"subject":"//;s/"$//')
check "GET /api/email/:uid  ->  email subject correct" '[[ "$OK" == "true" && "$SUBJ" == "Hello" ]]'

# 6b. Read sent email via ?folder=sent
RESP=$(curl -s -b $COOKIES "$BASE/api/email/$EMAIL_UID?folder=sent")
OK=$(json_val "ok" "$RESP")
check "GET /api/email/:uid?folder=sent  ->  sent viewable" '[[ "$OK" == "true" ]]'

# 7. Delete email -- use the inbox-folder uid (send-to-self produces a different
# uid in the recipient's inbox vs. the sender's sent folder; we must address
# delete by inbox uid + folder=inbox)
INBOX_RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
INBOX_UID=$(echo "$INBOX_RESP" | grep -o '"uid":"[^"]*"' | head -1 | sed 's/"uid":"//;s/"$//')
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/delete-email \
    --data-urlencode "uid=$INBOX_UID" --data-urlencode 'folder=inbox')
OK=$(json_val "ok" "$RESP")
check "POST /api/delete-email  ->  deleted" '[[ "$OK" == "true" ]]'

# 8. Inbox empty again (delete-email moves to trash; inbox should have 0 emails)
RESP=$(curl -s -b $COOKIES $BASE/api/inbox)
OK=$(json_val "ok" "$RESP")
EMAIL_COUNT=$(echo "$RESP" | grep -c '"uid"' || true)
check "GET /api/inbox  ->  inbox empty after delete" '[[ "$OK" == "true" && "$EMAIL_COUNT" -eq 0 ]]'

# 9. mkdir /docs
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/mkdir --data-urlencode 'path=/' --data-urlencode 'name=docs')
OK=$(json_val "ok" "$RESP")
check "POST /api/mkdir  ->  /docs folder created" '[[ "$OK" == "true" ]]'

# 10. drive list / shows docs folder
RESP=$(curl -s -b $COOKIES "$BASE/api/drive?path=/")
OK=$(json_val "ok" "$RESP")
HAS_DOCS=$(echo "$RESP" | grep -c '"docs"' || true)
check "GET /api/drive?path=/  ->  shows docs folder" '[[ "$OK" == "true" && "$HAS_DOCS" -ge 1 ]]'

# 11. Upload a file into /docs
TMPFILE=/tmp/pc_upload_$$.txt
echo -n 'NCloud Demo III file' > $TMPFILE
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/upload \
    -F "path=/docs" \
    -F "file=@${TMPFILE};filename=demo.txt;type=text/plain")
rm -f $TMPFILE
OK=$(json_val "ok" "$RESP")
FILE_UID=$(json_val "uid" "$RESP")
FILE_PATH=$(json_val "path" "$RESP")
check "POST /api/upload  ->  uploaded /docs/demo.txt" '[[ "$OK" == "true" ]]'

# 12. drive list /docs shows the file
RESP=$(curl -s -b $COOKIES "$BASE/api/drive?path=/docs")
OK=$(json_val "ok" "$RESP")
HAS_FILE=$(echo "$RESP" | grep -c '"demo.txt"' || true)
check "GET /api/drive?path=/docs  ->  1 file listed" '[[ "$OK" == "true" && "$HAS_FILE" -ge 1 ]]'

# 13. Download file
RESP=$(curl -s -b $COOKIES "$BASE/api/download/$FILE_UID")
check "GET /api/download/:uid  ->  bytes correct" '[[ "$RESP" == "NCloud Demo III file" ]]'

# 14. Rename file
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/rename \
    --data-urlencode "path=$FILE_PATH" --data-urlencode "name=demo-renamed.txt")
OK=$(json_val "ok" "$RESP")
NEW_PATH=$(json_val "path" "$RESP")
if [[ "$OK" == "true" && -n "$NEW_PATH" ]]; then
    FILE_PATH="$NEW_PATH"
fi
check "POST /api/rename  ->  renamed file" '[[ "$OK" == "true" ]]'

# 15. Move file back to / (handle_move expects `dst` param, not `parent`)
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/move \
    --data-urlencode "path=$FILE_PATH" --data-urlencode 'dst=/')
OK=$(json_val "ok" "$RESP")
NEW_PATH=$(json_val "path" "$RESP")
# Only adopt the new path when move actually succeeded; never overwrite to empty
if [[ "$OK" == "true" && -n "$NEW_PATH" ]]; then
    FILE_PATH="$NEW_PATH"
fi
check "POST /api/move  ->  moved file to /" '[[ "$OK" == "true" ]]'

# 15b. drive list / now has the moved file at root
RESP=$(curl -s -b $COOKIES "$BASE/api/drive?path=/")
HAS_MOVED=$(echo "$RESP" | grep -c '"demo-renamed.txt"' || true)
check "GET /api/drive?path=/  ->  moved file at root" '[[ "$HAS_MOVED" -ge 1 ]]'

# 16. Delete the file
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/delete-path --data-urlencode "path=$FILE_PATH")
OK=$(json_val "ok" "$RESP")
check "POST /api/delete-path  ->  deleted file" '[[ "$OK" == "true" ]]'

# 17. Delete the /docs folder
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/delete-path --data-urlencode 'path=/docs')
OK=$(json_val "ok" "$RESP")
check "POST /api/delete-path  ->  deleted /docs folder" '[[ "$OK" == "true" ]]'

# 18. Drive list / empty
RESP=$(curl -s -b $COOKIES "$BASE/api/drive?path=/")
OK=$(json_val "ok" "$RESP")
ITEM_COUNT=$(echo "$RESP" | grep -o '"name"' | wc -l)
check "GET /api/drive?path=/  ->  drive empty" '[[ "$OK" == "true" && "$ITEM_COUNT" -eq 0 ]]'

# 19. Logout
RESP=$(curl -s -b $COOKIES -X POST $BASE/api/logout -d '')
OK=$(json_val "ok" "$RESP")
check "POST /api/logout  ->  session destroyed" '[[ "$OK" == "true" ]]'

# 20. Inbox without auth -> 401
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' $BASE/api/inbox)
check "GET /api/inbox (no auth)  ->  401 Unauthorized" '[[ "$HTTP_CODE" == "401" ]]'

# 21. Admin status endpoint reachable
RESP=$(curl -s $BASE/api/admin/status)
OK=$(json_val "ok" "$RESP")
check "GET /api/admin/status  ->  responds with JSON" '[[ -n "$RESP" ]]'

# 22. Admin SPA page loads
RESP=$(curl -s "$BASE/admin")
HAS_ADMIN=$(echo "$RESP" | grep -c -i 'admin' || true)
check "GET /admin  ->  admin page loads" '[[ "$HAS_ADMIN" -ge 1 ]]'

echo
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ $FAIL -gt 0 ]; then
    exit 1
fi
echo "=== ALL SMOKE TESTS PASSED ==="
