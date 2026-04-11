#!/bin/bash
# PennCloud smoke test -- run after building all three components
# Usage: ./smoke_test.sh
set -e

KV_PORT=5050
FE_PORT=8090
DATA_DIR=/tmp/pc_smoke_$$

echo "=== PennCloud Smoke Test ==="
mkdir -p $DATA_DIR

# Start KV server
./kvstore/kvserver --port $KV_PORT --data $DATA_DIR --tablet test &
KV_PID=$!
sleep 0.5

# Start Frontend
./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT --id fe1 &
FE_PID=$!
sleep 0.5

cleanup() {
    kill $KV_PID $FE_PID 2>/dev/null
    rm -rf $DATA_DIR
}
trap cleanup EXIT

python3 - << 'PYEOF'
import socket, urllib.request, urllib.parse, http.cookiejar, json, sys

BASE = f"http://127.0.0.1:8090"
jar  = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

def post(path, data):
    body = urllib.parse.urlencode(data).encode()
    req  = urllib.request.Request(BASE+path, body,
           {'Content-Type':'application/x-www-form-urlencoded'})
    return json.loads(opener.open(req).read())

def get(path):
    return opener.open(BASE+path).read()

# 1. SPA shell
html = get('/')
assert b'PennCloud' in html, "SPA shell missing"
print("[PASS] GET /  ->  SPA shell")

# 2. Signup
r = post('/api/signup', {'username':'testuser','password':'testpass'})
assert r['ok'], f"signup failed: {r}"
print("[PASS] POST /api/signup  ->  account created")

# 3. Inbox (authenticated)
r = json.loads(get('/api/inbox'))
assert r['ok'], f"inbox failed: {r}"
print("[PASS] GET /api/inbox  ->  empty inbox (authenticated)")

# 4. Send email to self
r = post('/api/send', {'to':'testuser','subject':'Hello','body':'Test body'})
assert r['ok'], f"send failed: {r}"
uid = r.get('uid','')
print(f"[PASS] POST /api/send  ->  email sent, uid={uid[:16]}...")

# 5. Inbox now has one email, marked unread
r = json.loads(get('/api/inbox'))
assert r['ok'] and len(r['emails']) == 1, f"inbox should have 1 email: {r}"
assert r['emails'][0].get('read') == False, f"new email should be unread: {r['emails'][0]}"
print("[PASS] GET /api/inbox  ->  1 unread email in inbox")

# 5b. Sent box has one email
r = json.loads(get('/api/sent'))
assert r['ok'] and len(r['emails']) == 1, f"sent should have 1 email: {r}"
assert r['emails'][0]['subject'] == 'Hello', f"sent subject mismatch: {r['emails'][0]}"
print("[PASS] GET /api/sent  ->  1 email in sent box")

# 6. Read email -> marks it as read
r = json.loads(get(f'/api/email/{uid}'))
assert r['ok'] and r['email']['subject'] == 'Hello', f"read failed: {r}"
print("[PASS] GET /api/email/:uid  ->  email body correct")

# 6b. Inbox now shows email as read
r = json.loads(get('/api/inbox'))
assert r['ok'] and r['emails'][0].get('read') == True, f"email should be marked read: {r['emails'][0]}"
print("[PASS] GET /api/inbox  ->  email marked as read after viewing")

# 6c. View sent email via ?box=sent
sent_r = json.loads(get('/api/sent'))
sent_uid = sent_r['emails'][0]['uid']
r = json.loads(get(f'/api/email/{sent_uid}?box=sent'))
assert r['ok'] and r['email']['subject'] == 'Hello', f"sent email view failed: {r}"
assert 'body' in r['email'], f"sent email missing body field: {r['email']}"
print("[PASS] GET /api/email/:uid?box=sent  ->  sent email viewable")

# 7. Delete email
r = post('/api/delete-email', {'uid': uid})
assert r['ok'], f"delete failed: {r}"
print("[PASS] POST /api/delete-email  ->  deleted")

# 8. Inbox empty again
r = json.loads(get('/api/inbox'))
assert r['ok'] and len(r['emails']) == 0, f"inbox should be empty: {r}"
print("[PASS] GET /api/inbox  ->  inbox empty after delete")


# 9. Upload a file to Drive
boundary = '----PennCloudBoundary'
file_content = b'PennCloud Demo I file\n'
body = (
    f'--{boundary}\r\n'
    'Content-Disposition: form-data; name="path"\r\n\r\n'
    '/\r\n'
    f'--{boundary}\r\n'
    'Content-Disposition: form-data; name="file"; filename="demo.txt"\r\n'
    'Content-Type: text/plain\r\n\r\n'
).encode() + file_content + f'\r\n--{boundary}--\r\n'.encode()
req = urllib.request.Request(BASE + '/api/upload', data=body, headers={
    'Content-Type': f'multipart/form-data; boundary={boundary}'
})
r = json.loads(opener.open(req).read())
assert r['ok'], f"upload failed: {r}"
uid = r['uid']
path = r['path']
print("[PASS] POST /api/upload  ->  uploaded demo.txt")

# 10. Drive list shows uploaded file
r = json.loads(get('/api/drive?path=/'))
assert r['ok'] and len(r['items']) == 1 and r['items'][0]['name'] == 'demo.txt', f"drive list failed: {r}"
print("[PASS] GET /api/drive  ->  1 file listed")

# 11. Download file
blob = get(f'/api/download/{uid}')
assert blob == file_content, f"download mismatch: {blob!r}"
print("[PASS] GET /api/download/:uid  ->  file bytes correct")

# 12. Rename file
r = post('/api/rename', {'path': path, 'name': 'demo-renamed.txt'})
assert r['ok'], f"rename failed: {r}"
path = r['path']
print("[PASS] POST /api/rename  ->  renamed file")

# 13. Delete file
r = post('/api/delete-path', {'path': path})
assert r['ok'], f"delete path failed: {r}"
print("[PASS] POST /api/delete-path  ->  deleted file")

# 14. Drive list empty again
r = json.loads(get('/api/drive?path=/'))
assert r['ok'] and len(r['items']) == 0, f"drive should be empty: {r}"
print("[PASS] GET /api/drive  ->  drive empty after delete")

# 15. Logout
r = post('/api/logout', {})
assert r['ok']
print("[PASS] POST /api/logout  ->  session destroyed")

# 16. Inbox without auth -> 401
try:
    get('/api/inbox')
    print("[FAIL] should have returned 401")
    sys.exit(1)
except urllib.error.HTTPError as e:
    assert e.code == 401
    print("[PASS] GET /api/inbox (no auth)  ->  401 Unauthorized")

print()
print("=== ALL SMOKE TESTS PASSED ===")
PYEOF
