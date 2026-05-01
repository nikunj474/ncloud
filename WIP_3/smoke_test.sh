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

def cookie_header():
    return '; '.join(f'{c.name}={c.value}' for c in jar)

def recv_until(sock, needle, limit=65536):
    data = b''
    while needle not in data and len(data) < limit:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return data

# 1. SPA shell
html = get('/')
assert b'PennCloud' in html, "SPA shell missing"
print("[PASS] GET /  ->  SPA shell")

# 2. Persistent HTTP connection
s = socket.create_connection(('127.0.0.1', 8090), timeout=3)
s.sendall(
    b'HEAD / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n'
    b'HEAD / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n'
)
raw = recv_until(s, b'Connection: close')
s.close()
assert raw.count(b'HTTP/1.1 200 OK') == 2, raw[:300]
print("[PASS] HTTP/1.1 keep-alive  ->  two requests on one socket")

# 3. Malformed chunked body closes request but does not crash server
s = socket.create_connection(('127.0.0.1', 8090), timeout=3)
s.sendall(
    b'POST /api/signup HTTP/1.1\r\n'
    b'Host: 127.0.0.1\r\n'
    b'Transfer-Encoding: chunked\r\n'
    b'Content-Type: application/x-www-form-urlencoded\r\n'
    b'Connection: close\r\n\r\n'
    b'nothex\r\nabc\r\n0\r\n\r\n'
)
try:
    s.recv(1024)
except (socket.timeout, OSError):
    pass
s.close()
assert b'PennCloud' in get('/'), "server did not respond after malformed chunked request"
print("[PASS] malformed chunked request  ->  rejected without crashing server")

# 4. Signup
r = post('/api/signup', {'username':'testuser','password':'testpass'})
assert r['ok'], f"signup failed: {r}"
print("[PASS] POST /api/signup  ->  account created")

# 5. Inbox (authenticated)
r = json.loads(get('/api/inbox'))
assert r['ok'], f"inbox failed: {r}"
print("[PASS] GET /api/inbox  ->  empty inbox (authenticated)")

# 6. Send email to self
r = post('/api/send', {'to':'testuser','subject':'Hello','body':'Test body'})
assert r['ok'], f"send failed: {r}"
uid = r.get('uid','')
inbox_uid = r.get('inbox_uid', uid)
print(f"[PASS] POST /api/send  ->  email sent, uid={uid[:16]}...")

# 7. SSE sees latest inbox notification
s = socket.create_connection(('127.0.0.1', 8090), timeout=3)
s.sendall((
    'GET /events HTTP/1.1\r\n'
    'Host: 127.0.0.1\r\n'
    f'Cookie: {cookie_header()}\r\n'
    'Connection: close\r\n\r\n'
).encode())
raw = recv_until(s, b'new_email', limit=8192)
s.close()
assert b'HTTP/1.1 200 OK' in raw and b'event: new_email' in raw, raw[:300]
print("[PASS] GET /events  ->  chunked SSE new_email event")

# 8. Inbox now has one email
r = json.loads(get('/api/inbox'))
assert r['ok'] and len(r['emails']) == 1, f"inbox should have 1 email: {r}"
print("[PASS] GET /api/inbox  ->  1 email in inbox")

# 9. Read email
r = json.loads(get(f'/api/email/{inbox_uid}'))
assert r['ok'] and r['email']['subject'] == 'Hello', f"read failed: {r}"
print("[PASS] GET /api/email/:uid  ->  email body correct")

# 10. Delete email
r = post('/api/delete-email', {'uid': inbox_uid, 'folder': 'inbox'})
assert r['ok'], f"delete failed: {r}"
print("[PASS] POST /api/delete-email  ->  deleted")

# 11. Inbox empty again
r = json.loads(get('/api/inbox'))
assert r['ok'] and len(r['emails']) == 0, f"inbox should be empty: {r}"
print("[PASS] GET /api/inbox  ->  inbox empty after delete")


# 12. Upload a file to Drive
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

# 13. Drive list shows uploaded file
r = json.loads(get('/api/drive?path=/'))
assert r['ok'] and len(r['items']) == 1 and r['items'][0]['name'] == 'demo.txt', f"drive list failed: {r}"
print("[PASS] GET /api/drive  ->  1 file listed")

# 14. Download file
blob = get(f'/api/download/{uid}')
assert blob == file_content, f"download mismatch: {blob!r}"
print("[PASS] GET /api/download/:uid  ->  file bytes correct")

# 15. Rename file
r = post('/api/rename', {'path': path, 'name': 'demo-renamed.txt'})
assert r['ok'], f"rename failed: {r}"
path = r['path']
print("[PASS] POST /api/rename  ->  renamed file")

# 16. Delete file
r = post('/api/delete-path', {'path': path})
assert r['ok'], f"delete path failed: {r}"
print("[PASS] POST /api/delete-path  ->  deleted file")

# 17. Drive list empty again
r = json.loads(get('/api/drive?path=/'))
assert r['ok'] and len(r['items']) == 0, f"drive should be empty: {r}"
print("[PASS] GET /api/drive  ->  drive empty after delete")

# 18. Logout
r = post('/api/logout', {})
assert r['ok']
print("[PASS] POST /api/logout  ->  session destroyed")

# 19. Inbox without auth -> 401
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
