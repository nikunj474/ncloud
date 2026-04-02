#!/bin/bash
# =============================================================================
# test_smtp_unit.sh  --  Unit tests for SMTP inbound/outbound functionality
# =============================================================================
# Tests:
#   1. SMTP server accepts connections and sends 220 greeting
#   2. SMTP EHLO handshake
#   3. Deliver email via SMTP to a local user -> appears in inbox
#   4. RCPT TO unknown user -> 550 error
#   5. DATA with dot-stuffed body -> correct body stored
#   6. Multiple emails via single SMTP session (pipelining)
#   7. RSET clears state mid-session
#   8. Malformed commands -> 502 error
#   9. NOOP -> 250
#  10. QUIT -> 221
# =============================================================================
set -e

KV_PORT=5060
FE_PORT=8095
SMTP_PORT=2505
DATA_DIR=/tmp/pc_smtp_test_$$

PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); echo "[PASS] $1"; }
fail() { FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); echo "[FAIL] $1 -- $2"; }

echo "=== SMTP Unit Tests ==="
mkdir -p "$DATA_DIR"

# Build
echo "Building..."
make -C kvstore -s 2>/dev/null
make -C frontend -s 2>/dev/null

# Start services
./kvstore/kvserver --port $KV_PORT --data "$DATA_DIR" --tablet smtptest > /tmp/pc_smtp_kv_$$.log 2>&1 &
KV_PID=$!
sleep 0.5

./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT \
    --smtp-port $SMTP_PORT --id fe-smtp-test > /tmp/pc_smtp_fe_$$.log 2>&1 &
FE_PID=$!
sleep 1

cleanup() {
    kill $KV_PID $FE_PID 2>/dev/null || true
    rm -rf "$DATA_DIR" /tmp/pc_smtp_kv_$$.log /tmp/pc_smtp_fe_$$.log
}
trap cleanup EXIT

# Create a test user via HTTP API (needed so RCPT TO can validate)
BASE="http://127.0.0.1:${FE_PORT}"
COOKIES=/tmp/pc_smtp_cookies_$$

RESP=$(curl -s -c "$COOKIES" -X POST "$BASE/api/signup" \
    -d 'username=smtpuser&password=testpass123')
OK=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ok',False))" 2>/dev/null)
if [ "$OK" != "True" ]; then
    echo "FATAL: could not create test user: $RESP"
    exit 1
fi
echo "Created test user 'smtpuser'"

# Also create a second user for multi-recipient tests
curl -s -X POST "$BASE/api/signup" -d 'username=smtpuser2&password=testpass123' > /dev/null

echo
echo "--- Running SMTP protocol tests ---"
echo

python3 - "$SMTP_PORT" "$FE_PORT" << 'PYEOF'
import socket, json, sys, time
import urllib.request, urllib.parse, http.cookiejar

SMTP_PORT = int(sys.argv[1])
FE_PORT   = int(sys.argv[2])
BASE      = f"http://127.0.0.1:{FE_PORT}"
SMTP_HOST = "127.0.0.1"

pass_count = 0
fail_count = 0

def check(desc, ok, detail=""):
    global pass_count, fail_count
    if ok:
        pass_count += 1
        print(f"[PASS] {desc}")
    else:
        fail_count += 1
        print(f"[FAIL] {desc} -- {detail}")

# Helper: open SMTP connection
def smtp_connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((SMTP_HOST, SMTP_PORT))
    return s

def smtp_recv(s):
    data = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
        if data.endswith(b"\r\n"):
            break
    return data.decode().strip()

def smtp_send(s, cmd):
    s.sendall((cmd + "\r\n").encode())
    return smtp_recv(s)

def smtp_recv_multiline(s):
    """Read SMTP response, handling multi-line (code-dash) responses."""
    lines = []
    while True:
        line = smtp_recv(s)
        lines.append(line)
        # Multi-line: "250-..." continues, "250 ..." is final
        if len(line) >= 4 and line[3] == '-':
            continue
        break
    return "\n".join(lines)

# Helper: get inbox via HTTP
def get_inbox(username, password):
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    body = urllib.parse.urlencode({'username': username, 'password': password}).encode()
    req = urllib.request.Request(BASE + '/api/login', body,
            {'Content-Type': 'application/x-www-form-urlencoded'})
    opener.open(req)
    resp = opener.open(BASE + '/api/inbox')
    return json.loads(resp.read())

def get_email(username, password, uid):
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    body = urllib.parse.urlencode({'username': username, 'password': password}).encode()
    req = urllib.request.Request(BASE + '/api/login', body,
            {'Content-Type': 'application/x-www-form-urlencoded'})
    opener.open(req)
    resp = opener.open(f"{BASE}/api/email/{uid}")
    return json.loads(resp.read())

# =========================================================================
# Test 1: SMTP greeting
# =========================================================================
s = smtp_connect()
greeting = smtp_recv(s)
check("SMTP server sends 220 greeting", greeting.startswith("220"), greeting)
s.close()

# =========================================================================
# Test 2: EHLO handshake
# =========================================================================
s = smtp_connect()
smtp_recv(s)  # greeting
resp = smtp_send(s, "EHLO testclient.local")
check("EHLO returns 250", resp.startswith("250"), resp)
s.close()

# =========================================================================
# Test 3: Deliver email via SMTP -> appears in inbox
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
resp = smtp_send(s, "MAIL FROM:<sender@external.com>")
check("MAIL FROM accepted", resp.startswith("250"), resp)

resp = smtp_send(s, "RCPT TO:<smtpuser@penncloud>")
check("RCPT TO local user accepted", resp.startswith("250"), resp)

resp = smtp_send(s, "DATA")
check("DATA returns 354", resp.startswith("354"), resp)

# Send email body with headers
email_body = (
    "From: sender@external.com\r\n"
    "To: smtpuser@penncloud\r\n"
    "Subject: SMTP Unit Test\r\n"
    "\r\n"
    "This is a test email delivered via SMTP.\r\n"
    "Line two of the body.\r\n"
    ".\r\n"
)
s.sendall(email_body.encode())
resp = smtp_recv(s)
check("DATA accepted (250 after dot)", resp.startswith("250"), resp)

smtp_send(s, "QUIT")
s.close()

time.sleep(0.3)

# Verify email appears in inbox via HTTP API
inbox = get_inbox("smtpuser", "testpass123")
check("Inbox has at least 1 email after SMTP delivery",
      inbox.get('ok') and len(inbox.get('emails', [])) >= 1,
      json.dumps(inbox)[:200])

if inbox.get('ok') and inbox.get('emails'):
    email = inbox['emails'][0]
    check("Email from field correct",
          "sender@external.com" in email.get('from', ''),
          email.get('from', ''))
    check("Email subject correct",
          email.get('subject') == "SMTP Unit Test",
          email.get('subject', ''))

    uid = email.get('uid', '')
    if uid:
        full = get_email("smtpuser", "testpass123", uid)
        if full.get('ok') and full.get('email'):
            body_text = full['email'].get('body', '')
            check("Email body correct",
                  "This is a test email delivered via SMTP." in body_text,
                  body_text[:100])

# =========================================================================
# Test 4: RCPT TO unknown user -> 550
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
smtp_send(s, "MAIL FROM:<someone@test.com>")
resp = smtp_send(s, "RCPT TO:<nonexistent_user_xyz@penncloud>")
check("RCPT TO unknown user returns 550", resp.startswith("550"), resp)
smtp_send(s, "QUIT")
s.close()

# =========================================================================
# Test 5: Dot-stuffed body preserved correctly
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
smtp_send(s, "MAIL FROM:<dots@example.com>")
smtp_send(s, "RCPT TO:<smtpuser@penncloud>")
smtp_send(s, "DATA")

# Body with lines starting with "." -- must be dot-stuffed
dotted_body = (
    "From: dots@example.com\r\n"
    "To: smtpuser@penncloud\r\n"
    "Subject: Dot Stuffing Test\r\n"
    "\r\n"
    "Normal line\r\n"
    "..This line started with a dot\r\n"
    "...Two dots originally\r\n"
    "End of message\r\n"
    ".\r\n"
)
s.sendall(dotted_body.encode())
resp = smtp_recv(s)
check("Dot-stuffed email accepted", resp.startswith("250"), resp)
smtp_send(s, "QUIT")
s.close()

time.sleep(0.3)

inbox = get_inbox("smtpuser", "testpass123")
if inbox.get('ok') and inbox.get('emails'):
    # Find the dot-stuffing email (should be first = newest)
    dot_email = None
    for e in inbox['emails']:
        if e.get('subject') == 'Dot Stuffing Test':
            dot_email = e
            break
    if dot_email:
        full = get_email("smtpuser", "testpass123", dot_email['uid'])
        body = full.get('email', {}).get('body', '')
        check("Dot-unstuffing: single dot preserved",
              ".This line started with a dot" in body,
              body[:150])
        check("Dot-unstuffing: double dot preserved",
              "..Two dots originally" in body,
              body[:150])

# =========================================================================
# Test 6: Multiple emails in single SMTP session
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")

for i in range(3):
    smtp_send(s, f"MAIL FROM:<batch{i}@example.com>")
    smtp_send(s, "RCPT TO:<smtpuser2@penncloud>")
    smtp_send(s, "DATA")
    msg = (
        f"From: batch{i}@example.com\r\n"
        f"Subject: Batch email {i}\r\n"
        "\r\n"
        f"Batch body {i}\r\n"
        ".\r\n"
    )
    s.sendall(msg.encode())
    resp = smtp_recv(s)

smtp_send(s, "QUIT")
s.close()

time.sleep(0.3)

inbox2 = get_inbox("smtpuser2", "testpass123")
check("Multiple emails in single session: all 3 delivered",
      inbox2.get('ok') and len(inbox2.get('emails', [])) >= 3,
      f"got {len(inbox2.get('emails', []))} emails")

# =========================================================================
# Test 7: RSET clears envelope state
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
smtp_send(s, "MAIL FROM:<reset@example.com>")
smtp_send(s, "RCPT TO:<smtpuser@penncloud>")
resp = smtp_send(s, "RSET")
check("RSET returns 250", resp.startswith("250"), resp)

# DATA without RCPT TO should fail
resp = smtp_send(s, "DATA")
check("DATA after RSET returns 503 (no RCPT TO)",
      resp.startswith("503"), resp)
smtp_send(s, "QUIT")
s.close()

# =========================================================================
# Test 8: Unknown command -> 502
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
resp = smtp_send(s, "VRFY nobody")
check("Unknown command VRFY returns 502", resp.startswith("502"), resp)
resp = smtp_send(s, "XYZZY")
check("Unknown command XYZZY returns 502", resp.startswith("502"), resp)
smtp_send(s, "QUIT")
s.close()

# =========================================================================
# Test 9: NOOP -> 250
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
resp = smtp_send(s, "NOOP")
check("NOOP returns 250", resp.startswith("250"), resp)
smtp_send(s, "QUIT")
s.close()

# =========================================================================
# Test 10: QUIT -> 221
# =========================================================================
s = smtp_connect()
smtp_recv(s)
smtp_send(s, "EHLO testclient.local")
resp = smtp_send(s, "QUIT")
check("QUIT returns 221", resp.startswith("221"), resp)
s.close()

# =========================================================================
# Summary
# =========================================================================
print()
print(f"=== SMTP Unit Tests: {pass_count} passed, {fail_count} failed ===")
if fail_count > 0:
    sys.exit(1)
print("=== ALL SMTP UNIT TESTS PASSED ===")
PYEOF
