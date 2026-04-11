#!/bin/bash
# =============================================================================
# test_smtp_e2e.sh  --  End-to-end SMTP integration test
# =============================================================================
# Full workflow:
#   1. Start all services (KV, Frontend+SMTP)
#   2. Create two users (alice, bob)
#   3. Alice sends local email to Bob via HTTP API
#   4. Verify Bob sees it in inbox
#   5. External server sends email to Alice via SMTP
#   6. Verify Alice sees it in inbox
#   7. External server sends email to Bob via SMTP
#   8. Bob reads email, verifies body content
#   9. Bob deletes email, inbox updated
#  10. Alice sends email to Bob via SMTP (cross-user)
#  11. Send via HTTP to nonexistent local user -> still succeeds (no user check on local send)
#  12. External SMTP to nonexistent user -> 550 rejection
# =============================================================================
set -e

KV_PORT=5061
FE_PORT=8096
SMTP_PORT=2506
DATA_DIR=/tmp/pc_e2e_smtp_$$
BASE="http://127.0.0.1:${FE_PORT}"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "[PASS] $1"; }
fail() { FAIL=$((FAIL + 1)); echo "[FAIL] $1 -- $2"; }

echo "=== SMTP End-to-End Test ==="

# Build
echo "Building..."
make -C kvstore -s 2>/dev/null
make -C frontend -s 2>/dev/null

mkdir -p "$DATA_DIR"

# Start services
echo "Starting services..."
./kvstore/kvserver --port $KV_PORT --data "$DATA_DIR" --tablet e2etest > /tmp/pc_e2e_kv_$$.log 2>&1 &
KV_PID=$!
sleep 0.5

./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT \
    --smtp-port $SMTP_PORT --id fe-e2e-test > /tmp/pc_e2e_fe_$$.log 2>&1 &
FE_PID=$!
sleep 1

cleanup() {
    kill $KV_PID $FE_PID 2>/dev/null || true
    rm -rf "$DATA_DIR" /tmp/pc_e2e_kv_$$.log /tmp/pc_e2e_fe_$$.log
}
trap cleanup EXIT

echo "Services up (KV=$KV_PORT, HTTP=$FE_PORT, SMTP=$SMTP_PORT)"
echo

python3 - "$SMTP_PORT" "$FE_PORT" << 'PYEOF'
import socket, json, sys, time
import urllib.request, urllib.parse, http.cookiejar

SMTP_PORT = int(sys.argv[1])
FE_PORT   = int(sys.argv[2])
BASE      = f"http://127.0.0.1:{FE_PORT}"

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

class UserSession:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar))

    def post(self, path, data):
        body = urllib.parse.urlencode(data).encode()
        req = urllib.request.Request(BASE + path, body,
              {'Content-Type': 'application/x-www-form-urlencoded'})
        return json.loads(self.opener.open(req).read())

    def get_json(self, path):
        return json.loads(self.opener.open(BASE + path).read())

    def signup(self):
        return self.post('/api/signup',
                         {'username': self.username, 'password': self.password})

    def login(self):
        return self.post('/api/login',
                         {'username': self.username, 'password': self.password})

    def inbox(self):
        return self.get_json('/api/inbox')

    def get_email(self, uid):
        return self.get_json(f'/api/email/{uid}')

    def send(self, to, subject, body):
        return self.post('/api/send',
                         {'to': to, 'subject': subject, 'body': body})

    def delete_email(self, uid):
        return self.post('/api/delete-email', {'uid': uid})


def smtp_send_email(from_addr, to_addr, subject, body_text):
    """Send an email via raw SMTP to the local SMTP server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(("127.0.0.1", SMTP_PORT))

    def recv():
        data = b""
        while True:
            chunk = s.recv(4096)
            if not chunk: break
            data += chunk
            if data.endswith(b"\r\n"): break
        return data.decode().strip()

    def send_cmd(cmd):
        s.sendall((cmd + "\r\n").encode())
        return recv()

    greeting = recv()
    assert greeting.startswith("220"), f"Bad greeting: {greeting}"

    send_cmd("EHLO e2e-test.local")
    send_cmd(f"MAIL FROM:<{from_addr}>")
    resp = send_cmd(f"RCPT TO:<{to_addr}>")
    if resp.startswith("550"):
        s.close()
        return False, resp
    send_cmd("DATA")

    msg = (
        f"From: {from_addr}\r\n"
        f"To: {to_addr}\r\n"
        f"Subject: {subject}\r\n"
        "\r\n"
        f"{body_text}\r\n"
        ".\r\n"
    )
    s.sendall(msg.encode())
    resp = recv()
    send_cmd("QUIT")
    s.close()
    return resp.startswith("250"), resp


# =====================================================================
# Setup: create users
# =====================================================================
print("--- Setup ---")
alice = UserSession("alice", "alicepass")
bob   = UserSession("bob",   "bobpass")

r = alice.signup()
check("Create user alice", r.get('ok'), str(r))
r = bob.signup()
check("Create user bob", r.get('ok'), str(r))

print()
print("--- E2E Test Scenarios ---")
print()

# =====================================================================
# Scenario 1: Alice sends local email to Bob via HTTP
# =====================================================================
r = alice.send("bob", "Hello Bob", "Hi Bob, this is Alice!")
check("S1: Alice sends email to Bob via HTTP",
      r.get('ok'), str(r))

time.sleep(0.2)
inbox = bob.inbox()
check("S1: Bob's inbox has 1 email",
      inbox.get('ok') and len(inbox.get('emails', [])) == 1,
      f"emails={len(inbox.get('emails', []))}")

if inbox.get('emails'):
    e = inbox['emails'][0]
    check("S1: Email subject correct",
          e.get('subject') == "Hello Bob",
          e.get('subject', ''))
    check("S1: Email from correct",
          "alice" in e.get('from', ''),
          e.get('from', ''))

# =====================================================================
# Scenario 1b: Alice's sent box has the email
# =====================================================================
sent = alice.get_json('/api/sent')
check("S1b: Alice's sent box has 1 email",
      sent.get('ok') and len(sent.get('emails', [])) == 1,
      f"sent emails={len(sent.get('emails', []))}")
if sent.get('emails'):
    check("S1b: Sent email subject correct",
          sent['emails'][0].get('subject') == "Hello Bob",
          sent['emails'][0].get('subject', ''))

# =====================================================================
# Scenario 1c: Bob's inbox email is unread, becomes read after viewing
# (Must run BEFORE get_email which marks as read)
# =====================================================================
inbox = bob.inbox()
if inbox.get('emails'):
    e = inbox['emails'][0]
    check("S1c: Email is initially unread",
          e.get('read') == False,
          f"read={e.get('read')}")

    # Now read the full email (marks as read + verifies body)
    full = bob.get_email(e['uid'])
    check("S1: Email body correct",
          full.get('ok') and "Hi Bob, this is Alice!" in full.get('email', {}).get('body', ''),
          full.get('email', {}).get('body', '')[:80])

    inbox2 = bob.inbox()
    e2 = inbox2['emails'][0]
    check("S1c: Email is read after viewing",
          e2.get('read') == True,
          f"read={e2.get('read')}")

# =====================================================================
# Scenario 2: External server sends email to Alice via SMTP
# =====================================================================
ok, resp = smtp_send_email(
    "ceo@bigcorp.com", "alice@penncloud",
    "Job Offer", "We'd like to offer you a position!")
check("S2: External SMTP delivery to Alice succeeds", ok, resp)

time.sleep(0.3)
inbox = alice.inbox()
check("S2: Alice's inbox has 1 email",
      inbox.get('ok') and len(inbox.get('emails', [])) >= 1,
      f"emails={len(inbox.get('emails', []))}")

if inbox.get('emails'):
    e = inbox['emails'][0]
    check("S2: SMTP email subject correct",
          e.get('subject') == "Job Offer",
          e.get('subject', ''))
    check("S2: SMTP email from correct",
          "ceo@bigcorp.com" in e.get('from', ''),
          e.get('from', ''))

    full = alice.get_email(e['uid'])
    check("S2: SMTP email body correct",
          full.get('ok') and "offer you a position" in full.get('email', {}).get('body', ''),
          full.get('email', {}).get('body', '')[:80])

# =====================================================================
# Scenario 3: External server sends email to Bob via SMTP
# =====================================================================
ok, resp = smtp_send_email(
    "newsletter@news.com", "bob@penncloud",
    "Weekly Digest", "Here is your weekly digest.\nSection 1: Headlines\nSection 2: Tech")
check("S3: External SMTP delivery to Bob succeeds", ok, resp)

time.sleep(0.3)
inbox = bob.inbox()
bob_email_count = len(inbox.get('emails', []))
check("S3: Bob's inbox now has 2+ emails",
      inbox.get('ok') and bob_email_count >= 2,
      f"emails={bob_email_count}")

if inbox.get('emails'):
    # Newest email should be the SMTP one
    e = inbox['emails'][0]
    uid = e.get('uid', '')
    full = bob.get_email(uid)
    body = full.get('email', {}).get('body', '')
    check("S3: Multi-line body preserved",
          "Section 1: Headlines" in body and "Section 2: Tech" in body,
          body[:120])

# =====================================================================
# Scenario 4: Bob deletes an email, inbox updated
# =====================================================================
inbox = bob.inbox()
if inbox.get('emails'):
    uid = inbox['emails'][0]['uid']
    r = bob.delete_email(uid)
    check("S4: Bob deletes newest email", r.get('ok'), str(r))

    time.sleep(0.1)
    inbox2 = bob.inbox()
    check("S4: Bob's inbox count decreased by 1",
          len(inbox2.get('emails', [])) == len(inbox.get('emails', [])) - 1,
          f"before={len(inbox.get('emails', []))}, after={len(inbox2.get('emails', []))}")

# =====================================================================
# Scenario 5: External SMTP to nonexistent user -> 550
# =====================================================================
ok, resp = smtp_send_email(
    "spam@spam.com", "nonexistent_user_12345@penncloud",
    "Spam", "Buy stuff")
check("S5: SMTP to nonexistent user rejected (550)",
      not ok and "550" in resp, resp)

# =====================================================================
# Scenario 6: Rapid-fire SMTP deliveries (stress test)
# =====================================================================
for i in range(5):
    ok, resp = smtp_send_email(
        f"rapid{i}@test.com", "bob@penncloud",
        f"Rapid {i}", f"Body {i}")
    if not ok:
        fail_count += 1
        print(f"[FAIL] S6: Rapid delivery {i} failed: {resp}")
        break

time.sleep(0.5)
inbox = bob.inbox()
rapid_count = sum(1 for e in inbox.get('emails', []) if e.get('subject', '').startswith('Rapid'))
check("S6: All 5 rapid-fire emails delivered",
      rapid_count == 5,
      f"delivered {rapid_count}/5")

# =====================================================================
# Scenario 7: SMTP delivery with special characters in subject
# =====================================================================
ok, resp = smtp_send_email(
    "special@test.com", "alice@penncloud",
    'Re: "Important" <update> & more',
    "Body with special chars: <html>&amp;\"quotes\"")
check("S7: Email with special chars in subject delivered", ok, resp)

time.sleep(0.3)
inbox = alice.inbox()
found = any('Important' in e.get('subject', '') for e in inbox.get('emails', []))
check("S7: Special char email found in inbox", found,
      [e.get('subject') for e in inbox.get('emails', [])][:3])

# =====================================================================
# Summary
# =====================================================================
print()
print(f"=== E2E Tests: {pass_count} passed, {fail_count} failed ===")
if fail_count > 0:
    sys.exit(1)
print("=== ALL E2E TESTS PASSED ===")
PYEOF
