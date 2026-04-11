#!/bin/bash
# =============================================================================
# test_mail_features.sh  --  Tests for webmail: sent box, read/unread, sent view
# =============================================================================
set -e

KV_PORT=5070
FE_PORT=8097
SMTP_PORT=2507
DATA_DIR=/tmp/pc_mail_test_$$
BASE="http://127.0.0.1:${FE_PORT}"

echo "=== Mail Feature Tests ==="

make -C kvstore -s 2>/dev/null
make -C frontend -s 2>/dev/null

mkdir -p "$DATA_DIR"

./kvstore/kvserver --port $KV_PORT --data "$DATA_DIR" --tablet mailtest > /tmp/pc_mail_kv_$$.log 2>&1 &
KV_PID=$!
sleep 0.5

./frontend/feserver --port $FE_PORT --kv-host 127.0.0.1 --kv-port $KV_PORT \
    --smtp-port $SMTP_PORT --id fe-mail-test > /tmp/pc_mail_fe_$$.log 2>&1 &
FE_PID=$!
sleep 1

cleanup() {
    kill $KV_PID $FE_PID 2>/dev/null || true
    rm -rf "$DATA_DIR" /tmp/pc_mail_kv_$$.log /tmp/pc_mail_fe_$$.log
}
trap cleanup EXIT

python3 - "$FE_PORT" "$SMTP_PORT" << 'PYEOF'
import json, sys, time, socket
import urllib.request, urllib.parse, http.cookiejar

FE_PORT   = int(sys.argv[1])
SMTP_PORT = int(sys.argv[2])
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

class Session:
    def __init__(self):
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.jar))

    def post(self, path, data):
        body = urllib.parse.urlencode(data).encode()
        req = urllib.request.Request(BASE + path, body,
              {'Content-Type': 'application/x-www-form-urlencoded'})
        return json.loads(self.opener.open(req).read())

    def get(self, path):
        return json.loads(self.opener.open(BASE + path).read())

def smtp_deliver(from_addr, to_addr, subject, body_text):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(("127.0.0.1", SMTP_PORT))
    def recv():
        d = b""
        while True:
            c = s.recv(4096)
            if not c: break
            d += c
            if d.endswith(b"\r\n"): break
        return d.decode().strip()
    def cmd(c):
        s.sendall((c + "\r\n").encode())
        return recv()
    recv()
    cmd("EHLO test.local")
    cmd(f"MAIL FROM:<{from_addr}>")
    cmd(f"RCPT TO:<{to_addr}>")
    cmd("DATA")
    msg = f"From: {from_addr}\r\nTo: {to_addr}\r\nSubject: {subject}\r\n\r\n{body_text}\r\n.\r\n"
    s.sendall(msg.encode())
    resp = recv()
    cmd("QUIT")
    s.close()
    return resp.startswith("250")

# Setup
alice = Session()
bob = Session()
alice.post('/api/signup', {'username': 'alice', 'password': 'pass1'})
bob.post('/api/signup', {'username': 'bob', 'password': 'pass2'})

print("--- Sent Box Tests ---")
print()

# T1: Send -> appears in sent box
r = alice.post('/api/send', {'to': 'bob', 'subject': 'Test1', 'body': 'Body1'})
check("T1: Send email succeeds", r.get('ok'), str(r))
uid1 = r.get('uid', '')

sent = alice.get('/api/sent')
check("T1: Sent box has 1 email",
      sent.get('ok') and len(sent.get('emails', [])) == 1,
      f"count={len(sent.get('emails', []))}")

if sent.get('emails'):
    e = sent['emails'][0]
    check("T1: Sent email subject correct", e.get('subject') == 'Test1', e.get('subject'))
    check("T1: Sent email to field correct", 'bob' in e.get('to', ''), e.get('to'))

# T2: Multiple sends -> sent box accumulates
r = alice.post('/api/send', {'to': 'bob', 'subject': 'Test2', 'body': 'Body2'})
check("T2: Second email sends", r.get('ok'), str(r))

sent = alice.get('/api/sent')
check("T2: Sent box has 2 emails",
      sent.get('ok') and len(sent.get('emails', [])) == 2,
      f"count={len(sent.get('emails', []))}")

# T3: View sent email via ?box=sent
if sent.get('emails'):
    sent_uid = sent['emails'][0]['uid']
    r = alice.get(f'/api/email/{sent_uid}?box=sent')
    check("T3: View sent email succeeds", r.get('ok'), str(r))
    check("T3: Sent email has body field",
          'body' in r.get('email', {}),
          str(r.get('email', {}).keys()))

print()
print("--- Read/Unread Tests ---")
print()

# T4: New email is unread
inbox = bob.get('/api/inbox')
emails = inbox.get('emails', [])
check("T4: Bob has received emails", len(emails) >= 2, f"count={len(emails)}")

if emails:
    check("T4: Newest email is unread",
          emails[0].get('read') == False,
          f"read={emails[0].get('read')}")

# T5: Reading email marks it as read
if emails:
    uid = emails[0]['uid']
    bob.get(f'/api/email/{uid}')

    inbox2 = bob.get('/api/inbox')
    found = [e for e in inbox2.get('emails', []) if e['uid'] == uid]
    check("T5: Email marked as read after viewing",
          found and found[0].get('read') == True,
          f"read={found[0].get('read') if found else 'not found'}")

# T6: Other emails remain unread
if len(emails) >= 2:
    inbox2 = bob.get('/api/inbox')
    other = [e for e in inbox2.get('emails', []) if e['uid'] != uid]
    if other:
        check("T6: Unviewed email still unread",
              other[0].get('read') == False,
              f"read={other[0].get('read')}")

# T7: Delete cleans up read tracking (re-send and verify)
r = alice.post('/api/send', {'to': 'bob', 'subject': 'ToDelete', 'body': 'DeleteMe'})
del_uid = r.get('uid', '')
bob.get(f'/api/email/{del_uid}')
bob.post('/api/delete-email', {'uid': del_uid})
inbox3 = bob.get('/api/inbox')
deleted_present = any(e['uid'] == del_uid for e in inbox3.get('emails', []))
check("T7: Deleted email removed from inbox", not deleted_present, f"uid={del_uid}")

print()
print("--- SMTP + Read Tracking Tests ---")
print()

# T8: SMTP-delivered email is unread
ok = smtp_deliver("ext@example.com", "bob@penncloud", "SMTP Test", "Hello from outside")
check("T8: SMTP delivery succeeds", ok)
time.sleep(0.3)

inbox = bob.get('/api/inbox')
smtp_emails = [e for e in inbox.get('emails', []) if e.get('subject') == 'SMTP Test']
check("T8: SMTP email appears in inbox",
      len(smtp_emails) >= 1,
      f"found={len(smtp_emails)}")
if smtp_emails:
    check("T8: SMTP email is unread",
          smtp_emails[0].get('read') == False,
          f"read={smtp_emails[0].get('read')}")

# T9: Sent box is not affected by incoming SMTP (only outgoing)
# Alice has 3 sent emails: Test1, Test2, ToDelete (from T7)
alice_sent_before = len(alice.get('/api/sent').get('emails', []))
smtp_deliver("another@ext.com", "alice@penncloud", "Incoming Only", "Should not affect sent box")
time.sleep(0.3)
alice_sent_after = len(alice.get('/api/sent').get('emails', []))
check("T9: Inbound SMTP does not add to sent box",
      alice_sent_before == alice_sent_after,
      f"before={alice_sent_before}, after={alice_sent_after}")

print()
print(f"=== Mail Feature Tests: {pass_count} passed, {fail_count} failed ===")
if fail_count > 0:
    sys.exit(1)
print("=== ALL MAIL FEATURE TESTS PASSED ===")
PYEOF
