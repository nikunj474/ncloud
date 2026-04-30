#!/usr/bin/env python3
import socket, subprocess, time, os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))
N1, N2, N3 = 5500, 5501, 5502
TABLET='tablet0'
DATA_ROOT='/tmp/pc_abc_cluster'


def req(host, port, payload: bytes):
    s = socket.create_connection((host, port), timeout=3)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    out = b''
    while True:
        c = s.recv(4096)
        if not c:
            break
        out += c
    s.close()
    return out.decode(errors='replace')


def put(port, row, col, val):
    r = row.encode(); c = col.encode(); v = val.encode()
    return req('127.0.0.1', port, f'PUT {len(r)} {len(c)} {len(v)}\r\n'.encode() + r + c + v)


def get(port, row, col):
    r = row.encode(); c = col.encode()
    return req('127.0.0.1', port, f'GET {len(r)} {len(c)}\r\n'.encode() + r + c)


def assert_contains(label, text, needle):
    if needle not in text:
        print(f'[FAIL] {label}: expected {needle!r} in {text!r}')
        sys.exit(1)
    print(f'[PASS] {label}')


def find_promoted_primary(timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        for cand, other in ((N2, N3), (N3, N2)):
            try:
                out = get(cand, 'abc_r1', 'c1')
                if 'before-a-kill' not in out:
                    continue
                probe = f'probe-{cand}-{int(time.time()*1000)}'
                pout = put(cand, 'abc_probe', 'c1', probe)
                last = (cand, other, out, pout)
                if '+OK' in pout:
                    return cand, other
            except Exception as e:
                last = str(e)
        time.sleep(0.5)
    print('[FAIL] could not find promoted primary; last=', last)
    sys.exit(1)


def wait_until_node1_catches(value, timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            out = get(N1, 'abc_r4', 'c1')
            last = out
            if value in out:
                return out
        except Exception as e:
            last = str(e)
        time.sleep(0.5)
    print('[FAIL] node1 did not catch up; last=', last)
    sys.exit(1)


print('=== ABC KILL SCENARIO TEST ===')
print('\n1) Baseline write on node1 primary')
out = put(N1, 'abc_r1', 'c1', 'before-a-kill')
print(out.strip())
assert_contains('primary put', out, '+OK')
out2 = get(N2, 'abc_r1', 'c1')
out3 = get(N3, 'abc_r1', 'c1')
print('node2:', out2.strip())
print('node3:', out3.strip())
assert_contains('node2 baseline read', out2, 'before-a-kill')
assert_contains('node3 baseline read', out3, 'before-a-kill')

print('\n2) Kill A=node1 primary')
subprocess.run("pkill -f 'kvstore/kvserver --port 5500'", shell=True, check=False)
pport, sport = find_promoted_primary(timeout=15)
print('promoted primary port:', pport)
out = get(pport, 'abc_r1', 'c1')
print('read on promoted primary:', out.strip())
assert_contains('promoted primary has baseline value', out, 'before-a-kill')

print('\n3) Write after A kill')
out = put(pport, 'abc_r2', 'c1', 'after-a-kill')
print(out.strip())
assert_contains('write after A kill', out, '+OK')
time.sleep(0.5)
out = get(sport, 'abc_r2', 'c1')
print('secondary read:', out.strip())
assert_contains('secondary sees post-failover value', out, 'after-a-kill')

print('\n4) Kill B=remaining secondary')
subprocess.run(f"pkill -f 'kvstore/kvserver --port {sport}'", shell=True, check=False)
time.sleep(2)
out = put(pport, 'abc_r3', 'c1', 'only-one-left')
print(out.strip())
assert_contains('write with only one replica alive', out, '+OK')
out = get(pport, 'abc_r3', 'c1')
print('read on sole survivor:', out.strip())
assert_contains('sole survivor read', out, 'only-one-left')

print('\n5) Restart A=node1 and wait for rejoin')
subprocess.Popen([
    './kvstore/kvserver', '--port', '5500', '--data', f'{DATA_ROOT}/node1', '--tablet', TABLET,
    '--node-id', 'node1', '--repl-port', '5600'
], cwd=ROOT, stdout=open(os.path.join(ROOT, 'node1.log'), 'ab'), stderr=subprocess.STDOUT)

time.sleep(2)
out = put(pport, 'abc_r4', 'c1', 'post-rejoin')
print(out.strip())
assert_contains('write after node1 restart', out, '+OK')
out = wait_until_node1_catches('post-rejoin', timeout=15)
print('read recovered node1:', out.strip())
assert_contains('recovered node1 catches up', out, 'post-rejoin')

print('\n=== ABC KILL SCENARIO TEST PASSED ===')
