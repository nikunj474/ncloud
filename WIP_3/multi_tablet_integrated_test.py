import json
import socket
import sys

COORD_PORT = 7010

def req(port, payload: bytes) -> str:
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    out = b""
    while True:
        c = s.recv(4096)
        if not c:
            break
        out += c
    s.close()
    return out.decode(errors="replace")

def put(port, row, col, val):
    rb, cb, vb = row.encode(), col.encode(), val.encode()
    msg = f"PUT {len(rb)} {len(cb)} {len(vb)}\r\n".encode() + rb + cb + vb
    return req(port, msg)

def get(port, row, col):
    rb, cb = row.encode(), col.encode()
    msg = f"GET {len(rb)} {len(cb)}\r\n".encode() + rb + cb
    return req(port, msg)

def lookup(row):
    rb = row.encode()
    msg = f"LOOKUP {len(rb)}\r\n".encode() + rb
    return req(COORD_PORT, msg)

def tablets():
    out = req(COORD_PORT, b"TABLETS\r\n")
    if not out.startswith("+OK "):
        raise RuntimeError(out)
    header, body = out.split("\r\n", 1)
    return json.loads(body)

def assert_ok(cond, msg):
    if not cond:
        print("[FAIL]", msg)
        sys.exit(1)
    print("[PASS]", msg)

print("[1] tablet placement should list 3 tablets")
tabs = tablets()
assert_ok(len(tabs) == 3, "3 tablets visible in coordinator")

names = {t["name"] for t in tabs}
assert_ok(names == {"tabletA", "tabletB", "tabletC"}, "tablet names match expected")

print("[2] each tablet should have 3 replicas")
for t in tabs:
    assert_ok(len(t.get("replicas", [])) == 3, f'{t["name"]} has 3 replicas')

print("[3] write rows in all 3 ranges")
assert_ok(put(6500, "apple", "v", "A").startswith("+OK"), "write apple to tabletA range")
assert_ok(put(6500, "horse", "v", "H").startswith("+OK"), "write horse to tabletB range")
assert_ok(put(6500, "zebra", "v", "Z").startswith("+OK"), "write zebra to tabletC range")

print("[4] coordinator LOOKUP should route different ranges to different primaries")
la = lookup("apple")
lb = lookup("horse")
lc = lookup("zebra")
print("LOOKUP apple:", la.strip())
print("LOOKUP horse:", lb.strip())
print("LOOKUP zebra:", lc.strip())
assert_ok("6500" in la, "apple routed to node1 primary group")
assert_ok("6501" in lb, "horse routed to node2 primary group")
assert_ok("6502" in lc, "zebra routed to node3 primary group")

print("[5] reads should succeed for all rows")
ga = get(6500, "apple", "v")
gb = get(6501, "horse", "v")
gc = get(6502, "zebra", "v")
print(ga.strip())
print(gb.strip())
print(gc.strip())
assert_ok(ga.startswith("+OK 1"), "read apple works")
assert_ok(gb.startswith("+OK 1"), "read horse works")
assert_ok(gc.startswith("+OK 1"), "read zebra works")

print("\n=== MULTI-TABLET INTEGRATED DEMO PASSED ===")
