import json, socket, sys, time

COORD_PORT = 7110

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

def wait_for_coordinator(timeout=20):
    """Wait until coordinator is up AND has finished configuring tablet roles."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            # First check TABLETS responds at all.
            tabs_out = req(COORD_PORT, b"TABLETS\r\n")
            if not tabs_out.startswith("+OK "):
                last_err = f"TABLETS: {tabs_out[:80]!r}"
                time.sleep(0.5)
                continue
            # Then check READY -- coordinator sets this only after
            # configure_initial_tablet_roles() has completed (success or timeout).
            ready_out = req(COORD_PORT, b"READY\r\n")
            if ready_out.startswith("+OK"):
                print("[wait] coordinator ready")
                return
            last_err = f"READY: {ready_out.strip()!r}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.5)
    print(f"[FAIL] coordinator not ready after {timeout}s: {last_err}")
    sys.exit(1)

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
    assert out.startswith("+OK "), out
    return json.loads(out.split("\r\n", 1)[1])

def assert_ok(cond, msg):
    if not cond:
        print("[FAIL]", msg)
        sys.exit(1)
    print("[PASS]", msg)

# Wait for coordinator to finish role setup before running any tests.
wait_for_coordinator(timeout=20)

tabs = tablets()
assert_ok(len(tabs) == 4, "4 tablets visible")
for t in tabs:
    assert_ok(len(t["replicas"]) == 3, f'{t["name"]} has 3 replicas')

# prove no server contains the entire store
per_node = {}
for t in tabs:
    for r in t["replicas"]:
        per_node[r["id"]] = per_node.get(r["id"], 0) + 1
assert_ok(all(v < 4 for v in per_node.values()), "no node hosts all tablets")

# writes through coordinator-selected primaries
for row, expected_port, value in [("apple","7500","A"),("goat","7501","G"),("monkey","7502","M"),("zebra","7503","Z")]:
    lu = lookup(row)
    print("LOOKUP", row, lu.strip())
    assert_ok(expected_port in lu, f"{row} routes to expected primary port {expected_port}")
    assert_ok(put(int(expected_port), row, "v", value).startswith("+OK"), f"PUT {row} works")
    assert_ok(get(int(expected_port), row, "v").startswith("+OK 1"), f"GET {row} works")

print("\n=== MULTI-GROUP INTEGRATED DEMO PASSED ===")
