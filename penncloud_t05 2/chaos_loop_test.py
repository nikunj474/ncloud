#!/usr/bin/env python3
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COORD_PORT = int(os.environ.get("COORD_PORT", "6010"))
N1, N2, N3 = 5500, 5501, 5502
DATA_ROOT = Path("/tmp/pc_abc_cluster")

ROUNDS = int(os.environ.get("CHAOS_ROUNDS", "20"))


def read_all_resp(s):
    out = b""
    while True:
        c = s.recv(4096)
        if not c:
            break
        out += c
    return out.decode(errors="replace")


def send_cmd(port, payload: bytes):
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    out = read_all_resp(s)
    s.close()
    return out


def put(port, row, col, val):
    rb, cb, vb = row.encode(), col.encode(), val.encode()
    payload = f"PUT {len(rb)} {len(cb)} {len(vb)}\r\n".encode() + rb + cb + vb
    return send_cmd(port, payload)


def get(port, row, col):
    rb, cb = row.encode(), col.encode()
    payload = f"GET {len(rb)} {len(cb)}\r\n".encode() + rb + cb
    return send_cmd(port, payload)


def fail(msg):
    print("[FAIL]", msg)
    sys.exit(1)


def assert_contains(label, text, needle):
    if needle not in text:
        fail(f"{label}: expected {needle!r} in {text!r}")
    print("[PASS]", label)


def stop_cluster():
    subprocess.run([str(ROOT / "stop_abc_cluster.sh")], check=False)
    subprocess.run("pkill -f '/coordinator/coordinator' 2>/dev/null || true", shell=True, check=False)
    time.sleep(1.5)


def start_cluster():
    stop_cluster()
    subprocess.run(["rm", "-rf", str(DATA_ROOT)], check=False)
    env = os.environ.copy()
    env["COORD_PORT"] = str(COORD_PORT)
    subprocess.run([str(ROOT / "start_abc_cluster.sh")], check=True, env=env)
    time.sleep(2.0)


def start_node(node_id: str, port: int, repl_port: int):
    node_data = DATA_ROOT / node_id
    log_path = ROOT / f"{node_id}.log"
    with open(log_path, "a") as f:
        subprocess.Popen([
            str(ROOT / "kvstore" / "kvserver"),
            "--port", str(port),
            "--data", str(node_data),
            "--tablet", "tablet0",
            "--node-id", node_id,
            "--repl-port", str(repl_port),
        ], stdout=f, stderr=subprocess.STDOUT)
    time.sleep(6.0)


def kill_port(port: int, hard=False):
    if hard:
        subprocess.run(f"pkill -9 -f 'kvstore/kvserver --port {port}'", shell=True, check=False)
    else:
        subprocess.run(f"pkill -f 'kvstore/kvserver --port {port}'", shell=True, check=False)
    time.sleep(5.0)


def is_server_up(port: int):
    try:
        out = get(port, "probe_row", "probe_col")
        return bool(out)
    except Exception:
        return False


def alive_ports():
    out = []
    for p in (N1, N2, N3):
        if is_server_up(p):
            out.append(p)
    return out


def coord_lookup(row: str):
    rb = row.encode()
    s = socket.create_connection(("127.0.0.1", COORD_PORT), timeout=3)
    msg = f"LOOKUP {len(rb)}\r\n".encode() + rb
    s.sendall(msg)
    s.shutdown(socket.SHUT_WR)
    out = b""
    while True:
        c = s.recv(4096)
        if not c:
            break
        out += c
    s.close()
    txt = out.decode(errors="replace").strip()
    if not txt.startswith("+OK "):
        fail(f"LOOKUP failed for row {row!r}: {txt!r}")
    parts = txt[4:].split()
    if len(parts) < 3:
        fail(f"Malformed LOOKUP response: {txt!r}")
    host, port_s, role = parts[0], parts[1], parts[2]
    return host, int(port_s), role


def pick_primary_for_row(row: str):
    _, port, role = coord_lookup(row)
    if role != "primary":
        fail(f"LOOKUP for row {row!r} returned non-primary target: {role}")
    return port


def pick_any_alive_node():
    for _ in range(20):
        for p in (N3, N2, N1):
            if is_server_up(p):
                return p
        time.sleep(0.5)
    fail("could not find any alive node")


def wait_for_value(port: int, row: str, col: str, expected: str, timeout=12):
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            last = get(port, row, col)
            if expected in last:
                return last
        except Exception:
            pass
        time.sleep(0.5)
    fail(f"timed out waiting for {expected!r} on port {port}; last={last!r}")


def verify_on_all_alive(row: str, col: str, expected: str):
    ports = alive_ports()
    if not ports:
        fail("no alive ports during verification")
    for p in ports:
        out = get(p, row, col)
        assert_contains(f"port {p} has {row}/{col}", out, expected)


def run_round(i: int):
    print(f"\n=== CHAOS ROUND {i} ===")
    start_cluster()

    row1 = f"abc_r1_round_{i}"
    row2 = f"abc_r2_round_{i}"
    row3 = f"abc_r3_round_{i}"
    row4 = f"abc_r4_round_{i}"
    col = "c1"
    v1 = f"v{i}_before_a"
    v2 = f"v{i}_after_a"
    v3 = f"sole_{i}"
    v4 = f"v{i}_after_rejoin"

    # Baseline write on original primary.
    out = put(N1, row1, col, v1)
    assert_contains("baseline write on node1", out, "+OK")
    verify_on_all_alive(row1, col, v1)

    # Kill A=node1 primary.
    hard = (i % 2 == 0)
    kill_port(N1, hard=hard)

    # Write via current primary after failover.
    pport = pick_primary_for_row(row2)
    out = put(pport, row2, col, v2)
    assert_contains("write after killing node1", out, "+OK")

    # Kill one remaining secondary if it exists, leaving one survivor.
    survivors = [p for p in alive_ports() if p != pport]
    if survivors:
        kill_port(survivors[0], hard=(i % 3 == 0))

    # Sole survivor write.
    sole_primary = pick_primary_for_row(row3)
    out = put(sole_primary, row3, col, v3)
    assert_contains("write with one replica alive", out, "+OK")

    out = get(sole_primary, row3, col)
    assert_contains("sole survivor readback", out, v3)

    # Restart node1 and verify catch-up.
    start_node("node1", 5500, 5600)
    wait_for_value(N1, row2, col, v2, timeout=15)
    wait_for_value(N1, row3, col, v3, timeout=15)

    # Post-rejoin write MUST go to current primary, not first alive node.
    post_primary = pick_primary_for_row(row4)
    out = put(post_primary, row4, col, v4)
    assert_contains("post-rejoin write", out, "+OK")

    wait_for_value(N1, row4, col, v4, timeout=12)

    print(f"[PASS] round {i} complete")


def main():
    try:
        for i in range(1, ROUNDS + 1):
            run_round(i)
        print(f"\n=== CHAOS LOOP PASSED ({ROUNDS} rounds) ===")
    finally:
        stop_cluster()


if __name__ == "__main__":
    main()
