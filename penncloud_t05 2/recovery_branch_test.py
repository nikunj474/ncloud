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


def read_file(path: Path):
    if not path.exists():
        return ""
    return path.read_text(errors="replace")


def clear_logs():
    for name in ("node1.log", "node2.log", "node3.log", "coordinator.log"):
        p = ROOT / name
        if p.exists():
            p.unlink()


def stop_cluster():
    subprocess.run([str(ROOT / "stop_abc_cluster.sh")], check=False)
    time.sleep(1.5)  # let buffered logs flush


def start_cluster():
    subprocess.run([str(ROOT / "stop_abc_cluster.sh")], check=False)
    subprocess.run(["rm", "-rf", str(DATA_ROOT)], check=False)
    clear_logs()
    env = os.environ.copy()
    env["COORD_PORT"] = str(COORD_PORT)
    subprocess.run([str(ROOT / "start_abc_cluster.sh")], check=True, env=env)
    time.sleep(1.5)


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


def graceful_kill_port(port: int):
    subprocess.run(f"pkill -f 'kvstore/kvserver --port {port}'", shell=True, check=False)
    time.sleep(5.0)


def hard_kill_port(port: int):
    subprocess.run(f"pkill -9 -f 'kvstore/kvserver --port {port}'", shell=True, check=False)
    time.sleep(5.0)


def is_server_up(port: int):
    try:
        out = get(port, "probe_row", "probe_col")
        return bool(out)
    except Exception:
        return False


def pick_alive_primary_port():
    for _ in range(20):
        for port in (N3, N2):
            if is_server_up(port):
                return port
        time.sleep(0.5)
    fail("could not find promoted primary on node2/node3")


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


def assert_log_contains_after_stop(label: str, logfile: str, needle: str):
    text = read_file(ROOT / logfile)
    if needle not in text:
        print(f"===== log dump: {logfile} =====")
        print(text)
        fail(f"{label}: expected log marker {needle!r} in {logfile}")
    print("[PASS]", label)


def test_delta_branch():
    print("=== TEST DELTA BRANCH ===")
    start_cluster()

    out = put(N1, "delta_r1", "c1", "v1")
    assert_contains("baseline put", out, "+OK")

    # Hard-kill node1 so it does NOT write a checkpoint before going down.
    hard_kill_port(N1)

    pport = pick_alive_primary_port()
    out = put(pport, "delta_r2", "c1", "v2")
    assert_contains("write while node1 down", out, "+OK")

    start_node("node1", 5500, 5600)

    out = wait_for_value(N1, "delta_r2", "c1", "v2", timeout=12)
    assert_contains("restarted node1 sees delta-applied value", out, "v2")

    stop_cluster()
    assert_log_contains_after_stop("delta branch observed", "node1.log", "using DELTA")


def test_snapshot_branch():
    print("=== TEST SNAPSHOT BRANCH ===")
    start_cluster()

    out = put(N1, "snap_r1", "c1", "v1")
    assert_contains("baseline put", out, "+OK")

    # Gracefully stop node1 first so it writes a checkpoint and advances its
    # own checkpoint_version before going down.
    graceful_kill_port(N1)

    pport = pick_alive_primary_port()
    out = put(pport, "snap_r2", "c1", "v2")
    assert_contains("write while node1 down", out, "+OK")

    start_node("node1", 5500, 5600)

    out = wait_for_value(N1, "snap_r2", "c1", "v2", timeout=12)
    assert_contains("restarted node1 sees snapshot-applied value", out, "v2")

    stop_cluster()
    assert_log_contains_after_stop("snapshot branch observed", "node1.log", "using SNAPSHOT")


if __name__ == "__main__":
    try:
        test_delta_branch()
        test_snapshot_branch()
        print("\n=== RECOVERY BRANCH TESTS PASSED ===")
    finally:
        stop_cluster()
