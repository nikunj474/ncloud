#!/usr/bin/env python3
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COORD_PORT = int(os.environ.get("COORD_PORT", "6011"))
DATA_ROOT = Path("/tmp/pc_2tablet_cluster")
CFG_PATH = ROOT / "coordinator" / "coordinator_2tablet.conf"

N1, N2, N3 = 5500, 5501, 5502
R1, R2, R3 = 5600, 5601, 5602


def fail(msg):
    print("[FAIL]", msg)
    sys.exit(1)


def assert_true(label, cond, details=""):
    if not cond:
        fail(f"{label}: {details}")
    print("[PASS]", label)


def read_all_resp(sock):
    out = b""
    while True:
        c = sock.recv(4096)
        if not c:
            break
        out += c
    return out.decode(errors="replace")


def coord_query(op: str, row: str) -> str:
    rb = row.encode()
    s = socket.create_connection(("127.0.0.1", COORD_PORT), timeout=3)
    msg = f"{op} {len(rb)}\r\n".encode() + rb
    s.sendall(msg)
    s.shutdown(socket.SHUT_WR)
    out = read_all_resp(s)
    s.close()
    return out.strip()


def parse_lookup(resp: str):
    if not resp.startswith("+OK "):
        fail(f"lookup failed: {resp!r}")
    parts = resp[4:].split()
    if len(parts) < 3:
        fail(f"malformed lookup response: {resp!r}")
    return parts[0], int(parts[1]), parts[2]


def stop_cluster():
    subprocess.run([str(ROOT / "stop_abc_cluster.sh")], check=False)
    subprocess.run("pkill -f '/coordinator/coordinator' 2>/dev/null || true", shell=True, check=False)
    subprocess.run("pkill -f 'kvstore/kvserver --port 5500' 2>/dev/null || true", shell=True, check=False)
    subprocess.run("pkill -f 'kvstore/kvserver --port 5501' 2>/dev/null || true", shell=True, check=False)
    subprocess.run("pkill -f 'kvstore/kvserver --port 5502' 2>/dev/null || true", shell=True, check=False)
    subprocess.run(f"lsof -tiTCP:{COORD_PORT} -sTCP:LISTEN | xargs kill -9 2>/dev/null || true", shell=True, check=False)
    time.sleep(1.5)


def start_node(node_id: str, port: int, repl_port: int, replicas=()):
    node_data = DATA_ROOT / node_id
    node_data.mkdir(parents=True, exist_ok=True)
    log_path = ROOT / f"{node_id}_2tablet.log"

    cmd = [
        str(ROOT / "kvstore" / "kvserver"),
        "--port", str(port),
        "--data", str(node_data),
        "--tablet", "tablet0",
        "--node-id", node_id,
        "--repl-port", str(repl_port),
    ]
    for rep in replicas:
        cmd.extend(["--replica", rep])

    with open(log_path, "w") as f:
        subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)


def start_cluster():
    stop_cluster()
    subprocess.run(["rm", "-rf", str(DATA_ROOT)], check=False)

    # Start the same 3-node KV cluster pattern as ABC.
    start_node("node2", N2, R2)
    time.sleep(1.0)

    start_node("node3", N3, R3)
    time.sleep(1.0)

    start_node(
        "node1", N1, R1,
        replicas=(f"node2@127.0.0.1:{R2}", f"node3@127.0.0.1:{R3}")
    )
    time.sleep(2.0)

    # Start coordinator with the 2-tablet config.
    coord_log = ROOT / "coordinator_2tablet.log"
    with open(coord_log, "w") as f:
        subprocess.Popen([
            str(ROOT / "coordinator" / "coordinator"),
            "--port", str(COORD_PORT),
            "--config", str(CFG_PATH),
        ], stdout=f, stderr=subprocess.STDOUT)

    time.sleep(2.0)


def main():
    try:
        start_cluster()

        # Low-range row: should map successfully.
        low_lookup = coord_query("LOOKUP", "apple_row")
        host, port, role = parse_lookup(low_lookup)
        assert_true("LOOKUP works for low-range row", port in (5500, 5501, 5502), low_lookup)

        low_read = coord_query("READLOOKUP", "apple_row")
        host2, port2, role2 = parse_lookup(low_read)
        assert_true("READLOOKUP works for low-range row", port2 in (5500, 5501, 5502), low_read)

        # High-range row: should also map successfully.
        high_lookup = coord_query("LOOKUP", "zebra_row")
        host3, port3, role3 = parse_lookup(high_lookup)
        assert_true("LOOKUP works for high-range row", port3 in (5500, 5501, 5502), high_lookup)

        high_read = coord_query("READLOOKUP", "zebra_row")
        host4, port4, role4 = parse_lookup(high_read)
        assert_true("READLOOKUP works for high-range row", port4 in (5500, 5501, 5502), high_read)

        # Out-of-range row before 'a' should fail cleanly.
        bad = coord_query("LOOKUP", "0bad_row")
        assert_true("out-of-range row is rejected cleanly", bad.startswith("-ERR"), bad)

        print("\n=== TWO-TABLET ROUTING SANITY PASSED ===")
    finally:
        stop_cluster()


if __name__ == "__main__":
    main()
