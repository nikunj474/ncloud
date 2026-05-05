#!/usr/bin/env python3
import os
import shutil
import socket
import subprocess
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
KV = os.path.join(ROOT, "kvstore", "kvserver")
DATA = "/tmp/pc_multi_tablet_demo"

def req(msg: bytes) -> str:
    s = socket.create_connection(("127.0.0.1", 5055), timeout=3)
    s.sendall(msg)
    s.shutdown(socket.SHUT_WR)
    out = b""
    while True:
        c = s.recv(4096)
        if not c:
            break
        out += c
    s.close()
    return out.decode(errors="replace")

def put(row, col, val):
    rb, cb, vb = row.encode(), col.encode(), val.encode()
    msg = f"PUT {len(rb)} {len(cb)} {len(vb)}\r\n".encode() + rb + cb + vb
    return req(msg)

def get(row, col):
    rb, cb = row.encode(), col.encode()
    msg = f"GET {len(rb)} {len(cb)}\r\n".encode() + rb + cb
    return req(msg)

def stats():
    return req(b"STATS\r\n")

def main():
    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)

    p = subprocess.Popen([
        KV,
        "--port", "5055",
        "--data", DATA,
        "--tablet", "tabletA:a:m",
        "--tablet", "tabletB:n:",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    try:
        time.sleep(2)
        print("[1] PUT row in tabletA range")
        print(put("apple", "v", "A"))

        print("[2] PUT row in tabletB range")
        print(put("zebra", "v", "Z"))

        print("[3] GET row from tabletA")
        print(get("apple", "v"))

        print("[4] GET row from tabletB")
        print(get("zebra", "v"))

        print("[5] STATS should report tablets=2")
        print(stats())
    finally:
        p.terminate()
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()

if __name__ == "__main__":
    main()
