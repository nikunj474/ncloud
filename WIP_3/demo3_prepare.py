#!/usr/bin/env python3
"""
Prepare and verify Demo 3 data against a running PennCloud frontend.

Full mode creates two accounts, raises their drive quota, creates nested folders,
uploads 10 files (text, PDF, image, audio, video, and larger binary files), sends
pre-populated local emails, and verifies folder listing + download integrity.

Usage:
  python3 demo3_prepare.py --base http://127.0.0.1:8080
  python3 demo3_prepare.py --quick --base http://127.0.0.1:8090

For full Demo 3 compliance, provide a real video if ffmpeg is unavailable:
  DEMO3_VIDEO_PATH=/path/to/sample.mp4 python3 demo3_prepare.py
"""

import argparse
import hashlib
import http.cookiejar
import json
import mimetypes
import os
import shutil
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ACCOUNTS = [("demo_alice", "demoPass123"), ("demo_bob", "demoPass123")]
BOUNDARY = "----PennCloudDemo3Boundary"


def mb(n):
    return int(n * 1024 * 1024)


def pad_file(path: Path, target_size: int):
    current = path.stat().st_size if path.exists() else 0
    if current > target_size:
        return
    with path.open("ab") as f:
        remaining = target_size - current
        chunk = b"\0" * min(1024 * 1024, max(1, remaining))
        while remaining > 0:
            take = min(len(chunk), remaining)
            f.write(chunk[:take])
            remaining -= take


def write_text(path: Path, target_size: int):
    line = b"PennCloud Demo 3 text fixture. The quick brown fox jumps over the lazy dog.\n"
    with path.open("wb") as f:
        written = 0
        while written < target_size:
            take = min(len(line), target_size - written)
            f.write(line[:take])
            written += take


def write_pdf(path: Path, target_size: int):
    body = (
        b"%PDF-1.4\n"
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n"
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
        b"4 0 obj << /Length 73 >> stream\n"
        b"BT /F1 24 Tf 72 700 Td (PennCloud Demo 3 PDF fixture) Tj ET\n"
        b"endstream endobj\n"
        b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n"
    )
    xref_at = len(body)
    trailer = (
        b"xref\n0 6\n0000000000 65535 f \n"
        b"0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \n"
        b"0000000241 00000 n \n0000000365 00000 n \n"
        b"trailer << /Root 1 0 R /Size 6 >>\nstartxref\n"
        + str(xref_at).encode()
        + b"\n%%EOF\n"
    )
    with path.open("wb") as f:
        f.write(body)
        f.write(trailer)
    pad_file(path, target_size)


def write_bmp(path: Path, target_size: int):
    width = 1920
    height = max(1, (target_size - 54 + width * 3 - 1) // (width * 3))
    row_size = ((24 * width + 31) // 32) * 4
    pixel_bytes = row_size * height
    file_size = 54 + pixel_bytes
    header = b"BM" + struct.pack("<IHHI", file_size, 0, 0, 54)
    dib = struct.pack("<IIIHHIIIIII", 40, width, height, 1, 24, 0, pixel_bytes, 2835, 2835, 0, 0)
    with path.open("wb") as f:
        f.write(header)
        f.write(dib)
        for y in range(height):
            row = bytearray()
            for x in range(width):
                row.extend(((x + y) % 256, y % 256, x % 256))
            row.extend(b"\0" * (row_size - width * 3))
            f.write(row)
    pad_file(path, target_size)


def write_wav(path: Path, target_size: int):
    data_size = max(0, target_size - 44)
    byte_rate = 44100 * 2
    block_align = 2
    header = (
        b"RIFF"
        + struct.pack("<I", 36 + data_size)
        + b"WAVEfmt "
        + struct.pack("<IHHIIHH", 16, 1, 1, 44100, byte_rate, block_align, 16)
        + b"data"
        + struct.pack("<I", data_size)
    )
    with path.open("wb") as f:
        f.write(header)
        f.write(b"\0" * data_size)


def write_binary(path: Path, target_size: int, seed: int):
    chunk = hashlib.sha256(f"demo3-{seed}".encode()).digest() * 32768
    with path.open("wb") as f:
        written = 0
        while written < target_size:
            take = min(len(chunk), target_size - written)
            f.write(chunk[:take])
            written += take


def write_video(path: Path, target_size: int, quick: bool):
    provided = os.environ.get("DEMO3_VIDEO_PATH")
    if provided:
        shutil.copyfile(provided, path)
        pad_file(path, target_size)
        return
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        duration = "2" if quick else "8"
        cmd = [
            ffmpeg,
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"testsrc=size=640x360:rate=24:duration={duration}",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        pad_file(path, target_size)
        return
    if quick:
        write_binary(path, target_size, 999)
        return
    raise RuntimeError("Need a real video file: set DEMO3_VIDEO_PATH=/path/to/sample.mp4 or install ffmpeg.")


class Client:
    def __init__(self, base: str):
        self.base = base.rstrip("/")
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar))

    def post_form(self, path, data):
        body = urllib.parse.urlencode(data).encode()
        req = urllib.request.Request(
            self.base + path,
            body,
            {"Content-Type": "application/x-www-form-urlencoded"},
        )
        return json.loads(self.opener.open(req, timeout=60).read())

    def get_json(self, path):
        return json.loads(self.opener.open(self.base + path, timeout=60).read())

    def get_bytes(self, path):
        return self.opener.open(self.base + path, timeout=120).read()

    def upload(self, folder_path, file_path: Path):
        filename = file_path.name
        ctype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        chunks = []
        chunks.append(f"--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"path\"\r\n\r\n{folder_path}\r\n".encode())
        chunks.append(
            (
                f"--{BOUNDARY}\r\n"
                f"Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n"
                f"Content-Type: {ctype}\r\n\r\n"
            ).encode()
        )
        chunks.append(file_path.read_bytes())
        chunks.append(f"\r\n--{BOUNDARY}--\r\n".encode())
        body = b"".join(chunks)
        req = urllib.request.Request(
            self.base + "/api/upload",
            body,
            {"Content-Type": f"multipart/form-data; boundary={BOUNDARY}"},
        )
        return json.loads(self.opener.open(req, timeout=180).read())


def ensure_account(base, username, password):
    c = Client(base)
    r = c.post_form("/api/signup", {"username": username, "password": password})
    if not r.get("ok"):
        r = c.post_form("/api/login", {"username": username, "password": password})
    if not r.get("ok"):
        raise RuntimeError(f"could not login/signup {username}: {r}")
    return c


def ensure_folder(client: Client, parent, name):
    r = client.post_form("/api/mkdir", {"path": parent, "name": name})
    if not r.get("ok") and "exists" not in r.get("error", ""):
        raise RuntimeError(f"mkdir {parent}/{name} failed: {r}")


def generate_assets(asset_dir: Path, quick: bool, max_mb: int):
    asset_dir.mkdir(parents=True, exist_ok=True)
    sizes = [1, 1, 1, 1, 1, 1, 1, 2, 3, max(3, min(max_mb, 5))] if quick else [10, 10, 10, 10, 10, 12, 16, 20, 24, max_mb]
    files = [
        ("text", "01_text_10mb.txt", sizes[0], write_text),
        ("pdf", "02_pdf_10mb.pdf", sizes[1], write_pdf),
        ("image", "03_image_10mb.bmp", sizes[2], write_bmp),
        ("audio", "04_audio_10mb.wav", sizes[3], write_wav),
        ("video", "05_video_10mb.mp4", sizes[4], None),
        ("binary", "06_binary_12mb.bin", sizes[5], None),
        ("binary", "07_binary_16mb.bin", sizes[6], None),
        ("binary", "08_binary_20mb.bin", sizes[7], None),
        ("binary", "09_binary_24mb.bin", sizes[8], None),
        ("max", f"10_max_{sizes[9]}mb.bin", sizes[9], None),
    ]
    out = []
    for idx, (kind, name, size_mb, writer) in enumerate(files):
        path = asset_dir / name
        target = mb(size_mb)
        if not path.exists() or path.stat().st_size < target:
            print(f"[asset] generating {name} ({size_mb} MB)")
            if kind == "video":
                write_video(path, target, quick)
            elif writer:
                writer(path, target)
            else:
                write_binary(path, target, idx)
        out.append((kind, path, path.stat().st_size))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("DEMO3_BASE", "http://127.0.0.1:8080"))
    ap.add_argument("--asset-dir", default=os.environ.get("DEMO3_ASSET_DIR", "/tmp/pc_demo3_assets"))
    ap.add_argument("--quick", action="store_true", help="Use small files for endpoint testing only; not Demo 3 compliant.")
    ap.add_argument("--max-mb", type=int, default=int(os.environ.get("DEMO3_MAX_MB", "50")))
    args = ap.parse_args()

    if not args.quick and args.max_mb < 10:
        raise SystemExit("--max-mb must be at least 10 for full Demo 3 prep")
    if args.max_mb >= 63:
        print("[WARN] frontend rejects HTTP bodies over 64MB; keep --max-mb below ~63.")

    stamp = time.strftime("%Y%m%d_%H%M%S")
    demo_root = f"/demo3_{stamp}"
    asset_dir = Path(args.asset_dir)
    assets = generate_assets(asset_dir, args.quick, args.max_mb)

    clients = {u: ensure_account(args.base, u, p) for u, p in ACCOUNTS}
    for user, client in clients.items():
        quota_mb = 32 if args.quick else max(256, args.max_mb + 220)
        r = client.post_form("/api/quota", {"limit_mb": str(quota_mb)})
        if not r.get("ok"):
            raise RuntimeError(f"quota update failed for {user}: {r}")

    alice = clients["demo_alice"]
    bob = clients["demo_bob"]

    folders = [
        ("/", demo_root.strip("/")),
        (demo_root, "docs"),
        (demo_root, "media"),
        (f"{demo_root}/media", "images"),
        (f"{demo_root}/media", "audio"),
        (f"{demo_root}/media", "video"),
        (demo_root, "large"),
        (f"{demo_root}/large", "nested"),
    ]
    for parent, name in folders:
        ensure_folder(alice, parent, name)

    destinations = {
        "text": f"{demo_root}/docs",
        "pdf": f"{demo_root}/docs",
        "image": f"{demo_root}/media/images",
        "audio": f"{demo_root}/media/audio",
        "video": f"{demo_root}/media/video",
        "binary": f"{demo_root}/large/nested",
        "max": f"{demo_root}/large",
    }

    uploaded = []
    for kind, path, expected_size in assets:
        dest = destinations[kind]
        print(f"[upload] {path.name} -> {dest}")
        r = alice.upload(dest, path)
        if not r.get("ok"):
            raise RuntimeError(f"upload failed for {path.name}: {r}")
        uploaded.append((path, r["uid"], expected_size))

    print("[mail] seeding local emails")
    mail_cases = [
        (alice, "demo_bob", "Demo 3: welcome", "Pre-populated message from Alice to Bob."),
        (alice, "demo_bob", "Demo 3: reply/forward test", "Use this email to demo reply, forward, and delete."),
        (bob, "demo_alice", "Demo 3: Bob inbox seed", "Bob-to-Alice seed mail."),
        (bob, "demo_alice", "Demo 3: recovery scenario note", "Keep this visible during backend failover tests."),
    ]
    for sender, to, subject, body in mail_cases:
        r = sender.post_form("/api/send", {"to": to, "subject": subject, "body": body})
        if not r.get("ok"):
            raise RuntimeError(f"send failed: {r}")

    print("[verify] folder list and download integrity")
    listing = alice.get_json("/api/drive?path=" + urllib.parse.quote(f"{demo_root}/large", safe=""))
    if not listing.get("ok"):
        raise RuntimeError(f"drive listing failed: {listing}")
    for path, uid, expected_size in uploaded[:3] + uploaded[-1:]:
        data = alice.get_bytes(f"/api/download/{uid}")
        if len(data) != expected_size:
            raise RuntimeError(f"download size mismatch for {path.name}: {len(data)} != {expected_size}")

    alice_inbox = alice.get_json("/api/inbox")
    bob_inbox = bob.get_json("/api/inbox")
    if not alice_inbox.get("ok") or not bob_inbox.get("ok"):
        raise RuntimeError("inbox verification failed")

    print("\n=== DEMO 3 PREP COMPLETE ===")
    print(f"Frontend: {args.base}")
    print("Accounts:")
    for u, p in ACCOUNTS:
        print(f"  {u} / {p}")
    print(f"Drive root: {demo_root}")
    print(f"Uploaded files: {len(uploaded)}")
    print(f"Max prepared file: {uploaded[-1][0].name} ({uploaded[-1][2] // (1024 * 1024)} MB)")
    if args.quick:
        print("NOTE: --quick is only for endpoint testing; run full mode before the actual demo.")


if __name__ == "__main__":
    try:
        main()
    except (urllib.error.URLError, ConnectionError, RuntimeError, subprocess.CalledProcessError) as e:
        print(f"[FAIL] {e}", file=sys.stderr)
        sys.exit(1)
