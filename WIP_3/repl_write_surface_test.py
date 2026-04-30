import socket

def send_cmd(host, port, payload: bytes):
    s = socket.create_connection((host, port))
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    s.close()
    return data

def put(host, port, row, col, val):
    rb = row.encode()
    cb = col.encode()
    vb = val.encode()
    payload = f"PUT {len(rb)} {len(cb)} {len(vb)}\r\n".encode() + rb + cb + vb
    return send_cmd(host, port, payload)

def get(host, port, row, col):
    rb = row.encode()
    cb = col.encode()
    payload = f"GET {len(rb)} {len(cb)}\r\n".encode() + rb + cb
    return send_cmd(host, port, payload)

def delete(host, port, row, col):
    rb = row.encode()
    cb = col.encode()
    payload = f"DELETE {len(rb)} {len(cb)}\r\n".encode() + rb + cb
    return send_cmd(host, port, payload)

def cput(host, port, row, col, oldv, newv):
    rb = row.encode()
    cb = col.encode()
    ob = oldv.encode()
    nb = newv.encode()
    payload = (
        f"CPUT {len(rb)} {len(cb)} {len(ob)} {len(nb)}\r\n".encode()
        + rb + cb + ob + nb
    )
    return send_cmd(host, port, payload)

row = "demo:row2"
col = "demo:col2"

print("=== PUT baseline ===")
print(put("127.0.0.1", 5500, row, col, "v1").decode(errors="replace"))

print("\nGET primary after PUT:")
print(get("127.0.0.1", 5500, row, col).decode(errors="replace"))

print("\nGET secondary after PUT:")
print(get("127.0.0.1", 5501, row, col).decode(errors="replace"))

print("\n=== CPUT v1 -> v2 ===")
print(cput("127.0.0.1", 5500, row, col, "v1", "v2").decode(errors="replace"))

print("\nGET primary after CPUT:")
print(get("127.0.0.1", 5500, row, col).decode(errors="replace"))

print("\nGET secondary after CPUT:")
print(get("127.0.0.1", 5501, row, col).decode(errors="replace"))

print("\n=== DELETE ===")
print(delete("127.0.0.1", 5500, row, col).decode(errors="replace"))

print("\nGET primary after DELETE:")
print(get("127.0.0.1", 5500, row, col).decode(errors="replace"))

print("\nGET secondary after DELETE:")
print(get("127.0.0.1", 5501, row, col).decode(errors="replace"))
