import socket

def send_cmd(host, port, payload: bytes):
    s = socket.create_connection((host, port))
    s.settimeout(5)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        try:
            chunk = s.recv(4096)
        except socket.timeout:
            break
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

row = "fail:row"
col = "fail:col"

print("PUT to primary after secondary failure:")
print(put("127.0.0.1", 5500, row, col, "after-secondary-down").decode(errors="replace"))

print("\nGET from primary after failed-secondary write:")
print(get("127.0.0.1", 5500, row, col).decode(errors="replace"))
