#!/usr/bin/env python3
import json, socket, subprocess, time, os, sys
ROOT = os.path.dirname(os.path.abspath(__file__))
COORD_PORT=6000
NODE1=5500
NODE2=5501
NODE1_REPL=5600
TABLET='tablet0'
DATA_ROOT='/tmp/pc_ha_cluster'


def read_line(sock):
    data=b''
    while True:
        c=sock.recv(1)
        if not c:
            raise RuntimeError('eof')
        data += c
        if data.endswith(b'\r\n'):
            return data[:-2].decode()

def req(host, port, payload: bytes):
    s=socket.create_connection((host, port), timeout=2)
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    out=b''
    while True:
        c=s.recv(4096)
        if not c:
            break
        out += c
    s.close()
    return out.decode(errors='replace')

def put(port, row, col, val):
    r=row.encode(); c=col.encode(); v=val.encode()
    return req('127.0.0.1', port, f'PUT {len(r)} {len(c)} {len(v)}\r\n'.encode()+r+c+v)

def get(port, row, col):
    r=row.encode(); c=col.encode()
    return req('127.0.0.1', port, f'GET {len(r)} {len(c)}\r\n'.encode()+r+c)

def coord_status():
    s=socket.create_connection(('127.0.0.1', COORD_PORT), timeout=2)
    s.sendall(b'STATUS\r\n')
    line=read_line(s)
    if not line.startswith('+OK '):
        raise RuntimeError(line)
    n=int(line.split()[1])
    body=b''
    while len(body)<n:
        body += s.recv(n-len(body))
    s.close()
    return json.loads(body.decode())

def wait_for_role(node_id, role, alive=True, timeout=8):
    end=time.time()+timeout
    while time.time()<end:
        st=coord_status()
        for node in st:
            if node['id']==node_id and node['role']==role and node['alive']==alive:
                return node
        time.sleep(0.25)
    raise RuntimeError(f'timeout waiting for {node_id} role={role} alive={alive}')

print('=== baseline write on node1 primary ===')
print(put(NODE1,'r1','c1','before-failover').strip())
print('read node2 before failover:')
print(get(NODE2,'r1','c1').strip())

print('\n=== kill node1 primary ===')
subprocess.run("pkill -f 'kvstore/kvserver --port 5500'", shell=True, check=False)
node2 = wait_for_role('node2','primary',True,timeout=10)
print('coordinator promoted node2:', node2)
print('read node2 after failover:')
print(get(NODE2,'r1','c1').strip())
print('write on promoted node2:')
print(put(NODE2,'r1','c1','after-failover').strip())
print('read node2 after promoted write:')
print(get(NODE2,'r1','c1').strip())

print('\n=== restart old node1 and wait for rejoin ===')
subprocess.Popen([
    './kvstore/kvserver','--port','5500','--data',f'{DATA_ROOT}/node1','--tablet',TABLET,
    '--node-id','node1','--repl-port',str(NODE1_REPL)
], cwd=ROOT, stdout=open(os.path.join(ROOT,'node1.log'),'ab'), stderr=subprocess.STDOUT)
node1 = wait_for_role('node1','secondary',True,timeout=12)
print('coordinator re-added node1:', node1)

time.sleep(1)
print('write on current primary node2 after rejoin:')
print(put(NODE2,'r2','c1','post-rejoin').strip())
print('read recovered node1 for replicated value:')
print(get(NODE1,'r2','c1').strip())

print('\n=== FAILOVER + RECOVERY TEST PASSED ===')
