# PennCloud — Team T05
CIS 5050 Software Systems, Spring 2026, University of Pennsylvania

## Team
| Member | Email | Role |
|---|---|---|
| Nikunj | nikunj@seas.upenn.edu | Backend: KV store, WAL, Bloom filter |
| Rohit | rohit57@seas.upenn.edu | Backend: Coordinator, replication, recovery |
| Liudawei | liudawei@seas.upenn.edu | Frontend: HTTP server, webmail, SSE |
| Yke | yke@seas.upenn.edu | Frontend: Drive, admin console |

## Build

```bash
# Build KV store server
cd kvstore && make

# Build frontend server
cd frontend && make

# Build coordinator
cd coordinator && make
```

## Run

```bash
# Terminal 1: KV store (port 5000)
mkdir -p /tmp/pc_data
./kvstore/kvserver --port 5000 --data /tmp/pc_data --tablet main

# Terminal 2: Frontend (port 8080, SMTP on 2500)
./frontend/feserver --port 8080 --kv-host 127.0.0.1 --kv-port 5000 --id fe1 --smtp-port 2500

# Terminal 3: Coordinator (port 6000)
./coordinator/coordinator --port 6000 --config ./coordinator/coordinator.conf
```

Open browser at `http://127.0.0.1:8080`

## Directory Structure

```
kvstore/
  src/
    protocol.h       -- shared wire protocol (KV + frontend)
    bloom.h          -- Bloom filter (B4 innovation)
    tablet.h/.cc     -- in-memory KV store + WAL + checkpoint
    server.h/.cc     -- TCP server + thread pool + request dispatch
    replication.h    -- primary-backup replication + B2 write coalescing
    main.cc          -- entry point

frontend/
  src/
    http.h           -- HttpRequest / HttpResponse structs
    http_reader.h    -- HTTP/1.1 parser + response writer
    session.h        -- session management (stored in KV, FE stateless)
    kv_client.h      -- frontend KV client with connection pool
    fe_server.h      -- FEServer class declaration
    fe_server.cc     -- server impl + SPA shell + auth handlers
    handlers_mail.cc -- inbox, send, view, delete email + SMTP outbound client
    smtp_server.h    -- inbound SMTP server (receives external email)
    main.cc          -- entry point

coordinator/
  src/
    coordinator.cc   -- tablet map + heartbeat + leader election
```

## Coordinator Config (`coordinator/coordinator.conf`)

```
node node1 127.0.0.1 5001
node node2 127.0.0.1 5002
node node3 127.0.0.1 5003
tablet tablet_aa_am aa am node1 node2 node3
tablet tablet_an_zz an   node1 node2 node3
```



- [x] KV server: PUT/GET/CPUT/DELETE, WAL, checkpoint, recovery, Bloom filter, shared_mutex locking
- [x] Frontend: HTTP/1.1, cookies, sessions, SPA shell, auth, webmail handlers
- [x] Coordinator: heartbeat, fault detection, leader election, LOOKUP
- [x] Replication: primary-backup protocol, write coalescing (B2), LSN tracking
- [ ] Drive handlers (Yke — next)
- [ ] Replication wired into server.cc (Rohit — next)
- [x] SMTP inbound/outbound (Liudawei — Phase 2/3)
- [ ] Admin console with live metrics (Yke — Phase 2)
