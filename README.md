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

# Build inbound SMTP server
cd smtp_server && make

# Build coordinator
cd coordinator && make
```

## Run

```bash
# Terminal 1: KV store (port 5000)
mkdir -p /tmp/pc_data
./kvstore/kvserver --port 5000 --data /tmp/pc_data --tablet main

# Terminal 2: Frontend (port 8080)
./frontend/feserver --port 8080 --kv-host 127.0.0.1 --kv-port 5000 --id fe1

# Terminal 3: Inbound SMTP server (port 2525)
./smtp_server/smtp_server --port 2525 --kv-host 127.0.0.1 --kv-port 5000

# Terminal 4: Coordinator (port 6000)
./coordinator/coordinator --port 6000 --config ./coordinator/coordinator.conf
```

Open browser at `http://127.0.0.1:8080`

## Testing

```bash
# Builds/runs a local KV + frontend pair and checks:
# - SPA shell
# - HTTP/1.1 keep-alive
# - malformed chunked request rejection
# - signup/session/auth
# - local webmail send/read/delete
# - SSE new_email streaming
# - drive upload/download/rename/delete
bash smoke_test.sh

# Demo 3 data preparation against an already-running frontend.
# Full mode creates two accounts, nested folders, 10 uploaded files
# ranging from 10 MB to DEMO3_MAX_MB, and pre-populated emails.
# If ffmpeg is unavailable, provide a real sample video:
#   DEMO3_VIDEO_PATH=/path/to/sample.mp4 python3 demo3_prepare.py
python3 demo3_prepare.py --base http://127.0.0.1:8080

# Fast endpoint check with small files only; not Demo 3 compliant.
python3 demo3_prepare.py --quick --base http://127.0.0.1:8080

# Replication surface tests
./start_replication_test_cluster.sh
python3 repl_write_surface_test.py
python3 repl_secondary_failure_test.py
./stop_replication_test_cluster.sh

# 3-replica primary failure/recovery scenario
./stop_abc_cluster.sh
rm -rf /tmp/pc_abc_cluster
./start_abc_cluster.sh
python3 abc_kill_scenario_test.py
./stop_abc_cluster.sh

# Multi-tablet/multi-group placement check
./stop_multi_group_cluster.sh
./start_multi_group_cluster.sh
python3 multi_group_integrated_test.py
./stop_multi_group_cluster.sh
```

Inbound SMTP receives local mail on port `2525`. External relay from the inbound SMTP server is disabled by default to avoid an open relay; set `SMTP_ALLOW_INBOUND_RELAY=1` only for a controlled demo. Frontend outbound SMTP still uses `SMTP_MODE=direct` or `SMTP_MODE=relay` from `smtp.env.example`.

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
    handlers_mail.cc -- inbox, send, view, delete email handlers
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

For single-node development, no config file needed — defaults to one node on port 5000.

## Status (Mar 19, 2026)

- [x] KV server: PUT/GET/CPUT/DELETE, WAL, checkpoint, recovery, Bloom filter, shared_mutex locking
- [x] Frontend: HTTP/1.1, cookies, sessions, SPA shell, auth, webmail handlers
- [x] Coordinator: heartbeat, fault detection, leader election, LOOKUP
- [x] Replication: primary-backup protocol, write coalescing (B2), LSN tracking
- [ ] Drive handlers (Yke — next)
- [ ] Replication wired into server.cc (Rohit — next)
- [x] SMTP inbound/outbound (Liudawei)
- [ ] Admin console with live metrics (Yke — Phase 2)


## Replication test cluster

Use the helper script below to launch a real 2-node replicated KV pair for the replication surface tests:

```bash
./start_replication_test_cluster.sh
python3 repl_write_surface_test.py
python3 repl_secondary_failure_test.py
./stop_replication_test_cluster.sh
```

Important: `--replica` expects the peer's **replication port**, not its normal client port.
