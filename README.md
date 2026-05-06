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

## To RUN: 
Open browser at `http://127.0.0.1:8080`


## Testing
```bash
# Builds/runs a local KV + frontend pair using only bash + curl and checks:
# - SPA shell
# - HTTP/1.1 keep-alive
# - malformed chunked request rejection
# - signup/session/auth
# - local webmail send/read/delete
# - SSE new_email streaming
# - drive upload/download/rename/delete
bash smoke_test.sh

# 10 MB binary upload/download correctness and timing.
bash test_10mb.sh

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

## Replication test cluster

Use the helper script below to launch a real 2-node replicated KV pair for the replication surface tests:

```bash
./start_replication_test_cluster.sh
python3 repl_write_surface_test.py
python3 repl_secondary_failure_test.py
./stop_replication_test_cluster.sh
```

Inbound SMTP receives local mail on port `2525`. External relay from the inbound SMTP server is disabled by default to avoid an open relay; set `SMTP_ALLOW_INBOUND_RELAY=1` only for a controlled demo. The course build does not depend on libcurl or any external TLS library; outbound external mail uses direct MX delivery (`SMTP_MODE=direct`). If `SMTP_MODE=relay` is present in an old local `.smtp.env`, the no-libcurl build falls back to direct MX delivery.

## Directory Structure

```
kvstore/
  Makefile
  src/
    protocol.h       -- shared wire protocol (KV + frontend)
    bloom.h          -- Bloom filter (B4 innovation)
    tablet.h/.cc     -- in-memory KV store + WAL + checkpoint
    server.h/.cc     -- TCP server, thread pool, request dispatch, client ops
    replication.h    -- primary-backup replication + B2 write coalescing
    main.cc          -- KV Server entry point

coordinator/
  src/
    coordinator.cc   -- tablet routing, LOOKUP/READ LOOKUP, heartbeat, failover, recovery
  *.conf             -- coordinator configs for single-node, multi-tablet, and replicated demos


frontend/
  Makefile
  src/
    http.h           -- HttpRequest / HttpResponse structs
    http_reader.h    -- HTTP/1.1 parser + response writer, cookies, query params
    session.h        -- session management (stored in KV, FE stateless)
    kv_client.h      -- frontend KV client with connection pool, coordinator lookup/entry
    smtp_client.h    -- SMTP helper for frontend mail sending
    fe_server.h      -- FEServer class declaration
    fe_server.cc     -- server impl + SPA shell + auth handlers
    handlers_mail.cc -- inbox, send, view, delete email handlers
    main.cc          -- frontend server entry point


smtp_server/
  Makefile
  src/
    smtp_server.cc   -- inbound SMTP server and mail delivery into KV
    smtp_client.h    -- SMTP client helper for outbound delivery


frontend_lb/
  Makefile
  src/
    load_balancer.cc -- simple frontend health checker and redirecting load balancer


scripts/
  aws_deploy_ec2.sh   -- EC2 deployment helper
  aws_destroy_ec2.sh  -- EC2 teardown helper


Cluster helpers and tests:
  start_multi_group_cluster.sh      -- launches full 4-node replicated demo cluster
  stop_multi_group_cluster.sh       -- stops full multi-group demo cluster
  start_multi_tablet_cluster.sh     -- launches 3-node multi-tablet demo cluster
  stop_multi_tablet_cluster.sh      -- stops multi-tablet demo cluster
  start_abc_cluster.sh              -- launches 3-replica failover test cluster
  stop_abc_cluster.sh               -- stops ABC failover cluster
  start_replication_test_cluster.sh -- launches 2-node replication test cluster
  stop_replication_test_cluster.sh  -- stops replication test cluster
  smoke_test.sh                     -- end-to-end frontend/KV smoke test
  test_10mb.sh                      -- large upload/download correctness test
  *_test.py                         -- Python integration/failover/replication tests


Docs:
  README.md                -- build, run, and test guide
  SYSTEM_DESIGN.md         -- detailed architecture/design reference
  REPLICATION_TESTING.md   -- replication testing notes
  ABC_TESTING.md           -- 3-node failover scenario notes
  

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

## Status (May 6, 2026)

- [x] KV server: PUT/GET/CPUT/DELETE, WAL, checkpoint, recovery, Bloom filter, shared_mutex locking
- [x] Frontend: HTTP/1.1, cookies, sessions, SPA shell, auth, webmail handlers
- [x] Coordinator: heartbeat, fault detection, leader election, LOOKUP
- [x] Replication: primary-backup protocol, write coalescing (B2), LSN tracking
- [x] Drive handlers 
- [x] Replication wired into server.cc 
- [x] SMTP inbound/outbound 
- [x] Admin console with live metrics 



Important: `--replica` expects the peer's **replication port**, not its normal client port.
