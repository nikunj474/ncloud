<h1 align="center">NCloud</h1>

<p align="center">
  <strong>A fault-tolerant cloud platform — webmail, file storage, and chat — built from the socket layer up in C++17.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/C%2B%2B-17-00599C?style=flat-square&logo=cplusplus" alt="C++17">
  <a href="https://github.com/nikunj474/ncloud/actions/workflows/ci.yml"><img src="https://github.com/nikunj474/ncloud/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/dependencies-libcurl%20only-lightgrey?style=flat-square" alt="Dependencies">
  <img src="https://img.shields.io/badge/docker-compose%20ready-2496ED?style=flat-square&logo=docker" alt="Docker">
  <img src="https://img.shields.io/badge/license-MIT-yellow?style=flat-square" alt="MIT">
</p>

---

NCloud is a Google-Workspace-style suite — Mail, Drive, and Chat — running on a
**replicated, sharded key-value store written from scratch**. There is no web framework,
no ORM, and no database. The HTTP parser, the storage engine, the replication protocol,
the failure detector, the leader election, the SMTP server, and the load balancer are all
original code.

The point of the project is what happens when things break. Kill a storage node
mid-upload and the cluster elects a new primary in about 1.5 seconds, the request
completes, and the dead node resyncs and rejoins when it comes back — all without the
user noticing. You can do this from the browser: the admin console has a kill button for
every node in the cluster.

```
~16,000 lines of C++17  ·  5 services  ·  0 web frameworks  ·  1 third-party library
```

---

## Table of contents

- [Why this project](#why-this-project)
- [Architecture](#architecture)
- [The services](#the-services)
- [What it does](#what-it-does)
- [How the storage engine works](#how-the-storage-engine-works)
- [Fault tolerance in practice](#fault-tolerance-in-practice)
- [Running it](#running-it)
- [Testing](#testing)
- [Deployment](#deployment)
- [Engineering notes](#engineering-notes)
- [Project layout](#project-layout)

---

## Why this project

Most "build a cloud app" projects are a thin veneer over Postgres and Express. This one
goes the other direction: assume nothing exists above the POSIX socket API, and build up
until you have a working consumer product.

That constraint forces you to confront the problems real distributed systems have — write
durability across crashes, how a replica catches up after missing writes, what happens to
an in-flight request when the node serving it dies, how a stateless tier discovers where
data lives, and how to keep a partitioned keyspace balanced.

---

## Architecture

Four independent tiers. Any process in any tier can be killed without taking the system
down.

```
                            Browser
                               │  HTTP/1.1
                               ▼
                  ┌────────────────────────┐
                  │   Load balancer :8088  │  health-checks each frontend
                  └────────────┬───────────┘  round-robins over live frontends
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
      fe1 :8090            fe2 :8091            fe3 :8092     ← stateless app servers
          └────────────────────┼────────────────────┘
                               │  length-prefixed binary protocol over TCP
    ┌──────────────┬───────────┴───────────┬──────────────┐
    ▼              ▼                       ▼              ▼
node1 :7500    node2 :7501            node3 :7502    node4 :7503   ← replicated KV tablets
    └──────────────┴───────────┬───────────┴──────────────┘
                               │  heartbeats · LOOKUP · leader election
                               ▼
                     Coordinator :7110

  SMTP :2525 ─────────────────┘   inbound mail writes straight into the KV layer
```

**The frontends hold no state.** Sessions, mail, files, and chat history all live in the
KV store, so any frontend can serve any request for any user, and losing one costs
nothing but the in-flight connections.

**The coordinator is not in the data path.** It answers "which node owns this row?" and
runs failure detection and elections. Once a frontend has resolved a primary, reads and
writes flow directly to storage — so coordinator latency never appears in a user request.

---

## The services

| Service | Binary | Port | Responsibility |
|:--|:--|:--|:--|
| **Load balancer** | `frontend_lb` | 8088 | Public entry point. TCP health-checks each frontend every second and hands clients to a live one via round-robin redirects. |
| **Frontend** | `feserver` | 8090–8092 | Stateless HTTP app server — Mail, Drive, Chat, auth, SSE, admin console. Custom HTTP/1.1 parser, 32-thread pool. |
| **Coordinator** | `coordinator` | 7110 | Tablet routing, heartbeat failure detection, highest-LSN leader election, automatic node recovery. |
| **Storage node** | `kvserver` | 7500–7503 | Replicated `(row, column) → value` store. WAL, checkpoints, bloom filters, per-row locking. |
| **SMTP server** | `smtp_server` | 2525 | Accepts inbound mail from the public internet and delivers it into the KV store. |

---

## What it does

### Mail

A complete webmail client. Inbox, sent, and trash; compose with file attachments; move to
trash, restore, permanently delete; a contacts list.

Mail is real SMTP in both directions. Inbound messages arrive from external senders on
port 2525 and land in a user's inbox. Outbound mail leaves either through an
authenticated relay over STARTTLS or by direct MX lookup and delivery. When a new message
arrives, the open inbox updates over **Server-Sent Events** rather than polling.

A user's entire mailbox lives in one KV row, with folder indexes kept consistent under
concurrent delivery by compare-and-swap retry loops instead of locks.

### Drive

A virtual filesystem on top of a flat key-value store. Upload, download, create folders,
rename, move, recursively delete — with directory structure modelled as parent/child UID
references, since the storage layer has no concept of a directory.

Files are split into **1 MB chunks** so a large upload never forces the storage node to
hold the whole object in memory. Moves are checked for cycles so a folder can't become its
own ancestor, and per-user quotas are enforced server-side on every upload.

### Chat

Group rooms with persisted history, plus direct messages. DM threads are keyed on a
canonical sorted username pair, so both participants always resolve to the same thread.

### Admin console

The part worth demoing. A live cluster dashboard showing every node's liveness, its LSN,
and which tablets it is primary for — plus **kill and restart buttons for every node and
frontend**.

Killing the frontend you're currently connected to also works: the server finds a healthy
peer, hands your browser off to it, and leaves a redirect stub listening on the dead port
so any straggler request gets bounced somewhere alive.

---

## How the storage engine works

**Data model.** A two-dimensional map — `(row, column) → value` — with arbitrary bytes on
both sides. Four operations: `PUT`, `GET`, `CPUT` (atomic compare-and-swap), and `DELETE`.
The wire protocol is length-prefixed rather than delimited, so binary payloads pass
through without escaping.

**Durability.** Every mutation is appended to a write-ahead log and flushed before it
touches the in-memory map, so a crash between the two loses nothing. Periodic checkpoints
serialise the map and truncate the log, bounding recovery to *load snapshot → replay the
tail* instead of replaying all history.

**Concurrency.** Per-row `shared_mutex` locking, a thread pool per node, and a **bloom
filter** that answers "no such key" without touching disk.

**Sharding.** The keyspace splits into row-key ranges called *tablets*, each owned by its
own replica group. Range partitioning makes a routing lookup a string comparison — no
hash ring, no rebalancing when membership changes:

| Tablet | Rows | Replicas |
|:--|:--|:--|
| tabletA | `a` – `g` | node1, node2, node3 |
| tabletB | `g` – `m` | node2, node3, node4 |
| tabletC | `m` – `s` | node3, node4, node1 |
| tabletD | `s` – end | node4, node1, node2 |

Because each tablet elects its own primary, write load spreads across all four nodes
instead of funnelling through one.

**Replication.** Primary-backup with synchronous forwarding and a log sequence number
stamped on every write. A secondary that spots a gap in its LSN sequence triggers a
resync; a returning node rebuilds from a snapshot plus a WAL delta in seconds. Writes
commit on a quorum of acknowledgements, and a replica too slow to ack is dropped from the
quorum rather than allowed to stall the write path.

---

## Fault tolerance in practice

The coordinator pings every node every 500 ms and declares it dead after three misses. On
losing a primary it promotes **the secondary with the highest LSN** — the replica holding
the most committed data wins. A node that returns is resynced from the current primary and
re-added as a secondary automatically, with no failback churn.

A real session from the running cluster, killing the primary of `tabletA` mid-use:

```
$ ./ncloud_control.sh backend stop node1

  tabletA   primary=node2   alive=[node2, node3]      ← promoted, LSN 57
  tabletB   primary=node2   alive=[node2, node3, node4]
  tabletC   primary=node3   alive=[node3, node4]
  tabletD   primary=node4   alive=[node4, node2]

# with node1 still down — user traffic is unaffected:
  POST /api/signup   → {"ok":true}
  POST /api/send     → {"ok":true,"uid":"…"}
  POST /api/upload   → 5 MB file, SHA-256 verified byte-identical on download
  GET  /api/quota    → {"ok":true,"used_bytes":5200000}

$ ./ncloud_control.sh backend start node1

  node1 alive=True lsn=110   node2 lsn=110   node3 lsn=110   ← resynced and caught up
  tabletA primary=node2 alive=[node2, node1, node3]          ← rejoined as secondary
```

Losing a node is a non-event for the user. Losing every replica of a tablet correctly
fails writes for that key range rather than silently accepting data it cannot durably
store.

---

## Running it

### Docker

```bash
docker build -t ncloud .
docker compose up
```

Open **http://localhost:8088** — admin console at **/admin**.

Only 8088 and 2525 are published; the rest of the cluster talks over an internal network.
To watch failover live:

```bash
docker compose stop node2     # coordinator notices in ~1.5s and promotes a secondary
docker compose start node2    # node2 resyncs from the primary and rejoins
```

### From source

Needs `g++` ≥ 9 (C++17), `make`, libcurl headers, and `netcat`.

```bash
make                                        # builds all five binaries
./ncloud_control.sh coordinator start
./ncloud_control.sh backend start node1  # …node2, node3, node4
./ncloud_control.sh frontend start fe1   # …fe2, fe3
./ncloud_control.sh lb start
./ncloud_control.sh smtp start
```

Swap `start` for `stop` or `restart` on any component — restarting a backend mid-session
*is* the fault-tolerance demo.

For a minimal dev loop with no replication:

```bash
./kvstore/kvserver --port 5000 --data /tmp/pc_data --tablet main &
./frontend/feserver --port 8080 --kv-host 127.0.0.1 --kv-port 5000 --id fe1 &
```

Port tables and troubleshooting live in **[RUNNING.md](RUNNING.md)**.

### Outbound email

Local user-to-user mail works out of the box. To reach real external addresses:

```bash
cp smtp.env.example .smtp.env    # fill in relay host, user, app password
```

Docker Compose picks it up on the next `up`.

### Configuration

Cluster topology is a plain text file the coordinator reads at boot:

```
# node   <id>     <host>      <kv_port> <repl_port>
node     node1    127.0.0.1   7500      7600
node     node2    127.0.0.1   7501      7601

# tablet <id>     <row_start> <row_end> <replicas…>
tablet   tabletA  a           g         node1 node2
tablet   tabletB  g           -         node2 node1
```

Use `-` for an unbounded row bound — `-` as `row_start` means "from the beginning of the
keyspace" and as `row_end` means "to the end". Two rules matter: every tablet range must
be covered by some tablet, and adjacent ranges must share an endpoint (`a g` followed by
`g m`, not `a f` followed by `g l`). A gap between ranges makes every row that falls into
it permanently unroutable, since no tablet claims it.

Prebuilt topologies for single-tablet, two-tablet, three-node, and multi-tablet layouts
ship in [coordinator/](coordinator/). The load balancer takes an equally small
`frontend <id> <host> <port>` file.

| Environment variable | Purpose |
|:--|:--|
| `NCLOUD_MAIL_DOMAIN` | Domain for local mail addresses (default `ncloud.local`) |
| `ADMIN_TOKEN` | Shared secret guarding the admin API |
| `NCLOUD_OPEN_ADMIN` | Disables admin auth entirely — local demos only, never in production |
| `SMTP_MODE`, `SMTP_RELAY_*` | Outbound relay vs. direct MX delivery |

---

## Testing

Beyond unit-level checks, the interesting tests are the ones that break things:

```bash
./smoke_test_curl.sh                  # 25-check end-to-end HTTP surface

./start_abc_cluster.sh                # 3-node replica group
python3 abc_kill_scenario_test.py     # kill → promote → rejoin

./start_replication_test_cluster.sh   # replicated pair
python3 repl_write_surface_test.py
python3 repl_secondary_failure_test.py

./start_multi_tablet_cluster.sh       # sharded keyspace
python3 multi_tablet_integrated_test.py

python3 chaos_loop_test.py            # sustained randomised kill/restart loop
./fault_recovery_smoke.sh
```

`chaos_loop_test.py` is the harshest: each round cold-starts a cluster, writes, kills the
primary (alternating SIGTERM and SIGKILL), writes through the newly elected primary,
kills down to a sole survivor, verifies the data is still there, restarts the dead nodes,
and writes again.

Scenario details are in [ABC_TESTING.md](ABC_TESTING.md) and
[REPLICATION_TESTING.md](REPLICATION_TESTING.md).

---

## Deployment

[scripts/oracle_deploy.sh](scripts/oracle_deploy.sh) and
[scripts/aws_deploy_ec2.sh](scripts/aws_deploy_ec2.sh) take a bare Ubuntu VM to a running
public deployment: install Docker and nginx, upload and build the repo, start the cluster
in multi-tablet mode, configure nginx as the public gateway, and obtain a Let's Encrypt
certificate with a systemd timer for renewal.

---

## Engineering notes

Decisions worth defending, and the honest limits of the current build.

**Range partitioning over consistent hashing.** Routing becomes a string comparison and
the coordinator needs no ring state. The trade is that a hot key range concentrates load
on one replica group; consistent hashing would spread it but would make the coordinator
and rebalancing far more complex.

**One KV row per user's mailbox.** Everything for a user is co-located, so listing a
folder is a single row read. The trade is that all of a user's mail operations contend on
one row — fine for interactive use, a bottleneck for a bulk-mail workload.

**Quota computed on demand.** Usage is calculated by walking the drive tree rather than
maintaining a counter, which removes a whole class of drift bugs from deletes, moves, and
renames. It costs a tree traversal per upload.

**Polling behind the SSE façade.** The push endpoint is real SSE to the browser, but the
server discovers new mail by polling a notification key. True end-to-end push would mean
a pub/sub path from the SMTP server through the KV layer to the frontend holding the
connection.

**Password storage.** Credentials are stored as PBKDF2-HMAC-SHA256 with a per-user 16-byte
salt from `/dev/urandom` and 120,000 iterations — about 100 ms per verification, a cost an
offline guessing attack pays for every candidate. The plaintext never reaches the KV layer,
its write-ahead log, or a checkpoint. SHA-256, HMAC, and PBKDF2 are implemented in
[password.h](frontend/src/password.h) rather than pulled from a crypto library, keeping
libcurl the only third-party dependency; `password_selftest()` checks them against the
published FIPS 180-4, RFC 4231, and RFC 7914 vectors. Comparisons are constant-time, and
accounts predating the change are transparently re-hashed on their next successful login.

**Current limitations.** The storage and coordinator ports carry no authentication and
assume a trusted network — anything internet-facing belongs behind the nginx gateway the
deploy scripts configure, never exposed directly. `NCLOUD_OPEN_ADMIN=1` disables admin
authentication outright and is for local demos only. Replicated multi-tablet hosting is
limited to one tablet per process; lifting it needs the replication protocol to identify
tablets explicitly in sync and recovery messages.

Full design documentation — on-disk formats, wire protocols, per-service KV schemas, and
a longer discussion of bottlenecks — is in **[SYSTEM_DESIGN.md](SYSTEM_DESIGN.md)**.

---

## Project layout

```
coordinator/     tablet routing, heartbeats, leader election, cluster topologies
kvstore/         storage engine: tablets, WAL, checkpoints, bloom filter, replication
frontend/        HTTP server, mail/drive/chat handlers, sessions, KV and SMTP clients
frontend_lb/     health-checking round-robin load balancer
smtp_server/     inbound SMTP daemon
scripts/         AWS and Oracle Cloud deployment automation
docker/          container network configuration
*.sh, *.py       cluster launchers and fault-injection tests
```

| Document | Contents |
|:--|:--|
| [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md) | Complete design reference — formats, protocols, schemas, trade-offs |
| [RUNNING.md](RUNNING.md) | Build, run, ports, troubleshooting |
| [ABC_TESTING.md](ABC_TESTING.md) | Three-node kill-scenario harness |
| [REPLICATION_TESTING.md](REPLICATION_TESTING.md) | Replication test setup |

---

## Credits

Built by a team of five for CIS 5050 (Software Systems) at the University of Pennsylvania:
[David Liu](https://github.com/davidliu02k), [Rohit Sharma](https://github.com/rohit57),
[Kaiyuan Bai](https://github.com/kytttt), Nikunj Agrawal, and Yifan Zhang.

I owned the storage layer: the key-value server and its tablet engine, replication wiring
across multi-tablet groups, primary-backup sequencing, and coordinator failover. The
password hashing rework (PBKDF2-HMAC-SHA256) and this documentation are also mine.
`git shortlog -sne` gives the full breakdown.

## Licence

MIT. See [LICENSE](LICENSE).
