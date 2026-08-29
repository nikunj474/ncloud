# NCloud System Design — Complete Reference

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Layers](#2-architecture-layers)
3. [KV Storage Layer](#3-kv-storage-layer)
4. [Replication & Fault Tolerance](#4-replication--fault-tolerance)
5. [Coordinator](#5-coordinator)
6. [Frontend HTTP Servers](#6-frontend-http-servers)
7. [KV Client](#7-kv-client)
8. [Session Management](#8-session-management)
9. [Email (SMTP)](#9-email-smtp)
10. [Drive (File Storage)](#10-drive-file-storage)
11. [Storage Quota](#11-storage-quota)
12. [Chat](#12-chat)
13. [Admin Panel & HA](#13-admin-panel--ha)
14. [Bottlenecks & Tradeoffs](#14-bottlenecks--tradeoffs)
15. [Configuration Reference](#15-configuration-reference)

---

## 1. System Overview

NCloud is a distributed cloud platform providing email, file storage, and chat services. It is built entirely in C++17 with no external frameworks. The system is partitioned into four independent tiers:

```
Browser
   │  HTTP (port 8090/8091/8092)
   ▼
Frontend Servers (fe1, fe2, fe3)   ← stateless HTTP handlers
   │  custom binary protocol (TCP)
   ▼
KV Storage Nodes (node1, node2, node3)  ← replicated key-value store
   │  binary protocol (TCP)
   ▼
Coordinator (port 7010)            ← tablet assignment + failure detection
```

Every layer is independently scalable and fault-tolerant. There are no shared files, no shared memory, and no external databases.

---

## 2. Architecture Layers

### Why This Architecture?

**Separation of concerns**: The frontend is stateless — it holds no user data in memory. All state lives in the KV layer. This means any frontend can serve any request without coordination with other frontends.

**Tablet-based sharding**: Data is partitioned into row-key ranges (tablets). Each tablet is served by a group of KV nodes. This allows horizontal scaling of storage independently of the frontend tier.

**Single coordinator**: A lightweight coordinator tracks which nodes are alive and which node is primary for each tablet. It is not in the data path — it only answers LOOKUP requests (which KV node owns a row?) and manages failover. The coordinator is not a SPOF for reads/writes once the frontend has a cached primary address.

### Process Map (default multi-tablet cluster)

| Process | Port | Role |
|---|---|---|
| coordinator | 7010 | Tablet routing, failure detection |
| kvserver node1 | 6500 (kv) / 6600 (repl) | Storage replica |
| kvserver node2 | 6501 (kv) / 6601 (repl) | Storage replica |
| kvserver node3 | 6502 (kv) / 6602 (repl) | Storage replica |
| feserver fe1 | 8090 | HTTP frontend |
| feserver fe2 | 8091 | HTTP frontend |
| feserver fe3 | 8092 | HTTP frontend |
| smtp_server | 2525 | Inbound/outbound email |

---

## 3. KV Storage Layer

### Data Model

The KV store is a two-dimensional map: `(row, column) → value`. Both keys and values are arbitrary byte strings. There is no schema enforcement at the storage layer.

**Operations supported:**
- `PUT row col value` — unconditional write
- `GET row col` → value
- `CPUT row col expected replacement` — conditional write (atomic compare-and-swap)
- `DEL row col` — delete a column

### Tablet Partitioning

The keyspace is divided into non-overlapping row-key ranges called **tablets**:

```
tabletA: rows starting with 'a'–'g'  → node1 (primary), node2, node3
tabletB: rows starting with 'h'–'p'  → node2 (primary), node3, node1
tabletC: rows starting with 'q'–end  → node3 (primary), node1, node2
```

**Why tablets?** Partitioning allows different tablets to have different primaries, spreading write load across all three nodes. Without partitioning, a single primary would handle all writes.

**Why row-key ranges?** Range partitioning allows the coordinator to answer LOOKUP requests with a simple string comparison, with no hashing, no consistent hashing ring, and no re-hashing on membership changes.

### On-Disk Format

Each node stores tablet data on disk in two structures:

#### Write-Ahead Log (WAL)
A sequential binary log file (`wal.bin`). Every write (PUT, CPUT, DEL) is appended to the WAL before being applied to the in-memory map. Format per entry:

```
[type:1 byte][lsn:8 bytes LE][rowlen:4 bytes][collen:4 bytes][vallen:4 bytes]
[row bytes][col bytes][val bytes]
```

Type byte: `0x01` = PUT, `0x02` = DELETE.

**Why WAL first?** If the process crashes after writing the WAL entry but before updating the in-memory map, the WAL entry is replayed on restart and the write is not lost. This is the standard "write-ahead" durability guarantee.

#### Checkpoint (snapshot)
Periodically the entire in-memory map is serialized to a binary snapshot file (`checkpoint.bin`). The WAL is then truncated. On restart, recovery is: load checkpoint → replay all WAL entries written after the checkpoint.

**Why checkpoint + WAL instead of WAL-only?** A WAL-only design requires replaying the entire WAL from the beginning on every restart, which grows unboundedly. Checkpoints bound the replay cost.

### In-Memory Map

The active data is a `std::map<std::string, std::map<std::string, std::string>>` (row → column → value). Protected by a per-row mutex for concurrent access, plus a separate `wal_mu_` for serializing WAL writes.

### LSN (Log Sequence Number)

Every write increments a monotonically increasing `uint64_t lsn_`. The LSN is:
- Written into every WAL entry
- Sent to the coordinator on heartbeat pings (so the coordinator knows how up-to-date each replica is)
- Used by secondaries to detect missed operations (gap detection)
- Used during failover to pick the most up-to-date secondary as new primary

---

## 4. Replication & Fault Tolerance

### Replication Model

NCloud uses **primary-backup (1-primary, N-secondary) replication** with synchronous forwarding.

When a frontend writes to the primary:
1. Primary writes to its own WAL and in-memory map
2. Primary immediately forwards the write (with its LSN) to all known secondaries
3. Primary returns success to the frontend

**Why primary-backup and not Paxos/Raft?** Simpler to implement correctly and sufficient for the system's consistency requirements. The coordinator handles failover explicitly rather than through a distributed consensus protocol.

### Forward Path (Primary → Secondary)

The primary maintains a `ReplicationManager` with a list of secondary endpoints. For each write:

```cpp
replicated_put(row, col, val):
  lock(primary_write_mu_)          // serialize concurrent writes
  tablet_.put(row, col, val, &lsn) // write locally, get LSN out-param
  last_repl_lsn_.store(lsn)        // record LSN
  forward_to_all(make_repl_put(lsn, row, col, val))  // send to secondaries
```

`primary_write_mu_` serializes all writes so LSNs are forwarded in order. Secondaries apply operations in LSN order.

### Secondary Gap Detection

When a secondary receives a replicated write with LSN `N`:
- If `N == last_applied + 1`: apply normally
- If `N <= last_applied`: duplicate — discard silently
- If `N > last_applied + 1`: gap detected — log warning, trigger resync from primary

### Resync (Snapshot + Delta)

If a secondary misses operations (e.g., after a restart), it requests a full resync:
1. Primary serializes its entire in-memory map to a binary blob (snapshot)
2. Primary sends snapshot to secondary
3. Secondary atomically replaces its state with the snapshot
4. Primary then sends a WAL delta (all entries since snapshot LSN) so secondary catches up to present

This allows a restarted node to rejoin the cluster within seconds regardless of how many writes it missed.

### Availability-First Tradeoff

If `forward_to_all()` fails (a secondary is unreachable), the primary still returns success. **Writes succeed with just one replica alive.** This is an explicit availability-over-durability choice: the system stays writable during partial failures. The tradeoff is that if the primary crashes immediately after a write that was not forwarded, that write is lost.

**Why?** For a demo/course system, availability during node failures is more important than strict durability. Strict durability would require waiting for a quorum acknowledgment before confirming a write, which would cause write failures whenever any replica is unreachable.

---

## 5. Coordinator

### Responsibilities

1. **Tablet routing**: Answers LOOKUP requests from frontends: "which node is primary for row key X?"
2. **Failure detection**: Pings all KV nodes on a heartbeat timer
3. **Failover**: When a primary dies, promotes the most up-to-date secondary
4. **Recovery**: When a dead node comes back, syncs it from the current primary and adds it as a secondary

### Heartbeat & Failure Detection

- **Interval**: 500ms (configurable via `hb_interval_ms`)
- **Miss threshold**: 3 consecutive missed pings → node declared dead
- **Detection latency**: 500ms × 3 = **~1.5 seconds** to declare a failure

The heartbeat thread snapshots the node list under a shared lock, releases the lock, performs TCP ping to each node (100ms timeout per node), then re-acquires the lock only to write back results. This prevents blocking LOOKUP requests during heartbeat I/O.

### Primary Election (on failure)

When a primary is declared dead:
1. Coordinator scans all secondaries for that tablet
2. Picks the secondary with the **highest LSN** (most up-to-date)
3. Sends `BECOME_PRIMARY` command to that node
4. Sends `ADD_REPLICA` to inform the new primary of remaining secondaries

**Why highest LSN?** It minimizes data loss — the node that received the most writes is most likely to have everything the dead primary wrote.

### Recovery (node comes back)

1. Coordinator detects the node is alive again (ping succeeds)
2. Identifies which tablets the node participates in
3. Calls `demote_and_sync_node`: sends a snapshot + delta from the current primary
4. Once synced, sends `ADD_REPLICA` so the current primary starts forwarding writes to it again

### LOOKUP Protocol

The frontend's KV client sends:
```
LOOKUP <row_key>\r\n
```
Coordinator responds:
```
PRIMARY <host> <port>\r\n
```

The frontend caches this primary address per row prefix. On connection failure it re-LOOKUPs.

---

## 6. Frontend HTTP Servers

### Design

Three identical stateless HTTP server instances (fe1/fe2/fe3) run on ports 8090/8091/8092. Any of them can serve any request. They share no state — all state is in the KV layer.

**Why three frontends?** Single point of failure elimination. If one frontend crashes, the other two continue serving. The browser is redirected to a live frontend automatically.

### HTTP Server Internals

- **Thread pool**: 32 threads (configurable via `--threads`)
- **Protocol**: HTTP/1.0 — each connection is closed after one request (no keep-alive). This is simple but means each browser resource fetch requires a new TCP connection.
- **Request parsing**: Custom `http_reader.h` — no external library. Supports Content-Length and chunked transfer encoding.
- **Body size limit**: **64 MB hard limit** on any single request body

### Dispatch Flow

```
accept() → thread pool → handle_connection()
  → handle_one_request() → parse HTTP → dispatch()
    → match path/method
    → auth check (get_user via session cookie)
    → feature handler
    → serialize HttpResponse → write to socket
```

### Supported Routes

| Path | Method | Handler |
|---|---|---|
| `/api/login` | POST | Login |
| `/api/signup` | POST | Register |
| `/api/logout` | POST | Logout |
| `/api/session` | GET | Session check |
| `/api/change-password` | POST | Change password |
| `/api/inbox` | GET | Email inbox/sent/trash |
| `/api/send` | POST | Send email |
| `/api/delete-email` | POST | Move to trash |
| `/api/restore-email` | POST | Restore from trash |
| `/api/contacts` | GET | List contacts |
| `/api/contacts/add` | POST | Add contact |
| `/api/contacts/delete` | POST | Delete contact |
| `/api/mail/upload-attachment` | POST | Upload attachment |
| `/api/mail/download-attachment` | GET | Download attachment |
| `/api/drive/list` | GET | List folder contents |
| `/api/drive/upload` | POST | Upload file |
| `/api/drive/download/:uid` | GET | Download file |
| `/api/drive/rename` | POST | Rename file/folder |
| `/api/drive/move` | POST | Move file/folder |
| `/api/drive/mkdir` | POST | Create folder |
| `/api/drive/delete` | POST | Delete file/folder |
| `/api/quota` | GET/POST | Quota status/update |
| `/api/chat/rooms` | GET | Chat room list |
| `/api/chat/messages` | GET | Room messages |
| `/api/chat/send` | POST | Send to room |
| `/api/chat/dms` | GET | DM peer list |
| `/api/chat/dm-messages` | GET | DM messages |
| `/api/chat/dm-send` | POST | Send DM |
| `/events` | GET | SSE inbox push |
| `/api/admin/status` | GET | Cluster status |
| `/api/admin/control` | POST | Kill/restart nodes |
| `/admin` | GET | Admin dashboard UI |

---

## 7. KV Client

### Connection Pooling

Each frontend instance maintains one `KVClient` per target node. Each client holds a `ConnectionPool` with **2 persistent TCP connections** to the KV server. Connections are borrowed for the duration of a request and returned.

**Why 2 connections?** Reduces TCP handshake overhead. More connections would help under high concurrency but increase resource usage.

### Retry Logic

On connection failure or protocol error:
- Up to 4 attempts with backoffs of 0ms, 75ms, 150ms, 250ms
- On failure, a new TCP connection is established
- In coordinator mode, a fresh LOOKUP is performed to discover the current primary (in case of failover during the request)

### Socket Timeout

Default 350ms per KV operation. This bounds the worst-case latency of any single KV call. Large file uploads use a longer timeout proportional to the file size.

### Global Mutex — Key Bottleneck

There is one `std::mutex` per `KVClient` instance. Every KV operation acquires this mutex for the full duration of the TCP round-trip. This serializes all KV operations from a single frontend instance, regardless of how many threads are handling requests concurrently.

**Why?** The connection pool is shared state. Protecting it with a mutex is the simplest correct implementation. The practical impact is bounded because KV operations are fast (sub-millisecond on localhost) and the mutex is released immediately after each operation.

---

## 8. Session Management

### Session Token

On login, a 128-bit random session ID is generated from `/dev/urandom` and encoded as a 32-character lowercase hex string. This is the `sid` cookie.

**Why 128 bits?** Collision probability is negligible (2^{-128}) and brute-force guessing is computationally infeasible.

### KV Schema

```
session:<sid>  →  user   : "alice"
                  expires: "1746000000"   (Unix timestamp)
```

The session row stores both the username and expiry timestamp. On every request, the frontend:
1. Reads `session:<sid>` from KV
2. Checks the `expires` column against current time
3. If valid, returns the username; if expired, destroys the session

### TTL

Sessions expire after **24 hours** (`SESSION_TTL_SECONDS = 86400`).

**Why KV-backed sessions?** Any frontend instance can validate any session without inter-process communication, since all frontends share the same KV store. This is essential for stateless frontend HA.

### Cookie

The `sid` cookie is set with `HttpOnly` and a matching `Max-Age`. It is not marked `Secure` (no HTTPS), which is acceptable for a localhost demo environment.

---

## 9. Email (SMTP)

### Architecture

Two separate email paths:

**Inbound** (external → NCloud inbox):
- External SMTP server connects to NCloud SMTP server on port 2525
- SMTP server parses the message, extracts recipient username
- Validates recipient exists in KV (`pwd` column)
- Writes email body and metadata to KV
- Updates inbox index

**Outbound** (NCloud → external):
- Frontend calls `smtp_send_external()` via the SMTP client library
- Supports two modes: **relay** (authenticated SMTP via Gmail/etc. using libcurl) or **direct** (MX lookup + direct SMTP delivery)
- Mode configured via environment variables (`SMTP_MODE`, `SMTP_RELAY_HOST`, etc.)

### KV Email Schema

All email data for a user lives in a single KV row `<user>:mail`:

```
<user>:mail  →  folder:inbox  : "uid3,uid2,uid1"   (comma-separated, newest first)
                folder:sent   : "uid4,uid5"
                folder:trash  : "uid6"
                msg:<uid>     : {"uid":"...","from":"...","to":"...","subject":"...","time":"..."}
                body:<uid>    : "email body text"
                attach:<uid>  : "attachuid1,attachuid2"
```

**Why one row per user?** Simplifies lookups — all of a user's email data is co-located. The folder index (comma-separated UIDs) allows listing without scanning all columns.

**Tradeoff**: All email operations for a user contend on the same KV row. High-volume users create hot spots. For a demo system this is acceptable.

### Inbox Indexing (Concurrency-Safe)

Adding an email to the inbox uses a compare-and-swap retry loop:
1. Read current `folder:inbox` value (comma-separated UIDs)
2. Prepend new UID
3. CPUT: if value unchanged, commit; else retry

Up to 5 retries with 5ms sleep between. This handles concurrent email deliveries without locks.

### Folders

Three folders: **inbox**, **sent**, **trash**. Delete moves from inbox/sent to trash. Restore moves from trash back to inbox. Permanent delete removes `msg:uid`, `body:uid`, and all attachment entries from KV.

### Attachments

Stored as separate KV entries in the mail row:
```
attach-data:<attach_uid>  →  binary bytes (base64 encoded)
attach-meta:<attach_uid>  →  {"name":"file.pdf","size":12345,"owner":"alice","recipient":"bob"}
```

---

## 10. Drive (File Storage)

### Design Philosophy

The drive is a virtual filesystem layered on top of the flat KV store. There are no actual directories — directory structure is simulated using KV entries that store parent-child relationships.

### Object Model

Every file and folder has a randomly generated **UID** (unique identifier). Three KV row namespaces per object:

| KV Row | Purpose |
|---|---|
| `drive:obj:<uid>` | Object metadata (type, name, parent, size, timestamps) |
| `drive:dir:<uid>` | Directory children list (only for folders) |
| `drive:file:<uid>` | File content chunks (only for files) |

Plus a per-user root pointer:
```
<user>:drive  →  root_uid  : "<uid of user's root folder>"
                quota_bytes: "52428800"   (50 MB default)
```

### Object Metadata Schema

```
drive:obj:<uid>  →  type   : "file" | "dir"
                    name   : "report.pdf"
                    parent : "<parent_dir_uid>"
                    size   : "1048576"    (bytes, files only)
                    ctime  : "1746000000" (creation Unix timestamp)
                    mtime  : "1746001234" (modification Unix timestamp)
```

### Directory Children

```
drive:dir:<uid>  →  children : "uid1,uid2,uid3"  (comma-separated child UIDs)
```

Adding a child uses CPUT retry loop (same pattern as email indexing) for concurrency safety.

### File Chunking

Files are stored in 1 MB chunks:

```
drive:file:<uid>  →  chunks   : "5"             (number of chunks)
                     chunk:0  : <1MB of bytes>
                     chunk:1  : <1MB of bytes>
                     chunk:2  : <1MB of bytes>
                     chunk:3  : <1MB of bytes>
                     chunk:4  : <remaining bytes>
```

**Why 1 MB chunks?** KV values have no enforced size limit at the storage layer, but large values slow down WAL writes and checkpoint serialization. 1 MB is a practical balance between chunk overhead and per-chunk KV cost.

**Why chunk at all?** The KV protocol reads and writes entire values atomically. A 64 MB file as a single value would require 64 MB to be held in memory simultaneously on the KV server during every read and write. Chunking keeps individual KV operations small.

### Upload Flow

```
1. Browser sends multipart/form-data POST to /api/drive/upload
2. Frontend parses multipart body, extracts file bytes and target folder UID
3. Compute quota check: user_drive_used_bytes() + file_size ≤ quota_limit
4. If quota OK:
   a. Generate new file UID
   b. Write object metadata (drive:obj:<uid>)
   c. Write chunks (drive:file:<uid> chunk:0, chunk:1, ...)
   d. Append UID to parent directory's children list (CPUT retry)
5. Return {ok: true, uid: "..."}
```

### Download Flow

```
1. GET /api/drive/download/<uid>
2. Read drive:obj:<uid> to get type and name
3. Read drive:file:<uid> chunks count
4. Concatenate all chunks in order
5. Send as HTTP response with Content-Disposition: attachment; filename="..."
```

### Move/Rename Operations

**Rename**: Only updates `name` column in `drive:obj:<uid>`. No data movement.

**Move**: 
1. Remove UID from old parent's `children` (CPUT retry)
2. Update `parent` in `drive:obj:<uid>` to new parent UID
3. Append UID to new parent's `children` (CPUT retry)
4. Verify destination is not a descendant of source (cycle prevention)

**Delete**: Recursively deletes all children before deleting the object itself. Each deletion removes `drive:obj:<uid>`, `drive:dir:<uid>`, and all `drive:file:<uid>` chunk entries.

---

## 11. Storage Quota

### How It Works

**Default quota**: 50 MB per user (`kDefaultDriveQuotaBytes = 50 * 1024 * 1024`)

**Admin-adjustable**: Via admin panel, 1–1024 MB range. Stored in KV at `<user>:drive → quota_bytes`.

**Enforcement on upload**:
1. `user_drive_used_bytes()` is called — traverses entire drive tree summing file sizes
2. `used_bytes + new_file_size > quota_limit` → reject with error message showing overage
3. Client-side pre-check in JavaScript provides immediate UI feedback (but server-side is authoritative)

**Usage calculation**:
```
subtree_file_bytes_inner(uid, seen, depth):
  if type == "file": return stored size
  if type == "dir": sum subtree_file_bytes of all children
```
Cycles are prevented by a visited-set (`seen`), depth is capped at 4096.

**Why calculate dynamically instead of tracking a running total?** Simpler to implement correctly — no need to update a counter on every delete, move, or rename. Tradeoff: expensive for large drive trees (one KV round-trip per file).

### Maximum File Size

The HTTP layer enforces a **64 MB hard limit** on any single request body (`http_reader.h:175`):
```cpp
if (content_length > 64 * 1024 * 1024) {
    // Reject — return HTTP 413
}
```

Chunked transfer encoding is also capped at **64 MB** (`kMaxChunkedBodyBytes`).

This means the maximum uploadable file is **64 MB**, regardless of user quota.

### What Happens When You Exceed the Limit

**Scenario 1: File exceeds 64 MB (HTTP limit)**
- The HTTP parser rejects the body before it reaches the drive handler
- The TCP connection is closed immediately
- The browser sees a network error (no clean HTTP error response is sent)
- **Nothing is written to KV** — no partial state

**Scenario 2: File is within 64 MB but exceeds user quota**
- The HTTP body is fully received and parsed (file is in memory)
- `user_drive_used_bytes()` is called
- The check fails → HTTP 200 with `{"ok": false, "error": "quota exceeded by X bytes"}`
- **Nothing is written to KV** — no partial state

**Scenario 3: Quota limit is 50 MB but file is 40 MB and user already has 15 MB used**
- `15 + 40 = 55 > 50` → rejected with "quota exceeded by 5242880 bytes"

**Scenario 4: Upload succeeds but KV write fails mid-way (chunk write error)**
- Already-written chunks are rolled back (deleted)
- The object metadata entry is deleted
- Returns `{"ok": false, "error": "..."}` to client
- **Clean rollback, no orphaned data**

---

## 12. Chat

### Group Rooms

Static global chat rooms — no per-user creation. Messages stored in KV:

```
chat:room:<room_id>  →  msgs   : "uid1,uid2,uid3"   (comma-separated, oldest first)
                        msg:<uid> : {"user":"alice","text":"hello","time":"..."}
```

### Direct Messages

DM threads are keyed by a canonical pair of usernames (alphabetically sorted):

```
chat:dm:<userA>:<userB>  →  msgs    : "uid1,uid2"
                             msg:<uid>: {"from":"alice","text":"hi","time":"..."}
```

Each user's DM peer list:
```
<user>:dms  →  peers : "bob,carol"
```

### SSE (Server-Sent Events) for Real-Time Inbox

When a user's browser opens the mail inbox, it establishes an SSE connection to `/events`. The frontend holds a thread-pool thread open for each SSE connection and polls the KV store every 500ms for a notification key:

```
notify:<user>  →  latest : "uid:<latest_uid>"
```

When the SMTP server delivers a new email, it writes to this key. The SSE handler detects the change and pushes an event to the browser, which then refreshes the inbox. This is a polling-based push simulation — not true push.

**SSE concurrency limit**: To prevent SSE connections from starving the thread pool, at most `threads/4` (default: 8) simultaneous SSE connections are allowed. Beyond this limit, the server returns a 503 and the browser falls back to a 3-second auto-refresh.

---

## 13. Admin Panel & HA

### Admin Dashboard

Available at `http://127.0.0.1:8090/admin`. Shows:
- All KV nodes: alive/dead, LSN, role (primary/secondary)
- All frontend instances: alive/dead
- All tablets: which nodes are replicas, row ranges
- Kill and Restart buttons for each node and frontend

### Frontend High Availability

When a frontend is killed via the admin panel:

1. **Self-kill path** (killing the frontend you're currently talking to):
   - The server finds a real peer frontend (HTTP probe to `/api/admin/status` — stubs return 503, real servers return 200)
   - Launches a shell subshell: `sleep 0.15; kill <self>; sleep 0.05; launch redirect stub`
   - Returns 302 redirect to the live peer's `/admin` page
   - Browser follows redirect; 150ms later, the old server dies and a redirect stub starts

2. **Non-self kill** (killing a different frontend from your current one):
   - Directly kills the target process
   - After 50ms, starts a redirect stub on the freed port

**Redirect stub**: A feserver process started with `--redirect-to http://127.0.0.1:<peer_port>`. It returns 302 for all browser requests and 503 for `/api/*` (so health probes can distinguish it from a real server). Users visiting the dead port are transparently bounced to a live frontend.

### Backend Node Kill/Restart

- **Kill**: Sends SIGKILL to the KV server process. The coordinator detects the failure within ~1.5 seconds and promotes a secondary.
- **Restart**: Kills the process, waits for the port to be free, relaunches with the same arguments. The new process recovers from its checkpoint + WAL, then receives a sync from the current primary via the coordinator.

### Multi-Fault Scenario

The system can handle losing up to N-1 replicas per tablet (where N=3, so 2 simultaneous failures per tablet = **all writes blocked** since no alive replica remains). Practical demo sequence:

```
Kill node1 → coordinator promotes node2 as primary (1.5s)
Do writes → replicated to node3 only
Kill node2 → coordinator promotes node3 as primary (1.5s)
Do writes → node3 only (no replication, single node)
Restart node1 → coordinator syncs node1 from node3, adds as secondary
Kill node3 → coordinator promotes node1 as primary
System still serving with node1
```

---

## 14. Bottlenecks & Tradeoffs

### Bottleneck 1: Global KV Client Mutex (Most Significant)

**What**: One mutex per `KVClient` serializes all KV operations from a single frontend, regardless of thread count.

**Impact**: Under high concurrent load, frontend threads queue behind this mutex. Measured worst case: if each KV round-trip takes 2ms, a 32-thread frontend can only execute ~500 KV ops/second total, not 32 × 500.

**Tradeoff accepted**: Simplicity over throughput. For a demo system with <10 concurrent users, unnoticeable.

**Fix if needed**: Use per-row connection pools or a connection-per-thread model.

### Bottleneck 2: Quota Calculation on Every Upload

**What**: `user_drive_used_bytes()` traverses the entire drive tree on every upload — one KV round-trip per file/folder.

**Impact**: A user with 100 files incurs 100+ sequential KV reads before each upload. Adds ~200ms per upload on localhost.

**Tradeoff accepted**: Simplicity — no counter to maintain. Cache invalidation on delete/move is avoided.

**Fix if needed**: Maintain a running `used_bytes` counter in KV, updated atomically with CPUT on every upload/delete.

### Bottleneck 3: SSE Thread Consumption

**What**: Each SSE connection (inbox notification stream) holds one thread-pool thread indefinitely, polling KV every 500ms.

**Impact**: With 32 threads and 8 SSE slots allowed, 8 open browser tabs can consume 25% of thread capacity for background polling.

**Tradeoff accepted**: True async I/O (epoll/kqueue) would require significantly more complexity.

### Bottleneck 4: Sequential File Chunking

**What**: A 64 MB upload is stored as 64 sequential 1 MB KV PUT operations (each waiting for the previous to complete).

**Impact**: ~64 × 2ms = ~128ms of KV round-trips per large upload, all on the global mutex.

**Tradeoff accepted**: Parallel chunk writes would require lock-free connection pooling.

### Bottleneck 5: Coordinator Connection-Per-Lookup

**What**: The KV client opens a new TCP connection to the coordinator on every `exec_row()` call when the cached primary is unavailable.

**Impact**: First request after a failover incurs a full TCP handshake + LOOKUP before the actual KV call.

**Tradeoff accepted**: The common case (cached primary is alive) does not hit the coordinator at all. The coordinator is only consulted on miss/failure.

### Availability vs. Durability Tradeoff

**Decision**: Writes succeed with only 1 alive replica. No quorum requirement.

**Consequence**: If a write is not replicated before the primary crashes, that write is lost.

**Why chosen**: For a cloud storage demo, being able to continue uploading files even when 2 of 3 nodes are dead is more valuable than strict durability. Strict quorum writes would fail when even one secondary is unavailable.

### HTTP/1.0 (No Keep-Alive)

**Decision**: Each HTTP connection is closed after one request.

**Consequence**: Every browser resource fetch (JS, CSS, API calls) pays a full TCP handshake. A page load may involve 10+ handshakes.

**Impact**: Adds ~1ms per request on localhost (negligible). On a real network with >10ms RTT, this would be significant.

**Why chosen**: Eliminates the complexity of connection state management. Keep-alive requires tracking per-connection state and handling pipeline ordering.

---

## 15. Configuration Reference

### start_multi_tablet_cluster.sh Defaults

| Variable | Default | Description |
|---|---|---|
| `COORD_PORT` | 7010 | Coordinator TCP port |
| `DATA_ROOT` | `/tmp/pc_multi_tablet_demo` | KV data directory |
| `SMTP_PORT` | 2525 | Inbound SMTP listen port |

### Coordinator Defaults

| Parameter | Default | Description |
|---|---|---|
| `hb_interval_ms` | 500 | Heartbeat interval |
| `hb_miss_thresh` | 3 | Missed pings before declaring dead |
| Failure detection time | ~1.5s | `interval × threshold` |

### Frontend Defaults

| Parameter | Default | Description |
|---|---|---|
| `--port` | 8080 | Listen port |
| `--threads` | 32 | Thread pool size |
| `--kv-host` | 127.0.0.1 | KV server host |
| `--kv-port` | 5000 | KV server port |
| `--coord-port` | 0 (disabled) | Coordinator port |
| Max HTTP body | 64 MB | Hard limit in http_reader.h |
| SSE slots | `threads/4` = 8 | Max simultaneous SSE connections |
| Session TTL | 24 hours | `SESSION_TTL_SECONDS` |

### Drive Defaults

| Parameter | Value | Description |
|---|---|---|
| Chunk size | 1 MB | `kDriveChunkSize` |
| Default quota | 50 MB | `kDefaultDriveQuotaBytes` |
| Max quota (UI) | 1024 MB | Admin panel slider max |
| Max file size | 64 MB | HTTP body limit |
| Max tree depth | 4096 | `subtree_file_bytes_inner` limit |

### KV Client Defaults

| Parameter | Value | Description |
|---|---|---|
| Connection pool size | 2 | Persistent connections per node |
| Socket timeout | 350ms | `kDefaultSocketTimeoutMs` |
| Retry attempts | 4 | `kAttempts` in `exec_row` |
| Retry backoffs | 0/75/150/250ms | `kSleepMs` array |

---

*Document generated from source: `frontend/src/fe_server.cc`, `frontend/src/handlers_mail.cc`, `frontend/src/kv_client.h`, `frontend/src/session.h`, `frontend/src/http_reader.h`, `kvstore/src/tablet.cc`, `kvstore/src/replication.h`, `coordinator/src/coordinator.cc`, `smtp_server/src/smtp_server.cc`*
