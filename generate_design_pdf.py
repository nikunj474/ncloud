#!/usr/bin/env python3
"""NCloud System Design PDF Generator"""

from fpdf import FPDF
from fpdf.enums import XPos, YPos
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "NCloud_System_Design.pdf")

class PDF(FPDF):
    def __init__(self):
        super().__init__('P', 'mm', 'A4')
        self.set_auto_page_break(auto=True, margin=18)
        self.set_margins(18, 18, 18)
        self._toc = []  # (title, page)

    # -- helpers --------------------------------------------------------------
    def h1(self, text):
        self.set_font('Helvetica', 'B', 18)
        self.set_text_color(20, 60, 130)
        self.ln(4)
        self.multi_cell(0, 9, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def h2(self, text):
        self.set_font('Helvetica', 'B', 13)
        self.set_text_color(30, 80, 160)
        self.ln(3)
        self.multi_cell(0, 7, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def h3(self, text):
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(50, 50, 50)
        self.ln(2)
        self.multi_cell(0, 6, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(0, 0, 0)

    def body(self, text):
        self.set_font('Helvetica', '', 10)
        self.multi_cell(0, 5.5, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def bullet(self, text, indent=6):
        self.set_font('Helvetica', '', 10)
        x = self.get_x()
        self.set_x(self.l_margin + indent)
        self.multi_cell(0, 5.5, f"-  {text}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def kv(self, key, value):
        self.set_font('Helvetica', 'B', 10)
        self.set_x(self.l_margin + 6)
        self.cell(52, 5.5, key + ":", new_x=XPos.RIGHT)
        self.set_font('Helvetica', '', 10)
        self.multi_cell(0, 5.5, value, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def code(self, text):
        self.set_font('Courier', '', 8.5)
        self.set_fill_color(240, 240, 245)
        self.set_draw_color(200, 200, 210)
        lines = text.split('\n')
        self.ln(1)
        for line in lines:
            self.set_x(self.l_margin + 4)
            self.multi_cell(0, 4.8, line if line else ' ',
                            fill=True, border=0,
                            new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_font('Helvetica', '', 10)
        self.set_fill_color(255, 255, 255)
        self.ln(1)

    def divider(self):
        self.set_draw_color(180, 180, 200)
        self.ln(2)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(3)

    def header(self):
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 8, 'NCloud Distributed System - System Design Document', align='C')
        self.ln(0)
        self.set_text_color(0, 0, 0)

    def footer(self):
        self.set_y(-13)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(130, 130, 130)
        self.cell(0, 8, f'Page {self.page_no()}', align='C')
        self.set_text_color(0, 0, 0)

    def section(self, number, title):
        self.add_page()
        self._toc.append((f"{number}. {title}", self.page_no()))
        self.h1(f"{number}.  {title}")
        self.divider()


def build(pdf: PDF):

    # -- Cover page ------------------------------------------------------------
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 28)
    pdf.set_text_color(20, 60, 130)
    pdf.ln(30)
    pdf.multi_cell(0, 14, 'NCloud', align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', 'B', 18)
    pdf.set_text_color(50, 50, 50)
    pdf.multi_cell(0, 10, 'Distributed Cloud Platform', align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 13)
    pdf.set_text_color(80, 80, 80)
    pdf.ln(6)
    pdf.multi_cell(0, 7, 'Complete System Design Document', align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)
    pdf.multi_cell(0, 7, 'Architecture  -  Replication  -  Fault Tolerance  -  Storage  -  Tradeoffs', align='C',
                   new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(20)
    pdf.set_draw_color(20, 60, 130)
    pdf.set_line_width(0.5)
    pdf.line(40, pdf.get_y(), pdf.w - 40, pdf.get_y())
    pdf.ln(10)
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(60, 60, 60)
    pdf.multi_cell(0, 7, 'CIS 5550 - Penn Engineering  -  Team 05', align='C',
                   new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_text_color(0, 0, 0)

    # -- TOC placeholder -------------------------------------------------------
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(20, 60, 130)
    pdf.ln(6)
    pdf.cell(0, 10, 'Table of Contents', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_text_color(0, 0, 0)
    pdf.divider()
    toc_page = pdf.page_no()

    # ══════════════════════════════════════════════════════════════════════════
    # 1. SYSTEM OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(1, "System Overview")
    pdf.body(
        "NCloud is a distributed cloud platform built entirely in C++17 with no external "
        "frameworks. It delivers four user-facing services - Email, File Drive, Live Chat, and an "
        "Admin Console - on top of a custom replicated key-value store. Every component was written "
        "from scratch: HTTP server, SMTP server, KV tablet storage, primary-backup replication, "
        "coordinator-driven failure detection, and a session management layer."
    )
    pdf.h2("1.1  Service Catalogue")
    for svc, desc in [
        ("Email", "Send/receive MIME email. Supports attachments, folder management (inbox / sent / trash), and real-time SSE push for new-mail notification."),
        ("File Drive", "Hierarchical virtual filesystem stored in KV. Upload files up to 64 MB, create folders, move/copy/delete, and track per-user storage quota."),
        ("Live Chat", "Real-time group chat channels backed by KV rows; SSE streaming for instant message delivery."),
        ("Admin Console", "Kill / restart / redirect any of the three frontend nodes from the browser. Monitor node liveness. Coordinator-driven failover visible in real time."),
    ]:
        pdf.kv(svc, desc)

    pdf.h2("1.2  Technology Choices")
    pdf.body(
        "The entire system is written in C++17 with POSIX sockets (no Boost.Asio, no libuv, no "
        "Nginx, no external HTTP libraries). This was a deliberate constraint from the course "
        "specification and forced careful hand-crafted protocol design. The result is a system with "
        "very low external dependencies - the only third-party code is fpdf2 (this document "
        "generator) and the standard library."
    )
    for why, reason in [
        ("C++17", "Course requirement; gives direct control over threading, memory, and socket I/O with no GC pauses."),
        ("POSIX TCP sockets", "Maximum portability; works inside Docker containers that lack lsof/fuser/ss."),
        ("std::shared_mutex (rows)", "Allow many concurrent reads on hot rows while exclusive writes are rare."),
        ("std::mutex (WAL)", "WAL writes must be strictly serialised to maintain LSN monotonicity."),
        ("Bloom filter per tablet", "O(1) probabilistic guard to skip disk I/O for non-existent rows."),
        ("SSE over WebSocket", "Server-Sent Events require only standard HTTP; no upgrade negotiation, easier to implement from scratch."),
    ]:
        pdf.kv(why, reason)

    # ══════════════════════════════════════════════════════════════════════════
    # 2. ARCHITECTURE
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(2, "Three-Tier Architecture")
    pdf.body(
        "NCloud uses a strict three-tier architecture. No tier bypasses any other tier - "
        "browsers talk only to frontends, frontends talk only to KV nodes (via coordinator for "
        "routing), and KV nodes replicate among themselves."
    )
    pdf.h2("2.1  Tier Map")
    pdf.code(
        "  Browser (HTTP/1.1)\n"
        "       |\n"
        "  +-----------+    +-----------+    +-----------+\n"
        "  |  fe1:8080  |   |  fe2:8090  |   |  fe3:8095  |   <- Frontend tier\n"
        "  +-----------+    +-----------+    +-----------+\n"
        "       |                |                |\n"
        "       +--------+-------+--------+-------+\n"
        "                | KV client (TCP)\n"
        "  +-----------+    +-----------+    +-----------+\n"
        "  | node1:5000 |   | node2:5001 |   | node3:5002 |   <- KV / Tablet tier\n"
        "  +-----------+    +-----------+    +-----------+\n"
        "       |                |                |\n"
        "       +--------+-------+--------+-------+\n"
        "                | Heartbeat / Election\n"
        "           +------------+\n"
        "           | Coordinator|  :6000             <- Coordinator\n"
        "           +------------+\n"
        "                |\n"
        "        +---------------+\n"
        "        | smtp_server   |  :2525              <- SMTP tier\n"
        "        +---------------+"
    )

    pdf.h2("2.2  Frontend Tier")
    pdf.body(
        "Three frontend processes (fe1, fe2, fe3) run on ports 8080, 8090, and 8095. Each "
        "maintains a pool of up to 32 worker threads handling HTTP/1.1 connections. Frontends "
        "are fully stateless with respect to user data - all state lives in the KV store. Session "
        "cookies map to KV-backed session objects, so any frontend can serve any session."
    )
    pdf.bullet("fe1: primary frontend, handles the bulk of traffic under normal operation.")
    pdf.bullet("fe2 / fe3: standby frontends; promoted by admin action or failover.")
    pdf.bullet("Redirect stubs: a killed frontend is replaced by a lightweight feserver process that "
               "returns HTTP 302 to a live peer for all page requests and HTTP 503 for all /api/* "
               "requests (so the health probe can distinguish stubs from live servers).")

    pdf.h2("2.3  KV / Tablet Tier")
    pdf.body(
        "Three KV node processes (node1, node2, node3) host three tablets. Each tablet covers a "
        "letter-range of the KV key space and is replicated across all three nodes."
    )
    pdf.code(
        "  tabletA  ->  rows whose first character is in [a-g]\n"
        "  tabletB  ->  rows whose first character is in [h-p]\n"
        "  tabletC  ->  rows whose first character is in [q-z, 0-9, special]"
    )
    pdf.body(
        "Each tablet has one primary and up to two backups at any point in time. The primary "
        "handles all writes; backups receive replicated log entries. Reads can be served from "
        "any replica (the frontend always routes to the coordinator-designated primary for writes "
        "and tries the primary first for reads)."
    )

    pdf.h2("2.4  Coordinator")
    pdf.body(
        "A single coordinator process runs on port 6000. It does not store user data. Its only "
        "job is to track which KV nodes are alive and which replica is primary for each tablet. "
        "It drives heartbeats and primary election."
    )

    pdf.h2("2.5  SMTP Server")
    pdf.body(
        "A dedicated SMTP server listens on port 2525 (mapped to 25 externally). It accepts "
        "inbound mail for @ncloud.com and @ncloud domains, validates the recipient exists "
        "in the KV store, and writes the message directly into the KV mailbox. Outbound mail "
        "for external domains is forwarded via MX lookup or a configured relay."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 3. KEY-VALUE STORE
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(3, "Key-Value Store Design")
    pdf.body(
        "The KV store is a custom bigtable-inspired store: rows are string keys, each row "
        "contains a map of column-name -> value (both arbitrary byte strings). The API has "
        "four operations: GET, PUT, CPUT (compare-and-put / atomic swap), and DELETE."
    )

    pdf.h2("3.1  Data Model")
    pdf.code(
        "  GET    row col            -> value  (or NOT_FOUND)\n"
        "  PUT    row col value      -> OK\n"
        "  CPUT   row col old new    -> OK | FAIL (old value mismatch)\n"
        "  DELETE row col            -> OK"
    )
    pdf.body(
        "CPUT is the key primitive enabling safe concurrent updates. The frontend uses it "
        "extensively for directory entry appending, mail index updates, and session management - "
        "anywhere two concurrent writers could corrupt a shared value."
    )

    pdf.h2("3.2  In-Memory Layout")
    pdf.body(
        "Each tablet keeps all data in an in-memory hash map:"
    )
    pdf.code(
        "  std::unordered_map<string, unique_ptr<RowData>>  rows_\n"
        "  struct RowData {\n"
        "      std::shared_mutex mu;\n"
        "      std::unordered_map<string, string> cols;\n"
        "  };"
    )
    pdf.body(
        "Two-level locking: rows_ is protected by a shared_mutex allowing concurrent readers. "
        "Each RowData has its own shared_mutex for column-level concurrency. This means two "
        "threads writing to different rows never block each other."
    )
    pdf.h3("Why unordered_map?")
    pdf.body(
        "O(1) average-case lookup. The KV store is a pure lookup table - there is no range-scan "
        "API - so hash-map wins over B-tree here. The bloom filter provides a fast probabilistic "
        "pre-check before any map lookup."
    )

    pdf.h2("3.3  Bloom Filter")
    pdf.body(
        "Each tablet maintains a per-tablet Bloom filter seeded with all known row keys. Before "
        "any GET, CPUT, or DELETE touches rows_, the bloom filter is checked. If the filter says "
        "'definitely not present', the operation returns immediately without taking any lock. "
        "False positives are harmless (they fall through to the real check); false negatives "
        "cannot happen - the filter is add-only."
    )
    pdf.bullet("Benefit: eliminates lock contention for non-existent row lookups (e.g., checking "
               "quota on a new user).")
    pdf.bullet("Trade-off: small memory overhead per tablet (~8 KB for a 64-bit Bloom filter with "
               "a 1% false-positive rate at 10k rows).")

    pdf.h2("3.4  Write-Ahead Log (WAL)")
    pdf.body(
        "Every PUT and DELETE is written to the WAL before touching in-memory rows. This ensures "
        "crash recovery: on restart, the tablet replays the WAL to restore any operations that "
        "happened after the last checkpoint."
    )
    pdf.h3("WAL Binary Format (per entry)")
    pdf.code(
        "  [4 bytes]  magic = 0xDEADBEEF  (little-endian)\n"
        "  [1 byte ]  op type: 0x01 = PUT, 0x02 = DELETE\n"
        "  [4 bytes]  row length\n"
        "  [4 bytes]  column length\n"
        "  [4 bytes]  value length  (0 for DELETE)\n"
        "  [N bytes]  row key\n"
        "  [M bytes]  column key\n"
        "  [V bytes]  value  (absent for DELETE)"
    )
    pdf.body(
        "The magic header lets the replay loop detect truncated or corrupted entries and stop "
        "safely rather than applying garbage data."
    )
    pdf.h3("WAL Locking")
    pdf.body(
        "All WAL writes are serialised by a dedicated wal_mu_ mutex (std::mutex). This is separate "
        "from the row-level shared_mutex so WAL I/O does not block concurrent readers. The WAL "
        "mutex also serialises LSN assignment - the LSN is incremented atomically inside the "
        "WAL lock, guaranteeing that LSN order matches WAL entry order."
    )

    pdf.h2("3.5  Checkpointing")
    pdf.body(
        "Periodically (triggered by the coordinator or on snapshot-sync), the tablet writes a "
        "checkpoint: a binary snapshot of the entire in-memory state, then truncates the WAL. "
        "This bounds recovery time to only replaying the WAL entries since the last checkpoint."
    )
    pdf.h3("Checkpoint Binary Format")
    pdf.code(
        "  [4 bytes]  CKPT_MAGIC\n"
        "  [8 bytes]  checkpoint_version  (monotone counter)\n"
        "  [8 bytes]  LSN at checkpoint time\n"
        "  [4 bytes]  number of rows\n"
        "  for each row:\n"
        "      [4 bytes]  row key length\n"
        "      [N bytes]  row key\n"
        "      [4 bytes]  number of columns\n"
        "      for each column:\n"
        "          [4 bytes]  col key length\n"
        "          [M bytes]  col key\n"
        "          [4 bytes]  value length\n"
        "          [V bytes]  value\n"
        "  [bloom filter serialization]"
    )
    pdf.body(
        "The checkpoint is written atomically: first to a .tmp file, then renamed over the live "
        ".ckpt file (rename is atomic on Linux/macOS). This prevents a partial checkpoint from "
        "corrupting recovery."
    )
    pdf.h3("Why checkpoint_version?")
    pdf.body(
        "When a stale replica reconnects, the primary sends either a WAL delta (if the replica's "
        "checkpoint version matches) or a full snapshot (if not). The checkpoint_version allows "
        "the primary to detect whether the replica's WAL base is compatible, avoiding replaying "
        "deltas that no longer correspond to the WAL."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 4. REPLICATION
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(4, "Replication & Fault Tolerance")

    pdf.h2("4.1  Primary-Backup Model")
    pdf.body(
        "NCloud uses synchronous primary-backup replication (also called primary-secondary "
        "or primary-replica). There is exactly one primary per tablet at any time. All writes go "
        "to the primary; backups apply writes in the same order via forwarded WAL entries."
    )
    pdf.bullet("Writes: primary writes WAL + memory, then forwards to all configured replicas.")
    pdf.bullet("Reads: always from the primary (no stale reads). Frontends discover the primary "
               "via the coordinator.")
    pdf.bullet("Quorum: a write succeeds after being acknowledged by a strict majority "
               "(floor(N/2)+1 nodes). With N=3, quorum=2 (the primary + 1 replica).")

    pdf.h2("4.2  LSN (Log Sequence Number)")
    pdf.body(
        "Every write is assigned a monotonically increasing uint64_t LSN. The primary assigns the "
        "LSN inside the WAL lock, ensuring strict ordering. Each replica tracks the highest LSN "
        "it has applied. The coordinator uses LSN information to pick the best-LSN replica as the "
        "new primary during election."
    )
    pdf.code(
        "  // Inside wal_mu_ lock:\n"
        "  uint64_t new_lsn = lsn_.fetch_add(1) + 1;\n"
        "  // new_lsn is unique and strictly greater than all prior LSNs"
    )
    pdf.body(
        "LSNs are embedded in every REPLICATE message. The backup checks that the incoming LSN "
        "is exactly current+1 - any gap triggers a resync request."
    )

    pdf.h2("4.3  Replication Protocol")
    pdf.h3("Normal path (write)")
    pdf.code(
        "  Frontend  ->  Primary (TCP):  PUT row col value\n"
        "  Primary:\n"
        "    1. acquire primary_write_mu_\n"
        "    2. write WAL entry (assigns LSN N)\n"
        "    3. update in-memory rows_\n"
        "    4. forward \"REPLICATE tabletX N PUT ...\"\n"
        "       to all configured replicas\n"
        "    5. wait for quorum ACKs  (with 500ms timeout)\n"
        "    6. release primary_write_mu_\n"
        "    7. reply +OK LSN=N to frontend\n"
        "  Each backup:\n"
        "    1. verify expected_lsn == local_lsn + 1\n"
        "    2. write WAL entry\n"
        "    3. update in-memory rows_\n"
        "    4. reply +OK LSN=N"
    )
    pdf.h3("Gap detection and resync")
    pdf.body(
        "If a backup receives LSN N but its local LSN is < N-1, it replies with an error "
        "and the primary marks that replica as needing resync. On the next coordinator heartbeat, "
        "the replica is told to sync from the primary."
    )
    pdf.h3("Sync protocol (DELTA vs SNAPSHOT)")
    pdf.body(
        "When a replica reconnects or falls behind, it sends: SYNC_FROM tabletX <ckpt_ver> <lsn>"
    )
    pdf.bullet("If ckpt_ver matches and lsn is within the current WAL range: primary sends a WAL "
               "delta (only the missing entries). The replica applies the delta in-order.")
    pdf.bullet("Otherwise: primary sends a full snapshot blob (entire in-memory state + LSN + "
               "ckpt_ver). The replica loads the snapshot and checkpoints it to disk.")
    pdf.code(
        "  SYNC_FROM tabletA 3 1042\\r\\n\n"
        "  <- +DELTA 4096\\r\\n<4096 bytes of WAL entries>\n"
        "     or\n"
        "  <- +SNAPSHOT 102400\\r\\n<102400 bytes of full state blob>"
    )

    pdf.h2("4.4  Fault Tolerance Scenarios")
    pdf.h3("Scenario 1: One replica crashes")
    pdf.body(
        "System continues with quorum of 2/3. Writes still succeed. The dead replica is detected "
        "by the coordinator within ~1.5 seconds (3 missed heartbeats at 500ms intervals). When it "
        "restarts, it sends SYNC_FROM and the primary sends a delta or snapshot to bring it up to date."
    )
    pdf.h3("Scenario 2: Primary crashes")
    pdf.body(
        "Coordinator detects the failed primary after ~1.5 seconds. It selects the replica with "
        "the highest LSN as the new primary and broadcasts the new primary assignment to all nodes "
        "and frontends. The new primary begins accepting writes immediately. If the previously "
        "dead primary restarts, it is demoted to secondary and syncs from the new primary."
    )
    pdf.h3("Scenario 3: Two replicas crash (minority left)")
    pdf.body(
        "Writes fail quorum check (only 1/3 ack). The system enters a degraded mode where the "
        "primary logs locally but cannot achieve quorum. It logs a warning and returns success "
        "to the frontend in degraded mode (prioritising availability over strict consistency). "
        "This is a deliberate trade-off for a cloud demo system - in production, writes would "
        "be rejected."
    )
    pdf.h3("Scenario 4: Frontend crash")
    pdf.body(
        "Frontend state is stateless - no user data is lost. The admin console detects the "
        "crashed frontend (via the HTTP /api/admin/status probe) and can launch a redirect stub "
        "or restart the server. User sessions persisted in KV are still valid on any other frontend."
    )
    pdf.h3("Scenario 5: Coordinator crash")
    pdf.body(
        "The KV nodes continue serving reads and writes using the last known primary assignment. "
        "No new primary elections can happen until the coordinator restarts. This is acceptable "
        "because elections only happen on node failure, which is rare."
    )

    pdf.h2("4.5  Primary Election Algorithm")
    pdf.body(
        "When the coordinator detects a failed primary (3 consecutive missed heartbeats), it "
        "runs the following election:"
    )
    pdf.bullet("Collect LSNs from all alive replicas for the failed tablet.")
    pdf.bullet("Pick the replica with the highest LSN (most up-to-date).")
    pdf.bullet("Send PROMOTE message to the winning replica.")
    pdf.bullet("Send ADD_REPLICA messages to all other alive replicas pointing at the new primary.")
    pdf.bullet("Broadcast the new topology to all frontends.")
    pdf.body(
        "Why highest LSN? Because it minimises data loss. The highest-LSN replica has applied "
        "the most writes, so promoting it means the fewest writes need to be re-applied or lost."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 5. COORDINATOR
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(5, "Coordinator Design")
    pdf.body(
        "The coordinator is the central brain for cluster topology. It does not store user data "
        "and is not in the critical path of reads or writes - frontends cache the primary address "
        "and contact KV nodes directly."
    )

    pdf.h2("5.1  Heartbeat Loop")
    pdf.body(
        "The coordinator runs a background heartbeat loop every 500ms. It pings each registered "
        "KV node with a PING command over TCP and expects a PONG response within the timeout. "
        "If a node misses 3 consecutive heartbeats (~1.5 seconds), it is declared dead."
    )
    pdf.code(
        "  Coordinator  ->  KV Node:  PING\\r\\n\n"
        "  KV Node  ->  Coordinator:  +PONG\\r\\n"
    )
    pdf.h3("Lock design in heartbeat loop")
    pdf.body(
        "The heartbeat loop snapshots the node list under a shared (read) lock, releases the lock, "
        "performs all TCP pings outside the lock (avoiding holding a lock during network I/O), "
        "then re-acquires an exclusive (write) lock only to write back the results. This prevents "
        "the coordinator from becoming a bottleneck when many nodes are registered."
    )

    pdf.h2("5.2  Node Registration")
    pdf.body(
        "KV nodes register themselves with the coordinator at startup by sending a REGISTER "
        "message containing their node ID, host, port, and tablet name. The coordinator records "
        "this and begins including the node in heartbeat cycles."
    )
    pdf.code(
        "  KV Node  ->  Coordinator:  REGISTER node1 127.0.0.1 5000 tabletA\\r\\n\n"
        "  Coordinator  ->  KV Node:   +OK\\r\\n"
    )

    pdf.h2("5.3  Frontend Topology Updates")
    pdf.body(
        "When a primary changes (election or manual promotion), the coordinator broadcasts a "
        "topology update to all connected frontends. Frontends update their KV client's primary "
        "routing table. Between broadcasts, frontends use their cached routing information."
    )

    pdf.h2("5.4  Line Length")
    pdf.body(
        "The coordinator uses a custom line reader with a maximum buffer of 8192 bytes. This is "
        "necessary because SYNC_FROM messages can carry large metadata. The 8192 limit prevents "
        "unbounded memory growth from malformed clients."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 6. FRONTEND SERVER
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(6, "Frontend Server Design")

    pdf.h2("6.1  HTTP Server")
    pdf.body(
        "Each frontend runs a custom HTTP/1.1 server on a dedicated port. It accepts connections "
        "in a loop and dispatches each connection to a worker thread from a fixed-size thread pool "
        "(default 32 threads). The HTTP parser handles: method, path, query string, headers, and "
        "body. It supports chunked transfer encoding for large file uploads."
    )
    pdf.bullet("Thread pool size: configurable via --threads flag (default 32).")
    pdf.bullet("HTTP methods: GET, POST, PUT, DELETE.")
    pdf.bullet("Content-Length body reads: supports up to 64 MB per request.")
    pdf.bullet("Static file serving from configurable --static directory.")

    pdf.h2("6.2  Route Dispatch")
    pdf.body(
        "Routes are matched by a simple prefix/pattern matcher. Parameterised routes (:uid, :folder) "
        "extract values into a params map. Route handlers are registered as function pointers or "
        "lambdas. The dispatch table is built at startup; no dynamic registration occurs after "
        "the server starts."
    )

    pdf.h2("6.3  KV Client")
    pdf.body(
        "Each frontend process maintains a KVClient for each KV node. The client holds a "
        "ConnectionPool with 2 persistent TCP connections per node (reused across requests). "
        "Socket timeout is 350ms; the client retries up to 4 times with exponential backoff "
        "(0/75/150/250ms) before failing."
    )
    pdf.h3("Why 2 connections per node?")
    pdf.body(
        "One connection is rarely sufficient because the HTTP thread pool has 32 threads, all "
        "potentially making KV calls simultaneously. Two persistent connections reduce connection "
        "setup overhead and allow some parallelism without the complexity of a full connection pool. "
        "The trade-off: a global mutex per client serialises access to the connection pool, making "
        "it a bottleneck under high concurrency (see Section 10)."
    )
    pdf.h3("Retry backoff")
    pdf.code(
        "  attempt 0:  no wait  (immediate retry)\n"
        "  attempt 1:  75ms wait\n"
        "  attempt 2:  150ms wait\n"
        "  attempt 3:  250ms wait\n"
        "  -> give up, return error to HTTP handler"
    )

    pdf.h2("6.4  Session Management")
    pdf.body(
        "Sessions are backed by the KV store under the key session:<SID>. The session ID is a "
        "128-bit random value from /dev/urandom, hex-encoded to 32 characters. Sessions expire "
        "after 24 hours (stored as a Unix timestamp in the session value). On every authenticated "
        "request, the frontend reads the session from KV and validates the expiry."
    )
    pdf.bullet("SID is sent as an HTTP cookie (Set-Cookie: sid=<SID>; HttpOnly; Path=/).")
    pdf.bullet("Cookie is HttpOnly to prevent JavaScript access (XSS mitigation).")
    pdf.bullet("Session destroy is a KV DELETE of the session row.")
    pdf.bullet("Because sessions are in KV, any frontend can serve any session - no sticky sessions needed.")

    pdf.h2("6.5  High Availability (Admin Kill/Restart/Redirect)")
    pdf.body(
        "The admin console can kill, restart, or redirect-stub any frontend node. When a "
        "frontend is killed, it launches a replacement redirect stub before dying."
    )
    pdf.h3("Self-kill sequence")
    pdf.body(
        "The challenge: a process cannot kill itself and then launch a replacement - the replacement "
        "launch would die with the process. Solution: a shell background subshell is launched "
        "before the process kills itself. The subshell is a grandchild of the server process and "
        "survives SIGKILL."
    )
    pdf.code(
        "  // Server code (runs in HTTP handler, then returns response):\n"
        "  std::string cmd = \"(\"\n"
        "      \"sleep 0.15; \"  // let HTTP response escape\n"
        "      \"ps -axo pid=,command= | awk '/--port 8080/{print $1}' | xargs kill -KILL; \"\n"
        "      \"sleep 0.05; \"\n"
        "      \"nohup ./feserver --port 8080 --redirect-to http://127.0.0.1:8090 &\"\n"
        "  \") >/dev/null 2>&1 &\";\n"
        "  ::system(cmd.c_str());"
    )
    pdf.h3("Redirect stub")
    pdf.body(
        "The stub is a feserver process launched with --redirect-to http://peer:port. It returns "
        "HTTP 302 for all page requests and HTTP 503 for all /api/* requests. The health probe "
        "uses HTTP 200 vs 503 to distinguish a live server from a stub."
    )

    pdf.h2("6.6  Server-Sent Events (SSE)")
    pdf.body(
        "SSE is used for real-time inbox notifications and chat. An SSE connection is a long-lived "
        "HTTP response with Content-Type: text/event-stream. The server polls KV every 500ms "
        "and pushes a data: event when new messages arrive."
    )
    pdf.bullet("SSE slots are limited to threads/4 (default 8) to prevent SSE connections from "
               "consuming all thread pool slots and starving normal requests.")
    pdf.bullet("Slot acquisition uses a CAS loop on an atomic<int> counter.")
    pdf.bullet("Connection drop is detected by write() failure to the client socket.")

    # ══════════════════════════════════════════════════════════════════════════
    # 7. DRIVE (VIRTUAL FILESYSTEM)
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(7, "Drive - Virtual Filesystem")

    pdf.h2("7.1  KV Schema")
    pdf.body(
        "The virtual filesystem is entirely encoded in KV rows. There is no separate filesystem "
        "layer - everything is key-value lookups."
    )
    pdf.code(
        "  Row key                 Column           Value\n"
        "  ---------------------   --------------   -----------------------------\n"
        "  drive:obj:<uid>         type             'file' or 'dir'\n"
        "  drive:obj:<uid>         name             'filename.txt'\n"
        "  drive:obj:<uid>         parent           '<parent_uid>'\n"
        "  drive:obj:<uid>         owner            '<username>'\n"
        "  drive:obj:<uid>         size             '1048576'  (bytes)\n"
        "  drive:obj:<uid>         created          '1714000000'  (Unix epoch)\n"
        "  drive:obj:<uid>         modified         '1714000100'\n"
        "  drive:obj:<uid>         chunks           '3'  (number of chunks)\n"
        "  drive:obj:<uid>         mime             'application/pdf'\n"
        "  drive:dir:<uid>         children         'uid1,uid2,uid3,...'\n"
        "  drive:file:<uid>:0      data             <up to 1MB binary blob>\n"
        "  drive:file:<uid>:1      data             <up to 1MB binary blob>\n"
        "  drive:file:<uid>:N      data             <up to 1MB binary blob>\n"
        "  drive:root:<username>   root             '<root_dir_uid>'"
    )

    pdf.h2("7.2  File Chunking")
    pdf.body(
        "Files are split into 1 MB (1,048,576 byte) chunks. Each chunk is stored as a separate "
        "KV column value. This is necessary because KV values are held in memory - a single "
        "64 MB value would be wasteful. With 1 MB chunks, a 64 MB file uses 64 KV entries."
    )
    pdf.h3("Why 1 MB chunks?")
    pdf.bullet("KV values are byte strings in memory - no paging or streaming within a value.")
    pdf.bullet("1 MB fits comfortably in an HTTP response without chunked encoding complexity.")
    pdf.bullet("Aligns well with common filesystem block sizes and memory page allocators.")
    pdf.bullet("Minimises WAL entry sizes for large files (each chunk WAL entry is ~1 MB).")
    pdf.h3("Chunk assembly on download")
    pdf.body(
        "On file download, the frontend reads the 'chunks' metadata column, then reads each "
        "drive:file:<uid>:N chunk in sequence and streams them to the browser. The browser "
        "assembles the original file from the response body."
    )

    pdf.h2("7.3  Directory Structure")
    pdf.body(
        "Directories are KV rows with a 'children' column containing a comma-separated list "
        "of child UIDs. Adding a child updates this list using a CPUT retry loop (read current "
        "value, append new UID, CPUT old -> new). This is the standard optimistic concurrency "
        "pattern for shared append-only lists."
    )
    pdf.code(
        "  // append_child (simplified):\n"
        "  while (true) {\n"
        "      string old_val = kv.get(dir_row, 'children');\n"
        "      string new_val = old_val.empty() ? uid : old_val + ',' + uid;\n"
        "      if (kv.cput(dir_row, 'children', old_val, new_val)) break;\n"
        "      // retry on concurrent modification\n"
        "  }"
    )

    pdf.h2("7.4  UID Generation")
    pdf.body(
        "Each file and directory gets a UUID-like UID generated from /dev/urandom (128 bits, "
        "hex-encoded). UIDs are globally unique across users and directories, preventing "
        "collisions even under concurrent uploads."
    )

    pdf.h2("7.5  Move and Copy")
    pdf.body(
        "Move: updates the 'parent' column of the object and atomically removes/adds to the "
        "parent directories' children lists. Copy: creates a new UID, copies all metadata and "
        "chunk data to new KV rows, and inserts into the destination directory."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 8. STORAGE QUOTA
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(8, "Storage Quota")

    pdf.h2("8.1  Quota Design")
    pdf.body(
        "Each user has a storage quota enforced by the frontend. The default quota is 50 MB "
        "(52,428,800 bytes). The quota is checked before every upload."
    )
    pdf.code(
        "  static constexpr size_t DEFAULT_QUOTA_BYTES = 50ULL * 1024 * 1024;  // 50 MB"
    )

    pdf.h2("8.2  Usage Calculation")
    pdf.body(
        "Usage is calculated by traversing the entire virtual filesystem tree of the user, "
        "summing the 'size' column of every file object. The traversal is recursive (BFS/DFS), "
        "starting from the user's root directory UID."
    )
    pdf.h3("subtree_file_bytes algorithm")
    pdf.code(
        "  function subtree_file_bytes(uid, visited, depth):\n"
        "      if depth > 4096: return 0   // cycle guard\n"
        "      if uid in visited: return 0  // deduplication\n"
        "      visited.add(uid)\n"
        "      obj = kv.get_row('drive:obj:' + uid)\n"
        "      if obj.type == 'file': return obj.size\n"
        "      if obj.type == 'dir':\n"
        "          total = 0\n"
        "          for child_uid in obj.children.split(','):\n"
        "              total += subtree_file_bytes(child_uid, visited, depth+1)\n"
        "          return total"
    )
    pdf.bullet("visited set: prevents double-counting if the same UID appears in multiple "
               "directory listings (should not happen in a correct tree, but guards against bugs).")
    pdf.bullet("depth cap at 4096: prevents infinite recursion if a directory cycle is ever "
               "introduced by a bug.")

    pdf.h2("8.3  Quota Enforcement")
    pdf.body(
        "Before an upload, the frontend calculates current usage + new file size. If the sum "
        "exceeds DEFAULT_QUOTA_BYTES, the upload is rejected with HTTP 413 (Payload Too Large) "
        "and an error message to the user."
    )
    pdf.code(
        "  if (current_usage + file_size > DEFAULT_QUOTA_BYTES) {\n"
        "      return HttpResponse::error(413, \"Storage quota exceeded (50 MB)\");\n"
        "  }"
    )

    pdf.h2("8.4  Maximum File Upload Size")
    pdf.body(
        "The maximum single file upload is bounded by the HTTP server's body limit:"
    )
    pdf.code(
        "  static constexpr size_t MAX_BODY_BYTES = 64ULL * 1024 * 1024;  // 64 MB"
    )
    pdf.body(
        "This is a hard limit enforced by the HTTP parser - if the request body exceeds 64 MB, "
        "the server returns HTTP 413 immediately without reading the rest of the body."
    )
    pdf.h3("Summary of size limits")
    for limit, value, reason in [
        ("Per-file upload limit", "64 MB", "HTTP body parser hard limit (MAX_BODY_BYTES)"),
        ("Per-user quota", "50 MB", "Total storage across all files (DEFAULT_QUOTA_BYTES)"),
        ("KV value chunk size", "1 MB", "Each chunk stored as one KV column value"),
        ("Max chunks per file", "64", "64 MB / 1 MB = 64 chunks"),
        ("Tablet WAL entry", "~1 MB+header", "One WAL entry per chunk write"),
    ]:
        pdf.kv(limit, f"{value} - {reason}")

    pdf.h2("8.5  What Happens When Quota Is Exceeded?")
    pdf.body(
        "When a user attempts to upload a file that would push them over the 50 MB quota:"
    )
    pdf.bullet("The frontend calculates subtree_file_bytes for the user's root directory.")
    pdf.bullet("If current_usage + new_file_size > 50 MB: HTTP 413 is returned, no data is written.")
    pdf.bullet("If the file is exactly at the boundary (e.g., user has 49.9 MB and uploads 0.2 MB): "
               "the upload is allowed (total = 50.1 MB, which exceeds quota). This is a known "
               "edge case - the quota check uses < not <= because the quota is a soft cap.")
    pdf.bullet("After the upload is rejected, the user sees an error in the browser and can delete "
               "files to free space.")
    pdf.h3("What happens if 64 MB HTTP limit is exceeded?")
    pdf.body(
        "If a client sends a Content-Length header > 64 MB, the HTTP parser rejects the request "
        "with 413 before reading any body bytes. If the Content-Length is missing (streaming), "
        "the parser reads up to 64 MB and then returns 413. In both cases, no partial data is "
        "written to the KV store."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 9. EMAIL / SMTP
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(9, "Email System")

    pdf.h2("9.1  Mail KV Schema")
    pdf.code(
        "  Row key              Column           Value\n"
        "  ------------------   --------------   ----------------------------------\n"
        "  <user>:mail          folder:inbox     'uid1 uid2 uid3 ...'  (space-sep)\n"
        "  <user>:mail          folder:sent      'uid1 uid2 ...'\n"
        "  <user>:mail          folder:trash     'uid1 uid2 ...'\n"
        "  <user>:mail          msg:<uid>        '<from>|<to>|<subject>|<date>|<size>'\n"
        "  <user>:mail          body:<uid>       '<full MIME body>'\n"
        "  <user>:mail          attach:<uid>:<n> '<base64 attachment data>'"
    )

    pdf.h2("9.2  Inbound Mail Flow")
    pdf.body(
        "Inbound mail is handled by the SMTP server (port 2525). The flow:"
    )
    pdf.code(
        "  External MTA  ->  SMTP Server (2525)\n"
        "    EHLO, MAIL FROM:, RCPT TO:, DATA, .\n"
        "  SMTP Server:\n"
        "    1. Validate recipient domain (@ncloud.com or @ncloud)\n"
        "    2. Look up recipient in KV (check password row exists)\n"
        "    3. Write message body to KV: PUT <user>:mail body:<uid> <body>\n"
        "    4. Write message meta to KV: PUT <user>:mail msg:<uid> <meta>\n"
        "    5. CPUT retry loop to append uid to folder:inbox list\n"
        "    6. Reply 250 OK to sender"
    )

    pdf.h2("9.3  Outbound Mail Flow")
    pdf.body(
        "Outbound mail from a NCloud user:"
    )
    pdf.code(
        "  Browser  ->  Frontend (POST /api/mail/send)\n"
        "  Frontend:\n"
        "    1. Parse To:, Subject:, Body from HTTP form\n"
        "    2. If recipient is @ncloud.com: call deliver_local() directly\n"
        "    3. If external: open TCP to relay (port 25) or direct MX lookup\n"
        "       EHLO, MAIL FROM:, RCPT TO:, DATA, body, .\n"
        "    4. Write to sender's folder:sent index"
    )

    pdf.h2("9.4  Real-Time Inbox (SSE)")
    pdf.body(
        "When a user opens their inbox, the frontend establishes an SSE connection. A server "
        "thread polls the user's folder:inbox KV column every 500ms. When the UID list changes "
        "(new mail arrived), it pushes a 'data: new_mail' SSE event. The browser JavaScript "
        "handles the event and refreshes the inbox view."
    )
    pdf.bullet("SSE polling interval: 500ms (configurable).")
    pdf.bullet("SSE slot limit: threads/4 (default 8 concurrent SSE connections).")
    pdf.bullet("Disconnect detection: write() failure to the SSE socket closes the connection "
               "and releases the slot.")

    pdf.h2("9.5  Folder Management")
    pdf.body(
        "Users can move messages between inbox, sent, and trash. Move operations use a CPUT "
        "retry loop: remove UID from source folder list, add to destination folder list. "
        "Permanent delete removes the UID from the folder list and deletes the body and meta "
        "columns from the KV row."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 10. BOTTLENECKS
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(10, "Bottlenecks & Performance")

    pdf.h2("10.1  KV Client Global Mutex")
    pdf.body(
        "The KVClient uses a single global std::mutex per client instance to protect access to "
        "the connection pool. With 32 frontend threads all making KV calls, this mutex is a "
        "significant bottleneck. Every KV call blocks all other threads on the same frontend."
    )
    pdf.h3("Impact")
    pdf.body("At 32 threads, throughput is bounded by 1/mutex_hold_time KV ops/sec per frontend "
             "instance. Under high load, threads queue behind the mutex.")
    pdf.h3("Why not fixed?")
    pdf.body("Proper per-connection locking would require a connection pool of N connections "
             "with a semaphore. That adds significant complexity. For a course demo system, "
             "a global mutex is the acceptable trade-off.")

    pdf.h2("10.2  SSE Polling (500ms KV Reads)")
    pdf.body(
        "Each SSE connection performs a KV GET every 500ms. With 8 SSE slots, that is 16 KV "
        "reads/second just for SSE (2 per connection per second). These reads compete with "
        "user-initiated reads and writes in the connection pool."
    )
    pdf.h3("Mitigation")
    pdf.body("SSE slot cap (threads/4) limits the total SSE load. A proper system would use "
             "KV-level push notifications (pub/sub) instead of polling.")

    pdf.h2("10.3  Storage Quota Tree Walk")
    pdf.body(
        "Calculating storage usage requires traversing the entire virtual filesystem tree. "
        "For a user with thousands of files in deeply nested directories, this is O(N) KV "
        "reads where N is the number of objects. Each KV read has a round-trip latency."
    )
    pdf.h3("Mitigation")
    pdf.body("A production system would maintain a running usage counter updated atomically "
             "on every upload/delete. NCloud recalculates on every upload, which is "
             "acceptable for small datasets (< 100 files) but would be slow at scale.")

    pdf.h2("10.4  WAL Sequential Write")
    pdf.body(
        "The WAL mutex serialises all writes to a single WAL file per tablet. Two concurrent "
        "writes to the same tablet cannot interleave - they queue behind wal_mu_. This limits "
        "write throughput to one write at a time per tablet."
    )
    pdf.h3("Why acceptable?")
    pdf.body("The WAL write itself is fast (a few KB flush). The primary bottleneck is network "
             "round-trip to replicas (500ms timeout), not WAL write latency. For a course demo "
             "with a few concurrent users, WAL serialisation is never the bottleneck.")

    pdf.h2("10.5  Coordinator Single Point")
    pdf.body(
        "There is only one coordinator. If it crashes, no new primary elections can happen, "
        "and frontends cannot get topology updates. However, cached topology means existing "
        "operations continue to work until a node failure requires an election."
    )

    pdf.h2("10.6  Large File Upload Timeout")
    pdf.body(
        "The KV client socket timeout is 350ms. A 64 MB file upload requires 64 sequential "
        "1 MB KV PUT operations, each with replication forwarding. If any single PUT takes "
        "> 350ms (e.g., due to replication latency), the KV client times out and retries. "
        "This can cause false failures during large uploads under load."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 11. TRADEOFFS
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(11, "Design Tradeoffs")

    tradeoffs = [
        ("Primary-backup vs. Paxos/Raft",
         "Primary-backup is simpler to implement and debug. Paxos/Raft would give stronger "
         "consistency guarantees (no lost writes during leadership transitions) but requires "
         "significantly more code. For a course project, primary-backup is the right call."),
        ("Synchronous vs. Async replication",
         "Synchronous replication (wait for quorum ACK before returning) gives read-your-writes "
         "consistency. Async replication would be faster but a frontend reading from a different "
         "replica immediately after a write might see stale data. Synchronous wins for correctness."),
        ("In-memory store vs. disk-backed",
         "All data is in memory (WAL and checkpoints are durability layers, not primary storage). "
         "This gives O(1) lookup speed but limits dataset size to available RAM. A disk-backed "
         "store (like LevelDB) would support larger datasets at the cost of complexity."),
        ("Tablet-based sharding vs. consistent hashing",
         "Fixed letter-range sharding is simple and deterministic. Consistent hashing would "
         "allow dynamic resharding as nodes are added, but is overkill for a fixed 3-node cluster."),
        ("SSE vs. WebSocket for push",
         "SSE requires only standard HTTP - no upgrade handshake, no framing protocol, no "
         "library. WebSocket gives bidirectional communication (not needed here) at the cost "
         "of a more complex protocol. SSE is the right fit for server-to-client notifications."),
        ("Global KV client mutex vs. per-connection pool",
         "A global mutex is simple but serialises all KV calls on a frontend. A full "
         "connection pool (N connections, semaphore) would allow parallel KV calls at the cost "
         "of more complex lifecycle management (connection death, pool drain on shutdown)."),
        ("50 MB quota vs. per-user configurable",
         "Fixed quota simplifies the admin model. Configurable per-user quota would require "
         "an admin API and a quota row in KV per user. For a demo, fixed is fine."),
        ("ps+awk kill vs. lsof/fuser/ss",
         "lsof/fuser/ss are not available in Docker containers. ps+awk matching on the "
         "--port argument is universally available and achieves the same result."),
        ("1 MB chunk size vs. variable",
         "Fixed chunk size simplifies chunked retrieval (chunk index = byte_offset / CHUNK_SIZE). "
         "Variable chunk size would allow better space efficiency for small files but adds "
         "complexity in the metadata schema."),
        ("24-hour session TTL vs. sliding window",
         "A fixed 24-hour TTL is simple to implement (one timestamp comparison). A sliding "
         "window (TTL resets on every request) would require a KV write on every authenticated "
         "request, adding latency. Fixed TTL is the right trade-off for a course demo."),
    ]
    for title, explanation in tradeoffs:
        pdf.h2(title)
        pdf.body(explanation)

    # ══════════════════════════════════════════════════════════════════════════
    # 12. KV WIRE PROTOCOL
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(12, "KV Wire Protocol")
    pdf.body(
        "The frontend communicates with KV nodes over a custom binary-framed text protocol "
        "over TCP. Commands are newline-terminated text; binary payloads (values) are "
        "length-prefixed and follow the command line."
    )

    pdf.h2("12.1  Request Format")
    pdf.code(
        "  GET <row_len> <col_len>\\r\\n<row><col>\n"
        "  PUT <row_len> <col_len> <val_len>\\r\\n<row><col><val>\n"
        "  CPUT <row_len> <col_len> <old_len> <new_len>\\r\\n<row><col><old><new>\n"
        "  DELETE <row_len> <col_len>\\r\\n<row><col>"
    )

    pdf.h2("12.2  Response Format")
    pdf.code(
        "  +OK\\r\\n                          (success, no value)\n"
        "  +OK <val_len>\\r\\n<val>           (success with value)\n"
        "  -ERR <message>\\r\\n              (error)\n"
        "  -NOT_FOUND\\r\\n                  (GET on non-existent key)\n"
        "  -FAIL\\r\\n                       (CPUT old value mismatch)"
    )

    pdf.h2("12.3  Replication Protocol")
    pdf.code(
        "  Primary  ->  Replica:\n"
        "  REPLICATE <tablet> <lsn> PUT <row_len> <col_len> <val_len>\\r\\n<row><col><val>\n"
        "  REPLICATE <tablet> <lsn> DELETE <row_len> <col_len>\\r\\n<row><col>\n"
        "\n"
        "  Replica  ->  Primary:\n"
        "  +OK LSN=<lsn>\\r\\n"
    )

    pdf.h2("12.4  Coordinator Protocol")
    pdf.code(
        "  KV Node  ->  Coordinator:  REGISTER <id> <host> <port> <tablet>\\r\\n\n"
        "  Coordinator  ->  KV Node:  +OK\\r\\n\n"
        "\n"
        "  Coordinator  ->  KV Node:  PING\\r\\n\n"
        "  KV Node  ->  Coordinator:  +PONG\\r\\n\n"
        "\n"
        "  Coordinator  ->  KV Node:  PROMOTE\\r\\n       (become primary)\n"
        "  Coordinator  ->  KV Node:  DEMOTE\\r\\n        (become secondary)\n"
        "  Coordinator  ->  KV Node:  ADD_REPLICA <id> <host> <port>\\r\\n\n"
        "\n"
        "  KV Node  ->  KV Node:     SYNC_FROM <tablet> <ckpt_ver> <lsn>\\r\\n\n"
        "  Primary  ->  Replica:     +SNAPSHOT <len>\\r\\n<blob>\n"
        "                         or +DELTA <len>\\r\\n<blob>"
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 13. STARTUP & CONFIGURATION
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(13, "Startup & Configuration")

    pdf.h2("13.1  Process Inventory")
    processes = [
        ("node1", "KV store node 1, tablets A/B/C primary", "5000 (KV), 5100 (repl)"),
        ("node2", "KV store node 2, tablets A/B/C backup", "5001 (KV), 5101 (repl)"),
        ("node3", "KV store node 3, tablets A/B/C backup", "5002 (KV), 5102 (repl)"),
        ("coordinator", "Heartbeat, election, topology", "6000"),
        ("fe1", "Frontend HTTP server 1", "8080"),
        ("fe2", "Frontend HTTP server 2", "8090"),
        ("fe3", "Frontend HTTP server 3", "8095"),
        ("smtp_server", "SMTP inbound/outbound", "2525"),
    ]
    for name, role, ports in processes:
        pdf.kv(name, f"Port(s): {ports} - {role}")

    pdf.h2("13.2  Frontend CLI Flags")
    pdf.code(
        "  --port        <N>       HTTP listen port (default 8080)\n"
        "  --kv-host     <host>    KV node host (default 127.0.0.1)\n"
        "  --kv-port     <N>       KV node port (default 5000)\n"
        "  --threads     <N>       thread pool size (default 32)\n"
        "  --id          <str>     server identity label (default fe1)\n"
        "  --static      <dir>     static files directory (default ./static)\n"
        "  --coord-host  <host>    coordinator host (default 127.0.0.1)\n"
        "  --coord-port  <N>       coordinator port (default 6000)\n"
        "  --redirect-to <url>     run as redirect stub to this URL\n"
        "  --page-not-found-stub   run as 404 stub (all requests -> 404)"
    )

    pdf.h2("13.3  Startup Script")
    pdf.body(
        "start_multi_tablet_cluster.sh launches all 8 processes in order: "
        "KV nodes first (with data directories), then coordinator, then frontends, then SMTP. "
        "Each process is backgrounded with & and the PID is recorded for shutdown."
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 14. SECURITY
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(14, "Security Considerations")

    pdf.h2("14.1  Implemented Mitigations")
    for item, desc in [
        ("HttpOnly session cookie", "Session ID not accessible to JavaScript - prevents XSS session theft."),
        ("128-bit random SID", "Brute-force resistant session ID (2^128 entropy from /dev/urandom)."),
        ("CPUT for atomic updates", "Prevents lost-update race conditions on shared KV values."),
        ("JSON control-char escaping", "JSON strings sanitise 0x00-0x1F to prevent JSON injection."),
        ("Admin loopback check", "Admin control endpoints reject requests not originating from 127.0.0.1."),
        ("SMTP domain validation", "SMTP server only accepts mail for @ncloud.com / @ncloud."),
    ]:
        pdf.kv(item, desc)

    pdf.h2("14.2  Known Weaknesses (Out of Scope for Course)")
    for item, desc in [
        ("No TLS", "All HTTP and KV traffic is plaintext TCP. Credentials are sent unencrypted."),
        ("No CSRF protection", "State-changing API endpoints lack CSRF tokens."),
        ("No rate limiting", "Login endpoint has no brute-force protection."),
        ("Content-Disposition injection", "Filenames in download headers are not fully sanitised."),
        ("Open redirect", "Admin redirect URL comes from request Host header (not validated)."),
        ("SMTP dot-stuffing", "Only first leading dot removed; RFC 5321 violation for multi-dot lines."),
    ]:
        pdf.kv(item, desc)

    # ══════════════════════════════════════════════════════════════════════════
    # 15. SUMMARY
    # ══════════════════════════════════════════════════════════════════════════
    pdf.section(15, "Summary")
    pdf.body(
        "NCloud is a functionally complete distributed cloud platform built entirely in "
        "C++17 from scratch. It demonstrates the full stack of distributed systems concepts:"
    )
    pdf.bullet("Replicated storage with WAL, checkpointing, and delta/snapshot resync.")
    pdf.bullet("Primary election driven by highest-LSN selection after ~1.5s failure detection.")
    pdf.bullet("Stateless frontend tier with KV-backed sessions and SSE push notifications.")
    pdf.bullet("Virtual filesystem with 1 MB chunked storage, CPUT-based directory updates, "
               "and per-user 50 MB quota enforcement.")
    pdf.bullet("SMTP inbound/outbound email with MIME attachment support.")
    pdf.bullet("Admin console with kill/restart/redirect-stub for live frontend HA demonstration.")

    pdf.ln(6)
    pdf.h2("Key Numbers")
    for k, v in [
        ("Max file upload", "64 MB (HTTP body limit)"),
        ("Per-user quota", "50 MB (soft, checked pre-upload)"),
        ("File chunk size", "1 MB per KV value"),
        ("Session TTL", "24 hours"),
        ("Session ID entropy", "128 bits (/dev/urandom)"),
        ("Failure detection", "~1.5 seconds (3 × 500ms heartbeats)"),
        ("Replication quorum", "2 of 3 nodes (majority)"),
        ("SSE poll interval", "500ms"),
        ("KV socket timeout", "350ms"),
        ("KV retry backoff", "0 / 75 / 150 / 250ms"),
        ("Frontend threads", "32 per server"),
        ("SSE slot limit", "8 (threads/4)"),
    ]:
        pdf.kv(k, v)

    # -- Write TOC -------------------------------------------------------------
    pdf.page = toc_page
    pdf.set_y(38)
    pdf.set_font('Helvetica', '', 10)
    for title, page in pdf._toc:
        dots = '.' * max(4, 80 - len(title))
        pdf.cell(0, 7, f"  {title}  {dots}  {page}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)


def main():
    pdf = PDF()
    build(pdf)
    pdf.output(OUTPUT)
    print(f"PDF written to: {OUTPUT}")


if __name__ == "__main__":
    main()
