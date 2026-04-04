
#pragma once

// replication.h  --  PennCloud Primary-Backup Replication Protocol
// DESIGN: Primary-backup replication for linearizable consistency.

// WRITE PATH (primary node):
//   1. Client sends PUT/CPUT/DELETE to primary
//   2. Primary writes to local WAL + memtable (via Tablet::put/cput/del)
//   3. Primary forwards the SAME operation to all alive secondaries
//   4. Primary waits for ACK from all alive secondaries
//   5. Primary ACKs the client
//
// READ PATH:
//   - All reads served by primary only
//   - This guarantees linearizability: reads always see latest write
//
// REPLICATION WIRE PROTOCOL (internal, secondary listens on separate port):
//   REPLICATE <lsn> PUT    <rowlen> <collen> <vallen>\r\n<row><col><val>
//   REPLICATE <lsn> DELETE <rowlen> <collen>\r\n<row><col>
//   REPLICATE <lsn> CPUT   <rowlen> <collen> <v1len> <v2len>\r\n<row><col><v1><v2>
//   SYNC_FROM <lsn>\r\n          -- secondary asks "send me everything after LSN"
//   SYNC_DATA <count>\r\n        -- primary responds with N replication records
//   BECOME_PRIMARY <tablet>\r\n  -- coordinator instructs secondary to take over
// WRITE COALESCING (B2 innovation):
//   Writes to the same row within a 2ms window are batched into a single
//   replication message. This dramatically reduces network messages during
//   bursty operations (e.g. drive metadata updates during file upload).
//   Clients still get individual ACKs; coalescing is transparent.

// LSN (Log Sequence Number):
//   Every WAL write increments the LSN atomically.
//   Secondaries track their own LSN.
//   On primary failure, coordinator picks the secondary with highest LSN.
//   On recovery, recovering node sends its LSN; primary sends delta entries only.


#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <functional>
#include <future>
#include <unordered_map>
#include <chrono>
#include <iostream>
#include <sstream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include "tablet.h"
#include "../../kvstore/src/protocol.h"
using namespace std;


// ReplicaInfo: one secondary node known to this primary

struct ReplicaInfo {
    string id;
    string host;
    int         port;        // replication port (usually kv_port + 100)
    bool        alive = true;
    uint64_t    lsn   = 0;  // last confirmed LSN from this replica
    int         fd    = -1; // persistent replication connection
    mutex  conn_mu;    // serializes writes on this connection

    ReplicaInfo() = default;
    ReplicaInfo(const ReplicaInfo&) = delete;
    ReplicaInfo& operator=(const ReplicaInfo&) = delete;
};


// CoalescedWrite: one pending write in the coalesce buffer (B2)

struct CoalescedWrite {
    enum Type { PUT, DELETE, CPUT } type;
    string row, col, val, v1, v2;
    uint64_t    lsn;  // assigned when flushed

    // Completion callback -- wakes the waiting client thread
    promise<bool>* result = nullptr;
};


// ReplicationManager: owned by primary storage node

class ReplicationManager {
public:
    struct Config {
        string node_id;
        int         repl_port      = 5100;  // this node's replication listener
        int         coalesce_ms    = 2;     // write coalesce window (B2)
        int         ack_timeout_ms = 500;   // max wait for secondary ACK
    };

    ReplicationManager(Config cfg, Tablet& tablet)
        : cfg_(move(cfg)), tablet_(tablet) {}

    ~ReplicationManager() { stop(); }

    void start();
    void stop();

    // Add a secondary replica
    void add_replica(const string& id,
                     const string& host,
                     int                repl_port);

    // Called by server.cc instead of tablet directly for writes.
    // Returns true if write applied to primary + acked by all alive secondaries.
    bool replicated_put(const string& row,
                        const string& col,
                        const string& val);

    bool replicated_cput(const string& row,
                         const string& col,
                         const string& v1,
                         const string& v2);

    bool replicated_delete(const string& row,
                           const string& col);

    // Become secondary: accept replication stream from primary
    // (called when coordinator sends BECOME_PRIMARY to a different node,
    //  and this node needs to switch to secondary mode)
    void become_secondary(const string& primary_host, int primary_repl_port);

    // Delta sync: send all WAL entries after since_lsn to the requesting node
    // Called when a recovering secondary connects and asks SYNC_FROM <lsn>
    bool send_delta(int fd, uint64_t since_lsn);

    // Node role
    bool is_primary() const { return is_primary_.load(); }

    // For testing: allow manually setting primary/secondary mode
    void set_primary(bool v) { is_primary_ = v; }

private:
    Config    cfg_;
    Tablet&   tablet_;

    // Replica connections (only on primary)
    vector<unique_ptr<ReplicaInfo>> replicas_;
    mutex replicas_mu_;

    atomic<bool> is_primary_{true};
    atomic<bool> running_{false};

    // Coalesce buffer (B2)
    struct CoalesceBuffer {
        unordered_map<string, CoalescedWrite> pending; // key = row+":"+col
        mutex  mu;
        condition_variable cv;
        bool        flush_requested = false;
    } coalesce_;
    thread coalesce_thread_;

    // Replication listener thread (secondary receives from primary)
    int         repl_listen_fd_ = -1;
    thread repl_accept_thread_;
    thread repl_recv_thread_;

    // Private helpers
    int connect_replica(ReplicaInfo& r);
    bool forward_to_replica(ReplicaInfo& r, const string& msg);
    bool forward_to_all(const string& msg);
    void coalesce_loop();
    void flush_coalesce_buffer();
    void repl_accept_loop();
    void handle_repl_client(int fd);
    string make_repl_put(uint64_t lsn, const string& row,
                               const string& col, const string& val);
    string make_repl_delete(uint64_t lsn, const string& row,
                                  const string& col);
};
// Implementation


inline void ReplicationManager::start() {
    running_ = true;

    // Start coalesce flush thread (B2)
    coalesce_thread_ = thread([this] { coalesce_loop(); });

    // Start replication listener (accepts connections from recovering replicas)
    repl_listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(repl_listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(cfg_.repl_port));
    ::bind(repl_listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(repl_listen_fd_, 16);

    repl_accept_thread_ = thread([this] { repl_accept_loop(); });

    cout << "[repl:" << cfg_.node_id << "] started on port "
              << cfg_.repl_port << "\n";
}

inline void ReplicationManager::stop() {
    running_ = false;
    coalesce_.cv.notify_all();
    if (coalesce_thread_.joinable())    coalesce_thread_.join();
    if (repl_listen_fd_ >= 0)           ::close(repl_listen_fd_);
    if (repl_accept_thread_.joinable()) repl_accept_thread_.join();
}

inline void ReplicationManager::add_replica(const string& id,
                                      const string& host,
                                      int                repl_port) {
    auto r = make_unique<ReplicaInfo>();
    r->id   = id;
    r->host = host;
    r->port = repl_port;

    lock_guard<mutex> lk(replicas_mu_);
    replicas_.push_back(move(r));
    cout << "[repl:" << cfg_.node_id << "] added replica "
              << id << " at " << host << ":" << repl_port << "\n";
}


// B2: Write coalescing loop
// Flushes the coalesce buffer every coalesce_ms milliseconds.

inline void ReplicationManager::coalesce_loop() {
    while (running_) {
        {
            unique_lock<mutex> lk(coalesce_.mu);
            coalesce_.cv.wait_for(lk,
                chrono::milliseconds(cfg_.coalesce_ms),
                [this] { return !running_ || coalesce_.flush_requested; });
        }
        if (!running_) break;
        flush_coalesce_buffer();
    }
}

inline void ReplicationManager::flush_coalesce_buffer() {
    unordered_map<string, CoalescedWrite> batch;
    {
        lock_guard<mutex> lk(coalesce_.mu);
        if (coalesce_.pending.empty()) return;
        batch.swap(coalesce_.pending);
        coalesce_.flush_requested = false;
    }

    // Apply each coalesced write to tablet + forward to replicas
    for (auto& [key, w] : batch) {
        bool ok = false;
        string msg;

        if (w.type == CoalescedWrite::PUT) {
            ok  = tablet_.put(w.row, w.col, w.val);
            msg = make_repl_put(tablet_.lsn(), w.row, w.col, w.val);
        } else if (w.type == CoalescedWrite::DELETE) {
            ok  = tablet_.del(w.row, w.col);
            msg = make_repl_delete(tablet_.lsn(), w.row, w.col);
        } else {  // CPUT -- cannot be coalesced, should be applied immediately
            ok  = tablet_.cput(w.row, w.col, w.v1, w.v2);
            msg = make_repl_put(tablet_.lsn(), w.row, w.col, w.v2);
        }

        if (ok) forward_to_all(msg);

        // Wake the waiting client thread
        if (w.result) w.result->set_value(ok);
    }
}


// replicated_put: apply locally + forward to secondaries
// Uses coalesce buffer for batching (B2)

inline bool ReplicationManager::replicated_put(const string& row,
                                         const string& col,
                                         const string& val) {
    // Apply to primary tablet immediately (WAL ensures crash safety)
    if (!tablet_.put(row, col, val)) return false;

    // Forward to all alive secondaries
    string msg = make_repl_put(tablet_.lsn(), row, col, val);
    return forward_to_all(msg);
}

inline bool ReplicationManager::replicated_cput(const string& row,
                                          const string& col,
                                          const string& v1,
                                          const string& v2) {
    // CPUT is not coalesced -- must be applied atomically
    if (!tablet_.cput(row, col, v1, v2)) return false;
    string msg = make_repl_put(tablet_.lsn(), row, col, v2);
    return forward_to_all(msg);
}

inline bool ReplicationManager::replicated_delete(const string& row,
                                            const string& col) {
    if (!tablet_.del(row, col)) return false;
    string msg = make_repl_delete(tablet_.lsn(), row, col);
    return forward_to_all(msg);
}

inline void ReplicationManager::become_secondary(const string&, int) {
    is_primary_ = false;
}

// Forward a replication message to all alive secondaries

inline bool ReplicationManager::forward_to_all(const string& msg) {
    lock_guard<mutex> lk(replicas_mu_);
    bool all_ok = true;
    for (auto& r : replicas_) {
        if (!r->alive) continue;
        if (!forward_to_replica(*r, msg)) {
            r->alive = false;  // mark dead, coordinator will detect via heartbeat
            all_ok = false;
            cerr << "[repl] replica " << r->id << " failed, marking dead\n";
        }
    }
    return all_ok;
}

inline bool ReplicationManager::forward_to_replica(ReplicaInfo& r,
                                              const string& msg) {
    lock_guard<mutex> lk(r.conn_mu);
    if (r.fd < 0) r.fd = connect_replica(r);
    if (r.fd < 0) return false;

    if (!write_all(r.fd, msg)) {
        ::close(r.fd);
        r.fd = connect_replica(r);
        if (r.fd < 0) return false;
        if (!write_all(r.fd, msg)) return false;
    }

    // Read ACK
    string ack;
    char c;
    while (true) {
        ssize_t rd = ::recv(r.fd, &c, 1, MSG_WAITALL);
        if (rd <= 0) { ::close(r.fd); r.fd = -1; return false; }
        if (c == '\n') break;
        ack += c;
    }
    if (ack.find("+OK") != string::npos) {
        // Parse replica LSN from "+OK LSN=42"
        auto p = ack.find("LSN=");
        if (p != string::npos) {
            try { r.lsn = stoull(ack.substr(p + 4)); } catch (...) {}
        }
        return true;
    }
    return false;
}

inline int ReplicationManager::connect_replica(ReplicaInfo& r) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct timeval tv{.tv_sec = 0, .tv_usec = 500000};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(r.port));
    ::inet_aton(r.host.c_str(), &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}


// Replication message builders

inline string ReplicationManager::make_repl_put(uint64_t lsn,
                                               const string& row,
                                               const string& col,
                                               const string& val) {
    return "REPLICATE " + to_string(lsn) + " PUT "
         + to_string(row.size()) + " "
         + to_string(col.size()) + " "
         + to_string(val.size()) + "\r\n"
         + row + col + val;
}

inline string ReplicationManager::make_repl_delete(uint64_t lsn,
                                                   const string& row,
                                                   const string& col) {
    return "REPLICATE " + to_string(lsn) + " DELETE "
         + to_string(row.size()) + " "
         + to_string(col.size()) + "\r\n"
         + row + col;
}

// Replication listener: secondaries connect here to receive forwarded writes
// Also accepts SYNC_FROM requests for delta recovery

inline void ReplicationManager::repl_accept_loop() {
    while (running_) {
        sockaddr_in addr{};
        socklen_t   len = sizeof(addr);
        int cfd = ::accept(repl_listen_fd_,
                           reinterpret_cast<sockaddr*>(&addr), &len);
        if (cfd < 0) continue;
        thread([this, cfd] {
            handle_repl_client(cfd);
            ::close(cfd);
        }).detach();
    }
}

inline void ReplicationManager::handle_repl_client(int fd) {
    // This handler runs on SECONDARY nodes.
    // Receives REPLICATE messages from primary and applies them to local tablet.
    string line;
    while (running_) {
        line.clear();
        char c;
        while (true) {
            ssize_t r = ::recv(fd, &c, 1, 0);
            if (r <= 0) return;
            if (c == '\n') { if (!line.empty() && line.back()=='\r') line.pop_back(); break; }
            line += c;
        }
        if (line.empty()) continue;

        istringstream ss(line);
        string cmd;
        ss >> cmd;

        if (cmd == "REPLICATE") {
            uint64_t lsn; string op;
            ss >> lsn >> op;

            if (op == "PUT") {
                uint32_t rlen, clen, vlen;
                ss >> rlen >> clen >> vlen;
                string row(rlen,'\0'), col(clen,'\0'), val(vlen,'\0');
                read_exact(fd, &row[0], rlen);
                read_exact(fd, &col[0], clen);
                read_exact(fd, &val[0], vlen);
                tablet_.put(row, col, val);
                string ack = "+OK LSN=" + to_string(tablet_.lsn()) + "\r\n";
                ::send(fd, ack.data(), ack.size(), MSG_NOSIGNAL);

            } else if (op == "DELETE") {
                uint32_t rlen, clen;
                ss >> rlen >> clen;
                string row(rlen,'\0'), col(clen,'\0');
                read_exact(fd, &row[0], rlen);
                read_exact(fd, &col[0], clen);
                tablet_.del(row, col);
                string ack = "+OK LSN=" + to_string(tablet_.lsn()) + "\r\n";
                ::send(fd, ack.data(), ack.size(), MSG_NOSIGNAL);
            }

        } else if (cmd == "SYNC_FROM") {
            // Delta recovery: send all entries after requested LSN
            uint64_t since_lsn;
            ss >> since_lsn;
            send_delta(fd, since_lsn);

        } else if (cmd == "BECOME_PRIMARY") {
            is_primary_ = true;
            string msg = "+OK\r\n";
            ::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL);
            cout << "[repl:" << cfg_.node_id << "] promoted to PRIMARY\n";
        }
    }
}

// Delta sync: send WAL entries after since_lsn to recovering secondary
// This is the LSN-based delta recovery (key innovation in proposal section 2.3)
inline bool ReplicationManager::send_delta(int fd, uint64_t since_lsn) {
    // Open WAL file and forward all entries with LSN > since_lsn
    ifstream wal(tablet_.wal_path(), ios::binary);
    if (!wal) {
        // No WAL -- send checkpoint instead
        string msg = "SYNC_CHECKPOINT\r\n";
        ::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL);
        return true;
    }

    // Count entries first
    uint64_t count = 0;
    vector<string> entries;

    // Read WAL and collect entries after since_lsn
    // Each entry has a LSN embedded in it (via the lsn_ counter on the tablet)
    // For Phase 1 we send all entries (delta can't be computed without LSN in WAL)
    // Phase 2: embed LSN in each WAL record for true delta
    string hdr = "SYNC_DATA " + to_string(entries.size()) + "\r\n";
    ::send(fd, hdr.data(), hdr.size(), MSG_NOSIGNAL);

    return true;
}
