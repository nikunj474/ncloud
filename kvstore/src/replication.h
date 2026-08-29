#pragma once
#ifndef NCLOUD_KV_REPLICATION_H
#define NCLOUD_KV_REPLICATION_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "protocol.h"
#include "tablet.h"

struct ReplicaInfo {
    std::string id;
    std::string host;
    int         port = 0;
    bool        alive = true;
    uint64_t    lsn = 0;
    int         fd = -1;
    std::mutex  conn_mu;

    ReplicaInfo() = default;
    ReplicaInfo(const ReplicaInfo&) = delete;
    ReplicaInfo& operator=(const ReplicaInfo&) = delete;
};

namespace {
constexpr size_t kLargeReplicationMsgBytes = 1024ull * 1024ull;
constexpr int kLargeReplicationSendTimeoutMs = 300000;
constexpr int kLargeReplicationAckTimeoutMs = 30000;

inline void set_repl_socket_timeouts(int fd, int recv_timeout_ms, int send_timeout_ms) {
    timeval recv_tv{};
    recv_tv.tv_sec  = recv_timeout_ms / 1000;
    recv_tv.tv_usec = (recv_timeout_ms % 1000) * 1000;
    timeval send_tv{};
    send_tv.tv_sec  = send_timeout_ms / 1000;
    send_tv.tv_usec = (send_timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &recv_tv, sizeof(recv_tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &send_tv, sizeof(send_tv));
}

inline bool read_u64_le_from_string(const std::string& s, size_t off, uint64_t& out) {
    if (off + sizeof(uint64_t) > s.size()) return false;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data() + off);
    out = 0;
    for (int i = 0; i < 8; ++i) out |= (uint64_t(p[i]) << (8 * i));
    return true;
}
}

// Tri-state result for replicated writes so callers can distinguish a
// quorum-unavailable condition (tablets degraded) from other errors.
enum class KVPutStatus {
    OK,
    QuorumUnavailable,  // write quorum not met — storage temporarily unavailable
    Mismatch,           // CPUT expected-value mismatch (not a quorum issue)
    Error
};

class ReplicationManager {
public:
    struct Config {
        std::string node_id;
        std::string tablet_name;
        int         repl_port = 5100;
        int         ack_timeout_ms = 500;   // 500ms -- fail fast on dead/zombie replicas; quorum decides commit.
    };

    ReplicationManager(Config cfg, Tablet& tablet)
        : cfg_(std::move(cfg)), tablet_(tablet) {}

    ~ReplicationManager() { stop(); }

    void start();
    void stop();

    void add_replica(const std::string& id, const std::string& host, int repl_port);

    KVPutStatus replicated_put(const std::string& row, const std::string& col, const std::string& val);
    KVPutStatus replicated_cput(const std::string& row, const std::string& col,
                                const std::string& expected, const std::string& replacement);
    KVPutStatus replicated_delete(const std::string& row, const std::string& col);

    void promote_to_primary();
    void demote_to_secondary();
    void clear_replicas();
    bool become_secondary_and_sync(const std::string& host, int port);
    bool is_primary() const { return is_primary_.load(); }

    // Server-dispatched per-tablet replication control/data path.
    bool apply_replicate_put(uint64_t lsn, const std::string& row,
                             const std::string& col, const std::string& val);
    bool apply_replicate_delete(uint64_t lsn, const std::string& row,
                                const std::string& col);
    std::string build_sync_response(uint64_t requester_ckpt_ver, uint64_t requester_lsn);

    const std::string& tablet_name() const { return cfg_.tablet_name; }

private:
    Config cfg_;
    Tablet& tablet_;

    std::vector<std::unique_ptr<ReplicaInfo>> replicas_;
    std::mutex replicas_mu_;
    std::mutex primary_write_mu_;

    std::atomic<bool> running_{false};
    std::atomic<bool> is_primary_{true};
    std::atomic<uint64_t> last_repl_lsn_{0};

    int connect_replica(ReplicaInfo& r);
    int connect_host_port(const std::string& host, int port);
    bool forward_to_replica(ReplicaInfo& r, const std::string& msg);
    bool forward_to_all(const std::string& msg);
    bool sync_from_primary(const std::string& host, int port);
    bool resync_replica_from_self(ReplicaInfo& r);
    std::string advertised_primary_host() const;

    std::string make_repl_put(uint64_t lsn, const std::string& row,
                              const std::string& col, const std::string& val);
    std::string make_repl_delete(uint64_t lsn, const std::string& row,
                                 const std::string& col);
};

inline void ReplicationManager::promote_to_primary() {
    is_primary_.store(true);
    std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
              << "] promoted to PRIMARY\n";
}

inline void ReplicationManager::clear_replicas() {
    std::lock_guard<std::mutex> lk(replicas_mu_);
    for (auto& r : replicas_) {
        std::lock_guard<std::mutex> clk(r->conn_mu);
        if (r->fd >= 0) {
            ::close(r->fd);
            r->fd = -1;
        }
    }
    replicas_.clear();
}

inline void ReplicationManager::demote_to_secondary() {
    clear_replicas();
    is_primary_.store(false);
    std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
              << "] demoted to SECONDARY\n";
}

inline void ReplicationManager::start() {
    running_.store(true);
    last_repl_lsn_.store(tablet_.lsn());
}

inline void ReplicationManager::stop() {
    running_.store(false);
    std::lock_guard<std::mutex> lk(replicas_mu_);
    for (auto& r : replicas_) {
        std::lock_guard<std::mutex> clk(r->conn_mu);
        if (r->fd >= 0) {
            ::close(r->fd);
            r->fd = -1;
        }
    }
}

inline void ReplicationManager::add_replica(const std::string& id,
                                            const std::string& host,
                                            int repl_port) {
    std::lock_guard<std::mutex> lk(replicas_mu_);
    for (auto& r : replicas_) {
        if (r->id == id) {
            r->host = host;
            r->port = repl_port;
            r->alive = true;
            std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                      << "] re-activated replica " << id
                      << " at " << host << ":" << repl_port << "\n";
            return;
        }
    }

    auto r = std::make_unique<ReplicaInfo>();
    r->id = id;
    r->host = host;
    r->port = repl_port;
    replicas_.push_back(std::move(r));

    std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
              << "] added replica " << id
              << " at " << host << ":" << repl_port << "\n";
}

inline KVPutStatus ReplicationManager::replicated_put(const std::string& row,
                                                      const std::string& col,
                                                      const std::string& val) {
    if (!is_primary()) return KVPutStatus::Error;
    std::lock_guard<std::mutex> write_lock(primary_write_mu_);

    std::string old_val;
    const bool had_old = tablet_.get(row, col, old_val);

    uint64_t lsn = 0;
    if (!tablet_.put(row, col, val, &lsn)) return KVPutStatus::Error;
    last_repl_lsn_.store(lsn);
    if (forward_to_all(make_repl_put(lsn, row, col, val))) return KVPutStatus::OK;

    // Quorum not met: roll back local write, then forward the compensating
    // operation so any replica that DID receive the write also rolls back.
    // This keeps all replica LSNs in sync with the primary, preventing the
    // coordinator from seeing a primary LSN that no secondary can match on
    // the next election.
    uint64_t rollback_lsn = 0;
    bool rollback_ok = had_old
        ? tablet_.put(row, col, old_val, &rollback_lsn)
        : tablet_.del(row, col, nullptr, &rollback_lsn);
    if (rollback_ok) {
        last_repl_lsn_.store(rollback_lsn);
        const std::string rb_msg = had_old
            ? make_repl_put(rollback_lsn, row, col, old_val)
            : make_repl_delete(rollback_lsn, row, col);
        forward_to_all(rb_msg); // best-effort; replica will resync if this fails too
    } else {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] failed to roll back local PUT after quorum failure\n";
    }
    return KVPutStatus::QuorumUnavailable;
}

inline KVPutStatus ReplicationManager::replicated_cput(const std::string& row,
                                                       const std::string& col,
                                                       const std::string& expected,
                                                       const std::string& replacement) {
    if (!is_primary()) return KVPutStatus::Error;
    std::lock_guard<std::mutex> write_lock(primary_write_mu_);

    std::string old_val;
    if (!tablet_.get(row, col, old_val) || old_val != expected) return KVPutStatus::Mismatch;

    uint64_t lsn = 0;
    if (!tablet_.cput(row, col, expected, replacement, &lsn)) return KVPutStatus::Error;
    last_repl_lsn_.store(lsn);
    if (forward_to_all(make_repl_put(lsn, row, col, replacement))) return KVPutStatus::OK;

    uint64_t rollback_lsn = 0;
    if (tablet_.put(row, col, old_val, &rollback_lsn)) {
        last_repl_lsn_.store(rollback_lsn);
        forward_to_all(make_repl_put(rollback_lsn, row, col, old_val)); // best-effort
    } else {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] failed to roll back local CPUT after quorum failure\n";
    }
    return KVPutStatus::QuorumUnavailable;
}

inline KVPutStatus ReplicationManager::replicated_delete(const std::string& row,
                                                         const std::string& col) {
    if (!is_primary()) return KVPutStatus::Error;
    std::lock_guard<std::mutex> write_lock(primary_write_mu_);

    std::string old_val;
    const bool had_old = tablet_.get(row, col, old_val);

    bool deleted = false;
    uint64_t lsn = 0;
    if (!tablet_.del(row, col, &deleted, &lsn)) return KVPutStatus::Error;
    if (!deleted) return KVPutStatus::OK;
    last_repl_lsn_.store(lsn);
    if (forward_to_all(make_repl_delete(lsn, row, col))) return KVPutStatus::OK;

    uint64_t rollback_lsn = 0;
    if (had_old && tablet_.put(row, col, old_val, &rollback_lsn)) {
        last_repl_lsn_.store(rollback_lsn);
        forward_to_all(make_repl_put(rollback_lsn, row, col, old_val)); // best-effort
    } else {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] failed to roll back local DELETE after quorum failure\n";
    }
    return KVPutStatus::QuorumUnavailable;
}

inline bool ReplicationManager::apply_replicate_put(uint64_t lsn,
                                                    const std::string& row,
                                                    const std::string& col,
                                                    const std::string& val) {
    const uint64_t current = tablet_.lsn();
    if (lsn <= current) {
        last_repl_lsn_.store(current);
        return true;
    }
    if (lsn != current + 1) {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] rejecting out-of-order replicated PUT: have="
                  << current << " incoming=" << lsn
                  << " (replica must be resynced before it can ACK writes)\n";
        return false;
    }
    if (!tablet_.apply_replicated_put(row, col, val, lsn)) return false;
    last_repl_lsn_.store(lsn);
    return true;
}

inline bool ReplicationManager::apply_replicate_delete(uint64_t lsn,
                                                       const std::string& row,
                                                       const std::string& col) {
    const uint64_t current = tablet_.lsn();
    if (lsn <= current) {
        last_repl_lsn_.store(current);
        return true;
    }
    if (lsn != current + 1) {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] rejecting out-of-order replicated DELETE: have="
                  << current << " incoming=" << lsn
                  << " (replica must be resynced before it can ACK writes)\n";
        return false;
    }
    if (!tablet_.apply_replicated_delete(row, col, lsn)) return false;
    last_repl_lsn_.store(lsn);
    return true;
}

inline std::string ReplicationManager::build_sync_response(uint64_t requester_ckpt_ver,
                                                           uint64_t requester_lsn) {
    std::string blob;
    std::string hdr;
    const uint64_t local_lsn = tablet_.lsn();
    if (requester_lsn == local_lsn) {
        // Same LSN means the requester already has every committed operation.
        // Checkpoint versions can differ after local compaction, but sending a
        // full snapshot here is wasteful and can make large-file recovery loop.
        blob = tablet_.wal_delta_after(requester_lsn);
        hdr = "+DELTA " + std::to_string(blob.size()) + "\r\n";
        std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] SYNC_FROM requester ckpt=" << requester_ckpt_ver
                  << " lsn=" << requester_lsn
                  << " -> DELTA len=" << blob.size()
                  << " (already caught up)"
                  << " local_ckpt=" << tablet_.checkpoint_version()
                  << " local_lsn=" << local_lsn << "\n";
    } else if (requester_ckpt_ver == tablet_.checkpoint_version() && requester_lsn < local_lsn) {
        blob = tablet_.wal_delta_after(requester_lsn);
        bool send_snapshot = false;
        std::string snapshot_reason;
        if (blob.size() <= sizeof(uint64_t)) {
            // The requester is behind, but our WAL no longer contains the
            // missing entry because it was folded into a checkpoint. Send a
            // snapshot so the replica can close the LSN gap cleanly.
            send_snapshot = true;
            snapshot_reason = "delta unavailable";
        } else if (blob.size() > sizeof(uint64_t)) {
            uint64_t first_delta_lsn = 0;
            if (!read_u64_le_from_string(blob, sizeof(uint64_t), first_delta_lsn) ||
                first_delta_lsn != requester_lsn + 1) {
                send_snapshot = true;
                snapshot_reason = "delta gap first_lsn=" + std::to_string(first_delta_lsn);
            }
        }

        if (send_snapshot) {
            blob = tablet_.snapshot_blob();
            hdr = "+SNAPSHOT " + std::to_string(blob.size()) + "\r\n";
            std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                      << "] SYNC_FROM requester ckpt=" << requester_ckpt_ver
                      << " lsn=" << requester_lsn
                      << " -> SNAPSHOT len=" << blob.size()
                      << " (" << snapshot_reason << ")"
                      << " local_ckpt=" << tablet_.checkpoint_version()
                      << " local_lsn=" << local_lsn << "\n";
        } else {
            hdr = "+DELTA " + std::to_string(blob.size()) + "\r\n";
            std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                      << "] SYNC_FROM requester ckpt=" << requester_ckpt_ver
                      << " lsn=" << requester_lsn
                      << " -> DELTA len=" << blob.size()
                      << " local_ckpt=" << tablet_.checkpoint_version()
                      << " local_lsn=" << local_lsn << "\n";
        }
    } else {
        blob = tablet_.snapshot_blob();
        hdr = "+SNAPSHOT " + std::to_string(blob.size()) + "\r\n";
        std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] SYNC_FROM requester ckpt=" << requester_ckpt_ver
                  << " lsn=" << requester_lsn
                  << " -> SNAPSHOT len=" << blob.size()
                  << " local_ckpt=" << tablet_.checkpoint_version()
                  << " local_lsn=" << local_lsn << "\n";
    }
    return hdr + blob;
}

inline int ReplicationManager::connect_host_port(const std::string& host, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct timeval tv{};
    tv.tv_sec  = cfg_.ack_timeout_ms / 1000;
    tv.tv_usec = (cfg_.ack_timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_aton(host.c_str(), &addr.sin_addr) == 0) {
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

inline int ReplicationManager::connect_replica(ReplicaInfo& r) {
    return connect_host_port(r.host, r.port);
}

inline bool ReplicationManager::forward_to_replica(ReplicaInfo& r, const std::string& msg) {
    std::lock_guard<std::mutex> lk(r.conn_mu);
    const bool large_msg = msg.size() >= kLargeReplicationMsgBytes;

    if (r.fd < 0) r.fd = connect_replica(r);
    if (r.fd < 0) return false;

    if (large_msg) {
        set_repl_socket_timeouts(r.fd,
                                 kLargeReplicationAckTimeoutMs,
                                 kLargeReplicationSendTimeoutMs);
    }

    if (!write_all(r.fd, msg)) {
        ::close(r.fd);
        r.fd = connect_replica(r);
        if (r.fd >= 0 && large_msg) {
            set_repl_socket_timeouts(r.fd,
                                     kLargeReplicationAckTimeoutMs,
                                     kLargeReplicationSendTimeoutMs);
        }
        if (r.fd < 0 || !write_all(r.fd, msg)) {
            if (r.fd >= 0) ::close(r.fd);
            r.fd = -1;
            return false;
        }
    }

    std::string ack;
    if (!read_line(r.fd, ack)) {
        if (large_msg) {
            std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                      << "] replica " << r.id
                      << " did not ACK large payload within "
                      << kLargeReplicationAckTimeoutMs
                      << "ms; excluding it from write quorum\n";
        }
        ::close(r.fd);
        r.fd = -1;
        return false;
    }
    if (ack.rfind("+OK", 0) != 0) {
        ::close(r.fd);
        r.fd = -1;
        return false;
    }

    auto p = ack.find("LSN=");
    if (p != std::string::npos) {
        try { r.lsn = std::stoull(ack.substr(p + 4)); } catch (...) {}
    }
    if (large_msg && r.fd >= 0) {
        set_repl_socket_timeouts(r.fd, cfg_.ack_timeout_ms, cfg_.ack_timeout_ms);
    }
    return true;
}

inline std::string ReplicationManager::advertised_primary_host() const {
    const char* env_host = std::getenv("NCLOUD_REPL_HOST");
    if (env_host && *env_host) return env_host;
    // The course/demo deployment runs every replica on localhost.  Keep this
    // explicit so a gap repair never guesses a public hostname incorrectly.
    return "127.0.0.1";
}

inline bool ReplicationManager::resync_replica_from_self(ReplicaInfo& r) {
    int fd = connect_host_port(r.host, r.port);
    if (fd < 0) return false;

    set_repl_socket_timeouts(fd, 120000, 120000);

    const std::string cmd = "BECOME_SECONDARY " + cfg_.tablet_name + " "
        + advertised_primary_host() + " " + std::to_string(cfg_.repl_port) + "\r\n";
    if (!write_all(fd, cmd)) {
        ::close(fd);
        return false;
    }

    std::string resp;
    bool ok = read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
    ::close(fd);

    if (ok) {
        r.lsn = tablet_.lsn();
        std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] repaired replica " << r.id
                  << " to LSN=" << r.lsn << " before retrying write\n";
    } else {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] failed to repair replica " << r.id
                  << " resp='" << resp << "'\n";
    }
    return ok;
}

inline bool ReplicationManager::forward_to_all(const std::string& msg) {
    std::vector<ReplicaInfo*> replicas;
    {
        std::lock_guard<std::mutex> lk(replicas_mu_);
        replicas.reserve(replicas_.size());
        for (auto& r : replicas_) replicas.push_back(r.get());
    }
    const size_t total_nodes = replicas.size() + 1; // local primary + configured replicas
    const size_t quorum = total_nodes / 2 + 1;
    const size_t required_remote_acks = quorum > 0 ? quorum - 1 : 0;
    if (required_remote_acks == 0) return true;

    size_t acked = 0;
    for (auto* replica : replicas) {
        bool ok = forward_to_replica(*replica, msg);
        if (!ok && resync_replica_from_self(*replica)) {
            ok = forward_to_replica(*replica, msg);
        }
        replica->alive = ok;
        if (ok) {
            ++acked;
        } else {
            std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                      << "] replica " << replica->id << " did not ACK\n";
        }
    }

    if (acked < required_remote_acks) {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] write failed quorum: remote_acks=" << acked
                  << " required=" << required_remote_acks
                  << " configured_replicas=" << replicas.size()
                  << " (local WAL entry remains for recovery/cleanup)\n";
        return false;
    }
    return true;
}

inline std::string ReplicationManager::make_repl_put(uint64_t lsn,
                                                     const std::string& row,
                                                     const std::string& col,
                                                     const std::string& val) {
    return "REPLICATE " + cfg_.tablet_name + " " + std::to_string(lsn) + " PUT "
        + std::to_string(row.size()) + " "
        + std::to_string(col.size()) + " "
        + std::to_string(val.size()) + "\r\n" + row + col + val;
}

inline std::string ReplicationManager::make_repl_delete(uint64_t lsn,
                                                        const std::string& row,
                                                        const std::string& col) {
    return "REPLICATE " + cfg_.tablet_name + " " + std::to_string(lsn) + " DELETE "
        + std::to_string(row.size()) + " "
        + std::to_string(col.size()) + "\r\n" + row + col;
}

inline bool ReplicationManager::sync_from_primary(const std::string& host, int port) {
    int fd = connect_host_port(host, port);
    if (fd < 0) return false;

    std::string req = "SYNC_FROM " + cfg_.tablet_name + " "
        + std::to_string(tablet_.checkpoint_version()) + " "
        + std::to_string(tablet_.lsn()) + "\r\n";
    if (!write_all(fd, req)) {
        ::close(fd);
        return false;
    }

    std::string hdr;
    if (!read_line(fd, hdr)) {
        ::close(fd);
        return false;
    }

    std::istringstream ss(hdr);
    std::string kind;
    size_t len = 0;
    ss >> kind >> len;

    std::string blob;
    if (!read_exact(fd, blob, len)) {
        ::close(fd);
        return false;
    }
    ::close(fd);

    bool loaded = false;
    if (kind == "+SNAPSHOT") {
        loaded = tablet_.load_snapshot_blob(blob);
    } else if (kind == "+DELTA") {
        loaded = tablet_.apply_wal_delta_blob(blob);
    } else {
        return false;
    }

    if (loaded) {
        last_repl_lsn_.store(tablet_.lsn());
        std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] sync complete from "
                  << host << ":" << port
                  << " using " << (kind == "+DELTA" ? "DELTA" : "SNAPSHOT")
                  << " at LSN=" << tablet_.lsn()
                  << " ckpt_ver=" << tablet_.checkpoint_version() << "\n";
    }
    return loaded;
}

inline bool ReplicationManager::become_secondary_and_sync(const std::string& host, int port) {
    demote_to_secondary();
    if (!host.empty() && port > 0) return sync_from_primary(host, port);
    return true;
}

#endif  // NCLOUD_KV_REPLICATION_H
