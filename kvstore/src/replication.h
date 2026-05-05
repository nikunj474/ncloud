#pragma once
#ifndef PENNCLOUD_KV_REPLICATION_H
#define PENNCLOUD_KV_REPLICATION_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
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

class ReplicationManager {
public:
    struct Config {
        std::string node_id;
        std::string tablet_name;
        int         repl_port = 5100;
        int         ack_timeout_ms = 500;
    };

    ReplicationManager(Config cfg, Tablet& tablet)
        : cfg_(std::move(cfg)), tablet_(tablet) {}

    ~ReplicationManager() { stop(); }

    void start();
    void stop();

    void add_replica(const std::string& id, const std::string& host, int repl_port);

    bool replicated_put(const std::string& row, const std::string& col, const std::string& val);
    bool replicated_cput(const std::string& row, const std::string& col,
                         const std::string& expected, const std::string& replacement);
    bool replicated_delete(const std::string& row, const std::string& col);

    void promote_to_primary();
    void demote_to_secondary();
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

inline void ReplicationManager::demote_to_secondary() {
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

inline bool ReplicationManager::replicated_put(const std::string& row,
                                               const std::string& col,
                                               const std::string& val) {
    if (!is_primary()) return false;
    std::lock_guard<std::mutex> write_lock(primary_write_mu_);
    uint64_t lsn = 0;
    if (!tablet_.put(row, col, val, &lsn)) return false;
    last_repl_lsn_.store(lsn);
    return forward_to_all(make_repl_put(lsn, row, col, val));
}

inline bool ReplicationManager::replicated_cput(const std::string& row,
                                                const std::string& col,
                                                const std::string& expected,
                                                const std::string& replacement) {
    if (!is_primary()) return false;
    std::lock_guard<std::mutex> write_lock(primary_write_mu_);
    uint64_t lsn = 0;
    if (!tablet_.cput(row, col, expected, replacement, &lsn)) return false;
    last_repl_lsn_.store(lsn);
    return forward_to_all(make_repl_put(lsn, row, col, replacement));
}

inline bool ReplicationManager::replicated_delete(const std::string& row,
                                                  const std::string& col) {
    if (!is_primary()) return false;
    std::lock_guard<std::mutex> write_lock(primary_write_mu_);
    bool deleted = false;
    uint64_t lsn = 0;
    if (!tablet_.del(row, col, &deleted, &lsn)) return false;
    if (!deleted) return true;
    last_repl_lsn_.store(lsn);
    return forward_to_all(make_repl_delete(lsn, row, col));
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
                  << "] warning: gap in replicated LSN stream. have="
                  << current << " incoming=" << lsn << "\n";
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
                  << "] warning: gap in replicated LSN stream. have="
                  << current << " incoming=" << lsn << "\n";
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
    if (requester_ckpt_ver == tablet_.checkpoint_version() && requester_lsn <= local_lsn) {
        blob = tablet_.wal_delta_after(requester_lsn);
        hdr = "+DELTA " + std::to_string(blob.size()) + "\r\n";
        std::cout << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] SYNC_FROM requester ckpt=" << requester_ckpt_ver
                  << " lsn=" << requester_lsn
                  << " -> DELTA len=" << blob.size()
                  << " local_ckpt=" << tablet_.checkpoint_version()
                  << " local_lsn=" << local_lsn << "\n";
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
    tv.tv_sec = cfg_.ack_timeout_ms / 1000;
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

    if (r.fd < 0) r.fd = connect_replica(r);
    if (r.fd < 0) return false;

    if (!write_all(r.fd, msg)) {
        ::close(r.fd);
        r.fd = connect_replica(r);
        if (r.fd < 0 || !write_all(r.fd, msg)) {
            if (r.fd >= 0) ::close(r.fd);
            r.fd = -1;
            return false;
        }
    }

    std::string ack;
    if (!read_line(r.fd, ack)) {
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
    return true;
}

inline bool ReplicationManager::forward_to_all(const std::string& msg) {
    size_t acked = 0;
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

    std::vector<bool> replica_acked(replicas.size(), false);
    constexpr int kMaxAttempts = 2;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        for (size_t i = 0; i < replicas.size(); ++i) {
            if (replica_acked[i]) continue;
            auto& r = *replicas[i];
            if (!forward_to_replica(r, msg)) {
                r.alive = false;
                std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                          << "] replica " << r.id << " did not ACK"
                          << " on attempt " << (attempt + 1) << "\n";
            } else {
                r.alive = true;
                replica_acked[i] = true;
                ++acked;
            }
        }
        if (acked >= required_remote_acks || attempt + 1 >= kMaxAttempts) break;
        if (acked < required_remote_acks) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }
    if (acked < required_remote_acks) {
        std::cerr << "[repl:" << cfg_.node_id << ":" << cfg_.tablet_name
                  << "] write committed locally in degraded mode: remote_acks=" << acked
                  << " required=" << required_remote_acks
                  << " configured_replicas=" << replicas.size()
                  << " (replicas will catch up during recovery)\n";
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

#endif  // PENNCLOUD_KV_REPLICATION_H
