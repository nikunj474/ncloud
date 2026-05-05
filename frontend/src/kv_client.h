#pragma once
// =============================================================================
// kv_client.h  --  Frontend-side KV store client
// =============================================================================
//
// Failover + safe replica-read behavior:
//   - Frontend caches a current KV target.
//   - Writes still refresh via LOOKUP (primary path).
//   - Reads refresh via READLOOKUP, which prefers a fully caught-up secondary
//     when coordinator deems one safe, otherwise falls back to primary.
//   - Any stale sockets are aggressively dropped after failure.
//   - GET distinguishes between:
//       * Found
//       * NotFound
//       * Unavailable
// =============================================================================

#include <string>
#include <vector>
#include <mutex>
#include <queue>
#include <memory>
#include <stdexcept>
#include <iostream>
#include <unordered_map>
#include <functional>
#include <sstream>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <chrono>

#include "../../kvstore/src/protocol.h"

struct NodeInfo {
    std::string host;
    int         port;
    bool        alive = true;
};

enum class KVReadStatus {
    Found,
    NotFound,
    Unavailable
};

class ConnectionPool {
public:
    explicit ConnectionPool(const NodeInfo& node, int pool_size = 2)
        : node_(node) {
        for (int i = 0; i < pool_size; ++i) {
            int fd = connect_to_node();
            if (fd >= 0) conns_.push(fd);
        }
    }

    ~ConnectionPool() {
        clear();
    }

    int borrow() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            while (!conns_.empty()) {
                int fd = conns_.front();
                conns_.pop();
                if (fd >= 0) return fd;
            }
        }
        return connect_to_node();
    }

    void release(int fd) {
        if (fd < 0) return;
        std::lock_guard<std::mutex> lk(mu_);
        conns_.push(fd);
    }

    void clear() {
        std::lock_guard<std::mutex> lk(mu_);
        while (!conns_.empty()) {
            int fd = conns_.front();
            conns_.pop();
            if (fd >= 0) ::close(fd);
        }
    }

    const NodeInfo& node() const { return node_; }

private:
    static constexpr int kDefaultSocketTimeoutMs = 350;

    NodeInfo        node_;
    std::queue<int> conns_;
    std::mutex      mu_;

    int connect_to_node() {
        struct addrinfo hints{}, *res;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        std::string port_str = std::to_string(node_.port);

        if (::getaddrinfo(node_.host.c_str(), port_str.c_str(), &hints, &res) != 0)
            return -1;

        int fd = ::socket(res->ai_family, res->ai_socktype, 0);
        if (fd < 0) {
            ::freeaddrinfo(res);
            return -1;
        }

        timeval tv{};
        tv.tv_sec = kDefaultSocketTimeoutMs / 1000;
        tv.tv_usec = (kDefaultSocketTimeoutMs % 1000) * 1000;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
            ::close(fd);
            ::freeaddrinfo(res);
            return -1;
        }

        ::freeaddrinfo(res);
        return fd;
    }
};

class KVClient {
public:
    KVClient(const std::string& host, int port)
        : current_(NodeInfo{host, port}),
          pool_(std::make_shared<ConnectionPool>(current_)) {}

    KVClient(const std::string& host, int port,
             const std::string& coord_host, int coord_port)
        : current_(NodeInfo{host, port}),
          pool_(std::make_shared<ConnectionPool>(current_)),
          coord_host_(coord_host),
          coord_port_(coord_port),
          use_coordinator_(coord_port > 0) {}

    bool put(const std::string& row,
             const std::string& col,
             const std::string& val) {
        const int timeout_ms = val.size() >= kLargeValueThresholdBytes
                                 ? kLargeValueSocketTimeoutMs
                                 : kDefaultSocketTimeoutMs;
        return exec_row(row, [&](int fd) {
            if (!send_put(fd, row, col, val)) return false;
            KVResponse resp = read_response(fd);
            return resp.ok;
        }, timeout_ms);
    }

    KVReadStatus get_status(const std::string& row,
                            const std::string& col,
                            std::string& val_out) {
        constexpr int kAttempts = 4;
        constexpr int kSleepMs[kAttempts] = {0, 75, 150, 250};

        for (int attempt = 0; attempt < kAttempts; ++attempt) {
            // Always read from primary to avoid returning stale data from a
            // lagging secondary (which would hide recently-written drive children,
            // session tokens, etc. until replication catches up).
            if (use_coordinator_) {
                NodeInfo primary_node;
                if (lookup_primary(row, primary_node)) {
                    install_target(primary_node, false);
                }
            }

            auto pool = snapshot_pool();
            int fd = pool ? pool->borrow() : -1;
            if (fd < 0) {
                if (use_coordinator_) refresh_target_for_row(row, false);
                std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs[attempt]));
                continue;
            }

            KVReadStatus st = do_get_once(fd, row, col, val_out);

            if (st == KVReadStatus::Found || st == KVReadStatus::NotFound) {
                pool->release(fd);
                return st;
            }

            // Unavailable — primary unreachable, retry with fresh target
            ::close(fd);
            rebuild_current_pool();
            if (use_coordinator_) refresh_target_for_row(row, false);
            std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs[attempt]));
        }

        return KVReadStatus::Unavailable;
    }

    bool get(const std::string& row,
             const std::string& col,
             std::string& val_out) {
        return get_status(row, col, val_out) == KVReadStatus::Found;
    }

    bool cput(const std::string& row,
              const std::string& col,
              const std::string& v1,
              const std::string& v2) {
        return exec_row(row, [&](int fd) {
            if (!send_cput(fd, row, col, v1, v2)) return false;
            KVResponse resp = read_response(fd);
            return resp.ok;
        });
    }

    bool del(const std::string& row, const std::string& col) {
        return exec_row(row, [&](int fd) {
            if (!send_delete(fd, row, col)) return false;
            KVResponse resp = read_response(fd);
            return resp.ok;
        });
    }

    std::string get_str(const std::string& row, const std::string& col) {
        std::string val;
        if (get(row, col, val)) return val;
        return "";
    }

    bool ping(uint64_t& lsn_out) {
        auto pool = snapshot_pool();
        int fd = pool ? pool->borrow() : -1;
        if (fd < 0) return false;

        bool ok = false;
        if (write_all(fd, "PING\r\n")) {
            std::string line;
            if (read_line(fd, line) && line.rfind("+OK", 0) == 0) {
                auto p = line.find("LSN=");
                if (p != std::string::npos) {
                    try {
                        lsn_out = std::stoull(line.substr(p + 4));
                    } catch (...) {
                        lsn_out = 0;
                    }
                } else {
                    lsn_out = 0;
                }
                ok = true;
            }
        }

        if (ok) {
            pool->release(fd);
        } else {
            ::close(fd);
            rebuild_current_pool();
        }
        return ok;
    }

private:
    static constexpr int kDefaultSocketTimeoutMs = 350;
    static constexpr int kLargeValueSocketTimeoutMs = 30000;
    static constexpr size_t kLargeValueThresholdBytes = 256 * 1024;

    std::mutex                      mu_;
    NodeInfo                        current_;
    std::shared_ptr<ConnectionPool> pool_;
    std::string                     coord_host_;
    int                             coord_port_ = 0;
    bool                            use_coordinator_ = false;

    template <typename Op>
    bool exec_row(const std::string& row, Op op, int socket_timeout_ms = kDefaultSocketTimeoutMs) {
        constexpr int kAttempts = 4;
        constexpr int kSleepMs[kAttempts] = {0, 75, 150, 250};

        if (use_coordinator_) {
            refresh_target_for_row(row, true);
        }

        for (int attempt = 0; attempt < kAttempts; ++attempt) {
            auto pool = snapshot_pool();
            int fd = pool ? pool->borrow() : -1;

            if (fd >= 0) {
                set_socket_timeout(fd, socket_timeout_ms);
                bool ok = op(fd);
                if (ok) {
                    if (socket_timeout_ms != kDefaultSocketTimeoutMs) {
                        set_socket_timeout(fd, kDefaultSocketTimeoutMs);
                    }
                    pool->release(fd);
                    return true;
                }

                ::close(fd);
                rebuild_current_pool();
            }

            if (use_coordinator_) {
                refresh_target_for_row(row, true);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs[attempt]));
        }

        return false;
    }

    static void set_socket_timeout(int fd, int timeout_ms) {
        if (fd < 0) return;
        timeval tv{};
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }

    KVReadStatus do_get_once(int fd,
                             const std::string& row,
                             const std::string& col,
                             std::string& val_out) {
        if (!send_get(fd, row, col)) return KVReadStatus::Unavailable;

        std::string line;
        if (!read_line(fd, line)) return KVReadStatus::Unavailable;

        if (line.rfind("+OK ", 0) == 0) {
            std::istringstream ss(line.substr(4));
            uint32_t vallen = 0;
            if (!(ss >> vallen)) return KVReadStatus::Unavailable;

            std::string val;
            if (!read_exact(fd, val, vallen)) return KVReadStatus::Unavailable;
            val_out = std::move(val);
            return KVReadStatus::Found;
        }

        if (line.rfind("-ERR ", 0) == 0) {
            std::string msg = line.substr(5);
            if (msg == "not found") return KVReadStatus::NotFound;
            return KVReadStatus::Unavailable;
        }

        return KVReadStatus::Unavailable;
    }

    std::shared_ptr<ConnectionPool> snapshot_pool() {
        std::lock_guard<std::mutex> lk(mu_);
        return pool_;
    }

    void rebuild_current_pool() {
        NodeInfo target;
        {
            std::lock_guard<std::mutex> lk(mu_);
            target = current_;
        }
        auto fresh_pool = std::make_shared<ConnectionPool>(target);
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (same_node(current_, target)) pool_ = std::move(fresh_pool);
        }
    }

    bool install_target(const NodeInfo& node, bool log_result) {
        bool changed = false;
        while (true) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                changed = !pool_ || !same_node(node, current_);
                if (!changed) break;
            }

            auto fresh_pool = std::make_shared<ConnectionPool>(node);

            {
                std::lock_guard<std::mutex> lk(mu_);
                if (!pool_ || !same_node(node, current_)) {
                    current_ = node;
                    pool_ = std::move(fresh_pool);
                    changed = true;
                    break;
                }
            }
        }

        if (log_result) {
            std::cout << "[kv_client] "
                      << (changed ? "refreshed" : "revalidated")
                      << " KV write target to " << node.host << ":" << node.port
                      << "\n";
        }
        return true;
    }

    static bool same_node(const NodeInfo& a, const NodeInfo& b) {
        return a.host == b.host && a.port == b.port;
    }

    bool refresh_target_for_row(const std::string& row, bool log_result) {
        if (!use_coordinator_ || coord_host_.empty() || coord_port_ <= 0)
            return false;

        constexpr int kLookupAttempts = 4;
        constexpr int kSleepMs[kLookupAttempts] = {0, 75, 150, 250};

        NodeInfo node;
        for (int i = 0; i < kLookupAttempts; ++i) {
            if (lookup_primary(row, node)) {
                return install_target(node, log_result);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs[i]));
        }
        return false;
    }

    bool lookup_primary(const std::string& row, NodeInfo& out) {
        struct addrinfo hints{}, *res;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        std::string port_str = std::to_string(coord_port_);

        if (::getaddrinfo(coord_host_.c_str(), port_str.c_str(), &hints, &res) != 0)
            return false;

        int fd = ::socket(res->ai_family, res->ai_socktype, 0);
        if (fd < 0) {
            ::freeaddrinfo(res);
            return false;
        }

        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 500000; // 500 ms
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
            ::freeaddrinfo(res);
            ::close(fd);
            return false;
        }
        ::freeaddrinfo(res);

        std::string hdr = "LOOKUP " + std::to_string(row.size()) + "\r\n";
        if (!write_all(fd, hdr) || !write_all(fd, row)) {
            ::close(fd);
            return false;
        }

        std::string line;
        if (!read_line(fd, line)) {
            ::close(fd);
            return false;
        }
        ::close(fd);

        if (line.rfind("+OK ", 0) != 0) return false;

        std::istringstream ss(line.substr(4));
        std::string host, role;
        int port = -1;
        ss >> host >> port >> role;
        if (host.empty() || port <= 0) return false;

        out = NodeInfo{host, port};
        return true;
    }

    bool lookup_read_target(const std::string& row,
                            NodeInfo& out,
                            bool* is_secondary = nullptr) {
        struct addrinfo hints{}, *res;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        std::string port_str = std::to_string(coord_port_);

        if (::getaddrinfo(coord_host_.c_str(), port_str.c_str(), &hints, &res) != 0)
            return false;

        int fd = ::socket(res->ai_family, res->ai_socktype, 0);
        if (fd < 0) {
            ::freeaddrinfo(res);
            return false;
        }

        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 500000;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
            ::freeaddrinfo(res);
            ::close(fd);
            return false;
        }
        ::freeaddrinfo(res);

        std::string hdr = "READLOOKUP " + std::to_string(row.size()) + "\r\n";
        if (!write_all(fd, hdr) || !write_all(fd, row)) {
            ::close(fd);
            return false;
        }

        std::string line;
        if (!read_line(fd, line)) {
            ::close(fd);
            return false;
        }
        ::close(fd);

        if (line.rfind("+OK ", 0) != 0) return false;

        std::istringstream ss(line.substr(4));
        std::string host, role;
        int port = -1;
        ss >> host >> port >> role;
        if (host.empty() || port <= 0) return false;

        if (is_secondary) {
            *is_secondary = (role == "secondary");
        }

        out = NodeInfo{host, port};
        return true;
    }
};
