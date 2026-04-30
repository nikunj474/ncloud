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
        std::lock_guard<std::mutex> lk(mu_);
        while (!conns_.empty()) {
            int fd = conns_.front();
            conns_.pop();
            if (fd >= 0) return fd;
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
        tv.tv_sec = 0;
        tv.tv_usec = 350000; // 350 ms
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
          pool_(std::make_unique<ConnectionPool>(current_)) {}

    KVClient(const std::string& host, int port,
             const std::string& coord_host, int coord_port)
        : current_(NodeInfo{host, port}),
          pool_(std::make_unique<ConnectionPool>(current_)),
          coord_host_(coord_host),
          coord_port_(coord_port),
          use_coordinator_(coord_port > 0) {}

    bool put(const std::string& row,
             const std::string& col,
             const std::string& val) {
        return exec_row(row, [&](int fd) {
            if (!send_put(fd, row, col, val)) return false;
            KVResponse resp = read_response(fd);
            return resp.ok;
        });
    }

    KVReadStatus get_status(const std::string& row,
                            const std::string& col,
                            std::string& val_out) {
        std::lock_guard<std::mutex> lk(mu_);

        constexpr int kAttempts = 4;
        constexpr int kSleepMs[kAttempts] = {0, 75, 150, 250};

        for (int attempt = 0; attempt < kAttempts; ++attempt) {
            // Always read from primary to avoid returning stale data from a
            // lagging secondary (which would hide recently-written drive children,
            // session tokens, etc. until replication catches up).
            if (use_coordinator_) {
                NodeInfo primary_node;
                if (lookup_primary(row, primary_node)) {
                    bool changed = (primary_node.host != current_.host || primary_node.port != current_.port);
                    current_ = primary_node;
                    if (changed) rebuild_pool_locked(current_);
                }
            }

            int fd = pool_->borrow();
            if (fd < 0) {
                if (use_coordinator_) refresh_target_for_row_locked(row);
                std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs[attempt]));
                continue;
            }

            KVReadStatus st = do_get_once(fd, row, col, val_out);

            if (st == KVReadStatus::Found || st == KVReadStatus::NotFound) {
                pool_->release(fd);
                return st;
            }

            // Unavailable — primary unreachable, retry with fresh target
            ::close(fd);
            rebuild_pool_locked(current_);
            if (use_coordinator_) refresh_target_for_row_locked(row);
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
        std::lock_guard<std::mutex> lk(mu_);
        int fd = pool_->borrow();
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
            pool_->release(fd);
        } else {
            ::close(fd);
            rebuild_pool_locked(current_);
        }
        return ok;
    }

private:
    std::mutex                      mu_;
    NodeInfo                        current_;
    std::unique_ptr<ConnectionPool> pool_;
    std::string                     coord_host_;
    int                             coord_port_ = 0;
    bool                            use_coordinator_ = false;

    template <typename Op>
    bool exec_row(const std::string& row, Op op) {
        std::lock_guard<std::mutex> lk(mu_);

        constexpr int kAttempts = 4;
        constexpr int kSleepMs[kAttempts] = {0, 75, 150, 250};

        if (use_coordinator_) {
            refresh_target_for_row_locked(row);
        }

        for (int attempt = 0; attempt < kAttempts; ++attempt) {
            int fd = pool_->borrow();

            if (fd >= 0) {
                bool ok = op(fd);
                if (ok) {
                    pool_->release(fd);
                    return true;
                }

                ::close(fd);
                rebuild_pool_locked(current_);
            }

            if (use_coordinator_) {
                refresh_target_for_row_locked(row);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs[attempt]));
        }

        return false;
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

    void rebuild_pool_locked(const NodeInfo& node) {
        pool_.reset();
        pool_ = std::make_unique<ConnectionPool>(node);
    }

    bool refresh_target_for_row_locked(const std::string& row) {
        if (!use_coordinator_ || coord_host_.empty() || coord_port_ <= 0)
            return false;

        constexpr int kLookupAttempts = 4;
        constexpr int kSleepMs[kLookupAttempts] = {0, 75, 150, 250};

        NodeInfo node;
        for (int i = 0; i < kLookupAttempts; ++i) {
            if (lookup_primary(row, node)) {
                bool changed = (node.host != current_.host || node.port != current_.port);
                current_ = node;

                rebuild_pool_locked(current_);

                std::cout << "[kv_client] "
                          << (changed ? "refreshed" : "revalidated")
                          << " KV write target to " << current_.host << ":" << current_.port
                          << "\n";
                return true;
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
