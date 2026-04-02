#pragma once
// =============================================================================
// kv_client.h  --  Frontend-side KV store client
// =============================================================================
//
// The frontend does NOT talk to the coordinator on every request.
// It talks to the coordinator ONCE (at startup or on failover) to get the
// tablet map, then caches it and goes directly to storage nodes.
//
// This keeps the coordinator off the critical path -- exactly what Prof. Phan's
// slide 28 shows.
//
// Connection pooling: each storage node gets a pool of persistent TCP
// connections. Reusing connections avoids TCP handshake overhead on every
// GET/PUT call.
// =============================================================================

#include <string>
#include <vector>
#include <mutex>
#include <queue>
#include <memory>
#include <stdexcept>
#include <iostream>
#include <unordered_map>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <chrono>
using namespace std;

// Copy protocol.h from kvstore (same binary-safe protocol)
#include "../../kvstore/src/protocol.h"

// ---------------------------------------------------------------------------
// NodeInfo: one backend storage node
// ---------------------------------------------------------------------------
struct NodeInfo {
    string host;
    int         port;
    bool        alive = true;
};

// ---------------------------------------------------------------------------
// ConnectionPool: reusable persistent TCP connections to one node
// ---------------------------------------------------------------------------
class ConnectionPool {
public:
    explicit ConnectionPool(const NodeInfo& node, int pool_size = 4)
        : node_(node) {
        for (int i = 0; i < pool_size; ++i) {
            int fd = connect_to_node();
            if (fd >= 0) conns_.push(fd);
        }
    }

    ~ConnectionPool() {
        lock_guard<mutex> lk(mu_);
        while (!conns_.empty()) {
            ::close(conns_.front());
            conns_.pop();
        }
    }

    // Borrow a connection.  Returns -1 if none available.
    int borrow() {
        lock_guard<mutex> lk(mu_);
        if (conns_.empty()) {
            // Create a new one on demand
            return connect_to_node();
        }
        int fd = conns_.front();
        conns_.pop();
        return fd;
    }

    // Return a connection to the pool.  Pass fd=-1 to discard (broken conn).
    void release(int fd) {
        if (fd < 0) return;
        lock_guard<mutex> lk(mu_);
        conns_.push(fd);
    }

    const NodeInfo& node() const { return node_; }

private:
    NodeInfo         node_;
    queue<int>  conns_;
    mutex       mu_;

    int connect_to_node() {
        struct addrinfo hints{}, *res;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        string port_str = to_string(node_.port);

        if (::getaddrinfo(node_.host.c_str(), port_str.c_str(), &hints, &res) != 0)
            return -1;

        int fd = ::socket(res->ai_family, res->ai_socktype, 0);
        if (fd < 0) { ::freeaddrinfo(res); return -1; }

        if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
            ::close(fd);
            ::freeaddrinfo(res);
            return -1;
        }
        ::freeaddrinfo(res);
        return fd;
    }
};

// ---------------------------------------------------------------------------
// KVClient: the main interface used by handler functions
// ---------------------------------------------------------------------------
class KVClient {
public:
    // Simple single-node constructor (Phase 1)
    KVClient(const string& host, int port)
        : pool_(make_unique<ConnectionPool>(NodeInfo{host, port})) {}

    // ---- Core operations (same semantics as tablet.h) ----------------------

    bool put(const string& row,
             const string& col,
             const string& val) {
        return exec([&](int fd) { if (!send_put(fd, row, col, val)) return false; auto resp = read_response(fd); return resp.ok; });
    }

    bool get(const string& row,
             const string& col,
             string& val_out) {
        return exec([&](int fd) {
            if (!send_get(fd, row, col)) return false;
            auto resp = read_response(fd);
            if (resp.ok) { val_out = resp.value; return true; }
            return false;
        });
    }

    bool cput(const string& row,
              const string& col,
              const string& v1,
              const string& v2) {
        return exec([&](int fd) { if (!send_cput(fd, row, col, v1, v2)) return false; auto resp = read_response(fd); return resp.ok; });
    }

    bool del(const string& row, const string& col) {
        return exec([&](int fd) { if (!send_delete(fd, row, col)) return false; auto resp = read_response(fd); return resp.ok; });
    }

    // Convenience: get and parse as string (returns empty on missing)
    string get_str(const string& row, const string& col) {
        string val;
        get(row, col, val);
        return val;
    }

    // Ping the node -- used by coordinator heartbeat and admin console
    bool ping(uint64_t& lsn_out) {
        return exec([&](int fd) {
            if (!write_all(fd, "PING\r\n")) return false;
            auto resp = read_response(fd);
            if (resp.ok) {
                // "+OK LSN=42" -- parse LSN
                auto p = resp.value.find("LSN=");
                if (p == string::npos) {
                    // value is the line after "+OK " -- parse from error field
                }
                // For PING, value is empty; LSN is in the line itself
                // Re-read line approach: handled in server.cc PING handler
                lsn_out = 0;
                return true;
            }
            return false;
        });
    }

private:
    unique_ptr<ConnectionPool> pool_;

    // Execute an operation with automatic connection retry on failure
    bool exec(function<bool(int)> op) {
        for (int attempt = 0; attempt < 3; ++attempt) {
            int fd = pool_->borrow();
            if (fd < 0) {
                this_thread::sleep_for(chrono::milliseconds(50));
                continue;
            }
            bool ok = op(fd);
            if (ok) {
                pool_->release(fd);
                return true;
            }
            // Connection broken -- close and retry with new connection
            ::close(fd);
            pool_->release(-1);
        }
        return false;
    }
};
