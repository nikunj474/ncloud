#pragma once
#ifndef PENNCLOUD_KV_SERVER_H
#define PENNCLOUD_KV_SERVER_H
// =============================================================================
// server.h  --  PennCloud KV TCP server
// =============================================================================

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "tablet.h"

class ReplicationManager;

class ThreadPool {
public:
    // Start n_threads workers that wait for queued connection tasks.
    explicit ThreadPool(int n_threads);
    // Signal workers to exit after the queue drains and join them.
    ~ThreadPool();
    // Add one unit of work, typically an accepted client connection.
    void enqueue(std::function<void()> task);

private:
    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex                        mu_;
    std::condition_variable           cv_;
    bool                              stop_ = false;
};

class KVServer {
public:
    struct HostedTabletConfig {
        std::string name;
        std::string row_start;
        std::string row_end;
        std::string data_dir;
    };

    struct Config {
        int         port          = 5000;
        int         threads       = 16;
        std::string data_dir      = "./data";
        std::string tablet_name   = "tablet0";
        int         ckpt_interval = 300;

        // Multi-tablet hosting. If empty, the server synthesizes one tablet
        // from tablet_name covering the full row range.
        std::vector<HostedTabletConfig> hosted_tablets;

        // Replication knobs.
        bool        enable_replication = false;
        std::string node_id;
        int         repl_port = 5100;
        std::vector<std::tuple<std::string, std::string, int>> replicas;
    };

    // Build hosted tablets, replication managers, and the worker pool.
    explicit KVServer(const Config& cfg);
    // Close listening sockets that may still be open.
    ~KVServer();

    // Recover tablets, start listeners, and serve until stop() is called.
    void run();
    // Stop listeners, join background threads, and stop replication managers.
    void stop();

    // Backward-compatible helper for older code paths.
    Tablet& tablet() { return *hosted_.front().tablet; }

    // Force every hosted tablet to checkpoint its current state.
    bool checkpoint_all();

private:
    struct HostedTablet {
        HostedTabletConfig cfg;
        std::unique_ptr<Tablet> tablet;
        std::unique_ptr<ReplicationManager> repl;
    };

    Config                       cfg_;
    std::vector<HostedTablet>    hosted_;
    std::unique_ptr<ThreadPool>  pool_;
    int                          listen_fd_ = -1;
    int                          repl_listen_fd_ = -1;
    std::atomic<bool>            running_{false};
    std::thread                  ckpt_thread_;
    std::thread                  repl_accept_thread_;

    // Locate the hosted tablet whose row range contains row.
    HostedTablet* find_hosted_for_row(const std::string& row);
    // Const overload for row-to-tablet lookup.
    const HostedTablet* find_hosted_for_row(const std::string& row) const;
    // Locate a hosted tablet by tablet name.
    HostedTablet* find_hosted_by_name(const std::string& name);
    // Const overload for name-to-tablet lookup.
    const HostedTablet* find_hosted_by_name(const std::string& name) const;

    // Serve one client socket until it closes or a request fails.
    void handle_connection(int client_fd);
    // Parse and execute one client command from a connected socket.
    bool handle_request(int client_fd);
    // Periodically checkpoint all hosted tablets.
    void checkpoint_loop();
    // Create the normal client listen socket.
    int create_listen_socket();
    // Create the replication/control listen socket.
    int create_repl_listen_socket();

    // Accept replication/control connections.
    void repl_accept_loop();
    // Handle replication data and role-control commands for one socket.
    void handle_repl_connection(int fd);
};

#endif  // PENNCLOUD_KV_SERVER_H
