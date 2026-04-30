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
    explicit ThreadPool(int n_threads);
    ~ThreadPool();
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

    explicit KVServer(const Config& cfg);
    ~KVServer();

    void run();
    void stop();

    // Backward-compatible helper for older code paths.
    Tablet& tablet() { return *hosted_.front().tablet; }

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

    HostedTablet* find_hosted_for_row(const std::string& row);
    const HostedTablet* find_hosted_for_row(const std::string& row) const;
    HostedTablet* find_hosted_by_name(const std::string& name);
    const HostedTablet* find_hosted_by_name(const std::string& name) const;

    void handle_connection(int client_fd);
    bool handle_request(int client_fd);
    void checkpoint_loop();
    int create_listen_socket();
    int create_repl_listen_socket();

    void repl_accept_loop();
    void handle_repl_connection(int fd);
};

#endif  // PENNCLOUD_KV_SERVER_H
