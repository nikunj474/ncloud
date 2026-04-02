// server.h  --  PennCloud KV TCP server
// Architecture:
//   - One accept() loop on the main thread
//   - Thread pool handles connections (configurable size, default 16)
//   - Each connection is persistent (client sends multiple requests)
//   - One Tablet per server for now (Phase 1)
//     In Phase 2 (partitioning), the coordinator assigns tablet ranges
//     and each server may hold multiple tablets
//
// Request dispatch:
//   read_line() -> parse op + lengths -> read_exact() raw bytes ->
//   call Tablet method -> write response
//
// The server never closes a connection -- the client does.
// A dropped connection simply ends the handler thread's loop.


#include <string>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <memory>
#include "tablet.h"
using namespace std;

// ThreadPool: fixed-size pool, unbounded task queue

class ThreadPool {
public:
    explicit ThreadPool(int n_threads);
    ~ThreadPool();
    void enqueue(function<void()> task);

private:
    vector<thread>          workers_;
    queue<function<void()>> tasks_;
    mutex                        mu_;
    condition_variable           cv_;
    bool                              stop_ = false;
};


// KVServer

class KVServer {
public:
    struct Config {
        int         port          = 5000;
        int         threads       = 16;
        string data_dir      = "./data";
        string tablet_name   = "tablet0";
        int         ckpt_interval = 300;   // seconds between auto-checkpoints
    };

    explicit KVServer(const Config& cfg);
    ~KVServer();

    // Start listening and serving.  Blocks until stop() is called.
    void run();

    // Signal the server to stop accepting new connections.
    void stop();

    // Expose tablet for replication layer (Phase 2)
    Tablet& tablet() { return *tablet_; }

private:
    Config                  cfg_;
    unique_ptr<Tablet> tablet_;
    unique_ptr<ThreadPool> pool_;
    int                     listen_fd_ = -1;
    atomic<bool>       running_{false};
    thread             ckpt_thread_;

    // Handle one persistent client connection
    void handle_connection(int client_fd);

    // Process a single request on an open connection.
    // Returns false when connection should be closed.
    bool handle_request(int client_fd);

    // Auto-checkpoint background thread
    void checkpoint_loop();

    int create_listen_socket();
};
