// server.cc  --  KV server implementation
#include "server.h"
#include "protocol.h"
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <signal.h>
using namespace std;
// ThreadPool

ThreadPool::ThreadPool(int n) {
    workers_.reserve(n);
    for (int i = 0; i < n; ++i) {
        workers_.emplace_back([this] {
            while (true) {
                function<void()> task;
                {
                    unique_lock<mutex> lk(mu_);
                    cv_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
                    if (stop_ && tasks_.empty()) return;
                    task = move(tasks_.front());
                    tasks_.pop();
                }
                task();
            }
        });
    }
}

// Stop all threads and clean up
ThreadPool::~ThreadPool() {
    {
        lock_guard<mutex> lk(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& w : workers_) w.join();
}

void ThreadPool::enqueue(function<void()> task) {
    {
        lock_guard<mutex> lk(mu_);
        if (stop_) throw runtime_error("ThreadPool stopped");
        tasks_.push(move(task));
    }
    cv_.notify_one();
}

// KVServer constructor / destructor
KVServer::KVServer(const Config& cfg)
    : cfg_(cfg) {
    tablet_ = make_unique<Tablet>(cfg_.tablet_name, cfg_.data_dir);

    // For Phase 2, initialize the ReplicationManager here,
    // passing it the tablet and config.
    ReplicationManager::Config rcfg;
    rcfg.node_id    = cfg_.node_id;
    rcfg.repl_port  = cfg_.repl_port;

    repl_   = make_unique<ReplicationManager>(rcfg, *tablet_);
     
    // for testing: set replication mode before shutdown
    repl_->set_primary(cfg_.is_primary);
    pool_   = make_unique<ThreadPool>(cfg_.threads);
}

KVServer::~KVServer() {
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

// create_listen_socket: SO_REUSEADDR, non-blocking accept loop
// Returns the listening socket fd, or throws on error.

int KVServer::create_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw runtime_error("socket(): " + string(strerror(errno)));

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(cfg_.port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw runtime_error("bind(): " + string(strerror(errno)));

    if (::listen(fd, 128) < 0)
        throw runtime_error("listen(): " + string(strerror(errno)));

    return fd;
}

// run()
void KVServer::run() {
    // Ignore SIGPIPE so writes to closed sockets return -1 instead of crashing
    ::signal(SIGPIPE, SIG_IGN);

    // Recover state from disk before accepting any connections
    cout << "[kvserver] recovering tablet...\n";
    tablet_->recover();
    cout << "[kvserver] tablet ready: " << tablet_->row_count()
              << " rows, LSN=" << tablet_->lsn() << "\n";

    repl_->start();
    for (const auto& spec : cfg_.replica_specs) {
        size_t p1 = spec.find(':');
        size_t p2 = spec.find(':', p1 == string::npos ? p1 : p1 + 1);
        if (p1 == string::npos || p2 == string::npos) {
            cerr << "[kvserver] bad replica spec: " << spec << "\n";
            continue;
        }
        string id   = spec.substr(0, p1);
        string host = spec.substr(p1 + 1, p2 - p1 - 1);
        int port    = stoi(spec.substr(p2 + 1));
        repl_->add_replica(id, host, port);
    }

    listen_fd_ = create_listen_socket();
    running_   = true;

    // Start auto-checkpoint background thread
    ckpt_thread_ = thread(&KVServer::checkpoint_loop, this);

    cout << "[kvserver] listening on port " << cfg_.port
              << " with " << cfg_.threads << " threads\n";

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);
        int cfd = ::accept(listen_fd_,
                           reinterpret_cast<sockaddr*>(&client_addr),
                           &client_len);
        if (cfd < 0) {
            if (running_) {
                cerr << "[kvserver] accept error: " << strerror(errno) << "\n";
            }
            continue;
        }

        char ip[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));

        // Dispatch to thread pool -- connection is handled asynchronously
        pool_->enqueue([this, cfd, ip_str = string(ip)] {
            handle_connection(cfd);
        });
    }
}

void KVServer::stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (repl_) repl_->stop();
    if (ckpt_thread_.joinable()) ckpt_thread_.join();
}

// handle_connection: persistent connection loop
void KVServer::handle_connection(int fd) {
    // Set receive timeout so dead connections don't park threads forever
    struct timeval tv { .tv_sec = 60, .tv_usec = 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    while (handle_request(fd)) { /* loop until connection closed or error */ }
    ::close(fd);
}

// handle_request: parse one request, dispatch, respond
// Returns false to close the connection.
bool KVServer::handle_request(int fd) {
    string line;
    if (!read_line(fd, line)) return false;  // connection closed or timeout
    if (line.empty())         return true;   // keep-alive ping

    // Parse: <OP> <len1> [<len2> [<len3> [<len4>]]]
    istringstream ss(line);
    string op;
    ss >> op;

    auto send = [&](const string& resp) -> bool {
        return write_all(fd, resp);
    };

    if (op == "PUT") {
        if (!repl_->is_primary())
            return send(err_response("not primary"));

        uint32_t rowlen, collen, vallen;
        if (!(ss >> rowlen >> collen >> vallen))
            return send(err_response("malformed PUT header"));

        string row, col, val;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen) ||
            !read_exact(fd, val, vallen))
            return false;

        bool ok = repl_->replicated_put(row, col, val);
        return send(ok ? ok_response() : err_response("PUT failed"));
    }


    if (op == "GET") {
        if (!repl_->is_primary())
            return send(err_response("not primary"));

        uint32_t rowlen, collen;
        if (!(ss >> rowlen >> collen))
            return send(err_response("malformed GET header"));

        string row, col;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen))
            return false;

        string val;
        if (tablet_->get(row, col, val))
            return send(ok_value_response(val));
        else
            return send(err_response("not found"));
    }


    if (op == "CPUT") {
        if (!repl_->is_primary())
            return send(err_response("not primary"));

        uint32_t rowlen, collen, v1len, v2len;
        if (!(ss >> rowlen >> collen >> v1len >> v2len))
            return send(err_response("malformed CPUT header"));

        string row, col, v1, v2;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen) ||
            !read_exact(fd, v1,  v1len)  ||
            !read_exact(fd, v2,  v2len))
            return false;

        bool ok = repl_->replicated_cput(row, col, v1, v2);
        return send(ok ? ok_response() : err_response("value mismatch"));
    }


    if (op == "DELETE") {
        if (!repl_->is_primary())
            return send(err_response("not primary"));

        uint32_t rowlen, collen;
        if (!(ss >> rowlen >> collen))
            return send(err_response("malformed DELETE header"));

        string row, col;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen))
            return false;

        bool ok = repl_->replicated_delete(row, col);
        return send(ok ? ok_response() : err_response("DELETE failed"));
    }

    if (op == "BECOME_PRIMARY") {
        repl_->set_primary(true);
        return send(ok_response());
    }

    if (op == "PING") {
        string role = repl_->is_primary() ? "primary" : "secondary";
        return send("+OK LSN=" + to_string(tablet_->lsn())
                    + " ROLE=" + role + "\r\n");
    }


    if (op == "STATS") {
        string body = "rows="  + to_string(tablet_->row_count()) + " "
                         + "ops="   + to_string(tablet_->op_count())  + " "
                         + "lsn="   + to_string(tablet_->lsn())       + "\r\n";
        return send("+OK " + to_string(body.size()) + "\r\n" + body);
    }


    if (op == "CHECKPOINT") {
        bool ok = tablet_->checkpoint();
        return send(ok ? ok_response() : err_response("checkpoint failed"));
    }

    return send(err_response("unknown op: " + op));
}


// checkpoint_loop: auto-checkpoint every cfg_.ckpt_interval seconds

void KVServer::checkpoint_loop() {
    while (running_) {
        // Sleep in 1-second increments so we respond to stop() promptly
        for (int i = 0; i < cfg_.ckpt_interval && running_; ++i)
            this_thread::sleep_for(chrono::seconds(1));

        if (!running_) break;
        cout << "[kvserver] auto-checkpoint starting...\n";
        if (tablet_->checkpoint())
            cout << "[kvserver] auto-checkpoint done\n";
        else
            cerr << "[kvserver] auto-checkpoint FAILED\n";
    }
}
