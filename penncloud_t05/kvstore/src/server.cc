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
    auto specs = build_tablet_specs();
    tablets_.reserve(specs.size());
    for (size_t i = 0; i < specs.size(); ++i) {
        const auto& spec = specs[i];

        TabletInfo ti;
        ti.name = spec.name;
        ti.row_start = spec.row_start;
        ti.row_end = spec.row_end;
        ti.tablet = make_unique<Tablet>(spec.name, cfg_.data_dir);

        ReplicationManager::Config rcfg;
        rcfg.node_id = cfg_.node_id + ":" + spec.name;
        rcfg.repl_port = cfg_.repl_port + static_cast<int>(i);

        ti.repl = make_unique<ReplicationManager>(rcfg, *ti.tablet);
        ti.repl->set_primary(cfg_.is_primary);
        tablets_.push_back(move(ti));
    }

    pool_ = make_unique<ThreadPool>(cfg_.threads);
}

KVServer::~KVServer() {
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

bool KVServer::checkpoint_all() {
    bool ok = true;
    for (auto& t : tablets_) {
        ok = t.tablet->checkpoint() && ok;
    }
    return ok;
}

vector<KVServer::TabletSpec> KVServer::build_tablet_specs() const {
    vector<TabletSpec> specs;
    if (cfg_.tablet_specs.empty()) {
        specs.push_back(TabletSpec{cfg_.tablet_name, "", ""});
        return specs;
    }

    for (const auto& raw : cfg_.tablet_specs) {
        size_t p1 = raw.find(':');
        size_t p2 = raw.find(':', p1 == string::npos ? p1 : p1 + 1);
        if (p1 == string::npos || p2 == string::npos) {
            throw runtime_error("bad tablet spec: " + raw);
        }
        specs.push_back(TabletSpec{
            raw.substr(0, p1),
            raw.substr(p1 + 1, p2 - p1 - 1),
            raw.substr(p2 + 1)
        });
    }
    return specs;
}

KVServer::TabletInfo* KVServer::find_tablet_for_row(const string& row) {
    for (auto& t : tablets_) {
        bool after_start = t.row_start.empty() || row >= t.row_start;
        bool before_end  = t.row_end.empty()   || row < t.row_end;
        if (after_start && before_end) return &t;
    }
    return nullptr;
}

KVServer::TabletInfo* KVServer::find_tablet_by_name(const string& name) {
    for (auto& t : tablets_) {
        if (t.name == name) return &t;
    }
    return nullptr;
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
    cout << "[kvserver] recovering " << tablets_.size() << " tablet(s)...\n";
    for (size_t i = 0; i < tablets_.size(); ++i) {
        auto& t = tablets_[i];
        t.tablet->recover();
        cout << "[kvserver] tablet ready: " << t.name
             << " rows=" << t.tablet->row_count()
             << " LSN=" << t.tablet->lsn() << "\n";

        t.repl->start();
        for (const auto& spec : cfg_.replica_specs) {
            size_t p1 = spec.find(':');
            size_t p2 = spec.find(':', p1 == string::npos ? p1 : p1 + 1);
            if (p1 == string::npos || p2 == string::npos) {
                cerr << "[kvserver] bad replica spec: " << spec << "\n";
                continue;
            }
            string id   = spec.substr(0, p1);
            string host = spec.substr(p1 + 1, p2 - p1 - 1);
            int base_port = stoi(spec.substr(p2 + 1));
            t.repl->add_replica(id, host, base_port + static_cast<int>(i));
        }
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
    for (auto& t : tablets_) {
        if (t.repl) t.repl->stop();
    }
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
        uint32_t rowlen, collen, vallen;
        if (!(ss >> rowlen >> collen >> vallen))
            return send(err_response("malformed PUT header"));

        string row, col, val;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen) ||
            !read_exact(fd, val, vallen))
            return false;

        TabletInfo* t = find_tablet_for_row(row);
        if (!t) return send(err_response("no tablet for row"));
        if (!t->repl->is_primary())
            return send(err_response("not primary"));

        bool ok = t->repl->replicated_put(row, col, val);
        return send(ok ? ok_response() : err_response("PUT failed"));
    }


    if (op == "GET") {
        uint32_t rowlen, collen;
        if (!(ss >> rowlen >> collen))
            return send(err_response("malformed GET header"));

        string row, col;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen))
            return false;

        TabletInfo* t = find_tablet_for_row(row);
        if (!t) return send(err_response("no tablet for row"));
        if (!t->repl->is_primary())
            return send(err_response("not primary"));

        string val;
        if (t->tablet->get(row, col, val))
            return send(ok_value_response(val));
        else
            return send(err_response("not found"));
    }


    if (op == "CPUT") {
        uint32_t rowlen, collen, v1len, v2len;
        if (!(ss >> rowlen >> collen >> v1len >> v2len))
            return send(err_response("malformed CPUT header"));

        string row, col, v1, v2;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen) ||
            !read_exact(fd, v1,  v1len)  ||
            !read_exact(fd, v2,  v2len))
            return false;

        TabletInfo* t = find_tablet_for_row(row);
        if (!t) return send(err_response("no tablet for row"));
        if (!t->repl->is_primary())
            return send(err_response("not primary"));

        bool ok = t->repl->replicated_cput(row, col, v1, v2);
        return send(ok ? ok_response() : err_response("value mismatch"));
    }


    if (op == "DELETE") {
        uint32_t rowlen, collen;
        if (!(ss >> rowlen >> collen))
            return send(err_response("malformed DELETE header"));

        string row, col;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen))
            return false;

        TabletInfo* t = find_tablet_for_row(row);
        if (!t) return send(err_response("no tablet for row"));
        if (!t->repl->is_primary())
            return send(err_response("not primary"));

        bool ok = t->repl->replicated_delete(row, col);
        return send(ok ? ok_response() : err_response("DELETE failed"));
    }

    if (op == "BECOME_PRIMARY") {
        string tablet_name;
        ss >> tablet_name;
        TabletInfo* t = tablet_name.empty() ? nullptr : find_tablet_by_name(tablet_name);
        if (!t) return send(err_response("unknown tablet"));
        t->repl->set_primary(true);
        return send(ok_response());
    }

    if (op == "PING") {
        uint64_t max_lsn = 0;
        bool any_primary = false;
        for (const auto& t : tablets_) {
            max_lsn = max(max_lsn, t.tablet->lsn());
            any_primary = any_primary || t.repl->is_primary();
        }
        string role = any_primary ? "primary" : "secondary";
        return send("+OK LSN=" + to_string(max_lsn)
                    + " ROLE=" + role + "\r\n");
    }


    if (op == "STATS") {
        size_t rows = 0;
        uint64_t ops = 0;
        uint64_t max_lsn = 0;
        for (const auto& t : tablets_) {
            rows += t.tablet->row_count();
            ops += t.tablet->op_count();
            max_lsn = max(max_lsn, t.tablet->lsn());
        }
        string body = "tablets=" + to_string(tablets_.size()) + " "
                    + "rows="  + to_string(rows) + " "
                    + "ops="   + to_string(ops)  + " "
                    + "lsn="   + to_string(max_lsn) + "\r\n";
        return send("+OK " + to_string(body.size()) + "\r\n" + body);
    }


    if (op == "CHECKPOINT") {
        bool ok = checkpoint_all();
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
        bool ok = checkpoint_all();
        if (ok)
            cout << "[kvserver] auto-checkpoint done\n";
        else
            cerr << "[kvserver] auto-checkpoint FAILED\n";
    }
}
