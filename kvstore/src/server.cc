// server.cc  --  KV server implementation


#include "server.h"
#include "protocol.h"
#include "replication.h"
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
#include <algorithm>
#include <chrono>

namespace {
bool row_in_range(const std::string& row,
                  const std::string& start,
                  const std::string& end) {
    if (!start.empty() && row < start) return false;
    if (!end.empty() && row >= end) return false;  // exclusive end, matches coordinator
    return true;
}

std::string kv_json_str(const std::string& s) {
    std::string out = "\"";
    static const char hex[] = "0123456789abcdef";
    for (unsigned char c : s) {
        if (c == '"') out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c == '\t') out += "\\t";
        else if (c < 0x20) {
            out += "\\u00";
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0f]);
        } else {
            out.push_back(static_cast<char>(c));
        }
    }
    out += "\"";
    return out;
}
}

ThreadPool::ThreadPool(int n) {
    workers_.reserve(n);
    for (int i = 0; i < n; ++i) {
        workers_.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lk(mu_);
                    cv_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
                    if (stop_ && tasks_.empty()) return;
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
                task();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& w : workers_) w.join();
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (stop_) throw std::runtime_error("ThreadPool stopped");
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
}

KVServer::KVServer(const Config& cfg)
    : cfg_(cfg) {
    pool_ = std::make_unique<ThreadPool>(cfg_.threads);

    std::vector<HostedTabletConfig> specs = cfg_.hosted_tablets;
    if (specs.empty()) {
        HostedTabletConfig ht;
        ht.name = cfg_.tablet_name;
        ht.row_start = "";
        ht.row_end = "";
        ht.data_dir = cfg_.data_dir;
        specs.push_back(ht);
    }

    const bool wants_replication = cfg_.enable_replication || !cfg_.node_id.empty() || !cfg_.replicas.empty();

    hosted_.reserve(specs.size());
    for (const auto& spec : specs) {
        HostedTablet hosted;
        hosted.cfg = spec;
        hosted.tablet = std::make_unique<Tablet>(spec.name, spec.data_dir.empty() ? cfg_.data_dir : spec.data_dir);

        if (wants_replication) {
            ReplicationManager::Config rcfg;
            rcfg.node_id = cfg_.node_id.empty() ? "node" : cfg_.node_id;
            rcfg.tablet_name = spec.name;
            rcfg.repl_port = cfg_.repl_port;
            hosted.repl = std::make_unique<ReplicationManager>(rcfg, *hosted.tablet);
            if (cfg_.replicas.empty()) {
                hosted.repl->demote_to_secondary(); // coordinator will promote exact tablet primaries
            } else {
                for (const auto& [id, host, port] : cfg_.replicas) {
                    hosted.repl->add_replica(id, host, port);
                }
                hosted.repl->promote_to_primary();
            }
        }

        hosted_.push_back(std::move(hosted));
    }
}

KVServer::~KVServer() {
    if (listen_fd_ >= 0) ::close(listen_fd_);
    if (repl_listen_fd_ >= 0) ::close(repl_listen_fd_);
}

KVServer::HostedTablet* KVServer::find_hosted_for_row(const std::string& row) {
    for (auto& hosted : hosted_) {
        if (row_in_range(row, hosted.cfg.row_start, hosted.cfg.row_end)) return &hosted;
    }
    return nullptr;
}

const KVServer::HostedTablet* KVServer::find_hosted_for_row(const std::string& row) const {
    for (const auto& hosted : hosted_) {
        if (row_in_range(row, hosted.cfg.row_start, hosted.cfg.row_end)) return &hosted;
    }
    return nullptr;
}

KVServer::HostedTablet* KVServer::find_hosted_by_name(const std::string& name) {
    for (auto& hosted : hosted_) {
        if (hosted.cfg.name == name) return &hosted;
    }
    return nullptr;
}

const KVServer::HostedTablet* KVServer::find_hosted_by_name(const std::string& name) const {
    for (const auto& hosted : hosted_) {
        if (hosted.cfg.name == name) return &hosted;
    }
    return nullptr;
}

int KVServer::create_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket(): " + std::string(strerror(errno)));

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(cfg_.port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("bind(): " + std::string(strerror(errno)));

    if (::listen(fd, 128) < 0)
        throw std::runtime_error("listen(): " + std::string(strerror(errno)));

    return fd;
}

int KVServer::create_repl_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("repl socket(): " + std::string(strerror(errno)));

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(cfg_.repl_port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("repl bind(): " + std::string(strerror(errno)));

    if (::listen(fd, 128) < 0)
        throw std::runtime_error("repl listen(): " + std::string(strerror(errno)));

    return fd;
}

void KVServer::run() {
    ::signal(SIGPIPE, SIG_IGN);

    std::cout << "[kvserver] recovering hosted tablets...\n";
    for (auto& hosted : hosted_) {
        if (!hosted.tablet->recover()) {
            throw std::runtime_error("failed to recover tablet " + hosted.cfg.name);
        }
        std::cout << "[kvserver] tablet ready: " << hosted.cfg.name
                  << " range=[" << hosted.cfg.row_start << "," << hosted.cfg.row_end << "]"
                  << " rows=" << hosted.tablet->row_count()
                  << " LSN=" << hosted.tablet->lsn() << "\n";
    }

    for (auto& hosted : hosted_) {
        if (hosted.repl) hosted.repl->start();
    }

    listen_fd_ = create_listen_socket();
    bool any_repl = false;
    for (const auto& hosted : hosted_) if (hosted.repl) { any_repl = true; break; }
    if (any_repl) {
        repl_listen_fd_ = create_repl_listen_socket();
    }
    running_ = true;

    ckpt_thread_ = std::thread(&KVServer::checkpoint_loop, this);
    if (repl_listen_fd_ >= 0) repl_accept_thread_ = std::thread(&KVServer::repl_accept_loop, this);

    std::cout << "[kvserver] listening on port " << cfg_.port
              << " with " << cfg_.threads << " threads\n";
    if (repl_listen_fd_ >= 0) {
        std::cout << "[kvserver] replication control listening on port " << cfg_.repl_port << "\n";
    }

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);
        int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (cfd < 0) {
            if (running_) {
                std::cerr << "[kvserver] accept error: " << strerror(errno) << "\n";
            }
            continue;
        }

        pool_->enqueue([this, cfd] { handle_connection(cfd); });
    }
}

void KVServer::stop() {
    std::cout << "[kvserver] graceful shutdown triggered\n" << std::flush;
    running_ = false;
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (repl_listen_fd_ >= 0) {
        ::shutdown(repl_listen_fd_, SHUT_RDWR);
        ::close(repl_listen_fd_);
        repl_listen_fd_ = -1;
    }
    if (ckpt_thread_.joinable()) ckpt_thread_.join();
    if (repl_accept_thread_.joinable()) repl_accept_thread_.join();
    for (auto& hosted : hosted_) {
        if (hosted.repl) hosted.repl->stop();
    }
}

bool KVServer::checkpoint_all() {
    bool ok = true;
    for (auto& hosted : hosted_) {
        ok = hosted.tablet->checkpoint() && ok;
    }
    return ok;
}

void KVServer::handle_connection(int fd) {
    struct timeval tv { .tv_sec = 5, .tv_usec = 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    while (running_ && handle_request(fd)) {}
    ::close(fd);
}

bool KVServer::handle_request(int fd) {
    std::string line;
    if (!read_line(fd, line)) return false;
    if (line.empty())         return true;

    std::istringstream ss(line);
    std::string op;
    ss >> op;

    auto send = [&](const std::string& resp) -> bool {
        return write_all(fd, resp);
    };

    if (op == "PUT") {
        uint32_t rowlen, collen, vallen;
        if (!(ss >> rowlen >> collen >> vallen))
            return send(err_response("malformed PUT header"));

        std::string row, col, val;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen) ||
            !read_exact(fd, val, vallen))
            return false;

        HostedTablet* hosted = find_hosted_for_row(row);
        if (!hosted) return send(err_response("row not hosted on this server"));

        if (hosted->repl) {
            KVPutStatus st = hosted->repl->replicated_put(row, col, val);
            if (st == KVPutStatus::OK) return send(ok_response());
            if (st == KVPutStatus::QuorumUnavailable) return send(err_response("no quorum"));
            return send(err_response("PUT failed"));
        }
        return send(hosted->tablet->put(row, col, val) ? ok_response() : err_response("PUT failed"));
    }

    if (op == "GET") {
        uint32_t rowlen, collen;
        if (!(ss >> rowlen >> collen))
            return send(err_response("malformed GET header"));

        std::string row, col;
        if (!read_exact(fd, row, rowlen) || !read_exact(fd, col, collen))
            return false;

        HostedTablet* hosted = find_hosted_for_row(row);
        if (!hosted) return send(err_response("row not hosted on this server"));

        std::string val;
        if (hosted->tablet->get(row, col, val))
            return send(ok_value_response(val));
        return send(err_response("not found"));
    }

    if (op == "CPUT") {
        uint32_t rowlen, collen, v1len, v2len;
        if (!(ss >> rowlen >> collen >> v1len >> v2len))
            return send(err_response("malformed CPUT header"));

        std::string row, col, v1, v2;
        if (!read_exact(fd, row, rowlen) ||
            !read_exact(fd, col, collen) ||
            !read_exact(fd, v1,  v1len)  ||
            !read_exact(fd, v2,  v2len))
            return false;

        HostedTablet* hosted = find_hosted_for_row(row);
        if (!hosted) return send(err_response("row not hosted on this server"));

        if (hosted->repl) {
            KVPutStatus st = hosted->repl->replicated_cput(row, col, v1, v2);
            if (st == KVPutStatus::OK) return send(ok_response());
            if (st == KVPutStatus::QuorumUnavailable) return send(err_response("no quorum"));
            return send(err_response("value mismatch"));
        }
        return send(hosted->tablet->cput(row, col, v1, v2) ? ok_response() : err_response("value mismatch"));
    }

    if (op == "DELETE") {
        uint32_t rowlen, collen;
        if (!(ss >> rowlen >> collen))
            return send(err_response("malformed DELETE header"));

        std::string row, col;
        if (!read_exact(fd, row, rowlen) || !read_exact(fd, col, collen))
            return false;

        HostedTablet* hosted = find_hosted_for_row(row);
        if (!hosted) return send(err_response("row not hosted on this server"));

        if (hosted->repl) {
            KVPutStatus st = hosted->repl->replicated_delete(row, col);
            if (st == KVPutStatus::OK) return send(ok_response());
            if (st == KVPutStatus::QuorumUnavailable) return send(err_response("no quorum"));
            return send(err_response("DELETE failed"));
        }
        return send(hosted->tablet->del(row, col) ? ok_response() : err_response("DELETE failed"));
    }

    if (op == "PING") {
        uint64_t max_lsn = 0;
        std::string tablet_lsns;
        for (const auto& hosted : hosted_) max_lsn = std::max(max_lsn, hosted.tablet->lsn());
        for (const auto& hosted : hosted_) {
            if (!tablet_lsns.empty()) tablet_lsns += ",";
            tablet_lsns += hosted.cfg.name + ":" + std::to_string(hosted.tablet->lsn());
        }
        return send("+OK LSN=" + std::to_string(max_lsn) +
                    " PID=" + std::to_string(static_cast<long long>(::getpid())) +
                    " TABLETS=" + std::to_string(hosted_.size()) +
                    " TABLET_LSNS=" + tablet_lsns + "\r\n");
    }

    if (op == "STATS") {
        size_t total_rows = 0;
        uint64_t total_ops = 0;
        uint64_t max_lsn = 0;
        for (const auto& hosted : hosted_) {
            total_rows += hosted.tablet->row_count();
            total_ops  += hosted.tablet->op_count();
            max_lsn = std::max(max_lsn, hosted.tablet->lsn());
        }
        std::string body = "rows="  + std::to_string(total_rows) + " "
                         + "ops="   + std::to_string(total_ops)  + " "
                         + "lsn="   + std::to_string(max_lsn)    + " "
                         + "tablets=" + std::to_string(hosted_.size()) + "\r\n";
        return send("+OK " + std::to_string(body.size()) + "\r\n" + body);
    }

    if (op == "DUMP") {
        size_t limit = 10;
        size_t offset = 0;
        ss >> limit >> offset;
        if (limit == 0) limit = 10;
        limit = std::min<size_t>(limit, 200);

        std::vector<TabletDumpCell> cells;
        for (const auto& hosted : hosted_) {
            auto tablet_cells = hosted.tablet->dump_cells();
            cells.insert(cells.end(),
                         std::make_move_iterator(tablet_cells.begin()),
                         std::make_move_iterator(tablet_cells.end()));
        }
        std::sort(cells.begin(), cells.end(), [](const TabletDumpCell& a, const TabletDumpCell& b) {
            if (a.tablet != b.tablet) return a.tablet < b.tablet;
            if (a.row != b.row) return a.row < b.row;
            return a.col < b.col;
        });

        const size_t total = cells.size();
        const size_t start = std::min(offset, total);
        const size_t end = std::min(total, start + limit);
        const bool has_more = end < total;

        std::string body = "{\"total\":" + std::to_string(total) +
                           ",\"offset\":" + std::to_string(start) +
                           ",\"limit\":" + std::to_string(limit) +
                           ",\"next_offset\":" + std::to_string(end) +
                           ",\"has_more\":" + std::string(has_more ? "true" : "false") +
                           ",\"cells\":[";
        bool first = true;
        for (size_t i = start; i < end; ++i) {
            const auto& c = cells[i];
            if (!first) body += ",";
            first = false;
            body += "{";
            body += "\"tablet\":" + kv_json_str(c.tablet);
            body += ",\"row\":" + kv_json_str(c.row);
            body += ",\"column\":" + kv_json_str(c.col);
            body += ",\"value_size\":" + std::to_string(c.value_size);
            body += ",\"preview\":" + kv_json_str(c.value_preview);
            body += ",\"preview_format\":" + kv_json_str(c.value_is_text ? "text" : "hex");
            body += ",\"truncated\":" + std::string(c.truncated ? "true" : "false");
            body += "}";
        }
        body += "]}";
        return send("+OK " + std::to_string(body.size()) + "\r\n" + body);
    }

    if (op == "CHECKPOINT") {
        bool ok = checkpoint_all();
        return send(ok ? ok_response() : err_response("checkpoint failed"));
    }

    return send(err_response("unknown op: " + op));
}

void KVServer::repl_accept_loop() {
    while (running_) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int cfd = ::accept(repl_listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        if (cfd < 0) {
            if (!running_) break;
            continue;
        }
        std::thread([this, cfd] {
            handle_repl_connection(cfd);
            ::close(cfd);
        }).detach();
    }
}

void KVServer::handle_repl_connection(int fd) {
    struct timeval tv { .tv_sec = 5, .tv_usec = 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::string line;
    while (running_) {
        if (!read_line(fd, line)) return;
        if (line.empty()) continue;

        std::istringstream ss(line);
        std::string cmd;
        ss >> cmd;

        auto send = [&](const std::string& resp) -> bool {
            return write_all(fd, resp);
        };

        if (cmd == "REPLICATE") {
            std::string tablet_name;
            ss >> tablet_name;
            if (tablet_name.empty()) { send("-ERR missing tablet\r\n"); continue; }

            uint64_t lsn = 0;
            std::string op;
            ss >> lsn >> op;

            HostedTablet* hosted = find_hosted_by_name(tablet_name);
            if (!hosted && hosted_.size() == 1) hosted = &hosted_.front();
            if (!hosted || !hosted->repl) {
                send("-ERR tablet not hosted\r\n");
                continue;
            }

            if (op == "PUT") {
                uint32_t rlen = 0, clen = 0, vlen = 0;
                ss >> rlen >> clen >> vlen;
                std::string row, col, val;
                if (!read_exact(fd, row, rlen) ||
                    !read_exact(fd, col, clen) ||
                    !read_exact(fd, val, vlen)) return;
                bool ok = hosted->repl->apply_replicate_put(lsn, row, col, val);
                if (!ok) {
    send("-ERR replicate put failed\r\n");
    return;
}
                send("+OK LSN=" + std::to_string(hosted->tablet->lsn()) + "\r\n");
            } else if (op == "DELETE") {
                uint32_t rlen = 0, clen = 0;
                ss >> rlen >> clen;
                std::string row, col;
                if (!read_exact(fd, row, rlen) ||
                    !read_exact(fd, col, clen)) return;
                bool ok = hosted->repl->apply_replicate_delete(lsn, row, col);
                if (!ok) {
    send("-ERR replicate delete failed\r\n");
    return;
}
                send("+OK LSN=" + std::to_string(hosted->tablet->lsn()) + "\r\n");
            } else {
                send("-ERR unknown replicate op\r\n");
            }
        } else if (cmd == "SYNC_FROM") {
            std::string tablet_name;
            uint64_t requester_ckpt_ver = 0;
            uint64_t requester_lsn = 0;
            ss >> tablet_name >> requester_ckpt_ver >> requester_lsn;

            HostedTablet* hosted = find_hosted_by_name(tablet_name);
            if (!hosted && hosted_.size() == 1) hosted = &hosted_.front();
            if (!hosted || !hosted->repl) {
                send("-ERR tablet not hosted\r\n");
                continue;
            }

            std::string resp = hosted->repl->build_sync_response(requester_ckpt_ver, requester_lsn);
            if (!write_all(fd, resp)) return;
        } else if (cmd == "BECOME_PRIMARY") {
            std::string tablet_name;
            ss >> tablet_name;
            // Try by name; fall back to the sole hosted tablet when name doesn't
            // match (coordinator tablet name may differ from the node's --tablet arg).
            HostedTablet* hosted = find_hosted_by_name(tablet_name);
            if (!hosted && hosted_.size() == 1) hosted = &hosted_.front();
            if (!hosted || !hosted->repl) {
                send("-ERR tablet not hosted\r\n");
                continue;
            }
            hosted->repl->clear_replicas();
            hosted->repl->promote_to_primary();
            send("+OK\r\n");
        } else if (cmd == "BECOME_SECONDARY") {
            std::string tablet_name, host;
            int port = 0;
            ss >> tablet_name >> host >> port;
            HostedTablet* hosted = find_hosted_by_name(tablet_name);
            if (!hosted && hosted_.size() == 1) hosted = &hosted_.front();
            if (!hosted || !hosted->repl) {
                send("-ERR tablet not hosted\r\n");
                continue;
            }
            bool ok = hosted->repl->become_secondary_and_sync(host, port);
            send(ok ? std::string("+OK\r\n")
                    : std::string("-ERR sync failed\r\n"));
        } else if (cmd == "ADD_REPLICA") {
            std::string tablet_name, id, host;
            int port = 0;
            ss >> tablet_name >> id >> host >> port;
            HostedTablet* hosted = find_hosted_by_name(tablet_name);
            if (!hosted && hosted_.size() == 1) hosted = &hosted_.front();
            if (!hosted || !hosted->repl) {
                send("-ERR tablet not hosted\r\n");
                continue;
            }
            if (id.empty() || host.empty() || port <= 0) {
                send("-ERR malformed ADD_REPLICA\r\n");
            } else {
                hosted->repl->add_replica(id, host, port);
                send("+OK\r\n");
            }
        } else {
            send("-ERR unknown replication command\r\n");
        }
    }
}

void KVServer::checkpoint_loop() {
    while (running_) {
        for (int i = 0; i < cfg_.ckpt_interval * 10 && running_; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

        if (!running_) break;
        std::cout << "[kvserver] auto-checkpoint starting...\n";
        if (checkpoint_all())
            std::cout << "[kvserver] auto-checkpoint done\n";
        else
            std::cerr << "[kvserver] auto-checkpoint FAILED\n";
    }
}
