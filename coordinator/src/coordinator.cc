
// coordinator.cc PennCloud Backend Coordinator (Phase B recovery/rejoin)
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <stdexcept>
#include <cstring>
#include <csignal>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

enum class NodeRole { PRIMARY, SECONDARY, UNKNOWN };

struct StorageNode {
    std::string id;
    std::string host;
    int         port = -1;      // client/KV port
    int         repl_port = -1; // replication control port
    NodeRole    role   = NodeRole::UNKNOWN;
    bool        alive  = true;
    bool        was_down = false; // true only after a real failure transition
    uint64_t    lsn    = 0;
    std::unordered_map<std::string, uint64_t> tablet_lsns;
    int         process_id = 0;
    int         missed = 0;
    std::chrono::steady_clock::time_point last_seen;
};

struct TabletGroup {
    std::string name;
    std::string row_start;
    std::string row_end;
    std::vector<std::string> node_ids;  // node_ids[0] is primary
};

static std::string json_str_coord(const std::string& s) {
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
            out += hex[c >> 4];
            out += hex[c & 0x0F];
        } else {
            out += static_cast<char>(c);
        }
    }
    out += "\"";
    return out;
}

static bool coord_read_exact(int fd, std::string& out, size_t n) {
    out.assign(n, '\0');
    size_t got = 0;
    while (got < n) {
        ssize_t r = ::recv(fd, &out[got], n - got, 0);
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

class Coordinator {
public:
    struct Config {
        int         port           = 6000;
        std::string config_file    = "./coordinator.conf";
        int         hb_interval_ms = 500;
        int         hb_miss_thresh = 3;
    };

    explicit Coordinator(const Config& cfg) : cfg_(cfg) { load_config(cfg_.config_file); }

    void run();
    void stop() { running_ = false; }

private:
    Config cfg_;
    std::atomic<bool> running_{false};
    std::atomic<bool> roles_configured_{false};

    std::unordered_map<std::string, StorageNode> nodes_;
    mutable std::shared_mutex nodes_mu_;

    std::vector<TabletGroup> tablets_;
    mutable std::shared_mutex tablets_mu_;

    int listen_fd_ = -1;

    void load_config(const std::string& path);
    void heartbeat_loop();
    bool ping_node(StorageNode& node, bool* process_restarted = nullptr);
    bool refresh_node_lsn_now(StorageNode& node);
    void cache_refreshed_node(const StorageNode& node);
    void handle_node_failure(const std::string& dead_id);
    void handle_node_recovery(const std::string& live_id);
    bool promote_node(StorageNode& node, const std::string& tablet_name);
    bool demote_and_sync_node(StorageNode& node, const StorageNode& primary, const std::string& tablet_name);
    bool add_replica_to_primary(StorageNode& primary, const StorageNode& replica, const std::string& tablet_name);
    bool rebuild_primary_replicas(StorageNode& primary,
                                  const std::string& tablet_name,
                                  const std::vector<StorageNode>& followers);
    bool repl_port_ready(const StorageNode& node);
    void configure_initial_tablet_roles();

    void accept_loop();
    void handle_client(int fd);
    std::string handle_lookup(const std::string& row);
    std::string handle_read_lookup(const std::string& row);
    std::string handle_status();
    std::string handle_nodes();
    std::string handle_tablets();

    int create_listen_socket();
    bool read_line(int fd, std::string& line);
    int connect_host_port(const std::string& host, int port);
    int connect_node_kv(const StorageNode& node) { return connect_host_port(node.host, node.port); }
    int connect_node_repl(const StorageNode& node) { return connect_host_port(node.host, node.repl_port > 0 ? node.repl_port : node.port); }

    const TabletGroup* find_tablet(const std::string& row) const;
    void initialize_roles_from_tablets();
    uint64_t node_tablet_lsn(const StorageNode& node, const std::string& tablet_name) const;
    uint64_t tablet_max_known_lsn(const TabletGroup& tg) const;
};

void Coordinator::load_config(const std::string& path) {
    std::ifstream f(path);
    if (!f) {
        std::cout << "[coord] no config file found at " << path << " -- using single-node defaults\n";
        StorageNode n;
        n.id = "node1"; n.host = "127.0.0.1"; n.port = 5000; n.repl_port = 5100; n.role = NodeRole::PRIMARY;
        nodes_[n.id] = n;
        TabletGroup tg;
        tg.name = "tablet0"; tg.row_start = ""; tg.row_end = ""; tg.node_ids = {"node1"};
        tablets_.push_back(tg);
        return;
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::istringstream ss(line);
        std::string type;
        ss >> type;
        if (type == "node") {
            StorageNode n;
            ss >> n.id >> n.host >> n.port;
            if (!(ss >> n.repl_port)) {
                n.repl_port = n.port;
            }
            nodes_[n.id] = n;
            std::cout << "[coord] registered node " << n.id << " at " << n.host << ":" << n.port
                      << " repl=" << n.repl_port << "\n";
        } else if (type == "tablet") {
            TabletGroup tg;
            ss >> tg.name >> tg.row_start >> tg.row_end;
            if (tg.row_start == "-" || tg.row_start == "*") tg.row_start.clear();
            if (tg.row_end == "-" || tg.row_end == "*") tg.row_end.clear();
            std::string nid;
            while (ss >> nid) tg.node_ids.push_back(nid);
            tablets_.push_back(tg);
            std::cout << "[coord] tablet " << tg.name << " [" << tg.row_start << ", " << tg.row_end
                      << ") on " << tg.node_ids.size() << " nodes\n";
        }
    }
    initialize_roles_from_tablets();
}

void Coordinator::initialize_roles_from_tablets() {
    for (auto& [id, node] : nodes_) node.role = NodeRole::UNKNOWN;
    for (const auto& tg : tablets_) {
        for (size_t i = 0; i < tg.node_ids.size(); ++i) {
            auto it = nodes_.find(tg.node_ids[i]);
            if (it == nodes_.end()) continue;
            it->second.role = (i == 0) ? NodeRole::PRIMARY : NodeRole::SECONDARY;
        }
    }
}

uint64_t Coordinator::node_tablet_lsn(const StorageNode& node, const std::string& tablet_name) const {
    auto it = node.tablet_lsns.find(tablet_name);
    return it == node.tablet_lsns.end() ? node.lsn : it->second;
}

uint64_t Coordinator::tablet_max_known_lsn(const TabletGroup& tg) const {
    uint64_t max_lsn = 0;
    for (const auto& node_id : tg.node_ids) {
        auto it = nodes_.find(node_id);
        if (it == nodes_.end()) continue;
        max_lsn = std::max(max_lsn, node_tablet_lsn(it->second, tg.name));
    }
    return max_lsn;
}

static void parse_ping_metadata(StorageNode& node, const std::string& resp) {
    auto lpos = resp.find("LSN=");
    if (lpos != std::string::npos) {
        try { node.lsn = std::stoull(resp.substr(lpos + 4)); } catch (...) {}
    }

    auto tpos = resp.find("TABLET_LSNS=");
    if (tpos != std::string::npos) {
        size_t start = tpos + strlen("TABLET_LSNS=");
        size_t end = resp.find(' ', start);
        std::string encoded = resp.substr(start, end == std::string::npos ? std::string::npos : end - start);
        std::unordered_map<std::string, uint64_t> parsed;
        size_t off = 0;
        while (off < encoded.size()) {
            size_t comma = encoded.find(',', off);
            std::string item = encoded.substr(off, comma == std::string::npos ? std::string::npos : comma - off);
            size_t colon = item.rfind(':');
            if (colon != std::string::npos) {
                try {
                    parsed[item.substr(0, colon)] = std::stoull(item.substr(colon + 1));
                } catch (...) {}
            }
            if (comma == std::string::npos) break;
            off = comma + 1;
        }
        if (!parsed.empty()) node.tablet_lsns = std::move(parsed);
    }

    auto ppos = resp.find("PID=");
    if (ppos != std::string::npos) {
        int pid = 0;
        try { pid = std::stoi(resp.substr(ppos + 4)); } catch (...) { pid = 0; }
        if (pid > 0) node.process_id = pid;
    }
}

void Coordinator::run() {
    ::signal(SIGPIPE, SIG_IGN);
    running_ = true;
    listen_fd_ = create_listen_socket();
    std::cout << "[coord] listening on port " << cfg_.port << "\n";

    // Start heartbeat first so nodes get pinged and marked alive immediately.
    std::thread hb_thread([this] { heartbeat_loop(); });

    // Configure tablet roles in a detached thread so accept_loop() is never
    // blocked -- clients can connect right away regardless of how long setup
    // takes.  We wait until at least one node is confirmed alive (up to 5 s)
    // before sending BECOME_PRIMARY/SECONDARY, reducing the race where RPCs
    // arrive before the KV replication listeners are ready.
    std::thread([this] {
        for (int attempt = 0; attempt < 10 && running_; ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            bool any_alive = false;
            {
                std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
                for (const auto& [id, node] : nodes_)
                    if (node.alive) { any_alive = true; break; }
            }
            if (any_alive) {
                configure_initial_tablet_roles();
                break;
            }
        }
    }).detach();

    accept_loop();
    running_ = false;
    hb_thread.join();
    if (listen_fd_ >= 0) ::close(listen_fd_);
}


void Coordinator::configure_initial_tablet_roles() {
    std::vector<TabletGroup> tgs;
    {
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);
        tgs = tablets_;
    }
    // Retry because KV nodes may be alive on their client port before their
    // replication control listeners are fully ready.  Each round re-snapshots
    // node state so newly-alive nodes are picked up automatically.
    for (int round = 1; round <= 8 && running_; ++round) {
        bool all_ok = true;
        std::unordered_map<std::string, StorageNode> nodes_snapshot;
        {
            std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
            nodes_snapshot = nodes_;
        }
        for (auto& [id, node] : nodes_snapshot) {
            if (node.alive && refresh_node_lsn_now(node)) {
                cache_refreshed_node(node);
            }
        }

        std::vector<std::string> checked_nodes;
        bool repl_ready = true;
        for (const auto& tg : tgs) {
            for (const auto& node_id : tg.node_ids) {
                if (std::find(checked_nodes.begin(), checked_nodes.end(), node_id) != checked_nodes.end()) {
                    continue;
                }
                checked_nodes.push_back(node_id);
                auto it = nodes_snapshot.find(node_id);
                if (it == nodes_snapshot.end() || !it->second.alive) {
                    all_ok = false;
                    continue;
                }
                if (!repl_port_ready(it->second)) {
                    repl_ready = false;
                    all_ok = false;
                }
            }
        }
        if (!repl_ready) {
            std::cout << "[coord] initial role config round " << round
                      << " waiting for replication ports...\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        std::vector<TabletGroup> planned_tgs = tgs;
        for (auto& tg : planned_tgs) {
            if (tg.node_ids.empty()) continue;
            size_t best_idx = tg.node_ids.size();
            uint64_t best_lsn = 0;
            for (size_t i = 0; i < tg.node_ids.size(); ++i) {
                auto it = nodes_snapshot.find(tg.node_ids[i]);
                if (it == nodes_snapshot.end() || !it->second.alive) {
                    all_ok = false;
                    continue;
                }
                uint64_t candidate_lsn = node_tablet_lsn(it->second, tg.name);
                if (best_idx == tg.node_ids.size() || candidate_lsn > best_lsn) {
                    best_idx = i;
                    best_lsn = candidate_lsn;
                }
            }
            if (best_idx == tg.node_ids.size()) continue;
            if (best_idx != 0) {
                std::cout << "[coord] initial primary for " << tg.name
                          << " chosen by tablet LSN: " << tg.node_ids[best_idx]
                          << " LSN=" << best_lsn << "\n";
                std::swap(tg.node_ids[0], tg.node_ids[best_idx]);
            }
        }

        // 1) Promote freshest per-tablet primaries.
        for (const auto& tg : planned_tgs) {
            if (tg.node_ids.empty()) continue;
            auto it = nodes_snapshot.find(tg.node_ids[0]);
            if (it == nodes_snapshot.end() || !it->second.alive) { all_ok = false; continue; }
            StorageNode primary = it->second;
            if (!promote_node(primary, tg.name)) all_ok = false;
        }
        // 1b) Wait for each promoted primary's replication port to be ready
        //     before syncing secondaries to it, preventing a race where a
        //     secondary tries to pull from a primary that hasn't opened its
        //     replication listener yet.
        if (all_ok) {
            for (const auto& tg : planned_tgs) {
                if (tg.node_ids.empty()) continue;
                auto it = nodes_snapshot.find(tg.node_ids[0]);
                if (it == nodes_snapshot.end() || !it->second.alive) continue;
                const StorageNode& primary = it->second;
                for (int attempt = 0; attempt < 20; ++attempt) {
                    if (repl_port_ready(primary)) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                }
            }
        }

        // 2) Sync secondaries and register them under the primary.
        for (const auto& tg : planned_tgs) {
            if (tg.node_ids.empty()) continue;
            auto pit = nodes_snapshot.find(tg.node_ids[0]);
            if (pit == nodes_snapshot.end() || !pit->second.alive) { all_ok = false; continue; }
            StorageNode primary = pit->second;
            for (size_t i = 1; i < tg.node_ids.size(); ++i) {
                auto it = nodes_snapshot.find(tg.node_ids[i]);
                if (it == nodes_snapshot.end() || !it->second.alive) { all_ok = false; continue; }
                StorageNode follower = it->second;
                if (!demote_and_sync_node(follower, primary, tg.name)) { all_ok = false; continue; }
                if (!add_replica_to_primary(primary, follower, tg.name)) all_ok = false;
            }
        }
        if (all_ok) {
            {
                std::unique_lock<std::shared_mutex> tlk(tablets_mu_);
                tablets_ = planned_tgs;
            }
            {
                std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
                for (auto& [id, node] : nodes_) node.role = NodeRole::UNKNOWN;
                for (const auto& tg : planned_tgs) {
                    for (size_t i = 0; i < tg.node_ids.size(); ++i) {
                        auto it = nodes_.find(tg.node_ids[i]);
                        if (it == nodes_.end()) continue;
                        it->second.role = (i == 0) ? NodeRole::PRIMARY : NodeRole::SECONDARY;
                    }
                }
            }
            std::cout << "[coord] initial per-tablet roles configured successfully\n";
            roles_configured_.store(true);
            return;
        }
        std::cout << "[coord] initial role config round " << round << " incomplete, retrying...\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    roles_configured_.store(true);  // mark done even if partial -- unblock waiters
    std::cerr << "[coord] WARNING: initial tablet role configuration did not fully converge\n";
}
void Coordinator::heartbeat_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.hb_interval_ms));
        std::vector<std::string> newly_dead;
        std::vector<std::string> newly_live;

        std::vector<StorageNode> snapshot;
        {
            std::shared_lock<std::shared_mutex> lk(nodes_mu_);
            snapshot.reserve(nodes_.size());
            for (const auto& [id, node] : nodes_) {
                snapshot.push_back(node);
            }
        }

        for (auto node : snapshot) {
            bool eligible_recovery = node.was_down;
            bool process_restarted = false;
            bool just_failed = ping_node(node, &process_restarted);
            bool just_recovered = eligible_recovery && node.alive && !node.was_down;

            {
                std::unique_lock<std::shared_mutex> lk(nodes_mu_);
                auto it = nodes_.find(node.id);
                if (it == nodes_.end()) continue;
                it->second.missed = node.missed;
                it->second.alive = node.alive;
                it->second.was_down = node.was_down;
                it->second.lsn = node.lsn;
                it->second.tablet_lsns = node.tablet_lsns;
                it->second.process_id = node.process_id;
                it->second.last_seen = node.last_seen;
            }

            if (just_failed) newly_dead.push_back(node.id);
            if (just_recovered || process_restarted) newly_live.push_back(node.id);
        }

        for (const auto& id : newly_dead) handle_node_failure(id);
        for (const auto& id : newly_live) handle_node_recovery(id);
    }
}

bool Coordinator::ping_node(StorageNode& node, bool* process_restarted) {
    if (process_restarted) *process_restarted = false;
    int fd = connect_node_kv(node);
    if (fd < 0) {
        ++node.missed;
        std::cout << "[coord] node " << node.id << " missed ping " << node.missed
                  << "/" << cfg_.hb_miss_thresh << "\n";
        if (node.alive && node.missed >= cfg_.hb_miss_thresh) {
            node.alive = false;
            node.was_down = true;
            std::cout << "[coord] node " << node.id << " marked DOWN\n";
            return true;
        }
        return false;
    }

    ::send(fd, "PING\r\n", 6, MSG_NOSIGNAL);
    std::string resp;
    bool ok = read_line(fd, resp);
    ::close(fd);

    if (ok && resp.rfind("+OK", 0) == 0) {
        bool was_alive = node.alive;
        bool recovering = node.was_down;
        node.missed = 0;
        node.alive = true;
        node.last_seen = std::chrono::steady_clock::now();
        const int old_pid = node.process_id;
        parse_ping_metadata(node, resp);
        if (process_restarted && was_alive && !recovering &&
            old_pid > 0 && node.process_id > 0 && old_pid != node.process_id) {
            *process_restarted = true;
            std::cout << "[coord] node " << node.id
                      << " process restarted (pid " << old_pid
                      << " -> " << node.process_id << "), reapplying tablet roles\n";
        }
        if (!was_alive && recovering) {
            node.was_down = false;
            std::cout << "[coord] node " << node.id << " recovered\n";
        }
        return false;
    }

    ++node.missed;
    std::cout << "[coord] node " << node.id << " bad ping " << node.missed
              << "/" << cfg_.hb_miss_thresh << "\n";
    if (node.alive && node.missed >= cfg_.hb_miss_thresh) {
        node.alive = false;
        node.was_down = true;
        std::cout << "[coord] node " << node.id << " marked DOWN after bad ping\n";
        return true;
    }
    return false;
}

bool Coordinator::refresh_node_lsn_now(StorageNode& node) {
    int fd = connect_node_kv(node);
    if (fd < 0) return false;

    ::send(fd, "PING\r\n", 6, MSG_NOSIGNAL);
    std::string resp;
    bool ok = read_line(fd, resp);
    ::close(fd);
    if (!ok || resp.rfind("+OK", 0) != 0) return false;

    node.missed = 0;
    node.alive = true;
    node.was_down = false;
    node.last_seen = std::chrono::steady_clock::now();
    parse_ping_metadata(node, resp);
    return true;
}

void Coordinator::cache_refreshed_node(const StorageNode& node) {
    std::unique_lock<std::shared_mutex> lk(nodes_mu_);
    auto it = nodes_.find(node.id);
    if (it == nodes_.end()) return;
    it->second.missed = node.missed;
    it->second.alive = node.alive;
    it->second.was_down = node.was_down;
    it->second.lsn = node.lsn;
    it->second.tablet_lsns = node.tablet_lsns;
    it->second.process_id = node.process_id;
    it->second.last_seen = node.last_seen;
}

bool Coordinator::repl_port_ready(const StorageNode& node) {
    int fd = connect_node_repl(node);
    if (fd < 0) return false;
    ::close(fd);
    return true;
}

bool Coordinator::promote_node(StorageNode& node, const std::string& tablet_name) {
    std::string cmd = "BECOME_PRIMARY " + tablet_name + "\r\n";
    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(node);
        if (fd < 0) {
            if (attempt == 3) {
                std::cerr << "[coord] failed to connect to repl port for node " << node.id
                          << " after 3 attempts\n";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }
        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        std::string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);
        if (ok) {
            std::cout << "[coord] promoted " << node.id << " as new primary for " << tablet_name << "\n";
            return true;
        }
        if (attempt == 3) {
            std::cerr << "[coord] promotion failed for " << node.id << " on tablet " << tablet_name
                      << " after 3 attempts, resp='" << resp << "'\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool Coordinator::demote_and_sync_node(StorageNode& node, const StorageNode& primary, const std::string& tablet_name) {
    std::ostringstream oss;
    oss << "BECOME_SECONDARY " << tablet_name << " " << primary.host << " " << primary.repl_port << "\r\n";
    std::string cmd = oss.str();

    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(node);
        if (fd < 0) {
            if (attempt == 3) {
                std::cerr << "[coord] failed to connect to node " << node.id
                          << " repl port after 3 attempts\n";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        // BECOME_SECONDARY is synchronous on the kvserver: it waits for the
        // full snapshot/delta transfer before responding. Use a long timeout
        // so large tablets (hundreds of MB) have enough time to sync.
        timeval long_tv{};
        long_tv.tv_sec = 120;
        long_tv.tv_usec = 0;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &long_tv, sizeof(long_tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &long_tv, sizeof(long_tv));

        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        std::string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);

        if (ok) {
            std::cout << "[coord] node " << node.id << " synced from primary " << primary.id
                      << " for " << tablet_name << "\n";
            return true;
        }

        if (attempt == 3) {
            std::cerr << "[coord] failed to demote/sync " << node.id << " for tablet " << tablet_name
                      << " after 3 attempts, resp='" << resp << "'\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool Coordinator::add_replica_to_primary(StorageNode& primary, const StorageNode& replica, const std::string& tablet_name) {
    std::ostringstream oss;
    oss << "ADD_REPLICA " << tablet_name << " " << replica.id << " " << replica.host << " " << replica.repl_port << "\r\n";
    std::string cmd = oss.str();

    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(primary);
        if (fd < 0) {
            if (attempt == 3) {
                std::cerr << "[coord] failed to connect to primary " << primary.id
                          << " repl port for add-replica after 3 attempts\n";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        std::string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);

        if (ok) {
            std::cout << "[coord] added recovered node " << replica.id << " back to " << tablet_name
                      << " under primary " << primary.id << "\n";
            return true;
        }

        if (attempt == 3) {
            std::cerr << "[coord] primary " << primary.id << " rejected add-replica for " << replica.id
                      << " on " << tablet_name << " after 3 attempts, resp='" << resp << "'\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool Coordinator::rebuild_primary_replicas(StorageNode& primary,
                                           const std::string& tablet_name,
                                           const std::vector<StorageNode>& followers) {
    // BECOME_PRIMARY is idempotent for an already-primary tablet and clears the
    // primary's in-memory replica list. Re-add only currently alive followers so
    // a tablet can keep accepting writes when failed secondaries leave it with a
    // smaller live membership.
    if (!promote_node(primary, tablet_name)) {
        std::cerr << "[coord] failed to rebuild replica set for primary "
                  << primary.id << " on " << tablet_name << "\n";
        return false;
    }

    bool ok = true;
    for (const auto& follower : followers) {
        StorageNode follower_copy = follower;
        if (!demote_and_sync_node(follower_copy, primary, tablet_name)) {
            std::cerr << "[coord] failed to refresh live follower " << follower.id
                      << " while rebuilding " << tablet_name << "\n";
            ok = false;
            continue;
        }
        if (!add_replica_to_primary(primary, follower_copy, tablet_name)) {
            std::cerr << "[coord] failed to re-add live follower " << follower.id
                      << " while rebuilding " << tablet_name << "\n";
            ok = false;
        }
    }
    return ok;
}

void Coordinator::handle_node_failure(const std::string& dead_id) {
    struct ElectionCandidate {
        StorageNode primary;
        std::vector<StorageNode> followers;
        uint64_t lsn = 0;
    };
    struct FailurePlan {
        std::string tablet_name;
        std::string dead_id;
        std::vector<ElectionCandidate> candidates;
        uint64_t max_known_lsn = 0;
    };
    struct ReplicaRebuildPlan {
        std::string tablet_name;
        std::string dead_id;
        StorageNode primary;
        std::vector<StorageNode> followers;
    };

    std::vector<FailurePlan> plans;
    std::vector<ReplicaRebuildPlan> rebuild_plans;
    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);

        auto dead_it = nodes_.find(dead_id);
        if (dead_it == nodes_.end()) return;

        for (const auto& tg : tablets_) {
            if (tg.node_ids.empty()) continue;

            auto dead_pos = std::find(tg.node_ids.begin(), tg.node_ids.end(), dead_id);
            if (dead_pos == tg.node_ids.end()) continue;

            auto current_primary = nodes_.find(tg.node_ids[0]);
            const bool current_primary_alive =
                current_primary != nodes_.end() && current_primary->second.alive;

            if (tg.node_ids[0] != dead_id && current_primary_alive) {
                auto pit = nodes_.find(tg.node_ids[0]);
                if (pit == nodes_.end() || !pit->second.alive) continue;

                std::vector<StorageNode> followers;
                for (size_t i = 1; i < tg.node_ids.size(); ++i) {
                    if (tg.node_ids[i] == dead_id) continue;
                    auto fit = nodes_.find(tg.node_ids[i]);
                    if (fit != nodes_.end() && fit->second.alive) followers.push_back(fit->second);
                }

                rebuild_plans.push_back(ReplicaRebuildPlan{tg.name, dead_id, pit->second, followers});
                continue;
            }

            std::cout << "[coord] tablet " << tg.name << " primary "
                      << tg.node_ids[0] << " is down, electing...\n";

            std::vector<ElectionCandidate> candidates;
            const uint64_t max_known_lsn = tablet_max_known_lsn(tg);
            for (const auto& candidate_id : tg.node_ids) {
                if (candidate_id == dead_id) continue;
                auto it = nodes_.find(candidate_id);
                if (it == nodes_.end() || !it->second.alive) continue;
                ElectionCandidate c;
                c.primary = it->second;
                c.lsn = node_tablet_lsn(it->second, tg.name);
                for (const auto& follower_id : tg.node_ids) {
                    if (follower_id == dead_id || follower_id == candidate_id) continue;
                    auto fit = nodes_.find(follower_id);
                    if (fit != nodes_.end() && fit->second.alive) c.followers.push_back(fit->second);
                }
                candidates.push_back(std::move(c));
            }
            std::sort(candidates.begin(), candidates.end(),
                      [](const ElectionCandidate& a, const ElectionCandidate& b) {
                          return a.lsn > b.lsn;
                      });

            if (candidates.empty()) {
                std::cerr << "[coord] tablet " << tg.name << " has NO alive replicas -- unavailable!\n";
                continue;
            }

            plans.push_back(FailurePlan{tg.name, dead_id, candidates, max_known_lsn});
        }
    }

    for (auto& plan : rebuild_plans) {
        if (!refresh_node_lsn_now(plan.primary)) {
            std::cerr << "[coord] tablet " << plan.tablet_name
                      << " lost secondary " << plan.dead_id
                      << ", but primary " << plan.primary.id
                      << " no longer answers fresh LSN probe; waiting for heartbeat\n";
            continue;
        }
        cache_refreshed_node(plan.primary);

        std::vector<StorageNode> refreshed_followers;
        for (auto& follower : plan.followers) {
            if (refresh_node_lsn_now(follower)) {
                cache_refreshed_node(follower);
                refreshed_followers.push_back(follower);
            } else {
                std::cerr << "[coord] skipping follower " << follower.id
                          << " while rebuilding " << plan.tablet_name
                          << " because it failed a fresh LSN probe\n";
            }
        }
        plan.followers = std::move(refreshed_followers);

        StorageNode primary_copy = plan.primary;
        std::cout << "[coord] tablet " << plan.tablet_name
                  << " lost secondary " << plan.dead_id
                  << "; rebuilding live replica set under primary "
                  << plan.primary.id << " with " << plan.followers.size()
                  << " follower(s)\n";

        std::vector<std::string> healthy_followers;
        if (rebuild_primary_replicas(primary_copy, plan.tablet_name, plan.followers)) {
            for (const auto& follower : plan.followers) healthy_followers.push_back(follower.id);
        } else {
            for (const auto& follower : plan.followers) {
                if (follower.alive) healthy_followers.push_back(follower.id);
            }
        }

        std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
        auto pit = nodes_.find(plan.primary.id);
        if (pit != nodes_.end()) {
            pit->second.role = NodeRole::PRIMARY;
            pit->second.alive = true;
        }
        auto dit = nodes_.find(plan.dead_id);
        if (dit != nodes_.end()) dit->second.role = NodeRole::UNKNOWN;
        for (const auto& follower_id : healthy_followers) {
            auto fit = nodes_.find(follower_id);
            if (fit != nodes_.end()) fit->second.role = NodeRole::SECONDARY;
        }
    }

    for (auto& plan : plans) {
        std::vector<ElectionCandidate> refreshed_candidates;
        for (auto& candidate : plan.candidates) {
            if (!refresh_node_lsn_now(candidate.primary)) {
                std::cerr << "[coord] failover candidate " << candidate.primary.id
                          << " for " << plan.tablet_name
                          << " failed fresh LSN probe; skipping\n";
                continue;
            }
            cache_refreshed_node(candidate.primary);
            candidate.lsn = node_tablet_lsn(candidate.primary, plan.tablet_name);
            plan.max_known_lsn = std::max(plan.max_known_lsn, candidate.lsn);

            std::vector<StorageNode> refreshed_followers;
            for (auto& follower : candidate.followers) {
                if (refresh_node_lsn_now(follower)) {
                    cache_refreshed_node(follower);
                    plan.max_known_lsn = std::max(plan.max_known_lsn,
                                                  node_tablet_lsn(follower, plan.tablet_name));
                    refreshed_followers.push_back(follower);
                } else {
                    std::cerr << "[coord] follower " << follower.id
                              << " for " << plan.tablet_name
                              << " failed fresh LSN probe during election; excluding from new replica set\n";
                }
            }
            candidate.followers = std::move(refreshed_followers);
            refreshed_candidates.push_back(std::move(candidate));
        }
        plan.candidates = std::move(refreshed_candidates);
        std::sort(plan.candidates.begin(), plan.candidates.end(),
                  [](const ElectionCandidate& a, const ElectionCandidate& b) {
                      return a.lsn > b.lsn;
                  });

        if (plan.candidates.empty()) {
            std::cerr << "[coord] tablet " << plan.tablet_name
                      << " has no live candidates after fresh LSN probes -- unavailable\n";
            continue;
        }

        const ElectionCandidate* winner = nullptr;
        for (const auto& candidate : plan.candidates) {
            if (candidate.lsn < plan.max_known_lsn) {
                std::cerr << "[coord] refusing stale failover candidate "
                          << candidate.primary.id << " for " << plan.tablet_name
                          << ": candidate_lsn=" << candidate.lsn
                          << " max_known_lsn=" << plan.max_known_lsn
                          << " (waiting for freshest replica)\n";
                continue;
            }
            StorageNode primary_copy = candidate.primary;
            if (promote_node(primary_copy, plan.tablet_name)) {
                winner = &candidate;
                break;
            }
            std::cerr << "[coord] promotion RPC failed for " << candidate.primary.id
                      << " on " << plan.tablet_name << "; trying next candidate\n";
        }
        if (!winner) {
            std::cerr << "[coord] no up-to-date alive replica for " << plan.tablet_name
                      << "; tablet remains unavailable until a fresher replica recovers\n";
            continue;
        }

        std::vector<std::string> healthy_followers;
        for (const auto& replica : winner->followers) {
            std::cout << "[coord] reconfiguring " << replica.id
                      << " as secondary of new primary " << winner->primary.id
                      << " for " << plan.tablet_name << "\n";

            StorageNode replica_copy = replica;
            StorageNode primary_copy = winner->primary;
            if (!demote_and_sync_node(replica_copy, primary_copy, plan.tablet_name)) {
                std::cerr << "[coord] failed to re-sync replica " << replica.id
                          << " under new primary " << winner->primary.id
                          << " for " << plan.tablet_name << "\n";
                continue;
            }

            if (!add_replica_to_primary(primary_copy, replica_copy, plan.tablet_name)) {
                std::cerr << "[coord] failed to add replica " << replica.id
                          << " to new primary " << winner->primary.id
                          << " for " << plan.tablet_name << "\n";
                continue;
            }

            healthy_followers.push_back(replica.id);
        }

        {
            std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
            std::unique_lock<std::shared_mutex> tlk(tablets_mu_);

            auto dead_it = nodes_.find(plan.dead_id);
            auto new_it = nodes_.find(winner->primary.id);
            if (new_it == nodes_.end()) continue;

            for (auto& tg : tablets_) {
                if (tg.name != plan.tablet_name) continue;
                auto pos = std::find(tg.node_ids.begin(), tg.node_ids.end(), winner->primary.id);
                if (pos == tg.node_ids.end()) break;
                std::swap(tg.node_ids[0], *pos);
                break;
            }

            if (dead_it != nodes_.end()) dead_it->second.role = NodeRole::UNKNOWN;
            new_it->second.role = NodeRole::PRIMARY;
            new_it->second.alive = true;

            for (const auto& follower_id : healthy_followers) {
                auto fit = nodes_.find(follower_id);
                if (fit != nodes_.end()) fit->second.role = NodeRole::SECONDARY;
            }
        }

        std::cout << "[coord] promoted " << winner->primary.id
                  << " as new primary for " << plan.tablet_name << "\n";
    }
}

void Coordinator::handle_node_recovery(const std::string& live_id) {
    struct RecoveryPlan {
        std::string tablet_name;
        StorageNode live;
        StorageNode primary;
        std::vector<StorageNode> followers;
        bool promote_live = false;
        bool wait_for_fresher = false;
        std::string old_primary_id;
        uint64_t live_lsn = 0;
        uint64_t primary_lsn = 0;
        uint64_t max_known_lsn = 0;
    };

    std::vector<RecoveryPlan> plans;
    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);

        auto live_it = nodes_.find(live_id);
        if (live_it == nodes_.end()) return;

        for (const auto& tg : tablets_) {
            auto pos = std::find(tg.node_ids.begin(), tg.node_ids.end(), live_id);
            if (pos == tg.node_ids.end()) continue;
            if (tg.node_ids.empty()) continue;

            const uint64_t live_lsn = node_tablet_lsn(live_it->second, tg.name);
            // Only consider alive nodes for max_known_lsn — dead nodes' cached
            // LSNs must not block recovery when all replicas were down.
            uint64_t max_known_lsn = live_lsn;
            for (const auto& id : tg.node_ids) {
                auto it = nodes_.find(id);
                if (it != nodes_.end() && it->second.alive)
                    max_known_lsn = std::max(max_known_lsn, node_tablet_lsn(it->second, tg.name));
            }
            auto pit = nodes_.find(tg.node_ids[0]);
            const bool primary_alive = pit != nodes_.end() && pit->second.alive;
            const uint64_t primary_lsn = primary_alive ? node_tablet_lsn(pit->second, tg.name) : 0;

            std::vector<StorageNode> peers;
            for (const auto& id : tg.node_ids) {
                if (id == live_id) continue;
                auto fit = nodes_.find(id);
                if (fit != nodes_.end() && fit->second.alive) peers.push_back(fit->second);
            }

            if (live_lsn < max_known_lsn && (!primary_alive || primary_lsn < max_known_lsn)) {
                std::cerr << "[coord] recovering node " << live_id
                          << " for " << tg.name
                          << " is stale: live_lsn=" << live_lsn
                          << " max_known_lsn=" << max_known_lsn
                          << "; waiting for freshest replica before serving this tablet\n";
                plans.push_back(RecoveryPlan{tg.name, live_it->second,
                                             primary_alive ? pit->second : StorageNode{},
                                             peers, false, true,
                                             tg.node_ids[0], live_lsn, primary_lsn, max_known_lsn});
                continue;
            }

            if (primary_alive && tg.node_ids[0] != live_id && primary_lsn >= live_lsn) {
                plans.push_back(RecoveryPlan{tg.name, live_it->second, pit->second, peers, false, false,
                                             tg.node_ids[0], live_lsn, primary_lsn, max_known_lsn});
                continue;
            }

            if (!primary_alive) {
                std::cerr << "[coord] recovering node " << live_id
                          << " for " << tg.name
                          << ": no live up-to-date primary; promoting recovered node\n";
            } else if (tg.node_ids[0] != live_id && live_lsn > primary_lsn) {
                std::cerr << "[coord] recovered node " << live_id
                          << " is fresher for " << tg.name
                          << " (live_lsn=" << live_lsn
                          << ", current_primary_lsn=" << primary_lsn
                          << "); promoting it and resyncing old primary\n";
            }

            plans.push_back(RecoveryPlan{tg.name, live_it->second,
                                         primary_alive ? pit->second : StorageNode{},
                                         peers, true, false,
                                         tg.node_ids[0], live_lsn, primary_lsn, max_known_lsn});
        }
    }

    for (auto& plan : plans) {
        if (!refresh_node_lsn_now(plan.live)) {
            std::cerr << "[coord] recovered node " << live_id
                      << " stopped answering fresh LSN probe for "
                      << plan.tablet_name << "; skipping recovery action\n";
            continue;
        }
        cache_refreshed_node(plan.live);

        std::vector<StorageNode> refreshed_followers;
        for (auto& follower : plan.followers) {
            if (refresh_node_lsn_now(follower)) {
                cache_refreshed_node(follower);
                refreshed_followers.push_back(follower);
            }
        }
        plan.followers = std::move(refreshed_followers);

        if (!plan.primary.id.empty()) {
            bool refreshed_primary = false;
            for (const auto& follower : plan.followers) {
                if (follower.id == plan.primary.id) {
                    plan.primary = follower;
                    refreshed_primary = true;
                    break;
                }
            }
            if (!refreshed_primary && refresh_node_lsn_now(plan.primary)) {
                cache_refreshed_node(plan.primary);
                refreshed_primary = true;
            }
            if (!refreshed_primary) plan.primary.alive = false;
        } else if (!plan.old_primary_id.empty() && plan.old_primary_id != live_id) {
            for (const auto& follower : plan.followers) {
                if (follower.id == plan.old_primary_id) {
                    plan.primary = follower;
                    break;
                }
            }
        }

        plan.live_lsn = node_tablet_lsn(plan.live, plan.tablet_name);
        plan.primary_lsn = (!plan.primary.id.empty() && plan.primary.alive)
            ? node_tablet_lsn(plan.primary, plan.tablet_name)
            : 0;
        plan.max_known_lsn = std::max(plan.max_known_lsn, plan.live_lsn);
        plan.max_known_lsn = std::max(plan.max_known_lsn, plan.primary_lsn);
        for (const auto& follower : plan.followers) {
            plan.max_known_lsn = std::max(plan.max_known_lsn,
                                          node_tablet_lsn(follower, plan.tablet_name));
        }

        const bool primary_alive = !plan.primary.id.empty() && plan.primary.alive;
        if (plan.live_lsn < plan.max_known_lsn &&
            (!primary_alive || plan.primary_lsn < plan.max_known_lsn)) {
            std::cerr << "[coord] recovered node " << live_id
                      << " is stale for " << plan.tablet_name
                      << " after fresh probes: live_lsn=" << plan.live_lsn
                      << " primary_lsn=" << plan.primary_lsn
                      << " max_known_lsn=" << plan.max_known_lsn
                      << "; preserving availability fence\n";
            continue;
        }
        if (primary_alive && plan.old_primary_id != live_id && plan.primary_lsn >= plan.live_lsn) {
            plan.promote_live = false;
            plan.wait_for_fresher = false;
        } else {
            plan.promote_live = true;
            plan.wait_for_fresher = false;
        }

        if (plan.wait_for_fresher) {
            continue;
        }

        if (plan.promote_live) {
            std::cout << "[coord] node " << live_id
                      << " recovered for " << plan.tablet_name
                      << " -- sending promote RPC at LSN=" << plan.live_lsn << "\n";

            StorageNode live_copy = plan.live;
            if (!promote_node(live_copy, plan.tablet_name)) {
                std::cerr << "[coord] failed to promote recovered node "
                          << live_id << " for " << plan.tablet_name << "\n";
                continue;
            }

            std::vector<std::string> healthy_followers;
            for (const auto& follower : plan.followers) {
                StorageNode follower_copy = follower;
                if (!demote_and_sync_node(follower_copy, live_copy, plan.tablet_name)) {
                    std::cerr << "[coord] failed to sync follower " << follower.id
                              << " from recovered primary for " << plan.tablet_name << "\n";
                    continue;
                }
                if (!add_replica_to_primary(live_copy, follower_copy, plan.tablet_name)) {
                    std::cerr << "[coord] failed to re-add follower " << follower.id
                              << " after recovered primary promotion for " << plan.tablet_name << "\n";
                    continue;
                }
                healthy_followers.push_back(follower.id);
            }

            std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
            std::unique_lock<std::shared_mutex> tlk(tablets_mu_);

            for (auto& tg : tablets_) {
                if (tg.name != plan.tablet_name) continue;
                auto pos = std::find(tg.node_ids.begin(), tg.node_ids.end(), live_id);
                if (pos != tg.node_ids.end()) std::swap(tg.node_ids[0], *pos);
                break;
            }

            auto it = nodes_.find(live_id);
            if (it != nodes_.end()) {
                it->second.role = NodeRole::PRIMARY;
                it->second.alive = true;
            }
            for (const auto& follower_id : healthy_followers) {
                auto fit = nodes_.find(follower_id);
                if (fit != nodes_.end()) fit->second.role = NodeRole::SECONDARY;
            }
            if (!plan.old_primary_id.empty() && plan.old_primary_id != live_id) {
                auto old_it = nodes_.find(plan.old_primary_id);
                if (old_it != nodes_.end() && !old_it->second.alive) {
                    old_it->second.role = NodeRole::UNKNOWN;
                }
            }
            continue;
        }

        std::cout << "[coord] recovering node " << live_id
                  << " as secondary for " << plan.tablet_name
                  << " from primary " << plan.primary.id
                  << " (live_lsn=" << plan.live_lsn
                  << ", primary_lsn=" << plan.primary_lsn << ")\n";

        StorageNode live_copy = plan.live;
        StorageNode primary_copy = plan.primary;

        if (!demote_and_sync_node(live_copy, primary_copy, plan.tablet_name)) {
            std::cerr << "[coord] failed to sync recovered node " << live_id
                      << " from primary " << plan.primary.id
                      << " for " << plan.tablet_name << "\n";
            continue;
        }

        if (!add_replica_to_primary(primary_copy, live_copy, plan.tablet_name)) {
            std::cerr << "[coord] failed to add recovered replica " << live_id
                      << " to primary " << plan.primary.id
                      << " for " << plan.tablet_name << "\n";
            continue;
        }

        std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
        auto it = nodes_.find(live_id);
        if (it != nodes_.end()) {
            it->second.role = NodeRole::SECONDARY;
            it->second.alive = true;
        }
    }
}

const TabletGroup* Coordinator::find_tablet(const std::string& row) const {
    std::shared_lock<std::shared_mutex> lk(tablets_mu_);
    for (const auto& tg : tablets_) {
        bool after_start = tg.row_start.empty() || row >= tg.row_start;
        bool before_end = tg.row_end.empty() || row < tg.row_end;
        if (after_start && before_end) return &tg;
    }
    return nullptr;
}

std::string Coordinator::handle_lookup(const std::string& row) {
    TabletGroup tg_copy;
    {
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);
        const TabletGroup* tg = nullptr;
        for (const auto& t : tablets_) {
            bool after_start = t.row_start.empty() || row >= t.row_start;
            bool before_end = t.row_end.empty() || row < t.row_end;
            if (after_start && before_end) { tg = &t; break; }
        }
        if (!tg) return "-ERR no tablet for row\r\n";
        tg_copy = *tg;
    }

    StorageNode primary;
    bool have_primary = false;
    uint64_t max_known_lsn = 0;
    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        if (!tg_copy.node_ids.empty()) {
            auto it = nodes_.find(tg_copy.node_ids[0]);
            if (it != nodes_.end() && it->second.alive) {
                primary = it->second;
                have_primary = true;
                max_known_lsn = tablet_max_known_lsn(tg_copy);
            }
        }
    }

    if (!have_primary) return "-ERR primary unavailable\r\n";

    {
        StorageNode refreshed = primary;
        if (!refresh_node_lsn_now(refreshed)) {
            return "-ERR primary unavailable\r\n";
        }
        cache_refreshed_node(refreshed);
        primary = refreshed;
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        max_known_lsn = tablet_max_known_lsn(tg_copy);
    }

    uint64_t primary_lsn = node_tablet_lsn(primary, tg_copy.name);
    if (primary_lsn < max_known_lsn) {
        StorageNode refreshed = primary;
        if (refresh_node_lsn_now(refreshed)) {
            cache_refreshed_node(refreshed);
            primary = refreshed;
            primary_lsn = node_tablet_lsn(primary, tg_copy.name);
            std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
            max_known_lsn = tablet_max_known_lsn(tg_copy);
        }
        if (primary_lsn < max_known_lsn) {
            return "-ERR primary stale; waiting for freshest replica\r\n";
        }
    }

    return "+OK " + primary.host + " " + std::to_string(primary.port) + " primary\r\n";
}


std::string Coordinator::handle_read_lookup(const std::string& row) {
    TabletGroup tg_copy;
    {
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);
        const TabletGroup* tg = nullptr;
        for (const auto& t : tablets_) {
            bool after_start = t.row_start.empty() || row >= t.row_start;
            bool before_end = t.row_end.empty() || row < t.row_end;
            if (after_start && before_end) { tg = &t; break; }
        }
        if (!tg) return "-ERR no tablet for row\r\n";
        tg_copy = *tg;
    }

    StorageNode primary;
    bool have_primary = false;
    uint64_t max_known_lsn = 0;
    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        if (!tg_copy.node_ids.empty()) {
            auto it = nodes_.find(tg_copy.node_ids[0]);
            if (it != nodes_.end() && it->second.alive) {
                primary = it->second;
                have_primary = true;
                max_known_lsn = tablet_max_known_lsn(tg_copy);
            }
        }
    }

    if (!have_primary) return "-ERR primary unavailable\r\n";

    {
        StorageNode refreshed = primary;
        if (!refresh_node_lsn_now(refreshed)) {
            return "-ERR primary unavailable\r\n";
        }
        cache_refreshed_node(refreshed);
        primary = refreshed;
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        max_known_lsn = tablet_max_known_lsn(tg_copy);
    }

    uint64_t primary_lsn = node_tablet_lsn(primary, tg_copy.name);
    if (primary_lsn < max_known_lsn) {
        StorageNode refreshed = primary;
        if (refresh_node_lsn_now(refreshed)) {
            cache_refreshed_node(refreshed);
            primary = refreshed;
            primary_lsn = node_tablet_lsn(primary, tg_copy.name);
            std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
            max_known_lsn = tablet_max_known_lsn(tg_copy);
        }
        if (primary_lsn < max_known_lsn) {
            return "-ERR primary stale; waiting for freshest replica\r\n";
        }
    }

    return "+OK " + primary.host + " " + std::to_string(primary.port) + " primary\r\n";
}

std::string Coordinator::handle_tablets() {
    std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
    std::shared_lock<std::shared_mutex> tlk(tablets_mu_);

    std::ostringstream js;
    js << "[";
    bool first_tablet = true;

    for (const auto& tg : tablets_) {
        if (!first_tablet) js << ",";
        first_tablet = false;

        js << "{";
        js << "\"name\":" << json_str_coord(tg.name) << ",";
        js << "\"row_start\":" << json_str_coord(tg.row_start) << ",";
        js << "\"row_end\":" << json_str_coord(tg.row_end) << ",";
        js << "\"replicas\":[";

        bool first_replica = true;
        for (size_t i = 0; i < tg.node_ids.size(); ++i) {
            auto it = nodes_.find(tg.node_ids[i]);
            if (it == nodes_.end()) continue;
            const auto& n = it->second;

            if (!first_replica) js << ",";
            first_replica = false;

            js << "{";
            js << "\"id\":" << json_str_coord(n.id) << ",";
            js << "\"host\":" << json_str_coord(n.host) << ",";
            js << "\"port\":" << n.port << ",";
            js << "\"repl_port\":" << n.repl_port << ",";
            js << "\"alive\":" << (n.alive ? "true" : "false") << ",";
            js << "\"role\":\"" << (i == 0 ? "primary" : "secondary") << "\"";
            js << "}";
        }

        js << "]";
        js << "}";
    }

    js << "]";
    std::string body = js.str();
    return "+OK " + std::to_string(body.size()) + "\r\n" + body;
}

std::string Coordinator::handle_status() {
    std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
    std::ostringstream js;
    js << "[";
    bool first = true;
    for (const auto& [id, n] : nodes_) {
        if (!first) js << ",";
        first = false;
        js << "{\"id\":" << json_str_coord(id) << ","
           << "\"host\":" << json_str_coord(n.host) << ","
           << "\"port\":" << n.port << ","
           << "\"repl_port\":" << n.repl_port << ","
           << "\"alive\":" << (n.alive ? "true" : "false") << ","
           << "\"role\":\"" << (n.role == NodeRole::PRIMARY ? "primary" : (n.role == NodeRole::SECONDARY ? "secondary" : "unknown")) << "\"," 
           << "\"lsn\":" << n.lsn << ","
           << "\"missed\":" << n.missed << "}";
    }
    js << "]";
    std::string body = js.str();
    return "+OK " + std::to_string(body.size()) + "\r\n" + body;
}

std::string Coordinator::handle_nodes() { return handle_status(); }

void Coordinator::accept_loop() {
    while (running_) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        if (cfd < 0) continue;
        std::thread([this, cfd] {
            handle_client(cfd);
            ::close(cfd);
        }).detach();
    }
}

void Coordinator::handle_client(int fd) {
    // Give clients 10 s to send a full request (handles slow connections).
    timeval rcvtv{};
    rcvtv.tv_sec = 10;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcvtv, sizeof(rcvtv));
    // coord_send() loops internally; a short send timeout is fine here.
    timeval sndtv{};
    sndtv.tv_usec = 250000;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &sndtv, sizeof(sndtv));

    // Helper: write all bytes of a response to fd, break the client loop on failure.
    auto coord_send = [&](const char* data, size_t len) -> bool {
        size_t sent = 0;
        while (sent < len) {
            ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
            if (n <= 0) return false;
            sent += static_cast<size_t>(n);
        }
        return true;
    };

    std::string line;
    while (read_line(fd, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        std::string op;
        ss >> op;
        if (op == "LOOKUP") {
            uint32_t rowlen = 0;
            ss >> rowlen;
            std::string row;
            if (!coord_read_exact(fd, row, rowlen)) break;
            std::string resp = handle_lookup(row);
            if (!coord_send(resp.data(), resp.size())) break;
        } else if (op == "READLOOKUP") {
            uint32_t rowlen = 0;
            ss >> rowlen;
            std::string row;
            if (!coord_read_exact(fd, row, rowlen)) break;
            std::string resp = handle_read_lookup(row);
            if (!coord_send(resp.data(), resp.size())) break;
        } else if (op == "STATUS" || op == "NODES") {
            std::string resp = handle_status();
            if (!coord_send(resp.data(), resp.size())) break;
        } else if (op == "TABLETS") {
            std::string resp = handle_tablets();
            if (!coord_send(resp.data(), resp.size())) break;
        } else if (op == "READY") {
            const char* msg = roles_configured_.load() ? "+OK\r\n" : "-ERR not ready\r\n";
            if (!coord_send(msg, strlen(msg))) break;
        } else if (op == "PING") {
            if (!coord_send("+OK\r\n", 5)) break;
        } else {
            if (!coord_send("-ERR unknown op\r\n", 17)) break;
        }
    }
}

int Coordinator::create_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("coord socket() failed");
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("coord bind() failed");
    }
    if (::listen(fd, 64) < 0) {
        ::close(fd);
        throw std::runtime_error("coord listen() failed");
    }
    return fd;
}

bool Coordinator::read_line(int fd, std::string& line) {
    line.clear();
    char c;
    while (true) {
        ssize_t r = ::recv(fd, &c, 1, 0);
        if (r <= 0) return false;
        if (c == '\n') {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            return true;
        }
        line += c;
        if (line.size() > 8192) return false;
    }
}

int Coordinator::connect_host_port(const std::string& host, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
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

static Coordinator* g_coord = nullptr;
static void sig_handler(int) { if (g_coord) g_coord->stop(); }

int main(int argc, char* argv[]) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    Coordinator::Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--port"   && i + 1 < argc) cfg.port = std::stoi(argv[++i]);
        else if (a == "--config" && i + 1 < argc) cfg.config_file = argv[++i];
    }

    std::cout << "=== PennCloud Coordinator ===\n"
              << "  port:   " << cfg.port << "\n"
              << "  config: " << cfg.config_file << "\n\n";

    Coordinator coord(cfg);
    g_coord = &coord;
    std::signal(SIGINT, sig_handler);
    std::signal(SIGTERM, sig_handler);
    coord.run();
    return 0;
}
