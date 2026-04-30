
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
    int         missed = 0;
    std::chrono::steady_clock::time_point last_seen;
};

struct TabletGroup {
    std::string name;
    std::string row_start;
    std::string row_end;
    std::vector<std::string> node_ids;  // node_ids[0] is primary
};

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
    bool ping_node(StorageNode& node);
    void handle_node_failure(const std::string& dead_id);
    void handle_node_recovery(const std::string& live_id);
    bool promote_node(StorageNode& node, const std::string& tablet_name);
    bool demote_and_sync_node(StorageNode& node, const StorageNode& primary, const std::string& tablet_name);
    bool add_replica_to_primary(StorageNode& primary, const StorageNode& replica, const std::string& tablet_name);
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
        // 1) Promote designated primaries.
        for (const auto& tg : tgs) {
            if (tg.node_ids.empty()) continue;
            auto it = nodes_snapshot.find(tg.node_ids[0]);
            if (it == nodes_snapshot.end() || !it->second.alive) { all_ok = false; continue; }
            StorageNode primary = it->second;
            if (!promote_node(primary, tg.name)) all_ok = false;
        }
        // 2) Sync secondaries and register them under the primary.
        for (const auto& tg : tgs) {
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
        {
            std::unique_lock<std::shared_mutex> lk(nodes_mu_);
            for (auto& [id, node] : nodes_) {
                bool eligible_recovery = node.was_down;
                bool just_failed = ping_node(node);
                if (just_failed) newly_dead.push_back(id);
                if (eligible_recovery && node.alive && !node.was_down) newly_live.push_back(id);
            }
        }
        for (const auto& id : newly_dead) handle_node_failure(id);
        for (const auto& id : newly_live) handle_node_recovery(id);
    }
}

bool Coordinator::ping_node(StorageNode& node) {
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
        auto lpos = resp.find("LSN=");
        if (lpos != std::string::npos) {
            try { node.lsn = std::stoull(resp.substr(lpos + 4)); } catch (...) {}
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

bool Coordinator::promote_node(StorageNode& node, const std::string& tablet_name) {
    int fd = connect_node_repl(node);
    if (fd < 0) {
        std::cerr << "[coord] failed to connect to repl port for node " << node.id << "\n";
        return false;
    }
    std::string cmd = "BECOME_PRIMARY " + tablet_name + "\r\n";
    if (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) < 0) {
        ::close(fd);
        return false;
    }
    std::string resp;
    bool ok = read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
    ::close(fd);
    if (!ok) {
        std::cerr << "[coord] promotion failed for " << node.id << " on tablet " << tablet_name << "\n";
        return false;
    }
    std::cout << "[coord] promoted " << node.id << " as new primary for " << tablet_name << "\n";
    return true;
}

bool Coordinator::demote_and_sync_node(StorageNode& node, const StorageNode& primary, const std::string& tablet_name) {
    std::ostringstream oss;
    oss << "BECOME_SECONDARY " << tablet_name << " " << primary.host << " " << primary.repl_port << "\r\n";
    std::string cmd = oss.str();

    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(node);
        if (fd < 0) {
            std::cerr << "[coord] failed to connect to recovered node " << node.id
                      << " repl port (attempt " << attempt << "/3)\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        std::string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);

        if (ok) {
            std::cout << "[coord] node " << node.id << " synced from primary " << primary.id
                      << " for " << tablet_name << "\n";
            return true;
        }

        std::cerr << "[coord] failed to demote/sync " << node.id << " for tablet " << tablet_name
                  << " (attempt " << attempt << "/3, resp='" << resp << "')\n";
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
            std::cerr << "[coord] failed to connect to primary " << primary.id
                      << " repl port for add-replica (attempt " << attempt << "/3)\n";
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

        std::cerr << "[coord] primary " << primary.id << " rejected add-replica for " << replica.id
                  << " on " << tablet_name << " (attempt " << attempt << "/3, resp='" << resp << "')\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

void Coordinator::handle_node_failure(const std::string& dead_id) {
    struct FailurePlan {
        std::string tablet_name;
        std::string dead_id;
        StorageNode new_primary;
        size_t best_idx = 0;
        std::vector<StorageNode> followers;
    };

    std::vector<FailurePlan> plans;
    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);

        auto dead_it = nodes_.find(dead_id);
        if (dead_it == nodes_.end()) return;

        for (const auto& tg : tablets_) {
            if (tg.node_ids.empty() || tg.node_ids[0] != dead_id) continue;

            std::cout << "[coord] tablet " << tg.name << " primary " << dead_id << " is down, electing...\n";
            std::string best_id;
            uint64_t best_lsn = 0;
            size_t best_idx = 0;
            std::vector<StorageNode> followers;

            for (size_t i = 1; i < tg.node_ids.size(); ++i) {
                auto it = nodes_.find(tg.node_ids[i]);
                if (it == nodes_.end() || !it->second.alive) continue;
                if (best_id.empty() || it->second.lsn >= best_lsn) {
                    best_lsn = it->second.lsn;
                    best_id = it->second.id;
                    best_idx = i;
                }
            }
            if (best_id.empty()) {
                std::cerr << "[coord] tablet " << tg.name << " has NO alive secondaries -- unavailable!\n";
                continue;
            }

            auto pit = nodes_.find(best_id);
            if (pit == nodes_.end()) continue;

            for (size_t i = 1; i < tg.node_ids.size(); ++i) {
                if (i == best_idx) continue;
                auto it = nodes_.find(tg.node_ids[i]);
                if (it == nodes_.end() || !it->second.alive) continue;
                followers.push_back(it->second);
            }

            plans.push_back(FailurePlan{tg.name, dead_id, pit->second, best_idx, followers});
        }
    }

    for (const auto& plan : plans) {
        if (!promote_node(const_cast<StorageNode&>(plan.new_primary), plan.tablet_name)) {
            std::cerr << "[coord] promotion RPC failed for " << plan.new_primary.id << "\n";
            continue;
        }

        std::vector<std::string> healthy_followers;
        for (const auto& replica : plan.followers) {
            std::cout << "[coord] reconfiguring " << replica.id
                      << " as secondary of new primary " << plan.new_primary.id
                      << " for " << plan.tablet_name << "\n";

            if (!demote_and_sync_node(const_cast<StorageNode&>(replica), plan.new_primary, plan.tablet_name)) {
                std::cerr << "[coord] failed to re-sync replica " << replica.id
                          << " under new primary " << plan.new_primary.id
                          << " for " << plan.tablet_name << "\n";
                continue;
            }

            if (!add_replica_to_primary(const_cast<StorageNode&>(plan.new_primary), replica, plan.tablet_name)) {
                std::cerr << "[coord] failed to add replica " << replica.id
                          << " to new primary " << plan.new_primary.id
                          << " for " << plan.tablet_name << "\n";
                continue;
            }

            healthy_followers.push_back(replica.id);
        }

        {
            std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
            std::unique_lock<std::shared_mutex> tlk(tablets_mu_);

            auto dead_it = nodes_.find(plan.dead_id);
            auto new_it = nodes_.find(plan.new_primary.id);
            if (new_it == nodes_.end()) continue;

            for (auto& tg : tablets_) {
                if (tg.name != plan.tablet_name) continue;
                auto pos = std::find(tg.node_ids.begin(), tg.node_ids.end(), plan.new_primary.id);
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

        std::cout << "[coord] promoted " << plan.new_primary.id
                  << " as new primary for " << plan.tablet_name << "\n";
    }
}

void Coordinator::handle_node_recovery(const std::string& live_id) {
    struct RecoveryPlan {
        std::string tablet_name;
        StorageNode live;
        StorageNode primary;
        bool recover_as_primary = false;
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

            if (!tg.node_ids.empty() && tg.node_ids[0] == live_id) {
                plans.push_back(RecoveryPlan{tg.name, live_it->second, {}, true});
                continue;
            }

            if (tg.node_ids.empty()) continue;
            auto pit = nodes_.find(tg.node_ids[0]);
            if (pit == nodes_.end() || !pit->second.alive) {
                std::cerr << "[coord] cannot recover node " << live_id
                          << " for " << tg.name << ": no live primary\\n";
                continue;
            }

            plans.push_back(RecoveryPlan{tg.name, live_it->second, pit->second, false});
        }
    }

    for (const auto& plan : plans) {
        if (plan.recover_as_primary) {
            std::cout << "[coord] primary node " << live_id
                      << " recovered for " << plan.tablet_name
                      << " -- sending promote RPC\\n";

            StorageNode live_copy = plan.live;
            if (!promote_node(live_copy, plan.tablet_name)) {
                std::cerr << "[coord] failed to re-promote recovered primary "
                          << live_id << " for " << plan.tablet_name << "\\n";
                continue;
            }

            std::unique_lock<std::shared_mutex> nlk(nodes_mu_);
            auto it = nodes_.find(live_id);
            if (it != nodes_.end()) {
                it->second.role = NodeRole::PRIMARY;
                it->second.alive = true;
            }
            continue;
        }

        std::cout << "[coord] recovering node " << live_id
                  << " as secondary for " << plan.tablet_name
                  << " from primary " << plan.primary.id << "\\n";

        StorageNode live_copy = plan.live;
        StorageNode primary_copy = plan.primary;

        if (!demote_and_sync_node(live_copy, primary_copy, plan.tablet_name)) {
            std::cerr << "[coord] failed to sync recovered node " << live_id
                      << " from primary " << plan.primary.id
                      << " for " << plan.tablet_name << "\\n";
            continue;
        }

        if (!add_replica_to_primary(primary_copy, live_copy, plan.tablet_name)) {
            std::cerr << "[coord] failed to add recovered replica " << live_id
                      << " to primary " << plan.primary.id
                      << " for " << plan.tablet_name << "\\n";
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

    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        for (size_t i = 0; i < tg_copy.node_ids.size(); ++i) {
            auto it = nodes_.find(tg_copy.node_ids[i]);
            if (it == nodes_.end() || !it->second.alive) continue;
            const StorageNode& n = it->second;
            return "+OK " + n.host + " " + std::to_string(n.port) + " " +
                   (i == 0 ? "primary" : "secondary") + "\r\n";
        }
    }
    return "-ERR all replicas down\r\n";
}


std::string Coordinator::handle_read_lookup(const std::string& row) {
    std::vector<std::string> candidate_ids;
    {
        std::shared_lock<std::shared_mutex> tlk(tablets_mu_);
        const TabletGroup* tg = nullptr;
        for (const auto& t : tablets_) {
            bool after_start = t.row_start.empty() || row >= t.row_start;
            bool before_end = t.row_end.empty() || row < t.row_end;
            if (after_start && before_end) { tg = &t; break; }
        }
        if (!tg) return "-ERR no tablet for row\r\n";
        candidate_ids = tg->node_ids;
    }

    {
        std::shared_lock<std::shared_mutex> nlk(nodes_mu_);
        for (size_t i = 1; i < candidate_ids.size(); ++i) {
            auto it = nodes_.find(candidate_ids[i]);
            if (it == nodes_.end() || !it->second.alive) continue;
            const StorageNode& n = it->second;
            return "+OK " + n.host + " " + std::to_string(n.port) + " secondary\r\n";
        }
        if (!candidate_ids.empty()) {
            auto it = nodes_.find(candidate_ids[0]);
            if (it != nodes_.end() && it->second.alive) {
                const StorageNode& n = it->second;
                return "+OK " + n.host + " " + std::to_string(n.port) + " primary\r\n";
            }
        }
    }
    return "-ERR all replicas down\r\n";
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
        js << "\"name\":\"" << tg.name << "\",";
        js << "\"row_start\":\"" << tg.row_start << "\",";
        js << "\"row_end\":\"" << tg.row_end << "\",";
        js << "\"replicas\":[";

        bool first_replica = true;
        for (size_t i = 0; i < tg.node_ids.size(); ++i) {
            auto it = nodes_.find(tg.node_ids[i]);
            if (it == nodes_.end()) continue;
            const auto& n = it->second;

            if (!first_replica) js << ",";
            first_replica = false;

            js << "{";
            js << "\"id\":\"" << n.id << "\",";
            js << "\"host\":\"" << n.host << "\",";
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
        js << "{\"id\":\"" << id << "\"," 
           << "\"host\":\"" << n.host << "\"," 
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
    timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = 250000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    std::string line;
    while (read_line(fd, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        std::string op;
        ss >> op;
        if (op == "LOOKUP") {
            uint32_t rowlen = 0;
            ss >> rowlen;
            std::string row(rowlen, '\0');
            ssize_t r = ::recv(fd, &row[0], rowlen, MSG_WAITALL);
            if (r != static_cast<ssize_t>(rowlen)) break;
            std::string resp = handle_lookup(row);
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "READLOOKUP") {
            uint32_t rowlen = 0;
            ss >> rowlen;
            std::string row(rowlen, '\0');
            ssize_t r = ::recv(fd, &row[0], rowlen, MSG_WAITALL);
            if (r != static_cast<ssize_t>(rowlen)) break;
            std::string resp = handle_read_lookup(row);
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "STATUS" || op == "NODES") {
            std::string resp = handle_status();
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "TABLETS") {
            std::string resp = handle_tablets();
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "READY") {
            const char* msg = roles_configured_.load() ? "+OK\r\n" : "-ERR not ready\r\n";
            ::send(fd, msg, strlen(msg), MSG_NOSIGNAL);
        } else if (op == "PING") {
            ::send(fd, "+OK\r\n", 5, MSG_NOSIGNAL);
        } else {
            ::send(fd, "-ERR unknown op\r\n", 17, MSG_NOSIGNAL);
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
        if (line.size() > 1024) return false;
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
