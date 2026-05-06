
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

using namespace std;

enum class NodeRole { PRIMARY, SECONDARY, UNKNOWN };

struct StorageNode {
    string id;
    string host;
    int         port = -1;      // client/KV port
    int         repl_port = -1; // replication control port
    NodeRole    role   = NodeRole::UNKNOWN;
    bool        alive  = true;
    bool        was_down = false; // true only after a real failure transition
    uint64_t    lsn    = 0;
    int         process_id = 0;
    int         missed = 0;
    chrono::steady_clock::time_point last_seen;
};

struct TabletGroup {
    string name;
    string row_start;
    string row_end;
    vector<string> node_ids;  // config placement order; node_ids[0] is configured primary
    string active_primary_id;      // runtime failover primary, defaults to node_ids[0]
};

static string json_str_coord(const string& s) {
    string out = "\"";
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

static bool coord_read_exact(int fd, string& out, size_t n) {
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
        string config_file    = "./coordinator.conf";
        int         hb_interval_ms = 500;
        int         hb_miss_thresh = 3;
    };

    explicit Coordinator(const Config& cfg) : cfg_(cfg) { load_config(cfg_.config_file); }

    void run();
    void stop() { running_ = false; }

private:
    Config cfg_;
    atomic<bool> running_{false};
    atomic<bool> roles_configured_{false};

    unordered_map<string, StorageNode> nodes_;
    mutable shared_mutex nodes_mu_;

    vector<TabletGroup> tablets_;
    mutable shared_mutex tablets_mu_;

    int listen_fd_ = -1;

    void load_config(const string& path);
    void heartbeat_loop();
    bool ping_node(StorageNode& node, bool* process_restarted = nullptr);
    void handle_node_failure(const string& dead_id);
    void handle_node_recovery(const string& live_id);
    bool promote_node(StorageNode& node, const string& tablet_name);
    bool demote_and_sync_node(StorageNode& node, const StorageNode& primary, const string& tablet_name);
    bool add_replica_to_primary(StorageNode& primary, const StorageNode& replica, const string& tablet_name);
    void configure_initial_tablet_roles();

    void accept_loop();
    void handle_client(int fd);
    string handle_lookup(const string& row);
    string handle_read_lookup(const string& row);
    string handle_status();
    string handle_nodes();
    string handle_tablets();

    int create_listen_socket();
    bool read_line(int fd, string& line);
    int connect_host_port(const string& host, int port);
    int connect_node_kv(const StorageNode& node) { return connect_host_port(node.host, node.port); }
    int connect_node_repl(const StorageNode& node) { return connect_host_port(node.host, node.repl_port > 0 ? node.repl_port : node.port); }

    const TabletGroup* find_tablet(const string& row) const;
    void initialize_roles_from_tablets();
};

void Coordinator::load_config(const string& path) {
    ifstream f(path);
    if (!f) {
        cout << "[coord] no config file found at " << path << " -- using single-node defaults\n";
        StorageNode n;
        n.id = "node1"; n.host = "127.0.0.1"; n.port = 5000; n.repl_port = 5100; n.role = NodeRole::PRIMARY;
        nodes_[n.id] = n;
        TabletGroup tg;
        tg.name = "tablet0"; tg.row_start = ""; tg.row_end = ""; tg.node_ids = {"node1"};
        tg.active_primary_id = "node1";
        tablets_.push_back(tg);
        return;
    }

    string line;
    while (getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        istringstream ss(line);
        string type;
        ss >> type;
        if (type == "node") {
            StorageNode n;
            ss >> n.id >> n.host >> n.port;
            if (!(ss >> n.repl_port)) {
                n.repl_port = n.port;
            }
            nodes_[n.id] = n;
            cout << "[coord] registered node " << n.id << " at " << n.host << ":" << n.port
                      << " repl=" << n.repl_port << "\n";
        } else if (type == "tablet") {
            TabletGroup tg;
            ss >> tg.name >> tg.row_start >> tg.row_end;
            if (tg.row_start == "-" || tg.row_start == "*") tg.row_start.clear();
            if (tg.row_end == "-" || tg.row_end == "*") tg.row_end.clear();
            string nid;
            while (ss >> nid) tg.node_ids.push_back(nid);
            if (!tg.node_ids.empty()) tg.active_primary_id = tg.node_ids[0];
            tablets_.push_back(tg);
            cout << "[coord] tablet " << tg.name << " [" << tg.row_start << ", " << tg.row_end
                      << ") on " << tg.node_ids.size() << " nodes\n";
        }
    }
    initialize_roles_from_tablets();
}

void Coordinator::initialize_roles_from_tablets() {
    for (auto& [id, node] : nodes_) node.role = NodeRole::UNKNOWN;
    for (const auto& tg : tablets_) {
        const string primary_id = tg.active_primary_id.empty()
            ? (tg.node_ids.empty() ? string() : tg.node_ids[0])
            : tg.active_primary_id;
        for (size_t i = 0; i < tg.node_ids.size(); ++i) {
            auto it = nodes_.find(tg.node_ids[i]);
            if (it == nodes_.end()) continue;
            it->second.role = (tg.node_ids[i] == primary_id) ? NodeRole::PRIMARY : NodeRole::SECONDARY;
        }
    }
}

void Coordinator::run() {
    ::signal(SIGPIPE, SIG_IGN);
    running_ = true;
    listen_fd_ = create_listen_socket();
    cout << "[coord] listening on port " << cfg_.port << "\n";

    // Start heartbeat first so nodes get pinged and marked alive immediately.
    thread hb_thread([this] { heartbeat_loop(); });

    // Configure tablet roles in a detached thread so accept_loop() is never
    // blocked -- clients can connect right away regardless of how long setup
    // takes.  We wait until at least one node is confirmed alive (up to 5 s)
    // before sending BECOME_PRIMARY/SECONDARY, reducing the race where RPCs
    // arrive before the KV replication listeners are ready.
    thread([this] {
        for (int attempt = 0; attempt < 10 && running_; ++attempt) {
            this_thread::sleep_for(chrono::milliseconds(500));
            bool any_alive = false;
            {
                shared_lock<shared_mutex> nlk(nodes_mu_);
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
    vector<TabletGroup> tgs;
    {
        shared_lock<shared_mutex> tlk(tablets_mu_);
        tgs = tablets_;
    }
    // Retry because KV nodes may be alive on their client port before their
    // replication control listeners are fully ready.  Each round re-snapshots
    // node state so newly-alive nodes are picked up automatically.
    for (int round = 1; round <= 8 && running_; ++round) {
        bool all_ok = true;
        unordered_map<string, StorageNode> nodes_snapshot;
        {
            shared_lock<shared_mutex> nlk(nodes_mu_);
            nodes_snapshot = nodes_;
        }
        // 1) Promote designated primaries.
        for (const auto& tg : tgs) {
            if (tg.node_ids.empty()) continue;
            const string primary_id = tg.active_primary_id.empty() ? tg.node_ids[0] : tg.active_primary_id;
            auto it = nodes_snapshot.find(primary_id);
            if (it == nodes_snapshot.end() || !it->second.alive) { all_ok = false; continue; }
            StorageNode primary = it->second;
            if (!promote_node(primary, tg.name)) all_ok = false;
        }
        // 2) Sync secondaries and register them under the primary.
        for (const auto& tg : tgs) {
            if (tg.node_ids.empty()) continue;
            const string primary_id = tg.active_primary_id.empty() ? tg.node_ids[0] : tg.active_primary_id;
            auto pit = nodes_snapshot.find(primary_id);
            if (pit == nodes_snapshot.end() || !pit->second.alive) { all_ok = false; continue; }
            StorageNode primary = pit->second;
            for (const auto& node_id : tg.node_ids) {
                if (node_id == primary_id) continue;
                auto it = nodes_snapshot.find(node_id);
                if (it == nodes_snapshot.end() || !it->second.alive) { all_ok = false; continue; }
                StorageNode follower = it->second;
                if (!demote_and_sync_node(follower, primary, tg.name)) { all_ok = false; continue; }
                if (!add_replica_to_primary(primary, follower, tg.name)) all_ok = false;
            }
        }
        if (all_ok) {
            cout << "[coord] initial per-tablet roles configured successfully\n";
            roles_configured_.store(true);
            return;
        }
        cout << "[coord] initial role config round " << round << " incomplete, retrying...\n";
        this_thread::sleep_for(chrono::milliseconds(500));
    }
    roles_configured_.store(true);  // mark done even if partial -- unblock waiters
    cerr << "[coord] WARNING: initial tablet role configuration did not fully converge\n";
}
void Coordinator::heartbeat_loop() {
    while (running_) {
        this_thread::sleep_for(chrono::milliseconds(cfg_.hb_interval_ms));
        vector<string> newly_dead;
        vector<string> newly_live;

        vector<StorageNode> snapshot;
        {
            shared_lock<shared_mutex> lk(nodes_mu_);
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
                unique_lock<shared_mutex> lk(nodes_mu_);
                auto it = nodes_.find(node.id);
                if (it == nodes_.end()) continue;
                it->second.missed = node.missed;
                it->second.alive = node.alive;
                it->second.was_down = node.was_down;
                it->second.lsn = node.lsn;
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
        cout << "[coord] node " << node.id << " missed ping " << node.missed
                  << "/" << cfg_.hb_miss_thresh << "\n";
        if (node.alive && node.missed >= cfg_.hb_miss_thresh) {
            node.alive = false;
            node.was_down = true;
            cout << "[coord] node " << node.id << " marked DOWN\n";
            return true;
        }
        return false;
    }

    ::send(fd, "PING\r\n", 6, MSG_NOSIGNAL);
    string resp;
    bool ok = read_line(fd, resp);
    ::close(fd);

    if (ok && resp.rfind("+OK", 0) == 0) {
        bool was_alive = node.alive;
        bool recovering = node.was_down;
        node.missed = 0;
        node.alive = true;
        node.last_seen = chrono::steady_clock::now();
        auto lpos = resp.find("LSN=");
        if (lpos != string::npos) {
            try { node.lsn = stoull(resp.substr(lpos + 4)); } catch (...) {}
        }
        auto ppos = resp.find("PID=");
        if (ppos != string::npos) {
            int pid = 0;
            try { pid = stoi(resp.substr(ppos + 4)); } catch (...) { pid = 0; }
            if (pid > 0) {
                if (process_restarted && was_alive && !recovering &&
                    node.process_id > 0 && node.process_id != pid) {
                    *process_restarted = true;
                    cout << "[coord] node " << node.id
                              << " process restarted (pid " << node.process_id
                              << " -> " << pid << "), reapplying tablet roles\n";
                }
                node.process_id = pid;
            }
        }
        if (!was_alive && recovering) {
            node.was_down = false;
            cout << "[coord] node " << node.id << " recovered\n";
        }
        return false;
    }

    ++node.missed;
    cout << "[coord] node " << node.id << " bad ping " << node.missed
              << "/" << cfg_.hb_miss_thresh << "\n";
    if (node.alive && node.missed >= cfg_.hb_miss_thresh) {
        node.alive = false;
        node.was_down = true;
        cout << "[coord] node " << node.id << " marked DOWN after bad ping\n";
        return true;
    }
    return false;
}

bool Coordinator::promote_node(StorageNode& node, const string& tablet_name) {
    string cmd = "BECOME_PRIMARY " + tablet_name + "\r\n";
    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(node);
        if (fd < 0) {
            cerr << "[coord] failed to connect to repl port for node " << node.id
                      << " (attempt " << attempt << "/3)\n";
            this_thread::sleep_for(chrono::milliseconds(200));
            continue;
        }
        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);
        if (ok) {
            cout << "[coord] promoted " << node.id << " as new primary for " << tablet_name << "\n";
            return true;
        }
        cerr << "[coord] promotion failed for " << node.id << " on tablet " << tablet_name
                  << " (attempt " << attempt << "/3, resp='" << resp << "')\n";
        this_thread::sleep_for(chrono::milliseconds(200));
    }
    return false;
}

bool Coordinator::demote_and_sync_node(StorageNode& node, const StorageNode& primary, const string& tablet_name) {
    ostringstream oss;
    oss << "BECOME_SECONDARY " << tablet_name << " " << primary.host << " " << primary.repl_port << "\r\n";
    string cmd = oss.str();

    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(node);
        if (fd < 0) {
            cerr << "[coord] failed to connect to recovered node " << node.id
                      << " repl port (attempt " << attempt << "/3)\n";
            this_thread::sleep_for(chrono::milliseconds(200));
            continue;
        }

        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);

        if (ok) {
            cout << "[coord] node " << node.id << " synced from primary " << primary.id
                      << " for " << tablet_name << "\n";
            return true;
        }

        cerr << "[coord] failed to demote/sync " << node.id << " for tablet " << tablet_name
                  << " (attempt " << attempt << "/3, resp='" << resp << "')\n";
        this_thread::sleep_for(chrono::milliseconds(200));
    }
    return false;
}

bool Coordinator::add_replica_to_primary(StorageNode& primary, const StorageNode& replica, const string& tablet_name) {
    ostringstream oss;
    oss << "ADD_REPLICA " << tablet_name << " " << replica.id << " " << replica.host << " " << replica.repl_port << "\r\n";
    string cmd = oss.str();

    for (int attempt = 1; attempt <= 3; ++attempt) {
        int fd = connect_node_repl(primary);
        if (fd < 0) {
            cerr << "[coord] failed to connect to primary " << primary.id
                      << " repl port for add-replica (attempt " << attempt << "/3)\n";
            this_thread::sleep_for(chrono::milliseconds(200));
            continue;
        }

        bool sent = (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) >= 0);
        string resp;
        bool ok = sent && read_line(fd, resp) && resp.rfind("+OK", 0) == 0;
        ::close(fd);

        if (ok) {
            cout << "[coord] added recovered node " << replica.id << " back to " << tablet_name
                      << " under primary " << primary.id << "\n";
            return true;
        }

        cerr << "[coord] primary " << primary.id << " rejected add-replica for " << replica.id
                  << " on " << tablet_name << " (attempt " << attempt << "/3, resp='" << resp << "')\n";
        this_thread::sleep_for(chrono::milliseconds(200));
    }
    return false;
}

void Coordinator::handle_node_failure(const string& dead_id) {
    struct FailurePlan {
        string tablet_name;
        string dead_id;
        StorageNode new_primary;
        size_t best_idx = 0;
        vector<StorageNode> followers;
    };

    vector<FailurePlan> plans;
    {
        shared_lock<shared_mutex> nlk(nodes_mu_);
        shared_lock<shared_mutex> tlk(tablets_mu_);

        auto dead_it = nodes_.find(dead_id);
        if (dead_it == nodes_.end()) return;

        for (const auto& tg : tablets_) {
            if (tg.node_ids.empty()) continue;
            const string primary_id = tg.active_primary_id.empty() ? tg.node_ids[0] : tg.active_primary_id;
            if (primary_id != dead_id) continue;

            cout << "[coord] tablet " << tg.name << " primary " << dead_id << " is down, electing...\n";
            string best_id;
            uint64_t best_lsn = 0;
            size_t best_idx = 0;
            vector<StorageNode> followers;

            for (size_t i = 0; i < tg.node_ids.size(); ++i) {
                if (tg.node_ids[i] == dead_id) continue;
                auto it = nodes_.find(tg.node_ids[i]);
                if (it == nodes_.end() || !it->second.alive) continue;
                if (best_id.empty() || it->second.lsn >= best_lsn) {
                    best_lsn = it->second.lsn;
                    best_id = it->second.id;
                    best_idx = i;
                }
            }
            if (best_id.empty()) {
                cerr << "[coord] tablet " << tg.name << " has NO alive secondaries -- unavailable!\n";
                continue;
            }

            auto pit = nodes_.find(best_id);
            if (pit == nodes_.end()) continue;

            for (size_t i = 0; i < tg.node_ids.size(); ++i) {
                if (i == best_idx) continue;
                if (tg.node_ids[i] == dead_id) continue;
                auto it = nodes_.find(tg.node_ids[i]);
                if (it == nodes_.end() || !it->second.alive) continue;
                followers.push_back(it->second);
            }

            plans.push_back(FailurePlan{tg.name, dead_id, pit->second, best_idx, followers});
        }
    }

    for (const auto& plan : plans) {
        if (!promote_node(const_cast<StorageNode&>(plan.new_primary), plan.tablet_name)) {
            cerr << "[coord] promotion RPC failed for " << plan.new_primary.id << "\n";
            continue;
        }

        vector<string> healthy_followers;
        for (const auto& replica : plan.followers) {
            cout << "[coord] reconfiguring " << replica.id
                      << " as secondary of new primary " << plan.new_primary.id
                      << " for " << plan.tablet_name << "\n";

            if (!demote_and_sync_node(const_cast<StorageNode&>(replica), plan.new_primary, plan.tablet_name)) {
                cerr << "[coord] failed to re-sync replica " << replica.id
                          << " under new primary " << plan.new_primary.id
                          << " for " << plan.tablet_name << "\n";
                continue;
            }

            if (!add_replica_to_primary(const_cast<StorageNode&>(plan.new_primary), replica, plan.tablet_name)) {
                cerr << "[coord] failed to add replica " << replica.id
                          << " to new primary " << plan.new_primary.id
                          << " for " << plan.tablet_name << "\n";
                continue;
            }

            healthy_followers.push_back(replica.id);
        }

        {
            unique_lock<shared_mutex> nlk(nodes_mu_);
            unique_lock<shared_mutex> tlk(tablets_mu_);

            auto dead_it = nodes_.find(plan.dead_id);
            auto new_it = nodes_.find(plan.new_primary.id);
            if (new_it == nodes_.end()) continue;

            for (auto& tg : tablets_) {
                if (tg.name != plan.tablet_name) continue;
                tg.active_primary_id = plan.new_primary.id;
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

        cout << "[coord] promoted " << plan.new_primary.id
                  << " as new primary for " << plan.tablet_name << "\n";
    }
}

void Coordinator::handle_node_recovery(const string& live_id) {
    struct RecoveryPlan {
        string tablet_name;
        StorageNode live;
        StorageNode primary;
        vector<StorageNode> followers;
        bool recover_as_primary = false;
        bool swap_to_primary = false;
        string old_primary_id;
    };

    vector<RecoveryPlan> plans;
    {
        shared_lock<shared_mutex> nlk(nodes_mu_);
        shared_lock<shared_mutex> tlk(tablets_mu_);

        auto live_it = nodes_.find(live_id);
        if (live_it == nodes_.end()) return;

        for (const auto& tg : tablets_) {
            auto pos = find(tg.node_ids.begin(), tg.node_ids.end(), live_id);
            if (pos == tg.node_ids.end()) continue;
            const string configured_primary_id = tg.node_ids.empty() ? string() : tg.node_ids[0];
            const string active_primary_id = tg.active_primary_id.empty()
                ? configured_primary_id
                : tg.active_primary_id;

            if (live_id == configured_primary_id || live_id == active_primary_id) {
                vector<StorageNode> followers;
                for (const auto& node_id : tg.node_ids) {
                    if (node_id == live_id) continue;
                    auto fit = nodes_.find(node_id);
                    if (fit != nodes_.end() && fit->second.alive) followers.push_back(fit->second);
                }
                plans.push_back(RecoveryPlan{tg.name, live_it->second, {}, followers, true, false, ""});
                continue;
            }

            if (tg.node_ids.empty()) continue;
            auto pit = nodes_.find(active_primary_id);
            if (pit == nodes_.end() || !pit->second.alive) {
                cerr << "[coord] recovering node " << live_id
                          << " for " << tg.name
                          << ": no live primary; promoting recovered node\n";
                vector<StorageNode> followers;
                for (const auto& id : tg.node_ids) {
                    if (id == live_id) continue;
                    auto fit = nodes_.find(id);
                    if (fit != nodes_.end() && fit->second.alive) followers.push_back(fit->second);
                }
                plans.push_back(RecoveryPlan{tg.name, live_it->second, {}, followers, true, true, active_primary_id});
                continue;
            }

            plans.push_back(RecoveryPlan{tg.name, live_it->second, pit->second, {}, false, false, ""});
        }
    }

    for (const auto& plan : plans) {
            if (plan.recover_as_primary) {
                cout << "[coord] primary node " << live_id
                          << " recovered for " << plan.tablet_name
                          << " -- sending promote RPC\n";

            StorageNode live_copy = plan.live;
            if (!promote_node(live_copy, plan.tablet_name)) {
                cerr << "[coord] failed to re-promote recovered primary "
                          << live_id << " for " << plan.tablet_name << "\n";
                continue;
            }

            for (const auto& follower : plan.followers) {
                StorageNode follower_copy = follower;
                if (!demote_and_sync_node(follower_copy, live_copy, plan.tablet_name)) {
                    cerr << "[coord] failed to sync follower " << follower.id
                              << " after primary restart for " << plan.tablet_name << "\n";
                    continue;
                }
                if (!add_replica_to_primary(live_copy, follower_copy, plan.tablet_name)) {
                    cerr << "[coord] failed to re-add follower " << follower.id
                              << " after primary restart for " << plan.tablet_name << "\n";
                }
            }

            unique_lock<shared_mutex> nlk(nodes_mu_);
            unique_lock<shared_mutex> tlk(tablets_mu_);

            if (plan.swap_to_primary) {
                for (auto& tg : tablets_) {
                    if (tg.name != plan.tablet_name) continue;
                    tg.active_primary_id = live_id;
                    break;
                }
            } else {
                for (auto& tg : tablets_) {
                    if (tg.name != plan.tablet_name) continue;
                    if (!tg.node_ids.empty() && tg.node_ids[0] == live_id) tg.active_primary_id = live_id;
                    break;
                }
            }

            auto it = nodes_.find(live_id);
            if (it != nodes_.end()) {
                it->second.role = NodeRole::PRIMARY;
                it->second.alive = true;
            }
            for (const auto& follower : plan.followers) {
                auto fit = nodes_.find(follower.id);
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

        cout << "[coord] recovering node " << live_id
                  << " as secondary for " << plan.tablet_name
                  << " from primary " << plan.primary.id << "\n";

        StorageNode live_copy = plan.live;
        StorageNode primary_copy = plan.primary;

        if (!demote_and_sync_node(live_copy, primary_copy, plan.tablet_name)) {
            cerr << "[coord] failed to sync recovered node " << live_id
                      << " from primary " << plan.primary.id
                      << " for " << plan.tablet_name << "\n";
            continue;
        }

        if (!add_replica_to_primary(primary_copy, live_copy, plan.tablet_name)) {
            cerr << "[coord] failed to add recovered replica " << live_id
                      << " to primary " << plan.primary.id
                      << " for " << plan.tablet_name << "\n";
            continue;
        }

        unique_lock<shared_mutex> nlk(nodes_mu_);
        auto it = nodes_.find(live_id);
        if (it != nodes_.end()) {
            it->second.role = NodeRole::SECONDARY;
            it->second.alive = true;
        }
    }
}

const TabletGroup* Coordinator::find_tablet(const string& row) const {
    shared_lock<shared_mutex> lk(tablets_mu_);
    for (const auto& tg : tablets_) {
        bool after_start = tg.row_start.empty() || row >= tg.row_start;
        bool before_end = tg.row_end.empty() || row < tg.row_end;
        if (after_start && before_end) return &tg;
    }
    return nullptr;
}

string Coordinator::handle_lookup(const string& row) {
    TabletGroup tg_copy;
    {
        shared_lock<shared_mutex> tlk(tablets_mu_);
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
        shared_lock<shared_mutex> nlk(nodes_mu_);
        const string primary_id = tg_copy.active_primary_id.empty()
            ? (tg_copy.node_ids.empty() ? string() : tg_copy.node_ids[0])
            : tg_copy.active_primary_id;
        if (!primary_id.empty()) {
            auto pit = nodes_.find(primary_id);
            if (pit != nodes_.end() && pit->second.alive) {
                const StorageNode& n = pit->second;
                return "+OK " + n.host + " " + to_string(n.port) + " primary\r\n";
            }
        }
        for (const auto& node_id : tg_copy.node_ids) {
            if (node_id == primary_id) continue;
            auto it = nodes_.find(node_id);
            if (it == nodes_.end() || !it->second.alive) continue;
            const StorageNode& n = it->second;
            return "+OK " + n.host + " " + to_string(n.port) + " secondary\r\n";
        }
    }
    return "-ERR all replicas down\r\n";
}


string Coordinator::handle_read_lookup(const string& row) {
    vector<string> candidate_ids;
    {
        shared_lock<shared_mutex> tlk(tablets_mu_);
        const TabletGroup* tg = nullptr;
        for (const auto& t : tablets_) {
            bool after_start = t.row_start.empty() || row >= t.row_start;
            bool before_end = t.row_end.empty() || row < t.row_end;
            if (after_start && before_end) { tg = &t; break; }
        }
        if (!tg) return "-ERR no tablet for row\r\n";
        const string primary_id = tg->active_primary_id.empty()
            ? (tg->node_ids.empty() ? string() : tg->node_ids[0])
            : tg->active_primary_id;
        for (const auto& node_id : tg->node_ids) {
            if (node_id != primary_id) candidate_ids.push_back(node_id);
        }
        if (!primary_id.empty()) candidate_ids.push_back(primary_id);
    }

    {
        shared_lock<shared_mutex> nlk(nodes_mu_);
        for (size_t i = 0; i < candidate_ids.size(); ++i) {
            auto it = nodes_.find(candidate_ids[i]);
            if (it == nodes_.end() || !it->second.alive) continue;
            const StorageNode& n = it->second;
            const bool is_last = (i + 1 == candidate_ids.size());
            return "+OK " + n.host + " " + to_string(n.port) + " " + (is_last ? "primary" : "secondary") + "\r\n";
        }
    }
    return "-ERR all replicas down\r\n";
}

string Coordinator::handle_tablets() {
    shared_lock<shared_mutex> nlk(nodes_mu_);
    shared_lock<shared_mutex> tlk(tablets_mu_);

    ostringstream js;
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
        const string primary_id = tg.active_primary_id.empty()
            ? (tg.node_ids.empty() ? string() : tg.node_ids[0])
            : tg.active_primary_id;
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
            js << "\"role\":\"" << (n.id == primary_id ? "primary" : "secondary") << "\"";
            js << "}";
        }

        js << "]";
        js << "}";
    }

    js << "]";
    string body = js.str();
    return "+OK " + to_string(body.size()) + "\r\n" + body;
}

string Coordinator::handle_status() {
    shared_lock<shared_mutex> nlk(nodes_mu_);
    ostringstream js;
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
    string body = js.str();
    return "+OK " + to_string(body.size()) + "\r\n" + body;
}

string Coordinator::handle_nodes() { return handle_status(); }

void Coordinator::accept_loop() {
    while (running_) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        if (cfd < 0) continue;
        thread([this, cfd] {
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

    string line;
    while (read_line(fd, line)) {
        if (line.empty()) continue;
        istringstream ss(line);
        string op;
        ss >> op;
        if (op == "LOOKUP") {
            uint32_t rowlen = 0;
            ss >> rowlen;
            string row;
            if (!coord_read_exact(fd, row, rowlen)) break;
            string resp = handle_lookup(row);
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "READLOOKUP") {
            uint32_t rowlen = 0;
            ss >> rowlen;
            string row;
            if (!coord_read_exact(fd, row, rowlen)) break;
            string resp = handle_read_lookup(row);
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "STATUS" || op == "NODES") {
            string resp = handle_status();
            ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        } else if (op == "TABLETS") {
            string resp = handle_tablets();
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
    if (fd < 0) throw runtime_error("coord socket() failed");
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
        throw runtime_error("coord bind() failed");
    }
    if (::listen(fd, 64) < 0) {
        ::close(fd);
        throw runtime_error("coord listen() failed");
    }
    return fd;
}

bool Coordinator::read_line(int fd, string& line) {
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

int Coordinator::connect_host_port(const string& host, int port) {
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
        string a = argv[i];
        if      (a == "--port"   && i + 1 < argc) cfg.port = stoi(argv[++i]);
        else if (a == "--config" && i + 1 < argc) cfg.config_file = argv[++i];
    }

    cout << "=== PennCloud Coordinator ===\n"
              << "  port:   " << cfg.port << "\n"
              << "  config: " << cfg.config_file << "\n\n";

    Coordinator coord(cfg);
    g_coord = &coord;
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    coord.run();
    return 0;
}
