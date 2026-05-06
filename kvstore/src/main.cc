// main.cc  --  PennCloud KV server entry point

#include "server.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <string>
#include <stdexcept>
using namespace std;

static KVServer* g_server = nullptr;

// Convert SIGINT/SIGTERM into KVServer::stop() so shutdown checkpoints run.
static void signal_handler(int sig) {
    cout << "\n[main] caught signal " << sig
              << ", shutting down cleanly...\n";
    if (g_server) g_server->stop();
}

// Parse --replica id@host:repl_port and add it to the outbound peer list.
static void parse_replica_arg(const string& spec,
                              KVServer::Config& cfg,
                              int& replica_index) {
    string id;
    string hostport = spec;

    auto at = spec.find('@');
    if (at != string::npos) {
        id = spec.substr(0, at);
        hostport = spec.substr(at + 1);
    }

    auto colon = hostport.rfind(':');
    if (colon == string::npos || colon == 0 || colon == hostport.size() - 1) {
        throw runtime_error("invalid --replica spec: " + spec);
    }

    string host = hostport.substr(0, colon);
    int port = stoi(hostport.substr(colon + 1));
    if (id.empty()) id = "replica" + to_string(replica_index++);

    cfg.replicas.emplace_back(id, host, port);
    cfg.enable_replication = true;
}

// Parse --tablet name:row_start:row_end or --tablet name into a hosted tablet.
static void parse_tablet_arg(const string& spec, KVServer::Config& cfg) {
    KVServer::HostedTabletConfig ht;
    auto first = spec.find(':');
    if (first == string::npos) {
        // Legacy single-tablet: just a name, full row range.
        ht.name      = spec;
        ht.row_start = "";
        ht.row_end   = "";
    } else {
        ht.name = spec.substr(0, first);
        auto second = spec.find(':', first + 1);
        if (second == string::npos) {
            // name:row_start  (no end bound)
            ht.row_start = spec.substr(first + 1);
            ht.row_end   = "";
        } else {
            ht.row_start = spec.substr(first + 1, second - first - 1);
            ht.row_end   = spec.substr(second + 1);
        }
    }
    // Keep cfg.tablet_name pointing at the first tablet for backward compat.
    if (cfg.hosted_tablets.empty()) cfg.tablet_name = ht.name;
    cfg.hosted_tablets.push_back(ht);
}

// Convert CLI flags into a KVServer::Config used by the server constructor.
static KVServer::Config parse_args(int argc, char* argv[]) {
    KVServer::Config cfg;
    int replica_index = 1;
    bool saw_node_id = false;
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            cout
                << "Usage: ./kvserver [options]\n"
                << "  --port <client_port>\n"
                << "  --threads <num_threads>\n"
                << "  --data <data_dir>\n"
                << "  --tablet <name>[:<row_start>:<row_end>]  (repeatable)\n"
                << "  --ckpt-interval <seconds>\n"
                << "  --node-id <id>\n"
                << "  --repl-port <replication_port>\n"
                << "  --replica <id@host:repl_port>\n";
            exit(0);
        }
        else if (arg == "--port"         && i+1 < argc) cfg.port = stoi(argv[++i]);
        else if (arg == "--threads"      && i+1 < argc) cfg.threads = stoi(argv[++i]);
        else if (arg == "--data"         && i+1 < argc) cfg.data_dir = argv[++i];
        else if (arg == "--tablet"       && i+1 < argc) parse_tablet_arg(argv[++i], cfg);
        else if (arg == "--ckpt-interval"&& i+1 < argc) cfg.ckpt_interval = stoi(argv[++i]);
        else if (arg == "--node-id"      && i+1 < argc) { cfg.node_id = argv[++i]; saw_node_id = true; }
        else if (arg == "--repl-port"    && i+1 < argc) cfg.repl_port = stoi(argv[++i]);
        else if (arg == "--replica"      && i+1 < argc) parse_replica_arg(argv[++i], cfg, replica_index);
        else {
            cerr << "Unknown argument: " << arg << "\n";
            exit(1);
        }
    }

    if (saw_node_id) cfg.enable_replication = true;
    return cfg;
}

// Program entry point: parse config, install signals, run server, checkpoint on exit.
int main(int argc, char* argv[]) {
    KVServer::Config cfg = parse_args(argc, argv);

    cout << "=== PennCloud KV Server ===\n"
              << "  port:          " << cfg.port << "\n"
              << "  threads:       " << cfg.threads << "\n"
              << "  data_dir:      " << cfg.data_dir << "\n"
              << "  tablets:       " << cfg.hosted_tablets.size()
              <<   (cfg.hosted_tablets.empty() ? " (legacy: " + cfg.tablet_name + ")" : "") << "\n";
    for (const auto& ht : cfg.hosted_tablets)
        cout << "    " << ht.name << " [" << ht.row_start << ", " << ht.row_end << ")\n";
    cout << "  ckpt_interval: " << cfg.ckpt_interval << "s\n"
              << "  node_id:       " << cfg.node_id << "\n"
              << "  repl_port:     " << cfg.repl_port << "\n"
              << "  replication:   " << (cfg.enable_replication ? "enabled" : "standalone") << "\n\n";

    try {
        KVServer server(cfg);
        g_server = &server;

        signal(SIGINT,  signal_handler);
        signal(SIGTERM, signal_handler);

        server.run();

        cout << "[main] final checkpoint on shutdown...\n";
        server.checkpoint_all();
        cout << "[main] done. goodbye.\n";

    } catch (const exception& e) {
        cerr << "[main] fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
