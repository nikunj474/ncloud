// main.cc  --  NCloud KV server entry point


#include "server.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <atomic>

static KVServer* g_server = nullptr;
static std::atomic<bool> g_shutdown_started{false};

static void signal_handler(int sig) {
    if (g_shutdown_started.exchange(true)) return;
    std::cout << "\n[main] caught signal " << sig
              << ", shutting down cleanly...\n";
    if (g_server) g_server->stop();
}

static void parse_replica_arg(const std::string& spec,
                              KVServer::Config& cfg,
                              int& replica_index) {
    std::string id;
    std::string hostport = spec;

    auto at = spec.find('@');
    if (at != std::string::npos) {
        id = spec.substr(0, at);
        hostport = spec.substr(at + 1);
    }

    auto colon = hostport.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon == hostport.size() - 1) {
        throw std::runtime_error("invalid --replica spec: " + spec);
    }

    std::string host = hostport.substr(0, colon);
    int port = std::stoi(hostport.substr(colon + 1));
    if (id.empty()) id = "replica" + std::to_string(replica_index++);

    cfg.replicas.emplace_back(id, host, port);
    cfg.enable_replication = true;
}

// Parse --tablet name:row_start:row_end  or  --tablet name  (legacy, full range)
static void parse_tablet_arg(const std::string& spec, KVServer::Config& cfg) {
    KVServer::HostedTabletConfig ht;
    auto first = spec.find(':');
    if (first == std::string::npos) {
        // Legacy single-tablet: just a name, full row range.
        ht.name      = spec;
        ht.row_start = "";
        ht.row_end   = "";
    } else {
        ht.name = spec.substr(0, first);
        auto second = spec.find(':', first + 1);
        if (second == std::string::npos) {
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

static KVServer::Config parse_args(int argc, char* argv[]) {
    KVServer::Config cfg;
    int replica_index = 1;
    bool saw_node_id = false;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            std::cout
                << "Usage: ./kvserver [options]\n"
                << "  --port <client_port>\n"
                << "  --threads <num_threads>\n"
                << "  --data <data_dir>\n"
                << "  --tablet <name>[:<row_start>:<row_end>]  (repeatable)\n"
                << "  --ckpt-interval <seconds>\n"
                << "  --node-id <id>\n"
                << "  --repl-port <replication_port>\n"
                << "  --replica <id@host:repl_port>\n";
            std::exit(0);
        }
        else if (arg == "--port"         && i+1 < argc) cfg.port = std::stoi(argv[++i]);
        else if (arg == "--threads"      && i+1 < argc) cfg.threads = std::stoi(argv[++i]);
        else if (arg == "--data"         && i+1 < argc) cfg.data_dir = argv[++i];
        else if (arg == "--tablet"       && i+1 < argc) parse_tablet_arg(argv[++i], cfg);
        else if (arg == "--ckpt-interval"&& i+1 < argc) cfg.ckpt_interval = std::stoi(argv[++i]);
        else if (arg == "--node-id"      && i+1 < argc) { cfg.node_id = argv[++i]; saw_node_id = true; }
        else if (arg == "--repl-port"    && i+1 < argc) cfg.repl_port = std::stoi(argv[++i]);
        else if (arg == "--replica"      && i+1 < argc) parse_replica_arg(argv[++i], cfg, replica_index);
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            std::exit(1);
        }
    }

    if (saw_node_id) cfg.enable_replication = true;
    return cfg;
}

int main(int argc, char* argv[]) {
    KVServer::Config cfg = parse_args(argc, argv);

    std::cout << "=== NCloud KV Server ===\n"
              << "  port:          " << cfg.port << "\n"
              << "  threads:       " << cfg.threads << "\n"
              << "  data_dir:      " << cfg.data_dir << "\n"
              << "  tablets:       " << cfg.hosted_tablets.size()
              <<   (cfg.hosted_tablets.empty() ? " (legacy: " + cfg.tablet_name + ")" : "") << "\n";
    for (const auto& ht : cfg.hosted_tablets)
        std::cout << "    " << ht.name << " [" << ht.row_start << ", " << ht.row_end << ")\n";
    std::cout << "  ckpt_interval: " << cfg.ckpt_interval << "s\n"
              << "  node_id:       " << cfg.node_id << "\n"
              << "  repl_port:     " << cfg.repl_port << "\n"
              << "  replication:   " << (cfg.enable_replication ? "enabled" : "standalone") << "\n\n";

    try {
        KVServer server(cfg);
        g_server = &server;

        std::signal(SIGINT,  signal_handler);
        std::signal(SIGTERM, signal_handler);

        server.run();

        if (!g_shutdown_started.exchange(true)) {
            server.stop();
        }
        std::cout << "[main] final checkpoint on shutdown...\n";
        server.checkpoint_all();
        std::cout << "[main] done. goodbye.\n";

    } catch (const std::exception& e) {
        std::cerr << "[main] fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
