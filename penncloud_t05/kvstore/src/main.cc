// main.cc  --  PennCloud KV server entry point
// Usage:
//   ./kvserver [--port 5000] [--threads 16] [--data ./data]
//              [--tablet tablet0] [--tablet-range name:start:end]*
//              [--ckpt-interval 300]
//
// Example:
//   ./kvserver --port 5000 --data /var/penncloud/data --tablet tablet_aa_af

#include "server.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <string>
#include <stdexcept>
using namespace std;

// Global server pointer for signal handler
static KVServer* g_server = nullptr;

static void signal_handler(int sig) {
    cout << "\n[main] caught signal " << sig
              << ", shutting down cleanly...\n";
    if (g_server) g_server->stop();
}

static KVServer::Config parse_args(int argc, char* argv[]) {
    KVServer::Config cfg;
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "--port"         && i+1 < argc) cfg.port          = stoi(argv[++i]);
        else if (arg == "--threads" && i+1 < argc) cfg.threads       = stoi(argv[++i]);
        else if (arg == "--data"    && i+1 < argc) cfg.data_dir      = argv[++i];
        else if (arg == "--tablet"  && i+1 < argc) cfg.tablet_name   = argv[++i];
        else if (arg == "--tablet-range" && i+1 < argc) cfg.tablet_specs.push_back(argv[++i]);
        else if (arg == "--ckpt-interval" && i+1 < argc)
            cfg.ckpt_interval = stoi(argv[++i]);
        else if (arg == "--node-id" && i+1 < argc) cfg.node_id = argv[++i];
        else if (arg == "--repl-port" && i+1 < argc) cfg.repl_port = stoi(argv[++i]);
        else if (arg == "--role" && i+1 < argc) {
            string role = argv[++i];
            cfg.is_primary = (role == "primary");
        } else if (arg == "--replica" && i+1 < argc) {
            cfg.replica_specs.push_back(argv[++i]);
        }
        else {
            cerr << "Unknown argument: " << arg << "\n";
            exit(1);
        }
    }
    return cfg;
}

int main(int argc, char* argv[]) {
    KVServer::Config cfg = parse_args(argc, argv);

    cout << "=== PennCloud KV Server ===\n"
              << "  port:          " << cfg.port          << "\n"
              << "  threads:       " << cfg.threads        << "\n"
              << "  data_dir:      " << cfg.data_dir       << "\n"
              << "  tablet:        " << cfg.tablet_name    << "\n"
              << "  tablet_specs:  " << cfg.tablet_specs.size() << "\n"
              << "  ckpt_interval: " << cfg.ckpt_interval  << "s\n"
              << "  node_id:       " << cfg.node_id        << "\n"
              << "  repl_port:     " << cfg.repl_port      << "\n"
              << "  role:          " << (cfg.is_primary ? "primary" : "secondary")
              << "\n\n";

    try {
        KVServer server(cfg);
        g_server = &server;

        // Graceful shutdown on Ctrl-C or SIGTERM
        signal(SIGINT,  signal_handler);
        signal(SIGTERM, signal_handler);

        server.run();

        // Final checkpoint on clean shutdown
        cout << "[main] final checkpoint on shutdown...\n";
        server.checkpoint_all();
        cout << "[main] done. goodbye.\n";

    } catch (const exception& e) {
        cerr << "[main] fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
