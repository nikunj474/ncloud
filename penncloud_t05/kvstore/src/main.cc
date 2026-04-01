// main.cc  --  PennCloud KV server entry point
// Usage:
//   ./kvserver [--port 5000] [--threads 16] [--data ./data]
//              [--tablet tablet0] [--ckpt-interval 300]
//
// Example:
//   ./kvserver --port 5000 --data /var/penncloud/data --tablet tablet_aa_af


#include "server.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <string>
#include <stdexcept>

// Global server pointer for signal handler
static KVServer* g_server = nullptr;

static void signal_handler(int sig) {
    std::cout << "\n[main] caught signal " << sig
              << ", shutting down cleanly...\n";
    if (g_server) g_server->stop();
}

static KVServer::Config parse_args(int argc, char* argv[]) {
    KVServer::Config cfg;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--port"         && i+1 < argc) cfg.port          = std::stoi(argv[++i]);
        else if (arg == "--threads" && i+1 < argc) cfg.threads       = std::stoi(argv[++i]);
        else if (arg == "--data"    && i+1 < argc) cfg.data_dir      = argv[++i];
        else if (arg == "--tablet"  && i+1 < argc) cfg.tablet_name   = argv[++i];
        else if (arg == "--ckpt-interval" && i+1 < argc)
            cfg.ckpt_interval = std::stoi(argv[++i]);
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            std::exit(1);
        }
    }
    return cfg;
}

int main(int argc, char* argv[]) {
    KVServer::Config cfg = parse_args(argc, argv);

    std::cout << "=== PennCloud KV Server ===\n"
              << "  port:          " << cfg.port          << "\n"
              << "  threads:       " << cfg.threads        << "\n"
              << "  data_dir:      " << cfg.data_dir       << "\n"
              << "  tablet:        " << cfg.tablet_name    << "\n"
              << "  ckpt_interval: " << cfg.ckpt_interval  << "s\n\n";

    try {
        KVServer server(cfg);
        g_server = &server;

        // Graceful shutdown on Ctrl-C or SIGTERM
        std::signal(SIGINT,  signal_handler);
        std::signal(SIGTERM, signal_handler);

        server.run();

        // Final checkpoint on clean shutdown
        std::cout << "[main] final checkpoint on shutdown...\n";
        server.tablet().checkpoint();
        std::cout << "[main] done. goodbye.\n";

    } catch (const std::exception& e) {
        std::cerr << "[main] fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
