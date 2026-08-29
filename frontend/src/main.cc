// main.cc  --  NCloud Frontend entry point

// Usage:
//   ./feserver [--port 8080] [--kv-host 127.0.0.1] [--kv-port 5000]
//              [--threads 32] [--id fe1] [--redirect-to http://host:port]
//              [--page-not-found-stub]


#include "fe_server.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <stdexcept>

static FEServer* g_server = nullptr;

static void signal_handler(int sig) {
    std::cout << "\n[feserver] caught signal " << sig << ", stopping...\n";
    if (g_server) g_server->stop();
}

static FEServer::Config parse_args(int argc, char* argv[]) {
    FEServer::Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--port"       && i+1 < argc) cfg.port       = std::stoi(argv[++i]);
        else if (a == "--kv-host"    && i+1 < argc) cfg.kv_host    = argv[++i];
        else if (a == "--kv-port"    && i+1 < argc) cfg.kv_port    = std::stoi(argv[++i]);
        else if (a == "--threads"    && i+1 < argc) cfg.threads    = std::stoi(argv[++i]);
        else if (a == "--id"         && i+1 < argc) cfg.server_id  = argv[++i];
        else if (a == "--static"     && i+1 < argc) cfg.static_dir = argv[++i];
        else if (a == "--coord-host" && i+1 < argc) cfg.coord_host = argv[++i];
        else if (a == "--coord-port" && i+1 < argc) cfg.coord_port = std::stoi(argv[++i]);
        else if (a == "--redirect-to"&& i+1 < argc) cfg.redirect_to= argv[++i];
        else if (a == "--page-not-found-stub") cfg.page_not_found_stub = true;
    }
    return cfg;
}

int main(int argc, char* argv[]) {
    FEServer::Config cfg = parse_args(argc, argv);

    if (cfg.page_not_found_stub) {
        std::cout << "[feserver] 404-stub: port " << cfg.port << "\n";
    } else if (!cfg.redirect_to.empty()) {
        std::cout << "[feserver] redirect-stub: port " << cfg.port
                  << " -> " << cfg.redirect_to << "\n";
    } else {
        std::cout << "=== NCloud Frontend Server ===\n"
                  << "  port:    " << cfg.port     << "\n"
                  << "  kv:      " << cfg.kv_host << ":" << cfg.kv_port << "\n"
                  << "  threads: " << cfg.threads   << "\n"
                  << "  id:      " << cfg.server_id << "\n\n";
    }

    try {
        FEServer server(cfg);
        g_server = &server;
        std::signal(SIGINT,  signal_handler);
        std::signal(SIGTERM, signal_handler);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "[feserver] fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
