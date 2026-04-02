// =============================================================================
// main.cc  --  PennCloud Frontend entry point
// =============================================================================
// Usage:
//   ./feserver [--port 8080] [--kv-host 127.0.0.1] [--kv-port 5000]
//              [--threads 32] [--id fe1]
// =============================================================================

#include "fe_server.h"
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <stdexcept>

using namespace std;

static FEServer* g_server = nullptr;

static void signal_handler(int sig) {
    cout << "\n[feserver] caught signal " << sig << ", stopping...\n";
    if (g_server) g_server->stop();
}

static FEServer::Config parse_args(int argc, char* argv[]) {
    FEServer::Config cfg;
    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if      (a == "--port"     && i+1 < argc) cfg.port     = stoi(argv[++i]);
        else if (a == "--kv-host"  && i+1 < argc) cfg.kv_host  = argv[++i];
        else if (a == "--kv-port"  && i+1 < argc) cfg.kv_port  = stoi(argv[++i]);
        else if (a == "--threads"  && i+1 < argc) cfg.threads  = stoi(argv[++i]);
        else if (a == "--id"       && i+1 < argc) cfg.server_id= argv[++i];
        else if (a == "--static"   && i+1 < argc) cfg.static_dir = argv[++i];
    }
    return cfg;
}

int main(int argc, char* argv[]) {
    FEServer::Config cfg = parse_args(argc, argv);

    cout << "=== PennCloud Frontend Server ===\n"
              << "  port:    " << cfg.port     << "\n"
              << "  kv:      " << cfg.kv_host << ":" << cfg.kv_port << "\n"
              << "  threads: " << cfg.threads   << "\n"
              << "  id:      " << cfg.server_id << "\n\n";

    try {
        FEServer server(cfg);
        g_server = &server;
        signal(SIGINT,  signal_handler);
        signal(SIGTERM, signal_handler);
        server.run();
    } catch (const exception& e) {
        cerr << "[feserver] fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
