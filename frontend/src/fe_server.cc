// =============================================================================
// fe_server.cc  --  Frontend server implementation
// =============================================================================

#include "fe_server.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <condition_variable>
#include <cstring>
#include <cerrno>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>
#include <chrono>
#include <thread>
#include <map>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <cstdlib>
#include <cctype>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#ifdef __APPLE__
#  include <mach-o/dyld.h>
#endif


struct AdminFrontendNode {
    std::string id;
    std::string host;
    int port = -1;
    bool alive = false;
};

static bool frontend_admin_status_ok_admin(int port);

static std::string peer_addr_admin(int fd) {
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return "";

    char buf[INET6_ADDRSTRLEN] = {0};
    if (addr.ss_family == AF_INET) {
        auto* in = reinterpret_cast<sockaddr_in*>(&addr);
        if (::inet_ntop(AF_INET, &in->sin_addr, buf, sizeof(buf))) return buf;
    } else if (addr.ss_family == AF_INET6) {
        auto* in6 = reinterpret_cast<sockaddr_in6*>(&addr);
        if (::inet_ntop(AF_INET6, &in6->sin6_addr, buf, sizeof(buf))) return buf;
    }
    return "";
}

static bool is_loopback_peer_admin(const std::string& addr) {
    if (addr == "127.0.0.1" || addr == "::1" || addr == "::ffff:127.0.0.1") return true;
    auto starts_with = [](const std::string& s, const std::string& p) {
        return s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
    };
    if (starts_with(addr, "10.") || starts_with(addr, "192.168.")) return true;
    if (starts_with(addr, "172.")) {
        size_t dot = addr.find('.', 4);
        if (dot != std::string::npos) {
            int second = std::atoi(addr.substr(4, dot - 4).c_str());
            if (second >= 16 && second <= 31) return true;
        }
    }
    return false;
}

static bool tcp_probe_admin(const std::string& host, int port, int timeout_ms = 300) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_aton(host.c_str(), &addr.sin_addr) == 0) {
        ::close(fd);
        return false;
    }

    bool ok = (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    ::close(fd);
    return ok;
}

static bool read_line_admin(int fd, std::string& line) {
    line.clear();
    char c = 0;
    while (true) {
        ssize_t r = ::recv(fd, &c, 1, 0);
        if (r <= 0) return false;
        if (c == '\n') {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            return true;
        }
        line.push_back(c);
        if (line.size() > 8192) return false;
    }
}

static bool read_exact_admin(int fd, std::string& out, size_t n) {
    out.assign(n, '\0');
    size_t off = 0;
    while (off < n) {
        ssize_t r = ::recv(fd, &out[off], n - off, 0);
        if (r <= 0) return false;
        off += static_cast<size_t>(r);
    }
    return true;
}

static bool query_coordinator_json_admin(const std::string& host, int port,
                                         const std::string& op,
                                         std::string& json_out,
                                         int timeout_ms = 180) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_aton(host.c_str(), &addr.sin_addr) == 0) {
        ::close(fd);
        return false;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return false;
    }

    const std::string req = op + "\r\n";
    if (::send(fd, req.data(), req.size(), MSG_NOSIGNAL) < 0) {
        ::close(fd);
        return false;
    }

    std::string line;
    if (!read_line_admin(fd, line)) {
        ::close(fd);
        return false;
    }

    if (line.rfind("+OK ", 0) != 0) {
        ::close(fd);
        return false;
    }

    size_t body_len = 0;
    try {
        body_len = static_cast<size_t>(std::stoul(line.substr(4)));
    } catch (...) {
        ::close(fd);
        return false;
    }

    bool ok = read_exact_admin(fd, json_out, body_len);
    ::close(fd);
    return ok;
}

static bool query_coordinator_status_json(const std::string& host, int port, std::string& json_out) {
    return query_coordinator_json_admin(host, port, "STATUS", json_out);
}

static bool query_coordinator_tablets_json(const std::string& host, int port, std::string& json_out) {
    return query_coordinator_json_admin(host, port, "TABLETS", json_out);
}

static bool query_kv_dump_json_admin(const std::string& host, int port,
                                     size_t limit, size_t offset,
                                     std::string& json_out) {
    if (port <= 0) return false;
    const std::string op = "DUMP " + std::to_string(limit) + " " + std::to_string(offset);
    return query_coordinator_json_admin(host, port, op, json_out, 700);
}

static std::vector<AdminFrontendNode> admin_frontend_nodes() {
    std::vector<AdminFrontendNode> out;
    out.push_back({"fe1", "127.0.0.1", 8090, false});
    out.push_back({"fe2", "127.0.0.1", 8091, false});
    out.push_back({"fe3", "127.0.0.1", 8092, false});
    for (auto& n : out) n.alive = frontend_admin_status_ok_admin(n.port);
    return out;
}

static std::string frontend_nodes_json() {
    auto nodes = admin_frontend_nodes();
    std::string json = "[";
    bool first = true;
    for (const auto& n : nodes) {
        if (!first) json += ",";
        first = false;
        json += "{";
        json += "\"id\":" + json_str(n.id);
        json += ",\"host\":" + json_str(n.host);
        json += ",\"port\":" + std::to_string(n.port);
        json += ",\"alive\":" + std::string(n.alive ? "true" : "false");
        json += "}";
    }
    json += "]";
    return json;
}

static bool is_spa_shell_route(const std::string& path) {
    return path == "/" ||
           path == "/index.html" ||
           path == "/inbox" ||
           path == "/compose" ||
           path == "/drive" ||
           path == "/chat" ||
           path == "/email" ||
           path == "/settings";
}

static std::string admin_json_field(const std::string& body, const std::string& key) {
    std::string pat = "\"" + key + "\"";
    size_t p = body.find(pat);
    if (p == std::string::npos) return "";
    p = body.find(':', p + pat.size());
    if (p == std::string::npos) return "";
    ++p;
    while (p < body.size() && std::isspace(static_cast<unsigned char>(body[p]))) ++p;
    if (p >= body.size()) return "";
    if (body[p] == '"') {
        ++p;
        std::string out;
        while (p < body.size()) {
            char c = body[p++];
            if (c == '\\' && p < body.size()) {
                out.push_back(body[p++]);
                continue;
            }
            if (c == '"') break;
            out.push_back(c);
        }
        return out;
    }
    size_t start = p;
    while (p < body.size() && body[p] != ',' && body[p] != '}' && !std::isspace(static_cast<unsigned char>(body[p]))) ++p;
    return body.substr(start, p - start);
}

static std::string admin_location_token(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '_' || c == '-' || c == '.' || c == ',') {
            out.push_back(static_cast<char>(c));
        }
    }
    return out;
}

static bool run_shell_command(const std::string& cmd) {
    int rc = ::system(cmd.c_str());
    return rc == 0;
}

static void admin_log(const std::string& msg) {
    std::cout << "[admin] " << msg << "\n" << std::flush;
}

static std::string shell_escape_admin(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "'\\''";
        else out.push_back(c);
    }
    out += "'";
    return out;
}

struct AdminBackendSpec {
    std::string id;
    int kv_port = -1;
    int repl_port = -1;
    std::string data_dir;
    std::string log_path;
    std::vector<std::string> extra_args;
};

struct AdminClusterSpec {
    std::string name;
    std::string coord_host = "127.0.0.1";
    int coord_port = -1;
    int frontend_kv_port = -1;
    std::map<std::string, AdminBackendSpec> backends;
};

static bool file_exists_admin(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0;
}

static std::string first_existing_path_admin(const std::vector<std::string>& paths) {
    for (const auto& p : paths) {
        if (file_exists_admin(p)) return p;
    }
    return paths.empty() ? std::string() : paths.front();
}

static std::string resolve_project_root_admin() {
    std::string root;
#ifdef __linux__
    char buf[4096] = {};
    ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n > 0) {
        std::string exe(buf, static_cast<size_t>(n));
        auto pos = exe.rfind("/frontend/");
        if (pos != std::string::npos) root = exe.substr(0, pos);
    }
#elif defined(__APPLE__)
    char buf[4096] = {};
    uint32_t sz = sizeof(buf);
    if (_NSGetExecutablePath(buf, &sz) == 0) {
        std::string exe(buf);
        auto pos = exe.rfind("/frontend/");
        if (pos != std::string::npos) root = exe.substr(0, pos);
    }
#endif
    if (root.empty()) {
        char cwd[4096] = {};
        if (::getcwd(cwd, sizeof(cwd))) root = cwd;
    }
    if (file_exists_admin(root + "/frontend/feserver") && file_exists_admin(root + "/kvstore/kvserver")) return root;
    if (file_exists_admin(root + "/feserver") && file_exists_admin(root + "/../kvstore/kvserver")) return root + "/..";
    if (file_exists_admin(root + "/../frontend/feserver") && file_exists_admin(root + "/../kvstore/kvserver")) return root + "/..";
    return root;
}

static std::string kv_binary_path_admin(const std::string& root) {
    if (file_exists_admin(root + "/kvstore/kvserver")) return root + "/kvstore/kvserver";
    if (file_exists_admin(root + "/../kvstore/kvserver")) return root + "/../kvstore/kvserver";
    return root + "/kvstore/kvserver";
}

static std::string fe_binary_path_admin(const std::string& root) {
    if (file_exists_admin(root + "/frontend/feserver")) return root + "/frontend/feserver";
    if (file_exists_admin(root + "/feserver")) return root + "/feserver";
    if (file_exists_admin(root + "/../frontend/feserver")) return root + "/../frontend/feserver";
    return root + "/frontend/feserver";
}

static AdminClusterSpec abc_cluster_spec_admin(const std::string& root) {
    AdminClusterSpec c;
    c.name = "abc";
    c.coord_port = 6010;
    c.frontend_kv_port = 5500;
    c.backends["node1"] = {"node1", 5500, 5600, "/tmp/pc_abc_cluster/node1", root + "/node1.log", {}};
    c.backends["node2"] = {"node2", 5501, 5601, "/tmp/pc_abc_cluster/node2", root + "/node2.log", {}};
    c.backends["node3"] = {"node3", 5502, 5602, "/tmp/pc_abc_cluster/node3", root + "/node3.log", {}};
    return c;
}

static AdminClusterSpec multi_group_cluster_spec_admin(const std::string& root) {
    AdminClusterSpec c;
    c.name = "multi-group";
    c.coord_port = 7110;
    c.frontend_kv_port = 7500;
    const std::vector<std::string> node1_tablets = {
        "--tablet", "tabletA:a:f",
        "--tablet", "tabletC:m:r",
        "--tablet", "tabletD:s:"
    };
    const std::vector<std::string> node2_tablets = {
        "--tablet", "tabletA:a:f",
        "--tablet", "tabletB:g:l",
        "--tablet", "tabletD:s:"
    };
    const std::vector<std::string> node3_tablets = {
        "--tablet", "tabletA:a:f",
        "--tablet", "tabletB:g:l",
        "--tablet", "tabletC:m:r"
    };
    const std::vector<std::string> node4_tablets = {
        "--tablet", "tabletB:g:l",
        "--tablet", "tabletC:m:r",
        "--tablet", "tabletD:s:"
    };
    c.backends["node1"] = {"node1", 7500, 7600, first_existing_path_admin({"/tmp/pc_multi_group_demo/node1","/tmp/pc_multi_group/node1","/tmp/pc_multi_group_cluster/node1","/tmp/pc_mg_cluster/node1"}), root + "/mg_node1.log", node1_tablets};
    c.backends["node2"] = {"node2", 7501, 7601, first_existing_path_admin({"/tmp/pc_multi_group_demo/node2","/tmp/pc_multi_group/node2","/tmp/pc_multi_group_cluster/node2","/tmp/pc_mg_cluster/node2"}), root + "/mg_node2.log", node2_tablets};
    c.backends["node3"] = {"node3", 7502, 7602, first_existing_path_admin({"/tmp/pc_multi_group_demo/node3","/tmp/pc_multi_group/node3","/tmp/pc_multi_group_cluster/node3","/tmp/pc_mg_cluster/node3"}), root + "/mg_node3.log", node3_tablets};
    c.backends["node4"] = {"node4", 7503, 7603, first_existing_path_admin({"/tmp/pc_multi_group_demo/node4","/tmp/pc_multi_group/node4","/tmp/pc_multi_group_cluster/node4","/tmp/pc_mg_cluster/node4"}), root + "/mg_node4.log", node4_tablets};
    return c;
}

static AdminClusterSpec multi_tablet_cluster_spec_admin(const std::string& root) {
    AdminClusterSpec c;
    c.name = "multi-tablet";
    c.coord_port = 7010;
    c.frontend_kv_port = 6500;
    std::vector<std::string> tablets = {
        "--tablet", "tabletA:a:g",
        "--tablet", "tabletB:h:p",
        "--tablet", "tabletC:q:"
    };
    c.backends["node1"] = {"node1", 6500, 6600,
        first_existing_path_admin({"/opt/penncloud-data/kv/node1","/tmp/pc_multi_tablet_demo/node1","/tmp/pc_multi_tablet/node1","/tmp/pc_mt_demo/node1"}),
        root + "/mt_node1.log", tablets};
    c.backends["node2"] = {"node2", 6501, 6601,
        first_existing_path_admin({"/opt/penncloud-data/kv/node2","/tmp/pc_multi_tablet_demo/node2","/tmp/pc_multi_tablet/node2","/tmp/pc_mt_demo/node2"}),
        root + "/mt_node2.log", tablets};
    c.backends["node3"] = {"node3", 6502, 6602,
        first_existing_path_admin({"/opt/penncloud-data/kv/node3","/tmp/pc_multi_tablet_demo/node3","/tmp/pc_multi_tablet/node3","/tmp/pc_mt_demo/node3"}),
        root + "/mt_node3.log", tablets};
    return c;
}

static AdminClusterSpec single_cluster_spec_admin(const std::string& root) {
    AdminClusterSpec c;
    c.name = "single";
    c.coord_port = 6000;
    c.frontend_kv_port = 5000;
    c.backends["node1"] = {"node1", 5000, 5100, first_existing_path_admin({"/tmp/pc_cluster/node1","/tmp/pc_single_cluster/node1"}), root + "/node1.log", {}};
    c.backends["node2"] = {"node2", 5001, 5101, first_existing_path_admin({"/tmp/pc_cluster/node2","/tmp/pc_single_cluster/node2"}), root + "/node2.log", {}};
    c.backends["node3"] = {"node3", 5002, 5102, first_existing_path_admin({"/tmp/pc_cluster/node3","/tmp/pc_single_cluster/node3"}), root + "/node3.log", {}};
    return c;
}

// Build a cluster spec directly from the frontend's own --kv-port / --coord-port
// so admin status/control still works for non-standard KV port demos.
static AdminClusterSpec cfg_derived_cluster_spec_admin(const FEServer::Config& cfg) {
    const std::string root = resolve_project_root_admin();
    const int base = cfg.kv_port > 0 ? cfg.kv_port : 5000;
    const int repl_base = base + 100;
    AdminClusterSpec c;
    c.name = "configured";
    c.coord_port = cfg.coord_port;
    c.frontend_kv_port = base;
    c.backends["node1"] = {"node1", base,   repl_base,   "/tmp/pc_cluster/node1", root + "/node1.log", {}};
    c.backends["node2"] = {"node2", base+1, repl_base+1, "/tmp/pc_cluster/node2", root + "/node2.log", {}};
    c.backends["node3"] = {"node3", base+2, repl_base+2, "/tmp/pc_cluster/node3", root + "/node3.log", {}};
    return c;
}

static AdminClusterSpec configured_cluster_spec_admin(const FEServer::Config& cfg);

static AdminClusterSpec detect_active_cluster_admin(const FEServer::Config& cfg) {
    const std::string root = resolve_project_root_admin();
    std::string host = cfg.coord_host.empty() ? "127.0.0.1" : cfg.coord_host;
    if (cfg.coord_port > 0 && cfg.kv_port > 0) {
        if (tcp_probe_admin(host, cfg.coord_port, 250))
            return configured_cluster_spec_admin(cfg);
    }

    if (cfg.kv_port > 0) {
        AdminClusterSpec configured = cfg_derived_cluster_spec_admin(cfg);
        for (const auto& kv : configured.backends) {
            if (tcp_probe_admin("127.0.0.1", kv.second.kv_port, 80))
                return configured;
        }
    }

    if (tcp_probe_admin("127.0.0.1", 7110, 250)) return multi_group_cluster_spec_admin(root);
    if (tcp_probe_admin("127.0.0.1", 7010, 250)) return multi_tablet_cluster_spec_admin(root);
    if (tcp_probe_admin("127.0.0.1", 6010, 250)) return abc_cluster_spec_admin(root);
    if (tcp_probe_admin("127.0.0.1", 6000, 250)) return single_cluster_spec_admin(root);
    if (cfg.kv_port > 0) return cfg_derived_cluster_spec_admin(cfg);
    return single_cluster_spec_admin(root);
}

static AdminClusterSpec configured_cluster_spec_admin(const FEServer::Config& cfg) {
    const std::string root = resolve_project_root_admin();
    auto kv_matches = [&](int expected) {
        return cfg.kv_port <= 0 || cfg.kv_port == expected;
    };

    if (cfg.coord_port == 7110 && kv_matches(7500)) return multi_group_cluster_spec_admin(root);
    if (cfg.coord_port == 7010 && kv_matches(6500)) return multi_tablet_cluster_spec_admin(root);
    if (cfg.coord_port == 6010 && kv_matches(5500)) return abc_cluster_spec_admin(root);
    if (cfg.coord_port == 6000 && kv_matches(5000)) return single_cluster_spec_admin(root);
    if (cfg.kv_port > 0) return cfg_derived_cluster_spec_admin(cfg);
    return single_cluster_spec_admin(root);
}

static bool any_backend_listener_alive_admin(const FEServer::Config& cfg) {
    if (cfg.kv_port > 0 &&
        tcp_probe_admin(cfg.kv_host.empty() ? "127.0.0.1" : cfg.kv_host,
                        cfg.kv_port, 80)) {
        return true;
    }
    AdminClusterSpec cluster = detect_active_cluster_admin(cfg);
    for (const auto& kv : cluster.backends) {
        const auto& node = kv.second;
        if (tcp_probe_admin("127.0.0.1", node.kv_port, 80)) return true;
    }
    return false;
}

static bool verify_port_up_admin(int port, int attempts = 16, int sleep_ms = 250) {
    for (int i = 0; i < attempts; ++i) {
        if (tcp_probe_admin("127.0.0.1", port, 250)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
    return false;
}

static std::string kill_processes_for_port_cmd_admin(int port) {
    std::string p = std::to_string(port);
    std::string cmd;
    cmd += "ps -axo pid=,command= 2>/dev/null | ";
    cmd += "awk -v port=" + shell_escape_admin(p) + " '";
    cmd += "($0 ~ /frontend\\/feserver/ || $0 ~ /(^|[[:space:]\\/])feserver([[:space:]]|$)/ || ";
    cmd += "$0 ~ /kvstore\\/kvserver/ || $0 ~ /(^|[[:space:]\\/])kvserver([[:space:]]|$)/) && ";
    cmd += "(index($0, \"--port \" port) || index($0, \"--port=\" port) || ";
    cmd += " index($0, \"--repl-port \" port) || index($0, \"--repl-port=\" port)) && ";
    cmd += "$0 !~ /(^|[[:space:]])(sh|bash|zsh)[[:space:]]+-c[[:space:]]/ && ";
    cmd += "$0 !~ /(^|[[:space:]])awk[[:space:]]/ { print $1 }' | ";
    cmd += "while read pid; do [ -n \"$pid\" ] && kill -KILL \"$pid\" 2>/dev/null || true; done; true";
    return cmd;
}

static bool port_has_listener_admin(int port) {
    if (port <= 0) return false;
    return tcp_probe_admin("127.0.0.1", port, 80);
}

static int frontend_target_port_admin(const std::string& id) {
    if (id == "fe1") return 8090;
    if (id == "fe2") return 8091;
    if (id == "fe3") return 8092;
    return -1;
}

static std::vector<std::pair<std::string, int>> frontend_peer_order_admin() {
    return {{"fe1", 8090}, {"fe2", 8091}, {"fe3", 8092}};
}

// HTTP-level probe: only a real frontend that can serve admin status counts as
// a peer for self-kill handoff. This avoids mistaking stale/half-dead listeners
// for usable admin servers.
static bool frontend_admin_status_ok_admin(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    timeval tv{0, 250000}; // 250 ms
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_aton("127.0.0.1", &addr.sin_addr);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd); return false;
    }
    const char* req = "GET /api/admin/ping HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n";
    ::send(fd, req, strlen(req), MSG_NOSIGNAL);
    char buf[16] = {};
    ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
    ::close(fd);
    // "HTTP/1.x 200 " means the peer can answer admin requests.
    return n >= 12 && buf[9] == '2' && buf[10] == '0' && buf[11] == '0';
}

// Returns the next live peer port to redirect to when a given port is killed.
// Ring: 8090->8091->8092->8090
static std::vector<int> fe_redirect_candidates_admin(int killed_port) {
    if (killed_port == 8090) return {8091, 8092};
    if (killed_port == 8091) return {8092, 8090};
    if (killed_port == 8092) return {8090, 8091};
    return {};
}

static int real_fe_redirect_peer_admin(int killed_port) {
    for (int port : fe_redirect_candidates_admin(killed_port)) {
        if (frontend_admin_status_ok_admin(port)) return port;
    }
    return -1;
}

static std::string frontend_self_kill_helper_cmd_admin(const std::vector<int>& kill_ports,
                                                       int stub_port,
                                                       int stub_peer,
                                                       const std::string& fe_bin) {
    // No lsof in the container; use pkill -f matching the --port argument.
    std::string script = "sleep 0.15; ";
    for (int port : kill_ports) {
        if (port <= 0) continue;
        script += kill_processes_for_port_cmd_admin(port) + "; ";
    }
    if (stub_port > 0 && !fe_bin.empty() && file_exists_admin(fe_bin)) {
        std::string sp = std::to_string(stub_port);
        script += "sleep 0.10; ";
        script += "exec " + shell_escape_admin(fe_bin) + " --port " + sp;
        if (stub_peer > 0) {
            script += " --redirect-to peer-port:" + std::to_string(stub_peer);
        } else {
            script += " --page-not-found-stub";
        }
        script += "; ";
    }
    return "nohup /bin/sh -c " + shell_escape_admin(script) + " >/dev/null 2>&1 &";
}

// Spawn a redirect stub on killed_port that bounces browser traffic to a live peer.
// The stub returns 503 for /api/* so health probes can detect it is not a real server.
static void start_fe_redirect_stub(int killed_port, const std::string& fe_bin) {
    if (fe_bin.empty() || !file_exists_admin(fe_bin)) return;
    int peer = real_fe_redirect_peer_admin(killed_port);
    if (peer < 0) {
        admin_log("redirect stub skipped for port " + std::to_string(killed_port) +
                  ": no real frontend peer is alive");
        return;
    }
    std::string cmd =
        "nohup " + shell_escape_admin(fe_bin) +
        " --port " + std::to_string(killed_port) +
        " --redirect-to peer-port:" + std::to_string(peer) +
        " > /dev/null 2>&1 &";
    admin_log("starting redirect stub: port " + std::to_string(killed_port) +
              " -> peer-port:" + std::to_string(peer));
    run_shell_command(cmd);
}

static std::string kill_port_cmd_admin(int port) {
    return kill_processes_for_port_cmd_admin(port);
}

static void kill_ports_fast_admin(const std::string& label, const std::vector<int>& ports) {
    // Kill all ports in parallel in a single shell invocation to minimize latency.
    // The course container does not ship lsof/fuser/ss, so we match by command
    // line using pkill -f. Patterns are written so that "--port N" does not
    // accidentally match "--kv-port N" or "--repl-port N" -- the longer flags do
    // not contain "--port" as a substring.
    std::string combined;
    for (int port : ports) {
        if (port <= 0) continue;
        admin_log("graceful shutdown triggered for " + label + " port " + std::to_string(port));
        combined += "{ " + kill_processes_for_port_cmd_admin(port) + "; } & ";
    }
    if (!combined.empty()) {
        combined += "wait; true";
        (void)run_shell_command(combined);
    }
    for (int port : ports) {
        if (port <= 0) continue;
        admin_log("PID cleared for " + label + " port " + std::to_string(port));
    }
}

static std::string backend_launch_cmd_admin(const std::string& root,
                                            const std::string& kv_bin,
                                            const AdminBackendSpec& n) {
    std::string data_dir = n.data_dir.empty() ? ("/tmp/pc_admin/" + n.id) : n.data_dir;
    std::string log_path = n.log_path.empty() ? (root + "/" + n.id + ".log") : n.log_path;
    std::string cmd = "cd " + shell_escape_admin(root) + " && mkdir -p " + shell_escape_admin(data_dir) +
                      " && nohup " + shell_escape_admin(kv_bin);
    cmd += " --port " + std::to_string(n.kv_port);
    cmd += " --data " + shell_escape_admin(data_dir);
    if (std::find(n.extra_args.begin(), n.extra_args.end(), "--tablet") == n.extra_args.end()) {
        cmd += " --tablet tablet0";
    }
    cmd += " --node-id " + shell_escape_admin(n.id);
    cmd += " --repl-port " + std::to_string(n.repl_port);
    for (const auto& arg : n.extra_args) cmd += " " + shell_escape_admin(arg);
    cmd += " > " + shell_escape_admin(log_path) + " 2>&1 &";
    return cmd;
}

static std::string frontend_launch_cmd_admin(const std::string& root,
                                             const std::string& fe_bin,
                                             const AdminClusterSpec& cluster,
                                             const std::string& fe_id,
                                             int fe_port) {
    std::string log = root + "/frontend_" + std::to_string(fe_port) + ".log";
    std::string launch = "cd " + shell_escape_admin(root) + " && nohup " + shell_escape_admin(fe_bin);
    launch += " --port " + std::to_string(fe_port);
    launch += " --kv-host 127.0.0.1";
    launch += " --kv-port " + std::to_string(cluster.frontend_kv_port);
    launch += " --id " + shell_escape_admin(fe_id);
    launch += " --coord-host 127.0.0.1";
    launch += " --coord-port " + std::to_string(cluster.coord_port);
    launch += " > " + shell_escape_admin(log) + " 2>&1 &";
    return launch;
}

static std::string coord_binary_path_admin(const std::string& root) {
    if (file_exists_admin(root + "/coordinator/coordinator")) return root + "/coordinator/coordinator";
    if (file_exists_admin(root + "/../coordinator/coordinator")) return root + "/../coordinator/coordinator";
    return root + "/coordinator/coordinator";
}

static std::string coord_config_path_admin(const std::string& root, const std::string& name) {
    if (name == "multi-tablet") return root + "/coordinator/coordinator_multi_tablet_demo.conf";
    if (name == "multi-group")  return root + "/coordinator/coordinator_multi_group_demo.conf";
    if (name == "abc")          return root + "/coordinator/coordinator_abc.conf";
    return root + "/coordinator/coordinator.conf";
}

static bool start_coord_admin(const std::string& root, const AdminClusterSpec& cluster) {
    if (cluster.coord_port <= 0) return false;
    if (tcp_probe_admin("127.0.0.1", cluster.coord_port, 150)) return true;
    std::string cmd = "nohup " + shell_escape_admin(coord_binary_path_admin(root)) +
                      " --port " + std::to_string(cluster.coord_port) +
                      " --config " + shell_escape_admin(coord_config_path_admin(root, cluster.name)) +
                      " > " + shell_escape_admin(root + "/coordinator_admin.log") + " 2>&1 &";
    if (!run_shell_command(cmd)) return false;
    return verify_port_up_admin(cluster.coord_port, 20, 200);
}

static bool start_backend_admin2(const std::string& root, const std::string& kv_bin, const AdminBackendSpec& n) {
    if (n.kv_port <= 0 || n.repl_port <= 0) return false;
    if (tcp_probe_admin("127.0.0.1", n.kv_port, 150)) return true;
    std::string cmd = backend_launch_cmd_admin(root, kv_bin, n);
    if (!run_shell_command(cmd)) return false;
    return verify_port_up_admin(n.kv_port, 20, 200);
}

static bool ensure_storage_ready_admin(const FEServer::Config& cfg) {
    const std::string root = resolve_project_root_admin();
    const std::string kv_bin = kv_binary_path_admin(root);
    const AdminClusterSpec cluster = detect_active_cluster_admin(cfg);
    bool ok = true;
    for (const auto& kv : cluster.backends) {
        if (!tcp_probe_admin("127.0.0.1", kv.second.kv_port, 120)) {
            ok = start_backend_admin2(root, kv_bin, kv.second) && ok;
        }
    }
    ok = start_coord_admin(root, cluster) && ok;
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    return ok;
}

static std::string fallback_backend_json_admin(const FEServer::Config& cfg) {
    AdminClusterSpec cluster = configured_cluster_spec_admin(cfg);
    std::string out = "[";
    bool first = true;
    for (const auto& kv : cluster.backends) {
        const auto& n = kv.second;
        if (!first) out += ",";
        first = false;
        bool alive = tcp_probe_admin("127.0.0.1", n.kv_port, 120);
        out += "{\"id\":" + json_str(n.id) + ",\"host\":\"127.0.0.1\",\"port\":" + std::to_string(n.kv_port) +
               ",\"repl_port\":" + std::to_string(n.repl_port) + ",\"alive\":" + std::string(alive ? "true" : "false") +
               ",\"role\":" + json_str(n.id == "node1" ? "primary" : "unknown") + ",\"lsn\":0,\"missed\":0}";
    }
    out += "]";
    return out;
}

static void replace_json_bool_field_admin(std::string& json,
                                          const std::string& id,
                                          const std::string& field,
                                          bool value) {
    std::string id_pat = "\"id\":" + json_str(id);
    size_t obj = json.find(id_pat);
    if (obj == std::string::npos) return;
    size_t obj_end = json.find('}', obj);
    if (obj_end == std::string::npos) return;
    std::string field_pat = "\"" + field + "\":";
    size_t f = json.find(field_pat, obj);
    if (f == std::string::npos || f > obj_end) return;
    f += field_pat.size();
    size_t end = f;
    while (end < json.size() && std::isalpha(static_cast<unsigned char>(json[end]))) ++end;
    json.replace(f, end - f, value ? "true" : "false");
}

static void apply_backend_port_probe_admin(std::string& backend_json, const FEServer::Config& cfg) {
    AdminClusterSpec cluster = configured_cluster_spec_admin(cfg);
    for (const auto& kv : cluster.backends) {
        const auto& n = kv.second;
        // Coordinator heartbeat is the source of truth. A short opportunistic
        // port probe can correct stale false negatives, but should not turn a
        // coordinator-reported live node into DOWN just because the EC2 box is
        // briefly slow or swapping.
        if (tcp_probe_admin("127.0.0.1", n.kv_port, 300)) {
            replace_json_bool_field_admin(backend_json, n.id, "alive", true);
        }
    }
}

static bool perform_admin_control(const FEServer::Config& cfg, const std::string& kind, const std::string& action,
                                  const std::string& target, std::string& message) {
    const std::string root = resolve_project_root_admin();
    const std::string kv_bin = kv_binary_path_admin(root);
    const std::string fe_bin = fe_binary_path_admin(root);
    const AdminClusterSpec cluster = configured_cluster_spec_admin(cfg);

    if (kind == "backend") {
        auto it = cluster.backends.find(target);
        if (it == cluster.backends.end()) {
            message = "unknown backend target: " + target;
            return false;
        }
        const AdminBackendSpec& n = it->second;
        if (action == "kill") {
            admin_log("kill requested for node " + target);
            std::thread([target, n] {
                kill_ports_fast_admin("node " + target, {n.kv_port, n.repl_port});
            }).detach();
            message = "kill scheduled for " + target;
            return true;
        }
        if (action == "restart") {
            admin_log("restart requested for node " + target);
            std::thread([root, kv_bin, cluster, target, n] {
                kill_ports_fast_admin("node " + target, {n.kv_port, n.repl_port});
                admin_log("port availability check for node " + target + " port " + std::to_string(n.kv_port));
                // Retry up to 500ms: SIGKILL is delivered asynchronously and the
                // kernel may not release the port binding for a brief moment.
                bool port_free = false;
                for (int i = 0; i < 10 && !port_free; ++i) {
                    if (!port_has_listener_admin(n.kv_port)) { port_free = true; break; }
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                if (!port_free) {
                    admin_log("restart failure reason for node " + target + ": port still busy after 500ms");
                    return;
                }
                std::string cmd = backend_launch_cmd_admin(root, kv_bin, n);
                admin_log("restart command issued for node " + target + ": " + cmd);
                bool launched = run_shell_command(cmd);
                admin_log(std::string("restart ") + (launched ? "success" : "failure reason: launch command failed") +
                          " for node " + target);
                if (launched) (void)start_coord_admin(root, cluster);
            }).detach();
            message = "restart scheduled for " + target;
            return true;
        }
        message = "unknown backend action";
        return false;
    }

    if (kind == "frontend") {
        int fe_port = frontend_target_port_admin(target);
        if (fe_port < 0) {
            message = "unknown frontend target: " + target;
            return false;
        }
        if (action == "kill") {
            admin_log("kill requested for node " + target);
            if (fe_port == cfg.port) {
                // Self-kill fallback path. If no real peer exists, replace the
                // frontend tier with the tiny 404 stub on port 8090 so refreshes
                // still show "Page Not Found" instead of connection refused.
                int stub_peer = real_fe_redirect_peer_admin(fe_port);
                std::vector<int> ports_to_kill = stub_peer > 0
                    ? std::vector<int>{fe_port}
                    : std::vector<int>{8090, 8091, 8092};
                std::string kill_and_stub =
                    frontend_self_kill_helper_cmd_admin(ports_to_kill,
                                                        stub_peer > 0 ? fe_port : 8090,
                                                        stub_peer,
                                                        fe_bin);
                run_shell_command(kill_and_stub);
                admin_log("graceful shutdown triggered for node " + target + " via external self-kill helper");
                message = "kill scheduled for " + target;
                return true;
            }
            // Non-self kill: kill immediately, then start redirect stub on the freed port.
            std::thread([target, fe_port, fe_bin] {
                kill_ports_fast_admin("node " + target, {fe_port});
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                start_fe_redirect_stub(fe_port, fe_bin);
            }).detach();
            message = "kill scheduled for " + target;
            return true;
        }
        if (action == "restart") {
            admin_log("restart requested for node " + target);
            if (fe_port == cfg.port) {
                std::string launch = frontend_launch_cmd_admin(root, fe_bin, cluster, target, fe_port);
                // sleep 0.5s to flush the HTTP response, then kill instantly and
                // sleep 0.1s so the kernel fully releases the port before the new
                // process tries to bind it.
                std::string cmd = "( sleep 0.5; " + kill_port_cmd_admin(fe_port) + "; sleep 0.1; " + launch + " ) >/dev/null 2>&1 &";
                admin_log("port availability check for node " + target + " will run in external restart helper");
                admin_log("restart command issued for node " + target + ": " + launch);
                bool scheduled = run_shell_command(cmd);
                admin_log(std::string("restart ") + (scheduled ? "success" : "failure reason: helper launch failed") +
                          " for node " + target);
                message = scheduled ? ("restart scheduled for " + target) : ("failed to schedule restart for " + target);
                return scheduled;
            }
            std::thread([root, fe_bin, cluster, target, fe_port] {
                kill_ports_fast_admin("node " + target, {fe_port});
                admin_log("port availability check for node " + target + " port " + std::to_string(fe_port));
                // Retry up to 500ms for port release after SIGKILL
                bool port_free = false;
                for (int i = 0; i < 10 && !port_free; ++i) {
                    if (!port_has_listener_admin(fe_port)) { port_free = true; break; }
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                if (!port_free) {
                    admin_log("restart failure reason for node " + target + ": port still busy after 500ms");
                    return;
                }
                std::string launch = frontend_launch_cmd_admin(root, fe_bin, cluster, target, fe_port);
                admin_log("restart command issued for node " + target + ": " + launch);
                bool launched = run_shell_command(launch);
                admin_log(std::string("restart ") + (launched ? "success" : "failure reason: launch command failed") +
                          " for node " + target);
            }).detach();
            message = "restart scheduled for " + target;
            return true;
        }
        message = "unknown frontend action";
        return false;
    }

    message = "unknown kind";
    return false;
}

// ---------------------------------------------------------------------------
// Embedded ThreadPool (same design as kvserver)
// ---------------------------------------------------------------------------
struct FEServer::ThreadPool {
    std::vector<std::thread>          workers;
    std::queue<std::function<void()>> tasks;
    std::mutex                        mu;
    std::condition_variable           cv;
    bool                              stop = false;

    explicit ThreadPool(int n) {
        for (int i = 0; i < n; ++i)
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    { std::unique_lock<std::mutex> lk(mu);
                      cv.wait(lk, [this]{ return stop || !tasks.empty(); });
                      if (stop && tasks.empty()) return;
                      task = std::move(tasks.front()); tasks.pop(); }
                    task();
                }
            });
    }
    ~ThreadPool() {
        { std::lock_guard<std::mutex> lk(mu); stop = true; }
        cv.notify_all();
        for (auto& w : workers) w.join();
    }
    void enqueue(std::function<void()> f) {
        { std::lock_guard<std::mutex> lk(mu); tasks.push(std::move(f)); }
        cv.notify_one();
    }
};

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------
FEServer::FEServer(const Config& cfg) : cfg_(cfg) {
    // Redirect stub mode: no KV or session setup needed
    if (!cfg_.redirect_to.empty() || cfg_.page_not_found_stub) {
        pool_ = std::make_unique<ThreadPool>(cfg_.threads);
        return;
    }
    if (cfg_.coord_port > 0) {
        kv_ = std::make_unique<KVClient>(cfg_.kv_host, cfg_.kv_port,
                                         cfg_.coord_host, cfg_.coord_port);
    } else {
        kv_ = std::make_unique<KVClient>(cfg_.kv_host, cfg_.kv_port);
    }
    pool_ = std::make_unique<ThreadPool>(cfg_.threads);

    // Wire up session manager to KV client
    sessions_ = std::make_unique<SessionManager>(
    [this](const std::string& r, const std::string& c, std::string& v) {
        return kv_->get(r, c, v);
    },
    [this](const std::string& r, const std::string& c, const std::string& v) {
        return kv_->put(r, c, v);
    },
    [this](const std::string& r, const std::string& c) {
        return kv_->del(r, c);
    },
    [this](const std::string& r, const std::string& c, std::string& v) {
        auto st = kv_->get_status(r, c, v);
        switch (st) {
            case KVReadStatus::Found:
                return SessionKVReadStatus::Found;
            case KVReadStatus::NotFound:
                return SessionKVReadStatus::NotFound;
            case KVReadStatus::Unavailable:
                return SessionKVReadStatus::Unavailable;
        }
        return SessionKVReadStatus::Unavailable;
    }
);
}

FEServer::~FEServer() {
    running_ = false;
    for (int i = 0; i < 20 && active_sse_.load() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

// ---------------------------------------------------------------------------
// run()
// ---------------------------------------------------------------------------
void FEServer::run() {
    ::signal(SIGPIPE, SIG_IGN);
    listen_fd_ = create_listen_socket();
    running_ = true;

    std::cout << "[feserver:" << cfg_.server_id << "] listening on port "
              << cfg_.port << "\n";

    while (running_) {
        sockaddr_in addr{};
        socklen_t   len = sizeof(addr);
        int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        if (cfd < 0) { if (running_) continue; break; }

        pool_->enqueue([this, cfd] { handle_connection(cfd); });
    }
}

void FEServer::stop() {
    running_ = false;
    if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
}

int FEServer::create_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket(): " + std::string(strerror(errno)));
    int fd_flags = ::fcntl(fd, F_GETFD, 0);
    if (fd_flags >= 0) ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("bind(): " + std::string(strerror(errno)));
    ::listen(fd, 256);
    return fd;
}

// ---------------------------------------------------------------------------
// Connection handling -- persistent HTTP/1.1
// ---------------------------------------------------------------------------
void FEServer::handle_connection(int fd) {
    // Long enough for larger browser uploads without being infinite (guards
    // against zombie connections).
    struct timeval tv{.tv_sec = 900, .tv_usec = 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    bool detached = false;
    while (running_ && !detached && handle_one_request(fd, detached)) {}
    if (!detached) ::close(fd);
}

bool FEServer::handle_one_request(int fd, bool& detached) {
    HttpRequest req;
    if (!read_http_request(fd, req)) return false;
    req.remote_addr = peer_addr_admin(fd);

    auto finish_response = [&](HttpResponse& resp, bool is_head) {
        bool close_after_send = !req.keep_alive();
        auto it = resp.headers.find("Connection");
        if (it == resp.headers.end()) {
            resp.headers["Connection"] = req.keep_alive() ? "keep-alive" : "close";
        } else {
            std::string conn = it->second;
            for (char& c : conn) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            if (conn.find("close") != std::string::npos) close_after_send = true;
        }
        return send_http_response(fd, resp, is_head) && !close_after_send;
    };

    if (!cfg_.redirect_to.empty() || cfg_.page_not_found_stub) {
        HttpResponse resp = dispatch(req);
        bool is_head = (req.method == "HEAD");
        return finish_response(resp, is_head);
    }

    // SSE -- does not return through normal response path
    if (req.path == "/events" && req.method == "GET") {
        std::string user = get_user(req);
        if (user.empty()) {
            // Must send a real response; an empty close gives ERR_EMPTY_RESPONSE
            // which makes the browser retry every 3 s, exhausting the thread pool.
            std::string r401 =
                "HTTP/1.1 401 Unauthorized\r\n"
                "Content-Type: application/json\r\n"
                "Content-Length: 27\r\n"
                "Connection: close\r\n"
                "\r\n"
                "{\"ok\":false,\"error\":\"auth\"}";
            http_write_all(fd, r401);
        } else {
            if (!acquire_sse_slot()) {
                HttpResponse busy = HttpResponse::json(R"({"ok":false,"error":"too many live event streams"})");
                busy.status_code = 429;
                busy.status_text = "Too Many Requests";
                busy.headers["Retry-After"] = "5";
                send_http_response(fd, busy, false);
                return false;
            }
            try {
                std::thread([this, fd, req, user] {
                    handle_sse(fd, req, user);
                    release_sse_slot();
                    ::close(fd);
                }).detach();
                detached = true;
            } catch (...) {
                release_sse_slot();
                std::string body = "{\"ok\":false,\"error\":\"events failed\"}";
                std::string r503 = std::string(
                    "HTTP/1.1 503 Service Unavailable\r\n"
                    "Content-Type: application/json\r\n") +
                    "Content-Length: " + std::to_string(body.size()) + "\r\n"
                    "Connection: close\r\n"
                    "\r\n" + body;
                http_write_all(fd, r503);
            }
        }
        return false;  // SSE connection is long-lived and owned by the SSE thread.
    }

    HttpResponse resp = dispatch(req);
    // Only add a default Connection header if the handler did not set one.
    // Self-kill redirects must force Connection: close so the browser tears
    // down the connection before the helper SIGKILLs this process; with
    // keep-alive the browser may keep the dying socket and miss the redirect.
    auto conn_it = resp.headers.find("Connection");
    if (conn_it == resp.headers.end()) {
        resp.headers["Connection"] = req.keep_alive() ? "keep-alive" : "close";
    }
    bool is_head = (req.method == "HEAD");
    return finish_response(resp, is_head);
}

// ---------------------------------------------------------------------------
// Route dispatch
// ---------------------------------------------------------------------------
std::string FEServer::get_user(const HttpRequest& req) {
    return sessions_->authenticate(req);
}

bool FEServer::admin_control_allowed(const HttpRequest& req) const {
    if (is_loopback_peer_admin(req.remote_addr)) return true;

    const char* env_token = std::getenv("ADMIN_TOKEN");
    if (!env_token || !*env_token) return false;

    const std::string expected = env_token;
    if (req.header("x-admin-token") == expected) return true;
    if (req.param("admin_token") == expected) return true;

    std::string auth = req.header("authorization");
    const std::string bearer = "Bearer ";
    if (auth.rfind(bearer, 0) == 0 && auth.substr(bearer.size()) == expected) return true;
    return false;
}

bool FEServer::acquire_sse_slot() {
    const int limit = std::max(1, std::min(8, cfg_.threads / 4));
    int cur = active_sse_.load();
    while (cur < limit) {
        if (active_sse_.compare_exchange_weak(cur, cur + 1)) return true;
    }
    return false;
}

void FEServer::release_sse_slot() {
    int cur = active_sse_.load();
    while (cur > 0 && !active_sse_.compare_exchange_weak(cur, cur - 1)) {}
}

HttpResponse FEServer::dispatch(const HttpRequest& req) {
    const std::string& path   = req.path;
    const std::string& method = req.method;

    if (cfg_.page_not_found_stub) {
        (void)method;
        HttpResponse r;
        r.status_code = 404;
        r.status_text = "Not Found";
        r.body = "<html><body><h1>404 Not Found</h1></body></html>";
        r.headers["Content-Type"] = "text/html; charset=utf-8";
        return r;
    }

    // Redirect stub mode: return 503 for API probes so health checks can
    // distinguish this stub from a real server; redirect everything else.
    if (!cfg_.redirect_to.empty()) {
        // When a reverse proxy is in front, make the stub look unavailable so
        // the proxy retries another frontend without exposing :8091/:8092 URLs.
        if (path.rfind("/api/", 0) == 0 || !req.header("x-forwarded-host").empty()) {
            HttpResponse r;
            r.status_code = 503;
            r.status_text = "Service Unavailable";
            r.body = "{\"ok\":false,\"stub\":true}";
            r.headers["Content-Type"] = "application/json";
            return r;
        }
        std::string loc = cfg_.redirect_to;
        if (loc.rfind("peer-port:", 0) == 0) {
            std::string host = req.header("host");
            if (host.empty()) host = "127.0.0.1";
            if (!host.empty() && host.front() != '[') {
                size_t colon = host.rfind(':');
                if (colon != std::string::npos) host = host.substr(0, colon);
            }
            loc = "http://" + host + ":" + loc.substr(std::string("peer-port:").size());
        }
        if (!path.empty() && path != "/") loc += path;
        if (!req.query.empty()) loc += "?" + req.query;
        return HttpResponse::redirect(loc, 302);
    }

    // Admin endpoints must not depend on session lookup in KV, and they must
    // stay reachable even when every backend node is down.
    if (path == "/api/admin/ping" && method == "GET")     return HttpResponse::json(R"({"ok":true})");
    if (path == "/api/admin/status" && method == "GET") {
        if (!admin_control_allowed(req)) return HttpResponse::forbidden();
        return handle_admin_status(req);
    }
    if (path == "/api/admin/raw" && method == "GET") {
        if (!admin_control_allowed(req)) return HttpResponse::forbidden();
        return handle_admin_raw(req);
    }
    if (path == "/api/admin/control" && method == "POST") return handle_admin_control(req);
    if (path == "/admin/control" && method == "GET")      return handle_admin_control_redirect(req);
    if (path == "/admin/metrics" && method == "GET") {
        if (!admin_control_allowed(req)) return HttpResponse::forbidden();
        return handle_admin_metrics(req);
    }
    if ((path == "/admin" || path.rfind("/admin/", 0) == 0) && method == "GET") {
        if (!admin_control_allowed(req)) return HttpResponse::forbidden();
        return handle_admin_page(req);
    }

    // ---- Static / SPA shell -------------------------------------------------
    if ((method == "GET" || method == "HEAD") && is_spa_shell_route(path)) {
        if (!any_backend_listener_alive_admin(cfg_)) {
            return HttpResponse::not_found();
        }
        return handle_spa_shell(req);
    }

    if (path.rfind("/static/", 0) == 0)
        return handle_static(req);

    if (method == "GET" &&
        (path == "/inbox" || path == "/compose" || path == "/drive" ||
         path == "/chat" || path == "/settings" || path == "/email")) {
        return handle_spa_shell(req);
    }

    // ---- Auth endpoints (no session required) --------------------------------
    if (path == "/api/login"  && method == "POST") return handle_login(req);
    if (path == "/api/signup" && method == "POST") return handle_signup(req);
    if (path == "/api/session" && method == "GET") return handle_session(req);

    // ---- Protected endpoints (session required) ------------------------------
    std::string user = get_user(req);

    if (path == "/api/me" && method == "GET") {
        if (user.empty()) return HttpResponse::json(R"({"ok":false})");
        return HttpResponse::json(std::string("{\"ok\":true,\"user\":") + json_str(user) + "}");
    }

    if (path == "/api/logout"          && method == "POST") return handle_logout(req, user);
    if (path == "/api/change-password" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_change_password(req);
    }

    // Mail
    if (path == "/api/inbox" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_inbox(req, user);
    }
    if (path == "/api/send" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_send_email(req, user);
    }
    if (path == "/api/delete-email" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_delete_email(req, user);
    }
    if (path == "/api/restore-email" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_restore_email(req, user);
    }
    if (path == "/api/contacts" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_contacts_list(req, user);
    }
    if (path == "/api/contacts/add" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_contacts_add(req, user);
    }
    if (path == "/api/contacts/delete" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_contacts_delete(req, user);
    }
    if (path == "/api/mail/upload-attachment" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_mail_upload_attachment(req, user);
    }
    if (path == "/api/mail/attachment" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_mail_download_attachment(req, user);
    }
    if (path == "/api/chat/rooms" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_rooms(req, user);
    }
    if (path == "/api/chat/rooms" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_create_room(req, user);
    }
    if (path == "/api/chat/messages" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_messages(req, user);
    }
    if (path == "/api/chat/send" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_send(req, user);
    }
    if (path == "/api/chat/dms" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_dms(req, user);
    }
    if (path == "/api/chat/dm/messages" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_dm_messages(req, user);
    }
    if (path == "/api/chat/dm/send" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_chat_dm_send(req, user);
    }

    // /api/email/:uid
    {
        auto m = match_route("/api/email/:uid", path);
        if (m.matched && method == "GET") {
            if (user.empty()) return HttpResponse::error(401, "Unauthorized");
            return handle_get_email(req, user, m.params.at("uid"));
        }
    }

    // Drive
    if (path == "/api/upload" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload(req, user);
    }
    if (path == "/api/upload/start" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload_start(req, user);
    }
    if (path == "/api/upload/chunk" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload_chunk(req, user);
    }
    if (path == "/api/upload/status" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload_status(req, user);
    }
    if (path == "/api/upload/finish" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload_finish(req, user);
    }
    if (path == "/api/upload/cancel" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload_cancel(req, user);
    }
    if (path == "/api/rename" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_rename(req, user);
    }
    if (path == "/api/move" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_move(req, user);
    }
    if (path == "/api/mkdir" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_mkdir(req, user);
    }
    if (path == "/api/delete-path" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_delete_path(req, user);
    }
    if (path == "/api/quota" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_quota_status(req, user);
    }
    if (path == "/api/quota" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_quota_update(req, user);
    }

    // /api/drive/* folder listing
    if (path.rfind("/api/drive", 0) == 0 && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_drive_list(req, user);
    }

    // /api/download/:uid
    {
        auto m = match_route("/api/download/:uid", path);
        if (m.matched && method == "GET") {
            if (user.empty()) return HttpResponse::error(401, "Unauthorized");
            return handle_download(req, user, m.params.at("uid"));
        }
    }

    return HttpResponse::not_found();
}

// ---------------------------------------------------------------------------
// SPA shell: the single HTML page served for every non-API route.
// The JS router takes over from here.
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_spa_shell(const HttpRequest&) {
    // The entire SPA shell is embedded here so the frontend has zero
    // external file dependencies during development.
    // In production you would serve this from cfg_.static_dir.
    static const std::string html = R"HTML(
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PennCloud</title>
  <link rel="icon" type="image/png" href="/static/logo.png">
  <link rel="shortcut icon" type="image/png" href="/static/logo.png">
  <link rel="apple-touch-icon" href="/static/logo.png">
  <style>
    /* ---- Reset + base ---- */
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --penn-blue: #011F5B;
      --penn-blue-2: #0A2D72;
      --accent:    #1464C8;
      --accent-soft: #EAF3FF;
      --bg:        #F4F7FB;
      --surface:   #FFFFFF;
      --surface-soft: #FBFDFF;
      --border:    #DDE6F1;
      --text:      #182235;
      --muted:     #6C7A8C;
      --success:   #167246;
      --danger:    #C93636;
      --sidebar-w: 220px;
      --shadow-sm: 0 8px 22px rgba(1,31,91,0.07);
      --shadow-md: 0 18px 44px rgba(1,31,91,0.12);
      --ring: 0 0 0 3px rgba(20,100,200,0.13);
    }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background:
             radial-gradient(circle at 16% 0%, rgba(20,100,200,0.08), transparent 28%),
             linear-gradient(180deg, #FAFCFF 0%, var(--bg) 280px);
           color: var(--text); min-height: 100vh;
           -webkit-font-smoothing: antialiased; text-rendering: optimizeLegibility; }

    /* ---- Login page ---- */
    #login-page {
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh;
      background:
        radial-gradient(circle at 18% 12%, rgba(84,153,232,0.34), transparent 32%),
        radial-gradient(circle at 82% 86%, rgba(255,255,255,0.12), transparent 30%),
        linear-gradient(135deg, #011F5B 0%, #071B46 52%, #00112F 100%);
    }
    .login-card {
      background: rgba(255,255,255,0.98); border-radius: 18px;
      padding: 40px 36px; width: 360px;
      box-shadow: 0 26px 80px rgba(0,0,0,0.30);
      border: 1px solid rgba(255,255,255,0.58);
      backdrop-filter: blur(10px);
    }
    .login-brand {
      display: flex; align-items: center; gap: 14px; margin-bottom: 24px;
    }
    .login-brand h1 { color: var(--penn-blue); font-size: 28px; letter-spacing: -0.5px; margin-bottom: 5px; }
    .login-brand p  { color: var(--muted); font-size: 14px; margin-bottom: 0; }
    .pc-logo {
      display: inline-flex; align-items: center; justify-content: center;
      border-radius: 14px;
      background: #FFFFFF;
      box-shadow: 0 10px 26px rgba(1,31,91,0.13);
      border: 1px solid rgba(221,230,241,0.9);
      overflow: hidden;
      flex: 0 0 auto;
    }
    .pc-logo img {
      display: block; width: 100%; height: 100%;
      object-fit: contain; object-position: center;
    }
    .pc-logo-large { width: 60px; height: 60px; padding: 5px; }
    .pc-logo-small {
      width: 32px; height: 32px; padding: 3px; border-radius: 9px;
      box-shadow: 0 6px 16px rgba(1,31,91,0.18);
    }
    .form-group { margin-bottom: 16px; }
    .form-group label { display: block; font-size: 13px; font-weight: 600;
                        color: var(--muted); margin-bottom: 6px; }
    .form-group input {
      width: 100%; padding: 10px 14px; border: 1px solid var(--border);
      border-radius: 10px; font-size: 14px; outline: none; background: #FBFDFF;
      transition: border .15s, box-shadow .15s, background .15s;
    }
    .form-group input:focus {
      border-color: var(--accent);
      background: #FFFFFF;
      box-shadow: var(--ring);
    }
    .field-help { font-size: 12px; color: var(--muted); margin-top: 6px; line-height: 1.35; }
    .password-field { display: flex; gap: 8px; align-items: center; }
    .password-field input { margin-bottom: 0; min-width: 0; }
    .password-toggle {
      flex: 0 0 40px; width: 40px; height: 40px; padding: 0;
      display: inline-flex; align-items: center; justify-content: center;
      border: 1px solid var(--border);
      border-radius: 10px; background: var(--surface-soft); color: var(--muted);
      cursor: pointer;
      transition: background .15s, color .15s, border-color .15s, transform .12s;
    }
    .password-toggle:hover { background: #FFFFFF; color: var(--text); border-color: #B8C7DA; transform: translateY(-1px); }
    .password-toggle svg {
      width: 18px; height: 18px; display: block;
      stroke: currentColor; fill: none; stroke-width: 1.9;
      stroke-linecap: round; stroke-linejoin: round;
    }
    .password-toggle .icon-eye { display: none; }
    .password-toggle .icon-eye-off { display: block; }
    .password-toggle.showing .icon-eye { display: block; }
    .password-toggle.showing .icon-eye-off { display: none; }
    .btn {
      width: 100%; padding: 11px; border: none; border-radius: 10px;
      font-size: 14px; font-weight: 600; cursor: pointer;
      transition: opacity .15s, transform .12s, box-shadow .15s;
    }
    .btn-primary {
      background: linear-gradient(180deg, var(--penn-blue-2), var(--penn-blue));
      color: white; box-shadow: var(--shadow-sm);
    }
    .btn-primary:hover { opacity: .92; transform: translateY(-1px); box-shadow: var(--shadow-md); }
    .btn:disabled { transform: none; box-shadow: none; }
    .auth-divider {
      display: flex; align-items: center; gap: 10px;
      margin: 18px 0 12px; color: var(--muted); font-size: 12px;
    }
    .auth-divider::before, .auth-divider::after {
      content: ""; height: 1px; flex: 1; background: var(--border);
    }
    .btn-link {
      width: 100%; padding: 10px 12px; border-radius: 10px;
      border: 1px solid var(--border); background: var(--surface-soft);
      color: var(--penn-blue); font-size: 13px; font-weight: 650;
      cursor: pointer; transition: background .15s, border-color .15s, transform .12s, box-shadow .15s;
    }
    .btn-link:hover {
      background: #FFFFFF; border-color: #B8C7DA; transform: translateY(-1px);
      box-shadow: 0 8px 18px rgba(1,31,91,0.07);
    }
    .error-msg { color: var(--danger); font-size: 13px; margin-top: 8px; display: none; }

    /* ---- App shell ---- */
    #app { display: none; min-height: 100vh; flex-direction: column; }
    .topbar {
      background: linear-gradient(90deg, #011F5B 0%, #082A68 100%); color: white;
      padding: 0 24px; height: 52px;
      display: flex; align-items: center; justify-content: space-between;
      box-shadow: 0 3px 16px rgba(1,31,91,0.18);
      position: sticky; top: 0; z-index: 20;
    }
    .topbar .brand {
      display: inline-flex; align-items: center; gap: 10px;
      font-size: 18px; font-weight: 750; letter-spacing: -0.4px;
    }
    .topbar .user-info { font-size: 13px; opacity: .8; display: flex;
                         align-items: center; gap: 16px; }
    .topbar .logout-btn { background: rgba(255,255,255,0.15); border: none;
                          color: white; padding: 5px 12px; border-radius: 6px;
                          cursor: pointer; font-size: 12px; transition: background .15s; }
    .topbar .logout-btn:hover { background: rgba(255,255,255,0.24); }
    .main-layout { display: flex; flex: 1; }
    .sidebar {
      width: var(--sidebar-w); background: rgba(255,255,255,0.88);
      border-right: 1px solid var(--border); padding: 18px 12px;
      display: flex; flex-direction: column; gap: 2px;
      backdrop-filter: blur(10px);
    }
    .nav-item {
      display: flex; align-items: center; gap: 10px;
      padding: 10px 12px; cursor: pointer; border-radius: 12px;
      font-size: 14px; color: var(--muted); transition: background .14s, color .14s, transform .12s;
      border: none; background: none; width: 100%; text-align: left;
    }
    .nav-item:hover { background: #F3F7FC; color: var(--text); transform: translateX(2px); }
    .nav-item.active {
      background: linear-gradient(180deg, #EEF5FF, #E7F0FC);
      color: var(--penn-blue); font-weight: 650;
      box-shadow: inset 0 0 0 1px rgba(20,100,200,0.10);
    }
    .nav-icon {
      width: 24px; height: 24px; flex: 0 0 24px;
      display: inline-flex; align-items: center; justify-content: center;
      color: currentColor;
    }
    .nav-icon svg {
      width: 18px; height: 18px; display: block;
      stroke: currentColor; fill: none; stroke-width: 1.9;
      stroke-linecap: round; stroke-linejoin: round;
    }
    .content { flex: 1; padding: 30px; overflow-y: auto; }

    /* ---- Inbox ---- */
    .page-title { font-size: 22px; font-weight: 750; color: var(--penn-blue);
                  margin-bottom: 16px; display: flex; align-items: center;
                  justify-content: space-between; letter-spacing: -0.35px; }
    .compose-btn {
      background: var(--penn-blue); color: white; border: none;
      padding: 8px 16px; border-radius: 8px; cursor: pointer;
      font-size: 13px; font-weight: 650; box-shadow: var(--shadow-sm);
      transition: transform .12s, box-shadow .15s, opacity .15s;
    }
    .compose-btn:hover { transform: translateY(-1px); box-shadow: var(--shadow-md); opacity: .94; }
    .email-list { background: var(--surface); border-radius: 14px;
                  border: 1px solid var(--border); overflow: hidden; box-shadow: var(--shadow-sm); }
    .email-row {
      display: flex; align-items: center; padding: 14px 20px;
      border-bottom: 1px solid var(--border); cursor: pointer;
      transition: background .1s, transform .12s, box-shadow .12s; gap: 16px;
    }
    .email-row:hover { background: #F8FBFF; transform: translateX(2px); box-shadow: inset 3px 0 0 var(--accent-soft); }
    .email-row:last-child { border-bottom: none; }
    .email-row.unread .email-from { font-weight: 700; }
    .email-from  { width: 160px; flex-shrink: 0; font-size: 14px;
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .email-subj  { flex: 1; font-size: 14px; color: var(--text);
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .email-time  { font-size: 12px; color: var(--muted); flex-shrink: 0; }
    .badge-new {
      font-size: 10px; background: var(--accent); color: white;
      padding: 1px 6px; border-radius: 10px; flex-shrink: 0;
    }

    /* ---- Email view ---- */
    .email-view { background: var(--surface); border-radius: 14px;
                  border: 1px solid var(--border); padding: 28px; box-shadow: var(--shadow-sm); }
    .email-view h2 { font-size: 18px; margin-bottom: 12px; }
    .email-meta { color: var(--muted); font-size: 13px; margin-bottom: 16px;
                  display: flex; flex-direction: column; gap: 4px; }
    .email-body { white-space: pre-wrap; font-size: 14px; line-height: 1.6;
                  border-top: 1px solid var(--border); padding-top: 16px; }
    .email-actions { display: flex; gap: 8px; margin-bottom: 16px; }
    .action-btn {
      padding: 7px 14px; border-radius: 9px; font-size: 13px; cursor: pointer;
      border: 1px solid var(--border); background: var(--surface-soft);
      transition: background .12s, border-color .12s, transform .12s;
    }
    .action-btn:hover { background: #F8FBFF; border-color: #B8C7DA; transform: translateY(-1px); }
    .action-btn.danger { color: var(--danger); border-color: var(--danger); }

    /* ---- Chat ---- */
    .chat-panel {
      display: grid; grid-template-rows: 1fr auto; gap: 14px; min-height: 520px;
      background: linear-gradient(180deg, #FFFFFF 0%, #F8FBFF 100%);
      border: 1px solid var(--border); border-radius: 16px; padding: 16px;
      box-shadow: var(--shadow-sm);
    }
    .chat-messages {
      overflow: auto; max-height: 460px; padding: 4px 6px 4px 2px;
      scrollbar-width: thin;
    }
    .chat-empty {
      padding: 42px 16px; text-align: center; color: var(--muted);
      border: 1px dashed #D6E0EC; border-radius: 12px; background: rgba(255,255,255,.72);
    }
    .chat-bubble {
      max-width: 76%; padding: 10px 12px; border: 1px solid var(--border);
      border-radius: 16px; background: white; margin-bottom: 10px;
      box-shadow: 0 2px 8px rgba(15,23,42,.04);
    }
    .chat-bubble.mine {
      margin-left: auto; background: linear-gradient(180deg, #EEF5FF, #E8F1FC); border-color: #C9D6EA;
    }
    .chat-meta {
      display: flex; justify-content: space-between; gap: 12px; margin-bottom: 6px;
      font-size: 12px; color: var(--muted);
    }
    .chat-meta b { color: #111827; }
    .chat-text { white-space: pre-wrap; word-break: break-word; font-size: 14px; line-height: 1.45; }
    .chat-composer {
      display: flex; gap: 10px; align-items: flex-end;
      padding: 10px; border: 1px solid #D6E0EC; border-radius: 14px;
      background: rgba(255,255,255,.9);
    }
    .chat-composer:focus-within {
      border-color: var(--accent); box-shadow: 0 0 0 3px rgba(0,102,204,.10);
    }
    .chat-input {
      flex: 1; min-height: 48px; max-height: 150px; resize: vertical;
      border: none; outline: none; background: transparent; padding: 8px 10px;
      font: inherit; line-height: 1.45; color: var(--text);
    }
    .chat-input::placeholder { color: #8A97A8; }
    .chat-send {
      width: auto; padding: 10px 20px; border-radius: 10px;
      align-self: stretch; min-height: 44px;
    }
    .chat-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .chat-section-head {
      margin-top: -6px; margin-bottom: 8px;
      font-size: 12px; color: var(--muted); font-weight: 600;
    }
    .modal-backdrop {
      position: fixed; inset: 0; background: rgba(15,23,42,.36);
      display: flex; align-items: center; justify-content: center;
      z-index: 100; padding: 20px;
    }
    .chat-dialog {
      width: min(420px, 100%); background: var(--surface); border: 1px solid var(--border);
      border-radius: 16px; padding: 22px; box-shadow: 0 18px 50px rgba(15,23,42,.22);
    }
    .chat-dialog h3 { font-size: 17px; margin-bottom: 6px; color: var(--penn-blue); }
    .chat-dialog p { font-size: 13px; color: var(--muted); margin-bottom: 14px; }
    .chat-dialog input {
      width: 100%; padding: 10px 12px; border: 1px solid var(--border);
      border-radius: 9px; font: inherit; outline: none; margin-bottom: 8px;
    }
    .chat-dialog input:focus {
      border-color: var(--accent); box-shadow: 0 0 0 3px rgba(0,102,204,.10);
    }
    .chat-dialog-actions {
      display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px;
    }

    /* ---- Compose ---- */
    .compose-form { background: var(--surface); border-radius: 14px;
                    border: 1px solid var(--border); padding: 28px; max-width: 700px;
                    box-shadow: var(--shadow-sm); }
    .compose-form input, .compose-form textarea {
      width: 100%; padding: 10px 14px; border: 1px solid var(--border);
      border-radius: 10px; font-size: 14px; margin-bottom: 12px;
      font-family: inherit; outline: none;
      transition: border .15s, box-shadow .15s, background .15s;
    }
    .compose-form textarea { height: 200px; resize: vertical; }
    .compose-form input:focus, .compose-form textarea:focus {
      border-color: var(--accent);
      box-shadow: var(--ring);
    }
    .contact-form-row {
      display: grid; grid-template-columns: minmax(150px, 1fr) minmax(220px, 1.2fr) auto;
      gap: 10px; align-items: start; margin-bottom: 14px;
    }
    .contact-form-row input {
      height: 40px; margin-bottom: 0;
    }
    .contact-add-btn {
      width: auto; height: 40px; padding: 0 20px; align-self: start;
    }
    .contact-row {
      display: flex; align-items: center; justify-content: space-between;
      gap: 16px; padding: 10px 0; border-bottom: 1px solid rgba(214,224,236,.65);
    }
    .contact-row:last-child { border-bottom: none; }
    .contact-main { min-width: 0; flex: 1; }
    .contact-name {
      font-size: 14px; font-weight: 650; color: var(--text);
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    }
    .contact-email {
      margin-top: 2px; font-size: 12px; color: var(--muted);
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    }
    .contact-actions { flex-shrink: 0; }
    .contact-actions button {
      padding: 4px 9px; font-size: 11px; border-radius: 7px;
      border: 1px solid var(--border); cursor: pointer; background: var(--surface-soft);
      transition: background .12s, border-color .12s;
    }
    .contact-actions button:hover { background: var(--bg); border-color: #B8C7DA; }

    /* ---- Drive ---- */
    .drive-shell { display: flex; flex-direction: column; gap: 16px; }
    .drive-top {
      background: var(--surface); border: 1px solid var(--border); border-radius: 14px;
      padding: 18px 20px; display: grid; grid-template-columns: minmax(0,1fr) minmax(250px,340px);
      gap: 20px; align-items: center; box-shadow: var(--shadow-sm);
    }
    .drive-eyebrow { color: var(--danger); font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: .04em; }
    .drive-title { color: var(--penn-blue); font-size: 24px; font-weight: 800; margin-top: 3px; overflow-wrap: anywhere; }
    .drive-subtitle { color: var(--muted); font-size: 13px; margin-top: 6px; overflow-wrap: anywhere; }
    .drive-quota { background: #F8FBFF; border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
    .drive-quota-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 9px; font-size: 13px; }
    .drive-quota-row strong { color: var(--penn-blue); }
    .drive-quota-meta { color: var(--muted); font-size: 12px; white-space: nowrap; }
    .quota-track { height: 9px; background: #E2E8F0; border-radius: 999px; overflow: hidden; }
    .quota-fill { height: 100%; background: var(--accent); border-radius: inherit; transition: width .2s; }
    .quota-fill.warn { background: var(--danger); }
    .drive-command-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
    .drive-toolbar { display: flex; gap: 8px; flex-wrap: wrap; }
    .drive-toolbar button {
      min-height: 36px; padding: 8px 13px; border-radius: 9px; font-size: 13px; cursor: pointer;
      border: 1px solid var(--border); background: var(--surface-soft); color: var(--text);
      font-weight: 700; display: inline-flex; align-items: center; gap: 7px;
      transition: background .12s, border-color .12s, transform .12s;
    }
    .drive-toolbar button:hover { background: #F8FBFF; border-color: #B8C7DA; transform: translateY(-1px); }
    .drive-toolbar .primary-action { background: var(--penn-blue); color: #fff; border-color: var(--penn-blue); }
    .drive-toolbar .primary-action:hover { background: #0A2D72; border-color: #0A2D72; }
    .btn-symbol { font-size: 15px; line-height: 1; }
    .upload-progress-card {
      display: none; margin: 0; padding: 12px 14px; background: #F8FBFF;
      border: 1px solid #C8D8EA; border-radius: 10px; font-size: 13px; color: var(--text);
    }
    .upload-progress-card.active { display: block; }
    .upload-progress-list { display: grid; gap: 12px; }
    .upload-progress-head { display: flex; justify-content: space-between; gap: 12px; margin-bottom: 8px; }
    .upload-progress-name { font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .upload-progress-bar { height: 9px; background: #E2E8F0; border-radius: 999px; overflow: hidden; }
    .upload-progress-fill { height: 100%; width: 0%; background: #0066CC; transition: width 160ms ease; }
    .upload-progress-detail { margin-top: 7px; color: var(--muted); }
    .breadcrumb {
      display: flex; align-items: center; gap: 6px; flex-wrap: wrap; min-width: 0;
      font-size: 13px; color: var(--muted);
    }
    .crumb {
      border: 1px solid var(--border); background: var(--surface); color: var(--accent);
      border-radius: 999px; padding: 6px 10px; cursor: pointer; font-size: 13px; font-weight: 700;
    }
    .crumb:hover { background: #EAF3FF; border-color: #B8C7DA; }
    .crumb-sep { color: #A6B4C6; font-size: 12px; }
    .drive-table {
      background: var(--surface); border: 1px solid var(--border); border-radius: 14px;
      overflow: hidden; box-shadow: var(--shadow-sm);
    }
    .drive-table-header, .drive-row {
      display: grid; grid-template-columns: minmax(240px,1fr) 110px 100px 160px 220px;
      gap: 14px; align-items: center;
    }
    .drive-table-header {
      padding: 10px 18px; background: #F8FBFF; color: var(--muted);
      font-size: 12px; font-weight: 800; letter-spacing: .02em; text-transform: uppercase;
      border-bottom: 1px solid var(--border);
    }
    .drive-row {
      padding: 12px 18px; border-bottom: 1px solid var(--border); min-height: 64px;
      transition: background .12s;
    }
    .drive-row:last-child { border-bottom: none; }
    .drive-row:hover { background: #FAFCFF; }
    .drive-name-btn {
      border: none; background: none; padding: 0; color: var(--text); cursor: pointer;
      display: flex; align-items: center; gap: 12px; text-align: left; min-width: 0;
    }
    .drive-name-btn:hover .drive-item-name { color: var(--accent); text-decoration: underline; }
    .drive-icon {
      width: 38px; height: 38px; border-radius: 10px; display: inline-flex; align-items: center; justify-content: center;
      flex: 0 0 auto; color: var(--success); background: #F0F7F4; border: 1px solid #CFE7D8;
    }
    .drive-icon.folder { color: var(--penn-blue); background: #EAF3FF; border-color: #D3E2F2; }
    .drive-icon svg {
      width: 19px; height: 19px; stroke: currentColor; fill: none;
      stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round;
    }
    .drive-name-text { display: flex; flex-direction: column; gap: 3px; min-width: 0; }
    .drive-item-name { font-size: 14px; font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .drive-item-path { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .drive-kind, .drive-size, .drive-changed { color: #4A5568; font-size: 13px; }
    .drive-changed { color: var(--muted); }
    .drive-row-actions { display: flex; justify-content: flex-end; gap: 6px; flex-wrap: wrap; }
    .drive-row-actions button {
      padding: 6px 9px; font-size: 12px; border-radius: 7px;
      border: 1px solid var(--border); cursor: pointer; background: var(--surface-soft);
      color: #2D3748; font-weight: 600;
      transition: background .12s, border-color .12s;
    }
    .drive-row-actions button:hover { background: var(--bg); border-color: #B8C7DA; }
    .drive-row-actions .danger { color: var(--danger); border-color: rgba(201,54,54,0.45); }
    .drive-empty {
      padding: 46px 24px; text-align: center; color: var(--muted);
      display: flex; flex-direction: column; align-items: center; gap: 12px;
    }
    .drive-empty-icon {
      width: 56px; height: 56px; border-radius: 14px; background: #EAF3FF; color: var(--penn-blue);
      display: flex; align-items: center; justify-content: center; border: 1px solid #D3E2F2;
    }
    .drive-empty-icon svg {
      width: 28px; height: 28px; stroke: currentColor; fill: none;
      stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round;
    }
    .file-icon {
      width: 28px; height: 28px; border-radius: 9px;
      display: inline-flex; align-items: center; justify-content: center;
      color: var(--accent); background: var(--accent-soft);
    }
    .file-icon.file { color: #52647A; background: #F4F7FB; }
    .file-icon.folder { color: var(--penn-blue); background: #EAF3FF; }
    .file-icon svg {
      width: 17px; height: 17px; stroke: currentColor; fill: none;
      stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round;
    }
    .file-head .file-icon { background: transparent; }
    .inline-folder-icon {
      display: inline-flex; align-items: center; justify-content: center;
      width: 22px; height: 22px; border-radius: 7px;
      color: var(--penn-blue); background: var(--accent-soft); vertical-align: middle;
      margin-right: 8px;
    }
    .inline-folder-icon svg {
      width: 14px; height: 14px; stroke: currentColor; fill: none;
      stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round;
    }
    /* ---- Notifications ---- */
    .toast {
      position: fixed; bottom: 24px; right: 24px;
      background: #172033; color: white;
      padding: 12px 20px; border-radius: 10px; font-size: 13px;
      opacity: 0; transition: opacity .3s, transform .3s; pointer-events: none; z-index: 999;
      transform: translateY(8px); box-shadow: 0 14px 30px rgba(0,0,0,0.18);
    }
    .toast.show { opacity: 1; transform: translateY(0); }
    .spinner { text-align: center; padding: 40px; color: var(--muted); }
    @media (max-width: 760px) {
      :root { --sidebar-w: 170px; }
      .content { padding: 18px; }
      .email-from { width: 120px; }
      .drive-top { grid-template-columns: 1fr; }
      .drive-command-row { align-items: stretch; }
      .breadcrumb, .drive-toolbar { width: 100%; }
      .drive-toolbar button { flex: 1 1 140px; justify-content: center; }
      .drive-table-header { display: none; }
      .drive-row { grid-template-columns: 1fr; gap: 10px; align-items: start; }
      .drive-row-actions { justify-content: flex-start; }
      .drive-kind, .drive-size, .drive-changed { padding-left: 50px; }
      .contact-form-row { grid-template-columns: 1fr; }
      .contact-add-btn { width: 100%; }
    }
  </style>
</head>
<body>

<!-- Login Page -->
<div id="login-page">
  <div class="login-card">
    <div class="login-brand">
      <span class="pc-logo pc-logo-large" aria-hidden="true">
        <img src="/static/logo.png" alt="">
      </span>
      <div>
        <h1>PennCloud</h1>
        <p id="login-subtitle">Sign in to your account</p>
      </div>
    </div>
    <div class="form-group">
      <label>Username</label>
      <input id="username-in" type="text" placeholder="Your username" autocomplete="username">
      <div id="username-help" class="field-help signup-only" style="display:none">Use lowercase letters, numbers, dash, or underscore. Start with a letter.</div>
    </div>
    <div class="form-group">
      <label>Password</label>
      <div class="password-field">
        <input id="password-in" type="password" placeholder="Your password" autocomplete="current-password">
        <button class="password-toggle" type="button" onclick="toggleAuthPassword()" aria-label="Show password" title="Show password">
          <svg class="icon-eye" viewBox="0 0 24 24" aria-hidden="true">
            <path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"/>
            <circle cx="12" cy="12" r="3"/>
          </svg>
          <svg class="icon-eye-off" viewBox="0 0 24 24" aria-hidden="true">
            <path d="M3 3l18 18"/>
            <path d="M10.6 10.6A3 3 0 0 0 13.4 13.4"/>
            <path d="M7.1 7.6C4.2 9.3 2.5 12 2.5 12s3.5 6 9.5 6c1.8 0 3.3-.5 4.6-1.2"/>
            <path d="M14.1 6.3C18.7 7.2 21.5 12 21.5 12s-.9 1.5-2.5 3"/>
          </svg>
        </button>
      </div>
      <div id="password-help" class="field-help signup-only" style="display:none">Use at least 8 characters.</div>
    </div>
    <div class="form-group signup-only" id="confirm-password-group" style="display:none">
      <label>Confirm password</label>
      <input id="confirm-password-in" type="password" placeholder="Re-enter your password" autocomplete="new-password">
    </div>
    <button class="btn btn-primary" id="login-btn" onclick="doLogin()">Sign in</button>
    <div id="login-error" class="error-msg"></div>
    <div class="auth-divider" id="auth-divider">New to PennCloud?</div>
    <button class="btn-link" id="auth-switch-btn" onclick="showSignup()">Create an account</button>
  </div>
</div>

<!-- App Shell -->
<div id="app">
  <div class="topbar">
    <div class="brand">
      <span class="pc-logo pc-logo-small" aria-hidden="true">
        <img src="/static/logo.png" alt="">
      </span>
      <span>PennCloud</span>
    </div>
    <div class="user-info">
      <span id="topbar-user"></span>
      <button class="logout-btn" onclick="doLogout()">Sign out</button>
    </div>
  </div>
  <div class="main-layout">
    <div class="sidebar">
      <button class="nav-item" id="nav-inbox" onclick="navigate('inbox')">
        <span class="nav-icon" aria-hidden="true">
          <svg viewBox="0 0 24 24"><path d="M4 6h16v12H4z"/><path d="m4 7 8 6 8-6"/></svg>
        </span> Inbox
      </button>
      <button class="nav-item" id="nav-compose" onclick="navigate('compose')">
        <span class="nav-icon" aria-hidden="true">
          <svg viewBox="0 0 24 24"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L8 18l-4 1 1-4Z"/></svg>
        </span> Compose
      </button>
      <button class="nav-item" id="nav-drive" onclick="navigate('drive')">
        <span class="nav-icon" aria-hidden="true">
          <svg viewBox="0 0 24 24"><path d="M3 7h7l2 2h9v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z"/><path d="M3 7V5a2 2 0 0 1 2-2h4l2 4"/></svg>
        </span> Drive
      </button>
      <button class="nav-item" id="nav-chat" onclick="navigate('chat')">
        <span class="nav-icon" aria-hidden="true">
          <svg viewBox="0 0 24 24"><path d="M21 12a8 8 0 0 1-8 8H7l-4 3 1.4-5A8 8 0 1 1 21 12Z"/></svg>
        </span> Chat
      </button>
      <button class="nav-item" id="nav-settings" onclick="navigate('settings')">
        <span class="nav-icon" aria-hidden="true">
          <svg viewBox="0 0 24 24"><path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1a2 2 0 0 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.6V21a2 2 0 0 1-4 0v-.1a1.7 1.7 0 0 0-1-1.6 1.7 1.7 0 0 0-1.9.3l-.1.1A2 2 0 0 1 4.2 17l.1-.1A1.7 1.7 0 0 0 4.6 15a1.7 1.7 0 0 0-1.6-1H3a2 2 0 0 1 0-4h.1a1.7 1.7 0 0 0 1.6-1 1.7 1.7 0 0 0-.3-1.9l-.1-.1A2 2 0 0 1 7 4.2l.1.1a1.7 1.7 0 0 0 1.9.3 1.7 1.7 0 0 0 1-1.6V3a2 2 0 0 1 4 0v.1a1.7 1.7 0 0 0 1 1.6 1.7 1.7 0 0 0 1.9-.3l.1-.1A2 2 0 0 1 19.8 7l-.1.1a1.7 1.7 0 0 0-.3 1.9 1.7 1.7 0 0 0 1.6 1h.1a2 2 0 0 1 0 4H21a1.7 1.7 0 0 0-1.6 1Z"/></svg>
        </span> Settings
      </button>
    </div>
    <div class="content" id="content">
      <div class="spinner">Loading...</div>
    </div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
// =============================================================================
// PennCloud SPA Router (F4 innovation)
// Zero full-page reloads. All navigation is fetch() + DOM swap.
// =============================================================================

let currentUser = '';
let currentPath = '/';
let currentMailFolder = 'inbox';
let eventSource = null;
let sendInFlight = false;
let contactsCache = [];
let quotaCache = null;
let sseReconnectTimer = null;
let composeAttachments = [];
let composeDraft = { reply_to: '', subject: '', body: '' };
let authActive = false;
let inboxRenderSeq = 0;
let knownInboxUids = new Set();
let currentView = 'inbox';
let currentChatRoom = 'general';
let chatPollTimer = null;
let chatMessagesCache = [];
let chatSending = false;
let currentChatMode = 'room';
let currentChatPeer = '';
let chatDmPeers = [];
let dmUnreadCounts = {};
let dmLastSeenIds = {};
let appRouteSeq = 0;
let appHealthTimer = null;
let frontendHealthFailures = 0;
let frontendRedirecting = false;
let lastFrontendNodes = [];
let pendingRoute = null;
let authMode = 'login';
let uploadProgresses = {};
const frontendDownParam = 'pc_down';

function renderPageNotFound() {
  cleanupClientSession();
  document.title = '404 Not Found';
  document.body.innerHTML = '<html><body><h1>404 Not Found</h1></body></html>';
}

function frontendIdForPort(port) {
  if (port === '8090') return 'fe1';
  if (port === '8091') return 'fe2';
  if (port === '8092') return 'fe3';
  return '';
}

function downFrontendSet(extraId = '') {
  const params = new URLSearchParams(window.location.search || '');
  const set = new Set((params.get(frontendDownParam) || '').split(',').filter(Boolean));
  if (extraId) set.add(extraId);
  return set;
}

function queryWithDownFrontend(extraId = '') {
  const params = new URLSearchParams(window.location.search || '');
  const down = downFrontendSet(extraId);
  if (down.size) params.set(frontendDownParam, Array.from(down).join(','));
  else params.delete(frontendDownParam);
  const qs = params.toString();
  return qs ? '?' + qs : '';
}

function appRouteFromLocation() {
  const path = window.location.pathname || '/';
  const qs = new URLSearchParams(window.location.search || '');
  if (path === '/' || path === '/index.html') return {view: 'inbox', params: {folder: qs.get('folder') || 'inbox'}, explicit: false};
  if (path === '/inbox') return {view: 'inbox', params: {folder: qs.get('folder') || 'inbox'}, explicit: true};
  if (path === '/compose') return {view: 'compose', params: {}, explicit: true};
  if (path === '/drive') return {view: 'drive', params: {path: qs.get('path') || '/'}, explicit: true};
  if (path === '/chat') return {view: 'chat', params: {room: qs.get('room') || 'general', dm: qs.get('dm') || ''}, explicit: true};
  if (path === '/settings') return {view: 'settings', params: {}, explicit: true};
  return {view: 'inbox', params: {folder: 'inbox'}, explicit: false};
}

function appRouteUrl(view, params = {}) {
  const qs = new URLSearchParams();
  const down = Array.from(downFrontendSet());
  if (down.length) qs.set(frontendDownParam, down.join(','));
  if (view === 'inbox' && params.folder && params.folder !== 'inbox') qs.set('folder', params.folder);
  if (view === 'drive' && params.path && params.path !== '/') qs.set('path', params.path);
  if (view === 'chat') {
    if (params.dm) qs.set('dm', params.dm);
    else if (params.room && params.room !== 'general') qs.set('room', params.room);
  }
  const query = qs.toString();
  return '/' + view + (query ? '?' + query : '');
}

function routeLabel(route) {
  const labels = {inbox: 'Inbox', compose: 'Compose', drive: 'Drive', chat: 'Chat', settings: 'Settings'};
  return labels[route && route.view] || 'PennCloud';
}

function peerFrontendPorts(extraDownId = '') {
  const port = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
  const currentId = frontendIdForPort(port);
  const down = downFrontendSet(extraDownId || currentId);
  const livePeers = (lastFrontendNodes || [])
    .filter(n => n && n.alive && n.id !== currentId && !down.has(n.id) && n.port)
    .map(n => String(n.port));
  if (livePeers.length) return [...new Set(livePeers)];
  const fallback =
    port === '8090' ? ['8091', '8092'] :
    port === '8091' ? ['8092', '8090'] :
    port === '8092' ? ['8091', '8090'] :
    ['8090', '8091', '8092'];
  return fallback.filter(p => !down.has(frontendIdForPort(p)));
}

function redirectToPeerApp() {
  if (frontendRedirecting) return;
  const currentPort = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
  const currentId = frontendIdForPort(currentPort);
  const peers = peerFrontendPorts(currentId);
  if (!peers.length) return;
  frontendRedirecting = true;
  const path = window.location.pathname || '/';
  const query = queryWithDownFrontend(currentId);
  const hash = window.location.hash || '';
  window.location.replace(`${window.location.protocol}//${window.location.hostname}:${peers[0]}${path}${query}${hash}`);
}

async function probeFrontendStatus(port) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 450);
  try {
    const url = `${window.location.protocol}//${window.location.hostname}:${port}/api/admin/status`;
    const r = await fetch(url, { cache: 'no-store', signal: controller.signal });
    if (!r.ok) return null;
    const data = await r.json();
    return data && data.ok ? data : null;
  } catch (_) {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function markFrontendHealthFailure() {
  frontendHealthFailures += 1;
  const currentPort = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
  const currentId = frontendIdForPort(currentPort);
  const peers = peerFrontendPorts(currentId);

  for (const port of peers) {
    const data = await probeFrontendStatus(port);
    if (!data) continue;

    lastFrontendNodes = data.frontend_nodes || [];
    const backendAlive = (data.backend_nodes || []).some(n => !!n.alive);
    if (!backendAlive) return false;

    if (String(port) !== String(currentPort) && !frontendRedirecting) {
      frontendRedirecting = true;
      const path = window.location.pathname || '/';
      const query = queryWithDownFrontend(currentId);
      const hash = window.location.hash || '';
      window.location.replace(`${window.location.protocol}//${window.location.hostname}:${port}${path}${query}${hash}`);
      return null;
    }
    return true;
  }

  return false;
}

async function backendStorageAvailable() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 700);
  try {
    const r = await fetch('/api/admin/status', { cache: 'no-store', signal: controller.signal });
    // Public deployments protect admin status with ADMIN_TOKEN. A 403 here
    // means "health endpoint is private", not that user-facing storage is down.
    if (r.status === 403) return true;
    if (!r.ok) return await markFrontendHealthFailure();
    const data = await r.json();
    frontendHealthFailures = 0;
    lastFrontendNodes = data.frontend_nodes || [];
    const backendAlive = (data.backend_nodes || []).some(n => !!n.alive);
    // The user-facing app requires storage. A live frontend alone is only a
    // handoff target; if every backend is down, match admin's 404 behavior.
    return backendAlive;
  } catch (_) {
    // If this frontend was killed, local fetches fail even when other frontends
    // and backends are alive. Treat that as a peer-redirect condition, not 404.
    return await markFrontendHealthFailure();
  } finally {
    clearTimeout(timeout);
  }
}

async function ensureBackendStorageAvailable() {
  const available = await backendStorageAvailable();
  if (available === false) {
    renderPageNotFound();
    return false;
  }
  return available === true;
}

async function loadContacts(force = false) {
  if (!force && contactsCache.length) return contactsCache;
  try {
    const r = await fetch('/api/contacts');
    const data = await r.json();
    contactsCache = data.ok ? (data.contacts || []) : [];
  } catch (e) {
    contactsCache = [];
  }
  return contactsCache;
}

async function loadQuota(force = false) {
  if (!force && quotaCache) return quotaCache;
  try {
    const r = await fetch('/api/quota');
    const data = await r.json();
    quotaCache = data.ok ? data : null;
  } catch (e) {
    quotaCache = null;
  }
  return quotaCache;
}

function formatBytes(n) {
  const num = Number(n || 0);
  if (num < 1024) return `${num} B`;
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`;
  if (num < 1024 * 1024 * 1024) return `${(num / (1024 * 1024)).toFixed(1)} MB`;
  return `${(num / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatRate(bytesPerSecond) {
  const n = Number(bytesPerSecond || 0);
  if (!Number.isFinite(n) || n <= 0) return 'calculating speed...';
  return `${formatBytes(n)}/s`;
}

function driveIconSvg(type) {
  if (type === 'folder') {
    return `<svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M3.5 7.5h6.1l1.7 2h9.2v7.6a2.4 2.4 0 0 1-2.4 2.4H5.9a2.4 2.4 0 0 1-2.4-2.4V7.5Z"></path>
      <path d="M3.5 7.5V6.9a2.4 2.4 0 0 1 2.4-2.4h3.2l2 2.3"></path>
    </svg>`;
  }
  return `<svg viewBox="0 0 24 24" aria-hidden="true">
    <path d="M7 3.8h6.8L18 8v12.2H7V3.8Z"></path>
    <path d="M13.8 3.8V8H18"></path>
    <path d="M9.8 12.2h5.4"></path>
    <path d="M9.8 15.4h4.2"></path>
  </svg>`;
}

function driveItemIcon(type) {
  const cls = type === 'folder' ? 'folder' : 'file';
  return `<div class="file-icon ${cls}">${driveIconSvg(type)}</div>`;
}

function driveInlineFolderIcon() {
  return `<span class="inline-folder-icon">${driveIconSvg('folder')}</span>`;
}

function plural(n, word) {
  return `${n} ${word}${n === 1 ? '' : 's'}`;
}

function driveInlineArg(value) {
  return encodeURIComponent(String(value || ''));
}

function driveItemKind(item) {
  return item && item.type === 'folder' ? 'Folder' : 'File';
}

function driveDisplaySize(item) {
  if (item && item.type === 'folder') return 'Folder';
  return formatBytes(item && item.size ? item.size : 0);
}

function buildContactOptions() {
  return (contactsCache || []).map(c =>
    `<option value="${escHtml(c.email)}">${escHtml(c.name)} (${escHtml(c.email)})</option>`
  ).join('');
}

function emailLabel(e, key) {
  if (key === 'to') return e.to_display || e.to || '';
  return e.from_display || e.from || '';
}

// ---- Auth ------------------------------------------------------------------
function validUsername(u) {
  return /^[a-z][a-z0-9_-]{2,31}$/.test(u || '');
}

function validPassword(p) {
  return typeof p === 'string' && p.length >= 8;
}

function usernameHelp() {
  return 'Username must be 3-32 chars, start with a lowercase letter, and use only lowercase letters, numbers, dash, or underscore.';
}

function passwordHelp() {
  return 'Password must be at least 8 characters.';
}

async function doLogin() {
  const u = document.getElementById('username-in').value.trim();
  const p = document.getElementById('password-in').value;
  if (!u || !p) { showError('Please enter username and password.'); return; }
  if (!validUsername(u)) { showError('Invalid username or password.'); return; }

  const r = await fetch('/api/login', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `username=${encodeURIComponent(u)}&password=${encodeURIComponent(p)}`
  });
  const data = await r.json();
  if (data.ok) {
    currentUser = u;
    contactsCache = [];
    quotaCache = null;
    showApp();
  } else {
    showError(data.error || 'Invalid username or password.');
  }
}

function cleanupClientSession() {
  authActive = false;
  if (sseReconnectTimer) {
    clearTimeout(sseReconnectTimer);
    sseReconnectTimer = null;
  }
  if (eventSource) {
    try { eventSource.close(); } catch (_) {}
    eventSource = null;
  }
  knownInboxUids = new Set();
  currentMailFolder = 'inbox';
  sendInFlight = false;
  currentView = 'inbox';
  currentChatRoom = 'general';
  chatMessagesCache = [];
  if (chatPollTimer) { clearTimeout(chatPollTimer); chatPollTimer = null; }
  if (appHealthTimer) { clearInterval(appHealthTimer); appHealthTimer = null; }
  chatSending = false;
  currentChatMode = 'room';
  currentChatPeer = '';
  chatDmPeers = [];
  dmUnreadCounts = {};
  dmLastSeenIds = {};
  frontendHealthFailures = 0;
}

async function doLogout() {
  cleanupClientSession();
  try {
    await fetch('/api/logout', { method: 'POST' });
  } catch (_) {}
  contactsCache = [];
  quotaCache = null;
  pendingRoute = null;
  showLogin();
}

function showSignup() {
  setAuthMode('signup');
}

async function doSignup() {
  const u = document.getElementById('username-in').value.trim();
  const p = document.getElementById('password-in').value;
  const p2 = document.getElementById('confirm-password-in').value;
  if (!u || !p) { showError('Please enter username and password.'); return; }
  if (!validUsername(u)) { showError(usernameHelp()); return; }
  if (!validPassword(p)) { showError(passwordHelp()); return; }
  if (p !== p2) { showError('Passwords do not match.'); return; }
  const r = await fetch('/api/signup', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `username=${encodeURIComponent(u)}&password=${encodeURIComponent(p)}`
  });
  const data = await r.json();
  if (data.ok) {
    currentUser = u;
    contactsCache = [];
    quotaCache = null;
    showApp();
  }
  else showError(data.error || 'Signup failed.');
}

function toggleAuthPassword() {
  const p = document.getElementById('password-in');
  const p2 = document.getElementById('confirm-password-in');
  const btn = document.querySelector('.password-toggle');
  const next = p.type === 'password' ? 'text' : 'password';
  p.type = next;
  if (p2) p2.type = next;
  if (btn) {
    const showing = next === 'text';
    btn.classList.toggle('showing', showing);
    btn.setAttribute('aria-label', showing ? 'Hide password' : 'Show password');
    btn.setAttribute('title', showing ? 'Hide password' : 'Show password');
  }
}

function resetAuthPasswordVisibility() {
  const p = document.getElementById('password-in');
  const p2 = document.getElementById('confirm-password-in');
  const btn = document.querySelector('.password-toggle');
  if (p) p.type = 'password';
  if (p2) p2.type = 'password';
  if (btn) {
    btn.classList.remove('showing');
    btn.setAttribute('aria-label', 'Show password');
    btn.setAttribute('title', 'Show password');
  }
}

function setAuthMode(mode) {
  authMode = mode === 'signup' ? 'signup' : 'login';
  const signup = authMode === 'signup';
  document.getElementById('login-subtitle').textContent = signup ? 'Create a new account' : 'Sign in to your account';
  document.getElementById('login-btn').textContent = signup ? 'Create account' : 'Sign in';
  document.getElementById('login-btn').onclick = signup ? doSignup : doLogin;
  document.getElementById('auth-divider').textContent = signup ? 'Already have an account?' : 'New to PennCloud?';
  document.getElementById('auth-switch-btn').textContent = signup ? 'Back to sign in' : 'Create an account';
  document.getElementById('auth-switch-btn').onclick = signup ? (() => showLogin()) : showSignup;
  document.querySelectorAll('.signup-only').forEach(el => {
    el.style.display = signup ? '' : 'none';
  });
  document.getElementById('password-in').autocomplete = signup ? 'new-password' : 'current-password';
  resetAuthPasswordVisibility();
  const err = document.getElementById('login-error');
  err.textContent = '';
  err.style.display = 'none';
}

function showLogin(message = '') {
  document.getElementById('login-page').style.display = 'flex';
  document.getElementById('app').style.display = 'none';
  setAuthMode('login');
  const err = document.getElementById('login-error');
  if (message) {
    err.textContent = message;
    err.style.display = 'block';
  } else {
    err.textContent = '';
    err.style.display = 'none';
  }
}

async function showApp(route = null) {
  document.getElementById('login-page').style.display = 'none';
  document.getElementById('app').style.display = 'flex';
  document.getElementById('topbar-user').textContent = currentUser;
  authActive = true;
  knownInboxUids = new Set();
  await Promise.all([loadContacts(true), loadQuota(true)]);
  startSSE();
  startAppHealthMonitor();
  const target = route || pendingRoute || appRouteFromLocation();
  pendingRoute = null;
  navigate(target.view || 'inbox', target.params || {folder: 'inbox'}, {replace: true});
}

function showError(msg) {
  const el = document.getElementById('login-error');
  el.textContent = msg;
  el.style.display = 'block';
}

function startAppHealthMonitor() {
  if (appHealthTimer) clearInterval(appHealthTimer);
  appHealthTimer = setInterval(async () => {
    if (!authActive) return;
    const available = await backendStorageAvailable();
    if (available === false) renderPageNotFound();
  }, 1500);
}

// ---- Navigation ------------------------------------------------------------
function navigate(view, params = {}, options = {}) {
  currentView = view;
  const routeSeq = ++appRouteSeq;
  if (chatPollTimer) { clearTimeout(chatPollTimer); chatPollTimer = null; }
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  const navKey = view === 'email' ? 'inbox' : view;
  const navEl = document.getElementById('nav-' + navKey);
  if (navEl) navEl.classList.add('active');
  const url = appRouteUrl(view, params);
  if (options.replace) history.replaceState({view, params}, '', url);
  else history.pushState({view, params}, '', url);

  const content = document.getElementById('content');
  content.innerHTML = '<div class="spinner">Loading...</div>';
  renderProtectedView(view, params, routeSeq);
}

async function renderProtectedView(view, params = {}, routeSeq = appRouteSeq) {
  if (!(await ensureBackendStorageAvailable())) return;
  if (routeSeq !== appRouteSeq) return;
  switch (view) {
    case 'inbox':   renderInbox(params.folder || 'inbox');  break;
    case 'compose': renderCompose(params); break;
    case 'drive':   renderDrive(params.path || '/'); break;
    case 'chat':    renderChat(params.room || currentChatRoom || 'general', params.dm || ''); break;
    case 'email':   renderEmail(params.uid, params.folder || currentMailFolder || 'inbox'); break;
    case 'settings':renderSettings(); break;
  }
}

window.addEventListener('popstate', e => {
  const route = e.state || appRouteFromLocation();
  navigate(route.view, route.params || {}, {replace: true});
});

// Expose frequently used handlers explicitly for reliability with dynamic views.
window.navigate = navigate;
window.openReply = openReply;
window.openForward = openForward;
window.deleteEmail = deleteEmail;
window.restoreEmail = restoreEmail;
window.sendEmail = sendEmail;
window.sendChatMessage = sendChatMessage;
window.switchChatRoom = switchChatRoom;

function mailFolderTabs(activeFolder, counts = {}) {
  const folders = [
    {key: 'inbox', label: 'Inbox'},
    {key: 'sent', label: 'Sent'},
    {key: 'trash', label: 'Trash'}
  ];
  return `<div style="display:flex;gap:8px;margin:14px 0 18px 0">${folders.map(f => `
    <button class="action-btn ${f.key === activeFolder ? 'active-folder-tab' : ''}"
            style="${f.key === activeFolder ? 'background:var(--penn-blue);color:white;border-color:var(--penn-blue);' : ''}"
            onclick="navigate('inbox',{folder:'${f.key}'})">
      ${f.label} <span style="opacity:.7">${counts[f.key] || ''}</span>
    </button>`).join('')}</div>`;
}

async function renderInbox(folder = 'inbox', _retry = 0) {
  currentMailFolder = folder;
  const mySeq = ++inboxRenderSeq;
  const folders = ['inbox', 'sent', 'trash'];
  const results = await Promise.all(
    folders.map(async f => {
      try {
        const r = await fetch('/api/inbox?folder=' + encodeURIComponent(f));
        const data = await r.json();
        return data;
      } catch (_) {
        return { ok: false, emails: [] };
      }
    })
  );

  if (mySeq !== inboxRenderSeq) return;

  const counts = {};
  results.forEach((res, idx) => {
    counts[folders[idx]] = (res.emails || []).length;
  });

  const active = results[folders.indexOf(folder)] || results[0];
  const content = document.getElementById('content');

  if (!active.ok) {
    if (_retry < 2) {
      content.innerHTML = '<div class="spinner">Reconnecting to storage...</div>';
      await new Promise(r => setTimeout(r, 1500));
      if (mySeq !== inboxRenderSeq) return;
      return renderInbox(folder, _retry + 1);
    }
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<p>Error loading mail.</p>';
    return;
  }

  const emails = active.emails || [];
  if (folder === 'inbox') {
    knownInboxUids = new Set(emails.map(e => e.uid).filter(Boolean));
  }

  const title = folder.charAt(0).toUpperCase() + folder.slice(1);
  content.innerHTML = `
    <div class="page-title">
      ${title} <span style="font-size:14px;color:var(--muted);font-weight:400">${emails.length} messages</span>
      <button class="compose-btn" onclick="navigate('compose')">+ Compose</button>
    </div>
    ${mailFolderTabs(folder, counts)}
    <div class="email-list" id="email-list">
      ${emails.length === 0
        ? `<div style="padding:40px;text-align:center;color:var(--muted)">No messages in ${escHtml(title.toLowerCase())}</div>`
        : emails.map(e => `
          <div class="email-row" data-uid="${e.uid}" onclick="navigate('email', {uid:'${e.uid}', folder:'${folder}'})">
            <div class="email-from">${escHtml(folder === 'sent' ? emailLabel(e,'to') : emailLabel(e,'from'))}</div>
            <div class="email-subj">${escHtml(e.subject)}</div>
            <div class="email-time">${escHtml(e.time)}</div>
          </div>`).join('')}
    </div>`;
}

function prependEmailRow(email) {
  // SSE should act only as a refresh hint. The inbox response is the source of truth.
}


async function renderEmail(uid, folder = 'inbox') {
  currentMailFolder = folder;
  const r = await fetch(`/api/email/${encodeURIComponent(uid)}?folder=${encodeURIComponent(folder)}`);
  const data = await r.json();
  const content = document.getElementById('content');
  if (!data.ok) { content.innerHTML = '<p>Email not found.</p>'; return; }
  const e = data.email;
  const attachments = Array.isArray(e.attachments) ? e.attachments : [];
  const deleteLabel = folder === 'trash' ? 'Delete permanently' : 'Move to Trash';
  content.innerHTML = `
    <button id="mail-back-btn" style="margin-bottom:16px;background:none;border:none;cursor:pointer;color:var(--accent);font-size:13px;">
      &larr; Back to ${escHtml(folder)}
    </button>
    <div class="email-view">
      <h2>${escHtml(e.subject)}</h2>
      <div class="email-meta">
        <span><b>From:</b> ${escHtml(e.from_display || e.from)}</span>
        <span><b>To:</b> ${escHtml(e.to_display || e.to || currentUser)}</span>
        <span><b>Date:</b> ${escHtml(e.time)}</span>
        <span><b>Folder:</b> ${escHtml(folder)}</span>
      </div>
      ${attachments.length ? `<div style="margin:14px 0 10px"><b>Attachments:</b><div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">${attachments.map(a => `<a class="action-btn" style="text-decoration:none" href="/api/mail/attachment?uid=${encodeURIComponent(uid)}&att=${encodeURIComponent(a.id)}&folder=${encodeURIComponent(folder)}">📎 ${escHtml(a.name)} (${formatBytes(a.size || 0)})</a>`).join('')}</div></div>` : ''}
      <div class="email-actions">
        ${folder !== 'trash' ? `<button class="action-btn" id="reply-btn">Reply</button>
        <button class="action-btn" id="forward-btn">Forward</button>` : ''}
        ${folder === 'trash' ? `<button class="action-btn" id="restore-btn">Restore</button>` : ''}
        <button class="action-btn danger" id="delete-btn">${deleteLabel}</button>
      </div>
      <div class="email-body">${escHtml(e.body)}</div>
    </div>`;

  const backBtn = document.getElementById('mail-back-btn');
  if (backBtn) backBtn.onclick = () => navigate('inbox', {folder});
  const replyBtn = document.getElementById('reply-btn');
  if (replyBtn) replyBtn.onclick = () => openReply(uid, folder);
  const forwardBtn = document.getElementById('forward-btn');
  if (forwardBtn) forwardBtn.onclick = () => openForward(uid, folder);
  const restoreBtn = document.getElementById('restore-btn');
  if (restoreBtn) restoreBtn.onclick = () => restoreEmail(uid);
  const deleteBtn = document.getElementById('delete-btn');
  if (deleteBtn) deleteBtn.onclick = () => deleteEmail(uid, folder);
}

async function openReply(uid, folder = currentMailFolder || 'inbox') {
  try {
    const r = await fetch(`/api/email/${encodeURIComponent(uid)}?folder=${encodeURIComponent(folder)}`);
    const data = await r.json();
    if (!data.ok || !data.email) return showToast('Could not load email');
    const e = data.email;
    const replyBody = `

--- Original Message ---
From: ${e.from_display || e.from || ''}
Date: ${e.time || ''}
Subject: ${e.subject || ''}

${e.body || ''}`;
    navigate('compose', {
      reply_to: e.from || '',
      subject: (e.subject || '').startsWith('Re:') ? (e.subject || '') : 'Re: ' + (e.subject || ''),
      body: replyBody
    });
  } catch (err) {
    console.error('openReply failed', err);
    showToast('Could not open reply');
  }
}

async function openForward(uid, folder = currentMailFolder || 'inbox') {
  try {
    const r = await fetch(`/api/email/${encodeURIComponent(uid)}?folder=${encodeURIComponent(folder)}`);
    const data = await r.json();
    if (!data.ok || !data.email) return showToast('Could not load email');
    const e = data.email;
    const forwarded = `

--- Forwarded ---
From: ${e.from_display || e.from || ''}
To: ${e.to_display || e.to || currentUser || ''}
Date: ${e.time || ''}
Subject: ${e.subject || ''}

${e.body || ''}`;
    navigate('compose', {
      subject: 'Fwd: ' + (e.subject || ''),
      body: forwarded,
      attachments: Array.isArray(e.attachments) ? e.attachments : []
    });
  } catch (err) {
    showToast('Could not open forward');
  }
}

async function deleteEmail(uid, folder = currentMailFolder || 'inbox') {
  try {
    const r = await fetch('/api/delete-email', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: `uid=${encodeURIComponent(uid)}&folder=${encodeURIComponent(folder)}`
    });
    const data = await r.json();
    if (!data.ok) {
      showToast(data.error || 'Delete failed');
      return;
    }
    showToast(data.deleted ? 'Email permanently deleted' : 'Email moved to Trash');
    navigate('inbox', {folder: data.deleted ? 'trash' : 'trash'});
  } catch (err) {
    console.error('deleteEmail failed', err);
    showToast('Delete failed');
  }
}

async function restoreEmail(uid) {
  try {
    const r = await fetch('/api/restore-email', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: `uid=${encodeURIComponent(uid)}`
    });
    const data = await r.json();
    showToast(data.ok ? 'Email restored to Inbox' : (data.error || 'Restore failed'));
    if (data.ok) navigate('inbox', {folder: 'inbox'});
  } catch (err) {
    console.error('restoreEmail failed', err);
    showToast('Restore failed');
  }
}

function renderComposeAttachmentList() {
  return (composeAttachments || []).map(a => `
    <div style="display:flex;align-items:center;gap:8px;padding:6px 10px;border:1px solid var(--border);border-radius:8px;background:#fff">
      <span>📎 ${escHtml(a.name)} <span style="color:var(--muted)">(${formatBytes(a.size || 0)})</span></span>
      <button type="button" class="action-btn" onclick="removeComposeAttachment('${escHtml(a.id)}')">Remove</button>
    </div>`).join('');
}

function rememberComposeDraft() {
  const toEl = document.getElementById('to-in');
  const subjectEl = document.getElementById('subject-in');
  const bodyEl = document.getElementById('body-in');
  composeDraft = {
    reply_to: toEl ? toEl.value : (composeDraft.reply_to || ''),
    subject: subjectEl ? subjectEl.value : (composeDraft.subject || ''),
    body: bodyEl ? bodyEl.value : (composeDraft.body || '')
  };
}

async function renderCompose(params = {}) {
  await loadContacts();
  composeDraft = {
    reply_to: params.reply_to !== undefined ? params.reply_to : (composeDraft.reply_to || ''),
    subject: params.subject !== undefined ? params.subject : (composeDraft.subject || ''),
    body: params.body !== undefined ? params.body : (composeDraft.body || '')
  };
  if (Array.isArray(params.attachments)) composeAttachments = params.attachments.slice();
  document.getElementById('content').innerHTML = `
    <div class="page-title">Compose</div>
    <div class="compose-form">
      <input id="to-in" list="contacts-list" type="text" placeholder="To (username or user@example.com)" value="${escHtml(composeDraft.reply_to || '')}">
      <datalist id="contacts-list">${buildContactOptions()}</datalist>
      <div style="font-size:12px;color:var(--muted);margin-top:-6px;margin-bottom:10px">Address book suggestions are available as you type.</div>
      <input id="subject-in" type="text" placeholder="Subject" value="${escHtml(composeDraft.subject || '')}">
      <textarea id="body-in" placeholder="Write your message...">${escHtml(composeDraft.body || '')}</textarea>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
        <button class="action-btn" onclick="document.getElementById('mail-attachment-input').click()">Attach file</button>
        <input id="mail-attachment-input" type="file" style="display:none">
        <span style="font-size:12px;color:var(--muted)">Local mail attachments</span>
      </div>
      <div id="compose-attachments" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">${renderComposeAttachmentList()}</div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="sendEmail()">Send</button>
        <button class="action-btn" onclick="addCurrentRecipientToContacts()">Add recipient to contacts</button>
        <button class="action-btn" onclick="navigate('inbox',{folder:'inbox'})">Cancel</button>
      </div>
      <div id="send-error" class="error-msg"></div>
    </div>`;

  const fileInput = document.getElementById('mail-attachment-input');
  if (fileInput) {
    fileInput.addEventListener('change', async (ev) => {
      const file = ev.target.files && ev.target.files[0];
      if (!file) return;
      await uploadMailAttachment(file);
      ev.target.value = '';
    });
  }
}

async function uploadMailAttachment(file) {
  rememberComposeDraft();
  const errBox = document.getElementById('send-error');
  if (file.size > 10 * 1024 * 1024) {
    errBox.textContent = 'Attachment must be 10 MB or smaller.';
    errBox.style.display = 'block';
    return;
  }
  const fd = new FormData();
  fd.append('attachment', file);
  try {
    const r = await fetch('/api/mail/upload-attachment', { method: 'POST', body: fd });
    const data = await r.json();
    if (!data.ok) {
      errBox.textContent = data.error || 'Attachment upload failed.';
      errBox.style.display = 'block';
      return;
    }
    composeAttachments.push(data.attachment);
    renderCompose(composeDraft);
    showToast('Attachment uploaded');
  } catch (e) {
    errBox.textContent = 'Attachment upload failed.';
    errBox.style.display = 'block';
  }
}

function removeComposeAttachment(id) {
  rememberComposeDraft();
  composeAttachments = (composeAttachments || []).filter(a => a.id !== id);
  renderCompose(composeDraft);
}

async function sendEmail() {
  if (sendInFlight) return;

  const to      = document.getElementById('to-in').value.trim();
  const subject = document.getElementById('subject-in').value.trim();
  const body    = document.getElementById('body-in').value;
  const errBox  = document.getElementById('send-error');
  const sendBtn = document.querySelector('.compose-form .btn.btn-primary');

  composeDraft = { reply_to: to, subject, body };

  if (!to || !subject) {
    errBox.textContent = 'To and Subject are required.';
    errBox.style.display = 'block';
    return;
  }

  sendInFlight = true;
  if (sendBtn) {
    sendBtn.disabled = true;
    sendBtn.textContent = 'Sending...';
    sendBtn.style.opacity = '0.7';
    sendBtn.style.cursor = 'not-allowed';
  }

  try {
    const attachmentIds = (composeAttachments || []).map(a => a.id).join(',');
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const r = await fetch('/api/send', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      signal: controller.signal,
      body: `to=${encodeURIComponent(to)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}&attachment_ids=${encodeURIComponent(attachmentIds)}`
    });
    clearTimeout(timeout);
    const data = await r.json();
    if (data.ok) {
      composeAttachments = [];
      composeDraft = { reply_to: '', subject: '', body: '' };
      showToast(data.external ? 'External email accepted by SMTP. Saved in Sent.' : 'Email sent! Saved in Sent as well.');
      navigate('inbox', {folder: 'sent'});
    } else {
      errBox.textContent = data.error || 'Send failed.';
      errBox.style.display = 'block';
    }
  } catch (e) {
    errBox.textContent = e && e.name === 'AbortError' ? 'Send timed out. External SMTP may be blocked.' : 'Send failed.';
    errBox.style.display = 'block';
  } finally {
    sendInFlight = false;
    if (sendBtn) {
      sendBtn.disabled = false;
      sendBtn.textContent = 'Send';
      sendBtn.style.opacity = '1';
      sendBtn.style.cursor = 'pointer';
    }
  }
}

async function addCurrentRecipientToContacts() {
  rememberComposeDraft();
  const email = document.getElementById('to-in').value.trim();
  if (!email) return showToast('Enter a recipient first.');
  const name = await appPrompt({
    title: 'Save contact',
    description: `Add ${email} to your PennCloud contacts.`,
    placeholder: 'Display name',
    submitText: 'Save contact'
  });
  if (!name) return;
  const r = await fetch('/api/contacts/add', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `email=${encodeURIComponent(email)}&name=${encodeURIComponent(name)}`
  });
  const data = await r.json();
  if (data.ok) {
    await loadContacts(true);
    showToast('Contact saved.');
    renderCompose(composeDraft);
  } else {
    showToast(data.error || 'Could not save contact');
  }
}


function renderChatRooms(activeRoom, rooms, counts) {
  return `<div style="display:flex;gap:10px;flex-wrap:wrap;margin:12px 0 12px 0">${rooms.map(r => `
    <button class="action-btn ${currentChatMode === 'room' && r === activeRoom ? 'active-folder-tab' : ''}"
            style="${currentChatMode === 'room' && r === activeRoom ? 'background:var(--penn-blue);color:white;border-color:var(--penn-blue);' : ''}"
            onclick="switchChatRoom('${r}')">
      #${escHtml(r)} <span style="opacity:.75">${counts && counts[r] ? counts[r] : ''}</span>
    </button>`).join('')}</div>`;
}

function normalizeDmPeers(peers, activePeer) {
  const out = Array.isArray(peers) ? peers.filter(Boolean) : [];
  if (activePeer && !out.includes(activePeer)) out.unshift(activePeer);
  return [...new Set(out)];
}

function renderDmPeers(activePeer, peers) {
  const list = normalizeDmPeers(peers, activePeer);
  if (list.length === 0) {
    return `<div id="dm-peers-wrap" style="padding:10px 0 6px 0;color:var(--muted);font-size:13px">No direct conversations yet.</div>`;
  }
  return `<div id="dm-peers-wrap" style="display:flex;gap:10px;flex-wrap:wrap;margin:0 0 18px 0">${list.map(peer => {
    const active = currentChatMode === 'dm' && peer === activePeer;
    const unread = dmUnreadCounts[peer] || 0;
    const activeStyle = active
      ? 'background:var(--penn-blue);color:white;border-color:var(--penn-blue);box-shadow:0 0 0 2px rgba(1,31,91,.18);font-weight:600;'
      : '';
    const badge = unread > 0
      ? `<span style="display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;padding:0 6px;border-radius:999px;background:${active ? 'rgba(255,255,255,.22)' : '#dc2626'};color:white;font-size:11px;font-weight:700;margin-left:6px">${unread > 9 ? '9+' : unread}</span>`
      : '';
    return `
    <button class="action-btn ${active ? 'active-folder-tab' : ''}"
            style="display:inline-flex;align-items:center;${activeStyle}"
            onclick="switchDirectMessage('${peer}')">
      <span>@${escHtml(peer)}</span>${badge}
    </button>`;
  }).join('')}</div>`;
}

function refreshDmPeersUi() {
  const wrap = document.getElementById('dm-peers-wrap');
  if (!wrap) return;
  const html = renderDmPeers(currentChatPeer, chatDmPeers);
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  const next = tmp.firstElementChild;
  if (next) wrap.replaceWith(next);
}

function renderChatMessages(messages) {
  const box = document.getElementById('chat-messages');
  if (!box) return;
  box.innerHTML = messages.length === 0
    ? `<div class="chat-empty">No messages yet in this ${currentChatMode === 'dm' ? 'conversation' : 'room'}.</div>`
    : messages.map(m => {
        const mine = (m.from || '') === currentUser;
        return `
        <div class="chat-bubble ${mine ? 'mine' : ''}">
          <div class="chat-meta">
            <span><b>${escHtml(m.from || '')}</b>${m.to ? ` <span style="opacity:.7">→ ${escHtml(m.to)}</span>` : ''}</span>
            <span>${escHtml(m.time || '')}</span>
          </div>
          <div class="chat-text">${escHtml(m.text || '')}</div>
        </div>`;
      }).join('');
  box.scrollTop = box.scrollHeight;
}

function scheduleChatPoll() {
  if (chatPollTimer) {
    clearTimeout(chatPollTimer);
    chatPollTimer = null;
  }
  if (currentView === 'chat') {
    chatPollTimer = setTimeout(() => {
      chatPollTimer = null;
      if (currentView === 'chat') refreshChatMessages();
    }, 3000);
  }
}

async function loadDmPeers() {
  try {
    const r = await fetch('/api/chat/dms', { cache: 'no-store' });
    const data = await r.json();
    if (data.ok) chatDmPeers = normalizeDmPeers(data.peers || [], currentChatMode === 'dm' ? currentChatPeer : '');
  } catch (_) {}
}

function markCurrentDmSeen() {
  if (currentChatMode !== 'dm' || !currentChatPeer || !Array.isArray(chatMessagesCache) || chatMessagesCache.length === 0) return;
  const last = chatMessagesCache[chatMessagesCache.length - 1];
  if (last && last.id) dmLastSeenIds[currentChatPeer] = last.id;
  dmUnreadCounts[currentChatPeer] = 0;
}

async function syncDmSidebarState() {
  try {
    const peerResp = await fetch('/api/chat/dms', { cache: 'no-store' });
    const peerData = await peerResp.json().catch(() => ({ ok:false, peers:[] }));
    if (!peerData.ok) return;

    const peers = normalizeDmPeers(peerData.peers || [], currentChatMode === 'dm' ? currentChatPeer : '');
    chatDmPeers = peers;

    const results = await Promise.all(peers.map(async peer => {
      try {
        const r = await fetch('/api/chat/dm/messages?peer=' + encodeURIComponent(peer), { cache: 'no-store' });
        const data = await r.json().catch(() => ({ ok:false, messages:[] }));
        return { peer, messages: data.ok ? (data.messages || []) : [] };
      } catch (_) {
        return { peer, messages: [] };
      }
    }));

    const nextUnread = {};
    for (const entry of results) {
      const msgs = entry.messages || [];
      if (currentChatMode === 'dm' && entry.peer === currentChatPeer) {
        const last = msgs.length ? msgs[msgs.length - 1] : null;
        if (last && last.id) dmLastSeenIds[entry.peer] = last.id;
        nextUnread[entry.peer] = 0;
        continue;
      }
      let unread = 0;
      for (let i = msgs.length - 1; i >= 0; --i) {
        const m = msgs[i];
        if (!m || !m.id) continue;
        if (dmLastSeenIds[entry.peer] && m.id === dmLastSeenIds[entry.peer]) break;
        if ((m.from || '') !== currentUser) unread += 1;
      }
      nextUnread[entry.peer] = unread;
    }
    dmUnreadCounts = nextUnread;
    refreshDmPeersUi();
  } catch (_) {}
}

async function refreshChatMessages() {
  if (currentView !== 'chat') return;
  const url = currentChatMode === 'dm'
    ? '/api/chat/dm/messages?peer=' + encodeURIComponent(currentChatPeer)
    : '/api/chat/messages?room=' + encodeURIComponent(currentChatRoom);
  try {
    const r = await fetch(url, { cache: 'no-store' });
    const data = await r.json();
    if (!data.ok || currentView !== 'chat') {
      scheduleChatPoll();
      return;
    }
    chatMessagesCache = data.messages || [];
    if (currentChatMode === 'dm') markCurrentDmSeen();
    renderChatMessages(chatMessagesCache);
    await syncDmSidebarState();
  } catch (_) {
  }
  scheduleChatPoll();
}

function chatHeaderLabel() {
  return currentChatMode === 'dm' ? '@' + currentChatPeer : '#' + currentChatRoom;
}

async function renderChat(room = 'general', dmPeer = '') {
  currentChatMode = dmPeer ? 'dm' : 'room';
  currentChatPeer = dmPeer || '';
  currentChatRoom = room || 'general';
  const content = document.getElementById('content');
  content.innerHTML = '<div class="spinner">Loading...</div>';

  try {
    const [roomsRes, dmRes, msgsRes] = await Promise.all([
      fetch('/api/chat/rooms', { cache: 'no-store' }),
      fetch('/api/chat/dms', { cache: 'no-store' }),
      fetch(currentChatMode === 'dm'
        ? '/api/chat/dm/messages?peer=' + encodeURIComponent(currentChatPeer)
        : '/api/chat/messages?room=' + encodeURIComponent(currentChatRoom), { cache: 'no-store' })
    ]);

    const roomsData = await roomsRes.json().catch(() => ({ ok:false, rooms:[], counts:{} }));
    const dmData = await dmRes.json().catch(() => ({ ok:false, peers:[] }));
    const msgsData = await msgsRes.json().catch(() => ({ ok:false, messages:[] }));

    if (currentView !== 'chat') return;

    if (!roomsData.ok || !dmData.ok || !msgsData.ok) {
      if (!(await ensureBackendStorageAvailable())) return;
      content.innerHTML = '<div style="padding:24px;color:#b91c1c">Could not load chat.</div>';
      return;
    }

    const rooms = (roomsData.rooms || []).map(r => r.name).filter(Boolean);
    const counts = roomsData.counts || {};
    chatDmPeers = normalizeDmPeers(dmData.peers || [], currentChatMode === 'dm' ? currentChatPeer : '');
    chatMessagesCache = msgsData.messages || [];

    content.innerHTML = `
      <div class="page-title" style="display:flex;justify-content:space-between;align-items:center;gap:12px">
        <span>Chat <span id="chat-header-label" style="font-size:14px;color:var(--muted);font-weight:600">${escHtml(chatHeaderLabel())}</span></span>
        <div class="chat-actions">
          <button class="action-btn" onclick="openChatRoomDialog()">+ New room</button>
          <button class="action-btn" onclick="startDirectMessage()">+ Direct message</button>
        </div>
      </div>
      <div class="chat-section-head">Rooms</div>
      ${renderChatRooms(currentChatRoom, rooms, counts)}
      <div class="chat-section-head" style="margin-top:-4px">Direct messages</div>
      ${renderDmPeers(currentChatPeer, chatDmPeers)}
      <div class="chat-panel">
        <div id="chat-messages" class="chat-messages"></div>
        <div class="chat-composer">
          <textarea id="chat-input" class="chat-input" placeholder="Message ${escHtml(chatHeaderLabel())}"></textarea>
          <button id="chat-send-btn" class="btn btn-primary chat-send" onclick="sendChatMessage()">Send</button>
        </div>
      </div>`;

    if (currentChatMode === 'dm') markCurrentDmSeen();
    renderChatMessages(chatMessagesCache);
    const input = document.getElementById('chat-input');
    if (input) {
      input.addEventListener('keydown', e => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          sendChatMessage();
        }
      });
    }
    await syncDmSidebarState();
    scheduleChatPoll();
  } catch (err) {
    if (currentView !== 'chat') return;
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<div style="padding:24px;color:#b91c1c">Could not load chat.</div>';
  }
}

async function switchChatRoom(room) {
  currentChatMode = 'room';
  currentChatPeer = '';
  currentChatRoom = room || currentChatRoom || 'general';
  history.replaceState({view:'chat', params:{room: currentChatRoom}}, '', appRouteUrl('chat', {room: currentChatRoom}));
  await renderChat(currentChatRoom, '');
}

async function switchDirectMessage(peer) {
  if (!peer) return;
  dmUnreadCounts[peer] = 0;
  currentChatMode = 'dm';
  currentChatPeer = peer;
  if (!chatDmPeers.includes(peer)) chatDmPeers = normalizeDmPeers([peer, ...chatDmPeers], peer);

  const header = document.getElementById('chat-header-label');
  if (header) header.textContent = '@' + peer;
  const input = document.getElementById('chat-input');
  if (input) input.placeholder = 'Message @' + peer;
  refreshDmPeersUi();

  history.replaceState({view:'chat', params:{dm: peer}}, '', appRouteUrl('chat', {dm: peer}));
  await renderChat(currentChatRoom || 'general', peer);
}

function closeAppDialog() {
  const el = document.getElementById('app-modal-backdrop');
  if (el) el.remove();
}

function closeChatDialog() {
  closeAppDialog();
}

function openAppDialog({title, description, placeholder = '', submitText = 'Continue',
                        initialValue = '', showInput = true, danger = false, onSubmit}) {
  closeAppDialog();
  const overlay = document.createElement('div');
  overlay.id = 'app-modal-backdrop';
  overlay.className = 'modal-backdrop';
  const inputHtml = showInput
    ? `<input id="app-dialog-input" type="text" placeholder="${escHtml(placeholder)}" value="${escHtml(initialValue)}" autocomplete="off">`
    : '';
  const submitStyle = danger
    ? 'width:auto;padding:9px 18px;background:var(--danger)'
    : 'width:auto;padding:9px 18px';
  overlay.innerHTML = `
    <div class="chat-dialog" role="dialog" aria-modal="true">
      <h3>${escHtml(title)}</h3>
      <p>${escHtml(description)}</p>
      ${inputHtml}
      <div id="app-dialog-error" class="error-msg" style="display:none"></div>
      <div class="chat-dialog-actions">
        <button class="action-btn" type="button" id="app-dialog-cancel">Cancel</button>
        <button class="btn btn-primary" type="button" id="app-dialog-submit" style="${submitStyle}">${escHtml(submitText)}</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  const input = overlay.querySelector('#app-dialog-input');
  const err = overlay.querySelector('#app-dialog-error');
  const submit = async () => {
    err.style.display = 'none';
    err.textContent = '';
    const value = input ? input.value.trim() : '';
    try {
      await onSubmit(value, err);
    } catch (_) {
      err.textContent = 'Something went wrong. Please try again.';
      err.style.display = 'block';
    }
  };
  overlay.querySelector('#app-dialog-cancel').addEventListener('click', closeAppDialog);
  overlay.querySelector('#app-dialog-submit').addEventListener('click', submit);
  overlay.addEventListener('click', e => { if (e.target === overlay) closeAppDialog(); });
  overlay.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeAppDialog();
  });
  if (input) {
    input.addEventListener('keydown', e => {
      if (e.key === 'Enter') submit();
    });
    input.focus();
    input.select();
  } else {
    overlay.querySelector('#app-dialog-submit').focus();
  }
}

function openChatDialog(opts) {
  openAppDialog(opts);
}

function appPrompt({title, description, placeholder = '', submitText = 'Save', initialValue = ''}) {
  return new Promise(resolve => {
    openAppDialog({
      title, description, placeholder, submitText, initialValue,
      onSubmit: async (value, err) => {
        if (!value) {
          err.textContent = 'Please enter a value.';
          err.style.display = 'block';
          return;
        }
        closeAppDialog();
        resolve(value);
      }
    });
    const cancel = document.getElementById('app-dialog-cancel');
    const overlay = document.getElementById('app-modal-backdrop');
    const finishCancel = () => resolve(null);
    cancel.addEventListener('click', finishCancel, { once: true });
    overlay.addEventListener('click', e => { if (e.target === overlay) finishCancel(); });
    overlay.addEventListener('keydown', e => { if (e.key === 'Escape') finishCancel(); });
  });
}

function appConfirm({title, description, submitText = 'Confirm', danger = false}) {
  return new Promise(resolve => {
    openAppDialog({
      title, description, submitText, danger, showInput: false,
      onSubmit: async () => {
        closeAppDialog();
        resolve(true);
      }
    });
    const cancel = document.getElementById('app-dialog-cancel');
    const overlay = document.getElementById('app-modal-backdrop');
    const finishCancel = () => resolve(false);
    cancel.addEventListener('click', finishCancel, { once: true });
    overlay.addEventListener('click', e => { if (e.target === overlay) finishCancel(); });
    overlay.addEventListener('keydown', e => { if (e.key === 'Escape') finishCancel(); });
  });
}

function openDirectMessageDialog() {
  openChatDialog({
    title: 'Start direct message',
    description: 'Enter a PennCloud username to open a private conversation.',
    placeholder: 'e.g. demo_bob',
    submitText: 'Start chat',
    onSubmit: async (value, err) => {
      const peer = value.replace(/@penncloud$/i, '');
      if (!peer) {
        err.textContent = 'Please enter a username.';
        err.style.display = 'block';
        return;
      }
      if (peer === currentUser) {
        err.textContent = 'Choose another user.';
        err.style.display = 'block';
        return;
      }
      closeChatDialog();
      navigate('chat', { dm: peer });
    }
  });
}

function openChatRoomDialog() {
  openChatDialog({
    title: 'Create chat room',
    description: 'Room names can use letters, numbers, dashes, and underscores.',
    placeholder: 'e.g. demo-room',
    submitText: 'Create room',
    onSubmit: async (value, err) => {
      const room = value.replace(/^#/, '').trim();
      if (!room) {
        err.textContent = 'Please enter a room name.';
        err.style.display = 'block';
        return;
      }
      const r = await fetch('/api/chat/rooms', {
        method: 'POST',
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
        body: 'room=' + encodeURIComponent(room)
      });
      const data = await r.json().catch(() => ({ok:false, error:'bad response'}));
      if (!data.ok) {
        err.textContent = data.error || 'Could not create room.';
        err.style.display = 'block';
        return;
      }
      closeChatDialog();
      showToast('Room #' + data.room + ' ready.');
      navigate('chat', { room: data.room });
    }
  });
}

function startDirectMessage() {
  openDirectMessageDialog();
}

async function sendChatMessage() {
  if (chatSending) return;
  const input = document.getElementById('chat-input');
  const btn = document.getElementById('chat-send-btn');
  if (!input) return;

  const text = input.value.trim();
  if (!text) return;

  chatSending = true;
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Sending...';
    btn.style.opacity = '0.72';
    btn.style.cursor = 'not-allowed';
  }

  try {
    const body = currentChatMode === 'dm'
      ? `peer=${encodeURIComponent(currentChatPeer)}&text=${encodeURIComponent(text)}`
      : `room=${encodeURIComponent(currentChatRoom)}&text=${encodeURIComponent(text)}`;
    const endpoint = currentChatMode === 'dm' ? '/api/chat/dm/send' : '/api/chat/send';
    const r = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body
    });

    const data = await r.json().catch(() => ({ ok:false, error:'bad response' }));
    if (!data.ok || !data.message) {
      showToast(data.error || 'Could not send message');
      return;
    }

    input.value = '';
    chatMessagesCache = [...chatMessagesCache, data.message];
    renderChatMessages(chatMessagesCache);
    if (currentChatMode === 'dm' && currentChatPeer) {
      dmUnreadCounts[currentChatPeer] = 0;
      if (!chatDmPeers.includes(currentChatPeer)) {
        chatDmPeers = normalizeDmPeers([currentChatPeer, ...chatDmPeers], currentChatPeer);
      }
      const last = data.message && data.message.id ? data.message.id : '';
      if (last) dmLastSeenIds[currentChatPeer] = last;
      refreshDmPeersUi();
    }
    refreshChatMessages();
  } catch (err) {
    showToast('Could not send message');
  } finally {
    chatSending = false;
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Send';
      btn.style.opacity = '1';
      btn.style.cursor = 'pointer';
    }
  }
}

window.switchChatRoom = switchChatRoom;
window.switchDirectMessage = switchDirectMessage;
window.startDirectMessage = startDirectMessage;
window.sendEmail = sendEmail;
window.sendChatMessage = sendChatMessage;
window.switchChatRoom = switchChatRoom;

function mailFolderTabs(activeFolder, counts = {}) {
  const folders = [
    {key: 'inbox', label: 'Inbox'},
    {key: 'sent', label: 'Sent'},
    {key: 'trash', label: 'Trash'}
  ];
  return `<div style="display:flex;gap:8px;margin:14px 0 18px 0">${folders.map(f => `
    <button class="action-btn ${f.key === activeFolder ? 'active-folder-tab' : ''}"
            style="${f.key === activeFolder ? 'background:var(--penn-blue);color:white;border-color:var(--penn-blue);' : ''}"
            onclick="navigate('inbox',{folder:'${f.key}'})">
      ${f.label} <span style="opacity:.7">${counts[f.key] || ''}</span>
    </button>`).join('')}</div>`;
}

async function renderInbox(folder = 'inbox', _retry = 0) {
  currentMailFolder = folder;
  const mySeq = ++inboxRenderSeq;
  const folders = ['inbox', 'sent', 'trash'];
  const results = await Promise.all(
    folders.map(async f => {
      try {
        const r = await fetch('/api/inbox?folder=' + encodeURIComponent(f));
        const data = await r.json();
        return data;
      } catch (_) {
        return { ok: false, emails: [] };
      }
    })
  );

  if (mySeq !== inboxRenderSeq) return;

  const counts = {};
  results.forEach((res, idx) => {
    counts[folders[idx]] = (res.emails || []).length;
  });

  const active = results[folders.indexOf(folder)] || results[0];
  const content = document.getElementById('content');

  if (!active.ok) {
    if (_retry < 2) {
      content.innerHTML = '<div class="spinner">Reconnecting to storage...</div>';
      await new Promise(r => setTimeout(r, 1500));
      if (mySeq !== inboxRenderSeq) return;
      return renderInbox(folder, _retry + 1);
    }
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<p>Error loading mail.</p>';
    return;
  }

  const emails = active.emails || [];
  if (folder === 'inbox') {
    knownInboxUids = new Set(emails.map(e => e.uid).filter(Boolean));
  }

  const title = folder.charAt(0).toUpperCase() + folder.slice(1);
  content.innerHTML = `
    <div class="page-title">
      ${title} <span style="font-size:14px;color:var(--muted);font-weight:400">${emails.length} messages</span>
      <button class="compose-btn" onclick="navigate('compose')">+ Compose</button>
    </div>
    ${mailFolderTabs(folder, counts)}
    <div class="email-list" id="email-list">
      ${emails.length === 0
        ? `<div style="padding:40px;text-align:center;color:var(--muted)">No messages in ${escHtml(title.toLowerCase())}</div>`
        : emails.map(e => `
          <div class="email-row" data-uid="${e.uid}" onclick="navigate('email', {uid:'${e.uid}', folder:'${folder}'})">
            <div class="email-from">${escHtml(folder === 'sent' ? emailLabel(e,'to') : emailLabel(e,'from'))}</div>
            <div class="email-subj">${escHtml(e.subject)}</div>
            <div class="email-time">${escHtml(e.time)}</div>
          </div>`).join('')}
    </div>`;
}

function prependEmailRow(email) {
  // SSE should act only as a refresh hint. The inbox response is the source of truth.
}


async function renderEmail(uid, folder = 'inbox') {
  currentMailFolder = folder;
  const r = await fetch(`/api/email/${encodeURIComponent(uid)}?folder=${encodeURIComponent(folder)}`);
  const data = await r.json();
  const content = document.getElementById('content');
  if (!data.ok) { content.innerHTML = '<p>Email not found.</p>'; return; }
  const e = data.email;
  const attachments = Array.isArray(e.attachments) ? e.attachments : [];
  const deleteLabel = folder === 'trash' ? 'Delete permanently' : 'Move to Trash';
  content.innerHTML = `
    <button id="mail-back-btn" style="margin-bottom:16px;background:none;border:none;cursor:pointer;color:var(--accent);font-size:13px;">
      &larr; Back to ${escHtml(folder)}
    </button>
    <div class="email-view">
      <h2>${escHtml(e.subject)}</h2>
      <div class="email-meta">
        <span><b>From:</b> ${escHtml(e.from_display || e.from)}</span>
        <span><b>To:</b> ${escHtml(e.to_display || e.to || currentUser)}</span>
        <span><b>Date:</b> ${escHtml(e.time)}</span>
        <span><b>Folder:</b> ${escHtml(folder)}</span>
      </div>
      ${attachments.length ? `<div style="margin:14px 0 10px"><b>Attachments:</b><div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">${attachments.map(a => `<a class="action-btn" style="text-decoration:none" href="/api/mail/attachment?uid=${encodeURIComponent(uid)}&att=${encodeURIComponent(a.id)}&folder=${encodeURIComponent(folder)}">📎 ${escHtml(a.name)} (${formatBytes(a.size || 0)})</a>`).join('')}</div></div>` : ''}
      <div class="email-actions">
        ${folder !== 'trash' ? `<button class="action-btn" id="reply-btn">Reply</button>
        <button class="action-btn" id="forward-btn">Forward</button>` : ''}
        ${folder === 'trash' ? `<button class="action-btn" id="restore-btn">Restore</button>` : ''}
        <button class="action-btn danger" id="delete-btn">${deleteLabel}</button>
      </div>
      <div class="email-body">${escHtml(e.body)}</div>
    </div>`;

  const backBtn = document.getElementById('mail-back-btn');
  if (backBtn) backBtn.onclick = () => navigate('inbox', {folder});
  const replyBtn = document.getElementById('reply-btn');
  if (replyBtn) replyBtn.onclick = () => openReply(uid, folder);
  const forwardBtn = document.getElementById('forward-btn');
  if (forwardBtn) forwardBtn.onclick = () => openForward(uid, folder);
  const restoreBtn = document.getElementById('restore-btn');
  if (restoreBtn) restoreBtn.onclick = () => restoreEmail(uid);
  const deleteBtn = document.getElementById('delete-btn');
  if (deleteBtn) deleteBtn.onclick = () => deleteEmail(uid, folder);
}

async function openReply(uid, folder = currentMailFolder || 'inbox') {
  try {
    const r = await fetch(`/api/email/${encodeURIComponent(uid)}?folder=${encodeURIComponent(folder)}`);
    const data = await r.json();
    if (!data.ok || !data.email) return showToast('Could not load email');
    const e = data.email;
    const replyBody = `

--- Original Message ---
From: ${e.from_display || e.from || ''}
Date: ${e.time || ''}
Subject: ${e.subject || ''}

${e.body || ''}`;
    navigate('compose', {
      reply_to: e.from || '',
      subject: (e.subject || '').startsWith('Re:') ? (e.subject || '') : 'Re: ' + (e.subject || ''),
      body: replyBody
    });
  } catch (err) {
    console.error('openReply failed', err);
    showToast('Could not open reply');
  }
}

async function openForward(uid, folder = currentMailFolder || 'inbox') {
  try {
    const r = await fetch(`/api/email/${encodeURIComponent(uid)}?folder=${encodeURIComponent(folder)}`);
    const data = await r.json();
    if (!data.ok || !data.email) return showToast('Could not load email');
    const e = data.email;
    const forwarded = `

--- Forwarded ---
From: ${e.from_display || e.from || ''}
To: ${e.to_display || e.to || currentUser || ''}
Date: ${e.time || ''}
Subject: ${e.subject || ''}

${e.body || ''}`;
    navigate('compose', {
      subject: 'Fwd: ' + (e.subject || ''),
      body: forwarded,
      attachments: Array.isArray(e.attachments) ? e.attachments : []
    });
  } catch (err) {
    showToast('Could not open forward');
  }
}

async function deleteEmail(uid, folder = currentMailFolder || 'inbox') {
  try {
    const r = await fetch('/api/delete-email', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: `uid=${encodeURIComponent(uid)}&folder=${encodeURIComponent(folder)}`
    });
    const data = await r.json();
    if (!data.ok) {
      showToast(data.error || 'Delete failed');
      return;
    }
    showToast(data.deleted ? 'Email permanently deleted' : 'Email moved to Trash');
    navigate('inbox', {folder: data.deleted ? 'trash' : 'trash'});
  } catch (err) {
    console.error('deleteEmail failed', err);
    showToast('Delete failed');
  }
}

async function restoreEmail(uid) {
  try {
    const r = await fetch('/api/restore-email', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: `uid=${encodeURIComponent(uid)}`
    });
    const data = await r.json();
    showToast(data.ok ? 'Email restored to Inbox' : (data.error || 'Restore failed'));
    if (data.ok) navigate('inbox', {folder: 'inbox'});
  } catch (err) {
    console.error('restoreEmail failed', err);
    showToast('Restore failed');
  }
}

function renderComposeAttachmentList() {
  return (composeAttachments || []).map(a => `
    <div style="display:flex;align-items:center;gap:8px;padding:6px 10px;border:1px solid var(--border);border-radius:8px;background:#fff">
      <span>📎 ${escHtml(a.name)} <span style="color:var(--muted)">(${formatBytes(a.size || 0)})</span></span>
      <button type="button" class="action-btn" onclick="removeComposeAttachment('${escHtml(a.id)}')">Remove</button>
    </div>`).join('');
}

function rememberComposeDraft() {
  const toEl = document.getElementById('to-in');
  const subjectEl = document.getElementById('subject-in');
  const bodyEl = document.getElementById('body-in');
  composeDraft = {
    reply_to: toEl ? toEl.value : (composeDraft.reply_to || ''),
    subject: subjectEl ? subjectEl.value : (composeDraft.subject || ''),
    body: bodyEl ? bodyEl.value : (composeDraft.body || '')
  };
}

async function renderCompose(params = {}) {
  await loadContacts();
  composeDraft = {
    reply_to: params.reply_to !== undefined ? params.reply_to : (composeDraft.reply_to || ''),
    subject: params.subject !== undefined ? params.subject : (composeDraft.subject || ''),
    body: params.body !== undefined ? params.body : (composeDraft.body || '')
  };
  if (Array.isArray(params.attachments)) composeAttachments = params.attachments.slice();
  document.getElementById('content').innerHTML = `
    <div class="page-title">Compose</div>
    <div class="compose-form">
      <input id="to-in" list="contacts-list" type="text" placeholder="To (username or user@example.com)" value="${escHtml(composeDraft.reply_to || '')}">
      <datalist id="contacts-list">${buildContactOptions()}</datalist>
      <div style="font-size:12px;color:var(--muted);margin-top:-6px;margin-bottom:10px">Address book suggestions are available as you type.</div>
      <input id="subject-in" type="text" placeholder="Subject" value="${escHtml(composeDraft.subject || '')}">
      <textarea id="body-in" placeholder="Write your message...">${escHtml(composeDraft.body || '')}</textarea>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
        <button class="action-btn" onclick="document.getElementById('mail-attachment-input').click()">Attach file</button>
        <input id="mail-attachment-input" type="file" style="display:none">
        <span style="font-size:12px;color:var(--muted)">Local mail attachments</span>
      </div>
      <div id="compose-attachments" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">${renderComposeAttachmentList()}</div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="sendEmail()">Send</button>
        <button class="action-btn" onclick="addCurrentRecipientToContacts()">Add recipient to contacts</button>
        <button class="action-btn" onclick="navigate('inbox',{folder:'inbox'})">Cancel</button>
      </div>
      <div id="send-error" class="error-msg"></div>
    </div>`;

  const fileInput = document.getElementById('mail-attachment-input');
  if (fileInput) {
    fileInput.addEventListener('change', async (ev) => {
      const file = ev.target.files && ev.target.files[0];
      if (!file) return;
      await uploadMailAttachment(file);
      ev.target.value = '';
    });
  }
}

async function uploadMailAttachment(file) {
  rememberComposeDraft();
  const errBox = document.getElementById('send-error');
  if (file.size > 10 * 1024 * 1024) {
    errBox.textContent = 'Attachment must be 10 MB or smaller.';
    errBox.style.display = 'block';
    return;
  }
  const fd = new FormData();
  fd.append('attachment', file);
  try {
    const r = await fetch('/api/mail/upload-attachment', { method: 'POST', body: fd });
    const data = await r.json();
    if (!data.ok) {
      errBox.textContent = data.error || 'Attachment upload failed.';
      errBox.style.display = 'block';
      return;
    }
    composeAttachments.push(data.attachment);
    renderCompose(composeDraft);
    showToast('Attachment uploaded');
  } catch (e) {
    errBox.textContent = 'Attachment upload failed.';
    errBox.style.display = 'block';
  }
}

function removeComposeAttachment(id) {
  rememberComposeDraft();
  composeAttachments = (composeAttachments || []).filter(a => a.id !== id);
  renderCompose(composeDraft);
}

async function sendEmail() {
  if (sendInFlight) return;

  const to      = document.getElementById('to-in').value.trim();
  const subject = document.getElementById('subject-in').value.trim();
  const body    = document.getElementById('body-in').value;
  const errBox  = document.getElementById('send-error');
  const sendBtn = document.querySelector('.compose-form .btn.btn-primary');

  composeDraft = { reply_to: to, subject, body };

  if (!to || !subject) {
    errBox.textContent = 'To and Subject are required.';
    errBox.style.display = 'block';
    return;
  }

  sendInFlight = true;
  if (sendBtn) {
    sendBtn.disabled = true;
    sendBtn.textContent = 'Sending...';
    sendBtn.style.opacity = '0.7';
    sendBtn.style.cursor = 'not-allowed';
  }

  try {
    const attachmentIds = (composeAttachments || []).map(a => a.id).join(',');
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const r = await fetch('/api/send', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      signal: controller.signal,
      body: `to=${encodeURIComponent(to)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}&attachment_ids=${encodeURIComponent(attachmentIds)}`
    });
    clearTimeout(timeout);
    const data = await r.json();
    if (data.ok) {
      composeAttachments = [];
      composeDraft = { reply_to: '', subject: '', body: '' };
      showToast(data.external ? 'External email accepted by SMTP. Saved in Sent.' : 'Email sent! Saved in Sent as well.');
      navigate('inbox', {folder: 'sent'});
    } else {
      errBox.textContent = data.error || 'Send failed.';
      errBox.style.display = 'block';
    }
  } catch (e) {
    errBox.textContent = e && e.name === 'AbortError' ? 'Send timed out. External SMTP may be blocked.' : 'Send failed.';
    errBox.style.display = 'block';
  } finally {
    sendInFlight = false;
    if (sendBtn) {
      sendBtn.disabled = false;
      sendBtn.textContent = 'Send';
      sendBtn.style.opacity = '1';
      sendBtn.style.cursor = 'pointer';
    }
  }
}

async function addCurrentRecipientToContacts() {
  rememberComposeDraft();
  const email = document.getElementById('to-in').value.trim();
  if (!email) return showToast('Enter a recipient first.');
  const name = await appPrompt({
    title: 'Save contact',
    description: `Add ${email} to your PennCloud contacts.`,
    placeholder: 'Display name',
    submitText: 'Save contact'
  });
  if (!name) return;
  const r = await fetch('/api/contacts/add', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `email=${encodeURIComponent(email)}&name=${encodeURIComponent(name)}`
  });
  const data = await r.json();
  if (data.ok) {
    await loadContacts(true);
    showToast('Contact saved.');
    renderCompose(composeDraft);
  } else {
    showToast(data.error || 'Could not save contact');
  }
}


async function renderDrive(folderPath = '/', _retry = 0) {
  currentPath = folderPath;
  const content = document.getElementById('content');
  let r;
  try {
    r = await fetch('/api/drive?path=' + encodeURIComponent(folderPath)).then(x => x.json());
  } catch (_) {
    if (_retry < 2) {
      content.innerHTML = '<div class="spinner">Reconnecting to storage...</div>';
      await new Promise(res => setTimeout(res, 1500));
      return renderDrive(folderPath, _retry + 1);
    }
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<p>Error loading drive.</p>';
    return;
  }
  if (!r.ok) {
    if (_retry < 2) {
      content.innerHTML = '<div class="spinner">Reconnecting to storage...</div>';
      await new Promise(res => setTimeout(res, 1500));
      return renderDrive(folderPath, _retry + 1);
    }
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<p>Error loading drive.</p>';
    return;
  }

  const items = r.items || [];
  const folders = items.filter(it => it.type === 'folder').length;
  const files = items.length - folders;
  const pathParts = folderPath === '/' ? [] : folderPath.split('/').filter(Boolean);
  const folderLabel = pathParts.length ? pathParts[pathParts.length - 1] : 'Root';
  const crumbs = [{ label: 'Root', path: '/' }];
  pathParts.forEach((part, idx) => {
    crumbs.push({ label: part, path: '/' + pathParts.slice(0, idx + 1).join('/') });
  });
  const breadcrumbHtml = crumbs.map((c, i) => {
    const sep = i === 0 ? '' : '<span class="crumb-sep">/</span>';
    return `${sep}<button class="crumb" type="button" onclick="renderDrive(decodeURIComponent('${driveInlineArg(c.path)}'))">${escHtml(c.label)}</button>`;
  }).join('');

  const emptyState = `
    <div class="drive-empty">
      <div class="drive-empty-icon">${driveIconSvg('folder')}</div>
      <div>
        <div style="font-weight:800;color:var(--penn-blue);margin-bottom:4px">This folder is empty</div>
        <div style="font-size:13px">Upload a file or create a folder to start organizing.</div>
      </div>
      <div class="drive-toolbar">
        <button class="primary-action" onclick="showUpload()"><span class="btn-symbol">&uarr;</span> Upload file</button>
        <button onclick="makeFolder()"><span class="btn-symbol">+</span> New folder</button>
      </div>
    </div>`;

  content.innerHTML = `
    <div class="drive-shell">
      <div class="drive-top">
        <div>
          <div class="drive-eyebrow">Penn Drive</div>
          <div class="drive-title">${escHtml(folderLabel)}</div>
          <div class="drive-subtitle">${plural(folders, 'folder')} and ${plural(files, 'file')} in ${escHtml(folderPath)}</div>
        </div>
        <div class="drive-quota">
          <div class="drive-quota-row">
            <strong>Storage quota</strong>
            <span id="drive-quota-free" class="drive-quota-meta">Loading quota...</span>
          </div>
          <div class="drive-quota-row" style="margin-bottom:8px">
            <span id="drive-quota-label">Calculating storage usage...</span>
            <span id="drive-quota-pct" class="drive-quota-meta">--</span>
          </div>
          <div class="quota-track"><div id="drive-quota-fill" class="quota-fill" style="width:0%"></div></div>
        </div>
      </div>
      <div class="drive-command-row">
        <div class="breadcrumb">${breadcrumbHtml}</div>
        <div class="drive-toolbar">
          <button class="primary-action" onclick="showUpload()"><span class="btn-symbol">&uarr;</span> Upload file</button>
          <button onclick="makeFolder()"><span class="btn-symbol">+</span> New folder</button>
        </div>
      </div>
      <div id="upload-progress" class="upload-progress-card"><div id="upload-progress-list" class="upload-progress-list"></div></div>
      <input type="file" id="file-input" style="display:none" multiple onchange="uploadFile(this)">
      <div class="drive-table">
      ${items.length === 0
        ? emptyState
        : `<div class="drive-table-header">
             <div>Name</div><div>Type</div><div>Size</div><div>Last changed</div><div style="text-align:right">Actions</div>
           </div>` + items.map(it => {
             const pathArg = driveInlineArg(it.path);
             const uidArg = driveInlineArg(it.uid);
             const nameArg = driveInlineArg(it.name);
             const changedAt = it.updated_at || it.created_at || '—';
             const openAction = it.type === 'folder'
               ? `renderDrive(decodeURIComponent('${pathArg}'))`
               : `downloadFile(decodeURIComponent('${uidArg}'), decodeURIComponent('${nameArg}'))`;
             return `
          <div class="drive-row">
            <button class="drive-name-btn" type="button" onclick="${openAction}">
              <span class="drive-icon ${it.type === 'folder' ? 'folder' : 'file'}">${driveIconSvg(it.type)}</span>
              <span class="drive-name-text">
                <span class="drive-item-name">${escHtml(it.name)}</span>
                <span class="drive-item-path">${escHtml(it.path || folderPath)}</span>
              </span>
            </button>
            <div class="drive-kind">${escHtml(driveItemKind(it))}</div>
            <div class="drive-size">${escHtml(driveDisplaySize(it))}</div>
            <div class="drive-changed">${escHtml(changedAt)}</div>
            <div class="drive-row-actions">
              <button onclick="renameItem(decodeURIComponent('${pathArg}'))">Rename</button>
              <button onclick="moveItem(decodeURIComponent('${pathArg}'))">Move to</button>
              <button class="danger" onclick="deleteItem(decodeURIComponent('${pathArg}'))">Delete</button>
            </div>
          </div>`;
           }).join('')}
      </div>
    </div>`;
  loadQuota(true)
    .then(q => {
      if (currentView === 'drive' && currentPath === folderPath) updateDriveQuotaCard(q);
    })
    .catch(() => {
      if (currentView === 'drive' && currentPath === folderPath) updateDriveQuotaCard(null);
    });
  hydrateSavedUploadSessions(folderPath);
}

function updateDriveQuotaCard(quota) {
  const freeEl = document.getElementById('drive-quota-free');
  const labelEl = document.getElementById('drive-quota-label');
  const pctEl = document.getElementById('drive-quota-pct');
  const fillEl = document.getElementById('drive-quota-fill');
  if (!freeEl || !labelEl || !pctEl || !fillEl) return;
  if (!quota) {
    freeEl.textContent = 'Unavailable';
    labelEl.textContent = 'Storage unavailable';
    pctEl.textContent = '--';
    fillEl.style.width = '0%';
    fillEl.classList.remove('warn');
    return;
  }
  const usedPct = Math.min(100, Math.round((quota.used_bytes / Math.max(1, quota.limit_bytes)) * 100));
  freeEl.textContent = `${formatBytes(quota.remaining_bytes)} free`;
  labelEl.textContent = `${formatBytes(quota.used_bytes)} of ${formatBytes(quota.limit_bytes)} used`;
  pctEl.textContent = `${usedPct}%`;
  fillEl.style.width = `${usedPct}%`;
  fillEl.classList.toggle('warn', usedPct > 85);
}

function showUpload() { document.getElementById('file-input').click(); }

function renderUploadProgressList() {
  const box = document.getElementById('upload-progress');
  const list = document.getElementById('upload-progress-list');
  if (!box || !list) return;
  const entries = Object.values(uploadProgresses);
  box.classList.toggle('active', entries.length > 0);
  list.innerHTML = '';
  entries.forEach(item => {
    const pct = Math.max(0, Math.min(100, Math.round(item.percent || 0)));
    const row = document.createElement('div');
    row.className = 'upload-progress-item';
    row.innerHTML = `
      <div class="upload-progress-head">
        <span class="upload-progress-name"></span>
        <span class="upload-progress-pct">${pct}%</span>
      </div>
      <div class="upload-progress-bar"><div class="upload-progress-fill" style="width:${pct}%"></div></div>
      <div class="upload-progress-detail"></div>
      <div class="upload-progress-actions"></div>`;
    row.querySelector('.upload-progress-name').textContent = item.name || 'Uploading file';
    row.querySelector('.upload-progress-detail').textContent = item.detail || 'Preparing upload...';
    const actions = row.querySelector('.upload-progress-actions');
    if (item.resumeKey) {
      const btn = document.createElement('button');
      btn.className = 'action-btn';
      btn.type = 'button';
      btn.textContent = 'Resume';
      btn.onclick = () => resumeSavedUpload(item.resumeKey);
      actions.appendChild(btn);
    }
    list.appendChild(row);
  });
}

function setUploadProgress(id, {name = '', percent = 0, detail = '', resumeKey = undefined} = {}) {
  if (!id) return;
  uploadProgresses[id] = {
    ...(uploadProgresses[id] || {}),
    id,
    name: name || (uploadProgresses[id] && uploadProgresses[id].name) || 'Uploading file',
    percent,
    detail,
    resumeKey: resumeKey === undefined ? (uploadProgresses[id] && uploadProgresses[id].resumeKey) : resumeKey
  };
  renderUploadProgressList();
}

function clearUploadProgressSoon(id) {
  setTimeout(() => {
    delete uploadProgresses[id];
    renderUploadProgressList();
  }, 1800);
}

function savedUploadEntries() {
  const prefix = 'penncloud:upload:';
  const entries = [];
  for (let i = 0; i < localStorage.length; ++i) {
    const key = localStorage.key(i);
    if (!key || !key.startsWith(prefix)) continue;
    try {
      const value = JSON.parse(localStorage.getItem(key) || '{}');
      entries.push({key, value});
    } catch (_) {}
  }
  return entries;
}

async function hydrateSavedUploadSessions(folderPath) {
  const entries = savedUploadEntries().filter(({value}) =>
    value && value.user === currentUser && value.path === folderPath && value.upload_id);
  if (entries.length === 0) {
    renderUploadProgressList();
    return;
  }
  for (const {key, value} of entries) {
    try {
      const r = await fetch('/api/upload/status?id=' + encodeURIComponent(value.upload_id), {cache: 'no-store'});
      const data = await r.json();
      if (!data.ok || data.status === 'completed' || data.status === 'cancelled') {
        localStorage.removeItem(key);
        delete uploadProgresses[value.upload_id];
        continue;
      }
      const totalSize = Number(data.total_size || value.size || 0);
      const chunkSize = Number(data.chunk_size || value.chunk_size || (10 * 1024 * 1024));
      const received = Array.isArray(data.received) ? data.received.map(Number).filter(Number.isFinite) : [];
      const completedBytes = received.reduce((sum, idx) => {
        if (idx < 0) return sum;
        return sum + Math.max(0, Math.min(chunkSize, totalSize - idx * chunkSize));
      }, 0);
      const percent = totalSize ? (completedBytes / totalSize) * 100 : 0;
      setUploadProgress(value.upload_id, {
        name: value.filename || 'Paused upload',
        percent,
        detail: `Paused after refresh: ${formatBytes(completedBytes)} of ${formatBytes(totalSize)} stored.`,
        resumeKey: key
      });
    } catch (_) {
      setUploadProgress(value.upload_id, {
        name: value.filename || 'Paused upload',
        percent: 0,
        detail: 'Paused upload saved locally.',
        resumeKey: key
      });
    }
  }
  renderUploadProgressList();
}

function resumeSavedUpload(storageKey) {
  let saved = null;
  try { saved = JSON.parse(localStorage.getItem(storageKey) || '{}'); } catch (_) {}
  if (!saved || !saved.upload_id) {
    showToast('Could not find saved upload session.');
    return;
  }
  const input = document.createElement('input');
  input.type = 'file';
  input.style.display = 'none';
  input.onchange = () => {
    const file = input.files && input.files[0];
    input.remove();
    if (!file) return;
    const sameFile = file.name === saved.filename &&
      file.size === Number(saved.size || 0) &&
      Number(file.lastModified || 0) === Number(saved.lastModified || 0);
    if (!sameFile) {
      showToast('Please choose the exact same file to resume this upload.');
      return;
    }
    uploadSingleFile(file, saved.path || currentPath, {progressId: saved.upload_id});
  };
  document.body.appendChild(input);
  input.click();
}

async function uploadFile(input) {
  const files = Array.from(input.files || []);
  if (files.length === 0) return;
  const uploadPath = currentPath;
  input.value = '';
  files.forEach(file => uploadSingleFile(file, uploadPath));
}

function uploadFingerprint(file, uploadPath) {
  return `${currentUser}|${uploadPath}|${file.name}|${file.size}|${file.lastModified || 0}`;
}

function uploadStorageKey(fingerprint) {
  return 'penncloud:upload:' + encodeURIComponent(fingerprint);
}

async function postUrlEncoded(url, params) {
  const r = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: new URLSearchParams(params).toString()
  });
  return r.json();
}

function uploadChunkRequest(uploadId, index, blob, fileName, onProgress) {
  return new Promise((resolve, reject) => {
    const fd = new FormData();
    fd.append('upload_id', uploadId);
    fd.append('index', String(index));
    fd.append('chunk', blob, `${fileName}.part${index}`);
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/upload/chunk');
    xhr.timeout = 0;
    xhr.upload.onprogress = onProgress;
    xhr.onload = () => {
      let data = {};
      try { data = JSON.parse(xhr.responseText || '{}'); } catch (_) {}
      if (xhr.status >= 200 && xhr.status < 300 && data.ok) resolve(data);
      else reject(new Error(data.error || xhr.statusText || `HTTP ${xhr.status}`));
    };
    xhr.onerror = () => reject(new Error('network error'));
    xhr.onabort = () => reject(new Error('upload cancelled'));
    xhr.send(fd);
  });
}

async function uploadSingleFile(file, uploadPath, options = {}) {
  const MAX_FILE_BYTES = 1024 * 1024 * 1024;
  let uploadId = options.progressId || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  if (file.size > MAX_FILE_BYTES) {
    showToast(`Upload blocked: file exceeds 1 GB server limit (${formatBytes(file.size)})`);
    return;
  }
  const quota = await loadQuota(true);
  if (quota && quota.used_bytes + file.size > quota.limit_bytes) {
    showToast(`Upload blocked: quota exceeded by ${formatBytes(quota.used_bytes + file.size - quota.limit_bytes)}`);
    return;
  }
  showToast('Uploading...');
  const uploadStartedAt = Date.now();
  let lastUploadRate = 0;
  const fingerprint = uploadFingerprint(file, uploadPath);
  const storageKey = uploadStorageKey(fingerprint);
  setUploadProgress(uploadId, {
    name: file.name,
    percent: 0,
    detail: `Starting upload session for ${formatBytes(file.size)}...`
  });
  try {
    const start = await postUrlEncoded('/api/upload/start', {
      filename: file.name,
      path: uploadPath,
      total_size: String(file.size),
      fingerprint
    });
    if (!start.ok) throw new Error(start.error || 'failed to start upload');
    const serverUploadId = start.upload_id;
    if (serverUploadId && serverUploadId !== uploadId) {
      uploadProgresses[serverUploadId] = {
        ...(uploadProgresses[uploadId] || {}),
        id: serverUploadId,
        resumeKey: undefined
      };
      delete uploadProgresses[uploadId];
      uploadId = serverUploadId;
      renderUploadProgressList();
    }
    localStorage.setItem(storageKey, JSON.stringify({
      upload_id: serverUploadId,
      user: currentUser,
      path: uploadPath,
      filename: file.name,
      size: file.size,
      lastModified: file.lastModified || 0,
      fingerprint,
      chunk_size: start.chunk_size,
      total_chunks: start.total_chunks
    }));

    const chunkSize = Number(start.chunk_size || (10 * 1024 * 1024));
    const totalChunks = Number(start.total_chunks || Math.ceil(file.size / chunkSize));
    const received = new Set((start.received || []).map(x => Number(x)).filter(x => Number.isFinite(x)));
    let completedBytes = 0;
    const chunkBytes = (idx) => Math.min(chunkSize, file.size - idx * chunkSize);
    for (const idx of received) {
      if (idx >= 0 && idx < totalChunks) completedBytes += chunkBytes(idx);
    }
    const resumedBytes = completedBytes;
    const inFlightLoaded = {};
    const updateProgress = (phase) => {
      const inFlightBytes = Object.values(inFlightLoaded).reduce((a, b) => a + Number(b || 0), 0);
      const sent = Math.min(file.size, completedBytes + inFlightBytes);
      const elapsedSeconds = Math.max(0.25, (Date.now() - uploadStartedAt) / 1000);
      const uploadedThisSession = Math.max(0, completedBytes - resumedBytes) + inFlightBytes;
      lastUploadRate = uploadedThisSession / elapsedSeconds;
      const pct = file.size ? (sent / file.size) * 100 : 100;
      const resumedLabel = resumedBytes > 0 ? ` (${formatBytes(resumedBytes)} resumed)` : '';
      const rateLabel = uploadedThisSession > 0 ? formatRate(lastUploadRate) : 'calculating speed...';
      setUploadProgress(uploadId, {
        name: file.name,
        percent: pct,
        detail: `${phase}: ${formatBytes(sent)} of ${formatBytes(file.size)}${resumedLabel} · ${rateLabel}`,
        resumeKey: ''
      });
    };
    updateProgress(received.size ? 'Resuming upload' : 'Uploading chunks');

    let nextIndex = 0;
    const concurrency = 1;
    await new Promise((resolve, reject) => {
      let active = 0;
      let failed = false;
      const launch = () => {
        if (failed) return;
        while (active < concurrency) {
          while (nextIndex < totalChunks && received.has(nextIndex)) nextIndex++;
          if (nextIndex >= totalChunks) {
            if (active === 0) resolve();
            return;
          }
          const idx = nextIndex++;
          const startByte = idx * chunkSize;
          const blob = file.slice(startByte, startByte + chunkBytes(idx));
          active++;
          uploadChunkRequest(serverUploadId, idx, blob, file.name, (ev) => {
            if (ev.lengthComputable) {
              inFlightLoaded[idx] = ev.loaded;
              updateProgress('Uploading chunks');
            }
          }).then(() => {
            delete inFlightLoaded[idx];
            received.add(idx);
            completedBytes += chunkBytes(idx);
            updateProgress('Uploading chunks');
            active--;
            launch();
          }).catch(err => {
            failed = true;
            reject(err);
          });
        }
      };
      launch();
    });

    setUploadProgress(uploadId, {
      name: file.name,
      percent: 100,
      detail: `Upload sent at ${formatRate(lastUploadRate)}. Finalizing in replicated storage...`,
      resumeKey: ''
    });
    const done = await postUrlEncoded('/api/upload/finish', {upload_id: serverUploadId});
    if (!done.ok) throw new Error(done.error || 'failed to finish upload');
    localStorage.removeItem(storageKey);
    showToast('Uploaded!');
    setUploadProgress(uploadId, {
      name: file.name,
      percent: 100,
      detail: `Upload complete · average ${formatRate(lastUploadRate)}.`,
      resumeKey: ''
    });
    quotaCache = null;
    if (currentView === 'drive') renderDrive(currentPath);
    clearUploadProgressSoon(uploadId);
  } catch (err) {
    showToast('Upload failed: ' + (err && err.message ? err.message : err));
    const hasResumeSession = !!localStorage.getItem(storageKey);
    setUploadProgress(uploadId, {
      name: file.name,
      percent: uploadProgresses[uploadId] ? uploadProgresses[uploadId].percent : 0,
      detail: `Upload paused/failed: ${err && err.message ? err.message : err}.`,
      resumeKey: hasResumeSession ? storageKey : ''
    });
    if (!hasResumeSession) {
      // Permanent failure (no saved session to resume from) — auto-clear after
      // a brief moment so the failed-upload card doesn't linger forever.
      // Toast already displays the error message persistently for the user.
      clearUploadProgressSoon(uploadId);
    }
  }
}

async function downloadFile(uid, name) {
  const a = document.createElement('a');
  a.href = `/api/download/${uid}`;
  a.download = name;
  a.click();
}

async function renameItem(path) {
  const itemName = path.split('/').filter(Boolean).pop() || path;
  const newName = await appPrompt({
    title: 'Rename item',
    description: `Choose a new name for ${itemName}.`,
    placeholder: 'New name',
    submitText: 'Rename',
    initialValue: itemName
  });
  if (!newName) return;
  const r = await fetch('/api/rename', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `path=${encodeURIComponent(path)}&name=${encodeURIComponent(newName)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Renamed!' : 'Rename failed.');
  if (data.ok) renderDrive(currentPath);
}

async function moveItem(srcPath) {
  const existing = document.getElementById('move-modal-overlay');
  if (existing) existing.remove();

  const itemName = srcPath.split('/').filter(Boolean).pop() || srcPath;
  const pathParts = srcPath.split('/').filter(Boolean);
  const currentFolderPath = pathParts.length <= 1 ? '/' : '/' + pathParts.slice(0, -1).join('/');
  const currentFolderParts = currentFolderPath.split('/').filter(Boolean);
  const parentPath = currentFolderParts.length <= 1 ? '/' : '/' + currentFolderParts.slice(0, -1).join('/');

  const overlay = document.createElement('div');
  overlay.id = 'move-modal-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9999;display:flex;align-items:center;justify-content:center';

  const parentBtnHtml = (currentFolderPath !== '/')
    ? `<button data-dst="${escHtml(parentPath)}" class="move-quick-btn" style="text-align:left;padding:10px 14px;border:1px solid var(--border);border-radius:8px;background:var(--bg);cursor:pointer;width:100%;font-size:14px">
         ${driveInlineFolderIcon()}Parent folder&nbsp;<span style="font-size:12px;color:var(--muted)">${escHtml(parentPath)}</span>
       </button>`
    : '';

  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:12px;padding:28px;min-width:360px;max-width:480px;width:90%;box-shadow:0 20px 60px rgba(0,0,0,0.3)">
      <div style="font-size:16px;font-weight:600;margin-bottom:4px">Move &#8220;${escHtml(itemName)}&#8221;</div>
      <div style="font-size:13px;color:var(--muted);margin-bottom:18px">Choose a destination folder</div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:18px">
        <button data-dst="/" class="move-quick-btn" style="text-align:left;padding:10px 14px;border:1px solid var(--border);border-radius:8px;background:var(--bg);cursor:pointer;width:100%;font-size:14px">
          ${driveInlineFolderIcon()}Root folder&nbsp;<span style="font-size:12px;color:var(--muted)">/</span>
        </button>
        ${parentBtnHtml}
      </div>
      <div style="font-size:13px;font-weight:500;margin-bottom:8px;color:var(--muted)">Or enter a folder path:</div>
      <div style="display:flex;gap:8px">
        <input id="move-dst-input" type="text" placeholder="/folder/subfolder"
               style="flex:1;padding:10px 14px;border:1px solid var(--border);border-radius:8px;font-size:14px;outline:none">
        <button id="move-submit-btn" style="padding:10px 18px;background:var(--penn-blue);color:white;border:none;border-radius:8px;cursor:pointer;font-size:14px;white-space:nowrap">Move</button>
      </div>
      <div id="move-error" style="color:#C53030;font-size:13px;margin-top:8px;min-height:18px"></div>
      <div style="margin-top:16px;text-align:right">
        <button id="move-cancel-btn" style="padding:8px 20px;border:1px solid var(--border);border-radius:8px;background:none;cursor:pointer;font-size:14px">Cancel</button>
      </div>
    </div>`;

  document.body.appendChild(overlay);

  overlay.querySelector('#move-cancel-btn').addEventListener('click', closeMoveModal);
  overlay.addEventListener('click', e => { if (e.target === overlay) closeMoveModal(); });
  overlay.querySelectorAll('.move-quick-btn').forEach(btn => {
    btn.addEventListener('click', () => execMove(srcPath, btn.dataset.dst));
  });
  overlay.querySelector('#move-submit-btn').addEventListener('click', () => {
    execMove(srcPath, overlay.querySelector('#move-dst-input').value.trim() || '/');
  });
  overlay.querySelector('#move-dst-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') execMove(srcPath, overlay.querySelector('#move-dst-input').value.trim() || '/');
  });
  overlay.querySelector('#move-dst-input').focus();
}

function closeMoveModal() {
  const el = document.getElementById('move-modal-overlay');
  if (el) el.remove();
}

async function execMove(srcPath, dstPath) {
  const errEl = document.getElementById('move-error');
  if (errEl) errEl.textContent = '';
  try {
    const r = await fetch('/api/move', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: 'path=' + encodeURIComponent(srcPath) + '&dst=' + encodeURIComponent(dstPath)
    });
    const data = await r.json();
    if (data.ok) {
      closeMoveModal();
      showToast('Moved to ' + dstPath);
      renderDrive(currentPath);
    } else {
      if (errEl) errEl.textContent = data.error || 'Move failed';
      else showToast('Move failed: ' + (data.error || ''));
    }
  } catch (e) {
    if (errEl) errEl.textContent = 'Network error — please try again';
  }
}

async function deleteItem(path) {
  const itemName = path.split('/').filter(Boolean).pop() || path;
  const ok = await appConfirm({
    title: 'Delete item?',
    description: `Delete ${itemName}? This action cannot be undone.`,
    submitText: 'Delete',
    danger: true
  });
  if (!ok) return;
  const r = await fetch('/api/delete-path', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `path=${encodeURIComponent(path)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Deleted.' : 'Delete failed.');
  if (data.ok) {
    quotaCache = null;
    renderDrive(currentPath);
  }
}

async function makeFolder() {
  const name = await appPrompt({
    title: 'New folder',
    description: `Create a folder in ${currentPath || '/'}.`,
    placeholder: 'Folder name',
    submitText: 'Create folder'
  });
  if (!name) return;
  const r = await fetch('/api/mkdir', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `path=${encodeURIComponent(currentPath)}&name=${encodeURIComponent(name)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Folder created.' : 'Failed.');
  if (data.ok) renderDrive(currentPath);
}

async function renderSettings() {
  await Promise.all([loadContacts(true), loadQuota(true)]);
  const content = document.getElementById('content');
  const quota = quotaCache;
  content.innerHTML = `
    <div class="page-title">Settings</div>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:20px;align-items:start">
      <div class="compose-form" style="max-width:none">
        <h3 style="margin-bottom:16px;font-size:15px">Change password</h3>
        <input id="old-pw"  type="password" placeholder="Current password">
        <input id="new-pw"  type="password" placeholder="New password">
        <input id="new-pw2" type="password" placeholder="Confirm new password">
        <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="changePassword()">Update</button>
        <div id="pw-msg" class="error-msg"></div>
      </div>
      <div class="compose-form" style="max-width:none">
        <h3 style="margin-bottom:16px;font-size:15px">Storage quota</h3>
        <div style="font-size:13px;color:var(--muted);margin-bottom:12px">per-user drive quota with upload enforcement.</div>
        <input id="quota-mb" type="number" min="1" max="1024" value="${quota ? Math.round(quota.limit_bytes / (1024 * 1024)) : 50}">
        <div style="font-size:13px;color:var(--muted);margin-bottom:12px">Currently using ${quota ? `${formatBytes(quota.used_bytes)} of ${formatBytes(quota.limit_bytes)}` : 'unknown'}.</div>
        <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="updateQuota()">Save quota</button>
        <div id="quota-msg" class="error-msg"></div>
      </div>
      <div class="compose-form" style="max-width:none">
        <h3 style="margin-bottom:16px;font-size:15px">Address book</h3>
        <div class="contact-form-row">
          <input id="contact-name" type="text" placeholder="Name">
          <input id="contact-email" type="text" placeholder="Email or PennCloud username">
          <button class="btn btn-primary contact-add-btn" onclick="addContact()">Add</button>
        </div>
        <div id="contacts-list-box">
          ${(contactsCache || []).length === 0 ? '<div style="font-size:13px;color:var(--muted)">No contacts yet.</div>' : contactsCache.map(c => `
            <div class="contact-row">
              <div class="contact-main">
                <div class="contact-name" title="${escHtml(c.name)}">${escHtml(c.name)}</div>
                <div class="contact-email" title="${escHtml(c.email)}">${escHtml(c.email)}</div>
              </div>
              <div class="contact-actions"><button onclick="deleteContact('${escHtml(c.email)}')">Delete</button></div>
            </div>`).join('')}
        </div>
        <div id="contacts-msg" class="error-msg"></div>
      </div>
    </div>`;
}

async function changePassword() {
  const old = document.getElementById('old-pw').value;
  const n1  = document.getElementById('new-pw').value;
  const n2  = document.getElementById('new-pw2').value;
  if (!validPassword(n1)) { showPwMsg(passwordHelp()); return; }
  if (n1 !== n2) { showPwMsg('Passwords do not match.'); return; }
  const r = await fetch('/api/change-password', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `old_password=${encodeURIComponent(old)}&new_password=${encodeURIComponent(n1)}`
  });
  const data = await r.json();
  showPwMsg(data.ok ? 'Password updated!' : (data.error || 'Failed.'));
}

async function updateQuota() {
  const mb = document.getElementById('quota-mb').value;
  const r = await fetch('/api/quota', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `limit_mb=${encodeURIComponent(mb)}`
  });
  const data = await r.json();
  const el = document.getElementById('quota-msg');
  el.textContent = data.ok ? 'Quota updated.' : (data.error || 'Failed.');
  el.style.display = 'block';
  if (data.ok) await loadQuota(true);
}

async function addContact() {
  const name = document.getElementById('contact-name').value.trim();
  const email = document.getElementById('contact-email').value.trim();
  const msg = document.getElementById('contacts-msg');
  if (!name || !email) {
    msg.textContent = 'Name and email are required.';
    msg.style.display = 'block';
    return;
  }
  const r = await fetch('/api/contacts/add', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `name=${encodeURIComponent(name)}&email=${encodeURIComponent(email)}`
  });
  const data = await r.json();
  msg.textContent = data.ok ? 'Contact saved.' : (data.error || 'Failed.');
  msg.style.display = 'block';
  if (data.ok) {
    await loadContacts(true);
    renderSettings();
  }
}

async function deleteContact(email) {
  const r = await fetch('/api/contacts/delete', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `email=${encodeURIComponent(email)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Contact deleted.' : (data.error || 'Failed.'));
  if (data.ok) {
    await loadContacts(true);
    renderSettings();
  }
}

function showPwMsg(m) {
  const el = document.getElementById('pw-msg');
  if (!el) return;
  el.textContent = m;
  el.style.display = 'block';
}
// ---- SSE live inbox (F1 innovation) ----------------------------------------
function startSSE() {
  if (!authActive) return;

  if (sseReconnectTimer) {
    clearTimeout(sseReconnectTimer);
    sseReconnectTimer = null;
  }

  if (eventSource) {
    try { eventSource.close(); } catch (_) {}
    eventSource = null;
  }

  eventSource = new EventSource('/events');
  eventSource.addEventListener('new_email', async e => {
    if (!authActive) return;
    try {
      const email = JSON.parse(e.data || '{}');
      if (currentMailFolder === 'inbox' || window.location.pathname === '/inbox') {
        await renderInbox(currentMailFolder || 'inbox');
      } else {
        showToast('New email from ' + (email.from_display || email.from || 'unknown'));
      }
    } catch (_) {}
  });
  eventSource.onerror = () => {
    if (eventSource) {
      try { eventSource.close(); } catch (_) {}
      eventSource = null;
    }
    if (!authActive) return;
    if (!sseReconnectTimer) {
      sseReconnectTimer = setTimeout(() => {
        sseReconnectTimer = null;
        if (authActive) startSSE();
      }, 3000);
    }
  };
}

// ---- Utils -----------------------------------------------------------------
function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                  .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}

async function bootstrapApp() {
  if (!(await ensureBackendStorageAvailable())) return;
  const route = appRouteFromLocation();
  pendingRoute = route;
  try {
    const r = await fetch('/api/me', { cache: 'no-store' });
    const data = await r.json();
    if (data.ok && data.user) {
      currentUser = data.user;
      await showApp(route);
      return;
    }
  } catch (_) {}

  const msg = route.explicit
    ? `Please sign in to open ${routeLabel(route)}.`
    : '';
  showLogin(msg);
}

// Keyboard shortcut: Enter to login
document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.getElementById('login-page').style.display !== 'none') {
    doLogin();
  }
});

bootstrapApp();
</script>
</body>
</html>
)HTML";
    return HttpResponse::ok(html, "text/html; charset=utf-8");
}

namespace {

bool valid_account_username(const std::string& username) {
    if (username.size() < 3 || username.size() > 32) return false;
    if (username[0] < 'a' || username[0] > 'z') return false;
    for (unsigned char ch : username) {
        if (!(std::islower(ch) || std::isdigit(ch) || ch == '_' || ch == '-')) return false;
    }
    return true;
}

bool valid_account_password(const std::string& password) {
    return password.size() >= 8 && password.size() <= 128;
}

HttpResponse invalid_username_response() {
    return HttpResponse::json(
        R"({"ok":false,"error":"Username must be 3-32 chars, start with a lowercase letter, and use only lowercase letters, numbers, dash, or underscore"})");
}

HttpResponse invalid_password_response() {
    return HttpResponse::json(R"({"ok":false,"error":"Password must be at least 8 characters"})");
}

}  // namespace

// ---------------------------------------------------------------------------
// Auth handlers
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_login(const HttpRequest& req) {
    auto params = parse_urlencoded(req.body);
    std::string username = params["username"];
    std::string password = params["password"];

    if (username.empty() || password.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Missing credentials"})");
    if (!valid_account_username(username)) return HttpResponse::json(R"({"ok":false,"error":"Invalid username or password"})");

    // Fetch stored password from KV and distinguish not-found from unavailable.
    std::string stored_pwd;
    KVReadStatus st = kv_->get_status(username, "pwd", stored_pwd);
    if (st == KVReadStatus::Unavailable) {
        ensure_storage_ready_admin(cfg_);
        st = kv_->get_status(username, "pwd", stored_pwd);
        if (st == KVReadStatus::Unavailable) {
            return HttpResponse::json(
                R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
        }
    }
    if (st != KVReadStatus::Found || stored_pwd != password)
        return HttpResponse::json(R"({"ok":false,"error":"Invalid username or password"})");

    // Create session -- must succeed before returning ok:true.
    std::string sid = sessions_->create(username);
    if (sid.empty()) {
        ensure_storage_ready_admin(cfg_);
        sid = sessions_->create(username);
        if (sid.empty()) {
            return HttpResponse::json(
                R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
        }
    }

    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    resp.set_cookie("sid", sid);
    return resp;
}

HttpResponse FEServer::handle_logout(const HttpRequest& req, const std::string& user) {
    std::string sid = req.cookie("sid");
    if (!sid.empty() && !user.empty()) {
        sessions_->destroy(sid);
    }
    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    resp.set_cookie("sid", "", "/", true, 0);
    return resp;
}

HttpResponse FEServer::handle_session(const HttpRequest& req) {
    std::string user;
    SessionKVReadStatus st = sessions_->validate_status(req.cookie("sid"), user);
    if (st == SessionKVReadStatus::Found) {
        return HttpResponse::json("{\"ok\":true,\"authenticated\":true,\"user\":" +
                                  json_str(user) + "}");
    }
    if (st == SessionKVReadStatus::Unavailable) {
        HttpResponse r = HttpResponse::json(
            R"({"ok":false,"authenticated":false,"error":"storage unavailable"})");
        r.status_code = 503;
        r.status_text = "Service Unavailable";
        return r;
    }
    return HttpResponse::json(R"({"ok":true,"authenticated":false})");
}

HttpResponse FEServer::handle_signup(const HttpRequest& req) {
    auto params = parse_urlencoded(req.body);
    std::string username = params["username"];
    std::string password = params["password"];

    if (username.empty() || password.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Missing fields"})");
    if (!valid_account_username(username)) return invalid_username_response();
    if (!valid_account_password(password)) return invalid_password_response();

    // Check if user already exists and distinguish not-found from unavailable.
    std::string existing;
    KVReadStatus st = kv_->get_status(username, "pwd", existing);
    if (st == KVReadStatus::Unavailable) {
        ensure_storage_ready_admin(cfg_);
        st = kv_->get_status(username, "pwd", existing);
        if (st == KVReadStatus::Unavailable) {
            return HttpResponse::json(
                R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
        }
    }
    if (st == KVReadStatus::Found)
        return HttpResponse::json(R"({"ok":false,"error":"Username already taken"})");

    // Persist password -- must succeed before returning ok:true.
    if (!kv_->put(username, "pwd", password)) {
        return HttpResponse::json(
            R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
    }

    // Create session -- must also succeed.
    std::string sid = sessions_->create(username);
    if (sid.empty()) {
        (void)kv_->del(username, "pwd");
        return HttpResponse::json(
            R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
    }

    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    resp.set_cookie("sid", sid);
    return resp;
}

HttpResponse FEServer::handle_change_password(const HttpRequest& req) {
    std::string user = sessions_->authenticate(req);
    if (user.empty()) return HttpResponse::error(401, "Unauthorized");

    auto params = parse_urlencoded(req.body);
    std::string old_pw = params["old_password"];
    std::string new_pw = params["new_password"];
    if (!valid_account_password(new_pw)) return invalid_password_response();

    std::string stored;
    KVReadStatus st = kv_->get_status(user, "pwd", stored);
    if (st == KVReadStatus::Unavailable) {
        return HttpResponse::json(
            R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
    }
    if (st == KVReadStatus::NotFound) {
        return HttpResponse::error(401, "Unauthorized");
    }
    if (stored != old_pw)
        return HttpResponse::json(R"({"ok":false,"error":"Wrong current password"})");

    if (!kv_->put(user, "pwd", new_pw)) {
        return HttpResponse::json(
            R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
    }
    return HttpResponse::json(R"({"ok":true})");
}

// ---------------------------------------------------------------------------
// Admin console v1 (read-only dashboard + JSON status API)
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_admin_status(const HttpRequest&) {
    std::string backend_json = "[]";
    std::string tablets_json = "[]";

    auto try_coord = [&](const std::string& host, int port) -> bool {
        if (port <= 0 || host.empty()) return false;
        std::string tmp;
        bool got_any = false;
        if (query_coordinator_status_json(host, port, tmp)) {
            backend_json = tmp;
            got_any = true;
        }
        if (query_coordinator_tablets_json(host, port, tmp)) {
            tablets_json = tmp;
            got_any = true;
        }
        return got_any;
    };

    bool ok = false;
    if (cfg_.coord_port > 0 && !cfg_.coord_host.empty()) {
        ok = try_coord(cfg_.coord_host, cfg_.coord_port);
    } else {
        if (!ok) ok = try_coord("127.0.0.1", 7110);
        if (!ok) ok = try_coord("127.0.0.1", 7010);
        if (!ok) ok = try_coord("127.0.0.1", 6010);
        if (!ok) ok = try_coord("127.0.0.1", 6000);
    }
    if (!ok) { backend_json = fallback_backend_json_admin(cfg_); tablets_json = "[]"; }
    apply_backend_port_probe_admin(backend_json, cfg_);

    std::string fe_json = frontend_nodes_json();
    const char* public_host_env = std::getenv("PENNCLOUD_PUBLIC_HOST");
    std::string public_host = (public_host_env && *public_host_env) ? public_host_env : "";
    std::string body = "{\"ok\":true,\"public_host\":" + json_str(public_host) +
                       ",\"backend_nodes\":" + backend_json +
                       ",\"frontend_nodes\":" + fe_json +
                       ",\"tablets\":" + tablets_json + "}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_admin_raw(const HttpRequest& req) {
    auto parse_size_param = [&](const std::string& name, size_t fallback,
                                size_t min_value, size_t max_value) {
        std::string raw = req.param(name);
        if (raw.empty()) return fallback;
        try {
            size_t pos = 0;
            unsigned long long parsed = std::stoull(raw, &pos, 10);
            if (pos != raw.size()) return fallback;
            size_t value = static_cast<size_t>(parsed);
            if (value < min_value) return min_value;
            if (value > max_value) return max_value;
            return value;
        } catch (...) {
            return fallback;
        }
    };

    const size_t limit = parse_size_param("limit", 25, 1, 200);
    const size_t offset = parse_size_param("offset", 0, 0, static_cast<size_t>(-1));
    AdminClusterSpec cluster = configured_cluster_spec_admin(cfg_);

    std::string node_id = req.param("node");
    if (node_id.empty() && !cluster.backends.empty()) node_id = cluster.backends.begin()->first;

    auto it = cluster.backends.find(node_id);
    if (it == cluster.backends.end()) {
        HttpResponse r = HttpResponse::json(R"({"ok":false,"error":"unknown backend node"})");
        r.status_code = 400;
        r.status_text = "Bad Request";
        return r;
    }

    const AdminBackendSpec& n = it->second;
    std::string dump_json;
    if (!query_kv_dump_json_admin("127.0.0.1", n.kv_port, limit, offset, dump_json)) {
        HttpResponse r = HttpResponse::json("{\"ok\":false,\"error\":\"raw dump unavailable\",\"node\":" +
                                            json_str(n.id) + "}");
        r.status_code = 503;
        r.status_text = "Service Unavailable";
        return r;
    }

    std::string body = "{\"ok\":true,\"node\":" + json_str(n.id) +
                       ",\"host\":\"127.0.0.1\",\"port\":" + std::to_string(n.kv_port) +
                       ",\"dump\":" + dump_json + "}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_admin_control(const HttpRequest& req) {
    if (!admin_control_allowed(req)) {
        HttpResponse r = HttpResponse::json(R"({"ok":false,"error":"admin control forbidden"})");
        r.status_code = 403;
        r.status_text = "Forbidden";
        return r;
    }

    std::string kind = admin_json_field(req.body, "kind");
    std::string action = admin_json_field(req.body, "action");
    std::string target = admin_json_field(req.body, "target");
    if (kind.empty() || action.empty() || target.empty()) {
        auto params = parse_urlencoded(req.body);
        if (kind.empty()) kind = params["kind"];
        if (action.empty()) action = params["action"];
        if (target.empty()) target = params["target"];
    }
    if (kind.empty()) kind = req.param("kind");
    if (action.empty()) action = req.param("action");
    if (target.empty()) target = req.param("target");

    admin_log("control request kind=" + kind + " action=" + action + " target=" + target);

    if (kind.empty() || action.empty() || target.empty()) {
        return HttpResponse::json(R"({"ok":false,"error":"missing control fields"})");
    }

    std::string message;
    bool ok = false;
    try {
        ok = perform_admin_control(cfg_, kind, action, target, message);
    } catch (const std::exception& e) {
        message = std::string("admin control exception: ") + e.what();
        admin_log(message);
    } catch (...) {
        message = "admin control exception: unknown";
        admin_log(message);
    }
    std::string body = std::string("{\"ok\":") + (ok ? "true" : "false") +
                       ",\"message\":" + json_str(message.empty() ? (ok ? "ok" : "failed") : message) + "}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_admin_control_redirect(const HttpRequest& req) {
    if (!admin_control_allowed(req)) return HttpResponse::forbidden();

    std::string kind = req.param("kind");
    std::string action = req.param("action");
    std::string target = req.param("target");

    admin_log("control redirect request kind=" + kind + " action=" + action + " target=" + target);

    std::string down = admin_location_token(req.param("pc_down"));
    std::string admin_token = admin_location_token(req.param("admin_token"));
    std::string host = req.header("host");
    if (host.empty()) host = "127.0.0.1";
    size_t colon = host.find(':');
    if (colon != std::string::npos) host = host.substr(0, colon);
    std::string forwarded_host = req.header("x-forwarded-host");
    std::string forwarded_proto = req.header("x-forwarded-proto");
    if (forwarded_proto.empty()) forwarded_proto = "http";
    auto admin_base_url = [&](int port) {
        if (!forwarded_host.empty()) return forwarded_proto + "://" + forwarded_host + "/admin";
        return std::string("http://") + host + ":" + std::to_string(port) + "/admin";
    };

    int target_port = frontend_target_port_admin(target);
    if (kind == "frontend" && action == "kill" && target_port == cfg_.port) {
        // Self-kill: find a real (non-stub) peer, schedule our own kill with a
        // short delay so this response can flush, then send the browser directly
        // to that peer's /admin page. Do NOT redirect to /admin/control on the
        // peer — redirect stubs would intercept that and create an infinite loop.
        for (const auto& p : frontend_peer_order_admin()) {
            if (p.second == cfg_.port) continue;
            if (!frontend_admin_status_ok_admin(p.second)) continue; // skip stubs/dead
            {
                // Use an external helper so kill+stub-launch survives after this
                // process exits. FD_CLOEXEC on the listen socket keeps the helper
                // from inheriting our listener while it relaunches the stub.
                int my_port = cfg_.port;
                std::string fe_bin_loc = fe_binary_path_admin(resolve_project_root_admin());
                int stub_peer = p.second; // redirect stub points to the peer we're handing off to
                std::string kill_and_stub =
                    frontend_self_kill_helper_cmd_admin({my_port}, my_port, stub_peer, fe_bin_loc);
                run_shell_command(kill_and_stub);
            }
            std::string location = admin_base_url(p.second);
            bool has_q = false;
            if (!admin_token.empty()) {
                location += "?admin_token=" + admin_token;
                has_q = true;
            }
            std::string safe_target = admin_location_token(target);
            location += has_q ? "&" : "?";
            if (!down.empty()) location += "pc_down=" + down + "," + safe_target;
            else               location += "pc_down=" + safe_target;
            admin_log("self-kill: killing " + target + " via shell subshell, redirecting browser to " + p.first);
            HttpResponse resp = HttpResponse::redirect(location, 302);
            // Force close before SIGKILL so browsers do not reuse a dying socket
            // and miss the redirect.
            resp.headers["Connection"] = "close";
            return resp;
        }
        // No real peer found — fall through to perform_admin_control (self-kill fallback)
    }

    std::string message;
    bool ok = false;
    if (!kind.empty() && !action.empty() && !target.empty()) {
        try {
            ok = perform_admin_control(cfg_, kind, action, target, message);
        } catch (const std::exception& e) {
            message = std::string("admin control redirect exception: ") + e.what();
            admin_log(message);
        } catch (...) {
            message = "admin control redirect exception: unknown";
            admin_log(message);
        }
    }

    auto add_down = [&](const std::string& id) {
        if (id.empty()) return;
        std::vector<std::string> ids;
        std::istringstream ss(down);
        std::string tok;
        bool seen = false;
        while (std::getline(ss, tok, ',')) {
            if (tok.empty()) continue;
            if (tok == id) seen = true;
            ids.push_back(tok);
        }
        if (!seen) ids.push_back(id);
        down.clear();
        for (size_t i = 0; i < ids.size(); ++i) {
            if (i) down += ",";
            down += ids[i];
        }
    };
    if (kind == "frontend" && action == "kill") add_down(admin_location_token(target));

    int redirect_port = cfg_.port;
    if (kind == "frontend" && action == "kill" && target_port == cfg_.port) {
        admin_log("self-kill for " + target + " has no real frontend peer; returning 404 after scheduling kill");
        return HttpResponse::not_found();
    }

    std::string location = admin_base_url(redirect_port);
    bool has_q = false;
    if (!admin_token.empty()) {
        location += "?admin_token=" + admin_token;
        has_q = true;
    }
    if (!down.empty()) {
        location += has_q ? "&" : "?";
        location += "pc_down=" + down;
        has_q = true;
    }
    if (!message.empty()) {
        location += has_q ? "&" : "?";
        location += std::string("pc_msg=") + (ok ? "ok" : "failed");
    }
    HttpResponse resp = HttpResponse::redirect(location, 302);
    if (kind == "frontend" && action == "kill") resp.headers["Connection"] = "close";
    return resp;
}

HttpResponse FEServer::handle_admin_metrics(const HttpRequest& req) {
    return handle_admin_status(req);
}

HttpResponse FEServer::handle_admin_page(const HttpRequest&) {
    static const std::string html = R"HTML(
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PennCloud Admin</title>
  <link rel="icon" type="image/png" href="/static/logo.png">
  <style>
    :root {
      --penn-blue: #011F5B;
      --penn-blue-2: #0A2D72;
      --accent: #1464C8;
      --bg: #F4F7FB;
      --surface: #FFFFFF;
      --surface-2: #F8FBFF;
      --border: #DDE6F1;
      --border-2: #C9D6EA;
      --text: #182235;
      --muted: #6C7A8C;
      --success: #167246;
      --danger: #C93636;
      --shadow-sm: 0 8px 22px rgba(1,31,91,0.07);
      --shadow: 0 10px 28px rgba(1,31,91,0.08);
    }
    *, *::before, *::after {
      box-sizing: border-box;
    }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 12% 0%, rgba(20,100,200,0.09), transparent 28%),
        linear-gradient(180deg, #FAFCFF 0%, var(--bg) 280px);
      color: var(--text);
      margin: 0;
      padding: 28px;
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }
    .admin-shell {
      width: min(100%, 1500px);
      margin: 0 auto;
    }
    .admin-hero {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      background: linear-gradient(135deg, var(--penn-blue) 0%, var(--penn-blue-2) 100%);
      color: #fff;
      border-radius: 18px;
      padding: 22px 24px;
      margin-bottom: 18px;
      box-shadow: 0 18px 44px rgba(1,31,91,0.18);
      overflow: hidden;
    }
    .logo-lockup {
      display: flex;
      align-items: center;
      gap: 16px;
      min-width: 0;
    }
    .admin-logo {
      width: 48px;
      height: 56px;
      object-fit: contain;
      flex: 0 0 auto;
      filter: drop-shadow(0 8px 16px rgba(0,0,0,0.22));
    }
    .eyebrow {
      color: rgba(255,255,255,0.72);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    h1 {
      margin: 3px 0 5px;
      color: #fff;
      font-size: 28px;
      line-height: 1.1;
      letter-spacing: -0.5px;
    }
    .meta {
      color: rgba(255,255,255,0.72);
      font-size: 14px;
      margin-bottom: 0;
    }
    .hero-status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid rgba(255,255,255,0.22);
      background: rgba(255,255,255,0.12);
      border-radius: 999px;
      padding: 8px 11px;
      font-size: 13px;
      font-weight: 700;
      white-space: nowrap;
    }
    .live-dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: #38A169;
      box-shadow: 0 0 0 4px rgba(56,161,105,0.22);
    }
    .admin-metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(170px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .metric {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: var(--shadow);
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      margin-bottom: 6px;
    }
    .metric strong {
      display: block;
      color: var(--penn-blue);
      font-size: 24px;
      line-height: 1;
    }
    .section-title {
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 12px;
      margin: 22px 0 10px;
    }
    h2 {
      margin: 0;
      color: var(--penn-blue);
      font-size: 18px;
      letter-spacing: -0.25px;
    }
    .section-note {
      color: var(--muted);
      font-size: 13px;
    }
    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      overflow-x: auto;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
    }
    table {
      width: 100%;
      min-width: 850px;
      border-collapse: collapse;
    }
    #frontend-table, #replica-table { min-width: 640px; }
    #raw-table { min-width: 980px; }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      font-size: 14px;
      vertical-align: middle;
    }
    th {
      background: var(--surface-2);
      color: #4A5568;
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      white-space: nowrap;
    }
    tbody tr:hover { background: #FAFCFF; }
    td button {
      margin-right: 6px;
      padding: 6px 10px;
      border: 1px solid var(--border-2);
      background: #fff;
      border-radius: 7px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 700;
      color: #2D3748;
    }
    td button:hover { background: var(--surface-2); border-color: #A0AEC0; }
    td button:first-child { color: var(--danger); border-color: rgba(197,48,48,0.42); }
    tr:last-child td {
      border-bottom: none;
    }
    .alive, .down, .ok, .warn {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 3px 9px;
      font-size: 12px;
      font-weight: 800;
    }
    .alive, .ok { color: var(--success); background: #EDF7F1; }
    .down, .warn { color: var(--danger); background: #FFF1F1; }
    .muted { color: var(--muted); }
    .raw-toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
      flex-wrap: wrap;
      background: var(--surface-2);
    }
    .raw-toolbar label {
      font-size: 13px;
      color: #4A5568;
      font-weight: 700;
    }
    .raw-toolbar select, .raw-toolbar button {
      min-height: 34px;
      border: 1px solid var(--border-2);
      background: #fff;
      border-radius: 7px;
      padding: 6px 10px;
      font-size: 13px;
    }
    .raw-toolbar button { cursor: pointer; }
    .raw-toolbar button:disabled {
      color: #A0AEC0;
      cursor: default;
      background: var(--surface-2);
    }
    #raw-meta {
      padding: 10px 14px;
      color: var(--muted);
      font-size: 13px;
      border-bottom: 1px solid var(--border);
    }
    .preview {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      max-width: 520px;
    }
    #status {
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 11px 13px;
      box-shadow: var(--shadow);
    }
    @media (max-width: 820px) {
      body { padding: 18px 12px; }
      .admin-shell { width: min(100vw - 24px, 1500px); }
      .admin-hero { align-items: flex-start; flex-direction: column; }
      .admin-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .section-title { align-items: flex-start; flex-direction: column; }
    }
    @media (max-width: 520px) {
      .admin-metrics { grid-template-columns: 1fr; }
      h1 { font-size: 24px; }
      .admin-logo { width: 36px; height: 42px; }
    }
  </style>
</head>
<body>
  <div class="admin-shell">
  <header class="admin-hero">
    <div class="logo-lockup">
      <img class="admin-logo" src="/static/logo.png" alt="">
      <div>
        <div class="eyebrow">PennCloud Control Plane</div>
        <h1>Admin Console</h1>
        <div class="meta">Auto-refresh every 2 seconds with live frontend, backend, and tablet status.</div>
      </div>
    </div>
    <div class="hero-status"><span class="live-dot"></span><span>Live monitor</span></div>
  </header>

  <div class="admin-metrics">
    <div class="metric"><span>Backends up</span><strong id="backend-count">--</strong></div>
    <div class="metric"><span>Frontends up</span><strong id="frontend-count">--</strong></div>
    <div class="metric"><span>Tablets</span><strong id="tablet-count">--</strong></div>
    <div class="metric"><span>Replica health</span><strong id="replica-count">--</strong></div>
  </div>

  <div class="section-title">
    <h2>Backend Servers</h2>
    <div class="section-note">KV replicas, log position, and recovery actions</div>
  </div>
  <div class="card">
    <table id="backend-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Internal Host</th>
          <th>KV Port</th>
          <th>Repl Port</th>
          <th>Alive</th>
          <th>LSN</th>
          <th>Missed</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="section-title">
    <h2>Frontend Servers</h2>
    <div class="section-note">HTTP serving tier and failover controls</div>
  </div>
  <div class="card">
    <table id="frontend-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Host</th>
          <th>Port</th>
          <th>Alive</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="section-title">
    <h2>Tablet Placement</h2>
    <div class="section-note">Replica assignment by tablet range</div>
  </div>
  <div class="card">
    <table id="tablet-table">
      <thead>
        <tr>
          <th>Tablet</th>
          <th>Row Start</th>
          <th>Row End</th>
          <th>Replica Node</th>
          <th>Role</th>
          <th>Alive</th>
          <th>Internal Host</th>
          <th>KV Port</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="section-title">
    <h2>Replica Validation</h2>
    <div class="section-note">Checks for three distinct copies</div>
  </div>
  <div class="card">
    <table id="replica-table">
      <thead>
        <tr>
          <th>Tablet</th>
          <th>Total Copies</th>
          <th>Distinct Servers</th>
          <th>Live Copies</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="section-title">
    <h2>Raw KV Data</h2>
    <div class="section-note">Paged cell preview from the selected backend</div>
  </div>
  <div class="card">
    <div class="raw-toolbar">
      <label for="raw-node">Backend</label>
      <select id="raw-node" onchange="handleRawNodeChange()"></select>
      <button type="button" onclick="refreshRawData(true)">Refresh</button>
      <button id="raw-prev" type="button" onclick="rawPrev()">Prev</button>
      <button id="raw-next" type="button" onclick="rawNext()">Next</button>
    </div>
    <div id="raw-meta">Loading raw KV data</div>
    <table id="raw-table">
      <thead>
        <tr>
          <th>Tablet</th>
          <th>Row</th>
          <th>Column</th>
          <th>Bytes</th>
          <th>Preview</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div id="status">Loading…</div>
  </div>

<script>
function adminTokenHeaders(headers = {}) {
  const adminToken = new URLSearchParams(window.location.search || '').get('admin_token') || '';
  if (adminToken) headers['X-Admin-Token'] = adminToken;
  return headers;
}
function esc(s) {
  if (s === null || s === undefined) return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
function aliveCell(v) {
  return v ? '<span class="alive">UP</span>' : '<span class="down">DOWN</span>';
}
function byNodeId(a, b) {
  return String((a && a.id) || '').localeCompare(String((b && b.id) || ''), undefined, {numeric:true});
}
let adminPublicHost = '';
function publicHostForAdmin() {
  return adminPublicHost || window.location.hostname || 'localhost';
}
function frontendDisplayHost(n) {
  return publicHostForAdmin();
}
function internalDisplayHost(n) {
  return (n && n.host && n.host !== '127.0.0.1') ? n.host : 'internal';
}
function updateAdminSummary(data) {
  const backends = [...(data.backend_nodes || [])].sort(byNodeId);
  const frontends = [...(data.frontend_nodes || [])].sort(byNodeId);
  const tablets = data.tablets || [];
  const backendUp = backends.filter(n => n && n.alive).length;
  const frontendUp = frontends.filter(n => n && n.alive).length;
  const replicaOk = tablets.filter(t => {
    const reps = t.replicas || [];
    const ids = new Set(reps.map(r => r && r.id).filter(Boolean).map(String));
    return reps.length >= 3 && ids.size >= 3;
  }).length;
  document.getElementById('backend-count').textContent = `${backendUp}/${backends.length}`;
  document.getElementById('frontend-count').textContent = `${frontendUp}/${frontends.length}`;
  document.getElementById('tablet-count').textContent = String(tablets.length);
  document.getElementById('replica-count').textContent = tablets.length ? `${replicaOk}/${tablets.length}` : '--';
}
function renderBackend(nodes) {
  const tbody = document.querySelector('#backend-table tbody');
  tbody.innerHTML = '';
  for (const n of [...(nodes || [])].sort(byNodeId)) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${esc(n.id)}</td>
      <td title="${esc(n.host || '')}">${esc(internalDisplayHost(n))}</td>
      <td>${esc(n.port)}</td>
      <td>${esc(n.repl_port)}</td>
      <td>${aliveCell(!!n.alive)}</td>
      <td>${esc(n.lsn)}</td>
      <td>${esc(n.missed)}</td>
      <td>
        <button data-kind="backend" data-action="kill" data-target="${esc(n.id)}">Kill</button>
        <button data-kind="backend" data-action="restart" data-target="${esc(n.id)}">Restart</button>
      </td>
    `;
    tbody.appendChild(tr);
  }
}
function renderFrontend(nodes) {
  const tbody = document.querySelector('#frontend-table tbody');
  tbody.innerHTML = '';
  for (const n of [...(nodes || [])].sort(byNodeId)) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${esc(n.id)}</td>
      <td title="${esc(n.host || '')}">${esc(frontendDisplayHost(n))}</td>
      <td>${esc(n.port)}</td>
      <td>${aliveCell(!!n.alive)}</td>
      <td>
        <button data-kind="frontend" data-action="kill" data-target="${esc(n.id)}">Kill</button>
        <button data-kind="frontend" data-action="restart" data-target="${esc(n.id)}">Restart</button>
      </td>
    `;
    tbody.appendChild(tr);
  }
}

function adminControlUrl(kind, action, target) {
  const params = new URLSearchParams(window.location.search || '');
  params.set('kind', kind);
  params.set('action', action);
  params.set('target', target);
  params.delete(adminDownParam);
  params.delete('pc_msg');
  return `/admin/control?${params.toString()}`;
}

function adminFrontendKill(target) {
  const status = document.getElementById('status');
  status.textContent = `Sending kill to ${target}...`;
  if (isCurrentFrontendTarget(target)) status.textContent = `Finding a live peer before killing ${target}...`;
  window.location.href = adminControlUrl('frontend', 'kill', target);
}

function renderTablets(tablets) {
  const tbody = document.querySelector('#tablet-table tbody');
  tbody.innerHTML = '';
  for (const t of tablets) {
    const reps = t.replicas || [];
    if (reps.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${esc(t.name)}</td>
        <td>${esc(t.row_start)}</td>
        <td>${esc(t.row_end)}</td>
        <td colspan="5" class="muted">No replicas reported</td>
      `;
      tbody.appendChild(tr);
      continue;
    }
    reps.forEach((r, idx) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${idx === 0 ? esc(t.name) : ''}</td>
        <td>${idx === 0 ? esc(t.row_start) : ''}</td>
        <td>${idx === 0 ? esc(t.row_end) : ''}</td>
        <td>${esc(r.id)}</td>
        <td>${esc(r.role)}</td>
        <td>${aliveCell(!!r.alive)}</td>
        <td title="${esc(r.host || '')}">${esc(internalDisplayHost(r))}</td>
        <td>${esc(r.port)}</td>
      `;
      tbody.appendChild(tr);
    });
  }
}

let adminRawNode = '';
let adminRawOffset = 0;
let adminRawLimit = 25;
let adminRawHasMore = false;
let adminRawInFlight = false;
let adminRawLoaded = false;

function renderReplicaValidation(tablets) {
  const tbody = document.querySelector('#replica-table tbody');
  tbody.innerHTML = '';
  if (!tablets || tablets.length === 0) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="5" class="muted">No tablet metadata reported</td>';
    tbody.appendChild(tr);
    return;
  }
  for (const t of tablets) {
    const reps = t.replicas || [];
    const ids = new Set();
    let live = 0;
    for (const r of reps) {
      if (r && r.id) ids.add(String(r.id));
      if (r && r.alive) live += 1;
    }
    const placementOk = reps.length >= 3 && ids.size >= 3;
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${esc(t.name)}</td>
      <td>${esc(reps.length)}</td>
      <td>${esc(ids.size)}</td>
      <td>${esc(live)}</td>
      <td><span class="${placementOk ? 'ok' : 'warn'}">${placementOk ? '3-copy placement OK' : 'Needs 3 distinct replicas'}</span></td>
    `;
    tbody.appendChild(tr);
  }
}

function renderRawNodeOptions(nodes) {
  const select = document.getElementById('raw-node');
  if (!select) return;
  const previous = adminRawNode || select.value || '';
  const list = (nodes || []).filter(n => n && n.id);
  let next = previous;
  if (!next || !list.some(n => String(n.id) === String(next))) {
    const preferred = list.find(n => n.alive) || list[0];
    next = preferred ? String(preferred.id) : '';
  }
  if (next !== adminRawNode) {
    adminRawNode = next;
    adminRawOffset = 0;
    adminRawLoaded = false;
  }
  select.innerHTML = '';
  for (const n of list) {
    const opt = document.createElement('option');
    opt.value = String(n.id);
    opt.textContent = `${n.id}:${n.port || ''} ${n.alive ? 'UP' : 'DOWN'}`;
    select.appendChild(opt);
  }
  select.value = adminRawNode;
  select.disabled = list.length === 0;
}

function renderRawData(payload) {
  const tbody = document.querySelector('#raw-table tbody');
  const meta = document.getElementById('raw-meta');
  const prev = document.getElementById('raw-prev');
  const next = document.getElementById('raw-next');
  tbody.innerHTML = '';
  const dump = payload && payload.dump ? payload.dump : {};
  const cells = dump.cells || [];
  adminRawHasMore = !!dump.has_more;
  if (prev) prev.disabled = adminRawOffset === 0;
  if (next) next.disabled = !adminRawHasMore;
  meta.textContent = `${payload.node || adminRawNode} rows ${dump.offset || 0} through ${(dump.offset || 0) + cells.length} of ${dump.total || 0}`;
  if (!cells.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="5" class="muted">No cells on this backend page</td>';
    tbody.appendChild(tr);
    return;
  }
  for (const c of cells) {
    const suffix = c.truncated ? ' ...' : '';
    const format = c.preview_format === 'hex' ? 'hex: ' : '';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${esc(c.tablet)}</td>
      <td>${esc(c.row)}</td>
      <td>${esc(c.column)}</td>
      <td>${esc(c.value_size)}</td>
      <td class="preview">${esc(format + (c.preview || '') + suffix)}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function refreshRawData(resetOffset) {
  if (resetOffset) adminRawOffset = 0;
  const meta = document.getElementById('raw-meta');
  const tbody = document.querySelector('#raw-table tbody');
  if (!adminRawNode) {
    meta.textContent = 'No backend node available';
    tbody.innerHTML = '<tr><td colspan="5" class="muted">No backend node available</td></tr>';
    return;
  }
  if (adminRawInFlight) return;
  adminRawInFlight = true;
  try {
    meta.textContent = `Loading ${adminRawNode} raw data...`;
    const params = new URLSearchParams();
    params.set('node', adminRawNode);
    params.set('limit', String(adminRawLimit));
    params.set('offset', String(adminRawOffset));
    const r = await fetch(`/api/admin/raw?${params.toString()}`, {
      cache: 'no-store',
      headers: adminTokenHeaders({})
    });
    const data = await r.json();
    if (!r.ok || !data.ok) throw new Error(data.error || `raw ${r.status}`);
    adminRawLoaded = true;
    renderRawData(data);
  } catch (e) {
    adminRawLoaded = false;
    adminRawHasMore = false;
    const prev = document.getElementById('raw-prev');
    const next = document.getElementById('raw-next');
    if (prev) prev.disabled = adminRawOffset === 0;
    if (next) next.disabled = true;
    meta.textContent = 'Raw KV data unavailable: ' + (e && e.message ? e.message : e);
    tbody.innerHTML = '<tr><td colspan="5" class="muted">Unable to read this backend</td></tr>';
  } finally {
    adminRawInFlight = false;
  }
}

function handleRawNodeChange() {
  const select = document.getElementById('raw-node');
  adminRawNode = select ? select.value : '';
  adminRawOffset = 0;
  adminRawLoaded = false;
  refreshRawData(true);
}

function rawPrev() {
  adminRawOffset = Math.max(0, adminRawOffset - adminRawLimit);
  refreshRawData(false);
}

function rawNext() {
  if (!adminRawHasMore) return;
  adminRawOffset += adminRawLimit;
  refreshRawData(false);
}

async function adminControl(kind, action, target) {
  adminActionInFlight = true;
  if (kind === 'frontend' && action === 'kill') {
    adminFrontendKill(target);
    return;
  }
  const status = document.getElementById('status');
  status.textContent = `Sending ${action} to ${target}...`;
  document.querySelectorAll('#backend-table button, #frontend-table button').forEach(btn => {
    btn.disabled = true;
  });
  try {
    const headers = adminTokenHeaders({'Content-Type': 'application/json'});
    const r = await fetch('/api/admin/control', {
      method: 'POST',
      headers,
      body: JSON.stringify({kind, action, target})
    });
    const text = await r.text();
    if (!r.ok) throw new Error(`control ${r.status}: ${text.slice(0, 120)}`);
    let data;
    try {
      data = JSON.parse(text);
    } catch (parseErr) {
      throw new Error('control returned non-JSON response');
    }
    if (!data.ok && kind === 'frontend' && action === 'kill') {
      navigateAdminControlFallback(kind, action, target);
      return;
    }
    if (!data.ok) throw new Error(data.error || data.message || 'control failed');
    status.textContent = data.message || (data.ok ? 'ok' : 'failed');
    adminStatusFailures = 0;
    if (kind === 'frontend' && action === 'kill' && isCurrentFrontendTarget(target)) {
      adminLastFrontendKillAt = Date.now();
      adminStatusMutedUntil = Date.now() + 10000;
      if (adminPollTimer) clearInterval(adminPollTimer);
      status.textContent = 'This frontend is stopping. Moving to another frontend...';
      setTimeout(() => redirectToPeerFrontend(target), 25);
      return;
    }
    if (kind === 'frontend' && action === 'restart' && isCurrentFrontendTarget(target)) {
      adminStatusMutedUntil = Date.now() + 6000;
    }
    if (action === 'restart') {
      // Poll rapidly after restart: process needs time for WAL recovery before
      // its port becomes available. Keep polling until status stabilises.
      let polls = 0;
      const pollRestart = () => {
        refreshStatus();
        if (++polls < 8) setTimeout(pollRestart, 1000);
      };
      setTimeout(pollRestart, 800);
    } else {
      if (kind === 'frontend' && action === 'kill') {
        adminLastFrontendKillAt = Date.now();
        adminStatusMutedUntil = Date.now() + 1800;
        history.replaceState(history.state, '', `/admin${adminQueryWithDown(target)}`);
        status.textContent = `${target} kill scheduled. Refreshing status...`;
      }
      // Kill is instant; one quick poll is enough.
      setTimeout(refreshStatus, kind === 'frontend' ? 1000 : 400);
    }
  } catch (e) {
    if (kind === 'frontend' && action === 'kill') {
      adminLastFrontendKillAt = Date.now();
      adminStatusMutedUntil = Date.now() + 10000;
      if (adminPollTimer) clearInterval(adminPollTimer);
      status.textContent = `Retrying ${action} for ${target} through another frontend...`;
      if (!navigateAdminControlFallback(kind, action, target)) {
        status.textContent = 'admin control failed: ' + (e && e.message ? e.message : e);
      }
      return;
    }
    status.textContent = 'admin control failed: ' + (e && e.message ? e.message : e);
  } finally {
    adminActionInFlight = false;
    document.querySelectorAll('#backend-table button, #frontend-table button').forEach(btn => {
      btn.disabled = false;
    });
  }
}

let adminStatusFailures = 0;
let adminStatusInFlight = false;
let adminActionInFlight = false;
let adminStatusMutedUntil = 0;
let adminPollTimer = null;
let adminFrontendNodes = [];
let adminRedirecting = false;
const adminDownParam = 'pc_down';
let adminLastFrontendKillAt = 0;

function isCurrentFrontendTarget(target) {
  const port = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
  return (target === 'fe1' && port === '8090') ||
         (target === 'fe2' && port === '8091') ||
         (target === 'fe3' && port === '8092');
}

function adminFrontendIdForPort(port) {
  if (String(port) === '8090') return 'fe1';
  if (String(port) === '8091') return 'fe2';
  if (String(port) === '8092') return 'fe3';
  return '';
}

function adminDownFrontends(extraId = '') {
  const params = new URLSearchParams(window.location.search || '');
  const set = new Set((params.get(adminDownParam) || '').split(',').filter(Boolean));
  if (extraId) set.add(extraId);
  return set;
}

function adminQueryWithDown(extraId = '') {
  const params = new URLSearchParams(window.location.search || '');
  const down = adminDownFrontends(extraId);
  if (down.size) params.set(adminDownParam, Array.from(down).join(','));
  else params.delete(adminDownParam);
  const qs = params.toString();
  return qs ? '?' + qs : '';
}

function adminControlFallbackPorts(target) {
  const currentPort = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
  const currentId = adminFrontendIdForPort(currentPort);
  const down = adminDownFrontends('');
  const livePeers = (adminFrontendNodes || [])
    .filter(n => n && n.alive && String(n.port) !== currentPort && n.id !== target && !down.has(n.id))
    .map(n => String(n.port));
  const fallback =
    target === 'fe1' ? ['8091', '8092'] :
    target === 'fe2' ? ['8092', '8090'] :
    target === 'fe3' ? ['8091', '8090'] :
    ['8091', '8092', '8090'];
  const candidates = [...new Set([...livePeers, ...fallback])]
    .filter(p => !down.has(adminFrontendIdForPort(p)));
  if (currentPort && !candidates.includes(currentPort) && (!down.has(currentId) || currentId === target)) {
    candidates.push(currentPort);
  }
  return candidates;
}

function navigateAdminControlFallback(kind, action, target) {
  const ports = adminControlFallbackPorts(target);
  if (!ports.length) return false;
  const params = new URLSearchParams(window.location.search || '');
  params.set('kind', kind);
  params.set('action', action);
  params.set('target', target);
  const down = adminDownFrontends('');
  if (down.size) params.set(adminDownParam, Array.from(down).join(','));
  window.location.replace(`${window.location.protocol}//${window.location.hostname}:${ports[0]}/admin/control?${params.toString()}`);
  return true;
}

function redirectToPeerFrontend(stoppingTarget) {
  if (adminRedirecting) return;
  const down = adminDownFrontends(stoppingTarget);
  const livePeers = (adminFrontendNodes || [])
    .filter(n => n && n.alive && n.id !== stoppingTarget && !down.has(n.id) && n.port)
    .map(n => String(n.port));
  const fallbackPorts =
    stoppingTarget === 'fe1' ? ['8091', '8092'] :
    stoppingTarget === 'fe2' ? ['8092', '8090'] :
    ['8091', '8090'];
  const candidates = [...new Set([...livePeers, ...fallbackPorts])]
    .filter(p => !down.has(adminFrontendIdForPort(p)));
  if (!candidates.length) return;
  adminRedirecting = true;
  window.location.replace(`${window.location.protocol}//${window.location.hostname}:${candidates[0]}/admin${adminQueryWithDown(stoppingTarget)}`);
}

async function refreshStatus() {
  if (Date.now() < adminStatusMutedUntil) return;
  if (adminActionInFlight) return;
  if (adminStatusInFlight) return;
  adminStatusInFlight = true;
  const status = document.getElementById('status');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2500);
  try {
    const r = await fetch('/api/admin/status', {
      signal: controller.signal,
      headers: adminTokenHeaders({})
    });
    if (!r.ok) throw new Error('status ' + r.status);
    const data = await r.json();
    adminStatusFailures = 0;
    adminPublicHost = data.public_host || '';
    adminFrontendNodes = [...(data.frontend_nodes || [])].sort(byNodeId);
    const backendNodes = [...(data.backend_nodes || [])].sort(byNodeId);
    updateAdminSummary(data);
    renderBackend(backendNodes);
    renderFrontend(data.frontend_nodes || []);
    renderTablets(data.tablets || []);
    renderReplicaValidation(data.tablets || []);
    renderRawNodeOptions(backendNodes);
    if (!adminRawLoaded && !adminRawInFlight) refreshRawData(false);
    status.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
  } catch (e) {
    adminStatusFailures += 1;
    const frontendKillGrace = Date.now() - adminLastFrontendKillAt < 7000;
    if (!frontendKillGrace && adminStatusFailures >= 3) {
      status.textContent = 'Failed to refresh admin status';
    }
  } finally {
    clearTimeout(timeout);
    adminStatusInFlight = false;
  }
}
refreshStatus();
adminPollTimer = setInterval(refreshStatus, 2000);
document.addEventListener('click', function(e) {
  const btn = e.target && e.target.closest ? e.target.closest('button[data-kind][data-action][data-target]') : null;
  if (!btn) return;
  e.preventDefault();
  adminControl(btn.dataset.kind, btn.dataset.action, btn.dataset.target);
});
</script>
</body>
</html>
)HTML";

    return HttpResponse::ok(html, "text/html; charset=utf-8");
}

// ---------------------------------------------------------------------------
// Static file server (for CSS/JS if moved out of inline HTML)
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_static(const HttpRequest& req) {
    // Security: prevent directory traversal
    std::string path = req.path.substr(8);  // strip "/static/"
    if (path.find("..") != std::string::npos) return HttpResponse::forbidden();

    std::string full = cfg_.static_dir + "/" + path;
    std::ifstream f(full, std::ios::binary);
    if (!f && path == "logo.png") {
        full = "logo.png";
        f.open(full, std::ios::binary);
    }
    if (!f) return HttpResponse::not_found();

    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());

    // Simple MIME detection
    std::string mime = "application/octet-stream";
    if (path.size()>=4 && path.substr(path.size()-4)==".css")  mime = "text/css";
    else if (path.size()>=3 && path.substr(path.size()-3)==".js")   mime = "application/javascript";
    else if (path.size()>=4 && path.substr(path.size()-4)==".png")  mime = "image/png";
    else if (path.size()>=4 && path.substr(path.size()-4)==".svg")  mime = "image/svg+xml";

    return HttpResponse::ok(content, mime);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SSE handler -- holds connection open and streams new email events (F1).
// Long-lived SSE sockets are handed off to detached SSE threads so the main
// HTTP worker pool remains available for normal requests.
// The SMTP server writes to "notify:{user}" col "latest" when new mail arrives.
// We poll that key every 500ms and push SSE events on change.
// ---------------------------------------------------------------------------
void FEServer::handle_sse(int fd, const HttpRequest& req, const std::string& user) {
    // Send SSE response headers
    std::string headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n"
        "Transfer-Encoding: chunked\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n";
    if (!http_write_all(fd, headers)) return;

    std::string last_notify;
    int keepalive_ticks = 0;
    int auth_ticks = 0;

    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        if (++auth_ticks >= 30) {
            auth_ticks = 0;
            if (get_user(req) != user) break;
        }

        // Check for new email notification
        std::string notify = kv_->get_str("notify:" + user, "latest");
        if (!notify.empty() && notify != last_notify) {
            last_notify = notify;

            std::string uid;
            if (notify.rfind("uid:", 0) == 0) uid = notify.substr(4);

            if (!uid.empty()) {
                std::string from    = kv_->get_str(user + ":mail", "msg:" + uid);
                std::string payload;
                if (from.empty()) {
                    payload = std::string("{\"uid\":\"") + uid
                            + "\",\"from\":\"(new email)\",\"subject\":\"\"}";
                } else {
                    payload = from;
                    if (payload.find("\"uid\"") == std::string::npos && !payload.empty()) {
                        payload = std::string("{\"uid\":\"") + uid + "\"," + payload.substr(1);
                    }
                }
                // Break immediately if client is gone
                if (!send_sse_event(fd, "new_email", payload)) break;
            }
        }

        // Keep-alive comment every 15 seconds (hex-encoded chunk size)
        ++keepalive_ticks;
        if (keepalive_ticks >= 30) {
            keepalive_ticks = 0;
            std::string ping = ": keep-alive\n\n";
            std::string chunk = http_chunk_prefix(ping.size()) + "\r\n" + ping + "\r\n";
            if (!http_write_all(fd, chunk)) break;
        }
    }
}

// ---------------------------------------------------------------------------
// Stub handlers -- filled in by each team member
// Liudawei: handle_inbox, handle_get_email, handle_send_email, handle_delete_email
// Yke:      handle_drive_list, handle_upload, handle_download, handle_rename,
//           handle_move, handle_mkdir, handle_delete_path
// ---------------------------------------------------------------------------










namespace {

std::vector<std::string> chat_split_csv(const std::string& s) {
    std::vector<std::string> out;
    if (s.empty()) return out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        if (!tok.empty()) out.push_back(tok);
    }
    return out;
}

std::string chat_join_csv(const std::vector<std::string>& items) {
    std::string out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i) out += ',';
        out += items[i];
    }
    return out;
}

bool append_csv_item_cas(KVClient* kv, const std::string& row, const std::string& col,
                         const std::string& item, bool prepend, size_t max_items) {
    if (item.empty()) return false;
    for (int attempt = 0; attempt < 16; ++attempt) {
        std::string old_value;
        KVReadStatus st = kv->get_status(row, col, old_value);
        if (st == KVReadStatus::Unavailable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
            continue;
        }

        auto items = chat_split_csv(old_value);
        auto it = std::find(items.begin(), items.end(), item);
        if (it != items.end()) {
            if (!prepend) return true;
            items.erase(it);
            items.insert(items.begin(), item);
        } else if (prepend) {
            items.insert(items.begin(), item);
        } else {
            items.push_back(item);
        }

        if (max_items > 0 && items.size() > max_items) {
            if (prepend) {
                items.resize(max_items);
            } else {
                items.erase(items.begin(), items.begin() + static_cast<std::ptrdiff_t>(items.size() - max_items));
            }
        }

        std::string next = chat_join_csv(items);
        if (st == KVReadStatus::Found) {
            if (next == old_value || kv->cput(row, col, old_value, next)) return true;
        } else {
            // Missing columns cannot be CPUT-created in this KV API.  Put once,
            // then loop back and merge with CPUT so concurrent creators converge.
            (void)kv->put(row, col, next);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }
    return false;
}

const std::vector<std::string>& chat_default_room_names() {
    static const std::vector<std::string> kRooms = {"general", "team", "random"};
    return kRooms;
}

std::string chat_rooms_row() { return "chat:rooms"; }

bool valid_chat_room_name(const std::string& room) {
    if (room.empty() || room.size() > 32) return false;
    for (unsigned char ch : room) {
        if (!(std::isalnum(ch) || ch == '-' || ch == '_')) return false;
    }
    return true;
}

std::vector<std::string> chat_room_names(KVClient* kv) {
    std::vector<std::string> rooms = chat_default_room_names();
    auto custom = chat_split_csv(kv->get_str(chat_rooms_row(), "names"));
    for (const auto& room : custom) {
        if (valid_chat_room_name(room) &&
            std::find(rooms.begin(), rooms.end(), room) == rooms.end()) {
            rooms.push_back(room);
        }
    }
    return rooms;
}

bool valid_chat_room(KVClient* kv, const std::string& room) {
    if (!valid_chat_room_name(room)) return false;
    const auto rooms = chat_room_names(kv);
    return std::find(rooms.begin(), rooms.end(), room) != rooms.end();
}

bool append_chat_room(KVClient* kv, const std::string& room) {
    if (!valid_chat_room_name(room)) return false;
    const auto& defaults = chat_default_room_names();
    if (std::find(defaults.begin(), defaults.end(), room) != defaults.end()) return true;
    const std::string row = chat_rooms_row();
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_rooms = kv->get_str(row, "names");
        auto rooms = chat_split_csv(old_rooms);
        if (std::find(rooms.begin(), rooms.end(), room) != rooms.end()) return true;
        rooms.push_back(room);
        std::sort(rooms.begin(), rooms.end());
        std::string next = chat_join_csv(rooms);
        if (old_rooms.empty()) {
            if (kv->put(row, "names", next)) return true;
        } else {
            if (kv->cput(row, "names", old_rooms, next)) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

std::string chat_room_row(const std::string& room) { return "chat:room:" + room; }
std::string chat_message_row(const std::string& room, const std::string& id) { return "chat:msg:" + room + ":" + id; }

bool append_chat_message_id(KVClient* kv, const std::string& room, const std::string& id) {
    return append_csv_item_cas(kv, chat_room_row(room), "ids", id, false, 200);
}

std::string dm_canon_peer(const std::string& a, const std::string& b) {
    return a < b ? (a + "|" + b) : (b + "|" + a);
}

std::string chat_dm_row(const std::string& user) { return "chat:dm:user:" + user; }
std::string chat_dm_thread_row(const std::string& canon) { return "chat:dm:thread:" + canon; }
std::string chat_dm_message_row(const std::string& canon, const std::string& id) { return "chat:dm:msg:" + canon + ":" + id; }

bool append_dm_peer(KVClient* kv, const std::string& user, const std::string& peer) {
    return append_csv_item_cas(kv, chat_dm_row(user), "peers", peer, true, 100);
}

bool append_dm_message_id(KVClient* kv, const std::string& canon, const std::string& id) {
    return append_csv_item_cas(kv, chat_dm_thread_row(canon), "ids", id, false, 200);
}

std::string chat_timestamp_now() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[64];
    std::strftime(buf, sizeof(buf), "%b %d, %Y %H:%M UTC", &tm);
    return buf;
}

}  // namespace

namespace {

constexpr const char* kDriveRootCol = "root";

std::string drive_obj_row(const std::string& uid) { return "drive:obj:" + uid; }
std::string drive_dir_row(const std::string& uid) { return "drive:dir:" + uid; }
std::string drive_file_row(const std::string& uid) { return "drive:file:" + uid; }
std::string drive_user_row(const std::string& user) { return user + ":drive"; }
std::string drive_upload_row(const std::string& upload_id) { return "drive:upload:" + upload_id; }
std::string drive_upload_lookup_row(const std::string& user) { return user + ":drive:upload_lookup"; }

constexpr size_t kDriveChunkSize = 10ull * 1024ull * 1024ull;  // 10MB demo path fits in one KV write

std::string drive_file_chunk_col(size_t idx) { return "chunk:" + std::to_string(idx); }
std::string upload_received_col(size_t idx) { return "received:" + std::to_string(idx); }

std::string get_file_bytes(KVClient* kv, const std::string& uid);
std::vector<std::string> split_csv(const std::string& s);
std::string join_csv(const std::vector<std::string>& items);
bool ensure_drive_root(KVClient* kv, const std::string& user, std::string& root_uid);

std::string drive_quota_col() { return "quota_bytes"; }
std::string drive_used_col() { return "used_bytes"; }
constexpr size_t kMaxDriveFileBytes = 1024ull * 1024ull * 1024ull;
constexpr size_t kDefaultDriveQuotaBytes = 1024ull * 1024ull * 1024ull;

long long elapsed_ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start).count();
}

size_t parse_size_t_or(const std::string& s, size_t fallback) {
    if (s.empty()) return fallback;
    try { return static_cast<size_t>(std::stoull(s)); } catch (...) { return fallback; }
}

std::string upload_lookup_col(const std::string& fingerprint) {
    return "fp:" + fingerprint;
}

std::string json_index_array(const std::vector<std::string>& indexes) {
    std::string out = "[";
    bool first = true;
    for (const auto& idx : indexes) {
        if (idx.empty()) continue;
        if (!first) out += ",";
        first = false;
        out += idx;
    }
    out += "]";
    return out;
}

std::vector<std::string> upload_received_indexes(KVClient* kv, const std::string& row, size_t total_chunks) {
    std::unordered_set<size_t> seen;
    for (const auto& tok : split_csv(kv->get_str(row, "received"))) {
        size_t idx = parse_size_t_or(tok, total_chunks);
        if (idx < total_chunks) seen.insert(idx);
    }
    for (size_t idx = 0; idx < total_chunks; ++idx) {
        if (!kv->get_str(row, upload_received_col(idx)).empty()) seen.insert(idx);
    }
    std::vector<size_t> ordered(seen.begin(), seen.end());
    std::sort(ordered.begin(), ordered.end());
    std::vector<std::string> out;
    out.reserve(ordered.size());
    for (size_t idx : ordered) out.push_back(std::to_string(idx));
    return out;
}

bool upload_received_all(KVClient* kv, const std::string& row, size_t total_chunks) {
    std::unordered_set<size_t> seen;
    for (const auto& tok : upload_received_indexes(kv, row, total_chunks)) {
        size_t idx = parse_size_t_or(tok, total_chunks);
        if (idx < total_chunks) seen.insert(idx);
    }
    return seen.size() == total_chunks;
}

std::string upload_status_json(KVClient* kv, const std::string& upload_id) {
    const std::string row = drive_upload_row(upload_id);
    std::string status = kv->get_str(row, "status");
    if (status.empty()) status = "uploading";
    std::string total_size = kv->get_str(row, "total_size");
    std::string chunk_size = kv->get_str(row, "chunk_size");
    std::string total_chunks = kv->get_str(row, "total_chunks");
    std::string file_uid = kv->get_str(row, "file_uid");
    size_t chunks_n = parse_size_t_or(total_chunks, 0);
    return std::string("{\"ok\":true,\"upload_id\":") + json_str(upload_id) +
           ",\"status\":" + json_str(status) +
           ",\"file_uid\":" + json_str(file_uid) +
           ",\"total_size\":" + (total_size.empty() ? "0" : total_size) +
           ",\"chunk_size\":" + (chunk_size.empty() ? "0" : chunk_size) +
           ",\"total_chunks\":" + (total_chunks.empty() ? "0" : total_chunks) +
           ",\"received\":" + json_index_array(upload_received_indexes(kv, row, chunks_n)) + "}";
}

size_t user_drive_quota_bytes(KVClient* kv, const std::string& user) {
    return parse_size_t_or(kv->get_str(drive_user_row(user), drive_quota_col()), kDefaultDriveQuotaBytes);
}

size_t subtree_file_bytes_inner(KVClient* kv, const std::string& uid,
                                std::unordered_set<std::string>& seen,
                                size_t depth) {
    if (uid.empty() || depth > 4096 || !seen.insert(uid).second) return 0;
    std::string type = kv->get_str(drive_obj_row(uid), "type");
    if (type == "file") {
        std::string size_str = kv->get_str(drive_obj_row(uid), "size");
        if (!size_str.empty()) {
            return parse_size_t_or(size_str, 0);
        }
        return get_file_bytes(kv, uid).size();
    }
    size_t total = 0;
    for (const auto& child : split_csv(kv->get_str(drive_dir_row(uid), "children"))) {
        total += subtree_file_bytes_inner(kv, child, seen, depth + 1);
    }
    return total;
}

size_t subtree_file_bytes(KVClient* kv, const std::string& uid) {
    std::unordered_set<std::string> seen;
    return subtree_file_bytes_inner(kv, uid, seen, 0);
}

size_t user_drive_used_bytes(KVClient* kv, const std::string& user) {
    const std::string row = drive_user_row(user);
    std::string cached = kv->get_str(row, drive_used_col());
    if (!cached.empty()) return parse_size_t_or(cached, 0);

    std::string root_uid;
    if (!ensure_drive_root(kv, user, root_uid)) return 0;
    size_t scanned = subtree_file_bytes(kv, root_uid);
    kv->put(row, drive_used_col(), std::to_string(scanned));
    return scanned;
}

bool adjust_user_drive_used_bytes(KVClient* kv, const std::string& user, long long delta) {
    if (delta == 0) return true;
    const std::string row = drive_user_row(user);
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_raw = kv->get_str(row, drive_used_col());
        if (old_raw.empty()) {
            user_drive_used_bytes(kv, user);
            old_raw = kv->get_str(row, drive_used_col());
        }
        size_t old_value = parse_size_t_or(old_raw, 0);
        size_t next_value = old_value;
        if (delta > 0) {
            const size_t add = static_cast<size_t>(delta);
            next_value = old_value > static_cast<size_t>(-1) - add ? static_cast<size_t>(-1) : old_value + add;
        } else {
            const size_t sub = static_cast<size_t>(-delta);
            next_value = sub >= old_value ? 0 : old_value - sub;
        }
        std::string next_raw = std::to_string(next_value);
        if (old_raw == next_raw || kv->cput(row, drive_used_col(), old_raw, next_raw)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

size_t drive_chunk_count_for_size(size_t nbytes) {
    return (nbytes + kDriveChunkSize - 1) / kDriveChunkSize;
}

bool put_file_chunks(KVClient* kv, const std::string& uid, const std::string& data) {
    const std::string row = drive_file_row(uid);
    const size_t chunks = drive_chunk_count_for_size(data.size());
    if (!kv->put(row, "chunks", std::to_string(chunks))) return false;
    for (size_t i = 0; i < chunks; ++i) {
        size_t off = i * kDriveChunkSize;
        size_t len = std::min(kDriveChunkSize, data.size() - off);
        if (!kv->put(row, drive_file_chunk_col(i), data.substr(off, len))) {
            for (size_t j = 0; j < i; ++j) kv->del(row, drive_file_chunk_col(j));
            kv->del(row, "chunks");
            return false;
        }
    }
    return true;
}

std::string get_file_bytes(KVClient* kv, const std::string& uid) {
    const std::string row = drive_file_row(uid);
    std::string legacy = kv->get_str(row, "bytes");
    if (!legacy.empty()) return legacy;

    std::string chunks_s = kv->get_str(row, "chunks");
    if (chunks_s.empty()) return "";

    size_t chunks = 0;
    try {
        chunks = static_cast<size_t>(std::stoull(chunks_s));
    } catch (...) {
        return "";
    }

    std::string out;
    for (size_t i = 0; i < chunks; ++i) {
        out += kv->get_str(row, drive_file_chunk_col(i));
    }
    return out;
}

void delete_file_bytes(KVClient* kv, const std::string& uid) {
    const std::string row = drive_file_row(uid);
    kv->del(row, "bytes");
    std::string chunks_s = kv->get_str(row, "chunks");
    if (chunks_s.empty()) return;
    size_t chunks = 0;
    try {
        chunks = static_cast<size_t>(std::stoull(chunks_s));
    } catch (...) {
        kv->del(row, "chunks");
        return;
    }
    for (size_t i = 0; i < chunks; ++i) {
        kv->del(row, drive_file_chunk_col(i));
    }
    kv->del(row, "chunks");
}

std::string drive_new_uid() {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
    static std::atomic<unsigned long long> seq{0};
    return std::to_string(ns) + "_" + std::to_string(seq.fetch_add(1));
}

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    if (s.empty()) return out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        if (!tok.empty()) out.push_back(tok);
    }
    return out;
}

std::string join_csv(const std::vector<std::string>& items) {
    std::string out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i) out += ',';
        out += items[i];
    }
    return out;
}

std::string parent_path(const std::string& path) {
    if (path.empty() || path == "/") return "/";
    size_t end = path.find_last_not_of('/');
    if (end == std::string::npos || end == 0) return "/";
    size_t slash = path.rfind('/', end);
    if (slash == std::string::npos || slash == 0) return "/";
    return path.substr(0, slash);
}

std::string path_basename(const std::string& path) {
    if (path.empty() || path == "/") return "";
    size_t end = path.find_last_not_of('/');
    if (end == std::string::npos) return "";
    size_t slash = path.rfind('/', end);
    return path.substr(slash == std::string::npos ? 0 : slash + 1, end - (slash == std::string::npos ? 0 : slash + 1) + 1);
}

std::string join_path(const std::string& parent, const std::string& name) {
    if (parent.empty() || parent == "/") return "/" + name;
    return parent + "/" + name;
}

bool valid_component(const std::string& name) {
    if (name.empty() || name.find('/') != std::string::npos || name == "." || name == "..") return false;
    for (unsigned char c : name) {
        if (c < 0x20 || c == 0x7F) return false;
    }
    return true;
}

std::string content_disposition_attachment(const std::string& name) {
    std::string escaped;
    for (unsigned char c : name) {
        if (c == '\r' || c == '\n') continue;
        if (c < 0x20 || c == 0x7F) {
            escaped.push_back('_');
            continue;
        }
        if (c == '"' || c == '\\') escaped.push_back('\\');
        escaped.push_back(static_cast<char>(c));
    }
    if (escaped.empty()) escaped = "download";
    return "attachment; filename=\"" + escaped + "\"";
}

bool ensure_drive_root(KVClient* kv, const std::string& user, std::string& root_uid) {
    root_uid = kv->get_str(drive_user_row(user), kDriveRootCol);
    if (!root_uid.empty()) return true;

    root_uid = "root:" + user;
    if (!kv->put(drive_user_row(user), kDriveRootCol, root_uid)) return false;
    if (!kv->put(drive_obj_row(root_uid), "type", "folder")) return false;
    if (!kv->put(drive_obj_row(root_uid), "name", "/")) return false;
    if (!kv->put(drive_obj_row(root_uid), "parent", "")) return false;
    if (!kv->put(drive_obj_row(root_uid), "owner", user)) return false;
    if (!kv->put(drive_dir_row(root_uid), "children", "")) return false;
    return true;
}

bool resolve_path(KVClient* kv, const std::string& user, const std::string& path,
                  std::string& uid_out, std::string* type_out = nullptr) {
    std::string root_uid;
    if (!ensure_drive_root(kv, user, root_uid)) return false;
    if (path.empty() || path == "/") {
        uid_out = root_uid;
        if (type_out) *type_out = "folder";
        return true;
    }

    std::string cur = root_uid;
    std::string norm = path;
    if (norm.front() == '/') norm.erase(norm.begin());
    std::istringstream ss(norm);
    std::string part;
    while (std::getline(ss, part, '/')) {
        if (part.empty()) continue;
        std::string children = kv->get_str(drive_dir_row(cur), "children");
        bool found = false;
        for (const auto& child : split_csv(children)) {
            if (kv->get_str(drive_obj_row(child), "name") == part) {
                cur = child;
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    uid_out = cur;
    if (type_out) *type_out = kv->get_str(drive_obj_row(cur), "type");
    return true;
}

bool resolve_move_destination(KVClient* kv, const std::string& user,
                              const std::string& src_path,
                              const std::string& requested_dst,
                              std::string& dst_path_out,
                              std::string& dst_uid_out,
                              std::string& dst_type_out) {
    std::vector<std::string> candidates;
    auto add_candidate = [&](const std::string& p) {
        if (p.empty()) return;
        if (std::find(candidates.begin(), candidates.end(), p) == candidates.end()) {
            candidates.push_back(p);
        }
    };

    add_candidate(requested_dst);
    if (!requested_dst.empty() && requested_dst.front() != '/') {
        add_candidate("/" + requested_dst);
        add_candidate(join_path(parent_path(src_path), requested_dst));

        std::string cur = parent_path(src_path);
        while (!cur.empty() && cur != "/") {
            if (path_basename(cur) == requested_dst) add_candidate(cur);
            cur = parent_path(cur);
        }
    }

    for (const auto& candidate : candidates) {
        std::string uid, type;
        if (resolve_path(kv, user, candidate, uid, &type) && type == "folder") {
            dst_path_out = candidate;
            dst_uid_out = uid;
            dst_type_out = type;
            return true;
        }
    }
    return false;
}

bool append_child(KVClient* kv, const std::string& dir_uid, const std::string& child_uid) {
    return append_csv_item_cas(kv, drive_dir_row(dir_uid), "children", child_uid, false, 0);
}

bool remove_child(KVClient* kv, const std::string& dir_uid, const std::string& child_uid,
                  bool require_present = false) {
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_children = kv->get_str(drive_dir_row(dir_uid), "children");
        if (old_children.empty()) return !require_present;
        auto kids = split_csv(old_children);
        const size_t before = kids.size();
        kids.erase(std::remove(kids.begin(), kids.end(), child_uid), kids.end());
        if (kids.size() == before) return !require_present;
        std::string next = join_csv(kids);
        if (kv->cput(drive_dir_row(dir_uid), "children", old_children, next)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

bool child_name_exists(KVClient* kv, const std::string& dir_uid, const std::string& wanted, const std::string& skip_uid = "") {
    std::string children = kv->get_str(drive_dir_row(dir_uid), "children");
    for (const auto& child : split_csv(children)) {
        if (!skip_uid.empty() && child == skip_uid) continue;
        if (kv->get_str(drive_obj_row(child), "name") == wanted) return true;
    }
    return false;
}

bool collect_subtree_inner(KVClient* kv, const std::string& uid, std::vector<std::string>& out,
                           std::unordered_set<std::string>& seen, size_t depth) {
    if (uid.empty() || depth > 4096 || !seen.insert(uid).second) return true;
    out.push_back(uid);
    if (kv->get_str(drive_obj_row(uid), "type") != "folder") return true;
    for (const auto& child : split_csv(kv->get_str(drive_dir_row(uid), "children"))) {
        if (!collect_subtree_inner(kv, child, out, seen, depth + 1)) return false;
    }
    return true;
}

bool collect_subtree(KVClient* kv, const std::string& uid, std::vector<std::string>& out) {
    std::unordered_set<std::string> seen;
    return collect_subtree_inner(kv, uid, out, seen, 0);
}

bool is_descendant_of(KVClient* kv, const std::string& possible_descendant, const std::string& ancestor) {
    std::string cur = possible_descendant;
    std::unordered_set<std::string> seen;
    size_t depth = 0;
    while (!cur.empty()) {
        if (cur == ancestor) return true;
        if (++depth > 4096 || !seen.insert(cur).second) {
            std::cerr << "[drive] parent cycle detected while checking move\n";
            return true;
        }
        cur = kv->get_str(drive_obj_row(cur), "parent");
    }
    return false;
}

struct DriveListItem {
    std::string uid;
    std::string type;
    std::string name;
    std::string path;
    std::string size;
    std::string created_at;
    std::string updated_at;
};

DriveListItem read_drive_list_item(KVClient* kv, const std::string& uid, const std::string& folder_path) {
    DriveListItem item;
    item.uid = uid;
    item.type = kv->get_str(drive_obj_row(uid), "type");
    item.name = kv->get_str(drive_obj_row(uid), "name");
    item.path = join_path(folder_path, item.name);
    item.size = kv->get_str(drive_obj_row(uid), "size");
    item.created_at = kv->get_str(drive_obj_row(uid), "created_at");
    item.updated_at = kv->get_str(drive_obj_row(uid), "updated_at");
    return item;
}

std::string json_drive_item(const DriveListItem& item) {
    std::string json = "{";
    json += "\"uid\":" + json_str(item.uid);
    json += ",\"name\":" + json_str(item.name);
    json += ",\"type\":" + json_str(item.type);
    json += ",\"path\":" + json_str(item.path);
    if (item.type == "file" && !item.size.empty()) json += ",\"size\":" + json_str(item.size);
    if (!item.created_at.empty()) json += ",\"created_at\":" + json_str(item.created_at);
    if (!item.updated_at.empty()) json += ",\"updated_at\":" + json_str(item.updated_at);
    json += "}";
    return json;
}

}  // namespace

HttpResponse FEServer::handle_chat_rooms(const HttpRequest&, const std::string&) {
    std::string rooms = "[";
    std::string counts = "{";
    bool first = true;
    bool first_count = true;
    for (const auto& room : chat_room_names(kv_.get())) {
        if (!first) rooms += ",";
        rooms += "{\"name\":" + json_str(room) + "}";
        first = false;
        auto ids = chat_split_csv(kv_->get_str(chat_room_row(room), "ids"));
        if (!first_count) counts += ",";
        counts += json_str(room) + ":" + std::to_string(ids.size());
        first_count = false;
    }
    rooms += "]";
    counts += "}";
    return HttpResponse::json(std::string("{\"ok\":true,\"rooms\":") + rooms + ",\"counts\":" + counts + "}");
}

HttpResponse FEServer::handle_chat_create_room(const HttpRequest& req, const std::string&) {
    auto params = parse_urlencoded(req.body);
    std::string room = params.count("room") ? params["room"] : "";
    if (!valid_chat_room_name(room)) {
        return HttpResponse::json(R"({"ok":false,"error":"Use 1-32 letters, numbers, dashes, or underscores"})");
    }
    if (!append_chat_room(kv_.get(), room)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to create room"})");
    }
    return HttpResponse::json(std::string("{\"ok\":true,\"room\":") + json_str(room) + "}");
}

HttpResponse FEServer::handle_chat_messages(const HttpRequest& req, const std::string&) {
    std::string room = req.query_params.count("room") ? req.query_params.at("room") : "general";
    if (!valid_chat_room(kv_.get(), room)) {
        return HttpResponse::json(R"({"ok":false,"error":"unknown room"})");
    }
    auto ids = chat_split_csv(kv_->get_str(chat_room_row(room), "ids"));
    std::string msgs = "[";
    bool first = true;
    for (const auto& id : ids) {
        const std::string row = chat_message_row(room, id);
        std::string from = kv_->get_str(row, "from");
        std::string textv = kv_->get_str(row, "text");
        std::string timev = kv_->get_str(row, "time");
        if (from.empty() && textv.empty()) continue;
        if (!first) msgs += ",";
        msgs += "{";
        msgs += "\"id\":" + json_str(id);
        msgs += ",\"from\":" + json_str(from);
        msgs += ",\"text\":" + json_str(textv);
        msgs += ",\"time\":" + json_str(timev);
        msgs += "}";
        first = false;
    }
    msgs += "]";
    return HttpResponse::json(std::string("{\"ok\":true,\"room\":") + json_str(room) + ",\"messages\":" + msgs + "}");
}

HttpResponse FEServer::handle_chat_send(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string room = params.count("room") ? params["room"] : "general";
    std::string textv = params.count("text") ? params["text"] : "";
    if (!valid_chat_room(kv_.get(), room)) return HttpResponse::json(R"({"ok":false,"error":"unknown room"})");
    if (textv.empty()) return HttpResponse::json(R"({"ok":false,"error":"message cannot be empty"})");
    if (textv.size() > 2000) return HttpResponse::json(R"({"ok":false,"error":"message too long"})");

    const std::string id = drive_new_uid();
    const std::string row = chat_message_row(room, id);
    const std::string timev = chat_timestamp_now();
    if (!kv_->put(row, "from", user) || !kv_->put(row, "text", textv) || !kv_->put(row, "time", timev)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to store message"})");
    }
    if (!append_chat_message_id(kv_.get(), room, id)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to update room"})");
    }
    return HttpResponse::json(std::string("{\"ok\":true,\"message\":{")
                              + "\"id\":" + json_str(id)
                              + ",\"room\":" + json_str(room)
                              + ",\"from\":" + json_str(user)
                              + ",\"text\":" + json_str(textv)
                              + ",\"time\":" + json_str(timev)
                              + "}}");
}

HttpResponse FEServer::handle_chat_dms(const HttpRequest&, const std::string& user) {
    auto peers = chat_split_csv(kv_->get_str(chat_dm_row(user), "peers"));
    std::string out = "[";
    bool first = true;
    for (const auto& peer : peers) {
        if (peer.empty()) continue;
        if (!first) out += ",";
        out += json_str(peer);
        first = false;
    }
    out += "]";
    return HttpResponse::json(std::string("{\"ok\":true,\"peers\":") + out + "}");
}

HttpResponse FEServer::handle_chat_dm_messages(const HttpRequest& req, const std::string& user) {
    std::string peer = req.query_params.count("peer") ? req.query_params.at("peer") : "";
    if (peer.empty()) return HttpResponse::json(R"({"ok":false,"error":"peer required"})");
    if (peer == user) return HttpResponse::json(R"({"ok":false,"error":"choose another user"})");
    const std::string canon = dm_canon_peer(user, peer);
    auto ids = chat_split_csv(kv_->get_str(chat_dm_thread_row(canon), "ids"));
    std::string msgs = "[";
    bool first = true;
    for (const auto& id : ids) {
        const std::string row = chat_dm_message_row(canon, id);
        std::string from = kv_->get_str(row, "from");
        std::string to = kv_->get_str(row, "to");
        std::string textv = kv_->get_str(row, "text");
        std::string timev = kv_->get_str(row, "time");
        if (from.empty() && textv.empty()) continue;
        if (!first) msgs += ",";
        msgs += "{";
        msgs += "\"id\":" + json_str(id);
        msgs += ",\"from\":" + json_str(from);
        msgs += ",\"to\":" + json_str(to);
        msgs += ",\"text\":" + json_str(textv);
        msgs += ",\"time\":" + json_str(timev);
        msgs += "}";
        first = false;
    }
    msgs += "]";
    return HttpResponse::json(std::string("{\"ok\":true,\"peer\":") + json_str(peer) + ",\"messages\":" + msgs + "}");
}

HttpResponse FEServer::handle_chat_dm_send(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string peer = params.count("peer") ? params["peer"] : "";
    std::string textv = params.count("text") ? params["text"] : "";
    if (peer.empty()) return HttpResponse::json(R"({"ok":false,"error":"peer required"})");
    if (peer == user) return HttpResponse::json(R"({"ok":false,"error":"choose another user"})");
    if (textv.empty()) return HttpResponse::json(R"({"ok":false,"error":"message cannot be empty"})");
    if (textv.size() > 2000) return HttpResponse::json(R"({"ok":false,"error":"message too long"})");

    const std::string canon = dm_canon_peer(user, peer);
    const std::string id = drive_new_uid();
    const std::string row = chat_dm_message_row(canon, id);
    const std::string timev = chat_timestamp_now();
    if (!kv_->put(row, "from", user) || !kv_->put(row, "to", peer) || !kv_->put(row, "text", textv) || !kv_->put(row, "time", timev)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to store message"})");
    }
    if (!append_dm_message_id(kv_.get(), canon, id) || !append_dm_peer(kv_.get(), user, peer) || !append_dm_peer(kv_.get(), peer, user)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to update conversation"})");
    }
    return HttpResponse::json(std::string("{\"ok\":true,\"message\":{")
                              + "\"id\":" + json_str(id)
                              + ",\"peer\":" + json_str(peer)
                              + ",\"from\":" + json_str(user)
                              + ",\"to\":" + json_str(peer)
                              + ",\"text\":" + json_str(textv)
                              + ",\"time\":" + json_str(timev)
                              + "}}");
}

HttpResponse FEServer::handle_drive_list(const HttpRequest& req, const std::string& user) {
    std::string folder_path = req.param("path");
    if (folder_path.empty()) folder_path = "/";

    std::string folder_uid, type;
    if (!resolve_path(kv_.get(), user, folder_path, folder_uid, &type) || type != "folder") {
        return HttpResponse::json(R"({"ok":false,"error":"folder not found"})");
    }

    std::string children = kv_->get_str(drive_dir_row(folder_uid), "children");
    std::vector<std::string> ids = split_csv(children);
    std::vector<DriveListItem> items;
    items.reserve(ids.size());
    for (const auto& uid : ids) {
        DriveListItem item = read_drive_list_item(kv_.get(), uid, folder_path);
        if (!item.type.empty() && !item.name.empty()) items.push_back(item);
    }
    std::sort(items.begin(), items.end(), [](const DriveListItem& a, const DriveListItem& b) {
        if (a.type != b.type) return a.type == "folder";
        return a.name < b.name;
    });

    std::string json = "[";
    bool first = true;
    for (const auto& item : items) {
        if (!first) json += ',';
        first = false;
        json += json_drive_item(item);
    }
    json += "]";
    return HttpResponse::json("{\"ok\":true,\"items\":" + json + "}");
}

HttpResponse FEServer::handle_upload(const HttpRequest& req, const std::string& user) {
    auto started = std::chrono::steady_clock::now();
    if (!req.is_multipart()) {
        return HttpResponse::json(R"({"ok":false,"error":"expected multipart upload"})");
    }

    auto parse_started = std::chrono::steady_clock::now();
    auto parts = parse_multipart(req.body, req.header("content-type"));
    long long parse_ms = elapsed_ms_since(parse_started);
    std::string parent_path = "/";
    MultipartPart file_part;
    bool have_file = false;
    for (const auto& part : parts) {
        if (part.name == "path") parent_path = part.data;
        else if (part.name == "file") { file_part = part; have_file = true; }
    }
    if (!have_file || file_part.filename.empty()) {
        return HttpResponse::json(R"({"ok":false,"error":"missing file"})");
    }
    if (file_part.data.size() > kMaxDriveFileBytes) {
        return HttpResponse::json(R"({"ok":false,"error":"file exceeds 1 GB limit"})");
    }

    size_t used_bytes = user_drive_used_bytes(kv_.get(), user);
    size_t limit_bytes = user_drive_quota_bytes(kv_.get(), user);
    if (used_bytes + file_part.data.size() > limit_bytes) {
        size_t over = used_bytes + file_part.data.size() - limit_bytes;
        return HttpResponse::json(std::string("{\"ok\":false,\"error\":\"quota exceeded by ") +
                                  std::to_string(over) + " bytes\"}");
    }

    std::string parent_uid, type;
    if (!resolve_path(kv_.get(), user, parent_path, parent_uid, &type) || type != "folder") {
        return HttpResponse::json(R"({"ok":false,"error":"target folder not found"})");
    }
    if (!valid_component(file_part.filename)) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid filename"})");
    }
    if (child_name_exists(kv_.get(), parent_uid, file_part.filename)) {
        return HttpResponse::json(R"({"ok":false,"error":"name already exists"})");
    }

    std::string uid = drive_new_uid();
    std::string now = chat_timestamp_now();
    std::string fail_reason;
    auto kv_started = std::chrono::steady_clock::now();
    if (!kv_->put(drive_obj_row(uid), "type", "file")) fail_reason = "failed to store file type";
    else if (!kv_->put(drive_obj_row(uid), "name", file_part.filename)) fail_reason = "failed to store file name";
    else if (!kv_->put(drive_obj_row(uid), "parent", parent_uid)) fail_reason = "failed to store parent folder";
    else if (!kv_->put(drive_obj_row(uid), "owner", user)) fail_reason = "failed to store file owner";
    else if (!kv_->put(drive_obj_row(uid), "size", std::to_string(file_part.data.size()))) fail_reason = "failed to store file size";
    else if (!kv_->put(drive_obj_row(uid), "created_at", now)) fail_reason = "failed to store file timestamp";
    else if (!kv_->put(drive_obj_row(uid), "updated_at", now)) fail_reason = "failed to store file timestamp";
    else if (!put_file_chunks(kv_.get(), uid, file_part.data)) fail_reason = "failed to store file data";
    else if (!append_child(kv_.get(), parent_uid, uid)) fail_reason = "failed to update folder listing";
    long long kv_ms = elapsed_ms_since(kv_started);

    if (!fail_reason.empty()) {
        kv_->del(drive_obj_row(uid), "type");
        kv_->del(drive_obj_row(uid), "name");
        kv_->del(drive_obj_row(uid), "parent");
        kv_->del(drive_obj_row(uid), "owner");
        kv_->del(drive_obj_row(uid), "size");
        kv_->del(drive_obj_row(uid), "created_at");
        kv_->del(drive_obj_row(uid), "updated_at");
        delete_file_bytes(kv_.get(), uid);
        return HttpResponse::json("{\"ok\":false,\"error\":" + json_str(fail_reason) + "}");
    }

    std::string path = join_path(parent_path, file_part.filename);
    if (!adjust_user_drive_used_bytes(kv_.get(), user, static_cast<long long>(file_part.data.size()))) {
        std::cerr << "[drive] failed to update cached quota usage for " << user << "\n";
    }
    std::cerr << "[drive] legacy upload user=" << user
              << " file=" << file_part.filename
              << " bytes=" << file_part.data.size()
              << " parse_ms=" << parse_ms
              << " kv_ms=" << kv_ms
              << " total_ms=" << elapsed_ms_since(started) << "\n";
    return HttpResponse::json("{\"ok\":true,\"uid\":" + json_str(uid) + ",\"path\":" + json_str(path) + "}");
}

HttpResponse FEServer::handle_upload_start(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string filename = params["filename"];
    std::string parent_path = params["path"];
    std::string fingerprint = params["fingerprint"];
    size_t total_size = parse_size_t_or(params["total_size"], 0);
    if (parent_path.empty()) parent_path = "/";
    if (!valid_component(filename) || total_size == 0 || total_size > kMaxDriveFileBytes) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid upload request"})");
    }
    if (fingerprint.size() > 256) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid upload fingerprint"})");
    }

    if (!fingerprint.empty()) {
        std::string existing_id = kv_->get_str(drive_upload_lookup_row(user), upload_lookup_col(fingerprint));
        if (!existing_id.empty()) {
            std::string row = drive_upload_row(existing_id);
            std::string status = kv_->get_str(row, "status");
            if (kv_->get_str(row, "owner") == user &&
                kv_->get_str(row, "filename") == filename &&
                kv_->get_str(row, "path") == parent_path &&
                parse_size_t_or(kv_->get_str(row, "total_size"), 0) == total_size &&
                status != "completed" && status != "cancelled") {
                return HttpResponse::json(upload_status_json(kv_.get(), existing_id));
            }
        }
    }

    size_t used_bytes = user_drive_used_bytes(kv_.get(), user);
    size_t limit_bytes = user_drive_quota_bytes(kv_.get(), user);
    if (used_bytes + total_size > limit_bytes) {
        size_t over = used_bytes + total_size - limit_bytes;
        return HttpResponse::json(std::string("{\"ok\":false,\"error\":\"quota exceeded by ") +
                                  std::to_string(over) + " bytes\"}");
    }

    std::string parent_uid, type;
    if (!resolve_path(kv_.get(), user, parent_path, parent_uid, &type) || type != "folder") {
        return HttpResponse::json(R"({"ok":false,"error":"target folder not found"})");
    }
    if (child_name_exists(kv_.get(), parent_uid, filename)) {
        return HttpResponse::json(R"({"ok":false,"error":"name already exists"})");
    }

    std::string upload_id = drive_new_uid();
    std::string file_uid = drive_new_uid();
    size_t total_chunks = drive_chunk_count_for_size(total_size);
    std::string row = drive_upload_row(upload_id);
    std::string now = chat_timestamp_now();
    bool ok = kv_->put(row, "owner", user) &&
              kv_->put(row, "file_uid", file_uid) &&
              kv_->put(row, "filename", filename) &&
              kv_->put(row, "path", parent_path) &&
              kv_->put(row, "parent_uid", parent_uid) &&
              kv_->put(row, "total_size", std::to_string(total_size)) &&
              kv_->put(row, "chunk_size", std::to_string(kDriveChunkSize)) &&
              kv_->put(row, "total_chunks", std::to_string(total_chunks)) &&
              kv_->put(row, "received", "") &&
              kv_->put(row, "status", "uploading") &&
              kv_->put(row, "created_at", now) &&
              kv_->put(row, "updated_at", now);
    if (ok && !fingerprint.empty()) {
        ok = kv_->put(row, "fingerprint", fingerprint) &&
             kv_->put(drive_upload_lookup_row(user), upload_lookup_col(fingerprint), upload_id);
    }
    if (!ok) return HttpResponse::json(R"({"ok":false,"error":"failed to create upload session"})");

    std::cerr << "[drive] upload start user=" << user
              << " upload_id=" << upload_id
              << " file=" << filename
              << " bytes=" << total_size
              << " chunks=" << total_chunks << "\n";
    return HttpResponse::json(upload_status_json(kv_.get(), upload_id));
}

HttpResponse FEServer::handle_upload_chunk(const HttpRequest& req, const std::string& user) {
    auto started = std::chrono::steady_clock::now();
    if (!req.is_multipart()) {
        return HttpResponse::json(R"({"ok":false,"error":"expected multipart chunk"})");
    }
    auto parts = parse_multipart(req.body, req.header("content-type"));
    std::string upload_id;
    size_t index = static_cast<size_t>(-1);
    MultipartPart chunk_part;
    bool have_chunk = false;
    for (const auto& part : parts) {
        if (part.name == "upload_id") upload_id = part.data;
        else if (part.name == "index") index = parse_size_t_or(part.data, static_cast<size_t>(-1));
        else if (part.name == "chunk") { chunk_part = part; have_chunk = true; }
    }
    if (upload_id.empty() || !have_chunk || index == static_cast<size_t>(-1)) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid chunk request"})");
    }
    std::string row = drive_upload_row(upload_id);
    if (kv_->get_str(row, "owner") != user || kv_->get_str(row, "status") == "completed") {
        return HttpResponse::json(R"({"ok":false,"error":"upload session not found"})");
    }
    size_t total_chunks = parse_size_t_or(kv_->get_str(row, "total_chunks"), 0);
    size_t total_size = parse_size_t_or(kv_->get_str(row, "total_size"), 0);
    std::string file_uid = kv_->get_str(row, "file_uid");
    if (file_uid.empty() || index >= total_chunks || total_chunks == 0) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid upload session"})");
    }
    size_t expected_max = kDriveChunkSize;
    if (index + 1 == total_chunks) {
        size_t rem = total_size % kDriveChunkSize;
        expected_max = rem == 0 ? kDriveChunkSize : rem;
    }
    if (chunk_part.data.empty() || chunk_part.data.size() > expected_max) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid chunk size"})");
    }

    auto kv_started = std::chrono::steady_clock::now();
    if (!kv_->put(drive_file_row(file_uid), drive_file_chunk_col(index), chunk_part.data)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to store chunk"})");
    }
    long long kv_ms = elapsed_ms_since(kv_started);
    if (!kv_->put(row, upload_received_col(index), "1")) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to update upload session"})");
    }
    kv_->put(row, "updated_at", chat_timestamp_now());
    std::cerr << "[drive] upload chunk user=" << user
              << " upload_id=" << upload_id
              << " index=" << index
              << " bytes=" << chunk_part.data.size()
              << " kv_ms=" << kv_ms
              << " total_ms=" << elapsed_ms_since(started) << "\n";
    return HttpResponse::json(upload_status_json(kv_.get(), upload_id));
}

HttpResponse FEServer::handle_upload_status(const HttpRequest& req, const std::string& user) {
    std::string upload_id = req.param("id");
    if (upload_id.empty() || kv_->get_str(drive_upload_row(upload_id), "owner") != user) {
        return HttpResponse::json(R"({"ok":false,"error":"upload session not found"})");
    }
    return HttpResponse::json(upload_status_json(kv_.get(), upload_id));
}

HttpResponse FEServer::handle_upload_finish(const HttpRequest& req, const std::string& user) {
    auto started = std::chrono::steady_clock::now();
    auto params = parse_urlencoded(req.body);
    std::string upload_id = params["upload_id"];
    std::string row = drive_upload_row(upload_id);
    if (upload_id.empty() || kv_->get_str(row, "owner") != user) {
        return HttpResponse::json(R"({"ok":false,"error":"upload session not found"})");
    }
    if (kv_->get_str(row, "status") == "completed") {
        std::string file_uid = kv_->get_str(row, "file_uid");
        return HttpResponse::json("{\"ok\":true,\"uid\":" + json_str(file_uid) + "}");
    }
    size_t total_chunks = parse_size_t_or(kv_->get_str(row, "total_chunks"), 0);
    if (!upload_received_all(kv_.get(), row, total_chunks)) {
        return HttpResponse::json(R"({"ok":false,"error":"upload is missing chunks"})");
    }

    std::string file_uid = kv_->get_str(row, "file_uid");
    std::string filename = kv_->get_str(row, "filename");
    std::string parent_path = kv_->get_str(row, "path");
    std::string parent_uid = kv_->get_str(row, "parent_uid");
    size_t total_size = parse_size_t_or(kv_->get_str(row, "total_size"), 0);
    if (file_uid.empty() || filename.empty() || parent_uid.empty()) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid upload session"})");
    }
    if (child_name_exists(kv_.get(), parent_uid, filename)) {
        return HttpResponse::json(R"({"ok":false,"error":"name already exists"})");
    }

    auto meta_started = std::chrono::steady_clock::now();
    std::string now = chat_timestamp_now();
    std::string fail_reason;
    if (!kv_->put(drive_file_row(file_uid), "chunks", std::to_string(total_chunks))) fail_reason = "failed to store chunk count";
    else if (!kv_->put(drive_obj_row(file_uid), "type", "file")) fail_reason = "failed to store file type";
    else if (!kv_->put(drive_obj_row(file_uid), "name", filename)) fail_reason = "failed to store file name";
    else if (!kv_->put(drive_obj_row(file_uid), "parent", parent_uid)) fail_reason = "failed to store parent folder";
    else if (!kv_->put(drive_obj_row(file_uid), "owner", user)) fail_reason = "failed to store file owner";
    else if (!kv_->put(drive_obj_row(file_uid), "size", std::to_string(total_size))) fail_reason = "failed to store file size";
    else if (!kv_->put(drive_obj_row(file_uid), "created_at", now)) fail_reason = "failed to store file timestamp";
    else if (!kv_->put(drive_obj_row(file_uid), "updated_at", now)) fail_reason = "failed to store file timestamp";
    else if (!append_child(kv_.get(), parent_uid, file_uid)) fail_reason = "failed to update folder listing";
    else if (!kv_->put(row, "status", "completed")) fail_reason = "failed to complete upload";
    long long meta_ms = elapsed_ms_since(meta_started);
    if (!fail_reason.empty()) {
        return HttpResponse::json("{\"ok\":false,\"error\":" + json_str(fail_reason) + "}");
    }
    std::string path = join_path(parent_path, filename);
    if (!adjust_user_drive_used_bytes(kv_.get(), user, static_cast<long long>(total_size))) {
        std::cerr << "[drive] failed to update cached quota usage for " << user << "\n";
    }
    std::cerr << "[drive] upload finish user=" << user
              << " upload_id=" << upload_id
              << " file=" << filename
              << " bytes=" << total_size
              << " chunks=" << total_chunks
              << " meta_ms=" << meta_ms
              << " total_ms=" << elapsed_ms_since(started) << "\n";
    return HttpResponse::json("{\"ok\":true,\"uid\":" + json_str(file_uid) + ",\"path\":" + json_str(path) + "}");
}

HttpResponse FEServer::handle_upload_cancel(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string upload_id = params["upload_id"];
    std::string row = drive_upload_row(upload_id);
    if (upload_id.empty() || kv_->get_str(row, "owner") != user) {
        return HttpResponse::json(R"({"ok":false,"error":"upload session not found"})");
    }
    std::string file_uid = kv_->get_str(row, "file_uid");
    if (!file_uid.empty()) delete_file_bytes(kv_.get(), file_uid);
    kv_->put(row, "status", "cancelled");
    std::string fingerprint = kv_->get_str(row, "fingerprint");
    if (!fingerprint.empty()) kv_->del(drive_upload_lookup_row(user), upload_lookup_col(fingerprint));
    return HttpResponse::json(R"({"ok":true})");
}

HttpResponse FEServer::handle_download(const HttpRequest&, const std::string& user,
                                        const std::string& uid) {
    std::string owner = kv_->get_str(drive_obj_row(uid), "owner");
    std::string type = kv_->get_str(drive_obj_row(uid), "type");
    if (owner != user || type != "file") return HttpResponse::not_found();

    HttpResponse resp = HttpResponse::ok(get_file_bytes(kv_.get(), uid), "application/octet-stream");
    std::string name = kv_->get_str(drive_obj_row(uid), "name");
    if (!name.empty()) resp.headers["Content-Disposition"] = content_disposition_attachment(name);
    return resp;
}

HttpResponse FEServer::handle_rename(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string path = params["path"];
    std::string new_name = params["name"];
    if (!valid_component(new_name) || path.empty() || path == "/") {
        return HttpResponse::json(R"({"ok":false,"error":"invalid rename request"})");
    }

    std::string uid, type;
    if (!resolve_path(kv_.get(), user, path, uid, &type)) {
        return HttpResponse::json(R"({"ok":false,"error":"path not found"})");
    }
    std::string parent_uid = kv_->get_str(drive_obj_row(uid), "parent");
    if (child_name_exists(kv_.get(), parent_uid, new_name, uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"name already exists"})");
    }
    kv_->put(drive_obj_row(uid), "name", new_name);
    kv_->put(drive_obj_row(uid), "updated_at", chat_timestamp_now());
    return HttpResponse::json("{\"ok\":true,\"path\":" + json_str(join_path(parent_path(path), new_name)) + "}");
}

HttpResponse FEServer::handle_move(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string src_path = params["path"];
    std::string dst_path = params["dst"];
    if (src_path.empty() || dst_path.empty() || src_path == "/") {
        return HttpResponse::json(R"({"ok":false,"error":"invalid move request"})");
    }

    std::string uid, type, dst_uid, dst_type, resolved_dst_path;
    if (!resolve_path(kv_.get(), user, src_path, uid, &type) ||
        !resolve_move_destination(kv_.get(), user, src_path, dst_path, resolved_dst_path, dst_uid, dst_type)) {
        return HttpResponse::json(R"({"ok":false,"error":"move target not found"})");
    }
    std::string name = kv_->get_str(drive_obj_row(uid), "name");
    if (type == "folder" && is_descendant_of(kv_.get(), dst_uid, uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"cannot move a folder into itself or its descendant"})");
    }
    if (child_name_exists(kv_.get(), dst_uid, name, uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"destination already has that name"})");
    }
    std::string old_parent = kv_->get_str(drive_obj_row(uid), "parent");
    if (old_parent == dst_uid) {
        return HttpResponse::json("{\"ok\":true,\"path\":" + json_str(join_path(resolved_dst_path, name)) + "}");
    }
    if (!remove_child(kv_.get(), old_parent, uid, true)) {
        return HttpResponse::json(R"({"ok":false,"error":"move failed"})");
    }
    if (!append_child(kv_.get(), dst_uid, uid)) {
        if (!append_child(kv_.get(), old_parent, uid)) {
            std::cerr << "[drive] move rollback failed while restoring source listing for " << uid << "\n";
        }
        return HttpResponse::json(R"({"ok":false,"error":"move failed"})");
    }
    if (!kv_->put(drive_obj_row(uid), "parent", dst_uid)) {
        bool removed_dst = remove_child(kv_.get(), dst_uid, uid);
        bool restored_src = append_child(kv_.get(), old_parent, uid);
        if (!removed_dst || !restored_src) {
            std::cerr << "[drive] move rollback incomplete for " << uid
                      << " removed_dst=" << removed_dst
                      << " restored_src=" << restored_src << "\n";
        }
        return HttpResponse::json(R"({"ok":false,"error":"move failed"})");
    }
    kv_->put(drive_obj_row(uid), "updated_at", chat_timestamp_now());
    return HttpResponse::json("{\"ok\":true,\"path\":" + json_str(join_path(resolved_dst_path, name)) + "}");
}

HttpResponse FEServer::handle_mkdir(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string path = params["path"];
    std::string name = params["name"];
    if (path.empty()) path = "/";
    if (!valid_component(name)) {
        return HttpResponse::json(R"({"ok":false,"error":"invalid folder name"})");
    }

    std::string parent_uid, type;
    if (!resolve_path(kv_.get(), user, path, parent_uid, &type) || type != "folder") {
        return HttpResponse::json(R"({"ok":false,"error":"parent folder not found"})");
    }
    if (child_name_exists(kv_.get(), parent_uid, name)) {
        return HttpResponse::json(R"({"ok":false,"error":"name already exists"})");
    }

    std::string uid = drive_new_uid();
    std::string now = chat_timestamp_now();
    if (!kv_->put(drive_obj_row(uid), "type", "folder") ||
        !kv_->put(drive_obj_row(uid), "name", name) ||
        !kv_->put(drive_obj_row(uid), "parent", parent_uid) ||
        !kv_->put(drive_obj_row(uid), "owner", user) ||
        !kv_->put(drive_obj_row(uid), "created_at", now) ||
        !kv_->put(drive_obj_row(uid), "updated_at", now) ||
        !kv_->put(drive_dir_row(uid), "children", "") ||
        !append_child(kv_.get(), parent_uid, uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"mkdir failed"})");
    }
    return HttpResponse::json("{\"ok\":true,\"uid\":" + json_str(uid) + ",\"path\":" + json_str(join_path(path, name)) + "}");
}

HttpResponse FEServer::handle_delete_path(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string path = params["path"];
    if (path.empty() || path == "/") {
        return HttpResponse::json(R"({"ok":false,"error":"cannot delete root"})");
    }

    std::string uid, type;
    if (!resolve_path(kv_.get(), user, path, uid, &type)) {
        return HttpResponse::json(R"({"ok":false,"error":"path not found"})");
    }

    size_t removed_bytes = subtree_file_bytes(kv_.get(), uid);
    std::string parent_uid = kv_->get_str(drive_obj_row(uid), "parent");
    if (!remove_child(kv_.get(), parent_uid, uid, true)) {
        return HttpResponse::json(R"({"ok":false,"error":"delete failed"})");
    }

    std::vector<std::string> nodes;
    collect_subtree(kv_.get(), uid, nodes);
    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it) {
        std::string obj = *it;
        std::string obj_type = kv_->get_str(drive_obj_row(obj), "type");
        kv_->del(drive_obj_row(obj), "type");
        kv_->del(drive_obj_row(obj), "name");
        kv_->del(drive_obj_row(obj), "parent");
        kv_->del(drive_obj_row(obj), "owner");
        kv_->del(drive_obj_row(obj), "size");
        kv_->del(drive_obj_row(obj), "created_at");
        kv_->del(drive_obj_row(obj), "updated_at");
        if (obj_type == "folder") {
            kv_->del(drive_dir_row(obj), "children");
        } else {
            delete_file_bytes(kv_.get(), obj);
        }
    }
    if (!adjust_user_drive_used_bytes(kv_.get(), user, -static_cast<long long>(removed_bytes))) {
        std::cerr << "[drive] failed to update cached quota usage after delete for " << user << "\n";
    }
    return HttpResponse::json(R"({"ok":true})");
}
HttpResponse FEServer::handle_quota_status(const HttpRequest&, const std::string& user) {
    size_t limit_bytes = user_drive_quota_bytes(kv_.get(), user);
    size_t used_bytes = user_drive_used_bytes(kv_.get(), user);
    size_t remaining = used_bytes >= limit_bytes ? 0 : (limit_bytes - used_bytes);
    std::string body = std::string("{\"ok\":true,\"limit_bytes\":") + std::to_string(limit_bytes) +
                       ",\"used_bytes\":" + std::to_string(used_bytes) +
                       ",\"remaining_bytes\":" + std::to_string(remaining) + "}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_quota_update(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    size_t limit_mb = parse_size_t_or(params["limit_mb"], 0);
    // Hard cap: each user gets at most 1 GB of Drive storage.
    constexpr size_t kMaxQuotaMB = 1024;
    if (limit_mb == 0 || limit_mb > kMaxQuotaMB) {
        return HttpResponse::json(R"({"ok":false,"error":"limit must be between 1 and 1024 MB"})");
    }
    size_t new_limit = limit_mb * 1024ull * 1024ull;
    size_t used_bytes = user_drive_used_bytes(kv_.get(), user);
    if (new_limit < used_bytes) {
        return HttpResponse::json(R"({"ok":false,"error":"new quota is below current usage"})");
    }
    if (!kv_->put(drive_user_row(user), drive_quota_col(), std::to_string(new_limit))) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to persist quota"})");
    }
    return handle_quota_status(req, user);
}
