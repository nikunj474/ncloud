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
#include <algorithm>
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

constexpr int kMaxSSEConnections = 64;

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
    c.backends["node1"] = {"node1", 7500, 7600, first_existing_path_admin({"/tmp/pc_multi_group/node1","/tmp/pc_multi_group_cluster/node1","/tmp/pc_mg_cluster/node1"}), root + "/mg_node1.log", {}};
    c.backends["node2"] = {"node2", 7501, 7601, first_existing_path_admin({"/tmp/pc_multi_group/node2","/tmp/pc_multi_group_cluster/node2","/tmp/pc_mg_cluster/node2"}), root + "/mg_node2.log", {}};
    c.backends["node3"] = {"node3", 7502, 7602, first_existing_path_admin({"/tmp/pc_multi_group/node3","/tmp/pc_multi_group_cluster/node3","/tmp/pc_mg_cluster/node3"}), root + "/mg_node3.log", {}};
    c.backends["node4"] = {"node4", 7503, 7603, first_existing_path_admin({"/tmp/pc_multi_group/node4","/tmp/pc_multi_group_cluster/node4","/tmp/pc_mg_cluster/node4"}), root + "/mg_node4.log", {}};
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
        "--tablet", "tabletC:q:zzzzz"
    };
    c.backends["node1"] = {"node1", 6500, 6600,
        first_existing_path_admin({"/tmp/pc_multi_tablet_demo/node1","/tmp/pc_multi_tablet/node1","/tmp/pc_mt_demo/node1"}),
        root + "/mt_node1.log", tablets};
    c.backends["node2"] = {"node2", 6501, 6601,
        first_existing_path_admin({"/tmp/pc_multi_tablet_demo/node2","/tmp/pc_multi_tablet/node2","/tmp/pc_mt_demo/node2"}),
        root + "/mt_node2.log", tablets};
    c.backends["node3"] = {"node3", 6502, 6602,
        first_existing_path_admin({"/tmp/pc_multi_tablet_demo/node3","/tmp/pc_multi_tablet/node3","/tmp/pc_mt_demo/node3"}),
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

static AdminClusterSpec detect_active_cluster_admin(const FEServer::Config& cfg) {
    const std::string root = resolve_project_root_admin();
    std::string host = cfg.coord_host.empty() ? "127.0.0.1" : cfg.coord_host;
    if (cfg.coord_port > 0 && tcp_probe_admin(host, cfg.coord_port, 250)) {
        if (cfg.coord_port == 7110) return multi_group_cluster_spec_admin(root);
        if (cfg.coord_port == 7010) return multi_tablet_cluster_spec_admin(root);
        if (cfg.coord_port == 6000) return single_cluster_spec_admin(root);
        return abc_cluster_spec_admin(root);
    }
    if (tcp_probe_admin("127.0.0.1", 7110, 250)) return multi_group_cluster_spec_admin(root);
    if (tcp_probe_admin("127.0.0.1", 7010, 250)) return multi_tablet_cluster_spec_admin(root);
    if (tcp_probe_admin("127.0.0.1", 6010, 250)) return abc_cluster_spec_admin(root);
    if (tcp_probe_admin("127.0.0.1", 6000, 250)) return single_cluster_spec_admin(root);
    if (cfg.coord_port == 7110) return multi_group_cluster_spec_admin(root);
    if (cfg.coord_port == 7010) return multi_tablet_cluster_spec_admin(root);
    if (cfg.coord_port == 6000) return single_cluster_spec_admin(root);
    return abc_cluster_spec_admin(root);
}

static AdminClusterSpec configured_cluster_spec_admin(const FEServer::Config& cfg) {
    const std::string root = resolve_project_root_admin();
    if (cfg.coord_port == 7110) return multi_group_cluster_spec_admin(root);
    if (cfg.coord_port == 7010) return multi_tablet_cluster_spec_admin(root);
    if (cfg.coord_port == 6000) return single_cluster_spec_admin(root);
    return abc_cluster_spec_admin(root);
}

static bool verify_port_up_admin(int port, int attempts = 16, int sleep_ms = 250) {
    for (int i = 0; i < attempts; ++i) {
        if (tcp_probe_admin("127.0.0.1", port, 250)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
    return false;
}

static std::string pids_for_port_cmd_admin(int port) {
    std::string p = std::to_string(port);
    return "lsof -nP -tiTCP:" + p + " -sTCP:LISTEN 2>/dev/null";
}

static bool port_has_listener_admin(int port) {
    if (port <= 0) return false;
    return ::system(pids_for_port_cmd_admin(port).c_str()) == 0;
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
        std::string p = std::to_string(port);
        script += "pkill -KILL -f -- \"--port " + p + "\" 2>/dev/null || true; ";
    }
    if (stub_port > 0 && stub_peer > 0 && !fe_bin.empty() && file_exists_admin(fe_bin)) {
        std::string sp = std::to_string(stub_port);
        script += "for i in 1 2 3 4 5 6 7 8 9 10; do ";
        script += "if ! pgrep -f -- \"--port " + sp + "\" >/dev/null 2>&1; then ";
        script += "exec " + shell_escape_admin(fe_bin) +
                  " --port " + sp +
                  " --redirect-to http://127.0.0.1:" + std::to_string(stub_peer) + "; ";
        script += "fi; sleep 0.05; done; ";
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
        " --redirect-to http://127.0.0.1:" + std::to_string(peer) +
        " > /dev/null 2>&1 &";
    admin_log("starting redirect stub: port " + std::to_string(killed_port) +
              " -> 127.0.0.1:" + std::to_string(peer));
    run_shell_command(cmd);
}

static std::string kill_port_cmd_admin(int port) {
    std::string p = std::to_string(port);
    // No lsof in the container; match by --port argument with pkill -f.
    // Send SIGKILL immediately -- admin kill must be instant.
    std::string cmd;
    cmd += "pkill -KILL -f -- \"--port " + p + "\" 2>/dev/null || true; ";
    cmd += "true";
    return cmd;
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
        std::string p = std::to_string(port);
        // The "--" separator tells pkill that the pattern, which starts with
        // "--", is positional and not an option flag.
        combined += "{ pkill -KILL -f -- \"--port " + p + "\" 2>/dev/null; "
                    "pkill -KILL -f -- \"--repl-port " + p + "\" 2>/dev/null; "
                    "true; } & ";
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
        bool alive = tcp_probe_admin("127.0.0.1", n.kv_port, 60);
        replace_json_bool_field_admin(backend_json, n.id, "alive", alive);
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
                // Self-kill fallback path. If no real peer exists, remove all
                // frontend listeners/stubs so the whole frontend tier is truly down.
                int stub_peer = real_fe_redirect_peer_admin(fe_port);
                std::vector<int> ports_to_kill = stub_peer > 0
                    ? std::vector<int>{fe_port}
                    : std::vector<int>{8090, 8091, 8092};
                std::string kill_and_stub =
                    frontend_self_kill_helper_cmd_admin(ports_to_kill, fe_port, stub_peer, fe_bin);
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
    if (!cfg_.redirect_to.empty()) {
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
    struct timeval tv{.tv_sec = 5, .tv_usec = 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    bool detached = false;
    while (running_ && !detached && handle_one_request(fd, detached)) {}
    if (!detached) ::close(fd);
}

bool FEServer::handle_one_request(int fd, bool& detached) {
    HttpRequest req;
    if (!read_http_request(fd, req)) return false;

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
            int active = active_sse_.fetch_add(1) + 1;
            if (active > kMaxSSEConnections) {
                active_sse_.fetch_sub(1);
                std::string body = "{\"ok\":false,\"error\":\"too many events\"}";
                std::string r503 = std::string(
                    "HTTP/1.1 503 Service Unavailable\r\n"
                    "Content-Type: application/json\r\n") +
                    "Content-Length: " + std::to_string(body.size()) + "\r\n"
                    "Connection: close\r\n"
                    "\r\n" + body;
                http_write_all(fd, r503);
                return false;
            }
            try {
                std::thread([this, fd, req, user] {
                    handle_sse(fd, req, user);
                    active_sse_.fetch_sub(1);
                    ::close(fd);
                }).detach();
                detached = true;
            } catch (...) {
                active_sse_.fetch_sub(1);
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
    resp.headers["Connection"] = req.keep_alive() ? "keep-alive" : "close";
    bool is_head = (req.method == "HEAD");
    if (!send_http_response(fd, resp, is_head)) return false;
    return req.keep_alive();
}

// ---------------------------------------------------------------------------
// Route dispatch
// ---------------------------------------------------------------------------
std::string FEServer::get_user(const HttpRequest& req) {
    return sessions_->authenticate(req);
}

HttpResponse FEServer::dispatch(const HttpRequest& req) {
    const std::string& path   = req.path;
    const std::string& method = req.method;

    // Redirect stub mode: return 503 for API probes so health checks can
    // distinguish this stub from a real server; redirect everything else.
    if (!cfg_.redirect_to.empty()) {
        if (path.rfind("/api/", 0) == 0) {
            HttpResponse r;
            r.status_code = 503;
            r.status_text = "Service Unavailable";
            r.body = "{\"ok\":false,\"stub\":true}";
            r.headers["Content-Type"] = "application/json";
            return r;
        }
        std::string loc = cfg_.redirect_to;
        if (!path.empty() && path != "/") loc += path;
        if (!req.query.empty()) loc += "?" + req.query;
        return HttpResponse::redirect(loc, 302);
    }

    // Admin endpoints must not depend on session lookup in KV, and they must
    // stay reachable even when every backend node is down.
    if (path == "/api/admin/ping" && method == "GET")     return HttpResponse::json(R"({"ok":true})");
    if (path == "/api/admin/status" && method == "GET")   return handle_admin_status(req);
    if (path == "/api/admin/control" && method == "POST") return handle_admin_control(req);
    if (path == "/admin/control" && method == "GET")      return handle_admin_control_redirect(req);
    if (path == "/admin/metrics" && method == "GET")      return handle_admin_metrics(req);
    if ((path == "/admin" || path.rfind("/admin/", 0) == 0) && method == "GET")
        return handle_admin_page(req);

    // ---- Static / SPA shell -------------------------------------------------
    if (path == "/" || path == "/index.html")
        return handle_spa_shell(req);

    if (path.rfind("/static/", 0) == 0)
        return handle_static(req);

    // ---- Auth endpoints (no session required) --------------------------------
    if (path == "/api/login"  && method == "POST") return handle_login(req);
    if (path == "/api/signup" && method == "POST") return handle_signup(req);

    // ---- Protected endpoints (session required) ------------------------------
    std::string user = get_user(req);

    if (path == "/api/logout"          && method == "POST") return handle_logout(req);
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
  <style>
    /* ---- Reset + base ---- */
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --penn-blue: #011F5B;
      --accent:    #0066CC;
      --bg:        #F5F7FA;
      --surface:   #FFFFFF;
      --border:    #E2E8F0;
      --text:      #1A202C;
      --muted:     #718096;
      --success:   #1A6B3A;
      --danger:    #C53030;
      --sidebar-w: 220px;
    }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: var(--bg); color: var(--text); min-height: 100vh; }

    /* ---- Login page ---- */
    #login-page {
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; background: var(--penn-blue);
    }
    .login-card {
      background: var(--surface); border-radius: 12px;
      padding: 40px 36px; width: 360px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
    }
    .login-card h1 { color: var(--penn-blue); font-size: 26px; margin-bottom: 6px; }
    .login-card p  { color: var(--muted); font-size: 14px; margin-bottom: 24px; }
    .form-group { margin-bottom: 16px; }
    .form-group label { display: block; font-size: 13px; font-weight: 600;
                        color: var(--muted); margin-bottom: 6px; }
    .form-group input {
      width: 100%; padding: 10px 14px; border: 1px solid var(--border);
      border-radius: 8px; font-size: 14px; outline: none; transition: border .15s;
    }
    .form-group input:focus { border-color: var(--accent); }
    .btn {
      width: 100%; padding: 11px; border: none; border-radius: 8px;
      font-size: 14px; font-weight: 600; cursor: pointer; transition: opacity .15s;
    }
    .btn-primary { background: var(--penn-blue); color: white; }
    .btn-primary:hover { opacity: .88; }
    .btn-link { background: none; color: var(--accent); font-size: 13px;
                text-decoration: underline; cursor: pointer; border: none; }
    .error-msg { color: var(--danger); font-size: 13px; margin-top: 8px; display: none; }

    /* ---- App shell ---- */
    #app { display: none; min-height: 100vh; flex-direction: column; }
    .topbar {
      background: var(--penn-blue); color: white;
      padding: 0 24px; height: 52px;
      display: flex; align-items: center; justify-content: space-between;
    }
    .topbar .brand { font-size: 18px; font-weight: 700; letter-spacing: -0.3px; }
    .topbar .user-info { font-size: 13px; opacity: .8; display: flex;
                         align-items: center; gap: 16px; }
    .topbar .logout-btn { background: rgba(255,255,255,0.15); border: none;
                          color: white; padding: 5px 12px; border-radius: 6px;
                          cursor: pointer; font-size: 12px; }
    .main-layout { display: flex; flex: 1; }
    .sidebar {
      width: var(--sidebar-w); background: var(--surface);
      border-right: 1px solid var(--border); padding: 16px 0;
      display: flex; flex-direction: column; gap: 2px;
    }
    .nav-item {
      display: flex; align-items: center; gap: 10px;
      padding: 10px 20px; cursor: pointer; border-radius: 0;
      font-size: 14px; color: var(--muted); transition: all .1s;
      border: none; background: none; width: 100%; text-align: left;
    }
    .nav-item:hover { background: var(--bg); color: var(--text); }
    .nav-item.active { background: #EEF3FB; color: var(--penn-blue);
                       font-weight: 600; border-right: 3px solid var(--penn-blue); }
    .nav-icon { width: 18px; text-align: center; font-size: 16px; }
    .content { flex: 1; padding: 24px; overflow-y: auto; }

    /* ---- Inbox ---- */
    .page-title { font-size: 20px; font-weight: 700; color: var(--penn-blue);
                  margin-bottom: 16px; display: flex; align-items: center;
                  justify-content: space-between; }
    .compose-btn {
      background: var(--penn-blue); color: white; border: none;
      padding: 8px 16px; border-radius: 8px; cursor: pointer;
      font-size: 13px; font-weight: 600;
    }
    .email-list { background: var(--surface); border-radius: 10px;
                  border: 1px solid var(--border); overflow: hidden; }
    .email-row {
      display: flex; align-items: center; padding: 14px 20px;
      border-bottom: 1px solid var(--border); cursor: pointer;
      transition: background .1s; gap: 16px;
    }
    .email-row:hover { background: var(--bg); }
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
    .email-view { background: var(--surface); border-radius: 10px;
                  border: 1px solid var(--border); padding: 28px; }
    .email-view h2 { font-size: 18px; margin-bottom: 12px; }
    .email-meta { color: var(--muted); font-size: 13px; margin-bottom: 16px;
                  display: flex; flex-direction: column; gap: 4px; }
    .email-body { white-space: pre-wrap; font-size: 14px; line-height: 1.6;
                  border-top: 1px solid var(--border); padding-top: 16px; }
    .email-actions { display: flex; gap: 8px; margin-bottom: 16px; }
    .action-btn {
      padding: 7px 14px; border-radius: 7px; font-size: 13px; cursor: pointer;
      border: 1px solid var(--border); background: var(--surface);
    }
    .action-btn.danger { color: var(--danger); border-color: var(--danger); }

    /* ---- Compose ---- */
    .compose-form { background: var(--surface); border-radius: 10px;
                    border: 1px solid var(--border); padding: 28px; max-width: 700px; }
    .compose-form input, .compose-form textarea {
      width: 100%; padding: 10px 14px; border: 1px solid var(--border);
      border-radius: 8px; font-size: 14px; margin-bottom: 12px;
      font-family: inherit; outline: none;
    }
    .compose-form textarea { height: 200px; resize: vertical; }
    .compose-form input:focus, .compose-form textarea:focus { border-color: var(--accent); }

    /* ---- Drive ---- */
    .drive-toolbar { display: flex; gap: 8px; margin-bottom: 16px; }
    .drive-toolbar button {
      padding: 7px 14px; border-radius: 7px; font-size: 13px; cursor: pointer;
      border: 1px solid var(--border); background: var(--surface);
    }
    .breadcrumb { font-size: 13px; color: var(--muted); margin-bottom: 12px; }
    .breadcrumb span { cursor: pointer; color: var(--accent); }
    .breadcrumb span:hover { text-decoration: underline; }
    .file-grid { background: var(--surface); border-radius: 10px;
                 border: 1px solid var(--border); overflow: hidden; }
    .file-row {
      display: flex; align-items: center; padding: 12px 20px;
      border-bottom: 1px solid var(--border); gap: 12px;
    }
    .file-row:last-child { border-bottom: none; }
    .file-icon { font-size: 18px; width: 24px; text-align: center; }
    .file-name { flex: 1; font-size: 14px; cursor: pointer; }
    .file-name:hover { color: var(--accent); text-decoration: underline; }
    .file-size { font-size: 12px; color: var(--muted); min-width: 80px; }
    .file-actions { display: flex; gap: 6px; }
    .file-actions button {
      padding: 3px 8px; font-size: 11px; border-radius: 5px;
      border: 1px solid var(--border); cursor: pointer; background: var(--surface);
    }

    /* ---- Notifications ---- */
    .toast {
      position: fixed; bottom: 24px; right: 24px;
      background: var(--text); color: white;
      padding: 12px 20px; border-radius: 8px; font-size: 13px;
      opacity: 0; transition: opacity .3s; pointer-events: none; z-index: 999;
    }
    .toast.show { opacity: 1; }
    .spinner { text-align: center; padding: 40px; color: var(--muted); }
  </style>
</head>
<body>

<!-- Login Page -->
<div id="login-page">
  <div class="login-card">
    <h1>PennCloud</h1>
    <p id="login-subtitle">Sign in to your account</p>
    <div class="form-group">
      <label>Username</label>
      <input id="username-in" type="text" placeholder="e.g. nikunj" autocomplete="username">
    </div>
    <div class="form-group">
      <label>Password</label>
      <input id="password-in" type="password" placeholder="Your password" autocomplete="current-password">
    </div>
    <button class="btn btn-primary" id="login-btn" onclick="doLogin()">Sign in</button>
    <div id="login-error" class="error-msg"></div>
    <br>
    <button class="btn-link" onclick="showSignup()">Create an account</button>
  </div>
</div>

<!-- App Shell -->
<div id="app">
  <div class="topbar">
    <div class="brand">PennCloud</div>
    <div class="user-info">
      <span id="topbar-user"></span>
      <button class="logout-btn" onclick="doLogout()">Sign out</button>
    </div>
  </div>
  <div class="main-layout">
    <div class="sidebar">
      <button class="nav-item" id="nav-inbox" onclick="navigate('inbox')">
        <span class="nav-icon">&#9993;</span> Inbox
      </button>
      <button class="nav-item" id="nav-compose" onclick="navigate('compose')">
        <span class="nav-icon">&#9998;</span> Compose
      </button>
      <button class="nav-item" id="nav-drive" onclick="navigate('drive')">
        <span class="nav-icon">&#128193;</span> Drive
      </button>
      <button class="nav-item" id="nav-chat" onclick="navigate('chat')">
        <span class="nav-icon">&#128172;</span> Chat
      </button>
      <button class="nav-item" id="nav-settings" onclick="navigate('settings')">
        <span class="nav-icon">&#9881;</span> Settings
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

function markFrontendHealthFailure() {
  frontendHealthFailures += 1;
  if (frontendHealthFailures >= 1) redirectToPeerApp();
  return null;
}

async function backendStorageAvailable() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 700);
  try {
    const r = await fetch('/api/admin/status', { cache: 'no-store', signal: controller.signal });
    if (!r.ok) return true;
    const data = await r.json();
    frontendHealthFailures = 0;
    lastFrontendNodes = data.frontend_nodes || [];
    const backendAlive = (data.backend_nodes || []).some(n => !!n.alive);
    const frontendAlive = (data.frontend_nodes || []).some(n => !!n.alive);
    return backendAlive || frontendAlive;
  } catch (_) {
    // If this frontend was killed, local fetches fail even when other frontends
    // and backends are alive. Treat that as a peer-redirect condition, not 404.
    return markFrontendHealthFailure();
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
async function doLogin() {
  const u = document.getElementById('username-in').value.trim();
  const p = document.getElementById('password-in').value;
  if (!u || !p) { showError('Please enter username and password.'); return; }

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
  showLogin();
}

function showSignup() {
  document.getElementById('login-subtitle').textContent = 'Create a new account';
  document.getElementById('login-btn').textContent = 'Create account';
  document.getElementById('login-btn').onclick = doSignup;
}

async function doSignup() {
  const u = document.getElementById('username-in').value.trim();
  const p = document.getElementById('password-in').value;
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

function showLogin() {
  document.getElementById('login-page').style.display = 'flex';
  document.getElementById('app').style.display = 'none';
}

async function showApp() {
  document.getElementById('login-page').style.display = 'none';
  document.getElementById('app').style.display = 'flex';
  document.getElementById('topbar-user').textContent = currentUser;
  authActive = true;
  knownInboxUids = new Set();
  await Promise.all([loadContacts(true), loadQuota(true)]);
  startSSE();
  startAppHealthMonitor();
  navigate('inbox', {folder: 'inbox'});
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
function navigate(view, params = {}) {
  currentView = view;
  const routeSeq = ++appRouteSeq;
  if (chatPollTimer) { clearTimeout(chatPollTimer); chatPollTimer = null; }
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  const navKey = view === 'email' ? 'inbox' : view;
  const navEl = document.getElementById('nav-' + navKey);
  if (navEl) navEl.classList.add('active');
  history.pushState({view, params}, '', '/' + view);

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
  if (e.state) navigate(e.state.view, e.state.params || {});
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

async function renderInbox(folder = 'inbox') {
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
      <input id="to-in" list="contacts-list" type="text" placeholder="To (e.g. rohit or rohit@gmail.com)" value="${escHtml(composeDraft.reply_to || '')}">
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
    const r = await fetch('/api/send', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: `to=${encodeURIComponent(to)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}&attachment_ids=${encodeURIComponent(attachmentIds)}`
    });
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
    errBox.textContent = 'Send failed.';
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
  const name = prompt('Save contact as:');
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
    ? `<div style="padding:40px;text-align:center;color:var(--muted)">No messages yet in this ${currentChatMode === 'dm' ? 'conversation' : 'room'}.</div>`
    : messages.map(m => `
        <div style="padding:10px 12px;border:1px solid var(--border);border-radius:10px;background:white;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;gap:12px;margin-bottom:6px;font-size:12px;color:var(--muted)">
            <span><b style="color:#111827">${escHtml(m.from || '')}</b>${m.to ? ` <span style="opacity:.7">→ ${escHtml(m.to)}</span>` : ''}</span>
            <span>${escHtml(m.time || '')}</span>
          </div>
          <div style="white-space:pre-wrap;word-break:break-word">${escHtml(m.text || '')}</div>
        </div>`).join('');
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
        <button class="action-btn" onclick="startDirectMessage()">+ Direct message</button>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-top:-6px;margin-bottom:8px">Rooms</div>
      ${renderChatRooms(currentChatRoom, rooms, counts)}
      <div style="font-size:12px;color:var(--muted);margin-top:-4px;margin-bottom:8px">Direct messages</div>
      ${renderDmPeers(currentChatPeer, chatDmPeers)}
      <div style="display:grid;grid-template-rows:1fr auto;gap:14px;min-height:520px;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px">
        <div id="chat-messages" style="overflow:auto;max-height:460px;padding-right:6px"></div>
        <div style="display:flex;gap:10px;align-items:flex-end">
          <textarea id="chat-input" placeholder="Message ${escHtml(chatHeaderLabel())}" style="flex:1;min-height:70px;max-height:160px"></textarea>
          <button id="chat-send-btn" class="btn btn-primary" style="width:auto;padding:10px 22px" onclick="sendChatMessage()">Send</button>
        </div>
      </div>`;

    if (currentChatMode === 'dm') markCurrentDmSeen();
    renderChatMessages(chatMessagesCache);
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
  history.replaceState({view:'chat', params:{room: currentChatRoom}}, '', '/chat');
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

  history.replaceState({view:'chat', params:{dm: peer}}, '', '/chat');
  await renderChat(currentChatRoom || 'general', peer);
}

function startDirectMessage() {
  const peer = (prompt('Chat with which PennCloud user?') || '').trim().replace(/@penncloud$/i, '');
  if (!peer) return;
  if (peer === currentUser) {
    showToast('Choose another user.');
    return;
  }
  navigate('chat', { dm: peer });
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

async function renderInbox(folder = 'inbox') {
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
      <input id="to-in" list="contacts-list" type="text" placeholder="To (e.g. rohit or rohit@gmail.com)" value="${escHtml(composeDraft.reply_to || '')}">
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
    const r = await fetch('/api/send', {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded'},
      body: `to=${encodeURIComponent(to)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}&attachment_ids=${encodeURIComponent(attachmentIds)}`
    });
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
    errBox.textContent = 'Send failed.';
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
  const name = prompt('Save contact as:');
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


async function renderDrive(folderPath = '/') {
  currentPath = folderPath;
  const content = document.getElementById('content');
  let r, quota;
  try {
    [r, quota] = await Promise.all([
      fetch('/api/drive?path=' + encodeURIComponent(folderPath)).then(x => x.json()),
      loadQuota(true)
    ]);
  } catch (_) {
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<p>Error loading drive.</p>';
    return;
  }
  if (!r.ok) {
    if (!(await ensureBackendStorageAvailable())) return;
    content.innerHTML = '<p>Error loading drive.</p>';
    return;
  }

  const crumbs = folderPath === '/' ? ['/'] : ['/', ...folderPath.slice(1).split('/')];
  const breadcrumbHtml = crumbs.map((c, i) => {
    const p = '/' + crumbs.slice(1, i + 1).join('/');
    return `<span onclick="renderDrive('${escHtml(p)}')">${escHtml(c)}</span>`;
  }).join(' / ');

  const items = r.items || [];
  const usedPct = quota ? Math.min(100, Math.round((quota.used_bytes / Math.max(1, quota.limit_bytes)) * 100)) : 0;
  content.innerHTML = `
    <div class="page-title">Drive</div>
    <div style="margin-bottom:14px;padding:12px 14px;background:var(--surface);border:1px solid var(--border);border-radius:10px">
      <div style="display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;font-size:13px;margin-bottom:8px">
        <span><b>Storage quota</b> ${quota ? `${formatBytes(quota.used_bytes)} of ${formatBytes(quota.limit_bytes)}` : 'Unavailable'}</span>
        <span>${quota ? `${formatBytes(quota.remaining_bytes)} remaining` : ''}</span>
      </div>
      <div style="height:9px;background:#e2e8f0;border-radius:999px;overflow:hidden"><div style="height:9px;width:${usedPct}%;background:${usedPct > 85 ? '#C53030' : '#0066CC'}"></div></div>
    </div>
    <div class="breadcrumb">${breadcrumbHtml}</div>
    <div class="drive-toolbar">
      <button onclick="showUpload()">Upload file</button>
      <button onclick="makeFolder()">New folder</button>
    </div>
    <input type="file" id="file-input" style="display:none" onchange="uploadFile(this)">
    <div class="file-grid">
      ${items.length === 0
        ? '<div style="padding:40px;text-align:center;color:var(--muted)">This folder is empty</div>'
        : items.map(it => `
          <div class="file-row">
            <div class="file-icon">${it.type === 'folder' ? '&#128193;' : '&#128196;'}</div>
            <div class="file-name" onclick="${it.type === 'folder' ? `renderDrive('${escHtml(it.path)}')` : `downloadFile('${it.uid}','${escHtml(it.name)}')`}">${escHtml(it.name)}</div>
            <div class="file-size">${it.size || ''}</div>
            <div class="file-actions">
              <button onclick="renameItem('${escHtml(it.path)}')">Rename</button>
              <button onclick="moveItem('${escHtml(it.path)}')">Move to</button>
              <button onclick="deleteItem('${escHtml(it.path)}')">Delete</button>
            </div>
          </div>`).join('')}
    </div>`;
}

function showUpload() { document.getElementById('file-input').click(); }

async function uploadFile(input) {
  const file = input.files[0];
  if (!file) return;
  const quota = await loadQuota(true);
  if (quota && quota.used_bytes + file.size > quota.limit_bytes) {
    showToast(`Upload blocked: quota exceeded by ${formatBytes(quota.used_bytes + file.size - quota.limit_bytes)}`);
    input.value = '';
    return;
  }
  const fd = new FormData();
  fd.append('file', file);
  fd.append('path', currentPath);
  showToast('Uploading...');
  const r = await fetch('/api/upload', { method: 'POST', body: fd });
  const data = await r.json();
  showToast(data.ok ? 'Uploaded!' : 'Upload failed: ' + (data.error || ''));
  if (data.ok) {
    quotaCache = null;
    renderDrive(currentPath);
  }
  input.value = '';
}

async function downloadFile(uid, name) {
  const a = document.createElement('a');
  a.href = `/api/download/${uid}`;
  a.download = name;
  a.click();
}

async function renameItem(path) {
  const newName = prompt('New name:');
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
  const parentPath = pathParts.length <= 1 ? null : '/' + pathParts.slice(0, -1).join('/');

  const overlay = document.createElement('div');
  overlay.id = 'move-modal-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9999;display:flex;align-items:center;justify-content:center';

  const parentBtnHtml = (parentPath && parentPath !== srcPath)
    ? `<button data-dst="${escHtml(parentPath)}" class="move-quick-btn" style="text-align:left;padding:10px 14px;border:1px solid var(--border);border-radius:8px;background:var(--bg);cursor:pointer;width:100%;font-size:14px">
         📁 Parent folder&nbsp;<span style="font-size:12px;color:var(--muted)">${escHtml(parentPath)}</span>
       </button>`
    : '';

  overlay.innerHTML = `
    <div style="background:var(--surface);border-radius:12px;padding:28px;min-width:360px;max-width:480px;width:90%;box-shadow:0 20px 60px rgba(0,0,0,0.3)">
      <div style="font-size:16px;font-weight:600;margin-bottom:4px">Move &#8220;${escHtml(itemName)}&#8221;</div>
      <div style="font-size:13px;color:var(--muted);margin-bottom:18px">Choose a destination folder</div>
      <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:18px">
        <button data-dst="/" class="move-quick-btn" style="text-align:left;padding:10px 14px;border:1px solid var(--border);border-radius:8px;background:var(--bg);cursor:pointer;width:100%;font-size:14px">
          📁 Root folder&nbsp;<span style="font-size:12px;color:var(--muted)">/</span>
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
  if (!confirm('Delete this item?')) return;
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
  const name = prompt('Folder name:');
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
        <div style="font-size:13px;color:var(--muted);margin-bottom:12px">Explicit extra credit feature: per-user drive quota with upload enforcement.</div>
        <input id="quota-mb" type="number" min="1" max="1024" value="${quota ? Math.round(quota.limit_bytes / (1024 * 1024)) : 50}">
        <div style="font-size:13px;color:var(--muted);margin-bottom:12px">Currently using ${quota ? `${formatBytes(quota.used_bytes)} of ${formatBytes(quota.limit_bytes)}` : 'unknown'}.</div>
        <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="updateQuota()">Save quota</button>
        <div id="quota-msg" class="error-msg"></div>
      </div>
      <div class="compose-form" style="max-width:none">
        <h3 style="margin-bottom:16px;font-size:15px">Address book</h3>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">
          <input id="contact-name" type="text" placeholder="Name" style="flex:1;min-width:120px">
          <input id="contact-email" type="text" placeholder="Email or penncloud username" style="flex:1;min-width:160px">
          <button class="btn btn-primary" style="width:auto;padding:9px 18px" onclick="addContact()">Add</button>
        </div>
        <div id="contacts-list-box">
          ${(contactsCache || []).length === 0 ? '<div style="font-size:13px;color:var(--muted)">No contacts yet.</div>' : contactsCache.map(c => `
            <div class="file-row" style="padding:10px 0;border:none">
              <div class="file-name" style="cursor:default;text-decoration:none">${escHtml(c.name)}<div style="font-size:12px;color:var(--muted)">${escHtml(c.email)}</div></div>
              <div class="file-actions"><button onclick="deleteContact('${escHtml(c.email)}')">Delete</button></div>
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

// Keyboard shortcut: Enter to login
document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.getElementById('login-page').style.display !== 'none') {
    doLogin();
  }
});
</script>
</body>
</html>
)HTML";
    return HttpResponse::ok(html, "text/html; charset=utf-8");
}

// ---------------------------------------------------------------------------
// Auth handlers
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_login(const HttpRequest& req) {
    auto params = parse_urlencoded(req.body);
    std::string username = params["username"];
    std::string password = params["password"];

    if (username.empty() || password.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Missing credentials"})");

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

HttpResponse FEServer::handle_logout(const HttpRequest& req) {
    std::string sid = req.cookie("sid");
    if (!sid.empty()) sessions_->destroy(sid);
    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    // Clear cookie
    resp.headers["Set-Cookie"] = "sid=; Path=/; Max-Age=0; HttpOnly";
    return resp;
}

HttpResponse FEServer::handle_signup(const HttpRequest& req) {
    auto params = parse_urlencoded(req.body);
    std::string username = params["username"];
    std::string password = params["password"];

    if (username.empty() || password.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Missing fields"})");

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

    std::string stored = kv_->get_str(user, "pwd");
    if (stored != old_pw)
        return HttpResponse::json(R"({"ok":false,"error":"Wrong current password"})");

    kv_->put(user, "pwd", new_pw);
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
    std::string body = "{\"ok\":true,\"backend_nodes\":" + backend_json +
                       ",\"frontend_nodes\":" + fe_json +
                       ",\"tablets\":" + tablets_json + "}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_admin_control(const HttpRequest& req) {
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
    std::string kind = req.param("kind");
    std::string action = req.param("action");
    std::string target = req.param("target");

    admin_log("control redirect request kind=" + kind + " action=" + action + " target=" + target);

    std::string down = req.param("pc_down");
    std::string host = req.header("host");
    if (host.empty()) host = "127.0.0.1";
    size_t colon = host.find(':');
    if (colon != std::string::npos) host = host.substr(0, colon);

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
                // from appearing in lsof as another listener on our own port.
                int my_port = cfg_.port;
                std::string fe_bin_loc = fe_binary_path_admin(resolve_project_root_admin());
                int stub_peer = p.second; // redirect stub points to the peer we're handing off to
                std::string kill_and_stub =
                    frontend_self_kill_helper_cmd_admin({my_port}, my_port, stub_peer, fe_bin_loc);
                run_shell_command(kill_and_stub);
            }
            std::string location = "http://" + host + ":" + std::to_string(p.second) + "/admin";
            if (!down.empty()) location += "?pc_down=" + down + "," + target;
            else               location += "?pc_down=" + target;
            admin_log("self-kill: killing " + target + " via shell subshell, redirecting browser to " + p.first);
            return HttpResponse::redirect(location, 302);
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
    if (kind == "frontend" && action == "kill") add_down(target);

    int redirect_port = cfg_.port;
    if (kind == "frontend" && action == "kill" && target_port == cfg_.port) {
        admin_log("self-kill for " + target + " has no real frontend peer; returning 404 after scheduling kill");
        return HttpResponse::not_found();
    }

    std::string location = "http://" + host + ":" + std::to_string(redirect_port) + "/admin";
    bool has_q = false;
    if (!down.empty()) {
        location += "?pc_down=" + down;
        has_q = true;
    }
    if (!message.empty()) {
        location += has_q ? "&" : "?";
        location += std::string("pc_msg=") + (ok ? "ok" : "failed");
    }
    return HttpResponse::redirect(location, 302);
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
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f5f7fa;
      color: #1a202c;
      margin: 0;
      padding: 24px;
    }
    h1 {
      margin: 0 0 20px 0;
      color: #011F5B;
      font-size: 28px;
    }
    h2 {
      margin: 24px 0 12px 0;
      color: #011F5B;
      font-size: 20px;
    }
    .meta {
      color: #718096;
      font-size: 14px;
      margin-bottom: 16px;
    }
    .card {
      background: white;
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 6px 20px rgba(0,0,0,0.04);
      margin-bottom: 18px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid #e2e8f0;
      text-align: left;
      font-size: 14px;
    }
    th {
      background: #f8fafc;
      color: #4a5568;
      font-weight: 700;
    }
    td button {
      margin-right: 6px;
      padding: 5px 10px;
      border: 1px solid #cbd5e0;
      background: #fff;
      border-radius: 6px;
      cursor: pointer;
      font-size: 12px;
    }
    td button:hover { background: #f8fafc; }
    tr:last-child td {
      border-bottom: none;
    }
    .alive { color: #1A6B3A; font-weight: 700; }
    .down { color: #C53030; font-weight: 700; }
    #status {
      margin-top: 12px;
      color: #718096;
      font-size: 13px;
    }
  </style>
</head>
<body>
  <h1>PennCloud Admin Console</h1>
  <div class="meta">Auto-refresh every 2 seconds</div>

  <h2>Backend Servers</h2>
  <div class="card">
    <table id="backend-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Host</th>
          <th>KV Port</th>
          <th>Repl Port</th>
          <th>Alive</th>
          <th>Role</th>
          <th>LSN</th>
          <th>Missed</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <h2>Frontend Servers</h2>
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

  <h2>Tablet Placement</h2>
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
          <th>Host</th>
          <th>KV Port</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <div id="status">Loading…</div>

<script>
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
function renderBackend(nodes) {
  const tbody = document.querySelector('#backend-table tbody');
  tbody.innerHTML = '';
  for (const n of nodes) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${esc(n.id)}</td>
      <td>${esc(n.host)}</td>
      <td>${esc(n.port)}</td>
      <td>${esc(n.repl_port)}</td>
      <td>${aliveCell(!!n.alive)}</td>
      <td>${esc(n.role)}</td>
      <td>${esc(n.lsn)}</td>
      <td>${esc(n.missed)}</td>
      <td>
        <button onclick="adminControl('backend','kill','${esc(n.id)}')">Kill</button>
        <button onclick="adminControl('backend','restart','${esc(n.id)}')">Restart</button>
      </td>
    `;
    tbody.appendChild(tr);
  }
}
function renderFrontend(nodes) {
  const tbody = document.querySelector('#frontend-table tbody');
  tbody.innerHTML = '';
  for (const n of nodes) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${esc(n.id)}</td>
      <td>${esc(n.host)}</td>
      <td>${esc(n.port)}</td>
      <td>${aliveCell(!!n.alive)}</td>
      <td>
        <button onclick="adminFrontendKill('${esc(n.id)}')">Kill</button>
        <button onclick="adminControl('frontend','restart','${esc(n.id)}')">Restart</button>
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
        <td>${esc(r.host)}</td>
        <td>${esc(r.port)}</td>
      `;
      tbody.appendChild(tr);
    });
  }
}
async function adminControl(kind, action, target) {
  if (kind === 'frontend' && action === 'kill') {
    adminFrontendKill(target);
    return;
  }
  const status = document.getElementById('status');
  status.textContent = `Sending ${action} to ${target}...`;
  try {
    const r = await fetch('/api/admin/control', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
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
  }
}

let adminStatusFailures = 0;
let adminStatusInFlight = false;
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
  if (adminStatusInFlight) return;
  adminStatusInFlight = true;
  const status = document.getElementById('status');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2500);
  try {
    const r = await fetch('/api/admin/status', { signal: controller.signal });
    if (!r.ok) throw new Error('status ' + r.status);
    const data = await r.json();
    adminStatusFailures = 0;
    adminFrontendNodes = data.frontend_nodes || [];
    renderBackend(data.backend_nodes || []);
    renderFrontend(data.frontend_nodes || []);
    renderTablets(data.tablets || []);
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
void FEServer::handle_sse(int fd, const HttpRequest&, const std::string& user) {
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

    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

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

const std::vector<std::string>& chat_room_names() {
    static const std::vector<std::string> kRooms = {"general", "team", "random"};
    return kRooms;
}

bool valid_chat_room(const std::string& room) {
    const auto& rooms = chat_room_names();
    return std::find(rooms.begin(), rooms.end(), room) != rooms.end();
}

std::string chat_room_row(const std::string& room) { return "chat:room:" + room; }
std::string chat_message_row(const std::string& room, const std::string& id) { return "chat:msg:" + room + ":" + id; }

bool append_chat_message_id(KVClient* kv, const std::string& room, const std::string& id) {
    const std::string row = chat_room_row(room);
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_ids = kv->get_str(row, "ids");
        auto ids = chat_split_csv(old_ids);
        ids.push_back(id);
        if (ids.size() > 200) ids.erase(ids.begin(), ids.begin() + (ids.size() - 200));
        std::string next = chat_join_csv(ids);
        if (old_ids.empty()) {
            if (kv->put(row, "ids", next)) return true;
        } else {
            if (kv->cput(row, "ids", old_ids, next)) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

std::string dm_canon_peer(const std::string& a, const std::string& b) {
    return a < b ? (a + "|" + b) : (b + "|" + a);
}

std::string chat_dm_row(const std::string& user) { return "chat:dm:user:" + user; }
std::string chat_dm_thread_row(const std::string& canon) { return "chat:dm:thread:" + canon; }
std::string chat_dm_message_row(const std::string& canon, const std::string& id) { return "chat:dm:msg:" + canon + ":" + id; }

bool append_dm_peer(KVClient* kv, const std::string& user, const std::string& peer) {
    const std::string row = chat_dm_row(user);
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_peers = kv->get_str(row, "peers");
        auto peers = chat_split_csv(old_peers);
        peers.erase(std::remove(peers.begin(), peers.end(), peer), peers.end());
        peers.insert(peers.begin(), peer);
        if (peers.size() > 100) peers.resize(100);
        std::string next = chat_join_csv(peers);
        if (old_peers.empty()) {
            if (kv->put(row, "peers", next)) return true;
        } else {
            if (kv->cput(row, "peers", old_peers, next)) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

bool append_dm_message_id(KVClient* kv, const std::string& canon, const std::string& id) {
    const std::string row = chat_dm_thread_row(canon);
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_ids = kv->get_str(row, "ids");
        auto ids = chat_split_csv(old_ids);
        ids.push_back(id);
        if (ids.size() > 200) ids.erase(ids.begin(), ids.begin() + (ids.size() - 200));
        std::string next = chat_join_csv(ids);
        if (old_ids.empty()) {
            if (kv->put(row, "ids", next)) return true;
        } else {
            if (kv->cput(row, "ids", old_ids, next)) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

std::string chat_timestamp_now() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    char buf[64];
    std::strftime(buf, sizeof(buf), "%b %d, %Y %H:%M", &tm);
    return buf;
}

}  // namespace

namespace {

constexpr const char* kDriveRootCol = "root";

std::string drive_obj_row(const std::string& uid) { return "drive:obj:" + uid; }
std::string drive_dir_row(const std::string& uid) { return "drive:dir:" + uid; }
std::string drive_file_row(const std::string& uid) { return "drive:file:" + uid; }
std::string drive_user_row(const std::string& user) { return user + ":drive"; }

constexpr size_t kDriveChunkSize = 1024 * 1024;  // 1MB chunks

std::string drive_file_chunk_col(size_t idx) { return "chunk:" + std::to_string(idx); }

std::string get_file_bytes(KVClient* kv, const std::string& uid);
std::vector<std::string> split_csv(const std::string& s);
std::string join_csv(const std::vector<std::string>& items);
bool ensure_drive_root(KVClient* kv, const std::string& user, std::string& root_uid);

std::string drive_quota_col() { return "quota_bytes"; }
constexpr size_t kDefaultDriveQuotaBytes = 50ull * 1024ull * 1024ull;

size_t parse_size_t_or(const std::string& s, size_t fallback) {
    if (s.empty()) return fallback;
    try { return static_cast<size_t>(std::stoull(s)); } catch (...) { return fallback; }
}

size_t user_drive_quota_bytes(KVClient* kv, const std::string& user) {
    return parse_size_t_or(kv->get_str(drive_user_row(user), drive_quota_col()), kDefaultDriveQuotaBytes);
}

size_t subtree_file_bytes(KVClient* kv, const std::string& uid) {
    std::string type = kv->get_str(drive_obj_row(uid), "type");
    if (type == "file") {
        return parse_size_t_or(kv->get_str(drive_obj_row(uid), "size"), get_file_bytes(kv, uid).size());
    }
    size_t total = 0;
    for (const auto& child : split_csv(kv->get_str(drive_dir_row(uid), "children"))) {
        total += subtree_file_bytes(kv, child);
    }
    return total;
}

size_t user_drive_used_bytes(KVClient* kv, const std::string& user) {
    std::string root_uid;
    if (!ensure_drive_root(kv, user, root_uid)) return 0;
    return subtree_file_bytes(kv, root_uid);
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

std::string join_path(const std::string& parent, const std::string& name) {
    if (parent.empty() || parent == "/") return "/" + name;
    return parent + "/" + name;
}

bool valid_component(const std::string& name) {
    return !name.empty() && name.find('/') == std::string::npos && name != "." && name != "..";
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

bool append_child(KVClient* kv, const std::string& dir_uid, const std::string& child_uid) {
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_children = kv->get_str(drive_dir_row(dir_uid), "children");
        auto kids = split_csv(old_children);
        if (std::find(kids.begin(), kids.end(), child_uid) == kids.end()) kids.push_back(child_uid);
        std::string next = join_csv(kids);
        if (old_children.empty()) {
            if (kv->put(drive_dir_row(dir_uid), "children", next)) return true;
        } else {
            if (kv->cput(drive_dir_row(dir_uid), "children", old_children, next)) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

bool remove_child(KVClient* kv, const std::string& dir_uid, const std::string& child_uid) {
    for (int attempt = 0; attempt < 8; ++attempt) {
        std::string old_children = kv->get_str(drive_dir_row(dir_uid), "children");
        auto kids = split_csv(old_children);
        kids.erase(std::remove(kids.begin(), kids.end(), child_uid), kids.end());
        std::string next = join_csv(kids);
        if (old_children == next) return true;
        if (old_children.empty()) return true;
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

std::string build_path(KVClient* kv, const std::string& uid) {
    if (uid.empty()) return "/";
    std::vector<std::string> names;
    std::string cur = uid;
    while (!cur.empty()) {
        std::string name = kv->get_str(drive_obj_row(cur), "name");
        std::string parent = kv->get_str(drive_obj_row(cur), "parent");
        if (parent.empty()) break;
        names.push_back(name);
        cur = parent;
    }
    std::string path = "/";
    for (auto it = names.rbegin(); it != names.rend(); ++it) {
        if (path.size() > 1) path += "/";
        path += *it;
    }
    return path;
}

bool collect_subtree(KVClient* kv, const std::string& uid, std::vector<std::string>& out) {
    out.push_back(uid);
    if (kv->get_str(drive_obj_row(uid), "type") != "folder") return true;
    for (const auto& child : split_csv(kv->get_str(drive_dir_row(uid), "children"))) {
        if (!collect_subtree(kv, child, out)) return false;
    }
    return true;
}

bool is_descendant_of(KVClient* kv, const std::string& possible_descendant, const std::string& ancestor) {
    std::string cur = possible_descendant;
    while (!cur.empty()) {
        if (cur == ancestor) return true;
        cur = kv->get_str(drive_obj_row(cur), "parent");
    }
    return false;
}

std::string json_drive_item(KVClient* kv, const std::string& uid) {
    std::string type = kv->get_str(drive_obj_row(uid), "type");
    std::string name = kv->get_str(drive_obj_row(uid), "name");
    std::string path = build_path(kv, uid);
    std::string size = kv->get_str(drive_obj_row(uid), "size");
    std::string json = "{";
    json += "\"uid\":" + json_str(uid);
    json += ",\"name\":" + json_str(name);
    json += ",\"type\":" + json_str(type);
    json += ",\"path\":" + json_str(path);
    if (type == "file" && !size.empty()) json += ",\"size\":" + json_str(size);
    json += "}";
    return json;
}

}  // namespace

HttpResponse FEServer::handle_chat_rooms(const HttpRequest&, const std::string&) {
    std::string rooms = "[";
    std::string counts = "{";
    bool first = true;
    bool first_count = true;
    for (const auto& room : chat_room_names()) {
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

HttpResponse FEServer::handle_chat_messages(const HttpRequest& req, const std::string&) {
    std::string room = req.query_params.count("room") ? req.query_params.at("room") : "general";
    if (!valid_chat_room(room)) {
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
    if (!valid_chat_room(room)) return HttpResponse::json(R"({"ok":false,"error":"unknown room"})");
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
    std::sort(ids.begin(), ids.end(), [&](const std::string& a, const std::string& b) {
        std::string ta = kv_->get_str(drive_obj_row(a), "type");
        std::string tb = kv_->get_str(drive_obj_row(b), "type");
        if (ta != tb) return ta == "folder";
        return kv_->get_str(drive_obj_row(a), "name") < kv_->get_str(drive_obj_row(b), "name");
    });

    std::string json = "[";
    bool first = true;
    for (const auto& uid : ids) {
        if (!first) json += ',';
        first = false;
        json += json_drive_item(kv_.get(), uid);
    }
    json += "]";
    return HttpResponse::json("{\"ok\":true,\"items\":" + json + "}");
}

HttpResponse FEServer::handle_upload(const HttpRequest& req, const std::string& user) {
    if (!req.is_multipart()) {
        return HttpResponse::json(R"({"ok":false,"error":"expected multipart upload"})");
    }

    auto parts = parse_multipart(req.body, req.header("content-type"));
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
    bool ok =
        kv_->put(drive_obj_row(uid), "type", "file") &&
        kv_->put(drive_obj_row(uid), "name", file_part.filename) &&
        kv_->put(drive_obj_row(uid), "parent", parent_uid) &&
        kv_->put(drive_obj_row(uid), "owner", user) &&
        kv_->put(drive_obj_row(uid), "size", std::to_string(file_part.data.size())) &&
        put_file_chunks(kv_.get(), uid, file_part.data) &&
        append_child(kv_.get(), parent_uid, uid);

    if (!ok) {
        kv_->del(drive_obj_row(uid), "type");
        kv_->del(drive_obj_row(uid), "name");
        kv_->del(drive_obj_row(uid), "parent");
        kv_->del(drive_obj_row(uid), "owner");
        kv_->del(drive_obj_row(uid), "size");
        delete_file_bytes(kv_.get(), uid);
        return HttpResponse::json(R"({"ok":false,"error":"upload failed"})");
    }

    std::string path = join_path(parent_path, file_part.filename);
    return HttpResponse::json("{\"ok\":true,\"uid\":" + json_str(uid) + ",\"path\":" + json_str(path) + "}");
}

HttpResponse FEServer::handle_download(const HttpRequest&, const std::string& user,
                                        const std::string& uid) {
    std::string owner = kv_->get_str(drive_obj_row(uid), "owner");
    std::string type = kv_->get_str(drive_obj_row(uid), "type");
    if (owner != user || type != "file") return HttpResponse::not_found();

    HttpResponse resp = HttpResponse::ok(get_file_bytes(kv_.get(), uid), "application/octet-stream");
    std::string name = kv_->get_str(drive_obj_row(uid), "name");
    if (!name.empty()) resp.headers["Content-Disposition"] = "attachment; filename=\"" + name + "\"";
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
    return HttpResponse::json("{\"ok\":true,\"path\":" + json_str(join_path(parent_path(path), new_name)) + "}");
}

HttpResponse FEServer::handle_move(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string src_path = params["path"];
    std::string dst_path = params["dst"];
    if (src_path.empty() || dst_path.empty() || src_path == "/") {
        return HttpResponse::json(R"({"ok":false,"error":"invalid move request"})");
    }

    std::string uid, type, dst_uid, dst_type;
    if (!resolve_path(kv_.get(), user, src_path, uid, &type) ||
        !resolve_path(kv_.get(), user, dst_path, dst_uid, &dst_type) || dst_type != "folder") {
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
        return HttpResponse::json("{\"ok\":true,\"path\":" + json_str(join_path(dst_path, name)) + "}");
    }
    if (!append_child(kv_.get(), dst_uid, uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"move failed"})");
    }
    if (!kv_->put(drive_obj_row(uid), "parent", dst_uid)) {
        remove_child(kv_.get(), dst_uid, uid);
        return HttpResponse::json(R"({"ok":false,"error":"move failed"})");
    }
    if (!remove_child(kv_.get(), old_parent, uid)) {
        kv_->put(drive_obj_row(uid), "parent", old_parent);
        remove_child(kv_.get(), dst_uid, uid);
        return HttpResponse::json(R"({"ok":false,"error":"move failed"})");
    }
    return HttpResponse::json("{\"ok\":true,\"path\":" + json_str(join_path(dst_path, name)) + "}");
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
    if (!kv_->put(drive_obj_row(uid), "type", "folder") ||
        !kv_->put(drive_obj_row(uid), "name", name) ||
        !kv_->put(drive_obj_row(uid), "parent", parent_uid) ||
        !kv_->put(drive_obj_row(uid), "owner", user) ||
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

    std::string parent_uid = kv_->get_str(drive_obj_row(uid), "parent");
    if (!remove_child(kv_.get(), parent_uid, uid)) {
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
        if (obj_type == "folder") {
            kv_->del(drive_dir_row(obj), "children");
        } else {
            delete_file_bytes(kv_.get(), obj);
        }
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
    if (limit_mb == 0 || limit_mb > 1024) {
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
