#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

struct FrontendNode {
    std::string id;
    std::string host;
    int port = -1;
    bool alive = false;
};

static std::atomic<bool> g_running{true};

static void sig_handler(int) {
    g_running = false;
}

static int connect_host_port(const std::string& host, int port, int timeout_ms = 500) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

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
        return -1;
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

static bool read_http_request_head(int fd, std::string& out) {
    out.clear();
    char c;
    while (out.size() < 8192) {
        ssize_t r = ::recv(fd, &c, 1, 0);
        if (r <= 0) return false;
        out.push_back(c);
        if (out.size() >= 4 && out.substr(out.size() - 4) == "\r\n\r\n") {
            return true;
        }
    }
    return false;
}

static bool write_all(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

class LoadBalancer {
public:
    struct Config {
        int port = 8088;
        std::string config_file = "./frontend_lb.conf";
        int health_interval_ms = 1000;
    };

    explicit LoadBalancer(Config cfg) : cfg_(std::move(cfg)) {
        load_config(cfg_.config_file);
    }

    void run() {
        ::signal(SIGINT, sig_handler);
        ::signal(SIGTERM, sig_handler);
        ::signal(SIGPIPE, SIG_IGN);

        listen_fd_ = create_listen_socket();
        std::cout << "[lb] listening on port " << cfg_.port << "\n";

        std::thread health_thread([this] { health_loop(); });

        while (g_running) {
            sockaddr_in addr{};
            socklen_t len = sizeof(addr);
            int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
            if (cfd < 0) {
                if (!g_running) break;
                continue;
            }

            std::thread([this, cfd] {
                handle_client(cfd);
                ::close(cfd);
            }).detach();
        }

        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        g_running = false;
        health_thread.join();
    }

private:
    Config cfg_;
    std::vector<FrontendNode> nodes_;
    std::mutex nodes_mu_;
    size_t rr_idx_ = 0;
    int listen_fd_ = -1;

    void load_config(const std::string& path) {
        std::ifstream f(path);
        if (!f) {
            throw std::runtime_error("could not open frontend lb config: " + path);
        }

        std::string line;
        while (std::getline(f, line)) {
            if (line.empty() || line[0] == '#') continue;
            std::istringstream ss(line);
            std::string type;
            ss >> type;
            if (type == "frontend") {
                FrontendNode n;
                ss >> n.id >> n.host >> n.port;
                if (n.id.empty() || n.host.empty() || n.port <= 0) {
                    throw std::runtime_error("bad frontend config line: " + line);
                }
                nodes_.push_back(n);
            }
        }

        if (nodes_.empty()) {
            throw std::runtime_error("frontend lb config contains no frontend nodes");
        }
    }

    int create_listen_socket() {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("lb socket() failed");

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(fd);
            throw std::runtime_error("lb bind() failed");
        }
        if (::listen(fd, 64) < 0) {
            ::close(fd);
            throw std::runtime_error("lb listen() failed");
        }
        return fd;
    }

    void health_loop() {
        while (g_running) {
            {
                std::lock_guard<std::mutex> lk(nodes_mu_);
                for (auto& n : nodes_) {
                    int fd = connect_host_port(n.host, n.port, 300);
                    bool ok = (fd >= 0);
                    if (fd >= 0) ::close(fd);
                    n.alive = ok;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.health_interval_ms));
        }
    }

    bool pick_alive_node(FrontendNode& out) {
        std::lock_guard<std::mutex> lk(nodes_mu_);
        if (nodes_.empty()) return false;

        for (size_t step = 0; step < nodes_.size(); ++step) {
            size_t idx = (rr_idx_ + step) % nodes_.size();
            if (nodes_[idx].alive) {
                out = nodes_[idx];
                rr_idx_ = (idx + 1) % nodes_.size();
                return true;
            }
        }
        return false;
    }

    static std::string extract_path_from_request(const std::string& req) {
        std::istringstream ss(req);
        std::string method, path, version;
        ss >> method >> path >> version;
        if (method.empty() || path.empty()) return "/";
        return path;
    }

    void handle_client(int fd) {
        std::string req;
        if (!read_http_request_head(fd, req)) return;

        std::string path = extract_path_from_request(req);

        FrontendNode chosen;
        if (!pick_alive_node(chosen)) {
            std::string body = "Page Not Found\n";
            std::ostringstream oss;
            oss << "HTTP/1.1 404 Not Found\r\n"
                << "Content-Type: text/plain; charset=utf-8\r\n"
                << "Content-Length: " << body.size() << "\r\n"
                << "Connection: close\r\n\r\n"
                << body;
            write_all(fd, oss.str());
            return;
        }

        std::ostringstream location;
        location << "http://" << chosen.host << ":" << chosen.port << path;

        std::ostringstream resp;
        resp << "HTTP/1.1 307 Temporary Redirect\r\n"
             << "Location: " << location.str() << "\r\n"
             << "Content-Length: 0\r\n"
             << "Connection: close\r\n\r\n";

        write_all(fd, resp.str());
    }
};

int main(int argc, char* argv[]) {
    LoadBalancer::Config cfg;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) {
            cfg.port = std::stoi(argv[++i]);
        } else if (a == "--config" && i + 1 < argc) {
            cfg.config_file = argv[++i];
        }
    }

    std::cout << "=== NCloud Frontend Load Balancer ===\n"
              << "  port:   " << cfg.port << "\n"
              << "  config: " << cfg.config_file << "\n\n";

    LoadBalancer lb(cfg);
    lb.run();
    return 0;
}