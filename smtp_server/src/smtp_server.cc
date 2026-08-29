#include "../../frontend/src/kv_client.h"
#include "smtp_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <cstdlib>

static std::atomic<bool> g_running{true};

static void sig_handler(int) {
    g_running = false;
}

static bool write_all_fd(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

static bool read_line_fd(int fd, std::string& line) {
    line.clear();
    char c = 0;
    while (true) {
        ssize_t r = ::recv(fd, &c, 1, 0);
        if (r <= 0) return false;
        line.push_back(c);
        if (line.size() >= 2 &&
            line[line.size() - 2] == '\r' &&
            line[line.size() - 1] == '\n') {
            line.resize(line.size() - 2);
            return true;
        }
        if (line.size() > 16384) return false;
    }
}

static std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

static std::string lower_copy(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

static std::string ncloud_mail_domain() {
    const char* env = std::getenv("NCLOUD_MAIL_DOMAIN");
    std::string domain = env && *env ? env : "ncloud.local";
    domain = lower_copy(domain);
    if (domain.empty() || domain.find('@') != std::string::npos ||
        domain.find('/') != std::string::npos ||
        domain.find('\r') != std::string::npos ||
        domain.find('\n') != std::string::npos) {
        return "ncloud.local";
    }
    return domain;
}

static bool is_local_ncloud_domain(const std::string& domain_raw) {
    std::string domain = lower_copy(domain_raw);
    return domain == "ncloud" ||
           domain == "ncloud.com" ||
           domain == "ncloud.local" ||
           domain == ncloud_mail_domain();
}

static std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    if (s.empty()) return out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        if (!tok.empty()) out.push_back(tok);
    }
    return out;
}

static std::string join_csv(const std::vector<std::string>& items) {
    std::string out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i) out += ',';
        out += items[i];
    }
    return out;
}

static bool starts_with_ci(const std::string& s, const std::string& pfx) {
    if (s.size() < pfx.size()) return false;
    return lower_copy(s.substr(0, pfx.size())) == lower_copy(pfx);
}

static std::string strip_angle(const std::string& s) {
    std::string t = trim(s);
    if (!t.empty() && t.front() == '<') t.erase(t.begin());
    if (!t.empty() && t.back() == '>') t.pop_back();
    return trim(t);
}

static std::string extract_path_arg(const std::string& line, const std::string& key) {
    auto pos = lower_copy(line).find(lower_copy(key));
    if (pos == std::string::npos) return "";
    return strip_angle(line.substr(pos + key.size()));
}

static std::string new_uid() {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
    std::mt19937 rng(static_cast<unsigned>(ns));
    std::uniform_int_distribution<int> dist(0, 0xFFFF);
    std::ostringstream oss;
    oss << ns << "_" << std::hex << std::setw(4) << std::setfill('0') << dist(rng);
    return oss.str();
}

static std::string now_str() {
    auto t = std::time(nullptr);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    char buf[64];
    std::strftime(buf, sizeof(buf), "%b %d, %Y %H:%M", &tm);
    return buf;
}

static std::string esc_json(const std::string& s) {
    std::string out;
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
    return out;
}

static std::string make_meta_json(const std::string& from,
                                  const std::string& to,
                                  const std::string& subject,
                                  const std::string& time_str,
                                  const std::string& uid) {
    return std::string("{")
         + "\"uid\":\""     + esc_json(uid)      + "\","
         + "\"from\":\""    + esc_json(from)     + "\","
         + "\"to\":\""      + esc_json(to)       + "\","
         + "\"subject\":\"" + esc_json(subject)  + "\","
         + "\"time\":\""    + esc_json(time_str) + "\""
         + "}";
}

static std::string extract_local_user(const std::string& addr) {
    auto at = addr.find('@');
    if (at == std::string::npos) return trim(addr);
    std::string local = addr.substr(0, at);
    std::string domain = lower_copy(addr.substr(at + 1));
    if (!is_local_ncloud_domain(domain)) return "";
    return trim(local);
}

static bool is_external_recipient(const std::string& addr) {
    auto at = addr.find('@');
    if (at == std::string::npos || at == 0 || at + 1 >= addr.size()) return false;
    std::string domain = lower_copy(addr.substr(at + 1));
    return !is_local_ncloud_domain(domain);
}

static bool parse_subject_and_body(const std::string& raw_data,
                                   std::string& subject_out,
                                   std::string& body_out) {
    subject_out = "(no subject)";
    body_out.clear();

    std::istringstream ss(raw_data);
    std::string line;
    bool in_headers = true;
    bool first_body = true;

    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();

        if (in_headers) {
            if (line.empty()) {
                in_headers = false;
                continue;
            }
            if (starts_with_ci(line, "Subject:")) {
                subject_out = trim(line.substr(8));
            }
        } else {
            if (!first_body) body_out += "\n";
            first_body = false;
            body_out += line;
        }
    }
    return true;
}

class SMTPServer {
public:
    struct Config {
        int port = 2525;
        std::string kv_host = "127.0.0.1";
        int kv_port = 5000;
        std::string coord_host = "127.0.0.1";
        int coord_port = 0;
        int threads = 16;
    };

    explicit SMTPServer(const Config& cfg)
        : cfg_(cfg),
          kv_(cfg.coord_port > 0
                  ? KVClient(cfg.kv_host, cfg.kv_port, cfg.coord_host, cfg.coord_port)
                  : KVClient(cfg.kv_host, cfg.kv_port)) {}

    void run() {
        ::signal(SIGINT, sig_handler);
        ::signal(SIGTERM, sig_handler);
        ::signal(SIGPIPE, SIG_IGN);

        listen_fd_ = create_listen_socket();
        std::cout << "=== NCloud SMTP Server ===\n"
                  << "  port:    " << cfg_.port << "\n"
                  << "  kv:      " << cfg_.kv_host << ":" << cfg_.kv_port << "\n"
                  << "  coord:   "
                  << (cfg_.coord_port > 0 ? cfg_.coord_host + ":" + std::to_string(cfg_.coord_port) : "disabled")
                  << "\n\n";

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

        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

private:
    struct Session {
        bool greeted = false;
        std::string helo_name;
        std::string mail_from;
        std::vector<std::string> rcpt_to;
        std::string data;
    };

    Config cfg_;
    KVClient kv_;
    int listen_fd_ = -1;
    std::mutex kv_mu_;

    int create_listen_socket() {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("smtp socket() failed");

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(fd);
            throw std::runtime_error("smtp bind() failed");
        }
        if (::listen(fd, 64) < 0) {
            ::close(fd);
            throw std::runtime_error("smtp listen() failed");
        }
        return fd;
    }

    void reset_message_state(Session& s) {
        s.mail_from.clear();
        s.rcpt_to.clear();
        s.data.clear();
    }

    bool deliver_local(const std::string& from_addr,
                       const std::string& rcpt_addr,
                       const std::string& raw_data) {
        std::string recipient = extract_local_user(rcpt_addr);
        if (recipient.empty()) return false;

        // Optional existence check.
        std::string pwd;
        KVReadStatus st = kv_.get_status(recipient, "pwd", pwd);
        if (st != KVReadStatus::Found) return false;

        std::string subject, body;
        parse_subject_and_body(raw_data, subject, body);

        std::string uid = new_uid();
        std::string time_str = now_str();
        std::string meta = make_meta_json(from_addr, rcpt_addr, subject, time_str, uid);
        std::string row = recipient + ":mail";

        std::lock_guard<std::mutex> lk(kv_mu_);

        if (!kv_.put(row, "msg:" + uid, meta)) return false;
        if (!kv_.put(row, "body:" + uid, body)) return false;

        bool indexed = false;
        const std::string folder_col = "folder:inbox";
        std::string final_index;
        for (int attempt = 0; attempt < 5; ++attempt) {
            std::string old_index;
            KVReadStatus st = kv_.get_status(row, folder_col, old_index);
            if (st == KVReadStatus::Unavailable) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            if (st == KVReadStatus::NotFound) {
                old_index = kv_.get_str(row, "index");
                if (old_index.empty()) old_index = kv_.get_str(row, "inbox");
            }

            auto ids = split_csv(old_index);
            ids.erase(std::remove(ids.begin(), ids.end(), uid), ids.end());
            ids.insert(ids.begin(), uid);
            std::string new_index = join_csv(ids);

            if (st == KVReadStatus::Found) {
                if (new_index == old_index || kv_.cput(row, folder_col, old_index, new_index)) {
                    final_index = new_index;
                    indexed = true;
                    break;
                }
            } else {
                (void)kv_.put(row, folder_col, new_index);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        if (!indexed) {
            kv_.del(row, "msg:" + uid);
            kv_.del(row, "body:" + uid);
            return false;
        }

        if (!final_index.empty()) {
            kv_.put(row, "inbox", final_index);
            kv_.put(row, "index", final_index);
        }
        kv_.put("notify:" + recipient, "latest", "uid:" + uid);
        return true;
    }

    bool handle_data_mode(int fd, Session& s) {
        if (!write_all_fd(fd, "354 End data with <CR><LF>.<CR><LF>\r\n")) return false;

        std::string raw;
        std::string line;
        while (true) {
            if (!read_line_fd(fd, line)) return false;
            if (line == ".") break;

            // RFC dot-stuffing: ".." at start becomes "."
            if (!line.empty() && line[0] == '.' && line.size() >= 2 && line[1] == '.') {
                line.erase(line.begin());
            }
            raw += line;
            raw += "\r\n";
        }

        std::string subject, body;
        parse_subject_and_body(raw, subject, body);

        bool all_ok = true;
        for (const auto& rcpt : s.rcpt_to) {
            if (is_external_recipient(rcpt)) {
                SMTPExternalResult r = smtp_send_external(s.mail_from, rcpt, subject, body);
                if (r.ok) std::cerr << "[smtp-out] delivered to " << rcpt << "\n";
                else {
                    std::cerr << "[smtp-out] failed to " << rcpt << " error=" << r.error << "\n";
                    all_ok = false;
                }
            } else if (!deliver_local(s.mail_from, rcpt, raw)) {
                all_ok = false;
            }
        }

        reset_message_state(s);

        if (all_ok) return write_all_fd(fd, "250 OK\r\n");
        return write_all_fd(fd, "550 Delivery failed\r\n");
    }

    void handle_client(int fd) {
        Session s;
        if (!write_all_fd(fd, "220 NCloud SMTP Ready\r\n")) return;

        std::string line;
        while (g_running && read_line_fd(fd, line)) {
            if (starts_with_ci(line, "HELO ")) {
                s.greeted = true;
                s.helo_name = trim(line.substr(5));
                write_all_fd(fd, "250 Hello\r\n");

            } else if (starts_with_ci(line, "EHLO ")) {
                s.greeted = true;
                s.helo_name = trim(line.substr(5));
                write_all_fd(fd,
                    "250-NCloud\r\n"
                    "250-8BITMIME\r\n"
                    "250 HELP\r\n");

            } else if (starts_with_ci(line, "MAIL FROM:")) {
                if (!s.greeted) {
                    write_all_fd(fd, "503 Send HELO/EHLO first\r\n");
                    continue;
                }
                std::string addr = extract_path_arg(line, "MAIL FROM:");
                if (addr.empty()) {
                    write_all_fd(fd, "501 Bad MAIL FROM\r\n");
                    continue;
                }
                reset_message_state(s);
                s.mail_from = addr;
                write_all_fd(fd, "250 OK\r\n");

            } else if (starts_with_ci(line, "RCPT TO:")) {
                if (s.mail_from.empty()) {
                    write_all_fd(fd, "503 Need MAIL FROM first\r\n");
                    continue;
                }
                std::string addr = extract_path_arg(line, "RCPT TO:");
                std::string user = extract_local_user(addr);
                if (addr.empty() || (user.empty() && !is_external_recipient(addr))) {
                    write_all_fd(fd, "550 Bad recipient\r\n");
                    continue;
                }
                if (!user.empty()) {
                    std::string pwd;
                    KVReadStatus st = kv_.get_status(user, "pwd", pwd);
                    if (st == KVReadStatus::Unavailable) {
                        write_all_fd(fd, "451 Local storage temporarily unavailable\r\n");
                        continue;
                    }
                    if (st == KVReadStatus::NotFound) {
                        write_all_fd(fd, "550 No such local user\r\n");
                        continue;
                    }
                }
                s.rcpt_to.push_back(addr);
                write_all_fd(fd, "250 OK\r\n");

            } else if (starts_with_ci(line, "DATA")) {
                if (s.rcpt_to.empty()) {
                    write_all_fd(fd, "503 Need RCPT TO first\r\n");
                    continue;
                }
                if (!handle_data_mode(fd, s)) return;

            } else if (starts_with_ci(line, "RSET")) {
                reset_message_state(s);
                write_all_fd(fd, "250 OK\r\n");

            } else if (starts_with_ci(line, "NOOP")) {
                write_all_fd(fd, "250 OK\r\n");

            } else if (starts_with_ci(line, "QUIT")) {
                write_all_fd(fd, "221 Bye\r\n");
                return;

            } else {
                write_all_fd(fd, "500 Unsupported command\r\n");
            }
        }
    }
};

int main(int argc, char* argv[]) {
    SMTPServer::Config cfg;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) {
            cfg.port = std::stoi(argv[++i]);
        } else if (a == "--kv-host" && i + 1 < argc) {
            cfg.kv_host = argv[++i];
        } else if (a == "--kv-port" && i + 1 < argc) {
            cfg.kv_port = std::stoi(argv[++i]);
        } else if (a == "--coord-host" && i + 1 < argc) {
            cfg.coord_host = argv[++i];
        } else if (a == "--coord-port" && i + 1 < argc) {
            cfg.coord_port = std::stoi(argv[++i]);
        }
    }

    SMTPServer server(cfg);
    server.run();
    return 0;
}
