// smtp_client.h -- outbound SMTP client
//
// Modes:
//   direct -> direct MX delivery on port 25
//
// Environment variables:
//   SMTP_MODE=relay|direct
//
// Notes:
// - The course container has no libcurl/OpenSSL, so Gmail-style STARTTLS relay
//   is intentionally not compiled in. If SMTP_MODE=relay is set accidentally,
//   the sender falls back to direct MX delivery.

#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <resolv.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstring>
#include <ctime>
#include <cstdlib>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <iostream>

struct SMTPExternalResult {
    bool ok = false;
    std::string error;
};

namespace smtp_client_detail {

inline const char* env_or_null(const char* k) {
    const char* v = std::getenv(k);
    return (v && *v) ? v : nullptr;
}

inline std::string trim(std::string s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

inline std::string lower_copy(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

inline std::string recipient_domain_for(const std::string& addr) {
    auto at = addr.find('@');
    if (at == std::string::npos || at + 1 >= addr.size()) return "";
    return lower_copy(trim(addr.substr(at + 1)));
}

inline bool domain_matches_direct_list(const std::string& domain, const char* csv) {
    if (domain.empty() || !csv || !*csv) return false;
    std::istringstream ss(csv);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        tok = lower_copy(trim(tok));
        if (tok.empty()) continue;
        if (domain == tok) return true;
        if (domain.size() > tok.size() &&
            domain.compare(domain.size() - tok.size(), tok.size(), tok) == 0 &&
            domain[domain.size() - tok.size() - 1] == '.') {
            return true;
        }
    }
    return false;
}

inline bool env_flag_enabled(const char* key, bool fallback) {
    const char* v = env_or_null(key);
    if (!v) return fallback;
    std::string s = lower_copy(trim(v));
    return s == "1" || s == "true" || s == "yes" || s == "on";
}

inline std::string sanitize_header_value(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    bool last_space = false;
    for (char c : s) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (c == '\r' || c == '\n') {
            if (!last_space) out.push_back(' ');
            last_space = true;
        } else if (uc < 32 && c != '\t') {
            continue;
        } else {
            out.push_back(c);
            last_space = std::isspace(uc) != 0;
        }
    }
    return trim(out);
}

inline bool is_safe_mailbox(const std::string& addr) {
    if (addr.empty() || addr.size() > 254) return false;
    if (addr.front() == '.' || addr.back() == '.') return false;
    auto at = addr.find('@');
    if (at == std::string::npos || at == 0 || at + 1 >= addr.size()) return false;
    if (addr.find('@', at + 1) != std::string::npos) return false;
    for (char c : addr) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (uc <= 32 || c == '<' || c == '>' || c == '"' || c == '\\' || c == '\r' || c == '\n') {
            return false;
        }
    }
    return true;
}

inline bool starts_with(const std::string& s, const std::string& p) {
    return s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
}

inline std::string rfc2822_date_now() {
    std::time_t t = std::time(nullptr);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    char buf[128];
    std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S %z", &tm);
    return buf;
}

inline std::string message_id_now() {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
    return "<" + std::to_string(ns) + "." + std::to_string(::getpid()) + "@penncloud.com>";
}

inline std::string quote_header_phrase(std::string s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '\\' || c == '"') out.push_back('\\');
        if (c == '\r' || c == '\n') continue;
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

inline std::string sanitize_header_text(std::string s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (c == '\r' || c == '\n') {
            if (!out.empty() && out.back() != ' ') out.push_back(' ');
            continue;
        }
        if (c < 0x20 || c == 0x7F) continue;
        out.push_back(static_cast<char>(c));
    }
    return trim(out);
}

inline std::string relay_display_name_for(const std::string& original_sender) {
    auto at = original_sender.find('@');
    std::string local = at == std::string::npos ? original_sender : original_sender.substr(0, at);
    local = trim(local);
    if (local.empty()) return "PennCloud";
    return "PennCloud " + local;
}

inline std::string domain_part_or(const std::string& addr, const std::string& fallback) {
    auto at = addr.find('@');
    if (at == std::string::npos || at + 1 >= addr.size()) return fallback;
    std::string domain = trim(addr.substr(at + 1));
    return domain.empty() ? fallback : domain;
}

inline std::string message_id_for(const std::string& from_addr) {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    static std::mt19937 rng(static_cast<unsigned>(ns));
    std::uniform_int_distribution<unsigned> dist(0, 0xFFFFFF);
    std::ostringstream oss;
    oss << "<penncloud." << ns << "."
        << std::hex << std::setw(6) << std::setfill('0') << dist(rng)
        << "@" << domain_part_or(from_addr, "localhost") << ">";
    return oss.str();
}

inline std::string build_message(const std::string& from_addr,
                                 const std::string& to_addr,
                                 const std::string& subject,
                                 const std::string& body,
                                 const std::string& reply_to_addr = "",
                                 const std::string& from_display = "",
                                 const std::string& original_sender = "") {
    std::ostringstream oss;
    oss << "From: ";
    if (!from_display.empty()) oss << quote_header_phrase(from_display) << " ";
    oss << "<" << from_addr << ">\r\n";
    if (!reply_to_addr.empty() && reply_to_addr != from_addr) {
        oss << "Reply-To: <" << reply_to_addr << ">\r\n";
    }
    if (!original_sender.empty() && original_sender != from_addr) {
        oss << "X-PennCloud-From: <" << original_sender << ">\r\n";
    }
    oss << "To: <" << to_addr << ">\r\n";
    oss << "Subject: " << sanitize_header_text(subject) << "\r\n";
    oss << "Date: " << rfc2822_date_now() << "\r\n";
    oss << "Message-ID: " << message_id_for(from_addr) << "\r\n";
    oss << "X-Mailer: PennCloud\r\n";
    oss << "MIME-Version: 1.0\r\n";
    oss << "Content-Type: text/plain; charset=UTF-8\r\n";
    oss << "Content-Transfer-Encoding: 8bit\r\n";
    oss << "\r\n";
    oss << body << "\r\n";
    return oss.str();
}

inline std::string dot_stuff_data(const std::string& msg) {
    std::string out;
    out.reserve(msg.size() + 16);
    bool at_line_start = true;
    for (char c : msg) {
        if (at_line_start && c == '.') out.push_back('.');
        out.push_back(c);
        at_line_start = (c == '\n');
    }
    return out;
}

inline SMTPExternalResult smtp_send_via_relay(const std::string& /*from_addr*/,
                                              const std::string& /*to_addr*/,
                                              const std::string& /*subject*/,
                                              const std::string& /*body*/) {
    SMTPExternalResult res;
    res.error = "SMTP relay mode is disabled in the course build; use direct MX delivery";
    return res;
}

// Direct MX fallback path

struct MXRecord {
    int preference = 0;
    std::string host;
};

inline bool write_all(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

inline bool read_line(int fd, std::string& line) {
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
        if (line.size() > 16384) return false;
    }
}

inline bool read_smtp_reply(int fd, int& code_out, std::string& full_out) {
    full_out.clear();
    std::string line;
    bool first = true;
    int code = -1;

    while (true) {
        if (!read_line(fd, line)) return false;
        if (!full_out.empty()) full_out += "\n";
        full_out += line;

        if (line.size() < 3 ||
            !std::isdigit(static_cast<unsigned char>(line[0])) ||
            !std::isdigit(static_cast<unsigned char>(line[1])) ||
            !std::isdigit(static_cast<unsigned char>(line[2]))) {
            return false;
        }

        int cur = (line[0] - '0') * 100 + (line[1] - '0') * 10 + (line[2] - '0');
        if (first) {
            code = cur;
            first = false;
        }

        if (line.size() >= 4 && line[3] == '-') continue;
        code_out = code;
        return true;
    }
}

inline bool expect_code_class(int fd, int want_class, std::string* err = nullptr) {
    int code = 0;
    std::string reply;
    if (!read_smtp_reply(fd, code, reply)) {
        if (err) *err = "failed reading SMTP reply";
        return false;
    }
    if (code / 100 != want_class) {
        if (err) *err = reply;
        return false;
    }
    return true;
}

inline bool send_cmd_expect(int fd, const std::string& cmd, int want_class, std::string* err = nullptr) {
    if (!write_all(fd, cmd + "\r\n")) {
        if (err) *err = "failed sending SMTP command";
        return false;
    }
    return expect_code_class(fd, want_class, err);
}

inline int connect_host_port(const std::string& host, int port, int timeout_ms = 1000) {
    struct addrinfo hints{}, *res = nullptr, *rp = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_s = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res) != 0) return -1;

    int fd = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) continue;

        int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags >= 0) ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        bool connected = false;
        int rc = ::connect(fd, rp->ai_addr, rp->ai_addrlen);
        if (rc == 0) {
            connected = true;
        } else if (errno == EINPROGRESS) {
            fd_set wfds;
            FD_ZERO(&wfds);
            FD_SET(fd, &wfds);
            timeval tv{};
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            rc = ::select(fd + 1, nullptr, &wfds, nullptr, &tv);
            if (rc > 0 && FD_ISSET(fd, &wfds)) {
                int so_error = 0;
                socklen_t len = sizeof(so_error);
                connected = (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0 && so_error == 0);
            }
        }

        if (connected) {
            if (flags >= 0) ::fcntl(fd, F_SETFL, flags);
            timeval tv{};
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            break;
        }

        ::close(fd);
        fd = -1;
    }

    if (res) ::freeaddrinfo(res);
    return fd;
}

inline std::vector<MXRecord> lookup_mx(const std::string& domain) {
    std::vector<MXRecord> out;

    unsigned char answer[NS_PACKETSZ * 4];
    int len = res_query(domain.c_str(), ns_c_in, ns_t_mx, answer, sizeof(answer));
    if (len < 0) return out;

    ns_msg handle;
    if (ns_initparse(answer, len, &handle) < 0) return out;

    int count = ns_msg_count(handle, ns_s_an);
    for (int i = 0; i < count; ++i) {
        ns_rr rr;
        if (ns_parserr(&handle, ns_s_an, i, &rr) < 0) continue;
        if (ns_rr_type(rr) != ns_t_mx) continue;

        const unsigned char* rdata = ns_rr_rdata(rr);
        int pref = ns_get16(rdata);

        char host[NS_MAXDNAME];
        if (dn_expand(answer, answer + len, rdata + 2, host, sizeof(host)) < 0) continue;

        std::string mxh = host;
        if (!mxh.empty() && mxh.back() == '.') mxh.pop_back();
        out.push_back({pref, mxh});
    }

    std::sort(out.begin(), out.end(), [](const MXRecord& a, const MXRecord& b) {
        if (a.preference != b.preference) return a.preference < b.preference;
        return a.host < b.host;
    });
    return out;
}

inline bool try_send_once(const std::string& mx_host,
                          const std::string& from_addr,
                          const std::string& to_addr,
                          const std::string& subject,
                          const std::string& body,
                          std::string& err_out,
                          bool& temp_fail_out) {
    temp_fail_out = false;
    if (!is_safe_mailbox(from_addr) || !is_safe_mailbox(to_addr)) {
        err_out = "invalid SMTP address";
        return false;
    }

    std::cerr << "[smtp] trying " << mx_host << ":25\n";

    int fd = connect_host_port(mx_host, 25, 5000);
    if (fd < 0) {
        err_out = "connect failed to " + mx_host + ":25";
        return false;
    }

    auto finish = [&](bool ok) {
        ::close(fd);
        return ok;
    };

    int code = 0;
    std::string reply;
    if (!read_smtp_reply(fd, code, reply)) {
        err_out = "no greeting from " + mx_host;
        return finish(false);
    }
    if (code / 100 != 2) {
        err_out = reply;
        temp_fail_out = (code / 100 == 4);
        return finish(false);
    }

    if (!send_cmd_expect(fd, "EHLO penncloud", 2, &reply)) {
        if (!send_cmd_expect(fd, "HELO penncloud", 2, &reply)) {
            err_out = reply;
            return finish(false);
        }
    }

    if (!send_cmd_expect(fd, "MAIL FROM:<" + from_addr + ">", 2, &reply)) {
        err_out = reply;
        temp_fail_out = starts_with(reply, "4");
        return finish(false);
    }

    if (!send_cmd_expect(fd, "RCPT TO:<" + to_addr + ">", 2, &reply)) {
        err_out = reply;
        temp_fail_out = starts_with(reply, "4");
        return finish(false);
    }

    if (!send_cmd_expect(fd, "DATA", 3, &reply)) {
        err_out = reply;
        temp_fail_out = starts_with(reply, "4");
        return finish(false);
    }

    std::string msg = dot_stuff_data(build_message(from_addr, to_addr, subject, body));
    if (!write_all(fd, msg + "\r\n.\r\n")) {
        err_out = "failed sending DATA body";
        return finish(false);
    }

    if (!read_smtp_reply(fd, code, reply)) {
        err_out = "no final DATA response";
        return finish(false);
    }
    if (code / 100 != 2) {
        err_out = reply;
        temp_fail_out = (code / 100 == 4);
        return finish(false);
    }

    write_all(fd, "QUIT\r\n");
    read_smtp_reply(fd, code, reply);
    return finish(true);
}

inline SMTPExternalResult smtp_send_direct(const std::string& from_addr,
                                           const std::string& to_addr,
                                           const std::string& subject,
                                           const std::string& body) {
    SMTPExternalResult res;

    auto at = to_addr.find('@');
    if (at == std::string::npos || at + 1 >= to_addr.size() ||
        !is_safe_mailbox(from_addr) || !is_safe_mailbox(to_addr)) {
        res.error = "invalid recipient address";
        return res;
    }

    std::string domain = trim(to_addr.substr(at + 1));
    if (domain.empty()) {
        res.error = "invalid recipient domain";
        return res;
    }

    std::vector<MXRecord> mx = lookup_mx(domain);
    if (mx.empty()) {
        mx.push_back({0, domain});
    }

    if (mx.size() > 3) mx.resize(3);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

    std::string last_err;
    for (const auto& rec : mx) {
        if (std::chrono::steady_clock::now() >= deadline) {
            res.error = "direct MX delivery timed out; configure SMTP_MODE=relay for faster Gmail/SEAS delivery";
            return res;
        }

        bool temp_fail = false;
        if (try_send_once(rec.host, from_addr, to_addr, subject, body, last_err, temp_fail)) {
            res.ok = true;
            return res;
        }

        (void)temp_fail;
    }

    if (last_err.empty()) last_err = "all MX delivery attempts failed";
    res.error = last_err;
    return res;
}

}  // namespace smtp_client_detail

inline SMTPExternalResult smtp_send_external(const std::string& from_addr,
                                             const std::string& to_addr,
                                             const std::string& subject,
                                             const std::string& body) {
    const char* mode = smtp_client_detail::env_or_null("SMTP_MODE");
    const char* direct_domains = smtp_client_detail::env_or_null("SMTP_DIRECT_DOMAINS");
    std::string mode_s = mode ? mode : "direct";

    if (smtp_client_detail::domain_matches_direct_list(
            smtp_client_detail::recipient_domain_for(to_addr), direct_domains)) {
        SMTPExternalResult direct = smtp_client_detail::smtp_send_direct(from_addr, to_addr, subject, body);
        if (direct.ok || mode_s != "relay" ||
            !smtp_client_detail::env_flag_enabled("SMTP_DIRECT_FALLBACK_RELAY", true)) {
            return direct;
        }
        std::cerr << "[smtp] direct delivery failed for " << to_addr
                  << " (" << direct.error << "); falling back to relay\n";
        SMTPExternalResult relay = smtp_client_detail::smtp_send_via_relay(from_addr, to_addr, subject, body);
        if (!relay.ok) {
            relay.error = "direct delivery failed (" + direct.error + "); relay fallback failed (" + relay.error + ")";
        }
        return relay;
    }

    if (mode_s == "relay") {
        std::cerr << "[smtp] SMTP_MODE=relay requested, but this course build has no TLS SMTP relay client; "
                  << "falling back to direct MX delivery\n";
        return smtp_client_detail::smtp_send_direct(from_addr, to_addr, subject, body);
    }
    return smtp_client_detail::smtp_send_direct(from_addr, to_addr, subject, body);
}
