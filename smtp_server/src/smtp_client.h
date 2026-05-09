// smtp_client.h -- outbound SMTP client
//
// Modes:
//   1) relay  -> authenticated SMTP relay/submission via libcurl
//   2) direct -> direct MX delivery on port 25 (existing/fallback path)
//
// Environment variables:
//   SMTP_MODE=relay|direct
//
// Relay mode:
//   SMTP_RELAY_HOST=smtp.gmail.com
//   SMTP_RELAY_PORT=587
//   SMTP_RELAY_USER=your_email@gmail.com
//   SMTP_RELAY_PASS=app_password
//   SMTP_RELAY_FROM=your_email@gmail.com
//
// Notes:
// - Relay mode is the practical fix when direct MX port 25 is blocked.
// - For Gmail, use an App Password, not the normal account password. From hw

#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <resolv.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <iostream>

#ifdef USE_CURL_RELAY
#include <curl/curl.h>
#endif

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

inline std::string shell_quote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "'\\''";
        else out.push_back(c);
    }
    out.push_back('\'');
    return out;
}

inline std::string curl_config_quote(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '\\' || c == '"') out.push_back('\\');
        if (c == '\r' || c == '\n') out.push_back(' ');
        else out.push_back(c);
    }
    out.push_back('"');
    return out;
}

inline bool command_available(const std::string& name) {
    std::string cmd = "command -v " + shell_quote(name) + " >/dev/null 2>&1";
    int rc = std::system(cmd.c_str());
    return rc == 0;
}

inline bool make_temp_file(std::string& path_out) {
    char tmpl[] = "/tmp/penncloud_mail_XXXXXX";
    int fd = ::mkstemp(tmpl);
    if (fd < 0) return false;
    ::fchmod(fd, S_IRUSR | S_IWUSR);
    ::close(fd);
    path_out = tmpl;
    return true;
}

inline bool write_text_file(const std::string& path, const std::string& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    return out.good();
}

inline std::string read_small_file(const std::string& path, size_t max_bytes = 1000) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "";
    std::string out;
    char buf[256];
    while (in && out.size() < max_bytes) {
        in.read(buf, static_cast<std::streamsize>(std::min(sizeof(buf), max_bytes - out.size())));
        out.append(buf, static_cast<size_t>(in.gcount()));
    }
    return trim(out);
}

inline int shell_exit_code(int status) {
    if (status < 0) return status;
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return status;
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

inline bool relay_compiled() {
#ifdef USE_CURL_RELAY
    return true;
#else
    return false;
#endif
}

inline bool starts_with(const std::string& s, const std::string& p) {
    return s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
}

// Relay mode via libcurl SMTP submission

struct UploadStatus {
    size_t bytes_read = 0;
    std::string payload;
};

inline size_t curl_payload_source(char* ptr, size_t size, size_t nmemb, void* userp) {
    size_t max = size * nmemb;
    UploadStatus* st = static_cast<UploadStatus*>(userp);
    if (!st || st->bytes_read >= st->payload.size()) return 0;

    size_t remain = st->payload.size() - st->bytes_read;
    size_t take = remain < max ? remain : max;
    std::memcpy(ptr, st->payload.data() + st->bytes_read, take);
    st->bytes_read += take;
    return take;
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

inline SMTPExternalResult smtp_send_via_curl_cli(const std::string& from_addr,
                                                 const std::string& to_addr,
                                                 const std::string& subject,
                                                 const std::string& body) {
    SMTPExternalResult res;

    const char* host = env_or_null("SMTP_RELAY_HOST");
    const char* port_env = env_or_null("SMTP_RELAY_PORT");
    const char* user = env_or_null("SMTP_RELAY_USER");
    const char* pass = env_or_null("SMTP_RELAY_PASS");
    const char* from_env = env_or_null("SMTP_RELAY_FROM");
    const char* reply_to_env = env_or_null("SMTP_REPLY_TO");
    const char* display_env = env_or_null("SMTP_RELAY_DISPLAY_NAME");

    if (!host || !user || !pass) {
        res.error = "SMTP relay is not configured; set SMTP_MODE=relay, SMTP_RELAY_HOST, SMTP_RELAY_USER, and SMTP_RELAY_PASS";
        return res;
    }
    if (!command_available("curl")) {
        res.error = "relay helper curl command is not available";
        return res;
    }

    std::string port = port_env ? port_env : "587";
    std::string from = from_env ? from_env : user;
    std::string reply_to = reply_to_env ? reply_to_env : from;
    std::string display_from = display_env ? display_env : "";
    if (display_from.empty() && from != from_addr) {
        display_from = relay_display_name_for(from_addr);
    }

    std::string cfg_path;
    std::string msg_path;
    std::string err_path;
    if (!make_temp_file(cfg_path) || !make_temp_file(msg_path) || !make_temp_file(err_path)) {
        if (!cfg_path.empty()) ::unlink(cfg_path.c_str());
        if (!msg_path.empty()) ::unlink(msg_path.c_str());
        if (!err_path.empty()) ::unlink(err_path.c_str());
        res.error = "failed to create relay temp files";
        return res;
    }

    auto cleanup = [&] {
        ::unlink(cfg_path.c_str());
        ::unlink(msg_path.c_str());
        ::unlink(err_path.c_str());
    };

    std::string message = build_message(from, to_addr, subject, body, reply_to, display_from, from_addr);
    if (!write_text_file(msg_path, message)) {
        cleanup();
        res.error = "failed to write relay message";
        return res;
    }

    std::string url = std::string("smtp://") + host + ":" + port;
    std::ostringstream cfg;
    cfg << "url = " << curl_config_quote(url) << "\n"
        << "ssl-reqd\n"
        << "mail-from = " << curl_config_quote("<" + from + ">") << "\n"
        << "mail-rcpt = " << curl_config_quote("<" + to_addr + ">") << "\n"
        << "upload-file = " << curl_config_quote(msg_path) << "\n"
        << "user = " << curl_config_quote(std::string(user) + ":" + pass) << "\n"
        << "connect-timeout = 5\n"
        << "max-time = 20\n"
        << "silent\n"
        << "show-error\n";

    if (!write_text_file(cfg_path, cfg.str())) {
        cleanup();
        res.error = "failed to write relay config";
        return res;
    }

    std::cerr << "[smtp-relay] submitting via curl helper to " << host << ":" << port
              << " as " << user << "\n";
    std::string cmd = "curl --config " + shell_quote(cfg_path) +
                      " >/dev/null 2>" + shell_quote(err_path);
    int exit_code = shell_exit_code(std::system(cmd.c_str()));
    if (exit_code == 0) {
        cleanup();
        res.ok = true;
        std::cerr << "[smtp-relay] accepted by relay via curl helper\n";
        return res;
    }

    std::string detail = read_small_file(err_path);
    cleanup();
    if (detail.empty()) detail = "curl helper exited with code " + std::to_string(exit_code);
    res.error = "relay send failed: " + detail;
    return res;
}

#ifdef USE_CURL_RELAY
inline SMTPExternalResult smtp_send_via_relay(const std::string& from_addr,
                                              const std::string& to_addr,
                                              const std::string& subject,
                                              const std::string& body) {
    SMTPExternalResult res;

    const char* host = env_or_null("SMTP_RELAY_HOST");
    const char* port = env_or_null("SMTP_RELAY_PORT");
    const char* user = env_or_null("SMTP_RELAY_USER");
    const char* pass = env_or_null("SMTP_RELAY_PASS");
    const char* from = env_or_null("SMTP_RELAY_FROM");
    const char* reply_to_env = env_or_null("SMTP_REPLY_TO");
    const char* display_env = env_or_null("SMTP_RELAY_DISPLAY_NAME");

    if (!host || !port || !user || !pass || !from) {
        res.error = "relay env not fully configured";
        return res;
    }
    std::string reply_to = reply_to_env ? reply_to_env : from;
    std::string display_from = display_env ? display_env : "";
    if (display_from.empty() && std::string(from) != from_addr) {
        display_from = relay_display_name_for(from_addr);
    }

    CURLcode ginit = curl_global_init(CURL_GLOBAL_DEFAULT);
    if (ginit != CURLE_OK) {
        res.error = std::string("curl_global_init failed: ") + curl_easy_strerror(ginit);
        return res;
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        curl_global_cleanup();
        res.error = "curl_easy_init failed";
        return res;
    }

    std::string url = std::string("smtp://") + host + ":" + port;
    UploadStatus upload;
    upload.payload = build_message(from, to_addr, subject, body, reply_to, display_from, from_addr);

    struct curl_slist* recipients = nullptr;
    recipients = curl_slist_append(recipients, ("<" + to_addr + ">").c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_USERNAME, user);
    curl_easy_setopt(curl, CURLOPT_PASSWORD, pass);
    curl_easy_setopt(curl, CURLOPT_USE_SSL, (long)CURLUSESSL_ALL);
    curl_easy_setopt(curl, CURLOPT_MAIL_FROM, ("<" + std::string(from) + ">").c_str());
    curl_easy_setopt(curl, CURLOPT_MAIL_RCPT, recipients);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, curl_payload_source);
    curl_easy_setopt(curl, CURLOPT_READDATA, &upload);
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 15000L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    // Best-effort pragmatic defaults. For a demo/student project this is
    // usually enough. Tightening verification can be done later if needed.
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

    std::cerr << "[smtp-relay] submitting via " << host << ":" << port
              << " as " << user << "\n";

    CURLcode rc = curl_easy_perform(curl);
    long smtp_response = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &smtp_response);
    if (rc == CURLE_OK) {
        res.ok = true;
        std::cerr << "[smtp-relay] accepted by relay, smtp_response="
                  << smtp_response << "\n";
    } else {
        res.error = std::string("relay send failed: ") + curl_easy_strerror(rc);
    }

    curl_slist_free_all(recipients);
    curl_easy_cleanup(curl);
    curl_global_cleanup();
    return res;
}
#else
inline SMTPExternalResult smtp_send_via_relay(const std::string& from_addr,
                                              const std::string& to_addr,
                                              const std::string& subject,
                                              const std::string& body) {
    return smtp_send_via_curl_cli(from_addr, to_addr, subject, body);
}
#endif  // USE_CURL_RELAY

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
    hints.ai_family = AF_UNSPEC;
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

    std::cerr << "[smtp] trying " << mx_host << ":25\n";

    int fd = connect_host_port(mx_host, 25, 1000);
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

    std::string msg = build_message(from_addr, to_addr, subject, body);
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
    if (at == std::string::npos || at + 1 >= to_addr.size()) {
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
            res.error = "outbound SMTP timed out";
            return res;
        }

        bool temp_fail = false;
        if (try_send_once(rec.host, from_addr, to_addr, subject, body, last_err, temp_fail)) {
            res.ok = true;
            return res;
        }

        if (temp_fail) break;
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
    std::string mode_s = smtp_client_detail::lower_copy(smtp_client_detail::trim(mode ? mode : "direct"));

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
        SMTPExternalResult relay = smtp_client_detail::smtp_send_via_relay(from_addr, to_addr, subject, body);
        if (relay.ok ||
            !smtp_client_detail::env_flag_enabled("SMTP_RELAY_FALLBACK_DIRECT", true)) {
            return relay;
        }
        std::cerr << "[smtp] relay delivery failed for " << to_addr
                  << " (" << relay.error << "); trying direct MX delivery\n";
        SMTPExternalResult direct = smtp_client_detail::smtp_send_direct(from_addr, to_addr, subject, body);
        if (!direct.ok) {
            direct.error = "relay delivery failed (" + relay.error + "); direct MX fallback failed (" +
                           direct.error + ")";
        }
        return direct;
    }
    return smtp_client_detail::smtp_send_direct(from_addr, to_addr, subject, body);
}
