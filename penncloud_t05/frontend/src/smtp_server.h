#pragma once
// =============================================================================
// smtp_server.h  --  Inbound SMTP server for PennCloud
// =============================================================================
//
// Listens on a configurable port (default 2500) for incoming SMTP connections
// from external mail servers.  Implements the minimum subset of RFC 5321 needed
// to receive email:
//
//   220 greeting  ->  EHLO/HELO  ->  MAIL FROM  ->  RCPT TO  ->  DATA  ->  QUIT
//
// On successful DATA, the message is parsed (From/Subject/Body extracted from
// RFC 822 headers) and delivered into the recipient's KV store using the same
// schema as local send in handlers_mail.cc:
//
//   row: "{user}:mail"  col: "msg:{uid}"   val: JSON metadata
//   row: "{user}:mail"  col: "body:{uid}"  val: raw body text
//   row: "{user}:mail"  col: "index"       val: uid prepended to CSV list
//
// SSE notification is triggered by writing to "notify:{user}" / "latest".
// =============================================================================

#include "kv_client.h"
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <cstring>
#include <cerrno>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

class SMTPServer {
public:
    SMTPServer(int port, KVClient* kv)
        : port_(port), kv_(kv) {}

    ~SMTPServer() { stop(); }

    void start() {
        running_ = true;
        thread_ = std::thread([this] { run(); });
    }

    void stop() {
        running_ = false;
        if (listen_fd_ >= 0) {
            ::shutdown(listen_fd_, SHUT_RDWR);
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (thread_.joinable()) thread_.join();
    }

private:
    int           port_;
    KVClient*     kv_;
    int           listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread   thread_;

    // ---- UID / time helpers (same as handlers_mail.cc) ----------------------
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
        auto t = time(nullptr);
        char buf[64];
        strftime(buf, sizeof(buf), "%b %d, %Y %H:%M", localtime(&t));
        return buf;
    }

    static std::string json_escape(const std::string& s) {
        std::string out;
        for (char c : s) {
            if (c == '"') out += "\\\"";
            else if (c == '\\') out += "\\\\";
            else out += c;
        }
        return out;
    }

    static std::string make_meta_json(const std::string& from,
                                      const std::string& to,
                                      const std::string& subject,
                                      const std::string& time_str,
                                      const std::string& uid) {
        return std::string("{")
             + "\"uid\":\""     + json_escape(uid)      + "\","
             + "\"from\":\""   + json_escape(from)     + "\","
             + "\"to\":\""     + json_escape(to)       + "\","
             + "\"subject\":\"" + json_escape(subject)  + "\","
             + "\"time\":\""   + json_escape(time_str) + "\""
             + "}";
    }

    // ---- TCP line reader ----------------------------------------------------
    static bool read_line(int fd, std::string& line) {
        line.clear();
        char c;
        while (true) {
            ssize_t r = ::recv(fd, &c, 1, 0);
            if (r <= 0) return false;
            if (c == '\n') {
                if (!line.empty() && line.back() == '\r') line.pop_back();
                return true;
            }
            line += c;
            if (line.size() > 65536) return false;
        }
    }

    static bool send_line(int fd, const std::string& msg) {
        std::string full = msg + "\r\n";
        return ::send(fd, full.data(), full.size(), MSG_NOSIGNAL) ==
               static_cast<ssize_t>(full.size());
    }

    // ---- Extract <addr> from "MAIL FROM:<addr>" or "RCPT TO:<addr>" ---------
    static std::string extract_addr(const std::string& line) {
        auto lt = line.find('<');
        auto gt = line.find('>');
        if (lt != std::string::npos && gt != std::string::npos && gt > lt)
            return line.substr(lt + 1, gt - lt - 1);
        // Fallback: take everything after the colon
        auto col = line.find(':');
        if (col != std::string::npos) {
            std::string a = line.substr(col + 1);
            while (!a.empty() && a.front() == ' ') a.erase(a.begin());
            while (!a.empty() && a.back() == ' ') a.pop_back();
            return a;
        }
        return "";
    }

    // ---- Extract local username from "user@penncloud" or "user" -------------
    static std::string local_user(const std::string& addr) {
        auto at = addr.find('@');
        if (at != std::string::npos) return addr.substr(0, at);
        return addr;
    }

    // ---- Parse RFC 822 headers from DATA payload ----------------------------
    struct ParsedMail {
        std::string from;
        std::string to;
        std::string subject;
        std::string body;
    };

    static ParsedMail parse_rfc822(const std::string& data) {
        ParsedMail pm;
        std::istringstream ss(data);
        std::string line;
        bool in_body = false;
        std::string last_header;

        while (std::getline(ss, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();

            if (!in_body) {
                if (line.empty()) {
                    in_body = true;
                    continue;
                }
                // Continuation line (starts with whitespace)
                if ((line[0] == ' ' || line[0] == '\t') && !last_header.empty()) {
                    if (last_header == "subject") pm.subject += " " + line.substr(1);
                    continue;
                }
                auto col = line.find(':');
                if (col == std::string::npos) continue;
                std::string key = line.substr(0, col);
                std::string val = line.substr(col + 1);
                while (!val.empty() && val.front() == ' ') val.erase(val.begin());

                // Lowercase the key for comparison
                std::string lkey = key;
                for (char& c : lkey) c = static_cast<char>(std::tolower(c));
                last_header = lkey;

                if (lkey == "from")    pm.from    = val;
                if (lkey == "to")      pm.to      = val;
                if (lkey == "subject") pm.subject = val;
            } else {
                if (!pm.body.empty()) pm.body += "\n";
                pm.body += line;
            }
        }
        return pm;
    }

    // ---- Deliver a received email into the KV store -------------------------
    void deliver(const std::string& envelope_from,
                 const std::string& envelope_to,
                 const std::string& raw_data) {
        ParsedMail pm = parse_rfc822(raw_data);

        std::string from_addr = pm.from.empty() ? envelope_from : pm.from;
        std::string subject   = pm.subject.empty() ? "(no subject)" : pm.subject;
        std::string recipient = local_user(envelope_to);
        std::string uid       = new_uid();
        std::string time_str  = now_str();

        std::string meta = make_meta_json(from_addr, envelope_to, subject, time_str, uid);
        std::string rrow = recipient + ":mail";

        kv_->put(rrow, "msg:"  + uid, meta);
        kv_->put(rrow, "body:" + uid, pm.body);

        for (int attempt = 0; attempt < 5; ++attempt) {
            std::string old_index = kv_->get_str(rrow, "index");
            std::string new_index = uid + (old_index.empty() ? "" : "," + old_index);
            if (old_index.empty()) {
                if (kv_->put(rrow, "index", new_index)) break;
            } else {
                if (kv_->cput(rrow, "index", old_index, new_index)) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }

        kv_->put("notify:" + recipient, "latest", "uid:" + uid);

        std::cout << "[smtp] delivered email from " << from_addr
                  << " to " << recipient << " uid=" << uid << "\n";
    }

    // ---- Handle one SMTP session on fd --------------------------------------
    void handle_session(int fd) {
        struct timeval tv{};
        tv.tv_sec = 60;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        send_line(fd, "220 penncloud SMTP ready");

        std::string envelope_from;
        std::string envelope_to;
        std::string line;

        while (read_line(fd, line)) {
            // Case-insensitive command matching
            std::string upper = line;
            for (char& c : upper) c = static_cast<char>(std::toupper(c));

            if (upper.rfind("EHLO", 0) == 0 || upper.rfind("HELO", 0) == 0) {
                send_line(fd, "250 penncloud greets you");
            }
            else if (upper.rfind("MAIL FROM:", 0) == 0) {
                envelope_from = extract_addr(line);
                send_line(fd, "250 OK");
            }
            else if (upper.rfind("RCPT TO:", 0) == 0) {
                envelope_to = extract_addr(line);
                // Verify recipient is a local user
                std::string user = local_user(envelope_to);
                std::string pwd = kv_->get_str(user, "pwd");
                if (pwd.empty()) {
                    send_line(fd, "550 No such user - " + user);
                    envelope_to.clear();
                } else {
                    send_line(fd, "250 OK");
                }
            }
            else if (upper == "DATA") {
                if (envelope_to.empty()) {
                    send_line(fd, "503 Need RCPT TO first");
                    continue;
                }
                send_line(fd, "354 End data with <CR><LF>.<CR><LF>");

                // Read DATA until lone "." line
                std::string data;
                std::string dline;
                while (read_line(fd, dline)) {
                    if (dline == ".") break;
                    // Undo dot-stuffing (RFC 5321 §4.5.2)
                    if (dline.size() >= 2 && dline[0] == '.' && dline[1] == '.')
                        dline.erase(0, 1);
                    if (!data.empty()) data += "\n";
                    data += dline;
                }

                deliver(envelope_from, envelope_to, data);
                send_line(fd, "250 OK message delivered");

                envelope_from.clear();
                envelope_to.clear();
            }
            else if (upper == "RSET") {
                envelope_from.clear();
                envelope_to.clear();
                send_line(fd, "250 OK");
            }
            else if (upper == "NOOP") {
                send_line(fd, "250 OK");
            }
            else if (upper == "QUIT") {
                send_line(fd, "221 Bye");
                break;
            }
            else {
                send_line(fd, "502 Command not implemented");
            }
        }
        ::close(fd);
    }

    // ---- Main accept loop ---------------------------------------------------
    void run() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            std::cerr << "[smtp] socket(): " << strerror(errno) << "\n";
            return;
        }
        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::cerr << "[smtp] bind(" << port_ << "): " << strerror(errno) << "\n";
            ::close(listen_fd_);
            listen_fd_ = -1;
            return;
        }
        ::listen(listen_fd_, 64);
        std::cout << "[smtp] listening on port " << port_ << "\n";

        while (running_) {
            sockaddr_in cli{};
            socklen_t clen = sizeof(cli);
            int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli), &clen);
            if (cfd < 0) { if (running_) continue; break; }

            // Handle each SMTP session in a detached thread
            std::thread([this, cfd] { handle_session(cfd); }).detach();
        }
    }
};
