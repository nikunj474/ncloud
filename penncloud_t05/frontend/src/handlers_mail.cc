// =============================================================================
// handlers_mail.cc  --  Webmail handler implementations
// Owner: Liudawei
// =============================================================================
//
// KV SCHEMA (from proposal):
//   row:  "{user}:mail"
//   col:  "msg:{uid}"    val: JSON {"from":"...","subject":"...","time":"..."}
//   col:  "body:{uid}"   val: raw email body text
//
// SMTP NOTIFICATION for SSE (F1):
//   When a new email arrives (from SMTP server or local send),
//   PUT("notify:{recipient}", "latest", "uid:{uid}")
//   The SSE handler in fe_server.cc polls this key every 500ms.
//
// EMAIL UID: timestamp_nanoseconds + random suffix for uniqueness.
// =============================================================================

#include "fe_server.h"
#include <chrono>
#include <sstream>
#include <iomanip>
#include <random>
#include <fstream>
#include <ctime>
#include <climits>
#include <netdb.h>
#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <resolv.h>
#include <sys/socket.h>
#include <unistd.h>
using namespace std;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Generate a unique email UID: <timestamp_ns>_<4 hex random>
static string new_uid() {
    auto ns = chrono::duration_cast<chrono::nanoseconds>(
                  chrono::system_clock::now().time_since_epoch()).count();
    mt19937 rng(static_cast<unsigned>(ns));
    uniform_int_distribution<int> dist(0, 0xFFFF);
    ostringstream oss;
    oss << ns << "_" << hex << setw(4) << setfill('0') << dist(rng);
    return oss.str();
}

// Format current time as "Mar 19, 2026 14:32"
static string now_str() {
    auto t = time(nullptr);
    struct tm tm_buf;
    localtime_r(&t, &tm_buf);
    char buf[64];
    strftime(buf, sizeof(buf), "%b %d, %Y %H:%M", &tm_buf);
    return buf;
}

// Build the metadata JSON stored as msg:{uid}
// Escapes quotes in fields (basic -- full JSON escaping via json_str() in fe_server.h)
static string make_meta_json(const string& from,
                                   const string& to,
                                   const string& subject,
                                   const string& time_str,
                                   const string& uid) {
    auto esc = [](const string& s) {
        string out;
        for (char c : s) {
            if (c == '"')       out += "\\\"";
            else if (c == '\\') out += "\\\\";
            else if (c == '\n') out += "\\n";
            else if (c == '\r') out += "\\r";
            else if (c == '\t') out += "\\t";
            else out += c;
        }
        return out;
    };
    return string("{")
         + "\"uid\":"     + "\"" + esc(uid)      + "\","
         + "\"from\":"    + "\"" + esc(from)     + "\","
         + "\"to\":"      + "\"" + esc(to)       + "\","
         + "\"subject\":" + "\"" + esc(subject)  + "\","
         + "\"time\":"    + "\"" + esc(time_str) + "\""
         + "}";
}

// ---------------------------------------------------------------------------
// GET /api/inbox  -> {"ok":true,"emails":[{uid,from,subject,time},...]}
//
// We need all columns with prefix "msg:" from row "{user}:mail".
// Since our KV store has GET(row, col) for exact column lookup,
// we use a convention: store the email UID list separately.
//
// INBOX INDEX SCHEMA:
//   row: "{user}:mail"   col: "index"   val: "uid1,uid2,uid3,..." (newline sep)
//   row: "{user}:mail"   col: "msg:{uid}"  val: JSON metadata
//   row: "{user}:mail"   col: "body:{uid}" val: raw body
//
// WHY SEPARATE INDEX:
//   KV does not support prefix scans in our minimal implementation.
//   The index column gives us O(1) inbox listing without scanning all cols.
//   Trade-off: index must be updated on send + delete (use CPUT for safety).
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_inbox(const HttpRequest&, const string& user) {
    // Get the inbox index
    string row = user + ":mail";
    string index = kv_->get_str(row, "index");

    string json = "[";
    bool first = true;

    if (!index.empty()) {
        istringstream ss(index);
        string uid;
        while (getline(ss, uid, ',')) {
            if (uid.empty()) continue;
            string meta = kv_->get_str(row, "msg:" + uid);
            if (meta.empty()) continue;
            if (!first) json += ",";
            first = false;
            // Inject "read" field into metadata JSON
            string is_read = kv_->get_str(row, "read:" + uid);
            string annotated = meta;
            if (!annotated.empty() && annotated.back() == '}') {
                annotated.pop_back();
                annotated += ",\"read\":" + string(is_read.empty() ? "false" : "true") + "}";
            }
            json += annotated;
        }
    }
    json += "]";
    return HttpResponse::json("{\"ok\":true,\"emails\":" + json + "}");
}

// ---------------------------------------------------------------------------
// GET /api/email/:uid  -> {"ok":true,"email":{uid,from,to,subject,time,body}}
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_get_email(const HttpRequest& req,
                                         const string& user,
                                         const string& uid) {
    bool is_sent = (req.param("box") == "sent");
    string row  = is_sent ? (user + ":sent") : (user + ":mail");
    string meta = kv_->get_str(row, "msg:" + uid);
    string body = kv_->get_str(row, "body:" + uid);

    // Mark as read (inbox only)
    if (!is_sent && !meta.empty())
        kv_->put(user + ":mail", "read:" + uid, "1");

    if (meta.empty())
        return HttpResponse::json("{\"ok\":false,\"error\":\"not found\"}");

    // Inject body into meta JSON
    // meta = {...}, we add "body":"..." field
    auto esc = [](const string& s) {
        string out;
        for (char c : s) {
            if (c == '"')  out += "\\\"";
            else if (c == '\\') out += "\\\\";
            else if (c == '\n') out += "\\n";
            else if (c == '\r') out += "\\r";
            else out += c;
        }
        return out;
    };

    // Remove trailing } and append body field
    string full = meta;
    if (!full.empty() && full.back() == '}') full.pop_back();
    full += ",\"body\":\"" + esc(body) + "\"}";

    return HttpResponse::json("{\"ok\":true,\"email\":" + full + "}");
}

// ---------------------------------------------------------------------------
// SMTP outbound client -- DNS MX lookup + RFC 5321 SMTP dialog
// ---------------------------------------------------------------------------

static bool smtp_read_line(int fd, string& line) {
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
        if (line.size() > 4096) return false;
    }
}

// Read SMTP response; returns the 3-digit code, or -1 on error.
// Handles multi-line responses (code-SP... vs code-DASH...).
static int smtp_read_response(int fd) {
    string line;
    int code = -1;
    while (true) {
        if (!smtp_read_line(fd, line)) return -1;
        if (line.size() < 3) return -1;
        code = stoi(line.substr(0, 3));
        if (line.size() == 3 || line[3] != '-') break;
    }
    return code;
}

static bool smtp_send_cmd(int fd, const string& cmd) {
    string full = cmd + "\r\n";
    return ::send(fd, full.data(), full.size(), MSG_NOSIGNAL) ==
           static_cast<ssize_t>(full.size());
}

// Resolve the lowest-priority MX host for a domain.
// Falls back to the domain's A record if MX lookup fails.
static string resolve_mx(const string& domain) {
    unsigned char answer[4096];
    int len = res_query(domain.c_str(), ns_c_in, ns_t_mx, answer, sizeof(answer));
    if (len <= 0) return domain;

    ns_msg msg;
    if (ns_initparse(answer, len, &msg) != 0) return domain;

    int count = ns_msg_count(msg, ns_s_an);
    int best_prio = INT_MAX;
    string best_host = domain;

    for (int i = 0; i < count; ++i) {
        ns_rr rr;
        if (ns_parserr(&msg, ns_s_an, i, &rr) != 0) continue;
        if (ns_rr_type(rr) != ns_t_mx) continue;

        int prio = ns_get16(ns_rr_rdata(rr));
        char mx_name[256];
        if (dn_expand(ns_msg_base(msg), ns_msg_end(msg),
                      ns_rr_rdata(rr) + 2, mx_name, sizeof(mx_name)) > 0) {
            if (prio < best_prio) {
                best_prio = prio;
                best_host = mx_name;
            }
        }
    }
    return best_host;
}

// Perform SMTP dot-stuffing on message body (RFC 5321 §4.5.2).
static string dot_stuff(const string& body) {
    string out;
    out.reserve(body.size() + 64);
    bool at_line_start = true;
    for (char c : body) {
        if (at_line_start && c == '.') out += '.';
        out += c;
        at_line_start = (c == '\n');
    }
    return out;
}

// Send an email to an external address via SMTP.
static bool smtp_client_send(const string& from_addr, const string& to_addr,
                             const string& subject, const string& body,
                             string& error_out) {
    auto at = to_addr.find('@');
    if (at == string::npos) { error_out = "Invalid address"; return false; }
    string domain = to_addr.substr(at + 1);

    string mx_host = resolve_mx(domain);

    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (::getaddrinfo(mx_host.c_str(), "25", &hints, &res) != 0 || !res) {
        error_out = "Cannot resolve " + mx_host;
        return false;
    }

    int fd = ::socket(res->ai_family, res->ai_socktype, 0);
    if (fd < 0) { ::freeaddrinfo(res); error_out = "socket() failed"; return false; }

    struct timeval tv{};
    tv.tv_sec = 10;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        ::close(fd);
        ::freeaddrinfo(res);
        error_out = "Cannot connect to " + mx_host + ":25";
        return false;
    }
    ::freeaddrinfo(res);

    bool ok = true;
    ok = ok && (smtp_read_response(fd) == 220);
    ok = ok && smtp_send_cmd(fd, "EHLO penncloud.local") && (smtp_read_response(fd) == 250);
    ok = ok && smtp_send_cmd(fd, "MAIL FROM:<" + from_addr + ">") && (smtp_read_response(fd) == 250);
    ok = ok && smtp_send_cmd(fd, "RCPT TO:<" + to_addr + ">") && (smtp_read_response(fd) == 250);
    ok = ok && smtp_send_cmd(fd, "DATA") && (smtp_read_response(fd) == 354);

    if (ok) {
        string msg;
        msg += "From: <" + from_addr + ">\r\n";
        msg += "To: <" + to_addr + ">\r\n";
        msg += "Subject: " + subject + "\r\n";
        msg += "Date: " + now_str() + "\r\n";
        msg += "MIME-Version: 1.0\r\n";
        msg += "Content-Type: text/plain; charset=utf-8\r\n";
        msg += "\r\n";
        msg += dot_stuff(body);
        msg += "\r\n.\r\n";
        ok = (::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL) ==
              static_cast<ssize_t>(msg.size()));
        ok = ok && (smtp_read_response(fd) == 250);
    }

    smtp_send_cmd(fd, "QUIT");
    ::close(fd);

    if (!ok) error_out = "SMTP dialog failed with " + mx_host;
    return ok;
}

// ---------------------------------------------------------------------------
// POST /api/send  -> {"ok":true} or {"ok":false,"error":"..."}
// Body: to=<addr>&subject=<subj>&body=<text>
//
// ROUTING:
//   - If 'to' contains '@' and domain != 'penncloud' -> external SMTP client
//   - Otherwise -> local delivery (PUT into recipient's KV row)
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_send_email(const HttpRequest& req,
                                          const string& user) {
    auto params = parse_urlencoded(req.body);
    string to      = params["to"];
    string subject = params["subject"];
    string body    = params["body"];

    if (to.empty() || subject.empty())
        return HttpResponse::json("{\"ok\":false,\"error\":\"Missing to or subject\"}");

    string uid      = new_uid();
    string time_str = now_str();

    string from_addr = user + "@penncloud";

    // Determine recipient
    string recipient;
    bool        is_external = false;

    if (to.find('@') != string::npos) {
        auto at = to.find('@');
        string domain = to.substr(at + 1);
        if (domain == "penncloud") {
            recipient = to.substr(0, at);
        } else {
            is_external = true;
        }
    } else {
        recipient = to;
    }

    if (is_external) {
        string smtp_err;
        if (!smtp_client_send(from_addr, to, subject, body, smtp_err)) {
            auto esc = [](const string& s) {
                string out;
                for (char c : s) {
                    if (c == '"')       out += "\\\"";
                    else if (c == '\\') out += "\\\\";
                    else out += c;
                }
                return out;
            };
            return HttpResponse::json("{\"ok\":false,\"error\":\"" + esc(smtp_err) + "\"}");
        }
    }

    // Store in sender's sent box (only after delivery succeeds)
    {
        string sent_row = user + ":sent";
        kv_->put(sent_row, "msg:" + uid, make_meta_json(from_addr, to, subject, time_str, uid));
        kv_->put(sent_row, "body:" + uid, body);
        for (int attempt = 0; attempt < 5; ++attempt) {
            string old_index = kv_->get_str(sent_row, "index");
            string new_index = uid + (old_index.empty() ? "" : "," + old_index);
            if (old_index.empty()) {
                if (kv_->put(sent_row, "index", new_index)) break;
            } else {
                if (kv_->cput(sent_row, "index", old_index, new_index)) break;
                this_thread::sleep_for(chrono::milliseconds(5));
            }
        }
    }

    if (is_external)
        return HttpResponse::json("{\"ok\":true}");


    // Local delivery: PUT into recipient's KV store
    string meta = make_meta_json(from_addr, to, subject, time_str, uid);
    string rrow = recipient + ":mail";

    // Store metadata + body
    kv_->put(rrow, "msg:" + uid,  meta);
    kv_->put(rrow, "body:" + uid, body);

    // Update recipient's inbox index (prepend uid -- newest first)
    // Use CPUT for optimistic concurrency: if index changed between GET and PUT,
    // retry until it succeeds.
    for (int attempt = 0; attempt < 5; ++attempt) {
        string old_index = kv_->get_str(rrow, "index");
        string new_index = uid + (old_index.empty() ? "" : "," + old_index);

        if (old_index.empty()) {
            // First email -- simple PUT
            if (kv_->put(rrow, "index", new_index)) break;
        } else {
            // CPUT: only update if index hasn't changed since we read it
            if (kv_->cput(rrow, "index", old_index, new_index)) break;
            // If CPUT fails, another sender also delivered -- retry
            this_thread::sleep_for(chrono::milliseconds(5));
        }
    }

    // Notify SSE (F1): tell the recipient's live connection a new email arrived
    kv_->put("notify:" + recipient, "latest", "uid:" + uid);

    return HttpResponse::json("{\"ok\":true,\"uid\":\"" + uid + "\"}");
}

// ---------------------------------------------------------------------------
// POST /api/delete-email  -> {"ok":true}
// Body: uid=<uid>
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_delete_email(const HttpRequest& req,
                                            const string& user) {
    auto params = parse_urlencoded(req.body);
    string uid = params["uid"];
    if (uid.empty())
        return HttpResponse::json("{\"ok\":false,\"error\":\"Missing uid\"}");

    string row = user + ":mail";

    kv_->del(row, "msg:"  + uid);
    kv_->del(row, "body:" + uid);
    kv_->del(row, "read:" + uid);

    // Remove from index (CPUT-based -- filter out the uid)
    for (int attempt = 0; attempt < 5; ++attempt) {
        string old_index = kv_->get_str(row, "index");
        if (old_index.empty()) break;

        // Filter uid out of comma-separated list
        string new_index;
        istringstream ss(old_index);
        string tok;
        while (getline(ss, tok, ',')) {
            if (tok == uid) continue;
            if (!new_index.empty()) new_index += ',';
            new_index += tok;
        }

        if (old_index == new_index) break;  // uid not in index

        if (new_index.empty()) {
            kv_->put(row, "index", "");
            break;
        }
        if (kv_->cput(row, "index", old_index, new_index)) break;
        this_thread::sleep_for(chrono::milliseconds(5));
    }

    return HttpResponse::json("{\"ok\":true}");
}

// ---------------------------------------------------------------------------
// GET /api/sent  -> {"ok":true,"emails":[{uid,from,to,subject,time},...]}
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_sent(const HttpRequest&, const string& user) {
    string row = user + ":sent";
    string index = kv_->get_str(row, "index");

    string json = "[";
    bool first = true;

    if (!index.empty()) {
        istringstream ss(index);
        string uid;
        while (getline(ss, uid, ',')) {
            if (uid.empty()) continue;
            string meta = kv_->get_str(row, "msg:" + uid);
            if (meta.empty()) continue;
            if (!first) json += ",";
            first = false;
            json += meta;
        }
    }
    json += "]";
    return HttpResponse::json("{\"ok\":true,\"emails\":" + json + "}");
}
