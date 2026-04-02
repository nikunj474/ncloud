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
    char buf[64];
    strftime(buf, sizeof(buf), "%b %d, %Y %H:%M", localtime(&t));
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
            if (c == '"')  out += "\\\"";
            else if (c == '\\') out += "\\\\";
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
        // Parse comma-separated UIDs (newest first -- we prepend on send)
        istringstream ss(index);
        string uid;
        while (getline(ss, uid, ',')) {
            if (uid.empty()) continue;
            string meta = kv_->get_str(row, "msg:" + uid);
            if (meta.empty()) continue;  // deleted or missing
            if (!first) json += ",";
            first = false;
            json += meta;
        }
    }
    json += "]";
    return HttpResponse::json("{\"ok\":true,\"emails\":" + json + "}");
}

// ---------------------------------------------------------------------------
// GET /api/email/:uid  -> {"ok":true,"email":{uid,from,to,subject,time,body}}
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_get_email(const HttpRequest&,
                                         const string& user,
                                         const string& uid) {
    string row  = user + ":mail";
    string meta = kv_->get_str(row, "msg:" + uid);
    string body = kv_->get_str(row, "body:" + uid);

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

    // Determine sender address
    string from_addr = user + "@penncloud";

    // Store in SENDER's sent box (optional -- uncomment if you want sent mail)
    // kv_->put(user + ":sent", "msg:" + uid, make_meta_json(from_addr, to, subject, time_str, uid));

    // Determine recipient
    string recipient;
    bool        is_external = false;

    if (to.find('@') != string::npos) {
        // Check if it's a local @penncloud address
        auto at = to.find('@');
        string domain = to.substr(at + 1);
        if (domain == "penncloud") {
            recipient = to.substr(0, at);  // strip @penncloud
        } else {
            is_external = true;
        }
    } else {
        recipient = to;  // bare username = local user
    }

    if (is_external) {
        // TODO (Liudawei Phase 3): SMTP client -- DNS MX lookup + send
        // For now return success stub so UI works
        return HttpResponse::json("{\"ok\":false,\"error\":\"External SMTP not yet implemented\"}");
    }

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

    // Remove metadata and body
    kv_->del(row, "msg:"  + uid);
    kv_->del(row, "body:" + uid);

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
