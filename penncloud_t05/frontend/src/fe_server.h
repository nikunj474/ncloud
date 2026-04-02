#pragma once
// =============================================================================
// fe_server.h  --  PennCloud Frontend HTTP Server
// =============================================================================
//
// Routes:
//   GET  /              -> serve SPA shell (index.html)
//   GET  /static/*      -> static files (CSS, JS)
//
//   POST /api/login     -> authenticate, set session cookie
//   POST /api/logout    -> destroy session
//   POST /api/signup    -> create new account
//   POST /api/change-password
//
//   GET  /api/inbox           -> list emails (JSON)
//   GET  /api/email/:uid      -> get one email body (JSON)
//   POST /api/send            -> send email (local or external)
//   POST /api/delete-email    -> delete email
//
//   GET  /api/drive/*         -> list folder contents (JSON)
//   POST /api/upload          -> upload file (multipart)
//   GET  /api/download/:uid   -> download file (binary)
//   POST /api/rename          -> rename file or folder
//   POST /api/move            -> move file or folder
//   POST /api/mkdir           -> create folder
//   POST /api/delete-path     -> delete file or folder
//
//   GET  /events              -> SSE stream (F1: live inbox)
//
//   GET  /admin              -> admin console page
//   GET  /admin/metrics      -> JSON metrics from all backend nodes
//
// HANDLER PATTERN:
//   Every handler is:
//     HttpResponse handle_X(const HttpRequest& req, const string& username)
//   username is pre-validated by the middleware layer.
// =============================================================================

#include "http.h"
#include "http_reader.h"
#include "session.h"
#include "kv_client.h"
#include <string>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>
using namespace std;

// ---------------------------------------------------------------------------
// Route matcher: matches /api/email/abc123 and extracts "abc123"
// ---------------------------------------------------------------------------
struct RouteMatch {
    bool matched = false;
    unordered_map<string, string> params;
};

inline RouteMatch match_route(const string& pattern,
                               const string& path) {
    RouteMatch m;
    vector<string> pp, cp;

    auto split = [](const string& s, vector<string>& parts) {
        istringstream ss(s);
        string tok;
        while (getline(ss, tok, '/'))
            if (!tok.empty()) parts.push_back(tok);
    };
    split(pattern, pp);
    split(path, cp);

    if (pp.size() != cp.size()) return m;
    for (size_t i = 0; i < pp.size(); ++i) {
        if (pp[i][0] == ':') m.params[pp[i].substr(1)] = cp[i];
        else if (pp[i] != cp[i]) return m;
    }
    m.matched = true;
    return m;
}

// ---------------------------------------------------------------------------
// Simple JSON builder (avoids pulling in a library)
// ---------------------------------------------------------------------------
inline string json_str(const string& s) {
    // Escape special chars
    string out = "\"";
    for (char c : s) {
        if (c == '"')  out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c == '\t') out += "\\t";
        else out += c;
    }
    out += "\"";
    return out;
}

// ---------------------------------------------------------------------------
// FEServer
// ---------------------------------------------------------------------------
class FEServer {
public:
    struct Config {
        int         port          = 8080;
        int         threads       = 32;
        string kv_host       = "127.0.0.1";
        int         kv_port       = 5000;
        string static_dir    = "./static";
        string server_id     = "fe1";   // for load balancer identification
    };

    explicit FEServer(const Config& cfg);
    ~FEServer();
    void run();
    void stop();

    // Expose kv_client for testing
    KVClient& kv() { return *kv_; }

private:
    Config                      cfg_;
    unique_ptr<KVClient>   kv_;
    unique_ptr<SessionManager> sessions_;
    int                         listen_fd_ = -1;
    atomic<bool>           running_{false};

    // Thread pool (reuse pattern from kvserver)
    struct ThreadPool;
    unique_ptr<ThreadPool> pool_;

    // ---- Connection handling ------------------------------------------------
    void handle_connection(int fd);
    bool handle_one_request(int fd);

    // ---- Route dispatch -------------------------------------------------------
    HttpResponse dispatch(const HttpRequest& req);

    // ---- Auth middleware -------------------------------------------------------
    // Returns username if authenticated, "" if not.
    // Automatically redirects or returns 401 if called via require_auth().
    string get_user(const HttpRequest& req);

    // ---- Handler declarations --------------------------------------------------
    // (implementations in fe_handlers.cc)
    HttpResponse handle_spa_shell(const HttpRequest& req);
    HttpResponse handle_static(const HttpRequest& req);

    // Auth
    HttpResponse handle_login(const HttpRequest& req);
    HttpResponse handle_logout(const HttpRequest& req);
    HttpResponse handle_signup(const HttpRequest& req);
    HttpResponse handle_change_password(const HttpRequest& req);

    // Mail
    HttpResponse handle_inbox(const HttpRequest& req, const string& user);
    HttpResponse handle_get_email(const HttpRequest& req, const string& user,
                                   const string& uid);
    HttpResponse handle_send_email(const HttpRequest& req, const string& user);
    HttpResponse handle_delete_email(const HttpRequest& req, const string& user);

    // Drive
    HttpResponse handle_drive_list(const HttpRequest& req, const string& user);
    HttpResponse handle_upload(const HttpRequest& req, const string& user);
    HttpResponse handle_download(const HttpRequest& req, const string& user,
                                  const string& uid);
    HttpResponse handle_rename(const HttpRequest& req, const string& user);
    HttpResponse handle_move(const HttpRequest& req, const string& user);
    HttpResponse handle_mkdir(const HttpRequest& req, const string& user);
    HttpResponse handle_delete_path(const HttpRequest& req, const string& user);

    // SSE (F1)
    void handle_sse(int fd, const HttpRequest& req, const string& user);

    // Admin
    HttpResponse handle_admin_page(const HttpRequest& req);
    HttpResponse handle_admin_metrics(const HttpRequest& req);

    // ---- Helpers ---------------------------------------------------------------
    string serve_file(const string& path);
    int create_listen_socket();
};
