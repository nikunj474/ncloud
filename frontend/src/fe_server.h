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
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

struct RouteMatch {
    bool matched = false;
    std::unordered_map<std::string, std::string> params;
};

inline RouteMatch match_route(const std::string& pattern,
                              const std::string& path) {
    RouteMatch m;
    std::vector<std::string> pp, cp;
    auto split = [](const std::string& s, std::vector<std::string>& parts) {
        std::istringstream ss(s);
        std::string tok;
        while (std::getline(ss, tok, '/')) if (!tok.empty()) parts.push_back(tok);
    };
    split(pattern, pp);
    split(path, cp);
    if (pp.size() != cp.size()) return m;
    for (size_t i = 0; i < pp.size(); ++i) {
        if (!pp[i].empty() && pp[i][0] == ':') m.params[pp[i].substr(1)] = cp[i];
        else if (pp[i] != cp[i]) return m;
    }
    m.matched = true;
    return m;
}

inline std::string json_str(const std::string& s) {
    std::string out = "\"";
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
    out += "\"";
    return out;
}

class FEServer {
public:
    struct Config {
        int         port        = 8080;
        int         threads     = 32;
        std::string kv_host     = "127.0.0.1";
        int         kv_port     = 5000;
        std::string coord_host  = "127.0.0.1";
        int         coord_port  = 0;
        std::string static_dir  = "./static";
        std::string server_id   = "fe1";
        std::string redirect_to = ""; // if non-empty: stub mode — 302 all browser requests here
        bool        page_not_found_stub = false;
    };

    explicit FEServer(const Config& cfg);
    ~FEServer();
    void run();
    void stop();

    KVClient& kv() { return *kv_; }

private:
    Config cfg_;
    std::unique_ptr<KVClient> kv_;
    std::unique_ptr<SessionManager> sessions_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::atomic<int> active_sse_{0};

    struct ThreadPool;
    std::unique_ptr<ThreadPool> pool_;

    void handle_connection(int fd);
    bool handle_one_request(int fd, bool& detached);
    HttpResponse dispatch(const HttpRequest& req);
    std::string get_user(const HttpRequest& req);
    bool admin_control_allowed(const HttpRequest& req) const;
    bool acquire_sse_slot();
    void release_sse_slot();

    HttpResponse handle_spa_shell(const HttpRequest& req);
    HttpResponse handle_static(const HttpRequest& req);

    HttpResponse handle_login(const HttpRequest& req);
    HttpResponse handle_logout(const HttpRequest& req, const std::string& user);
    HttpResponse handle_signup(const HttpRequest& req);
    HttpResponse handle_session(const HttpRequest& req);
    HttpResponse handle_change_password(const HttpRequest& req);

    HttpResponse handle_inbox(const HttpRequest& req, const std::string& user);
    HttpResponse handle_get_email(const HttpRequest& req, const std::string& user, const std::string& uid);
    HttpResponse handle_send_email(const HttpRequest& req, const std::string& user);
    HttpResponse handle_delete_email(const HttpRequest& req, const std::string& user);
    HttpResponse handle_restore_email(const HttpRequest& req, const std::string& user);
    HttpResponse handle_contacts_list(const HttpRequest& req, const std::string& user);
    HttpResponse handle_contacts_add(const HttpRequest& req, const std::string& user);
    HttpResponse handle_contacts_delete(const HttpRequest& req, const std::string& user);
    HttpResponse handle_mail_upload_attachment(const HttpRequest& req, const std::string& user);
    HttpResponse handle_mail_download_attachment(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_rooms(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_create_room(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_messages(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_send(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_dms(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_dm_messages(const HttpRequest& req, const std::string& user);
    HttpResponse handle_chat_dm_send(const HttpRequest& req, const std::string& user);

    HttpResponse handle_drive_list(const HttpRequest& req, const std::string& user);
    HttpResponse handle_upload(const HttpRequest& req, const std::string& user);
    HttpResponse handle_upload_start(const HttpRequest& req, const std::string& user);
    HttpResponse handle_upload_chunk(const HttpRequest& req, const std::string& user);
    HttpResponse handle_upload_status(const HttpRequest& req, const std::string& user);
    HttpResponse handle_upload_finish(const HttpRequest& req, const std::string& user);
    HttpResponse handle_upload_cancel(const HttpRequest& req, const std::string& user);
    HttpResponse handle_download(const HttpRequest& req, const std::string& user, const std::string& uid);
    HttpResponse handle_rename(const HttpRequest& req, const std::string& user);
    HttpResponse handle_move(const HttpRequest& req, const std::string& user);
    HttpResponse handle_mkdir(const HttpRequest& req, const std::string& user);
    HttpResponse handle_delete_path(const HttpRequest& req, const std::string& user);
    HttpResponse handle_quota_status(const HttpRequest& req, const std::string& user);
    HttpResponse handle_quota_update(const HttpRequest& req, const std::string& user);

    void handle_sse(int fd, const HttpRequest& req, const std::string& user);

    HttpResponse handle_admin_page(const HttpRequest& req);
    HttpResponse handle_admin_status(const HttpRequest& req);
    HttpResponse handle_admin_control(const HttpRequest& req);
    HttpResponse handle_admin_control_redirect(const HttpRequest& req);
    HttpResponse handle_admin_metrics(const HttpRequest& req);
    HttpResponse handle_admin_raw(const HttpRequest& req);

    std::string serve_file(const std::string& path);
    int create_listen_socket();
};
