// session.h  --  Session management for PennCloud frontend


#include <array>
#include <string>
#include <random>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <ctime>
#include <functional>
#include <thread>
#include <chrono>
#include "http.h"

using namespace std;

using KVGetFn  = function<bool(const string& row,
                                    const string& col,
                                    string& val_out)>;
using KVPutFn  = function<bool(const string& row,
                                    const string& col,
                                    const string& val)>;
using KVDelFn  = function<bool(const string& row,
                                    const string& col)>;

enum class SessionKVReadStatus {
    Found,
    NotFound,
    Unavailable
};

using KVGetStatusFn = function<SessionKVReadStatus(
    const string& row,
    const string& col,
    string& val_out)>;

static constexpr int SESSION_TTL_SECONDS = 7 * 86400;  // 7 days

inline string generate_sid() {
    ifstream urandom("/dev/urandom", ios::binary);
    array<uint8_t, 16> buf{};
    if (!urandom.is_open()) return "";
    urandom.read(reinterpret_cast<char*>(buf.data()), static_cast<streamsize>(buf.size()));
    if (urandom.gcount() != static_cast<streamsize>(buf.size())) return "";

    ostringstream oss;
    for (uint8_t b : buf) {
        oss << hex << setw(2) << setfill('0')
            << static_cast<int>(b);
    }
    return oss.str();
}

class SessionManager {
public:
    SessionManager(KVGetFn get_fn, KVPutFn put_fn, KVDelFn del_fn)
        : kv_get_(move(get_fn)),
          kv_put_(move(put_fn)),
          kv_del_(move(del_fn)) {}

    SessionManager(KVGetFn get_fn, KVPutFn put_fn, KVDelFn del_fn,
                   KVGetStatusFn get_status_fn)
        : kv_get_(move(get_fn)),
          kv_put_(move(put_fn)),
          kv_del_(move(del_fn)),
          kv_get_status_(move(get_status_fn)) {}

    string create(const string& username) {
        string sid = generate_sid();
        if (sid.empty()) return "";
        string row = session_row(sid);

        if (!kv_put_(row, "user", username)) {
            return "";
        }
        if (!kv_put_(row, "expiry",
                to_string(time(nullptr) + SESSION_TTL_SECONDS))) {
            kv_del_(row, "user");
            return "";
        }
        return sid;
    }

    SessionKVReadStatus validate_status(const string& sid,
                                        string& username_out) {
        username_out.clear();
        if (sid.empty() || sid.size() != 32) return SessionKVReadStatus::NotFound;

        string row = session_row(sid);
        string username, expiry;

        if (kv_get_status_) {
            auto s1 = retry_get_status(row, "user", username);
            if (s1 != SessionKVReadStatus::Found) return s1;

            auto s2 = retry_get_status(row, "expiry", expiry);
            if (s2 != SessionKVReadStatus::Found) return s2;
        } else {
            if (!retry_get_legacy(row, "user", username)) return SessionKVReadStatus::NotFound;
            if (!retry_get_legacy(row, "expiry", expiry)) return SessionKVReadStatus::NotFound;
        }

        long exp = 0;
        try {
            exp = stol(expiry);
        } catch (...) {
            destroy(sid);
            return SessionKVReadStatus::NotFound;
        }

        if (time(nullptr) > exp) {
            destroy(sid);
            return SessionKVReadStatus::NotFound;
        }
        username_out = username;
        return SessionKVReadStatus::Found;
    }

    string validate(const string& sid) {
        string username;
        if (validate_status(sid, username) != SessionKVReadStatus::Found) return "";
        return username;
    }

    string authenticate(const HttpRequest& req) {
        return validate(req.cookie("sid"));
    }

    void destroy(const string& sid) {
        string row = session_row(sid);
        kv_del_(row, "user");
        kv_del_(row, "expiry");
    }

    void attach_cookie(HttpResponse& resp, const string& sid) {
        resp.set_cookie("sid", sid, "/", true, SESSION_TTL_SECONDS);
    }

private:
    KVGetFn       kv_get_;
    KVPutFn       kv_put_;
    KVDelFn       kv_del_;
    KVGetStatusFn kv_get_status_;

    static string session_row(const string& sid) {
        return "session:" + sid;
    }

    bool retry_get_legacy(const string& row,
                          const string& col,
                          string& out) {
        constexpr int kAttempts = 5;
        constexpr int kSleepMs[kAttempts] = {0, 75, 150, 250, 400};

        for (int i = 0; i < kAttempts; ++i) {
            if (kv_get_(row, col, out)) return true;
            this_thread::sleep_for(
                chrono::milliseconds(kSleepMs[i]));
        }
        return false;
    }

    SessionKVReadStatus retry_get_status(const string& row,
                                         const string& col,
                                         string& out) {
        constexpr int kAttempts = 6;
        constexpr int kSleepMs[kAttempts] = {0, 50, 100, 150, 250, 400};

        SessionKVReadStatus last = SessionKVReadStatus::Unavailable;
        for (int i = 0; i < kAttempts; ++i) {
            last = kv_get_status_(row, col, out);
            if (last == SessionKVReadStatus::Found) return last;
            if (last == SessionKVReadStatus::NotFound) return last;

            this_thread::sleep_for(
                chrono::milliseconds(kSleepMs[i]));
        }
        return last;
    }
};
