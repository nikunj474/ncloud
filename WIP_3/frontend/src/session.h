// session.h  --  Session management for PennCloud frontend


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

using KVGetFn  = std::function<bool(const std::string& row,
                                    const std::string& col,
                                    std::string& val_out)>;
using KVPutFn  = std::function<bool(const std::string& row,
                                    const std::string& col,
                                    const std::string& val)>;
using KVDelFn  = std::function<bool(const std::string& row,
                                    const std::string& col)>;

enum class SessionKVReadStatus {
    Found,
    NotFound,
    Unavailable
};

using KVGetStatusFn = std::function<SessionKVReadStatus(
    const std::string& row,
    const std::string& col,
    std::string& val_out)>;

static constexpr int SESSION_TTL_SECONDS = 86400;  // 24 hours

inline std::string generate_sid() {
    std::ifstream urandom("/dev/urandom", std::ios::binary);
    uint8_t buf[16];
    urandom.read(reinterpret_cast<char*>(buf), 16);

    std::ostringstream oss;
    for (uint8_t b : buf) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(b);
    }
    return oss.str();
}

class SessionManager {
public:
    SessionManager(KVGetFn get_fn, KVPutFn put_fn, KVDelFn del_fn)
        : kv_get_(std::move(get_fn)),
          kv_put_(std::move(put_fn)),
          kv_del_(std::move(del_fn)) {}

    SessionManager(KVGetFn get_fn, KVPutFn put_fn, KVDelFn del_fn,
                   KVGetStatusFn get_status_fn)
        : kv_get_(std::move(get_fn)),
          kv_put_(std::move(put_fn)),
          kv_del_(std::move(del_fn)),
          kv_get_status_(std::move(get_status_fn)) {}

    std::string create(const std::string& username) {
        std::string sid = generate_sid();
        std::string row = session_row(sid);

        if (!kv_put_(row, "user", username)) {
            return "";
        }
        if (!kv_put_(row, "expiry",
                std::to_string(std::time(nullptr) + SESSION_TTL_SECONDS))) {
            kv_del_(row, "user");
            return "";
        }
        return sid;
    }

    std::string validate(const std::string& sid) {
        if (sid.empty() || sid.size() != 32) return "";

        std::string row = session_row(sid);
        std::string username, expiry;

        if (kv_get_status_) {
            auto s1 = retry_get_status(row, "user", username);
            if (s1 != SessionKVReadStatus::Found) return "";

            auto s2 = retry_get_status(row, "expiry", expiry);
            if (s2 != SessionKVReadStatus::Found) return "";
        } else {
            if (!retry_get_legacy(row, "user", username)) return "";
            if (!retry_get_legacy(row, "expiry", expiry)) return "";
        }

        long exp = 0;
        try {
            exp = std::stol(expiry);
        } catch (...) {
            destroy(sid);
            return "";
        }

        if (std::time(nullptr) > exp) {
            destroy(sid);
            return "";
        }
        return username;
    }

    std::string authenticate(const HttpRequest& req) {
        return validate(req.cookie("sid"));
    }

    void destroy(const std::string& sid) {
        std::string row = session_row(sid);
        kv_del_(row, "user");
        kv_del_(row, "expiry");
    }

    void attach_cookie(HttpResponse& resp, const std::string& sid) {
        resp.set_cookie("sid", sid, "/", true, SESSION_TTL_SECONDS);
    }

private:
    KVGetFn       kv_get_;
    KVPutFn       kv_put_;
    KVDelFn       kv_del_;
    KVGetStatusFn kv_get_status_;

    static std::string session_row(const std::string& sid) {
        return "session:" + sid;
    }

    bool retry_get_legacy(const std::string& row,
                          const std::string& col,
                          std::string& out) {
        constexpr int kAttempts = 5;
        constexpr int kSleepMs[kAttempts] = {0, 75, 150, 250, 400};

        for (int i = 0; i < kAttempts; ++i) {
            if (kv_get_(row, col, out)) return true;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kSleepMs[i]));
        }
        return false;
    }

    SessionKVReadStatus retry_get_status(const std::string& row,
                                         const std::string& col,
                                         std::string& out) {
        constexpr int kAttempts = 6;
        constexpr int kSleepMs[kAttempts] = {0, 50, 100, 150, 250, 400};

        SessionKVReadStatus last = SessionKVReadStatus::Unavailable;
        for (int i = 0; i < kAttempts; ++i) {
            last = kv_get_status_(row, col, out);
            if (last == SessionKVReadStatus::Found) return last;
            if (last == SessionKVReadStatus::NotFound) return last;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(kSleepMs[i]));
        }
        return last;
    }
};