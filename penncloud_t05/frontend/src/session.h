#pragma once
// =============================================================================
// session.h  --  Session management for PennCloud frontend
// =============================================================================
//
// DESIGN: Frontend servers are completely stateless.
// All session state lives in the KV store:
//
//   row:  "session:<sid>"
//   col:  "user"    value: "<username>"
//   col:  "expiry"  value: "<unix_timestamp>"
//
// WHY KV store for sessions:
//   - Any frontend server can validate any session without talking to
//     the server that created it.
//   - If a frontend crashes, clients are redirected to another frontend
//     by the load balancer. Their session is intact because it lives in KV.
//   - This is exactly what Prof. Phan's lecture 11 slide 28 shows.
//
// SID generation: 128 bits from /dev/urandom encoded as 32 hex chars.
// This is cryptographically random and unguessable.
// =============================================================================

#include <string>
#include <random>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <ctime>
#include <functional>
#include "http.h"

// KV client interface -- implemented in kv_client.h
// We use a callback so session.h doesn't depend on a specific KV implementation.
using KVGetFn  = std::function<bool(const std::string& row,
                                    const std::string& col,
                                    std::string& val_out)>;
using KVPutFn  = std::function<bool(const std::string& row,
                                    const std::string& col,
                                    const std::string& val)>;
using KVDelFn  = std::function<bool(const std::string& row,
                                    const std::string& col)>;

static constexpr int SESSION_TTL_SECONDS = 86400;  // 24 hours

// ---------------------------------------------------------------------------
// Generate a 128-bit random session ID (32 hex chars)
// ---------------------------------------------------------------------------
inline std::string generate_sid() {
    // Read 16 bytes from /dev/urandom -- cryptographically secure
    std::ifstream urandom("/dev/urandom", std::ios::binary);
    uint8_t buf[16];
    urandom.read(reinterpret_cast<char*>(buf), 16);

    std::ostringstream oss;
    for (uint8_t b : buf)
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    return oss.str();
}

// ---------------------------------------------------------------------------
// SessionManager
// ---------------------------------------------------------------------------
class SessionManager {
public:
    SessionManager(KVGetFn get_fn, KVPutFn put_fn, KVDelFn del_fn)
        : kv_get_(get_fn), kv_put_(put_fn), kv_del_(del_fn) {}

    // Create a new session for username.  Returns the SID.
    std::string create(const std::string& username) {
        std::string sid = generate_sid();
        std::string row = session_row(sid);

        kv_put_(row, "user",   username);
        kv_put_(row, "expiry", std::to_string(std::time(nullptr) + SESSION_TTL_SECONDS));
        return sid;
    }

    // Validate SID from request.
    // Returns username if valid, empty string if invalid/expired.
    std::string validate(const std::string& sid) {
        if (sid.empty() || sid.size() != 32) return "";

        std::string row = session_row(sid);
        std::string username, expiry;

        if (!kv_get_(row, "user",   username)) return "";
        if (!kv_get_(row, "expiry", expiry))   return "";

        // Check expiry
        long exp = 0;
        try { exp = std::stol(expiry); } catch (...) { destroy(sid); return ""; }
        if (std::time(nullptr) > exp) {
            destroy(sid);  // clean up expired session
            return "";
        }
        return username;
    }

    // Destroy a session (logout)
    void destroy(const std::string& sid) {
        std::string row = session_row(sid);
        kv_del_(row, "user");
        kv_del_(row, "expiry");
    }

    // Get SID from request cookies, validate, return username or ""
    std::string authenticate(const HttpRequest& req) {
        return validate(req.cookie("sid"));
    }

    // Add session cookie to response
    void attach_cookie(HttpResponse& resp, const std::string& sid) {
        resp.set_cookie("sid", sid, "/", true, SESSION_TTL_SECONDS);
    }

private:
    KVGetFn kv_get_;
    KVPutFn kv_put_;
    KVDelFn kv_del_;

    static std::string session_row(const std::string& sid) {
        return "session:" + sid;
    }
};
