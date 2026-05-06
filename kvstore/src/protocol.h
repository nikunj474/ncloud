#pragma once
#ifndef PENNCLOUD_KV_PROTOCOL_H
#define PENNCLOUD_KV_PROTOCOL_H

using namespace std;
// =============================================================================
// protocol.h  --  PennCloud KV wire protocol (shared by KV server + FE server)
// =============================================================================
//
// Every message is LENGTH-PREFIXED so binary values (PDFs, images, video)
// pass through without any escaping or delimiting.
//
// CLIENT -> SERVER
//   PUT    <rowlen> <collen> <vallen>\r\n  <row><col><val>
//   GET    <rowlen> <collen>\r\n           <row><col>
//   CPUT   <rowlen> <collen> <v1len> <v2len>\r\n  <row><col><v1><v2>
//   DELETE <rowlen> <collen>\r\n           <row><col>
//
// SERVER -> CLIENT
//   +OK\r\n                    -- PUT / DELETE / CPUT success
//   +OK <vallen>\r\n<val>      -- GET success (val is raw bytes)
//   -ERR <message>\r\n         -- any failure (CPUT mismatch, key not found, etc.)
//
// All integer fields in the header line are ASCII decimal, space-separated.
// The \r\n terminates the header line.  After \r\n, raw bytes follow
// immediately (no extra separator).
// =============================================================================

#include <string>
#include <vector>
#include <stdexcept>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>

// ---------------------------------------------------------------------------
// Low-level I/O helpers
// ---------------------------------------------------------------------------

// Read exactly n bytes from fd into buf.  Returns false on EOF or error.
inline bool read_exact(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t r = ::read(fd, p, remaining);
        if (r <= 0) return false;   // EOF or error
        p         += r;
        remaining -= static_cast<size_t>(r);
    }
    return true;
}

// Read exactly n bytes into a string.
inline bool read_exact(int fd, string& out, size_t n) {
    out.resize(n);
    return read_exact(fd, &out[0], n);
}

// Write all bytes to fd.  Returns false on error.
inline bool write_all(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t w = ::write(fd, p, remaining);
        if (w <= 0) return false;
        p         += w;
        remaining -= static_cast<size_t>(w);
    }
    return true;
}

inline bool write_all(int fd, const string& s) {
    return write_all(fd, s.data(), s.size());
}

// ---------------------------------------------------------------------------
// Read one \r\n-terminated header line from fd (max 8KB)
// ---------------------------------------------------------------------------
inline bool read_line(int fd, string& line) {
    line.clear();
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return false;
        if (c == '\n') {
            // strip trailing \r if present
            if (!line.empty() && line.back() == '\r')
                line.pop_back();
            return true;
        }
        line += c;
        if (line.size() > 8192) return false;  // malformed
    }
}

// ---------------------------------------------------------------------------
// Response builders (server side)
// ---------------------------------------------------------------------------
inline string ok_response() {
    return "+OK\r\n";
}

inline string ok_value_response(const string& val) {
    return "+OK " + to_string(val.size()) + "\r\n" + val;
}

inline string err_response(const string& msg) {
    return "-ERR " + msg + "\r\n";
}

// ---------------------------------------------------------------------------
// Request sender helpers (client / FE side)
// ---------------------------------------------------------------------------
inline bool send_put(int fd,
                     const string& row,
                     const string& col,
                     const string& val) {
    string hdr = "PUT " + to_string(row.size()) + " "
                             + to_string(col.size()) + " "
                             + to_string(val.size()) + "\r\n";
    return write_all(fd, hdr) &&
           write_all(fd, row) &&
           write_all(fd, col) &&
           write_all(fd, val);
}

inline bool send_get(int fd, const string& row, const string& col) {
    string hdr = "GET " + to_string(row.size()) + " "
                             + to_string(col.size()) + "\r\n";
    return write_all(fd, hdr) && write_all(fd, row) && write_all(fd, col);
}

inline bool send_cput(int fd,
                      const string& row,
                      const string& col,
                      const string& v1,
                      const string& v2) {
    string hdr = "CPUT " + to_string(row.size()) + " "
                              + to_string(col.size()) + " "
                              + to_string(v1.size())  + " "
                              + to_string(v2.size())  + "\r\n";
    return write_all(fd, hdr) &&
           write_all(fd, row) &&
           write_all(fd, col) &&
           write_all(fd, v1)  &&
           write_all(fd, v2);
}

inline bool send_delete(int fd, const string& row, const string& col) {
    string hdr = "DELETE " + to_string(row.size()) + " "
                                + to_string(col.size()) + "\r\n";
    return write_all(fd, hdr) && write_all(fd, row) && write_all(fd, col);
}

// ---------------------------------------------------------------------------
// Response reader (client / FE side)
// ---------------------------------------------------------------------------
struct KVResponse {
    bool ok;            // true = +OK, false = -ERR
    string value;  // populated for GET responses
    string error;  // populated for -ERR responses
};

inline KVResponse read_response(int fd) {
    KVResponse resp{false, "", ""};
    string line;
    if (!read_line(fd, line)) {
        resp.error = "connection closed";
        return resp;
    }
    if (line.rfind("+OK", 0) == 0) {
        resp.ok = true;
        // check for "+OK <vallen>" (GET response)
        if (line.size() > 4) {
            size_t vallen = 0;
            try {
                size_t pos = 0;
                string len_s = line.substr(4);
                vallen = stoul(len_s, &pos);
                if (pos != len_s.size()) {
                    resp.ok = false;
                    resp.error = "bad value length";
                    return resp;
                }
            } catch (...) {
                resp.ok = false;
                resp.error = "bad value length";
                return resp;
            }
            if (!read_exact(fd, resp.value, vallen)) {
                resp.ok    = false;
                resp.error = "truncated value";
            }
        }
    } else if (line.rfind("-ERR", 0) == 0) {
        resp.error = (line.size() > 5) ? line.substr(5) : "";
    }
    return resp;
}

#endif  // PENNCLOUD_KV_PROTOCOL_H
