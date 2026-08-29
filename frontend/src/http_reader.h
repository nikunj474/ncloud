// http_reader.h  --  Read one HTTP/1.1 request from a socket fd

//
// Handles:
//   - Request line + headers parsing
//   - Content-Length bodies (POST login, POST send-email, etc.)
//   - Chunked transfer encoding (file uploads)
//   - Cookie header -> cookie map
//   - Query string -> param map
//
// Returns false on connection close or parse error.


#include "http.h"
#include <unistd.h>
#include <sys/socket.h>
#include <string>
#include <sstream>
#include <iostream>
#include <cctype>
#include <algorithm>

inline std::string http_trim_copy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(start, end - start);
}

inline bool http_parse_size_t(const std::string& raw, int base, size_t& out) {
    std::string s = http_trim_copy(raw);
    if (s.empty() || s[0] == '+' || s[0] == '-') return false;
    try {
        size_t pos = 0;
        unsigned long parsed = std::stoul(s, &pos, base);
        if (pos != s.size()) return false;
        out = static_cast<size_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

// Read one \r\n-terminated line from fd (max 8KB)
inline bool http_read_line(int fd, std::string& line) {
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
        if (line.size() > 8192) return false;
    }
}

// Read exactly n bytes from socket into string
inline bool http_read_exact(int fd, std::string& out, size_t n) {
    out.resize(n);
    size_t got = 0;
    while (got < n) {
        ssize_t r = ::recv(fd, &out[got], n - got, 0);
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

inline bool http_write_all(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static constexpr size_t kMaxUploadFileBytes = 1023ull * 1024ull * 1024ull;  // user-visible file cap
static constexpr size_t kMaxMultipartOverheadBytes = 1ull * 1024ull * 1024ull;
static constexpr size_t kMaxRequestBodyBytes = kMaxUploadFileBytes + kMaxMultipartOverheadBytes;

// Read a chunked body from socket
inline bool read_chunked_body(int fd, std::string& body) {
    static constexpr size_t kMaxChunkedBodyBytes = kMaxRequestBodyBytes;
    body.clear();
    while (true) {
        std::string size_line;
        if (!http_read_line(fd, size_line)) return false;
        // size_line is hex chunk size, possibly with extensions after ;
        size_t semi = size_line.find(';');
        if (semi != std::string::npos) size_line = size_line.substr(0, semi);

        size_t chunk_size = 0;
        if (!http_parse_size_t(size_line, 16, chunk_size)) return false;
        if (chunk_size == 0) {
            // Consume optional trailers, ending with an empty line.
            std::string trailer;
            do {
                if (!http_read_line(fd, trailer)) return false;
            } while (!trailer.empty());
            return true;
        }
        if (chunk_size > kMaxChunkedBodyBytes ||
            body.size() > kMaxChunkedBodyBytes - chunk_size) {
            return false;
        }

        std::string chunk;
        if (!http_read_exact(fd, chunk, chunk_size)) return false;
        body += chunk;

        // Consume trailing CRLF after chunk data
        std::string crlf;
        if (!http_read_line(fd, crlf)) return false;
    }
}

// read_http_request: reads one complete HTTP/1.1 request from fd
// Returns false on connection close / error.

inline bool read_http_request(int fd, HttpRequest& req) {
    req = HttpRequest{};

    // Request line 
    std::string line;
    if (!http_read_line(fd, line) || line.empty()) return false;

    std::istringstream rl(line);
    rl >> req.method >> req.path >> req.version;
    if (req.method.empty() || req.path.empty()) return false;

    // Split path and query string
    auto qpos = req.path.find('?');
    if (qpos != std::string::npos) {
        req.query = req.path.substr(qpos + 1);
        req.path  = req.path.substr(0, qpos);
        req.query_params = parse_query(req.query);
    }

    //  Headers 
    while (true) {
        if (!http_read_line(fd, line)) return false;
        if (line.empty()) break;   // blank line = end of headers

        auto colon = line.find(':');
        if (colon == std::string::npos) continue;

        std::string key   = line.substr(0, colon);
        std::string value = line.substr(colon + 1);

        // Lowercase key for case-insensitive lookup
        for (char& c : key)   c = static_cast<char>(std::tolower(c));
        // Trim leading space from value
        size_t vs = value.find_first_not_of(' ');
        if (vs != std::string::npos) value = value.substr(vs);

        req.headers[key] = value;
    }

    // Parse cookies
    auto cookie_hdr = req.header("cookie");
    if (!cookie_hdr.empty()) req.cookies = parse_cookies(cookie_hdr);

    //  Body 
    std::string transfer_enc = req.header("transfer-encoding");
    for (char& c : transfer_enc) c = static_cast<char>(std::tolower(c));
    std::string expect = req.header("expect");
    for (char& c : expect) c = static_cast<char>(std::tolower(c));
    const bool wants_continue = expect.find("100-continue") != std::string::npos;

    if (transfer_enc.find("chunked") != std::string::npos) {
        if (wants_continue) {
            static const std::string kContinue = "HTTP/1.1 100 Continue\r\n\r\n";
            if (!http_write_all(fd, kContinue)) return false;
        }
        if (!read_chunked_body(fd, req.body)) return false;
    } else {
        std::string cl_str = req.header("content-length");
        if (!cl_str.empty()) {
            size_t content_length = 0;
            if (!http_parse_size_t(cl_str, 10, content_length)) return false;
            if (content_length > kMaxRequestBodyBytes) {
                // Send a proper 413 before closing so the browser gets an error
                // instead of a silent dropped connection.
                const std::string body =
                    "{\"ok\":false,\"error\":\"file exceeds 1023 MB limit\"}";
                const std::string k413 =
                    "HTTP/1.1 413 Content Too Large\r\n"
                    "Content-Type: application/json\r\n"
                    "Content-Length: " + std::to_string(body.size()) + "\r\n"
                    "Connection: close\r\n\r\n" + body;
                http_write_all(fd, k413);
                return false;
            }
            if (wants_continue && content_length > 0) {
                static const std::string kContinue = "HTTP/1.1 100 Continue\r\n\r\n";
                if (!http_write_all(fd, kContinue)) return false;
            }
            if (!http_read_exact(fd, req.body, content_length)) return false;
        }
    }

    return true;
}

inline std::string http_chunk_prefix(size_t n) {
    std::ostringstream oss;
    oss << std::hex << n;
    return oss.str();
}

inline bool send_http_chunked_body(int fd, const std::string& body) {
    static constexpr size_t kChunkSize = 16 * 1024;
    size_t off = 0;
    while (off < body.size()) {
        size_t take = std::min(kChunkSize, body.size() - off);
        std::string chunk = http_chunk_prefix(take) + "\r\n";
        chunk.append(body.data() + off, take);
        chunk += "\r\n";
        if (!http_write_all(fd, chunk)) return false;
        off += take;
    }
    return http_write_all(fd, "0\r\n\r\n");
}

// send_http_response: serialize HttpResponse to socket
// Supports: Content-Length for normal responses,
//           Transfer-Encoding: chunked for streaming/larger responses.

inline bool send_http_response(int fd, const HttpResponse& resp,
                                bool is_head = false) {
    std::string out;
    out.reserve(512 + resp.body.size());

    // Status line
    out += "HTTP/1.1 " + std::to_string(resp.status_code)
         + " " + resp.status_text + "\r\n";

    // Standard headers
    out += "Server: NCloud/1.0\r\n";
    auto conn_it = resp.headers.find("Connection");
    out += "Connection: " + (conn_it == resp.headers.end() ? std::string("close") : conn_it->second) + "\r\n";

    // Content-Length (always set unless chunked)
    if (!resp.chunked) {
        out += "Content-Length: " + std::to_string(resp.body.size()) + "\r\n";
    } else {
        out += "Transfer-Encoding: chunked\r\n";
    }

    // Custom headers (Content-Type, Location, Set-Cookie, etc.)
    for (auto& [k, v] : resp.headers) {
        if (k == "Connection") continue;
        out += k + ": " + v + "\r\n";
    }
    for (const auto& cookie : resp.set_cookies) {
        out += "Set-Cookie: " + cookie + "\r\n";
    }

    // CORS header for API endpoints (useful during dev)
    out += "Access-Control-Allow-Origin: *\r\n";

    out += "\r\n";  // end of headers

    if (is_head) return http_write_all(fd, out);

    if (resp.chunked) {
        if (!http_write_all(fd, out)) return false;
        return send_http_chunked_body(fd, resp.body);
    }

    // Body
    if (!resp.chunked) {
        out += resp.body;
    }

    return http_write_all(fd, out);
}

// Send a single SSE event (for F1 -- live inbox)
// Format: "data: <payload>\n\n"
inline bool send_sse_event(int fd, const std::string& event_type,
                           const std::string& data) {
    std::string msg;
    if (!event_type.empty()) msg += "event: " + event_type + "\n";
    msg += "data: " + data + "\n\n";

    // HTTP chunk sizes must be HEX, not decimal
    std::string chunk = http_chunk_prefix(msg.size()) + "\r\n" + msg + "\r\n";
    return http_write_all(fd, chunk);
}
