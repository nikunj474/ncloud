#pragma once
// =============================================================================
// http.h  --  HTTP/1.1 request parser and response builder
// =============================================================================
//
// Handles: GET, POST, HEAD
// Supports: persistent connections, chunked transfer encoding,
//           cookie parsing, multipart/form-data (for file uploads),
//           query string parsing
//
// DESIGN: Every handler returns an HttpResponse. A single send_response()
// function serializes it to the socket. This avoids duplicating send logic
// across 20+ handler functions.
// =============================================================================

#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <functional>

// ---------------------------------------------------------------------------
// HttpRequest
// ---------------------------------------------------------------------------
struct HttpRequest {
    std::string method;      // GET POST HEAD
    std::string path;        // /api/inbox
    std::string query;       // after ?
    std::string version;     // HTTP/1.1
    std::unordered_map<std::string, std::string> headers;
    std::unordered_map<std::string, std::string> cookies;
    std::unordered_map<std::string, std::string> query_params;
    std::string body;        // raw body bytes

    // Convenience accessors
    std::string header(const std::string& name) const {
        // headers stored lowercase
        std::string low = name;
        for (char& c : low) c = static_cast<char>(std::tolower(c));
        auto it = headers.find(low);
        return (it != headers.end()) ? it->second : "";
    }

    std::string cookie(const std::string& name) const {
        auto it = cookies.find(name);
        return (it != cookies.end()) ? it->second : "";
    }

    std::string param(const std::string& name) const {
        auto it = query_params.find(name);
        return (it != query_params.end()) ? it->second : "";
    }

    bool has_cookie(const std::string& name) const {
        return cookies.count(name) > 0;
    }

    // Content-Type helpers
    bool is_json()       const { return header("content-type").find("application/json") != std::string::npos; }
    bool is_multipart()  const { return header("content-type").find("multipart/form-data") != std::string::npos; }
    bool is_urlencoded() const { return header("content-type").find("application/x-www-form-urlencoded") != std::string::npos; }

    bool keep_alive() const {
        std::string conn = header("connection");
        for (char& c : conn) c = static_cast<char>(std::tolower(c));
        // HTTP/1.1 default is keep-alive unless "close" specified
        if (version == "HTTP/1.1") return conn.find("close") == std::string::npos;
        return conn.find("keep-alive") != std::string::npos;
    }
};

// ---------------------------------------------------------------------------
// HttpResponse
// ---------------------------------------------------------------------------
struct HttpResponse {
    int status_code = 200;
    std::string status_text = "OK";
    std::unordered_map<std::string, std::string> headers;
    std::string body;
    bool chunked = false;   // set for SSE or large streaming responses

    // Factory methods for common responses
    static HttpResponse ok(const std::string& body,
                           const std::string& content_type = "text/html; charset=utf-8") {
        HttpResponse r;
        r.body = body;
        r.headers["Content-Type"] = content_type;
        return r;
    }

    static HttpResponse json(const std::string& body) {
        return ok(body, "application/json");
    }

    static HttpResponse redirect(const std::string& location, int code = 302) {
        HttpResponse r;
        r.status_code = code;
        r.status_text = (code == 301) ? "Moved Permanently" : "Found";
        r.headers["Location"] = location;
        return r;
    }

    static HttpResponse error(int code, const std::string& msg) {
        HttpResponse r;
        r.status_code = code;
        r.status_text = msg;
        r.body = "<html><body><h1>" + std::to_string(code) + " " + msg + "</h1></body></html>";
        r.headers["Content-Type"] = "text/html; charset=utf-8";
        return r;
    }

    static HttpResponse not_found()  { return error(404, "Not Found"); }
    static HttpResponse forbidden()  { return error(403, "Forbidden"); }
    static HttpResponse server_error() { return error(500, "Internal Server Error"); }

    void set_cookie(const std::string& name, const std::string& value,
                    const std::string& path = "/",
                    bool http_only = true,
                    int max_age = 86400) {
        std::string cookie = name + "=" + value
                           + "; Path=" + path
                           + "; Max-Age=" + std::to_string(max_age);
        if (http_only) cookie += "; HttpOnly";
        // Multiple Set-Cookie headers -- append index
        headers["Set-Cookie"] = cookie;
    }
};

// ---------------------------------------------------------------------------
// URL encoding / decoding
// ---------------------------------------------------------------------------
inline std::string url_decode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            int val = 0;
            for (int j = 1; j <= 2; ++j) {
                char c = static_cast<char>(std::tolower(s[i + j]));
                val = val * 16 + (c >= 'a' ? c - 'a' + 10 : c - '0');
            }
            out += static_cast<char>(val);
            i += 2;
        } else if (s[i] == '+') {
            out += ' ';
        } else {
            out += s[i];
        }
    }
    return out;
}

inline std::unordered_map<std::string, std::string>
parse_urlencoded(const std::string& body) {
    std::unordered_map<std::string, std::string> params;
    std::istringstream ss(body);
    std::string token;
    while (std::getline(ss, token, '&')) {
        auto eq = token.find('=');
        if (eq == std::string::npos) continue;
        params[url_decode(token.substr(0, eq))] = url_decode(token.substr(eq + 1));
    }
    return params;
}

// ---------------------------------------------------------------------------
// Cookie parser: "name1=val1; name2=val2"
// ---------------------------------------------------------------------------
inline std::unordered_map<std::string, std::string>
parse_cookies(const std::string& cookie_header) {
    std::unordered_map<std::string, std::string> cookies;
    std::istringstream ss(cookie_header);
    std::string token;
    while (std::getline(ss, token, ';')) {
        // trim leading spaces
        size_t start = token.find_first_not_of(' ');
        if (start == std::string::npos) continue;
        token = token.substr(start);
        auto eq = token.find('=');
        if (eq == std::string::npos) continue;
        cookies[token.substr(0, eq)] = token.substr(eq + 1);
    }
    return cookies;
}

// ---------------------------------------------------------------------------
// Query string parser: "key1=val1&key2=val2"
// ---------------------------------------------------------------------------
inline std::unordered_map<std::string, std::string>
parse_query(const std::string& query) {
    return parse_urlencoded(query);
}

// ---------------------------------------------------------------------------
// Multipart form data parser
// Returns map of field_name -> {filename (empty if not file), content}
// ---------------------------------------------------------------------------
struct MultipartPart {
    std::string name;
    std::string filename;   // empty for text fields
    std::string content_type;
    std::string data;
};

inline std::vector<MultipartPart>
parse_multipart(const std::string& body, const std::string& content_type_header) {
    std::vector<MultipartPart> parts;

    // Extract boundary from Content-Type: multipart/form-data; boundary=----WebKitFormBoundary...
    std::string boundary;
    auto bpos = content_type_header.find("boundary=");
    if (bpos == std::string::npos) return parts;
    boundary = "--" + content_type_header.substr(bpos + 9);
    // trim trailing whitespace/CR from boundary
    while (!boundary.empty() && (boundary.back() == '\r' || boundary.back() == ' '))
        boundary.pop_back();

    std::string delim = "\r\n" + boundary;
    size_t pos = body.find(boundary);
    if (pos == std::string::npos) return parts;
    pos += boundary.size() + 2;  // skip past boundary + \r\n

    while (pos < body.size()) {
        // Find end of this part's headers
        size_t hdr_end = body.find("\r\n\r\n", pos);
        if (hdr_end == std::string::npos) break;

        std::string part_headers = body.substr(pos, hdr_end - pos);
        size_t data_start = hdr_end + 4;

        // Find next boundary
        size_t next = body.find(delim, data_start);
        if (next == std::string::npos) break;

        MultipartPart part;
        part.data = body.substr(data_start, next - data_start);
        // Remove trailing \r\n before boundary
        if (part.data.size() >= 2 &&
            part.data[part.data.size()-2] == '\r' &&
            part.data[part.data.size()-1] == '\n')
            part.data.resize(part.data.size() - 2);

        // Parse Content-Disposition
        auto cdpos = part_headers.find("Content-Disposition:");
        if (cdpos != std::string::npos) {
            auto cdend = part_headers.find("\r\n", cdpos);
            std::string cd = part_headers.substr(cdpos, cdend - cdpos);

            auto npos = cd.find("name=\"");
            if (npos != std::string::npos) {
                npos += 6;
                part.name = cd.substr(npos, cd.find('"', npos) - npos);
            }
            auto fpos = cd.find("filename=\"");
            if (fpos != std::string::npos) {
                fpos += 10;
                part.filename = cd.substr(fpos, cd.find('"', fpos) - fpos);
            }
        }

        // Parse Content-Type of part
        auto ctpos = part_headers.find("Content-Type:");
        if (ctpos != std::string::npos) {
            auto ctend = part_headers.find("\r\n", ctpos);
            part.content_type = part_headers.substr(ctpos + 14,
                                    (ctend == std::string::npos ? part_headers.size() : ctend) - ctpos - 14);
        }

        if (!part.name.empty()) parts.push_back(std::move(part));

        pos = next + delim.size();
        if (pos + 2 <= body.size() && body[pos] == '\r') pos += 2;
        else break;  // final boundary "--"
    }
    return parts;
}
