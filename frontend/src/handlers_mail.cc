#include "fe_server.h"
#include "smtp_client.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>
#include <cstdlib>

namespace {

constexpr size_t kMailAttachmentChunkSize = 1024 * 1024; // 1 MB
constexpr size_t kMaxMailAttachmentBytes = 10ull * 1024ull * 1024ull;

std::string new_uid_mail() {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
    static std::atomic<unsigned long long> seq{0};
    std::mt19937 rng(static_cast<unsigned>(ns ^ static_cast<long long>(seq.fetch_add(1))));
    std::uniform_int_distribution<int> dist(0, 0xFFFF);
    std::ostringstream oss;
    oss << ns << "_" << std::hex << std::setw(4) << std::setfill('0') << dist(rng);
    return oss.str();
}

std::string now_str_mail() {
    auto t = std::time(nullptr);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[64];
    std::strftime(buf, sizeof(buf), "%b %d, %Y %H:%M UTC", &tm);
    return buf;
}

std::string esc_json_mail(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 16);
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
    return out;
}

std::vector<std::string> split_csv_mail(const std::string& s) {
    std::vector<std::string> out;
    if (s.empty()) return out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        if (!tok.empty()) out.push_back(tok);
    }
    return out;
}

std::string join_csv_mail(const std::vector<std::string>& items) {
    std::string out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i) out += ',';
        out += items[i];
    }
    return out;
}

std::string mail_row(const std::string& user) { return user + ":mail"; }
std::string mail_folder_col(const std::string& folder) { return "folder:" + folder; }
std::string contacts_row(const std::string& user) { return user + ":contacts"; }
std::string contacts_col() { return "list"; }
std::string attachment_row(const std::string& att_id) { return "mail:att:" + att_id; }
std::string attachment_chunk_col(size_t idx) { return "chunk:" + std::to_string(idx); }

std::string normalize_folder(const std::string& folder) {
    if (folder == "sent" || folder == "trash") return folder;
    return "inbox";
}

std::string lower_mail(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string penncloud_mail_domain() {
    const char* env = std::getenv("PENNCLOUD_MAIL_DOMAIN");
    std::string domain = env && *env ? env : "penncloud.local";
    domain = lower_mail(domain);
    if (domain.empty() || domain.find('@') != std::string::npos ||
        domain.find('/') != std::string::npos ||
        domain.find('\r') != std::string::npos ||
        domain.find('\n') != std::string::npos) {
        return "penncloud.local";
    }
    return domain;
}

bool is_local_penncloud_domain(const std::string& domain_raw) {
    std::string domain = lower_mail(domain_raw);
    return domain == "penncloud" ||
           domain == "penncloud.com" ||
           domain == "penncloud.local" ||
           domain == penncloud_mail_domain();
}

std::string mail_content_disposition_attachment(const std::string& name) {
    std::string escaped;
    for (unsigned char c : name) {
        if (c == '\r' || c == '\n') continue;
        if (c < 0x20 || c == 0x7F) {
            escaped.push_back('_');
            continue;
        }
        if (c == '"' || c == '\\') escaped.push_back('\\');
        escaped.push_back(static_cast<char>(c));
    }
    if (escaped.empty()) escaped = "attachment";
    return "attachment; filename=\"" + escaped + "\"";
}

std::string folder_index(KVClient* kv, const std::string& user, const std::string& folder) {
    std::string val = kv->get_str(mail_row(user), mail_folder_col(normalize_folder(folder)));
    if (val.empty() && normalize_folder(folder) == "inbox") {
        val = kv->get_str(mail_row(user), "index");
    }
    return val;
}

std::vector<std::string> folder_uids(KVClient* kv, const std::string& user, const std::string& folder) {
    return split_csv_mail(folder_index(kv, user, folder));
}

bool uid_in_folder(KVClient* kv, const std::string& user, const std::string& folder, const std::string& uid) {
    auto ids = folder_uids(kv, user, folder);
    return std::find(ids.begin(), ids.end(), uid) != ids.end();
}

bool remove_uid_from_folder(KVClient* kv, const std::string& user, const std::string& folder, const std::string& uid) {
    const std::string f = normalize_folder(folder);
    const std::string row = mail_row(user);
    const std::string col = mail_folder_col(f);
    for (int attempt = 0; attempt < 12; ++attempt) {
        std::string old_value;
        KVReadStatus st = kv->get_status(row, col, old_value);
        if (st == KVReadStatus::Unavailable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
            continue;
        }
        if (st == KVReadStatus::NotFound && f == "inbox") old_value = kv->get_str(row, "index");
        auto ids = split_csv_mail(old_value);
        size_t before = ids.size();
        ids.erase(std::remove(ids.begin(), ids.end(), uid), ids.end());
        if (ids.size() == before) return true;
        std::string next = join_csv_mail(ids);
        if (st == KVReadStatus::Found) {
            if (kv->cput(row, col, old_value, next)) {
                if (f == "inbox") kv->put(row, "index", next);
                return true;
            }
        } else {
            (void)kv->put(row, col, next);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }
    return false;
}

bool prepend_uid_to_folder(KVClient* kv, const std::string& user, const std::string& folder, const std::string& uid) {
    const std::string f = normalize_folder(folder);
    const std::string row = mail_row(user);
    const std::string col = mail_folder_col(f);
    for (int attempt = 0; attempt < 16; ++attempt) {
        std::string old_value;
        KVReadStatus st = kv->get_status(row, col, old_value);
        if (st == KVReadStatus::Unavailable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
            continue;
        }
        if (st == KVReadStatus::NotFound && f == "inbox") old_value = kv->get_str(row, "index");
        auto ids = split_csv_mail(old_value);
        ids.erase(std::remove(ids.begin(), ids.end(), uid), ids.end());
        ids.insert(ids.begin(), uid);
        std::string next = join_csv_mail(ids);
        if (st == KVReadStatus::Found) {
            if (next == old_value || kv->cput(row, col, old_value, next)) {
                if (f == "inbox") kv->put(row, "index", next);
                return true;
            }
        } else {
            (void)kv->put(row, col, next);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }
    return false;
}

std::string mail_meta_col(const std::string& uid) { return "msg:" + uid; }
std::string mail_body_col(const std::string& uid) { return "body:" + uid; }

size_t attachment_chunk_count_for_size(size_t nbytes) {
    return (nbytes + kMailAttachmentChunkSize - 1) / kMailAttachmentChunkSize;
}

bool put_attachment_bytes(KVClient* kv, const std::string& att_id, const std::string& data) {
    const std::string row = attachment_row(att_id);
    const size_t chunks = attachment_chunk_count_for_size(data.size());
    if (!kv->put(row, "chunks", std::to_string(chunks))) return false;
    for (size_t i = 0; i < chunks; ++i) {
        size_t off = i * kMailAttachmentChunkSize;
        size_t len = std::min(kMailAttachmentChunkSize, data.size() - off);
        if (!kv->put(row, attachment_chunk_col(i), data.substr(off, len))) {
            return false;
        }
    }
    return true;
}

std::string get_attachment_bytes(KVClient* kv, const std::string& att_id) {
    const std::string row = attachment_row(att_id);
    std::string chunks_s = kv->get_str(row, "chunks");
    if (chunks_s.empty()) return "";
    size_t chunks = 0;
    try { chunks = static_cast<size_t>(std::stoull(chunks_s)); }
    catch (...) { return ""; }
    std::string out;
    for (size_t i = 0; i < chunks; ++i) out += kv->get_str(row, attachment_chunk_col(i));
    return out;
}

std::string attachment_json(KVClient* kv, const std::string& att_id) {
    std::string name = kv->get_str(attachment_row(att_id), "name");
    std::string mime = kv->get_str(attachment_row(att_id), "mime");
    std::string size = kv->get_str(attachment_row(att_id), "size");
    return std::string("{") +
           "\"id\":\"" + esc_json_mail(att_id) + "\"," +
           "\"name\":\"" + esc_json_mail(name) + "\"," +
           "\"mime\":\"" + esc_json_mail(mime.empty() ? "application/octet-stream" : mime) + "\"," +
           "\"size\":" + (size.empty() ? "0" : size) +
           "}";
}

bool user_can_access_attachment(KVClient* kv, const std::string& user, const std::string& att_id) {
    if (user.empty() || att_id.empty()) return false;
    std::string owner = kv->get_str(attachment_row(att_id), "owner");
    std::string recipient = kv->get_str(attachment_row(att_id), "recipient");
    return (!owner.empty() && owner == user) || (!recipient.empty() && recipient == user);
}

void delete_attachment_bytes(KVClient* kv, const std::string& att_id) {
    const std::string row = attachment_row(att_id);
    std::string chunks_s = kv->get_str(row, "chunks");
    size_t chunks = 0;
    try { if (!chunks_s.empty()) chunks = static_cast<size_t>(std::stoull(chunks_s)); }
    catch (...) { chunks = 0; }
    for (size_t i = 0; i < chunks; ++i) kv->del(row, attachment_chunk_col(i));
    kv->del(row, "chunks");
}

void delete_attachment_record(KVClient* kv, const std::string& att_id) {
    if (att_id.empty()) return;
    const std::string row = attachment_row(att_id);
    delete_attachment_bytes(kv, att_id);
    kv->del(row, "name");
    kv->del(row, "mime");
    kv->del(row, "size");
    kv->del(row, "owner");
    kv->del(row, "recipient");
}

std::vector<std::string> parse_attachment_ids_csv(const std::string& csv) {
    auto ids = split_csv_mail(csv);
    ids.erase(std::remove_if(ids.begin(), ids.end(), [](const std::string& s){ return s.empty(); }), ids.end());
    return ids;
}

std::vector<std::string> finalize_attachment_ids_for_send(KVClient* kv,
                                                          const std::string& user,
                                                          const std::string& recipient,
                                                          const std::vector<std::string>& requested_ids,
                                                          std::string& err) {
    std::vector<std::string> out;
    for (const auto& src_id : requested_ids) {
        if (!user_can_access_attachment(kv, user, src_id)) {
            err = "attachment access denied";
            return {};
        }
        std::string name = kv->get_str(attachment_row(src_id), "name");
        std::string mime = kv->get_str(attachment_row(src_id), "mime");
        std::string size = kv->get_str(attachment_row(src_id), "size");
        std::string bytes = get_attachment_bytes(kv, src_id);
        if (!size.empty() && !bytes.empty()) {
            // keep as is
        }
        std::string dst_id = new_uid_mail();
        const std::string row = attachment_row(dst_id);
        if (!kv->put(row, "name", name) ||
            !kv->put(row, "mime", mime.empty() ? "application/octet-stream" : mime) ||
            !kv->put(row, "size", size.empty() ? std::to_string(bytes.size()) : size) ||
            !kv->put(row, "owner", user) ||
            !kv->put(row, "recipient", recipient) ||
            !put_attachment_bytes(kv, dst_id, bytes)) {
            delete_attachment_record(kv, dst_id);
            for (const auto& copied_id : out) delete_attachment_record(kv, copied_id);
            err = "failed to persist attachment";
            return {};
        }
        out.push_back(dst_id);
    }
    return out;
}

std::string attachments_json_from_ids(KVClient* kv, const std::vector<std::string>& ids) {
    std::string json = "[";
    bool first = true;
    for (const auto& id : ids) {
        if (!first) json += ",";
        first = false;
        json += attachment_json(kv, id);
    }
    json += "]";
    return json;
}

std::string make_meta_json(const std::string& from,
                           const std::string& to,
                           const std::string& subject,
                           const std::string& time_str,
                           const std::string& uid,
                           const std::string& attachments_json) {
    return std::string("{")
         + "\"uid\":\""     + esc_json_mail(uid)      + "\"," 
         + "\"from\":\""    + esc_json_mail(from)     + "\"," 
         + "\"to\":\""      + esc_json_mail(to)       + "\"," 
         + "\"subject\":\"" + esc_json_mail(subject)  + "\"," 
         + "\"time\":\""    + esc_json_mail(time_str) + "\"," 
         + "\"attachments\":" + attachments_json
         + "}";
}

std::string contacts_blob_get(KVClient* kv, const std::string& user) {
    return kv->get_str(contacts_row(user), contacts_col());
}

std::vector<std::pair<std::string, std::string>> parse_contacts_blob(const std::string& blob) {
    std::vector<std::pair<std::string, std::string>> out;
    std::istringstream ss(blob);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.empty()) continue;
        auto tab = line.find('\t');
        if (tab == std::string::npos) continue;
        out.push_back({line.substr(0, tab), line.substr(tab + 1)});
    }
    return out;
}

std::string contacts_blob_serialize(const std::vector<std::pair<std::string, std::string>>& contacts) {
    std::string out;
    for (const auto& c : contacts) {
        if (!out.empty()) out += '\n';
        out += c.first + '\t' + c.second;
    }
    return out;
}

std::string json_contacts(const std::vector<std::pair<std::string, std::string>>& contacts) {
    std::string json = "[";
    bool first = true;
    for (const auto& c : contacts) {
        if (!first) json += ",";
        first = false;
        json += std::string("{") +
                "\"name\":\"" + esc_json_mail(c.first) + "\"," +
                "\"email\":\"" + esc_json_mail(c.second) + "\"}";
    }
    json += "]";
    return json;
}

bool message_belongs_to_user_folder(KVClient* kv, const std::string& user, const std::string& uid, const std::string& folder) {
    return uid_in_folder(kv, user, folder, uid);
}

bool meta_contains_attachment(const std::string& meta, const std::string& att_id) {
    return meta.find(std::string("\"id\":\"") + att_id + "\"") != std::string::npos;
}

std::vector<std::string> attachment_ids_from_meta(const std::string& meta) {
    std::vector<std::string> ids;
    const std::string needle = "\"id\":\"";
    size_t pos = 0;
    while ((pos = meta.find(needle, pos)) != std::string::npos) {
        pos += needle.size();
        size_t end = meta.find('"', pos);
        if (end == std::string::npos) break;
        ids.push_back(meta.substr(pos, end - pos));
        pos = end + 1;
    }
    return ids;
}

void delete_self_owned_attachment(KVClient* kv, const std::string& user, const std::string& att_id) {
    const std::string row = attachment_row(att_id);
    if (kv->get_str(row, "owner") != user || kv->get_str(row, "recipient") != user) {
        return;
    }
    size_t chunks = 0;
    try { chunks = static_cast<size_t>(std::stoull(kv->get_str(row, "chunks"))); }
    catch (...) { chunks = 0; }
    for (size_t i = 0; i < chunks; ++i) kv->del(row, attachment_chunk_col(i));
    kv->del(row, "chunks");
    kv->del(row, "name");
    kv->del(row, "mime");
    kv->del(row, "size");
    kv->del(row, "owner");
    kv->del(row, "recipient");
}

std::string local_recipient_from_to(const std::string& to, bool& is_external) {
    is_external = false;
    if (to.find('@') == std::string::npos) return to;
    auto at = to.find('@');
    std::string domain = to.substr(at + 1);
    if (is_local_penncloud_domain(domain)) return to.substr(0, at);
    is_external = true;
    return "";
}

bool store_message(KVClient* kv,
                   const std::string& owner_user,
                   const std::string& folder,
                   const std::string& uid,
                   const std::string& meta,
                   const std::string& body) {
    const std::string row = mail_row(owner_user);
    if (!kv->put(row, mail_meta_col(uid), meta) || !kv->put(row, mail_body_col(uid), body)) return false;
    if (!prepend_uid_to_folder(kv, owner_user, folder, uid)) {
        kv->del(row, mail_meta_col(uid));
        kv->del(row, mail_body_col(uid));
        return false;
    }
    return true;
}

void delete_stored_message(KVClient* kv,
                           const std::string& owner_user,
                           const std::string& folder,
                           const std::string& uid) {
    remove_uid_from_folder(kv, owner_user, folder, uid);
    kv->del(mail_row(owner_user), mail_meta_col(uid));
    kv->del(mail_row(owner_user), mail_body_col(uid));
}

} // namespace

HttpResponse FEServer::handle_inbox(const HttpRequest& req, const std::string& user) {
    const std::string folder = normalize_folder(req.param("folder"));
    auto uids = folder_uids(kv_.get(), user, folder);
    std::string emails_json = "[";
    bool first = true;
    for (const auto& uid : uids) {
        std::string meta = kv_->get_str(mail_row(user), mail_meta_col(uid));
        if (meta.empty()) continue;
        if (!first) emails_json += ",";
        first = false;
        emails_json += meta;
    }
    emails_json += "]";
    return HttpResponse::json("{\"ok\":true,\"folder\":" + json_str(folder) + ",\"emails\":" + emails_json + "}");
}

HttpResponse FEServer::handle_get_email(const HttpRequest& req,
                                        const std::string& user,
                                        const std::string& uid) {
    const std::string folder = normalize_folder(req.param("folder"));
    if (!message_belongs_to_user_folder(kv_.get(), user, uid, folder) &&
        !message_belongs_to_user_folder(kv_.get(), user, uid, "inbox") &&
        !message_belongs_to_user_folder(kv_.get(), user, uid, "sent") &&
        !message_belongs_to_user_folder(kv_.get(), user, uid, "trash")) {
        return HttpResponse::json("{\"ok\":false,\"error\":\"not found\"}");
    }

    const std::string row = mail_row(user);
    std::string meta = kv_->get_str(row, mail_meta_col(uid));
    std::string body = kv_->get_str(row, mail_body_col(uid));
    if (meta.empty()) return HttpResponse::json("{\"ok\":false,\"error\":\"not found\"}");
    if (meta.front() != '{' || meta.back() != '}') {
        return HttpResponse::json("{\"ok\":false,\"error\":\"corrupt metadata\"}");
    }

    std::string full = meta;
    full.pop_back();
    full += ",\"body\":\"" + esc_json_mail(body) + "\"}";
    return HttpResponse::json("{\"ok\":true,\"email\":" + full + "}");
}

HttpResponse FEServer::handle_mail_upload_attachment(const HttpRequest& req, const std::string& user) {
    if (!req.is_multipart()) {
        return HttpResponse::json(R"({"ok":false,"error":"expected multipart form data"})");
    }
    auto parts = parse_multipart(req.body, req.header("content-type"));
    MultipartPart file_part;
    bool found = false;
    for (const auto& p : parts) {
        if (p.name == "attachment") {
            file_part = p;
            found = true;
            break;
        }
    }
    if (!found || file_part.filename.empty()) {
        return HttpResponse::json(R"({"ok":false,"error":"missing attachment file"})");
    }
    if (file_part.data.size() > kMaxMailAttachmentBytes) {
        return HttpResponse::json(R"({"ok":false,"error":"attachment exceeds 10 MB limit"})");
    }

    const std::string att_id = new_uid_mail();
    const std::string row = attachment_row(att_id);
    const std::string mime = file_part.content_type.empty() ? "application/octet-stream" : file_part.content_type;

    if (!kv_->put(row, "name", file_part.filename) ||
        !kv_->put(row, "mime", mime) ||
        !kv_->put(row, "size", std::to_string(file_part.data.size())) ||
        !kv_->put(row, "owner", user) ||
        !kv_->put(row, "recipient", "") ||
        !put_attachment_bytes(kv_.get(), att_id, file_part.data)) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to store attachment"})");
    }

    std::string body = std::string("{\"ok\":true,\"attachment\":") + attachment_json(kv_.get(), att_id) + "}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_mail_download_attachment(const HttpRequest& req, const std::string& user) {
    const std::string uid = req.param("uid");
    const std::string att_id = req.param("att");
    const std::string folder = normalize_folder(req.param("folder"));
    if (uid.empty() || att_id.empty()) return HttpResponse::error(400, "Bad Request");

    if (!message_belongs_to_user_folder(kv_.get(), user, uid, folder) &&
        !message_belongs_to_user_folder(kv_.get(), user, uid, "inbox") &&
        !message_belongs_to_user_folder(kv_.get(), user, uid, "sent") &&
        !message_belongs_to_user_folder(kv_.get(), user, uid, "trash")) {
        return HttpResponse::error(404, "Not Found");
    }

    std::string meta = kv_->get_str(mail_row(user), mail_meta_col(uid));
    if (meta.empty() || !meta_contains_attachment(meta, att_id)) {
        return HttpResponse::error(404, "Not Found");
    }

    std::string bytes = get_attachment_bytes(kv_.get(), att_id);
    if (bytes.empty() && kv_->get_str(attachment_row(att_id), "size") != "0") {
        return HttpResponse::error(404, "Not Found");
    }

    HttpResponse resp = HttpResponse::ok(bytes, kv_->get_str(attachment_row(att_id), "mime"));
    std::string name = kv_->get_str(attachment_row(att_id), "name");
    if (!name.empty()) resp.headers["Content-Disposition"] = mail_content_disposition_attachment(name);
    return resp;
}

HttpResponse FEServer::handle_send_email(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string to = params["to"];
    std::string subject = params["subject"];
    std::string body = params["body"];
    std::string attachment_ids_csv = params["attachment_ids"];

    if (to.empty() || subject.empty()) {
        return HttpResponse::json("{\"ok\":false,\"error\":\"Missing to or subject\"}");
    }

    const std::string from_addr = user + "@" + penncloud_mail_domain();
    const std::string time_str = now_str_mail();
    bool is_external = false;
    std::string recipient = local_recipient_from_to(to, is_external);
    std::vector<std::string> requested_attachment_ids = parse_attachment_ids_csv(attachment_ids_csv);

    if (is_external && !requested_attachment_ids.empty()) {
        return HttpResponse::json(R"({"ok":false,"error":"attachments are supported only for local PennCloud recipients"})");
    }

    if (is_external) {
        SMTPExternalResult delivery = smtp_send_external(from_addr, to, subject, body);
        if (!delivery.ok) {
            std::cerr << "[smtp-out] failed to " << to << " error=" << delivery.error << "\n";
            return HttpResponse::json("{\"ok\":false,\"error\":" +
                                      json_str("External delivery failed: " + delivery.error) + "}");
        }
        std::cerr << "[smtp-out] delivered to " << to << "\n";

        std::string sent_uid = new_uid_mail();
        std::string sent_meta = make_meta_json(from_addr, to, subject, time_str, sent_uid, "[]");
        if (!store_message(kv_.get(), user, "sent", sent_uid, sent_meta, body)) {
            return HttpResponse::json("{\"ok\":true,\"uid\":\"" + esc_json_mail(sent_uid) +
                                      "\",\"external\":true,\"delivered\":true," +
                                      "\"warning\":\"delivered externally, but failed to save sent copy\"}");
        }
        return HttpResponse::json("{\"ok\":true,\"uid\":\"" + esc_json_mail(sent_uid) +
                                  "\",\"external\":true,\"delivered\":true}");
    }

    std::string recipient_pwd;
    KVReadStatus recipient_status = kv_->get_status(recipient, "pwd", recipient_pwd);
    if (recipient_status == KVReadStatus::Unavailable) {
        return HttpResponse::json(
            R"({"ok":false,"error":"Storage temporarily unavailable, please try again"})");
    }
    if (recipient_status == KVReadStatus::NotFound) {
        return HttpResponse::json("{\"ok\":false,\"error\":\"No such local user\"}");
    }

    std::string att_err;
    std::vector<std::string> inbox_attachment_ids = finalize_attachment_ids_for_send(kv_.get(), user, recipient, requested_attachment_ids, att_err);
    if (!requested_attachment_ids.empty() && inbox_attachment_ids.empty() && !att_err.empty()) {
        return HttpResponse::json("{\"ok\":false,\"error\":" + json_str(att_err) + "}");
    }
    std::vector<std::string> sent_attachment_ids = finalize_attachment_ids_for_send(kv_.get(), user, user, requested_attachment_ids, att_err);
    if (!requested_attachment_ids.empty() && sent_attachment_ids.empty() && !att_err.empty()) {
        return HttpResponse::json("{\"ok\":false,\"error\":" + json_str(att_err) + "}");
    }

    std::string inbox_attachments_json = attachments_json_from_ids(kv_.get(), inbox_attachment_ids);
    std::string sent_attachments_json = attachments_json_from_ids(kv_.get(), sent_attachment_ids);
    std::string inbox_uid = new_uid_mail();
    std::string sent_uid = new_uid_mail();

    std::string inbox_meta = make_meta_json(from_addr, to, subject, time_str, inbox_uid, inbox_attachments_json);
    std::string sent_meta = make_meta_json(from_addr, to, subject, time_str, sent_uid, sent_attachments_json);

    if (!store_message(kv_.get(), user, "sent", sent_uid, sent_meta, body)) {
        return HttpResponse::json(R"({"ok":false,"error":"mail delivery failed"})");
    }
    if (!store_message(kv_.get(), recipient, "inbox", inbox_uid, inbox_meta, body)) {
        delete_stored_message(kv_.get(), user, "sent", sent_uid);
        return HttpResponse::json(R"({"ok":false,"error":"mail delivery failed"})");
    }

    kv_->put("notify:" + recipient, "latest", "uid:" + inbox_uid);
    return HttpResponse::json("{\"ok\":true,\"uid\":\"" + esc_json_mail(sent_uid) + "\",\"inbox_uid\":\"" + esc_json_mail(inbox_uid) + "\"}");
}

HttpResponse FEServer::handle_delete_email(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string uid = params["uid"];
    std::string folder = normalize_folder(params["folder"]);
    if (uid.empty()) return HttpResponse::json(R"({"ok":false,"error":"Missing uid"})");

    if (!uid_in_folder(kv_.get(), user, folder, uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"message not found in folder"})");
    }

    if (folder == "trash") {
        std::string meta = kv_->get_str(mail_row(user), mail_meta_col(uid));
        if (!remove_uid_from_folder(kv_.get(), user, "trash", uid)) {
            return HttpResponse::json(R"({"ok":false,"error":"delete failed"})");
        }
        if (!kv_->del(mail_row(user), mail_meta_col(uid)) ||
            !kv_->del(mail_row(user), mail_body_col(uid))) {
            return HttpResponse::json(R"({"ok":false,"error":"delete failed"})");
        }
        for (const auto& att_id : attachment_ids_from_meta(meta)) {
            delete_self_owned_attachment(kv_.get(), user, att_id);
        }
        return HttpResponse::json(R"({"ok":true,"deleted":true})");
    }

    if (!prepend_uid_to_folder(kv_.get(), user, "trash", uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"move to trash failed"})");
    }
    if (!remove_uid_from_folder(kv_.get(), user, folder, uid)) {
        bool rolled_back = remove_uid_from_folder(kv_.get(), user, "trash", uid);
        if (!rolled_back) {
            std::cerr << "[mail] move-to-trash rollback failed for uid " << uid
                      << " user " << user << "\n";
        }
        return HttpResponse::json(R"({"ok":false,"error":"move to trash failed"})");
    }
    return HttpResponse::json(R"({"ok":true,"deleted":false})");
}

HttpResponse FEServer::handle_restore_email(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string uid = params["uid"];
    if (uid.empty()) return HttpResponse::json(R"({"ok":false,"error":"Missing uid"})");
    if (!uid_in_folder(kv_.get(), user, "trash", uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"message not in trash"})");
    }
    if (!prepend_uid_to_folder(kv_.get(), user, "inbox", uid)) {
        return HttpResponse::json(R"({"ok":false,"error":"restore failed"})");
    }
    if (!remove_uid_from_folder(kv_.get(), user, "trash", uid)) {
        bool rolled_back = remove_uid_from_folder(kv_.get(), user, "inbox", uid);
        if (!rolled_back) {
            std::cerr << "[mail] restore rollback failed for uid " << uid
                      << " user " << user << "\n";
        }
        return HttpResponse::json(R"({"ok":false,"error":"restore failed"})");
    }
    return HttpResponse::json(R"({"ok":true})");
}

HttpResponse FEServer::handle_contacts_list(const HttpRequest&, const std::string& user) {
    auto contacts = parse_contacts_blob(contacts_blob_get(kv_.get(), user));
    return HttpResponse::json("{\"ok\":true,\"contacts\":" + json_contacts(contacts) + "}");
}

HttpResponse FEServer::handle_contacts_add(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string email = params["email"];
    std::string name = params["name"];
    if (email.empty() || name.empty()) return HttpResponse::json(R"({"ok":false,"error":"name and email required"})");

    auto contacts = parse_contacts_blob(contacts_blob_get(kv_.get(), user));
    bool updated = false;
    for (auto& c : contacts) {
        if (c.second == email) { c.first = name; updated = true; break; }
    }
    if (!updated) contacts.push_back({name, email});
    if (!kv_->put(contacts_row(user), contacts_col(), contacts_blob_serialize(contacts))) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to save contact"})");
    }
    return HttpResponse::json(R"({"ok":true})");
}

HttpResponse FEServer::handle_contacts_delete(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string email = params["email"];
    if (email.empty()) return HttpResponse::json(R"({"ok":false,"error":"email required"})");

    auto contacts = parse_contacts_blob(contacts_blob_get(kv_.get(), user));
    contacts.erase(std::remove_if(contacts.begin(), contacts.end(), [&](const auto& c) { return c.second == email; }), contacts.end());
    if (!kv_->put(contacts_row(user), contacts_col(), contacts_blob_serialize(contacts))) {
        return HttpResponse::json(R"({"ok":false,"error":"failed to delete contact"})");
    }
    return HttpResponse::json(R"({"ok":true})");
}
