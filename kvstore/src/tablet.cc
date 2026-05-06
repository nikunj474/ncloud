
#include "tablet.h"
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <sys/stat.h>
#include <unistd.h>
using namespace std;

// Write a 32-bit integer in little-endian order to a binary stream.
static void write_u32_le(ofstream& out, uint32_t v) {
    uint8_t buf[4] = {
        static_cast<uint8_t>(v),
        static_cast<uint8_t>(v >> 8),
        static_cast<uint8_t>(v >> 16),
        static_cast<uint8_t>(v >> 24)
    };
    out.write(reinterpret_cast<char*>(buf), 4);
}

// Read a 32-bit little-endian integer from a binary stream.
static bool read_u32_le(ifstream& in, uint32_t& out) {
    uint8_t buf[4];
    in.read(reinterpret_cast<char*>(buf), 4);
    if (in.gcount() != 4) return false;
    out = buf[0] | (uint32_t(buf[1]) << 8) |
          (uint32_t(buf[2]) << 16) | (uint32_t(buf[3]) << 24);
    return true;
}

// Write a 64-bit integer in little-endian order to a binary stream.
static void write_u64_le(ofstream& out, uint64_t v) {
    uint8_t buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<uint8_t>(v >> (8 * i));
    out.write(reinterpret_cast<char*>(buf), 8);
}

// Read a 64-bit little-endian integer from a binary stream.
static bool read_u64_le(ifstream& in, uint64_t& out) {
    uint8_t buf[8];
    in.read(reinterpret_cast<char*>(buf), 8);
    if (in.gcount() != 8) return false;
    out = 0;
    for (int i = 0; i < 8; ++i) out |= (uint64_t(buf[i]) << (8 * i));
    return true;
}

// String/stream little-endian helpers for snapshot/delta blobs
// Append a 32-bit little-endian integer to an in-memory blob.
static void append_u32_le(string& out, uint32_t v) {
    char buf[4] = {
        static_cast<char>(v & 0xFF),
        static_cast<char>((v >> 8) & 0xFF),
        static_cast<char>((v >> 16) & 0xFF),
        static_cast<char>((v >> 24) & 0xFF)
    };
    out.append(buf, 4);
}

// Append a 64-bit little-endian integer to an in-memory blob.
static void append_u64_le(string& out, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
    out.append(buf, 8);
}

// Read a 32-bit little-endian integer from an in-memory blob at off.
static bool read_u32_mem(const string& s, size_t& off, uint32_t& outv) {
    if (off + 4 > s.size()) return false;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data() + off);
    outv = p[0] | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
    off += 4;
    return true;
}

// Read a 64-bit little-endian integer from an in-memory blob at off.
static bool read_u64_mem(const string& s, size_t& off, uint64_t& outv) {
    if (off + 8 > s.size()) return false;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data() + off);
    outv = 0;
    for (int i = 0; i < 8; ++i) outv |= (uint64_t(p[i]) << (8 * i));
    off += 8;
    return true;
}

// Build a safe text or hex preview for admin DUMP output.
static string text_preview_value(const string& val,
                                      bool& is_text,
                                      bool& truncated) {
    constexpr size_t kTextPreviewBytes = 120;
    constexpr size_t kHexPreviewBytes = 48;
    truncated = val.size() > kTextPreviewBytes;
    is_text = true;
    const size_t inspect = min(val.size(), kTextPreviewBytes);
    for (size_t i = 0; i < inspect; ++i) {
        unsigned char c = static_cast<unsigned char>(val[i]);
        if (c == '\0' || (c < 0x20 && c != '\n' && c != '\r' && c != '\t')) {
            is_text = false;
            break;
        }
    }

    if (is_text) {
        return val.substr(0, min(val.size(), kTextPreviewBytes));
    }

    static const char hex[] = "0123456789abcdef";
    const size_t n = min(val.size(), kHexPreviewBytes);
    string out;
    out.reserve(n * 3);
    for (size_t i = 0; i < n; ++i) {
        if (i) out.push_back(' ');
        unsigned char c = static_cast<unsigned char>(val[i]);
        out.push_back(hex[c >> 4]);
        out.push_back(hex[c & 0x0f]);
    }
    truncated = val.size() > kHexPreviewBytes;
    return out;
}

// Initialize paths and open the WAL for append-only writes.
Tablet::Tablet(const string& name, const string& data_dir)
    : name_(name), data_dir_(data_dir) {

    filesystem::create_directories(data_dir_);

    wal_path_  = data_dir_ + "/" + name_ + ".wal";
    ckpt_path_ = data_dir_ + "/" + name_ + ".ckpt";

    wal_.open(wal_path_, ios::binary | ios::app);
    if (!wal_) {
        throw runtime_error("Cannot open WAL: " + wal_path_);
    }
}

// Flush and close the WAL stream during tablet destruction.
Tablet::~Tablet() {
    if (wal_.is_open()) {
        wal_.flush();
        wal_.close();
    }
}

// Append one PUT record to WAL and assign the next logical LSN.
bool Tablet::write_wal_put(const string& row,
                           const string& col,
                           const string& val,
                           uint64_t* assigned_lsn) {
    if (!wal_.is_open()) return false;
    write_u32_le(wal_, WAL_MAGIC);
    wal_.put(static_cast<char>(WAL_PUT));
    write_u32_le(wal_, static_cast<uint32_t>(row.size()));
    write_u32_le(wal_, static_cast<uint32_t>(col.size()));
    write_u32_le(wal_, static_cast<uint32_t>(val.size()));
    wal_.write(row.data(), row.size());
    wal_.write(col.data(), col.size());
    wal_.write(val.data(), val.size());
    wal_.flush();
    if (!wal_.good()) return false;

    uint64_t new_lsn = lsn_.fetch_add(1) + 1;
    if (assigned_lsn) *assigned_lsn = new_lsn;
    return true;
}

// Append one DELETE record to WAL and assign the next logical LSN.
bool Tablet::write_wal_delete(const string& row, const string& col, uint64_t* assigned_lsn) {
    if (!wal_.is_open()) return false;
    write_u32_le(wal_, WAL_MAGIC);
    wal_.put(static_cast<char>(WAL_DELETE));
    write_u32_le(wal_, static_cast<uint32_t>(row.size()));
    write_u32_le(wal_, static_cast<uint32_t>(col.size()));
    write_u32_le(wal_, 0u);
    wal_.write(row.data(), row.size());
    wal_.write(col.data(), col.size());
    wal_.flush();
    if (!wal_.good()) return false;

    uint64_t new_lsn = lsn_.fetch_add(1) + 1;
    if (assigned_lsn) *assigned_lsn = new_lsn;
    return true;
}

// Return an existing row pointer without changing tablet state.
RowData* Tablet::find_row(const string& row) {
    auto it = rows_.find(row);
    return (it != rows_.end()) ? it->second.get() : nullptr;
}

// Return an existing row or create a new one and add it to the Bloom filter.
RowData* Tablet::get_or_create_row(const string& row) {
    auto it = rows_.find(row);
    if (it != rows_.end()) return it->second.get();
    auto [ins, ok] = rows_.emplace(row, make_unique<RowData>());
    bloom_.add(row);
    return ins->second.get();
}

// Convenience PUT wrapper when the caller does not need the assigned LSN.
bool Tablet::put(const string& row,
                 const string& col,
                 const string& val) {
    uint64_t ignored_lsn = 0;
    return put(row, col, val, &ignored_lsn);
}

// Write a value durably to WAL first, then update the row/column map.
bool Tablet::put(const string& row,
                 const string& col,
                 const string& val,
                 uint64_t* assigned_lsn) {
    if (assigned_lsn) *assigned_lsn = 0;
    lock_guard<mutex> wal_lock(wal_mu_);
    uint64_t new_lsn = 0;
    if (!write_wal_put(row, col, val, &new_lsn)) return false;

    {
        shared_lock<shared_mutex> rlock(rows_mu_);
        RowData* rd = find_row(row);
        if (rd) {
            unique_lock<shared_mutex> wlock(rd->mu);
            rd->cols[col] = val;
            if (assigned_lsn) *assigned_lsn = new_lsn;
            op_count_.fetch_add(1);
            return true;
        }
    }

    {
        unique_lock<shared_mutex> wlock(rows_mu_);
        RowData* rd = get_or_create_row(row);
        unique_lock<shared_mutex> rdlock(rd->mu);
        rd->cols[col] = val;
    }
    if (assigned_lsn) *assigned_lsn = new_lsn;
    op_count_.fetch_add(1);
    return true;
}

// Read a value using Bloom filter and per-row shared locking.
bool Tablet::get(const string& row,
                 const string& col,
                 string&       val_out) {
    {
        shared_lock<shared_mutex> rlock(rows_mu_);
        if (!bloom_.may_contain(row)) return false;

        RowData* rd = find_row(row);
        if (!rd) return false;

        shared_lock<shared_mutex> rdlock(rd->mu);
        auto it = rd->cols.find(col);
        if (it == rd->cols.end()) return false;
        val_out = it->second;
    }
    op_count_.fetch_add(1);
    return true;
}

// Convenience CPUT wrapper when the caller does not need the assigned LSN.
bool Tablet::cput(const string& row,
                  const string& col,
                  const string& v1,
                  const string& v2) {
    uint64_t ignored_lsn = 0;
    return cput(row, col, v1, v2, &ignored_lsn);
}

// Atomically replace a value only if it currently equals the expected value.
bool Tablet::cput(const string& row,
                  const string& col,
                  const string& v1,
                  const string& v2,
                  uint64_t* assigned_lsn) {
    if (assigned_lsn) *assigned_lsn = 0;
    lock_guard<mutex> wal_lock(wal_mu_);
    shared_lock<shared_mutex> rlock(rows_mu_);

    if (!bloom_.may_contain(row)) return false;

    RowData* rd = find_row(row);
    if (!rd) return false;

    unique_lock<shared_mutex> wlock(rd->mu);
    auto it = rd->cols.find(col);
    if (it == rd->cols.end() || it->second != v1) return false;

    uint64_t new_lsn = 0;
    if (!write_wal_put(row, col, v2, &new_lsn)) return false;
    it->second = v2;
    if (assigned_lsn) *assigned_lsn = new_lsn;

    op_count_.fetch_add(1);
    return true;
}

// Convenience DELETE wrapper that ignores delete/no-op distinction.
bool Tablet::del(const string& row, const string& col) {
    bool deleted = false;
    return del(row, col, &deleted, nullptr);
}

// DELETE wrapper that reports whether a cell existed and was removed.
bool Tablet::del(const string& row, const string& col, bool* deleted_out) {
    return del(row, col, deleted_out, nullptr);
}

// Delete a cell durably, logging only when a row/column actually exists.
bool Tablet::del(const string& row, const string& col, bool* deleted_out, uint64_t* assigned_lsn) {
    if (deleted_out) *deleted_out = false;
    if (assigned_lsn) *assigned_lsn = 0;
    lock_guard<mutex> wal_lock(wal_mu_);
    shared_lock<shared_mutex> rlock(rows_mu_);
    if (!bloom_.may_contain(row)) return true;

    RowData* rd = find_row(row);
    if (!rd) return true;

    unique_lock<shared_mutex> wlock(rd->mu);
    auto it = rd->cols.find(col);
    if (it == rd->cols.end()) return true;

    uint64_t new_lsn = 0;
    if (!write_wal_delete(row, col, &new_lsn)) return false;
    rd->cols.erase(col);
    if (deleted_out) *deleted_out = true;
    if (assigned_lsn) *assigned_lsn = new_lsn;

    op_count_.fetch_add(1);
    return true;
}

// Apply a replicated delete while assigning a fresh local LSN.
bool Tablet::apply_replicated_delete(const string& row,
                                     const string& col,
                                     uint64_t* assigned_lsn) {
    if (assigned_lsn) *assigned_lsn = 0;
    lock_guard<mutex> wal_lock(wal_mu_);

    uint64_t new_lsn = 0;
    if (!write_wal_delete(row, col, &new_lsn)) return false;

    {
        unique_lock<shared_mutex> wlock(rows_mu_);
        RowData* rd = find_row(row);
        if (rd) {
            unique_lock<shared_mutex> rdlock(rd->mu);
            rd->cols.erase(col);
        }
    }

    if (assigned_lsn) *assigned_lsn = new_lsn;
    op_count_.fetch_add(1);
    return true;
}

// Apply a replicated PUT only when its LSN is the next expected value.
bool Tablet::apply_replicated_put(const string& row,
                                  const string& col,
                                  const string& val,
                                  uint64_t expected_lsn) {
    lock_guard<mutex> wal_lock(wal_mu_);
    uint64_t current_lsn = lsn_.load();
    if (expected_lsn <= current_lsn) return true;
    if (expected_lsn != current_lsn + 1) {
        cerr << "[tablet:" << name_ << "] replicated PUT gap: have="
                  << current_lsn << " incoming=" << expected_lsn << "\n";
        return false;
    }

    uint64_t assigned_lsn = 0;
    if (!write_wal_put(row, col, val, &assigned_lsn)) return false;
    if (assigned_lsn != expected_lsn) return false;

    {
        unique_lock<shared_mutex> rows_lock(rows_mu_);
        RowData* rd = get_or_create_row(row);
        unique_lock<shared_mutex> rdlock(rd->mu);
        rd->cols[col] = val;
    }

    op_count_.fetch_add(1);
    return true;
}

// Apply a replicated DELETE only when its LSN is the next expected value.
bool Tablet::apply_replicated_delete(const string& row,
                                     const string& col,
                                     uint64_t expected_lsn) {
    lock_guard<mutex> wal_lock(wal_mu_);
    uint64_t current_lsn = lsn_.load();
    if (expected_lsn <= current_lsn) return true;
    if (expected_lsn != current_lsn + 1) {
        cerr << "[tablet:" << name_ << "] replicated DELETE gap: have="
                  << current_lsn << " incoming=" << expected_lsn << "\n";
        return false;
    }

    uint64_t assigned_lsn = 0;
    if (!write_wal_delete(row, col, &assigned_lsn)) return false;
    if (assigned_lsn != expected_lsn) return false;

    {
        unique_lock<shared_mutex> rows_lock(rows_mu_);
        RowData* rd = find_row(row);
        if (rd) {
            unique_lock<shared_mutex> rdlock(rd->mu);
            rd->cols.erase(col);
        }
    }

    op_count_.fetch_add(1);
    return true;
}

// Public checkpoint entry point that serializes with WAL writes.
bool Tablet::checkpoint() {
    lock_guard<mutex> wal_lock(wal_mu_);
    return checkpoint_locked();
}

// Write a complete snapshot and truncate WAL; caller must hold wal_mu_.
bool Tablet::checkpoint_locked() {
    string tmp = ckpt_path_ + ".tmp";
    uint64_t next_ckpt_ver = checkpoint_version_.load() + 1;
    uint64_t ckpt_lsn = 0;

    {
        unique_lock<shared_mutex> wlock(rows_mu_);
        ckpt_lsn = lsn_.load();

        ofstream out(tmp, ios::binary | ios::trunc);
        if (!out) return false;

        write_u32_le(out, CKPT_MAGIC);
        write_u64_le(out, next_ckpt_ver);
        write_u64_le(out, ckpt_lsn);

        uint32_t nrows = static_cast<uint32_t>(rows_.size());
        write_u32_le(out, nrows);

        for (auto& [rkey, rd] : rows_) {
            write_u32_le(out, static_cast<uint32_t>(rkey.size()));
            out.write(rkey.data(), rkey.size());

            shared_lock<shared_mutex> rdlock(rd->mu);
            uint32_t ncols = static_cast<uint32_t>(rd->cols.size());
            write_u32_le(out, ncols);
            for (auto& [ckey, val] : rd->cols) {
                write_u32_le(out, static_cast<uint32_t>(ckey.size()));
                out.write(ckey.data(), ckey.size());
                write_u32_le(out, static_cast<uint32_t>(val.size()));
                out.write(val.data(), val.size());
            }
        }

        bloom_.serialize(out);
        out.flush();
        if (!out.good()) return false;
    }

    if (::rename(tmp.c_str(), ckpt_path_.c_str()) != 0) return false;

    wal_.close();
    wal_.open(wal_path_, ios::binary | ios::trunc);
    if (!wal_) {
        cerr << "[tablet:" << name_ << "] checkpoint wrote, but failed to reopen WAL\n";
        wal_.clear();
        wal_.open(wal_path_, ios::binary | ios::app);
        return false;
    }

    checkpoint_version_.store(next_ckpt_ver);
    lsn_.store(ckpt_lsn);

    cout << "[tablet:" << name_ << "] checkpoint written, ver="
              << checkpoint_version_.load() << ", LSN=" << lsn_.load()
              << ", WAL truncated, " << row_count() << " rows\n";
    return true;
}

// Recover durable state by loading the latest checkpoint and replaying WAL.
bool Tablet::recover() {
    bool ok = load_checkpoint();
    if (!ok) {
        cerr << "[tablet:" << name_ << "] checkpoint load failed; refusing WAL replay\n";
        return false;
    }
    return replay_wal();
}

// Load a checkpoint file, rebuilding rows, Bloom filter, checkpoint version, and LSN.
bool Tablet::load_checkpoint() {
    ifstream in(ckpt_path_, ios::binary);
    if (!in) {
        checkpoint_version_.store(0);
        cout << "[tablet:" << name_ << "] no checkpoint, starting fresh\n";
        return true;
    }

    uint32_t first_u32 = 0;
    if (!read_u32_le(in, first_u32)) return false;

    uint32_t nrows = 0;
    uint64_t loaded_ckpt_ver = 0;
    uint64_t loaded_ckpt_lsn = 0;

    if (first_u32 == CKPT_MAGIC) {
        if (!read_u64_le(in, loaded_ckpt_ver)) return false;
        if (!read_u64_le(in, loaded_ckpt_lsn)) return false;
        if (!read_u32_le(in, nrows)) return false;
    } else {
        nrows = first_u32;
        loaded_ckpt_ver = 0;
        loaded_ckpt_lsn = 0;
    }

    rows_.clear();
    bloom_ = BloomFilter();

    for (uint32_t i = 0; i < nrows; ++i) {
        uint32_t rowlen = 0;
        if (!read_u32_le(in, rowlen)) return false;
        string row(rowlen, '\0');
        in.read(&row[0], rowlen);
        if (static_cast<uint32_t>(in.gcount()) != rowlen) return false;

        auto rd = make_unique<RowData>();
        bloom_.add(row);

        uint32_t ncols = 0;
        if (!read_u32_le(in, ncols)) return false;
        for (uint32_t j = 0; j < ncols; ++j) {
            uint32_t clen = 0;
            if (!read_u32_le(in, clen)) return false;
            string col(clen, '\0');
            in.read(&col[0], clen);
            if (static_cast<uint32_t>(in.gcount()) != clen) return false;

            uint32_t vlen = 0;
            if (!read_u32_le(in, vlen)) return false;
            string val(vlen, '\0');
            in.read(&val[0], vlen);
            if (static_cast<uint32_t>(in.gcount()) != vlen) return false;

            rd->cols[col] = move(val);
        }
        rows_[row] = move(rd);
    }

    bloom_.deserialize(in);
    checkpoint_version_.store(loaded_ckpt_ver);
    lsn_.store(loaded_ckpt_lsn);

    cout << "[tablet:" << name_ << "] checkpoint loaded, ver="
              << checkpoint_version_.load() << ", LSN=" << lsn_.load()
              << ", rows=" << rows_.size() << "\n";
    return true;
}

// Replay each valid WAL entry after the checkpoint into the in-memory map.
bool Tablet::replay_wal() {
    ifstream in(wal_path_, ios::binary);
    if (!in) return true;

    uint64_t replayed = 0;
    while (true) {
        uint32_t magic = 0;
        if (!read_u32_le(in, magic)) break;
        if (magic != WAL_MAGIC) {
            cerr << "[tablet:" << name_
                      << "] WAL: bad magic, stopping replay at entry "
                      << replayed << "\n";
            break;
        }

        uint8_t op = 0;
        in.read(reinterpret_cast<char*>(&op), 1);
        if (in.gcount() != 1) break;

        uint32_t rowlen = 0, collen = 0, vallen = 0;
        if (!read_u32_le(in, rowlen)) break;
        if (!read_u32_le(in, collen)) break;
        if (!read_u32_le(in, vallen)) break;

        string row(rowlen, '\0'), col(collen, '\0'), val(vallen, '\0');
        in.read(&row[0], rowlen);
        if (static_cast<uint32_t>(in.gcount()) != rowlen) break;
        in.read(&col[0], collen);
        if (static_cast<uint32_t>(in.gcount()) != collen) break;
        if (vallen > 0) {
            in.read(&val[0], vallen);
            if (static_cast<uint32_t>(in.gcount()) != vallen) break;
        }

        if (op == WAL_PUT) {
            auto it = rows_.find(row);
            if (it == rows_.end()) {
                auto rd = make_unique<RowData>();
                rd->cols[col] = val;
                bloom_.add(row);
                rows_[row] = move(rd);
            } else {
                it->second->cols[col] = val;
            }
        } else if (op == WAL_DELETE) {
            auto it = rows_.find(row);
            if (it != rows_.end()) it->second->cols.erase(col);
        }

        lsn_.fetch_add(1);
        ++replayed;
    }

    cout << "[tablet:" << name_ << "] replayed " << replayed
              << " WAL entries, current LSN=" << lsn_.load() << "\n";
    return true;
}

// Return the number of rows currently resident in memory.
size_t Tablet::row_count() const {
    shared_lock<shared_mutex> rlock(rows_mu_);
    return rows_.size();
}

// Collect sorted cell previews for admin/debug dump responses.
vector<TabletDumpCell> Tablet::dump_cells() const {
    vector<TabletDumpCell> cells;
    {
        shared_lock<shared_mutex> rlock(rows_mu_);
        for (const auto& [row, rd] : rows_) {
            shared_lock<shared_mutex> rdlock(rd->mu);
            for (const auto& [col, val] : rd->cols) {
                TabletDumpCell cell;
                cell.tablet = name_;
                cell.row = row;
                cell.col = col;
                cell.value_size = val.size();
                cell.value_preview = text_preview_value(val, cell.value_is_text, cell.truncated);
                cells.push_back(move(cell));
            }
        }
    }
    sort(cells.begin(), cells.end(), [](const TabletDumpCell& a, const TabletDumpCell& b) {
        if (a.tablet != b.tablet) return a.tablet < b.tablet;
        if (a.row != b.row) return a.row < b.row;
        return a.col < b.col;
    });
    return cells;
}

// Serialize a full in-memory snapshot for secondary catch-up.
string Tablet::snapshot_blob() const {
    string out;
    shared_lock<shared_mutex> rlock(rows_mu_);

    append_u32_le(out, static_cast<uint32_t>(rows_.size()));
    append_u64_le(out, lsn_.load());
    append_u64_le(out, checkpoint_version_.load());

    for (const auto& [rkey, rd] : rows_) {
        append_u32_le(out, static_cast<uint32_t>(rkey.size()));
        out.append(rkey.data(), rkey.size());
        shared_lock<shared_mutex> rdlock(rd->mu);
        append_u32_le(out, static_cast<uint32_t>(rd->cols.size()));
        for (const auto& [ckey, val] : rd->cols) {
            append_u32_le(out, static_cast<uint32_t>(ckey.size()));
            out.append(ckey.data(), ckey.size());
            append_u32_le(out, static_cast<uint32_t>(val.size()));
            out.append(val.data(), val.size());
        }
    }
    return out;
}

// Replace local state from a snapshot blob and checkpoint the replacement.
bool Tablet::load_snapshot_blob(const string& blob) {
    size_t off = 0;
    uint32_t nrows = 0;
    uint64_t new_lsn = 0;
    uint64_t new_ckpt_ver = 0;
    if (!read_u32_mem(blob, off, nrows)) return false;
    if (!read_u64_mem(blob, off, new_lsn)) return false;
    if (!read_u64_mem(blob, off, new_ckpt_ver)) return false;

    unordered_map<string, unique_ptr<RowData>> new_rows;
    BloomFilter new_bloom;
    for (uint32_t i = 0; i < nrows; ++i) {
        uint32_t rlen = 0, ncols = 0;
        if (!read_u32_mem(blob, off, rlen) || off + rlen > blob.size()) return false;
        string rkey = blob.substr(off, rlen);
        off += rlen;
        if (!read_u32_mem(blob, off, ncols)) return false;
        auto rd = make_unique<RowData>();
        for (uint32_t j = 0; j < ncols; ++j) {
            uint32_t clen = 0, vlen = 0;
            if (!read_u32_mem(blob, off, clen) || off + clen > blob.size()) return false;
            string ckey = blob.substr(off, clen);
            off += clen;
            if (!read_u32_mem(blob, off, vlen) || off + vlen > blob.size()) return false;
            string val = blob.substr(off, vlen);
            off += vlen;
            rd->cols[ckey] = val;
        }
        new_bloom.add(rkey);
        new_rows.emplace(rkey, move(rd));
    }

    lock_guard<mutex> wal_lock(wal_mu_);
    {
        unique_lock<shared_mutex> wlock(rows_mu_);
        rows_.swap(new_rows);
        bloom_ = move(new_bloom);
        lsn_.store(new_lsn);
        checkpoint_version_.store(new_ckpt_ver);
    }

    return checkpoint_locked();
}

// Export WAL entries with logical LSNs greater than after_lsn.
string Tablet::wal_delta_after(uint64_t after_lsn) const {
    lock_guard<mutex> wal_lock(wal_mu_);
    // Export WAL entries with logical LSN > after_lsn.
    // Blob format:
    //   [8 bytes base_lsn]
    //   repeated:
    //     [8 bytes entry_lsn]
    //     [4 bytes magic]
    //     [1 byte op]
    //     [4 bytes rowlen]
    //     [4 bytes collen]
    //     [4 bytes vallen]
    //     payload
    //
    // We rebuild entry LSNs by scanning the WAL in order from the latest
    // checkpoint boundary. This is sufficient for the current single-tablet,
    // truncated-WAL design.
    ifstream in(wal_path_, ios::binary);
    string out;
    append_u64_le(out, after_lsn);

    if (!in) return out;

    // Better reconstruction of WAL-entry LSN range:
    // current WAL only contains entries after last checkpoint; if current LSN
    // is total durable LSN and checkpoint_lsn is stored in checkpoint file,
    // replayed WAL entries count from there. We approximate the first WAL entry
    // LSN by scanning and counting entries, then derive each entry's LSN.
    vector<string> entries;
    while (true) {
        uint32_t magic = 0;
        if (!read_u32_le(in, magic)) break;
        if (magic != WAL_MAGIC) break;

        char op = 0;
        in.read(&op, 1);
        if (in.gcount() != 1) break;

        uint32_t rowlen = 0, collen = 0, vallen = 0;
        if (!read_u32_le(in, rowlen)) break;
        if (!read_u32_le(in, collen)) break;
        if (!read_u32_le(in, vallen)) break;

        string row(rowlen, '\0'), col(collen, '\0'), val(vallen, '\0');
        in.read(&row[0], rowlen);
        if (static_cast<uint32_t>(in.gcount()) != rowlen) break;
        in.read(&col[0], collen);
        if (static_cast<uint32_t>(in.gcount()) != collen) break;
        if (vallen > 0) {
            in.read(&val[0], vallen);
            if (static_cast<uint32_t>(in.gcount()) != vallen) break;
        }

        string entry;
        append_u32_le(entry, magic);
        entry.push_back(op);
        append_u32_le(entry, rowlen);
        append_u32_le(entry, collen);
        append_u32_le(entry, vallen);
        entry.append(row);
        entry.append(col);
        entry.append(val);
        entries.push_back(move(entry));
    }

    uint64_t total_lsn = lsn_.load();
    uint64_t first_entry_lsn = (entries.size() > total_lsn) ? 1 : (total_lsn - entries.size() + 1);

    for (size_t i = 0; i < entries.size(); ++i) {
        uint64_t entry_lsn = first_entry_lsn + i;
        if (entry_lsn <= after_lsn) continue;
        append_u64_le(out, entry_lsn);
        out.append(entries[i]);
    }

    return out;
}

// Apply a WAL delta blob in explicit LSN order, skipping duplicates.
bool Tablet::apply_wal_delta_blob(const string& blob) {
    size_t off = 0;
    uint64_t base_lsn = 0;
    if (!read_u64_mem(blob, off, base_lsn)) return false;

    lock_guard<mutex> wal_lock(wal_mu_);
    while (off < blob.size()) {
        uint64_t entry_lsn = 0;
        if (!read_u64_mem(blob, off, entry_lsn)) return false;

        uint32_t magic = 0;
        if (!read_u32_mem(blob, off, magic)) return false;
        if (magic != WAL_MAGIC) return false;

        uint64_t current_lsn = lsn_.load();
        if (entry_lsn <= current_lsn) {
            // Skip already applied delta entries.
            if (off + 1 > blob.size()) return false;
            off += 1;

            uint32_t rowlen = 0, collen = 0, vallen = 0;
            if (!read_u32_mem(blob, off, rowlen)) return false;
            if (!read_u32_mem(blob, off, collen)) return false;
            if (!read_u32_mem(blob, off, vallen)) return false;
            if (off + rowlen + collen + vallen > blob.size()) return false;
            off += rowlen + collen + vallen;
            continue;
        }

        if (entry_lsn != current_lsn + 1) {
            cerr << "[tablet:" << name_ << "] WAL delta gap: have="
                      << current_lsn << " incoming=" << entry_lsn << "\n";
            return false;
        }

        if (off + 1 > blob.size()) return false;
        uint8_t op = static_cast<uint8_t>(blob[off]);
        off += 1;

        uint32_t rowlen = 0, collen = 0, vallen = 0;
        if (!read_u32_mem(blob, off, rowlen)) return false;
        if (!read_u32_mem(blob, off, collen)) return false;
        if (!read_u32_mem(blob, off, vallen)) return false;

        if (off + rowlen + collen + vallen > blob.size()) return false;
        string row = blob.substr(off, rowlen); off += rowlen;
        string col = blob.substr(off, collen); off += collen;
        string val = blob.substr(off, vallen); off += vallen;

        uint64_t assigned_lsn = 0;
        if (op == WAL_PUT) {
            if (!write_wal_put(row, col, val, &assigned_lsn)) return false;
        } else if (op == WAL_DELETE) {
            if (!write_wal_delete(row, col, &assigned_lsn)) return false;
        } else {
            return false;
        }
        if (assigned_lsn != entry_lsn) {
            cerr << "[tablet:" << name_ << "] WAL delta LSN mismatch: assigned="
                      << assigned_lsn << " incoming=" << entry_lsn << "\n";
            return false;
        }

        {
            unique_lock<shared_mutex> rows_lock(rows_mu_);
            if (op == WAL_PUT) {
                RowData* rd = get_or_create_row(row);
                unique_lock<shared_mutex> rdlock(rd->mu);
                rd->cols[col] = val;
            } else if (op == WAL_DELETE) {
                RowData* rd = find_row(row);
                if (rd) {
                    unique_lock<shared_mutex> rdlock(rd->mu);
                    rd->cols.erase(col);
                }
            }
        }
    }

    return true;
}
