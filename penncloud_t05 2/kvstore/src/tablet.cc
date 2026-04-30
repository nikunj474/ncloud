#include "tablet.h"
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

static void write_u32_le(std::ofstream& out, uint32_t v) {
    uint8_t buf[4] = {
        static_cast<uint8_t>(v),
        static_cast<uint8_t>(v >> 8),
        static_cast<uint8_t>(v >> 16),
        static_cast<uint8_t>(v >> 24)
    };
    out.write(reinterpret_cast<char*>(buf), 4);
}

static bool read_u32_le(std::ifstream& in, uint32_t& out) {
    uint8_t buf[4];
    in.read(reinterpret_cast<char*>(buf), 4);
    if (in.gcount() != 4) return false;
    out = buf[0] | (uint32_t(buf[1]) << 8) |
          (uint32_t(buf[2]) << 16) | (uint32_t(buf[3]) << 24);
    return true;
}

static void write_u64_le(std::ofstream& out, uint64_t v) {
    uint8_t buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<uint8_t>(v >> (8 * i));
    out.write(reinterpret_cast<char*>(buf), 8);
}

static bool read_u64_le(std::ifstream& in, uint64_t& out) {
    uint8_t buf[8];
    in.read(reinterpret_cast<char*>(buf), 8);
    if (in.gcount() != 8) return false;
    out = 0;
    for (int i = 0; i < 8; ++i) out |= (uint64_t(buf[i]) << (8 * i));
    return true;
}

// String/stream little-endian helpers for snapshot/delta blobs
static void append_u32_le(std::string& out, uint32_t v) {
    char buf[4] = {
        static_cast<char>(v & 0xFF),
        static_cast<char>((v >> 8) & 0xFF),
        static_cast<char>((v >> 16) & 0xFF),
        static_cast<char>((v >> 24) & 0xFF)
    };
    out.append(buf, 4);
}

static void append_u64_le(std::string& out, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
    out.append(buf, 8);
}

static bool read_u32_mem(const std::string& s, size_t& off, uint32_t& outv) {
    if (off + 4 > s.size()) return false;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data() + off);
    outv = p[0] | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
    off += 4;
    return true;
}

static bool read_u64_mem(const std::string& s, size_t& off, uint64_t& outv) {
    if (off + 8 > s.size()) return false;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data() + off);
    outv = 0;
    for (int i = 0; i < 8; ++i) outv |= (uint64_t(p[i]) << (8 * i));
    off += 8;
    return true;
}

Tablet::Tablet(const std::string& name, const std::string& data_dir)
    : name_(name), data_dir_(data_dir) {

    std::filesystem::create_directories(data_dir_);

    wal_path_  = data_dir_ + "/" + name_ + ".wal";
    ckpt_path_ = data_dir_ + "/" + name_ + ".ckpt";

    wal_.open(wal_path_, std::ios::binary | std::ios::app);
    if (!wal_) {
        throw std::runtime_error("Cannot open WAL: " + wal_path_);
    }
}

Tablet::~Tablet() {
    if (wal_.is_open()) {
        wal_.flush();
        wal_.close();
    }
}

bool Tablet::write_wal_put(const std::string& row,
                           const std::string& col,
                           const std::string& val) {
    std::lock_guard<std::mutex> lk(wal_mu_);

    write_u32_le(wal_, WAL_MAGIC);
    wal_.put(static_cast<char>(WAL_PUT));
    write_u32_le(wal_, static_cast<uint32_t>(row.size()));
    write_u32_le(wal_, static_cast<uint32_t>(col.size()));
    write_u32_le(wal_, static_cast<uint32_t>(val.size()));
    wal_.write(row.data(), row.size());
    wal_.write(col.data(), col.size());
    wal_.write(val.data(), val.size());
    wal_.flush();

    lsn_.fetch_add(1);
    return wal_.good();
}

bool Tablet::write_wal_delete(const std::string& row, const std::string& col) {
    std::lock_guard<std::mutex> lk(wal_mu_);

    write_u32_le(wal_, WAL_MAGIC);
    wal_.put(static_cast<char>(WAL_DELETE));
    write_u32_le(wal_, static_cast<uint32_t>(row.size()));
    write_u32_le(wal_, static_cast<uint32_t>(col.size()));
    write_u32_le(wal_, 0u);
    wal_.write(row.data(), row.size());
    wal_.write(col.data(), col.size());
    wal_.flush();

    lsn_.fetch_add(1);
    return wal_.good();
}

RowData* Tablet::find_row(const std::string& row) {
    auto it = rows_.find(row);
    return (it != rows_.end()) ? it->second.get() : nullptr;
}

RowData* Tablet::get_or_create_row(const std::string& row) {
    auto it = rows_.find(row);
    if (it != rows_.end()) return it->second.get();
    auto [ins, ok] = rows_.emplace(row, std::make_unique<RowData>());
    bloom_.add(row);
    return ins->second.get();
}

bool Tablet::put(const std::string& row,
                 const std::string& col,
                 const std::string& val) {
    if (!write_wal_put(row, col, val)) return false;

    {
        std::shared_lock<std::shared_mutex> rlock(rows_mu_);
        RowData* rd = find_row(row);
        if (rd) {
            std::unique_lock<std::shared_mutex> wlock(rd->mu);
            rd->cols[col] = val;
            op_count_.fetch_add(1);
            return true;
        }
    }

    {
        std::unique_lock<std::shared_mutex> wlock(rows_mu_);
        RowData* rd = get_or_create_row(row);
        std::unique_lock<std::shared_mutex> rdlock(rd->mu);
        rd->cols[col] = val;
    }
    op_count_.fetch_add(1);
    return true;
}

bool Tablet::get(const std::string& row,
                 const std::string& col,
                 std::string&       val_out) {
    {
        std::shared_lock<std::shared_mutex> rlock(rows_mu_);
        if (!bloom_.may_contain(row)) return false;

        RowData* rd = find_row(row);
        if (!rd) return false;

        std::shared_lock<std::shared_mutex> rdlock(rd->mu);
        auto it = rd->cols.find(col);
        if (it == rd->cols.end()) return false;
        val_out = it->second;
    }
    op_count_.fetch_add(1);
    return true;
}

bool Tablet::cput(const std::string& row,
                  const std::string& col,
                  const std::string& v1,
                  const std::string& v2) {
    std::shared_lock<std::shared_mutex> rlock(rows_mu_);

    if (!bloom_.may_contain(row)) return false;

    RowData* rd = find_row(row);
    if (!rd) return false;

    std::unique_lock<std::shared_mutex> wlock(rd->mu);
    auto it = rd->cols.find(col);
    if (it == rd->cols.end() || it->second != v1) return false;

    wlock.unlock();
    rlock.unlock();

    if (!write_wal_put(row, col, v2)) return false;

    std::shared_lock<std::shared_mutex> rlock2(rows_mu_);
    rd = find_row(row);
    if (!rd) return false;
    std::unique_lock<std::shared_mutex> wlock2(rd->mu);
    auto it2 = rd->cols.find(col);
    if (it2 == rd->cols.end() || it2->second != v1) return false;
    it2->second = v2;

    op_count_.fetch_add(1);
    return true;
}

bool Tablet::del(const std::string& row, const std::string& col) {
    if (!write_wal_delete(row, col)) return false;

    std::shared_lock<std::shared_mutex> rlock(rows_mu_);
    if (!bloom_.may_contain(row)) return true;

    RowData* rd = find_row(row);
    if (!rd) return true;

    std::unique_lock<std::shared_mutex> wlock(rd->mu);
    rd->cols.erase(col);

    op_count_.fetch_add(1);
    return true;
}

bool Tablet::checkpoint() {
    std::string tmp = ckpt_path_ + ".tmp";
    uint64_t next_ckpt_ver = checkpoint_version_.load() + 1;
    uint64_t ckpt_lsn = 0;

    {
        std::unique_lock<std::shared_mutex> wlock(rows_mu_);
        ckpt_lsn = lsn_.load();

        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) return false;

        write_u32_le(out, CKPT_MAGIC);
        write_u64_le(out, next_ckpt_ver);
        write_u64_le(out, ckpt_lsn);

        uint32_t nrows = static_cast<uint32_t>(rows_.size());
        write_u32_le(out, nrows);

        for (auto& [rkey, rd] : rows_) {
            write_u32_le(out, static_cast<uint32_t>(rkey.size()));
            out.write(rkey.data(), rkey.size());

            std::shared_lock<std::shared_mutex> rdlock(rd->mu);
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

    {
        std::lock_guard<std::mutex> lk(wal_mu_);
        wal_.close();
        wal_.open(wal_path_, std::ios::binary | std::ios::trunc);
        if (!wal_) return false;
    }

    checkpoint_version_.store(next_ckpt_ver);
    lsn_.store(ckpt_lsn);

    std::cout << "[tablet:" << name_ << "] checkpoint written, ver="
              << checkpoint_version_.load() << ", LSN=" << lsn_.load()
              << ", WAL truncated, " << row_count() << " rows\n";
    return true;
}

bool Tablet::recover() {
    bool ok = load_checkpoint();
    ok = replay_wal() && ok;
    return ok;
}

bool Tablet::load_checkpoint() {
    std::ifstream in(ckpt_path_, std::ios::binary);
    if (!in) {
        checkpoint_version_.store(0);
        std::cout << "[tablet:" << name_ << "] no checkpoint, starting fresh\n";
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
        std::string row(rowlen, '\0');
        in.read(&row[0], rowlen);
        if (static_cast<uint32_t>(in.gcount()) != rowlen) return false;

        auto rd = std::make_unique<RowData>();
        bloom_.add(row);

        uint32_t ncols = 0;
        if (!read_u32_le(in, ncols)) return false;
        for (uint32_t j = 0; j < ncols; ++j) {
            uint32_t clen = 0;
            if (!read_u32_le(in, clen)) return false;
            std::string col(clen, '\0');
            in.read(&col[0], clen);
            if (static_cast<uint32_t>(in.gcount()) != clen) return false;

            uint32_t vlen = 0;
            if (!read_u32_le(in, vlen)) return false;
            std::string val(vlen, '\0');
            in.read(&val[0], vlen);
            if (static_cast<uint32_t>(in.gcount()) != vlen) return false;

            rd->cols[col] = std::move(val);
        }
        rows_[row] = std::move(rd);
    }

    bloom_.deserialize(in);
    checkpoint_version_.store(loaded_ckpt_ver);
    lsn_.store(loaded_ckpt_lsn);

    std::cout << "[tablet:" << name_ << "] checkpoint loaded, ver="
              << checkpoint_version_.load() << ", LSN=" << lsn_.load()
              << ", rows=" << rows_.size() << "\n";
    return true;
}

bool Tablet::replay_wal() {
    std::ifstream in(wal_path_, std::ios::binary);
    if (!in) return true;

    uint64_t replayed = 0;
    while (true) {
        uint32_t magic = 0;
        if (!read_u32_le(in, magic)) break;
        if (magic != WAL_MAGIC) {
            std::cerr << "[tablet:" << name_
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

        std::string row(rowlen, '\0'), col(collen, '\0'), val(vallen, '\0');
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
                auto rd = std::make_unique<RowData>();
                rd->cols[col] = val;
                bloom_.add(row);
                rows_[row] = std::move(rd);
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

    std::cout << "[tablet:" << name_ << "] replayed " << replayed
              << " WAL entries, current LSN=" << lsn_.load() << "\n";
    return true;
}

size_t Tablet::row_count() const {
    std::shared_lock<std::shared_mutex> rlock(rows_mu_);
    return rows_.size();
}

std::string Tablet::snapshot_blob() const {
    std::string out;
    std::shared_lock<std::shared_mutex> rlock(rows_mu_);

    append_u32_le(out, static_cast<uint32_t>(rows_.size()));
    append_u64_le(out, lsn_.load());
    append_u64_le(out, checkpoint_version_.load());

    for (const auto& [rkey, rd] : rows_) {
        append_u32_le(out, static_cast<uint32_t>(rkey.size()));
        out.append(rkey.data(), rkey.size());
        std::shared_lock<std::shared_mutex> rdlock(rd->mu);
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

bool Tablet::load_snapshot_blob(const std::string& blob) {
    size_t off = 0;
    uint32_t nrows = 0;
    uint64_t new_lsn = 0;
    uint64_t new_ckpt_ver = 0;
    if (!read_u32_mem(blob, off, nrows)) return false;
    if (!read_u64_mem(blob, off, new_lsn)) return false;
    if (!read_u64_mem(blob, off, new_ckpt_ver)) return false;

    std::unordered_map<std::string, std::unique_ptr<RowData>> new_rows;
    BloomFilter new_bloom;
    for (uint32_t i = 0; i < nrows; ++i) {
        uint32_t rlen = 0, ncols = 0;
        if (!read_u32_mem(blob, off, rlen) || off + rlen > blob.size()) return false;
        std::string rkey = blob.substr(off, rlen);
        off += rlen;
        if (!read_u32_mem(blob, off, ncols)) return false;
        auto rd = std::make_unique<RowData>();
        for (uint32_t j = 0; j < ncols; ++j) {
            uint32_t clen = 0, vlen = 0;
            if (!read_u32_mem(blob, off, clen) || off + clen > blob.size()) return false;
            std::string ckey = blob.substr(off, clen);
            off += clen;
            if (!read_u32_mem(blob, off, vlen) || off + vlen > blob.size()) return false;
            std::string val = blob.substr(off, vlen);
            off += vlen;
            rd->cols[ckey] = val;
        }
        new_bloom.add(rkey);
        new_rows.emplace(rkey, std::move(rd));
    }

    {
        std::unique_lock<std::shared_mutex> wlock(rows_mu_);
        rows_.swap(new_rows);
        bloom_ = std::move(new_bloom);
        lsn_.store(new_lsn);
        checkpoint_version_.store(new_ckpt_ver);
    }

    return checkpoint();
}

std::string Tablet::wal_delta_after(uint64_t after_lsn) const {
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
    std::ifstream in(wal_path_, std::ios::binary);
    std::string out;
    append_u64_le(out, after_lsn);

    if (!in) return out;

    // Better reconstruction of WAL-entry LSN range:
    // current WAL only contains entries after last checkpoint; if current LSN
    // is total durable LSN and checkpoint_lsn is stored in checkpoint file,
    // replayed WAL entries count from there. We approximate the first WAL entry
    // LSN by scanning and counting entries, then derive each entry's LSN.
    std::vector<std::string> entries;
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

        std::string row(rowlen, '\0'), col(collen, '\0'), val(vallen, '\0');
        in.read(&row[0], rowlen);
        if (static_cast<uint32_t>(in.gcount()) != rowlen) break;
        in.read(&col[0], collen);
        if (static_cast<uint32_t>(in.gcount()) != collen) break;
        if (vallen > 0) {
            in.read(&val[0], vallen);
            if (static_cast<uint32_t>(in.gcount()) != vallen) break;
        }

        std::string entry;
        append_u32_le(entry, magic);
        entry.push_back(op);
        append_u32_le(entry, rowlen);
        append_u32_le(entry, collen);
        append_u32_le(entry, vallen);
        entry.append(row);
        entry.append(col);
        entry.append(val);
        entries.push_back(std::move(entry));
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

bool Tablet::apply_wal_delta_blob(const std::string& blob) {
    size_t off = 0;
    uint64_t base_lsn = 0;
    if (!read_u64_mem(blob, off, base_lsn)) return false;

    while (off < blob.size()) {
        uint64_t entry_lsn = 0;
        if (!read_u64_mem(blob, off, entry_lsn)) return false;

        uint32_t magic = 0;
        if (!read_u32_mem(blob, off, magic)) return false;
        if (magic != WAL_MAGIC) return false;

        if (entry_lsn <= lsn_.load()) {
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

        if (off + 1 > blob.size()) return false;
        uint8_t op = static_cast<uint8_t>(blob[off]);
        off += 1;

        uint32_t rowlen = 0, collen = 0, vallen = 0;
        if (!read_u32_mem(blob, off, rowlen)) return false;
        if (!read_u32_mem(blob, off, collen)) return false;
        if (!read_u32_mem(blob, off, vallen)) return false;

        if (off + rowlen + collen + vallen > blob.size()) return false;
        std::string row = blob.substr(off, rowlen); off += rowlen;
        std::string col = blob.substr(off, collen); off += collen;
        std::string val = blob.substr(off, vallen); off += vallen;

        // Apply directly to memory without rewriting WAL.
        {
            std::unique_lock<std::shared_mutex> rows_lock(rows_mu_);
            if (op == WAL_PUT) {
                RowData* rd = get_or_create_row(row);
                std::unique_lock<std::shared_mutex> rdlock(rd->mu);
                rd->cols[col] = val;
            } else if (op == WAL_DELETE) {
                RowData* rd = find_row(row);
                if (rd) {
                    std::unique_lock<std::shared_mutex> rdlock(rd->mu);
                    rd->cols.erase(col);
                }
            } else {
                return false;
            }
        }

        lsn_.store(entry_lsn);
    }

    return true;
}
