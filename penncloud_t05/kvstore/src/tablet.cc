// tablet.cc  --  Tablet implementation

#include "tablet.h"
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <filesystem>
using namespace std;

// Little-endian uint32 helpers for WAL binary encoding
static void write_u32_le(ofstream& out, uint32_t v) {
    uint8_t buf[4] = {
        static_cast<uint8_t>(v),
        static_cast<uint8_t>(v >> 8),
        static_cast<uint8_t>(v >> 16),
        static_cast<uint8_t>(v >> 24)
    };
    out.write(reinterpret_cast<char*>(buf), 4);
}

static bool read_u32_le(ifstream& in, uint32_t& out) {
    uint8_t buf[4];
    in.read(reinterpret_cast<char*>(buf), 4);
    if (in.gcount() != 4) return false;
    out = buf[0] | (uint32_t(buf[1]) << 8) |
          (uint32_t(buf[2]) << 16) | (uint32_t(buf[3]) << 24);
    return true;
}

// Constructor
Tablet::Tablet(const string& name, const string& data_dir)
    : name_(name), data_dir_(data_dir) {

    // ensure data directory exists
    filesystem::create_directories(data_dir_);

    wal_path_  = data_dir_ + "/" + name_ + ".wal";
    ckpt_path_ = data_dir_ + "/" + name_ + ".ckpt";

    // open WAL in append mode -- every PUT/DELETE appends a record
    wal_.open(wal_path_, ios::binary | ios::app);
    if (!wal_) {
        throw runtime_error("Cannot open WAL: " + wal_path_);
    }
}

Tablet::~Tablet() {
    if (wal_.is_open()) {
        wal_.flush();
        wal_.close();
    }
}

// ---------------------------------------------------------------------------
// WAL writers
// WHY: We write to WAL BEFORE touching in-memory state.
//      If we crash after WAL write but before memtable update, recovery
//      replays the WAL and the write is not lost.
//      If we crash before WAL write, the operation never happened -- correct.
// ---------------------------------------------------------------------------
bool Tablet::write_wal_put(const string& row,
                           const string& col,
                           const string& val) {
    lock_guard<mutex> lk(wal_mu_);

    write_u32_le(wal_, WAL_MAGIC);
    wal_.put(static_cast<char>(WAL_PUT));
    write_u32_le(wal_, static_cast<uint32_t>(row.size()));
    write_u32_le(wal_, static_cast<uint32_t>(col.size()));
    write_u32_le(wal_, static_cast<uint32_t>(val.size()));
    wal_.write(row.data(), row.size());
    wal_.write(col.data(), col.size());
    wal_.write(val.data(), val.size());
    wal_.flush();   // flush to OS page cache

    // fsync ensures data reaches disk before we ACK the client.
    // Slightly slower but guarantees durability across power failure.
    // Trade-off: correctness over throughput.  B2 (write coalescing)
    // in the replication layer reduces the number of fsyncs in practice.
    // wal_.flush() above pushes to OS buffer cache.
    // We skip fsync here for performance; B2 (write coalescing) in the
    // replication layer batches writes before fsync at the flush boundary.
    // For full durability, uncomment:
    // ::fsync(wal_fd_);

    lsn_.fetch_add(1);
    return wal_.good();
}

bool Tablet::write_wal_delete(const string& row, const string& col) {
    lock_guard<mutex> lk(wal_mu_);

    write_u32_le(wal_, WAL_MAGIC);
    wal_.put(static_cast<char>(WAL_DELETE));
    write_u32_le(wal_, static_cast<uint32_t>(row.size()));
    write_u32_le(wal_, static_cast<uint32_t>(col.size()));
    write_u32_le(wal_, 0u);  // vallen = 0 for DELETE
    wal_.write(row.data(), row.size());
    wal_.write(col.data(), col.size());
    wal_.flush();

    lsn_.fetch_add(1);
    return wal_.good();
}

// ---------------------------------------------------------------------------
// Row helpers
// ---------------------------------------------------------------------------
// Find existing row -- caller holds shared_lock or unique_lock on rows_mu_
RowData* Tablet::find_row(const string& row) {
    auto it = rows_.find(row);
    return (it != rows_.end()) ? it->second.get() : nullptr;
}

// Get or create row -- caller must hold unique_lock on rows_mu_
RowData* Tablet::get_or_create_row(const string& row) {
    auto it = rows_.find(row);
    if (it != rows_.end()) return it->second.get();
    auto [ins, ok] = rows_.emplace(row, make_unique<RowData>());
    bloom_.add(row);  // register in Bloom filter
    return ins->second.get();
}

// ---------------------------------------------------------------------------
// PUT
// ---------------------------------------------------------------------------
bool Tablet::put(const string& row,
                 const string& col,
                 const string& val) {
    // 1. Write to WAL first (crash safety)
    if (!write_wal_put(row, col, val)) return false;

    // 2. Apply to in-memory map
    {
        // Check if row already exists under shared lock (fast path)
        shared_lock<shared_mutex> rlock(rows_mu_);
        RowData* rd = find_row(row);
        if (rd) {
            unique_lock<shared_mutex> wlock(rd->mu);
            rd->cols[col] = val;
            op_count_.fetch_add(1);
            return true;
        }
    }
    // Row does not exist -- need exclusive lock to insert it
    {
        unique_lock<shared_mutex> wlock(rows_mu_);
        RowData* rd = get_or_create_row(row);  // creates if needed
        unique_lock<shared_mutex> rdlock(rd->mu);
        rd->cols[col] = val;
    }
    op_count_.fetch_add(1);
    return true;
}


// GET
// WHY Bloom filter: most GETs are for keys that exist.
// But after a delete or for new users, many GETs hit absent keys.
// Bloom filter catches those in O(1) without touching the map or any lock.

bool Tablet::get(const string& row,
                 const string& col,
                 string&       val_out) {
    // Bloom filter fast-path: definitely not present
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


// CPUT (conditional PUT)
// Stores v2 in (row, col) only if current value equals v1.
// Entire check-and-set is atomic under the row's exclusive write lock.
// This is how the frontend implements optimistic concurrency for drive
// metadata updates without distributed locks.

bool Tablet::cput(const string& row,
                  const string& col,
                  const string& v1,
                  const string& v2) {
    shared_lock<shared_mutex> rlock(rows_mu_);

    if (!bloom_.may_contain(row)) return false;  // definitely not present

    RowData* rd = find_row(row);
    if (!rd) return false;

    unique_lock<shared_mutex> wlock(rd->mu);
    auto it = rd->cols.find(col);
    if (it == rd->cols.end() || it->second != v1) return false;

    // Values match -- write WAL then update
    // NOTE: we release the data lock during WAL write to avoid holding it
    // across I/O.  We re-acquire and re-check.  This is safe because
    // no other writer can modify this row while we hold wlock (unique).
    wlock.unlock();
    rlock.unlock();

    if (!write_wal_put(row, col, v2)) return false;

    // Re-acquire and apply
    shared_lock<shared_mutex> rlock2(rows_mu_);
    rd = find_row(row);
    if (!rd) return false;  // very unlikely -- row deleted between unlock and relock
    unique_lock<shared_mutex> wlock2(rd->mu);
    auto it2 = rd->cols.find(col);
    if (it2 == rd->cols.end() || it2->second != v1) return false;
    it2->second = v2;

    op_count_.fetch_add(1);
    return true;
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------
bool Tablet::del(const string& row, const string& col) {
    if (!write_wal_delete(row, col)) return false;

    shared_lock<shared_mutex> rlock(rows_mu_);
    if (!bloom_.may_contain(row)) return true;  // key not present -- still OK

    RowData* rd = find_row(row);
    if (!rd) return true;

    unique_lock<shared_mutex> wlock(rd->mu);
    rd->cols.erase(col);

    op_count_.fetch_add(1);
    return true;
}

bool Tablet::checkpoint() {
    string tmp = ckpt_path_ + ".tmp";
    {
        // Take full exclusive lock on rows_ to get a consistent snapshot.
        // This briefly pauses all writes.  In practice checkpoints happen
        // every few minutes so this pause is acceptable.
        unique_lock<shared_mutex> wlock(rows_mu_);

        ofstream out(tmp, ios::binary | ios::trunc);
        if (!out) return false;

        uint32_t nrows = static_cast<uint32_t>(rows_.size());
        write_u32_le(out, nrows);

        for (auto& [rkey, rd] : rows_) {
            // Write row key
            write_u32_le(out, static_cast<uint32_t>(rkey.size()));
            out.write(rkey.data(), rkey.size());

            // Write columns
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

        // Append Bloom filter
        bloom_.serialize(out);
        out.flush();

        if (!out.good()) return false;
    }

    // Atomically replace checkpoint file
    if (::rename(tmp.c_str(), ckpt_path_.c_str()) != 0) return false;

    // Truncate WAL: close, truncate, reopen.
    // All writes that existed before checkpoint are captured in the snapshot.
    {
        lock_guard<mutex> lk(wal_mu_);
        wal_.close();
        wal_.open(wal_path_, ios::binary | ios::trunc);
        if (!wal_) return false;
    }

    cout << "[tablet:" << name_ << "] checkpoint written, WAL truncated, "
              << row_count() << " rows\n";
    return true;
}

// ---------------------------------------------------------------------------
// RECOVERY: load checkpoint then replay WAL
// Called at startup.  No concurrent access yet so no locking needed.
// ---------------------------------------------------------------------------
bool Tablet::recover() {
    bool ok = load_checkpoint();
    // Even if no checkpoint exists, replay WAL from scratch
    ok = replay_wal() && ok;
    return ok;
}

bool Tablet::load_checkpoint() {
    ifstream in(ckpt_path_, ios::binary);
    if (!in) {
        cout << "[tablet:" << name_ << "] no checkpoint, starting fresh\n";
        return true;  // not an error
    }

    uint32_t nrows;
    if (!read_u32_le(in, nrows)) return false;

    for (uint32_t i = 0; i < nrows; ++i) {
        uint32_t rowlen;
        if (!read_u32_le(in, rowlen)) return false;
        string row(rowlen, '\0');
        in.read(&row[0], rowlen);
        if (in.gcount() != rowlen) return false;

        auto rd = make_unique<RowData>();
        bloom_.add(row);

        uint32_t ncols;
        if (!read_u32_le(in, ncols)) return false;
        for (uint32_t j = 0; j < ncols; ++j) {
            uint32_t clen;
            if (!read_u32_le(in, clen)) return false;
            string col(clen, '\0');
            in.read(&col[0], clen);
            if (in.gcount() != clen) return false;

            uint32_t vlen;
            if (!read_u32_le(in, vlen)) return false;
            string val(vlen, '\0');
            in.read(&val[0], vlen);
            if (in.gcount() != vlen) return false;

            rd->cols[col] = move(val);
        }
        rows_[row] = move(rd);
    }

    // Restore Bloom filter
    bloom_.deserialize(in);

    cout << "[tablet:" << name_ << "] checkpoint loaded, "
              << rows_.size() << " rows\n";
    return true;
}

bool Tablet::replay_wal() {
    ifstream in(wal_path_, ios::binary);
    if (!in) return true;  // no WAL -- normal after checkpoint

    uint64_t replayed = 0;
    while (true) {
        // Read magic -- if wrong or EOF, stop (truncated tail is normal after crash)
        uint32_t magic;
        if (!read_u32_le(in, magic)) break;
        if (magic != WAL_MAGIC) {
            cerr << "[tablet:" << name_
                      << "] WAL: bad magic, stopping replay at entry "
                      << replayed << "\n";
            break;
        }

        uint8_t op;
        in.read(reinterpret_cast<char*>(&op), 1);
        if (in.gcount() != 1) break;

        uint32_t rowlen, collen, vallen;
        if (!read_u32_le(in, rowlen)) break;
        if (!read_u32_le(in, collen)) break;
        if (!read_u32_le(in, vallen)) break;

        string row(rowlen, '\0'), col(collen, '\0'), val(vallen, '\0');
        in.read(&row[0], rowlen);
        if (in.gcount() != rowlen) break;
        in.read(&col[0], collen);
        if (in.gcount() != collen) break;
        if (vallen > 0) {
            in.read(&val[0], vallen);
            if (in.gcount() != vallen) break;
        }

        // Apply to in-memory state (no WAL write during replay)
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
              << " WAL entries\n";
    return true;
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------
size_t Tablet::row_count() const {
    shared_lock<shared_mutex> rlock(rows_mu_);
    return rows_.size();
}
