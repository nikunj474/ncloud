#pragma once

// tablet.h  --  PennCloud KV tablet: in-memory store + WAL + checkpoint
//
// A Tablet is one horizontal slice of the key-value table.
// The coordinator assigns row-key ranges to tablets.
// Each tablet lives on one storage node (replicated elsewhere separately).
//
// CONCURRENCY MODEL (B5 innovation -- per-row shared_mutex):
//
//   rows_mu_   (shared_mutex)   -- guards the rows_ map STRUCTURE
//                                  (inserting/deleting row entries)
//
//   RowData::mu (shared_mutex)  -- guards one row's column data
//
//   Locking hierarchy: always acquire rows_mu_ before RowData::mu.
//   Never hold both in exclusive mode simultaneously for different rows.
//
//   GET  : shared_lock(rows_mu_)  ->  shared_lock(row->mu)
//          Multiple concurrent GETs to ANY row never block each other.
//
//   PUT  : shared_lock(rows_mu_)  ->  unique_lock(row->mu)   [existing row]
//          unique_lock(rows_mu_)  ->  unique_lock(row->mu)   [new row]
//          Only blocks other writers to the SAME row, not other rows.
//
// WAL (Write-Ahead Log):
//   Every PUT/DELETE is written to the WAL file BEFORE modifying the
//   in-memory map.  If we crash mid-operation:
//     - Crash after WAL write: replay on recovery restores the write.
//     - Crash before WAL write: operation never happened -- correct.
//   This is the fundamental guarantee of WAL-based durability.
//
// CHECKPOINT:
//   Periodically we serialize the full in-memory map to a checkpoint file
//   and truncate the WAL.  Recovery = load checkpoint + replay WAL.


#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <fstream>
#include <mutex>
#include <atomic>
#include <cstdint>
#include "bloom.h"
using namespace std;

// WAL record format (binary, written before each mutation)

//   [4 bytes magic: 0xABCD1234]
//   [1 byte op:     PUT=1, DELETE=2]
//   [4 bytes LE:    rowlen]
//   [4 bytes LE:    collen]
//   [4 bytes LE:    vallen]
//   [rowlen bytes:  row key]
//   [collen bytes:  col key]
//   [vallen bytes:  value (empty for DELETE)]
//
// On recovery: read magic -- if wrong, stop (truncated/corrupt tail).
//
static constexpr uint32_t WAL_MAGIC  = 0xABCD1234;
static constexpr uint8_t  WAL_PUT    = 1;
static constexpr uint8_t  WAL_DELETE = 2;

// RowData: one row's column map + its own shared_mutex

// shared_mutex is not copyable or movable, so RowData cannot be stored
// directly in unordered_map.  We store unique_ptr<RowData> instead.

struct RowData {
    unordered_map<string, string> cols;
    mutable shared_mutex mu;  // B5: per-row RW lock

    RowData()                            = default;
    RowData(const RowData&)              = delete;
    RowData& operator=(const RowData&)   = delete;
};

// Tablet

class Tablet {
public:
    // name      : tablet identifier, e.g. "tablet_aa_af"
    // data_dir  : directory for WAL and checkpoint files
    explicit Tablet(const string& name, const string& data_dir);
    ~Tablet();

    // Core KV operations (thread-safe)
    bool        put(const string& row,
                    const string& col,
                    const string& val);

    bool        get(const string& row,
                    const string& col,
                    string&       val_out);

    // CPUT: store v2 only if current value equals v1.
    // Returns true on success, false if value did not match.
    bool        cput(const string& row,
                     const string& col,
                     const string& v1,
                     const string& v2);

    bool        del(const string& row, const string& col);

    // Checkpoint + recovery
    bool        checkpoint();          // write full snapshot, truncate WAL
    bool        recover();             // load checkpoint then replay WAL

    // Stats (for admin console metrics -- F5)
    uint64_t    op_count() const { return op_count_.load(); }
    size_t      row_count() const;

    // Serialize Bloom filter into checkpoint (called by checkpoint())
    // Returns the WAL path for external use (e.g. replication layer)
    string wal_path()        const { return wal_path_; }
    string checkpoint_path() const { return ckpt_path_; }

    // LSN: log sequence number -- incremented on every WAL write.
    // Used by replication layer to track lag between primary and secondary.
    uint64_t    lsn() const { return lsn_.load(); }

private:
    // ---- data members -------------------------------------------------------
    string name_;
    string data_dir_;
    string wal_path_;
    string ckpt_path_;

    // Two-level locking (B5):
    //   rows_mu_  protects the rows_ map structure (insertions/deletions)
    //   RowData::mu protects each row's column data
    unordered_map<string, unique_ptr<RowData>> rows_;
    mutable shared_mutex rows_mu_;

    // Bloom filter for fast negative-lookup (B4)
    BloomFilter bloom_;
    // bloom_ is protected by rows_mu_ (same lifecycle as rows_ structure)

    // WAL file handle (append-only, opened at construction, closed at destruction)
    ofstream wal_;
    mutex    wal_mu_;   // serializes WAL writes (separate from data locks)

    // Monotonically increasing log sequence number
    atomic<uint64_t> lsn_{0};

    // Operation counter for metrics
    atomic<uint64_t> op_count_{0};

    // ---- private helpers ----------------------------------------------------
    bool write_wal_put(const string& row,
                       const string& col,
                       const string& val);

    bool write_wal_delete(const string& row,
                          const string& col);

    // Get or create RowData for a row key.
    // Caller must hold shared_lock(rows_mu_) for existing rows,
    // or unique_lock(rows_mu_) for potentially new rows.
    RowData* get_or_create_row(const string& row);
    RowData* find_row(const string& row);  // returns nullptr if absent

    bool load_checkpoint();
    bool replay_wal();
};
