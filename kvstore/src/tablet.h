#pragma once
#ifndef PENNCLOUD_KV_TABLET_H
#define PENNCLOUD_KV_TABLET_H
// =============================================================================
// tablet.h  --  PennCloud KV tablet: in-memory store + WAL + checkpoint
// =============================================================================
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
// =============================================================================

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <fstream>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <vector>
#include "bloom.h"

static constexpr uint32_t WAL_MAGIC  = 0xABCD1234;
static constexpr uint8_t  WAL_PUT    = 1;
static constexpr uint8_t  WAL_DELETE = 2;

static constexpr uint32_t CKPT_MAGIC = 0xC0DEFACE;

struct RowData {
    std::unordered_map<std::string, std::string> cols;
    mutable std::shared_mutex mu;

    RowData()                          = default;
    RowData(const RowData&)            = delete;
    RowData& operator=(const RowData&) = delete;
};

struct TabletDumpCell {
    std::string tablet;
    std::string row;
    std::string col;
    size_t      value_size = 0;
    std::string value_preview;
    bool        value_is_text = true;
    bool        truncated = false;
};

class Tablet {
public:
    explicit Tablet(const std::string& name, const std::string& data_dir);
    ~Tablet();

    bool put(const std::string& row,
             const std::string& col,
             const std::string& val);
    bool put(const std::string& row,
             const std::string& col,
             const std::string& val,
             uint64_t* assigned_lsn);

    bool get(const std::string& row,
             const std::string& col,
             std::string&       val_out);

    bool cput(const std::string& row,
              const std::string& col,
              const std::string& v1,
              const std::string& v2);
    bool cput(const std::string& row,
              const std::string& col,
              const std::string& v1,
              const std::string& v2,
              uint64_t* assigned_lsn);

    bool del(const std::string& row, const std::string& col);
    bool del(const std::string& row, const std::string& col, bool* deleted_out);
    bool del(const std::string& row, const std::string& col, bool* deleted_out, uint64_t* assigned_lsn);
    bool apply_replicated_delete(const std::string& row,
                                 const std::string& col,
                                 uint64_t* assigned_lsn);
    bool apply_replicated_put(const std::string& row,
                              const std::string& col,
                              const std::string& val,
                              uint64_t expected_lsn);
    bool apply_replicated_delete(const std::string& row,
                                 const std::string& col,
                                 uint64_t expected_lsn);

    bool checkpoint();
    bool recover();

    uint64_t op_count() const { return op_count_.load(); }
    size_t   row_count() const;
    std::vector<TabletDumpCell> dump_cells() const;

    std::string wal_path()        const { return wal_path_; }
    std::string checkpoint_path() const { return ckpt_path_; }

    uint64_t lsn() const { return lsn_.load(); }
    uint64_t checkpoint_version() const { return checkpoint_version_.load(); }

    // Phase B / R1 snapshot helpers
    std::string snapshot_blob() const;
    bool        load_snapshot_blob(const std::string& blob);

    // Phase R2 delta helpers
    // Export WAL entries strictly AFTER the given LSN.
    std::string wal_delta_after(uint64_t after_lsn) const;
    // Apply a delta blob containing WAL-format entries with explicit LSNs.
    bool        apply_wal_delta_blob(const std::string& blob);

private:
    std::string name_;
    std::string data_dir_;
    std::string wal_path_;
    std::string ckpt_path_;

    std::unordered_map<std::string, std::unique_ptr<RowData>> rows_;
    mutable std::shared_mutex rows_mu_;

    BloomFilter bloom_;

    std::ofstream wal_;
    mutable std::mutex wal_mu_;

    std::atomic<uint64_t> lsn_{0};
    std::atomic<uint64_t> checkpoint_version_{0};
    std::atomic<uint64_t> op_count_{0};

    RowData* get_or_create_row(const std::string& row);
    RowData* find_row(const std::string& row);

    bool write_wal_put(const std::string& row,
                       const std::string& col,
                       const std::string& val,
                       uint64_t* assigned_lsn = nullptr);

    bool write_wal_delete(const std::string& row,
                          const std::string& col,
                          uint64_t* assigned_lsn = nullptr);

    bool checkpoint_locked();

    bool load_checkpoint();
    bool replay_wal();
};

#endif  // PENNCLOUD_KV_TABLET_H
