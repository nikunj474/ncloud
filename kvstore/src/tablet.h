#pragma once
#ifndef PENNCLOUD_KV_TABLET_H
#define PENNCLOUD_KV_TABLET_H

using namespace std;
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
    unordered_map<string, string> cols;
    mutable shared_mutex mu;

    RowData()                          = default;
    RowData(const RowData&)            = delete;
    RowData& operator=(const RowData&) = delete;
};

struct TabletDumpCell {
    string tablet;
    string row;
    string col;
    size_t      value_size = 0;
    string value_preview;
    bool        value_is_text = true;
    bool        truncated = false;
};

class Tablet {
public:
    // Create a tablet object backed by files under data_dir.
    explicit Tablet(const string& name, const string& data_dir);
    // Flush and close the WAL stream.
    ~Tablet();

    // Store a value and assign an internal LSN.
    bool put(const string& row,
             const string& col,
             const string& val);
    // Store a value and return the assigned LSN to replication callers.
    bool put(const string& row,
             const string& col,
             const string& val,
             uint64_t* assigned_lsn);

    // Read one cell value if the row/column exists.
    bool get(const string& row,
             const string& col,
             string&       val_out);

    // Compare-and-put using the tablet's internal LSN assignment.
    bool cput(const string& row,
              const string& col,
              const string& v1,
              const string& v2);
    // Compare-and-put and expose the assigned LSN for replication.
    bool cput(const string& row,
              const string& col,
              const string& v1,
              const string& v2,
              uint64_t* assigned_lsn);

    // Delete a cell if it exists.
    bool del(const string& row, const string& col);
    // Delete a cell and report whether anything was actually removed.
    bool del(const string& row, const string& col, bool* deleted_out);
    // Delete a cell and expose the assigned LSN when a delete is logged.
    bool del(const string& row, const string& col, bool* deleted_out, uint64_t* assigned_lsn);
    // Apply a delete received from replication while assigning a local LSN.
    bool apply_replicated_delete(const string& row,
                                 const string& col,
                                 uint64_t* assigned_lsn);
    // Apply a replicated put only if its expected LSN is the next operation.
    bool apply_replicated_put(const string& row,
                              const string& col,
                              const string& val,
                              uint64_t expected_lsn);
    // Apply a replicated delete only if its expected LSN is the next operation.
    bool apply_replicated_delete(const string& row,
                                 const string& col,
                                 uint64_t expected_lsn);

    // Serialize current memory state to checkpoint and truncate WAL.
    bool checkpoint();
    // Restore tablet state from checkpoint plus WAL replay.
    bool recover();

    // Return count of logical operations seen by this tablet.
    uint64_t op_count() const { return op_count_.load(); }
    // Count rows currently present in memory.
    size_t   row_count() const;
    // Return a sorted, preview-safe view of cells for admin/debug DUMP.
    vector<TabletDumpCell> dump_cells() const;

    // Expose the WAL path for diagnostics.
    string wal_path()        const { return wal_path_; }
    // Expose the checkpoint path for diagnostics.
    string checkpoint_path() const { return ckpt_path_; }

    // Return the latest logical sequence number applied to this tablet.
    uint64_t lsn() const { return lsn_.load(); }
    // Return the current checkpoint generation used for sync decisions.
    uint64_t checkpoint_version() const { return checkpoint_version_.load(); }

    // Serialize the full tablet state for replica catch-up.
    string snapshot_blob() const;
    // Replace local state with a snapshot received from a primary.
    bool        load_snapshot_blob(const string& blob);

    // Export WAL entries strictly after the given LSN.
    string wal_delta_after(uint64_t after_lsn) const;
    // Apply a delta blob containing WAL-format entries with explicit LSNs.
    bool        apply_wal_delta_blob(const string& blob);

private:
    string name_;
    string data_dir_;
    string wal_path_;
    string ckpt_path_;

    unordered_map<string, unique_ptr<RowData>> rows_;
    mutable shared_mutex rows_mu_;

    BloomFilter bloom_;

    ofstream wal_;
    mutable mutex wal_mu_;

    atomic<uint64_t> lsn_{0};
    atomic<uint64_t> checkpoint_version_{0};
    atomic<uint64_t> op_count_{0};

    // Find an existing row or create an empty row and update the Bloom filter.
    RowData* get_or_create_row(const string& row);
    // Look up a row pointer without creating it.
    RowData* find_row(const string& row);

    // Append a PUT entry to the WAL and assign the next LSN.
    bool write_wal_put(const string& row,
                       const string& col,
                       const string& val,
                       uint64_t* assigned_lsn = nullptr);

    // Append a DELETE entry to the WAL and assign the next LSN.
    bool write_wal_delete(const string& row,
                          const string& col,
                          uint64_t* assigned_lsn = nullptr);

    // Checkpoint while wal_mu_ is already held.
    bool checkpoint_locked();

    // Load checkpoint contents into memory.
    bool load_checkpoint();
    // Replay WAL entries written after the last checkpoint.
    bool replay_wal();
};

#endif  // PENNCLOUD_KV_TABLET_H
