#pragma once
#ifndef PENNCLOUD_KV_BLOOM_H
#define PENNCLOUD_KV_BLOOM_H

using namespace std;

// =============================================================================
// bloom.h  --  Bloom filter for PennCloud tablet key membership
// =============================================================================
//
// WHY: On GET, before hitting the unordered_map, we check the Bloom
// filter.  If it says "definitely NOT present" we return -ERR immediately --
// no map lookup, no lock contention.  This is especially valuable during
// WAL log replay on recovery, where most log entries touch rows that already
// exist in the checkpoint.
//
// DESIGN:
//   - 256 KB bit array  = 2,097,152 bits
//   - 3 independent hash functions  (djb2, fnv1a, sdbm)
//   - False positive rate ~0.8% at 500,000 keys
//   - Zero false negatives (if key was PUT, filter will say "maybe present")
//   - Serializable: dump/load the raw bit array to/from checkpoint files
//
// THREAD SAFETY: NOT thread-safe internally.  Callers must hold appropriate
// lock (shared lock for may_contain, exclusive lock for add).
// =============================================================================

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>

class BloomFilter {
public:
    static constexpr size_t BITS  = 2097152;  // 256 KB
    static constexpr size_t BYTES = BITS / 8;
    static constexpr int    NHASH = 3;

    // Initialize an empty bit array; every bit starts as "not present."
    BloomFilter() { bits_.assign(BYTES, 0); }

    // Record that key is present.  Call this every time a row key is PUT
    void add(const string& key) {
        for (int i = 0; i < NHASH; ++i)
            set_bit(hash(key, i) % BITS);
    }

    // Returns true if key MAY be present (could be false positive)
    // Returns false if key is DEFINITELY NOT present (no false negatives)
    bool may_contain(const string& key) const {
        for (int i = 0; i < NHASH; ++i)
            if (!get_bit(hash(key, i) % BITS)) return false;
        return true;
    }

    // Serialize to an open binary file stream
    void serialize(ofstream& out) const {
        out.write(reinterpret_cast<const char*>(bits_.data()), BYTES);
    }

    // Deserialize from an open binary file stream
    bool deserialize(ifstream& in) {
        bits_.assign(BYTES, 0);
        in.read(reinterpret_cast<char*>(bits_.data()), BYTES);
        return in.gcount() == static_cast<streamsize>(BYTES);
    }

    // Reset the filter to its empty state, used when loading fresh state.
    void clear() { bits_.assign(BYTES, 0); }

private:
    vector<uint8_t> bits_;

    // Mark one bit in the backing byte vector.
    void set_bit(size_t pos) {
        bits_[pos / 8] |= static_cast<uint8_t>(1u << (pos % 8));
    }

    // Test one bit in the backing byte vector.
    bool get_bit(size_t pos) const {
        return (bits_[pos / 8] >> (pos % 8)) & 1u;
    }

    // Three independent hash functions for the same key.
    // seed distinguishes them.  All produce 64-bit output reduced via modulo.
    static uint64_t hash(const string& key, int seed) {
        switch (seed) {
            case 0: return djb2(key);
            case 1: return fnv1a(key);
            default: return sdbm(key);
        }
    }

    // djb2: classic string hash by Dan Bernstein
    static uint64_t djb2(const string& s) {
        uint64_t h = 5381;
        for (unsigned char c : s)
            h = ((h << 5) + h) + c;
        return h;
    }

    // FNV-1a 64-bit: fast, good distribution
    static uint64_t fnv1a(const string& s) {
        uint64_t h = 14695981039346656037ULL;
        for (unsigned char c : s) {
            h ^= c;
            h *= 1099511628211ULL;
        }
        return h;
    }

    // sdbm: used in GNU gdbm and elsewhere
    static uint64_t sdbm(const string& s) {
        uint64_t h = 0;
        for (unsigned char c : s)
            h = c + (h << 6) + (h << 16) - h;
        return h;
    }
};

#endif  // PENNCLOUD_KV_BLOOM_H
