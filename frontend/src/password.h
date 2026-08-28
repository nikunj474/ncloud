#pragma once
#ifndef PENNCLOUD_PASSWORD_H
#define PENNCLOUD_PASSWORD_H
// =============================================================================
// password.h  --  Salted password hashing for PennCloud
// =============================================================================
//
// Stored credential format (single KV value in column "pwd"):
//
//   pbkdf2_sha256$<iterations>$<salt_hex>$<derived_key_hex>
//
// Every user gets 16 bytes of salt from /dev/urandom, so identical passwords
// produce different stored values and a precomputed table is useless.  The
// derived key is PBKDF2-HMAC-SHA256, which makes each guess cost `iterations`
// HMAC rounds instead of one hash.
//
// SHA-256, HMAC and PBKDF2 are implemented here rather than pulled from a
// crypto library, matching the rest of the project: libcurl is the only
// third-party dependency.  The primitives are verified against the published
// NIST / RFC 6070 / RFC 7914 test vectors by password_selftest().
//
// Legacy plaintext credentials (written before hashing existed) are still
// accepted by verify_password(), which reports them via `needs_rehash` so the
// caller can transparently upgrade the record on the next successful login.
// =============================================================================

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace pcpw {

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4)
// ---------------------------------------------------------------------------
class Sha256 {
public:
    static constexpr size_t kDigestSize = 32;
    static constexpr size_t kBlockSize  = 64;

    Sha256() { reset(); }

    void reset() {
        state_ = {0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
                  0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u};
        buffer_len_ = 0;
        total_bits_ = 0;
    }

    void update(const uint8_t* data, size_t len) {
        total_bits_ += static_cast<uint64_t>(len) * 8u;
        while (len > 0) {
            size_t take = kBlockSize - buffer_len_;
            if (take > len) take = len;
            std::memcpy(buffer_.data() + buffer_len_, data, take);
            buffer_len_ += take;
            data        += take;
            len         -= take;
            if (buffer_len_ == kBlockSize) {
                transform(buffer_.data());
                buffer_len_ = 0;
            }
        }
    }

    void update(const std::string& s) {
        update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }

    // Writes kDigestSize bytes into out.  The object must not be reused after
    // this without calling reset().
    void final(uint8_t* out) {
        const uint64_t bits = total_bits_;

        // Padding: 0x80, then zeros, then the 64-bit big-endian length.
        static const uint8_t pad_start = 0x80;
        update(&pad_start, 1);
        static const uint8_t zero = 0x00;
        while (buffer_len_ != 56) update(&zero, 1);

        uint8_t len_be[8];
        for (int i = 0; i < 8; ++i)
            len_be[i] = static_cast<uint8_t>((bits >> (56 - 8 * i)) & 0xffu);
        // Feed the length directly; update() would corrupt total_bits_.
        std::memcpy(buffer_.data() + buffer_len_, len_be, 8);
        transform(buffer_.data());
        buffer_len_ = 0;

        for (int i = 0; i < 8; ++i) {
            out[4 * i + 0] = static_cast<uint8_t>((state_[i] >> 24) & 0xffu);
            out[4 * i + 1] = static_cast<uint8_t>((state_[i] >> 16) & 0xffu);
            out[4 * i + 2] = static_cast<uint8_t>((state_[i] >>  8) & 0xffu);
            out[4 * i + 3] = static_cast<uint8_t>((state_[i]      ) & 0xffu);
        }
    }

private:
    static uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

    void transform(const uint8_t* block) {
        static const uint32_t k[64] = {
            0x428a2f98u,0x71374491u,0xb5c0fbcfu,0xe9b5dba5u,0x3956c25bu,0x59f111f1u,
            0x923f82a4u,0xab1c5ed5u,0xd807aa98u,0x12835b01u,0x243185beu,0x550c7dc3u,
            0x72be5d74u,0x80deb1feu,0x9bdc06a7u,0xc19bf174u,0xe49b69c1u,0xefbe4786u,
            0x0fc19dc6u,0x240ca1ccu,0x2de92c6fu,0x4a7484aau,0x5cb0a9dcu,0x76f988dau,
            0x983e5152u,0xa831c66du,0xb00327c8u,0xbf597fc7u,0xc6e00bf3u,0xd5a79147u,
            0x06ca6351u,0x14292967u,0x27b70a85u,0x2e1b2138u,0x4d2c6dfcu,0x53380d13u,
            0x650a7354u,0x766a0abbu,0x81c2c92eu,0x92722c85u,0xa2bfe8a1u,0xa81a664bu,
            0xc24b8b70u,0xc76c51a3u,0xd192e819u,0xd6990624u,0xf40e3585u,0x106aa070u,
            0x19a4c116u,0x1e376c08u,0x2748774cu,0x34b0bcb5u,0x391c0cb3u,0x4ed8aa4au,
            0x5b9cca4fu,0x682e6ff3u,0x748f82eeu,0x78a5636fu,0x84c87814u,0x8cc70208u,
            0x90befffau,0xa4506cebu,0xbef9a3f7u,0xc67178f2u};

        uint32_t w[64];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<uint32_t>(block[4 * i + 0]) << 24) |
                   (static_cast<uint32_t>(block[4 * i + 1]) << 16) |
                   (static_cast<uint32_t>(block[4 * i + 2]) <<  8) |
                   (static_cast<uint32_t>(block[4 * i + 3]));
        }
        for (int i = 16; i < 64; ++i) {
            const uint32_t s0 = rotr(w[i-15],  7) ^ rotr(w[i-15], 18) ^ (w[i-15] >>  3);
            const uint32_t s1 = rotr(w[i- 2], 17) ^ rotr(w[i- 2], 19) ^ (w[i- 2] >> 10);
            w[i] = w[i-16] + s0 + w[i-7] + s1;
        }

        uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
        uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];

        for (int i = 0; i < 64; ++i) {
            const uint32_t S1    = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            const uint32_t ch    = (e & f) ^ ((~e) & g);
            const uint32_t temp1 = h + S1 + ch + k[i] + w[i];
            const uint32_t S0    = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            const uint32_t maj   = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t temp2 = S0 + maj;

            h = g; g = f; f = e; e = d + temp1;
            d = c; c = b; b = a; a = temp1 + temp2;
        }

        state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d;
        state_[4] += e; state_[5] += f; state_[6] += g; state_[7] += h;
    }

    std::array<uint32_t, 8>  state_{};
    std::array<uint8_t, 64>  buffer_{};
    size_t                   buffer_len_ = 0;
    uint64_t                 total_bits_ = 0;
};

inline void sha256(const uint8_t* data, size_t len, uint8_t* out) {
    Sha256 h;
    h.update(data, len);
    h.final(out);
}

// ---------------------------------------------------------------------------
// HMAC-SHA256 (RFC 2104)
// ---------------------------------------------------------------------------
inline void hmac_sha256(const uint8_t* key, size_t key_len,
                        const uint8_t* msg, size_t msg_len,
                        uint8_t* out) {
    uint8_t k_block[Sha256::kBlockSize];
    std::memset(k_block, 0, sizeof(k_block));

    if (key_len > Sha256::kBlockSize) {
        sha256(key, key_len, k_block);
    } else {
        std::memcpy(k_block, key, key_len);
    }

    uint8_t ipad[Sha256::kBlockSize], opad[Sha256::kBlockSize];
    for (size_t i = 0; i < Sha256::kBlockSize; ++i) {
        ipad[i] = static_cast<uint8_t>(k_block[i] ^ 0x36u);
        opad[i] = static_cast<uint8_t>(k_block[i] ^ 0x5cu);
    }

    uint8_t inner[Sha256::kDigestSize];
    Sha256 h1;
    h1.update(ipad, sizeof(ipad));
    h1.update(msg, msg_len);
    h1.final(inner);

    Sha256 h2;
    h2.update(opad, sizeof(opad));
    h2.update(inner, sizeof(inner));
    h2.final(out);
}

// ---------------------------------------------------------------------------
// PBKDF2-HMAC-SHA256 (RFC 2898)
// ---------------------------------------------------------------------------
inline void pbkdf2_sha256(const std::string& password,
                          const std::vector<uint8_t>& salt,
                          uint32_t iterations,
                          uint8_t* out, size_t out_len) {
    const uint8_t* pw     = reinterpret_cast<const uint8_t*>(password.data());
    const size_t   pw_len = password.size();

    uint32_t block_index = 1;
    size_t   produced    = 0;

    while (produced < out_len) {
        // U1 = HMAC(pw, salt || INT_BE32(block_index))
        std::vector<uint8_t> msg(salt);
        msg.push_back(static_cast<uint8_t>((block_index >> 24) & 0xffu));
        msg.push_back(static_cast<uint8_t>((block_index >> 16) & 0xffu));
        msg.push_back(static_cast<uint8_t>((block_index >>  8) & 0xffu));
        msg.push_back(static_cast<uint8_t>((block_index      ) & 0xffu));

        uint8_t u[Sha256::kDigestSize];
        hmac_sha256(pw, pw_len, msg.data(), msg.size(), u);

        uint8_t acc[Sha256::kDigestSize];
        std::memcpy(acc, u, sizeof(acc));

        for (uint32_t i = 1; i < iterations; ++i) {
            hmac_sha256(pw, pw_len, u, sizeof(u), u);
            for (size_t j = 0; j < sizeof(acc); ++j) acc[j] ^= u[j];
        }

        const size_t take = (out_len - produced < sizeof(acc))
                          ? (out_len - produced) : sizeof(acc);
        std::memcpy(out + produced, acc, take);
        produced += take;
        ++block_index;
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
inline std::string to_hex(const uint8_t* data, size_t len) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(digits[(data[i] >> 4) & 0x0fu]);
        out.push_back(digits[data[i] & 0x0fu]);
    }
    return out;
}

inline bool from_hex(const std::string& hex, std::vector<uint8_t>& out) {
    if (hex.size() % 2 != 0) return false;
    out.clear();
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        int hi = -1, lo = -1;
        for (int p = 0; p < 2; ++p) {
            const char c = hex[i + static_cast<size_t>(p)];
            int v;
            if      (c >= '0' && c <= '9') v = c - '0';
            else if (c >= 'a' && c <= 'f') v = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') v = c - 'A' + 10;
            else return false;
            if (p == 0) hi = v; else lo = v;
        }
        out.push_back(static_cast<uint8_t>((hi << 4) | lo));
    }
    return true;
}

// Length-independent equality: compares in time proportional to the longer
// input and never short-circuits on the first differing byte.
inline bool constant_time_equals(const std::string& a, const std::string& b) {
    unsigned diff = static_cast<unsigned>(a.size() ^ b.size());
    const size_t n = a.size() < b.size() ? a.size() : b.size();
    for (size_t i = 0; i < n; ++i)
        diff |= static_cast<unsigned>(
            static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]));
    // Mix in every byte of the longer string so the loop count does not leak
    // which prefix matched.
    const std::string& longer = a.size() >= b.size() ? a : b;
    for (size_t i = n; i < longer.size(); ++i)
        diff |= static_cast<unsigned>(static_cast<unsigned char>(longer[i]));
    return diff == 0;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Cost factor.  Chosen so a single verification costs roughly 100 ms on the
// development machine -- high enough to make offline guessing expensive, low
// enough that an interactive login stays responsive.
static constexpr uint32_t kPbkdf2Iterations = 120000;
static constexpr size_t   kSaltBytes        = 16;
static constexpr size_t   kDerivedKeyBytes  = 32;
static constexpr const char* kHashPrefix    = "pbkdf2_sha256$";

inline bool random_bytes(uint8_t* out, size_t len) {
    std::ifstream urandom("/dev/urandom", std::ios::binary);
    if (!urandom.is_open()) return false;
    urandom.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(len));
    return urandom.gcount() == static_cast<std::streamsize>(len);
}

// Returns "pbkdf2_sha256$<iters>$<salt_hex>$<key_hex>", or "" if the system
// entropy source could not be read (callers must treat "" as a hard failure
// and refuse to store a credential).
inline std::string hash_password(const std::string& password,
                                 uint32_t iterations = kPbkdf2Iterations) {
    std::vector<uint8_t> salt(kSaltBytes);
    if (!random_bytes(salt.data(), salt.size())) return "";

    uint8_t dk[kDerivedKeyBytes];
    pbkdf2_sha256(password, salt, iterations, dk, sizeof(dk));

    return std::string(kHashPrefix) + std::to_string(iterations) + "$" +
           to_hex(salt.data(), salt.size()) + "$" + to_hex(dk, sizeof(dk));
}

inline bool is_hashed(const std::string& stored) {
    return stored.rfind(kHashPrefix, 0) == 0;
}

// Verifies `password` against a stored credential.
//
// Accepts both the hashed format and a bare legacy plaintext value.  When a
// legacy value matches, `needs_rehash` is set so the caller can re-store the
// credential in hashed form.
inline bool verify_password(const std::string& password,
                            const std::string& stored,
                            bool* needs_rehash = nullptr) {
    if (needs_rehash) *needs_rehash = false;
    if (stored.empty()) return false;

    if (!is_hashed(stored)) {
        // Legacy plaintext record written before hashing was introduced.
        const bool ok = constant_time_equals(password, stored);
        if (ok && needs_rehash) *needs_rehash = true;
        return ok;
    }

    // pbkdf2_sha256$<iters>$<salt_hex>$<key_hex>
    const std::string body = stored.substr(std::strlen(kHashPrefix));
    const size_t d1 = body.find('$');
    if (d1 == std::string::npos) return false;
    const size_t d2 = body.find('$', d1 + 1);
    if (d2 == std::string::npos) return false;

    uint32_t iterations = 0;
    try {
        const unsigned long parsed = std::stoul(body.substr(0, d1));
        if (parsed == 0 || parsed > 10000000ul) return false;
        iterations = static_cast<uint32_t>(parsed);
    } catch (...) {
        return false;
    }

    std::vector<uint8_t> salt;
    if (!from_hex(body.substr(d1 + 1, d2 - d1 - 1), salt) || salt.empty())
        return false;

    const std::string expected_hex = body.substr(d2 + 1);
    if (expected_hex.empty() || expected_hex.size() % 2 != 0) return false;

    std::vector<uint8_t> dk(expected_hex.size() / 2);
    pbkdf2_sha256(password, salt, iterations, dk.data(), dk.size());

    const bool ok = constant_time_equals(to_hex(dk.data(), dk.size()), expected_hex);
    if (ok && needs_rehash && iterations < kPbkdf2Iterations) *needs_rehash = true;
    return ok;
}

// Verifies the primitives against published test vectors.  Returns true when
// every vector matches; used by the unit test and callable at startup.
inline bool password_selftest() {
    // SHA-256 -- FIPS 180-4 examples.
    {
        uint8_t d[32];
        sha256(reinterpret_cast<const uint8_t*>(""), 0, d);
        if (to_hex(d, 32) !=
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            return false;

        sha256(reinterpret_cast<const uint8_t*>("abc"), 3, d);
        if (to_hex(d, 32) !=
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
            return false;

        const std::string long_msg =
            "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
        sha256(reinterpret_cast<const uint8_t*>(long_msg.data()),
               long_msg.size(), d);
        if (to_hex(d, 32) !=
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")
            return false;
    }

    // HMAC-SHA256 -- RFC 4231 test case 1.
    {
        const std::vector<uint8_t> key(20, 0x0b);
        const std::string msg = "Hi There";
        uint8_t mac[32];
        hmac_sha256(key.data(), key.size(),
                    reinterpret_cast<const uint8_t*>(msg.data()), msg.size(), mac);
        if (to_hex(mac, 32) !=
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7")
            return false;
    }

    // PBKDF2-HMAC-SHA256 -- RFC 7914 section 11 vectors.
    {
        const std::vector<uint8_t> salt{'s', 'a', 'l', 't'};
        uint8_t dk[32];

        pbkdf2_sha256("password", salt, 1, dk, sizeof(dk));
        if (to_hex(dk, 32) !=
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b")
            return false;

        pbkdf2_sha256("password", salt, 2, dk, sizeof(dk));
        if (to_hex(dk, 32) !=
            "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43")
            return false;

        pbkdf2_sha256("password", salt, 4096, dk, sizeof(dk));
        if (to_hex(dk, 32) !=
            "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a")
            return false;
    }

    return true;
}

}  // namespace pcpw

#endif  // PENNCLOUD_PASSWORD_H
