#include "kvstore/kvstore.hpp"
#include "kvstore/wal.hpp"
#include "kvstore/crc32.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

// ============================================================================
// Lightweight Test Framework
// ============================================================================

struct TestCase {
    std::string name;
    std::function<void()> func;
};

inline std::vector<TestCase>& get_test_registry() {
    static std::vector<TestCase> registry;
    return registry;
}

struct TestRegistrar {
    TestRegistrar(const std::string& name, std::function<void()> func) {
        get_test_registry().push_back({name, std::move(func)});
    }
};

class TestFailureException : public std::runtime_error {
public:
    explicit TestFailureException(const std::string& msg) : std::runtime_error(msg) {}
};

#define TEST_CASE(name) \
    void test_##name(); \
    static TestRegistrar registrar_##name(#name, test_##name); \
    void test_##name()

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #cond ") at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

#define ASSERT_FALSE(cond) do { \
    if (cond) { \
        std::ostringstream oss; \
        oss << "Assertion failed (expected false): (" #cond ") at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

template <typename T>
inline std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt.has_value()) {
        return os << "Some(" << opt.value() << ")";
    }
    return os << "None";
}

template <typename T, typename U>
inline bool test_equal(const T& a, const U& b) {
    if constexpr (std::is_integral_v<T> && std::is_integral_v<U>) {
        return std::cmp_equal(a, b);
    } else {
        return a == b;
    }
}

#define ASSERT_EQ(a, b) do { \
    auto _a = (a); \
    auto _b = (b); \
    if (!test_equal(_a, _b)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #a " == " #b ") [" << _a << " != " << _b << "] at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

#define ASSERT_NE(a, b) do { \
    auto _a = (a); \
    auto _b = (b); \
    if (test_equal(_a, _b)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #a " != " #b ") [" << _a << " == " << _b << "] at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

#define ASSERT_THROWS(expr, ExceptionType) do { \
    bool caught = false; \
    try { \
        expr; \
    } catch (const ExceptionType&) { \
        caught = true; \
    } catch (...) { \
        std::ostringstream oss; \
        oss << "Expected " #ExceptionType " but caught different exception at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
    if (!caught) { \
        std::ostringstream oss; \
        oss << "Expected " #ExceptionType " was not thrown at " << __FILE__ << ":" << __LINE__; \
        throw TestFailureException(oss.str()); \
    } \
} while(0)

// Helper: RAII temporary test file cleanup
class TempWalFile {
public:
    explicit TempWalFile(const std::string& prefix) {
        char tmpl[256];
        snprintf(tmpl, sizeof(tmpl), "/tmp/%s_XXXXXX", prefix.c_str());
        int fd = ::mkstemp(tmpl);
        if (fd >= 0) {
            ::close(fd);
            path_ = tmpl;
        } else {
            path_ = "/tmp/" + prefix + "_" + std::to_string(rand()) + ".wal";
        }
    }

    ~TempWalFile() {
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    const std::string& path() const { return path_; }

    size_t size() const {
        struct stat st;
        if (::stat(path_.c_str(), &st) == 0) {
            return static_cast<size_t>(st.st_size);
        }
        return 0;
    }

    void append_raw(const void* data, size_t len) {
        int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
        if (fd < 0) {
            throw std::runtime_error("TempWalFile: failed to open for append");
        }
        ssize_t written = ::write(fd, data, len);
        ::close(fd);
        if (written != static_cast<ssize_t>(len)) {
            throw std::runtime_error("TempWalFile: partial write in append_raw");
        }
    }

private:
    std::string path_;
};

// Helper: Raw record builder
inline std::vector<uint8_t> build_raw_record(kvstore::OpCode op, uint64_t ts, const std::string& key, const std::string& value) {
    constexpr size_t HEADER_SIZE = 20;
    constexpr size_t CRC_SIZE = 4;
    uint32_t klen = static_cast<uint32_t>(key.size());
    uint32_t vlen = static_cast<uint32_t>(value.size());
    size_t total_size = HEADER_SIZE + klen + vlen + CRC_SIZE;

    std::vector<uint8_t> buf(total_size);
    buf[0] = 0x57; // 'W'
    buf[1] = 0x4C; // 'L'
    buf[2] = static_cast<uint8_t>(op);
    buf[3] = 0x00;

    for (int i = 0; i < 8; ++i) {
        buf[4 + i] = static_cast<uint8_t>((ts >> ((7 - i) * 8)) & 0xFF);
    }
    for (int i = 0; i < 4; ++i) {
        buf[12 + i] = static_cast<uint8_t>((klen >> ((3 - i) * 8)) & 0xFF);
    }
    for (int i = 0; i < 4; ++i) {
        buf[16 + i] = static_cast<uint8_t>((vlen >> ((3 - i) * 8)) & 0xFF);
    }

    if (klen > 0) {
        std::memcpy(&buf[HEADER_SIZE], key.data(), klen);
    }
    if (vlen > 0) {
        std::memcpy(&buf[HEADER_SIZE + klen], value.data(), vlen);
    }

    uint32_t crc = kvstore::Crc32::compute(buf.data(), HEADER_SIZE + klen + vlen);
    for (int i = 0; i < 4; ++i) {
        buf[HEADER_SIZE + klen + vlen + i] = static_cast<uint8_t>((crc >> ((3 - i) * 8)) & 0xFF);
    }

    return buf;
}

// ============================================================================
// Adversarial Stress Tests
// ============================================================================

// 1. Systematic Byte-by-Byte Torn Write Sweep on 24-byte OP_CLEAR Record (Empty File Prefix)
TEST_CASE(sweep_torn_writes_clear_record_empty_file) {
    auto full_record = build_raw_record(kvstore::OpCode::OP_CLEAR, 123456789ULL, "", "");
    ASSERT_EQ(full_record.size(), static_cast<size_t>(24));

    // Test every single truncated length from 1 byte up to 23 bytes
    for (size_t torn_len = 1; torn_len < 24; ++torn_len) {
        TempWalFile temp("torn_clear_empty");
        temp.append_raw(full_record.data(), torn_len);
        ASSERT_EQ(temp.size(), torn_len);

        // Recover: must detect torn write, truncate file to 0, return 0
        kvstore::KVStore store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(store);
            ASSERT_EQ(recovered, static_cast<size_t>(0));
            ASSERT_EQ(store.size(), static_cast<size_t>(0));
            ASSERT_EQ(wal.file_size(), static_cast<size_t>(0));
        }

        // Verify file on disk was truncated to 0
        ASSERT_EQ(temp.size(), static_cast<size_t>(0));

        // Verify subsequent writes proceed cleanly without corruption
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            ASSERT_TRUE(wal.append_set("key_after_torn", "val_after_torn"));
        }

        // Re-recover and verify store has the single valid subsequent record
        kvstore::KVStore re_store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(re_store);
            ASSERT_EQ(recovered, static_cast<size_t>(1));
            ASSERT_EQ(re_store.size(), static_cast<size_t>(1));
            ASSERT_EQ(re_store.get("key_after_torn").value(), "val_after_torn");
        }
    }
}

// 2. Systematic Byte-by-Byte Torn Write Sweep on 24-byte OP_CLEAR Record (Valid Prefix)
TEST_CASE(sweep_torn_writes_clear_record_valid_prefix) {
    auto full_record = build_raw_record(kvstore::OpCode::OP_CLEAR, 999999ULL, "", "");
    ASSERT_EQ(full_record.size(), static_cast<size_t>(24));

    for (size_t torn_len = 1; torn_len < 24; ++torn_len) {
        TempWalFile temp("torn_clear_prefix");
        size_t valid_size = 0;

        // Write 3 valid records first
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            wal.append_set("k1", "v1");
            wal.append_set("k2", "v2");
            wal.append_set("k3", "v3");
        }
        valid_size = temp.size();
        ASSERT_TRUE(valid_size > 0);

        // Inject torn record of torn_len bytes
        temp.append_raw(full_record.data(), torn_len);
        ASSERT_EQ(temp.size(), valid_size + torn_len);

        // Recover: must recover 3 valid prefix records, truncate tail back to valid_size
        kvstore::KVStore store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(store);
            ASSERT_EQ(recovered, static_cast<size_t>(3));
            ASSERT_EQ(store.size(), static_cast<size_t>(3));
            ASSERT_EQ(store.get("k1").value(), "v1");
            ASSERT_EQ(store.get("k2").value(), "v2");
            ASSERT_EQ(store.get("k3").value(), "v3");
            ASSERT_EQ(wal.file_size(), valid_size);
        }

        // Verify file on disk is truncated back to valid_size
        ASSERT_EQ(temp.size(), valid_size);

        // Verify subsequent append on the same WAL works
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            ASSERT_TRUE(wal.append_set("k4", "v4"));
        }

        // Re-recover and verify store has all 4 records
        kvstore::KVStore re_store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(re_store);
            ASSERT_EQ(recovered, static_cast<size_t>(4));
            ASSERT_EQ(re_store.size(), static_cast<size_t>(4));
            ASSERT_EQ(re_store.get("k4").value(), "v4");
        }
    }
}

// 3. Systematic Byte-by-Byte Torn Write Sweep with Non-Empty Payload (SET Operation, 48 Bytes Total)
// Tests truncation at every byte: truncated magic, truncated header, truncated key, truncated val, truncated CRC
TEST_CASE(sweep_torn_writes_set_record_all_47_offsets) {
    std::string key = "alpha_1";      // 7 bytes
    std::string val = "beta_value_2"; // 12 bytes
    // Header (20) + Key (7) + Val (12) + CRC (4) = 43 bytes
    auto full_record = build_raw_record(kvstore::OpCode::OP_SET, 7777777ULL, key, val);
    ASSERT_EQ(full_record.size(), static_cast<size_t>(43));

    for (size_t torn_len = 1; torn_len < 43; ++torn_len) {
        TempWalFile temp("torn_set_sweep");
        size_t valid_size = 0;

        // Write 2 valid prefix records
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            wal.append_set("init_key1", "init_val1");
            wal.append_set("init_key2", "init_val2");
        }
        valid_size = temp.size();

        // Inject torn record of torn_len bytes
        temp.append_raw(full_record.data(), torn_len);
        ASSERT_EQ(temp.size(), valid_size + torn_len);

        // Recover: must recover exactly 2 prefix records, truncate back to valid_size
        kvstore::KVStore store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(store);
            ASSERT_EQ(recovered, static_cast<size_t>(2));
            ASSERT_EQ(store.size(), static_cast<size_t>(2));
            ASSERT_EQ(store.get("init_key1").value(), "init_val1");
            ASSERT_EQ(store.get("init_key2").value(), "init_val2");
            ASSERT_FALSE(store.get(key).has_value());
            ASSERT_EQ(wal.file_size(), valid_size);

            // Test immediate write on the SAME WalManager handle without reopening
            ASSERT_TRUE(wal.append_set("subsequent_key", "subsequent_val"));
        }

        // Verify subsequent write was persisted and tail was not corrupted
        kvstore::KVStore re_store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(re_store);
            ASSERT_EQ(recovered, static_cast<size_t>(3));
            ASSERT_EQ(re_store.size(), static_cast<size_t>(3));
            ASSERT_EQ(re_store.get("init_key1").value(), "init_val1");
            ASSERT_EQ(re_store.get("init_key2").value(), "init_val2");
            ASSERT_EQ(re_store.get("subsequent_key").value(), "subsequent_val");
            ASSERT_FALSE(re_store.get(key).has_value());
        }
    }
}

// 4. Systematic Byte-by-Byte Torn Write Sweep with DEL Operation (37 Bytes Total)
TEST_CASE(sweep_torn_writes_del_record_all_offsets) {
    std::string del_key = "target_del_key"; // 14 bytes
    // Header (20) + Key (14) + Val (0) + CRC (4) = 38 bytes
    auto full_record = build_raw_record(kvstore::OpCode::OP_DEL, 8888888ULL, del_key, "");
    ASSERT_EQ(full_record.size(), static_cast<size_t>(38));

    for (size_t torn_len = 1; torn_len < 38; ++torn_len) {
        TempWalFile temp("torn_del_sweep");

        // Write record to be deleted, then inject torn deletion record
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            wal.append_set(del_key, "original_content");
        }
        size_t valid_size = temp.size();

        temp.append_raw(full_record.data(), torn_len);
        ASSERT_EQ(temp.size(), valid_size + torn_len);

        // Recover: del record was torn, so del should NOT be applied; key must remain
        kvstore::KVStore store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(store);
            ASSERT_EQ(recovered, static_cast<size_t>(1));
            ASSERT_EQ(store.get(del_key).value(), "original_content");
            ASSERT_EQ(wal.file_size(), valid_size);
        }

        ASSERT_EQ(temp.size(), valid_size);
    }
}

// 5. Corrupted CRC32 Checksums (Tail Bit Flips, Inversions, and Zeroes)
TEST_CASE(corrupted_crc32_checksums_at_tail) {
    std::string key = "sample_key";
    std::string val = "sample_val";
    auto valid_rec = build_raw_record(kvstore::OpCode::OP_SET, 1000ULL, key, val);
    size_t rec_len = valid_rec.size();

    std::vector<std::string> test_labels = {
        "flip_crc_byte_0", "flip_crc_byte_1", "flip_crc_byte_2", "flip_crc_byte_3",
        "invert_all_crc_bits", "zero_crc"
    };

    for (size_t variant = 0; variant < test_labels.size(); ++variant) {
        TempWalFile temp("bad_crc_" + std::to_string(variant));
        size_t valid_size = 0;

        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            wal.append_set("prefix_1", "p1");
            wal.append_set("prefix_2", "p2");
        }
        valid_size = temp.size();

        auto corrupted_rec = valid_rec;
        size_t crc_offset = rec_len - 4;

        if (variant == 0) corrupted_rec[crc_offset + 0] ^= 0x01;
        else if (variant == 1) corrupted_rec[crc_offset + 1] ^= 0x01;
        else if (variant == 2) corrupted_rec[crc_offset + 2] ^= 0x01;
        else if (variant == 3) corrupted_rec[crc_offset + 3] ^= 0x01;
        else if (variant == 4) {
            corrupted_rec[crc_offset + 0] = ~corrupted_rec[crc_offset + 0];
            corrupted_rec[crc_offset + 1] = ~corrupted_rec[crc_offset + 1];
            corrupted_rec[crc_offset + 2] = ~corrupted_rec[crc_offset + 2];
            corrupted_rec[crc_offset + 3] = ~corrupted_rec[crc_offset + 3];
        } else if (variant == 5) {
            corrupted_rec[crc_offset + 0] = 0x00;
            corrupted_rec[crc_offset + 1] = 0x00;
            corrupted_rec[crc_offset + 2] = 0x00;
            corrupted_rec[crc_offset + 3] = 0x00;
        }

        temp.append_raw(corrupted_rec.data(), corrupted_rec.size());

        // Recover: must detect CRC error, truncate to valid_size, recover only prefix_1 & prefix_2
        kvstore::KVStore store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(store);
            ASSERT_EQ(recovered, static_cast<size_t>(2));
            ASSERT_EQ(store.size(), static_cast<size_t>(2));
            ASSERT_EQ(store.get("prefix_1").value(), "p1");
            ASSERT_EQ(store.get("prefix_2").value(), "p2");
            ASSERT_FALSE(store.get(key).has_value());
            ASSERT_EQ(wal.file_size(), valid_size);
        }

        ASSERT_EQ(temp.size(), valid_size);

        // Verify subsequent write after CRC error truncation
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            ASSERT_TRUE(wal.append_set("subsequent", "valid"));
        }

        kvstore::KVStore re_store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(re_store);
            ASSERT_EQ(recovered, static_cast<size_t>(3));
            ASSERT_EQ(re_store.get("subsequent").value(), "valid");
        }
    }
}

// 6. Corrupted Record in Middle of Log (Mid-Stream Corruption Truncates to Last Valid Point)
TEST_CASE(corrupted_record_in_middle_of_wal) {
    TempWalFile temp("mid_corruption");
    size_t prefix_size = 0;

    // Phase 1: Write 5 valid records
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        for (int i = 1; i <= 5; ++i) {
            wal.append_set("key_" + std::to_string(i), "val_" + std::to_string(i));
        }
    }
    prefix_size = temp.size();

    // Phase 2: Append 6th record with corrupted key payload (CRC mismatch)
    auto rec6 = build_raw_record(kvstore::OpCode::OP_SET, 6000ULL, "key_6", "val_6");
    rec6[22] ^= 0xFF; // Corrupt key character
    temp.append_raw(rec6.data(), rec6.size());

    // Phase 3: Append 7th and 8th records (valid format, but trailing corrupted rec6)
    auto rec7 = build_raw_record(kvstore::OpCode::OP_SET, 7000ULL, "key_7", "val_7");
    temp.append_raw(rec7.data(), rec7.size());
    auto rec8 = build_raw_record(kvstore::OpCode::OP_SET, 8000ULL, "key_8", "val_8");
    temp.append_raw(rec8.data(), rec8.size());

    // Recover: must halt at corrupted rec6, truncate back to prefix_size, recover 5 records
    kvstore::KVStore store;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(5));
        ASSERT_EQ(store.size(), static_cast<size_t>(5));
        ASSERT_FALSE(store.get("key_6").has_value());
        ASSERT_FALSE(store.get("key_7").has_value());
        ASSERT_FALSE(store.get("key_8").has_value());
        ASSERT_EQ(wal.file_size(), prefix_size);
    }

    ASSERT_EQ(temp.size(), prefix_size);
}

// 7. Corrupted Magic Bytes at Tail
TEST_CASE(corrupted_magic_bytes_detection) {
    TempWalFile temp("bad_magic");

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("valid_k", "valid_v");
    }
    size_t valid_size = temp.size();

    // Append record with invalid magic bytes {0xFF, 0x4C}
    auto rec = build_raw_record(kvstore::OpCode::OP_SET, 2000ULL, "bad_magic_k", "v");
    rec[0] = 0xFF;
    temp.append_raw(rec.data(), rec.size());

    kvstore::KVStore store;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(1));
        ASSERT_EQ(store.get("valid_k").value(), "valid_v");
        ASSERT_FALSE(store.get("bad_magic_k").has_value());
        ASSERT_EQ(wal.file_size(), valid_size);
    }
    ASSERT_EQ(temp.size(), valid_size);
}

// 8. Corrupted OpCode Detection
TEST_CASE(corrupted_opcode_detection) {
    TempWalFile temp("bad_opcode");

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("valid_k", "valid_v");
    }
    size_t valid_size = temp.size();

    // OpCode 0x99 is undefined
    auto rec = build_raw_record(kvstore::OpCode::OP_SET, 3000ULL, "k", "v");
    rec[2] = 0x99;
    temp.append_raw(rec.data(), rec.size());

    kvstore::KVStore store;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(1));
        ASSERT_EQ(wal.file_size(), valid_size);
    }
    ASSERT_EQ(temp.size(), valid_size);
}

// 9. Length Sanity Guard Against Unbounded Allocations
TEST_CASE(corrupted_length_unbounded_guard) {
    TempWalFile temp("bad_length");

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("pre_key", "pre_val");
    }
    size_t valid_size = temp.size();

    // Craft record header claiming key_len = 0xFFFFFFFF (4 GB)
    uint8_t bad_hdr[20];
    std::memset(bad_hdr, 0, sizeof(bad_hdr));
    bad_hdr[0] = 0x57; bad_hdr[1] = 0x4C;
    bad_hdr[2] = static_cast<uint8_t>(kvstore::OpCode::OP_SET);
    bad_hdr[12] = 0xFF; bad_hdr[13] = 0xFF; bad_hdr[14] = 0xFF; bad_hdr[15] = 0xFF; // 4GB key_len
    temp.append_raw(bad_hdr, sizeof(bad_hdr));

    kvstore::KVStore store;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        // Recovery must not throw bad_alloc or OOM; it must truncate and return 1
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(1));
        ASSERT_EQ(wal.file_size(), valid_size);
    }
    ASSERT_EQ(temp.size(), valid_size);
}

// 10. Zero-Byte Files & Empty File Idempotency
TEST_CASE(zero_byte_file_handling) {
    TempWalFile temp("zero_byte");
    ASSERT_EQ(temp.size(), static_cast<size_t>(0));

    kvstore::KVStore store1;
    kvstore::KVStore store2;

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        // Recovering empty file returns 0
        ASSERT_EQ(wal.recover(store1), static_cast<size_t>(0));
        ASSERT_EQ(store1.size(), static_cast<size_t>(0));
        ASSERT_EQ(wal.file_size(), static_cast<size_t>(0));

        // Immediate second recovery on same handle is idempotent
        ASSERT_EQ(wal.recover(store2), static_cast<size_t>(0));
        ASSERT_EQ(store2.size(), static_cast<size_t>(0));

        // Append to initially empty file
        ASSERT_TRUE(wal.append_set("k_initial", "v_initial"));
        ASSERT_TRUE(wal.file_size() > 0);
    }

    // Recover into fresh store
    kvstore::KVStore store3;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        ASSERT_EQ(wal.recover(store3), static_cast<size_t>(1));
        ASSERT_EQ(store3.get("k_initial").value(), "v_initial");
    }
}

// 11. Unreadable and Inaccessible File Fault Injection
TEST_CASE(unreadable_file_handling) {
    TempWalFile temp("unreadable");

    // Case A: Read-only permissions (chmod 0400).
    // WalManager opens with O_RDWR, so open() must fail with EACCES and constructor throw std::runtime_error.
    ::chmod(temp.path().c_str(), 0400);
    ASSERT_THROWS(
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS),
        std::runtime_error
    );

    // Case B: No permissions (chmod 0000)
    ::chmod(temp.path().c_str(), 0000);
    ASSERT_THROWS(
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS),
        std::runtime_error
    );

    // Restore permissions for clean temp file removal
    ::chmod(temp.path().c_str(), 0644);

    // Case C: Path is a directory
    ASSERT_THROWS(
        kvstore::WalManager wal("/tmp", kvstore::SyncMode::SYNC_ALWAYS),
        std::runtime_error
    );

    // Case D: Non-existent directory path
    ASSERT_THROWS(
        kvstore::WalManager wal("/nonexistent_dir_12345/sub/test.wal", kvstore::SyncMode::SYNC_ALWAYS),
        std::runtime_error
    );
}

// 12. Closed WAL Safety (Method Invocations on Closed Handle)
TEST_CASE(closed_wal_handle_safety) {
    TempWalFile temp("closed_wal");
    kvstore::KVStore store;

    kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
    wal.append_set("k1", "v1");
    ASSERT_TRUE(wal.is_open());

    wal.close();
    ASSERT_FALSE(wal.is_open());
    ASSERT_EQ(wal.file_size(), static_cast<size_t>(0));

    // Operations after close() must fail gracefully without crashing
    ASSERT_FALSE(wal.append_set("k2", "v2"));
    ASSERT_FALSE(wal.append_del("k1"));
    ASSERT_FALSE(wal.append_clear());
    ASSERT_EQ(wal.recover(store), static_cast<size_t>(0));

    // Repeated close() must be idempotent
    wal.close();
    wal.sync();
}

// 13. Multi-Cycle Torn Write -> Repair -> Append -> Torn Write -> Repair Stress
TEST_CASE(multi_cycle_recovery_and_append_ping_pong) {
    TempWalFile temp("ping_pong");

    for (int cycle = 0; cycle < 5; ++cycle) {
        size_t valid_size = 0;

        // Write 2 valid records in this cycle
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            wal.append_set("cycle_" + std::to_string(cycle) + "_a", "val_a");
            wal.append_set("cycle_" + std::to_string(cycle) + "_b", "val_b");
        }
        valid_size = temp.size();

        // Inject 11 bytes of a torn header
        uint8_t torn_hdr[11] = {0x57, 0x4C, 0x01, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77};
        temp.append_raw(torn_hdr, sizeof(torn_hdr));

        // Recover: must repair tail to valid_size
        kvstore::KVStore store;
        {
            kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
            size_t recovered = wal.recover(store);
            ASSERT_EQ(recovered, static_cast<size_t>((cycle + 1) * 2));
            ASSERT_EQ(wal.file_size(), valid_size);
        }
        ASSERT_EQ(temp.size(), valid_size);
    }

    // Final verification: exactly 10 records exist in store
    kvstore::KVStore final_store;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t count = wal.recover(final_store);
        ASSERT_EQ(count, static_cast<size_t>(10));
        ASSERT_EQ(final_store.size(), static_cast<size_t>(10));
        for (int c = 0; c < 5; ++c) {
            ASSERT_EQ(final_store.get("cycle_" + std::to_string(c) + "_a").value(), "val_a");
            ASSERT_EQ(final_store.get("cycle_" + std::to_string(c) + "_b").value(), "val_b");
        }
    }
}

// 14. Large Record Torn Write Handling (64KB payload truncated halfway)
TEST_CASE(large_record_torn_write) {
    TempWalFile temp("large_torn");

    // Write a valid record first
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("lead_in", "lead_val");
    }
    size_t valid_size = temp.size();

    // Construct 64 KB value record
    std::string big_val(64 * 1024, 'X');
    auto big_rec = build_raw_record(kvstore::OpCode::OP_SET, 99999ULL, "big_key", big_val);

    // Truncate halfway (32 KB + 20 bytes)
    size_t torn_len = 32 * 1024 + 20;
    temp.append_raw(big_rec.data(), torn_len);

    kvstore::KVStore store;
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(1));
        ASSERT_EQ(store.size(), static_cast<size_t>(1));
        ASSERT_FALSE(store.get("big_key").has_value());
        ASSERT_EQ(wal.file_size(), valid_size);
    }
    ASSERT_EQ(temp.size(), valid_size);
}

// ============================================================================
// Main Runner
// ============================================================================

int main() {
    std::cout << "\n=======================================================\n";
    std::cout << "  KVStore Adversarial Crash, Torn Write & Tail Repair  \n";
    std::cout << "=======================================================\n\n";

    int passed = 0;
    int failed = 0;
    auto t_start = std::chrono::steady_clock::now();

    for (const auto& test : get_test_registry()) {
        auto t0 = std::chrono::steady_clock::now();
        try {
            test.func();
            auto t1 = std::chrono::steady_clock::now();
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            std::cout << "[  PASSED  ] " << test.name << " (" << us / 1000.0 << " ms)" << std::endl;
            passed++;
        } catch (const TestFailureException& e) {
            std::cout << "[  FAILED  ] " << test.name << std::endl;
            std::cout << "             " << e.what() << std::endl;
            failed++;
        } catch (const std::exception& e) {
            std::cout << "[  FAILED  ] " << test.name << " (unexpected std::exception)" << std::endl;
            std::cout << "             " << e.what() << std::endl;
            failed++;
        } catch (...) {
            std::cout << "[  FAILED  ] " << test.name << " (unknown exception)" << std::endl;
            failed++;
        }
    }

    auto t_end = std::chrono::steady_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();

    std::cout << "\n-------------------------------------------------------\n";
    std::cout << "Total Adversarial Tests : " << (passed + failed) << "\n";
    std::cout << "Passed                  : " << passed << "\n";
    std::cout << "Failed                  : " << failed << "\n";
    std::cout << "Elapsed Time            : " << total_ms << " ms\n";
    std::cout << "-------------------------------------------------------\n";

    if (failed == 0) {
        std::cout << ">>> ALL ADVERSARIAL TORN WRITE TESTS PASSED SUCCESSFULLY! <<<\n\n";
        return 0;
    } else {
        std::cout << ">>> ADVERSARIAL STRESS DETECTED FAILURES! <<<\n\n";
        return 1;
    }
}
