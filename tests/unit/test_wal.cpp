#include "kvstore/kvstore.hpp"
#include "kvstore/wal.hpp"
#include "kvstore/crc32.hpp"

#include <atomic>
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
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

// ============================================================================
// Lightweight Zero-Dependency Test Framework
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
            path_ = "/tmp/" + prefix + "_fallback.wal";
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

private:
    std::string path_;
};

// ============================================================================
// Milestone 2 Test Cases
// ============================================================================

// 0. CRC32 Standard Vectors & Slicing Equivalence
TEST_CASE(crc32_standard_vectors_and_slicing) {
    ASSERT_EQ(kvstore::Crc32::compute(""), 0x00000000U);
    ASSERT_EQ(kvstore::Crc32::compute("123456789"), 0xCBF43926U);
    ASSERT_EQ(kvstore::Crc32::compute("hello"), 0x3610A686U);

    // Verify slicing-by-8 matches byte-by-byte for arbitrary payloads
    std::string test_payload = "The quick brown fox jumps over the lazy dog 0123456789!@#$%^&*()";
    uint32_t crc_slicing = kvstore::Crc32::compute(test_payload);
    uint32_t crc_byte = kvstore::Crc32::compute_byte_by_byte(test_payload.data(), test_payload.size());
    ASSERT_EQ(crc_slicing, crc_byte);
}

// 1. Clean Append and Replay Test
TEST_CASE(wal_clean_append_and_recover) {
    TempWalFile temp("test_wal_clean");
    
    // Phase 1: Write mutations through WalManager
    {
        kvstore::KVStore store;
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        
        ASSERT_TRUE(wal.append_set("key1", "val1"));
        store.set("key1", "val1");
        
        ASSERT_TRUE(wal.append_set("key2", "val2"));
        store.set("key2", "val2");
        
        ASSERT_TRUE(wal.append_del("key1"));
        store.del("key1");
        
        ASSERT_TRUE(wal.append_set("key3", "val3"));
        store.set("key3", "val3");
        
        wal.sync();
    }

    ASSERT_TRUE(temp.size() > 0);

    // Phase 2: Recover into a fresh store
    {
        kvstore::KVStore recovered_store;
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        
        size_t count = wal.recover(recovered_store);
        ASSERT_EQ(count, static_cast<size_t>(4));
        ASSERT_EQ(recovered_store.size(), static_cast<size_t>(2));
        ASSERT_FALSE(recovered_store.get("key1").has_value());
        ASSERT_EQ(recovered_store.get("key2").value(), "val2");
        ASSERT_EQ(recovered_store.get("key3").value(), "val3");
    }
}

// 2. Clear Operation Replay Test
TEST_CASE(wal_clear_operation_recovery) {
    TempWalFile temp("test_wal_clear");
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("alpha", "100");
        wal.append_set("beta", "200");
        wal.append_clear();
        wal.append_set("gamma", "300");
    }

    kvstore::KVStore store;
    kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
    size_t count = wal.recover(store);

    ASSERT_EQ(count, static_cast<size_t>(4));
    ASSERT_EQ(store.size(), static_cast<size_t>(1));
    ASSERT_FALSE(store.get("alpha").has_value());
    ASSERT_FALSE(store.get("beta").has_value());
    ASSERT_EQ(store.get("gamma").value(), "300");
}

// 3. Replay Idempotency & State Equivalence Test
TEST_CASE(wal_replay_idempotency_and_state_equivalence) {
    TempWalFile temp("test_wal_idempotency");
    
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        for (int i = 0; i < 50; ++i) {
            wal.append_set("k_" + std::to_string(i), "v_" + std::to_string(i));
        }
        for (int i = 0; i < 20; ++i) {
            wal.append_del("k_" + std::to_string(i * 2));
        }
    }

    kvstore::KVStore store_A;
    kvstore::KVStore store_B;

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t count_A = wal.recover(store_A);
        ASSERT_EQ(count_A, static_cast<size_t>(70));
    }

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t count_B = wal.recover(store_B);
        ASSERT_EQ(count_B, static_cast<size_t>(70));
    }

    // Verify state equivalence
    ASSERT_EQ(store_A.size(), store_B.size());
    for (int i = 0; i < 50; ++i) {
        std::string key = "k_" + std::to_string(i);
        ASSERT_EQ(store_A.get(key), store_B.get(key));
    }
}

// 4. Torn Write Simulation: Truncated Header at Tail
TEST_CASE(wal_torn_write_truncated_header) {
    TempWalFile temp("test_wal_torn_hdr");
    size_t valid_size = 0;

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        for (int i = 0; i < 5; ++i) {
            wal.append_set("key_" + std::to_string(i), "val_" + std::to_string(i));
        }
    }
    valid_size = temp.size();
    ASSERT_TRUE(valid_size > 0);

    // Simulate crash mid-write: append 7 bytes (incomplete 20-byte header)
    {
        int fd = ::open(temp.path().c_str(), O_WRONLY | O_APPEND);
        ASSERT_TRUE(fd >= 0);
        uint8_t garbage[7] = {0x57, 0x4C, 0x01, 0x00, 0xAA, 0xBB, 0xCC};
        ASSERT_EQ(::write(fd, garbage, sizeof(garbage)), static_cast<ssize_t>(sizeof(garbage)));
        ::close(fd);
    }

    ASSERT_EQ(temp.size(), valid_size + 7);

    // Recover and verify tail is truncated to valid_size
    {
        kvstore::KVStore store;
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(5));
        ASSERT_EQ(store.size(), static_cast<size_t>(5));
    }

    ASSERT_EQ(temp.size(), valid_size);

    // Verify subsequent writes proceed cleanly after repair
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        ASSERT_TRUE(wal.append_set("after_torn", "new_val"));
    }

    {
        kvstore::KVStore store;
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(6));
        ASSERT_EQ(store.get("after_torn").value(), "new_val");
    }
}

// 5. Torn Write Simulation: Truncated Payload at Tail
TEST_CASE(wal_torn_write_truncated_payload) {
    TempWalFile temp("test_wal_torn_payload");
    size_t valid_size = 0;

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        for (int i = 0; i < 5; ++i) {
            wal.append_set("k" + std::to_string(i), "v" + std::to_string(i));
        }
    }
    valid_size = temp.size();

    // Append complete 20B header claiming key_len=10, val_len=20, but write only 4 bytes of payload
    {
        int fd = ::open(temp.path().c_str(), O_WRONLY | O_APPEND);
        ASSERT_TRUE(fd >= 0);
        uint8_t torn_rec[24];
        std::memset(torn_rec, 0, sizeof(torn_rec));
        torn_rec[0] = 0x57; torn_rec[1] = 0x4C; // Magic
        torn_rec[2] = 0x01;                     // OP_SET
        torn_rec[15] = 10;                      // key_len = 10 (big endian)
        torn_rec[19] = 20;                      // val_len = 20 (big endian)
        // 4 bytes payload
        torn_rec[20] = 'k'; torn_rec[21] = 'e'; torn_rec[22] = 'y'; torn_rec[23] = '_';
        ASSERT_EQ(::write(fd, torn_rec, sizeof(torn_rec)), static_cast<ssize_t>(sizeof(torn_rec)));
        ::close(fd);
    }

    ASSERT_TRUE(temp.size() > valid_size);

    // Recover: should truncate back to valid_size
    {
        kvstore::KVStore store;
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(5));
        ASSERT_EQ(store.size(), static_cast<size_t>(5));
    }

    ASSERT_EQ(temp.size(), valid_size);
}

// 6. Torn Write Simulation: Corrupted CRC32 at Tail
TEST_CASE(wal_corrupted_crc32_at_tail) {
    TempWalFile temp("test_wal_bad_crc");
    size_t valid_size = 0;

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("valid1", "data1");
        wal.append_set("valid2", "data2");
    }
    valid_size = temp.size();

    // Append 3rd record normally
    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        wal.append_set("corrupt_tail", "bad_crc");
    }

    // Corrupt the last 2 bytes of the CRC32 in the file
    {
        int fd = ::open(temp.path().c_str(), O_RDWR);
        ASSERT_TRUE(fd >= 0);
        ::lseek(fd, -2, SEEK_END);
        uint8_t flip[2] = {0xFF, 0xFF};
        ASSERT_EQ(::write(fd, flip, 2), 2);
        ::close(fd);
    }

    // Recover: 3rd record CRC mismatch -> truncated to valid_size
    {
        kvstore::KVStore store;
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(2));
        ASSERT_EQ(store.size(), static_cast<size_t>(2));
        ASSERT_EQ(store.get("valid1").value(), "data1");
        ASSERT_EQ(store.get("valid2").value(), "data2");
    }

    ASSERT_EQ(temp.size(), valid_size);
}

// 7. Recovery from Empty File
TEST_CASE(wal_empty_file_recovery) {
    TempWalFile temp("test_wal_empty");
    ASSERT_EQ(temp.size(), static_cast<size_t>(0));

    kvstore::KVStore store;
    kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
    size_t recovered = wal.recover(store);

    ASSERT_EQ(recovered, static_cast<size_t>(0));
    ASSERT_EQ(store.size(), static_cast<size_t>(0));

    // Verify writing to empty WAL works
    ASSERT_TRUE(wal.append_set("initial", "entry"));
    ASSERT_TRUE(temp.size() > 0);
}

// 8. Recovery from Non-Existent Path
TEST_CASE(wal_nonexistent_file_recovery) {
    std::string missing_path = "/tmp/test_wal_nonexistent_" + std::to_string(rand()) + ".wal";
    ::unlink(missing_path.c_str());

    {
        kvstore::KVStore store;
        kvstore::WalManager wal(missing_path, kvstore::SyncMode::SYNC_ALWAYS);
        size_t recovered = wal.recover(store);
        ASSERT_EQ(recovered, static_cast<size_t>(0));
        ASSERT_EQ(store.size(), static_cast<size_t>(0));
        ASSERT_TRUE(wal.append_set("k", "v"));
    }

    struct stat st;
    ASSERT_EQ(::stat(missing_path.c_str(), &st), 0);
    ::unlink(missing_path.c_str());
}

// 9. Recovery into Capacity-Bounded Store Respects Eviction Limit
TEST_CASE(wal_recovery_into_capacity_bounded_store) {
    TempWalFile temp("test_wal_cap_bound");

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
        for (int i = 0; i < 20; ++i) {
            wal.append_set("k_" + std::to_string(i), "v_" + std::to_string(i));
        }
    }

    // Recover into store with capacity = 5
    kvstore::KVStore bounded_store(5);
    kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
    size_t count = wal.recover(bounded_store);

    ASSERT_EQ(count, static_cast<size_t>(20));
    ASSERT_EQ(bounded_store.size(), static_cast<size_t>(5));
}

// 10. Multi-Threaded Concurrent Appends + Crash Recovery Stress Test
TEST_CASE(wal_concurrent_appends_and_recovery_stress) {
    TempWalFile temp("test_wal_concurrent");
    constexpr int NUM_THREADS = 8;
    constexpr int OPS_PER_THREAD = 1250; // Total 10,000 operations
    std::atomic<bool> start_flag{false};

    {
        kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_BUFFERED);
        std::vector<std::thread> workers;

        for (int t = 0; t < NUM_THREADS; ++t) {
            workers.emplace_back([t, &wal, &start_flag]() {
                while (!start_flag.load()) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    std::string key = "th_" + std::to_string(t) + "_k_" + std::to_string(i);
                    std::string val = "th_" + std::to_string(t) + "_v_" + std::to_string(i);
                    wal.append_set(key, val);
                }
            });
        }

        start_flag.store(true);
        for (auto& w : workers) {
            w.join();
        }
        wal.sync();
    }

    // Replay all 10,000 operations into a fresh KVStore
    kvstore::KVStore recovered_store;
    kvstore::WalManager wal(temp.path(), kvstore::SyncMode::SYNC_ALWAYS);
    size_t recovered = wal.recover(recovered_store);

    ASSERT_EQ(recovered, static_cast<size_t>(NUM_THREADS * OPS_PER_THREAD));
    ASSERT_EQ(recovered_store.size(), static_cast<size_t>(NUM_THREADS * OPS_PER_THREAD));
}

// 11. Sync Modes Durability Equivalence
TEST_CASE(wal_sync_modes_durability_equivalence) {
    TempWalFile f_always("test_sync_always");
    TempWalFile f_batch("test_sync_batch");
    TempWalFile f_buffered("test_sync_buffered");

    {
        kvstore::WalManager w1(f_always.path(), kvstore::SyncMode::SYNC_ALWAYS);
        kvstore::WalManager w2(f_batch.path(), kvstore::SyncMode::SYNC_BATCH);
        kvstore::WalManager w3(f_buffered.path(), kvstore::SyncMode::SYNC_BUFFERED);

        for (int i = 0; i < 30; ++i) {
            std::string k = "key_" + std::to_string(i);
            std::string v = "val_" + std::to_string(i);
            w1.append_set(k, v);
            w2.append_set(k, v);
            w3.append_set(k, v);
        }
        w2.sync();
        w3.sync();
    }

    ASSERT_EQ(f_always.size(), f_batch.size());
    ASSERT_EQ(f_batch.size(), f_buffered.size());

    kvstore::KVStore s1, s2, s3;
    kvstore::WalManager r1(f_always.path());
    kvstore::WalManager r2(f_batch.path());
    kvstore::WalManager r3(f_buffered.path());

    ASSERT_EQ(r1.recover(s1), static_cast<size_t>(30));
    ASSERT_EQ(r2.recover(s2), static_cast<size_t>(30));
    ASSERT_EQ(r3.recover(s3), static_cast<size_t>(30));
}

// ============================================================================
// Main Runner
// ============================================================================

int main() {
    std::cout << "\n=======================================================\n";
    std::cout << "  KVStore Milestone 2 WAL & Crash Recovery Test Suite  \n";
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
    std::cout << "Total Tests : " << (passed + failed) << "\n";
    std::cout << "Passed      : " << passed << "\n";
    std::cout << "Failed      : " << failed << "\n";
    std::cout << "Elapsed Time: " << total_ms << " ms\n";
    std::cout << "-------------------------------------------------------\n";

    if (failed == 0) {
        std::cout << ">>> ALL MILESTONE 2 WAL TESTS PASSED SUCCESSFULLY! <<<\n\n";
        return 0;
    } else {
        std::cout << ">>> SOME TESTS FAILED! <<<\n\n";
        return 1;
    }
}
