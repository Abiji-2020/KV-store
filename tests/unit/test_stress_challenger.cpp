#include "kvstore/kvstore.hpp"
#include "kvstore/lru_shard.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// ============================================================================
// Adversarial Stress Test Framework
// ============================================================================

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::ostringstream oss; \
        oss << "Assertion failed: (" #cond ") at " << __FILE__ << ":" << __LINE__; \
        throw std::runtime_error(oss.str()); \
    } \
} while(0)

#define ASSERT_FALSE(cond) do { \
    if (cond) { \
        std::ostringstream oss; \
        oss << "Assertion failed (expected false): (" #cond ") at " << __FILE__ << ":" << __LINE__; \
        throw std::runtime_error(oss.str()); \
    } \
} while(0)

template <typename T, typename U>
inline bool test_equal(const T& a, const U& b) {
    if constexpr (std::is_integral_v<T> && std::is_integral_v<U> &&
                  !std::is_same_v<T, char> && !std::is_same_v<U, char> &&
                  !std::is_same_v<T, bool> && !std::is_same_v<U, bool>) {
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
        throw std::runtime_error(oss.str()); \
    } \
} while(0)

// ----------------------------------------------------------------------------
// Test 1: Isolated LruShard with 16 concurrent threads under capacity pressure
// ----------------------------------------------------------------------------
void test_isolated_shard_stress() {
    std::cout << "--- [Test 1] Isolated LruShard Concurrency (16 threads, capacity=64) ---" << std::endl;
    const size_t num_threads = 16;
    const size_t ops_per_thread = 8000;
    const size_t shard_capacity = 64;

    kvstore::LruShard shard(shard_capacity);
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> errors{0};
    std::vector<std::thread> workers;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(100 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                try {
                    int op = rng() % 100;
                    std::string key = "key_" + std::to_string(rng() % 256);
                    if (op < 45) { // 45% SET
                        std::string val = "val_" + std::to_string(tid) + "_" + std::to_string(i);
                        shard.set(key, val);
                    } else if (op < 80) { // 35% GET
                        shard.get(key);
                    } else if (op < 95) { // 15% DEL
                        shard.del(key);
                    } else { // 5% exists
                        shard.exists(key);
                    }
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(shard.size() <= shard_capacity);
    auto stats = shard.get_stats();
    ASSERT_EQ(stats.key_count, shard.size());
    ASSERT_TRUE(stats.hits + stats.misses > 0);
    ASSERT_TRUE(stats.evictions > 0);
    std::cout << "  Passed: " << (num_threads * ops_per_thread) << " ops executed, final size="
              << shard.size() << ", evictions=" << stats.evictions
              << ", hits=" << stats.hits << ", misses=" << stats.misses << std::endl;
}

// ----------------------------------------------------------------------------
// Test 2: KVStore 32 concurrent threads executing mixed operations
// ----------------------------------------------------------------------------
void test_kvstore_32_threads_mixed() {
    std::cout << "--- [Test 2] KVStore 32 Concurrent Threads (Capacity=128, 32 shards) ---" << std::endl;
    const size_t num_threads = 32;
    const size_t ops_per_thread = 10000; // 320,000 total operations
    const size_t total_capacity = 128;

    kvstore::KVStore store(total_capacity, 32);
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> errors{0};
    std::vector<std::thread> workers;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(1000 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                try {
                    int op = rng() % 100;
                    std::string key = "user_" + std::to_string(rng() % 1000);
                    if (op < 40) { // 40% SET
                        std::string val = "data_" + std::to_string(tid) + "_" + std::to_string(i);
                        store.set(key, val);
                    } else if (op < 80) { // 40% GET
                        store.get(key);
                    } else if (op < 95) { // 15% DEL
                        store.del(key);
                    } else { // 5% exists
                        store.exists(key);
                    }
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(store.size() <= total_capacity);
    auto stats = store.get_stats();
    ASSERT_EQ(stats.key_count, store.size());
    ASSERT_TRUE(stats.hits + stats.misses > 0);
    ASSERT_TRUE(stats.evictions > 0);
    std::cout << "  Passed: " << (num_threads * ops_per_thread) << " ops executed, final size="
              << store.size() << ", evictions=" << stats.evictions
              << ", hits=" << stats.hits << ", misses=" << stats.misses << std::endl;
}

// ----------------------------------------------------------------------------
// Test 3: Extreme Hotspot Contention (32 threads hammering the same key)
// ----------------------------------------------------------------------------
void test_hotspot_single_key_32_threads() {
    std::cout << "--- [Test 3] Extreme Hotspot Contention (32 threads on single key) ---" << std::endl;
    const size_t num_threads = 32;
    const size_t ops_per_thread = 8000;
    const std::string hot_key = "hotspot_key_prime";

    kvstore::KVStore store;
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> valid_reads{0};
    std::vector<std::thread> workers;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(5000 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                try {
                    int op = rng() % 10;
                    if (op < 4) { // 40% SET
                        std::string val = "v:" + std::to_string(tid) + ":" + std::to_string(i);
                        store.set(hot_key, val);
                    } else if (op < 8) { // 40% GET
                        auto res = store.get(hot_key);
                        if (res.has_value()) {
                            const std::string& v = res.value();
                            if (v.rfind("v:", 0) != 0) {
                                errors.fetch_add(1, std::memory_order_relaxed);
                            } else {
                                valid_reads.fetch_add(1, std::memory_order_relaxed);
                            }
                        }
                    } else { // 20% DEL
                        store.del(hot_key);
                    }
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(valid_reads.load() > 0);
    std::cout << "  Passed: " << (num_threads * ops_per_thread) << " hotspot ops, valid reads="
              << valid_reads.load() << ", zero data corruption." << std::endl;
}

// ----------------------------------------------------------------------------
// Test 4: Monotonic Read Consistency / Linearizability
// ----------------------------------------------------------------------------
void test_monotonic_read_consistency() {
    std::cout << "--- [Test 4] Monotonic Read Consistency & Linearizability (1 Writer, 15 Readers) ---" << std::endl;
    const size_t num_readers = 15;
    const uint64_t max_seq = 40000;
    const std::string seq_key = "monotonic_seq_key";

    kvstore::KVStore store;
    std::atomic<bool> start_signal{false};
    std::atomic<bool> writer_done{false};
    std::atomic<size_t> monotonicity_violations{0};
    std::atomic<uint64_t> total_reads{0};

    // 1 Dedicated Writer
    std::thread writer([&]() {
        while (!start_signal.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
        }
        for (uint64_t seq = 1; seq <= max_seq; ++seq) {
            store.set(seq_key, std::to_string(seq));
        }
        writer_done.store(true, std::memory_order_release);
    });

    // 15 Dedicated Readers
    std::vector<std::thread> readers;
    for (size_t r = 0; r < num_readers; ++r) {
        readers.emplace_back([&]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            uint64_t last_seen = 0;
            while (!writer_done.load(std::memory_order_acquire)) {
                auto opt = store.get(seq_key);
                if (opt.has_value()) {
                    uint64_t curr = std::stoull(opt.value());
                    if (curr < last_seen) {
                        monotonicity_violations.fetch_add(1, std::memory_order_relaxed);
                    }
                    last_seen = curr;
                    total_reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
            // Final drain read
            auto opt = store.get(seq_key);
            if (opt.has_value()) {
                uint64_t curr = std::stoull(opt.value());
                if (curr < last_seen) {
                    monotonicity_violations.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    writer.join();
    for (auto& r : readers) {
        r.join();
    }

    ASSERT_EQ(monotonicity_violations.load(), 0);
    ASSERT_TRUE(total_reads.load() > 0);
    auto final_val = store.get(seq_key);
    ASSERT_TRUE(final_val.has_value());
    ASSERT_EQ(std::stoull(final_val.value()), max_seq);
    std::cout << "  Passed: " << total_reads.load() << " consistent reads observed, 0 monotonicity violations." << std::endl;
}

// ----------------------------------------------------------------------------
// Test 5: Deadlock Freedom Under Interleaved Clear, Size, Stats, & Writes
// ----------------------------------------------------------------------------
void test_interleaved_deadlock_freedom() {
    std::cout << "--- [Test 5] Deadlock Freedom with Interleaved Clear & Stats (24 mutators, 4 clearers, 4 reporters) ---" << std::endl;
    kvstore::KVStore store(256, 32);

    std::atomic<bool> running{true};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> clear_count{0};
    std::atomic<size_t> stats_count{0};
    std::atomic<size_t> mut_count{0};

    // 24 Mutator workers
    std::vector<std::thread> mutators;
    for (size_t tid = 0; tid < 24; ++tid) {
        mutators.emplace_back([&, tid]() {
            std::mt19937 rng(9999 + static_cast<unsigned int>(tid));
            while (running.load(std::memory_order_relaxed)) {
                try {
                    int k = rng() % 500;
                    std::string key = "item_" + std::to_string(k);
                    int op = rng() % 3;
                    if (op == 0) {
                        store.set(key, "v_" + std::to_string(tid));
                    } else if (op == 1) {
                        store.get(key);
                    } else {
                        store.del(key);
                    }
                    mut_count.fetch_add(1, std::memory_order_relaxed);
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // 4 Clearers
    std::vector<std::thread> clearers;
    for (size_t tid = 0; tid < 4; ++tid) {
        clearers.emplace_back([&]() {
            while (running.load(std::memory_order_relaxed)) {
                try {
                    store.clear();
                    clear_count.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // 4 Stats and Size inspectors
    std::vector<std::thread> inspectors;
    for (size_t tid = 0; tid < 4; ++tid) {
        inspectors.emplace_back([&]() {
            while (running.load(std::memory_order_relaxed)) {
                try {
                    auto s = store.size();
                    auto st = store.get_stats();
                    (void)s;
                    (void)st;
                    stats_count.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // Run concurrency storm for 1.5 seconds
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    running.store(false, std::memory_order_release);

    for (auto& t : mutators) t.join();
    for (auto& t : clearers) t.join();
    for (auto& t : inspectors) t.join();

    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(clear_count.load() > 0);
    ASSERT_TRUE(stats_count.load() > 0);
    ASSERT_TRUE(mut_count.load() > 10000);
    std::cout << "  Passed: Mutator ops=" << mut_count.load()
              << ", Clear ops=" << clear_count.load()
              << ", Stats/Size snapshots=" << stats_count.load()
              << ", Deadlocks detected: 0" << std::endl;
}

// ----------------------------------------------------------------------------
// Test 6: Capacity Boundary Invariants (Capacity=1 and Capacity=0 Unbounded)
// ----------------------------------------------------------------------------
void test_capacity_boundary_invariants() {
    std::cout << "--- [Test 6] Capacity Boundary Invariants (Total Capacity 1 & 0) ---" << std::endl;

    // Subtest A: Total Capacity = 1 under 16 concurrent writers
    {
        kvstore::KVStore store(1);
        ASSERT_EQ(store.capacity(), 1);
        ASSERT_EQ(store.num_shards(), 1);

        const size_t num_threads = 16;
        const size_t ops_per_thread = 2000;
        std::atomic<bool> start_signal{false};
        std::atomic<size_t> errors{0};
        std::vector<std::thread> workers;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            workers.emplace_back([&, tid]() {
                while (!start_signal.load(std::memory_order_relaxed)) {
                    std::this_thread::yield();
                }
                for (size_t i = 0; i < ops_per_thread; ++i) {
                    try {
                        std::string k = "t" + std::to_string(tid) + "_k" + std::to_string(i);
                        store.set(k, "val");
                        size_t sz = store.size();
                        if (sz > 1) {
                            errors.fetch_add(1, std::memory_order_relaxed);
                        }
                    } catch (...) {
                        errors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        start_signal.store(true, std::memory_order_release);
        for (auto& w : workers) w.join();

        ASSERT_EQ(errors.load(), 0);
        ASSERT_EQ(store.size(), 1);
        auto stats = store.get_stats();
        ASSERT_EQ(stats.key_count, 1);
        ASSERT_EQ(stats.evictions, (num_threads * ops_per_thread) - 1);
        std::cout << "  Subtest A passed: Capacity=1 maintained perfectly, evictions="
                  << stats.evictions << std::endl;
    }

    // Subtest B: Total Capacity = 0 (Unbounded) under 16 concurrent writers
    {
        kvstore::KVStore store(0, 32);
        ASSERT_EQ(store.capacity(), 0);

        const size_t num_threads = 16;
        const size_t keys_per_thread = 1000;
        std::atomic<bool> start_signal{false};
        std::vector<std::thread> workers;

        for (size_t tid = 0; tid < num_threads; ++tid) {
            workers.emplace_back([&, tid]() {
                while (!start_signal.load(std::memory_order_relaxed)) {
                    std::this_thread::yield();
                }
                for (size_t i = 0; i < keys_per_thread; ++i) {
                    std::string k = "unbound_" + std::to_string(tid) + "_" + std::to_string(i);
                    store.set(k, "payload");
                }
            });
        }

        start_signal.store(true, std::memory_order_release);
        for (auto& w : workers) w.join();

        ASSERT_EQ(store.size(), num_threads * keys_per_thread);
        auto stats = store.get_stats();
        ASSERT_EQ(stats.evictions, 0);
        ASSERT_EQ(stats.key_count, num_threads * keys_per_thread);
        std::cout << "  Subtest B passed: Unbounded store size=" << store.size()
                  << ", evictions=0" << std::endl;
    }
}

// ----------------------------------------------------------------------------
// Test 7: Move Semantics Concurrency with Large Payloads
// ----------------------------------------------------------------------------
void test_move_semantics_concurrency() {
    std::cout << "--- [Test 7] Move Semantics Concurrency (16 threads, large payloads) ---" << std::endl;
    kvstore::KVStore store;
    const size_t num_threads = 16;
    const size_t ops_per_thread = 1000;
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> errors{0};
    std::vector<std::thread> workers;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::string payload_template(2048, static_cast<char>('A' + (tid % 26)));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                try {
                    std::string key = "move_key_" + std::to_string(tid) + "_" + std::to_string(i);
                    std::string value = payload_template + "_" + std::to_string(i);
                    // Explicit rvalue move
                    store.set(std::move(key), std::move(value));
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) w.join();

    ASSERT_EQ(errors.load(), 0);
    ASSERT_EQ(store.size(), num_threads * ops_per_thread);

    // Verify content of spot-checked keys
    for (size_t tid = 0; tid < num_threads; ++tid) {
        for (size_t i = 0; i < ops_per_thread; i += 200) {
            std::string key = "move_key_" + std::to_string(tid) + "_" + std::to_string(i);
            auto val = store.get(key);
            ASSERT_TRUE(val.has_value());
            ASSERT_TRUE(val.value().size() > 2048);
            ASSERT_EQ(val.value().back(), std::to_string(i).back());
        }
    }
    std::cout << "  Passed: Move semantics under concurrent load verified cleanly." << std::endl;
}

// ----------------------------------------------------------------------------
// Test 8: Mathematical Invariant Oracle on LruShard (32 threads, capacity=75)
// ----------------------------------------------------------------------------
void test_mathematical_oracle_shard_invariants() {
    std::cout << "--- [Test 8] Mathematical Invariant Oracle on LruShard (32 threads) ---" << std::endl;
    const size_t num_threads = 32;
    const size_t ops_per_thread = 5000;
    const size_t capacity = 75;

    kvstore::LruShard shard(capacity);
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> total_created{0};
    std::atomic<size_t> total_updated{0};
    std::atomic<size_t> total_evicted{0};
    std::atomic<size_t> total_deleted{0};
    std::atomic<size_t> total_hits{0};
    std::atomic<size_t> total_misses{0};

    std::vector<std::thread> workers;
    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(4242 + static_cast<unsigned int>(tid));
            size_t c = 0, u = 0, e = 0, d = 0, h = 0, m = 0;
            for (size_t i = 0; i < ops_per_thread; ++i) {
                int op = rng() % 100;
                std::string k = "oracle_k_" + std::to_string(rng() % 300);
                if (op < 50) { // SET
                    auto res = shard.set(k, "val_" + std::to_string(i));
                    if (res == kvstore::SetResult::CREATED) ++c;
                    else if (res == kvstore::SetResult::UPDATED) ++u;
                    else if (res == kvstore::SetResult::EVICTED) ++e;
                } else if (op < 80) { // GET
                    auto res = shard.get(k);
                    if (res.has_value()) ++h;
                    else ++m;
                } else { // DEL
                    auto res = shard.del(k);
                    if (res == kvstore::DelResult::DELETED) ++d;
                }
            }
            total_created.fetch_add(c, std::memory_order_relaxed);
            total_updated.fetch_add(u, std::memory_order_relaxed);
            total_evicted.fetch_add(e, std::memory_order_relaxed);
            total_deleted.fetch_add(d, std::memory_order_relaxed);
            total_hits.fetch_add(h, std::memory_order_relaxed);
            total_misses.fetch_add(m, std::memory_order_relaxed);
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) w.join();

    auto stats = shard.get_stats();
    size_t expected_size = total_created.load() - total_deleted.load();

    ASSERT_EQ(shard.size(), expected_size);
    ASSERT_EQ(stats.key_count, expected_size);
    ASSERT_TRUE(shard.size() <= capacity);
    ASSERT_EQ(stats.evictions, total_evicted.load());
    ASSERT_EQ(stats.hits, total_hits.load());
    ASSERT_EQ(stats.misses, total_misses.load());

    // Structural validation of LRU list and table
    auto entries = shard.get_entries_mru_order();
    ASSERT_EQ(entries.size(), shard.size());
    std::unordered_map<std::string, std::string> seen;
    for (const auto& [k, v] : entries) {
        ASSERT_FALSE(seen.contains(k)); // Unique keys in LRU list
        seen[k] = v;
        auto val = shard.get(k);
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(val.value(), v);
    }

    std::cout << "  Passed: Invariant verified: size(" << shard.size()
              << ") == created(" << total_created.load()
              << ") - deleted(" << total_deleted.load()
              << "), evictions=" << stats.evictions
              << ", hits=" << stats.hits
              << ", misses=" << stats.misses << std::endl;
}

// ----------------------------------------------------------------------------
// Test 9: Mathematical Invariant Oracle on KVStore (32 threads, capacity=160)
// ----------------------------------------------------------------------------
void test_mathematical_oracle_kvstore_invariants() {
    std::cout << "--- [Test 9] Mathematical Invariant Oracle on KVStore (32 threads) ---" << std::endl;
    const size_t num_threads = 32;
    const size_t ops_per_thread = 5000;
    const size_t total_capacity = 160;

    kvstore::KVStore store(total_capacity, 32);
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> total_created{0};
    std::atomic<size_t> total_updated{0};
    std::atomic<size_t> total_evicted{0};
    std::atomic<size_t> total_deleted{0};
    std::atomic<size_t> total_hits{0};
    std::atomic<size_t> total_misses{0};

    std::vector<std::thread> workers;
    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(8888 + static_cast<unsigned int>(tid));
            size_t c = 0, u = 0, e = 0, d = 0, h = 0, m = 0;
            for (size_t i = 0; i < ops_per_thread; ++i) {
                int op = rng() % 100;
                std::string k = "kv_k_" + std::to_string(rng() % 600);
                if (op < 50) { // SET
                    auto res = store.set(k, "v_" + std::to_string(i));
                    if (res == kvstore::SetResult::CREATED) ++c;
                    else if (res == kvstore::SetResult::UPDATED) ++u;
                    else if (res == kvstore::SetResult::EVICTED) ++e;
                } else if (op < 80) { // GET
                    auto res = store.get(k);
                    if (res.has_value()) ++h;
                    else ++m;
                } else { // DEL
                    auto res = store.del(k);
                    if (res == kvstore::DelResult::DELETED) ++d;
                }
            }
            total_created.fetch_add(c, std::memory_order_relaxed);
            total_updated.fetch_add(u, std::memory_order_relaxed);
            total_evicted.fetch_add(e, std::memory_order_relaxed);
            total_deleted.fetch_add(d, std::memory_order_relaxed);
            total_hits.fetch_add(h, std::memory_order_relaxed);
            total_misses.fetch_add(m, std::memory_order_relaxed);
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) w.join();

    auto stats = store.get_stats();
    size_t expected_size = total_created.load() - total_deleted.load();

    ASSERT_EQ(store.size(), expected_size);
    ASSERT_EQ(stats.key_count, expected_size);
    ASSERT_TRUE(store.size() <= total_capacity);
    ASSERT_EQ(stats.evictions, total_evicted.load());
    ASSERT_EQ(stats.hits, total_hits.load());
    ASSERT_EQ(stats.misses, total_misses.load());

    std::cout << "  Passed: KVStore Invariant verified: size(" << store.size()
              << ") == created(" << total_created.load()
              << ") - deleted(" << total_deleted.load()
              << "), evictions=" << stats.evictions
              << ", hits=" << stats.hits
              << ", misses=" << stats.misses << std::endl;
}

// ----------------------------------------------------------------------------
// Test 10: Oversubscribed 64-Thread Contention Storm
// ----------------------------------------------------------------------------
void test_oversubscribed_thread_contention_64_threads() {
    std::cout << "--- [Test 10] Oversubscribed 64 Concurrent Threads Contention Storm ---" << std::endl;
    const size_t num_threads = 64;
    const size_t ops_per_thread = 5000; // 320,000 total ops
    kvstore::KVStore store(512, 32);

    std::atomic<bool> start_signal{false};
    std::atomic<size_t> errors{0};
    std::vector<std::thread> workers;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(7777 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                try {
                    int op = rng() % 100;
                    std::string key = "oversub_" + std::to_string(rng() % 200);
                    if (op < 45) {
                        store.set(key, "data_" + std::to_string(tid) + "_" + std::to_string(i));
                    } else if (op < 85) {
                        store.get(key);
                    } else if (op < 98) {
                        store.del(key);
                    } else {
                        store.get_stats();
                    }
                } catch (...) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) w.join();

    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(store.size() <= 512);
    auto stats = store.get_stats();
    ASSERT_EQ(stats.key_count, store.size());
    std::cout << "  Passed: 64 oversubscribed threads executed " << (num_threads * ops_per_thread)
              << " operations with 0 errors and perfect capacity bounding." << std::endl;
}

// ============================================================================
// Main Runner
// ============================================================================
int main() {
    std::cout << "\n=======================================================\n";
    std::cout << "  KVStore Milestone 1 Adversarial Concurrency Stress Suite\n";
    std::cout << "=======================================================\n\n";

    auto t_start = std::chrono::steady_clock::now();
    try {
        test_isolated_shard_stress();
        test_kvstore_32_threads_mixed();
        test_hotspot_single_key_32_threads();
        test_monotonic_read_consistency();
        test_interleaved_deadlock_freedom();
        test_capacity_boundary_invariants();
        test_move_semantics_concurrency();
        test_mathematical_oracle_shard_invariants();
        test_mathematical_oracle_kvstore_invariants();
        test_oversubscribed_thread_contention_64_threads();
    } catch (const std::exception& e) {
        std::cerr << "\n[STRESS TEST FAILED]: " << e.what() << std::endl;
        return 1;
    }

    auto t_end = std::chrono::steady_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();

    std::cout << "\n-------------------------------------------------------\n";
    std::cout << "Adversarial Stress Suite Elapsed Time: " << total_ms << " ms\n";
    std::cout << ">>> ALL ADVERSARIAL STRESS TESTS PASSED SUCCESSFULLY! <<<\n";
    std::cout << "-------------------------------------------------------\n\n";
    return 0;
}

