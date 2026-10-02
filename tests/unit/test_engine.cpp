#include "kvstore/kvstore.hpp"
#include "kvstore/lru_shard.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
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

// ============================================================================
// Part 1: Basic Operations & Functional Correctness Tests
// ============================================================================

TEST_CASE(basic_set_and_get) {
    kvstore::KVStore store;
    ASSERT_EQ(store.size(), 0);
    ASSERT_FALSE(store.exists("user_100"));

    auto res = store.set("user_100", "Alice");
    ASSERT_EQ(res, kvstore::SetResult::CREATED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_TRUE(store.exists("user_100"));

    auto val = store.get("user_100");
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value(), "Alice");
}

TEST_CASE(get_nonexistent_key) {
    kvstore::KVStore store;
    auto val = store.get("missing_key");
    ASSERT_FALSE(val.has_value());
    ASSERT_FALSE(store.exists("missing_key"));

    auto stats = store.get_stats();
    ASSERT_EQ(stats.misses, 1);
    ASSERT_EQ(stats.hits, 0);
}

TEST_CASE(set_overwrite_existing_key) {
    kvstore::KVStore store;
    ASSERT_EQ(store.set("key1", "val1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_EQ(store.get("key1").value(), "val1");

    ASSERT_EQ(store.set("key1", "val2"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_EQ(store.get("key1").value(), "val2");

    ASSERT_EQ(store.set("key1", "val3"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_EQ(store.get("key1").value(), "val3");
}

TEST_CASE(del_existing_key) {
    kvstore::KVStore store;
    store.set("temp_key", "temporary_value");
    ASSERT_EQ(store.size(), 1);

    auto del_res = store.del("temp_key");
    ASSERT_EQ(del_res, kvstore::DelResult::DELETED);
    ASSERT_EQ(store.size(), 0);
    ASSERT_FALSE(store.exists("temp_key"));
    ASSERT_FALSE(store.get("temp_key").has_value());
}

TEST_CASE(del_nonexistent_key) {
    kvstore::KVStore store;
    auto del_res = store.del("never_existed");
    ASSERT_EQ(del_res, kvstore::DelResult::NOT_FOUND);
    ASSERT_EQ(store.size(), 0);
}

TEST_CASE(del_idempotence) {
    kvstore::KVStore store;
    store.set("k", "v");
    ASSERT_EQ(store.del("k"), kvstore::DelResult::DELETED);
    ASSERT_EQ(store.del("k"), kvstore::DelResult::NOT_FOUND);
    ASSERT_EQ(store.size(), 0);
}

TEST_CASE(empty_keys_and_values) {
    kvstore::KVStore store;
    // Empty key, valid value
    ASSERT_EQ(store.set("", "value_for_empty_key"), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists(""));
    ASSERT_EQ(store.get("").value(), "value_for_empty_key");

    // Valid key, empty value
    ASSERT_EQ(store.set("key_for_empty_value", ""), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists("key_for_empty_value"));
    ASSERT_EQ(store.get("key_for_empty_value").value(), "");

    // Empty key overwrite with empty value
    ASSERT_EQ(store.set("", ""), kvstore::SetResult::UPDATED);
    ASSERT_EQ(store.get("").value(), "");

    ASSERT_EQ(store.del(""), kvstore::DelResult::DELETED);
    ASSERT_FALSE(store.exists(""));
    ASSERT_EQ(store.del("key_for_empty_value"), kvstore::DelResult::DELETED);
    ASSERT_EQ(store.size(), 0);
}

TEST_CASE(binary_and_special_characters) {
    kvstore::KVStore store;
    std::string bin_key("key\0hidden\xFF", 12);
    std::string bin_val("data\r\n\t\0payload\x01\x02", 16);

    ASSERT_EQ(store.set(bin_key, bin_val), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists(bin_key));

    auto retrieved = store.get(bin_key);
    ASSERT_TRUE(retrieved.has_value());
    ASSERT_EQ(retrieved.value().size(), bin_val.size());
    ASSERT_EQ(retrieved.value(), bin_val);

    // UTF-8 string
    std::string utf8_key = "键名_🔑";
    std::string utf8_val = "数值_🚀_español";
    store.set(utf8_key, utf8_val);
    ASSERT_EQ(store.get(utf8_key).value(), utf8_val);

    // Large payload (64 KB)
    std::string large_val(65536, 'X');
    store.set("large_key", large_val);
    ASSERT_EQ(store.get("large_key").value(), large_val);
}

TEST_CASE(clear_operation) {
    kvstore::KVStore store;
    for (int i = 0; i < 100; ++i) {
        store.set("key_" + std::to_string(i), "val_" + std::to_string(i));
    }
    ASSERT_EQ(store.size(), 100);

    store.clear();
    ASSERT_EQ(store.size(), 0);

    for (int i = 0; i < 100; ++i) {
        ASSERT_FALSE(store.exists("key_" + std::to_string(i)));
        ASSERT_FALSE(store.get("key_" + std::to_string(i)).has_value());
    }

    // Able to insert after clear
    ASSERT_EQ(store.set("new_key", "new_val"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.size(), 1);
}

TEST_CASE(stats_telemetry) {
    kvstore::KVStore store;
    store.set("k1", "v1");
    store.set("k2", "v2");
    store.get("k1"); // hit
    store.get("k1"); // hit
    store.get("k3"); // miss
    store.get("k4"); // miss

    auto stats = store.get_stats();
    ASSERT_EQ(stats.key_count, 2);
    ASSERT_EQ(stats.hits, 2);
    ASSERT_EQ(stats.misses, 2);
    ASSERT_EQ(stats.evictions, 0);
}

// ============================================================================
// Part 2: Capacity-Bounded LRU Eviction Tests
// ============================================================================

TEST_CASE(lru_shard_strict_fifo_when_unread) {
    // Test isolated LruShard with capacity = 3
    kvstore::LruShard shard(3);
    ASSERT_EQ(shard.capacity(), 3);

    ASSERT_EQ(shard.set("k1", "v1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.set("k2", "v2"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.set("k3", "v3"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.size(), 3);

    // Inserting k4 must evict least recently used (k1)
    ASSERT_EQ(shard.set("k4", "v4"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_FALSE(shard.get("k1").has_value());
    ASSERT_EQ(shard.get("k2").value(), "v2");
    ASSERT_EQ(shard.get("k3").value(), "v3");
    ASSERT_EQ(shard.get("k4").value(), "v4");
    ASSERT_EQ(shard.get_stats().evictions, 1);
}

TEST_CASE(lru_shard_get_refreshes_recency) {
    kvstore::LruShard shard(3);
    shard.set("k1", "v1");
    shard.set("k2", "v2");
    shard.set("k3", "v3");
    // Recency list: [k3 (MRU), k2, k1 (LRU)]

    // Read k1: moves k1 to MRU!
    // Recency list becomes: [k1 (MRU), k3, k2 (LRU)]
    auto val = shard.get("k1");
    ASSERT_TRUE(val.has_value());

    // Insert k4: capacity 3 reached. LRU item (k2) must be evicted!
    ASSERT_EQ(shard.set("k4", "v4"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 3);

    ASSERT_TRUE(shard.get("k1").has_value());  // k1 was spared!
    ASSERT_FALSE(shard.get("k2").has_value()); // k2 was evicted!
    ASSERT_TRUE(shard.get("k3").has_value());
    ASSERT_TRUE(shard.get("k4").has_value());
}

TEST_CASE(lru_shard_set_update_refreshes_recency) {
    kvstore::LruShard shard(3);
    shard.set("k1", "v1");
    shard.set("k2", "v2");
    shard.set("k3", "v3");
    // Recency list: [k3, k2, k1]

    // Overwrite k1: moves k1 to MRU!
    // Recency list becomes: [k1, k3, k2]
    ASSERT_EQ(shard.set("k1", "v1_updated"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 3);

    // Insert k4: LRU item (k2) must be evicted!
    ASSERT_EQ(shard.set("k4", "v4"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 3);

    ASSERT_EQ(shard.get("k1").value(), "v1_updated");
    ASSERT_FALSE(shard.get("k2").has_value()); // k2 evicted
    ASSERT_TRUE(shard.get("k3").has_value());
    ASSERT_TRUE(shard.get("k4").has_value());
}

TEST_CASE(lru_shard_del_prevents_eviction) {
    kvstore::LruShard shard(3);
    shard.set("k1", "v1");
    shard.set("k2", "v2");
    shard.set("k3", "v3");
    ASSERT_EQ(shard.size(), 3);

    ASSERT_EQ(shard.del("k2"), kvstore::DelResult::DELETED);
    ASSERT_EQ(shard.size(), 2);

    // Inserting k4 now should NOT trigger eviction
    ASSERT_EQ(shard.set("k4", "v4"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 0);
}

TEST_CASE(lru_shard_capacity_one) {
    kvstore::LruShard shard(1);
    ASSERT_EQ(shard.set("k1", "v1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_EQ(shard.get("k1").value(), "v1");

    ASSERT_EQ(shard.set("k2", "v2"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_FALSE(shard.get("k1").has_value());
    ASSERT_EQ(shard.get("k2").value(), "v2");

    ASSERT_EQ(shard.set("k2", "v2_updated"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_EQ(shard.get("k2").value(), "v2_updated");
}

TEST_CASE(lru_shard_capacity_zero_unbounded) {
    kvstore::LruShard shard(0); // 0 means unbounded
    for (int i = 0; i < 500; ++i) {
        shard.set("key_" + std::to_string(i), "val_" + std::to_string(i));
    }
    ASSERT_EQ(shard.size(), 500);
    ASSERT_EQ(shard.get_stats().evictions, 0);
    for (int i = 0; i < 500; ++i) {
        ASSERT_TRUE(shard.get("key_" + std::to_string(i)).has_value());
    }
}

TEST_CASE(kvstore_total_capacity_bounding) {
    // 32 shards with capacity total = 64 (2 per shard)
    kvstore::KVStore store(64);
    for (int i = 0; i < 300; ++i) {
        store.set("item_" + std::to_string(i), "data_" + std::to_string(i));
    }
    // Total size cannot exceed capacity
    ASSERT_TRUE(store.size() <= 64);
    auto stats = store.get_stats();
    ASSERT_TRUE(stats.evictions > 0);
    ASSERT_EQ(stats.key_count, store.size());
    ASSERT_EQ(stats.key_count + stats.evictions, 300);
}

// ============================================================================
// Part 3: High-Concurrency Stress Test Harness
// ============================================================================

TEST_CASE(concurrency_disjoint_keys_throughput) {
    const size_t num_threads = 8;
    const size_t ops_per_thread = 5000;
    kvstore::KVStore store;

    std::atomic<bool> start_signal{false};
    std::atomic<size_t> error_count{0};
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::string prefix = "t" + std::to_string(tid) + "_k";
            try {
                // 1. Insert phase
                for (size_t i = 0; i < ops_per_thread; ++i) {
                    store.set(prefix + std::to_string(i), "val_" + std::to_string(i));
                }
                // 2. Read phase
                for (size_t i = 0; i < ops_per_thread; ++i) {
                    auto res = store.get(prefix + std::to_string(i));
                    if (!res.has_value() || res.value() != ("val_" + std::to_string(i))) {
                        error_count.fetch_add(1);
                    }
                }
                // 3. Update half phase
                for (size_t i = 0; i < ops_per_thread / 2; ++i) {
                    store.set(prefix + std::to_string(i), "new_" + std::to_string(i));
                }
                // 4. Delete quarter phase
                for (size_t i = 0; i < ops_per_thread / 4; ++i) {
                    store.del(prefix + std::to_string(i));
                }
            } catch (...) {
                error_count.fetch_add(1);
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(error_count.load(), 0);
    size_t expected_size_per_thread = ops_per_thread - (ops_per_thread / 4);
    ASSERT_EQ(store.size(), num_threads * expected_size_per_thread);
}

TEST_CASE(concurrency_hot_spot_contention) {
    const size_t num_threads = 8;
    const size_t ops_per_thread = 10000;
    kvstore::KVStore store;

    const std::vector<std::string> hot_keys = {"hot_0", "hot_1", "hot_2", "hot_3"};
    std::atomic<bool> start_signal{false};
    std::atomic<size_t> error_count{0};
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(1337 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                const auto& key = hot_keys[rng() % hot_keys.size()];
                int op = rng() % 10;
                try {
                    if (op < 4) { // 40% SET
                        std::string val = "tid:" + std::to_string(tid) + ":seq:" + std::to_string(i);
                        store.set(key, val);
                    } else if (op < 8) { // 40% GET
                        auto res = store.get(key);
                        if (res.has_value()) {
                            // Check format consistency
                            if (res.value().rfind("tid:", 0) != 0) {
                                error_count.fetch_add(1);
                            }
                        }
                    } else { // 20% DEL
                        store.del(key);
                    }
                } catch (...) {
                    error_count.fetch_add(1);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(error_count.load(), 0);
}

TEST_CASE(concurrency_eviction_churn) {
    const size_t num_writers = 4;
    const size_t num_readers = 4;
    const size_t ops_per_thread = 5000;
    // Bounded capacity = 128
    kvstore::KVStore store(128);

    std::atomic<bool> start_signal{false};
    std::atomic<size_t> error_count{0};
    std::vector<std::thread> threads;

    // Writers
    for (size_t tid = 0; tid < num_writers; ++tid) {
        threads.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(2000 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string key = "churn_key_" + std::to_string(rng() % 5000);
                std::string val = "data_" + std::to_string(i);
                try {
                    store.set(key, val);
                } catch (...) {
                    error_count.fetch_add(1);
                }
            }
        });
    }

    // Readers
    for (size_t tid = 0; tid < num_readers; ++tid) {
        threads.emplace_back([&, tid]() {
            while (!start_signal.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            std::mt19937 rng(5000 + static_cast<unsigned int>(tid));
            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string key = "churn_key_" + std::to_string(rng() % 5000);
                try {
                    store.get(key);
                } catch (...) {
                    error_count.fetch_add(1);
                }
            }
        });
    }

    start_signal.store(true, std::memory_order_release);
    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(error_count.load(), 0);
    ASSERT_TRUE(store.size() <= 128);
    auto stats = store.get_stats();
    ASSERT_TRUE(stats.evictions > 0);
}

TEST_CASE(concurrency_deadlock_freedom_global_ops) {
    kvstore::KVStore store;
    std::atomic<bool> running{true};
    std::atomic<size_t> error_count{0};
    const size_t num_workers = 6;
    std::vector<std::thread> workers;

    // Mutator workers
    for (size_t tid = 0; tid < num_workers; ++tid) {
        workers.emplace_back([&, tid]() {
            std::mt19937 rng(42 + static_cast<unsigned int>(tid));
            while (running.load(std::memory_order_relaxed)) {
                int key_id = rng() % 200;
                std::string key = "k_" + std::to_string(key_id);
                try {
                    int op = rng() % 3;
                    if (op == 0) {
                        store.set(key, "v");
                    } else if (op == 1) {
                        store.get(key);
                    } else {
                        store.del(key);
                    }
                } catch (...) {
                    error_count.fetch_add(1);
                }
            }
        });
    }

    // Global coordinator thread
    std::thread coordinator([&]() {
        for (int i = 0; i < 20; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            try {
                auto s = store.size();
                (void)s;
                auto stats = store.get_stats();
                (void)stats;
                if (i % 5 == 0) {
                    store.clear();
                }
            } catch (...) {
                error_count.fetch_add(1);
            }
        }
        running.store(false, std::memory_order_release);
    });

    coordinator.join();
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(error_count.load(), 0);
}

// ============================================================================
// Main Runner
// ============================================================================

int main() {
    std::cout << "\n=======================================================\n";
    std::cout << "  KVStore Milestone 1 Engine & Concurrency Test Suite  \n";
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
        std::cout << ">>> ALL MILESTONE 1 TESTS PASSED SUCCESSFULLY! <<<\n\n";
        return 0;
    } else {
        std::cout << ">>> SOME TESTS FAILED! <<<\n\n";
        return 1;
    }
}
