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

inline std::vector<TestCase>& get_adversarial_registry() {
    static std::vector<TestCase> registry;
    return registry;
}

struct AdversarialTestRegistrar {
    AdversarialTestRegistrar(const std::string& name, std::function<void()> func) {
        get_adversarial_registry().push_back({name, std::move(func)});
    }
};

class TestFailureException : public std::runtime_error {
public:
    explicit TestFailureException(const std::string& msg) : std::runtime_error(msg) {}
};

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

#define ADV_TEST(name) \
    void test_##name(); \
    static AdversarialTestRegistrar registrar_adv_##name(#name, test_##name); \
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
// Section 1: Capacity Boundary Adversarial Tests
// ============================================================================

ADV_TEST(boundary_cap_1_lru_shard) {
    kvstore::LruShard shard(1);
    ASSERT_EQ(shard.capacity(), 1);
    ASSERT_EQ(shard.size(), 0);

    // Initial insertion
    ASSERT_EQ(shard.set("k1", "v1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_TRUE(shard.exists("k1"));
    ASSERT_EQ(shard.get("k1").value(), "v1");
    ASSERT_EQ(shard.get_stats().evictions, 0);

    // Second insertion must evict k1
    ASSERT_EQ(shard.set("k2", "v2"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_FALSE(shard.exists("k1"));
    ASSERT_FALSE(shard.get("k1").has_value());
    ASSERT_TRUE(shard.exists("k2"));
    ASSERT_EQ(shard.get("k2").value(), "v2");
    ASSERT_EQ(shard.get_stats().evictions, 1);

    // Overwrite existing key k2: must NOT evict and size remains 1
    ASSERT_EQ(shard.set("k2", "v2_updated"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_EQ(shard.get("k2").value(), "v2_updated");
    ASSERT_EQ(shard.get_stats().evictions, 1);

    // Rvalue set overload at capacity 1
    ASSERT_EQ(shard.set(std::string("k3"), std::string("v3")), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_FALSE(shard.exists("k2"));
    ASSERT_EQ(shard.get("k3").value(), "v3");
    ASSERT_EQ(shard.get_stats().evictions, 2);

    // Rvalue overwrite existing key
    ASSERT_EQ(shard.set(std::string("k3"), std::string("v3_rval")), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_EQ(shard.get("k3").value(), "v3_rval");
    ASSERT_EQ(shard.get_stats().evictions, 2);

    // Delete at cap 1
    ASSERT_EQ(shard.del("k3"), kvstore::DelResult::DELETED);
    ASSERT_EQ(shard.size(), 0);
    ASSERT_FALSE(shard.exists("k3"));

    // Insert after deletion
    ASSERT_EQ(shard.set("k4", "v4"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_EQ(shard.get_stats().evictions, 2);
}

ADV_TEST(boundary_cap_1_kvstore) {
    kvstore::KVStore store(1);
    ASSERT_EQ(store.capacity(), 1);
    ASSERT_EQ(store.num_shards(), 1);
    ASSERT_EQ(store.shard_capacity(0), 1);
    ASSERT_EQ(store.size(), 0);

    ASSERT_EQ(store.set("first", "data1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_TRUE(store.exists("first"));
    ASSERT_EQ(store.get("first").value(), "data1");

    // Second key evicts first
    ASSERT_EQ(store.set("second", "data2"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_FALSE(store.exists("first"));
    ASSERT_FALSE(store.get("first").has_value());
    ASSERT_TRUE(store.exists("second"));
    ASSERT_EQ(store.get("second").value(), "data2");

    auto stats = store.get_stats();
    ASSERT_EQ(stats.capacity, 1);
    ASSERT_EQ(stats.key_count, 1);
    ASSERT_EQ(stats.evictions, 1);

    // Overwrite at capacity 1
    ASSERT_EQ(store.set("second", "data2_mod"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(store.size(), 1);
    ASSERT_EQ(store.get("second").value(), "data2_mod");
    ASSERT_EQ(store.get_stats().evictions, 1);

    // Del
    ASSERT_EQ(store.del("second"), kvstore::DelResult::DELETED);
    ASSERT_EQ(store.size(), 0);
    ASSERT_FALSE(store.exists("second"));
}

ADV_TEST(boundary_cap_0_unbounded_lru_shard) {
    kvstore::LruShard shard(0);
    ASSERT_EQ(shard.capacity(), 0);

    const int total_keys = 2000;
    for (int i = 0; i < total_keys; ++i) {
        auto res = shard.set("key_" + std::to_string(i), "val_" + std::to_string(i));
        ASSERT_EQ(res, kvstore::SetResult::CREATED);
    }

    ASSERT_EQ(shard.size(), total_keys);
    auto stats = shard.get_stats();
    ASSERT_EQ(stats.capacity, 0);
    ASSERT_EQ(stats.key_count, total_keys);
    ASSERT_EQ(stats.evictions, 0);

    // Spot check all
    for (int i = 0; i < total_keys; i += 20) {
        ASSERT_TRUE(shard.exists("key_" + std::to_string(i)));
        ASSERT_EQ(shard.get("key_" + std::to_string(i)).value(), "val_" + std::to_string(i));
    }

    // Delete 500
    for (int i = 0; i < 500; ++i) {
        ASSERT_EQ(shard.del("key_" + std::to_string(i)), kvstore::DelResult::DELETED);
    }
    ASSERT_EQ(shard.size(), total_keys - 500);
    ASSERT_EQ(shard.get_stats().evictions, 0);
}

ADV_TEST(boundary_cap_0_unbounded_kvstore) {
    kvstore::KVStore store(0);
    ASSERT_EQ(store.capacity(), 0);
    ASSERT_EQ(store.num_shards(), 32);

    for (size_t i = 0; i < 32; ++i) {
        ASSERT_EQ(store.shard_capacity(i), 0);
    }

    const int total_keys = 5000;
    for (int i = 0; i < total_keys; ++i) {
        auto res = store.set("unbounded_" + std::to_string(i), "content_" + std::to_string(i));
        ASSERT_EQ(res, kvstore::SetResult::CREATED);
    }

    ASSERT_EQ(store.size(), total_keys);
    auto stats = store.get_stats();
    ASSERT_EQ(stats.capacity, 0);
    ASSERT_EQ(stats.key_count, total_keys);
    ASSERT_EQ(stats.evictions, 0);

    for (int i = 0; i < total_keys; i += 50) {
        ASSERT_TRUE(store.exists("unbounded_" + std::to_string(i)));
        ASSERT_EQ(store.get("unbounded_" + std::to_string(i)).value(), "content_" + std::to_string(i));
    }
}

ADV_TEST(boundary_cap_31_kvstore) {
    // Capacity 31 is strictly less than DEFAULT_SHARDS (32)
    // Constructor clamps num_shards to floor_power_of_two(31) = 16
    kvstore::KVStore store(31);
    ASSERT_EQ(store.capacity(), 31);
    ASSERT_EQ(store.num_shards(), 16);

    // Remainder distribution: 31 % 16 = 15
    // Shards 0..14 have capacity 2; Shard 15 has capacity 1.
    size_t cap_sum = 0;
    for (size_t s = 0; s < 16; ++s) {
        size_t c = store.shard_capacity(s);
        if (s < 15) {
            ASSERT_EQ(c, 2);
        } else {
            ASSERT_EQ(c, 1);
        }
        cap_sum += c;
    }
    ASSERT_EQ(cap_sum, 31);

    // Stream 1,000 keys and verify store.size() <= 31 at EVERY single step
    for (int i = 0; i < 1000; ++i) {
        store.set("item_" + std::to_string(i), "data_" + std::to_string(i));
        ASSERT_TRUE(store.size() <= 31);
    }

    auto stats = store.get_stats();
    ASSERT_TRUE(stats.key_count <= 31);
    ASSERT_EQ(stats.key_count + stats.evictions, 1000);

    // Specifically test shard 15 (which has capacity 1)
    // Find keys that hash to shard 15
    std::vector<std::string> shard_15_keys;
    for (int i = 10000; shard_15_keys.size() < 5; ++i) {
        std::string k = "shard15_probe_" + std::to_string(i);
        if (store.get_shard_index(k) == 15) {
            shard_15_keys.push_back(k);
        }
    }

    // Insert first key into shard 15
    store.set(shard_15_keys[0], "v0");
    ASSERT_TRUE(store.exists(shard_15_keys[0]));

    // Under global LRU coordination, inserting subsequent keys when at capacity evicts global oldest
    for (size_t j = 1; j < shard_15_keys.size(); ++j) {
        auto res = store.set(shard_15_keys[j], "v" + std::to_string(j));
        ASSERT_EQ(res, kvstore::SetResult::EVICTED);
        ASSERT_TRUE(store.size() <= 31);
        ASSERT_TRUE(store.exists(shard_15_keys[j]));
    }
}

ADV_TEST(boundary_cap_32_kvstore) {
    kvstore::KVStore store(32);
    ASSERT_EQ(store.capacity(), 32);
    ASSERT_EQ(store.num_shards(), 32);

    for (size_t s = 0; s < 32; ++s) {
        ASSERT_EQ(store.shard_capacity(s), 1);
    }

    // Stream 2,000 keys: store size must never exceed 32
    for (int i = 0; i < 2000; ++i) {
        store.set("cap32_" + std::to_string(i), "payload_" + std::to_string(i));
        ASSERT_TRUE(store.size() <= 32);
    }

    auto stats = store.get_stats();
    ASSERT_TRUE(stats.key_count <= 32);
    ASSERT_EQ(stats.key_count + stats.evictions, 2000);
}

ADV_TEST(boundary_cap_1000_kvstore) {
    kvstore::KVStore store(1000);
    ASSERT_EQ(store.capacity(), 1000);
    ASSERT_EQ(store.num_shards(), 32);

    // 1000 / 32 = 31 base, remainder = 8
    // Shards 0..7 have 32, Shards 8..31 have 31
    size_t total_cap = 0;
    for (size_t s = 0; s < 32; ++s) {
        size_t c = store.shard_capacity(s);
        if (s < 8) {
            ASSERT_EQ(c, 32);
        } else {
            ASSERT_EQ(c, 31);
        }
        total_cap += c;
    }
    ASSERT_EQ(total_cap, 1000);

    for (int i = 0; i < 10000; ++i) {
        store.set("large_" + std::to_string(i), "v_" + std::to_string(i));
        ASSERT_TRUE(store.size() <= 1000);
    }

    auto stats = store.get_stats();
    ASSERT_TRUE(stats.key_count <= 1000);
    ASSERT_EQ(stats.key_count + stats.evictions, 10000);
    ASSERT_TRUE(stats.evictions > 0);

    // Clear operation resets count
    store.clear();
    ASSERT_EQ(store.size(), 0);
    ASSERT_EQ(store.get_stats().key_count, 0);
}

// ============================================================================
// Section 2: Eviction Recency Order Adversarial Tests
// ============================================================================

ADV_TEST(eviction_strict_fifo_when_unread) {
    kvstore::LruShard shard(4);
    shard.set("k0", "v0");
    shard.set("k1", "v1");
    shard.set("k2", "v2");
    shard.set("k3", "v3");
    // MRU -> LRU order: [k3, k2, k1, k0]

    // Next insert k4 must evict k0
    ASSERT_EQ(shard.set("k4", "v4"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("k0"));
    ASSERT_TRUE(shard.exists("k1"));

    // Next insert k5 must evict k1
    ASSERT_EQ(shard.set("k5", "v5"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("k1"));
    ASSERT_TRUE(shard.exists("k2"));

    // Next insert k6 must evict k2
    ASSERT_EQ(shard.set("k6", "v6"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("k2"));
    ASSERT_TRUE(shard.exists("k3"));

    // Next insert k7 must evict k3
    ASSERT_EQ(shard.set("k7", "v7"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("k3"));

    // Remaining items must be exactly k7, k6, k5, k4
    auto entries = shard.get_entries_mru_order();
    ASSERT_EQ(entries.size(), 4);
    ASSERT_EQ(entries[0].first, "k7");
    ASSERT_EQ(entries[1].first, "k6");
    ASSERT_EQ(entries[2].first, "k5");
    ASSERT_EQ(entries[3].first, "k4");
}

ADV_TEST(eviction_get_protects_keys_from_eviction) {
    kvstore::LruShard shard(5);
    shard.set("K0", "val0");
    shard.set("K1", "val1");
    shard.set("K2", "val2");
    shard.set("K3", "val3");
    shard.set("K4", "val4");
    // Initial order MRU -> LRU: [K4, K3, K2, K1, K0]

    // Read K0 via get: moves K0 to MRU!
    // Order becomes: [K0, K4, K3, K2, K1]
    ASSERT_TRUE(shard.get("K0").has_value());

    // Read K1 via get: moves K1 to MRU!
    // Order becomes: [K1, K0, K4, K3, K2]
    ASSERT_TRUE(shard.get("K1").has_value());

    // Insert K5: Capacity is 5, LRU is K2!
    // K2 MUST be evicted, K0 and K1 MUST be spared!
    ASSERT_EQ(shard.set("K5", "val5"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("K2"));
    ASSERT_TRUE(shard.exists("K0"));
    ASSERT_TRUE(shard.exists("K1"));
    ASSERT_TRUE(shard.exists("K3"));
    ASSERT_TRUE(shard.exists("K4"));
    ASSERT_TRUE(shard.exists("K5"));

    // Current order: [K5, K1, K0, K4, K3]
    // Insert K6: LRU is K3!
    ASSERT_EQ(shard.set("K6", "val6"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("K3"));
    ASSERT_TRUE(shard.exists("K0"));
    ASSERT_TRUE(shard.exists("K1"));

    // Current order: [K6, K5, K1, K0, K4]
    // Insert K7: LRU is K4!
    ASSERT_EQ(shard.set("K7", "val7"), kvstore::SetResult::EVICTED);
    ASSERT_FALSE(shard.exists("K4"));
    ASSERT_TRUE(shard.exists("K0"));
    ASSERT_TRUE(shard.exists("K1"));

    // After 3 rounds of eviction, K0 and K1 (the first inserted keys) are STILL alive!
    ASSERT_EQ(shard.get("K0").value(), "val0");
    ASSERT_EQ(shard.get("K1").value(), "val1");
}

ADV_TEST(eviction_churn_continuous_survival) {
    kvstore::LruShard shard(3);
    shard.set("immortal", "anchor");
    shard.set("buffer1", "init1");
    shard.set("buffer2", "init2");

    // Continuous access loop: 500 churn keys inserted, immortal key is touched every cycle
    for (int i = 0; i < 500; ++i) {
        auto val = shard.get("immortal");
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(val.value(), "anchor");

        shard.set("churn_" + std::to_string(i), "tmp");
    }

    ASSERT_EQ(shard.size(), 3);
    ASSERT_TRUE(shard.exists("immortal"));
    ASSERT_EQ(shard.get("immortal").value(), "anchor");
    ASSERT_EQ(shard.get_stats().evictions, 500);
}

ADV_TEST(eviction_set_capacity_shrink_order) {
    kvstore::LruShard shard(10);
    for (int i = 0; i < 10; ++i) {
        shard.set("k" + std::to_string(i), "v" + std::to_string(i));
    }
    ASSERT_EQ(shard.size(), 10);

    // Promote k0 and k1 to MRU
    shard.get("k0");
    shard.get("k1");
    // Order MRU -> LRU: [k1, k0, k9, k8, k7, k6, k5, k4, k3, k2]

    // Shrink capacity from 10 down to 3
    // Must evict 7 oldest items: k2, k3, k4, k5, k6, k7, k8
    // Keeping top 3: k1, k0, k9
    shard.set_capacity(3);
    ASSERT_EQ(shard.capacity(), 3);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 7);

    ASSERT_TRUE(shard.exists("k1"));
    ASSERT_TRUE(shard.exists("k0"));
    ASSERT_TRUE(shard.exists("k9"));

    ASSERT_FALSE(shard.exists("k8"));
    ASSERT_FALSE(shard.exists("k7"));
    ASSERT_FALSE(shard.exists("k2"));
}

// ============================================================================
// Section 3: Edge Case Inputs Adversarial Tests
// ============================================================================

ADV_TEST(edge_empty_keys_and_values) {
    kvstore::KVStore store;

    // 1. Empty key with standard value
    ASSERT_EQ(store.set("", "empty_key_val"), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists(""));
    ASSERT_EQ(store.get("").value(), "empty_key_val");
    ASSERT_EQ(store.size(), 1);

    // 2. Standard key with empty value
    ASSERT_EQ(store.set("has_key", ""), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists("has_key"));
    ASSERT_EQ(store.get("has_key").value(), "");
    ASSERT_EQ(store.size(), 2);

    // 3. Both empty key and empty value
    ASSERT_EQ(store.set("", ""), kvstore::SetResult::UPDATED);
    ASSERT_TRUE(store.exists(""));
    ASSERT_EQ(store.get("").value(), "");
    ASSERT_EQ(store.size(), 2);

    // 4. Delete empty key
    ASSERT_EQ(store.del(""), kvstore::DelResult::DELETED);
    ASSERT_FALSE(store.exists(""));
    ASSERT_EQ(store.size(), 1);

    // 5. Delete empty value key
    ASSERT_EQ(store.del("has_key"), kvstore::DelResult::DELETED);
    ASSERT_FALSE(store.exists("has_key"));
    ASSERT_EQ(store.size(), 0);
}

ADV_TEST(edge_embedded_null_bytes_and_differentiation) {
    kvstore::KVStore store;

    // Keys with null bytes in different positions
    std::string k_null_mid("hello\0world", 11);
    std::string k_null_mid2("hello\0earth", 11);
    std::string k_no_null("hello", 5);
    std::string k_null_end("hello\0", 6);
    std::string k_null_start("\0hello", 6);
    std::string k_sole_null("\0", 1);
    std::string k_triple_null("\0\0\0", 3);

    // All 7 keys MUST be treated as completely distinct entities
    ASSERT_EQ(store.set(k_null_mid, "val_mid"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.set(k_null_mid2, "val_mid2"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.set(k_no_null, "val_no_null"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.set(k_null_end, "val_null_end"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.set(k_null_start, "val_null_start"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.set(k_sole_null, "val_sole"), kvstore::SetResult::CREATED);
    ASSERT_EQ(store.set(k_triple_null, "val_triple"), kvstore::SetResult::CREATED);

    ASSERT_EQ(store.size(), 7);

    // Verify exact value matching for each key
    ASSERT_EQ(store.get(k_null_mid).value(), "val_mid");
    ASSERT_EQ(store.get(k_null_mid2).value(), "val_mid2");
    ASSERT_EQ(store.get(k_no_null).value(), "val_no_null");
    ASSERT_EQ(store.get(k_null_end).value(), "val_null_end");
    ASSERT_EQ(store.get(k_null_start).value(), "val_null_start");
    ASSERT_EQ(store.get(k_sole_null).value(), "val_sole");
    ASSERT_EQ(store.get(k_triple_null).value(), "val_triple");

    // Deleting "hello" must NOT delete "hello\0"
    ASSERT_EQ(store.del(k_no_null), kvstore::DelResult::DELETED);
    ASSERT_FALSE(store.exists(k_no_null));
    ASSERT_TRUE(store.exists(k_null_end));
    ASSERT_EQ(store.get(k_null_end).value(), "val_null_end");
    ASSERT_EQ(store.size(), 6);
}

ADV_TEST(edge_full_binary_payload_values) {
    kvstore::KVStore store;

    // Binary payload with all 256 byte values repeated 500 times = 128 KB
    std::string bin_payload;
    bin_payload.resize(256 * 500);
    for (size_t i = 0; i < bin_payload.size(); ++i) {
        bin_payload[i] = static_cast<char>(i % 256);
    }

    std::string bin_key("bin_key_\x00\xFF\xAA\x55", 12);
    ASSERT_EQ(store.set(bin_key, bin_payload), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists(bin_key));

    auto retrieved = store.get(bin_key);
    ASSERT_TRUE(retrieved.has_value());
    ASSERT_EQ(retrieved.value().size(), bin_payload.size());
    ASSERT_EQ(retrieved.value(), bin_payload);

    // Overwrite with modified binary payload
    bin_payload[0] = static_cast<char>(0xDE);
    bin_payload[bin_payload.size() - 1] = static_cast<char>(0xAD);
    ASSERT_EQ(store.set(bin_key, bin_payload), kvstore::SetResult::UPDATED);

    auto retrieved2 = store.get(bin_key);
    ASSERT_TRUE(retrieved2.has_value());
    ASSERT_EQ(retrieved2.value(), bin_payload);
}

ADV_TEST(edge_very_large_keys_and_values) {
    // 1 MB key and 2 MB value
    std::string large_key(1024 * 1024, 'K');
    std::string large_val(2 * 1024 * 1024, 'V');

    kvstore::KVStore store(5);
    ASSERT_EQ(store.set(large_key, large_val), kvstore::SetResult::CREATED);
    ASSERT_TRUE(store.exists(large_key));

    auto val = store.get(large_key);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val.value().size(), 2 * 1024 * 1024);
    ASSERT_EQ(val.value().front(), 'V');
    ASSERT_EQ(val.value().back(), 'V');

    // Update with 3 MB value
    std::string larger_val(3 * 1024 * 1024, 'W');
    ASSERT_EQ(store.set(large_key, larger_val), kvstore::SetResult::UPDATED);
    ASSERT_EQ(store.size(), 1);

    auto val2 = store.get(large_key);
    ASSERT_TRUE(val2.has_value());
    ASSERT_EQ(val2.value().size(), 3 * 1024 * 1024);
    ASSERT_EQ(val2.value().front(), 'W');

    // Evict the large key by pushing other keys
    for (int i = 0; i < 20; ++i) {
        store.set("small_" + std::to_string(i), "v");
    }
    ASSERT_FALSE(store.exists(large_key));
    ASSERT_FALSE(store.get(large_key).has_value());
}

// ============================================================================
// Section 4: Overwrite at Capacity Adversarial Tests
// ============================================================================

ADV_TEST(overwrite_at_capacity_lru_shard) {
    kvstore::LruShard shard(3);
    ASSERT_EQ(shard.set("A", "val_A_1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.set("B", "val_B_1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.set("C", "val_C_1"), kvstore::SetResult::CREATED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 0);

    // Overwrite A at capacity
    ASSERT_EQ(shard.set("A", "val_A_2"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 0);
    ASSERT_TRUE(shard.exists("A"));
    ASSERT_TRUE(shard.exists("B"));
    ASSERT_TRUE(shard.exists("C"));
    ASSERT_EQ(shard.get("A").value(), "val_A_2");

    // Overwrite B at capacity
    ASSERT_EQ(shard.set("B", "val_B_2"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 0);
    ASSERT_EQ(shard.get("B").value(), "val_B_2");

    // Overwrite C at capacity via rvalues
    ASSERT_EQ(shard.set(std::string("C"), std::string("val_C_2")), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 0);
    ASSERT_EQ(shard.get("C").value(), "val_C_2");

    // All 3 keys are still intact; 0 evictions occurred
    ASSERT_EQ(shard.get_stats().evictions, 0);
}

ADV_TEST(overwrite_lru_tail_promotes_and_protects) {
    kvstore::LruShard shard(3);
    shard.set("oldest", "v1");
    shard.set("middle", "v2");
    shard.set("newest", "v3");
    // Order MRU -> LRU: [newest, middle, oldest]

    // Updating "oldest" must promote it to MRU!
    // New order: [oldest, newest, middle]
    ASSERT_EQ(shard.set("oldest", "v1_promoted"), kvstore::SetResult::UPDATED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_EQ(shard.get_stats().evictions, 0);

    // Now insert "incoming": capacity 3 reached.
    // LRU item is "middle"! "middle" must be evicted, "oldest" must survive!
    ASSERT_EQ(shard.set("incoming", "v4"), kvstore::SetResult::EVICTED);
    ASSERT_EQ(shard.size(), 3);
    ASSERT_FALSE(shard.exists("middle"));
    ASSERT_TRUE(shard.exists("oldest"));
    ASSERT_TRUE(shard.exists("newest"));
    ASSERT_TRUE(shard.exists("incoming"));
    ASSERT_EQ(shard.get("oldest").value(), "v1_promoted");
}

ADV_TEST(overwrite_at_capacity_kvstore) {
    // 1. Precise per-shard capacity saturation:
    // KVStore(16) has 16 shards of capacity 1 each.
    kvstore::KVStore store(16);
    ASSERT_EQ(store.num_shards(), 16);

    // Pick exactly 1 unique key for each of the 16 shards
    std::vector<std::string> keys_per_shard(16);
    std::vector<bool> shard_found(16, false);
    size_t found_count = 0;
    for (int i = 0; found_count < 16; ++i) {
        std::string k = "targeted_key_" + std::to_string(i);
        size_t shard = store.get_shard_index(k);
        if (!shard_found[shard]) {
            shard_found[shard] = true;
            keys_per_shard[shard] = k;
            found_count++;
        }
    }

    // Insert 1 key per shard: all must return CREATED
    for (size_t s = 0; s < 16; ++s) {
        auto res = store.set(keys_per_shard[s], "orig_" + std::to_string(s));
        ASSERT_EQ(res, kvstore::SetResult::CREATED);
    }

    // Now all 16 shards are at 100% capacity (1/1 each, total 16/16)
    ASSERT_EQ(store.size(), 16);
    ASSERT_EQ(store.get_stats().evictions, 0);

    // Overwrite every existing key at 100% capacity:
    // Every single set MUST return UPDATED, size MUST remain 16, evictions MUST remain 0
    for (size_t s = 0; s < 16; ++s) {
        auto res = store.set(keys_per_shard[s], "updated_" + std::to_string(s));
        ASSERT_EQ(res, kvstore::SetResult::UPDATED);
    }

    ASSERT_EQ(store.size(), 16);
    ASSERT_EQ(store.get_stats().evictions, 0);

    // Verify all 16 values were updated in place
    for (size_t s = 0; s < 16; ++s) {
        auto val = store.get(keys_per_shard[s]);
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(val.value(), "updated_" + std::to_string(s));
    }

    // 2. Test re-insertion of an evicted key:
    // Inserting a NEW key into shard 0 must evict keys_per_shard[0]
    std::string new_key_shard_0;
    for (int i = 10000; ; ++i) {
        std::string k = "shard0_intruder_" + std::to_string(i);
        if (store.get_shard_index(k) == 0) {
            new_key_shard_0 = k;
            break;
        }
    }

    auto evict_res = store.set(new_key_shard_0, "intruder_val");
    ASSERT_EQ(evict_res, kvstore::SetResult::EVICTED);
    ASSERT_FALSE(store.exists(keys_per_shard[0])); // Old key evicted
    ASSERT_TRUE(store.exists(new_key_shard_0));
    ASSERT_EQ(store.get_stats().evictions, 1);
    ASSERT_EQ(store.size(), 16);

    // Setting the evicted key again MUST be treated as a new insert (EVICTED), NOT an update!
    auto reinsert_res = store.set(keys_per_shard[0], "reborn");
    ASSERT_EQ(reinsert_res, kvstore::SetResult::EVICTED);
    ASSERT_EQ(store.get_stats().evictions, 2);
    ASSERT_EQ(store.size(), 16);
}

// ============================================================================
// Section 5: Concurrency Boundary Adversarial Tests
// ============================================================================

ADV_TEST(concurrent_overwrite_at_capacity_one) {
    kvstore::LruShard shard(1);
    shard.set("contested_key", "val_init");

    const size_t num_threads = 6;
    const size_t iters_per_thread = 5000;
    std::atomic<bool> start{false};
    std::atomic<size_t> errors{0};
    std::vector<std::thread> workers;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        workers.emplace_back([&, tid]() {
            while (!start.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
            for (size_t i = 0; i < iters_per_thread; ++i) {
                try {
                    auto res = shard.set("contested_key", "val_t" + std::to_string(tid) + "_" + std::to_string(i));
                    if (res != kvstore::SetResult::UPDATED) {
                        errors.fetch_add(1);
                    }
                    auto v = shard.get("contested_key");
                    if (!v.has_value() || v.value().empty()) {
                        errors.fetch_add(1);
                    }
                } catch (...) {
                    errors.fetch_add(1);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& w : workers) {
        w.join();
    }

    ASSERT_EQ(errors.load(), 0);
    ASSERT_EQ(shard.size(), 1);
    ASSERT_EQ(shard.get_stats().evictions, 0);
}

ADV_TEST(concurrent_eviction_and_get_race) {
    // Sharded store with bounded capacity 32
    kvstore::KVStore store(32);
    const size_t num_writers = 4;
    const size_t num_readers = 4;
    const size_t duration_ms = 500;
    std::atomic<bool> running{true};
    std::atomic<size_t> errors{0};
    std::vector<std::thread> threads;

    // Writers churn keys across shards
    for (size_t tid = 0; tid < num_writers; ++tid) {
        threads.emplace_back([&, tid]() {
            std::mt19937 rng(400 + static_cast<unsigned int>(tid));
            size_t seq = 0;
            while (running.load(std::memory_order_relaxed)) {
                std::string k = "churn_race_" + std::to_string(rng() % 500);
                std::string v = "payload:" + std::to_string(tid) + ":" + std::to_string(seq++);
                try {
                    store.set(k, v);
                } catch (...) {
                    errors.fetch_add(1);
                }
            }
        });
    }

    // Readers read random keys concurrently
    for (size_t tid = 0; tid < num_readers; ++tid) {
        threads.emplace_back([&, tid]() {
            std::mt19937 rng(800 + static_cast<unsigned int>(tid));
            while (running.load(std::memory_order_relaxed)) {
                std::string k = "churn_race_" + std::to_string(rng() % 500);
                try {
                    auto res = store.get(k);
                    if (res.has_value()) {
                        // Integrity check
                        if (res.value().rfind("payload:", 0) != 0) {
                            errors.fetch_add(1);
                        }
                    }
                } catch (...) {
                    errors.fetch_add(1);
                }
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
    running.store(false, std::memory_order_release);

    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(store.size() <= 32);
    auto stats = store.get_stats();
    ASSERT_TRUE(stats.evictions > 0);
    ASSERT_EQ(stats.capacity, 32);
}

// ============================================================================
// Section 6: Differential Oracle Fuzzing & Dynamic Capacity Resizing
// ============================================================================

struct OracleEntry {
    std::string key;
    std::string value;
};

class LruOracle {
public:
    explicit LruOracle(size_t cap) : cap_(cap) {}

    kvstore::SetResult set(const std::string& key, const std::string& value) {
        for (auto it = list_.begin(); it != list_.end(); ++it) {
            if (it->key == key) {
                it->value = value;
                list_.splice(list_.begin(), list_, it);
                return kvstore::SetResult::UPDATED;
            }
        }
        kvstore::SetResult res = kvstore::SetResult::CREATED;
        if (cap_ > 0 && list_.size() >= cap_) {
            list_.pop_back();
            evictions_++;
            res = kvstore::SetResult::EVICTED;
        }
        list_.push_front(OracleEntry{key, value});
        return res;
    }

    std::optional<std::string> get(const std::string& key) {
        for (auto it = list_.begin(); it != list_.end(); ++it) {
            if (it->key == key) {
                std::string val = it->value;
                list_.splice(list_.begin(), list_, it);
                hits_++;
                return val;
            }
        }
        misses_++;
        return std::nullopt;
    }

    kvstore::DelResult del(const std::string& key) {
        for (auto it = list_.begin(); it != list_.end(); ++it) {
            if (it->key == key) {
                list_.erase(it);
                return kvstore::DelResult::DELETED;
            }
        }
        return kvstore::DelResult::NOT_FOUND;
    }

    bool exists(const std::string& key) const {
        for (const auto& entry : list_) {
            if (entry.key == key) return true;
        }
        return false;
    }

    size_t size() const { return list_.size(); }
    uint64_t evictions() const { return evictions_; }
    uint64_t hits() const { return hits_; }
    uint64_t misses() const { return misses_; }

    std::vector<std::pair<std::string, std::string>> get_entries() const {
        std::vector<std::pair<std::string, std::string>> res;
        for (const auto& e : list_) {
            res.emplace_back(e.key, e.value);
        }
        return res;
    }

private:
    size_t cap_{0};
    std::list<OracleEntry> list_;
    uint64_t evictions_{0};
    uint64_t hits_{0};
    uint64_t misses_{0};
};

ADV_TEST(differential_oracle_fuzz_lru_shard) {
    const size_t cap = 8;
    kvstore::LruShard shard(cap);
    LruOracle oracle(cap);

    std::mt19937 rng(1337);
    const size_t pool_size = 35;
    std::vector<std::string> key_pool;
    for (size_t i = 0; i < pool_size; ++i) {
        key_pool.push_back("fuzz_k_" + std::to_string(i));
    }

    const size_t total_ops = 5000;
    for (size_t op_idx = 0; op_idx < total_ops; ++op_idx) {
        int action = rng() % 100;
        const auto& key = key_pool[rng() % pool_size];

        if (action < 45) { // 45% SET
            std::string val = "v_" + std::to_string(rng() % 1000);
            auto shard_res = shard.set(key, val);
            auto oracle_res = oracle.set(key, val);
            ASSERT_EQ(shard_res, oracle_res);
        } else if (action < 85) { // 40% GET
            auto shard_val = shard.get(key);
            auto oracle_val = oracle.get(key);
            ASSERT_EQ(shard_val.has_value(), oracle_val.has_value());
            if (shard_val.has_value()) {
                ASSERT_EQ(shard_val.value(), oracle_val.value());
            }
        } else { // 15% DEL
            auto shard_del = shard.del(key);
            auto oracle_del = oracle.del(key);
            ASSERT_EQ(shard_del, oracle_del);
        }

        // Continuous invariant checks
        ASSERT_EQ(shard.size(), oracle.size());
        auto stats = shard.get_stats();
        ASSERT_EQ(stats.evictions, oracle.evictions());
        ASSERT_EQ(stats.hits, oracle.hits());
        ASSERT_EQ(stats.misses, oracle.misses());

        // Periodic full list ordering alignment check
        if (op_idx % 100 == 0) {
            auto shard_entries = shard.get_entries_mru_order();
            auto oracle_entries = oracle.get_entries();
            ASSERT_EQ(shard_entries.size(), oracle_entries.size());
            for (size_t j = 0; j < shard_entries.size(); ++j) {
                ASSERT_EQ(shard_entries[j].first, oracle_entries[j].first);
                ASSERT_EQ(shard_entries[j].second, oracle_entries[j].second);
            }
        }
    }
}

ADV_TEST(dynamic_set_capacity_shrink_and_expand) {
    // 1. Start unbounded (cap = 0)
    kvstore::LruShard shard(0);
    ASSERT_EQ(shard.capacity(), 0);

    for (int i = 0; i < 500; ++i) {
        shard.set("seq_" + std::to_string(i), "v_" + std::to_string(i));
    }
    ASSERT_EQ(shard.size(), 500);
    ASSERT_EQ(shard.get_stats().evictions, 0);

    // 2. Shrink capacity to 20
    shard.set_capacity(20);
    ASSERT_EQ(shard.capacity(), 20);
    ASSERT_EQ(shard.size(), 20);
    ASSERT_EQ(shard.get_stats().evictions, 480);

    // Verify remaining 20 are seq_480 to seq_499
    for (int i = 480; i < 500; ++i) {
        ASSERT_TRUE(shard.exists("seq_" + std::to_string(i)));
    }
    for (int i = 0; i < 480; ++i) {
        ASSERT_FALSE(shard.exists("seq_" + std::to_string(i)));
    }

    // 3. Expand capacity to 40
    shard.set_capacity(40);
    ASSERT_EQ(shard.capacity(), 40);
    ASSERT_EQ(shard.size(), 20); // Still 20 items

    // Insert 20 more items
    for (int i = 500; i < 520; ++i) {
        ASSERT_EQ(shard.set("seq_" + std::to_string(i), "v_" + std::to_string(i)), kvstore::SetResult::CREATED);
    }
    ASSERT_EQ(shard.size(), 40);
    ASSERT_EQ(shard.get_stats().evictions, 480);

    // 4. Return to unbounded (cap = 0)
    shard.set_capacity(0);
    ASSERT_EQ(shard.capacity(), 0);
    ASSERT_EQ(shard.size(), 40);

    for (int i = 520; i < 620; ++i) {
        ASSERT_EQ(shard.set("seq_" + std::to_string(i), "v"), kvstore::SetResult::CREATED);
    }
    ASSERT_EQ(shard.size(), 140);
    ASSERT_EQ(shard.get_stats().evictions, 480);
}

// ============================================================================
// Main Runner
// ============================================================================

int main() {
    std::cout << "\n=======================================================\n";
    std::cout << "  KVStore Milestone 1 Challenger Adversarial Suite     \n";
    std::cout << "=======================================================\n\n";

    int passed = 0;
    int failed = 0;
    auto t_start = std::chrono::steady_clock::now();

    for (const auto& test : get_adversarial_registry()) {
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
        std::cout << ">>> ALL ADVERSARIAL TESTS PASSED SUCCESSFULLY! <<<\n\n";
        return 0;
    } else {
        std::cout << ">>> SOME ADVERSARIAL TESTS FAILED! <<<\n\n";
        return 1;
    }
}
