#include "kvstore/benchmark.hpp"
#include "kvstore/client.hpp"
#include "kvstore/kvstore.hpp"
#include "kvstore/server.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// ==============================================================================
// Lightweight Adversarial Assertion Macros
// ==============================================================================

#define ADV_ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::cerr << "[-] ASSERT_TRUE FAILED: " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ADV_ASSERT_FALSE(cond) do { \
    if (cond) { \
        std::cerr << "[-] ASSERT_FALSE FAILED: " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ADV_ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        std::cerr << "[-] ASSERT_EQ FAILED: " #a " == " #b " at " << __FILE__ << ":" << __LINE__ \
                  << " [Actual: " << (a) << " vs Expected: " << (b) << "]" << std::endl; \
        std::exit(1); \
    } \
} while(0)

// Helper to verify monotonicity invariant
static void verify_monotonicity(const kvstore::LatencyStats& s) {
    ADV_ASSERT_TRUE(s.min_us <= s.p50_us);
    ADV_ASSERT_TRUE(s.p50_us <= s.p95_us);
    ADV_ASSERT_TRUE(s.p95_us <= s.p99_us);
    ADV_ASSERT_TRUE(s.p99_us <= s.p999_us);
    ADV_ASSERT_TRUE(s.p999_us <= s.max_us);
}

// ==============================================================================
// Suite 1: Mathematical Rigor of Percentile Algorithms
// ==============================================================================

// 1.1 Degenerate: Empty latency vector
void test_percentiles_empty() {
    std::vector<uint64_t> empty;
    auto s = kvstore::LatencyStats::calculate(empty);

    ADV_ASSERT_EQ(s.min_us, 0ULL);
    ADV_ASSERT_EQ(s.max_us, 0ULL);
    ADV_ASSERT_EQ(s.p50_us, 0ULL);
    ADV_ASSERT_EQ(s.p95_us, 0ULL);
    ADV_ASSERT_EQ(s.p99_us, 0ULL);
    ADV_ASSERT_EQ(s.p999_us, 0ULL);
    ADV_ASSERT_EQ(s.avg_us, 0.0);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_empty" << std::endl;
}

// 1.2 Degenerate: Single sample
void test_percentiles_single_sample() {
    std::vector<uint64_t> single{1337};
    auto s = kvstore::LatencyStats::calculate(single);

    ADV_ASSERT_EQ(s.min_us, 1337ULL);
    ADV_ASSERT_EQ(s.max_us, 1337ULL);
    ADV_ASSERT_EQ(s.p50_us, 1337ULL);
    ADV_ASSERT_EQ(s.p95_us, 1337ULL);
    ADV_ASSERT_EQ(s.p99_us, 1337ULL);
    ADV_ASSERT_EQ(s.p999_us, 1337ULL);
    ADV_ASSERT_EQ(s.avg_us, 1337.0);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_single_sample" << std::endl;
}

// 1.3 Degenerate: Identical / Constant values (N = 5000)
void test_percentiles_identical_values() {
    std::vector<uint64_t> constant(5000, 4200);
    auto s = kvstore::LatencyStats::calculate(constant);

    ADV_ASSERT_EQ(s.min_us, 4200ULL);
    ADV_ASSERT_EQ(s.max_us, 4200ULL);
    ADV_ASSERT_EQ(s.p50_us, 4200ULL);
    ADV_ASSERT_EQ(s.p95_us, 4200ULL);
    ADV_ASSERT_EQ(s.p99_us, 4200ULL);
    ADV_ASSERT_EQ(s.p999_us, 4200ULL);
    ADV_ASSERT_EQ(s.avg_us, 4200.0);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_identical_values" << std::endl;
}

// 1.4 Boundary: Exactly two samples
void test_percentiles_two_samples() {
    std::vector<uint64_t> two{10, 9990};
    auto s = kvstore::LatencyStats::calculate(two);

    ADV_ASSERT_EQ(s.min_us, 10ULL);
    ADV_ASSERT_EQ(s.max_us, 9990ULL);
    // Nearest-rank: p50 -> ceil(0.5 * 2) = 1 -> idx 0 -> 10
    ADV_ASSERT_EQ(s.p50_us, 10ULL);
    // p95 -> ceil(0.95 * 2) = 2 -> idx 1 -> 9990
    ADV_ASSERT_EQ(s.p95_us, 9990ULL);
    ADV_ASSERT_EQ(s.p99_us, 9990ULL);
    ADV_ASSERT_EQ(s.p999_us, 9990ULL);
    ADV_ASSERT_EQ(s.avg_us, 5000.0);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_two_samples" << std::endl;
}

// 1.5 Boundary: Exactly three samples
void test_percentiles_three_samples() {
    std::vector<uint64_t> three{10, 20, 30};
    auto s = kvstore::LatencyStats::calculate(three);

    ADV_ASSERT_EQ(s.min_us, 10ULL);
    ADV_ASSERT_EQ(s.max_us, 30ULL);
    // ceil(0.50 * 3) = 2 -> idx 1 -> 20
    ADV_ASSERT_EQ(s.p50_us, 20ULL);
    // ceil(0.95 * 3) = 3 -> idx 2 -> 30
    ADV_ASSERT_EQ(s.p95_us, 30ULL);
    ADV_ASSERT_EQ(s.p99_us, 30ULL);
    ADV_ASSERT_EQ(s.p999_us, 30ULL);
    ADV_ASSERT_EQ(s.avg_us, 20.0);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_three_samples" << std::endl;
}

// 1.6 Uniform distribution: N = 10,000, values 1..10000 shuffled
void test_percentiles_uniform_distribution_10k() {
    const size_t N = 10000;
    std::vector<uint64_t> latencies(N);
    std::iota(latencies.begin(), latencies.end(), 1);

    // Shuffle into random order
    std::mt19937_64 rng(42);
    std::shuffle(latencies.begin(), latencies.end(), rng);

    auto s = kvstore::LatencyStats::calculate(latencies);

    ADV_ASSERT_EQ(s.min_us, 1ULL);
    ADV_ASSERT_EQ(s.max_us, 10000ULL);
    ADV_ASSERT_EQ(s.p50_us, 5000ULL);
    ADV_ASSERT_EQ(s.p95_us, 9500ULL);
    ADV_ASSERT_EQ(s.p99_us, 9900ULL);
    ADV_ASSERT_EQ(s.p999_us, 9990ULL);
    ADV_ASSERT_TRUE(std::abs(s.avg_us - 5000.5) < 0.001);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_uniform_distribution_10k" << std::endl;
}

// 1.7 Heavily skewed / Long-tail distribution: 9,800 @ 100us, 200 @ 50,000us
void test_percentiles_skewed_distribution() {
    std::vector<uint64_t> latencies;
    latencies.reserve(10000);
    latencies.insert(latencies.end(), 9800, 100ULL);
    latencies.insert(latencies.end(), 200, 50000ULL);

    std::mt19937_64 rng(12345);
    std::shuffle(latencies.begin(), latencies.end(), rng);

    auto s = kvstore::LatencyStats::calculate(latencies);

    ADV_ASSERT_EQ(s.min_us, 100ULL);
    ADV_ASSERT_EQ(s.max_us, 50000ULL);
    // 50% falls in the 100us bulk
    ADV_ASSERT_EQ(s.p50_us, 100ULL);
    // 95% falls in the 100us bulk (9500 < 9800)
    ADV_ASSERT_EQ(s.p95_us, 100ULL);
    // 99% falls in the tail (9900 >= 9800)
    ADV_ASSERT_EQ(s.p99_us, 50000ULL);
    // 99.9% falls in the tail
    ADV_ASSERT_EQ(s.p999_us, 50000ULL);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_skewed_distribution" << std::endl;
}

// 1.8 Bimodal distribution: 5,000 @ 50us (fast cache hits), 5,000 @ 5,000us (slow disk reads)
void test_percentiles_bimodal_distribution() {
    std::vector<uint64_t> latencies;
    latencies.reserve(10000);
    latencies.insert(latencies.end(), 5000, 50ULL);
    latencies.insert(latencies.end(), 5000, 5000ULL);

    std::mt19937_64 rng(999);
    std::shuffle(latencies.begin(), latencies.end(), rng);

    auto s = kvstore::LatencyStats::calculate(latencies);

    ADV_ASSERT_EQ(s.min_us, 50ULL);
    ADV_ASSERT_EQ(s.max_us, 5000ULL);
    ADV_ASSERT_EQ(s.p50_us, 50ULL);     // ceil(0.50 * 10000) = 5000 -> idx 4999 -> 50
    ADV_ASSERT_EQ(s.p95_us, 5000ULL);   // idx 9499 -> 5000
    ADV_ASSERT_EQ(s.p99_us, 5000ULL);   // idx 9899 -> 5000
    ADV_ASSERT_EQ(s.p999_us, 5000ULL);  // idx 9989 -> 5000
    ADV_ASSERT_TRUE(std::abs(s.avg_us - 2525.0) < 0.001);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_bimodal_distribution" << std::endl;
}

// 1.9 Reverse sorted input: ensures sorting handles descending arrays
void test_percentiles_reverse_sorted() {
    std::vector<uint64_t> latencies(1000);
    for (size_t i = 0; i < 1000; ++i) {
        latencies[i] = 1000 - i;
    }
    auto s = kvstore::LatencyStats::calculate(latencies);

    ADV_ASSERT_EQ(s.min_us, 1ULL);
    ADV_ASSERT_EQ(s.max_us, 1000ULL);
    ADV_ASSERT_EQ(s.p50_us, 500ULL);
    ADV_ASSERT_EQ(s.p95_us, 950ULL);
    ADV_ASSERT_EQ(s.p99_us, 990ULL);
    ADV_ASSERT_EQ(s.p999_us, 999ULL);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_reverse_sorted" << std::endl;
}

// 1.10 Large values: prevent uint64_t overflow during summation
void test_percentiles_large_values_no_overflow() {
    std::vector<uint64_t> latencies(10000, 1000000000ULL); // 1,000,000,000 us = 1000 s
    auto s = kvstore::LatencyStats::calculate(latencies);

    ADV_ASSERT_EQ(s.min_us, 1000000000ULL);
    ADV_ASSERT_EQ(s.max_us, 1000000000ULL);
    ADV_ASSERT_EQ(s.p50_us, 1000000000ULL);
    ADV_ASSERT_EQ(s.avg_us, 1000000000.0);
    verify_monotonicity(s);
    std::cout << "  [PASS] test_percentiles_large_values_no_overflow" << std::endl;
}

// ==============================================================================
// Suite 2: Configuration Validation and Alias Semantics
// ==============================================================================

void test_config_validation_and_aliases() {
    kvstore::BenchmarkConfig cfg;
    std::string err;

    // Default configuration must be valid
    ADV_ASSERT_TRUE(cfg.validate(err));

    // Zero threads rejected
    cfg.threads = 0;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.threads = 1;
    ADV_ASSERT_TRUE(cfg.validate(err));
    cfg.threads = 16;
    ADV_ASSERT_TRUE(cfg.validate(err));

    // Zero requests rejected
    cfg.requests = 0;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.requests = 1;
    ADV_ASSERT_TRUE(cfg.validate(err));

    // Read ratio bounds
    cfg.ratio = -0.001;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.ratio = 1.0001;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.ratio = 0.0; // 100% write
    ADV_ASSERT_TRUE(cfg.validate(err));
    cfg.ratio = 1.0; // 100% read
    ADV_ASSERT_TRUE(cfg.validate(err));

    // Read ratio alias consistency
    cfg.read_ratio = 0.33;
    ADV_ASSERT_EQ(cfg.ratio, 0.33);
    cfg.ratio = 0.67;
    ADV_ASSERT_EQ(cfg.read_ratio, 0.67);

    // Copy constructor preserves read_ratio alias to the new object
    kvstore::BenchmarkConfig copy_cfg(cfg);
    ADV_ASSERT_EQ(copy_cfg.ratio, 0.67);
    ADV_ASSERT_EQ(copy_cfg.read_ratio, 0.67);
    copy_cfg.read_ratio = 0.99;
    ADV_ASSERT_EQ(copy_cfg.ratio, 0.99);
    ADV_ASSERT_EQ(cfg.ratio, 0.67); // Original unchanged

    // Copy assignment preserves alias
    kvstore::BenchmarkConfig assigned_cfg;
    assigned_cfg = cfg;
    ADV_ASSERT_EQ(assigned_cfg.ratio, 0.67);
    ADV_ASSERT_EQ(assigned_cfg.read_ratio, 0.67);
    assigned_cfg.read_ratio = 0.12;
    ADV_ASSERT_EQ(assigned_cfg.ratio, 0.12);
    ADV_ASSERT_EQ(cfg.ratio, 0.67);

    // Keyspace validation
    cfg.keyspace = 0;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.keyspace = 1; // Single key contention valid
    ADV_ASSERT_TRUE(cfg.validate(err));

    // Value size: 0-byte payload is explicitly allowed
    cfg.val_size = 0;
    ADV_ASSERT_TRUE(cfg.validate(err));
    cfg.val_size = 8192;
    ADV_ASSERT_TRUE(cfg.validate(err));

    // Port validation
    cfg.port = 0;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.port = -1;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.port = 65536;
    ADV_ASSERT_FALSE(cfg.validate(err));
    cfg.port = 7379;
    ADV_ASSERT_TRUE(cfg.validate(err));

    std::cout << "  [PASS] test_config_validation_and_aliases" << std::endl;
}

// ==============================================================================
// Suite 3: Small Requests Sub-Concurrency (Division-by-Zero Resilience)
// ==============================================================================

void test_small_requests_sub_concurrency() {
    kvstore::KVStore store(1000);

    // Case 1: n = 1 request with c = 4 threads (3 threads do 0 requests)
    {
        kvstore::BenchmarkConfig cfg;
        cfg.threads = 4;
        cfg.requests = 1;
        cfg.ratio = 0.0; // 100% write
        cfg.keyspace = 10;
        cfg.val_size = 16;

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_in_memory(store);

        ADV_ASSERT_EQ(res.total_requests, 1ULL);
        ADV_ASSERT_EQ(res.completed_requests, 1ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);
        ADV_ASSERT_TRUE(res.elapsed_sec > 0.0);
        ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
        verify_monotonicity(res.latency);
    }

    // Case 2: n = 2 requests with c = 16 threads (14 threads do 0 requests)
    {
        kvstore::BenchmarkConfig cfg;
        cfg.threads = 16;
        cfg.requests = 2;
        cfg.ratio = 0.5;
        cfg.keyspace = 5;
        cfg.val_size = 32;

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_in_memory(store);

        ADV_ASSERT_EQ(res.total_requests, 2ULL);
        ADV_ASSERT_EQ(res.completed_requests, 2ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);
        ADV_ASSERT_TRUE(res.elapsed_sec > 0.0);
        ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
        verify_monotonicity(res.latency);
    }

    std::cout << "  [PASS] test_small_requests_sub_concurrency" << std::endl;
}

// ==============================================================================
// Suite 4: Boundary Payload Sizes & Workload Ratios (In-Memory Engine)
// ==============================================================================

void test_boundary_payload_sizes_in_memory() {
    // 4.1: 0-byte payload (val_size = 0)
    {
        kvstore::KVStore store(500);
        kvstore::BenchmarkConfig cfg;
        cfg.threads = 4;
        cfg.requests = 400;
        cfg.ratio = 0.0; // 100% SET
        cfg.keyspace = 10;
        cfg.val_size = 0; // Empty string payload

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_in_memory(store);

        ADV_ASSERT_EQ(res.completed_requests, 400ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);

        // Verify keys in store indeed have empty value
        for (size_t i = 0; i < 10; ++i) {
            std::string key = "key_" + std::to_string(i);
            auto val = store.get(key);
            ADV_ASSERT_TRUE(val.has_value());
            ADV_ASSERT_EQ(val.value(), "");
        }
    }

    // 4.2: 8KB large payload (val_size = 8192)
    {
        kvstore::KVStore store(500);
        kvstore::BenchmarkConfig cfg;
        cfg.threads = 4;
        cfg.requests = 200;
        cfg.ratio = 0.5;
        cfg.keyspace = 20;
        cfg.val_size = 8192; // 8KB payload

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_in_memory(store);

        ADV_ASSERT_EQ(res.completed_requests, 200ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);
        ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
        verify_monotonicity(res.latency);
    }

    std::cout << "  [PASS] test_boundary_payload_sizes_in_memory" << std::endl;
}

void test_boundary_workload_ratios_in_memory() {
    // 4.3: 100% Read Workload (ratio = 1.0)
    {
        kvstore::KVStore store(100);
        // Pre-populate keys
        for (size_t i = 0; i < 20; ++i) {
            store.set("key_" + std::to_string(i), "init_val");
        }
        auto initial_stats = store.get_stats();

        kvstore::BenchmarkConfig cfg;
        cfg.threads = 4;
        cfg.requests = 1000;
        cfg.ratio = 1.0; // 100% GET
        cfg.keyspace = 20;

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_in_memory(store);

        ADV_ASSERT_EQ(res.completed_requests, 1000ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);

        auto final_stats = store.get_stats();
        // Hits should increase by exactly 1000
        ADV_ASSERT_EQ(final_stats.hits - initial_stats.hits, 1000ULL);
        // Evictions must remain 0
        ADV_ASSERT_EQ(final_stats.evictions, 0ULL);
    }

    // 4.4: 100% Write Workload (ratio = 0.0)
    {
        kvstore::KVStore store(500);
        kvstore::BenchmarkConfig cfg;
        cfg.threads = 4;
        cfg.requests = 1000;
        cfg.ratio = 0.0; // 100% SET
        cfg.keyspace = 50;
        cfg.val_size = 64;

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_in_memory(store);

        ADV_ASSERT_EQ(res.completed_requests, 1000ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);
        ADV_ASSERT_EQ(store.size(), 50ULL); // All 50 keys populated
    }

    std::cout << "  [PASS] test_boundary_workload_ratios_in_memory" << std::endl;
}

// ==============================================================================
// Suite 5: High-Concurrency Maximum Contention & Capacity Eviction Stress
// ==============================================================================

// 5.1: 16 concurrent threads attacking a single key (k = 1)
void test_single_key_maximum_contention_in_memory() {
    kvstore::KVStore store(100);
    kvstore::BenchmarkConfig cfg;
    cfg.threads = 16;
    cfg.requests = 10000;
    cfg.ratio = 0.5; // 50% GET, 50% SET
    cfg.keyspace = 1; // Exactly 1 key: "key_0"
    cfg.val_size = 128;

    kvstore::Benchmark runner(cfg);
    auto res = runner.run_in_memory(store);

    ADV_ASSERT_EQ(res.completed_requests, 10000ULL);
    ADV_ASSERT_EQ(res.failed_requests, 0ULL);
    ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
    verify_monotonicity(res.latency);

    // Verify key_0 is valid and has length 128
    auto val = store.get("key_0");
    ADV_ASSERT_TRUE(val.has_value());
    ADV_ASSERT_EQ(val.value().size(), 128ULL);

    std::cout << "  [PASS] test_single_key_maximum_contention_in_memory (16 threads, 10k ops)" << std::endl;
}

// 5.2: Eviction pressure under heavy benchmark load
void test_capacity_eviction_pressure_in_memory() {
    // Capacity = 32 items total across all shards
    kvstore::KVStore store(32);
    kvstore::BenchmarkConfig cfg;
    cfg.threads = 8;
    cfg.requests = 5000;
    cfg.ratio = 0.1; // 90% SET, 10% GET
    cfg.keyspace = 500; // 500 keys >> 32 capacity
    cfg.val_size = 64;

    kvstore::Benchmark runner(cfg);
    auto res = runner.run_in_memory(store);

    ADV_ASSERT_EQ(res.completed_requests, 5000ULL);
    ADV_ASSERT_EQ(res.failed_requests, 0ULL);

    auto stats = store.get_stats();
    // Capacity bound must be strictly respected
    ADV_ASSERT_TRUE(store.size() <= 32);
    // Evictions must have occurred
    ADV_ASSERT_TRUE(stats.evictions > 0);
    verify_monotonicity(res.latency);

    std::cout << "  [PASS] test_capacity_eviction_pressure_in_memory (evictions: " << stats.evictions << ")" << std::endl;
}

// ==============================================================================
// Suite 6: Live Network Benchmark Adversarial Scenarios
// ==============================================================================

// 6.1: Live network benchmark with 0-byte and 8KB payloads
void test_network_boundary_payloads() {
    kvstore::KVStore store(500);
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 4);
    server.start();
    int port = server.port();
    ADV_ASSERT_TRUE(port > 0);

    // 0-byte payload over network
    {
        kvstore::BenchmarkConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = port;
        cfg.threads = 4;
        cfg.requests = 200;
        cfg.ratio = 0.0; // 100% SET
        cfg.keyspace = 20;
        cfg.val_size = 0; // Empty payload

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_network();

        ADV_ASSERT_EQ(res.completed_requests, 200ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);
        ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
        verify_monotonicity(res.latency);
    }

    // 8KB payload over network
    {
        kvstore::BenchmarkConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = port;
        cfg.threads = 2;
        cfg.requests = 100;
        cfg.ratio = 0.5;
        cfg.keyspace = 10;
        cfg.val_size = 8192; // 8KB payload

        kvstore::Benchmark runner(cfg);
        auto res = runner.run_network();

        ADV_ASSERT_EQ(res.completed_requests, 100ULL);
        ADV_ASSERT_EQ(res.failed_requests, 0ULL);
        ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
        verify_monotonicity(res.latency);
    }

    server.stop();
    std::cout << "  [PASS] test_network_boundary_payloads (0B & 8KB)" << std::endl;
}

// 6.2: Live network benchmark with 16 threads and single-key contention
void test_network_16_threads_single_key_contention() {
    kvstore::KVStore store(100);
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 8);
    server.start();
    int port = server.port();
    ADV_ASSERT_TRUE(port > 0);

    kvstore::BenchmarkConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.threads = 16;
    cfg.requests = 320;
    cfg.ratio = 0.5;
    cfg.keyspace = 1; // Single key contention
    cfg.val_size = 64;

    kvstore::Benchmark runner(cfg);
    auto res = runner.run_network();

    ADV_ASSERT_EQ(res.completed_requests, 320ULL);
    ADV_ASSERT_EQ(res.failed_requests, 0ULL);
    ADV_ASSERT_TRUE(res.throughput_ops_sec > 0.0);
    verify_monotonicity(res.latency);

    server.stop();
    std::cout << "  [PASS] test_network_16_threads_single_key_contention" << std::endl;
}

// 6.3: Unreachable server port error handling (pre-flight & runner)
void test_network_unreachable_server() {
    // Port 59996 is closed / unreachable
    kvstore::BenchmarkConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 59996;
    cfg.threads = 2;
    cfg.requests = 50;
    cfg.connect_timeout_sec = 0.2; // Fast timeout for test

    kvstore::Benchmark runner(cfg);
    auto res = runner.run_network();

    // Must return immediately without throwing uncaught exceptions
    ADV_ASSERT_EQ(res.completed_requests, 0ULL);
    ADV_ASSERT_EQ(res.failed_requests, 50ULL);

    std::cout << "  [PASS] test_network_unreachable_server" << std::endl;
}

// ==============================================================================
// Suite 7: Summary Reporter Banner Tokens Verification
// ==============================================================================

void test_summary_reporter_tokens() {
    kvstore::BenchmarkResult res;
    res.config.host = "127.0.0.1";
    res.config.port = 7379;
    res.config.threads = 4;
    res.config.requests = 1000;
    res.config.ratio = 0.8;
    res.config.keyspace = 100;
    res.config.val_size = 64;
    res.completed_requests = 1000;
    res.failed_requests = 0;
    res.total_duration_sec = 0.05;
    res.throughput_ops_sec = 20000.0;

    std::vector<uint64_t> sample_latencies{10, 20, 30, 40, 50, 100, 200, 500};
    res.latency = kvstore::LatencyStats::calculate(sample_latencies);

    std::ostringstream oss;
    res.print_summary(oss);
    std::string out = oss.str();

    // Verify all required tokens for E2E harnesses are present
    ADV_ASSERT_TRUE(out.find("BENCHMARK") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Target Server") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Concurrency") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Throughput") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("ops/sec") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Total Time") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("seconds") != std::string::npos || out.find(" s") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("p50") != std::string::npos || out.find("Median") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("p95") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("p99") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("p99.9") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Min") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Max") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("Average") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("us") != std::string::npos);
    ADV_ASSERT_TRUE(out.find("ms") != std::string::npos);

    std::cout << "  [PASS] test_summary_reporter_tokens" << std::endl;
}

// ==============================================================================
// Main Test Runner
// ==============================================================================

int main() {
    std::cout << "======================================================================" << std::endl;
    std::cout << "STARTING ADVERSARIAL BENCHMARK STRESS TEST SUITE" << std::endl;
    std::cout << "======================================================================" << std::endl;

    std::cout << "[Suite 1: Mathematical Rigor of Percentile Algorithms]" << std::endl;
    test_percentiles_empty();
    test_percentiles_single_sample();
    test_percentiles_identical_values();
    test_percentiles_two_samples();
    test_percentiles_three_samples();
    test_percentiles_uniform_distribution_10k();
    test_percentiles_skewed_distribution();
    test_percentiles_bimodal_distribution();
    test_percentiles_reverse_sorted();
    test_percentiles_large_values_no_overflow();

    std::cout << "\n[Suite 2: Configuration Validation and Alias Semantics]" << std::endl;
    test_config_validation_and_aliases();

    std::cout << "\n[Suite 3: Small Requests Sub-Concurrency (Division-by-Zero)]" << std::endl;
    test_small_requests_sub_concurrency();

    std::cout << "\n[Suite 4: Boundary Payload Sizes & Workload Ratios (In-Memory)]" << std::endl;
    test_boundary_payload_sizes_in_memory();
    test_boundary_workload_ratios_in_memory();

    std::cout << "\n[Suite 5: High-Concurrency Maximum Contention & Capacity Eviction]" << std::endl;
    test_single_key_maximum_contention_in_memory();
    test_capacity_eviction_pressure_in_memory();

    std::cout << "\n[Suite 6: Live Network Benchmark Adversarial Scenarios]" << std::endl;
    test_network_boundary_payloads();
    test_network_16_threads_single_key_contention();
    test_network_unreachable_server();

    std::cout << "\n[Suite 7: Summary Reporter Banner Tokens Verification]" << std::endl;
    test_summary_reporter_tokens();

    std::cout << "======================================================================" << std::endl;
    std::cout << "ALL ADVERSARIAL BENCHMARK STRESS TESTS PASSED SUCCESSFULLY" << std::endl;
    std::cout << "======================================================================" << std::endl;

    return 0;
}
