#include "kvstore/benchmark.hpp"
#include "kvstore/kvstore.hpp"
#include "kvstore/server.hpp"

#include <cassert>
#include <iostream>
#include <numeric>
#include <vector>

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        std::cerr << "Assertion failed: " #cond " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        std::exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        std::cerr << "Assertion failed: " #a " == " #b " at " << __FILE__ << ":" << __LINE__ \
                  << " [Actual: '" << (a) << "' vs Expected: '" << (b) << "']" << std::endl; \
        std::exit(1); \
    } \
} while(0)

void test_percentile_math_synthetic() {
    // 1000 items: 1, 2, ..., 1000
    std::vector<uint64_t> latencies(1000);
    std::iota(latencies.begin(), latencies.end(), 1);

    auto stats = kvstore::LatencyStats::calculate(latencies);

    ASSERT_EQ(stats.min_us, 1ULL);
    ASSERT_EQ(stats.max_us, 1000ULL);
    ASSERT_EQ(stats.p50_us, 500ULL);
    ASSERT_EQ(stats.p95_us, 950ULL);
    ASSERT_EQ(stats.p99_us, 990ULL);
    ASSERT_EQ(stats.p999_us, 999ULL);
    ASSERT_TRUE(stats.avg_us >= 500.0 && stats.avg_us <= 501.0);

    // Monotonicity verification: min <= p50 <= p95 <= p99 <= p99.9 <= max
    ASSERT_TRUE(stats.min_us <= stats.p50_us);
    ASSERT_TRUE(stats.p50_us <= stats.p95_us);
    ASSERT_TRUE(stats.p95_us <= stats.p99_us);
    ASSERT_TRUE(stats.p99_us <= stats.p999_us);
    ASSERT_TRUE(stats.p999_us <= stats.max_us);

    std::cout << "[PASS] test_percentile_math_synthetic" << std::endl;
}

void test_percentile_math_boundaries() {
    // Single item
    {
        std::vector<uint64_t> single{42};
        auto s = kvstore::LatencyStats::calculate(single);
        ASSERT_EQ(s.min_us, 42ULL);
        ASSERT_EQ(s.max_us, 42ULL);
        ASSERT_EQ(s.p50_us, 42ULL);
        ASSERT_EQ(s.p95_us, 42ULL);
        ASSERT_EQ(s.p99_us, 42ULL);
        ASSERT_EQ(s.p999_us, 42ULL);
    }
    // Two items
    {
        std::vector<uint64_t> two{10, 20};
        auto s = kvstore::LatencyStats::calculate(two);
        ASSERT_EQ(s.min_us, 10ULL);
        ASSERT_EQ(s.max_us, 20ULL);
        ASSERT_TRUE(s.min_us <= s.p50_us && s.p50_us <= s.p95_us && s.p95_us <= s.max_us);
    }
    // Constant distribution
    {
        std::vector<uint64_t> constant(50, 100);
        auto s = kvstore::LatencyStats::calculate(constant);
        ASSERT_EQ(s.min_us, 100ULL);
        ASSERT_EQ(s.max_us, 100ULL);
        ASSERT_EQ(s.p50_us, 100ULL);
        ASSERT_EQ(s.p95_us, 100ULL);
        ASSERT_EQ(s.p99_us, 100ULL);
    }
    // Empty vector
    {
        std::vector<uint64_t> empty;
        auto s = kvstore::LatencyStats::calculate(empty);
        ASSERT_EQ(s.min_us, 0ULL);
        ASSERT_EQ(s.max_us, 0ULL);
    }

    std::cout << "[PASS] test_percentile_math_boundaries" << std::endl;
}

void test_config_validation() {
    kvstore::BenchmarkConfig cfg;
    std::string err;

    ASSERT_TRUE(cfg.validate(err));

    cfg.threads = 0;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.threads = 4;

    cfg.requests = 0;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.requests = 1000;

    cfg.ratio = -0.1;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.ratio = 1.1;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.ratio = 0.5;

    cfg.read_ratio = -0.2;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.read_ratio = 1.2;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.read_ratio = 0.75;
    ASSERT_TRUE(cfg.validate(err));
    ASSERT_TRUE(cfg.ratio == 0.75);

    cfg.keyspace = 0;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.keyspace = 100;

    cfg.val_size = 0;
    ASSERT_TRUE(cfg.validate(err)); // 0 byte payload is explicitly valid

    cfg.port = 0;
    ASSERT_TRUE(!cfg.validate(err));
    cfg.port = 7379;

    std::cout << "[PASS] test_config_validation" << std::endl;
}

void test_in_memory_engine_benchmark() {
    kvstore::KVStore store(5000);
    kvstore::BenchmarkConfig cfg;
    cfg.threads = 4;
    cfg.requests = 2000;
    cfg.ratio = 0.5;
    cfg.keyspace = 100;
    cfg.val_size = 32;

    kvstore::Benchmark runner(cfg);
    auto res = runner.run_in_memory(store);

    ASSERT_EQ(res.completed_requests, 2000ULL);
    ASSERT_EQ(res.failed_requests, 0ULL);
    ASSERT_TRUE(res.elapsed_sec > 0.0);
    ASSERT_TRUE(res.throughput_ops_sec > 0.0);
    ASSERT_TRUE(res.min_latency_us <= res.p50_latency_us);
    ASSERT_TRUE(res.p50_latency_us <= res.p95_latency_us);
    ASSERT_TRUE(res.p95_latency_us <= res.p99_latency_us);
    ASSERT_TRUE(res.p99_latency_us <= res.p999_latency_us);
    ASSERT_TRUE(res.p999_latency_us <= res.max_latency_us);

    std::cout << "[PASS] test_in_memory_engine_benchmark" << std::endl;
}

void test_network_local_server_benchmark() {
    kvstore::KVStore store;
    kvstore::Server server(store, nullptr, "127.0.0.1", 0, 4);
    server.start();

    int port = server.port();
    ASSERT_TRUE(port > 0);

    kvstore::BenchmarkConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.threads = 4;
    cfg.requests = 400;
    cfg.ratio = 0.8;
    cfg.keyspace = 50;
    cfg.val_size = 64;

    kvstore::Benchmark runner(cfg);
    auto res = runner.run_network();

    ASSERT_EQ(res.completed_requests, 400ULL);
    ASSERT_EQ(res.failed_requests, 0ULL);
    ASSERT_TRUE(res.elapsed_sec > 0.0);
    ASSERT_TRUE(res.throughput_ops_sec > 0.0);
    ASSERT_TRUE(res.min_latency_us <= res.p50_latency_us);
    ASSERT_TRUE(res.p50_latency_us <= res.p95_latency_us);
    ASSERT_TRUE(res.p95_latency_us <= res.p99_latency_us);
    ASSERT_TRUE(res.p99_latency_us <= res.p999_latency_us);
    ASSERT_TRUE(res.p999_latency_us <= res.max_latency_us);

    server.stop();
    std::cout << "[PASS] test_network_local_server_benchmark" << std::endl;
}

int main() {
    std::cout << "==================================================" << std::endl;
    std::cout << "RUNNING BENCHMARK UNIT TESTS" << std::endl;
    std::cout << "==================================================" << std::endl;

    test_percentile_math_synthetic();
    test_percentile_math_boundaries();
    test_config_validation();
    test_in_memory_engine_benchmark();
    test_network_local_server_benchmark();

    std::cout << "==================================================" << std::endl;
    std::cout << "ALL BENCHMARK UNIT TESTS PASSED" << std::endl;
    std::cout << "==================================================" << std::endl;
    return 0;
}
