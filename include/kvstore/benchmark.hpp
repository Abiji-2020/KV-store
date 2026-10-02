#pragma once

#include "kvstore/client.hpp"
#include "kvstore/kvstore.hpp"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace kvstore {

/// Configuration parameters for benchmark execution.
struct BenchmarkConfig {
    std::string host{"127.0.0.1"};
    int port{7379};
    size_t threads{4};
    size_t requests{1000};
    double ratio{0.8};       // Read ratio [0.0, 1.0]: 1.0 = 100% GET, 0.0 = 100% SET
    double& read_ratio;      // Direct alias reference to ratio
    size_t keyspace{100};    // Range of distinct keys [0, keyspace)
    size_t val_size{64};     // SET payload size in bytes
    double connect_timeout_sec{3.0};

    BenchmarkConfig() : read_ratio(ratio) {}

    BenchmarkConfig(const BenchmarkConfig& other)
        : host(other.host), port(other.port), threads(other.threads),
          requests(other.requests), ratio(other.ratio), read_ratio(ratio),
          keyspace(other.keyspace), val_size(other.val_size),
          connect_timeout_sec(other.connect_timeout_sec) {}

    BenchmarkConfig& operator=(const BenchmarkConfig& other) {
        if (this != &other) {
            host = other.host;
            port = other.port;
            threads = other.threads;
            requests = other.requests;
            ratio = other.ratio;
            keyspace = other.keyspace;
            val_size = other.val_size;
            connect_timeout_sec = other.connect_timeout_sec;
        }
        return *this;
    }

    BenchmarkConfig(BenchmarkConfig&& other) noexcept
        : host(std::move(other.host)), port(other.port), threads(other.threads),
          requests(other.requests), ratio(other.ratio), read_ratio(ratio),
          keyspace(other.keyspace), val_size(other.val_size),
          connect_timeout_sec(other.connect_timeout_sec) {}

    BenchmarkConfig& operator=(BenchmarkConfig&& other) noexcept {
        if (this != &other) {
            host = std::move(other.host);
            port = other.port;
            threads = other.threads;
            requests = other.requests;
            ratio = other.ratio;
            keyspace = other.keyspace;
            val_size = other.val_size;
            connect_timeout_sec = other.connect_timeout_sec;
        }
        return *this;
    }

    /// Validates configuration parameters. Returns true if valid, false otherwise.
    bool validate(std::string& err_msg) const;
};

/// Exact latency metrics in microseconds.
struct LatencyStats {
    uint64_t min_us{0};
    double avg_us{0.0};
    uint64_t p50_us{0};
    uint64_t p95_us{0};
    uint64_t p99_us{0};
    uint64_t p999_us{0};
    uint64_t max_us{0};

    /// Computes percentiles from latency samples.
    static LatencyStats calculate(std::vector<uint64_t>& latencies);
};

/// Aggregated benchmark execution results.
struct BenchmarkResult {
    BenchmarkConfig config;
    size_t total_requests{0};
    size_t completed_requests{0};
    size_t failed_requests{0};
    double total_duration_sec{0.0};
    double elapsed_sec{0.0};            // Alias for total_duration_sec
    double throughput_ops_sec{0.0};
    std::vector<uint64_t> latencies_us;
    std::vector<uint64_t> all_latencies; // Alias for latencies_us
    uint64_t min_latency_us{0};
    double avg_latency_us{0.0};
    uint64_t max_latency_us{0};
    uint64_t p50_latency_us{0};
    uint64_t p95_latency_us{0};
    uint64_t p99_latency_us{0};
    uint64_t p999_latency_us{0};
    LatencyStats latency;

    /// Formats results into structured tabular terminal report.
    void print_summary(std::ostream& os = std::cout) const;
};

/// High-performance multi-threaded load generator and benchmark coordinator.
class BenchmarkRunner {
public:
    explicit BenchmarkRunner(BenchmarkConfig config);

    /// Runs network benchmark against running TCP kv_server.
    BenchmarkResult run_network();

    /// Runs direct in-memory benchmark against KVStore (without TCP network overhead).
    BenchmarkResult run_in_memory(KVStore& store);

private:
    BenchmarkConfig config_;
};

/// Convenient alias for BenchmarkRunner.
using Benchmark = BenchmarkRunner;

} // namespace kvstore
