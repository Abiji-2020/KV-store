#include "kvstore/benchmark.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <latch>
#include <numeric>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

namespace kvstore {

bool BenchmarkConfig::validate(std::string& err_msg) const {
    if (threads == 0) {
        err_msg = "Thread count (-c, --threads) must be greater than 0";
        return false;
    }
    if (requests == 0) {
        err_msg = "Request count (-n, --requests) must be greater than 0";
        return false;
    }
    if (ratio < 0.0 || ratio > 1.0) {
        err_msg = "Read ratio (-r, --ratio) must be between 0.0 and 1.0";
        return false;
    }
    if (keyspace == 0) {
        err_msg = "Keyspace size (-k, --keys) must be greater than 0";
        return false;
    }
    if (port <= 0 || port > 65535) {
        err_msg = "Port (-p, --port) must be between 1 and 65535";
        return false;
    }
    return true;
}

LatencyStats LatencyStats::calculate(std::vector<uint64_t>& latencies) {
    LatencyStats stats;
    if (latencies.empty()) {
        return stats;
    }

    std::sort(latencies.begin(), latencies.end());
    const size_t n = latencies.size();

    stats.min_us = latencies.front();
    stats.max_us = latencies.back();

    uint64_t sum = std::accumulate(latencies.begin(), latencies.end(), 0ULL);
    stats.avg_us = static_cast<double>(sum) / static_cast<double>(n);

    auto rank_percentile = [&](double p) -> uint64_t {
        size_t k = static_cast<size_t>(std::ceil((p * static_cast<double>(n)) / 100.0));
        size_t idx = (k == 0) ? 0 : k - 1;
        if (idx >= n) idx = n - 1;
        return latencies[idx];
    };

    stats.p50_us = rank_percentile(50.0);
    stats.p95_us = rank_percentile(95.0);
    stats.p99_us = rank_percentile(99.0);
    stats.p999_us = rank_percentile(99.9);

    // Safeguard mathematical monotonicity: min <= p50 <= p95 <= p99 <= p99.9 <= max
    if (stats.p50_us < stats.min_us) stats.p50_us = stats.min_us;
    if (stats.p95_us < stats.p50_us) stats.p95_us = stats.p50_us;
    if (stats.p99_us < stats.p95_us) stats.p99_us = stats.p95_us;
    if (stats.p999_us < stats.p99_us) stats.p999_us = stats.p99_us;
    if (stats.max_us < stats.p999_us) stats.max_us = stats.p999_us;

    return stats;
}

void BenchmarkResult::print_summary(std::ostream& os) const {
    os << "================================================================================\n"
       << "                         BENCHMARK RESULTS\n"
       << "================================================================================\n"
       << "Target Server:         " << config.host << ":" << config.port << "\n"
       << "Concurrency:           " << config.threads << " threads\n"
       << "Total Requests:        " << config.requests << "\n"
       << std::fixed << std::setprecision(2)
       << "Read / Write Ratio:    " << config.ratio << " ("
       << (config.ratio * 100.0) << "% GET / " << ((1.0 - config.ratio) * 100.0) << "% SET)\n"
       << "Keyspace:              " << config.keyspace << " keys\n"
       << "Value Size:            " << config.val_size << " bytes\n"
       << "--------------------------------------------------------------------------------\n"
       << "EXECUTION RESULTS\n"
       << "--------------------------------------------------------------------------------\n"
       << "Total Completed:       " << completed_requests << " requests\n"
       << "Total Errors:          " << failed_requests << "\n"
       << std::fixed << std::setprecision(6)
       << "Total Time (elapsed):  " << total_duration_sec << " seconds (s)\n"
       << std::fixed << std::setprecision(2)
       << "Throughput:            " << throughput_ops_sec << " ops/sec\n\n"
       << "LATENCY PERCENTILES:\n"
       << "--------------------------------------------------------------------------------\n"
       << "  Metric               Latency (us)         Latency (ms)\n"
       << "--------------------------------------------------------------------------------\n"
       << "  Min (min):           " << std::setw(12) << latency.min_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.min_us / 1000.0) << " ms\n"
       << "  Average (avg):       " << std::fixed << std::setprecision(1) << std::setw(12) << latency.avg_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.avg_us / 1000.0) << " ms\n"
       << "  Median (p50):        " << std::setw(12) << latency.p50_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.p50_us / 1000.0) << " ms\n"
       << "  p95:                 " << std::setw(12) << latency.p95_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.p95_us / 1000.0) << " ms\n"
       << "  p99:                 " << std::setw(12) << latency.p99_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.p99_us / 1000.0) << " ms\n"
       << "  p99.9:               " << std::setw(12) << latency.p999_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.p999_us / 1000.0) << " ms\n"
       << "  Max (max):           " << std::setw(12) << latency.max_us << " us       "
       << std::fixed << std::setprecision(3) << std::setw(10) << (latency.max_us / 1000.0) << " ms\n"
       << "================================================================================\n";
}

BenchmarkRunner::BenchmarkRunner(BenchmarkConfig config) : config_(std::move(config)) {}

BenchmarkResult BenchmarkRunner::run_network() {
    BenchmarkResult result;
    result.config = config_;

    std::string err;
    if (!config_.validate(err)) {
        throw std::invalid_argument(err);
    }

    // Pre-generate key table and value payload
    std::vector<std::string> keys(config_.keyspace);
    for (size_t i = 0; i < config_.keyspace; ++i) {
        keys[i] = "key_" + std::to_string(i);
    }
    std::string payload(config_.val_size, 'x');

    const size_t num_threads = config_.threads;
    const size_t total_req = config_.requests;
    const size_t base_req = total_req / num_threads;
    const size_t rem_req = total_req % num_threads;

    std::vector<std::vector<uint64_t>> thread_latencies(num_threads);
    std::vector<size_t> thread_errors(num_threads, 0);
    std::atomic<bool> connect_failed{false};

    std::latch start_latch(static_cast<std::ptrdiff_t>(num_threads + 1));
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        size_t my_requests = base_req + (t < rem_req ? 1 : 0);
        thread_latencies[t].reserve(my_requests);

        workers.emplace_back([this, t, my_requests, &keys, &payload,
                              &thread_latencies, &thread_errors, &start_latch, &connect_failed]() {
            Client client(config_.host, config_.port, config_.connect_timeout_sec);
            if (!client.connect(3, 50)) {
                connect_failed.store(true, std::memory_order_relaxed);
                start_latch.arrive_and_wait();
                return;
            }

            start_latch.arrive_and_wait();
            if (connect_failed.load(std::memory_order_relaxed)) {
                return;
            }

            std::mt19937_64 rng(1337 + t * 10007);
            std::uniform_int_distribution<size_t> key_dist(0, config_.keyspace - 1);
            std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);

            for (size_t i = 0; i < my_requests; ++i) {
                const std::string& key = keys[key_dist(rng)];
                bool is_read;
                if (config_.ratio >= 1.0) {
                    is_read = true;
                } else if (config_.ratio <= 0.0) {
                    is_read = false;
                } else {
                    is_read = (ratio_dist(rng) < config_.ratio);
                }

                auto t0 = std::chrono::steady_clock::now();
                std::string resp;
                if (is_read) {
                    resp = client.get(key);
                } else {
                    if (payload.empty()) {
                        resp = client.send_command("SET " + key + " \"\"");
                    } else {
                        resp = client.set(key, payload);
                    }
                }
                auto t1 = std::chrono::steady_clock::now();

                if (resp.empty() || resp == "-ERR connection lost\r\n" || resp == "-ERR connection closed\r\n") {
                    thread_errors[t]++;
                } else {
                    uint64_t lat_us = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
                    thread_latencies[t].push_back(lat_us);
                }
            }
        });
    }

    // Wait for all workers to connect and arrive at barrier
    start_latch.arrive_and_wait();
    auto wall_start = std::chrono::steady_clock::now();

    for (auto& th : workers) {
        th.join();
    }
    auto wall_stop = std::chrono::steady_clock::now();

    if (connect_failed.load(std::memory_order_relaxed)) {
        result.failed_requests = config_.requests;
        result.total_requests = config_.requests;
        return result;
    }

    double elapsed = std::chrono::duration<double>(wall_stop - wall_start).count();
    if (elapsed <= 0.0) {
        elapsed = 0.000001; // Avoid division by zero
    }
    result.total_duration_sec = elapsed;
    result.elapsed_sec = elapsed;

    for (size_t t = 0; t < num_threads; ++t) {
        result.all_latencies.insert(result.all_latencies.end(),
                                    thread_latencies[t].begin(), thread_latencies[t].end());
        result.failed_requests += thread_errors[t];
    }
    result.latencies_us = result.all_latencies;
    result.completed_requests = result.all_latencies.size();
    result.total_requests = config_.requests;

    if (result.completed_requests > 0 && result.total_duration_sec > 0.0) {
        result.throughput_ops_sec = static_cast<double>(result.completed_requests) / result.total_duration_sec;
    } else {
        result.throughput_ops_sec = 0.0;
    }

    result.latency = LatencyStats::calculate(result.all_latencies);
    result.min_latency_us = result.latency.min_us;
    result.avg_latency_us = result.latency.avg_us;
    result.max_latency_us = result.latency.max_us;
    result.p50_latency_us = result.latency.p50_us;
    result.p95_latency_us = result.latency.p95_us;
    result.p99_latency_us = result.latency.p99_us;
    result.p999_latency_us = result.latency.p999_us;

    return result;
}

BenchmarkResult BenchmarkRunner::run_in_memory(KVStore& store) {
    BenchmarkResult result;
    result.config = config_;

    std::string err;
    if (!config_.validate(err)) {
        throw std::invalid_argument(err);
    }

    std::vector<std::string> keys(config_.keyspace);
    for (size_t i = 0; i < config_.keyspace; ++i) {
        keys[i] = "key_" + std::to_string(i);
    }
    std::string payload(config_.val_size, 'x');

    const size_t num_threads = config_.threads;
    const size_t total_req = config_.requests;
    const size_t base_req = total_req / num_threads;
    const size_t rem_req = total_req % num_threads;

    std::vector<std::vector<uint64_t>> thread_latencies(num_threads);
    std::latch start_latch(static_cast<std::ptrdiff_t>(num_threads + 1));
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        size_t my_requests = base_req + (t < rem_req ? 1 : 0);
        thread_latencies[t].reserve(my_requests);

        workers.emplace_back([this, t, my_requests, &store, &keys, &payload,
                              &thread_latencies, &start_latch]() {
            start_latch.arrive_and_wait();

            std::mt19937_64 rng(1337 + t * 10007);
            std::uniform_int_distribution<size_t> key_dist(0, config_.keyspace - 1);
            std::uniform_real_distribution<double> ratio_dist(0.0, 1.0);

            for (size_t i = 0; i < my_requests; ++i) {
                const std::string& key = keys[key_dist(rng)];
                bool is_read;
                if (config_.ratio >= 1.0) {
                    is_read = true;
                } else if (config_.ratio <= 0.0) {
                    is_read = false;
                } else {
                    is_read = (ratio_dist(rng) < config_.ratio);
                }

                auto t0 = std::chrono::steady_clock::now();
                if (is_read) {
                    [[maybe_unused]] auto val = store.get(key);
                } else {
                    store.set(key, payload);
                }
                auto t1 = std::chrono::steady_clock::now();

                uint64_t lat_us = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
                thread_latencies[t].push_back(lat_us);
            }
        });
    }

    start_latch.arrive_and_wait();
    auto wall_start = std::chrono::steady_clock::now();

    for (auto& th : workers) {
        th.join();
    }
    auto wall_stop = std::chrono::steady_clock::now();

    double elapsed = std::chrono::duration<double>(wall_stop - wall_start).count();
    if (elapsed <= 0.0) {
        elapsed = 0.000001; // Avoid division by zero
    }
    result.total_duration_sec = elapsed;
    result.elapsed_sec = elapsed;

    for (size_t t = 0; t < num_threads; ++t) {
        result.all_latencies.insert(result.all_latencies.end(),
                                    thread_latencies[t].begin(), thread_latencies[t].end());
    }
    result.latencies_us = result.all_latencies;
    result.completed_requests = result.all_latencies.size();
    result.total_requests = config_.requests;

    if (result.completed_requests > 0 && result.total_duration_sec > 0.0) {
        result.throughput_ops_sec = static_cast<double>(result.completed_requests) / result.total_duration_sec;
    } else {
        result.throughput_ops_sec = 0.0;
    }

    result.latency = LatencyStats::calculate(result.all_latencies);
    result.min_latency_us = result.latency.min_us;
    result.avg_latency_us = result.latency.avg_us;
    result.max_latency_us = result.latency.max_us;
    result.p50_latency_us = result.latency.p50_us;
    result.p95_latency_us = result.latency.p95_us;
    result.p99_latency_us = result.latency.p99_us;
    result.p999_latency_us = result.latency.p999_us;

    return result;
}

} // namespace kvstore
