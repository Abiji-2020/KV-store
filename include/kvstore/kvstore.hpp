#pragma once

#include "kvstore/lru_shard.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

/// High-performance concurrent in-memory key-value store coordinator.
/// Partitions keys across N = 32 cache-line aligned independent LruShard instances.
/// Implements Dijkstra's ascending-index lock hierarchy for deadlock-free global operations.
class KVStore {
public:
    static constexpr size_t DEFAULT_SHARDS = 32;

    /// Constructs KVStore with specified total capacity and shard count (default 32)
    explicit KVStore(size_t total_capacity = 0, size_t num_shards = DEFAULT_SHARDS);
    ~KVStore() = default;

    // Non-copyable and non-movable due to synchronization primitives
    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;
    KVStore(KVStore&&) = delete;
    KVStore& operator=(KVStore&&) = delete;

    // Core Point Operations (Single-Shard Mutex Acquisition)
    SetResult set(const std::string& key, const std::string& value);
    SetResult set(std::string&& key, std::string&& value);
    std::optional<std::string> get(const std::string& key);
    DelResult del(const std::string& key);
    bool exists(const std::string& key) const;

    // Global Multi-Shard Operations (Deadlock-Free Ascending Lock Acquisition [0..N-1])
    void clear();
    size_t size() const;
    size_t capacity() const noexcept;
    Stats get_stats() const;

    // Inspection & Telemetry
    size_t num_shards() const noexcept;
    size_t shard_capacity(size_t shard_idx) const;

    // Single-instruction bitwise key routing: Hash(key) & (N - 1)
    size_t get_shard_index(const std::string& key) const noexcept;

private:
    // RAII guard for acquiring all shard locks in strictly ascending index order [0..N-1]
    // and releasing in descending order [N-1..0]
    class MultiShardLockGuard {
    public:
        explicit MultiShardLockGuard(const std::vector<std::unique_ptr<LruShard>>& shards);
        ~MultiShardLockGuard();

        MultiShardLockGuard(const MultiShardLockGuard&) = delete;
        MultiShardLockGuard& operator=(const MultiShardLockGuard&) = delete;

    private:
        const std::vector<std::unique_ptr<LruShard>>& shards_;
    };

    size_t total_capacity_{0};
    size_t num_shards_{DEFAULT_SHARDS};
    size_t shard_mask_{DEFAULT_SHARDS - 1};
    mutable std::atomic<uint64_t> access_seq_counter_{0};
    std::vector<std::unique_ptr<LruShard>> shards_;
};

} // namespace kvstore
