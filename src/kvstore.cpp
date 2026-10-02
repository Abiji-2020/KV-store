#include "kvstore/kvstore.hpp"

#include <functional>

namespace kvstore {

static constexpr bool is_power_of_two(size_t v) noexcept {
    return v > 0 && ((v & (v - 1)) == 0);
}

static constexpr size_t floor_power_of_two(size_t v) noexcept {
    if (v == 0) return 1;
    size_t p = 1;
    while ((p << 1) <= v) {
        p <<= 1;
    }
    return p;
}

// Exception-safe RAII MultiShardLockGuard
KVStore::MultiShardLockGuard::MultiShardLockGuard(
    const std::vector<std::unique_ptr<LruShard>>& shards)
    : shards_(shards) {
    size_t acquired = 0;
    try {
        for (size_t i = 0; i < shards_.size(); ++i) {
            shards_[i]->mutex().lock();
            acquired++;
        }
    } catch (...) {
        // Rollback all previously acquired locks in reverse order on failure
        for (size_t i = acquired; i > 0; --i) {
            shards_[i - 1]->mutex().unlock();
        }
        throw;
    }
}

KVStore::MultiShardLockGuard::~MultiShardLockGuard() {
    // Release locks in strictly descending index order [N-1 .. 0]
    for (size_t i = shards_.size(); i > 0; --i) {
        shards_[i - 1]->mutex().unlock();
    }
}

KVStore::KVStore(size_t total_capacity, size_t num_shards)
    : total_capacity_(total_capacity) {
    // If total_capacity > 0 and total_capacity < num_shards:
    // clamp shard count to floor power of 2 <= total_capacity to ensure no shard has capacity 0
    if (total_capacity_ > 0 && total_capacity_ < num_shards) {
        num_shards_ = floor_power_of_two(total_capacity_);
    } else {
        num_shards_ = is_power_of_two(num_shards) ? num_shards : DEFAULT_SHARDS;
    }

    shard_mask_ = num_shards_ - 1;
    shards_.reserve(num_shards_);

    if (total_capacity_ == 0) {
        // Unbounded store: all shards have capacity 0
        for (size_t i = 0; i < num_shards_; ++i) {
            shards_.push_back(std::make_unique<LruShard>(0));
        }
    } else {
        // Partition capacity across shards with remainder distribution
        size_t base_cap = total_capacity_ / num_shards_;
        size_t rem = total_capacity_ % num_shards_;
        for (size_t i = 0; i < num_shards_; ++i) {
            size_t shard_cap = base_cap + (i < rem ? 1 : 0);
            shards_.push_back(std::make_unique<LruShard>(shard_cap));
        }
    }
}

size_t KVStore::get_shard_index(const std::string& key) const noexcept {
    return std::hash<std::string>{}(key) & shard_mask_;
}

SetResult KVStore::set(const std::string& key, const std::string& value) {
    uint64_t seq = ++access_seq_counter_;
    size_t target_idx = get_shard_index(key);

    // Fast path: Key exists and is updated in-place (no eviction needed, size unchanged)
    {
        std::lock_guard<std::mutex> lock(shards_[target_idx]->mutex());
        if (shards_[target_idx]->exists_unlocked(key)) {
            shards_[target_idx]->set_unlocked(key, value);
            shards_[target_idx]->touch_unlocked(key, seq);
            return SetResult::UPDATED;
        }
    }

    if (total_capacity_ > 0) {
        MultiShardLockGuard guard(shards_);

        // Double check if key was inserted concurrently
        if (shards_[target_idx]->exists_unlocked(key)) {
            shards_[target_idx]->set_unlocked(key, value);
            shards_[target_idx]->touch_unlocked(key, seq);
            return SetResult::UPDATED;
        }

        size_t cur_size = 0;
        for (const auto& shard : shards_) {
            cur_size += shard->size_unlocked();
        }

        SetResult res = SetResult::CREATED;
        if (cur_size >= total_capacity_) {
            // Find global LRU shard (oldest access_seq among non-empty shards)
            size_t victim_idx = target_idx;
            uint64_t oldest_seq = UINT64_MAX;
            for (size_t s = 0; s < num_shards_; ++s) {
                if (shards_[s]->size_unlocked() > 0) {
                    uint64_t s_seq = shards_[s]->lru_access_seq_unlocked();
                    if (s_seq < oldest_seq) {
                        oldest_seq = s_seq;
                        victim_idx = s;
                    }
                }
            }
            shards_[victim_idx]->evict_lru_unlocked();
            res = SetResult::EVICTED;
        }

        shards_[target_idx]->insert_unlocked(key, value, seq);
        return res;
    }

    return shards_[target_idx]->set(key, value);
}

SetResult KVStore::set(std::string&& key, std::string&& value) {
    uint64_t seq = ++access_seq_counter_;
    size_t target_idx = get_shard_index(key);

    // Fast path: Key exists and is updated in-place (no eviction needed, size unchanged)
    {
        std::lock_guard<std::mutex> lock(shards_[target_idx]->mutex());
        if (shards_[target_idx]->exists_unlocked(key)) {
            shards_[target_idx]->set_unlocked(key, std::move(value));
            shards_[target_idx]->touch_unlocked(key, seq);
            return SetResult::UPDATED;
        }
    }

    if (total_capacity_ > 0) {
        MultiShardLockGuard guard(shards_);

        // Double check if key was inserted concurrently
        if (shards_[target_idx]->exists_unlocked(key)) {
            shards_[target_idx]->set_unlocked(key, std::move(value));
            shards_[target_idx]->touch_unlocked(key, seq);
            return SetResult::UPDATED;
        }

        size_t cur_size = 0;
        for (const auto& shard : shards_) {
            cur_size += shard->size_unlocked();
        }

        SetResult res = SetResult::CREATED;
        if (cur_size >= total_capacity_) {
            // Find global LRU shard (oldest access_seq among non-empty shards)
            size_t victim_idx = target_idx;
            uint64_t oldest_seq = UINT64_MAX;
            for (size_t s = 0; s < num_shards_; ++s) {
                if (shards_[s]->size_unlocked() > 0) {
                    uint64_t s_seq = shards_[s]->lru_access_seq_unlocked();
                    if (s_seq < oldest_seq) {
                        oldest_seq = s_seq;
                        victim_idx = s;
                    }
                }
            }
            shards_[victim_idx]->evict_lru_unlocked();
            res = SetResult::EVICTED;
        }

        shards_[target_idx]->insert_unlocked(std::move(key), std::move(value), seq);
        return res;
    }

    return shards_[target_idx]->set(std::move(key), std::move(value));
}

std::optional<std::string> KVStore::get(const std::string& key) {
    size_t idx = get_shard_index(key);
    uint64_t seq = ++access_seq_counter_;
    std::lock_guard<std::mutex> lock(shards_[idx]->mutex());
    auto res = shards_[idx]->get_unlocked(key);
    if (res.has_value()) {
        shards_[idx]->touch_unlocked(key, seq);
    }
    return res;
}

DelResult KVStore::del(const std::string& key) {
    size_t idx = get_shard_index(key);
    return shards_[idx]->del(key);
}

bool KVStore::exists(const std::string& key) const {
    size_t idx = get_shard_index(key);
    return shards_[idx]->exists(key);
}

void KVStore::clear() {
    MultiShardLockGuard guard(shards_);
    for (auto& shard : shards_) {
        shard->clear_unlocked();
    }
}

size_t KVStore::size() const {
    MultiShardLockGuard guard(shards_);
    size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->size_unlocked();
    }
    return total;
}

size_t KVStore::capacity() const noexcept {
    return total_capacity_;
}

Stats KVStore::get_stats() const {
    MultiShardLockGuard guard(shards_);
    Stats stats;
    stats.capacity = total_capacity_;
    for (const auto& shard : shards_) {
        Stats s = shard->get_stats_unlocked();
        stats.key_count += s.key_count;
        stats.hits += s.hits;
        stats.misses += s.misses;
        stats.evictions += s.evictions;
    }
    return stats;
}

size_t KVStore::num_shards() const noexcept {
    return num_shards_;
}

size_t KVStore::shard_capacity(size_t shard_idx) const {
    if (shard_idx >= shards_.size()) {
        return 0;
    }
    return shards_[shard_idx]->capacity();
}

} // namespace kvstore
