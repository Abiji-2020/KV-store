#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace kvstore {

/// Status result for SET operations
enum class SetResult {
    CREATED,  ///< Key was newly inserted within capacity limits
    UPDATED,  ///< Existing key was updated in-place and moved to MRU head
    EVICTED   ///< Capacity was exceeded; LRU tail was evicted to make space for the new key
};

/// Status result for DEL operations
enum class DelResult {
    DELETED,   ///< Key existed and was removed from list and hash table
    NOT_FOUND  ///< Key did not exist in shard
};

/// Telemetry and point-in-time statistics
struct Stats {
    size_t key_count{0};    ///< Current number of live keys
    size_t capacity{0};     ///< Maximum capacity limit (0 = unbounded)
    uint64_t hits{0};       ///< Number of successful get() hits
    uint64_t misses{0};     ///< Number of failed get() misses
    uint64_t evictions{0};  ///< Number of keys evicted due to capacity bounds

    bool operator==(const Stats& other) const = default;
};

inline std::ostream& operator<<(std::ostream& os, SetResult res) {
    switch (res) {
        case SetResult::CREATED: return os << "SetResult::CREATED";
        case SetResult::UPDATED: return os << "SetResult::UPDATED";
        case SetResult::EVICTED: return os << "SetResult::EVICTED";
    }
    return os << "SetResult::UNKNOWN";
}

inline std::ostream& operator<<(std::ostream& os, DelResult res) {
    switch (res) {
        case DelResult::DELETED:   return os << "DelResult::DELETED";
        case DelResult::NOT_FOUND: return os << "DelResult::NOT_FOUND";
    }
    return os << "DelResult::UNKNOWN";
}

/// Cache-line aligned (alignas(64)) LRU shard with O(1) recency promotion and eviction.
/// Dual API pattern supports both self-locking public operations and caller-locked _unlocked operations.
class alignas(64) LruShard {
public:
    struct Entry {
        std::string key;
        std::string value;
        uint64_t access_seq{0};
    };

    explicit LruShard(size_t capacity = 0);
    ~LruShard() = default;

    // Non-copyable and non-movable due to internal std::mutex
    LruShard(const LruShard&) = delete;
    LruShard& operator=(const LruShard&) = delete;
    LruShard(LruShard&&) = delete;
    LruShard& operator=(LruShard&&) = delete;

    // --- Thread-Safe Public API (acquires mtx_) ---
    SetResult set(const std::string& key, const std::string& value);
    SetResult set(std::string&& key, std::string&& value);
    std::optional<std::string> get(const std::string& key);
    DelResult del(const std::string& key);
    bool exists(const std::string& key) const;
    void clear();
    size_t size() const;
    size_t capacity() const noexcept;
    void set_capacity(size_t cap);
    Stats get_stats() const;
    void reset_stats();
    std::vector<std::pair<std::string, std::string>> get_entries_mru_order() const;

    // --- Shard Mutex Access for Global Multi-Shard Operations ---
    [[nodiscard]] std::mutex& mutex() const noexcept { return mtx_; }

    // --- Unlocked API (Caller MUST hold mutex()) ---
    SetResult set_unlocked(const std::string& key, const std::string& value);
    SetResult set_unlocked(std::string&& key, std::string&& value);
    std::optional<std::string> get_unlocked(const std::string& key);
    DelResult del_unlocked(const std::string& key);
    bool exists_unlocked(const std::string& key) const;
    void clear_unlocked();
    size_t size_unlocked() const noexcept;
    size_t capacity_unlocked() const noexcept;
    void set_capacity_unlocked(size_t cap);
    Stats get_stats_unlocked() const noexcept;
    void reset_stats_unlocked() noexcept;
    std::vector<std::pair<std::string, std::string>> get_entries_mru_order_unlocked() const;
    uint64_t lru_access_seq_unlocked() const noexcept;
    bool evict_lru_unlocked();
    void insert_unlocked(const std::string& key, const std::string& value, uint64_t seq);
    void insert_unlocked(std::string&& key, std::string&& value, uint64_t seq);
    bool touch_unlocked(const std::string& key, uint64_t seq);

private:
    mutable std::mutex mtx_;
    std::list<Entry> lru_list_;
    std::unordered_map<std::string, std::list<Entry>::iterator> table_;
    size_t capacity_{0};
    uint64_t hits_{0};
    uint64_t misses_{0};
    uint64_t evictions_{0};
};

} // namespace kvstore
