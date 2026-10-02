#include "kvstore/lru_shard.hpp"

namespace kvstore {

LruShard::LruShard(size_t capacity) : capacity_(capacity) {}

// ============================================================================
// Thread-Safe Public API
// ============================================================================

SetResult LruShard::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx_);
    return set_unlocked(key, value);
}

SetResult LruShard::set(std::string&& key, std::string&& value) {
    std::lock_guard<std::mutex> lock(mtx_);
    return set_unlocked(std::move(key), std::move(value));
}

std::optional<std::string> LruShard::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mtx_);
    return get_unlocked(key);
}

DelResult LruShard::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(mtx_);
    return del_unlocked(key);
}

bool LruShard::exists(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mtx_);
    return exists_unlocked(key);
}

void LruShard::clear() {
    std::lock_guard<std::mutex> lock(mtx_);
    clear_unlocked();
}

size_t LruShard::size() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return size_unlocked();
}

size_t LruShard::capacity() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    return capacity_unlocked();
}

void LruShard::set_capacity(size_t cap) {
    std::lock_guard<std::mutex> lock(mtx_);
    set_capacity_unlocked(cap);
}

Stats LruShard::get_stats() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return get_stats_unlocked();
}

void LruShard::reset_stats() {
    std::lock_guard<std::mutex> lock(mtx_);
    reset_stats_unlocked();
}

std::vector<std::pair<std::string, std::string>> LruShard::get_entries_mru_order() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return get_entries_mru_order_unlocked();
}

// ============================================================================
// Unlocked API (Caller MUST hold mutex())
// ============================================================================

SetResult LruShard::set_unlocked(const std::string& key, const std::string& value) {
    auto it = table_.find(key);
    if (it != table_.end()) {
        it->second->value = value;
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return SetResult::UPDATED;
    }

    SetResult result = SetResult::CREATED;
    if (capacity_ > 0 && table_.size() >= capacity_) {
        const std::string& evict_key = lru_list_.back().key;
        table_.erase(evict_key);
        lru_list_.pop_back();
        ++evictions_;
        result = SetResult::EVICTED;
    }

    lru_list_.push_front(Entry{key, value});
    try {
        table_.emplace(key, lru_list_.begin());
    } catch (...) {
        lru_list_.pop_front();
        throw;
    }
    return result;
}

SetResult LruShard::set_unlocked(std::string&& key, std::string&& value) {
    auto it = table_.find(key);
    if (it != table_.end()) {
        it->second->value = std::move(value);
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return SetResult::UPDATED;
    }

    SetResult result = SetResult::CREATED;
    if (capacity_ > 0 && table_.size() >= capacity_) {
        const std::string& evict_key = lru_list_.back().key;
        table_.erase(evict_key);
        lru_list_.pop_back();
        ++evictions_;
        result = SetResult::EVICTED;
    }

    std::string key_copy = key;
    lru_list_.push_front(Entry{std::move(key), std::move(value)});
    try {
        table_.emplace(std::move(key_copy), lru_list_.begin());
    } catch (...) {
        lru_list_.pop_front();
        throw;
    }
    return result;
}

std::optional<std::string> LruShard::get_unlocked(const std::string& key) {
    auto it = table_.find(key);
    if (it == table_.end()) {
        ++misses_;
        return std::nullopt;
    }
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    ++hits_;
    return it->second->value;
}

DelResult LruShard::del_unlocked(const std::string& key) {
    auto it = table_.find(key);
    if (it == table_.end()) {
        return DelResult::NOT_FOUND;
    }
    lru_list_.erase(it->second);
    table_.erase(it);
    return DelResult::DELETED;
}

bool LruShard::exists_unlocked(const std::string& key) const {
    return table_.find(key) != table_.end();
}

void LruShard::clear_unlocked() {
    table_.clear();
    lru_list_.clear();
}

size_t LruShard::size_unlocked() const noexcept {
    return table_.size();
}

size_t LruShard::capacity_unlocked() const noexcept {
    return capacity_;
}

void LruShard::set_capacity_unlocked(size_t cap) {
    capacity_ = cap;
    if (capacity_ > 0) {
        while (table_.size() > capacity_) {
            const std::string& evict_key = lru_list_.back().key;
            table_.erase(evict_key);
            lru_list_.pop_back();
            ++evictions_;
        }
    }
}

Stats LruShard::get_stats_unlocked() const noexcept {
    return Stats{
        .key_count = table_.size(),
        .capacity = capacity_,
        .hits = hits_,
        .misses = misses_,
        .evictions = evictions_
    };
}

void LruShard::reset_stats_unlocked() noexcept {
    hits_ = 0;
    misses_ = 0;
    evictions_ = 0;
}

std::vector<std::pair<std::string, std::string>> LruShard::get_entries_mru_order_unlocked() const {
    std::vector<std::pair<std::string, std::string>> result;
    result.reserve(lru_list_.size());
    for (const auto& entry : lru_list_) {
        result.emplace_back(entry.key, entry.value);
    }
    return result;
}

uint64_t LruShard::lru_access_seq_unlocked() const noexcept {
    if (lru_list_.empty()) return UINT64_MAX;
    return lru_list_.back().access_seq;
}

bool LruShard::evict_lru_unlocked() {
    if (lru_list_.empty()) return false;
    const std::string& evict_key = lru_list_.back().key;
    table_.erase(evict_key);
    lru_list_.pop_back();
    ++evictions_;
    return true;
}

void LruShard::insert_unlocked(const std::string& key, const std::string& value, uint64_t seq) {
    lru_list_.push_front(Entry{key, value, seq});
    try {
        table_.emplace(key, lru_list_.begin());
    } catch (...) {
        lru_list_.pop_front();
        throw;
    }
}

void LruShard::insert_unlocked(std::string&& key, std::string&& value, uint64_t seq) {
    std::string key_copy = key;
    lru_list_.push_front(Entry{std::move(key), std::move(value), seq});
    try {
        table_.emplace(std::move(key_copy), lru_list_.begin());
    } catch (...) {
        lru_list_.pop_front();
        throw;
    }
}

bool LruShard::touch_unlocked(const std::string& key, uint64_t seq) {
    auto it = table_.find(key);
    if (it != table_.end()) {
        it->second->access_seq = seq;
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return true;
    }
    return false;
}

} // namespace kvstore
