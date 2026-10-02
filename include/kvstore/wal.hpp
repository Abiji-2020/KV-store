#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

class KVStore; // Forward declaration for recovery replay

/// Synchronization durability policy for WAL appends.
enum class SyncMode {
    SYNC_ALWAYS,    ///< Synchronously issues fdatasync() after every append (highest durability)
    SYNC_BATCH,     ///< Accumulates writes in memory buffer up to 64KB / 64 ops before fdatasync()
    SYNC_BUFFERED   ///< Relies on OS page cache writeback; fdatasync() only upon sync() or close()
};

/// Operation codes for WAL binary record framing.
enum class OpCode : uint8_t {
    OP_SET   = 1,   ///< Mutation: Insert or update key-value pair
    OP_DEL   = 2,   ///< Mutation: Delete key
    OP_CLEAR = 3    ///< Mutation: Clear all entries from store
};

/// High-performance thread-safe Write-Ahead Logging (WAL) manager.
/// Manages an append-only log file on disk using POSIX direct file descriptor APIs.
/// Provides strictly monotonic sequence numbers, CRC32 record integrity validation,
/// configurable sync modes, and deterministic crash recovery.
class WalManager {
public:
    static constexpr size_t DEFAULT_BATCH_BYTES_LIMIT = 64 * 1024; // 64 KB
    static constexpr size_t DEFAULT_BATCH_OPS_LIMIT = 64;           // 64 ops

    /// Opens or creates the append-only log file at log_path with specified sync mode.
    /// @param log_path Path to the WAL file on disk.
    /// @param mode Synchronization policy (default: SYNC_ALWAYS).
    /// @param batch_bytes_limit Memory threshold for SYNC_BATCH mode (default: 64 KB).
    /// @param batch_ops_limit Count threshold for SYNC_BATCH mode (default: 64 ops).
    /// @throws std::runtime_error if file cannot be opened.
    explicit WalManager(const std::string& log_path,
                        SyncMode mode = SyncMode::SYNC_ALWAYS,
                        size_t batch_bytes_limit = DEFAULT_BATCH_BYTES_LIMIT,
                        size_t batch_ops_limit = DEFAULT_BATCH_OPS_LIMIT);

    /// Destructor flushes pending batch writes and safely closes the file descriptor.
    ~WalManager();

    // Non-copyable and non-movable due to file descriptor and synchronization primitives
    WalManager(const WalManager&) = delete;
    WalManager& operator=(const WalManager&) = delete;
    WalManager(WalManager&&) = delete;
    WalManager& operator=(WalManager&&) = delete;

    // --- Core Mutation Logging (Thread-Safe, serialized via mtx_) ---

    /// Appends an OP_SET mutation record to the WAL.
    bool append_set(std::string_view key, std::string_view value);

    /// Appends an OP_DEL mutation record to the WAL.
    bool append_del(std::string_view key);

    /// Appends an OP_CLEAR mutation record to the WAL.
    bool append_clear();

    /// Forces all unwritten batch data and OS page cache dirty pages to disk via fdatasync().
    void sync();

    /// Flushes pending data and closes the underlying file descriptor. Safe to call multiple times.
    void close();

    /// Deterministic crash recovery replay:
    /// Replays valid records sequentially from log start into KVStore,
    /// verifies CRC32 integrity, detects torn writes at tail and truncates them with ftruncate(),
    /// aligns internal sequence number with maximum observed LSN, and positions append offset.
    /// @param store Target KVStore to populate.
    /// @return Number of successfully replayed operations.
    size_t recover(KVStore& store);

    // --- Inspection & Telemetry ---

    /// Returns true if the underlying file descriptor is currently open.
    bool is_open() const noexcept;

    /// Returns the filesystem path to the WAL file.
    const std::string& path() const noexcept;

    /// Returns the active synchronization mode.
    SyncMode sync_mode() const noexcept;

    /// Returns the highest assigned sequence number (LSN).
    uint64_t last_sequence_number() const noexcept;

    /// Returns the current size of the WAL file on disk in bytes (0 if closed).
    size_t file_size() const;

private:
    bool append_record_unlocked(OpCode op, std::string_view key, std::string_view value);
    bool flush_batch_unlocked();

    // POSIX I/O Robust Helpers
    static bool write_all(int fd, const void* data, size_t len);
    static bool sync_fd(int fd);

    std::string log_path_;
    SyncMode sync_mode_{SyncMode::SYNC_ALWAYS};
    int fd_{-1};
    mutable std::mutex mtx_;
    uint64_t seq_num_{0};
    uint64_t last_ts_ns_{0};

    // User-space batch buffer for SyncMode::SYNC_BATCH
    std::vector<uint8_t> batch_buffer_;
    size_t batch_ops_count_{0};
    size_t batch_bytes_threshold_{DEFAULT_BATCH_BYTES_LIMIT};
    size_t batch_ops_threshold_{DEFAULT_BATCH_OPS_LIMIT};
};

} // namespace kvstore
