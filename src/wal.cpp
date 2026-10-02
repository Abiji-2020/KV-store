#include "kvstore/wal.hpp"
#include "kvstore/kvstore.hpp"
#include "kvstore/crc32.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace kvstore {

namespace {

// Magic bytes: 'W' (0x57), 'L' (0x4C)
constexpr uint8_t WAL_MAGIC_0 = 0x57;
constexpr uint8_t WAL_MAGIC_1 = 0x4C;

// Header size: magic(2) + opcode(1) + reserved(1) + timestamp_ns(8) + key_len(4) + val_len(4) = 20 bytes
// Trailing CRC: IEEE 802.3 CRC32 (4 bytes, Big-Endian)
// Total framing overhead = 24 bytes per record (compliant with tests/e2e/harness_utils.py WalDecoder)
constexpr size_t WAL_HEADER_SIZE = 20;
constexpr size_t WAL_CRC_SIZE = 4;
constexpr uint32_t MAX_SANITY_PAYLOAD = 64 * 1024 * 1024; // 64 MB guard

inline void write_u32_be(uint8_t* dst, uint32_t val) {
    dst[0] = static_cast<uint8_t>((val >> 24) & 0xFF);
    dst[1] = static_cast<uint8_t>((val >> 16) & 0xFF);
    dst[2] = static_cast<uint8_t>((val >> 8) & 0xFF);
    dst[3] = static_cast<uint8_t>(val & 0xFF);
}

inline void write_u64_be(uint8_t* dst, uint64_t val) {
    for (int i = 0; i < 8; ++i) {
        dst[i] = static_cast<uint8_t>((val >> ((7 - i) * 8)) & 0xFF);
    }
}

inline uint32_t read_u32_be(const uint8_t* src) {
    return (static_cast<uint32_t>(src[0]) << 24) |
           (static_cast<uint32_t>(src[1]) << 16) |
           (static_cast<uint32_t>(src[2]) << 8) |
           (static_cast<uint32_t>(src[3]));
}

inline uint64_t read_u64_be(const uint8_t* src) {
    uint64_t val = 0;
    for (int i = 0; i < 8; ++i) {
        val = (val << 8) | static_cast<uint64_t>(src[i]);
    }
    return val;
}

inline uint64_t current_timestamp_ns() {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

} // anonymous namespace

WalManager::WalManager(const std::string& log_path,
                       SyncMode mode,
                       size_t batch_bytes_limit,
                       size_t batch_ops_limit)
    : log_path_(log_path),
      sync_mode_(mode),
      batch_bytes_threshold_(batch_bytes_limit),
      batch_ops_threshold_(batch_ops_limit) {
    batch_buffer_.reserve(batch_bytes_threshold_);

    // Open file in read-write append mode; create with 0644 if non-existent
    fd_ = ::open(log_path_.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("WalManager: failed to open WAL file '" + log_path_ + "': " + std::strerror(errno));
    }
}

WalManager::~WalManager() {
    try {
        close();
    } catch (...) {
        // Destructors must never throw
    }
}

bool WalManager::write_all(int fd, const void* data, size_t len) {
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t written = ::write(fd, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue; // Retry on signal interruption
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        ptr += written;
        remaining -= static_cast<size_t>(written);
    }
    return true;
}

bool WalManager::sync_fd(int fd) {
    while (::fdatasync(fd) < 0) {
        if (errno == EINTR) {
            continue; // Retry on signal interruption
        }
        return false;
    }
    return true;
}

bool WalManager::append_record_unlocked(OpCode op, std::string_view key, std::string_view value) {
    if (fd_ < 0) {
        return false;
    }

    ++seq_num_;
    uint64_t ts = current_timestamp_ns();
    if (ts <= last_ts_ns_) {
        ts = last_ts_ns_ + 1;
    }
    last_ts_ns_ = ts;

    const uint32_t key_len = static_cast<uint32_t>(key.size());
    const uint32_t val_len = static_cast<uint32_t>(value.size());

    // Total record framing: Header (20 bytes) + Key + Value + CRC32 (4 bytes) = 24 + payload
    const size_t payload_len = key_len + val_len;
    const size_t total_record_len = WAL_HEADER_SIZE + payload_len + WAL_CRC_SIZE;

    std::vector<uint8_t> record(total_record_len);

    // 1. Serialize Header (Big-Endian per E2E harness specification)
    record[0] = WAL_MAGIC_0;
    record[1] = WAL_MAGIC_1;
    record[2] = static_cast<uint8_t>(op);
    record[3] = 0x00; // Reserved / Flags
    write_u64_be(&record[4], ts);
    write_u32_be(&record[12], key_len);
    write_u32_be(&record[16], val_len);

    // 2. Serialize Payloads
    if (key_len > 0) {
        std::memcpy(&record[WAL_HEADER_SIZE], key.data(), key_len);
    }
    if (val_len > 0) {
        std::memcpy(&record[WAL_HEADER_SIZE + key_len], value.data(), val_len);
    }

    // 3. Compute and append IEEE 802.3 CRC32 over header + payloads (Big-Endian)
    uint32_t checksum = Crc32::compute(record.data(), WAL_HEADER_SIZE + payload_len);
    write_u32_be(&record[WAL_HEADER_SIZE + payload_len], checksum);

    // 4. Write according to SyncMode
    switch (sync_mode_) {
        case SyncMode::SYNC_ALWAYS: {
            if (!write_all(fd_, record.data(), record.size())) {
                return false;
            }
            return sync_fd(fd_);
        }

        case SyncMode::SYNC_BUFFERED: {
            return write_all(fd_, record.data(), record.size());
        }

        case SyncMode::SYNC_BATCH: {
            batch_buffer_.insert(batch_buffer_.end(), record.begin(), record.end());
            batch_ops_count_++;
            if (batch_buffer_.size() >= batch_bytes_threshold_ ||
                batch_ops_count_ >= batch_ops_threshold_) {
                return flush_batch_unlocked();
            }
            return true;
        }
    }

    return false;
}

bool WalManager::flush_batch_unlocked() {
    if (batch_buffer_.empty()) {
        return true;
    }
    if (!write_all(fd_, batch_buffer_.data(), batch_buffer_.size())) {
        return false;
    }
    if (!sync_fd(fd_)) {
        return false;
    }
    batch_buffer_.clear();
    batch_ops_count_ = 0;
    return true;
}

bool WalManager::append_set(std::string_view key, std::string_view value) {
    std::lock_guard<std::mutex> lock(mtx_);
    return append_record_unlocked(OpCode::OP_SET, key, value);
}

bool WalManager::append_del(std::string_view key) {
    std::lock_guard<std::mutex> lock(mtx_);
    return append_record_unlocked(OpCode::OP_DEL, key, "");
}

bool WalManager::append_clear() {
    std::lock_guard<std::mutex> lock(mtx_);
    return append_record_unlocked(OpCode::OP_CLEAR, "", "");
}

void WalManager::sync() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (fd_ < 0) {
        return;
    }
    if (sync_mode_ == SyncMode::SYNC_BATCH) {
        flush_batch_unlocked();
    } else {
        sync_fd(fd_);
    }
}

void WalManager::close() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (fd_ >= 0) {
        if (sync_mode_ == SyncMode::SYNC_BATCH) {
            flush_batch_unlocked();
        } else {
            sync_fd(fd_);
        }
        ::close(fd_);
        fd_ = -1;
    }
}

size_t WalManager::recover(KVStore& store) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (fd_ < 0) {
        return 0;
    }

    // Flush any pending in-memory batch writes before replaying
    if (sync_mode_ == SyncMode::SYNC_BATCH && !batch_buffer_.empty()) {
        flush_batch_unlocked();
    }

    // Seek to the beginning of the WAL file
    if (::lseek(fd_, 0, SEEK_SET) < 0) {
        return 0;
    }

    size_t replayed_count = 0;
    off_t last_valid_offset = 0;
    uint64_t max_seq = 0;

    while (true) {
        uint8_t header_buf[WAL_HEADER_SIZE];
        size_t h_read = 0;
        bool eof_clean = false;

        while (h_read < WAL_HEADER_SIZE) {
            ssize_t n = ::read(fd_, header_buf + h_read, WAL_HEADER_SIZE - h_read);
            if (n < 0) {
                if (errno == EINTR) continue;
                break;
            }
            if (n == 0) {
                if (h_read == 0) {
                    eof_clean = true;
                }
                break;
            }
            h_read += static_cast<size_t>(n);
        }

        if (eof_clean) {
            // Clean EOF at record boundary
            break;
        }

        if (h_read < WAL_HEADER_SIZE) {
            // Incomplete header at EOF: torn write, truncate file
            ::ftruncate(fd_, last_valid_offset);
            break;
        }

        // Validate Magic bytes
        if (header_buf[0] != WAL_MAGIC_0 || header_buf[1] != WAL_MAGIC_1) {
            ::ftruncate(fd_, last_valid_offset);
            break;
        }

        // Validate OpCode
        uint8_t op_val = header_buf[2];
        if (op_val != static_cast<uint8_t>(OpCode::OP_SET) &&
            op_val != static_cast<uint8_t>(OpCode::OP_DEL) &&
            op_val != static_cast<uint8_t>(OpCode::OP_CLEAR)) {
            ::ftruncate(fd_, last_valid_offset);
            break;
        }
        OpCode op = static_cast<OpCode>(op_val);

        uint64_t ts = read_u64_be(&header_buf[4]);
        uint32_t key_len = read_u32_be(&header_buf[12]);
        uint32_t val_len = read_u32_be(&header_buf[16]);

        // Guard against corrupted lengths causing unbounded allocations
        if (key_len > MAX_SANITY_PAYLOAD || val_len > MAX_SANITY_PAYLOAD) {
            ::ftruncate(fd_, last_valid_offset);
            break;
        }

        size_t payload_and_crc_len = key_len + val_len + WAL_CRC_SIZE;
        std::vector<uint8_t> payload_buf(payload_and_crc_len);
        size_t p_read = 0;
        bool p_eof = false;

        while (p_read < payload_and_crc_len) {
            ssize_t n = ::read(fd_, payload_buf.data() + p_read, payload_and_crc_len - p_read);
            if (n < 0) {
                if (errno == EINTR) continue;
                break;
            }
            if (n == 0) {
                p_eof = true;
                break;
            }
            p_read += static_cast<size_t>(n);
        }

        if (p_eof || p_read < payload_and_crc_len) {
            // Incomplete payload or CRC: torn write, truncate file
            ::ftruncate(fd_, last_valid_offset);
            break;
        }

        // Validate IEEE 802.3 CRC32
        uint32_t computed_crc = Crc32::compute(header_buf, WAL_HEADER_SIZE);
        if (key_len + val_len > 0) {
            computed_crc = Crc32::compute(payload_buf.data(), key_len + val_len, computed_crc);
        }
        uint32_t stored_crc = read_u32_be(payload_buf.data() + key_len + val_len);

        if (computed_crc != stored_crc) {
            // CRC mismatch: corrupted trailing write, truncate file
            ::ftruncate(fd_, last_valid_offset);
            break;
        }

        // Apply mutation to store
        std::string key(reinterpret_cast<const char*>(payload_buf.data()), key_len);
        std::string val(reinterpret_cast<const char*>(payload_buf.data() + key_len), val_len);

        switch (op) {
            case OpCode::OP_SET:
                store.set(key, val);
                break;
            case OpCode::OP_DEL:
                store.del(key);
                break;
            case OpCode::OP_CLEAR:
                store.clear();
                break;
        }

        max_seq = std::max(max_seq, ts);
        replayed_count++;
        last_valid_offset += static_cast<off_t>(WAL_HEADER_SIZE + payload_and_crc_len);
    }

    // Align internal sequence number with maximum observed sequence number / count
    seq_num_ = std::max(seq_num_, max_seq);
    seq_num_ = std::max(seq_num_, static_cast<uint64_t>(replayed_count));
    last_ts_ns_ = std::max(last_ts_ns_, max_seq);

    // Reposition file pointer to the end of valid records for future appends
    ::lseek(fd_, last_valid_offset, SEEK_SET);

    return replayed_count;
}

bool WalManager::is_open() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    return fd_ >= 0;
}

const std::string& WalManager::path() const noexcept {
    return log_path_;
}

SyncMode WalManager::sync_mode() const noexcept {
    return sync_mode_;
}

uint64_t WalManager::last_sequence_number() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    return seq_num_;
}

size_t WalManager::file_size() const {
    std::lock_guard<std::mutex> lock(mtx_);
    if (fd_ < 0) {
        return 0;
    }
    struct stat st;
    if (::fstat(fd_, &st) == 0) {
        return static_cast<size_t>(st.st_size);
    }
    return 0;
}

} // namespace kvstore
