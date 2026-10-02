#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace kvstore {

/// High-performance, zero-dependency IEEE 802.3 CRC32 calculator.
/// Utilizes the slicing-by-8 algorithm (Intel / Kounavis-Berry) processing
/// 8 bytes per iteration for over 1.4 GB/s throughput on modern x86_64 CPUs.
class Crc32 {
public:
    /// Standard IEEE 802.3 reflected polynomial (same as zlib, Ethernet, PNG)
    static constexpr uint32_t POLYNOMIAL = 0xEDB88320U;

    /// Computes IEEE 802.3 CRC32 over raw memory buffer.
    /// Supports rolling / incremental updates via prev_crc (default 0).
    /// @param data Pointer to raw byte buffer
    /// @param length Number of bytes to process
    /// @param prev_crc Previous CRC32 result (for incremental hashing; 0 for new stream)
    /// @return 32-bit unsigned CRC32 checksum
    static uint32_t compute(const void* data, size_t length, uint32_t prev_crc = 0) noexcept;

    /// Convenience overload for std::string_view
    static uint32_t compute(std::string_view sv, uint32_t prev_crc = 0) noexcept {
        return compute(sv.data(), sv.size(), prev_crc);
    }

    /// Reference byte-at-a-time calculator (used for validation and testing)
    static uint32_t compute_byte_by_byte(const void* data, size_t length, uint32_t prev_crc = 0) noexcept;
};

} // namespace kvstore
