#include "kvstore/crc32.hpp"

#include <array>
#include <bit>
#include <cstring>

namespace kvstore {
namespace {

// Compile-time generation of the 8x256 lookup tables for slicing-by-8
using CrcTable8 = std::array<std::array<uint32_t, 256>, 8>;

constexpr CrcTable8 make_crc32_tables() {
    CrcTable8 tables{};
    // Table 0: standard IEEE 802.3 table
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) {
            c = (c & 1) ? (Crc32::POLYNOMIAL ^ (c >> 1)) : (c >> 1);
        }
        tables[0][i] = c;
    }
    // Tables 1..7: slicing-by-8 parallel lookup tables
    for (uint32_t i = 0; i < 256; ++i) {
        for (int k = 1; k < 8; ++k) {
            tables[k][i] = (tables[k - 1][i] >> 8) ^ tables[0][tables[k - 1][i] & 0xFF];
        }
    }
    return tables;
}

// 64-byte aligned static lookup table residing in L1 data cache
alignas(64) const CrcTable8 CRC32_TABLES = make_crc32_tables();

} // anonymous namespace

uint32_t Crc32::compute(const void* data, size_t length, uint32_t prev_crc) noexcept {
    const auto* ptr = static_cast<const uint8_t*>(data);
    uint32_t crc = ~prev_crc;

    // Process 8 bytes per iteration via slicing-by-8
    while (length >= 8) {
        uint32_t one, two;
        if constexpr (std::endian::native == std::endian::little) {
            std::memcpy(&one, ptr, sizeof(uint32_t));
            std::memcpy(&two, ptr + 4, sizeof(uint32_t));
        } else {
            one = static_cast<uint32_t>(ptr[0]) |
                  (static_cast<uint32_t>(ptr[1]) << 8) |
                  (static_cast<uint32_t>(ptr[2]) << 16) |
                  (static_cast<uint32_t>(ptr[3]) << 24);
            two = static_cast<uint32_t>(ptr[4]) |
                  (static_cast<uint32_t>(ptr[5]) << 8) |
                  (static_cast<uint32_t>(ptr[6]) << 16) |
                  (static_cast<uint32_t>(ptr[7]) << 24);
        }

        crc ^= one;
        crc = CRC32_TABLES[7][crc & 0xFF] ^
              CRC32_TABLES[6][(crc >> 8) & 0xFF] ^
              CRC32_TABLES[5][(crc >> 16) & 0xFF] ^
              CRC32_TABLES[4][(crc >> 24) & 0xFF] ^
              CRC32_TABLES[3][two & 0xFF] ^
              CRC32_TABLES[2][(two >> 8) & 0xFF] ^
              CRC32_TABLES[1][(two >> 16) & 0xFF] ^
              CRC32_TABLES[0][(two >> 24) & 0xFF];

        ptr += 8;
        length -= 8;
    }

    // Process trailing 0..7 remaining bytes using Table 0
    while (length > 0) {
        crc = CRC32_TABLES[0][(crc ^ *ptr) & 0xFF] ^ (crc >> 8);
        ++ptr;
        --length;
    }

    return ~crc;
}

uint32_t Crc32::compute_byte_by_byte(const void* data, size_t length, uint32_t prev_crc) noexcept {
    const auto* ptr = static_cast<const uint8_t*>(data);
    uint32_t crc = ~prev_crc;
    for (size_t i = 0; i < length; ++i) {
        crc = CRC32_TABLES[0][(crc ^ ptr[i]) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

} // namespace kvstore
