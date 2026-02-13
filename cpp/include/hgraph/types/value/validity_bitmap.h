#pragma once

/**
 * @file validity_bitmap.h
 * @brief Arrow-compatible validity bitmap helpers backed by nanoarrow.
 */

#include <cstddef>
#include <cstdint>

#if __has_include(<nanoarrow/nanoarrow.h>)
#include <nanoarrow/nanoarrow.h>
#elif __has_include(<nanoarrow.h>)
#include <nanoarrow.h>
#else
#error "nanoarrow headers not found. Ensure nanoarrow is available to this target."
#endif

namespace hgraph::value {

inline constexpr size_t validity_mask_bytes(size_t count) {
    return (count + 7u) / 8u;
}

inline bool validity_bit_get(const std::byte* bits, size_t index) {
    if (!bits) return true;
    return ArrowBitGet(reinterpret_cast<const uint8_t*>(bits), static_cast<int64_t>(index)) != 0;
}

inline void validity_bit_set(std::byte* bits, size_t index, bool valid) {
    if (!bits) return;
    ArrowBitSetTo(reinterpret_cast<uint8_t*>(bits),
                  static_cast<int64_t>(index),
                  static_cast<uint8_t>(valid ? 1u : 0u));
}

inline void validity_set_range(std::byte* bits, size_t start, size_t count, bool valid) {
    if (!bits || count == 0) return;
    ArrowBitsSetTo(reinterpret_cast<uint8_t*>(bits),
                   static_cast<int64_t>(start),
                   static_cast<int64_t>(count),
                   static_cast<uint8_t>(valid ? 1u : 0u));
}

inline void validity_set_all(std::byte* bits, size_t count, bool valid) {
    validity_set_range(bits, 0, count, valid);
}

inline void validity_clear_unused_trailing_bits(std::byte* bits, size_t count) {
    if (!bits || count == 0) return;
    const size_t remainder = count % 8u;
    if (remainder == 0u) return;
    validity_set_range(bits, count, 8u - remainder, false);
}

}  // namespace hgraph::value
