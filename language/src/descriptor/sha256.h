#ifndef HGL_DESCRIPTOR_SHA256_H
#define HGL_DESCRIPTOR_SHA256_H

#include <array>
#include <cstddef>
#include <span>

namespace hgl::descriptor::detail
{
    /// Return the lowercase SHA-256 digest of bytes. This small private helper
    /// keeps descriptor tooling independent of the hgraph runtime libraries.
    [[nodiscard]] std::array<char, 64> sha256_hex(std::span<const std::byte> bytes) noexcept;
}  // namespace hgl::descriptor::detail

#endif  // HGL_DESCRIPTOR_SHA256_H
