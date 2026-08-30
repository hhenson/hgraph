#ifndef HGRAPH_UTIL_SHA256_H
#define HGRAPH_UTIL_SHA256_H

/**
 * @file sha256.h
 * Minimal self-contained SHA-256 (FIPS 180-4) for manifest identity.
 *
 * RFC 0022 uses SHA-256 over a canonical descriptor as a LOOKUP HANDLE; the
 * descriptor bytes themselves remain the identity compared on attach. A
 * self-contained implementation keeps the default build free of a
 * cryptography dependency (the digest is an integrity/lookup device here,
 * not a security boundary).
 */

#include <hgraph/hgraph_export.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace hgraph::util
{
    /** A SHA-256 digest: 32 raw bytes. */
    struct Sha256Digest
    {
        std::array<std::byte, 32> bytes{};

        friend bool operator==(const Sha256Digest &, const Sha256Digest &) = default;
    };

    /** Streaming SHA-256 hasher. */
    class HGRAPH_CLASS_EXPORT Sha256 final
    {
      public:
        Sha256() noexcept;

        void update(std::span<const std::byte> data) noexcept;
        [[nodiscard]] Sha256Digest finish() noexcept;

      private:
        void process_block(const std::uint8_t *block) noexcept;

        std::array<std::uint32_t, 8> state_{};
        std::array<std::uint8_t, 64> buffer_{};
        std::uint64_t total_bytes_{0};
        std::size_t buffered_{0};
    };

    /** One-shot digest of a byte span. */
    [[nodiscard]] HGRAPH_EXPORT Sha256Digest sha256(std::span<const std::byte> data) noexcept;

    /** Render a digest as lowercase hex (diagnostics only, 64 chars). */
    [[nodiscard]] HGRAPH_EXPORT std::array<char, 64> sha256_hex(const Sha256Digest &digest) noexcept;
}  // namespace hgraph::util

#endif  // HGRAPH_UTIL_SHA256_H
