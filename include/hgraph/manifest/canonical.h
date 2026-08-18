#ifndef HGRAPH_MANIFEST_CANONICAL_H
#define HGRAPH_MANIFEST_CANONICAL_H

/**
 * @file canonical.h
 * The canonical binary writer/reader for manifest descriptors (RFC 0022).
 *
 * The grammar is a versioned, length-delimited binary tree:
 *
 * - a FIELD is ``varint tag`` followed by a payload determined by the tag's
 *   fixed wire class (the schema is static per format version; tags do not
 *   self-describe their wire class);
 * - unsigned integers are LEB128 varints; signed integers are zigzag
 *   varints; floating-point payloads are IEEE-754 little-endian fixed64;
 * - byte strings and nested scopes are varint-length-delimited; and
 * - writers emit fields in ascending canonical tag order with map-like
 *   content pre-sorted by its canonical encoded key, so equal semantic
 *   content produces byte-identical descriptors across processes.
 *
 * This layer defines the byte grammar RFC 0022 names and is deliberately
 * free of manifest semantics, so RFC 0017's codec work can adopt it as the
 * shared substrate (RFC 0022 "Unresolved questions" records that choice).
 */

#include <cstddef>
#include <cstring>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace hgraph::manifest
{
    /** Raised by ``CanonicalReader`` on malformed or truncated input. */
    class CanonicalDecodeError : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /** Appends canonical primitives to an owned byte buffer. */
    class CanonicalWriter final
    {
      public:
        [[nodiscard]] const std::vector<std::byte> &bytes() const noexcept { return buffer_; }
        [[nodiscard]] std::vector<std::byte> take() noexcept { return std::move(buffer_); }
        [[nodiscard]] std::size_t size() const noexcept { return buffer_.size(); }

        /** LEB128 unsigned varint. */
        void varint(std::uint64_t value)
        {
            while (value >= 0x80u)
            {
                buffer_.push_back(static_cast<std::byte>((value & 0x7fu) | 0x80u));
                value >>= 7u;
            }
            buffer_.push_back(static_cast<std::byte>(value));
        }

        /** Zigzag-encoded signed varint. */
        void svarint(std::int64_t value)
        {
            varint((static_cast<std::uint64_t>(value) << 1u) ^
                   static_cast<std::uint64_t>(value >> 63));
        }

        /** IEEE-754 double as little-endian fixed64. */
        void fixed_double(double value)
        {
            std::uint64_t bits{};
            static_assert(sizeof(bits) == sizeof(value));
            std::memcpy(&bits, &value, sizeof(bits));
            for (unsigned i = 0; i < 8; ++i)
            {
                buffer_.push_back(static_cast<std::byte>(bits >> (8u * i)));
            }
        }

        /** Varint-length-delimited raw bytes. */
        void bytes_field(std::span<const std::byte> data)
        {
            varint(data.size());
            buffer_.insert(buffer_.end(), data.begin(), data.end());
        }

        /** Varint-length-delimited UTF-8 string. */
        void string_field(std::string_view text)
        {
            bytes_field(std::as_bytes(std::span{text.data(), text.size()}));
        }

        /** Field tag (ascending canonical order is the writer's contract). */
        void tag(std::uint32_t field_tag) { varint(field_tag); }

        /** Embed a fully built nested scope, length-delimited. */
        void scope(const CanonicalWriter &child) { bytes_field(child.bytes()); }

      private:
        std::vector<std::byte> buffer_{};
    };

    /** Bounds-checked reader over a canonical byte span. */
    class CanonicalReader final
    {
      public:
        explicit CanonicalReader(std::span<const std::byte> data) noexcept
            : data_(data)
        {
        }

        [[nodiscard]] bool at_end() const noexcept { return offset_ == data_.size(); }
        [[nodiscard]] std::size_t remaining() const noexcept { return data_.size() - offset_; }

        [[nodiscard]] std::uint64_t varint()
        {
            std::uint64_t value = 0;
            unsigned shift = 0;
            for (;;)
            {
                if (offset_ >= data_.size())
                {
                    throw CanonicalDecodeError("canonical descriptor truncated inside varint");
                }
                const auto byte_value = static_cast<std::uint8_t>(data_[offset_++]);
                if (shift >= 64u || (shift == 63u && byte_value > 1u))
                {
                    throw CanonicalDecodeError("canonical varint overflows 64 bits");
                }
                value |= static_cast<std::uint64_t>(byte_value & 0x7fu) << shift;
                if ((byte_value & 0x80u) == 0u) { return value; }
                shift += 7u;
            }
        }

        [[nodiscard]] std::int64_t svarint()
        {
            const std::uint64_t raw = varint();
            return static_cast<std::int64_t>((raw >> 1u) ^ (~(raw & 1u) + 1u));
        }

        [[nodiscard]] double fixed_double()
        {
            if (remaining() < 8u)
            {
                throw CanonicalDecodeError("canonical descriptor truncated inside fixed64");
            }
            std::uint64_t bits = 0;
            for (unsigned i = 0; i < 8; ++i)
            {
                bits |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(data_[offset_++]))
                        << (8u * i);
            }
            double value{};
            std::memcpy(&value, &bits, sizeof(value));
            return value;
        }

        [[nodiscard]] std::span<const std::byte> bytes_field()
        {
            const std::uint64_t length = varint();
            if (length > remaining())
            {
                throw CanonicalDecodeError("canonical descriptor truncated inside byte field");
            }
            const auto result = data_.subspan(offset_, static_cast<std::size_t>(length));
            offset_ += static_cast<std::size_t>(length);
            return result;
        }

        [[nodiscard]] std::string_view string_field()
        {
            const auto raw = bytes_field();
            return {reinterpret_cast<const char *>(raw.data()), raw.size()};
        }

        [[nodiscard]] std::uint32_t tag()
        {
            const std::uint64_t value = varint();
            if (value > 0xffffffffu) { throw CanonicalDecodeError("canonical tag overflows 32 bits"); }
            return static_cast<std::uint32_t>(value);
        }

        [[nodiscard]] CanonicalReader scope() { return CanonicalReader{bytes_field()}; }

      private:
        std::span<const std::byte> data_;
        std::size_t offset_{0};
    };
}  // namespace hgraph::manifest

#endif  // HGRAPH_MANIFEST_CANONICAL_H
