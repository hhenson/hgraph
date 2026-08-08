#ifndef HGRAPH_TYPES_PRIMITIVE_TYPES_H
#define HGRAPH_TYPES_PRIMITIVE_TYPES_H

#include <hgraph/util/date_time.h>

#include <compare>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <string_view>

namespace hgraph
{
    using Bool  = bool;
    using Int   = std::int64_t;
    using Float = double;
    using Str   = std::string;

    [[nodiscard]] constexpr Bool bool_(bool value) noexcept { return value; }

    template <typename T>
    [[nodiscard]] constexpr Int int_(T value) noexcept
    {
        return static_cast<Int>(value);
    }

    template <typename T>
    [[nodiscard]] constexpr Float float_(T value) noexcept
    {
        return static_cast<Float>(value);
    }

    [[nodiscard]] inline Str str_(std::string_view value) { return Str{value}; }
    [[nodiscard]] inline Str str_(const char *value) { return Str{value}; }
    [[nodiscard]] inline Str str_(Str value) { return value; }

    /**
     * Binary-data scalar mirroring Python's ``bytes``. A distinct strong type
     * (not an alias of ``Str``) so schema identity and operator dispatch can
     * tell text from binary; the payload is byte-safe (embedded NULs are
     * fine).
     */
    struct Bytes
    {
        std::string data{};

        friend bool operator==(const Bytes &, const Bytes &) noexcept = default;
        friend std::strong_ordering operator<=>(const Bytes &, const Bytes &) noexcept = default;
    };

    [[nodiscard]] inline Bytes bytes_(std::string_view value) { return Bytes{std::string{value}}; }

    /** Render as ``b'…'`` with non-printable bytes hex-escaped (the Python repr shape). */
    std::ostream &operator<<(std::ostream &os, const Bytes &value);

    namespace literals
    {
        [[nodiscard]] constexpr Int operator""_i(unsigned long long value) noexcept
        {
            return static_cast<Int>(value);
        }

        [[nodiscard]] constexpr Float operator""_f(unsigned long long value) noexcept
        {
            return static_cast<Float>(value);
        }

        [[nodiscard]] constexpr Float operator""_f(long double value) noexcept
        {
            return static_cast<Float>(value);
        }

        [[nodiscard]] inline Str operator""_str(const char *value, std::size_t size)
        {
            return Str{value, size};
        }
    }  // namespace literals
}  // namespace hgraph

/** ``std::hash`` for ``Bytes`` (usable as a TSS element / TSD key). */
template <>
struct std::hash<hgraph::Bytes>
{
    [[nodiscard]] std::size_t operator()(const hgraph::Bytes &value) const noexcept
    {
        return std::hash<std::string>{}(value.data);
    }
};

#endif  // HGRAPH_TYPES_PRIMITIVE_TYPES_H
