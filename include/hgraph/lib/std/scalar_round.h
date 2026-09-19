#ifndef HGRAPH_LIB_STD_SCALAR_ROUND_H
#define HGRAPH_LIB_STD_SCALAR_ROUND_H

#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace hgraph::stdlib
{
    /// Round binary64 to a decimal quantum, nearest with ties to even.
    /// Conversion is locale independent and storage is bounded by the binary64
    /// exponent range. Preserve non-finite values and the sign of zero; throw
    /// overflow_error when a finite rounded result exceeds binary64.
    inline double scalar_round_decimal(double value, std::int64_t digits) {
        if (!std::isfinite(value) || value == 0.0 || digits >= 324) { return value; }
        if (digits < -308) { return std::copysign(0.0, value); }
        std::array<char, 768> buffer{};
        // For negative precision keep enough fractional digits to distinguish
        // either neighbour of a decimal midpoint (whose magnitude is >= 5).
        const int precision = digits >= 0 ? static_cast<int>(digits) : std::numeric_limits<double>::max_digits10;
        auto [end, error] =
            std::to_chars(buffer.data(), buffer.data() + buffer.size(), std::abs(value), std::chars_format::fixed, precision);
        if (error != std::errc{}) { throw std::runtime_error("decimal rounding conversion failed"); }
        if (digits < 0) {
            char *decimal = buffer.data();
            while (decimal != end && *decimal != '.') { ++decimal; }
            const auto places         = static_cast<std::ptrdiff_t>(-digits);
            const auto integer_digits = decimal - buffer.data();
            if (places > integer_digits) { return std::copysign(0.0, value); }
            char *cut          = decimal - places;
            bool  tail_nonzero = false;
            for (char *p = cut + 1; p != end; ++p) {
                if (*p != '0' && *p != '.') {
                    tail_nonzero = true;
                    break;
                }
            }
            const bool odd       = cut != buffer.data() && ((cut[-1] - '0') % 2 != 0);
            const bool increment = *cut > '5' || (*cut == '5' && (tail_nonzero || odd));
            for (char *p = cut; p != decimal; ++p) { *p = '0'; }
            end = decimal;
            if (increment) {
                char *p = cut;
                while (p != buffer.data() && p[-1] == '9') { *--p = '0'; }
                if (p != buffer.data()) {
                    ++p[-1];
                } else {
                    // Carry adds one digit; all existing integer digits are zero.
                    *buffer.data() = '1';
                    *end++         = '0';
                }
            }
        }
        double     rounded{};
        const auto parsed = std::from_chars(buffer.data(), end, rounded, std::chars_format::fixed);
        if (parsed.ec == std::errc::result_out_of_range) { throw std::overflow_error("rounded value is too large"); }
        if (parsed.ec != std::errc{} || parsed.ptr != end) { throw std::runtime_error("decimal rounding conversion failed"); }
        return std::copysign(rounded, value);
    }
}  // namespace hgraph::stdlib
#endif
