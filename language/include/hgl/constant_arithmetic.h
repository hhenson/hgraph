#ifndef HGL_CONSTANT_ARITHMETIC_H
#define HGL_CONSTANT_ARITHMETIC_H
// Wiring-time scalar arithmetic, shared by HIR folding, direct execution and
// generated composition code. Runtime node arithmetic has its own contract.
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
namespace hgl::constant_arithmetic
{
    [[nodiscard]] inline std::optional<std::int64_t> checked_add(std::int64_t lhs, std::int64_t rhs) noexcept {
        constexpr auto min = std::numeric_limits<std::int64_t>::min();
        constexpr auto max = std::numeric_limits<std::int64_t>::max();
        if ((rhs > 0 && lhs > max - rhs) || (rhs < 0 && lhs < min - rhs)) { return std::nullopt; }
        return lhs + rhs;
    }

    [[nodiscard]] inline std::optional<std::int64_t> checked_sub(std::int64_t lhs, std::int64_t rhs) noexcept {
        constexpr auto min = std::numeric_limits<std::int64_t>::min();
        constexpr auto max = std::numeric_limits<std::int64_t>::max();
        if ((rhs > 0 && lhs < min + rhs) || (rhs < 0 && lhs > max + rhs)) { return std::nullopt; }
        return lhs - rhs;
    }

    [[nodiscard]] inline std::optional<std::int64_t> checked_mul(std::int64_t lhs, std::int64_t rhs) noexcept {
        constexpr auto min = std::numeric_limits<std::int64_t>::min();
        constexpr auto max = std::numeric_limits<std::int64_t>::max();
        if (lhs == 0 || rhs == 0) { return 0; }
        if ((lhs == -1 && rhs == min) || (rhs == -1 && lhs == min)) { return std::nullopt; }
        if (lhs > 0) {
            if ((rhs > 0 && lhs > max / rhs) || (rhs < 0 && rhs < min / lhs)) { return std::nullopt; }
        } else if ((rhs > 0 && lhs < min / rhs) || (rhs < 0 && lhs < max / rhs)) {
            return std::nullopt;
        }
        return lhs * rhs;
    }

    inline std::int64_t require_integer(std::optional<std::int64_t> value) {
        if (!value) throw std::overflow_error("overflow in an integer constant expression");
        return *value;
    }
    inline std::int64_t negate(std::int64_t value) { return require_integer(checked_sub(0, value)); }
    inline double       modulo(double lhs, double rhs) {
        if (rhs == 0.0) throw std::domain_error("remainder by zero in a constant expression");
        const double remainder = std::fmod(lhs, rhs);
        return remainder == 0.0 ? std::copysign(0.0, rhs) : ((remainder < 0.0) != (rhs < 0.0) ? remainder + rhs : remainder);
    }
}  // namespace hgl::constant_arithmetic

// Compile-time temporal folding. Each function reproduces the checked hgraph
// kernel the same expression reaches at run time (src/hgraph/types/temporal.cpp
// and the date operators in arithmetic_impl.h), so a folded constant equals
// the value a runtime body or a wired operator would publish. An empty result
// is an overflow or a value outside the supported calendar.
namespace hgl::constant_arithmetic::temporal
{
    inline constexpr std::int64_t micros_per_day = 86'400'000'000;
    /// 0001-01-01 and 9999-12-31 as days since 1970-01-01, hgraph's civil range.
    inline constexpr std::int64_t first_day = -719'162;
    inline constexpr std::int64_t last_day  = 2'932'896;

    [[nodiscard]] inline std::int64_t floor_div(std::int64_t numerator, std::int64_t denominator) noexcept {
        const std::int64_t quotient = numerator / denominator;
        return quotient - ((numerator % denominator != 0 && ((numerator < 0) != (denominator < 0))) ? 1 : 0);
    }

    /// `date + duration`: the duration's floor-based whole-day component, as
    /// Python's date arithmetic and hgraph's add_date_timedelta use.
    [[nodiscard]] inline std::optional<std::int64_t> add_days(std::int64_t day, std::int64_t days) noexcept {
        if (day < first_day || day > last_day) { return std::nullopt; }
        const auto result = checked_add(day, days);
        if (!result || *result < first_day || *result > last_day) { return std::nullopt; }
        return result;
    }

    /// `date - duration`: Python subtracts the duration's day component, so a
    /// part-day duration leaves the date unchanged (hgraph's sub_date_timedelta).
    [[nodiscard]] inline std::optional<std::int64_t> subtract_days(std::int64_t day, std::int64_t days) noexcept {
        const auto negated = checked_sub(0, days);
        if (!negated) { return std::nullopt; }
        return add_days(day, *negated);
    }

    /// `date - date` in microseconds.
    [[nodiscard]] inline std::optional<std::int64_t> date_difference(std::int64_t lhs, std::int64_t rhs) noexcept {
        if (lhs < first_day || lhs > last_day || rhs < first_day || rhs > last_day) { return std::nullopt; }
        return checked_mul(lhs - rhs, micros_per_day);
    }

    /// A civil datetime's local epoch microseconds, if inside the civil range.
    [[nodiscard]] inline std::optional<std::int64_t> civil(std::optional<std::int64_t> micros) noexcept {
        if (!micros || *micros < first_day * micros_per_day || *micros >= (last_day + 1) * micros_per_day) {
            return std::nullopt;
        }
        return micros;
    }

    /// `date + time` as local epoch microseconds.
    [[nodiscard]] inline std::optional<std::int64_t> combine(std::int64_t day, std::int64_t time) noexcept {
        if (day < first_day || day > last_day || time < 0 || time >= micros_per_day) { return std::nullopt; }
        return civil(day * micros_per_day + time);
    }

    /// Round half to even into int64, hgraph's round_to_int64.
    [[nodiscard]] inline std::optional<std::int64_t> round_to_int64(long double value) noexcept {
        constexpr auto min = static_cast<long double>(std::numeric_limits<std::int64_t>::min());
        constexpr auto max = static_cast<long double>(std::numeric_limits<std::int64_t>::max());
        if (!std::isfinite(value) || value < min - 0.5L || value > max + 0.5L) { return std::nullopt; }
        const long double base     = std::floor(value);
        const long double fraction = value - base;
        long double       rounded  = base;
        if (fraction > 0.5L || (fraction == 0.5L && std::fmod(std::fabs(base), 2.0L) == 1.0L)) { rounded += 1.0L; }
        if (rounded < min || rounded > max) { return std::nullopt; }
        return static_cast<std::int64_t>(rounded);
    }

    /// `duration * f64`. A non-finite factor is rejected like the runtime's.
    [[nodiscard]] inline std::optional<std::int64_t> rounded_product(std::int64_t micros, double factor) noexcept {
        if (!std::isfinite(factor)) { return std::nullopt; }
        return round_to_int64(static_cast<long double>(micros) * static_cast<long double>(factor));
    }

    /// `duration / f64` for a nonzero, non-NaN divisor; an infinite divisor gives zero.
    [[nodiscard]] inline std::optional<std::int64_t> rounded_ratio(std::int64_t micros, double divisor) noexcept {
        if (std::isinf(divisor)) { return 0; }
        return round_to_int64(static_cast<long double>(micros) / static_cast<long double>(divisor));
    }

    /// `duration / i64` for a nonzero divisor, rounding half to even.
    [[nodiscard]] inline std::optional<std::int64_t> rounded_integer_ratio(std::int64_t micros, std::int64_t divisor) noexcept {
        if (micros == std::numeric_limits<std::int64_t>::min() && divisor == -1) { return std::nullopt; }
        std::int64_t       quotient  = micros / divisor;
        const std::int64_t remainder = micros % divisor;
        if (remainder == 0) { return quotient; }
        const auto magnitude = [](std::int64_t number) {
            const auto bits = static_cast<std::uint64_t>(number);
            return number < 0 ? std::uint64_t{0} - bits : bits;
        };
        const std::uint64_t remainder_magnitude = magnitude(remainder);
        const std::uint64_t divisor_magnitude   = magnitude(divisor);
        const bool          above_half          = remainder_magnitude > divisor_magnitude - remainder_magnitude;
        const bool          tie                 = remainder_magnitude == divisor_magnitude - remainder_magnitude;
        if (above_half || (tie && (quotient & 1) != 0)) {
            const auto adjusted = checked_add(quotient, (micros < 0) == (divisor < 0) ? 1 : -1);
            if (!adjusted) { return std::nullopt; }
            quotient = *adjusted;
        }
        return quotient;
    }
}  // namespace hgl::constant_arithmetic::temporal
#endif
