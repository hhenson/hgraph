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
#endif
