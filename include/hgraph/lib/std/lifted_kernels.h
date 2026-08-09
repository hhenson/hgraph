#ifndef HGRAPH_LIB_STD_LIFTED_KERNELS_H
#define HGRAPH_LIB_STD_LIFTED_KERNELS_H

#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/types/primitive_types.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph::stdlib
{
    namespace lifted_kernel_detail
    {
        template <typename L, typename R>
        using add_result_t = std::remove_cvref_t<decltype(std::declval<L>() + std::declval<R>())>;

        template <typename L, typename R>
        using sub_result_t = std::remove_cvref_t<decltype(std::declval<L>() - std::declval<R>())>;

        template <typename L, typename R>
        using mul_result_t = std::remove_cvref_t<decltype(std::declval<L>() * std::declval<R>())>;

        template <typename L, typename R, bool Numeric = std::is_arithmetic_v<L> && std::is_arithmetic_v<R>>
        struct true_div_result
        {
            using type = std::remove_cvref_t<decltype(std::declval<L>() / std::declval<R>())>;
        };

        template <typename L, typename R>
        struct true_div_result<L, R, true>
        {
            using type = Float;
        };

        template <typename L, typename R>
        using true_div_result_t = typename true_div_result<L, R>::type;

        template <typename L, typename R>
        struct floor_div_result
        {
            using type = std::conditional_t<std::is_same_v<L, Int> && std::is_same_v<R, Int>, Int, Float>;
        };

        template <typename L, typename R>
        using floor_div_result_t = typename floor_div_result<L, R>::type;

        template <typename L, typename R>
        using modulo_result_t = floor_div_result_t<L, R>;

        template <typename L, typename R>
        using pow_result_t = std::conditional_t<
            std::is_same_v<L, Int> && std::is_same_v<R, Int>, Int, Float>;

        template <typename L, typename R>
        struct ordered_result
        {
            using type = std::conditional_t<std::is_same_v<L, R>, L, Float>;
        };

        template <typename L, typename R>
        using ordered_result_t = typename ordered_result<L, R>::type;

        template <typename T>
        [[nodiscard]] Bool truthy(const T &value)
        {
            return static_cast<Bool>(value);
        }

        template <>
        [[nodiscard]] inline Bool truthy<Str>(const Str &value)
        {
            return !value.empty();
        }

        [[nodiscard]] inline Int floor_divide_int(Int lhs, Int rhs)
        {
            if (rhs == 0) { throw std::domain_error("floordiv_: division by zero"); }
            if (lhs == std::numeric_limits<Int>::min() && rhs == Int{-1})
            {
                throw std::overflow_error("floordiv_: integer overflow");
            }
            Int quotient  = lhs / rhs;
            const Int rem = lhs % rhs;
            if (rem != 0 && ((rem < 0) != (rhs < 0))) { --quotient; }
            return quotient;
        }

        [[nodiscard]] inline Int modulo_int(Int lhs, Int rhs)
        {
            return lhs - floor_divide_int(lhs, rhs) * rhs;
        }

        [[nodiscard]] inline Int checked_multiply_int(Int lhs, Int rhs)
        {
            constexpr Int min = std::numeric_limits<Int>::min();
            constexpr Int max = std::numeric_limits<Int>::max();
            if (lhs == 0 || rhs == 0) { return 0; }
            if ((lhs > 0 && rhs > 0 && lhs > max / rhs) ||
                (lhs > 0 && rhs < 0 && rhs < min / lhs) ||
                (lhs < 0 && rhs > 0 && lhs < min / rhs) ||
                (lhs < 0 && rhs < 0 && lhs < max / rhs))
            {
                throw std::overflow_error("pow_: integer result overflow");
            }
            return lhs * rhs;
        }

        [[nodiscard]] inline Int integer_power(Int base, Int exponent)
        {
            if (exponent < 0)
            {
                throw std::domain_error("pow_: negative exponent cannot produce an integer result");
            }
            Int result = 1;
            while (exponent != 0)
            {
                if ((exponent & 1) != 0) { result = checked_multiply_int(result, base); }
                exponent /= 2;
                if (exponent != 0) { base = checked_multiply_int(base, base); }
            }
            return result;
        }

        [[nodiscard]] inline Float floor_divide_float(Float lhs, Float rhs)
        {
            if (rhs == Float{0}) { throw std::domain_error("floordiv_: division by zero"); }
            return std::floor(lhs / rhs);
        }

        [[nodiscard]] inline Float modulo_float(Float lhs, Float rhs)
        {
            if (rhs == Float{0}) { throw std::domain_error("mod_: division by zero"); }
            return lhs - std::floor(lhs / rhs) * rhs;
        }

        [[nodiscard]] inline Int checked_shift_count(Int value)
        {
            if (value < 0) { throw std::domain_error("shift count must be non-negative"); }
            if (value >= static_cast<Int>(std::numeric_limits<Int>::digits))
            {
                throw std::domain_error("shift count is too large");
            }
            return value;
        }
    }  // namespace lifted_kernel_detail

    template <typename L, typename R = L, typename O = lifted_kernel_detail::add_result_t<L, R>>
    struct scalar_add
    {
        static constexpr const char *name = "scalar_add";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static O identity()
            requires(std::is_same_v<L, R> && std::is_same_v<L, O> && std::default_initializable<O>)
        {
            return O{};
        }

        [[nodiscard]] static O apply(const L &lhs, const R &rhs) { return static_cast<O>(lhs + rhs); }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::sub_result_t<L, R>>
    struct scalar_sub
    {
        static constexpr const char *name = "scalar_sub";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static O apply(const L &lhs, const R &rhs) { return static_cast<O>(lhs - rhs); }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::mul_result_t<L, R>>
    struct scalar_mul
    {
        static constexpr const char *name = "scalar_mul";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static O identity()
            requires(std::is_same_v<L, R> && std::is_same_v<L, O> && std::constructible_from<O, int>)
        {
            return O{1};
        }

        [[nodiscard]] static O apply(const L &lhs, const R &rhs) { return static_cast<O>(lhs * rhs); }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::mul_result_t<L, R>>
    using scalar_mult = scalar_mul<L, R, O>;

    template <typename L, typename R = L, typename O = lifted_kernel_detail::true_div_result_t<L, R>>
    struct scalar_div
    {
        static constexpr const char *name = "scalar_div";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static O apply(const L &lhs, const R &rhs)
        {
            if (rhs == R{}) { throw std::domain_error("div_: division by zero"); }
            if constexpr (std::is_same_v<O, Float> && std::is_arithmetic_v<L> && std::is_arithmetic_v<R>)
            {
                return static_cast<Float>(lhs) / static_cast<Float>(rhs);
            }
            else
            {
                return static_cast<O>(lhs / rhs);
            }
        }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::floor_div_result_t<L, R>>
    struct scalar_floordiv
    {
        static constexpr const char *name = "scalar_floordiv";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static O apply(const L &lhs, const R &rhs)
        {
            if constexpr (std::is_same_v<L, Int> && std::is_same_v<R, Int> && std::is_same_v<O, Int>)
            {
                return lifted_kernel_detail::floor_divide_int(lhs, rhs);
            }
            else
            {
                return static_cast<O>(lifted_kernel_detail::floor_divide_float(static_cast<Float>(lhs),
                                                                               static_cast<Float>(rhs)));
            }
        }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::modulo_result_t<L, R>>
    struct scalar_mod
    {
        static constexpr const char *name = "scalar_mod";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static O apply(const L &lhs, const R &rhs)
        {
            if constexpr (std::is_same_v<L, Int> && std::is_same_v<R, Int> && std::is_same_v<O, Int>)
            {
                return lifted_kernel_detail::modulo_int(lhs, rhs);
            }
            else
            {
                return static_cast<O>(lifted_kernel_detail::modulo_float(static_cast<Float>(lhs),
                                                                         static_cast<Float>(rhs)));
            }
        }
    };

    template <typename L, typename R = L>
    struct scalar_pow
    {
        using result_type = lifted_kernel_detail::pow_result_t<L, R>;

        static constexpr const char *name = "scalar_pow";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static result_type apply(const L &lhs, const R &rhs)
        {
            if constexpr (std::is_same_v<result_type, Int>)
            {
                static_assert(std::is_same_v<L, Int> && std::is_same_v<R, Int>);
                return lifted_kernel_detail::integer_power(lhs, rhs);
            }
            else
            {
                if (static_cast<Float>(lhs) == Float{0} && static_cast<Float>(rhs) < Float{0})
                {
                    throw std::domain_error("pow_: zero cannot be raised to a negative power");
                }
                return std::pow(static_cast<Float>(lhs), static_cast<Float>(rhs));
            }
        }
    };

    template <typename T>
    struct scalar_neg
    {
        static constexpr const char *name = "scalar_neg";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static T apply(const T &ts) { return static_cast<T>(-ts); }
    };

    template <typename T>
    struct scalar_pos
    {
        static constexpr const char *name = "scalar_pos";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static T apply(const T &ts) { return static_cast<T>(+ts); }
    };

    template <typename T>
    struct scalar_abs
    {
        static constexpr const char *name = "scalar_abs";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static T apply(const T &ts) { return static_cast<T>(std::abs(ts)); }
    };

    template <typename T>
    struct scalar_sign
    {
        static constexpr const char *name = "scalar_sign";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static T apply(const T &ts)
        {
            return ts < T{0} ? T{-1} : T{1};
        }
    };

    struct scalar_ln
    {
        static constexpr const char *name = "scalar_ln";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static Float apply(Float ts) { return std::log(ts); }
    };

    template <typename L, typename R = L>
    struct scalar_eq
    {
        static constexpr const char *name = "scalar_eq";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool commutative = true;

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs) { return lhs == rhs; }
    };

    template <typename L, typename R = L>
    struct scalar_ne
    {
        static constexpr const char *name = "scalar_ne";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool commutative = true;

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs) { return lhs != rhs; }
    };

    template <typename L, typename R = L>
    struct scalar_lt
    {
        static constexpr const char *name = "scalar_lt";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs) { return lhs < rhs; }
    };

    template <typename L, typename R = L>
    struct scalar_le
    {
        static constexpr const char *name = "scalar_le";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs) { return lhs <= rhs; }
    };

    template <typename L, typename R = L>
    struct scalar_gt
    {
        static constexpr const char *name = "scalar_gt";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs) { return lhs > rhs; }
    };

    template <typename L, typename R = L>
    struct scalar_ge
    {
        static constexpr const char *name = "scalar_ge";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs) { return lhs >= rhs; }
    };

    template <typename L, typename R = L>
    struct scalar_cmp
    {
        static constexpr const char *name = "scalar_cmp";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static CmpResult apply(const L &lhs, const R &rhs)
        {
            return lhs < rhs ? CmpResult::LT : rhs < lhs ? CmpResult::GT : CmpResult::EQ;
        }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::ordered_result_t<L, R>>
    struct scalar_min
    {
        static constexpr const char *name = "scalar_min";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static O apply(const L &lhs, const R &rhs)
        {
            if constexpr (std::is_same_v<L, R>)
            {
                return static_cast<O>(std::min(lhs, rhs));
            }
            else
            {
                return static_cast<O>(std::min(static_cast<Float>(lhs), static_cast<Float>(rhs)));
            }
        }
    };

    template <typename L, typename R = L, typename O = lifted_kernel_detail::ordered_result_t<L, R>>
    struct scalar_max
    {
        static constexpr const char *name = "scalar_max";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static O apply(const L &lhs, const R &rhs)
        {
            if constexpr (std::is_same_v<L, R>)
            {
                return static_cast<O>(std::max(lhs, rhs));
            }
            else
            {
                return static_cast<O>(std::max(static_cast<Float>(lhs), static_cast<Float>(rhs)));
            }
        }
    };

    template <typename L, typename R = L>
    struct scalar_mean
    {
        static constexpr const char *name = "scalar_mean";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Float apply(const L &lhs, const R &rhs)
        {
            return (static_cast<Float>(lhs) + static_cast<Float>(rhs)) / Float{2.0};
        }
    };

    template <typename L, typename R = L>
    struct scalar_var
    {
        static constexpr const char *name = "scalar_var";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Float apply(const L &lhs, const R &rhs)
        {
            const Float delta = static_cast<Float>(lhs) - static_cast<Float>(rhs);
            return (delta * delta) / Float{2.0};
        }
    };

    template <typename L, typename R = L>
    struct scalar_std
    {
        static constexpr const char *name = "scalar_std";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Float apply(const L &lhs, const R &rhs)
        {
            return std::sqrt(scalar_var<L, R>::apply(lhs, rhs));
        }
    };

    template <typename L, typename R = L>
    struct scalar_and
    {
        static constexpr const char *name = "scalar_and";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static Bool identity()
            requires(std::is_same_v<L, Bool> && std::is_same_v<R, Bool>)
        {
            return true;
        }

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs)
        {
            return lifted_kernel_detail::truthy(lhs) && lifted_kernel_detail::truthy(rhs);
        }
    };

    template <typename L, typename R = L>
    struct scalar_or
    {
        static constexpr const char *name = "scalar_or";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static Bool identity()
            requires(std::is_same_v<L, Bool> && std::is_same_v<R, Bool>)
        {
            return false;
        }

        [[nodiscard]] static Bool apply(const L &lhs, const R &rhs)
        {
            return lifted_kernel_detail::truthy(lhs) || lifted_kernel_detail::truthy(rhs);
        }
    };

    template <typename T>
    struct scalar_not
    {
        static constexpr const char *name = "scalar_not";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static Bool apply(const T &ts) { return !lifted_kernel_detail::truthy(ts); }
    };

    template <typename T>
    struct scalar_invert
    {
        static constexpr const char *name = "scalar_invert";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static T apply(const T &ts) { return static_cast<T>(~ts); }
    };

    struct scalar_invert_bool
    {
        static constexpr const char *name = "scalar_invert_bool";
        static constexpr std::array<std::string_view, 1> parameter_names{"ts"};

        [[nodiscard]] static Int apply(Bool ts) { return ~static_cast<Int>(ts); }
    };

    template <typename T>
    struct scalar_bit_and
    {
        static constexpr const char *name = "scalar_bit_and";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static T identity()
            requires(std::is_same_v<T, Bool>)
        {
            return true;
        }

        [[nodiscard]] static T identity()
            requires(std::is_integral_v<T> && !std::is_same_v<T, Bool>)
        {
            return static_cast<T>(~T{});
        }

        [[nodiscard]] static T apply(const T &lhs, const T &rhs)
        {
            if constexpr (std::is_same_v<T, Bool>) { return lhs && rhs; }
            else { return static_cast<T>(lhs & rhs); }
        }
    };

    template <typename T>
    struct scalar_bit_or
    {
        static constexpr const char *name = "scalar_bit_or";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static T identity() { return T{}; }

        [[nodiscard]] static T apply(const T &lhs, const T &rhs)
        {
            if constexpr (std::is_same_v<T, Bool>) { return lhs || rhs; }
            else { return static_cast<T>(lhs | rhs); }
        }
    };

    template <typename T>
    struct scalar_bit_xor
    {
        static constexpr const char *name = "scalar_bit_xor";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;

        [[nodiscard]] static T identity() { return T{}; }

        [[nodiscard]] static T apply(const T &lhs, const T &rhs)
        {
            if constexpr (std::is_same_v<T, Bool>) { return lhs != rhs; }
            else { return static_cast<T>(lhs ^ rhs); }
        }
    };

    struct scalar_lshift
    {
        static constexpr const char *name = "scalar_lshift";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Int apply(Int lhs, Int rhs)
        {
            return lhs << lifted_kernel_detail::checked_shift_count(rhs);
        }
    };

    struct scalar_rshift
    {
        static constexpr const char *name = "scalar_rshift";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};

        [[nodiscard]] static Int apply(Int lhs, Int rhs)
        {
            return lhs >> lifted_kernel_detail::checked_shift_count(rhs);
        }
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_LIFTED_KERNELS_H
