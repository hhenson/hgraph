#pragma once

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/scalar_round.h>
#include <hgraph/types/primitive_types.h>
#include <native.h>
#include <stdexcept>

namespace hgl::stdlib
{

    struct ScalarNative
    {
        static hgraph::Int len(const hgraph::Str &value) noexcept { return static_cast<hgraph::Int>(value.size()); }

        static hgraph::Bool is_empty(const hgraph::Str &value) noexcept { return value.empty(); }

        static hgraph::Bool contains(const hgraph::Str &value, const hgraph::Str &needle) noexcept {
            return value.contains(needle);
        }

        static hgraph::Bool starts_with(const hgraph::Str &value, const hgraph::Str &prefix) noexcept {
            return value.starts_with(prefix);
        }

        static hgraph::Bool ends_with(const hgraph::Str &value, const hgraph::Str &suffix) noexcept {
            return value.ends_with(suffix);
        }

        static hgraph::Bool truthy(hgraph::Bool value) noexcept { return static_cast<hgraph::Bool>(value); }

        static hgraph::Bool truthy(hgraph::Int value) noexcept { return static_cast<hgraph::Bool>(value); }

        static hgraph::Bool truthy(hgraph::Float value) noexcept { return static_cast<hgraph::Bool>(value); }

        static hgraph::Bool truthy(const hgraph::Str &value) noexcept { return !value.empty(); }

        static hgraph::Int absolute(hgraph::Int value) noexcept { return std::abs(value); }

        static hgraph::Float absolute(hgraph::Float value) noexcept { return std::abs(value); }

        static hgraph::Float logarithm(hgraph::Float value) noexcept { return std::log(value); }

        static hgraph::Float round_decimal(hgraph::Float value, hgraph::Int digits) {
            return hgraph::stdlib::scalar_round_decimal(value, digits);
        }

        static hgraph::Int invert(hgraph::Bool value) noexcept { return ~static_cast<hgraph::Int>(value); }

        static hgraph::Bool bit_and(hgraph::Bool lhs, hgraph::Bool rhs) noexcept { return static_cast<hgraph::Bool>(lhs & rhs); }

        static hgraph::Bool bit_or(hgraph::Bool lhs, hgraph::Bool rhs) noexcept { return static_cast<hgraph::Bool>(lhs | rhs); }

        static hgraph::Bool bit_xor(hgraph::Bool lhs, hgraph::Bool rhs) noexcept { return static_cast<hgraph::Bool>(lhs ^ rhs); }

        static hgraph::Int invert(hgraph::Int value) noexcept { return ~static_cast<hgraph::Int>(value); }

        static hgraph::Int bit_and(hgraph::Int lhs, hgraph::Int rhs) noexcept { return static_cast<hgraph::Int>(lhs & rhs); }

        static hgraph::Int bit_or(hgraph::Int lhs, hgraph::Int rhs) noexcept { return static_cast<hgraph::Int>(lhs | rhs); }

        static hgraph::Int bit_xor(hgraph::Int lhs, hgraph::Int rhs) noexcept { return static_cast<hgraph::Int>(lhs ^ rhs); }

        static hgraph::Int as_int(hgraph::Bool value) noexcept { return static_cast<hgraph::Int>(value); }

        static void require_positive_delay(const hgraph::TimeDelta &delay) {
            if (delay <= hgraph::TimeDelta{}) { throw std::invalid_argument("delay must be positive"); }
        }

        static hgraph::Int power(hgraph::Int lhs, hgraph::Int rhs) {
            return hgraph::stdlib::scalar_pow<hgraph::Int>::apply(lhs, rhs);
        }

        static hgraph::Float power(hgraph::Float lhs, hgraph::Float rhs) {
            return hgraph::stdlib::scalar_pow<hgraph::Float>::apply(lhs, rhs);
        }

        static hgraph::Int shift_left(hgraph::Int lhs, hgraph::Int rhs) { return hgraph::stdlib::scalar_lshift::apply(lhs, rhs); }

        static hgraph::Int shift_right(hgraph::Int lhs, hgraph::Int rhs) { return hgraph::stdlib::scalar_rshift::apply(lhs, rhs); }

        static hgraph::Str slice(const hgraph::Str &value, hgraph::Int begin, hgraph::Int end) noexcept {
            const auto size      = static_cast<hgraph::Int>(value.size());
            const auto normalize = [size](hgraph::Int index) {
                return static_cast<std::size_t>(std::clamp(index < 0 ? size + index : index, hgraph::Int{0}, size));
            };
            const std::size_t first  = normalize(begin);
            const std::size_t finish = std::max(first, normalize(end));
            return value.substr(first, finish - first);
        }

        static hgraph::Int as_int(hgraph::Float value) noexcept { return static_cast<hgraph::Int>(value); }

        static hgraph::Float as_float(hgraph::Bool value) noexcept { return static_cast<hgraph::Float>(value); }

        static hgraph::Float as_float(hgraph::Int value) noexcept { return static_cast<hgraph::Float>(value); }
    };

    inline constexpr auto scalar_native = hgraph_::native::native_interface::bind<ScalarNative>();

}  // namespace hgl::stdlib
