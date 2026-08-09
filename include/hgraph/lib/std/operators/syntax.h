#ifndef HGRAPH_LIB_STD_OPERATORS_SYNTAX_H
#define HGRAPH_LIB_STD_OPERATORS_SYNTAX_H

#include <hgraph/lib/std/operators/operators.h>
#include <hgraph/types/graph_wiring.h>

#include <stdexcept>
#include <type_traits>

namespace hgraph::stdlib::syntax
{
    /**
     * Opt-in C++ expression syntax for wiring standard operators inside a graph compose body.
     *
     * Include ``<hgraph/lib/std/std_operators.h>`` and opt in locally with
     * ``using namespace hgraph::stdlib::syntax;``. The overloads below do not implement
     * operator semantics themselves; they forward to ``wire<stdlib::...>`` so the
     * normal operator registry remains the single overload-resolution path.
     */
    namespace detail
    {
        template <typename T>
        concept port_arg = graph_wiring_detail::is_port<std::remove_cvref_t<T>>::value;

        template <typename L, typename R>
        concept has_port_arg = port_arg<L> || port_arg<R>;

        template <typename L, typename R>
            requires has_port_arg<L, R>
        [[nodiscard]] Wiring &common_wiring(const L &lhs, const R &rhs)
        {
            if constexpr (port_arg<L> && port_arg<R>)
            {
                Wiring &w = lhs.checked_wiring();
                if (&rhs.checked_wiring() != &w)
                {
                    throw std::logic_error("Port operator operands belong to different Wiring instances");
                }
                return w;
            }
            else if constexpr (port_arg<L>)
            {
                return lhs.checked_wiring();
            }
            else
            {
                return rhs.checked_wiring();
            }
        }

        template <typename Operator, typename L, typename R>
            requires has_port_arg<L, R>
        [[nodiscard]] auto binary(const L &lhs, const R &rhs)
        {
            return wire<Operator>(common_wiring(lhs, rhs), lhs, rhs);
        }

        template <typename Operator, port_arg P>
        [[nodiscard]] auto unary(const P &port)
        {
            return wire<Operator>(port.checked_wiring(), port);
        }
    }  // namespace detail

    template <detail::port_arg P>
    [[nodiscard]] auto operator+(const P &port)
    {
        return detail::unary<pos_>(port);
    }

    template <detail::port_arg P>
    [[nodiscard]] auto operator-(const P &port)
    {
        return detail::unary<neg_>(port);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator+(const L &lhs, const R &rhs)
    {
        return detail::binary<add_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator-(const L &lhs, const R &rhs)
    {
        return detail::binary<sub_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator*(const L &lhs, const R &rhs)
    {
        return detail::binary<mul_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator/(const L &lhs, const R &rhs)
    {
        return detail::binary<div_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator%(const L &lhs, const R &rhs)
    {
        return detail::binary<mod_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator==(const L &lhs, const R &rhs)
    {
        return detail::binary<eq_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator!=(const L &lhs, const R &rhs)
    {
        return detail::binary<ne_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator<(const L &lhs, const R &rhs)
    {
        return detail::binary<lt_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator<=(const L &lhs, const R &rhs)
    {
        return detail::binary<le_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator>(const L &lhs, const R &rhs)
    {
        return detail::binary<gt_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator>=(const L &lhs, const R &rhs)
    {
        return detail::binary<ge_>(lhs, rhs);
    }

    template <detail::port_arg P>
    [[nodiscard]] auto operator!(const P &port)
    {
        return detail::unary<not_>(port);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator&&(const L &lhs, const R &rhs)
    {
        return detail::binary<and_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator||(const L &lhs, const R &rhs)
    {
        return detail::binary<or_>(lhs, rhs);
    }

    template <detail::port_arg P>
    [[nodiscard]] auto operator~(const P &port)
    {
        return detail::unary<invert_>(port);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator&(const L &lhs, const R &rhs)
    {
        return detail::binary<bit_and>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator|(const L &lhs, const R &rhs)
    {
        return detail::binary<bit_or>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator^(const L &lhs, const R &rhs)
    {
        return detail::binary<bit_xor>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator<<(const L &lhs, const R &rhs)
    {
        return detail::binary<lshift_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto operator>>(const L &lhs, const R &rhs)
    {
        return detail::binary<rshift_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto floordiv(const L &lhs, const R &rhs)
    {
        return detail::binary<floordiv_>(lhs, rhs);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto pow(const L &lhs, const R &rhs)
    {
        return detail::binary<pow_>(lhs, rhs);
    }

    template <detail::port_arg P>
    [[nodiscard]] auto abs(const P &port)
    {
        return detail::unary<abs_>(port);
    }

    template <detail::port_arg P>
    [[nodiscard]] auto sign(const P &port)
    {
        return detail::unary<hgraph::stdlib::sign>(port);
    }

    template <detail::port_arg P>
    [[nodiscard]] auto ln(const P &port)
    {
        return detail::unary<hgraph::stdlib::ln>(port);
    }

    template <typename L, typename R>
        requires detail::has_port_arg<L, R>
    [[nodiscard]] auto cmp(const L &lhs, const R &rhs)
    {
        return detail::binary<cmp_>(lhs, rhs);
    }
}  // namespace hgraph::stdlib::syntax

#endif  // HGRAPH_LIB_STD_OPERATORS_SYNTAX_H
