#ifndef HGRAPH_LIB_STD_OPERATORS_COMPARISON_H
#define HGRAPH_LIB_STD_OPERATORS_COMPARISON_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <string_view>

namespace hgraph::stdlib
{
    /**
     * Comparison operator **definitions** (markers only). Mirrors the Python ``hgraph``
     * comparison operators. The relational operators compare two same-typed operands and
     * yield ``TS<Bool>``; ``cmp_`` yields a three-way ``CmpResult``; ``min_`` / ``max_``
     * are variadic (unary = over a collection / running, n-ary = element-wise).
     */

    /** Three-way comparison result (``LT`` / ``EQ`` / ``GT``); registered as a scalar. */
    enum class CmpResult : std::int64_t
    {
        LT = -1,
        EQ = 0,
        GT = 1
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::CmpResult>
    {
        static constexpr std::string_view value{"CmpResult"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** Compare two current values for equality. Python's ``lhs == rhs`` syntax wires
        this operator, including structural overloads for hgraph collections.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return True when the values compare equal.
        @par Python example
        @code{.py}
        unchanged = current == previous
        @endcode */
    struct eq_ : Operator<"eq_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Compare two current values for inequality. Python's ``lhs != rhs`` syntax wires it.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return True when the values do not compare equal.
        @par Python example
        @code{.py}
        changed = current != previous
        @endcode */
    struct ne_ : Operator<"ne_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Test whether ``lhs`` sorts before ``rhs`` using the selected value semantics.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return The result of ``lhs < rhs``.
        @par Python example
        @code{.py}
        below_limit = value < limit
        @endcode */
    struct lt_ : Operator<"lt_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Test whether ``lhs`` sorts before or equals ``rhs``.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return The result of ``lhs <= rhs``.
        @par Python example
        @code{.py}
        within_limit = value <= limit
        @endcode */
    struct le_ : Operator<"le_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Test whether ``lhs`` sorts after ``rhs``.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return The result of ``lhs > rhs``.
        @par Python example
        @code{.py}
        above_limit = value > limit
        @endcode */
    struct gt_ : Operator<"gt_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Test whether ``lhs`` sorts after or equals ``rhs``.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return The result of ``lhs >= rhs``.
        @par Python example
        @code{.py}
        reached_limit = value >= limit
        @endcode */
    struct ge_ : Operator<"ge_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Compare two values once and classify the result as ``LT``, ``EQ``, or ``GT``.
        This is useful with ``if_cmp`` when three branches must share one comparison.
        @param lhs Left-hand value.
        @param rhs Right-hand value.
        @return A ``CmpResult`` classification.
        @par Python example
        @code{.py}
        ordering = hg.cmp_(lhs, rhs)
        @endcode */
    struct cmp_ : Operator<"cmp_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<CmpResult>>>
    {
    };

    /** Select minima according to input shape and arity.
        Unary scalar input produces a running minimum; unary collection input reduces its
        current values; multiple inputs select element-wise minima. A reset restarts
        running state and ``default_value`` covers empty collections.
        @param ts Scalar, collection, or variadic values.
        @param lhs Left-hand value in binary overloads.
        @param rhs Right-hand value in binary overloads.
        @param reset Optional signal that clears running state.
        @param default_value Value used when an input collection is empty.
        @param __strict__ When true, every variadic input must be valid.
        @return The running, reduced, or element-wise minimum.
        @par Python example
        @code{.py}
        running_low = hg.min_(price, reset=session_start)
        lowest_price = hg.min_(prices_by_venue)
        @endcode */
    struct min_ : Operator<"min_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Select maxima according to input shape and arity.
        Unary scalar input produces a running maximum; unary collection input reduces its
        current values; multiple inputs select element-wise maxima. A reset restarts
        running state and ``default_value`` covers empty collections.
        @param ts Scalar, collection, or variadic values.
        @param lhs Left-hand value in binary overloads.
        @param rhs Right-hand value in binary overloads.
        @param reset Optional signal that clears running state.
        @param default_value Value used when an input collection is empty.
        @param __strict__ When true, every variadic input must be valid.
        @return The running, reduced, or element-wise maximum.
        @par Python example
        @code{.py}
        running_high = hg.max_(price, reset=session_start)
        highest_price = hg.max_(prices_by_venue)
        @endcode */
    struct max_ : Operator<"max_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib


#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>

namespace hgraph
{
    /** Python conversion binds to the type AT DEFINITION (type-erasure rule:
        every ops_for<CmpResult> instantiation must see this). */
    template <>
    struct python_conversion_traits<stdlib::CmpResult>
    {
        static nb::object to_python(const stdlib::CmpResult &value)
        {
            nb::object &enum_class = python_bridge::cmp_result_enum_slot();
            const auto  raw        = static_cast<std::int64_t>(value);
            return enum_class.is_valid() ? enum_class(raw) : nb::cast(raw);
        }

        static stdlib::CmpResult from_python(nb::handle source)
        {
            if (nb::hasattr(source, "value"))
            {
                return static_cast<stdlib::CmpResult>(nb::cast<std::int64_t>(source.attr("value")));
            }
            return static_cast<stdlib::CmpResult>(nb::cast<std::int64_t>(source));
        }
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_LIB_STD_OPERATORS_COMPARISON_H
