#ifndef HGRAPH_LIB_STD_OPERATORS_LOGICAL_H
#define HGRAPH_LIB_STD_OPERATORS_LOGICAL_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Boolean and bitwise operator **definitions** (markers only). Mirrors the Python
     * ``hgraph`` logical (``and`` / ``or`` / ``not``) and bitwise
     * (``&`` / ``|`` / ``^`` / ``~`` / ``<<`` / ``>>``) operators.
     */

    /** Return the boolean conjunction of two current values using their truth semantics.
        This is an eager graph operator: both ports are wired, unlike Python's scalar
        short-circuit ``and``.
        @param lhs Left-hand truth-valued input.
        @param rhs Right-hand truth-valued input.
        @return True only when both values are truthy.
        @par Python example
        @code{.py}
        ready = hg.and_(has_data, market_open)
        @endcode */
    struct and_ : Operator<"and_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Return the boolean disjunction of two current values using their truth semantics.
        Both input ports remain wired and active.
        @param lhs Left-hand truth-valued input.
        @param rhs Right-hand truth-valued input.
        @return True when either value is truthy.
        @par Python example
        @code{.py}
        alert = hg.or_(price_alert, risk_alert)
        @endcode */
    struct or_ : Operator<"or_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Negate the truth value of each input tick.
        @param ts Truth-valued input.
        @return Boolean logical negation.
        @par Python example
        @code{.py}
        closed = hg.not_(market_open)
        @endcode */
    struct not_ : Operator<"not_", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Apply bitwise or collection inversion selected by the input schema.
        Python's ``~ts`` syntax wires this operator.
        @param ts Integer, boolean, or compatible collection input.
        @return The overload-selected inverted value.
        @par Python example
        @code{.py}
        inverted_mask = ~mask
        @endcode */
    struct invert_ : Operator<"invert_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Apply bitwise AND or the corresponding structural collection operation.
        @param lhs Left-hand input.
        @param rhs Right-hand input.
        @return ``lhs & rhs`` with overload-selected structure.
        @par Python example
        @code{.py}
        common_flags = flags & allowed
        @endcode */
    struct bit_and : Operator<"bit_and", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Apply bitwise OR or the corresponding structural collection operation.
        @param lhs Left-hand input.
        @param rhs Right-hand input.
        @return ``lhs | rhs`` with overload-selected structure.
        @par Python example
        @code{.py}
        combined_flags = flags | defaults
        @endcode */
    struct bit_or : Operator<"bit_or", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Apply bitwise exclusive OR or the corresponding structural operation.
        @param lhs Left-hand input.
        @param rhs Right-hand input.
        @return ``lhs ^ rhs`` with overload-selected structure.
        @par Python example
        @code{.py}
        changed_flags = before ^ after
        @endcode */
    struct bit_xor : Operator<"bit_xor", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Shift integer bits left by the current right-hand value.
        @param lhs Integer value to shift.
        @param rhs Non-negative shift distance.
        @return ``lhs << rhs``.
        @par Python example
        @code{.py}
        mask = value << bit_count
        @endcode */
    struct lshift_ : Operator<"lshift_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Shift integer bits right by the current right-hand value.
        @param lhs Integer value to shift.
        @param rhs Non-negative shift distance.
        @return ``lhs >> rhs``.
        @par Python example
        @code{.py}
        bucket = value >> bit_count
        @endcode */
    struct rshift_ : Operator<"rshift_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_LOGICAL_H
