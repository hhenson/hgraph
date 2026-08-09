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

    /** ``and_`` — the ``and`` operator (truthy combination), yielding ``TS<Bool>``. */
    struct and_ : Operator<"and_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``or_`` — the ``or`` operator, yielding ``TS<Bool>``. */
    struct or_ : Operator<"or_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``not_`` — the unary ``not`` operator, yielding ``TS<Bool>``. */
    struct not_ : Operator<"not_", In<"ts", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``invert_`` — the unary ``~`` (bitwise invert) operator (``~ts -> O``). */
    struct invert_ : Operator<"invert_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``bit_and`` — the ``&`` operator (``L & R -> O``). */
    struct bit_and : Operator<"bit_and", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``bit_or`` — the ``|`` operator (``L | R -> O``). */
    struct bit_or : Operator<"bit_or", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``bit_xor`` — the ``^`` operator (``L ^ R -> O``). */
    struct bit_xor : Operator<"bit_xor", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``lshift_`` — the ``<<`` operator (``L << R -> O``). */
    struct lshift_ : Operator<"lshift_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``rshift_`` — the ``>>`` operator (``L >> R -> O``). */
    struct rshift_ : Operator<"rshift_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_LOGICAL_H
