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
    /** ``eq_`` — the ``==`` operator. */
    struct eq_ : Operator<"eq_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``ne_`` — the ``!=`` operator. */
    struct ne_ : Operator<"ne_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``lt_`` — the ``<`` operator. */
    struct lt_ : Operator<"lt_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``le_`` — the ``<=`` operator. */
    struct le_ : Operator<"le_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``gt_`` — the ``>`` operator. */
    struct gt_ : Operator<"gt_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``ge_`` — the ``>=`` operator. */
    struct ge_ : Operator<"ge_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``cmp_`` — three-way comparison; returns ``LT`` / ``EQ`` / ``GT`` in one step. */
    struct cmp_ : Operator<"cmp_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<CmpResult>>>
    {
    };

    /** ``min_`` — binary element-wise minimum. Collection / variadic forms are separate overloads. */
    struct min_ : Operator<"min_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``max_`` — binary element-wise maximum. Collection / variadic forms are separate overloads. */
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
