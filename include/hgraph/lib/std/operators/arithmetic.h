#ifndef HGRAPH_LIB_STD_OPERATORS_ARITHMETIC_H
#define HGRAPH_LIB_STD_OPERATORS_ARITHMETIC_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <string_view>

namespace hgraph::stdlib
{
    /**
     * Arithmetic operator **definitions** — the abstract markers only; implementations
     * are registered separately (see ``register_standard_operators``). Mirrors the
     * Python ``hgraph`` arithmetic operators (``ext/main/hgraph/_operators/_operators.py``).
     *
     * The operator signature is a *suggestion*: binary operators declare independent
     * type variables for ``lhs`` / ``rhs`` / the result so a single name spans
     * homogeneous (``int + int``), mixed (``int + float``) and heterogeneous
     * (``DateTime + TimeDelta``) cases. Matching is driven by each implementation's own
     * signature, not by these abstract ones.
     */

    /**
     * Divide-by-zero policy — the wiring-time choice of what ``div_`` (and other dividing
     * operators) produce when the divisor is zero. Mirrors ``ext/main``'s ``DivideByZero``:
     * ``Error`` raise · ``Nan`` · ``Inf`` · ``NoTick`` (no tick; Python ``NONE``) · ``Zero`` · ``One``.
     * Registered as a scalar so it can be a wiring-time ``Scalar<>`` argument.
     */
    enum class DivideByZero : std::int64_t
    {
        Error,
        Nan,
        Inf,
        NoTick,
        Zero,
        One
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::DivideByZero>
    {
        static constexpr std::string_view value{"DivideByZero"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** ``add_`` — the ``+`` operator. Operands and result may all differ (``L + R -> O``). */
    struct add_ : Operator<"add_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``sub_`` — the ``-`` operator (``L - R -> O``). */
    struct sub_ : Operator<"sub_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``mul_`` — the ``*`` operator (``L * R -> O``). (Python takes an optional ``__strict__`` flag.) */
    struct mul_ : Operator<"mul_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``div_`` — the ``/`` (true division) operator (``L / R -> O``). Implementations may take an
        optional ``Scalar<"divide_by_zero", DivideByZero>`` wiring-time policy. */
    struct div_ : Operator<"div_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``floordiv_`` — the ``//`` (floor division) operator (``L // R -> O``). */
    struct floordiv_ : Operator<"floordiv_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``mod_`` — the ``%`` operator (``L % R -> O``). */
    struct mod_ : Operator<"mod_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``divmod_`` — the ``divmod`` operator. Result is a 2-element list ``(quotient, remainder)``. */
    struct divmod_ : Operator<"divmod_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``pow_`` — the ``**`` operator (``L ** R -> O``). */
    struct pow_ : Operator<"pow_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``round_`` — round a float to ``n_digits`` decimal places (python's
        correctly-rounded decimal semantics). */
    struct round_ : Operator<"round_", In<"ts", TS<Float>>, In<"n_digits", TS<Int>>, Out<TS<Float>>>
    {
    };

    /** ``neg_`` — the unary ``-`` operator (``-ts -> O``). */
    struct neg_ : Operator<"neg_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``pos_`` — the unary ``+`` operator (``+ts -> O``). */
    struct pos_ : Operator<"pos_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``abs_`` — the ``abs`` operator (``abs(ts) -> O``). */
    struct abs_ : Operator<"abs_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``sign`` — Python-compatible numeric sign: ``-1`` for negative values, ``+1`` otherwise. */
    struct sign : Operator<"sign", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``ln`` — the natural logarithm of a ``TS<Float>`` value. */
    struct ln : Operator<"ln", In<"ts", TS<Float>>, Out<TS<Float>>>
    {
    };
}  // namespace hgraph::stdlib


#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>

namespace hgraph
{
    /** Python conversion binds to the type AT DEFINITION (type-erasure rule). */
    template <>
    struct python_conversion_traits<stdlib::DivideByZero>
    {
        static nb::object to_python(const stdlib::DivideByZero &value)
        {
            nb::object &enum_class = python_bridge::divide_by_zero_enum_slot();
            const auto  raw        = static_cast<std::int64_t>(value);
            return enum_class.is_valid() ? enum_class(raw) : nb::cast(raw);
        }

        static stdlib::DivideByZero from_python(nb::handle source)
        {
            if (nb::hasattr(source, "value"))
            {
                return static_cast<stdlib::DivideByZero>(nb::cast<std::int64_t>(source.attr("value")));
            }
            return static_cast<stdlib::DivideByZero>(nb::cast<std::int64_t>(source));
        }
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_LIB_STD_OPERATORS_ARITHMETIC_H
