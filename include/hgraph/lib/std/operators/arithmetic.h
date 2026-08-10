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
     * Python ``hgraph`` arithmetic operators (``release/0.5:hgraph/_operators/_operators.py``).
     *
     * The operator signature is a *suggestion*: binary operators declare independent
     * type variables for ``lhs`` / ``rhs`` / ``OUT`` so a single name spans
     * homogeneous (``int + int``), mixed (``int + float``) and heterogeneous
     * (``DateTime + TimeDelta``) cases. Matching is driven by each implementation's own
     * signature, not by these abstract ones.
     */

    /**
     * Divide-by-zero policy — the wiring-time choice of what ``div_`` (and other dividing
     * operators) produce when the divisor is zero. Mirrors ``release/0.5``'s ``DivideByZero``:
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
    /** Add two time-series values using the overload selected for their schemas.
        Supports numeric promotion, string concatenation, temporal arithmetic,
        collection broadcasting, and keyed-set insertion. Python's ``lhs + rhs``
        syntax wires this operator.
        @param lhs Left-hand value. A tick triggers a new result once the overload's
                   validity requirements are met.
        @param rhs Right-hand value; compatible plain values are lifted to constants.
        @param __strict__ When true, wait until both operands are valid. Non-strict
                          overloads may forward the valid operand when the other is absent.
        @param month_end_policy Policy used when adding a calendar period to a date
                                whose day does not exist in the target month.
        @return The sum, with its schema selected from both operand schemas.
        @par Python example
        @code{.py}
        total = lhs + rhs  # equivalent to hg.add_(lhs, rhs)
        @endcode */
    struct add_ : Operator<"add_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Subtract ``rhs`` from ``lhs`` using the overload selected for their schemas.
        Numeric, temporal, collection, and keyed-set forms are supported; Python's
        ``lhs - rhs`` syntax wires this operator.
        @param lhs Value from which ``rhs`` is subtracted.
        @param rhs Value to subtract; compatible plain values are lifted to constants.
        @param __strict__ When true, require both operands to be valid before ticking.
        @param month_end_policy Policy used when subtracting a calendar period near month end.
        @return The difference, with its schema selected from both operand schemas.
        @par Python example
        @code{.py}
        change = lhs - rhs  # equivalent to hg.sub_(lhs, rhs)
        @endcode */
    struct sub_ : Operator<"sub_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Multiply two values, including numeric promotion and collection broadcasting.
        Python's ``lhs * rhs`` syntax uses the normal strict overload. Call ``mul_``
        directly to select non-strict validity behaviour where it is supported.
        @param lhs Left-hand multiplicand.
        @param rhs Right-hand multiplicand.
        @param __strict__ When true, an invalid operand suppresses output. When false,
                          an overload may forward the valid operand instead.
        @return The product selected from the operand schemas.
        @par Python example
        @code{.py}
        scaled = hg.mul_(price, quantity, __strict__=True)
        @endcode */
    struct mul_ : Operator<"mul_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Divide ``lhs`` by ``rhs`` using true-division semantics.
        The scalar ``divide_by_zero`` choice is fixed at wiring time, so the selected
        behaviour adds no per-tick policy dispatch.
        @param lhs Dividend.
        @param rhs Divisor.
        @param divide_by_zero Behaviour for a zero divisor: raise, emit NaN or infinity,
                              emit zero or one, or suppress the tick.
        @return The quotient, normally promoted to a floating-point schema.
        @par Python example
        @code{.py}
        ratio = hg.div_(numerator, denominator, divide_by_zero=hg.DivideByZero.NAN)
        @endcode */
    struct div_ : Operator<"div_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Divide ``lhs`` by ``rhs`` and round the quotient toward negative infinity.
        Python's ``lhs // rhs`` syntax wires this operator.
        @param lhs Dividend.
        @param rhs Divisor.
        @return The floor-division quotient.
        @par Python example
        @code{.py}
        whole_batches = items // batch_size
        @endcode */
    struct floordiv_ : Operator<"floordiv_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Return the remainder paired with floor division of ``lhs`` by ``rhs``.
        Python's ``lhs % rhs`` syntax wires this operator.
        @param lhs Dividend.
        @param rhs Divisor.
        @return The remainder, with the divisor's sign under Python semantics.
        @par Python example
        @code{.py}
        bucket_offset = sequence % bucket_count
        @endcode */
    struct mod_ : Operator<"mod_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Compute floor quotient and remainder together.
        @param lhs Dividend.
        @param rhs Divisor.
        @return A two-element time-series list containing quotient then remainder.
        @par Python example
        @code{.py}
        quotient_and_remainder = hg.divmod_(items, batch_size)
        @endcode */
    struct divmod_ : Operator<"divmod_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Raise ``lhs`` to the power ``rhs``. Python's ``lhs ** rhs`` syntax wires
        this operator; overloads select integer or floating-point result semantics.
        @param lhs Base value.
        @param rhs Exponent.
        @param divide_by_zero Policy used by overloads where a negative exponent
                              would divide by a zero base.
        @return ``lhs`` raised to ``rhs``.
        @par Python example
        @code{.py}
        squared = values ** 2
        @endcode */
    struct pow_ : Operator<"pow_", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** Round a floating-point value to ``n_digits`` decimal places using Python-compatible
        rounding semantics.
        @param ts Values to round.
        @param n_digits Decimal precision; this may itself vary as a time series.
        @return The rounded floating-point value.
        @par Python example
        @code{.py}
        display_price = hg.round_(price, 2)
        @endcode */
    struct round_ : Operator<"round_", In<"ts", TS<Float>>, In<"n_digits", TS<Int>>, Out<TS<Float>>>
    {
    };

    /** Negate each input value. Python's ``-ts`` syntax wires this operator.
        @param ts Numeric, duration, or compatible collection input.
        @return The additive inverse of ``ts``.
        @par Python example
        @code{.py}
        debit = -credit
        @endcode */
    struct neg_ : Operator<"neg_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Apply unary plus to each input value. Python's ``+ts`` syntax wires this
        operator and preserves the resolved value schema.
        @param ts Numeric or compatible collection input.
        @return The positive form of ``ts``.
        @par Python example
        @code{.py}
        normalized = +value
        @endcode */
    struct pos_ : Operator<"pos_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Return the absolute magnitude of a numeric value, duration, or compatible
        collection. Python's ``abs(ts)`` syntax wires this operator.
        @param ts Input whose sign is removed.
        @return The non-negative magnitude with the overload-selected schema.
        @par Python example
        @code{.py}
        magnitude = abs(change)
        @endcode */
    struct abs_ : Operator<"abs_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Classify the sign of a numeric time-series value.
        @param ts Numeric input.
        @return ``-1`` for a negative value and ``+1`` otherwise, including zero.
        @par Python example
        @code{.py}
        direction = hg.sign(change)
        @endcode */
    struct sign : Operator<"sign", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Compute the natural logarithm of a floating-point time-series value.
        @param ts Positive floating-point input.
        @return The base-e logarithm of ``ts``.
        @par Python example
        @code{.py}
        log_price = hg.ln(price)
        @endcode */
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
