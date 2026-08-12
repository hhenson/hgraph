#ifndef HGRAPH_ANALYTICS_OPERATORS_H
#define HGRAPH_ANALYTICS_OPERATORS_H

#include <hgraph/analytics/export.h>

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::analytics
{
    /** Subtract the preceding valid observation from the current observation.
        The first valid observation warms the retained prior-value state and does
        not produce output. Invalid cycles neither update that state nor trigger.
        @param ts Integer or floating-point input stream.
        @return Successive differences with the same scalar type as ``ts``.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        change = hga.diff(price)
        @endcode */
    struct diff
        : Operator<"hgraph.analytics.diff", In<"ts", TS<ScalarVar<"T">>>,
                   Out<TS<ScalarVar<"T">>>>
    {
    };

    /** Count valid input ticks cumulatively, with an optional reset signal.
        A same-cycle reset is applied before a source tick is admitted. A reset-only
        cycle clears state without producing output.
        @param ts Signal or stream whose valid ticks are counted; values are ignored.
        @param reset Optional signal that restarts the count from one on the next
                     same-cycle or subsequent source tick.
        @return Running integer tick count.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        session_count = hga.count(updates, reset=session_start)
        @endcode */
    struct count
        : Operator<"hgraph.analytics.count", In<"ts", SIGNAL>, Out<TS<Int>>>
    {
    };

    /** Constrain each numeric observation to an inclusive range.
        Bounds are fixed at wiring time and must share the resolved integer or
        floating-point type of the input. The node has no warm-up state and emits
        for every valid source tick.
        @param ts Numeric input stream.
        @param min Lower inclusive bound.
        @param max Upper inclusive bound.
        @return ``min`` below the range, ``max`` above it, otherwise ``ts``.
        @throws std::invalid_argument during node start when ``min > max``.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        bounded = hga.clip(ratio, 0.0, 1.0)
        @endcode */
    struct clip
        : Operator<"hgraph.analytics.clip", In<"ts", TS<ScalarVar<"T">>>,
                   Scalar<"min", ScalarVar<"T">>, Scalar<"max", ScalarVar<"T">>,
                   Out<TS<ScalarVar<"T">>>>
    {
    };

    /** Compute an exponentially weighted moving average of floating observations.
        The first valid observation initializes the retained average. Each later
        valid tick emits ``alpha * current + (1 - alpha) * previous``. Invalid
        cycles neither update state nor trigger output.
        @param ts Floating-point input stream.
        @param alpha Wiring-time smoothing factor. Values are applied as supplied
                     for compatibility with the migrated 0.8 operator.
        @return Running exponentially weighted moving average.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        smoothed = hga.ewma(price, alpha=0.2)
        @endcode */
    struct ewma
        : Operator<"hgraph.analytics.ewma", In<"ts", TS<Float>>,
                   Scalar<"alpha", Float>, Out<TS<Float>>>
    {
    };

    /** Compute fractional change from an earlier valid observation.
        For a positive observation-count ``period``, the result is
        ``(current - prior) / prior``. The first ``period`` valid observations
        produce no output. Invalid source cycles are not counted, and the graph
        ticks only when a valid source observation arrives.
        @param ts Integer or floating-point input stream.
        @param period Positive valid-observation count fixed at wiring time;
                      defaults to one. Durations and negative look-ahead periods
                      are not supported.
        @param divide_by_zero Wiring-time policy applied when the prior value is
                              zero; defaults to ``DivideByZero::Error``.
        @return Floating-point fractional change. ``0.05`` denotes five percent.
        @throws std::invalid_argument while wiring when ``period`` is not positive.
        @throws std::domain_error during evaluation for a zero prior value when
                                  ``divide_by_zero`` is ``Error``.
        @par Python example
        @code{.py}
        import hgraph as hg
        import hgraph_analytics as hga

        change = hga.pct_change(price, period=12,
                                divide_by_zero=hg.DivideByZero.NAN)
        @endcode */
    struct pct_change
        : Operator<"hgraph.analytics.pct_change",
                   In<"ts", TS<ScalarVar<"T">>>,
                   Scalar<"period", Int>,
                   Scalar<"divide_by_zero", stdlib::DivideByZero>,
                   Out<TS<Float>>>
    {
    };

    /** Materialize a fixed tick window as an array, padding its warm-up suffix.
        Each valid window tick emits after the ``TSW`` reaches its configured
        minimum population. The output capacity is the wiring-time window
        period; positions beyond the current population use ``zero`` or the
        element type's default value. The node owns no history.
        @param window Fixed tick-count ``TSW``. Duration windows are rejected
                      because they have no fixed output capacity.
        @param zero Optional live or wiring-time padding value with the window's
                    element type. It defaults to the type's zero value.
        @return Fixed-capacity array containing chronological window values.
        @throws std::invalid_argument if a materialized value cannot fit the
                                     resolved output shape.
        @par Python example
        @code{.py}
        import hgraph as hg
        import hgraph_analytics as hga
        values = hga.window_values(hg.to_window(price, 20, 5), zero=0.0)
        @endcode */
    struct window_values
        : Operator<"hgraph.analytics.window_values",
                   In<"window", TsVar<"W">>, Out<TsVar<"__out__">>>
    {
    };

    /** Select an item or lower-rank slice from a shaped array.
        Every valid array tick emits. ``index`` is fixed while wiring and may be
        an integer or an integer tuple; negative components count from the end.
        The node retains no state.
        @param values Shaped array input.
        @param index Wiring-time integer or integer tuple.
        @return Selected scalar or the remaining lower-rank array.
        @throws std::out_of_range for an invalid component or too many components.
        @throws std::invalid_argument when ``index`` is not integer-valued.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        second_row = hga.array_get_item(matrix, 1)
        @endcode */
    struct array_get_item
        : Operator<"hgraph.analytics.array_get_item",
                   In<"values", TsVar<"A">>,
                   Scalar<"index", ScalarVar<"I">>, Out<TsVar<"__out__">>>
    {
    };

    /** Compute the cumulative sum of a numeric shaped array.
        Every valid array tick emits and the node retains no state. Omitting
        ``axis`` flattens the result; supplying a wiring-time axis preserves the
        input shape and accepts negative indexing. Integer addition has defined
        two's-complement wrapping.
        @param values Integer or floating-point shaped array.
        @param axis Optional wiring-time axis. It defaults to flattened traversal.
        @return Cumulative sums with the input leaf type.
        @throws std::out_of_range when ``axis`` is outside the input rank.
        @throws std::invalid_argument when the runtime value is not rectangular.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        running_rows = hga.cumulative_sum(matrix, axis=1)
        @endcode */
    struct cumulative_sum
        : Operator<"hgraph.analytics.cumulative_sum",
                   In<"values", TsVar<"A">>, Out<TsVar<"__out__">>>
    {
    };

    /** Compute correlation coefficients for one or two numeric arrays.
        A tick on either array schedules evaluation; output requires every
        supplied array to be valid. One- and two-dimensional arrays are
        supported. ``rowvar`` is fixed while wiring and defaults to true. The
        node retains no state and uses Boost.Math's correlation algorithm.
        @param x First numeric shaped array.
        @param y Optional second numeric shaped array.
        @param rowvar Treat rows as variables when true and columns when false.
        @return A scalar for one vector, otherwise a square coefficient matrix.
                Constant variables produce NaN coefficients.
        @throws std::invalid_argument for unsupported ranks, empty variables, or
                                     mismatched observation counts.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        coefficients = hga.correlation(observations, rowvar=False)
        @endcode */
    struct correlation
        : Operator<"hgraph.analytics.correlation",
                   In<"x", TsVar<"X">>, Scalar<"rowvar", Bool>,
                   Out<TsVar<"__out__">>>
    {
    };

    /** Select a scalar quantile from a numeric shaped array or tick window.
        A tick on either input schedules evaluation; the operator emits only
        when both inputs are valid and a selected window has reached its minimum
        population. It retains no state beyond state already owned by a supplied
        ``TSW``. Arrow Compute performs the interpolation using one of
        ``linear``, ``lower``, ``higher``, ``midpoint``, or ``nearest``.
        @param values Integer or floating-point ``Array`` or ``TSW`` input.
        @param q Live quantile in the inclusive range ``[0, 1]``.
        @param method Wiring-time interpolation method; defaults to ``linear``.
        @return Floating-point quantile. An Arrow null result is represented by NaN.
        @throws std::invalid_argument for an empty input, out-of-range ``q``, or
                                     unsupported interpolation method.
        @throws std::runtime_error if Arrow Compute rejects the operation.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        median = hga.quantile(returns, 0.5)
        @endcode */
    struct quantile
        : Operator<"hgraph.analytics.quantile", In<"values", TsVar<"A">>,
                   In<"q", TS<Float>>, Scalar<"method", Str>, Out<TS<Float>>>
    {
    };

    /** Compute population or sample standard deviation over a numeric shaped array.
        The node has no warm-up state and emits for every valid input array.
        Arrow Compute uses the divisor ``N - ddof``; ``ddof`` is fixed at
        wiring time and defaults to zero.
        @param values Integer or floating-point shaped array.
        @param ddof Delta degrees of freedom subtracted from the observation count.
        @return Floating-point standard deviation. An Arrow null result is NaN.
        @throws std::invalid_argument when ``ddof`` is outside Arrow's integer range.
        @throws std::runtime_error if Arrow Compute rejects the operation.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        sample_volatility = hga.array_std(observations, ddof=1)
        @endcode */
    struct array_std
        : Operator<"hgraph.analytics.array_std", In<"values", TsVar<"A">>,
                   Scalar<"ddof", Int>, Out<TS<Float>>>
    {
    };

    /** Publish a typed trailing window as shaped value and timestamp arrays.
        The result bundle contains ``buffer`` and ``index`` fields. Each input
        window tick emits after the ``TSW`` reaches its minimum population. A
        partially warming result uses a dynamic leading dimension, while a
        full-period result retains its fixed array capacity. The node owns no
        history; use core ``to_window`` to define retention, reset, and warm-up
        policy.
        @param window Fixed tick-count ``TSW`` to materialize. Duration windows
                      are rejected by overload resolution because their array
                      shape has no fixed maximum.
        @return ``RollingWindowResult`` with shaped ``buffer`` and ``index`` arrays.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        recent = hga.rolling_window(price, period=20, min_window_period=5)
        @endcode */
    struct rolling_window
        : Operator<"hgraph.analytics.rolling_window",
                   In<"window", TsVar<"W">>, Out<TsVar<"__out__">>>
    {
    };

    /** Calculate standard deviation according to input shape and arity.
        A scalar stream produces the running population standard deviation.
        A numeric collection reduces its current valid members using the
        existing collection contract, while a ``TSW`` reduces the retained
        observations and accepts an optional wiring-time ``ddof``. Binary and
        fixed-list inputs are evaluated element by element. Invalid source
        cycles do not trigger running or windowed results.
        @param ts Numeric stream, collection, or typed window.
        @param ddof Delta degrees of freedom for a typed window; defaults to
                    zero and yields NaN when the retained count does not exceed
                    it.
        @param default_value Compatibility fallback accepted by scalar-container
                             reductions.
        @param lhs Left input for binary or element-wise standard deviation.
        @param rhs Right input for binary or element-wise standard deviation.
        @return Floating-point standard deviation selected by input shape.
        @par Python example
        @code{.py}
        import hgraph as hg
        import hgraph_analytics as hga
        sample_volatility = hga.std(hg.to_window(returns, 20, 20), ddof=1)
        @endcode */
    struct std_
        : Operator<"hgraph.analytics.std", In<"ts", TsVar<"S">>,
                   Scalar<"ddof", Int>, Out<TsVar<"O">>>
    {
    };

    /** Calculate variance according to input shape and arity.
        A scalar stream produces running population variance. Numeric
        collections use the migrated collection contract: current valid
        members are treated as a sample and fewer than two members produce
        zero. Binary and fixed-list inputs are evaluated element by element.
        Invalid source cycles do not update running state or trigger output.
        @param ts Numeric stream or collection.
        @param default_value Compatibility fallback accepted by scalar-container
                             reductions.
        @param lhs Left input for binary or element-wise variance.
        @param rhs Right input for binary or element-wise variance.
        @return Floating-point variance selected by input shape.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        running_variance = hga.var(observations)
        @endcode */
    struct var_
        : Operator<"hgraph.analytics.var", In<"ts", TsVar<"S">>,
                   Out<TsVar<"O">>>
    {
    };

    /** Compute the mean over a trailing observation-count or duration window.
        The migrated graph retains the original warm-up contract. Tick windows
        emit after ``min_window_period`` observations (the full period when the
        minimum is zero); duration windows use the covered tick count as their
        denominator. Invalid input cycles do not add observations.
        @param ts Numeric input stream.
        @param period Positive observation count or duration fixed while wiring.
        @param min_window_period Optional minimum observation count or duration;
                                 zero selects the full period.
        @return Floating-point trailing mean.
        @throws std::invalid_argument while wiring for invalid period/minimum
                                     combinations.
        @par Python example
        @code{.py}
        import hgraph_analytics as hga
        moving_average = hga.rolling_mean(price, 20)
        @endcode */
    struct rolling_mean
        : Operator<"hgraph.analytics.rolling_mean",
                   In<"ts", TS<ScalarVar<"T">>>, Scalar<"period", Int>,
                   Out<TS<Float>>>
    {
    };

    /** Retick the latest valid input on a regular engine-time schedule.
        The first valid input establishes the value sampled at subsequent
        schedule boundaries. Once valid, the node continues to emit even when
        no new source tick arrives. It retains only the latest input value and
        its scheduler state.
        @param ts Input stream whose latest value is sampled.
        @param period Positive fixed engine-time interval.
        @return The latest value repeated on the regular schedule.
        @throws std::invalid_argument during start when ``period`` is not
                                     positive.
        @par Python example
        @code{.py}
        from datetime import timedelta
        import hgraph_analytics as hga
        every_five_seconds = hga.resample(price, timedelta(seconds=5))
        @endcode */
    struct resample
        : Operator<"hgraph.analytics.resample", In<"ts", TsVar<"S">>,
                   Scalar<"period", TimeDelta>, Out<TsVar<"S">>>
    {
    };

    /** Register the hgraph-analytics overloads in the current hgraph registry.
        Call once per registry lifetime after core standard operators are
        available and before wiring analytics graphs. */
    HGRAPH_ANALYTICS_EXPORT void register_analytics_operators();
}  // namespace hgraph::analytics

#endif  // HGRAPH_ANALYTICS_OPERATORS_H
