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

    /** Register the hgraph-analytics overloads in the current hgraph registry.
        Call once per registry lifetime after core standard operators are
        available and before wiring analytics graphs. */
    HGRAPH_ANALYTICS_EXPORT void register_analytics_operators();
}  // namespace hgraph::analytics

#endif  // HGRAPH_ANALYTICS_OPERATORS_H
