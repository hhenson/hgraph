#ifndef HGRAPH_ANALYTICS_OPERATORS_H
#define HGRAPH_ANALYTICS_OPERATORS_H

#include <hgraph/analytics/export.h>

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::analytics
{
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
