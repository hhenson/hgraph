#include <hgraph/analytics/operators.h>

#include "operator_registration.h"

#include <hgraph/lib/std/operators/impl/stream_impl.h>

#include <stdexcept>

namespace hgraph::analytics::detail
{
    namespace
    {
        namespace stream = hgraph::stdlib::stream_impl_detail;

        /** Validate the analytics contract before delegating to the shared
            tick-window rolling-average graph. */
        struct rolling_mean_tick_compose : hgraph::stdlib::rolling_average_tick_compose
        {
            static constexpr auto name = "rolling_mean_tick";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *period  = context.scalar_as<Int>("period");
                const auto *minimum = context.scalar_as<Int>("min_window_period");
                if (period != nullptr && *period <= 0)
                {
                    throw std::invalid_argument("rolling_mean: period must be positive");
                }
                if (period != nullptr && minimum != nullptr && (*minimum < 0 || *minimum > *period))
                {
                    throw std::invalid_argument(
                        "rolling_mean: min_window_period must be between zero and period");
                }
                hgraph::stdlib::rolling_average_tick_compose::resolve_default_types(resolution, context);
            }
        };

        /** Validate the analytics contract before delegating to the shared
            duration-window rolling-average graph. */
        struct rolling_mean_time_compose : hgraph::stdlib::rolling_average_time_compose
        {
            static constexpr auto name = "rolling_mean_time";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *period  = context.scalar_as<TimeDelta>("period");
                const auto *minimum = context.scalar_as<TimeDelta>("min_window_period");
                if (period != nullptr && *period <= TimeDelta{0})
                {
                    throw std::invalid_argument("rolling_mean: period must be positive");
                }
                if (period != nullptr && minimum != nullptr &&
                    (*minimum < TimeDelta{0} || *minimum > *period))
                {
                    throw std::invalid_argument(
                        "rolling_mean: min_window_period must be between zero and period");
                }
                hgraph::stdlib::rolling_average_time_compose::resolve_default_types(resolution, context);
            }
        };
    }  // namespace

    // Windowed statistics: std_ over TSW, rolling_mean and resample; one
    // registration group per translation unit (see "Registration translation
    // units" in the operators developer guide).
    void register_statistics_window_overloads()
    {
        register_overload<std_, stream::tsw_std_impl<Int>>();
        register_overload<std_, stream::tsw_std_impl<Float>>();
        register_overload<std_, stream::tsw_std_ddof_impl<Int>>();
        register_overload<std_, stream::tsw_std_ddof_impl<Float>>();

        register_graph_overload<rolling_mean, rolling_mean_tick_compose>();
        register_graph_overload<rolling_mean, rolling_mean_time_compose>();
        register_overload<resample, hgraph::stdlib::resample_impl>();
    }
}  // namespace hgraph::analytics::detail
