#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::analytics
{
    namespace
    {
        template <typename T>
        struct PctChangeCompose
        {
            static constexpr auto name = std::is_same_v<T, Int>
                                             ? "hgraph.analytics.pct_change.int"
                                             : "hgraph.analytics.pct_change.float";

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {
                    {"period", Value{Int{1}}},
                    {"divide_by_zero", Value{stdlib::DivideByZero::Error}},
                };
            }

            static Port<TS<Float>> compose(
                Wiring &w,
                NamedPort<"ts", TS<T>> ts,
                Scalar<"period", Int> period,
                Scalar<"divide_by_zero", stdlib::DivideByZero> divide_by_zero)
            {
                if (period.value() <= 0)
                {
                    throw std::invalid_argument(
                        "hgraph.analytics.pct_change: period must be positive");
                }

                // The retained observation history and readiness contract stay
                // in core lag. Reusing the same prior port for subtraction and
                // division guarantees both operations observe one causal anchor.
                auto prior = wire<stdlib::lag>(w, ts, period.value());
                auto delta = wire<stdlib::sub_>(w, ts, prior);
                return wire<stdlib::div_, TS<Float>>(
                    w, delta, prior, divide_by_zero.value());
            }
        };
    }  // namespace

    void register_analytics_operators()
    {
        register_graph_overload<pct_change, PctChangeCompose<Int>>();
        register_graph_overload<pct_change, PctChangeCompose<Float>>();
    }
}  // namespace hgraph::analytics
