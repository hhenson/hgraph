#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/value/value.h>

#include "operator_registration.h"

#include <algorithm>
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
        struct diff_impl
        {
            static void eval(In<"ts", TS<T>> ts, RecordableState<TS<T>> prior,
                             Out<TS<T>> out)
            {
                // Adjacent difference is O(1) per tick and retains only the
                // preceding valid observation. State is advanced after output
                // so both operands belong to consecutive accepted observations.
                if (prior.valid()) { out.set(ts.value() - prior.value().template checked_as<T>()); }
                prior.set(ts.value());
            }
        };

        struct count_impl
        {
            static void eval(In<"ts", SIGNAL> ts, State<Int> running_count,
                             Out<TS<Int>> out)
            {
                static_cast<void>(ts);
                const Int next = running_count.get() + 1;
                running_count.set(next);
                out.set(next);
            }
        };

        struct count_reset_impl
        {
            static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts,
                             In<"reset", SIGNAL, InputValidity::Unchecked> reset,
                             State<Int> running_count, Out<TS<Int>> out)
            {
                // Reset wins on a shared cycle, then that cycle's source tick
                // becomes observation one. A reset by itself intentionally does
                // not tick the output.
                if (reset.modified()) { running_count.set(Int{0}); }
                if (!ts.modified()) { return; }
                const Int next = running_count.get() + 1;
                running_count.set(next);
                out.set(next);
            }
        };

        template <typename T>
        struct clip_impl
        {
            static void start(Scalar<"min", T> minimum, Scalar<"max", T> maximum)
            {
                if (minimum.value() > maximum.value())
                {
                    throw std::invalid_argument(
                        "hgraph.analytics.clip: min must be <= max");
                }
            }

            static void eval(In<"ts", TS<T>> ts, Scalar<"min", T> minimum,
                             Scalar<"max", T> maximum, Out<TS<T>> out)
            {
                out.set(std::clamp(ts.value(), minimum.value(), maximum.value()));
            }
        };

        struct ewma_impl
        {
            static void eval(In<"ts", TS<Float>> ts, Scalar<"alpha", Float> alpha,
                             RecordableState<TS<Float>> average, Out<TS<Float>> out)
            {
                // This is the migrated core recurrence, kept algebraically
                // unchanged so the package move cannot alter floating-point
                // rounding. It is O(1) per tick and initializes from the first
                // observation.
                const Float value = average.valid()
                                        ? alpha.value() * ts.value() +
                                              (Float{1.0} - alpha.value()) * average.value().checked_as<Float>()
                                        : ts.value();
                average.set(value);
                out.set(value);
            }
        };

        template <typename T>
        struct pct_change_compose
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
                Wiring &w, NamedPort<"ts", TS<T>> ts,
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

    void detail::register_numerical_operators()
    {
        register_overload<diff, diff_impl<Int>>();
        register_overload<diff, diff_impl<Float>>();
        register_overload<count, count_impl>();
        register_overload<count, count_reset_impl>();
        register_overload<clip, clip_impl<Int>>();
        register_overload<clip, clip_impl<Float>>();
        register_overload<ewma, ewma_impl>();
        register_graph_overload<pct_change, pct_change_compose<Int>>();
        register_graph_overload<pct_change, pct_change_compose<Float>>();
    }
}  // namespace hgraph::analytics
