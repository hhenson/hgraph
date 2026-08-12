#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H

#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/lib/std/operators/arithmetic.h>    // sub_ / div_ (rolling_average)
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/comparison.h>    // min_ / eq_ (rolling_average)
#include <hgraph/lib/std/operators/control.h>       // if_then_else (rolling_average)
#include <hgraph/lib/std/operators/conversion.h>    // const_ / default_ / cast_ (rolling_average)
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/runtime/service_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value_callable.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace stream_impl_detail
    {
        // ----- TSW: to_window + window aggregates --------------------------

        /** to_window(ts, period, min_window_period): push each tick into a
            size-based TSW (the TSW data layer handles eviction + validity). */
        struct to_window_impl
        {
            static constexpr auto name = "to_window";

            static auto defaults() { return std::tuple{arg<"min_window_period">(Int{0})}; }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return time_series_arg_matches<AnyTS>(context, 0);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
                const Int  *period = context.scalar_as<Int>("period");
                if (schema == nullptr || period == nullptr) { return; }
                const Int *min_period = context.scalar_as<Int>("min_window_period");
                if (*period <= 0)
                {
                    throw std::invalid_argument("to_window: period must be positive");
                }
                if (min_period != nullptr && (*min_period < 0 || *min_period > *period))
                {
                    throw std::invalid_argument(
                        "to_window: min_window_period must be between zero and period");
                }
                // hgraph: an unset minimum IS the period - the window's value
                // stays invalid until it holds a full period of ticks.
                const auto minimum = min_period != nullptr && *min_period > 0
                                         ? static_cast<std::size_t>(*min_period)
                                         : static_cast<std::size_t>(*period);
                bind_output(resolution,
                            TypeRegistry::instance().tsw(schema->value_schema,
                                                         static_cast<std::size_t>(*period), minimum));
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Scalar<"period", Int>,
                             Scalar<"min_window_period", Int>, Out<TsVar<"__out__">> out)
            {
                const auto &erased   = static_cast<const TSOutputView &>(out);
                auto        window   = erased.as_window();
                auto        mutation = window.begin_mutation(erased.evaluation_time());
                mutation.push(ts.base().value());
            }
        };

        /**
         * Resettable tick-count TSW. Reset is processed before a simultaneous
         * source tick, so the resulting window begins with the current value.
         */
        struct to_window_reset_impl : to_window_impl
        {
            static constexpr auto name = "to_window_reset";

            static void eval(In<"ts", TS<ScalarVar<"T">>, InputValidity::Unchecked> ts,
                             Scalar<"period", Int>,
                             Scalar<"min_window_period", Int>,
                             In<"reset", SIGNAL, InputValidity::Unchecked> reset,
                             Out<TsVar<"__out__">> out)
            {
                const auto &erased   = static_cast<const TSOutputView &>(out);
                auto        window   = erased.as_window();
                auto        mutation = window.begin_mutation(erased.evaluation_time());
                if (reset.modified()) { mutation.clear(); }
                if (ts.modified() && ts.valid()) { mutation.push(ts.base().value()); }
            }
        };

        /** Window readiness: size windows need min_period elements, duration
            windows need the newest-oldest span to reach min_time_range
            (mirrors the data layer's all_valid). */
        [[nodiscard]] inline bool tsw_ready(const TSWDataView &window)
        {
            if (window.size() == 0) { return false; }
            if (window.time_based())
            {
                const auto min_range = window.min_time_range();
                if (min_range <= TimeDelta{0}) { return true; }
                return window.time_at(window.size() - 1) - window.time_at(0) >= min_range;
            }
            return window.size() >= window.min_period();
        }

        /** to_window(ts, timedelta[, timedelta]): the DURATION window form -
            ticks within the trailing time range; ready once the span reaches
            the minimum (defaults to the full range, hgraph parity). */
        struct to_window_duration_impl
        {
            static constexpr auto name = "to_window_duration";

            static auto defaults() { return std::tuple{arg<"min_window_period">(TimeDelta{0})}; }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return time_series_arg_matches<AnyTS>(context, 0) &&
                       context.scalar_as<TimeDelta>("period") != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
                const auto *period = context.scalar_as<TimeDelta>("period");
                if (schema == nullptr || period == nullptr) { return; }
                const auto *min_period = context.scalar_as<TimeDelta>("min_window_period");
                if (*period <= TimeDelta{})
                {
                    throw std::invalid_argument("to_window: period must be positive");
                }
                if (min_period != nullptr &&
                    (*min_period < TimeDelta{} || *min_period > *period))
                {
                    throw std::invalid_argument(
                        "to_window: min_window_period must be between zero and period");
                }
                const TimeDelta min_range =
                    min_period != nullptr && *min_period > TimeDelta{0} ? *min_period : *period;
                bind_output(
                    resolution, TypeRegistry::instance().tsw_duration(schema->value_schema, *period, min_range));
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Scalar<"period", TimeDelta>,
                             Scalar<"min_window_period", TimeDelta>, Out<TsVar<"__out__">> out)
            {
                const auto &erased   = static_cast<const TSOutputView &>(out);
                auto        window   = erased.as_window();
                auto        mutation = window.begin_mutation(erased.evaluation_time());
                mutation.push(ts.base().value());
            }
        };

        /** Resettable duration TSW with the same reset-before-source ordering. */
        struct to_window_duration_reset_impl : to_window_duration_impl
        {
            static constexpr auto name = "to_window_duration_reset";

            static void eval(In<"ts", TS<ScalarVar<"T">>, InputValidity::Unchecked> ts,
                             Scalar<"period", TimeDelta>,
                             Scalar<"min_window_period", TimeDelta>,
                             In<"reset", SIGNAL, InputValidity::Unchecked> reset,
                             Out<TsVar<"__out__">> out)
            {
                const auto &erased   = static_cast<const TSOutputView &>(out);
                auto        window   = erased.as_window();
                auto        mutation = window.begin_mutation(erased.evaluation_time());
                if (reset.modified()) { mutation.clear(); }
                if (ts.modified() && ts.valid()) { mutation.push(ts.base().value()); }
            }
        };

        /** abs_ over a TSW: elementwise on the arriving tick - the output is
            a window of the same shape whose newest element is |newest|. The
            element type is a template parameter (typed kernel, selected at
            node-selection time). */
        template <typename T>
        struct abs_tsw_impl
        {
            static constexpr auto name = std::same_as<T, Int> ? "abs_tsw_int" : "abs_tsw_float";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTSW>(context, 0);
                return schema != nullptr &&
                       schema->value_schema->element_type == scalar_descriptor<T>::value_meta();
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *schema = time_series_schema_at_as<AnyTSW>(context, 0);
                if (schema == nullptr) { return; }
                bind_output(resolution, schema);
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                const TSWInputView window_input{ts.base().borrowed_ref()};
                auto window = window_input.data_view();
                if (window.size() == 0) { return; }

                Value result{std::abs(window.at(window.size() - 1).checked_as<T>())};
                auto out_window = erased.as_window();
                auto mutation   = out_window.begin_mutation(erased.evaluation_time());
                mutation.push(result.view());
            }
        };

        /** min_/max_ over a TSW: full-window recompute, fully ERASED via
            Value::compare. Recompute EMITS every window tick (no dedup: a
            sliding window re-ticks the same extremum as it slides). */
        template <bool Min>
        struct tsw_extremum_impl
        {
            static constexpr auto name = Min ? "min_tsw" : "max_tsw";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return time_series_arg_matches<AnyTSW>(context, 0);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *schema = time_series_schema_at_as<AnyTSW>(context, 0);
                if (schema == nullptr) { return; }
                bind_output(
                    resolution, TypeRegistry::instance().ts(schema->value_schema->element_type));
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                if (!ts.valid()) { return; }
                const TSWInputView window_input{ts.base().borrowed_ref()};
                // A LIVE local data view: ranges/at() borrow its storage.
                auto window = window_input.data_view();
                if (!tsw_ready(window)) { return; }

                std::optional<Value> best;
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    const ValueView value  = window.at(index);
                    const bool      better = !best.has_value() ||
                                        (Min ? value.compare(best->view()) == std::partial_ordering::less
                                             : value.compare(best->view()) == std::partial_ordering::greater);
                    if (better) { best.emplace(value); }
                }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(*best)));
            }
        };

        /** sum_/mean over a NUMERIC TSW. The element type is a TEMPLATE
            parameter selected at node-selection time (requires_ gates on the
            element meta) - the per-tick path never branches on type. */
        template <bool Mean, typename T>
        struct tsw_numeric_aggregate_impl
        {
            static constexpr auto name = Mean ? (std::same_as<T, Int> ? "mean_tsw_int" : "mean_tsw_float")
                                              : (std::same_as<T, Int> ? "sum_tsw_int" : "sum_tsw_float");

            [[nodiscard]] static bool matches(OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTSW>(context, 0);
                return schema != nullptr &&
                       schema->value_schema->element_type == scalar_descriptor<T>::value_meta();
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context) { return matches(context); }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                if (!matches(context)) { return; }
                auto &registry = TypeRegistry::instance();
                bind_output(
                    resolution,
                    registry.ts(Mean ? scalar_descriptor<Float>::value_meta() : scalar_descriptor<T>::value_meta()));
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                if (!ts.valid()) { return; }
                const TSWInputView window_input{ts.base().borrowed_ref()};
                auto window = window_input.data_view();
                if (!tsw_ready(window)) { return; }

                T total{};
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    total += window.at(index).checked_as<T>();
                }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                if constexpr (Mean)
                {
                    static_cast<void>(mutation.move_value_from(
                        Value{static_cast<Float>(total) / static_cast<Float>(window.size())}));
                }
                else { static_cast<void>(mutation.move_value_from(Value{total})); }
            }
        };

        /** std over a NUMERIC TSW. The no-ddof overload uses the population
            divisor; the explicit overload uses N - ddof. */
        template <typename T>
        struct tsw_std_impl
        {
            static constexpr auto name = std::same_as<T, Int> ? "std_tsw_int" : "std_tsw_float";

            [[nodiscard]] static bool matches(OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTSW>(context, 0);
                return schema != nullptr &&
                       schema->value_schema->element_type == scalar_descriptor<T>::value_meta();
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context) { return matches(context); }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                if (!matches(context)) { return; }
                bind_output(resolution,
                            TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()));
            }

            static void eval_with_ddof(
                const In<"ts", TsVar<"S">> &ts, Int ddof,
                const Out<TsVar<"__out__">> &out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                if (!ts.valid()) { return; }
                const TSWInputView window_input{ts.base().borrowed_ref()};
                auto window = window_input.data_view();
                if (!tsw_ready(window)) { return; }

                const auto size = window.size();
                Float      total{};
                for (std::size_t index = 0; index < size; ++index)
                {
                    total += static_cast<Float>(window.at(index).checked_as<T>());
                }
                const Float mean_value = total / static_cast<Float>(size);
                Float       squares{};
                for (std::size_t index = 0; index < size; ++index)
                {
                    const Float delta = static_cast<Float>(window.at(index).checked_as<T>()) - mean_value;
                    squares += delta * delta;
                }
                const Float divisor =
                    static_cast<Float>(size) - static_cast<Float>(ddof);
                const Float variance =
                    divisor > 0.0
                        ? squares / divisor
                        : std::numeric_limits<Float>::quiet_NaN();
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(
                    Value{std::sqrt(variance)}));
            }

            static void eval(In<"ts", TsVar<"S">> ts,
                             Out<TsVar<"__out__">> out)
            {
                eval_with_ddof(ts, 0, out);
            }
        };

        template <typename T>
        struct tsw_std_ddof_impl : tsw_std_impl<T>
        {
            static constexpr auto name =
                std::same_as<T, Int> ? "std_tsw_ddof_int" : "std_tsw_ddof_float";

            static void eval(In<"ts", TsVar<"S">> ts,
                             Scalar<"ddof", Int> ddof,
                             Out<TsVar<"__out__">> out)
            {
                tsw_std_impl<T>::eval_with_ddof(ts, ddof.value(), out);
            }
        };

        /** min_/max_ over a TSW with a default while the window is below its
            minimum period (hgraph's default_value kwarg; a separate variant -
            the extremum_tss_default pattern). */
        template <bool Min>
        struct tsw_extremum_default_impl
        {
            static constexpr auto name = Min ? "min_tsw_default" : "max_tsw_default";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return time_series_arg_matches<AnyTSW>(context, 0);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *schema = time_series_schema_at_as<AnyTSW>(context, 0);
                if (schema == nullptr) { return; }
                bind_output(
                    resolution, TypeRegistry::instance().ts(schema->value_schema->element_type));
            }

            static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                             Scalar<"default_value", ScalarVar<"D">> default_value,
                             Out<TsVar<"__out__">> out)
            {
                if (!ts.modified()) { return; }
                const auto &erased = static_cast<const TSOutputView &>(out);
                const auto publish = [&](const ValueView &value) {
                    // Recompute EMITS every tick (no dedup - the aggregate rule).
                    auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.copy_value_from(value));
                };

                if (!ts.valid())
                {
                    // A DURATION window still filling its span default-fills;
                    // a tick window below its minimum stays silent (hgraph:
                    // that input is simply invalid).
                    const auto *schema = ts.base().schema();
                    if (schema->kind == TSTypeKind::TSW && schema->data.tsw.is_duration_based)
                    {
                        publish(default_value.value());
                    }
                    return;
                }
                const TSWInputView window_input{ts.base().borrowed_ref()};
                auto window = window_input.data_view();
                if (!tsw_ready(window))
                {
                    // The default fills DURATION windows still reaching their
                    // minimum span; a size window below minimum is an INVALID
                    // input (never evaluated), so no default fires there.
                    if (window.time_based()) { publish(default_value.value()); }
                    return;
                }
                std::optional<Value> best;
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    const ValueView value = window.at(index);
                    const bool better = !best.has_value() ||
                                        (Min ? value.compare(best->view()) == std::partial_ordering::less
                                             : value.compare(best->view()) == std::partial_ordering::greater);
                    if (better) { best.emplace(value); }
                }
                publish(best->view());
            }
        };

        struct DeltaQueueState
        {
            std::deque<Value> buffer{};
        };

        struct TimedDeltaQueueState
        {
            std::deque<std::pair<DateTime, Value>> buffer{};
        };

        struct LagProxyState
        {
            // Deltas captured per proxy count, replayed (in arrival order,
            // so container deltas net) when the LAGGED count reaches it.
            ankerl::unordered_dense::map<Int, std::deque<Value>> cache{};
        };

        struct ThrottleState
        {
            TimeDelta period{MIN_TD};
            bool      has_period{false};
            // Deltas accumulated while a throttle window is open; applied in
            // arrival order on release so container deltas MERGE (dict ticks
            // within one window emit as one combined delta).
            std::deque<Value> pending{};
        };

        inline void require_positive(Int value, const char *name)
        {
            if (value <= 0) { throw std::invalid_argument(std::string{name} + " must be positive"); }
        }

        inline void require_positive(TimeDelta value, const char *name)
        {
            if (value <= TimeDelta{}) { throw std::invalid_argument(std::string{name} + " must be positive"); }
        }

        inline void append_delta(DeltaQueueState &state, Value delta, std::size_t limit)
        {
            if (state.buffer.size() >= limit) { throw std::overflow_error("gate buffer length exceeded"); }
            state.buffer.push_back(std::move(delta));
        }

        /** Net a queue of canonical set deltas ({added, removed} bundles) into
            one; an empty optional means the queue cancels out (add then
            remove of the same element) and nothing should be emitted. */
        inline std::optional<Value> net_set_deltas(std::deque<Value> &pending)
        {
            std::vector<Value> added;
            std::vector<Value> removed;
            const auto erase_matching = [](std::vector<Value> &values, const ValueView &value) {
                for (auto it = values.begin(); it != values.end(); ++it)
                {
                    if (it->view().equals(value)) { values.erase(it); return true; }
                }
                return false;
            };
            const auto push_unique = [&](std::vector<Value> &values, const ValueView &value) {
                for (const Value &existing : values)
                {
                    if (existing.view().equals(value)) { return; }
                }
                values.emplace_back(value);
            };

            const ValueTypeMetaData *bundle_meta  = nullptr;
            const ValueTypeMetaData *element_meta = nullptr;
            for (Value &delta : pending)
            {
                const auto bundle = delta.view().as_bundle();
                if (bundle_meta == nullptr)
                {
                    bundle_meta  = delta.view().schema();
                    element_meta = bundle.field("added").schema()->element_type;
                }
                // Canonical order inside one delta is remove-before-add.
                const auto gone = bundle.field("removed").as_indexed_view();
                for (std::size_t i = 0; i < gone.size(); ++i)
                {
                    const auto value = gone.at(i);
                    if (!erase_matching(added, value)) { push_unique(removed, value); }
                }
                const auto fresh = bundle.field("added").as_indexed_view();
                for (std::size_t i = 0; i < fresh.size(); ++i)
                {
                    const auto value = fresh.at(i);
                    static_cast<void>(erase_matching(removed, value));
                    push_unique(added, value);
                }
            }
            if (added.empty() && removed.empty()) { return std::nullopt; }

            auto &factory = ValuePlanFactory::instance();
            SetBuilder added_builder{factory.type_for(element_meta)};
            for (const Value &value : added) { static_cast<void>(added_builder.insert_copy(value.view().data())); }
            SetBuilder removed_builder{factory.type_for(element_meta)};
            for (const Value &value : removed) { static_cast<void>(removed_builder.insert_copy(value.view().data())); }
            BundleBuilder bundle{factory.type_for(bundle_meta)};
            bundle.set("added", added_builder.build());
            bundle.set("removed", removed_builder.build());
            return bundle.build();
        }
    }  // namespace stream_impl_detail
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::stream_impl_detail::DeltaQueueState>
    {
        static constexpr std::string_view value{"stdlib.delta_queue_state"};
    };

    template <>
    struct scalar_name<stdlib::stream_impl_detail::TimedDeltaQueueState>
    {
        static constexpr std::string_view value{"stdlib.timed_delta_queue_state"};
    };

    template <>
    struct scalar_name<stdlib::stream_impl_detail::ThrottleState>
    {
        static constexpr std::string_view value{"stdlib.throttle_state"};
    };

    template <>
    struct scalar_name<stdlib::stream_impl_detail::LagProxyState>
    {
        static constexpr std::string_view value{"stdlib.lag_proxy_state"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace stream_impl_detail
    {
        /** Internal running tick count used by core stream compositions.
            The public analytical ``count`` contract lives in hgraph-analytics;
            this marker is deliberately not part of the core operator surface. */
        struct tick_count : Operator<"__tick_count", In<"ts", SIGNAL>, Out<TS<Int>>>
        {
        };

        struct tick_count_impl
        {
            static void eval(In<"ts", SIGNAL> ts, State<Int> count, Out<TS<Int>> out)
            {
                static_cast<void>(ts);
                const Int next = count.get() + 1;
                count.set(next);
                out.set(next);
            }
        };
    }  // namespace stream_impl_detail

    struct sample_impl
    {
        static void eval(In<"signal", SIGNAL> signal,
                         In<"ts", TsVar<"S">, InputActivity::Passive, InputValidity::Unchecked> ts,
                         Out<TsVar<"S">> out)
        {
            static_cast<void>(signal);
            if (ts.valid()) { out.apply(ts.value()); }
        }
    };

    struct filter_impl
    {
        static void eval(In<"condition", TS<Bool>, InputValidity::Unchecked> condition,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         RecordableState<TS<Bool>> last_condition,
                         Out<TsVar<"S">> out)
        {
            const bool condition_true = condition.valid() && condition.value();
            const bool was_true = last_condition.valid() && last_condition.value().checked_as<Bool>();
            if (condition_true && ts.valid())
            {
                if (!was_true)
                {
                    // Re-opened: re-sync the FULL value (per-key diff apply);
                    // skip when nothing ticked while blocked.
                    const auto &erased = static_cast<const TSOutputView &>(out);
                    if (erased.data_view().last_modified_time() < ts.base().last_modified_time())
                    {
                        if (erased.schema()->kind == TSTypeKind::TSD)
                        {
                            // Dict resync honours per-child TS-VALIDITY (a
                            // created-but-never-ticked element - e.g. an
                            // empty-REF map child - is NOT part of the value;
                            // the raw value memory cannot tell).
                            auto dict_out = erased.data_view().as_dict();
                            auto mutation = dict_out.begin_mutation(erased.evaluation_time());
                            auto dict_in  = ts.base().as_dict();
                            std::vector<Value> live;
                            for (auto &&[key, child] : dict_in.valid_items())
                            {
                                live.emplace_back(key);
                                // READ-side compare first (the flip lesson):
                                // an unchanged entry must not re-record.
                                if (dict_out.contains(key))
                                {
                                    auto current = dict_out.at(key);
                                    if (current.has_current_value() && current.value().equals(child.value()))
                                    {
                                        continue;
                                    }
                                }
                                auto element = mutation.at(key);
                                apply_current_value(
                                    TSOutputView{erased.output(), element, erased.evaluation_time()},
                                    child.value());
                            }
                            std::vector<Value> removals;
                            for (const auto key : mutation.keys())
                            {
                                bool present = false;
                                for (const Value &kept : live)
                                {
                                    if (kept.view().equals(key)) { present = true; break; }
                                }
                                if (!present) { removals.emplace_back(key); }
                            }
                            for (const Value &key : removals) { static_cast<void>(mutation.erase(key.view())); }
                        }
                        else { out.apply(ts.value()); }
                    }
                }
                else if (ts.modified())
                {
                    // Open and flowing: forward the DELTA (a whole-value
                    // apply would re-publish unchanged container entries).
                    apply_delta(out, capture_delta(ts.base()).view());
                }
            }
            else if (condition_true && was_true && ts.modified() && ts.base().schema() != nullptr &&
                     (ts.base().schema()->kind == TSTypeKind::TSS ||
                      ts.base().schema()->kind == TSTypeKind::TSD))
            {
                // Structural REF unbinds are invalid current values with a
                // removal delta over the previously published key set.
                bool has_removal = false;
                if (ts.base().schema()->kind == TSTypeKind::TSS)
                {
                    const auto input = ts.base().as_set();
                    const auto removed = input.removed();
                    has_removal = removed.begin() != removed.end();
                }
                else
                {
                    const auto input = ts.base().as_dict();
                    const auto removed = input.removed_keys();
                    has_removal = removed.begin() != removed.end();
                }
                if (has_removal) { apply_delta(out, capture_delta(ts.base()).view()); }
            }
            last_condition.set(condition_true);
        }
    };

    struct lag_tick_impl
    {
        static void start(Scalar<"period", Int> period)
        {
            stream_impl_detail::require_positive(period.value(), "period");
        }

        static void eval(In<"ts", TsVar<"S">> ts,
                         Scalar<"period", Int> period,
                         State<stream_impl_detail::DeltaQueueState> state,
                         Out<TsVar<"S">> out)
        {
            auto current = state.get();
            current.buffer.push_back(capture_delta(ts.base()));

            const auto delay = static_cast<std::size_t>(period.value());
            if (current.buffer.size() > delay)
            {
                Value delta = std::move(current.buffer.front());
                current.buffer.pop_front();
                apply_delta(out, delta.view());
            }

            state.set(std::move(current));
        }
    };

    /** lag(ts, delay: TS[timedelta]) — the TIME lag with a live delay port
        (each tick defers by the CURRENT delay value). */
    struct lag_time_ts_impl
    {
        static constexpr auto name = "lag_time_ts";

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"period", TS<TimeDelta>> period,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::TimedDeltaQueueState> state,
                         Out<TsVar<"S">> out)
        {
            const auto now     = static_cast<const TSOutputView &>(out).evaluation_time();
            auto       current = state.get();

            if (ts.modified() && ts.valid())
            {
                stream_impl_detail::require_positive(period.value(), "period");
                current.buffer.emplace_back(now + period.value(), capture_delta(ts.base()));
                scheduler.schedule(period.value());
            }

            while (!current.buffer.empty() && current.buffer.front().first <= now)
            {
                Value delta = std::move(current.buffer.front().second);
                current.buffer.pop_front();
                apply_delta(out, delta.view());
            }

            state.set(std::move(current));
        }
    };

    /** ``__lag_proxy``: one erased node for hgraph's whole _lag_proxy family -
        each ``ts`` tick's delta is cached under the live proxy count and
        replayed when the LAGGED count reaches it (deltas apply in arrival
        order, so whole-value kinds keep the last and container deltas net).
        ``lag_c`` starts passive and is activated/passivated at runtime as the
        cache fills and drains (upstream's make_active/make_passive dance). */
    struct lag_proxy_node_impl
    {
        static void eval(In<"ts", TsVar<"S">, InputActivity::Active, InputValidity::Unchecked> ts,
                         In<"c", TS<Int>, InputActivity::Passive> c,
                         In<"lag_c", TS<Int>, InputActivity::Passive, InputValidity::Unchecked> lag_c,
                         State<stream_impl_detail::LagProxyState> state,
                         Out<TsVar<"S">> out)
        {
            auto current = state.get();
            if (ts.modified() && ts.valid())
            {
                lag_c.make_active();
                current.cache[c.value()].push_back(capture_delta(ts.base()));
            }
            if (lag_c.modified() && lag_c.valid())
            {
                if (auto entry = current.cache.find(lag_c.value()); entry != current.cache.end())
                {
                    for (Value &delta : entry->second) { apply_delta(out, delta.view()); }
                    current.cache.erase(entry);
                }
                if (current.cache.empty()) { lag_c.make_passive(); }
            }
            state.set(std::move(current));
        }
    };

    namespace stream_impl_detail
    {
        /** Shared requires_ gate for the ``lag(ts, period, proxy)`` overloads:
            three arguments, a tick-count period, a time-series proxy, and the
            first argument of the given kind class. */
        [[nodiscard]] inline bool lag_proxy_args_match(OperatorCallContext context)
        {
            return context.args.size() == 3 && context.args[0].kind == WiringArg::Kind::TimeSeries &&
                   context.args[0].port.schema != nullptr && context.scalar_as<Int>("period") != nullptr &&
                   context.args[2].kind == WiringArg::Kind::TimeSeries;
        }

        /** ``lag`` preserves the input shape. */
        inline void bind_lag_proxy_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            if (context.args.empty() || context.args[0].kind != WiringArg::Kind::TimeSeries) { return; }
            bind_output(resolution, context.args[0].port.schema);
        }
    }  // namespace stream_impl_detail

    /** ``lag(ts, period, proxy)`` over a leaf kind: count the proxy, tick-lag
        the count, replay through ``__lag_proxy``. */
    struct lag_proxy_compose
    {
        static constexpr auto name = "lag_proxy";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (!stream_impl_detail::lag_proxy_args_match(context)) { return false; }
            const auto kind = context.args[0].port.schema->kind;
            return kind != TSTypeKind::TSD && kind != TSTypeKind::TSB && kind != TSTypeKind::TSL;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            stream_impl_detail::bind_lag_proxy_output(resolution, context);
        }

        static auto compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts, Scalar<"period", Int> period,
                            NamedPort<"proxy", SIGNAL> proxy)
        {
            auto ticks  = wire<stream_impl_detail::tick_count>(w, proxy);
            auto lagged = wire<lag>(w, ticks, period.value());
            return wire<lag_proxy_node>(w, ts, ticks, lagged);
        }
    };

    /** ``lag(ts, period, proxy)`` over a TSD: keys follow
        ``union(lag(keys, period, proxy), keys)`` so entries stay alive until
        their lagged values flush; per-key values lag via ``map_`` (upstream's
        lag_proxy_tsd). */
    struct lag_proxy_tsd_compose
    {
        static constexpr auto name = "lag_proxy_tsd";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return stream_impl_detail::lag_proxy_args_match(context) &&
                   context.args[0].port.schema->kind == TSTypeKind::TSD;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            stream_impl_detail::bind_lag_proxy_output(resolution, context);
        }

        static auto compose(Wiring &w, NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts,
                            Scalar<"period", Int> period, NamedPort<"proxy", SIGNAL> proxy)
        {
            // ONE shared count/lagged-count pair broadcast into every keyed
            // child (upstream wires the chain per key and dedups to this).
            auto ticks       = wire<stream_impl_detail::tick_count>(w, proxy);
            auto lagged      = wire<lag>(w, ticks, period.value());
            auto keys        = wire<keys_>(w, ts);
            auto lagged_keys = wire<union_>(w, wire<lag>(w, keys, period.value(), proxy), keys);
            return wire<map_>(w, fn<lag_proxy_node>(), ts, ticks, lagged, arg<"__keys__">(lagged_keys));
        }
    };

    /** ``lag(ts, period, proxy)`` over a fixed TSL: recursively dispatch lag
        for every element so nested structural kinds retain their specialised
        lifecycle semantics. */
    struct lag_proxy_tsl_compose
    {
        static constexpr auto name = "lag_proxy_tsl";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return stream_impl_detail::lag_proxy_args_match(context) &&
                   fixed_tsl_arg(context, 0) != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            stream_impl_detail::bind_lag_proxy_output(resolution, context);
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"ts", TSL<TsVar<"V">>> ts,
                                     Scalar<"period", Int> period,
                                     NamedPort<"proxy", SIGNAL> proxy)
        {
            WiringPortRef source = ts.erased();
            const auto   *schema = source.schema;
            std::vector<WiringPortRef> children;
            children.reserve(schema->fixed_size());
            for (std::size_t index = 0; index < schema->fixed_size(); ++index)
            {
                WiringPortRef element = subgraph_wiring_detail::tsl_element_ref(
                    source, index, schema->element_ts());
                Port<void> lagged_element = wire<lag>(
                    w, Port<void>{w, std::move(element)}, period.value(), proxy);
                children.push_back(lagged_element.erased());
            }
            return WiringPortRef::structural_source(schema, std::move(children));
        }
    };

    /** ``lag(ts, period, proxy)`` over a TSB: recursively dispatch lag for
        every field and assemble the SAME named bundle shape (upstream's
        lag_proxy_tsb). */
    struct lag_proxy_tsb_compose
    {
        static constexpr auto name = "lag_proxy_tsb";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return stream_impl_detail::lag_proxy_args_match(context) &&
                   context.args[0].port.schema->kind == TSTypeKind::TSB;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            stream_impl_detail::bind_lag_proxy_output(resolution, context);
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts, Scalar<"period", Int> period,
                                     NamedPort<"proxy", SIGNAL> proxy)
        {
            WiringPortRef source = ts.erased();
            const auto *schema = source.schema;
            std::vector<WiringPortRef> children;
            children.reserve(schema->field_count());
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                WiringPortRef field =
                    subgraph_wiring_detail::tsb_field_ref(source, index, schema->fields()[index].type);
                Port<void> lagged_field =
                    wire<lag>(w, Port<void>{w, std::move(field)}, period.value(), proxy);
                children.push_back(lagged_field.erased());
            }
            return WiringPortRef::structural_source(schema, std::move(children));
        }
    };

    /** lag(ts, timedelta): the TIME lag - each tick's delta re-emits
        ``period`` later (scheduler-driven deferred-delta buffer; the tick
        variant above is the count form). */
    struct lag_time_impl
    {
        static constexpr auto name = "lag_time";

        static void start(Scalar<"period", TimeDelta> period)
        {
            stream_impl_detail::require_positive(period.value(), "period");
        }

        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Scalar<"period", TimeDelta> period,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::TimedDeltaQueueState> state,
                         Out<TsVar<"S">> out)
        {
            const auto now     = static_cast<const TSOutputView &>(out).evaluation_time();
            auto       current = state.get();

            if (ts.modified() && ts.valid())
            {
                current.buffer.emplace_back(now + period.value(), capture_delta(ts.base()));
                scheduler.schedule(period.value());
            }

            while (!current.buffer.empty() && current.buffer.front().first <= now)
            {
                Value delta = std::move(current.buffer.front().second);
                current.buffer.pop_front();
                apply_delta(out, delta.view());
            }

            state.set(std::move(current));
        }
    };

    struct until_true_bool_impl
    {
        static void eval(In<"ts", TS<Bool>> ts, Out<TS<Bool>> out)
        {
            out.set(ts.value());
            if (ts.value()) { ts.make_passive(); }
        }
    };

    /** ``until_true(predicate, ts)`` with a runtime VALUE callable. Unlike a
        WiredFn, the predicate evaluates inside the node and passivating the
        input also stops subsequent callable invocations. */
    struct until_true_value_callable_impl
    {
        static constexpr auto name = "until_true_value_callable";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 &&
                   context.scalar_as<ValueCallable>("predicate") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void eval(Scalar<"predicate", ValueCallable> predicate,
                         In<"ts", TsVar<"S">> ts, Out<TS<Bool>> out)
        {
            const ValueCallArg argument{ts.base().value()};
            Value result = predicate.value().invoke(
                std::span<const ValueCallArg>{&argument, 1},
                scalar_descriptor<Bool>::value_meta());
            const Bool stop = result.view().checked_as<Bool>();
            out.set(stop);
            if (stop) { ts.make_passive(); }
        }
    };

    /** ``until_true(predicate, ts)`` with a WIRED-FUNCTION predicate — the
        C++ form of hgraph's callable predicate (``fn<Op>()`` or a lifted
        kernel): the predicate inlines over ``ts`` and the boolean primitive
        passivates the flag once true. */
    struct until_true_fn_compose
    {
        static constexpr auto name = "until_true_fn";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && context.scalar_as<WiredFn>("predicate") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            static_cast<void>(context);
            if (output_bound(resolution)) { return; }
            bind_output(resolution, TypeRegistry::instance().ts(scalar_descriptor<Bool>::value_meta()));
        }

        static auto compose(Wiring &w, Scalar<"predicate", WiredFn> predicate, NamedPort<"ts", TsVar<"S">> ts)
        {
            const WiringPortRef args[]{ts.erased()};
            WiringPortRef       flag = predicate.value().wire(w, {args, 1});
            return wire<until_true>(w, Port<void>{w, std::move(flag)});
        }
    };

    /** ``freeze(predicate, ts)`` with a wired-function predicate — hgraph's
        freeze_predicate: freeze once ``until_true(predicate, ts)`` fires. */
    struct freeze_fn_compose
    {
        static constexpr auto name = "freeze_fn";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && context.scalar_as<WiredFn>("predicate") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            if (context.args.size() < 2 || context.args[1].kind != WiringArg::Kind::TimeSeries) { return; }
            bind_output(resolution, context.args[1].port.schema);
        }

        static auto compose(Wiring &w, Scalar<"predicate", WiredFn> predicate, NamedPort<"ts", TsVar<"S">> ts)
        {
            auto flag = wire<until_true>(w, predicate.value(), ts);
            return wire<freeze>(w, flag, ts);
        }
    };

    /** ``freeze(predicate, ts)`` with a runtime value callable. */
    struct freeze_value_callable_compose
    {
        static constexpr auto name = "freeze_value_callable";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 &&
                   context.scalar_as<ValueCallable>("predicate") != nullptr &&
                   context.args[1].kind == WiringArg::Kind::TimeSeries;
        }

        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            if (context.args.size() < 2 ||
                context.args[1].kind != WiringArg::Kind::TimeSeries)
            {
                return;
            }
            bind_output(resolution, context.args[1].port.schema);
        }

        static auto compose(Wiring &w, Scalar<"predicate", ValueCallable> predicate,
                            NamedPort<"ts", TsVar<"S">> ts)
        {
            auto flag = wire<until_true>(w, predicate.value(), ts);
            return wire<freeze>(w, flag, ts);
        }
    };

    struct freeze_impl
    {
        static void eval(In<"predicate", TS<Bool>> predicate,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Out<TsVar<"S">> out)
        {
            if (predicate.value())
            {
                if (ts.valid()) { out.apply(ts.value()); }
                predicate.make_passive();
                ts.make_passive();
                return;
            }

            if (ts.modified() && ts.valid()) { apply_delta(out, capture_delta(ts.base()).view()); }
        }
    };

    struct schedule_impl
    {
        static void start(Scalar<"delay", TimeDelta> delay, Scalar<"initial_delay", Bool> initial_delay,
                          Scalar<"max_ticks", Int> max_ticks, NodeScheduler scheduler)
        {
            stream_impl_detail::require_positive(delay.value(), "delay");
            if (max_ticks.value() <= 0) { return; }
            scheduler.schedule(initial_delay.value() ? delay.value() : TimeDelta{});
        }

        static void eval(Scalar<"delay", TimeDelta> delay, Scalar<"initial_delay", Bool>,
                         Scalar<"max_ticks", Int> max_ticks, NodeScheduler scheduler,
                         State<Int> ticks, Out<TS<Bool>> out)
        {
            out.set(true);
            const Int emitted = ticks.get() + 1;
            ticks.set(emitted);
            if (emitted < max_ticks.value()) { scheduler.schedule(delay.value()); }
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"initial_delay", Value{true}}, {"max_ticks", Value{std::numeric_limits<Int>::max()}}};
        }
    };

    struct request_id_impl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"hash", Int>, Out<TS<Int>> out)
        {
            out.set(next_request_id());
        }
    };

    namespace stream_impl_detail
    {
        /* The shared body of the TS-delay ``schedule`` overloads (hgraph's
           schedule_ts): reschedules on every evaluation; emits on the
           schedule firing (or immediately when the delay ticks and
           ``initial_delay`` is off); a ``start`` tick re-bases the grid and
           resets the tick budget. */
        template <typename StartIn>
        inline void schedule_ts_eval(In<"delay", TS<TimeDelta>> &delay, StartIn *start, bool initial_delay,
                                     Int max_ticks, const NodeScheduler &scheduler, State<Int> &ticks,
                                     DateTime now, Out<TS<Bool>> &out)
        {
            const bool start_modified = start != nullptr && start->modified();
            if (ticks.get() >= max_ticks && !start_modified) { return; }  // budget spent: stop rescheduling
            const bool scheduled = scheduler.is_scheduled_now();

            if (start != nullptr && start->valid())
            {
                const DateTime start_at = start->value();
                if (now < start_at && !initial_delay) { scheduler.schedule(start_at); }
                else
                {
                    const auto elapsed = std::max(now, start_at) - start_at;
                    scheduler.schedule(start_at + delay.value() * (1 + elapsed / delay.value()));
                }
                if (start->modified()) { ticks.set(Int{0}); }
            }
            else { scheduler.schedule(now + delay.value()); }

            if ((delay.modified() && !initial_delay) || (scheduled && !delay.modified()))
            {
                if (ticks.get() < max_ticks)
                {
                    ticks.set(ticks.get() + 1);
                    out.set(true);
                }
            }
        }
    }  // namespace stream_impl_detail

    struct schedule_ts_impl
    {
        static void eval(In<"delay", TS<TimeDelta>> delay, Scalar<"initial_delay", Bool> initial_delay,
                         Scalar<"max_ticks", Int> max_ticks, NodeScheduler scheduler, State<Int> ticks,
                         DateTime now, Out<TS<Bool>> out)
        {
            stream_impl_detail::schedule_ts_eval<In<"start", TS<DateTime>>>(
                delay, nullptr, initial_delay.value(), max_ticks.value(), scheduler, ticks, now, out);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"initial_delay", Value{true}}, {"max_ticks", Value{std::numeric_limits<Int>::max()}}};
        }
    };

    struct schedule_ts_start_impl
    {
        static void eval(In<"delay", TS<TimeDelta>> delay, In<"start", TS<DateTime>, InputValidity::Unchecked> start,
                         Scalar<"initial_delay", Bool> initial_delay, Scalar<"max_ticks", Int> max_ticks,
                         NodeScheduler scheduler, State<Int> ticks, DateTime now, Out<TS<Bool>> out)
        {
            stream_impl_detail::schedule_ts_eval(delay, &start, initial_delay.value(), max_ticks.value(),
                                                 scheduler, ticks, now, out);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"initial_delay", Value{true}}, {"max_ticks", Value{std::numeric_limits<Int>::max()}}};
        }
    };

    struct resample_impl
    {
        static void start(Scalar<"period", TimeDelta> period, NodeScheduler scheduler)
        {
            stream_impl_detail::require_positive(period.value(), "period");
            scheduler.schedule(period.value());
        }

        static void eval(In<"ts", TsVar<"S">, InputActivity::Passive, InputValidity::Unchecked> ts,
                         Scalar<"period", TimeDelta> period,
                         NodeScheduler scheduler,
                         Out<TsVar<"S">> out)
        {
            if (ts.valid()) { out.apply(ts.value()); }
            scheduler.schedule(period.value());
        }
    };

    struct gate_impl
    {
        static void start(Scalar<"buffer_length", Int> buffer_length)
        {
            // Negative = keep only the newest |n| queued deltas (hgraph's
            // gate(..., -1) keeps the latest value); zero is meaningless.
            if (buffer_length.value() >= 0)
            {
                stream_impl_detail::require_positive(buffer_length.value(), "buffer_length");
            }
        }

        static void eval(In<"condition", TS<Bool>, InputValidity::Unchecked> condition,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Scalar<"buffer_length", Int> buffer_length,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::DeltaQueueState> state,
                         Out<TsVar<"S">> out)
        {
            auto       current        = state.get();
            const bool keep_newest    = buffer_length.value() < 0;
            const auto limit          = static_cast<std::size_t>(
                keep_newest ? -buffer_length.value() : buffer_length.value());
            const bool condition_true = condition.valid() && condition.value();
            bool       emitted        = false;

            if (ts.modified() && ts.valid())
            {
                Value delta = capture_delta(ts.base());
                if (condition_true && current.buffer.empty())
                {
                    apply_delta(out, delta.view());
                    emitted = true;
                }
                else if (keep_newest)
                {
                    while (current.buffer.size() >= limit) { current.buffer.pop_front(); }
                    current.buffer.push_back(std::move(delta));
                }
                else { stream_impl_detail::append_delta(current, std::move(delta), limit); }
            }

            if (!emitted && condition_true && !current.buffer.empty())
            {
                Value delta = std::move(current.buffer.front());
                current.buffer.pop_front();
                apply_delta(out, delta.view());
                emitted = true;
            }

            if (condition_true && !current.buffer.empty()) { scheduler.schedule(MIN_TD); }
            state.set(std::move(current));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"buffer_length", Value{std::numeric_limits<Int>::max()}}};
        }
    };

    namespace stream_impl_detail
    {
        /** The window() result meta: the named WindowResult bundle
            {buffer: TS[tuple[T,...]], index: TS[tuple[datetime,...]]}. */
        inline const TSValueTypeMetaData *window_result_meta(const ValueTypeMetaData *element)
        {
            auto &registry = TypeRegistry::instance();
            const auto *buffer = registry.list(element, 0, true);
            const auto *index  = registry.list(scalar_descriptor<DateTime>::value_meta(), 0, true);
            return registry.tsb("WindowResult", {{"buffer", registry.ts(buffer)}, {"index", registry.ts(index)}});
        }

        /** Emit the {buffer, index} bundle for the queued (time, value) entries. */
        inline void emit_window_result(const TSOutputView &out,
                                       const std::deque<std::pair<DateTime, Value>> &entries)
        {
            const auto *meta = out.schema()->value_schema;
            auto &factory = ValuePlanFactory::instance();
            const auto *element_meta = meta->fields[0].type->element_type;
            ListBuilder values{factory.type_for(element_meta)};
            ListBuilder times{factory.type_for(scalar_descriptor<DateTime>::value_meta())};
            for (const auto &[time, value] : entries)
            {
                values.push_back_copy(value.view().data());
                times.push_back_copy(&time);
            }
            BundleBuilder bundle{factory.type_for(meta)};
            bundle.set("buffer", values.build());
            bundle.set("index", times.build());
            auto mutation = out.data_view().begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.move_value_from(bundle.build()));
        }
    }  // namespace stream_impl_detail

    /** window(ts, period[, min_window_period]) — the deprecated cyclic-buffer
        window emitting a {buffer, index} bundle each tick (hgraph parity;
        prefer to_window). Tick-count form. */
    struct window_tick_impl
    {
        static constexpr auto name = "window_tick";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
            if (schema == nullptr || context.scalar_as<Int>("period") == nullptr) { return; }
            bind_output(resolution, stream_impl_detail::window_result_meta(schema->value_schema));
        }

        static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Scalar<"period", Int> period,
                         Scalar<"min_window_period", Int> min_window_period,
                         State<stream_impl_detail::TimedDeltaQueueState> state,
                         DateTime now, Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            current.buffer.emplace_back(now, Value{ts.base().value()});
            while (current.buffer.size() > static_cast<std::size_t>(period.value()))
            {
                current.buffer.pop_front();
            }
            const auto minimum = static_cast<std::size_t>(
                min_window_period.value() > 0 ? min_window_period.value() : period.value());
            if (current.buffer.size() >= minimum)
            {
                stream_impl_detail::emit_window_result(static_cast<const TSOutputView &>(out), current.buffer);
            }
            state.set(std::move(current));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"min_window_period", Value{Int{0}}}};
        }
    };

    /** window(ts, timedelta[, timedelta]) — the duration form. */
    struct window_time_impl
    {
        static constexpr auto name = "window_time";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
            if (schema == nullptr || context.scalar_as<TimeDelta>("period") == nullptr) { return; }
            bind_output(resolution, stream_impl_detail::window_result_meta(schema->value_schema));
        }

        static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Scalar<"period", TimeDelta> period,
                         Scalar<"min_window_period", TimeDelta> min_window_period,
                         State<stream_impl_detail::TimedDeltaQueueState> state,
                         DateTime now, Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            current.buffer.emplace_back(now, Value{ts.base().value()});
            while (!current.buffer.empty() && current.buffer.front().first < now - period.value())
            {
                current.buffer.pop_front();
            }
            const TimeDelta minimum =
                min_window_period.value() > TimeDelta{0} ? min_window_period.value() : period.value();
            if (now - current.buffer.front().first >= minimum)
            {
                stream_impl_detail::emit_window_result(static_cast<const TSOutputView &>(out), current.buffer);
            }
            state.set(std::move(current));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"min_window_period", Value{TimeDelta{0}}}};
        }
    };

    struct batch_impl
    {
        /* hgraph's batch: buffer ts values while blocked; a true condition
           releases them as ONE tuple - immediately on the condition tick,
           else ``delay`` after the last data tick. */
        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at_as<AnyTS>(context, 1);
            if (schema == nullptr) { return; }
            auto &registry = TypeRegistry::instance();
            bind_output(resolution, registry.ts(registry.list(schema->value_schema, 0, true)));
        }

        static void eval(In<"condition", TS<Bool>, InputValidity::Unchecked> condition,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         Scalar<"delay", TimeDelta> delay,
                         Scalar<"buffer_length", Int> buffer_length,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::DeltaQueueState> state,
                         Out<TsVar<"__out__">> out)
        {
            auto current = state.get();
            if (ts.modified() && ts.valid())
            {
                if (current.buffer.size() >= static_cast<std::size_t>(buffer_length.value()))
                {
                    throw std::overflow_error("batch buffer length exceeded");
                }
                current.buffer.push_back(capture_delta(ts.base()));
            }

            const bool condition_true = condition.valid() && condition.value();
            if (condition_true)
            {
                const bool scheduled = scheduler.is_scheduled() || scheduler.is_scheduled_now();
                if (!scheduled && !condition.modified()) { scheduler.schedule(delay.value()); }
                if ((scheduler.is_scheduled_now() || condition.modified()) && !current.buffer.empty())
                {
                    const auto &erased = static_cast<const TSOutputView &>(out);
                    const auto *meta   = erased.schema()->value_schema;
                    ListBuilder builder{ValuePlanFactory::instance().type_for(meta->element_type)};
                    for (Value &value : current.buffer) { builder.push_back_copy(value.view().data()); }
                    current.buffer.clear();
                    Value result   = builder.build();
                    auto  mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.move_value_from(std::move(result)));
                }
            }
            state.set(std::move(current));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"buffer_length", Value{std::numeric_limits<Int>::max()}}};
        }
    };

    struct throttle_impl
    {
        /* hgraph's model: an open throttle window IS a pending scheduler
           event. Ticks inside the window accumulate; the window closing
           releases them merged. ``delay_first_tick`` buffers the tick that
           would otherwise pass straight through on a closed window. */
        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"period", TS<TimeDelta>, InputValidity::Unchecked> period,
                         Scalar<"delay_first_tick", Bool> delay_first_tick,
                         NodeScheduler scheduler,
                         State<stream_impl_detail::ThrottleState> state,
                         DateTime now,
                         Out<TsVar<"S">> out)
        {
            auto current = state.get();
            if (period.modified() && period.valid())
            {
                stream_impl_detail::require_positive(period.value(), "period");
                current.period     = period.value();
                current.has_period = true;
            }

            if (ts.modified() && ts.valid())
            {
                if (!current.has_period) { throw std::invalid_argument("throttle requires a valid period"); }

                Value delta = capture_delta(ts.base());
                if (scheduler.is_scheduled() || scheduler.is_scheduled_now())
                {
                    current.pending.push_back(std::move(delta));
                }
                else if (delay_first_tick.value())
                {
                    current.pending.push_back(std::move(delta));
                    scheduler.schedule(now + current.period);
                }
                else
                {
                    apply_delta(out, delta.view());
                    scheduler.schedule(now + current.period);
                }
            }

            if (scheduler.is_scheduled_now() && !current.pending.empty())
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                bool        emitted = true;
                if (erased.schema()->kind == TSTypeKind::TSS)
                {
                    // Net the window's set deltas: an add-then-remove pair
                    // cancels; a fully-cancelled window emits nothing.
                    auto net = stream_impl_detail::net_set_deltas(current.pending);
                    if (net.has_value()) { apply_delta(out, net->view()); }
                    else { emitted = false; }
                }
                else
                {
                    for (Value &delta : current.pending) { apply_delta(out, delta.view()); }
                }
                current.pending.clear();
                if (emitted) { scheduler.schedule(now + current.period); }
            }

            state.set(std::move(current));
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"delay_first_tick", Value{false}}};
        }
    };

    struct take_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"count", Int> count, State<Int> seen, Out<TsVar<"S">> out)
        {
            if (count.value() <= 0) { return; }
            const Int index = seen.get();
            if (index < count.value()) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct drop_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"count", Int> count, State<Int> seen, Out<TsVar<"S">> out)
        {
            const Int index = seen.get();
            if (index >= count.value()) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct take_reset_impl
    {
        /* ``take(ts, reset, count)``: forward the first ``count`` ticks, then
           passivate ts; a reset tick re-arms the counter and reactivates. */
        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"reset", SIGNAL, InputValidity::Unchecked> reset,
                         Scalar<"count", Int> count, State<Int> seen, Out<TsVar<"S">> out)
        {
            if (reset.modified())
            {
                seen.set(Int{0});
                ts.make_active();
            }
            if (!ts.modified() || count.value() <= 0) { return; }
            const Int index = seen.get() + 1;
            seen.set(index);
            if (index == count.value()) { ts.make_passive(); }
            out.apply(ts.value());
        }
    };

    struct drop_time_impl
    {
        /* ``drop(ts, period)``: drop ticks until ``period`` has elapsed since
           the first tick, then forward the rest. */
        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"period", TimeDelta> period,
                         State<DateTime> first, DateTime now, Out<TsVar<"S">> out)
        {
            if (first.get() == MIN_DT) { first.set(now); }
            if (now - first.get() > period.value()) { out.apply(ts.value()); }
        }
    };

    struct step_impl
    {
        static void start(Scalar<"step_size", Int> step_size)
        {
            stream_impl_detail::require_positive(step_size.value(), "step_size");
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"step_size", Int> step_size, State<Int> seen,
                         Out<TsVar<"S">> out)
        {
            const Int index = seen.get();
            if (index % step_size.value() == 0) { out.apply(ts.value()); }
            seen.set(index + 1);
        }
    };

    struct slice_impl
    {
        static void start(Scalar<"step_size", Int> step_size)
        {
            stream_impl_detail::require_positive(step_size.value(), "step_size");
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"start", Int> start, Scalar<"stop", Int> stop,
                         Scalar<"step_size", Int> step_size, State<Int> seen, Out<TsVar<"S">> out)
        {
            const Int index = seen.get();
            seen.set(index + 1);

            if (start.value() < 0) { return; }
            if (index < start.value()) { return; }
            if (stop.value() >= 0 && index >= stop.value()) { return; }
            if ((index - start.value()) % step_size.value() == 0) { out.apply(ts.value()); }
        }
    };

    /** rolling_average(ts, period[, min_window_period]) — the TICK form
        (hgraph's window helper): ``(sum(ts) - sum(lag(ts, period)))`` over
        the full period, with partial-window denominators from
        ``min_window_period`` onward. */
    struct rolling_average_tick_compose
    {
        static constexpr auto name = "rolling_average_tick";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.scalar_as<Int>("period") != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (output_bound(resolution)) { return; }
            bind_output(resolution, TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()));
        }

        static auto compose(Wiring &w, NamedPort<"ts", TS<ScalarVar<"T">>> ts, Scalar<"period", Int> period,
                            Scalar<"min_window_period", Int> min_window_period)
        {
            auto current = wire<sum_>(w, ts);
            auto delayed = wire<sum_>(w, wire<lag>(w, ts, period.value()));
            if (min_window_period.value() <= 0)
            {
                auto denom = wire<const_, TS<Float>>(w, Float(period.value()));
                return wire<div_>(w, wire<sub_>(w, current, delayed), denom);
            }
            // The early window: ticks counted from min_window_period up to
            // period cap the denominator, and the missing lagged sum is a
            // sampled zero (upstream's if_then_else/count/take/drop shape).
            auto counted = wire<drop>(w, wire<stream_impl_detail::tick_count>(w, wire<take>(w, ts, period.value())),
                                      min_window_period.value() - 1);
            auto capped  = wire<min_>(w, counted, wire<const_, TS<Int>>(w, Int{period.value()}));
            const auto *element =
                time_series_schema_as<AnyTS>(ts.erased().schema)->value_schema;
            Value zero{ValuePlanFactory::instance().type_for(element)};
            auto fallback     = wire<sample>(w, counted, wire<const_>(w, std::move(zero)));
            auto safe_delayed = wire<default_>(w, delayed, fallback);
            return wire<div_>(w, wire<sub_>(w, current, safe_delayed), capped);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"min_window_period", Value{Int{0}}}};
        }
    };

    /** rolling_average(ts, timedelta[, timedelta]) — the DURATION form:
        the tick-count denominator is the covered window's tick delta, NaN
        while empty. */
    struct rolling_average_time_compose
    {
        static constexpr auto name = "rolling_average_time";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.scalar_as<TimeDelta>("period") != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (output_bound(resolution)) { return; }
            bind_output(resolution, TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()));
        }

        static auto compose(Wiring &w, NamedPort<"ts", TS<ScalarVar<"T">>> ts,
                            Scalar<"period", TimeDelta> period,
                            Scalar<"min_window_period", TimeDelta> min_window_period)
        {
            auto lagged        = wire<lag>(w, ts, period.value());
            auto current       = wire<sum_>(w, ts);
            auto delayed       = wire<sum_>(w, lagged);
            auto delayed_count = wire<stream_impl_detail::tick_count>(w, lagged);
            if (min_window_period.value() > TimeDelta{0})
            {
                delayed_count =
                    wire<default_>(w, delayed_count, wire<const_, TS<Int>>(w, Int{0}, period.value()));
            }
            auto delta_ticks = wire<sub_>(w, wire<stream_impl_detail::tick_count>(w, ts), delayed_count);
            auto empty       = wire<eq_>(w, delta_ticks, wire<const_, TS<Int>>(w, Int{0}));
            auto nan   = wire<const_, TS<Float>>(w, std::numeric_limits<Float>::quiet_NaN());
            auto denom = wire<if_then_else>(w, empty, nan, wire<convert, TS<Float>>(w, delta_ticks));
            return wire<div_>(w, wire<convert, TS<Float>>(w, wire<sub_>(w, current, delayed)), denom);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"min_window_period", Value{TimeDelta{0}}}};
        }
    };

    struct dedup_scalar_impl
    {
        static void eval(In<"ts", TS<ScalarVar<"T">>> ts,
                         RecordableState<TS<ScalarVar<"T">>> last,
                         Out<TS<ScalarVar<"T">>> out)
        {
            const ValueView value = ts.base().value();
            if (!last.valid() || !last.value().equals(value)) { out.apply(value); }
            last.apply(value);
        }
    };

    /** dedup over a TSD: per-element dedup via map_ — unchanged element
        values do not tick, key removals forward (hgraph's type-directed
        dedup_default for dicts). */
    struct dedup_tsd_map
    {
        static constexpr auto name = "dedup_tsd_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            tsl_itemwise_impl_detail::resolve_map_output<dedup>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 1 && time_series_schema_at_as<AnyTSD>(context, 0) != nullptr;
        }

        static auto compose(Wiring &w, NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
        {
            return wire<map_>(w, fn<dedup>(), ts);
        }
    };

    /**
     * TSS deltas are already canonical membership changes: applying an add
     * for a present key or a remove for an absent key does not tick.
     * Reapplying only that canonical delta filters empty replay ticks while
     * preserving real additions and removals.
     */
    struct dedup_tss_impl
    {
        static void eval(In<"ts", TSS<ScalarVar<"K">>> ts,
                         Out<TSS<ScalarVar<"K">>> out)
        {
            const TSSInputView &set = ts;
            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const ValueView &key : set.removed()) { (void)mutation.remove(key); }
            for (const ValueView &key : set.added()) { (void)mutation.add(key); }
        }
    };

    struct dedup_float_tol_impl
    {
        /* ``dedup(ts, abs_tol=...)``: floats within ``abs_tol`` of the last
           emitted value count as duplicates. The default matches upstream's
           float overload (abs_tol=1e-15), so a plain ``dedup(TS[float])``
           is tolerance-based, not exact (parity issue #69). */
        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"abs_tol", Value{Float{1e-15}}}};
        }

        static void eval(In<"ts", TS<Float>> ts, In<"abs_tol", TS<Float>> abs_tol,
                         RecordableState<TS<Float>> last, Out<TS<Float>> out)
        {
            const Float value = ts.value();
            if (last.valid())
            {
                const Float difference = value - last.value().checked_as<Float>();
                const Float tolerance  = abs_tol.value();
                if (-tolerance < difference && difference < tolerance) { return; }
            }
            out.set(value);
            last.set(value);
        }
    };

    void register_stream_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_STREAM_IMPL_H
