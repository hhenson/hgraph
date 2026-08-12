#include <hgraph/analytics/operators.h>

#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/time_series/ts_input/window_view.h>
#include <hgraph/types/value/value_builder.h>

#include "operator_registration.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>

#include <concepts>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/format.h>

namespace hgraph::analytics
{
    namespace
    {
        using namespace hgraph::operator_type_resolution;

        [[nodiscard]] ValueTypeRef binding_for(const ValueTypeMetaData *meta)
        {
            const auto binding = ValuePlanFactory::instance().type_for(meta);
            if (binding == nullptr)
            {
                throw std::logic_error(
                    "hgraph analytics array output has no value binding");
            }
            return binding;
        }

        [[nodiscard]] bool numeric_array(const ValueTypeMetaData *meta,
                                         const ValueTypeMetaData *element) noexcept
        {
            return TypeRegistry::is_array(meta) &&
                   TypeRegistry::array_element(meta) == element;
        }

        template <typename T>
        void flatten_numeric_array(const ValueView &value, std::vector<T> &out)
        {
            if (!TypeRegistry::is_array(value.schema()))
            {
                out.push_back(value.checked_as<T>());
                return;
            }
            for (ValueView child : value.as_list())
            {
                flatten_numeric_array<T>(child, out);
            }
        }

        template <typename T>
        [[nodiscard]] arrow::Datum numeric_datum(const std::vector<T> &values)
        {
            static_assert(std::same_as<T, Int> || std::same_as<T, Float>);
            const auto length = static_cast<std::int64_t>(values.size());
            auto buffer = arrow::Buffer::Wrap(
                values.data(),
                static_cast<std::int64_t>(values.size() * sizeof(T)));
            auto type = []
            {
                if constexpr (std::same_as<T, Int>) { return arrow::int64(); }
                else { return arrow::float64(); }
            }();
            return arrow::Datum{arrow::ArrayData::Make(
                std::move(type), length, {nullptr, std::move(buffer)}, 0)};
        }

        [[nodiscard]] arrow::Datum checked_arrow_result(
            arrow::Result<arrow::Datum> result, std::string_view operation)
        {
            if (!result.ok())
            {
                throw std::runtime_error(fmt::format(
                    "{}: Arrow Compute failed: {}", operation,
                    result.status().ToString()));
            }
            return std::move(result).ValueUnsafe();
        }

        [[nodiscard]] arrow::compute::QuantileOptions::Interpolation
        quantile_interpolation(std::string_view method)
        {
            using Interpolation = arrow::compute::QuantileOptions::Interpolation;
            if (method == "linear") { return Interpolation::LINEAR; }
            if (method == "lower") { return Interpolation::LOWER; }
            if (method == "higher") { return Interpolation::HIGHER; }
            if (method == "nearest") { return Interpolation::NEAREST; }
            if (method == "midpoint") { return Interpolation::MIDPOINT; }
            throw std::invalid_argument(
                "hgraph.analytics.quantile: unsupported method " +
                std::string{method});
        }

        [[nodiscard]] Float quantile_result(const arrow::Datum &datum)
        {
            if (!datum.is_array() || datum.array()->length != 1)
            {
                throw std::runtime_error(
                    "hgraph.analytics.quantile: Arrow Compute returned a non-scalar array");
            }
            if (datum.array()->null_count != 0)
            {
                return std::numeric_limits<Float>::quiet_NaN();
            }
            switch (datum.type()->id())
            {
                case arrow::Type::DOUBLE:
                    return datum.array()->GetValues<Float>(1)[0];
                case arrow::Type::INT64:
                    return static_cast<Float>(datum.array()->GetValues<Int>(1)[0]);
                default:
                    throw std::runtime_error(fmt::format(
                        "hgraph.analytics.quantile: Arrow Compute returned type {}",
                        datum.type()->ToString()));
            }
        }

        [[nodiscard]] Float standard_deviation_result(const arrow::Datum &datum)
        {
            if (!datum.is_scalar() || datum.type()->id() != arrow::Type::DOUBLE)
            {
                throw std::runtime_error(
                    "hgraph.analytics.array_std: Arrow Compute returned an unexpected result");
            }
            const auto &scalar = datum.scalar_as<arrow::DoubleScalar>();
            return scalar.is_valid
                       ? scalar.value
                       : std::numeric_limits<Float>::quiet_NaN();
        }

        [[nodiscard]] bool window_ready(const TSWInputView &window)
        {
            if (window.empty()) { return false; }
            if (window.time_based())
            {
                const auto minimum = window.min_time_range();
                return minimum <= TimeDelta{0} ||
                       window.time_at(window.size() - 1) - window.time_at(0) >= minimum;
            }
            return window.size() >= window.min_period();
        }

        template <typename T, bool IsWindow>
        struct quantile_impl
        {
            static constexpr auto name = []
            {
                if constexpr (IsWindow)
                {
                    return std::same_as<T, Int>
                               ? "hgraph.analytics.quantile.window.int"
                               : "hgraph.analytics.quantile.window.float";
                }
                else
                {
                    return std::same_as<T, Int>
                               ? "hgraph.analytics.quantile.array.int"
                               : "hgraph.analytics.quantile.array.float";
                }
            }();

            static auto defaults()
            {
                return std::tuple{arg<"method">(Str{"linear"})};
            }

            static bool requires_(const ResolutionMap &,
                                  OperatorCallContext context)
            {
                if constexpr (IsWindow)
                {
                    const auto *window =
                        time_series_schema_at_as<AnyTSW>(context, 0);
                    return window != nullptr &&
                           window->value_schema->element_type ==
                               scalar_descriptor<T>::value_meta();
                }
                else
                {
                    return numeric_array(ts_value_schema_at(context, 0),
                                         scalar_descriptor<T>::value_meta());
                }
            }

            static void eval(In<"values", TsVar<"A">> input,
                             In<"q", TS<Float>> q,
                             Scalar<"method", Str> method,
                             Out<TS<Float>> out)
            {
                // Arrow's exact quantile kernel provides the five interpolation
                // policies in O(N) auxiliary storage. Window readiness is checked
                // before copying so warm-up cycles remain invalid and allocation-free.
                std::vector<T> values;
                if constexpr (IsWindow)
                {
                    const TSWInputView window{input.base().borrowed_ref()};
                    if (!window_ready(window)) { return; }
                    values.reserve(window.size());
                    for (std::size_t index = 0; index < window.size(); ++index)
                    {
                        values.push_back(window.at(index).checked_as<T>());
                    }
                }
                else
                {
                    flatten_numeric_array<T>(input.value(), values);
                }

                if (values.empty())
                {
                    throw std::invalid_argument(
                        "hgraph.analytics.quantile: values must not be empty");
                }
                if (!(q.value() >= 0.0 && q.value() <= 1.0))
                {
                    throw std::invalid_argument(
                        "hgraph.analytics.quantile: q must be in [0, 1]");
                }
                const arrow::compute::QuantileOptions options{
                    q.value(), quantile_interpolation(method.value()), false, 1};
                out.set(quantile_result(checked_arrow_result(
                    arrow::compute::Quantile(numeric_datum(values), options),
                    "hgraph.analytics.quantile")));
            }
        };

        template <typename T>
        struct array_std_impl
        {
            static constexpr auto name = std::same_as<T, Int>
                                             ? "hgraph.analytics.array_std.int"
                                             : "hgraph.analytics.array_std.float";

            static auto defaults()
            {
                return std::tuple{arg<"ddof">(Int{0})};
            }

            static bool requires_(const ResolutionMap &,
                                  OperatorCallContext context)
            {
                return numeric_array(ts_value_schema_at(context, 0),
                                     scalar_descriptor<T>::value_meta());
            }

            static void eval(In<"values", TsVar<"A">> input,
                             Scalar<"ddof", Int> ddof, Out<TS<Float>> out)
            {
                // Arrow's two-pass variance kernel avoids the catastrophic
                // cancellation of E[x^2] - E[x]^2. Flattening gives ndarray-like
                // whole-array semantics and uses O(N) temporary storage.
                std::vector<T> values;
                flatten_numeric_array<T>(input.value(), values);
                if (ddof.value() < std::numeric_limits<int>::min() ||
                    ddof.value() > std::numeric_limits<int>::max())
                {
                    throw std::invalid_argument(
                        "hgraph.analytics.array_std: ddof is outside Arrow's supported range");
                }
                const arrow::compute::VarianceOptions options{
                    static_cast<int>(ddof.value()), false, 0};
                out.set(standard_deviation_result(checked_arrow_result(
                    arrow::compute::Stddev(numeric_datum(values), options),
                    "hgraph.analytics.array_std")));
            }
        };

        struct rolling_window_arrays_impl
        {
            static constexpr auto name = "hgraph.analytics.rolling_window";

            static bool requires_(const ResolutionMap &,
                                  OperatorCallContext context)
            {
                const auto *window =
                    time_series_schema_at_as<AnyTSW>(context, 0);
                return window != nullptr && !window->is_duration_based() &&
                       window->period() != 0;
            }

            static void resolve_default_types(ResolutionMap &resolution,
                                              OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *window =
                    time_series_schema_at_as<AnyTSW>(context, 0);
                if (window == nullptr || window->is_duration_based() ||
                    window->period() == 0)
                {
                    return;
                }
                auto &registry = TypeRegistry::instance();
                const std::size_t dimension =
                    window->period() == window->min_period()
                        ? window->period()
                        : std::size_t{0};
                const auto *buffer = registry.array(
                    window->value_schema->element_type, dimension);
                const auto *index = registry.array(
                    scalar_descriptor<DateTime>::value_meta(), dimension);
                const std::string schema_name =
                    window->period() == window->min_period()
                        ? fmt::format("RollingWindowResult[{},{}]",
                                      window->value_schema->element_type->name(),
                                      window->period())
                        : fmt::format("RollingWindowResult[{},{},{}]",
                                      window->value_schema->element_type->name(),
                                      window->period(), window->min_period());
                bind_output(resolution, registry.tsb(
                    schema_name, {{"buffer", registry.ts(buffer)},
                                  {"index", registry.ts(index)}}));
            }

            static void eval(In<"window", TsVar<"W">> input,
                             Out<TsVar<"__out__">> out)
            {
                // The core TSW owns the circular history. This node performs an
                // O(N) chronological materialization into the public Array bundle
                // only after the requested minimum population has been reached.
                const TSWInputView window{input.base().borrowed_ref()};
                if (!window_ready(window)) { return; }

                const auto &erased = static_cast<const TSOutputView &>(out);
                auto bundle = erased.as_bundle();
                auto buffer_field = bundle.field("buffer");
                auto index_field = bundle.field("index");
                const auto *buffer_meta = buffer_field.schema()->value_schema;
                const auto *index_meta = index_field.schema()->value_schema;

                Value buffer;
                if (buffer_meta->fixed_size == 0)
                {
                    ListBuilder builder{binding_for(buffer_meta->element_type)};
                    for (std::size_t index = 0; index < window.size(); ++index)
                    {
                        builder.push_back_copy(window.at(index).data());
                    }
                    ListStorage storage = builder.build_storage();
                    buffer = Value{binding_for(buffer_meta), &storage};
                }
                else
                {
                    if (window.size() != buffer_meta->fixed_size)
                    {
                        throw std::logic_error(
                            "hgraph.analytics.rolling_window: values do not match the fixed shape");
                    }
                    buffer = Value{binding_for(buffer_meta)};
                    auto values = buffer.as_list().begin_mutation();
                    values.resize(buffer_meta->fixed_size);
                    for (std::size_t index = 0; index < window.size(); ++index)
                    {
                        values.at(index).copy_from(window.at(index));
                    }
                }

                Value times;
                if (index_meta->fixed_size == 0)
                {
                    ListBuilder builder{binding_for(index_meta->element_type)};
                    for (std::size_t index = 0; index < window.size(); ++index)
                    {
                        const DateTime time = window.time_at(index);
                        builder.push_back_copy(&time);
                    }
                    ListStorage storage = builder.build_storage();
                    times = Value{binding_for(index_meta), &storage};
                }
                else
                {
                    if (window.size() != index_meta->fixed_size)
                    {
                        throw std::logic_error(
                            "hgraph.analytics.rolling_window: timestamps do not match the fixed shape");
                    }
                    times = Value{binding_for(index_meta)};
                    auto values = times.as_list().begin_mutation();
                    values.resize(index_meta->fixed_size);
                    for (std::size_t index = 0; index < window.size(); ++index)
                    {
                        values.at(index).checked_mutable_as<DateTime>() =
                            window.time_at(index);
                    }
                }

                {
                    auto mutation =
                        buffer_field.begin_mutation(erased.evaluation_time());
                    static_cast<void>(
                        mutation.move_value_from(std::move(buffer)));
                }
                {
                    auto mutation =
                        index_field.begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.move_value_from(std::move(times)));
                }
            }
        };

    }  // namespace

    void detail::register_array_operators()
    {
        static const bool arrow_compute_initialised = []
        {
            const auto status = arrow::compute::Initialize();
            if (!status.ok())
            {
                throw std::runtime_error(
                    "hgraph analytics: Arrow Compute initialization failed: " +
                    status.ToString());
            }
            return true;
        }();
        static_cast<void>(arrow_compute_initialised);

        register_overload<quantile, quantile_impl<Int, false>>();
        register_overload<quantile, quantile_impl<Float, false>>();
        register_overload<quantile, quantile_impl<Int, true>>();
        register_overload<quantile, quantile_impl<Float, true>>();
        register_overload<array_std, array_std_impl<Int>>();
        register_overload<array_std, array_std_impl<Float>>();
        register_overload<rolling_window, rolling_window_arrays_impl>();
    }
}  // namespace hgraph::analytics
