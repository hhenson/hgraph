#ifndef HGRAPH_ANALYTICS_SHAPED_ARRAY_OPERATORS_H
#define HGRAPH_ANALYTICS_SHAPED_ARRAY_OPERATORS_H

#include <hgraph/analytics/operators.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_input/window_view.h>
#include <hgraph/types/value/value.h>

#include <algorithm>
#include <concepts>
#include <functional>
#include <numeric>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace hgraph::analytics
{
    namespace array_detail
    {
        [[nodiscard]] bool numeric_array(const ValueTypeMetaData *meta,
                                         const ValueTypeMetaData *element) noexcept;
        [[nodiscard]] std::vector<Int> index_components(const ValueView &index);
        [[nodiscard]] const ValueTypeMetaData *indexed_result(
            const ValueTypeMetaData *array, std::size_t components);
        [[nodiscard]] ValueView index_value(ValueView array,
                                            std::span<const Int> components);

        [[nodiscard]] Value array_from_window(const TSWInputView &window,
                                              std::optional<ValueView> zero,
                                              const ValueTypeMetaData *output);

        template <typename T>
        [[nodiscard]] Value cumulative_sum(ValueView input,
                                           const ValueTypeMetaData *output,
                                           std::optional<Int> axis);

        template <typename T>
        [[nodiscard]] Value correlation(ValueView x, std::optional<ValueView> y,
                                        bool rowvar,
                                        const ValueTypeMetaData *output);
        void resolve_correlation_output(ResolutionMap &resolution,
                                        OperatorCallContext context,
                                        bool has_y, bool rowvar);

    }  // namespace array_detail

    using namespace hgraph::operator_type_resolution;

    struct array_get_item_impl
    {
        static constexpr auto name = "hgraph.analytics.array_get_item";

        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"values", TsVar<"A">> values,
                         Scalar<"index", ScalarVar<"I">> index,
                         Out<TsVar<"__out__">> out);
    };

    template <bool HasZero>
    struct window_values_impl;

    template <>
    struct window_values_impl<false>
    {
        static constexpr auto name = "hgraph.analytics.window_values.default";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"window", TsVar<"W">> window,
                         Out<TsVar<"__out__">> out);
    };

    template <>
    struct window_values_impl<true>
    {
        static constexpr auto name = "hgraph.analytics.window_values.live_zero";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"window", TsVar<"W">> window,
                         In<"zero", TsVar<"Z">> zero,
                         Out<TsVar<"__out__">> out);
    };

    struct window_values_scalar_zero_impl
    {
        static constexpr auto name = "hgraph.analytics.window_values.scalar_zero";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"window", TsVar<"W">> window,
                         Scalar<"zero", ScalarVar<"Z">> zero,
                         Out<TsVar<"__out__">> out);
    };

    template <typename T, bool HasAxis>
    struct cumulative_sum_impl;

    template <typename T>
    struct cumulative_sum_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int>
                                         ? "hgraph.analytics.cumulative_sum.int"
                                         : "hgraph.analytics.cumulative_sum.float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return array_detail::numeric_array(ts_value_schema_at(context, 0),
                                               scalar_descriptor<T>::value_meta());
        }
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (!output_bound(resolution))
            {
                if (const auto *input = ts_value_schema_at(context, 0);
                    TypeRegistry::is_array(input))
                {
                    const auto dimensions = TypeRegistry::array_dimensions(input);
                    const std::size_t size = std::ranges::find(dimensions, 0) != dimensions.end()
                                                 ? std::size_t{0}
                                                 : std::accumulate(dimensions.begin(), dimensions.end(),
                                                                   std::size_t{1}, std::multiplies<>{});
                    bind_output(resolution, TypeRegistry::instance().ts(
                        TypeRegistry::instance().array(
                            TypeRegistry::array_element(input), size)));
                }
            }
        }
        static void eval(In<"values", TsVar<"A">> input,
                         Out<TsVar<"__out__">> out)
        {
            // A flattened prefix scan is O(n). With an axis, contiguous
            // row-major strides form independent O(n) scans without reshaping.
            Value value = array_detail::cumulative_sum<T>(
                input.value(), static_cast<const TSOutputView &>(out).schema()->value_schema,
                std::nullopt);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct cumulative_sum_impl<T, true> : cumulative_sum_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int>
                                         ? "hgraph.analytics.cumulative_sum.axis.int"
                                         : "hgraph.analytics.cumulative_sum.axis.float";
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            if (!output_bound(resolution))
            {
                if (const auto *input = time_series_schema_at(context, 0))
                {
                    bind_output(resolution, input);
                }
            }
        }
        static void eval(In<"values", TsVar<"A">> input,
                         Scalar<"axis", Int> axis,
                         Out<TsVar<"__out__">> out)
        {
            // Traverse row-major strides along the selected axis so the input
            // shape is retained while each axis line receives its own scan.
            Value value = array_detail::cumulative_sum<T>(
                input.value(), static_cast<const TSOutputView &>(out).schema()->value_schema,
                axis.value());
            out.apply(value.view());
        }
    };

    template <typename T, bool HasY>
    struct correlation_impl;

    template <typename T, bool HasY>
    struct correlation_default_impl;

    template <typename T>
    struct correlation_default_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int>
                                         ? "hgraph.analytics.correlation.default.int"
                                         : "hgraph.analytics.correlation.default.float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            array_detail::resolve_correlation_output(resolution, context, false, true);
        }
        static void eval(In<"x", TsVar<"X">> x, Out<TsVar<"__out__">> out)
        {
            // Boost.Math bivariate_statistics supplies each coefficient; one
            // vector needs a single O(observations) pass.
            Value value = array_detail::correlation<T>(
                x.value(), std::nullopt, true,
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct correlation_default_impl<T, true>
    {
        static constexpr auto name = std::same_as<T, Int>
                                         ? "hgraph.analytics.correlation.xy.default.int"
                                         : "hgraph.analytics.correlation.xy.default.float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            array_detail::resolve_correlation_output(resolution, context, true, true);
        }
        static void eval(In<"x", TsVar<"X">> x, In<"y", TsVar<"Y">> y,
                         Out<TsVar<"__out__">> out)
        {
            // Build the combined variable set once, then evaluate its symmetric
            // coefficient matrix in O(variables^2 * observations).
            Value value = array_detail::correlation<T>(
                x.value(), y.value(), true,
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct correlation_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int>
                                         ? "hgraph.analytics.correlation.int"
                                         : "hgraph.analytics.correlation.float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"x", TsVar<"X">> x, Scalar<"rowvar", Bool> rowvar,
                         Out<TsVar<"__out__">> out)
        {
            // Resolve row/column orientation at wiring time, then delegate the
            // O(variables^2 * observations) pairs to Boost.Math.
            Value value = array_detail::correlation<T>(
                x.value(), std::nullopt, rowvar.value(),
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct correlation_impl<T, true>
    {
        static constexpr auto name = std::same_as<T, Int>
                                         ? "hgraph.analytics.correlation.xy.int"
                                         : "hgraph.analytics.correlation.xy.float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"x", TsVar<"X">> x, In<"y", TsVar<"Y">> y,
                         Scalar<"rowvar", Bool> rowvar,
                         Out<TsVar<"__out__">> out)
        {
            // Concatenate variables logically and fill only one matrix triangle;
            // mirroring avoids a second Boost.Math pass for symmetric pairs.
            Value value = array_detail::correlation<T>(
                x.value(), y.value(), rowvar.value(),
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

}  // namespace hgraph::analytics

#endif  // HGRAPH_ANALYTICS_SHAPED_ARRAY_OPERATORS_H
