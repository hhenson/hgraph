#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_NUMPY_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_NUMPY_IMPL_H

#include <hgraph/lib/std/operators/numpy.h>
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

namespace hgraph::stdlib
{
    namespace numpy_detail
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
        [[nodiscard]] Value array_from_window_times(const TSWInputView &window,
                                                    const ValueTypeMetaData *output);
        [[nodiscard]] bool window_ready(const TSWInputView &window);

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

        template <typename T>
        [[nodiscard]] Float quantile(ValueView input, Float q,
                                     std::string_view method);

        template <typename T>
        [[nodiscard]] Float quantile(const TSWInputView &input, Float q,
                                     std::string_view method);

        template <typename T>
        [[nodiscard]] Float standard_deviation(ValueView input, Int ddof);
    }  // namespace numpy_detail

    using namespace hgraph::operator_type_resolution;

    struct array_get_item_impl
    {
        static constexpr auto name = "array_get_item";

        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"ts", TsVar<"A">> ts,
                         Scalar<"idx", ScalarVar<"I">> index,
                         Out<TsVar<"__out__">> out);
    };

    template <bool HasZero>
    struct as_array_erased_impl;

    template <>
    struct as_array_erased_impl<false>
    {
        static constexpr auto name = "as_array_erased";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"tsw", TsVar<"W">> tsw,
                         Out<TsVar<"__out__">> out);
    };

    template <>
    struct as_array_erased_impl<true>
    {
        static constexpr auto name = "as_array_erased_zero";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"tsw", TsVar<"W">> tsw, In<"zero", TsVar<"Z">> zero,
                         Out<TsVar<"__out__">> out);
    };

    struct as_array_scalar_zero_impl
    {
        static constexpr auto name = "as_array_erased_scalar_zero";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"tsw", TsVar<"W">> tsw,
                         Scalar<"zero", ScalarVar<"Z">> zero,
                         Out<TsVar<"__out__">> out);
    };

    template <typename T, bool HasAxis>
    struct cumsum_impl;

    template <typename T>
    struct cumsum_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int> ? "cumsum_int" : "cumsum_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return numpy_detail::numeric_array(ts_value_schema_at(context, 0),
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
        static void eval(In<"a", TsVar<"A">> input, Out<TsVar<"__out__">> out)
        {
            Value value = numpy_detail::cumulative_sum<T>(
                input.value(), static_cast<const TSOutputView &>(out).schema()->value_schema,
                std::nullopt);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct cumsum_impl<T, true> : cumsum_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int> ? "cumsum_axis_int" : "cumsum_axis_float";
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
        static void eval(In<"a", TsVar<"A">> input, Scalar<"axis", Int> axis,
                         Out<TsVar<"__out__">> out)
        {
            Value value = numpy_detail::cumulative_sum<T>(
                input.value(), static_cast<const TSOutputView &>(out).schema()->value_schema,
                axis.value());
            out.apply(value.view());
        }
    };

    template <typename T, bool HasY>
    struct corrcoef_impl;

    template <typename T, bool HasY>
    struct corrcoef_default_impl;

    template <typename T>
    struct corrcoef_default_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int> ? "corrcoef_default_int" : "corrcoef_default_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            numpy_detail::resolve_correlation_output(resolution, context, false, true);
        }
        static void eval(In<"x", TsVar<"X">> x, Out<TsVar<"__out__">> out)
        {
            Value value = numpy_detail::correlation<T>(
                x.value(), std::nullopt, true,
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct corrcoef_default_impl<T, true>
    {
        static constexpr auto name = std::same_as<T, Int> ? "corrcoef_xy_default_int" : "corrcoef_xy_default_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context)
        {
            numpy_detail::resolve_correlation_output(resolution, context, true, true);
        }
        static void eval(In<"x", TsVar<"X">> x, In<"y", TsVar<"Y">> y,
                         Out<TsVar<"__out__">> out)
        {
            Value value = numpy_detail::correlation<T>(
                x.value(), y.value(), true,
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct corrcoef_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int> ? "corrcoef_int" : "corrcoef_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"x", TsVar<"X">> x, Scalar<"rowvar", Bool> rowvar,
                         Out<TsVar<"__out__">> out)
        {
            Value value = numpy_detail::correlation<T>(
                x.value(), std::nullopt, rowvar.value(),
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct corrcoef_impl<T, true>
    {
        static constexpr auto name = std::same_as<T, Int> ? "corrcoef_xy_int" : "corrcoef_xy_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"x", TsVar<"X">> x, In<"y", TsVar<"Y">> y,
                         Scalar<"rowvar", Bool> rowvar,
                         Out<TsVar<"__out__">> out)
        {
            Value value = numpy_detail::correlation<T>(
                x.value(), y.value(), rowvar.value(),
                static_cast<const TSOutputView &>(out).schema()->value_schema);
            out.apply(value.view());
        }
    };

    template <typename T>
    struct quantile_array_impl
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_array_int" : "quantile_array_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return numpy_detail::numeric_array(ts_value_schema_at(context, 0),
                                               scalar_descriptor<T>::value_meta());
        }
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Scalar<"method", Str> method, Scalar<"keepdims", Bool>,
                         Out<TS<Float>> out)
        {
            out.set(numpy_detail::quantile<T>(input.value(), q.value(), method.value()));
        }
    };

    template <typename T, bool HasMethod>
    struct quantile_array_default_impl;

    template <typename T>
    struct quantile_array_keepdims_impl : quantile_array_impl<T>
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_array_keepdims_int" : "quantile_array_keepdims_float";
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Scalar<"keepdims", Bool>, Out<TS<Float>> out)
        {
            out.set(numpy_detail::quantile<T>(input.value(), q.value(), "linear"));
        }
    };

    template <typename T>
    struct quantile_array_default_impl<T, false> : quantile_array_impl<T>
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_array_default_int" : "quantile_array_default_float";
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Out<TS<Float>> out)
        {
            out.set(numpy_detail::quantile<T>(input.value(), q.value(), "linear"));
        }
    };

    template <typename T>
    struct quantile_array_default_impl<T, true> : quantile_array_impl<T>
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_array_method_int" : "quantile_array_method_float";
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Scalar<"method", Str> method, Out<TS<Float>> out)
        {
            out.set(numpy_detail::quantile<T>(input.value(), q.value(), method.value()));
        }
    };

    template <typename T>
    struct quantile_window_impl
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_window_int" : "quantile_window_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
            return window != nullptr && window->value_schema->element_type ==
                                            scalar_descriptor<T>::value_meta();
        }
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Scalar<"method", Str> method, Scalar<"keepdims", Bool>,
                         Out<TS<Float>> out)
        {
            const TSWInputView window{input.base().borrowed_ref()};
            if (!numpy_detail::window_ready(window)) { return; }
            out.set(numpy_detail::quantile<T>(window, q.value(), method.value()));
        }
    };

    template <typename T, bool HasMethod>
    struct quantile_window_default_impl;

    template <typename T>
    struct quantile_window_keepdims_impl : quantile_window_impl<T>
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_window_keepdims_int" : "quantile_window_keepdims_float";
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Scalar<"keepdims", Bool>, Out<TS<Float>> out)
        {
            const TSWInputView window{input.base().borrowed_ref()};
            if (!numpy_detail::window_ready(window)) { return; }
            out.set(numpy_detail::quantile<T>(window, q.value(), "linear"));
        }
    };

    template <typename T>
    struct quantile_window_default_impl<T, false> : quantile_window_impl<T>
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_window_default_int" : "quantile_window_default_float";
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Out<TS<Float>> out)
        {
            const TSWInputView window{input.base().borrowed_ref()};
            if (!numpy_detail::window_ready(window)) { return; }
            out.set(numpy_detail::quantile<T>(window, q.value(), "linear"));
        }
    };

    template <typename T>
    struct quantile_window_default_impl<T, true> : quantile_window_impl<T>
    {
        static constexpr auto name = std::same_as<T, Int> ? "quantile_window_method_int" : "quantile_window_method_float";
        static void eval(In<"a", TsVar<"A">> input, In<"q", TS<Float>> q,
                         Scalar<"method", Str> method, Out<TS<Float>> out)
        {
            const TSWInputView window{input.base().borrowed_ref()};
            if (!numpy_detail::window_ready(window)) { return; }
            out.set(numpy_detail::quantile<T>(window, q.value(), method.value()));
        }
    };

    struct rolling_window_arrays_impl
    {
        static constexpr auto name = "rolling_window_arrays";
        static bool requires_(const ResolutionMap &, OperatorCallContext context);
        static void resolve_default_types(ResolutionMap &resolution,
                                          OperatorCallContext context);
        static void eval(In<"window", TsVar<"W">> input,
                         Out<TsVar<"__out__">> out);
    };

    template <typename T, bool HasDdof>
    struct standard_deviation_impl;

    template <typename T>
    struct standard_deviation_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int> ? "np_std_int" : "np_std_float";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return numpy_detail::numeric_array(ts_value_schema_at(context, 0),
                                               scalar_descriptor<T>::value_meta());
        }
        static void eval(In<"ts", TsVar<"A">> input, Out<TS<Float>> out)
        {
            out.set(numpy_detail::standard_deviation<T>(input.value(), 0));
        }
    };

    template <typename T>
    struct standard_deviation_impl<T, true> : standard_deviation_impl<T, false>
    {
        static constexpr auto name = std::same_as<T, Int> ? "np_std_ddof_int" : "np_std_ddof_float";
        static void eval(In<"ts", TsVar<"A">> input, Scalar<"ddof", Int> ddof,
                         Out<TS<Float>> out)
        {
            out.set(numpy_detail::standard_deviation<T>(input.value(), ddof.value()));
        }
    };

    void register_numpy_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_NUMPY_IMPL_H
