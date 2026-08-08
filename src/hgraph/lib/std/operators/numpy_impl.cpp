#include <hgraph/lib/std/operators/impl/numpy_impl.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>
#include <boost/math/statistics/bivariate_statistics.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <concepts>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <type_traits>

#include <fmt/format.h>

namespace hgraph::stdlib
{
    namespace numpy_detail
    {
        namespace
        {
            [[nodiscard]] ValueTypeRef binding_for(const ValueTypeMetaData *meta)
            {
                const auto binding = ValuePlanFactory::instance().type_for(meta);
                if (binding == nullptr)
                {
                    throw std::logic_error("numpy operator output has no value binding");
                }
                return binding;
            }

            [[nodiscard]] std::size_t product(std::span<const std::size_t> shape)
            {
                std::size_t result = 1;
                for (const std::size_t dimension : shape)
                {
                    if (dimension != 0 && result > std::numeric_limits<std::size_t>::max() / dimension)
                    {
                        throw std::overflow_error("Array shape exceeds addressable storage");
                    }
                    result *= dimension;
                }
                return result;
            }

            void append_shape(const ValueView &value, std::vector<std::size_t> &shape)
            {
                if (!TypeRegistry::is_array(value.schema())) { return; }
                const auto indexed = value.as_list();
                shape.push_back(indexed.size());
                if (indexed.empty()) { return; }

                std::vector<std::size_t> child_shape;
                append_shape(indexed.front(), child_shape);
                for (std::size_t index = 1; index < indexed.size(); ++index)
                {
                    std::vector<std::size_t> candidate;
                    append_shape(indexed.at(index), candidate);
                    if (candidate != child_shape)
                    {
                        throw std::invalid_argument("Array value is not rectangular");
                    }
                }
                shape.insert(shape.end(), child_shape.begin(), child_shape.end());
            }

            template <typename T>
            void flatten(const ValueView &value, std::vector<T> &out)
            {
                if (!TypeRegistry::is_array(value.schema()))
                {
                    out.push_back(value.checked_as<T>());
                    return;
                }
                for (ValueView child : value.as_list()) { flatten<T>(child, out); }
            }

            template <typename T>
            [[nodiscard]] Value build_array_level(const ValueTypeMetaData *meta,
                                                  std::span<const std::size_t> shape,
                                                  std::span<const T> values,
                                                  std::size_t &offset)
            {
                if (!TypeRegistry::is_array(meta) || shape.empty())
                {
                    if (offset >= values.size())
                    {
                        throw std::invalid_argument("Array data does not fill its declared shape");
                    }
                    return Value{values[offset++]};
                }

                const std::size_t count = shape.front();
                if (meta->fixed_size != 0 && count > meta->fixed_size)
                {
                    throw std::invalid_argument("Array value exceeds its declared shape");
                }

                const auto binding = binding_for(meta);
                if (meta->fixed_size == 0)
                {
                    const auto element_binding = binding_for(meta->element_type);
                    ListBuilder builder{element_binding};
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        Value child = build_array_level<T>(meta->element_type,
                                                          shape.subspan(1), values, offset);
                        builder.push_back_copy(child.view().data());
                    }
                    ListStorage storage = builder.build_storage();
                    return Value{binding, &storage};
                }

                Value result{binding};
                auto output = result.as_list().begin_mutation();
                output.resize(count);
                for (std::size_t index = 0; index < count; ++index)
                {
                    Value child = build_array_level<T>(meta->element_type,
                                                      shape.subspan(1), values, offset);
                    output.at(index).copy_from(child.view());
                }
                return result;
            }

            template <typename T>
            [[nodiscard]] Value build_array(const ValueTypeMetaData *meta,
                                            std::span<const std::size_t> shape,
                                            std::span<const T> values)
            {
                if (TypeRegistry::array_element(meta) != scalar_descriptor<T>::value_meta())
                {
                    throw std::invalid_argument("Array output leaf type does not match its kernel");
                }
                if (product(shape) != values.size())
                {
                    throw std::invalid_argument("Array data size does not match its shape");
                }
                std::size_t offset = 0;
                Value result = build_array_level<T>(meta, shape, values, offset);
                if (offset != values.size())
                {
                    throw std::logic_error("Array builder did not consume all values");
                }
                return result;
            }

            template <typename T>
            [[nodiscard]] std::vector<std::vector<double>> variables(const ValueView &value,
                                                                      bool rowvar)
            {
                std::vector<std::size_t> shape;
                append_shape(value, shape);
                if (shape.empty() || shape.size() > 2)
                {
                    throw std::invalid_argument("corrcoef supports one- or two-dimensional arrays");
                }
                std::vector<T> flat;
                flat.reserve(product(shape));
                flatten<T>(value, flat);
                if (shape.size() == 1)
                {
                    std::vector<double> one;
                    one.reserve(flat.size());
                    for (const T item : flat) { one.push_back(static_cast<double>(item)); }
                    return {std::move(one)};
                }

                const std::size_t rows = shape[0];
                const std::size_t columns = shape[1];
                const std::size_t variable_count = rowvar ? rows : columns;
                const std::size_t observation_count = rowvar ? columns : rows;
                std::vector<std::vector<double>> result(
                    variable_count, std::vector<double>(observation_count));
                for (std::size_t row = 0; row < rows; ++row)
                {
                    for (std::size_t column = 0; column < columns; ++column)
                    {
                        const double item = static_cast<double>(flat[row * columns + column]);
                        if (rowvar) { result[row][column] = item; }
                        else { result[column][row] = item; }
                    }
                }
                return result;
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
                        "{}: Arrow compute failed: {}", operation,
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
                    "unsupported quantile method: " + std::string{method});
            }

            [[nodiscard]] Float numeric_array_scalar(const arrow::Datum &datum,
                                                     std::string_view operation)
            {
                if (!datum.is_array() || datum.array()->length != 1)
                {
                    throw std::runtime_error(fmt::format(
                        "{}: Arrow compute returned a non-scalar array", operation));
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
                            "{}: Arrow compute returned unexpected type {}", operation,
                            datum.type()->ToString()));
                }
            }

            [[nodiscard]] Float numeric_scalar(const arrow::Datum &datum,
                                               std::string_view operation)
            {
                if (!datum.is_scalar() || datum.type()->id() != arrow::Type::DOUBLE)
                {
                    throw std::runtime_error(fmt::format(
                        "{}: Arrow compute returned an unexpected result", operation));
                }
                const auto &scalar = datum.scalar_as<arrow::DoubleScalar>();
                return scalar.is_valid
                           ? scalar.value
                           : std::numeric_limits<Float>::quiet_NaN();
            }

            template <typename T>
            [[nodiscard]] constexpr T cumulative_add(T lhs, T rhs) noexcept
            {
                if constexpr (std::same_as<T, Int>)
                {
                    using Unsigned = std::make_unsigned_t<Int>;
                    return std::bit_cast<Int>(
                        std::bit_cast<Unsigned>(lhs) + std::bit_cast<Unsigned>(rhs));
                }
                else { return lhs + rhs; }
            }
        }  // namespace

        bool numeric_array(const ValueTypeMetaData *meta,
                           const ValueTypeMetaData *element) noexcept
        {
            return TypeRegistry::is_array(meta) &&
                   TypeRegistry::array_element(meta) == element;
        }

        std::vector<Int> index_components(const ValueView &index)
        {
            if (const auto *single = index.try_as<Int>()) { return {*single}; }
            if (!index.is_list() && !index.is_tuple())
            {
                throw std::invalid_argument("Array index must be an int or tuple of ints");
            }
            std::vector<Int> result;
            for (const ValueView item : index.as_indexed_view())
            {
                result.push_back(item.checked_as<Int>());
            }
            return result;
        }

        const ValueTypeMetaData *indexed_result(const ValueTypeMetaData *array,
                                                std::size_t components)
        {
            const ValueTypeMetaData *result = array;
            for (std::size_t index = 0; index < components; ++index)
            {
                if (!TypeRegistry::is_array(result)) { return nullptr; }
                result = result->element_type;
            }
            return result;
        }

        ValueView index_value(ValueView array, std::span<const Int> components)
        {
            ValueView result = std::move(array);
            for (const Int requested : components)
            {
                if (!TypeRegistry::is_array(result.schema()))
                {
                    throw std::out_of_range("Array index has too many components");
                }
                const auto values = result.as_list();
                const Int size = static_cast<Int>(values.size());
                const Int normalized = requested < 0 ? size + requested : requested;
                if (normalized < 0 || normalized >= size)
                {
                    throw std::out_of_range("Array index is out of range");
                }
                const ValueView child = values.at(static_cast<std::size_t>(normalized));
                result = ValueView{child.binding(), child.data()};
            }
            return result;
        }

        Value array_from_window(const TSWInputView &window,
                                std::optional<ValueView> zero,
                                const ValueTypeMetaData *output)
        {
            const std::size_t size = output->fixed_size;
            const auto binding = binding_for(output);
            if (size == 0)
            {
                ListBuilder builder{binding_for(output->element_type)};
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    builder.push_back_copy(window.at(index).data());
                }
                ListStorage storage = builder.build_storage();
                return Value{binding, &storage};
            }
            Value result{binding};
            auto values = result.as_list().begin_mutation();
            values.resize(size);
            for (std::size_t index = 0; index < window.size(); ++index)
            {
                values.at(index).copy_from(window.at(index));
            }
            if (window.size() < size)
            {
                Value default_zero;
                ValueView fill;
                if (zero.has_value())
                {
                    fill = ValueView{zero->binding(), zero->data()};
                }
                else
                {
                    default_zero = Value{binding_for(output->element_type)};
                    fill = default_zero.view();
                }
                for (std::size_t index = window.size(); index < size; ++index)
                {
                    values.at(index).copy_from(fill);
                }
            }
            return result;
        }

        Value array_from_window_times(const TSWInputView &window,
                                      const ValueTypeMetaData *output)
        {
            const auto binding = binding_for(output);
            if (output->fixed_size == 0)
            {
                ListBuilder builder{binding_for(output->element_type)};
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    const DateTime time = window.time_at(index);
                    builder.push_back_copy(&time);
                }
                ListStorage storage = builder.build_storage();
                return Value{binding, &storage};
            }
            if (window.size() != output->fixed_size)
            {
                throw std::invalid_argument(
                    "rolling window timestamps do not match the declared shape");
            }
            Value result{binding};
            auto values = result.as_list().begin_mutation();
            values.resize(output->fixed_size);
            for (std::size_t index = 0; index < window.size(); ++index)
            {
                values.at(index).checked_mutable_as<DateTime>() = window.time_at(index);
            }
            return result;
        }

        bool window_ready(const TSWInputView &window)
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

        template <typename T>
        Value cumulative_sum(ValueView input, const ValueTypeMetaData *output,
                             std::optional<Int> axis)
        {
            std::vector<std::size_t> shape;
            append_shape(input, shape);
            std::vector<T> values;
            values.reserve(product(shape));
            flatten<T>(input, values);
            if (axis.has_value())
            {
                Int normalized = *axis;
                if (normalized < 0) { normalized += static_cast<Int>(shape.size()); }
                if (normalized < 0 || normalized >= static_cast<Int>(shape.size()))
                {
                    throw std::out_of_range("cumsum axis is out of range");
                }
                const std::size_t axis_index = static_cast<std::size_t>(normalized);
                const std::size_t inner = product(std::span{shape}.subspan(axis_index + 1));
                const std::size_t axis_size = shape[axis_index];
                const std::size_t outer = product(std::span{shape}.first(axis_index));
                for (std::size_t block = 0; block < outer; ++block)
                {
                    const std::size_t base = block * axis_size * inner;
                    for (std::size_t offset = 0; offset < inner; ++offset)
                    {
                        for (std::size_t item = 1; item < axis_size; ++item)
                        {
                            const std::size_t current = base + item * inner + offset;
                            values[current] = cumulative_add(
                                values[current - inner], values[current]);
                        }
                    }
                }
            }
            else
            {
                for (std::size_t index = 1; index < values.size(); ++index)
                {
                    values[index] = cumulative_add(values[index - 1], values[index]);
                }
                shape = {values.size()};
            }
            return build_array<T>(output, shape, values);
        }

        template <typename T>
        Value correlation(ValueView x, std::optional<ValueView> y, bool rowvar,
                          const ValueTypeMetaData *output)
        {
            auto series = variables<T>(x, rowvar);
            if (y.has_value())
            {
                auto additional = variables<T>(*y, rowvar);
                if (!series.empty() && !additional.empty() &&
                    series.front().size() != additional.front().size())
                {
                    throw std::invalid_argument("corrcoef inputs have different observation counts");
                }
                series.insert(series.end(), std::make_move_iterator(additional.begin()),
                              std::make_move_iterator(additional.end()));
            }
            const std::size_t count = series.size();
            for (const auto &variable : series)
            {
                if (variable.empty())
                {
                    throw std::invalid_argument("corrcoef requires at least one observation");
                }
            }

            if (series.size() == 1 && !TypeRegistry::is_array(output))
            {
                return Value{Float{boost::math::statistics::correlation_coefficient(
                    series.front(), series.front())}};
            }

            std::vector<Float> result(count * count);
            for (std::size_t row = 0; row < count; ++row)
            {
                for (std::size_t column = row; column < count; ++column)
                {
                    const Float coefficient =
                        boost::math::statistics::correlation_coefficient(
                            series[row], series[column]);
                    result[row * count + column] = coefficient;
                    result[column * count + row] = coefficient;
                }
            }
            const std::array<std::size_t, 2> shape{count, count};
            return build_array<Float>(output, shape, result);
        }

        void resolve_correlation_output(ResolutionMap &resolution,
                                        OperatorCallContext context,
                                        bool has_y, bool rowvar)
        {
            if (output_bound(resolution)) { return; }
            const auto x = TypeRegistry::array_dimensions(ts_value_schema_at(context, 0));
            if (x.empty() || x.size() > 2) { return; }
            if (!has_y && x.size() == 1)
            {
                bind_output(resolution, TypeRegistry::instance().ts(
                    scalar_descriptor<Float>::value_meta()));
                return;
            }
            const auto variables = [rowvar](const std::vector<std::size_t> &shape) {
                return shape.size() == 1 ? std::size_t{1}
                                         : (rowvar ? shape[0] : shape[1]);
            };
            std::size_t count = variables(x);
            if (has_y)
            {
                const auto y = TypeRegistry::array_dimensions(ts_value_schema_at(context, 1));
                if (y.empty() || y.size() > 2) { return; }
                count += variables(y);
            }
            const std::array<std::size_t, 2> shape{count, count};
            bind_output(resolution, TypeRegistry::instance().ts(
                TypeRegistry::instance().array(scalar_descriptor<Float>::value_meta(), shape)));
        }

        template <typename T>
        Float quantile(ValueView input, Float q, std::string_view method)
        {
            std::vector<T> raw;
            flatten<T>(input, raw);
            if (raw.empty()) { throw std::invalid_argument("quantile requires at least one value"); }
            if (!(q >= 0.0 && q <= 1.0))
            {
                throw std::invalid_argument("quantile q must be in [0, 1]");
            }
            const arrow::compute::QuantileOptions options{
                q, quantile_interpolation(method), false, 1};
            return numeric_array_scalar(
                checked_arrow_result(
                    arrow::compute::Quantile(numeric_datum(raw), options), "quantile"),
                "quantile");
        }

        template <typename T>
        Float quantile(const TSWInputView &input, Float q, std::string_view method)
        {
            std::vector<T> values;
            values.reserve(input.size());
            for (std::size_t index = 0; index < input.size(); ++index)
            {
                values.push_back(input.at(index).checked_as<T>());
            }
            if (values.empty()) { throw std::invalid_argument("quantile requires at least one value"); }
            if (!(q >= 0.0 && q <= 1.0))
            {
                throw std::invalid_argument("quantile q must be in [0, 1]");
            }
            const arrow::compute::QuantileOptions options{
                q, quantile_interpolation(method), false, 1};
            return numeric_array_scalar(
                checked_arrow_result(
                    arrow::compute::Quantile(numeric_datum(values), options), "quantile"),
                "quantile");
        }

        template <typename T>
        Float standard_deviation(ValueView input, Int ddof)
        {
            std::vector<T> raw;
            flatten<T>(input, raw);
            if (ddof < std::numeric_limits<int>::min() ||
                ddof > std::numeric_limits<int>::max())
            {
                throw std::invalid_argument("standard deviation ddof is outside Arrow's supported range");
            }
            const arrow::compute::VarianceOptions options{
                static_cast<int>(ddof), false, 0};
            return numeric_scalar(
                checked_arrow_result(
                    arrow::compute::Stddev(numeric_datum(raw), options),
                    "standard_deviation"),
                "standard_deviation");
        }

        template Value cumulative_sum<Int>(ValueView, const ValueTypeMetaData *,
                                           std::optional<Int>);
        template Value cumulative_sum<Float>(ValueView, const ValueTypeMetaData *,
                                             std::optional<Int>);
        template Value correlation<Int>(ValueView, std::optional<ValueView>, bool,
                                        const ValueTypeMetaData *);
        template Value correlation<Float>(ValueView, std::optional<ValueView>, bool,
                                          const ValueTypeMetaData *);
        template Float quantile<Int>(ValueView, Float, std::string_view);
        template Float quantile<Float>(ValueView, Float, std::string_view);
        template Float quantile<Int>(const TSWInputView &, Float, std::string_view);
        template Float quantile<Float>(const TSWInputView &, Float, std::string_view);
        template Float standard_deviation<Int>(ValueView, Int);
        template Float standard_deviation<Float>(ValueView, Int);
    }  // namespace numpy_detail

    bool array_get_item_impl::requires_(const ResolutionMap &,
                                        OperatorCallContext context)
    {
        if (!TypeRegistry::is_array(ts_value_schema_at(context, 0))) { return false; }
        const auto *index = scalar_arg_at(context, 1);
        if (index == nullptr) { return false; }
        const auto *meta = index->scalar_value.schema();
        if (meta == scalar_descriptor<Int>::value_meta()) { return true; }
        const auto kind = meta != nullptr ? meta->try_value_kind() : std::nullopt;
        if (kind != ValueTypeKind::List && kind != ValueTypeKind::Tuple) { return false; }
        const auto value = index->scalar_value.view();
        const auto items = value.as_indexed_view();
        for (const ValueView item : items)
        {
            if (item.schema() != scalar_descriptor<Int>::value_meta()) { return false; }
        }
        return true;
    }

    void array_get_item_impl::resolve_default_types(ResolutionMap &resolution,
                                                    OperatorCallContext context)
    {
        if (output_bound(resolution)) { return; }
        const auto *array = ts_value_schema_at(context, 0);
        const auto *index = scalar_arg_at(context, 1);
        if (!TypeRegistry::is_array(array) || index == nullptr) { return; }
        const auto components = numpy_detail::index_components(index->scalar_value.view());
        const auto *result = numpy_detail::indexed_result(array, components.size());
        if (result != nullptr)
        {
            bind_output(resolution, TypeRegistry::instance().ts(result));
        }
    }

    void array_get_item_impl::eval(In<"ts", TsVar<"A">> ts,
                                   Scalar<"idx", ScalarVar<"I">> index,
                                   Out<TsVar<"__out__">> out)
    {
        const auto components = numpy_detail::index_components(index.value());
        out.apply(numpy_detail::index_value(ts.value(), components));
    }

    bool as_array_erased_impl<false>::requires_(const ResolutionMap &,
                                                OperatorCallContext context)
    {
        const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
        return window != nullptr && !window->is_duration_based() &&
               window->period() != 0;
    }

    void as_array_erased_impl<false>::resolve_default_types(
        ResolutionMap &resolution, OperatorCallContext context)
    {
        if (output_bound(resolution)) { return; }
        const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
        if (window == nullptr || window->is_duration_based()) { return; }
        bind_output(resolution, TypeRegistry::instance().ts(
            TypeRegistry::instance().array(window->value_schema->element_type,
                                           window->period())));
    }

    void as_array_erased_impl<false>::eval(In<"tsw", TsVar<"W">> tsw,
                                           Out<TsVar<"__out__">> out)
    {
        const TSWInputView window{tsw.base().borrowed_ref()};
        if (window.size() < window.min_period()) { return; }
        Value value = numpy_detail::array_from_window(
            window, std::nullopt,
            static_cast<const TSOutputView &>(out).schema()->value_schema);
        out.apply(value.view());
    }

    bool as_array_erased_impl<true>::requires_(const ResolutionMap &,
                                               OperatorCallContext context)
    {
        const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
        const auto *zero = time_series_schema_at(context, 1);
        return window != nullptr && !window->is_duration_based() &&
               window->period() != 0 && zero != nullptr && zero->kind == TSTypeKind::TS &&
               zero->value_schema == window->value_schema->element_type;
    }

    void as_array_erased_impl<true>::resolve_default_types(
        ResolutionMap &resolution, OperatorCallContext context)
    {
        as_array_erased_impl<false>::resolve_default_types(resolution, context);
    }

    void as_array_erased_impl<true>::eval(In<"tsw", TsVar<"W">> tsw,
                                          In<"zero", TsVar<"Z">> zero,
                                          Out<TsVar<"__out__">> out)
    {
        const TSWInputView window{tsw.base().borrowed_ref()};
        if (window.size() < window.min_period()) { return; }
        Value value = numpy_detail::array_from_window(
            window, zero.value(),
            static_cast<const TSOutputView &>(out).schema()->value_schema);
        out.apply(value.view());
    }

    bool as_array_scalar_zero_impl::requires_(const ResolutionMap &,
                                              OperatorCallContext context)
    {
        const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
        const auto *zero = scalar_arg_at(context, 1);
        return window != nullptr && !window->is_duration_based() &&
               window->period() != 0 && zero != nullptr &&
               zero->scalar_value.schema() == window->value_schema->element_type;
    }

    void as_array_scalar_zero_impl::resolve_default_types(
        ResolutionMap &resolution, OperatorCallContext context)
    {
        as_array_erased_impl<false>::resolve_default_types(resolution, context);
    }

    void as_array_scalar_zero_impl::eval(In<"tsw", TsVar<"W">> tsw,
                                         Scalar<"zero", ScalarVar<"Z">> zero,
                                         Out<TsVar<"__out__">> out)
    {
        const TSWInputView window{tsw.base().borrowed_ref()};
        if (window.size() < window.min_period()) { return; }
        Value value = numpy_detail::array_from_window(
            window, zero.value(),
            static_cast<const TSOutputView &>(out).schema()->value_schema);
        out.apply(value.view());
    }

    bool rolling_window_arrays_impl::requires_(const ResolutionMap &,
                                               OperatorCallContext context)
    {
        const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
        return window != nullptr && !window->is_duration_based() &&
               window->period() != 0;
    }

    void rolling_window_arrays_impl::resolve_default_types(
        ResolutionMap &resolution, OperatorCallContext context)
    {
        if (output_bound(resolution)) { return; }
        const auto *window = time_series_schema_at_as<AnyTSW>(context, 0);
        if (window == nullptr || window->is_duration_based() || window->period() == 0) { return; }
        auto &registry = TypeRegistry::instance();
        const std::size_t dimension = window->period() == window->min_period()
                                          ? window->period()
                                          : std::size_t{0};
        const auto *buffer = registry.array(window->value_schema->element_type, dimension);
        const auto *index = registry.array(scalar_descriptor<DateTime>::value_meta(), dimension);
        const std::string name = window->period() == window->min_period()
                                     ? fmt::format("NpRollingWindowResult[{},{}]",
                                                   window->value_schema->element_type->name(),
                                                   window->period())
                                     : fmt::format("NpRollingWindowResult[{},{},{}]",
                                                   window->value_schema->element_type->name(),
                                                   window->period(), window->min_period());
        bind_output(resolution, registry.tsb(
            name, {{"buffer", registry.ts(buffer)}, {"index", registry.ts(index)}}));
    }

    void rolling_window_arrays_impl::eval(In<"window", TsVar<"W">> input,
                                          Out<TsVar<"__out__">> out)
    {
        const TSWInputView window{input.base().borrowed_ref()};
        if (!numpy_detail::window_ready(window)) { return; }
        const auto &erased = static_cast<const TSOutputView &>(out);
        auto bundle = erased.as_bundle();
        Value buffer = numpy_detail::array_from_window(
            window, std::nullopt, bundle.field("buffer").schema()->value_schema);
        Value index = numpy_detail::array_from_window_times(
            window, bundle.field("index").schema()->value_schema);
        {
            auto field = bundle.field("buffer");
            auto mutation = field.begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(buffer)));
        }
        {
            auto field = bundle.field("index");
            auto mutation = field.begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(index)));
        }
    }

    template <typename T>
    bool corrcoef_impl<T, false>::requires_(const ResolutionMap &,
                                            OperatorCallContext context)
    {
        const auto *array = ts_value_schema_at(context, 0);
        const auto dimensions = TypeRegistry::array_dimensions(array);
        return numpy_detail::numeric_array(array, scalar_descriptor<T>::value_meta()) &&
               (dimensions.size() == 1 || dimensions.size() == 2) &&
               scalar_arg_at(context, 1) != nullptr;
    }

    template <typename T>
    bool corrcoef_default_impl<T, false>::requires_(const ResolutionMap &,
                                                    OperatorCallContext context)
    {
        const auto *array = ts_value_schema_at(context, 0);
        const auto dimensions = TypeRegistry::array_dimensions(array);
        return numpy_detail::numeric_array(array, scalar_descriptor<T>::value_meta()) &&
               (dimensions.size() == 1 || dimensions.size() == 2);
    }

    template <typename T>
    bool corrcoef_default_impl<T, true>::requires_(const ResolutionMap &,
                                                   OperatorCallContext context)
    {
        const auto *x = ts_value_schema_at(context, 0);
        const auto *y = ts_value_schema_at(context, 1);
        const auto x_dimensions = TypeRegistry::array_dimensions(x);
        const auto y_dimensions = TypeRegistry::array_dimensions(y);
        return numpy_detail::numeric_array(x, scalar_descriptor<T>::value_meta()) &&
               numpy_detail::numeric_array(y, scalar_descriptor<T>::value_meta()) &&
               (x_dimensions.size() == 1 || x_dimensions.size() == 2) &&
               (y_dimensions.size() == 1 || y_dimensions.size() == 2);
    }

    template <typename T>
    void corrcoef_impl<T, false>::resolve_default_types(ResolutionMap &resolution,
                                                        OperatorCallContext context)
    {
        if (output_bound(resolution)) { return; }
        const auto *array = ts_value_schema_at(context, 0);
        const auto dimensions = TypeRegistry::array_dimensions(array);
        const auto *rowvar = context.scalar_as<Bool>("rowvar");
        if (dimensions.empty() || dimensions.size() > 2 || rowvar == nullptr) { return; }
        if (dimensions.size() == 1)
        {
            bind_output(resolution, TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()));
            return;
        }
        const std::size_t count = *rowvar ? dimensions[0] : dimensions[1];
        const std::array<std::size_t, 2> shape{count, count};
        bind_output(resolution, TypeRegistry::instance().ts(
            TypeRegistry::instance().array(scalar_descriptor<Float>::value_meta(), shape)));
    }

    template <typename T>
    bool corrcoef_impl<T, true>::requires_(const ResolutionMap &,
                                           OperatorCallContext context)
    {
        const auto *x = ts_value_schema_at(context, 0);
        const auto *y = ts_value_schema_at(context, 1);
        const auto x_dimensions = TypeRegistry::array_dimensions(x);
        const auto y_dimensions = TypeRegistry::array_dimensions(y);
        return numpy_detail::numeric_array(x, scalar_descriptor<T>::value_meta()) &&
               numpy_detail::numeric_array(y, scalar_descriptor<T>::value_meta()) &&
               (x_dimensions.size() == 1 || x_dimensions.size() == 2) &&
               (y_dimensions.size() == 1 || y_dimensions.size() == 2) &&
               scalar_arg_at(context, 2) != nullptr;
    }

    template <typename T>
    void corrcoef_impl<T, true>::resolve_default_types(ResolutionMap &resolution,
                                                       OperatorCallContext context)
    {
        if (output_bound(resolution)) { return; }
        const auto x = TypeRegistry::array_dimensions(ts_value_schema_at(context, 0));
        const auto y = TypeRegistry::array_dimensions(ts_value_schema_at(context, 1));
        const auto *rowvar = context.scalar_as<Bool>("rowvar");
        if (x.empty() || y.empty() || rowvar == nullptr) { return; }
        const auto variables = [rowvar](const std::vector<std::size_t> &shape) {
            return shape.size() == 1 ? std::size_t{1} : (*rowvar ? shape[0] : shape[1]);
        };
        const std::size_t count = variables(x) + variables(y);
        const std::array<std::size_t, 2> shape{count, count};
        bind_output(resolution, TypeRegistry::instance().ts(
            TypeRegistry::instance().array(scalar_descriptor<Float>::value_meta(), shape)));
    }

    void register_numpy_operators()
    {
        static const bool arrow_compute_initialised = [] {
            const auto status = arrow::compute::Initialize();
            if (!status.ok())
            {
                throw std::runtime_error(
                    "numpy operators: Arrow compute initialization failed: " +
                    status.ToString());
            }
            return true;
        }();
        static_cast<void>(arrow_compute_initialised);

        register_overload<numpy::as_array, as_array_erased_impl<false>>();
        register_overload<numpy::as_array, as_array_erased_impl<true>>();
        register_overload<numpy::as_array, as_array_scalar_zero_impl>();
        register_overload<numpy::get_item, array_get_item_impl>();
        register_overload<numpy::cumsum, cumsum_impl<Int, false>>();
        register_overload<numpy::cumsum, cumsum_impl<Float, false>>();
        register_overload<numpy::cumsum, cumsum_impl<Int, true>>();
        register_overload<numpy::cumsum, cumsum_impl<Float, true>>();
        register_overload<numpy::corrcoef, corrcoef_default_impl<Int, false>>();
        register_overload<numpy::corrcoef, corrcoef_default_impl<Float, false>>();
        register_overload<numpy::corrcoef, corrcoef_default_impl<Int, true>>();
        register_overload<numpy::corrcoef, corrcoef_default_impl<Float, true>>();
        register_overload<numpy::corrcoef, corrcoef_impl<Int, false>>();
        register_overload<numpy::corrcoef, corrcoef_impl<Float, false>>();
        register_overload<numpy::corrcoef, corrcoef_impl<Int, true>>();
        register_overload<numpy::corrcoef, corrcoef_impl<Float, true>>();
        register_overload<numpy::quantile, quantile_array_impl<Int>>();
        register_overload<numpy::quantile, quantile_array_impl<Float>>();
        register_overload<numpy::quantile, quantile_window_impl<Int>>();
        register_overload<numpy::quantile, quantile_window_impl<Float>>();
        register_overload<numpy::quantile, quantile_array_default_impl<Int, false>>();
        register_overload<numpy::quantile, quantile_array_default_impl<Float, false>>();
        register_overload<numpy::quantile, quantile_array_default_impl<Int, true>>();
        register_overload<numpy::quantile, quantile_array_default_impl<Float, true>>();
        register_overload<numpy::quantile, quantile_array_keepdims_impl<Int>>();
        register_overload<numpy::quantile, quantile_array_keepdims_impl<Float>>();
        register_overload<numpy::quantile, quantile_window_default_impl<Int, false>>();
        register_overload<numpy::quantile, quantile_window_default_impl<Float, false>>();
        register_overload<numpy::quantile, quantile_window_default_impl<Int, true>>();
        register_overload<numpy::quantile, quantile_window_default_impl<Float, true>>();
        register_overload<numpy::quantile, quantile_window_keepdims_impl<Int>>();
        register_overload<numpy::quantile, quantile_window_keepdims_impl<Float>>();
        register_overload<numpy::rolling_window_arrays, rolling_window_arrays_impl>();
        register_overload<numpy::standard_deviation, standard_deviation_impl<Int, false>>();
        register_overload<numpy::standard_deviation, standard_deviation_impl<Float, false>>();
        register_overload<numpy::standard_deviation, standard_deviation_impl<Int, true>>();
        register_overload<numpy::standard_deviation, standard_deviation_impl<Float, true>>();
    }
}  // namespace hgraph::stdlib
