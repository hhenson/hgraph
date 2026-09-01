#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/series.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value.h>

#include <arrow/array.h>
#include <arrow/array/util.h>
#include <arrow/compute/api.h>
#include <arrow/scalar.h>

#include <stdexcept>
#include <string>

namespace hgraph::stdlib::series_impl_detail
{
    struct FrameRowState
    {
        const TableConverter *converter{nullptr};
    };
}

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<stdlib::series_impl_detail::FrameRowState>
    {
        static constexpr std::string_view value{"stdlib.frame_row_state"};
    };
}

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace series_impl_detail
    {
        /** Any Series scalar (base or parameterised Series[T]). */
        [[nodiscard]] inline bool is_series_value(const ValueTypeMetaData *value)
        {
            return TypeRegistry::instance().is_series(value);
        }

        [[nodiscard]] inline bool is_series_arg(OperatorCallContext context, std::size_t index)
        {
            const auto *schema = time_series_schema_at_as<AnyTS>(context, index);
            return schema != nullptr && is_series_value(schema->value_schema);
        }

        /** The scalar element of one operand: a Series' element, or a plain
            scalar operand's own type (int/float). */
        [[nodiscard]] inline const ValueTypeMetaData *operand_element(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr || schema->kind != TSTypeKind::TS) { return nullptr; }
            const auto *value = schema->value_schema;
            return is_series_value(value) ? value->element_type : value;
        }

        /** Arrow arithmetic promotion (hgraph parity): any float -> float. */
        [[nodiscard]] inline const ValueTypeMetaData *promote(const ValueTypeMetaData *lhs,
                                                              const ValueTypeMetaData *rhs)
        {
            auto &registry = TypeRegistry::instance();
            const auto *flt = registry.value_type("float");
            return (lhs == flt || rhs == flt) ? flt : registry.value_type("int");
        }

        /** A ``arrow::Datum`` for one operand: the Series' array, or a scalar
            built from an ``int``/``float`` TS value. Discriminates by the
            value's OWN ops table (``try_as`` is an ops-pointer compare — all
            ``Series[T]`` metas share the base series ops), so the per-tick
            path never consults the registry (lock-free per-tick ruling). */
        [[nodiscard]] inline arrow::Datum operand_datum(const TSInputView &input)
        {
            const auto value = input.value();
            if (const auto *series = value.try_as<Series>())
            {
                return arrow::Datum{series->array};
            }
            if (const auto *as_int = value.try_as<Int>())
            {
                return arrow::Datum{arrow::MakeScalar(static_cast<std::int64_t>(*as_int))};
            }
            if (const auto *as_float = value.try_as<Float>())
            {
                return arrow::Datum{arrow::MakeScalar(*as_float)};
            }
            throw std::invalid_argument("series operator operand must be a Series, int or float");
        }

        [[nodiscard]] inline Series call_binary(const char *fn, const arrow::Datum &lhs, const arrow::Datum &rhs)
        {
            auto result = arrow::compute::CallFunction(fn, {lhs, rhs});
            if (!result.ok())
            {
                throw std::runtime_error(std::string{"arrow "} + fn + " failed: " + result.status().ToString());
            }
            return Series{.array = result->make_array()};
        }

        [[nodiscard]] inline arrow::Datum to_float(const arrow::Datum &value)
        {
            auto cast = arrow::compute::Cast(value, arrow::float64());
            if (!cast.ok()) { throw std::runtime_error("arrow cast to float failed: " + cast.status().ToString()); }
            return *cast;
        }

        /** Publish an owned erased value without a registry lookup. */
        inline void publish_value(Value result, const TSOutputView &erased)
        {
            if (!result.has_value()) { return; }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }

        /** Read ``series[index]`` using the bound element codec. Arrow nulls
            do not tick, matching a Python compute node returning ``None``. */
        inline void publish_element(const Series &series, Int index, const TSOutputView &erased)
        {
            if (!series.has_value()) { throw std::invalid_argument("cannot index an empty Series"); }
            const Int length = static_cast<Int>(series.array->length());
            index = index < 0 ? length + index : index;
            if (index < 0 || index >= length)
            {
                throw std::out_of_range("Series index out of range");
            }
            publish_value(array_cell(*series.array, erased.schema()->value_schema, index), erased);
        }

        [[nodiscard]] inline const ValueTypeMetaData *frame_meta(OperatorCallContext context)
        {
            const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
            if (schema == nullptr || !TypeRegistry::instance().is_frame(schema->value_schema)) { return nullptr; }
            return schema->value_schema->element_type != nullptr ? schema->value_schema : nullptr;
        }

        [[nodiscard]] inline const ValueTypeMetaData *frame_field(const ValueTypeMetaData *frame,
                                                                  std::string_view name)
        {
            if (frame == nullptr || frame->element_type == nullptr) { return nullptr; }
            const auto *row = frame->element_type;
            for (std::size_t index = 0; index < row->field_count; ++index)
            {
                if (row->fields[index].name != nullptr && name == row->fields[index].name)
                {
                    return row->fields[index].type;
                }
            }
            return nullptr;
        }
    }  // namespace series_impl_detail

    /** Elementwise Series arithmetic via arrow compute. ``Div`` is TRUE
        division (int/int -> float, hgraph semantics), so both operands cast
        to float first. Handles Series (+) Series and Series (+) scalar (and
        scalar first) - at least one operand is a Series. */
    template <fixed_string FnName, bool Div, fixed_string DisplayName = FnName>
    struct series_binary_impl
    {
        // Diagnostic name identifies the SERIES specialization; FnName stays
        // the arrow compute function it invokes.
        static constexpr const char *name = DisplayName.value;

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return series_impl_detail::is_series_arg(context, 0) || series_impl_detail::is_series_arg(context, 1);
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            if (!series_impl_detail::is_series_arg(context, 0) && !series_impl_detail::is_series_arg(context, 1))
            {
                return;
            }
            auto       &registry = TypeRegistry::instance();
            const auto *lhs      = series_impl_detail::operand_element(time_series_schema_at(context, 0));
            const auto *rhs      = series_impl_detail::operand_element(time_series_schema_at(context, 1));
            const auto *element  = Div ? registry.value_type("float") : series_impl_detail::promote(lhs, rhs);
            bind_output(resolution, registry.ts(registry.series(element)));
        }

        static void eval(In<"lhs", TsVar<"L">> lhs, In<"rhs", TsVar<"R">> rhs, Out<TsVar<"__out__">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        lhs_d  = series_impl_detail::operand_datum(lhs);
            auto        rhs_d  = series_impl_detail::operand_datum(rhs);
            Series      result = Div
                                     ? series_impl_detail::call_binary("divide", series_impl_detail::to_float(lhs_d),
                                                                       series_impl_detail::to_float(rhs_d))
                                     : series_impl_detail::call_binary(FnName.value, lhs_d, rhs_d);
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(Value{std::move(result)}));
        }
    };

    /** Reduce a Series to its non-null minimum or maximum. Empty and all-null
        arrays do not tick, matching the Python compute-node contract. */
    template <bool Min>
    struct series_extremum_impl
    {
        static constexpr auto name = Min ? "min_series" : "max_series";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            if (!series_impl_detail::is_series_arg(context, 0)) { return false; }
            const auto *element = series_impl_detail::operand_element(time_series_schema_at(context, 0));
            const auto *requested = resolution.find_ts("O");
            return element != nullptr &&
                   (requested == nullptr || requested == TypeRegistry::instance().ts(element));
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (local_output_bound(resolution, "O")) { return; }
            const auto *element = series_impl_detail::operand_element(time_series_schema_at(context, 0));
            if (element != nullptr)
            {
                bind_local_output(resolution, TypeRegistry::instance().ts(element), "O");
            }
        }

        static void eval(In<"ts", TS<SeriesOf<ScalarVar<"S">>>> ts, Out<TsVar<"O">> out)
        {
            const Series series = ts.base().value().checked_as<Series>();
            if (!series.has_value()) { throw std::invalid_argument("cannot reduce an empty Series"); }
            auto reduced = arrow::compute::CallFunction(Min ? "min" : "max", {arrow::Datum{series.array}});
            if (!reduced.ok())
            {
                throw std::runtime_error(std::string{"arrow Series reduction failed: "} +
                                         reduced.status().ToString());
            }
            const auto scalar = reduced->scalar();
            if (scalar == nullptr || !scalar->is_valid) { return; }
            auto array = arrow::MakeArrayFromScalar(*scalar, 1);
            if (!array.ok())
            {
                throw std::runtime_error(std::string{"Arrow Series reduction conversion failed: "} +
                                         array.status().ToString());
            }
            const auto &erased = static_cast<const TSOutputView &>(out);
            series_impl_detail::publish_value(array_cell(**array, erased.schema()->value_schema, 0), erased);
        }
    };

    /** Project a declared Frame column as Series[T]. The name is a fixed
        scalar, while the frame value may carry its columns in any order. */
    template <typename FrameScalar, fixed_string KeyName, fixed_string NodeName>
    struct frame_column_impl
    {
        static constexpr const char *name = NodeName.value;

        [[nodiscard]] static const ValueTypeMetaData *field(OperatorCallContext context)
        {
            const auto *key = context.scalar_as<Str>(KeyName.sv());
            return key != nullptr
                       ? series_impl_detail::frame_field(series_impl_detail::frame_meta(context), *key)
                       : nullptr;
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *element = field(context);
            if (element == nullptr) { return false; }
            const auto *requested = resolution.find_ts("O");
            const auto *expected = TypeRegistry::instance().ts(TypeRegistry::instance().series(element));
            return requested == nullptr || requested == expected;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (local_output_bound(resolution, "O")) { return; }
            const auto *element = field(context);
            if (element != nullptr)
            {
                bind_local_output(
                    resolution, TypeRegistry::instance().ts(TypeRegistry::instance().series(element)), "O");
            }
        }

        static void eval(In<"ts", TS<FrameScalar>> ts, Scalar<KeyName, Str> key,
                         Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *series = erased.schema()->value_schema;
            Series result = frame_column(ts.base().value().template checked_as<Frame>(), key.value(),
                                         series->element_type);
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(Value{std::move(result)}));
        }
    };

    /** Select one Frame row and reconstruct the declared row scalar. KeyParam
        is either a fixed ``Scalar<key, int>`` or a live ``In<key, TS[int]>``. */
    template <typename FrameScalar, typename KeyParam, fixed_string NodeName>
    struct frame_row_impl
    {
        static constexpr const char *name = NodeName.value;

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
        {
            const auto *frame = series_impl_detail::frame_meta(context);
            const auto *requested = resolution.find_ts("O");
            return frame != nullptr &&
                   (requested == nullptr || requested == TypeRegistry::instance().ts(frame->element_type));
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (local_output_bound(resolution, "O")) { return; }
            const auto *frame = series_impl_detail::frame_meta(context);
            if (frame != nullptr)
            {
                bind_local_output(resolution, TypeRegistry::instance().ts(frame->element_type), "O");
            }
        }

        static void start(State<series_impl_detail::FrameRowState> state, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            state.set(series_impl_detail::FrameRowState{
                &table_converter(erased.schema()->value_schema)});
        }

        static void eval(In<"ts", TS<FrameScalar>> ts, KeyParam key,
                         State<series_impl_detail::FrameRowState> state, Out<TsVar<"O">> out)
        {
            const Frame frame = ts.base().value().template checked_as<Frame>();
            const Int size = static_cast<Int>(frame_rows(frame));
            const Int index = key.value() < 0 ? size + key.value() : key.value();
            Value row = read_row(*state.get().converter, frame, index);
            out.apply(row.view());
        }
    };

    /** getitem_(series, index): the element at ``index`` (a scalar TS). */
    struct series_getitem_impl
    {
        static constexpr auto name = "getitem_series";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return series_impl_detail::is_series_arg(context, 0) &&
                   time_series_arg_matches<AnyTS>(context, 1);
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (local_output_bound(resolution, "O")) { return; }
            const auto *schema  = time_series_schema_at(context, 0);
            const auto *element = series_impl_detail::operand_element(schema);
            if (element == nullptr) { return; }   // element-untyped Series: caller must declare TS[T]
            bind_local_output(resolution, TypeRegistry::instance().ts(element), "O");
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, In<"key", TS<Int>> key, Out<TsVar<"O">> out)
        {
            series_impl_detail::publish_element(ts.base().value().checked_as<Series>(), key.value(),
                                                static_cast<const TSOutputView &>(out));
        }
    };

    /** getitem_(series, index) with a SCALAR index (ts[i] where i is a plain
        int param). */
    struct series_getitem_scalar_impl
    {
        static constexpr auto name = "getitem_series_scalar";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return series_impl_detail::is_series_arg(context, 0) && scalar_arg_at(context, 1) != nullptr;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (local_output_bound(resolution, "O")) { return; }
            const auto *element = series_impl_detail::operand_element(time_series_schema_at(context, 0));
            if (element == nullptr) { return; }
            bind_local_output(resolution, TypeRegistry::instance().ts(element), "O");
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, Scalar<"key", Int> key, Out<TsVar<"O">> out)
        {
            series_impl_detail::publish_element(ts.base().value().checked_as<Series>(), key.value(),
                                                static_cast<const TSOutputView &>(out));
        }
    };

    /** contains_(series, item): membership via arrow ``is_in``. */
    struct series_contains_impl
    {
        static constexpr auto name = "contains_series";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *series = time_series_schema_at_as<AnyTS>(context, 0);
            const auto *item = time_series_schema_at_as<AnyTS>(context, 1);
            return series != nullptr && series_impl_detail::is_series_value(series->value_schema) &&
                   item != nullptr && item->value_schema == series->value_schema->element_type;
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, In<"item", TS<ScalarVar<"I">>> item, Out<TS<Bool>> out)
        {
            const auto series = ts.base().value().checked_as<Series>();
            if (!series.has_value()) { out.set(false); return; }
            // is_in(item, value_set=series) -> is the item a member.
            arrow::compute::SetLookupOptions options{arrow::Datum{series.array}};
            auto item_datum = arrow_scalar(item.base().value(), ts.base().schema()->value_schema->element_type);
            if (!item_datum.type()->Equals(series.array->type()))
            {
                auto cast = arrow::compute::Cast(item_datum, series.array->type());
                if (!cast.ok())
                {
                    throw std::runtime_error("arrow Series membership item cast failed: " +
                                             cast.status().ToString());
                }
                item_datum = std::move(*cast);
            }
            auto found = arrow::compute::CallFunction("is_in", {item_datum}, &options);
            if (!found.ok()) { throw std::runtime_error("arrow is_in failed: " + found.status().ToString()); }
            out.set(std::static_pointer_cast<arrow::BooleanScalar>(found->scalar())->value);
        }
    };

    void register_series_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H
