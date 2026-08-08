#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H

#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/series.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/scalar.h>

#include <stdexcept>
#include <string>

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
            built from an ``int``/``float`` TS value. */
        [[nodiscard]] inline arrow::Datum operand_datum(const TSInputView &input)
        {
            const auto *schema = input.schema();
            if (is_series_value(schema->value_schema))
            {
                return arrow::Datum{input.value().checked_as<Series>().array};
            }
            auto &registry = TypeRegistry::instance();
            if (schema->value_schema == registry.value_type("int"))
            {
                return arrow::Datum{arrow::MakeScalar(static_cast<std::int64_t>(input.value().checked_as<Int>()))};
            }
            if (schema->value_schema == registry.value_type("float"))
            {
                return arrow::Datum{arrow::MakeScalar(input.value().checked_as<Float>())};
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

        /** Read ``series[index]`` and publish it as the erased output's scalar
            (int/float following the bound element type). */
        inline void publish_element(const Series &series, Int index, const TSOutputView &erased)
        {
            if (!series.has_value() || index < 0 || index >= series.array->length())
            {
                throw std::out_of_range("Series index out of range");
            }
            auto scalar = series.array->GetScalar(index);
            if (!scalar.ok()) { throw std::runtime_error("Series element read failed"); }
            auto  &registry = TypeRegistry::instance();
            Value  result;
            if (erased.schema()->value_schema == registry.value_type("float"))
            {
                auto v = arrow::compute::Cast(arrow::Datum{*scalar}, arrow::float64());
                if (!v.ok()) { throw std::runtime_error("Series element cast failed"); }
                result = Value{std::static_pointer_cast<arrow::DoubleScalar>(v->scalar())->value};
            }
            else
            {
                auto v = arrow::compute::Cast(arrow::Datum{*scalar}, arrow::int64());
                if (!v.ok()) { throw std::runtime_error("Series element cast failed"); }
                result = Value{static_cast<Int>(std::static_pointer_cast<arrow::Int64Scalar>(v->scalar())->value)};
            }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    }  // namespace series_impl_detail

    /** Elementwise Series arithmetic via arrow compute. ``Div`` is TRUE
        division (int/int -> float, hgraph semantics), so both operands cast
        to float first. Handles Series (+) Series and Series (+) scalar (and
        scalar first) - at least one operand is a Series. */
    template <fixed_string FnName, bool Div>
    struct series_binary_impl
    {
        static constexpr const char *name = FnName.value;

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
            return series_impl_detail::is_series_arg(context, 0);
        }

        static void eval(In<"ts", TS<ScalarVar<"S">>> ts, In<"item", TS<ScalarVar<"I">>> item, Out<TS<Bool>> out)
        {
            const auto series = ts.base().value().checked_as<Series>();
            if (!series.has_value()) { out.set(false); return; }
            // is_in(item, value_set=series) -> is the item a member.
            arrow::compute::SetLookupOptions options{arrow::Datum{series.array}};
            auto item_datum = series_impl_detail::operand_datum(item);
            auto found = arrow::compute::CallFunction("is_in", {item_datum}, &options);
            if (!found.ok()) { throw std::runtime_error("arrow is_in failed: " + found.status().ToString()); }
            out.set(std::static_pointer_cast<arrow::BooleanScalar>(found->scalar())->value);
        }
    };

    void register_series_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_SERIES_IMPL_H
