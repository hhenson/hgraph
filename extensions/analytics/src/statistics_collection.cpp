#include <hgraph/analytics/operators.h>

#include "operator_registration.h"

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>

#include <concepts>
#include <cstddef>

namespace hgraph::analytics::detail
{
    namespace
    {
        namespace collection = hgraph::stdlib::collection_impl_detail;

        /** Sample variance across the valid fields of a homogeneous numeric
            bundle. This analytics-owned implementation keeps the separately
            built package compatible with the released core SDK. */
        template <typename T> struct tsb_numeric_variance_impl
        {
            static constexpr auto name = std::same_as<T, Int> ? "var_tsb_int" : "var_tsb_float";

            [[nodiscard]] static bool matches(OperatorCallContext context)
            {
                if (context.args.size() != 1) { return false; }
                const auto *schema = collection::homogeneous_tsb(context);
                return schema != nullptr &&
                       schema->fields()[0].type->value_schema == scalar_descriptor<T>::value_meta();
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context) { return matches(context); }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                using namespace hgraph::operator_type_resolution;
                if (output_bound(resolution) || !matches(context)) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()));
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto        &erased = static_cast<const TSOutputView &>(out);
                const TSInputView &bundle = ts;
                const std::size_t  count  = bundle.schema()->field_count();

                Float total = 0.0;
                for (std::size_t index = 0; index < count; ++index)
                {
                    total += static_cast<Float>(
                        bundle.indexed_child_at(index).value().checked_as<T>());
                }

                // A two-pass sample variance (ddof=1) matches the migrated
                // collection contract and avoids a cancellation-prone sum of
                // squares. Homogeneous TSB schemas cannot be empty.
                const Float mean = total / static_cast<Float>(count);
                Float       squared_deviation = 0.0;
                for (std::size_t index = 0; index < count; ++index)
                {
                    const auto value =
                        static_cast<Float>(bundle.indexed_child_at(index).value().checked_as<T>());
                    squared_deviation += (value - mean) * (value - mean);
                }
                const Float variance =
                    count > 1 ? squared_deviation / static_cast<Float>(count - 1) : 0.0;
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(Value{variance}));
            }
        };

    }  // namespace

    // std_ / var_ over TSB, TSS, TSD and TSL inputs and the item-wise maps;
    // one registration group per translation unit (see "Registration
    // translation units" in the operators developer guide).
    void register_statistics_collection_overloads()
    {
        using hgraph::stdlib::register_numeric_binary_collection_overloads;
        using hgraph::stdlib::register_numeric_binary_tsl_lifted_maps;
        using hgraph::stdlib::scalar_std;
        using hgraph::stdlib::scalar_var;
        using hgraph::stdlib::tsb_itemwise_impl_detail::tsb_binary_map;

        using collection::tsb_numeric_aggregate_impl;
        using collection::TsbAggregate;
        register_overload<std_, tsb_numeric_aggregate_impl<TsbAggregate::Std, Int>>();
        register_overload<std_, tsb_numeric_aggregate_impl<TsbAggregate::Std, Float>>();
        register_overload<var_, tsb_numeric_variance_impl<Int>>();
        register_overload<var_, tsb_numeric_variance_impl<Float>>();

        register_overload<std_, collection::std_tss_unary<Int>>();
        register_overload<std_, collection::std_tss_unary<Float>>();
        register_overload<var_, collection::var_tss_unary<Int>>();
        register_overload<var_, collection::var_tss_unary<Float>>();
        register_overload<std_, collection::std_tsd_unary<Int>>();
        register_overload<std_, collection::std_tsd_unary<Float>>();
        register_overload<var_, collection::var_tsd_unary<Int>>();
        register_overload<var_, collection::var_tsd_unary<Float>>();
        register_overload<std_, collection::std_tsl_unary<Int>>();
        register_overload<std_, collection::std_tsl_unary<Float>>();
        register_overload<var_, collection::var_tsl_unary<Int>>();
        register_overload<var_, collection::var_tsl_unary<Float>>();

        register_numeric_binary_collection_overloads<std_, scalar_std>();
        register_numeric_binary_collection_overloads<var_, scalar_var>();
        register_numeric_binary_tsl_lifted_maps<std_, scalar_std>();
        register_numeric_binary_tsl_lifted_maps<var_, scalar_var>();
        register_graph_overload<std_, tsb_binary_map<std_>>();
        register_graph_overload<var_, tsb_binary_map<var_>>();
    }
}  // namespace hgraph::analytics::detail
