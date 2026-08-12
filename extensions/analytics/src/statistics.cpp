#include <hgraph/analytics/operators.h>

#include "operator_registration.h"

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/lib/std/operators/impl/stream_impl.h>

namespace hgraph::analytics::detail
{
    namespace
    {
        namespace arithmetic = hgraph::stdlib::arithmetic_impl_detail;
        namespace collection = hgraph::stdlib::collection_impl_detail;
        namespace stream     = hgraph::stdlib::stream_impl_detail;

        template <ValueTypeKind Kind, typename Element> void register_container_dispersion() {
            register_overload<std_, arithmetic::container_numeric_agg_plain<arithmetic::ContainerAgg::Std, Kind, Element>>();
            register_overload<std_, arithmetic::container_numeric_agg_default<arithmetic::ContainerAgg::Std, Kind, Element>>();
            register_overload<var_, arithmetic::container_numeric_agg_plain<arithmetic::ContainerAgg::Var, Kind, Element>>();
            register_overload<var_, arithmetic::container_numeric_agg_default<arithmetic::ContainerAgg::Var, Kind, Element>>();
        }

        template <ValueTypeKind Kind> void register_container_dispersion() {
            register_container_dispersion<Kind, Int>();
            register_container_dispersion<Kind, Float>();
        }
    }  // namespace

    void register_statistics_operators() {
        using hgraph::stdlib::register_numeric_binary_collection_overloads;
        using hgraph::stdlib::register_numeric_binary_tsl_lifted_maps;
        using hgraph::stdlib::scalar_std;
        using hgraph::stdlib::scalar_var;
        using hgraph::stdlib::tsb_itemwise_impl_detail::tsb_binary_map;

        register_container_dispersion<ValueTypeKind::Map>();
        register_container_dispersion<ValueTypeKind::Set>();
        register_container_dispersion<ValueTypeKind::List>();

        register_overload<std_, arithmetic::running_moments_impl<Int, true>>();
        register_overload<std_, arithmetic::running_moments_impl<Float, true>>();
        register_overload<var_, arithmetic::running_moments_impl<Int, false>>();
        register_overload<var_, arithmetic::running_moments_impl<Float, false>>();

        using collection::tsb_numeric_aggregate_impl;
        using collection::TsbAggregate;
        register_overload<std_, tsb_numeric_aggregate_impl<TsbAggregate::Std, Int>>();
        register_overload<std_, tsb_numeric_aggregate_impl<TsbAggregate::Std, Float>>();

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

        register_overload<std_, stream::tsw_std_impl<Int>>();
        register_overload<std_, stream::tsw_std_impl<Float>>();
        register_overload<std_, stream::tsw_std_ddof_impl<Int>>();
        register_overload<std_, stream::tsw_std_ddof_impl<Float>>();

        register_graph_overload<rolling_mean, hgraph::stdlib::rolling_average_tick_compose>();
        register_graph_overload<rolling_mean, hgraph::stdlib::rolling_average_time_compose>();
        register_overload<resample, hgraph::stdlib::resample_impl>();
    }
}  // namespace hgraph::analytics::detail
