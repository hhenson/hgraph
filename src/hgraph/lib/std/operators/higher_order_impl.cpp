#include <hgraph/lib/std/operators/impl/higher_order_impl.h>

namespace hgraph::stdlib
{
    void register_higher_order_operators()
    {
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_variadic_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_lifted_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsl_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_ordered_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd_ts_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_tsd_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_ordered_tsd>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_dynamic_tsl>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_dynamic_tsl_ts_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_dynamic_tsl_zero>();
        register_graph_overload<reduce_, higher_order_impl_detail::reduce_ordered_dynamic_tsl>();

        register_graph_overload<switch_, higher_order_impl_detail::switch_impl>();
        register_graph_overload<switch_, higher_order_impl_detail::switch_sink_impl>();
        register_graph_overload<dispatch_, higher_order_impl_detail::dispatch_impl>();
        register_graph_overload<try_except, higher_order_impl_detail::try_except_impl>();

        register_graph_overload<map_, higher_order_impl_detail::map_impl_tsd>();
        register_graph_overload<map_, higher_order_impl_detail::map_sink_impl_tsd>();
        register_graph_overload<map_, higher_order_impl_detail::map_lifted_tsl>();
        register_graph_overload<map_, higher_order_impl_detail::map_impl_tsl>();

        register_graph_overload<map_, higher_order_impl_detail::map_sink_impl_tsl>();

        register_graph_overload<mesh_, higher_order_impl_detail::mesh_impl_tsd>();
    }
}  // namespace hgraph::stdlib
