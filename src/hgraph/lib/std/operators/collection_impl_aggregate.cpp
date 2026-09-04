#include <hgraph/lib/std/operators/impl/collection_impl.h>

namespace hgraph::stdlib
{
    // Unary min_ / max_ / sum_ / mean over TSS / TSD / TSL inputs, and the
    // element-wise sum_ / mean maps over TSL and TSB operands.
    void register_collection_aggregate_overloads()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        register_overload<min_, collection_impl_detail::min_tss_unary>();
        register_overload<min_, collection_impl_detail::extremum_tss_default<true>>();
        register_overload<add_, collection_impl_detail::tss_scalar_adjust<true>>();
        register_overload<sub_, collection_impl_detail::tss_scalar_adjust<false>>();
        register_overload<max_, collection_impl_detail::extremum_tss_default<false>>();
        register_overload<max_, collection_impl_detail::max_tss_unary>();
        register_overload<min_, collection_impl_detail::min_tsd_unary>();
        register_overload<max_, collection_impl_detail::max_tsd_unary>();
        register_overload<min_, collection_impl_detail::min_tsl_unary>();
        register_overload<max_, collection_impl_detail::max_tsl_unary>();
        register_overload<sum_, collection_impl_detail::sum_tss_unary<Int>>();
        register_overload<sum_, collection_impl_detail::sum_tss_unary<Float>>();
        register_overload<sum_, collection_impl_detail::sum_tsd_unary<Int>>();
        register_overload<sum_, collection_impl_detail::sum_tsd_unary<Float>>();
        register_overload<sum_, collection_impl_detail::sum_tsl_unary<Int>>();
        register_overload<sum_, collection_impl_detail::sum_tsl_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tss_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tss_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tsd_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tsd_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tsl_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tsl_unary<Float>>();
        register_graph_overload<sum_, tsl_binary_map<add_>>();
        register_graph_overload<sum_, tsl_rhs_broadcast_map<add_>>();
        register_graph_overload<sum_, tsl_lhs_broadcast_map<add_>>();
        register_graph_overload<sum_, tsb_binary_map<add_>>();

        register_numeric_binary_collection_overloads<mean, scalar_mean>();
        register_numeric_binary_tsl_lifted_maps<mean, scalar_mean>();
        register_graph_overload<mean, tsb_binary_map<mean>>();
    }
}  // namespace hgraph::stdlib
