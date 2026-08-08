#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>

namespace hgraph::stdlib
{
    void register_collection_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        register_graph_overload<keys_, collection_impl_detail::keys_tsd>();
        register_overload<keys_, collection_impl_detail::keys_tsd_as_set>();
        register_overload<values_, collection_impl_detail::values_tsd_as_tss>();
        register_overload<keys_, collection_impl_detail::keys_map_scalar>();
        register_overload<values_, collection_impl_detail::values_map_scalar>();
        register_overload<rekey, collection_impl_detail::rekey_map_scalar>();
        register_overload<flip, collection_impl_detail::flip_map_scalar>();
        register_overload<partition, collection_impl_detail::partition_map_scalar>();
        register_overload<flip_keys, collection_impl_detail::flip_keys_map_scalar>();
        register_overload<flip_keys, collection_impl_detail::flip_keys_tsd>();
        register_overload<filter_tsd_by_matches, collection_impl_detail::filter_tsd_by_matches_impl>();
        register_overload<collapse_keys, collection_impl_detail::collapse_keys_map_scalar>();
        register_overload<collapse_keys, collection_impl_detail::collapse_keys_tsd>();
        register_overload<uncollapse_keys, collection_impl_detail::uncollapse_keys_map_scalar>();
        register_overload<uncollapse_keys, collection_impl_detail::uncollapse_keys_tsd>();
        register_overload<combine, collection_impl_detail::combine_bundles_impl>();
        register_overload<combine_cs, collection_impl_detail::combine_cs_from_fields<true>>();
        register_overload<combine_cs, collection_impl_detail::combine_cs_from_fields<false>>();
        register_overload<make_tsd, collection_impl_detail::make_tsd_impl>();
        register_overload<make_tsd_with_remove, collection_impl_detail::make_tsd_with_remove_impl>();
        register_overload<mul_, collection_impl_detail::mul_tuple_int>();
        register_overload<getitem_, collection_impl_detail::getitem_ts_list>();
        register_overload<getitem_, collection_impl_detail::getitem_ts_fixed_tuple>();
        register_overload<index_of, collection_impl_detail::index_of_ts_list>();
        register_overload<contains_, collection_impl_detail::contains_ts_list>();
        register_overload<extremum_ts_list_marker, collection_impl_detail::extremum_ts_list_node<true>>();
        register_overload<extremum_ts_list_max_marker, collection_impl_detail::extremum_ts_list_node<false>>();
        register_graph_overload<min_, collection_impl_detail::extremum_ts_list_graph<true>>();
        register_graph_overload<max_, collection_impl_detail::extremum_ts_list_graph<false>>();
        register_overload<add_, collection_impl_detail::add_ts_list_concat>();
        register_overload<add_, collection_impl_detail::add_ts_list_scalar>();
        register_overload<sub_, collection_impl_detail::sub_ts_list_scalar>();
        using collection_impl_detail::TsbAggregate;
        using collection_impl_detail::tsb_extremum_impl;
        using collection_impl_detail::tsb_numeric_aggregate_impl;
        register_overload<min_, tsb_extremum_impl<TsbAggregate::Min>>();
        register_overload<max_, tsb_extremum_impl<TsbAggregate::Max>>();
        register_overload<sum_, tsb_numeric_aggregate_impl<TsbAggregate::Sum, Int>>();
        register_overload<sum_, tsb_numeric_aggregate_impl<TsbAggregate::Sum, Float>>();
        register_overload<mean, tsb_numeric_aggregate_impl<TsbAggregate::Mean, Int>>();
        register_overload<mean, tsb_numeric_aggregate_impl<TsbAggregate::Mean, Float>>();
        register_overload<std_, tsb_numeric_aggregate_impl<TsbAggregate::Std, Int>>();
        register_overload<std_, tsb_numeric_aggregate_impl<TsbAggregate::Std, Float>>();
        register_overload<eq_, collection_impl_detail::eq_tsb_impl>();
        register_graph_overload<sub_, collection_impl_detail::sub_str_invalid>();
        register_overload<combine_tsd, collection_impl_detail::combine_tsd_tsls>();
        register_overload<combine_tsd, collection_impl_detail::combine_tsd_tuple_values>();
        register_overload<combine_tsd, collection_impl_detail::combine_tsd_tuples>();
        register_graph_overload<combine_tsd, collection_impl_detail::combine_tsd_variadic>();
        register_overload<combine_map, collection_impl_detail::combine_map_pair>();
        register_overload<combine_map, collection_impl_detail::combine_map_tuples>();
        register_overload<combine_map, collection_impl_detail::combine_map_tsls>();
        register_overload<rekey, collection_impl_detail::rekey_tsd_scalar>();
        register_overload<rekey, collection_impl_detail::rekey_tsd_set>();
        register_overload<flip, collection_impl_detail::flip_tsd_unique>();
        register_overload<flip, collection_impl_detail::flip_tsd_non_unique>();
        register_overload<partition, collection_impl_detail::partition_tsd_scalar>();
        register_overload<unpartition, collection_impl_detail::unpartition_tsd>();
        register_overload<not_, collection_impl_detail::not_tss>();
        register_overload<not_, collection_impl_detail::not_tsd>();
        register_overload<and_, collection_impl_detail::and_tss>();
        register_overload<or_, collection_impl_detail::or_tss>();
        register_overload<eq_, collection_impl_detail::eq_tss>();
        register_overload<eq_, collection_impl_detail::eq_tsd>();
        register_overload<eq_, collection_impl_detail::eq_tsd_epsilon>();
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
        register_overload<std_, collection_impl_detail::std_tss_unary<Int>>();
        register_overload<std_, collection_impl_detail::std_tss_unary<Float>>();
        register_overload<var_, collection_impl_detail::var_tss_unary<Int>>();
        register_overload<var_, collection_impl_detail::var_tss_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tsd_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tsd_unary<Float>>();
        register_overload<std_, collection_impl_detail::std_tsd_unary<Int>>();
        register_overload<std_, collection_impl_detail::std_tsd_unary<Float>>();
        register_overload<var_, collection_impl_detail::var_tsd_unary<Int>>();
        register_overload<var_, collection_impl_detail::var_tsd_unary<Float>>();
        register_overload<mean, collection_impl_detail::mean_tsl_unary<Int>>();
        register_overload<mean, collection_impl_detail::mean_tsl_unary<Float>>();
        register_overload<std_, collection_impl_detail::std_tsl_unary<Int>>();
        register_overload<std_, collection_impl_detail::std_tsl_unary<Float>>();
        register_overload<var_, collection_impl_detail::var_tsl_unary<Int>>();
        register_overload<var_, collection_impl_detail::var_tsl_unary<Float>>();
        register_graph_overload<sum_, tsl_binary_map<add_>>();
        register_graph_overload<sum_, tsl_rhs_broadcast_map<add_>>();
        register_graph_overload<sum_, tsl_lhs_broadcast_map<add_>>();
        register_graph_overload<sum_, tsb_binary_map<add_>>();

        register_numeric_binary_collection_overloads<mean, scalar_mean>();
        register_numeric_binary_collection_overloads<std_, scalar_std>();
        register_numeric_binary_collection_overloads<var_, scalar_var>();
        register_numeric_binary_tsl_lifted_maps<mean, scalar_mean>();
        register_numeric_binary_tsl_lifted_maps<std_, scalar_std>();
        register_numeric_binary_tsl_lifted_maps<var_, scalar_var>();
        register_graph_overload<mean, tsb_binary_map<mean>>();
        register_graph_overload<std_, tsb_binary_map<std_>>();
        register_graph_overload<var_, tsb_binary_map<var_>>();

        register_graph_overload<union_, collection_impl_detail::union_tss_fold>();
        register_graph_overload<intersection_, collection_impl_detail::intersection_tss_fold>();
        register_graph_overload<difference_, collection_impl_detail::difference_tss_fold>();
        register_graph_overload<symmetric_difference_, collection_impl_detail::symmetric_difference_tss_fold>();

        register_graph_overload<bit_or, collection_impl_detail::union_tss_fold>();
        register_graph_overload<bit_and, collection_impl_detail::intersection_tss_fold>();
        register_graph_overload<sub_, collection_impl_detail::difference_tss_fold>();
        register_graph_overload<bit_xor, collection_impl_detail::symmetric_difference_tss_fold>();

        register_overload<bit_or, collection_impl_detail::union_tsd_binary>();
        register_overload<bit_and, collection_impl_detail::intersection_tsd_binary>();
        register_overload<sub_, collection_impl_detail::difference_tsd_binary>();
        register_overload<bit_xor, collection_impl_detail::symmetric_difference_tsd_binary>();
    }
}  // namespace hgraph::stdlib
