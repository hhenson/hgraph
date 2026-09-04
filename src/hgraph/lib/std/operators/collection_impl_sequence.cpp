#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>

namespace hgraph::stdlib
{
    // TSL / tuple / TSB operators: indexing, membership, list arithmetic,
    // the TSL extremum graphs and the TSB field reductions.
    void register_collection_sequence_overloads()
    {
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
        register_overload<eq_, collection_impl_detail::eq_tsb_impl>();
        register_graph_overload<sub_, collection_impl_detail::sub_str_invalid>();
    }
}  // namespace hgraph::stdlib
