#include <hgraph/lib/std/operators/impl/collection_impl.h>

namespace hgraph::stdlib
{
    // TSD / map-scalar construction and key manipulation: keys_ / values_,
    // rekey / flip / partition / collapse_keys, make_tsd, combine_tsd and
    // combine_map.
    void register_collection_mapping_overloads()
    {
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
    }
}  // namespace hgraph::stdlib
