#include <hgraph/lib/std/operators/impl/container_impl.h>

namespace hgraph::stdlib
{
    void register_container_operators()
    {
        register_overload<getitem_, getitem_string>();
        register_overload<contains_, contains_string>();
        register_overload<len_, len_string>();
        register_overload<is_empty, is_empty_string>();

        register_overload<len_, len_tss>();
        register_overload<len_, len_tsd>();
        register_overload<len_, len_tsl>();
        register_overload<len_, len_tsw>();

        register_overload<is_empty, is_empty_tss>();
        register_overload<is_empty, is_empty_tsd>();

        register_overload<contains_, contains_tss_item>();
        register_overload<contains_, contains_tss_subset>();
        register_overload<contains_, contains_tsd_key>();
        register_overload<contains_, contains_set_scalar>();

        register_overload<getitem_, getitem_tsl_by_index>();
        register_overload<getitem_, getitem_tsd_by_key>();
        register_overload<index_of, index_of_tsl>();
        register_overload<getattr_, tsb_ref_field_node<Str, "attr", "getattr_tsb_ref">>();
        register_overload<getitem_, tsb_ref_field_node<Str, "key", "getitem_tsb_ref">>();
        register_overload<getitem_, tsb_ref_field_node<Int, "key", "getitem_tsb_ref_index">>();
        register_overload<dereference, dereference_tsb_ref_node>();

        register_graph_overload<getitem_, getitem_tsb_by_name>();
        register_graph_overload<getitem_, getitem_tsb_by_index>();
        register_graph_overload<getattr_, getattr_tsb>();
        register_overload<getattr_, getattr_tsd>();
        register_overload<getitem_, getitem_tsd_by_keys>();
        register_overload<getattr_, getattr_enum>();
        register_overload<getattr_, getattr_ts_tuple_bundle>();
        register_overload<getattr_, getattr_ts_tuple_bundle_default>();
        register_overload<getattr_, getattr_ts_bundle>();
        register_overload<getattr_, getattr_ts_bundle_default>();
        register_overload<setattr_, setattr_ts_bundle>();
        register_graph_overload<getattr_, getattr_tsd_nested>();
        register_graph_overload<len_, len_tsb>();
        register_graph_overload<is_empty, is_empty_tsb>();
    }
}  // namespace hgraph::stdlib
