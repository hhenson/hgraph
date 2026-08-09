#include <hgraph/lib/std/operators/impl/control_impl.h>

namespace hgraph::stdlib
{
    void register_control_operators()
    {
        register_graph_overload<merge, merge_graph_impl>();
        register_graph_overload<merge, merge_tsd_graph_impl>();
        register_graph_overload<merge, merge_tsd_disjoint_graph_impl>();
        register_overload<merge_tsd_disjoint_marker, control_impl_detail::merge_tsd_disjoint_node>();
        register_graph_overload<merge, merge_tsl_graph_impl>();
        register_graph_overload<race, race_graph_impl>();
        register_graph_overload<reduce_tsd_with_race, race_tsd_graph_impl>();
        register_graph_overload<reduce_tsd_of_bundles_with_race, race_tsd_graph_impl>();
        register_graph_overload<all_, all_graph_impl>();
        register_graph_overload<any_, any_graph_impl>();
        register_overload<all_, all_tsd_impl>();
        register_overload<any_, any_tsd_impl>();
// race_ref_impl is wired ONLY through race_graph_impl::compose - a direct
        // registration would let 2-arg calls match its (refs, values) shape.
        register_overload<if_, if_ref_impl>();
        register_overload<route_by_index, route_by_index_ref_impl>();
        register_overload<if_true, if_true_impl>();
        register_overload<if_then_else, if_then_else_impl>();
        register_overload<if_cmp, if_cmp_impl>();
    }
}  // namespace hgraph::stdlib
