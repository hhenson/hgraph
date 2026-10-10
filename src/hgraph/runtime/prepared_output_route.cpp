#include <hgraph/runtime/prepared_input_routes.h>
#include <hgraph/types/time_series/ts_input/target_link.h>

namespace hgraph::prepared_output_detail
{
    void resolve_forwarding_terminal(PreparedOutputRoute &route, const TSDataView &data)
    {
        const auto &table = data.ops();
        if (!table.allows_mutation || !detail::is_target_link_view(data)) { return; }
        const auto *link = detail::target_link_storage(data);
        if (link == nullptr || !link->target_output().bound()) { return; }
        const TSDataView target = link->target_view();
        if (!target.valid()) { return; }
        const auto &target_table = target.ops();
        if (!direct_native_atomic(target_table)) { return; }
        resolve_native_slot(route, target, target_table);
        // The link's current-target handle lives in the link storage, which
        // the node's output owns for the route's lifetime (start to stop):
        // the per-use identity check reads it in place.
        route.link_target_ref = &link->target_output();
        route.link_target     = link->target_output();
        route.link_tracking   = table.mutable_tracking_impl(table.context, data.mutable_data());
    }
}  // namespace hgraph::prepared_output_detail
