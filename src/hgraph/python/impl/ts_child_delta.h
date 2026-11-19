#pragma once

#include <hgraph/python/conversion.h>

namespace hgraph::python_bridge
{
    /** An absent handle means the child has no public update. A valid child
        that changed this cycle can explicitly project Python None, for example
        when a window loses its last sample without adding another. */
    inline nb::object published_child_delta(const TSDataOps &ops, const void *child, DateTime now)
    {
        auto result = take(ops.delta_to_python_impl(ops.context, child, now));
        if (!result.is_none()) { return result; }
        const auto *tracking = ops.tracking_impl(ops.context, child);
        if (tracking && tracking->last_modified_time == now && ops.has_current_value_impl(ops.context, child))
            return result;
        return {};
    }
}
