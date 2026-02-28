#include "ts_ops_internal.h"

namespace hgraph {

void op_from_python_scalar(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }

    if (vd.path.indices.empty() && src.is_none()) {
        auto* value_root = static_cast<Value*>(vd.value_data);
        if (value_root != nullptr) {
            value_root->reset();
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (src.is_none()) {
        // Non-root TS assignments of None invalidate the leaf while still
        // ticking parent containers in this cycle.
        maybe_dst->from_python(src);
        stamp_time_paths(vd, current_time);
        set_leaf_time_path(vd, MIN_DT);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    maybe_dst->from_python(src);
    stamp_time_paths(vd, current_time);
    mark_tsd_parent_child_modified(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    bind_view_data_ops(vd);
    if (vd.ops != nullptr && vd.ops->from_python != nullptr) {
        vd.ops->from_python(vd, src, current_time);
        return;
    }

    op_from_python_scalar(vd, src, current_time);
}

}  // namespace hgraph
