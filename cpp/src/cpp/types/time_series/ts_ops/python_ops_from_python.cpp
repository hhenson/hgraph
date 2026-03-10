#include "ts_ops_internal.h"

namespace hgraph {

void op_from_python_scalar(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    if (HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_FROM_PYTHON")) {
        std::fprintf(stderr, "[from_py] path=%s depth=%zu level=%p level_depth=%u meta=%p root_meta=%p time=%lld\n",
                     vd.to_short_path().to_string().c_str(),
                     vd.path_depth(),
                     static_cast<void*>(vd.level),
                     vd.level_depth,
                     static_cast<const void*>(vd.meta),
                     static_cast<const void*>(vd.root_meta),
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }

    const bool had_value_before = maybe_dst->valid();
    value::Value old_value{};
    if (had_value_before) {
        old_value = maybe_dst->clone();
    }

    if (vd.path_depth() == 0 && src.is_none()) {
        auto* value_root = static_cast<Value*>(vd.value_data);
        if (value_root != nullptr) {
            if (value_root->has_value()) {
                invalidate_python_value_cache(vd);
            }
            value_root->reset();
            seed_python_value_cache_slot(vd, nb::none());
            stamp_time_paths(vd, current_time);
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (src.is_none()) {
        // Non-root TS assignments of None invalidate the leaf while still
        // ticking parent containers in this cycle.
        maybe_dst->from_python(src);
        if (had_value_before) {
            invalidate_python_value_cache(vd);
        }
        seed_python_value_cache_slot(vd, nb::none());
        stamp_time_paths(vd, current_time);
        set_leaf_time_path(vd, MIN_DT);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    maybe_dst->from_python(src);
    const bool has_value_after = maybe_dst->valid();
    bool value_changed = had_value_before != has_value_after;
    if (!value_changed && had_value_before && has_value_after) {
        const value::Value new_value = maybe_dst->clone();
        value_changed = !old_value.equals(new_value);
    }
    if (value_changed) {
        invalidate_python_value_cache(vd);
    }
    seed_python_value_cache_slot(vd, maybe_dst->valid() ? maybe_dst->to_python() : nb::none());
    stamp_time_paths(vd, current_time);
    mark_tsd_parent_child_modified(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void op_from_python(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    bind_view_data_ops(vd);
    if (vd.ops != nullptr && vd.ops->from_python != nullptr) {
        vd.ops->from_python(vd, src, current_time);
    } else {
        op_from_python_scalar(vd, src, current_time);
    }
}

}  // namespace hgraph
