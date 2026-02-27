#include "ts_ops_internal.h"

namespace hgraph {

bool op_set_add(ViewData& vd, const View& elem, engine_time_t current_time) {
    if (!elem.valid()) {
        return false;
    }
    auto maybe_set = resolve_value_slot_mut(vd);
    if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
        return false;
    }

    auto set = maybe_set->as_set();
    const bool added = set.add(elem);
    if (!added) {
        return false;
    }

    mark_tsd_parent_child_modified(vd, current_time);

    bool consumed_remove = false;
    if (!vd.path.indices.empty()) {
        ViewData parent = vd;
        const size_t child_slot = parent.path.indices.back();
        parent.path.indices.pop_back();
        const TSMeta* parent_meta = meta_at_path(parent.meta, parent.path.indices);
        if (dispatch_meta_is_tsd(parent_meta)) {
            ensure_tsd_child_delta_slot(parent, child_slot);
        }
    }

    auto slots = resolve_tss_delta_slots(vd);
    clear_tss_delta_if_new_tick(vd, current_time, slots);
    if (slots.slot.valid()) {
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            auto removed = slots.removed_set.as_set();
            if (removed.contains(elem)) {
                removed.remove(elem);
                consumed_remove = true;
            }
        }
        if (!consumed_remove && slots.added_set.valid() && slots.added_set.is_set()) {
            slots.added_set.as_set().add(elem);
        }
    }

    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
    return true;
}

bool op_set_remove(ViewData& vd, const View& elem, engine_time_t current_time) {
    if (!elem.valid()) {
        return false;
    }
    auto maybe_set = resolve_value_slot_mut(vd);
    if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
        return false;
    }

    auto set = maybe_set->as_set();
    const bool removed = set.remove(elem);
    if (!removed) {
        return false;
    }

    mark_tsd_parent_child_modified(vd, current_time);

    bool consumed_add = false;
    if (!vd.path.indices.empty()) {
        ViewData parent = vd;
        const size_t child_slot = parent.path.indices.back();
        parent.path.indices.pop_back();
        const TSMeta* parent_meta = meta_at_path(parent.meta, parent.path.indices);
        if (dispatch_meta_is_tsd(parent_meta)) {
            ensure_tsd_child_delta_slot(parent, child_slot);
        }
    }

    auto slots = resolve_tss_delta_slots(vd);
    clear_tss_delta_if_new_tick(vd, current_time, slots);
    if (slots.slot.valid()) {
        if (slots.added_set.valid() && slots.added_set.is_set()) {
            auto added = slots.added_set.as_set();
            if (added.contains(elem)) {
                added.remove(elem);
                consumed_add = true;
            }
        }
        if (!consumed_add && slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().add(elem);
        }
    }

    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
    return true;
}

void op_set_clear(ViewData& vd, engine_time_t current_time) {
    auto maybe_set = resolve_value_slot_mut(vd);
    if (!maybe_set.has_value() || !maybe_set->valid() || !maybe_set->is_set()) {
        return;
    }

    auto set = maybe_set->as_set();
    if (set.size() == 0) {
        return;
    }

    std::vector<Value> existing_values;
    existing_values.reserve(set.size());
    for (View elem : set) {
        existing_values.emplace_back(elem.clone());
    }

    mark_tsd_parent_child_modified(vd, current_time);

    if (!vd.path.indices.empty()) {
        ViewData parent = vd;
        const size_t child_slot = parent.path.indices.back();
        parent.path.indices.pop_back();
        const TSMeta* parent_meta = meta_at_path(parent.meta, parent.path.indices);
        if (dispatch_meta_is_tsd(parent_meta)) {
            ensure_tsd_child_delta_slot(parent, child_slot);
        }
    }

    auto slots = resolve_tss_delta_slots(vd);
    clear_tss_delta_if_new_tick(vd, current_time, slots);

    if (slots.slot.valid()) {
        for (const Value& value : existing_values) {
            View elem = value.view();
            bool consumed_add = false;
            if (slots.added_set.valid() && slots.added_set.is_set()) {
                auto added = slots.added_set.as_set();
                if (added.contains(elem)) {
                    added.remove(elem);
                    consumed_add = true;
                }
            }
            if (!consumed_add && slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().add(elem);
            }
        }
    }
    set.clear();
    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
}


const ts_set_ops k_set_ops{
    &op_set_add,
    &op_set_remove,
    &op_set_clear,
};

}  // namespace hgraph
