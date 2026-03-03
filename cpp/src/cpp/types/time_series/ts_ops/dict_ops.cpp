#include "ts_ops_internal.h"

namespace hgraph {

bool op_dict_remove(ViewData& vd, const View& key, engine_time_t current_time) {
    if (!key.valid()) {
        return false;
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        return false;
    }

    auto map = maybe_map->as_map();
    const auto removed_slot = map_slot_for_key(map, key);
    if (!removed_slot.has_value()) {
        return false;
    }
    Value canonical_key_value = canonical_map_key_for_slot(map, *removed_slot, key);
    const View canonical_key = canonical_key_value.view();
    ViewData child_vd = vd;
    child_vd.path.indices.push_back(*removed_slot);
    const bool removed_was_valid = tsd_child_was_visible_before_removal(child_vd);
    record_tsd_removed_child_snapshot(vd, canonical_key, child_vd, current_time);

    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    bool added_this_cycle = false;
    if (slots.added_set.valid() && slots.added_set.is_set()) {
        auto added = slots.added_set.as_set();
        added_this_cycle = added.contains(canonical_key);
        if (added_this_cycle) {
            added.remove(canonical_key);
        }
    }

    if (!map.remove(canonical_key)) {
        return false;
    }
    invalidate_python_value_cache(vd);

    compact_tsd_child_time_slot(vd, *removed_slot);
    compact_tsd_child_delta_slot(vd, *removed_slot);
    compact_tsd_child_link_slot(vd, *removed_slot);

    if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
        slots.changed_values_map.as_map().remove(canonical_key);
    }

    if (!added_this_cycle) {
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().add(canonical_key);
        }
        if (!removed_was_valid &&
            slots.added_set.valid() && slots.added_set.is_set()) {
            // Marker: removed from key-space without a visible value removal.
            slots.added_set.as_set().add(canonical_key);
        }
    }

    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
    return true;
}

TSView op_dict_create(ViewData& vd, const View& key, engine_time_t current_time) {
    const bool debug_create = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_TSD_CREATE_CORE");
    if (!key.valid()) {
        if (debug_create) {
            std::fprintf(stderr, "[op_dict_create] invalid key\n");
        }
        return {};
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        if (debug_create) {
            std::fprintf(stderr,
                         "[op_dict_create] map slot missing valid=%d is_map=%d\n",
                         (maybe_map.has_value() && maybe_map->valid()) ? 1 : 0,
                         (maybe_map.has_value() && maybe_map->valid() && maybe_map->is_map()) ? 1 : 0);
        }
        return {};
    }

    auto map = maybe_map->as_map();
    auto existing_slot = map_slot_for_key(map, key);
    if (debug_create) {
        std::fprintf(stderr,
                     "[op_dict_create] begin size=%zu key=%s key_schema=%p map_key=%p value_type=%p contains_before=%d\n",
                     map.size(),
                     key.to_string().c_str(),
                     static_cast<const void*>(key.schema()),
                     static_cast<const void*>(map.key_type()),
                     static_cast<const void*>(map.value_type()),
                     existing_slot.has_value() ? 1 : 0);
    }
    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    if (!existing_slot.has_value()) {
        const value::TypeMeta* value_type = map.value_type();
        if (value_type == nullptr) {
            if (debug_create) {
                std::fprintf(stderr, "[op_dict_create] value_type null\n");
            }
            return {};
        }
        value::Value default_value(value_type);
        default_value.emplace();
        map.set(key, default_value.view());
        existing_slot = map_slot_for_key(map, key);
        if (debug_create) {
            std::fprintf(stderr,
                         "[op_dict_create] after set size=%zu contains=%d\n",
                         map.size(),
                         existing_slot.has_value() ? 1 : 0);
        }
        if (existing_slot.has_value()) {
            ensure_tsd_child_time_slot(vd, *existing_slot);
            ensure_tsd_child_delta_slot(vd, *existing_slot);
            ensure_tsd_child_link_slot(vd, *existing_slot);
        } else if (debug_create) {
            std::fprintf(stderr, "[op_dict_create] slot lookup failed after set\n");
        }

        Value canonical_key_value = key.clone();
        if (existing_slot.has_value()) {
            canonical_key_value = canonical_map_key_for_slot(map, *existing_slot, key);
        }
        const View canonical_key = canonical_key_value.view();

        if (slots.added_set.valid() && slots.added_set.is_set()) {
            slots.added_set.as_set().add(canonical_key);
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(canonical_key);
        }
        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            slots.changed_values_map.as_map().remove(canonical_key);
        }

        invalidate_python_value_cache(vd);
        stamp_time_paths(vd, current_time);
        notify_link_target_observers(vd, current_time);
    }
    if (debug_create) {
        std::fprintf(stderr,
                     "[op_dict_create] end size=%zu contains=%d\n",
                     map.size(),
                     map_slot_for_key(map, key).has_value() ? 1 : 0);
    }
    if (!existing_slot.has_value()) {
        existing_slot = map_slot_for_key(map, key);
    }
    if (!existing_slot.has_value()) {
        return {};
    }
    return op_child_at(vd, *existing_slot, current_time);
}

TSView op_dict_set(ViewData& vd, const View& key, const View& value, engine_time_t current_time) {
    if (!key.valid() || !value.valid()) {
        return {};
    }
    auto maybe_map = resolve_value_slot_mut(vd);
    if (!maybe_map.has_value() || !maybe_map->valid() || !maybe_map->is_map()) {
        return {};
    }

    auto map = maybe_map->as_map();
    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    const bool existed = map_slot_for_key(map, key).has_value();
    if (!existed) {
        const value::TypeMeta* value_type = map.value_type();
        if (value_type == nullptr) {
            return {};
        }
        value::Value default_value(value_type);
        default_value.emplace();
        map.set(key, default_value.view());
    }

    const auto slot = map_slot_for_key(map, key);
    if (!slot.has_value()) {
        return {};
    }
    Value canonical_key_value = canonical_map_key_for_slot(map, *slot, key);
    const View canonical_key = canonical_key_value.view();

    ensure_tsd_child_time_slot(vd, *slot);
    ensure_tsd_child_delta_slot(vd, *slot);
    ensure_tsd_child_link_slot(vd, *slot);

    ViewData child_vd = vd;
    child_vd.path.indices.push_back(*slot);
    op_set_value(child_vd, value, current_time);

    if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
        View child_value = op_value(child_vd);
        if (child_value.valid()) {
            slots.changed_values_map.as_map().set(canonical_key, child_value);
        } else {
            slots.changed_values_map.as_map().remove(canonical_key);
        }
    }
    if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
        slots.added_set.as_set().add(canonical_key);
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        slots.removed_set.as_set().remove(canonical_key);
    }

    return op_child_at(vd, *slot, current_time);
}



const ts_dict_ops k_dict_ops{
    &op_dict_remove,
    &op_dict_create,
    &op_dict_set,
};

const ts_list_ops k_list_ops{
    &op_child_at,
    &op_list_size,
};

const ts_bundle_ops k_bundle_ops{
    &op_child_at,
    &op_child_by_name,
    &op_bundle_size,
};


}  // namespace hgraph
