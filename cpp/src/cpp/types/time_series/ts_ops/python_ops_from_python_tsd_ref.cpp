#include "ts_ops_internal.h"

namespace hgraph {

namespace {

void op_from_python_tsd_ref_impl(ViewData& vd,
                                 const nb::object& src,
                                 engine_time_t current_time,
                                 const TSMeta* current) {
    if (reset_root_value_and_delta_on_none(vd, src, current_time)) {
        return;
    }

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_map()) {
        return;
    }

    const bool was_valid = op_valid(vd);
    auto dst_map = maybe_dst->as_map();
    auto slots = resolve_tsd_delta_slots(vd);
    clear_tsd_delta_if_new_tick(vd, current_time, slots);

    nb::object item_attr = nb::getattr(src, "items", nb::none());
    nb::iterator items = item_attr.is_none() ? nb::iter(src) : nb::iter(item_attr());

    const value::TypeMeta* key_type = current->key_type();
    const value::TypeMeta* value_type = current->element_ts() != nullptr ? current->element_ts()->value_type : nullptr;
    nb::object remove = get_remove();
    nb::object remove_if_exists = get_remove_if_exists();

    enum class RemoveMarkerKind {
        None,
        Remove,
        RemoveIfExists,
    };

    const auto classify_remove_marker = [&remove, &remove_if_exists](const nb::object& obj) -> RemoveMarkerKind {
        if (obj.is(remove)) {
            return RemoveMarkerKind::Remove;
        }
        if (obj.is(remove_if_exists)) {
            return RemoveMarkerKind::RemoveIfExists;
        }

        nb::object current_name = nb::getattr(obj, "name", nb::none());
        for (size_t depth = 0; depth < 4 && !current_name.is_none(); ++depth) {
            if (current_name.is(remove)) {
                return RemoveMarkerKind::Remove;
            }
            if (current_name.is(remove_if_exists)) {
                return RemoveMarkerKind::RemoveIfExists;
            }
            if (nb::isinstance<nb::str>(current_name)) {
                std::string name = nb::cast<std::string>(current_name);
                if (name == "REMOVE") {
                    return RemoveMarkerKind::Remove;
                }
                if (name == "REMOVE_IF_EXISTS") {
                    return RemoveMarkerKind::RemoveIfExists;
                }
                return RemoveMarkerKind::None;
            }
            current_name = nb::getattr(current_name, "name", nb::none());
        }

        return RemoveMarkerKind::None;
    };

    bool changed = false;
    std::vector<size_t> directly_mutated_slots;

    for (const auto& kv : items) {
        value::Value key_value(key_type);
        key_value.emplace();
        key_type->ops().from_python(key_value.data(), kv[0], key_type);
        View key = key_value.view();

        nb::object value_obj = kv[1];
        if (value_obj.is_none()) {
            continue;
        }

        const RemoveMarkerKind marker = classify_remove_marker(value_obj);
        const bool is_remove = marker == RemoveMarkerKind::Remove;
        const bool is_remove_if_exists = marker == RemoveMarkerKind::RemoveIfExists;
        if (is_remove || is_remove_if_exists) {
            const auto removed_slot = map_slot_for_key(dst_map, key);
            const bool existed = removed_slot.has_value();
            if (!existed) {
                const bool already_removed_this_tick =
                    slots.removed_set.valid() &&
                    slots.removed_set.is_set() &&
                    set_contains_key_relaxed(slots.removed_set.as_set(), key);
                if (is_remove) {
                    if (already_removed_this_tick) {
                        continue;
                    }
                    throw nb::key_error("TSD key not found for REMOVE");
                }
                continue;
            }

            Value canonical_key_value = canonical_map_key_for_slot(dst_map, *removed_slot);
            const View canonical_key = canonical_key_value.view();
            bool removed_was_visible = false;
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(*removed_slot);
            removed_was_visible = tsd_child_was_visible_before_removal(child_vd);
            record_tsd_removed_child_snapshot(vd, canonical_key, child_vd, current_time);

            bool added_this_cycle = false;
            if (slots.added_set.valid() && slots.added_set.is_set()) {
                auto added = slots.added_set.as_set();
                added_this_cycle = added.contains(canonical_key);
                if (added_this_cycle) {
                    added.remove(canonical_key);
                }
            }

            dst_map.remove(canonical_key);
            changed = true;
            directly_mutated_slots.push_back(*removed_slot);
            compact_tsd_child_time_slot(vd, *removed_slot);
            compact_tsd_child_delta_slot(vd, *removed_slot);
            compact_tsd_child_link_slot(vd, *removed_slot);

            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                slots.changed_values_map.as_map().remove(canonical_key);
            }

            if (!added_this_cycle &&
                slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().add(canonical_key);
                if (!removed_was_visible &&
                    slots.added_set.valid() && slots.added_set.is_set()) {
                    slots.added_set.as_set().add(canonical_key);
                }
            }
            continue;
        }

        if (value_type == nullptr) {
            continue;
        }

        if (nb::isinstance<TimeSeriesReference>(value_obj)) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(value_obj);
            if (!ref.is_valid()) {
                const bool existed = map_slot_for_key(dst_map, key).has_value();
                if (!existed) {
                    value::Value blank_value(value_type);
                    blank_value.emplace();
                    dst_map.set(key, blank_value.view());

                    auto slot = map_slot_for_key(dst_map, key);
                    if (slot.has_value()) {
                        Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot);
                        const View canonical_key = canonical_key_value.view();
                        ensure_tsd_child_time_slot(vd, *slot);
                        ensure_tsd_child_delta_slot(vd, *slot);
                        ensure_tsd_child_link_slot(vd, *slot);
                        if (slots.added_set.valid() && slots.added_set.is_set()) {
                            slots.added_set.as_set().add(canonical_key);
                        }
                        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                            slots.removed_set.as_set().remove(canonical_key);
                        }
                        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                            slots.changed_values_map.as_map().remove(canonical_key);
                        }
                        changed = true;
                        directly_mutated_slots.push_back(*slot);
                    }
                }
                continue;
            }
        }

        value::Value value_value(value_type);
        value_value.emplace();
        value_type->ops().from_python(value_value.data(), value_obj, value_type);

        const bool existed = map_slot_for_key(dst_map, key).has_value();
        dst_map.set(key, value_value.view());
        changed = true;

        auto slot = map_slot_for_key(dst_map, key);
        if (!slot.has_value()) {
            continue;
        }
        directly_mutated_slots.push_back(*slot);

        Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot);
        const View canonical_key = canonical_key_value.view();
        ensure_tsd_child_time_slot(vd, *slot);
        ensure_tsd_child_link_slot(vd, *slot);

        ViewData child_vd = vd;
        child_vd.path.indices.push_back(*slot);
        stamp_time_paths(child_vd, current_time);
        seed_python_value_cache_slot(child_vd, value_value.view().to_python());

        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            slots.changed_values_map.as_map().set(canonical_key, value_value.view());
        }

        if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
            slots.added_set.as_set().add(canonical_key);
        }

        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(canonical_key);
        }
    }

    bool ref_child_target_modified = false;
    auto current_value = resolve_value_slot_const(vd);
    if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
        for_each_map_key_slot(current_value->as_map(), [&](View key, size_t slot) {
            ViewData child_vd = vd;
            child_vd.path.indices.push_back(slot);
            ViewData target_vd{};
            const bool target_modified =
                resolve_bound_target_view_data(child_vd, target_vd) && op_modified(target_vd, current_time);
            if (!target_modified) {
                return;
            }

            ref_child_target_modified = true;
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                auto changed_map = slots.changed_values_map.as_map();
                if (!map_slot_for_key(changed_map, key).has_value()) {
                    View child_value = op_value(child_vd);
                    if (child_value.valid()) {
                        changed_map.set(key, child_value);
                    }
                }
            }
        });
    }

    if (changed || !was_valid) {
        invalidate_python_delta_cache(vd);
        if (!directly_mutated_slots.empty()) {
            std::sort(directly_mutated_slots.begin(), directly_mutated_slots.end());
            directly_mutated_slots.erase(
                std::unique(directly_mutated_slots.begin(), directly_mutated_slots.end()),
                directly_mutated_slots.end());
            for (size_t slot : directly_mutated_slots) {
                ViewData child_vd = vd;
                child_vd.path.indices.push_back(slot);
                invalidate_python_value_cache(child_vd);
            }
        }
        if (nb::object* cache_slot = resolve_python_value_cache_slot(vd, true); cache_slot != nullptr) {
            *cache_slot = maybe_dst->to_python();
            vd.python_value_cache_slot = cache_slot;
        }
        stamp_time_paths(vd, current_time);
        notify_link_target_observers(vd, current_time);
    } else if (ref_child_target_modified) {
        invalidate_python_delta_cache(vd);
        notify_link_target_observers(vd, current_time);
    }
}

}  // namespace

void op_from_python_tsd_ref(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr) {
        return;
    }
    op_from_python_tsd_ref_impl(vd, src, current_time, current);
}

}  // namespace hgraph
