#include "ts_ops_internal.h"
#include <hgraph/types/ref.h>

namespace hgraph {

void op_copy_scalar(ViewData dst, const ViewData& src, engine_time_t current_time) {
    if (!op_valid(src)) {
        op_invalidate(dst);
        return;
    }
    op_set_value(dst, op_value(src), current_time);
}

void op_copy_ref(ViewData dst, const ViewData& src, engine_time_t current_time) {
    if (auto local = resolve_value_slot_const(src);
        local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
        const auto& local_ref = *static_cast<const TimeSeriesReference*>(local->data());
        if (!local_ref.is_empty()) {
            op_set_value(dst, *local, current_time);
            return;
        }

        // Empty local REF wrappers can be placeholders on linked inputs. Prefer
        // resolved REF payloads and, when needed, synthesize a bound reference
        // from the resolved target without round-tripping through Python.
        View ref_payload = op_value(src);
        if (ref_payload.valid() && ref_payload.schema() == ts_reference_meta()) {
            op_set_value(dst, ref_payload, current_time);
            return;
        }

        if (auto bound = resolve_bound_view_data(src); bound.has_value()) {
            value::Value bound_ref(ts_reference_meta());
            bound_ref.emplace();
            *static_cast<TimeSeriesReference*>(bound_ref.data()) = TimeSeriesReference::make(*bound);
            op_set_value(dst, bound_ref.view(), current_time);
            return;
        }

        op_set_value(dst, *local, current_time);
        return;
    }
    op_set_value(dst, op_value(src), current_time);
}

void op_copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time) {
    copy_tss(dst, src, current_time);
}

void op_copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time) {
    copy_tsd(dst, src, current_time);
}

void op_copy_tsl(ViewData dst, const ViewData& src, engine_time_t current_time) {
    const size_t n = std::min(op_list_size(dst), op_list_size(src));
    for (size_t i = 0; i < n; ++i) {
        TSView src_child = op_child_at(src, i, current_time);
        TSView dst_child = op_child_at(dst, i, current_time);
        if (!src_child || !dst_child) {
            continue;
        }
        if (!op_valid(src_child.view_data())) {
            op_invalidate(dst_child.view_data());
            continue;
        }
        copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
    }
}

void op_copy_tsb(ViewData dst, const ViewData& src, engine_time_t current_time) {
    const TSMeta* dst_meta = meta_at_path(dst.meta, dst.path.indices);
    if (dst_meta == nullptr) {
        return;
    }

    for (size_t i = 0; i < dst_meta->field_count(); ++i) {
        TSView src_child = op_child_at(src, i, current_time);
        TSView dst_child = op_child_at(dst, i, current_time);
        if (!src_child || !dst_child) {
            continue;
        }
        if (!op_valid(src_child.view_data())) {
            op_invalidate(dst_child.view_data());
            continue;
        }
        copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
    }
}

void copy_tss(ViewData dst, const ViewData& src, engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(dst);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_set()) {
        return;
    }

    auto dst_set = maybe_dst->as_set();
    const View src_value = op_value(src);

    std::vector<Value> source_values;
    if (src_value.valid() && src_value.is_set()) {
        auto src_set = src_value.as_set();
        source_values.reserve(src_set.size());
        for (View elem : src_set) {
            source_values.emplace_back(elem.clone());
        }
    }

    std::vector<Value> to_remove;
    to_remove.reserve(dst_set.size());
    for (View elem : dst_set) {
        bool keep = false;
        for (const auto& src_elem : source_values) {
            const View src_view = src_elem.view();
            if (src_view.schema() == elem.schema() && src_view.equals(elem)) {
                keep = true;
                break;
            }
        }
        if (!keep) {
            to_remove.emplace_back(elem.clone());
        }
    }

    std::vector<Value> to_add;
    to_add.reserve(source_values.size());
    for (const auto& src_elem : source_values) {
        if (!dst_set.contains(src_elem.view())) {
            to_add.emplace_back(src_elem.view().clone());
        }
    }

    auto slots = resolve_tss_delta_slots(dst);
    clear_tss_delta_if_new_tick(dst, current_time, slots);

    bool changed = false;
    for (const auto& elem : to_remove) {
        changed = dst_set.remove(elem.view()) || changed;
    }
    for (const auto& elem : to_add) {
        changed = dst_set.add(elem.view()) || changed;
    }

    if (slots.added_set.valid() && slots.added_set.is_set()) {
        auto added = slots.added_set.as_set();
        for (const auto& elem : to_add) {
            added.add(elem.view());
        }
    }
    if (slots.removed_set.valid() && slots.removed_set.is_set()) {
        auto removed = slots.removed_set.as_set();
        for (const auto& elem : to_remove) {
            removed.add(elem.view());
        }
    }

    if (changed) {
        stamp_time_paths(dst, current_time);
        notify_link_target_observers(dst, current_time);
    }
}

void copy_tsd(ViewData dst, const ViewData& src, engine_time_t current_time) {
    auto maybe_dst = resolve_value_slot_mut(dst);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || !maybe_dst->is_map()) {
        return;
    }
    auto dst_map = maybe_dst->as_map();

    const View src_value = op_value(src);
    const bool src_is_map = src_value.valid() && src_value.is_map();
    const auto src_map = src_is_map ? src_value.as_map() : value::MapView{};

    std::vector<Value> source_keys;
    if (src_is_map) {
        source_keys.reserve(src_map.size());
        for (View key : src_map.keys()) {
            source_keys.emplace_back(key.clone());
        }
    }

    std::vector<Value> to_remove;
    to_remove.reserve(dst_map.size());
    const auto keys_match = [](const View& a, const View& b) -> bool {
        if (!a.valid() || !b.valid()) {
            return false;
        }
        if (a.schema() != b.schema()) {
            return false;
        }
        return a.equals(b);
    };
    for (View key : dst_map.keys()) {
        bool keep = false;
        for (const auto& source_key : source_keys) {
            const View source_view = source_key.view();
            if (keys_match(source_view, key)) {
                keep = true;
                break;
            }
        }
        if (!keep) {
            to_remove.emplace_back(key.clone());
        }
    }

    auto slots = resolve_tsd_delta_slots(dst);
    clear_tsd_delta_if_new_tick(dst, current_time, slots);
    bool changed = false;
    const bool debug_copy_keys = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_COPY_TSD_KEYS");

    for (const auto& key : to_remove) {
        const auto removed_slot = map_slot_for_key(dst_map, key.view());
        bool removed_was_valid = false;
        if (removed_slot.has_value()) {
            ViewData child_vd = dst;
            child_vd.path.indices.push_back(*removed_slot);
            removed_was_valid = tsd_child_was_visible_before_removal(child_vd);
            record_tsd_removed_child_snapshot(dst, key.view(), child_vd, current_time);
        }
        dst_map.remove(key.view());
        changed = true;
        if (removed_slot.has_value()) {
            compact_tsd_child_time_slot(dst, *removed_slot);
            compact_tsd_child_delta_slot(dst, *removed_slot);
            compact_tsd_child_link_slot(dst, *removed_slot);
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().add(key.view());
        }
        if (!removed_was_valid && slots.added_set.valid() && slots.added_set.is_set()) {
            // Marker for key-set change without visible value removal.
            slots.added_set.as_set().add(key.view());
        }
        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            slots.changed_values_map.as_map().remove(key.view());
        }
        stamp_time_paths(dst, current_time);
    }

    const value::TypeMeta* value_type = dst_map.value_type();
    if (debug_copy_keys) {
        std::fprintf(stderr,
                     "[copy_tsd_keys] path=%s now=%lld src_keys=%zu dst_keys=%zu\n",
                     dst.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     source_keys.size(),
                     dst_map.size());
        for (View key : dst_map.keys()) {
            std::fprintf(stderr, "[copy_tsd_keys]  dst_key=%s\n", key.to_string().c_str());
        }
    }
    for (const auto& key : source_keys) {
        const bool existed = map_slot_for_key(dst_map, key.view()).has_value();
        if (debug_copy_keys) {
            std::fprintf(stderr,
                         "[copy_tsd_keys]  src_key=%s existed=%d\n",
                         key.view().to_string().c_str(),
                         existed ? 1 : 0);
        }
        if (!existed) {
            if (value_type == nullptr) {
                continue;
            }
            value::Value default_value(value_type);
            default_value.emplace();
            dst_map.set(key.view(), default_value.view());
            stamp_time_paths(dst, current_time);
            changed = true;
        }

        auto slot = map_slot_for_key(dst_map, key.view());
        if (!slot.has_value()) {
            continue;
        }
        Value canonical_key_value = canonical_map_key_for_slot(dst_map, *slot, key.view());
        const View canonical_key = canonical_key_value.view();

        ensure_tsd_child_time_slot(dst, *slot);
        ensure_tsd_child_delta_slot(dst, *slot);
        ensure_tsd_child_link_slot(dst, *slot);
        if (!existed && slots.added_set.valid() && slots.added_set.is_set()) {
            slots.added_set.as_set().add(canonical_key);
        }

        TSView src_child = op_child_by_key(src, key.view(), current_time);
        TSView dst_child = op_child_at(dst, *slot, current_time);
        if (!dst_child) {
            continue;
        }
        if (!src_child) {
            const engine_time_t before = op_last_modified_time(dst_child.view_data());
            if (const auto* map_schema = dst_map.schema(); map_schema != nullptr) {
                map_schema->ops().set_item(dst_map.data(), canonical_key.data(), nullptr, map_schema);
            }
            compact_tsd_child_link_slot(dst, *slot);
            op_invalidate(dst_child.view_data());
            const bool child_changed = op_last_modified_time(dst_child.view_data()) > before;
            if (child_changed) {
                changed = true;
            }
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                slots.changed_values_map.as_map().remove(canonical_key);
            }
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().remove(canonical_key);
            }
            continue;
        }

        const bool source_child_modified = op_modified(src_child.view_data(), current_time);
        bool source_child_valid = op_valid(src_child.view_data());
        if (src_is_map) {
            if (auto source_slot = map_slot_for_key(src_map, key.view()); source_slot.has_value()) {
                TSView source_slot_child = op_child_at(src, *source_slot, current_time);
                if (!source_slot_child || !op_valid(source_slot_child.view_data())) {
                    source_child_valid = false;
                }
            }
        }
        if (source_child_valid && src_child.view_data().uses_link_target) {
            ViewData source_bound_target{};
            if (!resolve_bound_target_view_data(src_child.view_data(), source_bound_target) ||
                !op_valid(source_bound_target)) {
                source_child_valid = false;
            }
        }
        if (debug_copy_keys) {
            const TSMeta* src_child_meta = op_ts_meta(src_child.view_data());
            View src_child_value = op_value(src_child.view_data());
            std::string src_child_value_schema{"<invalid>"};
            std::string src_child_value_repr{"<invalid>"};
            if (src_child_value.valid()) {
                if (const auto* schema = src_child_value.schema(); schema != nullptr) {
                    if (schema->name != nullptr) {
                        src_child_value_schema = schema->name;
                    } else {
                        src_child_value_schema = "<null_name>";
                    }
                }
                try {
                    src_child_value_repr = src_child_value.to_string();
                } catch (...) {
                    src_child_value_repr = "<to_string_error>";
                }
            }
            std::fprintf(stderr,
                         "[copy_tsd_keys]   key=%s src_mod=%d src_valid=%d src_uses_lt=%d src_kind=%d src_value_schema=%s src_value=%s\n",
                         key.view().to_string().c_str(),
                         source_child_modified ? 1 : 0,
                         source_child_valid ? 1 : 0,
                         src_child.view_data().uses_link_target ? 1 : 0,
                         src_child_meta != nullptr ? static_cast<int>(src_child_meta->kind) : -1,
                         src_child_value_schema.c_str(),
                         src_child_value_repr.c_str());
        }
        if (!source_child_modified && source_child_valid && existed) {
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                slots.changed_values_map.as_map().remove(canonical_key);
            }
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                slots.removed_set.as_set().remove(canonical_key);
            }
            continue;
        }

        const engine_time_t before = op_last_modified_time(dst_child.view_data());
        if (!source_child_valid) {
            if (const auto* map_schema = dst_map.schema(); map_schema != nullptr) {
                map_schema->ops().set_item(dst_map.data(), canonical_key.data(), nullptr, map_schema);
            }
            compact_tsd_child_link_slot(dst, *slot);
            op_invalidate(dst_child.view_data());
        } else {
            copy_view_data_value_impl(dst_child.view_data(), src_child.view_data(), current_time);
        }
        if (debug_copy_keys) {
            const bool dst_valid = op_valid(dst_child.view_data());
            View dst_value = op_value(dst_child.view_data());
            std::string dst_value_repr{"<invalid>"};
            if (dst_value.valid()) {
                try {
                    dst_value_repr = dst_value.to_string();
                } catch (...) {
                    dst_value_repr = "<to_string_error>";
                }
            }
            std::fprintf(stderr,
                         "[copy_tsd_keys]   key=%s dst_valid=%d dst_value=%s\n",
                         key.view().to_string().c_str(),
                         dst_valid ? 1 : 0,
                         dst_value_repr.c_str());
        }
        const bool child_changed = op_last_modified_time(dst_child.view_data()) > before;
        if (child_changed || !existed) {
            changed = true;
        }

        if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
            if (child_changed || !existed) {
                if (tsd_child_was_visible_before_removal(dst_child.view_data())) {
                    View child_value = op_value(dst_child.view_data());
                    if (child_value.valid()) {
                        slots.changed_values_map.as_map().set(canonical_key, child_value);
                    } else {
                        slots.changed_values_map.as_map().remove(canonical_key);
                    }
                } else {
                    // Structural key presence without visible payload should not
                    // materialize a changed-value delta entry.
                    slots.changed_values_map.as_map().remove(canonical_key);
                }
            } else {
                slots.changed_values_map.as_map().remove(canonical_key);
            }
        }
        if (slots.removed_set.valid() && slots.removed_set.is_set()) {
            slots.removed_set.as_set().remove(canonical_key);
        }
    }

    if (changed) {
        notify_link_target_observers(dst, current_time);
    }
}

void copy_view_data_value_impl(ViewData dst, const ViewData& src, engine_time_t current_time) {
    const TSMeta* dst_meta = meta_at_path(dst.meta, dst.path.indices);
    const TSMeta* src_meta = meta_at_path(src.meta, src.path.indices);
    if (dst_meta == nullptr || src_meta == nullptr) {
        return;
    }

    invalidate_python_value_cache(dst);

    if (dispatch_meta_ops(dst_meta) != dispatch_meta_ops(src_meta)) {
        throw std::runtime_error("copy_view_data_value: source/destination schema kinds differ");
    }

    const ts_ops* dst_ops = get_ts_ops(dst_meta);
    if (dst_ops->copy_value == nullptr) {
        return;
    }
    dst_ops->copy_value(dst, src, current_time);
}

}  // namespace hgraph
