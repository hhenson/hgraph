#pragma once

#include "ts_ops_internal.h"

namespace hgraph {

template <bool DeclaredRefElement,
          bool AssumeNonRefElement,
          bool HasDeclaredNestedElement,
          bool DeclaredNestedElement,
          typename KeyInAddedFn,
          typename KeyInRemovedFn,
          typename KeyInChangedFn,
          typename ResolvePreviousMapViewFn,
          typename PreviousMapEntryVisibleFn>
void tsd_emit_removed_phase(const ViewData& vd,
                            const ViewData* data,
                            const TSMeta* current,
                            const TSMeta* element_meta,
                            engine_time_t current_time,
                            bool sampled_like,
                            bool debug_tsd_delta,
                            bool debug_ref_payload,
                            const View& added_keys,
                            const View& removed_keys,
                            KeyInAddedFn&& key_in_added_set,
                            KeyInRemovedFn&& key_in_removed_set,
                            KeyInChangedFn&& key_in_changed_map,
                            ResolvePreviousMapViewFn&& resolve_previous_map_view,
                            PreviousMapEntryVisibleFn&& previous_map_entry_visible,
                            nb::dict& delta_out) {
            (void)HasDeclaredNestedElement;
            (void)DeclaredNestedElement;
            const auto meta_is_ref_wrapper = [](const TSMeta* meta) -> bool {
                const ts_ops* ops = dispatch_meta_ops(meta);
                return ops != nullptr && ops->value == &op_value_ref;
            };
            const bool element_is_ref_wrapper = [&]() -> bool {
                if constexpr (DeclaredRefElement) {
                    return true;
                }
                if constexpr (AssumeNonRefElement) {
                    // Declared non-ref scenarios can still resolve through REF wrappers.
                    return meta_is_ref_wrapper(element_meta);
                }
                return meta_is_ref_wrapper(element_meta);
            }();
            const bool ref_link_target_input = element_is_ref_wrapper && vd.uses_link_target;
            const TSMeta* ref_element_meta =
                element_is_ref_wrapper && element_meta != nullptr ? element_meta->element_ts() : nullptr;
            const ts_ops* ref_element_ops = dispatch_meta_ops(ref_element_meta);
            const bool ref_targets_container =
                ref_element_ops != nullptr &&
                (ref_element_ops->dict != nullptr ||
                 ref_element_ops->set != nullptr ||
                 ref_element_ops->list != nullptr ||
                 ref_element_ops->bundle != nullptr);
            if (!sampled_like && removed_keys.valid() && removed_keys.is_set()) {
                auto set = removed_keys.as_set();
                const bool has_added_set = added_keys.valid() && added_keys.is_set();
                nb::object remove = get_remove();
                const auto key_visible_in_previous_view = [&](const View& key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        (void)current;
                        if (ref_link_target_input) {
                            return false;
                        }
                        return true;
                    }
                    return previous_map_entry_visible(previous, previous_map, key);
                };
                const auto key_present_in_previous_map = [&](const View& key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        return false;
                    }
                    return map_slot_for_key(previous_map, key).has_value();
                };
                const auto key_visible_in_removed_snapshot = [&](const View& key) -> bool {
                    auto snapshot = resolve_tsd_removed_child_snapshot(*data, key, current_time);
                    if (!snapshot.has_value()) {
                        return false;
                    }

                    ViewData snapshot_view = snapshot->view_data();
                    const TSMeta* snapshot_meta = snapshot->ts_meta();
                    if (meta_is_ref_wrapper(snapshot_meta)) {
                        nb::object payload = tsd_ref_view_payload_to_python(
                            snapshot_view,
                            snapshot_meta,
                            current_time,
                            true,
                            debug_ref_payload);
                        return !payload.is_none();
                    }

                    if (!op_valid(snapshot_view)) {
                        return false;
                    }
                    return !op_to_python(snapshot_view).is_none();
                };
                const auto removed_snapshot_ref_target_written = [&](const View& key) -> bool {
                    auto snapshot = resolve_tsd_removed_child_snapshot(*data, key, current_time);
                    if (!snapshot.has_value()) {
                        return false;
                    }

                    ViewData snapshot_view = snapshot->view_data();
                    ViewData bound_target{};
                    if (!resolve_bound_target_view_data(snapshot_view, bound_target)) {
                        return false;
                    }
                    return op_last_modified_time(bound_target) > MIN_DT;
                };
                for (View key : set) {
                    const bool in_added_set = key_in_added_set(key);
                    const bool in_changed_map = key_in_changed_map(key);
                    const bool in_removed_set = key_in_removed_set(key);
                    const bool seen_visible_before = has_tsd_visible_key_history(*data, key);
                    if (debug_tsd_delta) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] remove_probe path=%s key=%s has_added=%d in_added=%d in_removed=%d in_changed=%d seen_visible=%d\n",
                                     vd.path.to_string().c_str(),
                                     key.to_string().c_str(),
                                     has_added_set ? 1 : 0,
                                     in_added_set ? 1 : 0,
                                     in_removed_set ? 1 : 0,
                                     in_changed_map ? 1 : 0,
                                     seen_visible_before ? 1 : 0);
                    }
                    const bool was_visible = key_visible_in_previous_view(key);
                    if (in_added_set &&
                        in_removed_set &&
                        !in_changed_map &&
                        !ref_link_target_input &&
                        !seen_visible_before) {
                        if (debug_tsd_delta) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] remove_skip_structural_unseen path=%s key=%s\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        continue;
                    }
                    if (in_added_set &&
                        !in_changed_map &&
                        !was_visible &&
                        !key_present_in_previous_map(key) &&
                        !ref_link_target_input) {
                        if (debug_tsd_delta) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] remove_skip_added_removed path=%s key=%s\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        continue;
                    }
                    if (!was_visible) {
                        if (in_added_set && key_present_in_previous_map(key)) {
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_emit_added_prev_present path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                            delta_out[key.to_python()] = remove;
                            continue;
                        }
                        if (!in_added_set && key_present_in_previous_map(key)) {
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_emit_prev_present path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                            delta_out[key.to_python()] = remove;
                            continue;
                        }
                        if (ref_link_target_input && !in_changed_map) {
                            const bool visible_in_snapshot = key_visible_in_removed_snapshot(key);
                            const bool target_written =
                                in_added_set && !visible_in_snapshot && removed_snapshot_ref_target_written(key);
                            const bool seen_visible_before =
                                in_added_set && !visible_in_snapshot && has_tsd_visible_key_history(*data, key);
                            if (!in_added_set || visible_in_snapshot || target_written || seen_visible_before) {
                                if (debug_tsd_delta) {
                                    std::fprintf(stderr,
                                                 "[tsd_delta_dbg] remove_emit_ref_link_target path=%s key=%s snapshot_visible=%d target_written=%d seen_visible=%d\n",
                                                 vd.path.to_string().c_str(),
                                                 key.to_string().c_str(),
                                                 visible_in_snapshot ? 1 : 0,
                                                 target_written ? 1 : 0,
                                                 seen_visible_before ? 1 : 0);
                                }
                                delta_out[key.to_python()] = remove;
                                continue;
                            }
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_skip_ref_link_target_invisible path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                        }
                        if (ref_targets_container) {
                            if (debug_tsd_delta) {
                                std::fprintf(stderr,
                                             "[tsd_delta_dbg] remove_emit_container_ref path=%s key=%s\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str());
                            }
                            delta_out[key.to_python()] = remove;
                            continue;
                        }
                        if (debug_tsd_delta) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] remove_skip_not_visible path=%s key=%s\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        continue;
                    }
                    if (debug_tsd_delta) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] remove_emit path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     key.to_string().c_str());
                    }
                    delta_out[key.to_python()] = remove;
                }
            }
}

template <bool DeclaredRefElement,
          bool AssumeNonRefElement,
          bool HasDeclaredNestedElement,
          bool DeclaredNestedElement,
          typename KeyInAddedFn,
          typename KeyInRemovedFn,
          typename KeyInChangedFn,
          typename RefChildReboundForKeyFn,
          typename RefTargetModifiedThisTickFn,
          typename ResolvePreviousMapViewFn,
          typename PreviousMapEntryVisibleFn>
void tsd_emit_phase(const ViewData& vd,
                    const ViewData* data,
                    engine_time_t current_time,
                    bool sampled_like,
                    bool include_unmodified_override,
                    bool has_changed_map,
                    bool nested_element,
                    bool single_changed_key,
                    bool has_added_keys,
                    bool has_removed_keys,
                    bool debug_tsd_delta,
                    bool debug_ref_payload,
                    const View& changed_values,
                    const View& added_keys,
                    const View& removed_keys,
                    const value::MapView& value_map,
                    KeyInAddedFn&& key_in_added_set,
                    KeyInRemovedFn&& key_in_removed_set,
                    KeyInChangedFn&& key_in_changed_map,
                    RefChildReboundForKeyFn&& ref_child_rebound_for_key,
                    RefTargetModifiedThisTickFn&& ref_target_modified_this_tick,
                    ResolvePreviousMapViewFn&& resolve_previous_map_view,
                    PreviousMapEntryVisibleFn&& previous_map_entry_visible,
                    nb::dict& delta_out) {
            const bool use_nested_element = [&]() {
                if constexpr (HasDeclaredNestedElement) {
                    return DeclaredNestedElement;
                }
                return nested_element;
            }();
            const auto child_is_ref = [](const ViewData& child) {
                if constexpr (DeclaredRefElement) {
                    return true;
                }
                if constexpr (AssumeNonRefElement) {
                    // Declared non-ref TSD element paths can still resolve to REF
                    // children after rebinding through wrapper indirection.
                    return child.ops != nullptr && child.ops->value == &op_value_ref;
                }
                return child.ops != nullptr && child.ops->value == &op_value_ref;
            };
            const auto child_is_scalar_like = [](const ViewData& child) {
                if constexpr (HasDeclaredNestedElement) {
                    return !DeclaredNestedElement;
                }
                if (child.ops == nullptr) {
                    return true;
                }
                return child.ops->dict == nullptr &&
                       child.ops->set == nullptr &&
                       child.ops->list == nullptr &&
                       child.ops->bundle == nullptr;
            };
            if (!sampled_like && has_changed_map) {
                const auto changed_map = changed_values.as_map();
                const bool debug_changed_map = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_TSD_CHANGED_MAP");
                for_each_map_key_slot(value_map, [&](View key, size_t /*slot_index*/) {
                    if (!key_in_changed_map(key)) {
                        return;
                    }
                    auto slot = map_slot_for_key(value_map, key);
                    if (!slot.has_value()) {
                        if (debug_changed_map) {
                            std::fprintf(stderr,
                                         "[tsd_changed_map] path=%s key=%s slot=<none>\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str());
                        }
                        return;
                    }
                    View changed_entry = changed_map.at(key);
                    DeltaView changed_entry_delta = DeltaView::from_stored(changed_entry);
                    const bool changed_entry_has_delta = tsd_has_delta_payload(changed_entry_delta);

                    ViewData child = *data;
                    child.path.indices.push_back(*slot);
                    bind_view_data_ops(child);
                    const TSMeta* child_meta = op_ts_meta(child);
                    const bool child_ref = child_is_ref(child);
                    const TSMeta* child_ref_meta = child_ref ? child_meta : nullptr;

                    if (use_nested_element) {
                        const bool child_valid = op_valid(child);
                        const bool ref_child_rebound =
                            child_ref &&
                            ref_child_rebound_for_key(child, key);
                        const bool ref_target_modified_now =
                            child_ref &&
                            ref_target_modified_this_tick(child);
                        const bool include_unmodified_ref_payload =
                            key_in_added_set(key) ||
                            !child_valid ||
                            ref_child_rebound ||
                            !ref_target_modified_now ||
                            !changed_entry_has_delta;
                        if (child_ref) {
                            nb::object child_delta =
                                tsd_ref_view_payload_to_python(
                                    child,
                                    child_ref_meta,
                                    current_time,
                                    include_unmodified_ref_payload,
                                    debug_ref_payload);
                            if (debug_changed_map) {
                                std::fprintf(stderr,
                                             "[tsd_changed_map] path=%s key=%s slot=%zu child_kind=REF include_unmod=%d out_none=%d\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str(),
                                             *slot,
                                             include_unmodified_ref_payload ? 1 : 0,
                                             child_delta.is_none() ? 1 : 0);
                            }
                            if (!child_delta.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta);
                            }
                            return;
                        }
                        if (!child_valid) {
                            if (debug_changed_map) {
                                std::fprintf(stderr,
                                             "[tsd_changed_map] path=%s key=%s slot=%zu child_valid=0\n",
                                             vd.path.to_string().c_str(),
                                             key.to_string().c_str(),
                                             *slot);
                            }
                            return;
                        }
                        const bool include_unmodified_nested_payload = key_in_added_set(key);
                        nb::object child_delta;
                        if (include_unmodified_nested_payload) {
                            // Added nested keys should emit full visible snapshots,
                            // not only the nested native delta payload.
                            ViewData sampled_child = child;
                            sampled_child.sampled = true;
                            child_delta = op_delta_to_python(sampled_child, current_time);
                            if (child_delta.is_none()) {
                                child_delta = op_to_python(child);
                            }
                        } else {
                            child_delta = op_delta_to_python(child, current_time);
                        }
                        if (debug_changed_map) {
                            std::fprintf(stderr,
                                         "[tsd_changed_map] path=%s key=%s slot=%zu child_kind=%d include_unmod=%d out_none=%d\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str(),
                                         *slot,
                                         child_meta != nullptr ? static_cast<int>(child_meta->kind) : -1,
                                         include_unmodified_nested_payload ? 1 : 0,
                                         child_delta.is_none() ? 1 : 0);
                        }
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                    } else {
                        const bool child_valid = op_valid(child);
                        if (child_ref) {
                            const bool include_unmodified_ref_payload =
                                key_in_added_set(key) ||
                                !child_valid ||
                                ref_child_rebound_for_key(child, key) ||
                                !has_tsd_visible_key_history(*data, key);
                            nb::object child_delta_py =
                                tsd_ref_view_payload_to_python(
                                    child,
                                    child_ref_meta,
                                    current_time,
                                    include_unmodified_ref_payload,
                                    debug_ref_payload);
                            const TSMeta* ref_element_meta =
                                child_ref_meta != nullptr ? child_ref_meta->element_ts() : nullptr;
                            const ts_ops* ref_target_ops = dispatch_meta_ops(ref_element_meta);
                            const bool scalar_ref_target =
                                ref_target_ops == nullptr ||
                                (ref_target_ops->dict == nullptr &&
                                 ref_target_ops->set == nullptr &&
                                 ref_target_ops->list == nullptr &&
                                 ref_target_ops->bundle == nullptr);
                            if (child_delta_py.is_none() &&
                                !include_unmodified_ref_payload &&
                                changed_entry_has_delta &&
                                scalar_ref_target &&
                                vd.path.port_type == PortType::INPUT &&
                                !has_added_keys &&
                                !has_removed_keys &&
                                single_changed_key) {
                                child_delta_py = tsd_ref_view_payload_to_python(
                                    child,
                                    child_ref_meta,
                                    current_time,
                                    true,
                                    debug_ref_payload);
                            }
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                            }
                            return;
                        }
                        if (!child_valid) {
                            return;
                        }
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (tsd_has_delta_payload(child_delta)) {
                            nb::object child_delta_py = nb::none();
                            if (child_delta.valid() && child_delta.schema() == ts_reference_meta()) {
                                child_delta_py = tsd_ref_view_payload_to_python(
                                    child,
                                    child_ref_meta,
                                    current_time,
                                    key_in_added_set(key),
                                    debug_ref_payload);
                                if (child_delta_py.is_none()) {
                                    child_delta_py = op_to_python(child);
                                }
                            } else {
                                child_delta_py = computed_delta_to_python_with_refs(child_delta, current_time);
                            }
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                            }
                            return;
                        }

                        // changed_values already identified an effective visible value change
                        // (for example precedence/carry updates). Emit current value when the
                        // scalar child has no native delta payload.
                        View child_value = op_value(child);
                        if (!child_value.valid()) {
                            return;
                        }
                        nb::object child_value_py = nb::none();
                        if (child_value.schema() == ts_reference_meta()) {
                            child_value_py = tsd_ref_view_payload_to_python(
                                child,
                                child_ref_meta,
                                current_time,
                                key_in_added_set(key),
                                debug_ref_payload);
                            if (child_value_py.is_none()) {
                                child_value_py = op_to_python(child);
                            }
                        } else {
                            child_value_py = stored_delta_to_python_with_refs(child_value, current_time);
                        }
                        if (!child_value_py.is_none()) {
                            delta_out[key.to_python()] = std::move(child_value_py);
                        }
                    }
                });
            } else {
                const bool include_unmodified = sampled_like || include_unmodified_override;
                const auto key_visible_in_previous_map = [&](View key) -> std::optional<bool> {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        return std::nullopt;
                    }
                    return previous_map_entry_visible(previous, previous_map, key);
                };
                const auto key_present_in_previous_map_for_sample = [&](View key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        return false;
                    }
                    return map_slot_for_key(previous_map, key).has_value();
                };
                const auto key_is_structural_add = [&](View key) {
                    if (!key_in_added_set(key)) {
                        return false;
                    }
                    if (key_in_removed_set(key)) {
                        return false;
                    }
                    const auto was_visible = key_visible_in_previous_map(key);
                    if (!was_visible.has_value()) {
                        return false;
                    }
                    return !(*was_visible);
                };
                for_each_map_key_slot(value_map, [&](View key, size_t slot) {
                    if (debug_tsd_delta) {
                        bool entry_valid = false;
                        bool entry_is_map = false;
                        size_t entry_map_size = 0;
                        std::string key_s{"<key>"};
                        std::string entry_s{"<entry>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        try {
                            View entry = value_map.at(key);
                            entry_valid = entry.valid();
                            entry_is_map = entry.valid() && entry.is_map();
                            entry_map_size = entry_is_map ? entry.as_map().size() : 0;
                            entry_s = entry.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] value_entry path=%s key=%s slot=%zu valid=%d is_map=%d map_size=%zu value=%s\n",
                                     data->path.to_string().c_str(),
                                     key_s.c_str(),
                                     slot,
                                     entry_valid ? 1 : 0,
                                     entry_is_map ? 1 : 0,
                                     entry_map_size,
                                     entry_s.c_str());
                    }
                    ViewData child = *data;
                    child.path.indices.push_back(slot);
                    child.sampled = false;
                    if (include_unmodified) {
                        bind_view_data_ops(child);
                        const TSMeta* child_meta = op_ts_meta(child);
                        const bool child_ref = child_is_ref(child);
                        const TSMeta* child_ref_meta = child_ref ? child_meta : nullptr;
                        const bool child_valid = op_valid(child);
                        if (child_ref) {
                            nb::object entry_py = nb::none();
                            if (child_valid) {
                                entry_py = tsd_ref_view_payload_to_python(
                                    child,
                                    child_ref_meta,
                                    current_time,
                                    true,
                                    debug_ref_payload);
                            }
                            if (entry_py.is_none()) {
                                if constexpr (DeclaredRefElement) {
                                    View current_entry = value_map.at(key);
                                    if (current_entry.valid() && current_entry.schema() == ts_reference_meta()) {
                                        entry_py = current_entry.to_python();
                                    }
                                }
                            }
                            if (entry_py.is_none()) {
                                if (key_present_in_previous_map_for_sample(key) || !key_in_added_set(key)) {
                                    entry_py = get_remove();
                                }
                            }
                            if (entry_py.is_none()) {
                                return;
                            }
                            delta_out[key.to_python()] = std::move(entry_py);
                            return;
                        }
                        if (!child_valid) {
                            return;
                        }

                        ViewData sampled_child = child;
                        sampled_child.sampled = true;
                        nb::object entry_py = op_delta_to_python(sampled_child, current_time);
                        if (entry_py.is_none()) {
                            entry_py = op_to_python(child);
                        }
                        if (entry_py.is_none()) {
                            return;
                        }
                        delta_out[key.to_python()] = std::move(entry_py);
                        return;
                    }
                    bind_view_data_ops(child);
                    const TSMeta* child_meta = op_ts_meta(child);
                    const bool child_ref = child_is_ref(child);
                    const TSMeta* child_ref_meta = child_ref ? child_meta : nullptr;
                    const bool child_valid = op_valid(child);
                    if (!child_valid &&
                        !child_ref) {
                        return;
                    }
                    if (debug_tsd_delta && !include_unmodified) {
                        const engine_time_t child_last = op_last_modified_time(child);
                        std::string key_s{"<key>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] child_probe path=%s key=%s slot=%zu last=%lld now=%lld\n",
                                     child.path.to_string().c_str(),
                                     key_s.c_str(),
                                     slot,
                                     static_cast<long long>(child_last.time_since_epoch().count()),
                                     static_cast<long long>(current_time.time_since_epoch().count()));
                    }
                    const bool forced_from_changed_map = !include_unmodified && key_in_changed_map(key);
                    const bool forced_from_structural_add =
                        !include_unmodified &&
                        (key_is_structural_add(key) || (key_in_added_set(key) && !key_in_removed_set(key)));
                    const bool ref_child_rebound =
                        !include_unmodified &&
                        child_ref &&
                        ref_child_rebound_for_key(child, key);
                    bool child_modified =
                        include_unmodified || forced_from_changed_map || forced_from_structural_add || ref_child_rebound ||
                        op_modified(child, current_time);
                    if (debug_tsd_delta && !include_unmodified) {
                        std::string key_s{"<key>"};
                        try {
                            key_s = key.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] child_flags path=%s key=%s child_kind=%d in_added=%d in_removed=%d forced_changed=%d forced_add=%d rebound=%d\n",
                                     child.path.to_string().c_str(),
                                     key_s.c_str(),
                                     child_meta != nullptr ? static_cast<int>(child_meta->kind) : -1,
                                     key_in_added_set(key) ? 1 : 0,
                                     key_in_removed_set(key) ? 1 : 0,
                                     forced_from_changed_map ? 1 : 0,
                                     forced_from_structural_add ? 1 : 0,
                                     ref_child_rebound ? 1 : 0);
                    }
                    if (!include_unmodified && !child_modified &&
                        child_ref) {
                        child_modified = ref_target_modified_this_tick(child);
                    }
                    if (!include_unmodified && !child_modified) {
                        return;
                    }
                    if (child_ref) {
                        nb::object child_delta =
                            tsd_ref_view_payload_to_python(
                                child,
                                child_ref_meta,
                                current_time,
                                include_unmodified || forced_from_changed_map || forced_from_structural_add ||
                                    ref_child_rebound,
                                debug_ref_payload);
                        if (child_delta.is_none()) {
                            const auto was_visible = key_visible_in_previous_map(key);
                            if (was_visible.has_value() && *was_visible) {
                                child_delta = get_remove();
                            }
                        }
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                        return;
                    }
                    if (child_is_scalar_like(child)) {
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (tsd_has_delta_payload(child_delta)) {
                            nb::object child_delta_py = nb::none();
                            if (child_delta.valid() && child_delta.schema() == ts_reference_meta()) {
                                child_delta_py = tsd_ref_view_payload_to_python(
                                    child,
                                    nullptr,
                                    current_time,
                                    forced_from_changed_map || forced_from_structural_add,
                                    debug_ref_payload);
                                if (child_delta_py.is_none()) {
                                    child_delta_py = op_to_python(child);
                                }
                            } else {
                                child_delta_py = computed_delta_to_python_with_refs(child_delta, current_time);
                            }
                            if (!child_delta_py.is_none()) {
                                delta_out[key.to_python()] = std::move(child_delta_py);
                                return;
                            }
                        }
                        if (forced_from_changed_map || forced_from_structural_add) {
                            View child_value = op_value(child);
                            if (child_value.valid()) {
                                nb::object child_value_py = nb::none();
                                if (child_value.schema() == ts_reference_meta()) {
                                    child_value_py = tsd_ref_view_payload_to_python(
                                        child,
                                        nullptr,
                                        current_time,
                                        true,
                                        debug_ref_payload);
                                    if (child_value_py.is_none()) {
                                        child_value_py = op_to_python(child);
                                    }
                                } else {
                                    child_value_py = stored_delta_to_python_with_refs(child_value, current_time);
                                }
                                if (!child_value_py.is_none()) {
                                    delta_out[key.to_python()] = std::move(child_value_py);
                                }
                            }
                        }
                    } else {
                        nb::object child_delta = nb::none();
                        if (forced_from_structural_add) {
                            // Structural adds should materialize the full visible child payload.
                            child_delta = op_to_python(child);
                            if (child_delta.is_none()) {
                                ViewData sampled_child = child;
                                sampled_child.sampled = true;
                                child_delta = op_delta_to_python(sampled_child, current_time);
                            }
                        } else {
                            child_delta = op_delta_to_python(child, current_time);
                            if (child_delta.is_none() && forced_from_changed_map) {
                                child_delta = op_to_python(child);
                            }
                        }
                        if (!child_delta.is_none()) {
                            delta_out[key.to_python()] = std::move(child_delta);
                        }
                    }
                });
            }
}

template <bool DeclaredRefElement,
          bool AssumeNonRefElement,
          bool HasDeclaredNestedElement,
          bool DeclaredNestedElement,
          typename KeyInAddedFn,
          typename RefChildReboundForKeyFn,
          typename RefTargetModifiedThisTickFn>
void tsd_emit_backfill_phase(const ViewData& vd,
                             const ViewData* data,
                             engine_time_t current_time,
                             bool sampled_like,
                             bool debug_ref_payload,
                             const View& changed_values,
                             const value::MapView& value_map,
                             KeyInAddedFn&& key_in_added_set,
                             RefChildReboundForKeyFn&& ref_child_rebound_for_key,
                             RefTargetModifiedThisTickFn&& ref_target_modified_this_tick,
                             nb::dict& delta_out) {
            const auto child_is_ref = [](const ViewData& child) {
                if constexpr (DeclaredRefElement) {
                    return true;
                }
                if constexpr (AssumeNonRefElement) {
                    return child.ops != nullptr && child.ops->value == &op_value_ref;
                }
                return child.ops != nullptr && child.ops->value == &op_value_ref;
            };
            const auto child_is_scalar_like = [](const ViewData& child) {
                if constexpr (HasDeclaredNestedElement) {
                    return !DeclaredNestedElement;
                }
                if (child.ops == nullptr) {
                    return true;
                }
                return child.ops->dict == nullptr &&
                       child.ops->set == nullptr &&
                       child.ops->list == nullptr &&
                       child.ops->bundle == nullptr;
            };
            if (!sampled_like && changed_values.valid() && changed_values.is_map()) {
                const auto changed_map = changed_values.as_map();
                for (View key : changed_map.keys()) {
                    nb::object py_key = key.to_python();
                    if (PyDict_Contains(delta_out.ptr(), py_key.ptr()) == 1) {
                        continue;
                    }
                    auto slot = map_slot_for_key(value_map, key);
                    if (!slot.has_value()) {
                        continue;
                    }
                    ViewData child = *data;
                    child.path.indices.push_back(*slot);
                    bind_view_data_ops(child);
                    const TSMeta* child_meta = op_ts_meta(child);
                    const bool child_ref = child_is_ref(child);
                    const TSMeta* child_ref_meta = child_ref ? child_meta : nullptr;
                    const bool in_added_set = key_in_added_set(key);
                    const bool ref_child_rebound =
                        child_ref &&
                        ref_child_rebound_for_key(child, key);
                    bool child_modified_now = op_modified(child, current_time);
                    if (!child_modified_now &&
                        child_ref) {
                        child_modified_now = ref_target_modified_this_tick(child);
                    }
                    const bool child_valid = op_valid(child);
                    if (!in_added_set &&
                        !child_modified_now &&
                        !ref_child_rebound &&
                        child_valid) {
                        continue;
                    }

                    nb::object entry = nb::none();
                    if (child_ref) {
                        entry = tsd_ref_view_payload_to_python(
                            child,
                            child_ref_meta,
                            current_time,
                            in_added_set ||
                                ref_child_rebound ||
                                !child_valid,
                            debug_ref_payload);
                    } else if (child_is_scalar_like(child)) {
                        if (!child_valid) {
                            continue;
                        }
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (tsd_has_delta_payload(child_delta)) {
                            if (child_delta.valid() && child_delta.schema() == ts_reference_meta()) {
                                entry = tsd_ref_view_payload_to_python(
                                    child,
                                    nullptr,
                                    current_time,
                                    in_added_set || ref_child_rebound,
                                    debug_ref_payload);
                                if (entry.is_none()) {
                                    entry = op_to_python(child);
                                }
                            } else {
                                entry = computed_delta_to_python_with_refs(child_delta, current_time);
                            }
                        }
                        if (entry.is_none()) {
                            View child_value = op_value(child);
                            if (child_value.valid()) {
                                if (child_value.schema() == ts_reference_meta()) {
                                    entry = tsd_ref_view_payload_to_python(
                                        child,
                                        nullptr,
                                        current_time,
                                        true,
                                        debug_ref_payload);
                                    if (entry.is_none()) {
                                        entry = op_to_python(child);
                                    }
                                } else {
                                    entry = stored_delta_to_python_with_refs(child_value, current_time);
                                }
                            }
                        }
                    } else {
                        if (!child_valid) {
                            continue;
                        }
                        entry = op_delta_to_python(child, current_time);
                        if (entry.is_none()) {
                            entry = op_to_python(child);
                        }
                    }

                    if (!entry.is_none()) {
                        delta_out[std::move(py_key)] = std::move(entry);
                    }
                }
            }
}

}  // namespace hgraph
