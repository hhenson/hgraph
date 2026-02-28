#include "ts_ops_internal.h"
#include "python_ops_delta_tsd_map_phases.h"

namespace hgraph {

namespace {

template <bool DeclaredRefElement, bool HasDeclaredNestedElement, bool DeclaredNestedElement>
void tsd_emit_map_delta_impl(const ViewData& vd,
                             const ViewData* data,
                             const TSMeta* current,
                             engine_time_t current_time,
                             bool wrapper_modified,
                             bool resolved_modified,
                             bool debug_tsd_delta,
                             bool debug_ref_payload,
                             const View& changed_values,
                             const View& added_keys,
                             const View& removed_keys,
                             nb::dict& delta_out) {
        constexpr bool AssumeNonRefElement = HasDeclaredNestedElement && !DeclaredRefElement;
        auto current_value = resolve_value_slot_const(*data);
        if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
            const auto value_map = current_value->as_map();
            const TSMeta* element_meta = current->element_ts();
            const bool nested_element = [&]() {
                if constexpr (HasDeclaredNestedElement) {
                    return DeclaredNestedElement;
                }
                return element_meta != nullptr && !dispatch_meta_is_scalar_like(element_meta);
            }();

            const engine_time_t rebind_time = rebind_time_for_view(vd);
            const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
            const bool has_changed_map =
                changed_values.valid() && changed_values.is_map() && changed_values.as_map().size() > 0;
            const bool single_changed_key =
                changed_values.valid() && changed_values.is_map() && changed_values.as_map().size() == 1;
            const bool has_added_keys =
                added_keys.valid() && added_keys.is_set() && added_keys.as_set().size() > 0;
            const bool has_removed_keys =
                removed_keys.valid() && removed_keys.is_set() && removed_keys.as_set().size() > 0;
            const auto key_in_added_set = [&added_keys](View key) {
                return view_is_set_and_contains_key_relaxed(added_keys, key);
            };
            const auto key_in_removed_set = [&removed_keys](View key) {
                return view_is_set_and_contains_key_relaxed(removed_keys, key);
            };
            const auto key_in_changed_map = [&changed_values](View key) {
                return changed_values.valid() &&
                       changed_values.is_map() &&
                       map_slot_for_key(changed_values.as_map(), key).has_value();
            };
            const auto ref_target_modified_this_tick = [&](const ViewData& ref_child) -> bool {
                ViewData target{};
                return resolve_bound_target_view_data(ref_child, target) &&
                       op_modified(target, current_time);
            };
            const auto meta_is_ref_wrapper = [](const TSMeta* meta) -> bool {
                const ts_ops* ops = dispatch_meta_ops(meta);
                return ops != nullptr && ops->value == &op_value_ref;
            };

            bool sampled_like = data->sampled;
            if (!sampled_like && wrapper_modified && !resolved_modified && !has_changed_map) {
                sampled_like = true;
            }
            if (!sampled_like &&
                vd.uses_link_target &&
                rebind_time == current_time) {
                if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices); link_target != nullptr) {
                    // First bind should sample current visible values and avoid carrying
                    // stale remove markers from an unrelated previous binding.
                    sampled_like = !link_target->has_previous_target;
                }
            }
            if (!sampled_like && wrapper_time == current_time && !has_changed_map) {
                sampled_like = true;
            }
            if (!sampled_like &&
                element_meta != nullptr &&
                meta_is_ref_wrapper(element_meta) &&
                vd.uses_link_target &&
                vd.path.port_type == PortType::OUTPUT &&
                resolved_modified &&
                !has_changed_map &&
                !has_added_keys &&
                !has_removed_keys) {
                sampled_like = true;
            }

            const auto resolve_previous_map_view =
                [&](ViewData& previous, value::MapView& previous_map) -> bool {
                    if (!resolve_previous_bound_target_view_data(vd, previous)) {
                        if (!resolve_previous_bound_target_view_data(*data, previous)) {
                            return false;
                        }
                    }
                    auto previous_value = resolve_value_slot_const(previous);
                    if (!previous_value.has_value() || !previous_value->valid() || !previous_value->is_map()) {
                        return false;
                    }
                    previous_map = previous_value->as_map();
                    return true;
                };
            const auto previous_map_entry_visible =
                [&](const ViewData& previous, const value::MapView& previous_map, const View& key) -> bool {
                    auto previous_slot = map_slot_for_key(previous_map, key);
                    if (!previous_slot.has_value()) {
                        return false;
                    }

                    ViewData previous_child = previous;
                    previous_child.path.indices.push_back(*previous_slot);
                    const TSMeta* previous_child_meta = meta_at_path(previous_child.meta, previous_child.path.indices);
                    if (meta_is_ref_wrapper(previous_child_meta)) {
                        nb::object payload = tsd_ref_view_payload_to_python(
                            previous_child,
                            previous_child_meta,
                            current_time,
                            true,
                            debug_ref_payload);
                        if (payload.is_none()) {
                            View previous_entry = previous_map.at(key);
                            if (previous_entry.valid() && previous_entry.schema() == ts_reference_meta()) {
                                TimeSeriesReference previous_ref = nb::cast<TimeSeriesReference>(previous_entry.to_python());
                                payload = tsd_ref_payload_to_python(
                                    previous_ref,
                                    previous_child_meta,
                                    current_time,
                                    true);
                            }
                        }
                        if (!payload.is_none()) {
                            return true;
                        }
                        const TSMeta* ref_element_meta = previous_child_meta->element_ts();
                        const ts_ops* ref_target_ops = dispatch_meta_ops(ref_element_meta);
                        const bool ref_targets_container =
                            ref_target_ops != nullptr &&
                            (ref_target_ops->dict != nullptr ||
                             ref_target_ops->set != nullptr ||
                             ref_target_ops->list != nullptr ||
                             ref_target_ops->bundle != nullptr);
                        if (debug_tsd_delta && payload.is_none()) {
                            std::fprintf(stderr,
                                         "[tsd_delta_dbg] ref_prev_visibility path=%s key=%s ref_elem_kind=%d container=%d\n",
                                         vd.path.to_string().c_str(),
                                         key.to_string().c_str(),
                                         ref_element_meta != nullptr ? static_cast<int>(ref_element_meta->kind) : -1,
                                         ref_targets_container ? 1 : 0);
                        }
                        if (ref_targets_container) {
                            return true;
                        }
                    }
                    if (!op_valid(previous_child)) {
                        return false;
                    }
                    return !op_to_python(previous_child).is_none();
                };
            const auto ref_binding_changed_from_previous = [&](View key) -> bool {
                ViewData previous{};
                value::MapView previous_map;
                if (!resolve_previous_map_view(previous, previous_map)) {
                    return false;
                }
                auto current_slot = map_slot_for_key(value_map, key);
                auto previous_slot = map_slot_for_key(previous_map, key);
                if (!current_slot.has_value()) {
                    return previous_slot.has_value();
                }
                if (!previous_slot.has_value()) {
                    return true;
                }

                ViewData current_child = *data;
                current_child.path.indices.push_back(*current_slot);
                ViewData previous_child = previous;
                previous_child.path.indices.push_back(*previous_slot);

                ViewData current_target{};
                ViewData previous_target{};
                const bool current_has_target = resolve_bound_target_view_data(current_child, current_target);
                const bool previous_has_target = resolve_bound_target_view_data(previous_child, previous_target);
                if (current_has_target != previous_has_target) {
                    return true;
                }
                if (current_has_target && previous_has_target) {
                    return !same_view_identity(current_target, previous_target);
                }

                View current_entry = value_map.at(key);
                View previous_entry = previous_map.at(key);
                if (!current_entry.valid() || !previous_entry.valid()) {
                    return current_entry.valid() != previous_entry.valid();
                }
                if (current_entry.schema() != ts_reference_meta() ||
                    previous_entry.schema() != ts_reference_meta()) {
                    return current_entry.schema() != previous_entry.schema();
                }

                try {
                    TimeSeriesReference current_ref = nb::cast<TimeSeriesReference>(current_entry.to_python());
                    TimeSeriesReference previous_ref = nb::cast<TimeSeriesReference>(previous_entry.to_python());

                    const ViewData* current_bound = current_ref.bound_view();
                    const ViewData* previous_bound = previous_ref.bound_view();
                    if (current_bound != nullptr || previous_bound != nullptr) {
                        if (current_bound == nullptr || previous_bound == nullptr) {
                            return true;
                        }
                        return !same_view_identity(*current_bound, *previous_bound);
                    }

                    return !(current_ref == previous_ref);
                } catch (...) {
                    return false;
                }
            };
            const auto ref_child_rebound_for_key = [&](const ViewData& child, View key) -> bool {
                return ref_child_rebound_this_tick(child) || ref_binding_changed_from_previous(key);
            };

            tsd_emit_removed_phase<DeclaredRefElement, AssumeNonRefElement, HasDeclaredNestedElement, DeclaredNestedElement>(
                vd,
                data,
                current,
                element_meta,
                current_time,
                sampled_like,
                debug_tsd_delta,
                debug_ref_payload,
                added_keys,
                removed_keys,
                key_in_added_set,
                key_in_removed_set,
                key_in_changed_map,
                resolve_previous_map_view,
                previous_map_entry_visible,
                delta_out);

            if (debug_tsd_delta) {
                size_t changed_size = 0;
                size_t added_size = 0;
                size_t removed_size = 0;
                if (changed_values.valid() && changed_values.is_map()) {
                    changed_size = changed_values.as_map().size();
                }
                if (added_keys.valid() && added_keys.is_set()) {
                    added_size = added_keys.as_set().size();
                }
                if (removed_keys.valid() && removed_keys.is_set()) {
                    removed_size = removed_keys.as_set().size();
                }
                std::fprintf(stderr,
                             "[tsd_delta_dbg] path=%s kind=%d elem=%d modified=1 sampled=%d sampled_like=%d rebind=%lld wrapper=%lld changed=%zu added=%zu removed=%zu now=%lld\n",
                             vd.path.to_string().c_str(),
                             static_cast<int>(current->kind),
                             element_meta != nullptr ? static_cast<int>(element_meta->kind) : -1,
                             data->sampled ? 1 : 0,
                             sampled_like ? 1 : 0,
                             static_cast<long long>(rebind_time.time_since_epoch().count()),
                             static_cast<long long>(wrapper_time.time_since_epoch().count()),
                             changed_size,
                             added_size,
                             removed_size,
                             static_cast<long long>(current_time.time_since_epoch().count()));
                if (added_keys.valid() && added_keys.is_set()) {
                    for (View dbg_key : added_keys.as_set()) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] added_key path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     dbg_key.to_string().c_str());
                    }
                }
                if (removed_keys.valid() && removed_keys.is_set()) {
                    for (View dbg_key : removed_keys.as_set()) {
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] removed_key path=%s key=%s\n",
                                     vd.path.to_string().c_str(),
                                     dbg_key.to_string().c_str());
                    }
                }
                if (changed_values.valid() && changed_values.is_map()) {
                    for (View dbg_key : changed_values.as_map().keys()) {
                        std::string key_s{"<key>"};
                        std::string val_s{"<value>"};
                        bool entry_valid = false;
                        bool entry_is_map = false;
                        size_t entry_map_size = 0;
                        try {
                            key_s = dbg_key.to_string();
                        } catch (...) {}
                        try {
                            View entry = changed_values.as_map().at(dbg_key);
                            entry_valid = entry.valid();
                            entry_is_map = entry.valid() && entry.is_map();
                            entry_map_size = entry_is_map ? entry.as_map().size() : 0;
                            val_s = entry.to_string();
                        } catch (...) {}
                        std::fprintf(stderr,
                                     "[tsd_delta_dbg] changed_entry path=%s key=%s valid=%d is_map=%d map_size=%zu value=%s\n",
                                     vd.path.to_string().c_str(),
                                     key_s.c_str(),
                                     entry_valid ? 1 : 0,
                                     entry_is_map ? 1 : 0,
                                     entry_map_size,
                                     val_s.c_str());
                    }
                }
            }

            tsd_emit_phase<DeclaredRefElement, AssumeNonRefElement, HasDeclaredNestedElement, DeclaredNestedElement>(
                vd,
                data,
                current_time,
                sampled_like,
                has_changed_map,
                nested_element,
                single_changed_key,
                has_added_keys,
                has_removed_keys,
                debug_tsd_delta,
                debug_ref_payload,
                changed_values,
                added_keys,
                removed_keys,
                value_map,
                key_in_added_set,
                key_in_removed_set,
                key_in_changed_map,
                ref_child_rebound_for_key,
                ref_target_modified_this_tick,
                resolve_previous_map_view,
                previous_map_entry_visible,
                delta_out);

            // Ensure changed keys materialize visible payloads even when child-native
            // deltas are empty (for example REF rebinding/carry-forward updates).
            tsd_emit_backfill_phase<DeclaredRefElement, AssumeNonRefElement, HasDeclaredNestedElement, DeclaredNestedElement>(
                vd,
                data,
                current_time,
                sampled_like,
                debug_ref_payload,
                changed_values,
                value_map,
                key_in_added_set,
                ref_child_rebound_for_key,
                ref_target_modified_this_tick,
                delta_out);

        }
        tsd_remove_empty_mapping_payloads(delta_out);
        tsd_update_visible_key_history_from_delta(*data, current, delta_out, current_time);
}

}  // namespace

void tsd_emit_map_delta_plain(const ViewData& vd,
                              const ViewData* data,
                              const TSMeta* current,
                              engine_time_t current_time,
                              bool wrapper_modified,
                              bool resolved_modified,
                              bool debug_tsd_delta,
                              bool debug_ref_payload,
                              const View& changed_values,
                              const View& added_keys,
                              const View& removed_keys,
                              nb::dict& delta_out) {
    tsd_emit_map_delta_impl<false, false, false>(vd,
                                                 data,
                                                 current,
                                                 current_time,
                                                 wrapper_modified,
                                                 resolved_modified,
                                                 debug_tsd_delta,
                                                 debug_ref_payload,
                                                 changed_values,
                                                 added_keys,
                                                 removed_keys,
                                                 delta_out);
}

void tsd_emit_map_delta_plain_scalar(const ViewData& vd,
                                     const ViewData* data,
                                     const TSMeta* current,
                                     engine_time_t current_time,
                                     bool wrapper_modified,
                                     bool resolved_modified,
                                     bool debug_tsd_delta,
                                     bool debug_ref_payload,
                                     const View& changed_values,
                                     const View& added_keys,
                                     const View& removed_keys,
                                     nb::dict& delta_out) {
    tsd_emit_map_delta_impl<false, true, false>(vd,
                                                data,
                                                current,
                                                current_time,
                                                wrapper_modified,
                                                resolved_modified,
                                                debug_tsd_delta,
                                                debug_ref_payload,
                                                changed_values,
                                                added_keys,
                                                removed_keys,
                                                delta_out);
}

void tsd_emit_map_delta_plain_nested(const ViewData& vd,
                                     const ViewData* data,
                                     const TSMeta* current,
                                     engine_time_t current_time,
                                     bool wrapper_modified,
                                     bool resolved_modified,
                                     bool debug_tsd_delta,
                                     bool debug_ref_payload,
                                     const View& changed_values,
                                     const View& added_keys,
                                     const View& removed_keys,
                                     nb::dict& delta_out) {
    tsd_emit_map_delta_impl<false, true, true>(vd,
                                               data,
                                               current,
                                               current_time,
                                               wrapper_modified,
                                               resolved_modified,
                                               debug_tsd_delta,
                                               debug_ref_payload,
                                               changed_values,
                                               added_keys,
                                               removed_keys,
                                               delta_out);
}

void tsd_emit_map_delta_ref_elements(const ViewData& vd,
                                     const ViewData* data,
                                     const TSMeta* current,
                                     engine_time_t current_time,
                                     bool wrapper_modified,
                                     bool resolved_modified,
                                     bool debug_tsd_delta,
                                     bool debug_ref_payload,
                                     const View& changed_values,
                                     const View& added_keys,
                                     const View& removed_keys,
                                     nb::dict& delta_out) {
    tsd_emit_map_delta_impl<true, false, false>(vd,
                                                data,
                                                current,
                                                current_time,
                                                wrapper_modified,
                                                resolved_modified,
                                                debug_tsd_delta,
                                                debug_ref_payload,
                                                changed_values,
                                                added_keys,
                                                removed_keys,
                                                delta_out);
}

}  // namespace hgraph
