#include "ts_ops_internal.h"

namespace hgraph {
nb::object op_delta_to_python_tsd_impl(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_delta_kind = std::getenv("HGRAPH_DEBUG_DELTA_KIND") != nullptr;
    const bool key_set_projection = is_tsd_key_set_projection(vd);
    if (auto key_set_delta = maybe_tsd_key_set_delta_to_python(
            vd,
            current_time,
            debug_delta_kind,
            debug_keyset_bridge,
            key_set_projection,
            key_set_projection);
        key_set_delta.has_value()) {
        return std::move(*key_set_delta);
    }

    const auto ref_payload_to_python =
        [current_time](const TimeSeriesReference& ref,
                       const TSMeta* element_meta,
                       bool include_unmodified,
                       const auto& self) -> nb::object {
            if (ref.is_empty()) {
                return nb::none();
            }

            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                if (!include_unmodified) {
                    if (target->ops == nullptr || !target->ops->modified(*target, current_time)) {
                        return nb::none();
                    }
                    nb::object delta_obj = op_delta_to_python(*target, current_time);
                    if (!delta_obj.is_none()) {
                        return delta_obj;
                    }
                    return nb::none();
                }

                if (target->ops != nullptr && target->ops->modified(*target, current_time)) {
                    nb::object delta_obj = op_delta_to_python(*target, current_time);
                    if (!delta_obj.is_none()) {
                        return delta_obj;
                    }
                }

                ViewData sampled_target = *target;
                sampled_target.sampled = true;
                nb::object sampled_delta = op_delta_to_python(sampled_target, current_time);
                if (!sampled_delta.is_none()) {
                    return sampled_delta;
                }
                return op_to_python(*target);
            }

            if (!ref.is_unbound()) {
                return nb::none();
            }

            const auto& items = ref.items();
            if (dispatch_meta_is_tsb(element_meta) && element_meta->fields() != nullptr) {
                nb::dict out;
                const size_t n = std::min(items.size(), element_meta->field_count());
                for (size_t i = 0; i < n; ++i) {
                    const char* field_name = element_meta->fields()[i].name;
                    if (field_name == nullptr) {
                        continue;
                    }
                    const TSMeta* field_meta = element_meta->fields()[i].ts_type;
                    nb::object item_py = self(items[i], field_meta, include_unmodified, self);
                    if (!item_py.is_none()) {
                        out[nb::str(field_name)] = std::move(item_py);
                    }
                }
                return PyDict_Size(out.ptr()) == 0 ? nb::none() : nb::object(out);
            }

            if (dispatch_meta_is_tsl(element_meta)) {
                nb::dict out;
                const TSMeta* child_meta = element_meta->element_ts();
                for (size_t i = 0; i < items.size(); ++i) {
                    nb::object item_py = self(items[i], child_meta, include_unmodified, self);
                    if (!item_py.is_none()) {
                        out[nb::int_(i)] = std::move(item_py);
                    }
                }
                return PyDict_Size(out.ptr()) == 0 ? nb::none() : nb::object(out);
            }

            if (items.size() == 1) {
                return self(items[0], element_meta, include_unmodified, self);
            }

            nb::list out;
            for (const auto& item : items) {
                nb::object item_py = self(item, element_meta, include_unmodified, self);
                if (!item_py.is_none()) {
                    out.append(std::move(item_py));
                }
            }
            return out.empty() ? nb::none() : nb::object(out);
        };
        const bool debug_ref_payload = std::getenv("HGRAPH_DEBUG_TSD_REF_PAYLOAD") != nullptr;
        const auto ref_view_payload_to_python =
            [&ref_payload_to_python, debug_ref_payload, current_time](const ViewData& ref_child,
                                                                      const TSMeta* ref_meta,
                                                                      bool include_unmodified) -> nb::object {
                View ref_value = op_value(ref_child);
                if (!ref_value.valid() || ref_value.schema() != ts_reference_meta()) {
                    ViewData target{};
                    bool has_target = resolve_bound_target_view_data(ref_child, target);
                    if (!has_target) {
                        if (auto rebound = resolve_bound_view_data(ref_child); rebound.has_value()) {
                            target = *rebound;
                            has_target = true;
                        }
                    }
                    if (has_target) {
                        const bool target_modified =
                            target.ops != nullptr && target.ops->modified != nullptr &&
                            target.ops->modified(target, current_time);
                        if (target_modified) {
                            nb::object delta_obj = op_delta_to_python(target, current_time);
                            if (!delta_obj.is_none()) {
                                return delta_obj;
                            }
                        }
                        if (include_unmodified) {
                            ViewData sampled_target = target;
                            sampled_target.sampled = true;
                            nb::object sampled_delta = op_delta_to_python(sampled_target, current_time);
                            if (!sampled_delta.is_none()) {
                                return sampled_delta;
                            }
                            nb::object value_obj = op_to_python(target);
                            if (!value_obj.is_none()) {
                                return value_obj;
                            }
                        } else if (target_modified) {
                            nb::object value_obj = op_to_python(target);
                            if (!value_obj.is_none()) {
                                return value_obj;
                            }
                        }
                    }
                    if (debug_ref_payload) {
                        std::fprintf(stderr,
                                     "[tsd_ref_payload] path=%s now=%lld include=%d ref_valid=0\n",
                                     ref_child.path.to_string().c_str(),
                                     static_cast<long long>(current_time.time_since_epoch().count()),
                                     include_unmodified ? 1 : 0);
                    }
                    return nb::none();
                }
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value.to_python());
                const TSMeta* element_meta = ref_meta != nullptr ? ref_meta->element_ts() : nullptr;
                nb::object payload = ref_payload_to_python(ref, element_meta, include_unmodified, ref_payload_to_python);
                if (debug_ref_payload) {
                    std::string payload_s{"<none>"};
                    try {
                        payload_s = nb::cast<std::string>(nb::repr(payload));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_ref_payload] path=%s now=%lld include=%d ref_kind=%d elem_kind=%d payload=%s\n",
                                 ref_child.path.to_string().c_str(),
                                 static_cast<long long>(current_time.time_since_epoch().count()),
                                 include_unmodified ? 1 : 0,
                                 static_cast<int>(ref.kind()),
                                 element_meta != nullptr ? static_cast<int>(element_meta->kind) : -1,
                                 payload_s.c_str());
                }
                return payload;
            };
            const auto has_delta_payload_view = [](const View& view) -> bool {
                if (!view.valid()) {
                    return false;
                }
                if (view.schema() == ts_reference_meta()) {
                    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
                    return ref.bound_view() != nullptr;
                }
                return true;
            };
            const auto has_delta_payload = [&has_delta_payload_view](const DeltaView& delta) -> bool {
                return has_delta_payload_view(delta.value());
            };

    {
        nb::object bridge_delta;
        if (try_container_bridge_delta_to_python(
                vd, self_meta, current_time, true, false, bridge_delta)) {
            return bridge_delta;
        }
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return nb::none();
    }
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (debug_delta_kind) {
        std::fprintf(stderr,
                     "[delta_kind] path=%s self_kind=%d resolved_kind=%d self_proj=%d resolved_proj=%d uses_lt=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     current != nullptr ? static_cast<int>(current->kind) : -1,
                     static_cast<int>(vd.projection),
                     static_cast<int>(data->projection),
                     vd.uses_link_target ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }
    const bool debug_tsd_bridge = std::getenv("HGRAPH_DEBUG_TSD_BRIDGE") != nullptr;
    nb::object bridge_delta;
    if (try_container_bridge_delta_to_python(
            vd, current, current_time, false, debug_tsd_bridge, bridge_delta)) {
        // Python parity: when bindings change, container REF deltas are computed
        // from full previous/current snapshots (not current native delta only).
        return bridge_delta;
    }

    const bool debug_tsd_delta = std::getenv("HGRAPH_DEBUG_TSD_DELTA") != nullptr;
    const bool wrapper_modified = op_modified(vd, current_time);
    const bool resolved_modified = op_modified(*data, current_time);
    if (!wrapper_modified && !resolved_modified) {
        if (debug_tsd_delta) {
            std::fprintf(stderr,
                         "[tsd_delta_dbg] path=%s wrapper_modified=0 resolved_modified=0 now=%lld\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()));
        }
        // Non-scalar delta contract: containers return empty payloads, not None.
        return get_frozendict()(nb::dict{});
    }

    nb::dict delta_out;
    View changed_values;
    View added_keys;
    View removed_keys;
    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        std::optional<View> maybe_delta;
        if (auto delta_path = ts_path_to_delta_path(data->meta, data->path.indices); delta_path.has_value()) {
            if (delta_path->empty()) {
                maybe_delta = delta_root->view();
            } else {
                maybe_delta = navigate_const(delta_root->view(), *delta_path);
            }
        }

        if (maybe_delta.has_value() && maybe_delta->valid() && maybe_delta->is_tuple()) {
            auto tuple = maybe_delta->as_tuple();
            if (tuple.size() > 0) {
                changed_values = tuple.at(0);
            }
            if (tuple.size() > 1) {
                added_keys = tuple.at(1);
            }
            if (tuple.size() > 2) {
                removed_keys = tuple.at(2);
            }
        }
    }

    {
        auto current_value = resolve_value_slot_const(*data);
        if (current_value.has_value() && current_value->valid() && current_value->is_map()) {
            const auto value_map = current_value->as_map();
            const TSMeta* element_meta = current->element_ts();
            const bool declared_ref_element =
                self_meta != nullptr &&
                self_meta->element_ts() != nullptr &&
                dispatch_meta_is_ref(self_meta->element_ts());
            const bool nested_element = element_meta != nullptr && !dispatch_meta_is_scalar_like(element_meta);

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
                dispatch_meta_is_ref(element_meta) &&
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
                    if (dispatch_meta_is_ref(previous_child_meta)) {
                        nb::object payload = ref_view_payload_to_python(previous_child, previous_child_meta, true);
                        if (payload.is_none()) {
                            View previous_entry = previous_map.at(key);
                            if (previous_entry.valid() && previous_entry.schema() == ts_reference_meta()) {
                                TimeSeriesReference previous_ref = nb::cast<TimeSeriesReference>(previous_entry.to_python());
                                payload = ref_payload_to_python(
                                    previous_ref,
                                    previous_child_meta->element_ts(),
                                    true,
                                    ref_payload_to_python);
                            }
                        }
                        if (!payload.is_none()) {
                            return true;
                        }
                        const TSMeta* ref_element_meta = previous_child_meta->element_ts();
                        const bool ref_targets_container =
                            ref_element_meta != nullptr &&
                            dispatch_meta_is_container_like(ref_element_meta);
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

            if (!sampled_like && removed_keys.valid() && removed_keys.is_set()) {
                auto set = removed_keys.as_set();
                const bool has_added_set = added_keys.valid() && added_keys.is_set();
                nb::object remove = get_remove();
                const auto key_visible_in_previous_view = [&](const View& key) -> bool {
                    ViewData previous{};
                    value::MapView previous_map;
                    if (!resolve_previous_map_view(previous, previous_map)) {
                        const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
                        if (dispatch_meta_is_ref(element_meta) && vd.uses_link_target) {
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
                    if (dispatch_meta_is_ref(snapshot_meta)) {
                        nb::object payload = ref_view_payload_to_python(snapshot_view, snapshot_meta, true);
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
                    const bool ref_link_target_input =
                        element_meta != nullptr &&
                        dispatch_meta_is_ref(element_meta) &&
                        vd.uses_link_target;
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
                        const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
                        const TSMeta* ref_element_meta =
                            dispatch_meta_is_ref(element_meta)
                                ? element_meta->element_ts()
                                : nullptr;
                        const bool ref_targets_container =
                            ref_element_meta != nullptr &&
                            dispatch_meta_is_container_like(ref_element_meta);
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

            if (!sampled_like && has_changed_map) {
                const auto changed_map = changed_values.as_map();
                const bool debug_changed_map = std::getenv("HGRAPH_DEBUG_TSD_CHANGED_MAP") != nullptr;
                for_each_map_key_slot(value_map, [&](View key, size_t /*slot_index*/) {
                    if (!map_slot_for_key(changed_map, key).has_value()) {
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
                    const bool changed_entry_has_delta = has_delta_payload(changed_entry_delta);

                    ViewData child = *data;
                    child.path.indices.push_back(*slot);
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);

                    if (nested_element) {
                        const bool child_valid = op_valid(child);
                        const bool ref_child_rebound =
                            child_meta != nullptr &&
                            dispatch_meta_is_ref(child_meta) &&
                            ref_child_rebound_for_key(child, key);
                        const bool ref_target_modified_now =
                            child_meta != nullptr &&
                            dispatch_meta_is_ref(child_meta) &&
                            ref_target_modified_this_tick(child);
                        const bool include_unmodified_ref_payload =
                            key_in_added_set(key) ||
                            !child_valid ||
                            ref_child_rebound ||
                            !ref_target_modified_now ||
                            !changed_entry_has_delta;
                        if (dispatch_meta_is_ref(child_meta)) {
                            nb::object child_delta =
                                ref_view_payload_to_python(child, child_meta, include_unmodified_ref_payload);
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
                        if (dispatch_meta_is_ref(child_meta)) {
                            const bool include_unmodified_ref_payload =
                                key_in_added_set(key) ||
                                !child_valid ||
                                ref_child_rebound_for_key(child, key) ||
                                !has_tsd_visible_key_history(*data, key);
                            nb::object child_delta_py =
                                ref_view_payload_to_python(child, child_meta, include_unmodified_ref_payload);
                            const TSMeta* ref_element_meta = child_meta->element_ts();
                            const bool scalar_ref_target = dispatch_meta_is_scalar_like(ref_element_meta);
                            if (child_delta_py.is_none() &&
                                !include_unmodified_ref_payload &&
                                changed_entry_has_delta &&
                                scalar_ref_target &&
                                vd.path.port_type == PortType::INPUT &&
                                !has_added_keys &&
                                !has_removed_keys &&
                                single_changed_key) {
                                child_delta_py = ref_view_payload_to_python(child, child_meta, true);
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
                        if (has_delta_payload(child_delta)) {
                            nb::object child_delta_py = computed_delta_to_python_with_refs(child_delta, current_time);
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
                                nb::object child_value_py = stored_delta_to_python_with_refs(child_value, current_time);
                        if (!child_value_py.is_none()) {
                            delta_out[key.to_python()] = std::move(child_value_py);
                        }
                    }
                });
            } else {
                const bool include_unmodified = sampled_like;
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
                        const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                        const bool child_valid = op_valid(child);
                        if (dispatch_meta_is_ref(child_meta)) {
                            nb::object entry_py = nb::none();
                            if (child_valid) {
                                entry_py = ref_view_payload_to_python(child, child_meta, true);
                            }
                            if (entry_py.is_none()) {
                                if (declared_ref_element) {
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
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                    const bool child_valid = op_valid(child);
                    if (!child_valid &&
                        !dispatch_meta_is_ref(child_meta)) {
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
                        dispatch_meta_is_ref(child_meta) &&
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
                        dispatch_meta_is_ref(child_meta)) {
                        child_modified = ref_target_modified_this_tick(child);
                    }
                    if (!include_unmodified && !child_modified) {
                        return;
                    }
                    if (dispatch_meta_is_ref(child_meta)) {
                        nb::object child_delta =
                            ref_view_payload_to_python(
                                child,
                                child_meta,
                                include_unmodified || forced_from_changed_map || forced_from_structural_add ||
                                    ref_child_rebound);
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
                    if (dispatch_meta_is_scalar_like(child_meta)) {
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (has_delta_payload(child_delta)) {
                            nb::object child_delta_py = nb::none();
                            if (child_delta.valid() && child_delta.schema() == ts_reference_meta()) {
                                child_delta_py = ref_view_payload_to_python(
                                    child,
                                    nullptr,
                                    forced_from_changed_map || forced_from_structural_add);
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
                                    child_value_py = ref_view_payload_to_python(child, nullptr, true);
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

            // Ensure changed keys materialize visible payloads even when child-native
            // deltas are empty (for example REF rebinding/carry-forward updates).
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
                    const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                    const bool in_added_set = key_in_added_set(key);
                    const bool ref_child_rebound =
                        dispatch_meta_is_ref(child_meta) &&
                        ref_child_rebound_for_key(child, key);
                    bool child_modified_now = op_modified(child, current_time);
                    if (!child_modified_now &&
                        dispatch_meta_is_ref(child_meta)) {
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
                    if (dispatch_meta_is_ref(child_meta)) {
                        entry = ref_view_payload_to_python(
                            child,
                            child_meta,
                            in_added_set ||
                                ref_child_rebound ||
                                !child_valid);
                    } else if (dispatch_meta_is_scalar_like(child_meta)) {
                        if (!child_valid) {
                            continue;
                        }
                        DeltaView child_delta = DeltaView::from_computed(child, current_time);
                        if (has_delta_payload(child_delta)) {
                            entry = computed_delta_to_python_with_refs(child_delta, current_time);
                        }
                        if (entry.is_none()) {
                            View child_value = op_value(child);
                            if (child_value.valid()) {
                                entry = stored_delta_to_python_with_refs(child_value, current_time);
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

        const auto is_empty_mapping_payload = [](const nb::object& value_obj) {
            if (value_obj.is_none()) {
                return false;
            }
            if (nb::isinstance<nb::dict>(value_obj)) {
                return nb::len(value_obj) == 0;
            }
            nb::object items = nb::getattr(value_obj, "items", nb::none());
            if (items.is_none() || PyCallable_Check(items.ptr()) == 0) {
                return false;
            }
            nb::object iter_items = items();
            if (iter_items.is_none()) {
                return false;
            }
            Py_ssize_t size = PyObject_Length(iter_items.ptr());
            if (size < 0) {
                PyErr_Clear();
                return false;
            }
            return size == 0;
        };

        if (PyDict_Size(delta_out.ptr()) > 0) {
            nb::list keys_to_remove;
            for (const auto& kv : delta_out) {
                nb::object key_obj = nb::cast<nb::object>(kv.first);
                nb::object value_obj = nb::cast<nb::object>(kv.second);
                if (is_empty_mapping_payload(value_obj)) {
                    keys_to_remove.append(key_obj);
                }
            }
            for (const auto& key_item : keys_to_remove) {
                nb::object key_obj = nb::cast<nb::object>(key_item);
                PyDict_DelItem(delta_out.ptr(), key_obj.ptr());
            }
        }

        if (PyDict_Size(delta_out.ptr()) > 0) {
            const value::TypeMeta* key_type = current->key_type();
            if (key_type != nullptr) {
                nb::object remove_marker = get_remove();
                nb::object remove_if_exists_marker = get_remove_if_exists();
                for (const auto& kv : delta_out) {
                    nb::object py_key = nb::cast<nb::object>(kv.first);
                    nb::object py_value = nb::cast<nb::object>(kv.second);
                    value::Value key_value(key_type);
                    key_value.emplace();
                    try {
                        key_type->ops().from_python(key_value.data(), py_key, key_type);
                    } catch (...) {
                        continue;
                    }
                    const View key_view = key_value.view();
                    if (py_value.is(remove_marker) || py_value.is(remove_if_exists_marker)) {
                        clear_tsd_visible_key_history(*data, key_view);
                    } else {
                        mark_tsd_visible_key_history(*data, key_view, current_time);
                    }
                }
            }
        }
    }

    if (debug_tsd_delta) {
        std::string out_repr{"<repr_error>"};
        try {
            out_repr = nb::cast<std::string>(nb::repr(delta_out));
        } catch (...) {}
        std::fprintf(stderr,
                     "[tsd_delta_dbg] final_delta path=%s out=%s\n",
                     vd.path.to_string().c_str(),
                     out_repr.c_str());
    }
    return get_frozendict()(delta_out);
}

}  // namespace hgraph
