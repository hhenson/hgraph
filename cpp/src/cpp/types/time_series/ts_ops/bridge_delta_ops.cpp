#include "ts_ops_internal.h"

namespace hgraph {

nb::object tsd_bridge_delta_to_python(const ViewData& previous_data,
                                      const ViewData& current_data,
                                      engine_time_t current_time) {
    const bool debug_bridge_delta = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_TSD_BRIDGE_DELTA");
    nb::dict delta_out;

    const auto bridge_entry_to_python = [](const View& entry) -> nb::object {
        if (!entry.valid()) {
            return nb::none();
        }
        if (entry.schema() != ts_reference_meta()) {
            return entry.to_python();
        }

        TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(entry.data());
        if (const ViewData* target = ref.bound_view(); target != nullptr &&
            target->ops != nullptr) {
            return op_to_python(*target);
        }
        return nb::none();
    };

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);
    const TSMeta* previous_meta = meta_at_path(previous_data.meta, previous_data.path.indices);
    const TSMeta* current_meta = meta_at_path(current_data.meta, current_data.path.indices);
    const auto is_tsd_of_ref = [](const TSMeta* meta) {
        return meta != nullptr &&
               dispatch_meta_is_tsd(meta) &&
               meta->element_ts() != nullptr &&
               dispatch_meta_is_ref(meta->element_ts());
    };
    const bool carry_missing_from_previous_ref =
        is_tsd_of_ref(previous_meta) && !is_tsd_of_ref(current_meta);

    const auto extract_delta_key_sets = [&](const ViewData& data, View& added_keys, View& removed_keys) {
        if (!op_modified(data, current_time)) {
            return;
        }
        View native_delta = op_delta_value(data);
        if (native_delta.valid() && native_delta.is_tuple()) {
            auto tuple = native_delta.as_tuple();
            if (tuple.size() > 1) {
                added_keys = tuple.at(1);
            }
            if (tuple.size() > 2) {
                removed_keys = tuple.at(2);
            }
        }
    };

    View previous_added_keys{};
    View previous_removed_keys{};
    extract_delta_key_sets(previous_data, previous_added_keys, previous_removed_keys);

    View current_removed_keys{};
    View ignored_added_keys{};
    extract_delta_key_sets(current_data, ignored_added_keys, current_removed_keys);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();
    const auto in_key_set = [](const View& keys, const View& key) -> bool {
        if (!(keys.valid() && keys.is_set())) {
            return false;
        }
        auto set = keys.as_set();
        if (set.contains(key)) {
            return true;
        }
        for (View candidate : set) {
            if (candidate.equals(key)) {
                return true;
            }
        }
        return false;
    };

    if (debug_bridge_delta) {
        std::fprintf(stderr,
                     "[tsd_bridge_delta] now=%lld prev_path=%s curr_path=%s has_prev=%d has_curr=%d carry_prev_ref=%d\n",
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     previous_data.path.to_string().c_str(),
                     current_data.path.to_string().c_str(),
                     has_previous ? 1 : 0,
                     has_current ? 1 : 0,
                     carry_missing_from_previous_ref ? 1 : 0);
    }
    const auto entry_visible = [&](const value::MapView& map, const View& key) -> bool {
        if (!map.contains(key)) {
            return false;
        }
        return !bridge_entry_to_python(map.at(key)).is_none();
    };

    if (has_current) {
        const auto current_map = current_value->as_map();
        if (has_previous) {
            const auto previous_map = previous_value->as_map();
            for (View key : current_map.keys()) {
                View current_entry = current_map.at(key);
                bool include = !previous_map.contains(key);
                if (!include) {
                    View previous_entry = previous_map.at(key);
                    include = !(previous_entry.schema() == current_entry.schema() && previous_entry.equals(current_entry));
                }
                if (include) {
                    nb::object py_entry = bridge_entry_to_python(current_entry);
                    if (!py_entry.is_none()) {
                        delta_out[key.to_python()] = std::move(py_entry);
                    }
                }
            }
        } else {
            for (View key : current_map.keys()) {
                nb::object py_entry = bridge_entry_to_python(current_map.at(key));
                if (!py_entry.is_none()) {
                    delta_out[key.to_python()] = std::move(py_entry);
                }
            }
        }
    }

    if (has_previous) {
        const auto previous_map = previous_value->as_map();
        nb::object remove = get_remove();
        const auto is_prev_cycle_add = [&](const View& key) -> bool {
            return in_key_set(previous_added_keys, key);
        };
        const auto emit_remove_if_missing_from_current = [&](const View& key) {
            if (is_prev_cycle_add(key)) {
                return;
            }
            const bool in_prev = previous_map.contains(key);
            const bool prev_visible = in_prev && entry_visible(previous_map, key);
            if (in_prev && !prev_visible) {
                return;
            }
            const bool in_curr = has_current && current_value->as_map().contains(key);
            const bool curr_visible = in_curr && entry_visible(current_value->as_map(), key);
            if (curr_visible) {
                return;
            }
            if (!has_current) {
                if (debug_bridge_delta) {
                    std::fprintf(stderr,
                                 "[tsd_bridge_delta] key=%s action=remove(no_current)\n",
                                 key.to_string().c_str());
                }
                delta_out[key.to_python()] = remove;
                return;
            }
            if (in_curr) {
                // Explicitly present but unresolved/empty in current target.
                if (debug_bridge_delta) {
                    std::fprintf(stderr,
                                 "[tsd_bridge_delta] key=%s action=remove(current_unresolved)\n",
                                 key.to_string().c_str());
                }
                delta_out[key.to_python()] = remove;
                return;
            }
            if (in_key_set(current_removed_keys, key)) {
                // Explicit remove on current tick.
                if (debug_bridge_delta) {
                    std::fprintf(stderr,
                                 "[tsd_bridge_delta] key=%s action=remove(explicit_removed)\n",
                                 key.to_string().c_str());
                }
                delta_out[key.to_python()] = remove;
                return;
            }

            if (carry_missing_from_previous_ref) {
                nb::object carried = bridge_entry_to_python(previous_map.at(key));
                if (!carried.is_none()) {
                    if (debug_bridge_delta) {
                        std::fprintf(stderr,
                                     "[tsd_bridge_delta] key=%s action=carry(missing_prev_ref)\n",
                                     key.to_string().c_str());
                    }
                    delta_out[key.to_python()] = std::move(carried);
                    return;
                }
            }

            if (debug_bridge_delta) {
                std::fprintf(stderr,
                             "[tsd_bridge_delta] key=%s action=remove(missing_from_current)\n",
                             key.to_string().c_str());
            }
            delta_out[key.to_python()] = remove;
        };

        if (has_current) {
            for (View key : previous_map.keys()) {
                emit_remove_if_missing_from_current(key);
            }
        } else {
            for (View key : previous_map.keys()) {
                emit_remove_if_missing_from_current(key);
            }
        }

        if (previous_removed_keys.valid() && previous_removed_keys.is_set()) {
            for (View key : previous_removed_keys.as_set()) {
                emit_remove_if_missing_from_current(key);
            }
        }
    }

    return get_frozendict()(delta_out);
}

nb::object tss_bridge_delta_to_python(const ViewData& previous_data,
                                      const ViewData& current_data,
                                      engine_time_t current_time) {
    nb::set added_set;
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_set();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_set();

    value::SetView previous_added_on_tick{};
    value::SetView previous_removed_on_tick{};
    if (has_previous && op_last_modified_time(previous_data) == current_time) {
        View native_delta = op_delta_value(previous_data);
        if (native_delta.valid() && native_delta.is_tuple()) {
            auto tuple = native_delta.as_tuple();
            if (tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_set()) {
                previous_added_on_tick = tuple.at(0).as_set();
            }
            if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
                previous_removed_on_tick = tuple.at(1).as_set();
            }
        }
    }

    const auto existed_before_tick = [&](const View& elem) {
        if (!has_previous) {
            return false;
        }
        const auto previous_set = previous_value->as_set();
        const bool in_previous_current = previous_set.contains(elem);
        const bool added_on_tick =
            previous_added_on_tick.valid() && previous_added_on_tick.contains(elem);
        const bool removed_on_tick =
            previous_removed_on_tick.valid() && previous_removed_on_tick.contains(elem);
        return (in_previous_current && !added_on_tick) || removed_on_tick;
    };

    if (has_current) {
        auto current_set = current_value->as_set();
        for (View elem : current_set) {
            if (!existed_before_tick(elem)) {
                added_set.add(elem.to_python());
            }
        }
    }

    if (has_previous) {
        auto previous_set = previous_value->as_set();
        const bool has_current_set = has_current;
        auto current_set = has_current_set ? current_value->as_set() : value::SetView{};

        for (View elem : previous_set) {
            if (previous_added_on_tick.valid() && previous_added_on_tick.contains(elem)) {
                continue;
            }
            if (!has_current_set || !current_set.contains(elem)) {
                removed_set.add(elem.to_python());
            }
        }
        if (previous_removed_on_tick.valid()) {
            for (View elem : previous_removed_on_tick) {
                if (!has_current_set || !current_set.contains(elem)) {
                    removed_set.add(elem.to_python());
                }
            }
        }
    }

    return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

namespace {

enum class BridgeDeltaScenario : uint8_t {
    TSD = 0,
    TSS = 1,
};

nb::object bridge_delta_to_python(BridgeDeltaScenario scenario,
                                  const ViewData& previous_data,
                                  const ViewData& current_data,
                                  engine_time_t current_time) {
    if (scenario == BridgeDeltaScenario::TSS) {
        return tss_bridge_delta_to_python(previous_data, current_data, current_time);
    }
    return tsd_bridge_delta_to_python(previous_data, current_data, current_time);
}

bool try_container_bridge_delta_to_python_impl(const ViewData& vd,
                                               const TSMeta* container_meta,
                                               engine_time_t current_time,
                                               bool require_kind_mismatch,
                                               bool debug_bridge,
                                               BridgeDeltaScenario scenario,
                                               nb::object& out_delta) {
    ViewData previous_bridge{};
    ViewData current_bridge{};
    if (!resolve_container_rebind_bridge_views(
            vd, container_meta, current_time, require_kind_mismatch, previous_bridge, current_bridge)) {
        return false;
    }

    if (debug_bridge) {
        std::fprintf(stderr,
                     "[tsd_bridge_dbg] path=%s kind=%d prev=%s curr=%s now=%lld curr_modified=%d\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(container_meta->kind),
                     previous_bridge.path.to_string().c_str(),
                     current_bridge.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     op_modified(current_bridge, current_time) ? 1 : 0);
    }

    out_delta = bridge_delta_to_python(
        scenario, previous_bridge, current_bridge, current_time);
    return true;
}

}  // namespace

bool try_container_bridge_delta_to_python(const ViewData& vd,
                                          const TSMeta* container_meta,
                                          engine_time_t current_time,
                                          bool require_kind_mismatch,
                                          bool debug_bridge,
                                          nb::object& out_delta) {
    const BridgeDeltaScenario scenario =
        dispatch_meta_is_tss(container_meta) ? BridgeDeltaScenario::TSS : BridgeDeltaScenario::TSD;
    return try_container_bridge_delta_to_python_impl(
        vd, container_meta, current_time, require_kind_mismatch, debug_bridge, scenario, out_delta);
}

bool try_tsd_bridge_delta_to_python(const ViewData& vd,
                                    const TSMeta* container_meta,
                                    engine_time_t current_time,
                                    bool require_kind_mismatch,
                                    bool debug_bridge,
                                    nb::object& out_delta) {
    return try_container_bridge_delta_to_python_impl(
        vd,
        container_meta,
        current_time,
        require_kind_mismatch,
        debug_bridge,
        BridgeDeltaScenario::TSD,
        out_delta);
}

bool try_tss_bridge_delta_to_python(const ViewData& vd,
                                    const TSMeta* container_meta,
                                    engine_time_t current_time,
                                    bool require_kind_mismatch,
                                    bool debug_bridge,
                                    nb::object& out_delta) {
    return try_container_bridge_delta_to_python_impl(
        vd,
        container_meta,
        current_time,
        require_kind_mismatch,
        debug_bridge,
        BridgeDeltaScenario::TSS,
        out_delta);
}

bool is_tsd_key_set_projection(const ViewData& vd) {
    return vd.projection == ViewProjection::TSD_KEY_SET;
}

bool resolve_tsd_key_set_projection_view(const ViewData& vd, ViewData& out) {
    if (!is_tsd_key_set_projection(vd)) {
        return false;
    }
    out = vd;
    return true;
}

bool resolve_tsd_key_set_source(const ViewData& vd, ViewData& out) {
    if (is_tsd_key_set_projection(vd)) {
        ViewData source = vd;
        source.projection = ViewProjection::NONE;
        const TSMeta* source_meta = meta_at_path(source.meta, source.path.indices);
        if (!dispatch_meta_is_tsd(source_meta)) {
            return false;
        }

        if (!resolve_read_view_data(source, out)) {
            return false;
        }

        out.projection = ViewProjection::NONE;
        const TSMeta* current = meta_at_path(out.meta, out.path.indices);
        return dispatch_meta_is_tsd(current);
    }

    // Explicit key_set bridge: graph TSS endpoint linked to backing TSD source.
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (!dispatch_meta_is_tss(self_meta)) {
        return false;
    }

    if (!resolve_read_view_data(vd, out)) {
        return false;
    }

    out.projection = ViewProjection::NONE;
    const TSMeta* current = meta_at_path(out.meta, out.path.indices);
    return dispatch_meta_is_tsd(current);
}

// Bridge paths can carry raw TSD views (projection::NONE) when rebinding
// between concrete and empty REF states. Treat those as key_set sources for
// bridge delta synthesis only.
bool resolve_tsd_key_set_bridge_source(const ViewData& vd, ViewData& out) {
    if (resolve_tsd_key_set_source(vd, out)) {
        return true;
    }

    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (!dispatch_meta_is_tsd(self_meta)) {
        return false;
    }

    if (!resolve_read_view_data(vd, out)) {
        return false;
    }

    out.projection = ViewProjection::NONE;
    const TSMeta* current = meta_at_path(out.meta, out.path.indices);
    return dispatch_meta_is_tsd(current);
}

TSDKeySetBridgeState resolve_tsd_key_set_bridge_state(const ViewData& vd,
                                                      engine_time_t current_time) {
    TSDKeySetBridgeState state{};
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return state;
    }

    state.has_bridge = resolve_rebind_bridge_views(
        vd, self_meta, current_time, state.previous_bridge, state.current_bridge);
    if (!state.has_bridge) {
        return state;
    }

    state.has_previous_source =
        resolve_tsd_key_set_bridge_source(state.previous_bridge, state.previous_source);
    state.has_current_source =
        resolve_tsd_key_set_bridge_source(state.current_bridge, state.current_source);
    return state;
}

TSDKeySetDeltaState tsd_key_set_delta_state(const ViewData& source) {
    TSDKeySetDeltaState state{};

    auto* delta_root = static_cast<const Value*>(source.delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return state;
    }

    std::optional<View> maybe_delta;
    if (auto delta_path = ts_path_to_delta_path(source.meta, source.path.indices); delta_path.has_value()) {
        if (delta_path->empty()) {
            maybe_delta = delta_root->view();
        } else {
            maybe_delta = navigate_const(delta_root->view(), *delta_path);
        }
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return state;
    }

    state.has_delta_tuple = true;
    auto tuple = maybe_delta->as_tuple();
    state.has_changed_values_map =
        tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_map() && tuple.at(0).as_map().size() > 0;
    state.has_added = tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    state.has_removed =
        tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set() && tuple.at(2).as_set().size() > 0;
    return state;
}

bool tsd_key_set_has_added_or_removed(const ViewData& source) {
    const auto state = tsd_key_set_delta_state(source);
    if (!state.has_delta_tuple) {
        return false;
    }
    return state.has_added || state.has_removed;
}

bool tsd_key_set_has_added_or_removed_this_tick(const ViewData& source, engine_time_t current_time) {
    if (op_last_modified_time(source) != current_time) {
        return false;
    }

    const auto state = tsd_key_set_delta_state(source);
    if (!state.has_delta_tuple) {
        return false;
    }
    return state.has_added || state.has_removed;
}

bool tsd_key_set_modified_this_tick(const ViewData& source, engine_time_t current_time) {
    if (op_last_modified_time(source) != current_time) {
        return false;
    }

    const auto state = tsd_key_set_delta_state(source);
    if (!state.has_delta_tuple) {
        return false;
    }

    if (state.has_added || state.has_removed) {
        return true;
    }

    // Python marks key_set as modified on:
    // 1) initial empty materialization (invalid -> empty valid)
    // 2) same-tick key churn with no net add/remove (add then remove)
    // In both cases, changed_values_map remains empty.
    if (state.has_changed_values_map) {
        return false;
    }

    auto value = resolve_value_slot_const(source);
    return value.has_value() && value->valid() && value->is_map();
}

nb::object tsd_key_set_to_python(const ViewData& source) {
    nb::set keys_out;
    auto value = resolve_value_slot_const(source);
    if (value.has_value() && value->valid() && value->is_map()) {
        for (View key : value->as_map().keys()) {
            keys_out.add(key.to_python());
        }
    }
    return nb::frozenset(keys_out);
}

nb::object tsd_key_set_delta_to_python(const ViewData& source) {
    nb::set added_out;
    nb::set removed_out;

    View native_delta = op_delta_value(source);
    if (native_delta.valid() && native_delta.is_tuple()) {
        auto tuple = native_delta.as_tuple();
        View removed_keys_view;
        if (tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set()) {
            removed_keys_view = tuple.at(2);
            for (View elem : tuple.at(2).as_set()) {
                removed_out.add(elem.to_python());
            }
        }
        const bool has_removed_keys = removed_keys_view.valid() && removed_keys_view.is_set();

        if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
            for (View elem : tuple.at(1).as_set()) {
                // Invalid-key removals are encoded as add+remove markers in the
                // TSD delta slot so key_set consumers can still observe removal.
                if (has_removed_keys && removed_keys_view.as_set().contains(elem)) {
                    continue;
                }
                added_out.add(elem.to_python());
            }
        }
    }

    return python_set_delta(nb::frozenset(added_out), nb::frozenset(removed_out));
}

nb::object tsd_key_set_all_added_to_python(const ViewData& source) {
    nb::set added_out;
    auto value = resolve_value_slot_const(source);
    if (value.has_value() && value->valid() && value->is_map()) {
        for (View key : value->as_map().keys()) {
            added_out.add(key.to_python());
        }
    }
    return python_set_delta(nb::frozenset(added_out), nb::frozenset(nb::set{}));
}

nb::object tsd_key_set_bridge_delta_to_python(const ViewData& previous_data, const ViewData& current_data) {
    nb::set added_set;
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    auto current_value = resolve_value_slot_const(current_data);

    const bool has_previous = previous_value.has_value() && previous_value->valid() && previous_value->is_map();
    const bool has_current = current_value.has_value() && current_value->valid() && current_value->is_map();

    if (has_current) {
        auto current_map = current_value->as_map();
        if (has_previous) {
            auto previous_map = previous_value->as_map();
            for (View key : current_map.keys()) {
                if (!previous_map.contains(key)) {
                    added_set.add(key.to_python());
                }
            }
        } else {
            for (View key : current_map.keys()) {
                added_set.add(key.to_python());
            }
        }
    }

    if (has_previous) {
        auto previous_map = previous_value->as_map();
        if (has_current) {
            auto current_map = current_value->as_map();
            for (View key : previous_map.keys()) {
                if (!current_map.contains(key)) {
                    removed_set.add(key.to_python());
                }
            }
        } else {
            for (View key : previous_map.keys()) {
                removed_set.add(key.to_python());
            }
        }
    }

    return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

nb::object tsd_key_set_unbind_delta_to_python(const ViewData& previous_data) {
    nb::set removed_set;

    auto previous_value = resolve_value_slot_const(previous_data);
    if (!(previous_value.has_value() && previous_value->valid() && previous_value->is_map())) {
        return python_set_delta(nb::frozenset(nb::set{}), nb::frozenset(removed_set));
    }

    nb::set added_on_tick;
    nb::set removed_on_tick;
    View native_delta = op_delta_value(previous_data);
    if (native_delta.valid() && native_delta.is_tuple()) {
        auto tuple = native_delta.as_tuple();
        if (tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set()) {
            for (View elem : tuple.at(1).as_set()) {
                added_on_tick.add(elem.to_python());
            }
        }
        if (tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set()) {
            for (View elem : tuple.at(2).as_set()) {
                removed_on_tick.add(elem.to_python());
            }
        }
    }

    // Reconstruct the key-set visible before this tick:
    // previous_keys = (current_keys - added_on_tick) U removed_on_tick.
    for (View key : previous_value->as_map().keys()) {
        nb::object py_key = key.to_python();
        if (!added_on_tick.contains(py_key)) {
            removed_set.add(py_key);
        }
    }
    for (nb::handle py_key : removed_on_tick) {
        removed_set.add(py_key);
    }

    return python_set_delta(nb::frozenset(nb::set{}), nb::frozenset(removed_set));
}



}  // namespace hgraph
