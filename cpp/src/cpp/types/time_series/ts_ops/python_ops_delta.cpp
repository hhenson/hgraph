#include "ts_ops_internal.h"

namespace hgraph {

nb::object delta_view_to_python_with_refs(const View& view, engine_time_t current_time) {
    if (!view.valid()) {
        return nb::none();
    }
    if (view.schema() == ts_reference_meta()) {
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(view.to_python());
        if (const ViewData* target = ref.bound_view(); target != nullptr) {
            if (target->ops != nullptr) {
                if (target->ops->modified != nullptr && target->ops->modified(*target, current_time)) {
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
            }
            return op_to_python(*target);
        }
        return nb::none();
    }
    return view.to_python();
}

nb::object computed_delta_to_python_with_refs(const DeltaView& delta, engine_time_t current_time) {
    // Do not call DeltaView::to_python() from inside op_delta_to_python():
    // computed backings route through ts_ops::delta_to_python.
    if (!delta.valid()) {
        return nb::none();
    }
    return delta_view_to_python_with_refs(delta.value(), current_time);
}

nb::object stored_delta_to_python_with_refs(const View& view, engine_time_t current_time) {
    return computed_delta_to_python_with_refs(DeltaView::from_stored(view), current_time);
}

std::optional<nb::object> maybe_tsd_key_set_delta_to_python(const ViewData& vd,
                                                            engine_time_t current_time,
                                                            bool debug_delta_kind,
                                                            bool debug_keyset_bridge,
                                                            bool emit_first_bind_all_added,
                                                            bool allow_bridge_fallback) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        if (debug_delta_kind) {
            std::fprintf(stderr,
                         "[delta_kind] keyset path=%s self_kind=%d proj=%d uses_lt=%d now=%lld\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         static_cast<int>(vd.projection),
                         vd.uses_link_target ? 1 : 0,
                         static_cast<long long>(current_time.time_since_epoch().count()));
        }
        if (debug_keyset_bridge) {
            std::fprintf(stderr,
                         "[keyset_delta] direct path=%s self_kind=%d uses_lt=%d source=%s\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         vd.uses_link_target ? 1 : 0,
                         key_set_source.path.to_string().c_str());
        }
        const auto bridge_state = resolve_tsd_key_set_bridge_state(vd, current_time);
        if (bridge_state.has_previous_source || bridge_state.has_current_source) {
            if (debug_keyset_bridge) {
                std::fprintf(stderr,
                             "[keyset_delta] bridge path=%s prev=%s curr=%s has_prev=%d has_curr=%d\n",
                             vd.path.to_string().c_str(),
                             (bridge_state.has_previous_source ? bridge_state.previous_source : bridge_state.previous_bridge)
                                 .path.to_string().c_str(),
                             (bridge_state.has_current_source ? bridge_state.current_source : bridge_state.current_bridge)
                                 .path.to_string().c_str(),
                             bridge_state.has_previous_source ? 1 : 0,
                             bridge_state.has_current_source ? 1 : 0);
            }
            if (bridge_state.has_previous_source && !bridge_state.has_current_source) {
                return tsd_key_set_unbind_delta_to_python(bridge_state.previous_source);
            }
            return tsd_key_set_bridge_delta_to_python(
                bridge_state.has_previous_source ? bridge_state.previous_source : bridge_state.previous_bridge,
                bridge_state.has_current_source ? bridge_state.current_source : bridge_state.current_bridge);
        }

        // First bind from empty REF -> concrete TSD has no previous bridge yet.
        // Emit full "added" snapshot so key_set consumers observe immediate adds.
        if (emit_first_bind_all_added) {
            if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
                is_first_bind_rebind_tick(lt, current_time)) {
                if (debug_keyset_bridge) {
                    std::fprintf(stderr,
                                 "[keyset_delta] first_bind path=%s linked=%d prev=%d rebind=%lld now=%lld\n",
                                 vd.path.to_string().c_str(),
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                }
                return tsd_key_set_all_added_to_python(key_set_source);
            }
        }

        if (!op_modified(vd, current_time)) {
            return nb::none();
        }
        return tsd_key_set_delta_to_python(key_set_source);
    }

    if (allow_bridge_fallback) {
        const auto bridge_state = resolve_tsd_key_set_bridge_state(vd, current_time);
        if (bridge_state.has_bridge) {
            if (debug_keyset_bridge) {
                std::fprintf(stderr,
                             "[keyset_delta] fallback path=%s prev_bridge=%s curr_bridge=%s has_prev=%d has_curr=%d\n",
                             vd.path.to_string().c_str(),
                             bridge_state.previous_bridge.path.to_string().c_str(),
                             bridge_state.current_bridge.path.to_string().c_str(),
                             bridge_state.has_previous_source ? 1 : 0,
                             bridge_state.has_current_source ? 1 : 0);
            }
            if (bridge_state.has_previous_source || bridge_state.has_current_source) {
                if (bridge_state.has_previous_source && !bridge_state.has_current_source) {
                    return tsd_key_set_unbind_delta_to_python(bridge_state.previous_source);
                }
                return tsd_key_set_bridge_delta_to_python(
                    bridge_state.has_previous_source ? bridge_state.previous_source : bridge_state.previous_bridge,
                    bridge_state.has_current_source ? bridge_state.current_source : bridge_state.current_bridge);
            }
        } else if (debug_keyset_bridge) {
            if (vd.uses_link_target) {
                if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
                    const TSMeta* target_meta = meta_at_path(lt->meta, lt->target_path.indices);
                    std::fprintf(stderr,
                                 "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d linked=%d prev=%d resolved=%d source_kind=%d last_rebind=%lld now=%lld\n",
                                 vd.path.to_string().c_str(),
                                 self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                                 vd.uses_link_target ? 1 : 0,
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 lt->has_resolved_target ? 1 : 0,
                                 target_meta != nullptr ? static_cast<int>(target_meta->kind) : -1,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                } else {
                    std::fprintf(stderr,
                                 "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d link_target=<none>\n",
                                 vd.path.to_string().c_str(),
                                 self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                                 vd.uses_link_target ? 1 : 0);
                }
            }
            std::fprintf(stderr,
                         "[keyset_delta] no_bridge path=%s self_kind=%d uses_lt=%d\n",
                         vd.path.to_string().c_str(),
                         self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                         vd.uses_link_target ? 1 : 0);
        }
    }

    return std::nullopt;
}

nb::object op_delta_to_python_tsvalue(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    if (current_time != MIN_DT && !op_modified(vd, current_time)) {
        return nb::none();
    }
    DeltaView delta = DeltaView::from_computed(vd, current_time);
    return computed_delta_to_python_with_refs(delta, current_time);
}

namespace {

nb::object op_delta_to_python_ref_common(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    if (current_time != MIN_DT && !op_modified(vd, current_time)) {
        return nb::none();
    }
    DeltaView delta = DeltaView::from_computed(vd, current_time);
    return computed_delta_to_python_with_refs(delta, current_time);
}

}  // namespace

nb::object op_delta_to_python_ref(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_ref_common(vd, current_time);
}

nb::object op_delta_to_python_ref_scalar(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_ref_common(vd, current_time);
}

nb::object op_delta_to_python_ref_list(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_ref_common(vd, current_time);
}

nb::object op_delta_to_python_ref_bundle(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_ref_common(vd, current_time);
}

nb::object op_delta_to_python_ref_dynamic(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_ref_common(vd, current_time);
}

nb::object op_delta_to_python_tss(const ViewData& vd, engine_time_t current_time) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    refresh_dynamic_ref_binding(vd, current_time);
    const bool debug_keyset_bridge = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_BRIDGE");
    const bool debug_delta_kind = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_DELTA_KIND");

    if (auto key_set_delta = maybe_tsd_key_set_delta_to_python(
            vd,
            current_time,
            debug_delta_kind,
            debug_keyset_bridge,
            true,
            true);
        key_set_delta.has_value()) {
        return std::move(*key_set_delta);
    }

    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return nb::none();
    }
    const ViewData* data = &resolved;
    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return nb::none();
    }

    if (debug_delta_kind) {
        std::fprintf(stderr,
                     "[delta_kind] path=%s self_kind=%d resolved_kind=%d self_proj=%d resolved_proj=%d uses_lt=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     static_cast<int>(current->kind),
                     static_cast<int>(vd.projection),
                     static_cast<int>(data->projection),
                     vd.uses_link_target ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    const bool debug_tsd_bridge = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_TSD_BRIDGE");
    nb::object bridge_delta;
    if (try_tss_bridge_delta_to_python(
            vd, current, current_time, false, debug_tsd_bridge, bridge_delta)) {
        // Python parity: when bindings change, container REF deltas are computed
        // from full previous/current snapshots (not current native delta only).
        return bridge_delta;
    }

    const bool wrapper_modified = op_modified(vd, current_time);
    const bool resolved_modified = op_modified(*data, current_time);
    if (!wrapper_modified && !resolved_modified) {
        return nb::none();
    }

    nb::set added_set;
    nb::set removed_set;
    bool has_native_delta = false;

    View native_delta = op_delta_value(*data);
    if (native_delta.valid() && native_delta.is_tuple()) {
        auto tuple = native_delta.as_tuple();
        if (tuple.size() > 0) {
            View added = tuple.at(0);
            if (added.valid() && added.is_set()) {
                for (View elem : added.as_set()) {
                    added_set.add(elem.to_python());
                }
                has_native_delta = has_native_delta || added.as_set().size() > 0;
            }
        }
        if (tuple.size() > 1) {
            View removed = tuple.at(1);
            if (removed.valid() && removed.is_set()) {
                for (View elem : removed.as_set()) {
                    removed_set.add(elem.to_python());
                }
                has_native_delta = has_native_delta || removed.as_set().size() > 0;
            }
        }
    }

    bool sampled_like = data->sampled;
    if (!sampled_like) {
        const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
        if (wrapper_time == current_time && !has_native_delta) {
            sampled_like = true;
        }
    }
    if (sampled_like) {
        added_set = nb::set();
        removed_set = nb::set();
        View current_value = op_value(*data);
        if (current_value.valid()) {
            if (current_value.is_set()) {
                for (View elem : current_value.as_set()) {
                    added_set.add(elem.to_python());
                }
            } else if (current_value.is_map()) {
                for (View key : current_value.as_map().keys()) {
                    added_set.add(key.to_python());
                }
            }
        }
    }

    return python_set_delta(nb::frozenset(added_set), nb::frozenset(removed_set));
}

nb::object op_delta_to_python_tsw_tick(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    ViewData with_time = vd;
    if (with_time.engine_time_ptr == nullptr && current_time != MIN_DT) {
        with_time.engine_time_ptr = &current_time;
    }
    View delta = op_delta_value_tsw_tick(with_time);
    return delta_view_to_python_with_refs(delta, current_time);
}

nb::object op_delta_to_python_tsw_duration(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    ViewData with_time = vd;
    if (with_time.engine_time_ptr == nullptr && current_time != MIN_DT) {
        with_time.engine_time_ptr = &current_time;
    }
    View delta = op_delta_value_tsw_duration(with_time);
    return delta_view_to_python_with_refs(delta, current_time);
}

nb::object op_delta_to_python_tsw(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_tsw_tick(vd, current_time);
}

nb::object op_delta_to_python_default(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    const bool debug_delta_kind = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_DELTA_KIND");

    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return nb::none();
    }
    if (debug_delta_kind) {
        const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
        std::fprintf(stderr,
                     "[delta_kind] path=%s self_kind=%d resolved_kind=%d self_proj=%d resolved_proj=%d uses_lt=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     resolved_meta != nullptr ? static_cast<int>(resolved_meta->kind) : -1,
                     static_cast<int>(vd.projection),
                     static_cast<int>(resolved.projection),
                     vd.uses_link_target ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    DeltaView delta = DeltaView::from_computed(vd, current_time);
    return computed_delta_to_python_with_refs(delta, current_time);
}

nb::object op_delta_to_python(const ViewData& vd, engine_time_t current_time) {
    ViewData dispatch_view = vd;
    bind_view_data_ops(dispatch_view);

    PythonDeltaCacheEntry* delta_cache_slot = nullptr;
    if (is_delta_to_python_cacheable(dispatch_view)) {
        delta_cache_slot = resolve_python_delta_cache_slot(dispatch_view, true);
        if (delta_cache_slot != nullptr && delta_cache_slot->is_valid_for(current_time)) {
            return delta_cache_slot->value;
        }
    }

    nb::object out = nb::none();
    if (dispatch_view.ops != nullptr &&
        dispatch_view.ops->delta_to_python != nullptr) {
        out = dispatch_view.ops->delta_to_python(dispatch_view, current_time);
    } else {
        out = op_delta_to_python_default(dispatch_view, current_time);
    }

    if (delta_cache_slot != nullptr) {
        delta_cache_slot->time = current_time;
        delta_cache_slot->value = out;
    }
    return out;
}

nb::object op_delta_to_python_tsd(const ViewData& vd, engine_time_t current_time) {
    ViewData dispatch_view = vd;
    bind_view_data_ops(dispatch_view);
    return op_delta_to_python_tsd_impl(dispatch_view, current_time);
}

nb::object op_delta_to_python_tsd_scalar(const ViewData& vd, engine_time_t current_time) {
    ViewData dispatch_view = vd;
    bind_view_data_ops(dispatch_view);
    return op_delta_to_python_tsd_scalar_impl(dispatch_view, current_time);
}

nb::object op_delta_to_python_tsd_nested(const ViewData& vd, engine_time_t current_time) {
    ViewData dispatch_view = vd;
    bind_view_data_ops(dispatch_view);
    return op_delta_to_python_tsd_nested_impl(dispatch_view, current_time);
}

nb::object op_delta_to_python_tsd_ref(const ViewData& vd, engine_time_t current_time) {
    ViewData dispatch_view = vd;
    bind_view_data_ops(dispatch_view);
    return op_delta_to_python_tsd_ref_impl(dispatch_view, current_time);
}

}  // namespace hgraph
