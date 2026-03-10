#include "ts_ops_internal.h"

namespace hgraph {

engine_time_t signal_last_modified_time(const ViewData& vd,
                                        const LinkTarget& signal_link,
                                        engine_time_t base_time);

ViewData dispatch_view_for_path(const ViewData& view) {
    ViewData out = view;
    bind_view_data_ops(out);
    return out;
}

engine_time_t dispatch_last_modified_time(const ViewData& view) {
    ViewData dispatch_view = dispatch_view_for_path(view);
    if (dispatch_view.ops != nullptr && dispatch_view.ops->last_modified_time != nullptr) {
        return dispatch_view.ops->last_modified_time(dispatch_view);
    }
    return op_last_modified_time(dispatch_view);
}

bool dispatch_modified(const ViewData& view, engine_time_t current_time) {
    ViewData dispatch_view = dispatch_view_for_path(view);
    if (dispatch_view.ops != nullptr && dispatch_view.ops->modified != nullptr) {
        return dispatch_view.ops->modified(dispatch_view, current_time);
    }
    return op_modified(dispatch_view, current_time);
}

bool dispatch_valid(const ViewData& view) {
    ViewData dispatch_view = dispatch_view_for_path(view);
    if (dispatch_view.ops != nullptr && dispatch_view.ops->valid != nullptr) {
        return dispatch_view.ops->valid(dispatch_view);
    }
    return op_valid(dispatch_view);
}

bool meta_is_scalar_non_ref(const TSMeta* meta) {
    return dispatch_meta_is_scalar_like(meta) && !dispatch_meta_is_ref(meta);
}

bool meta_is_scalar_like_or_ref(const TSMeta* meta) {
    return meta == nullptr || dispatch_meta_is_scalar_like(meta);
}

bool rebind_bridge_has_container_meta_value(const ViewData& vd,
                                            const TSMeta* self_meta,
                                            engine_time_t current_time,
                                            const TSMeta* container_meta) {
    return rebind_bridge_has_container_kind_value(vd, self_meta, current_time, container_meta);
}

bool view_matches_container_meta_value(const std::optional<View>& value, const TSMeta* container_meta) {
    return view_matches_container_kind(value, container_meta);
}

bool modified_default_tail(const ViewData& vd, engine_time_t current_time) {
    const engine_time_t last_modified = dispatch_last_modified_time(vd);
    if (vd.sampled || last_modified == current_time) {
        return true;
    }

    if (vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd);
            link_target != nullptr &&
            link_target->has_previous_target &&
            link_target->modified(current_time) &&
            !dispatch_valid(vd)) {
            return true;
        }
    }

    return false;
}

std::optional<bool> modified_from_key_set_source(const ViewData& vd,
                                                 engine_time_t  current_time,
                                                 bool           debug_keyset_bridge,
                                                 bool           enable_bridge_logic) {
    const TSMeta* self_meta = vd.meta;
    ViewData key_set_source{};
    if (!resolve_tsd_key_set_source(vd, key_set_source)) {
        return std::nullopt;
    }

    const bool key_set_modified = tsd_key_set_modified_this_tick(key_set_source, current_time);
    if (debug_keyset_bridge) {
        std::fprintf(stderr,
                     "[keyset_mod] direct path=%s self_kind=%d source=%s modified=%d\n",
                     vd.to_short_path().to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     key_set_source.to_short_path().to_string().c_str(),
                     key_set_modified ? 1 : 0);
    }
    if (key_set_modified) {
        return true;
    }

    if (enable_bridge_logic) {
        const auto bridge_state = resolve_tsd_key_set_bridge_state(vd, current_time);
        if (bridge_state.has_bridge) {
            if (debug_keyset_bridge) {
                std::fprintf(stderr,
                             "[keyset_mod] bridge path=%s prev=%s curr=%s has_prev=%d has_curr=%d\n",
                             vd.to_short_path().to_string().c_str(),
                             bridge_state.previous_bridge.to_short_path().to_string().c_str(),
                             bridge_state.current_bridge.to_short_path().to_string().c_str(),
                             bridge_state.has_previous_source ? 1 : 0,
                             bridge_state.has_current_source ? 1 : 0);
            }
            if (bridge_state.has_previous_source || bridge_state.has_current_source) {
                return true;
            }
        } else {
            LinkTarget* lt = resolve_link_target(vd);
            if (debug_keyset_bridge && lt != nullptr) {
                const bool first_bind = is_first_bind_rebind_tick(lt, current_time);
                std::fprintf(stderr,
                             "[keyset_mod] no_bridge path=%s linked=%d prev=%d resolved=%d rebind=%lld now=%lld first_bind=%d\n",
                             vd.to_short_path().to_string().c_str(),
                             lt->is_linked ? 1 : 0,
                             lt->has_previous_target ? 1 : 0,
                             lt->has_resolved_target ? 1 : 0,
                             static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                             static_cast<long long>(current_time.time_since_epoch().count()),
                             first_bind ? 1 : 0);
            }
            if (is_first_bind_rebind_tick(lt, current_time)) {
                // First bind from an empty REF wrapper to a concrete TSD should
                // tick key_set immediately even before a previous-target bridge exists.
                if (debug_keyset_bridge && lt != nullptr) {
                    std::fprintf(stderr,
                                 "[keyset_mod] first_bind path=%s linked=%d prev=%d rebind=%lld now=%lld\n",
                                 vd.to_short_path().to_string().c_str(),
                                 lt->is_linked ? 1 : 0,
                                 lt->has_previous_target ? 1 : 0,
                                 static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()),
                                 static_cast<long long>(current_time.time_since_epoch().count()));
                }
                return true;
            }
        }
    }

    return false;
}

std::optional<bool> valid_from_key_set_source(const ViewData& vd, bool debug_keyset_valid) {
    ViewData key_set_source{};
    if (!resolve_tsd_key_set_source(vd, key_set_source)) {
        return std::nullopt;
    }

    auto value = resolve_value_slot_const(key_set_source);
    if (debug_keyset_valid) {
        std::fprintf(stderr,
                     "[keyset_valid] direct path=%s source=%s has_value=%d valid=%d is_map=%d\n",
                     vd.to_short_path().to_string().c_str(),
                     key_set_source.to_short_path().to_string().c_str(),
                     value.has_value() ? 1 : 0,
                     (value.has_value() && value->valid()) ? 1 : 0,
                     (value.has_value() && value->valid() && value->is_map()) ? 1 : 0);
    }
    if (value.has_value() && value->valid() && value->is_map()) {
        return true;
    }

    // Bridge/key_set sources can transiently resolve through REF wrappers on the
    // activation tick. If the direct key_set source is not yet materialized as a
    // map, fall through to the generic read-view validity path.
    return std::nullopt;
}

bool valid_from_resolved_slot(const ViewData& vd, const TSMeta* /*self_meta*/, bool ref_wrapper_mode) {
    const bool debug_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_VALID_SLOT");
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        if (debug_valid) {
            std::fprintf(stderr, "[valid_slot] path=%s -> false (resolve_read failed)\n",
                vd.to_short_path().to_string().c_str());
        }
        return false;
    }
    if (ref_wrapper_mode && same_view_identity(resolved, vd)) {
        auto local = resolve_value_slot_const(resolved);
        if (!local.has_value() || !local->valid()) {
            return false;
        }
        // Empty REF payloads are still valid once ticked (Python parity).
        return dispatch_last_modified_time(resolved) > MIN_DT;
    }
    const ViewData* data = &resolved;

    auto* value_root = static_cast<const Value*>(data->value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        if (debug_valid) {
            std::fprintf(stderr, "[valid_slot] path=%s -> false (value_root=%p has_value=%d)\n",
                vd.to_short_path().to_string().c_str(),
                (const void*)value_root,
                value_root != nullptr ? value_root->has_value() ? 1 : 0 : -1);
        }
        return false;
    }
    auto maybe = navigate_const(value_root->view(), data->path_indices());
    if (!maybe.has_value() || !maybe->valid()) {
        if (debug_valid) {
            auto indices = data->path_indices();
            std::string idx_str;
            for (auto i : indices) { idx_str += "/" + std::to_string(i); }
            std::fprintf(stderr, "[valid_slot] path=%s resolved_path=%s -> false (navigate failed has_val=%d indices=%s)\n",
                vd.to_short_path().to_string().c_str(),
                resolved.to_short_path().to_string().c_str(),
                maybe.has_value() ? 1 : 0,
                idx_str.c_str());
        }
        return false;
    }

    auto lmt = dispatch_last_modified_time(*data);
    if (debug_valid && lmt <= MIN_DT) {
        auto resolved_indices = data->path_indices();
        std::string ridx_str;
        for (auto i : resolved_indices) { ridx_str += "/" + std::to_string(i); }
        const TSMeta* rmeta = data->meta;
        engine_time_t dlmt = direct_last_modified_time(*data);
        engine_time_t lvl_lmt = (data->level != nullptr) ? data->level->last_modified_time : MIN_DT;
        std::fprintf(stderr, "[valid_slot] path=%s resolved_path=%s -> false (lmt=%lld dlmt=%lld lvl_lmt=%lld vdata=%p tdata=%p ridx=%s rmeta_kind=%d uses_lt=%d level=%p lvl_depth=%u path_depth=%zu)\n",
            vd.to_short_path().to_string().c_str(),
            resolved.to_short_path().to_string().c_str(),
            static_cast<long long>(lmt.time_since_epoch().count()),
            static_cast<long long>(dlmt.time_since_epoch().count()),
            static_cast<long long>(lvl_lmt.time_since_epoch().count()),
            data->value_data, data->time_data,
            ridx_str.c_str(),
            rmeta != nullptr ? static_cast<int>(rmeta->kind) : -1,
            data->uses_link_target ? 1 : 0,
            (void*)data->level,
            (unsigned)data->level_depth,
            data->path_depth());
    }
    // A default-constructed value slot is not valid until the underlying view has been stamped.
    return lmt > MIN_DT;
}

engine_time_t last_modified_fallback_no_dispatch(const ViewData& vd,
                                                 bool allow_ops_dispatch,
                                                 bool include_wrapper_time,
                                                 bool include_map_children,
                                                 bool include_static_children) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    const bool debug_keyset_bridge = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_BRIDGE");
    if (debug_keyset_bridge && is_tsd_key_set_projection(vd) && vd.uses_link_target) {
        if (LinkTarget* lt = resolve_link_target(vd); lt != nullptr) {
            std::fprintf(stderr,
                         "[keyset_lmt] path=%s linked=%d prev=%d resolved=%d rebind=%lld\n",
                         vd.to_short_path().to_string().c_str(),
                         lt->is_linked ? 1 : 0,
                         lt->has_previous_target ? 1 : 0,
                         lt->has_resolved_target ? 1 : 0,
                         static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()));
        } else {
            std::fprintf(stderr,
                         "[keyset_lmt] path=%s link_target=<none>\n",
                         vd.to_short_path().to_string().c_str());
        }
    }
    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const engine_time_t ref_wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const TSMeta* self_meta = vd.meta;
    engine_time_t base_time = include_wrapper_time ? std::max(rebind_time, ref_wrapper_time) : rebind_time;
    if (!include_wrapper_time && vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd);
            link_target != nullptr &&
            link_target->is_linked &&
            link_target->has_resolved_target &&
            !link_target->has_previous_target) {
            // First empty->resolved binds for non-scalar containers should sample
            // at the wrapper transition tick, matching Python clone_binding.
            base_time = std::max(base_time, ref_wrapper_time);
        }
    }

    if (debug_keyset_bridge) {
        int linked = -1;
        int prev = -1;
        int resolved = -1;
        if (vd.uses_link_target) {
            if (LinkTarget* lt = resolve_link_target(vd); lt != nullptr) {
                linked = lt->is_linked ? 1 : 0;
                prev = lt->has_previous_target ? 1 : 0;
                resolved = lt->has_resolved_target ? 1 : 0;
            }
        }
        std::fprintf(stderr,
                     "[keyset_lmt] enter path=%s base=%lld rebind=%lld ref_wrapper=%lld linked=%d prev=%d resolved=%d\n",
                     vd.to_short_path().to_string().c_str(),
                     static_cast<long long>(base_time.time_since_epoch().count()),
                     static_cast<long long>(rebind_time.time_since_epoch().count()),
                     static_cast<long long>(ref_wrapper_time.time_since_epoch().count()),
                     linked,
                     prev,
                     resolved);
    }
    if (allow_ops_dispatch && self_meta != nullptr) {
        if (const ts_ops* ops = get_ts_ops(self_meta); ops != nullptr && ops->last_modified_time != nullptr) {
            ViewData dispatch_vd = vd;
            dispatch_vd.ops = ops;
            return ops->last_modified_time(dispatch_vd);
        }
    }

    if (is_unpeered_static_container_view(vd, self_meta)) {
        engine_time_t out = base_time;
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = make_child_view_data(vd, i);
            out = std::max(out, dispatch_last_modified_time(child));
        }
        return out;
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return base_time;
    }
    const ViewData* data = &resolved;

    engine_time_t out = base_time;
    if (data->level != nullptr && data->level_depth == data->path_depth()) {
        out = std::max(data->level->last_modified_time, base_time);
    } else {
        auto* time_root = static_cast<const Value*>(data->time_data);
        if (time_root != nullptr && time_root->has_value()) {
            auto time_path = ts_path_to_time_path(data->root_meta, data->path_indices());
            std::optional<View> maybe_time;
            if (time_path.empty()) {
                maybe_time = time_root->view();
            } else {
                maybe_time = navigate_const(time_root->view(), time_path);
            }
            if (maybe_time.has_value()) {
                out = std::max(extract_time_value(*maybe_time), base_time);
            }
        }
    }

    const TSMeta* current = data->meta;
    if (include_map_children) {
        auto value = resolve_value_slot_const(*data);
        if (value.has_value() && value->valid() && value->is_map()) {
            for_each_map_key_slot(value->as_map(), [&](View /*key*/, size_t slot) {
                ViewData child = make_child_view_data(*data, slot);
                out = std::max(out, dispatch_last_modified_time(child));
            });
        }
    }
    if (include_static_children && current != nullptr) {
        const size_t child_count = static_container_child_count(current);
        for (size_t i = 0; i < child_count; ++i) {
            ViewData child = make_child_view_data(*data, i);
            out = std::max(out, dispatch_last_modified_time(child));
        }
    }

    return out;
}

bool modified_fallback_no_dispatch(const ViewData& vd,
                                   engine_time_t current_time,
                                   bool           allow_ops_dispatch) {
    refresh_dynamic_ref_binding(vd, current_time);
    const bool debug_keyset_bridge = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_BRIDGE");
    const bool debug_op_modified = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_OP_MODIFIED");
    const TSMeta* self_meta = vd.meta;
    if (allow_ops_dispatch && self_meta != nullptr) {
        if (const ts_ops* ops = get_ts_ops(self_meta); ops != nullptr && ops->modified != nullptr) {
            ViewData dispatch_vd = vd;
            dispatch_vd.ops = ops;
            return ops->modified(dispatch_vd, current_time);
        }
    }
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.to_short_path().to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, is_tsd_key_set_projection(vd));
        key_set_result.has_value()) {
        return *key_set_result;
    }

    return modified_default_tail(vd, current_time);
}

bool valid_fallback_no_dispatch(const ViewData& vd, bool allow_ops_dispatch) {
    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }
    const TSMeta* self_meta = vd.meta;
    if (allow_ops_dispatch && self_meta != nullptr) {
        if (const ts_ops* ops = get_ts_ops(self_meta); ops != nullptr && ops->valid != nullptr) {
            ViewData dispatch_vd = vd;
            dispatch_vd.ops = ops;
            return ops->valid(dispatch_vd);
        }
    }
    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    return valid_from_resolved_slot(vd, self_meta, false);
}

const TSMeta* op_ts_meta(const ViewData& vd) {
    if (is_tsd_key_set_projection(vd)) {
        const TSMeta* source_meta = vd.meta;
        if (source_meta != nullptr && dispatch_meta_is_tsd(source_meta) && source_meta->key_type() != nullptr) {
            return TSTypeRegistry::instance().tss(source_meta->key_type());
        }
    }
    return vd.meta;
}

bool resolve_signal_source_view(const ViewData& vd,
                                const LinkTarget& signal_link,
                                ViewData& source_view,
                                bool& source_is_signal) {
    if (!signal_link.is_linked) {
        return false;
    }

    source_view = signal_link.has_resolved_target
                      ? signal_link.resolved_target
                      : signal_link.as_view_data(vd.sampled);
    bind_view_data_ops(source_view);
    const TSMeta* source_meta = source_view.meta;
    source_is_signal = dispatch_meta_is_signal(source_meta);
    return true;
}

engine_time_t signal_last_modified_time(const ViewData& vd,
                                        const LinkTarget& signal_link,
                                        engine_time_t base_time) {
    bool source_is_signal = false;
    ViewData source_view{};
    engine_time_t source_lmt = MIN_DT;
    const engine_time_t current_eval_time = view_evaluation_time(vd);
    if (resolve_signal_source_view(vd, signal_link, source_view, source_is_signal)) {
        if (!source_is_signal && !dispatch_valid(source_view)) {
            source_lmt = MIN_DT;
        } else if (!source_is_signal && is_tsd_key_set_projection(source_view)) {
            ViewData key_set_source{};
            if (resolve_tsd_key_set_source(source_view, key_set_source)) {
                source_lmt =
                    (current_eval_time != MIN_DT && tsd_key_set_modified_this_tick(key_set_source, current_eval_time))
                        ? current_eval_time
                        : MIN_DT;
            } else {
                source_lmt = direct_last_modified_time(source_view);
            }
        } else {
            source_lmt = dispatch_last_modified_time(source_view);
        }
    }

    const engine_time_t signal_rebind_time =
        (signal_link.has_resolved_target && signal_link.has_previous_target)
            ? signal_link.last_rebind_time
            : MIN_DT;
    if (source_is_signal && signal_link.owner_time_ptr != nullptr) {
        return std::max(signal_rebind_time, *signal_link.owner_time_ptr);
    }
    if (signal_link.is_linked) {
        return std::max(signal_rebind_time, source_lmt);
    }
    return base_time;
}

std::optional<bool> signal_valid_override(const ViewData& vd, const LinkTarget& signal_link) {
    bool source_is_signal = false;
    ViewData source_view{};
    const bool has_source = resolve_signal_source_view(vd, signal_link, source_view, source_is_signal);

    if (source_is_signal &&
        signal_link.owner_time_ptr != nullptr &&
        *signal_link.owner_time_ptr > MIN_DT) {
        return true;
    }
    if (!has_source) {
        return std::nullopt;
    }

    ViewData source_dispatch = dispatch_view_for_path(source_view);
    const TSMeta* source_meta = source_dispatch.meta;
    const bool source_is_ref_wrapper = dispatch_meta_is_ref(source_meta);
    if (source_is_ref_wrapper) {
        View ref_value = op_value(source_dispatch);
        if (!(ref_value.valid() && ref_value.schema() == ts_reference_meta())) {
            return false;
        }
        TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(ref_value.data());
        return ref.is_valid();
    }
    return dispatch_valid(source_view);
}

engine_time_t op_last_modified_time(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->last_modified_time != nullptr) {
        return dispatch_vd.ops->last_modified_time(dispatch_vd);
    }
    return last_modified_fallback_no_dispatch(dispatch_vd, true, true, false, false);
}

engine_time_t op_last_modified_ref(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    const TSMeta* self_meta = vd.meta;
    if (self_meta == nullptr) {
        return last_modified_fallback_no_dispatch(vd, true, true, false, false);
    }

    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const engine_time_t ref_wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    engine_time_t       base_time = std::max(rebind_time, ref_wrapper_time);

    if (!vd.uses_link_target) {
        const TSMeta* element_meta = self_meta->element_ts();
        const bool scalar_non_ref_wrapper =
            element_meta != nullptr && meta_is_scalar_non_ref(element_meta);
        if (scalar_non_ref_wrapper) {
            bool local_payload_valid = true;
            if (auto local = resolve_value_slot_const(vd);
                local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
                TimeSeriesReference local_ref = *static_cast<const TimeSeriesReference*>(local->data());
                local_payload_valid = local_ref.is_valid();
            }
            if (local_payload_valid) {
                base_time = std::max(base_time, direct_last_modified_time(vd));
            }
        }
    }

    // Mirror Python REF wrapper semantics: first-bind materialization of an
    // invalid payload (for example REF[REF[<UnSet>], ...]) does not surface
    // as modified time on the wrapper itself.
    if (vd.uses_link_target) {
        bool has_local_ref_value = false;
        bool local_ref_payload_valid = false;
        if (auto local = resolve_value_slot_const(vd);
            local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
            TimeSeriesReference local_ref = *static_cast<const TimeSeriesReference*>(local->data());
            has_local_ref_value = true;
            local_ref_payload_valid = local_ref.is_valid();
        }

        if (LinkTarget* link_target = resolve_link_target(vd);
            link_target != nullptr &&
            !link_target->has_previous_target &&
            !link_target->has_resolved_target &&
            (!has_local_ref_value || !local_ref_payload_valid)) {
            return MIN_DT;
        }
    }
    return base_time;
}

engine_time_t op_last_modified_tsvalue(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    return last_modified_fallback_no_dispatch(vd, false, true, false, false);
}

engine_time_t op_last_modified_tsw(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    return last_modified_fallback_no_dispatch(vd, false, false, false, false);
}

engine_time_t op_last_modified_tss(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    return last_modified_fallback_no_dispatch(vd, false, false, false, false);
}

engine_time_t op_last_modified_signal(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    const TSMeta* self_meta = vd.meta;

    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const engine_time_t ref_wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const bool include_wrapper_time = self_meta == nullptr;
    engine_time_t base_time = include_wrapper_time ? std::max(rebind_time, ref_wrapper_time) : rebind_time;
    if (!include_wrapper_time && vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd);
            link_target != nullptr &&
            link_target->is_linked &&
            link_target->has_resolved_target &&
            !link_target->has_previous_target) {
            base_time = std::max(base_time, ref_wrapper_time);
        }
    }

    if (vd.uses_link_target) {
        if (LinkTarget* signal_link = resolve_link_target(vd); signal_link != nullptr) {
            return signal_last_modified_time(vd, *signal_link, base_time);
        }
    }

    return last_modified_fallback_no_dispatch(vd, false, include_wrapper_time, false, false);
}

engine_time_t op_last_modified_tsd(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    return last_modified_fallback_no_dispatch(vd, false, false, true, false);
}

engine_time_t op_last_modified_tsb(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    return last_modified_fallback_no_dispatch(vd, false, false, false, true);
}

engine_time_t op_last_modified_tsl(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    return last_modified_fallback_no_dispatch(vd, false, false, false, true);
}
}  // namespace hgraph
