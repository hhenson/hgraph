#include "ts_ops_internal.h"

namespace hgraph {

engine_time_t signal_last_modified_time(const ViewData& vd,
                                        const LinkTarget& signal_link,
                                        engine_time_t base_time);

namespace {

inline ViewData dispatch_view_for_path(const ViewData& view) {
    ViewData out = view;
    bind_view_data_ops(out);
    return out;
}

inline engine_time_t dispatch_last_modified_time(const ViewData& view) {
    ViewData dispatch_view = dispatch_view_for_path(view);
    if (dispatch_view.ops != nullptr && dispatch_view.ops->last_modified_time != nullptr) {
        return dispatch_view.ops->last_modified_time(dispatch_view);
    }
    return op_last_modified_time(dispatch_view);
}

inline bool dispatch_modified(const ViewData& view, engine_time_t current_time) {
    ViewData dispatch_view = dispatch_view_for_path(view);
    if (dispatch_view.ops != nullptr && dispatch_view.ops->modified != nullptr) {
        return dispatch_view.ops->modified(dispatch_view, current_time);
    }
    return op_modified(dispatch_view, current_time);
}

inline bool dispatch_valid(const ViewData& view) {
    ViewData dispatch_view = dispatch_view_for_path(view);
    if (dispatch_view.ops != nullptr && dispatch_view.ops->valid != nullptr) {
        return dispatch_view.ops->valid(dispatch_view);
    }
    return op_valid(dispatch_view);
}

inline bool meta_is_scalar_non_ref(const TSMeta* meta) {
    return dispatch_meta_is_scalar_like(meta) && !dispatch_meta_is_ref(meta);
}

inline bool meta_is_scalar_like_or_ref(const TSMeta* meta) {
    return meta == nullptr || dispatch_meta_is_scalar_like(meta);
}

inline bool rebind_bridge_has_container_meta_value(const ViewData& vd,
                                                   const TSMeta* self_meta,
                                                   engine_time_t current_time,
                                                   const TSMeta* container_meta) {
    return rebind_bridge_has_container_kind_value(vd, self_meta, current_time, container_meta);
}

inline bool view_matches_container_meta_value(const std::optional<View>& value, const TSMeta* container_meta) {
    return view_matches_container_kind(value, container_meta);
}

inline bool modified_default_tail(const ViewData& vd, engine_time_t current_time) {
    const engine_time_t last_modified = dispatch_last_modified_time(vd);
    if (vd.sampled || last_modified == current_time) {
        return true;
    }

    if (vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
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
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData key_set_source{};
    if (!resolve_tsd_key_set_source(vd, key_set_source)) {
        return std::nullopt;
    }

    const bool key_set_modified = tsd_key_set_modified_this_tick(key_set_source, current_time);
    if (debug_keyset_bridge) {
        std::fprintf(stderr,
                     "[keyset_mod] direct path=%s self_kind=%d source=%s modified=%d\n",
                     vd.path.to_string().c_str(),
                     self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1,
                     key_set_source.path.to_string().c_str(),
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
                             vd.path.to_string().c_str(),
                             bridge_state.previous_bridge.path.to_string().c_str(),
                             bridge_state.current_bridge.path.to_string().c_str(),
                             bridge_state.has_previous_source ? 1 : 0,
                             bridge_state.has_current_source ? 1 : 0);
            }
            if (bridge_state.has_previous_source || bridge_state.has_current_source) {
                return true;
            }
        } else {
            LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
            if (debug_keyset_bridge && lt != nullptr) {
                const bool first_bind = is_first_bind_rebind_tick(lt, current_time);
                std::fprintf(stderr,
                             "[keyset_mod] no_bridge path=%s linked=%d prev=%d resolved=%d rebind=%lld now=%lld first_bind=%d\n",
                             vd.path.to_string().c_str(),
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
                                 vd.path.to_string().c_str(),
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
                     vd.path.to_string().c_str(),
                     key_set_source.path.to_string().c_str(),
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

bool valid_from_resolved_slot(const ViewData& vd, const TSMeta* self_meta, bool ref_wrapper_mode) {
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
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
        return false;
    }
    auto maybe = navigate_const(value_root->view(), data->path.indices);
    if (!maybe.has_value() || !maybe->valid()) {
        return false;
    }

    // A default-constructed value slot is not valid until the underlying view has been stamped.
    return dispatch_last_modified_time(*data) > MIN_DT;
}

engine_time_t last_modified_fallback_no_dispatch(const ViewData& vd,
                                                 bool allow_ops_dispatch = true,
                                                 bool include_wrapper_time = true,
                                                 bool include_map_children = false,
                                                 bool include_static_children = false) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    if (debug_keyset_bridge && is_tsd_key_set_projection(vd) && vd.uses_link_target) {
        if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
            std::fprintf(stderr,
                         "[keyset_lmt] path=%s linked=%d prev=%d resolved=%d rebind=%lld\n",
                         vd.path.to_string().c_str(),
                         lt->is_linked ? 1 : 0,
                         lt->has_previous_target ? 1 : 0,
                         lt->has_resolved_target ? 1 : 0,
                         static_cast<long long>(lt->last_rebind_time.time_since_epoch().count()));
        } else {
            std::fprintf(stderr,
                         "[keyset_lmt] path=%s link_target=<none>\n",
                         vd.path.to_string().c_str());
        }
    }
    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const engine_time_t ref_wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    engine_time_t base_time = include_wrapper_time ? std::max(rebind_time, ref_wrapper_time) : rebind_time;
    if (!include_wrapper_time && vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
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
            if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices); lt != nullptr) {
                linked = lt->is_linked ? 1 : 0;
                prev = lt->has_previous_target ? 1 : 0;
                resolved = lt->has_resolved_target ? 1 : 0;
            }
        }
        std::fprintf(stderr,
                     "[keyset_lmt] enter path=%s base=%lld rebind=%lld ref_wrapper=%lld linked=%d prev=%d resolved=%d\n",
                     vd.path.to_string().c_str(),
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
            ViewData child = vd;
            child.path.indices.push_back(i);
            out = std::max(out, dispatch_last_modified_time(child));
        }
        return out;
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return base_time;
    }
    const ViewData* data = &resolved;

    auto* time_root = static_cast<const Value*>(data->time_data);
    engine_time_t out = base_time;
    if (time_root != nullptr && time_root->has_value()) {
        auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
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

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (include_map_children) {
        auto value = resolve_value_slot_const(*data);
        if (value.has_value() && value->valid() && value->is_map()) {
            for_each_map_key_slot(value->as_map(), [&](View /*key*/, size_t slot) {
                ViewData child = *data;
                child.path.indices.push_back(slot);
                out = std::max(out, dispatch_last_modified_time(child));
            });
        }
    }
    if (include_static_children && current != nullptr) {
        const size_t child_count = static_container_child_count(current);
        for (size_t i = 0; i < child_count; ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            out = std::max(out, dispatch_last_modified_time(child));
        }
    }

    return out;
}

bool modified_fallback_no_dispatch(const ViewData& vd,
                                   engine_time_t current_time,
                                   bool           allow_ops_dispatch = true) {
    refresh_dynamic_ref_binding(vd, current_time);
    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
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
                     vd.path.to_string().c_str(),
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

bool valid_fallback_no_dispatch(const ViewData& vd, bool allow_ops_dispatch = true) {
    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
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

}  // namespace

const TSMeta* op_ts_meta(const ViewData& vd) {
    if (is_tsd_key_set_projection(vd)) {
        const TSMeta* source_meta = meta_at_path(vd.meta, vd.path.indices);
        if (source_meta != nullptr && dispatch_meta_is_tsd(source_meta) && source_meta->key_type() != nullptr) {
            return TSTypeRegistry::instance().tss(source_meta->key_type());
        }
    }
    return meta_at_path(vd.meta, vd.path.indices);
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
    const TSMeta* source_meta = meta_at_path(source_view.meta, source_view.path.indices);
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
        if (!source_is_signal && is_tsd_key_set_projection(source_view)) {
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
        signal_link.has_previous_target ? signal_link.last_rebind_time : MIN_DT;
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
    const TSMeta* source_meta = meta_at_path(source_dispatch.meta, source_dispatch.path.indices);
    const bool source_is_ref_wrapper = dispatch_meta_is_ref(source_meta);
    if (source_is_ref_wrapper) {
        View ref_value = op_value(source_dispatch);
        if (!(ref_value.valid() && ref_value.schema() == ts_reference_meta())) {
            return false;
        }
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value.to_python());
        return ref.is_valid();
    }
    return dispatch_valid(source_view);
}

engine_time_t op_last_modified_time(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->last_modified_time != nullptr) {
        return dispatch_vd.ops->last_modified_time(dispatch_vd);
    }
    return last_modified_fallback_no_dispatch(dispatch_vd);
}

engine_time_t op_last_modified_ref(const ViewData& vd) {
    refresh_dynamic_ref_binding(vd, MIN_DT);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return last_modified_fallback_no_dispatch(vd);
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
                TimeSeriesReference local_ref = nb::cast<TimeSeriesReference>(local->to_python());
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
            TimeSeriesReference local_ref = nb::cast<TimeSeriesReference>(local->to_python());
            has_local_ref_value = true;
            local_ref_payload_valid = local_ref.is_valid();
        }

        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
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
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);

    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const engine_time_t ref_wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const bool include_wrapper_time = self_meta == nullptr;
    engine_time_t base_time = include_wrapper_time ? std::max(rebind_time, ref_wrapper_time) : rebind_time;
    if (!include_wrapper_time && vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
            link_target != nullptr &&
            link_target->is_linked &&
            link_target->has_resolved_target &&
            !link_target->has_previous_target) {
            base_time = std::max(base_time, ref_wrapper_time);
        }
    }

    if (vd.uses_link_target) {
        if (LinkTarget* signal_link = resolve_link_target(vd, vd.path.indices); signal_link != nullptr) {
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

bool op_modified_ref(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const TSMeta* element_meta = self_meta->element_ts();
    const bool static_ref_container = element_meta != nullptr && dispatch_meta_is_static_container(element_meta);
    const bool dynamic_ref_container = element_meta != nullptr && dispatch_meta_is_dynamic_container(element_meta);
    bool suppress_wrapper_local_time = vd.uses_link_target && dynamic_ref_container;
    bool resolved_target_modified = false;
    std::string resolved_target_path{"<none>"};
    if (vd.uses_link_target &&
        (!static_ref_container || !has_bound_ref_static_children(vd))) {
        ViewData resolved_target{};
        if (resolve_bound_target_view_data(vd, resolved_target) &&
            !same_view_identity(resolved_target, vd)) {
            suppress_wrapper_local_time = true;
            resolved_target_path = resolved_target.path.to_string();

            // REF->REF chains must preserve wrapper-modified semantics on the
            // immediate source wrapper (rebinds without target-value changes).
            ViewData direct_target{};
            bool direct_target_is_ref = false;
            if (resolve_direct_bound_view_data(vd, direct_target) &&
                !same_view_identity(direct_target, vd)) {
                const TSMeta* direct_target_meta = meta_at_path(direct_target.meta, direct_target.path.indices);
                if (dispatch_meta_is_ref(direct_target_meta)) {
                    direct_target_is_ref = true;
                    resolved_target_path = direct_target.path.to_string();
                }
            }

            if (direct_target_is_ref) {
                // Mirror Python behavior for REF->REF chains: downstream
                // wrappers should tick only when the immediate source REF
                // is itself modified this cycle.
                if (direct_target.ops != nullptr && direct_target.ops->modified != nullptr) {
                    resolved_target_modified = direct_target.ops->modified(direct_target, current_time);
                } else {
                    const engine_time_t direct_rebind = rebind_time_for_view(direct_target);
                    resolved_target_modified = direct_rebind == current_time;
                }
            } else if (resolved_target.ops != nullptr && resolved_target.ops->modified != nullptr) {
                resolved_target_modified = resolved_target.ops->modified(resolved_target, current_time);
            }
        }
    }
    if (!suppress_wrapper_local_time && vd.uses_link_target && static_ref_container) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
            link_target != nullptr && link_target->peered && !has_bound_ref_static_children(vd)) {
            // Peered REF[TSS/TSB]-style wrappers should not tick on child value updates.
            // They tick on bind/rebind, while un-peered child links drive child ticks directly.
            suppress_wrapper_local_time = true;
        }
    }
    const bool debug_ref_modified = std::getenv("HGRAPH_DEBUG_REF_MODIFIED") != nullptr;

    const engine_time_t rebind_time = rebind_time_for_view(vd);
    if (debug_ref_modified) {
        int linked = -1;
        int peered = -1;
        int has_children = has_bound_ref_static_children(vd) ? 1 : 0;
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices); link_target != nullptr) {
            linked = link_target->is_linked ? 1 : 0;
            peered = link_target->peered ? 1 : 0;
        }
        std::fprintf(stderr,
                     "[ref_mod] path=%s sampled=%d now=%lld rebind=%lld suppress=%d resolved_mod=%d resolved_path=%s linked=%d peered=%d has_children=%d\n",
                     vd.path.to_string().c_str(),
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     static_cast<long long>(rebind_time.time_since_epoch().count()),
                     suppress_wrapper_local_time ? 1 : 0,
                     resolved_target_modified ? 1 : 0,
                     resolved_target_path.c_str(),
                     linked,
                     peered,
                     has_children);
    }
    bool rebind_modified = rebind_time == current_time;
    if (rebind_modified) {
        // Suppress only true first-bind empty-wrapper transitions. Python
        // does not surface first-bind transitions where payload is not yet valid.
        bool has_local_ref_value = false;
        bool local_ref_payload_valid = false;
        if (auto local = resolve_value_slot_const(vd);
            local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
            TimeSeriesReference local_ref = nb::cast<TimeSeriesReference>(local->to_python());
            has_local_ref_value = true;
            local_ref_payload_valid = local_ref.is_valid();
        }

        bool suppress_first_empty_bind = false;
        if (vd.uses_link_target) {
            if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices); link_target != nullptr) {
                suppress_first_empty_bind =
                    !link_target->has_previous_target &&
                    !link_target->has_resolved_target &&
                    (!has_local_ref_value || !local_ref_payload_valid);
            } else {
                suppress_first_empty_bind =
                    !has_local_ref_value || !local_ref_payload_valid;
            }
        } else {
            bool parent_is_static_container = false;
            if (!vd.path.indices.empty()) {
                std::vector<size_t> parent_path = vd.path.indices;
                parent_path.pop_back();
                if (const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
                    parent_meta != nullptr) {
                    parent_is_static_container = dispatch_meta_is_static_container(parent_meta);
                }
            }

            const bool has_prior_write = direct_last_modified_time(vd) != MIN_DT;
            suppress_first_empty_bind =
                (!has_prior_write && (!has_local_ref_value || !local_ref_payload_valid)) ||
                (parent_is_static_container && (!has_local_ref_value || !local_ref_payload_valid));
        }
        if (suppress_first_empty_bind) {
            rebind_modified = false;
        }
    }

    if (vd.sampled ||
        rebind_modified ||
        resolved_target_modified) {
        return true;
    }
    if (!vd.uses_link_target && direct_last_modified_time(vd) == current_time) {
        bool local_payload_valid = true;
        if (auto local = resolve_value_slot_const(vd);
            local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
            TimeSeriesReference local_ref = nb::cast<TimeSeriesReference>(local->to_python());
            local_payload_valid = local_ref.is_valid();
        }
        if (local_payload_valid) {
            return true;
        }
    }

    // Non-linked scalar REF wrappers (for example map stub outputs) can keep
    // a stable local reference while the bound target ticks. Surface target
    // modified state so downstream active/valid wiring observes TS semantics.
    const bool scalar_non_ref_wrapper =
        !vd.uses_link_target &&
        element_meta != nullptr &&
        meta_is_scalar_non_ref(element_meta);
    if (scalar_non_ref_wrapper) {
        ViewData direct_target{};
        if (resolve_direct_bound_view_data(vd, direct_target) &&
            !same_view_identity(direct_target, vd)) {
            if (dispatch_modified(direct_target, current_time)) {
                return true;
            }
        }
    }

    if (static_ref_container) {
        const bool has_unpeered_children =
            !vd.uses_link_target || has_bound_ref_static_children(vd);
        if (has_unpeered_children && dispatch_valid(vd)) {
            const size_t n = static_container_child_count(element_meta);
            for (size_t i = 0; i < n; ++i) {
                ViewData child = vd;
                child.path.indices.push_back(i);
                if (vd.uses_link_target) {
                    LinkTarget* child_link = resolve_link_target(vd, child.path.indices);
                    if (child_link == nullptr || !child_link->is_linked) {
                        continue;
                    }
                }
                if (dispatch_modified(child, current_time)) {
                    return true;
                }
            }
        }
    }

    // Type-erased REF wrappers can route concrete bindings through child[0].
    // Keep this only for type-erased/scalar-like wrappers; concrete dynamic
    // container wrappers (REF[TSS]/REF[TSD]) are handled via resolved target
    // and bridge logic above.
    const bool type_erased_or_scalar_ref =
        meta_is_scalar_like_or_ref(element_meta);
    if (vd.uses_link_target && type_erased_or_scalar_ref) {
        ViewData child = vd;
        child.path.indices.push_back(0);
        if (LinkTarget* child_link = resolve_link_target(vd, child.path.indices);
            child_link != nullptr && child_link->is_linked) {
            return dispatch_modified(child, current_time);
        }
    }

    // Dynamic REF container rebind/unbind (e.g. REF[TSD]) should surface as
    // modified on the transition tick so container adapters can emit bridge
    // deltas (add/remove snapshots) even when wrapper local time is unchanged.
    if (vd.uses_link_target && element_meta != nullptr && dispatch_meta_is_dynamic_container(element_meta)) {
        if (rebind_bridge_has_container_meta_value(vd, self_meta, current_time, element_meta)) {
            return true;
        }
    }
    return false;
}

bool op_modified_tsvalue(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(self_meta->kind),
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    const bool valid_now = dispatch_valid(vd);
    if (rebind_time_for_view(vd) == current_time && valid_now) {
        return true;
    }

    if (vd.uses_link_target) {
        if (!valid_now) {
            return false;
        }

        ViewData bound_target{};
        if (resolve_bound_target_view_data(vd, bound_target) &&
            !same_view_identity(bound_target, vd)) {
            if (bound_target.ops != nullptr && bound_target.ops->modified != nullptr) {
                return bound_target.ops->modified(bound_target, current_time);
            }
            return dispatch_last_modified_time(bound_target) == current_time;
        }
    }

    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, is_tsd_key_set_projection(vd));
        key_set_result.has_value()) {
        return *key_set_result;
    }

    return modified_default_tail(vd, current_time);
}

bool op_modified_signal(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(self_meta->kind),
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

bool op_modified_tsw(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    return modified_fallback_no_dispatch(vd, current_time, false);
}

bool op_modified_tss(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(self_meta->kind),
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, true);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    const auto bridge_state = resolve_tsd_key_set_bridge_state(vd, current_time);
    if (bridge_state.has_previous_source || bridge_state.has_current_source) {
        return true;
    }

    return modified_default_tail(vd, current_time);
}

bool op_modified_tsd(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(self_meta->kind),
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, is_tsd_key_set_projection(vd));
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (rebind_bridge_has_container_meta_value(vd, self_meta, current_time, self_meta)) {
        return true;
    }

    if (vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
            is_first_bind_rebind_tick(link_target, current_time)) {
            ViewData current_view =
                link_target->has_resolved_target ? link_target->resolved_target : link_target->as_view_data(vd.sampled);
            if (view_matches_container_meta_value(resolve_value_slot_const(current_view), self_meta)) {
                return true;
            }
        }
    }

    const bool declared_ref_valued_tsd =
        self_meta->element_ts() != nullptr && dispatch_meta_is_ref(self_meta->element_ts());
    ViewData resolved{};
    if (resolve_read_view_data(vd, resolved)) {
        bool any_child_modified = false;
        const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
        const TSMeta* element_meta = resolved_meta != nullptr ? resolved_meta->element_ts() : nullptr;
        const bool ref_valued_tsd = element_meta != nullptr && dispatch_meta_is_ref(element_meta);
        const bool suppress_ref_target_child_mods =
            ref_valued_tsd && vd.path.port_type == PortType::INPUT;
        const bool include_ref_target_child_mods =
            ref_valued_tsd && vd.path.port_type == PortType::OUTPUT;
        auto value = resolve_value_slot_const(resolved);

        // Dynamic TSD key-slot views (path .../key_slot) resolve to scalar
        // payloads, not maps. Treat those as scalar leaves instead of
        // container key_set projections.
        if (value.has_value() && value->valid() && !value->is_map()) {
            return modified_default_tail(resolved, current_time);
        }

        if (value.has_value() && value->valid() && value->is_map()) {
            for_each_map_key_slot(value->as_map(), [&](View /*key*/, size_t slot) {
                if (any_child_modified) {
                    return;
                }
                ViewData child = resolved;
                child.path.indices.push_back(slot);
                if (suppress_ref_target_child_mods) {
                    // Python parity: TSD[REF[...]] container modified state tracks
                    // structural/reference wrapper changes, not target value updates.
                    any_child_modified = direct_last_modified_time(child) == current_time;
                } else {
                    any_child_modified = dispatch_modified(child, current_time);
                    if (!any_child_modified && include_ref_target_child_mods) {
                        const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
                        if (child_meta != nullptr && dispatch_meta_is_ref(child_meta)) {
                            ViewData target{};
                            any_child_modified =
                                resolve_bound_target_view_data(child, target) &&
                                dispatch_modified(target, current_time);
                        }
                    }
                }
            });
        }

        if (any_child_modified) {
            return true;
        }

        const bool key_set_modified = tsd_key_set_modified_this_tick(resolved, current_time);
        if (debug_op_modified) {
            const auto state = tsd_key_set_delta_state(resolved);
            auto value = resolve_value_slot_const(resolved);
            std::fprintf(stderr,
                         "[op_mod_tsd] path=%s now=%lld key_set_mod=%d has_tuple=%d has_changed_map=%d has_added=%d has_removed=%d value_valid=%d value_is_map=%d value_size=%zu sampled=%d\n",
                         resolved.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         key_set_modified ? 1 : 0,
                         state.has_delta_tuple ? 1 : 0,
                         state.has_changed_values_map ? 1 : 0,
                         state.has_added ? 1 : 0,
                         state.has_removed ? 1 : 0,
                         (value.has_value() && value->valid()) ? 1 : 0,
                         (value.has_value() && value->valid() && value->is_map()) ? 1 : 0,
                         (value.has_value() && value->valid() && value->is_map()) ? value->as_map().size() : 0,
                         vd.sampled ? 1 : 0);
        }
        if (key_set_modified) {
            return true;
        }

        if (!vd.sampled) {
            return false;
        }
    } else if (declared_ref_valued_tsd && !vd.sampled) {
        if (tsd_key_set_modified_this_tick(vd, current_time)) {
            return true;
        }

        bool any_wrapper_child_modified = false;
        auto value = resolve_value_slot_const(vd);
        if (value.has_value() && value->valid() && value->is_map()) {
            for_each_map_key_slot(value->as_map(), [&](View /*key*/, size_t slot) {
                if (any_wrapper_child_modified) {
                    return;
                }
                ViewData child = vd;
                child.path.indices.push_back(slot);
                any_wrapper_child_modified = direct_last_modified_time(child) == current_time;
            });
        }

        if (any_wrapper_child_modified) {
            return true;
        }

        return false;
    }

    return modified_default_tail(vd, current_time);
}

bool op_modified_tsb(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(self_meta->kind),
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, is_tsd_key_set_projection(vd));
        key_set_result.has_value()) {
        return *key_set_result;
    }

    const size_t n = static_container_child_count(self_meta);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);
        if (dispatch_modified(child, current_time)) {
            return true;
        }
    }
    if (!vd.sampled) {
        return false;
    }
    return modified_default_tail(vd, current_time);
}

bool op_modified_tsl(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_op_modified = std::getenv("HGRAPH_DEBUG_OP_MODIFIED") != nullptr;
    if (debug_op_modified) {
        std::fprintf(stderr,
                     "[op_mod] path=%s kind=%d uses_lt=%d sampled=%d now=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<int>(self_meta->kind),
                     vd.uses_link_target ? 1 : 0,
                     vd.sampled ? 1 : 0,
                     static_cast<long long>(current_time.time_since_epoch().count()));
    }

    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, is_tsd_key_set_projection(vd));
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (self_meta->fixed_size() > 0) {
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (dispatch_modified(child, current_time)) {
                return true;
            }
        }
        if (!vd.sampled) {
            return false;
        }
    }
    return modified_default_tail(vd, current_time);
}

bool op_modified(const ViewData& vd, engine_time_t current_time) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->modified != nullptr) {
        return dispatch_vd.ops->modified(dispatch_vd, current_time);
    }
    return modified_fallback_no_dispatch(dispatch_vd, current_time);
}

bool op_valid_tsvalue(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    if (vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
            link_target != nullptr && !link_target->is_linked) {
            ViewData bound_target{};
            if (!resolve_bound_target_view_data(vd, bound_target)) {
                return false;
            }
        }
    }

    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    return valid_from_resolved_slot(vd, self_meta, false);
}

bool op_valid_ref(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const bool debug_ref_valid = std::getenv("HGRAPH_DEBUG_REF_VALID") != nullptr;
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    if (auto local = resolve_value_slot_const(vd);
        local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
        if (debug_ref_valid) {
            std::fprintf(stderr,
                         "[ref_valid] path=%s now=%lld uses_lt=%d local_valid=1 empty=%d unbound=%d payload_valid=%d lmt=%lld\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         vd.uses_link_target ? 1 : 0,
                         ref.is_empty() ? 1 : 0,
                         ref.is_unbound() ? 1 : 0,
                         ref.is_valid() ? 1 : 0,
                         static_cast<long long>(direct_last_modified_time(vd).time_since_epoch().count()));
        }
        if (ref.is_empty()) {
            const TSMeta* element_meta = self_meta->element_ts();
            const bool static_ref_container =
                element_meta != nullptr && dispatch_meta_is_static_container(element_meta);
            if (!vd.uses_link_target && !static_ref_container) {
                if (debug_ref_valid) {
                    std::fprintf(stderr, "[ref_valid]  -> true (direct non-static empty)\n");
                }
                return true;
            }

            // Placeholder empty wrappers on linked REF inputs should mirror
            // source REF validity (an empty REF payload is still valid).
            if (auto bound = resolve_bound_view_data(vd); bound.has_value()) {
                if (auto source_local = resolve_value_slot_const(*bound);
                    source_local.has_value() && source_local->valid() &&
                    source_local->schema() == ts_reference_meta()) {
                    if (debug_ref_valid) {
                        std::fprintf(stderr, "[ref_valid]  -> true (bound source has ref value)\n");
                    }
                    return true;
                }
            }
        } else {
            if (debug_ref_valid) {
                std::fprintf(stderr, "[ref_valid]  -> true (non-empty ref)\n");
            }
            return true;
        }
    }

    if (vd.uses_link_target) {
        if (LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
            link_target != nullptr && link_target->is_linked) {
            if (debug_ref_valid) {
                std::fprintf(stderr, "[ref_valid]  -> true (linked wrapper)\n");
            }
            return true;
        }

        ViewData bound_target{};
        if (resolve_bound_target_view_data(vd, bound_target) &&
            !same_view_identity(bound_target, vd)) {
            if (debug_ref_valid) {
                std::fprintf(stderr, "[ref_valid]  -> true (ancestor/derived bound target)\n");
            }
            return true;
        }
    }

    const TSMeta* element_meta = self_meta->element_ts();
    if (element_meta != nullptr && dispatch_meta_is_static_container(element_meta)) {
        const size_t n = static_container_child_count(element_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (dispatch_valid(child)) {
                if (debug_ref_valid) {
                    std::fprintf(stderr, "[ref_valid]  -> true (static child valid idx=%zu)\n", i);
                }
                return true;
            }
        }
    }

    // Type-erased REF wrappers can route concrete bindings through child[0].
    if (vd.uses_link_target) {
        ViewData child = vd;
        child.path.indices.push_back(0);
        if (LinkTarget* child_link = resolve_link_target(vd, child.path.indices);
            child_link != nullptr && child_link->is_linked) {
            if (debug_ref_valid) {
                std::fprintf(stderr, "[ref_valid]  -> delegate child0\n");
            }
            return dispatch_valid(child);
        }
    }

    const auto ref_wrapper_valid = [](const ViewData& ref_vd) -> bool {
        auto local = resolve_value_slot_const(ref_vd);
        if (!local.has_value() || !local->valid()) {
            return false;
        }
        return direct_last_modified_time(ref_vd) > MIN_DT;
    };
    if (debug_ref_valid) {
        auto local = resolve_value_slot_const(vd);
        std::fprintf(stderr,
                     "[ref_valid] path=%s now=%lld uses_lt=%d local_has=%d local_valid=%d lmt=%lld\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(view_evaluation_time(vd).time_since_epoch().count()),
                     vd.uses_link_target ? 1 : 0,
                     local.has_value() ? 1 : 0,
                     (local.has_value() && local->valid()) ? 1 : 0,
                     static_cast<long long>(dispatch_last_modified_time(vd).time_since_epoch().count()));
    }
    if (ref_wrapper_valid(vd)) {
        if (debug_ref_valid) {
            std::fprintf(stderr, "[ref_valid] path=%s source=local -> true\n", vd.path.to_string().c_str());
        }
        return true;
    }

    ViewData direct_bound{};
    if (resolve_direct_bound_view_data(vd, direct_bound)) {
        const TSMeta* direct_bound_meta = meta_at_path(direct_bound.meta, direct_bound.path.indices);
        const bool direct_bound_is_ref_wrapper = dispatch_meta_is_ref(direct_bound_meta);
        const bool bound_valid =
            direct_bound_is_ref_wrapper ? ref_wrapper_valid(direct_bound) : dispatch_valid(direct_bound);
        if (bound_valid) {
            if (debug_ref_valid) {
                std::fprintf(stderr,
                             "[ref_valid] path=%s source=direct_bound bound=%s -> true\n",
                             vd.path.to_string().c_str(),
                             direct_bound.path.to_string().c_str());
            }
            return true;
        }
    }

    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (debug_ref_valid) {
        std::fprintf(stderr,
                     "[ref_valid] path=%s now=%lld uses_lt=%d -> false (no ref-valid path)\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     vd.uses_link_target ? 1 : 0);
        std::fprintf(stderr, "[ref_valid] path=%s source=ref_wrapper -> false\n", vd.path.to_string().c_str());
    }
    return valid_from_resolved_slot(vd, self_meta, true);
}

bool op_valid_signal(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (vd.uses_link_target) {
        if (LinkTarget* signal_link = resolve_link_target(vd, vd.path.indices); signal_link != nullptr) {
            if (std::optional<bool> signal_valid = signal_valid_override(vd, *signal_link);
                signal_valid.has_value()) {
                return *signal_valid;
            }
        }
    }

    return valid_from_resolved_slot(vd, self_meta, false);
}

bool op_valid_tsw(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    return valid_fallback_no_dispatch(vd, false);
}

bool op_valid_tss(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (vd.uses_link_target) {
        if (LinkTarget* lt = resolve_link_target(vd, vd.path.indices);
            lt != nullptr && lt->is_linked && lt->has_previous_target) {
            return true;
        }
    }

    if (debug_keyset_valid) {
        auto bound = resolve_bound_view_data(vd);
        if (bound.has_value()) {
            auto local = resolve_value_slot_const(*bound);
            int is_empty_ref = 0;
            if (local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
                is_empty_ref = ref.is_empty() ? 1 : 0;
            }
            std::fprintf(stderr,
                         "[keyset_valid] fallback path=%s bound=%s local_ref_empty=%d\n",
                         vd.path.to_string().c_str(),
                         bound->path.to_string().c_str(),
                         is_empty_ref);
        } else {
            std::fprintf(stderr,
                         "[keyset_valid] fallback path=%s bound=<none>\n",
                         vd.path.to_string().c_str());
        }
    }

    return valid_from_resolved_slot(vd, self_meta, false);
}

bool op_valid_tsd(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (vd.uses_link_target && current_time != MIN_DT &&
        rebind_bridge_has_container_meta_value(vd, self_meta, current_time, self_meta)) {
        return true;
    }

    return valid_from_resolved_slot(vd, self_meta, false);
}

bool op_valid_tsb(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    const size_t n = static_container_child_count(self_meta);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);
        if (dispatch_valid(child)) {
            return true;
        }
    }
    return false;
}

bool op_valid_tsl(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        refresh_dynamic_ref_binding(vd, current_time);
    }

    if (self_meta->fixed_size() > 0) {
        const size_t n = static_container_child_count(self_meta);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = vd;
            child.path.indices.push_back(i);
            if (dispatch_valid(child)) {
                return true;
            }
        }
        return false;
    }

    const bool debug_keyset_valid = std::getenv("HGRAPH_DEBUG_KEYSET_VALID") != nullptr;
    if (std::optional<bool> key_set_result = valid_from_key_set_source(vd, debug_keyset_valid);
        key_set_result.has_value()) {
        return *key_set_result;
    }
    return valid_from_resolved_slot(vd, self_meta, false);
}

bool op_valid(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->valid != nullptr) {
        return dispatch_vd.ops->valid(dispatch_vd);
    }
    return valid_fallback_no_dispatch(dispatch_vd);
}

bool op_all_valid_tsw_tick(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return op_all_valid(vd);
    }
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->all_valid != nullptr) {
        return dispatch_data.ops->all_valid(dispatch_data);
    }
    if (!dispatch_valid(*data)) {
        return false;
    }
    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return false;
    }

    View window_value = op_value(*data);
    const size_t length =
        window_value.valid() && window_value.is_cyclic_buffer() ? window_value.as_cyclic_buffer().size() : 0;
    return length >= current->min_period();
}

bool op_all_valid_tsw_duration(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return op_all_valid(vd);
    }
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->all_valid != nullptr) {
        return dispatch_data.ops->all_valid(dispatch_data);
    }
    if (!dispatch_valid(*data)) {
        return false;
    }

    auto* time_root = static_cast<const Value*>(data->time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return false;
    }
    auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
    if (time_path.empty()) {
        return false;
    }
    time_path.pop_back();
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time.has_value() || !maybe_time->valid() || !maybe_time->is_tuple()) {
        return false;
    }
    auto tuple = maybe_time->as_tuple();
    if (tuple.size() < 4) {
        return false;
    }
    View ready = tuple.at(3);
    return ready.valid() && ready.is_scalar_type<bool>() && ready.as<bool>();
}

bool op_all_valid_tsw(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->all_valid != nullptr) {
        return dispatch_vd.ops->all_valid(dispatch_vd);
    }
    return op_all_valid_tsw_tick(dispatch_vd);
}

bool op_all_valid_tsb(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return op_all_valid(vd);
    }

    const size_t n = static_container_child_count(self_meta);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);
        if (!container_child_valid_for_aggregation(child)) {
            return false;
        }
    }
    return true;
}

bool op_all_valid_tsl(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr || self_meta->fixed_size() == 0) {
        return op_all_valid(vd);
    }

    const size_t n = static_container_child_count(self_meta);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);
        if (!container_child_valid_for_aggregation(child)) {
            return false;
        }
    }
    return true;
}

bool op_all_valid(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->all_valid != nullptr) {
        return dispatch_vd.ops->all_valid(dispatch_vd);
    }
    return dispatch_valid(dispatch_vd);
}

bool op_sampled(const ViewData& vd) {
    return vd.sampled;
}

View op_value_non_ref(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;

    auto* value_root = static_cast<const Value*>(data->value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return {};
    }
    auto maybe = navigate_const(value_root->view(), data->path.indices);
    return maybe.has_value() ? *maybe : View{};
}

View op_value_ref(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return op_value_non_ref(vd);
    }

    const bool    debug_ref_value = std::getenv("HGRAPH_DEBUG_REF_VALUE_PATH") != nullptr;
    const TSMeta* element_meta = self_meta->element_ts();
    const bool    static_ref_container =
        element_meta != nullptr && dispatch_meta_is_static_container(element_meta);

    // REF[TSD]/REF[TSB]/REF[TSL-fixed] wrappers can be driven by child bindings
    // without a direct parent bind. Materialize the composite REF payload so
    // Python-facing stubs observe REF[child_0, child_1, ...] semantics.
    if (vd.uses_link_target && static_ref_container && has_bound_ref_static_children(vd)) {
        ViewData mutable_vd = vd;
        if (assign_ref_value_from_bound_static_children(mutable_vd)) {
            if (auto synthesized = resolve_value_slot_const(vd);
                synthesized.has_value() && synthesized->valid()) {
                return *synthesized;
            }
        }
    }

    if (auto local = resolve_value_slot_const(vd); local.has_value() && local->valid()) {
        if (local->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
            if (!ref.is_empty()) {
                if (vd.uses_link_target) {
                    // Linked REF inputs can cache a local self-reference to the bound
                    // peer REF endpoint. In that case, expose the peer payload instead
                    // of the wrapper reference itself.
                    const auto same_bound_endpoint = [](const ViewData& lhs, const ViewData& rhs) {
                        return lhs.value_data == rhs.value_data &&
                               lhs.time_data == rhs.time_data &&
                               lhs.observer_data == rhs.observer_data &&
                               lhs.delta_data == rhs.delta_data &&
                               lhs.link_data == rhs.link_data &&
                               lhs.path.indices == rhs.path.indices;
                    };
                    if (const ViewData* local_target = ref.bound_view(); local_target != nullptr) {
                        if (auto peer = resolve_bound_view_data(vd); peer.has_value()) {
                            const bool same_endpoint = same_bound_endpoint(*local_target, *peer);
                            if (debug_ref_value) {
                                std::fprintf(stderr,
                                             "[op_value_ref] path=%s local_target=%s peer=%s same=%d\n",
                                             vd.path.to_string().c_str(),
                                             local_target->path.to_string().c_str(),
                                             peer->path.to_string().c_str(),
                                             same_endpoint ? 1 : 0);
                            }
                            if (!same_endpoint) {
                                return *local;
                            }
                        } else {
                            if (debug_ref_value) {
                                std::fprintf(stderr,
                                             "[op_value_ref] path=%s local_target=%s peer=<none>\n",
                                             vd.path.to_string().c_str(),
                                             local_target->path.to_string().c_str());
                            }
                            return *local;
                        }
                    } else {
                        return *local;
                    }
                } else {
                    return *local;
                }
            }
        } else {
            return *local;
        }
    }

    if (auto bound = resolve_bound_view_data(vd); bound.has_value()) {
        ViewData resolved = *bound;
        bind_view_data_ops(resolved);
        const bool same_view = resolved.value_data == vd.value_data &&
                               resolved.time_data == vd.time_data &&
                               resolved.link_data == vd.link_data &&
                               resolved.path.indices == vd.path.indices;
        if (!same_view) {
            const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
            const bool resolved_is_ref_wrapper = dispatch_meta_is_ref(resolved_meta);
            if (resolved_is_ref_wrapper) {
                if (resolved.ops != nullptr && resolved.ops->value != nullptr) {
                    return resolved.ops->value(resolved);
                }
                return op_value(resolved);
            }
        }
    }

    return {};
}

View op_value(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->value != nullptr) {
        return dispatch_vd.ops->value(dispatch_vd);
    }
    return op_value_non_ref(dispatch_vd);
}

View op_delta_value_scalar(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }
    return op_value(vd);
}

View op_delta_value_container(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        if (auto delta_path = ts_path_to_delta_path(data->meta, data->path.indices); delta_path.has_value()) {
            std::optional<View> maybe;
            if (delta_path->empty()) {
                maybe = delta_root->view();
            } else {
                maybe = navigate_const(delta_root->view(), *delta_path);
            }
            if (maybe.has_value()) {
                return *maybe;
            }
        }
    }

    return {};
}

View op_delta_value_tsw_tick(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return {};
    }
    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->delta_value != nullptr) {
        return dispatch_data.ops->delta_value(dispatch_data);
    }
    const TSMeta*   current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return {};
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }

    View window_value = op_value(*data);
    if (!window_value.valid()) {
        return {};
    }
    if (!window_value.is_cyclic_buffer() || window_value.as_cyclic_buffer().size() == 0) {
        return {};
    }
    auto buffer = window_value.as_cyclic_buffer();
    const void* newest =
        value::CyclicBufferOps::get_element_ptr_const(buffer.data(), buffer.size() - 1, buffer.schema());
    return newest != nullptr ? View(newest, current->value_type) : View{};
}

View op_delta_value_tsw_duration(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return {};
    }
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return {};
    }
    const ViewData* data = &resolved;
    ViewData dispatch_data = dispatch_view_for_path(*data);
    const ts_ops* self_ops = get_ts_ops(self_meta);
    if (dispatch_data.ops != nullptr &&
        dispatch_data.ops != self_ops &&
        dispatch_data.ops->delta_value != nullptr) {
        return dispatch_data.ops->delta_value(dispatch_data);
    }
    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return {};
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return {};
    }

    View window_value = op_value(*data);
    if (!window_value.valid()) {
        return {};
    }

    auto* time_root = static_cast<const Value*>(data->time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return {};
    }
    auto time_path = ts_path_to_time_path(data->meta, data->path.indices);
    if (time_path.empty()) {
        return {};
    }
    time_path.pop_back();
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time.has_value() || !maybe_time->valid() || !maybe_time->is_tuple()) {
        return {};
    }
    auto tuple = maybe_time->as_tuple();
    if (tuple.size() < 4) {
        return {};
    }
    View ready = tuple.at(3);
    if (!ready.valid() || !ready.is_scalar_type<bool>() || !ready.as<bool>()) {
        return {};
    }
    if (!window_value.is_queue() || window_value.as_queue().size() == 0) {
        return {};
    }
    auto queue = window_value.as_queue();
    const void* newest = value::QueueOps::get_element_ptr_const(queue.data(), queue.size() - 1, queue.schema());
    return newest != nullptr ? View(newest, current->value_type) : View{};
}

View op_delta_value_tsw(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->delta_value != nullptr) {
        return dispatch_vd.ops->delta_value(dispatch_vd);
    }
    return op_delta_value_tsw_tick(dispatch_vd);
}

View op_delta_value(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->delta_value != nullptr) {
        return dispatch_vd.ops->delta_value(dispatch_vd);
    }
    return op_delta_value_container(dispatch_vd);
}

bool op_has_delta_default(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        return true;
    }

    return false;
}

bool op_has_delta(const ViewData& vd) {
    ViewData dispatch_vd = dispatch_view_for_path(vd);
    if (dispatch_vd.ops != nullptr && dispatch_vd.ops->has_delta != nullptr) {
        return dispatch_vd.ops->has_delta(dispatch_vd);
    }
    return op_has_delta_default(dispatch_vd);
}

bool op_has_delta_scalar(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root != nullptr && delta_root->has_value()) {
        return true;
    }

    return dispatch_valid(*data);
}

bool op_has_delta_tss(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return false;
    }

    std::optional<View> maybe_delta;
    if (data->path.indices.empty()) {
        maybe_delta = delta_root->view();
    } else {
        maybe_delta = navigate_const(delta_root->view(), data->path.indices);
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return false;
    }

    auto tuple = maybe_delta->as_tuple();
    const bool has_added =
        tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_set() && tuple.at(0).as_set().size() > 0;
    const bool has_removed =
        tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    return has_added || has_removed;
}

bool op_has_delta_tsd(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }

    ViewData      resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return false;
    }
    const ViewData* data = &resolved;

    auto* delta_root = static_cast<const Value*>(data->delta_data);
    if (delta_root == nullptr || !delta_root->has_value()) {
        return false;
    }

    std::optional<View> maybe_delta;
    if (data->path.indices.empty()) {
        maybe_delta = delta_root->view();
    } else {
        maybe_delta = navigate_const(delta_root->view(), data->path.indices);
    }
    if (!maybe_delta.has_value() || !maybe_delta->valid() || !maybe_delta->is_tuple()) {
        return false;
    }

    auto tuple = maybe_delta->as_tuple();
    const bool has_changed =
        tuple.size() > 0 && tuple.at(0).valid() && tuple.at(0).is_map() && tuple.at(0).as_map().size() > 0;
    const bool has_added =
        tuple.size() > 1 && tuple.at(1).valid() && tuple.at(1).is_set() && tuple.at(1).as_set().size() > 0;
    const bool has_removed =
        tuple.size() > 2 && tuple.at(2).valid() && tuple.at(2).is_set() && tuple.at(2).as_set().size() > 0;
    return has_changed || has_added || has_removed;
}

void op_set_value(ViewData& vd, const View& src, engine_time_t current_time) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr || value_root->schema() == nullptr) {
        return;
    }

    if (!value_root->has_value()) {
        value_root->emplace();
    }

    if (vd.path.indices.empty()) {
        if (!src.valid()) {
            value_root->reset();
        } else {
            if (value_root->schema() != src.schema()) {
                throw std::runtime_error(
                    "TS scaffolding set_value root schema mismatch at path " + vd.path.to_string());
            }
            value_root->schema()->ops().copy(value_root->data(), src.data(), value_root->schema());
        }
    } else {
        auto maybe_dst = navigate_mut(value_root->view(), vd.path.indices);
        if (maybe_dst.has_value() && src.valid()) {
            copy_view_data(*maybe_dst, src);
        }
    }
    mark_tsd_parent_child_modified(vd, current_time);
    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void op_apply_delta_scalar(ViewData& vd, const View& delta, engine_time_t current_time) {
    op_set_value(vd, delta, current_time);
}

void op_apply_delta_container(ViewData& vd, const View& delta, engine_time_t current_time) {
    auto* delta_root = static_cast<Value*>(vd.delta_data);
    if (delta_root == nullptr || delta_root->schema() == nullptr) {
        return;
    }

    if (!delta_root->has_value()) {
        delta_root->emplace();
    }

    if (vd.path.indices.empty()) {
        if (!delta.valid()) {
            delta_root->reset();
        } else if (delta_root->schema() == delta.schema()) {
            delta_root->schema()->ops().copy(delta_root->data(), delta.data(), delta_root->schema());
        }
    } else {
        auto maybe_dst = navigate_mut(delta_root->view(), vd.path.indices);
        if (maybe_dst.has_value() && delta.valid()) {
            copy_view_data(*maybe_dst, delta);
        }
    }

    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

void op_apply_delta(ViewData& vd, const View& delta, engine_time_t current_time) {
    bind_view_data_ops(vd);
    if (vd.ops != nullptr && vd.ops->apply_delta != nullptr) {
        vd.ops->apply_delta(vd, delta, current_time);
    } else {
        op_apply_delta_container(vd, delta, current_time);
    }
}

void op_invalidate(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }

    const engine_time_t current_time = view_evaluation_time(vd);

    if (vd.path.indices.empty()) {
        value_root->reset();
    }

    if (current_time != MIN_DT) {
        notify_link_target_observers(vd, current_time);
    }
}

void op_invalidate_tsd(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }

    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT) {
        auto maybe_current = resolve_value_slot_const(vd);
        if (maybe_current.has_value() && maybe_current->valid() && maybe_current->is_map()) {
            auto slots = resolve_tsd_delta_slots(vd);
            clear_tsd_delta_if_new_tick(vd, current_time, slots);

            const auto current_map = maybe_current->as_map();
            const bool has_added_set = slots.added_set.valid() && slots.added_set.is_set();
            if (slots.removed_set.valid() && slots.removed_set.is_set()) {
                auto removed_set = slots.removed_set.as_set();
                for (View key : current_map.keys()) {
                    if (has_added_set && slots.added_set.as_set().contains(key)) {
                        continue;
                    }
                    removed_set.add(key);
                }
            }
            if (slots.changed_values_map.valid() && slots.changed_values_map.is_map()) {
                auto changed_map = slots.changed_values_map.as_map();
                for (View key : current_map.keys()) {
                    changed_map.remove(key);
                }
            }
        }
    }

    if (vd.path.indices.empty()) {
        value_root->reset();
    }

    if (current_time != MIN_DT) {
        notify_link_target_observers(vd, current_time);
    }
}

bool reset_root_value_and_delta_on_none(ViewData& vd,
                                        const nb::object& src,
                                        engine_time_t current_time) {
    if (!vd.path.indices.empty() || !src.is_none()) {
        return false;
    }

    if (auto* value_root = static_cast<Value*>(vd.value_data); value_root != nullptr) {
        value_root->reset();
    }
    if (auto* delta_root = static_cast<Value*>(vd.delta_data); delta_root != nullptr) {
        delta_root->reset();
    }
    stamp_time_paths(vd, current_time);
    notify_link_target_observers(vd, current_time);
    return true;
}


}  // namespace hgraph
