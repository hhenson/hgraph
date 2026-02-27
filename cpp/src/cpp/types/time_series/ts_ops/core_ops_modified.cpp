#include "ts_ops_internal.h"

namespace hgraph {

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
    return modified_fallback_no_dispatch(dispatch_vd, current_time, true);
}

}  // namespace hgraph
