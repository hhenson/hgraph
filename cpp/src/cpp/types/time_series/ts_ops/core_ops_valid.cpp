#include "ts_ops_internal.h"

namespace hgraph {

bool op_valid_tsvalue(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
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

namespace {

template <bool DeclaredScalarLike, bool DeclaredStaticContainer, bool DeclaredDynamicContainer>
bool is_static_ref_container_for_scenario(const TSMeta* element_meta) {
    if constexpr (DeclaredStaticContainer) {
        return true;
    }
    if constexpr (DeclaredDynamicContainer || DeclaredScalarLike) {
        return false;
    }
    return element_meta != nullptr && dispatch_meta_is_static_container(element_meta);
}

template <bool DeclaredScalarLike, bool DeclaredStaticContainer, bool DeclaredDynamicContainer>
bool use_ref_child_zero_dispatch_for_scenario(const TSMeta* element_meta) {
    if constexpr (DeclaredScalarLike) {
        return true;
    }
    if constexpr (DeclaredStaticContainer || DeclaredDynamicContainer) {
        return false;
    }
    return meta_is_scalar_like_or_ref(element_meta);
}

template <bool DeclaredScalarLike, bool DeclaredStaticContainer, bool DeclaredDynamicContainer>
bool op_valid_ref_impl(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const TSMeta* element_meta = self_meta->element_ts();
    const bool static_ref_container =
        is_static_ref_container_for_scenario<DeclaredScalarLike, DeclaredStaticContainer, DeclaredDynamicContainer>(
            element_meta);
    const bool use_ref_child_zero_dispatch =
        use_ref_child_zero_dispatch_for_scenario<DeclaredScalarLike, DeclaredStaticContainer, DeclaredDynamicContainer>(
            element_meta);
    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
    const bool debug_ref_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_VALID");
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

    if (static_ref_container && element_meta != nullptr) {
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
    if (vd.uses_link_target && use_ref_child_zero_dispatch) {
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
        bind_view_data_ops(direct_bound);
        const bool direct_bound_is_ref_wrapper =
            direct_bound.ops != nullptr && direct_bound.ops->value == &op_value_ref;
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

}  // namespace

bool op_valid_ref(const ViewData& vd) {
    return op_valid_ref_impl<false, false, false>(vd);
}

bool op_valid_ref_scalar(const ViewData& vd) {
    return op_valid_ref_impl<true, false, false>(vd);
}

bool op_valid_ref_static_container(const ViewData& vd) {
    return op_valid_ref_impl<false, true, false>(vd);
}

bool op_valid_ref_dynamic_container(const ViewData& vd) {
    return op_valid_ref_impl<false, false, true>(vd);
}

bool op_valid_signal(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
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
    return valid_fallback_no_dispatch(vd, false);
}

bool op_valid_tss(const ViewData& vd) {
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return valid_fallback_no_dispatch(vd, false);
    }

    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
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

    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
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

    const bool debug_keyset_valid = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_KEYSET_VALID");
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
    return valid_fallback_no_dispatch(dispatch_vd, true);
}

}  // namespace hgraph
