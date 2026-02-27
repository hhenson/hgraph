#include "ts_ops_internal.h"

namespace hgraph {

const TSMeta* op_ts_meta_tsd_key_set(const ViewData& vd) {
    const TSMeta* source_meta = meta_at_path(vd.meta, vd.path.indices);
    if (source_meta != nullptr &&
        dispatch_meta_is_tsd(source_meta) &&
        source_meta->key_type() != nullptr) {
        return TSTypeRegistry::instance().tss(source_meta->key_type());
    }
    return op_ts_meta(vd);
}

bool op_modified_tsd_key_set(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    if (self_meta == nullptr) {
        return modified_fallback_no_dispatch(vd, current_time, false);
    }

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    if (std::optional<bool> key_set_result = modified_from_key_set_source(
            vd, current_time, debug_keyset_bridge, true);
        key_set_result.has_value()) {
        return *key_set_result;
    }

    if (rebind_bridge_has_container_meta_value(vd, self_meta, current_time, self_meta)) {
        return true;
    }

    return modified_default_tail(vd, current_time);
}

bool op_valid_tsd_key_set(const ViewData& vd) {
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

bool op_has_delta_tsd_key_set(const ViewData& vd) {
    const engine_time_t current_time = view_evaluation_time(vd);
    if (current_time != MIN_DT && !dispatch_modified(vd, current_time)) {
        return false;
    }

    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_has_added_or_removed(key_set_source);
    }
    return false;
}

nb::object op_to_python_tsd_key_set(const ViewData& vd) {
    ViewData key_set_source{};
    if (resolve_tsd_key_set_source(vd, key_set_source)) {
        return tsd_key_set_to_python(key_set_source);
    }
    return nb::frozenset(nb::set{});
}

nb::object op_delta_to_python_tsd_key_set(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);

    const bool debug_keyset_bridge = std::getenv("HGRAPH_DEBUG_KEYSET_BRIDGE") != nullptr;
    const bool debug_delta_kind = std::getenv("HGRAPH_DEBUG_DELTA_KIND") != nullptr;

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
    return nb::none();
}

}  // namespace hgraph
