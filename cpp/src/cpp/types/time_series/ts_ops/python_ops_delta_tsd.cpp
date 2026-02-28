#include "ts_ops_internal.h"

namespace hgraph {

namespace {

template <bool DeclaredRefElement, bool HasDeclaredNestedElement, bool DeclaredNestedElement>
nb::object op_delta_to_python_tsd_impl_for_bound_scenario(const ViewData& vd, engine_time_t current_time) {
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

    const bool debug_ref_payload = std::getenv("HGRAPH_DEBUG_TSD_REF_PAYLOAD") != nullptr;

    {
        nb::object bridge_delta;
        if (try_tsd_bridge_delta_to_python(
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
    if (try_tsd_bridge_delta_to_python(
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

    if constexpr (DeclaredRefElement) {
        tsd_emit_map_delta_ref_elements(
            vd,
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
    } else if constexpr (HasDeclaredNestedElement) {
        if constexpr (DeclaredNestedElement) {
            tsd_emit_map_delta_plain_nested(
                vd,
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
        } else {
            tsd_emit_map_delta_plain_scalar(
                vd,
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
    } else {
        tsd_emit_map_delta_plain(
            vd,
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

}  // namespace

nb::object op_delta_to_python_tsd_impl(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_tsd_impl_for_bound_scenario<false, false, false>(vd, current_time);
}

nb::object op_delta_to_python_tsd_scalar_impl(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_tsd_impl_for_bound_scenario<false, true, false>(vd, current_time);
}

nb::object op_delta_to_python_tsd_nested_impl(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_tsd_impl_for_bound_scenario<false, true, true>(vd, current_time);
}

nb::object op_delta_to_python_tsd_ref_impl(const ViewData& vd, engine_time_t current_time) {
    return op_delta_to_python_tsd_impl_for_bound_scenario<true, false, false>(vd, current_time);
}

}  // namespace hgraph
