#include "ts_ops_internal.h"

namespace hgraph {

void op_from_python_ref(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr) {
        return;
    }

    const bool debug_ref_from = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_FROM");

    // Python TimeSeriesReferenceOutput.apply_result(None) is a no-op.
    if (src.is_none()) {
        return;
    }

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value()) {
        return;
    }

    nb::object normalized_src = src;
    bool same_ref_identity = false;
    TimeSeriesReference existing_ref = TimeSeriesReference::make();
    bool existing_ref_valid = false;
    TimeSeriesReference incoming_ref = TimeSeriesReference::make();
    bool incoming_ref_valid = false;

    if (maybe_dst->valid() && maybe_dst->schema() == ts_reference_meta()) {
        existing_ref = nb::cast<TimeSeriesReference>(maybe_dst->to_python());
        existing_ref_valid = true;
        incoming_ref = nb::cast<TimeSeriesReference>(normalized_src);
        incoming_ref_valid = true;
        same_ref_identity = (existing_ref == incoming_ref);
    }

    const bool existing_payload_valid = existing_ref_valid && existing_ref.is_valid();
    const bool incoming_payload_valid = incoming_ref_valid && incoming_ref.is_valid();
    const bool has_prior_write = direct_last_modified_time(vd) != MIN_DT;
    const bool suppress_invalid_rebind_tick =
        vd.uses_link_target &&
        incoming_ref_valid &&
        existing_ref_valid &&
        !incoming_payload_valid &&
        !existing_payload_valid;

    if (same_ref_identity) {
        if (debug_ref_from) {
            std::fprintf(stderr,
                         "[ref_from_same] path=%s now=%lld existing_ref_valid=%d existing_payload_valid=%d incoming_ref_valid=%d incoming_payload_valid=%d has_prior_write=%d suppress=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         existing_ref_valid ? 1 : 0,
                         existing_payload_valid ? 1 : 0,
                         incoming_ref_valid ? 1 : 0,
                         incoming_payload_valid ? 1 : 0,
                         has_prior_write ? 1 : 0,
                         suppress_invalid_rebind_tick ? 1 : 0);
        }
        if (suppress_invalid_rebind_tick) {
            return;
        }
        const TSMeta* element_meta = current->element_ts();
        const bool dynamic_ref_container =
            element_meta != nullptr &&
            (dispatch_meta_is_tss(element_meta) || dispatch_meta_is_tsd(element_meta));
        if (dynamic_ref_container) {
            bool bound_target_modified = false;
            if (incoming_ref_valid && incoming_payload_valid) {
                if (const ViewData* bound = incoming_ref.bound_view(); bound != nullptr) {
                    ViewData bound_view = *bound;
                    bound_view.sampled = bound_view.sampled || vd.sampled;
                    if (bound_view.ops != nullptr && bound_view.ops->modified != nullptr) {
                        bound_target_modified = bound_view.ops->modified(bound_view, current_time);
                    }
                }
            }
            if (!bound_target_modified) {
                return;
            }
        }
        stamp_time_paths(vd, current_time);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    if (debug_ref_from) {
        std::string in_repr{"<repr_error>"};
        try {
            in_repr = nb::cast<std::string>(nb::repr(normalized_src));
        } catch (...) {}
        std::fprintf(stderr,
                     "[ref_from] path=%s now=%lld before_valid=%d same=%d existing_payload_valid=%d incoming_payload_valid=%d suppress=%d in=%s\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     maybe_dst->valid() ? 1 : 0,
                     same_ref_identity ? 1 : 0,
                     existing_payload_valid ? 1 : 0,
                     incoming_payload_valid ? 1 : 0,
                     suppress_invalid_rebind_tick ? 1 : 0,
                     in_repr.c_str());
    }

    maybe_dst->from_python(normalized_src);

    TimeSeriesReference written_ref = TimeSeriesReference::make();
    bool written_ref_valid = false;
    if (maybe_dst->valid() && maybe_dst->schema() == ts_reference_meta()) {
        written_ref = incoming_ref_valid ? incoming_ref : nb::cast<TimeSeriesReference>(maybe_dst->to_python());
        written_ref_valid = true;
    }

    if (auto* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
        unregister_ref_link_observer(*ref_link);

        bool has_bound_target = false;
        if (written_ref_valid) {
            if (const ViewData* bound = written_ref.bound_view(); bound != nullptr) {
                store_to_ref_link(*ref_link, *bound);
                has_bound_target = true;
            }
        }

        if (!has_bound_target) {
            ref_link->unbind();
        }

        if (current_time != MIN_DT && !suppress_invalid_rebind_tick) {
            ref_link->last_rebind_time = current_time;
        }

        if (has_bound_target) {
            register_ref_link_observer(*ref_link, &vd);
        }
    }

    if (debug_ref_from && maybe_dst->valid()) {
        std::string out_repr{"<repr_error>"};
        try {
            out_repr = nb::cast<std::string>(nb::repr(maybe_dst->to_python()));
        } catch (...) {}
        std::fprintf(stderr,
                     "[ref_from] path=%s now=%lld after=%s\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     out_repr.c_str());
    }

    if (suppress_invalid_rebind_tick) {
        // Keep REF wrapper invalid-time semantics (no modified/lmt tick)
        // but still notify active linked inputs that the wrapper payload
        // changed shape on first invalid materialization.
        if (current_time != MIN_DT && !has_prior_write) {
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    // When writing REF[TSB]/REF[TSL] composites, only stamp child timestamps
    // for item-level reference identity changes. This preserves per-field delta
    // behavior when only a subset of item bindings re-point.
    if (current_time != MIN_DT && written_ref_valid && written_ref.is_unbound()) {
        const auto& new_items = written_ref.items();
        std::vector<size_t> changed_indices;
        bool can_compare_items =
            existing_ref_valid &&
            existing_ref.is_unbound() &&
            existing_ref.items().size() == new_items.size();

        for (size_t i = 0; i < new_items.size(); ++i) {
            bool changed_item = true;
            if (can_compare_items) {
                changed_item = !(existing_ref.items()[i] == new_items[i]);
            }
            if (debug_ref_from) {
                std::fprintf(stderr,
                             "[ref_from_items] path=%s now=%lld idx=%zu changed=%d can_compare=%d\n",
                             vd.path.to_string().c_str(),
                             static_cast<long long>(current_time.time_since_epoch().count()),
                             i,
                             changed_item ? 1 : 0,
                             can_compare_items ? 1 : 0);
            }
            if (!changed_item) {
                continue;
            }
            changed_indices.push_back(i);
            ViewData child = vd;
            child.path.indices.push_back(i);
            stamp_time_paths(child, current_time);
        }

        record_unbound_ref_item_changes(vd, changed_indices, current_time);
    }

    stamp_time_paths(vd, current_time);
    mark_tsd_parent_child_modified(vd, current_time);
    notify_link_target_observers(vd, current_time);
}

}  // namespace hgraph
