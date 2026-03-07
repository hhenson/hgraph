#include "ts_ops_internal.h"

namespace hgraph {

namespace {

bool extract_ref(const value::View& view, TimeSeriesReference& out) {
    if (!view.valid() || view.schema() != ts_reference_meta()) {
        return false;
    }
    out = *static_cast<const TimeSeriesReference*>(view.data());
    return true;
}

bool ref_bound_target_modified_this_tick(const TimeSeriesReference& ref, const ViewData& owner_vd, engine_time_t current_time) {
    if (!ref.is_valid()) {
        return false;
    }

    const ViewData* bound = ref.bound_view();
    if (bound == nullptr) {
        return false;
    }

    ViewData bound_view = *bound;
    bound_view.sampled = bound_view.sampled || owner_vd.sampled;
    bind_view_data_ops(bound_view);

    const TSMeta* bound_meta = meta_at_path(bound_view.meta, bound_view.path.indices);
    if (dispatch_meta_is_scalar_like(bound_meta) && !dispatch_meta_is_ref(bound_meta)) {
        if (direct_last_modified_time(bound_view) != current_time) {
            return false;
        }

        ViewData previous_bound{};
        if (resolve_previous_bound_target_view_data(owner_vd, previous_bound) &&
            same_view_identity(previous_bound, bound_view)) {
            const bool current_valid = op_valid(bound_view);
            const bool previous_valid = op_valid(previous_bound);
            if (current_valid == previous_valid) {
                if (!current_valid) {
                    return false;
                }
                View current_value = op_value(bound_view);
                View previous_value = op_value(previous_bound);
                if (current_value.valid() &&
                    previous_value.valid() &&
                    current_value.schema() == previous_value.schema() &&
                    current_value.equals(previous_value)) {
                    return false;
                }
            }
        }

        return true;
    }
    if (bound_view.ops != nullptr && bound_view.ops->modified != nullptr) {
        return bound_view.ops->modified(bound_view, current_time);
    }
    return false;
}

}  // namespace

TimeSeriesReference resolve_ref_payload_from_view(const ViewData& src) {
    if (auto local = resolve_value_slot_const(src);
        local.has_value() &&
        local->valid() &&
        local->schema() == ts_reference_meta()) {
        const auto& local_ref = *static_cast<const TimeSeriesReference*>(local->data());
        if (local_ref.is_empty()) {
            ViewData bound_target{};
            const bool has_distinct_bound_target =
                resolve_bound_target_view_data(src, bound_target) &&
                !same_view_identity(bound_target, src);
            if (!has_distinct_bound_target) {
                return TimeSeriesReference::make();
            }
        }
    }

    TimeSeriesReference resolved = TimeSeriesReference::make();
    if (extract_ref(op_value(src), resolved)) {
        return resolved;
    }
    if (auto bound = resolve_bound_view_data(src); bound.has_value()) {
        return TimeSeriesReference::make(*bound);
    }
    return TimeSeriesReference::make();
}

void apply_ref_payload(ViewData& vd, const TimeSeriesReference& incoming_ref, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr) {
        return;
    }

    const bool debug_ref_from = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_FROM");

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return;
    }

    TimeSeriesReference existing_ref = *static_cast<const TimeSeriesReference*>(maybe_dst->data());
    const bool existing_ref_valid = true;
    const bool same_ref_identity = (existing_ref == incoming_ref);
    const bool existing_payload_valid = existing_ref.is_valid();
    const bool incoming_payload_valid = incoming_ref.is_valid();
    const bool has_prior_write = direct_last_modified_time(vd) != MIN_DT;
    const bool suppress_invalid_rebind_tick =
        vd.uses_link_target &&
        existing_ref_valid &&
        !incoming_payload_valid &&
        !existing_payload_valid;

    if (same_ref_identity) {
        if (debug_ref_from) {
            std::fprintf(stderr,
                         "[ref_from_same] path=%s now=%lld existing_payload_valid=%d incoming_payload_valid=%d has_prior_write=%d suppress=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<long long>(current_time.time_since_epoch().count()),
                         existing_payload_valid ? 1 : 0,
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
            if (!ref_bound_target_modified_this_tick(incoming_ref, vd, current_time)) {
                return;
            }
        }
        stamp_time_paths(vd, current_time);
        mark_tsd_parent_child_modified(vd, current_time);
        notify_link_target_observers(vd, current_time);
        return;
    }

    if (debug_ref_from) {
        std::fprintf(stderr,
                     "[ref_from] path=%s now=%lld before_valid=%d same=%d existing_payload_valid=%d incoming_payload_valid=%d suppress=%d\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     maybe_dst->valid() ? 1 : 0,
                     same_ref_identity ? 1 : 0,
                     existing_payload_valid ? 1 : 0,
                     incoming_payload_valid ? 1 : 0,
                     suppress_invalid_rebind_tick ? 1 : 0);
    }

    invalidate_python_value_cache(vd);
    *static_cast<TimeSeriesReference*>(maybe_dst->data()) = incoming_ref;

    if (auto* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
        unregister_ref_link_observer(*ref_link);

        bool has_bound_target = false;
        if (const ViewData* bound = incoming_ref.bound_view(); bound != nullptr) {
            store_to_ref_link(*ref_link, *bound);
            has_bound_target = true;
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

    if (suppress_invalid_rebind_tick) {
        if (current_time != MIN_DT && !has_prior_write) {
            notify_link_target_observers(vd, current_time);
        }
        return;
    }

    if (current_time != MIN_DT && incoming_ref.is_unbound()) {
        const auto& new_items = incoming_ref.items();
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

void op_from_python_ref(ViewData& vd, const nb::object& src, engine_time_t current_time) {
    // Python TimeSeriesReferenceOutput.apply_result(None) is a no-op.
    if (src.is_none()) {
        return;
    }

    apply_ref_payload(vd, nb::cast<TimeSeriesReference>(src), current_time);
}

}  // namespace hgraph
