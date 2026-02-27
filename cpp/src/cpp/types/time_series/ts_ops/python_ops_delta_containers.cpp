#include "ts_ops_internal.h"

namespace hgraph {

namespace {

nb::object sampled_delta_or_value_for_child(const ViewData& child, engine_time_t current_time) {
    ViewData sampled_child = child;
    sampled_child.sampled = true;
    nb::object child_delta = op_delta_to_python(sampled_child, current_time);
    if (child_delta.is_none()) {
        View child_value = op_value(child);
        if (child_value.valid()) {
            child_delta = stored_delta_to_python_with_refs(child_value, current_time);
        }
    }
    return child_delta;
}

nb::object sampled_delta_or_python_for_child(const ViewData& child, engine_time_t current_time) {
    ViewData sampled_child = child;
    sampled_child.sampled = true;
    nb::object child_delta = op_delta_to_python(sampled_child, current_time);
    if (child_delta.is_none()) {
        child_delta = op_to_python(child);
    }
    return child_delta;
}

}  // namespace

nb::object op_delta_to_python_tsl(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return nb::none();
    }
    bind_view_data_ops(resolved);
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return nb::none();
    }

    ViewData current_bridge{};
    if (resolve_rebind_current_bridge_view(vd, self_meta, current_time, current_bridge)) {
        nb::dict delta_out;
        const size_t n = op_list_size(current_bridge);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = current_bridge;
            child.path.indices.push_back(i);
            if (!op_valid(child)) {
                continue;
            }
            View child_value = op_value(child);
            if (child_value.valid()) {
                delta_out[nb::int_(i)] = stored_delta_to_python_with_refs(child_value, current_time);
            }
        }
        return delta_out;
    }

    nb::dict delta_out;
    const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const bool wrapper_ticked =
        wrapper_time == current_time ||
        rebind_time == current_time;
    const bool debug_tsl_delta = std::getenv("HGRAPH_DEBUG_TSL_DELTA") != nullptr;
    if (debug_tsl_delta) {
        int has_bound = 0;
        int bound_kind = -1;
        ViewData bound_dbg{};
        if (resolve_bound_target_view_data(vd, bound_dbg)) {
            has_bound = 1;
            if (const TSMeta* bm = meta_at_path(bound_dbg.meta, bound_dbg.path.indices); bm != nullptr) {
                bound_kind = static_cast<int>(bm->kind);
            }
        }
        std::fprintf(stderr,
                     "[tsl_delta] path=%s now=%lld wrapper_ticked=%d wrapper_time=%lld rebind=%lld has_bound=%d bound_kind=%d\n",
                     vd.path.to_string().c_str(),
                     static_cast<long long>(current_time.time_since_epoch().count()),
                     wrapper_ticked ? 1 : 0,
                     static_cast<long long>(wrapper_time.time_since_epoch().count()),
                     static_cast<long long>(rebind_time.time_since_epoch().count()),
                     has_bound,
                     bound_kind);
    }

    bool sample_all = wrapper_ticked;
    if (sample_all) {
        const size_t n = op_list_size(*data);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (op_modified(child, current_time)) {
                sample_all = false;
                break;
            }
        }
    }

    if (sample_all) {
        const size_t n = op_list_size(*data);
        for (size_t i = 0; i < n; ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (debug_tsl_delta) {
                std::fprintf(stderr,
                             "[tsl_delta]  sampled_child path=%s valid=%d modified=%d\n",
                             child.path.to_string().c_str(),
                             op_valid(child) ? 1 : 0,
                             op_modified(child, current_time) ? 1 : 0);
            }
            if (!op_valid(child)) {
                continue;
            }

            nb::object child_delta = sampled_delta_or_value_for_child(child, current_time);
            if (!child_delta.is_none()) {
                delta_out[nb::int_(i)] = std::move(child_delta);
            }
        }
        return delta_out;
    }

    const size_t n = op_list_size(*data);
    for (size_t i = 0; i < n; ++i) {
        ViewData child = *data;
        child.path.indices.push_back(i);
        if (debug_tsl_delta) {
            std::fprintf(stderr,
                         "[tsl_delta]  child path=%s valid=%d modified=%d\n",
                         child.path.to_string().c_str(),
                         op_valid(child) ? 1 : 0,
                         op_modified(child, current_time) ? 1 : 0);
        }
        if (!op_modified(child, current_time) || !op_valid(child)) {
            continue;
        }
        nb::object child_delta = sampled_delta_or_value_for_child(child, current_time);
        if (!child_delta.is_none()) {
            delta_out[nb::int_(i)] = std::move(child_delta);
        }
    }
    return delta_out;
}

nb::object op_delta_to_python_tsb(const ViewData& vd, engine_time_t current_time) {
    refresh_dynamic_ref_binding(vd, current_time);
    const TSMeta* self_meta = meta_at_path(vd.meta, vd.path.indices);
    ViewData resolved{};
    if (!resolve_read_view_data(vd, resolved)) {
        return nb::none();
    }
    bind_view_data_ops(resolved);
    const ViewData* data = &resolved;

    const TSMeta* current = meta_at_path(data->meta, data->path.indices);
    if (current == nullptr) {
        return nb::none();
    }

    nb::dict delta_out;
    if (current->fields() == nullptr) {
        return delta_out;
    }

    ViewData current_bridge{};
    if (resolve_rebind_current_bridge_view(vd, self_meta, current_time, current_bridge)) {
        const TSMeta* bridge_meta = meta_at_path(current_bridge.meta, current_bridge.path.indices);
        if (bridge_meta != nullptr &&
            bridge_meta->fields() != nullptr &&
            dispatch_meta_is_tsb(bridge_meta)) {
            for_each_named_bundle_field(bridge_meta, [&](size_t i, const char* field_name) {
                ViewData child = current_bridge;
                child.path.indices.push_back(i);
                if (!op_valid(child)) {
                    return;
                }

                nb::object child_delta = sampled_delta_or_python_for_child(child, current_time);
                if (child_delta.is_none()) {
                    return;
                }
                delta_out[nb::str(field_name)] = std::move(child_delta);
            });
            // Non-scalar delta contract: containers return empty payloads, not None.
            return delta_out;
        }
    }

    const engine_time_t wrapper_time = ref_wrapper_last_modified_time_on_read_path(vd);
    const engine_time_t rebind_time = rebind_time_for_view(vd);
    const bool wrapper_ticked =
        wrapper_time == current_time ||
        rebind_time == current_time;
    bool suppress_wrapper_sampling = false;
    if (wrapper_ticked) {
        ViewData bound_target{};
        if (resolve_bound_target_view_data(vd, bound_target)) {
            const TSMeta* bound_meta = meta_at_path(bound_target.meta, bound_target.path.indices);
            suppress_wrapper_sampling = dispatch_meta_is_ref(bound_meta);
        }
    }
    bool sample_all = wrapper_ticked && !suppress_wrapper_sampling;
    std::vector<bool> ref_item_rebound;
    if (wrapper_ticked && suppress_wrapper_sampling && current != nullptr) {
        ViewData bound_target{};
        if (resolve_bound_target_view_data(vd, bound_target)) {
            const TSMeta* bound_meta = meta_at_path(bound_target.meta, bound_target.path.indices);
            if (dispatch_meta_is_ref(bound_meta)) {
                const TSMeta* element_meta = bound_meta->element_ts();
                if (dispatch_meta_is_tsb(element_meta)) {
                    ref_item_rebound.assign(current->field_count(), false);
                    const size_t n = std::min(current->field_count(), element_meta->field_count());
                    for (size_t i = 0; i < n; ++i) {
                        ViewData item = bound_target;
                        item.path.indices.push_back(i);
                        ViewData resolved_item{};
                        const bool has_resolved_item = resolve_bound_target_view_data(item, resolved_item);
                        ViewData previous_item{};
                        const bool has_previous_item = resolve_previous_bound_target_view_data(item, previous_item);
                        ViewData previous_resolved_item{};
                        const bool has_previous_resolved_item =
                            has_resolved_item && resolve_previous_bound_target_view_data(resolved_item, previous_resolved_item);
                        const bool resolved_item_modified =
                            has_resolved_item ? op_modified(resolved_item, current_time) : false;
                        const bool recorded_item_change = unbound_ref_item_changed_this_tick(item, i, current_time);
                        bool item_rebound = false;
                        if (has_resolved_item && has_previous_item) {
                            item_rebound = !is_same_view_data(resolved_item, previous_item);
                        } else if (has_resolved_item && has_previous_resolved_item) {
                            item_rebound = !is_same_view_data(resolved_item, previous_resolved_item);
                        }
                        ref_item_rebound[i] = item_rebound || resolved_item_modified || recorded_item_change;
                    }
                }
            }
        }
    }
    const auto child_rebound_this_tick = [wrapper_ticked, &ref_item_rebound](size_t index, const ViewData& child) {
        if (!wrapper_ticked) {
            return false;
        }
        if (index < ref_item_rebound.size() && ref_item_rebound[index]) {
            return true;
        }
        ViewData previous{};
        if (!resolve_previous_bound_target_view_data(child, previous)) {
            return false;
        }
        ViewData current_child{};
        if (!resolve_bound_target_view_data(child, current_child)) {
            return false;
        }
        return !is_same_view_data(previous, current_child);
    };
    if (!sample_all && wrapper_ticked && suppress_wrapper_sampling && current != nullptr) {
        // On wrapper ticks sourced through REF[TSB], keep Python parity by
        // carrying unmodified non-scalar siblings when only scalar fields
        // advanced (for example switch branch changes).
        bool scalar_child_changed = false;
        bool has_unmodified_non_scalar_sibling = false;
        for (size_t i = 0; i < current->field_count(); ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
            const bool scalar_like = dispatch_meta_is_scalar_like(child_meta);
            const bool child_changed = child_rebound_this_tick(i, child) || op_modified(child, current_time);
            if (child_changed) {
                if (scalar_like) {
                    scalar_child_changed = true;
                } else {
                    scalar_child_changed = false;
                    has_unmodified_non_scalar_sibling = false;
                    break;
                }
            } else if (!scalar_like && op_valid(child)) {
                has_unmodified_non_scalar_sibling = true;
            }
        }
        if (scalar_child_changed && has_unmodified_non_scalar_sibling) {
            sample_all = true;
        }
    }
    if (sample_all) {
        // Only synthesize full wrapper snapshots when wrapper ticks require
        // carrying unmodified non-scalar siblings (for example switch/rebind).
        // For regular scalar-only sibling updates, emit normal per-child deltas.
        bool modified_scalar_like = false;
        bool modified_non_scalar = false;
        bool has_unmodified_non_scalar_sibling = false;
        for (size_t i = 0; i < current->field_count(); ++i) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
            const bool scalar_like = dispatch_meta_is_scalar_like(child_meta);
            const bool child_rebound = child_rebound_this_tick(i, child);
            const bool child_advanced =
                op_last_modified_time(child) == current_time || child_rebound;
            if (child_advanced) {
                if (scalar_like) {
                    modified_scalar_like = true;
                } else {
                    modified_non_scalar = true;
                }
            } else if (!scalar_like && op_valid(child)) {
                has_unmodified_non_scalar_sibling = true;
            }
        }

        if (modified_non_scalar) {
            sample_all = false;
        } else if (modified_scalar_like && !has_unmodified_non_scalar_sibling) {
            sample_all = false;
        }
    }

    if (sample_all) {
        for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
            ViewData child = *data;
            child.path.indices.push_back(i);
            if (!op_valid(child)) {
                return;
            }

            nb::object child_delta = sampled_delta_or_value_for_child(child, current_time);
            if (!child_delta.is_none()) {
                delta_out[nb::str(field_name)] = std::move(child_delta);
            }
        });
        // Non-scalar delta contract: containers return empty payloads, not None.
        return delta_out;
    }

    for_each_named_bundle_field(current, [&](size_t i, const char* field_name) {
        ViewData child = *data;
        child.path.indices.push_back(i);
        const bool child_rebound = child_rebound_this_tick(i, child);
        if ((!op_modified(child, current_time) && !child_rebound) || !op_valid(child)) {
            return;
        }

        const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
        if (dispatch_meta_is_scalar_like(child_meta)) {
            nb::object child_delta_py = sampled_delta_or_value_for_child(child, current_time);
            if (!child_delta_py.is_none()) {
                delta_out[nb::str(field_name)] = std::move(child_delta_py);
            } else if (child_rebound) {
                View child_value = op_value(child);
                if (child_value.valid()) {
                    nb::object child_value_py = stored_delta_to_python_with_refs(child_value, current_time);
                    if (!child_value_py.is_none()) {
                        delta_out[nb::str(field_name)] = std::move(child_value_py);
                    }
                }
            }
            return;
        }

        nb::object child_delta = op_delta_to_python(child, current_time);
        if (child_delta.is_none() && child_rebound) {
            child_delta = sampled_delta_or_python_for_child(child, current_time);
        }
        if (!child_delta.is_none()) {
            delta_out[nb::str(field_name)] = std::move(child_delta);
        }
    });
    // Non-scalar delta contract: containers return empty payloads, not None.
    return delta_out;
}


}  // namespace hgraph
