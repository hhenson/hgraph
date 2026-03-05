#include "ts_ops_internal.h"

namespace hgraph {

LinkTarget* resolve_parent_link_target(const ViewData& vd) {
    if (vd.path.indices.empty()) {
        return nullptr;
    }

    LinkTarget* self = resolve_link_target(vd, vd.path.indices);
    std::vector<size_t> parent_path = vd.path.indices;
    parent_path.pop_back();
    LinkTarget* parent = resolve_link_target(vd, parent_path);
    if (parent == nullptr || self == nullptr) {
        return parent;
    }

    if (parent == self) {
        return nullptr;
    }

    // Parent links must form a strict ancestor chain. Reject cyclic candidates.
    std::unordered_set<const LinkTarget*> visited;
    for (const LinkTarget* cursor = parent; cursor != nullptr; cursor = cursor->parent_link) {
        if (cursor == self) {
            return nullptr;
        }
        if (!visited.insert(cursor).second) {
            return nullptr;
        }
    }

    return parent;
}

const TSMeta* resolve_meta_or_ancestor(const ViewData& vd, bool& used_ancestor) {
    used_ancestor = false;
    if (const TSMeta* current = meta_at_path(vd.meta, vd.path.indices); current != nullptr) {
        return current;
    }

    std::vector<size_t> ancestor = vd.path.indices;
    while (!ancestor.empty()) {
        ancestor.pop_back();
        if (const TSMeta* current = meta_at_path(vd.meta, ancestor); current != nullptr) {
            used_ancestor = true;
            return current;
        }
    }

    return nullptr;
}

std::vector<size_t> link_residual_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> residual;
    const TSMeta* current = root_meta;
    bool collecting_residual = false;

    for (size_t index : ts_path) {
        while (!collecting_residual && dispatch_meta_is_ref(current)) {
            current = current->element_ts();
        }

        if (collecting_residual || current == nullptr) {
            residual.push_back(index);
            continue;
        }

        switch (dispatch_meta_path_kind(current)) {
            case DispatchMetaPathKind::TSB:
                if (current->fields() == nullptr || index >= current->field_count()) {
                    collecting_residual = true;
                    residual.push_back(index);
                    current = nullptr;
                } else {
                    current = current->fields()[index].ts_type;
                }
                continue;
            case DispatchMetaPathKind::TSLFixed:
                current = current->element_ts();
                continue;
            case DispatchMetaPathKind::TSLDynamic:
            case DispatchMetaPathKind::TSD:
                collecting_residual = true;
                residual.push_back(index);
                current = current->element_ts();
                continue;
            default:
                collecting_residual = true;
                residual.push_back(index);
                continue;
        }
    }

    return residual;
}

engine_time_t rebind_time_for_view(const ViewData& vd) {
    if (vd.uses_link_target) {
        engine_time_t out = MIN_DT;
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices); target != nullptr) {
            // First empty->resolved binds must surface as modified time so
            // non-REF consumers (for example lag over switch stubs) sample the
            // newly bound value on the bind tick.
            if (target->has_previous_target || target->has_resolved_target) {
                out = std::max(out, target->last_rebind_time);
            }
        }

        auto is_static_container_parent = [](const TSMeta* meta) {
            if (dispatch_meta_is_ref(meta)) {
                return dispatch_meta_is_static_container(meta != nullptr ? meta->element_ts() : nullptr);
            }
            switch (dispatch_meta_path_kind(meta)) {
                case DispatchMetaPathKind::TSB:
                case DispatchMetaPathKind::TSLFixed:
                    return true;
                default:
                    return false;
            }
        };

        // Descendant views only inherit ancestor rebind markers for static
        // container ancestry (TSB / fixed-size TSL). Propagating dynamic
        // container rebinds (for example TSD key-set churn) over-reports
        // unchanged children as modified.
        if (!vd.path.indices.empty()) {
            for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                std::vector<size_t> parent_path(
                    vd.path.indices.begin(),
                    vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
                const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
                if (!is_static_container_parent(parent_meta)) {
                    continue;
                }
                if (LinkTarget* parent = resolve_link_target(vd, parent_path);
                    parent != nullptr && parent->is_linked && parent->has_previous_target) {
                    out = std::max(out, parent->last_rebind_time);
                }
            }
        }
        return out;
    }

    engine_time_t out = MIN_DT;
    if (REFLink* ref_link = resolve_ref_link(vd, vd.path.indices); ref_link != nullptr) {
        out = std::max(out, ref_link->last_rebind_time);
    }
    if (!vd.path.indices.empty()) {
        for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
            std::vector<size_t> parent_path(
                vd.path.indices.begin(),
                vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
            if (REFLink* parent = resolve_ref_link(vd, parent_path); parent != nullptr) {
                out = std::max(out, parent->last_rebind_time);
            }
        }
    }
    return out;
}

static bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
    return lhs.value_data == rhs.value_data &&
           lhs.time_data == rhs.time_data &&
           lhs.observer_data == rhs.observer_data &&
           lhs.delta_data == rhs.delta_data &&
           lhs.link_data == rhs.link_data &&
           lhs.python_value_cache_data == rhs.python_value_cache_data &&
           lhs.link_observer_registry == rhs.link_observer_registry &&
           lhs.projection == rhs.projection &&
           lhs.path.indices == rhs.path.indices;
}

bool same_or_descendant_view(const ViewData& base, const ViewData& candidate) {
    return base.value_data == candidate.value_data &&
           base.time_data == candidate.time_data &&
           base.observer_data == candidate.observer_data &&
           base.delta_data == candidate.delta_data &&
           base.link_data == candidate.link_data &&
           base.python_value_cache_data == candidate.python_value_cache_data &&
           base.link_observer_registry == candidate.link_observer_registry &&
           base.projection == candidate.projection &&
           is_prefix_path(base.path.indices, candidate.path.indices);
}

bool ref_child_payload_valid(const ViewData& ref_child_vd) {
    if (!op_valid(ref_child_vd)) {
        return false;
    }
    View ref_value = op_value(ref_child_vd);
    TimeSeriesReference ref = TimeSeriesReference::make();
    if (!extract_time_series_reference(ref_value, ref)) {
        return false;
    }
    return ref.is_valid();
}

bool container_child_valid_for_aggregation(const ViewData& child_vd) {
    // Python all_valid semantics treat REF children as valid based on wrapper
    // validity, not referent payload validity.
    return op_valid(child_vd);
}

bool ref_child_rebound_this_tick(const ViewData& ref_child) {
    ViewData previous{};
    if (!resolve_previous_bound_target_view_data(ref_child, previous)) {
        return false;
    }

    ViewData current{};
    if (!resolve_bound_target_view_data(ref_child, current)) {
        return false;
    }

    return !same_view_identity(previous, current);
}

engine_time_t direct_last_modified_time(const ViewData& vd) {
    auto* time_root = static_cast<const Value*>(vd.time_data);
    if (time_root == nullptr || !time_root->has_value()) {
        return MIN_DT;
    }

    const auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    std::optional<View> maybe_time;
    if (time_path.empty()) {
        maybe_time = time_root->view();
    } else {
        maybe_time = navigate_const(time_root->view(), time_path);
    }
    if (!maybe_time.has_value()) {
        return MIN_DT;
    }
    return extract_time_value(*maybe_time);
}

engine_time_t ref_wrapper_last_modified_time_on_read_path(const ViewData& vd) {
    // Rebind on REF wrappers must be visible to both REF and non-REF consumers
    // that resolve through those wrappers (e.g. DEFAULT[TIME_SERIES_TYPE] sinks).
    const bool include_wrapper_time = true;

    engine_time_t out = MIN_DT;
    ViewData probe = vd;
    probe.sampled = probe.sampled || vd.sampled;

    for (size_t depth = 0; depth < 64; ++depth) {
        const TSMeta* current = meta_at_path(probe.meta, probe.path.indices);
        const bool current_is_ref = dispatch_meta_is_ref(current);

        auto rebound = resolve_bound_view_data(probe);

        if (current_is_ref && include_wrapper_time) {
            const bool has_rebound_target =
                rebound.has_value() && !same_view_identity(*rebound, probe);
            const TSMeta* element_meta = current != nullptr ? current->element_ts() : nullptr;
            const bool static_ref_container = dispatch_meta_is_static_container(element_meta);

            // Non-REF consumers should observe REF wrapper *rebinds*, but not
            // wrapper-local rewrites that preserve target identity.
            if (has_rebound_target) {
                engine_time_t rebind_time = rebind_time_for_view(probe);
                if (probe.uses_link_target) {
                    if (LinkTarget* link_target = resolve_link_target(probe, probe.path.indices);
                        link_target != nullptr &&
                        link_target->is_linked &&
                        !link_target->has_previous_target) {
                        rebind_time = std::max(rebind_time, link_target->last_rebind_time);
                    }
                }
                out = std::max(out, rebind_time);
            } else if (static_ref_container) {
                // Static REF containers (for example REF[TSB]) can update local
                // reference payload without exposing a single bound_view target.
                // Surface wrapper-local time so consumers can sample unchanged
                // siblings on switch-style graph resets.
                out = std::max(out, direct_last_modified_time(probe));
            }
        }

        if (rebound.has_value()) {
            if (same_view_identity(*rebound, probe)) {
                return out;
            }
            probe = *rebound;
            probe.sampled = probe.sampled || vd.sampled;
            continue;
        }

        if (!current_is_ref) {
            return out;
        }

        auto local = resolve_value_slot_const(probe);
        if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
            return out;
        }

        TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(local->data());
        const ViewData* bound = ref.bound_view();
        if (bound == nullptr) {
            return out;
        }

        probe = *bound;
        probe.sampled = probe.sampled || vd.sampled;
    }

    return out;
}

bool resolve_ref_bound_target_view_data(const ViewData& ref_view, ViewData& out) {
    const bool debug_ref_ancestor = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_ANCESTOR");
    const TSMeta* ref_meta = meta_at_path(ref_view.meta, ref_view.path.indices);
    if (!dispatch_meta_is_ref(ref_meta)) {
        return false;
    }

    auto ref_value = resolve_value_slot_const(ref_view);
    if (!ref_value.has_value() || !ref_value->valid() || ref_value->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(ref_value->data());
    const ViewData* bound = ref.bound_view();
    if (bound == nullptr) {
        return false;
    }

    out = *bound;
    out.sampled = out.sampled || ref_view.sampled;
    out.projection = merge_projection(ref_view.projection, out.projection);
    if (debug_ref_ancestor) {
        std::string bound_repr{"<none>"};
        if (bound->ops != nullptr && bound->ops->to_python != nullptr) {
            try {
                bound_repr = nb::cast<std::string>(nb::repr(op_to_python(*bound)));
            } catch (...) {
                bound_repr = "<repr_error>";
            }
        }
        std::fprintf(stderr,
                     "[ref_bound] ref_path=%s ref_value_data=%p bound_value_data=%p bound_path=%s bound=%s\n",
                     ref_view.path.to_string().c_str(),
                     ref_value->data(),
                     bound->value_data,
                     bound->path.to_string().c_str(),
                     bound_repr.c_str());
    }
    return true;
}

bool resolve_unbound_ref_item_view_data(const TimeSeriesReference& ref,
                                        const std::vector<size_t>& residual_path,
                                        size_t residual_offset,
                                        ViewData& out) {
    if (!ref.is_unbound()) {
        return false;
    }
    if (residual_offset >= residual_path.size()) {
        return false;
    }

    const auto& items = ref.items();
    const size_t index = residual_path[residual_offset];
    if (index >= items.size()) {
        return false;
    }

    const TimeSeriesReference& item_ref = items[index];
    if (const ViewData* bound = item_ref.bound_view(); bound != nullptr) {
        out = *bound;
        out.path.indices.insert(
            out.path.indices.end(),
            residual_path.begin() + static_cast<std::ptrdiff_t>(residual_offset + 1),
            residual_path.end());
        return true;
    }

    return resolve_unbound_ref_item_view_data(item_ref, residual_path, residual_offset + 1, out);
}

bool split_path_at_first_ref_ancestor(const TSMeta* root_meta,
                                      const std::vector<size_t>& ts_path,
                                      size_t& ref_depth_out) {
    if (root_meta == nullptr) {
        return false;
    }
    if (ts_path.empty()) {
        if (dispatch_meta_is_ref(root_meta)) {
            ref_depth_out = 0;
            return true;
        }
        return false;
    }

    const TSMeta* current = root_meta;
    for (size_t depth = 0; depth < ts_path.size(); ++depth) {
        if (dispatch_meta_is_ref(current)) {
            ref_depth_out = depth;
            return true;
        }

        if (current == nullptr) {
            return false;
        }

        const size_t index = ts_path[depth];
        if (!dispatch_meta_step_child_no_ref(current, index)) {
            return false;
        }
    }

    // If the traversed leaf itself is REF (for example TSD slot values in
    // TSD[K, REF[...]]), treat that leaf depth as the first REF ancestor.
    if (dispatch_meta_is_ref(current)) {
        ref_depth_out = ts_path.size();
        return true;
    }

    return false;
}

std::optional<ViewData> resolve_ref_ancestor_descendant_view_data(const ViewData& vd) {
    const bool debug_ref_ancestor = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_ANCESTOR");
    size_t ref_depth = 0;
    if (!split_path_at_first_ref_ancestor(vd.meta, vd.path.indices, ref_depth)) {
        return std::nullopt;
    }

    ViewData ref_view = vd;
    ref_view.path.indices.assign(vd.path.indices.begin(), vd.path.indices.begin() + static_cast<std::ptrdiff_t>(ref_depth));
    std::vector<size_t> residual_path(
        vd.path.indices.begin() + static_cast<std::ptrdiff_t>(ref_depth),
        vd.path.indices.end());

    ViewData resolved_ref{};
    if (!resolve_ref_bound_target_view_data(ref_view, resolved_ref)) {
        auto local = resolve_value_slot_const(ref_view);
        if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
            return std::nullopt;
        }

        TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(local->data());
        if (!resolve_unbound_ref_item_view_data(ref, residual_path, 0, resolved_ref)) {
            return std::nullopt;
        }
    } else {
        resolved_ref.path.indices.insert(
            resolved_ref.path.indices.end(),
            residual_path.begin(),
            residual_path.end());
    }

    resolved_ref.sampled = resolved_ref.sampled || vd.sampled;
    resolved_ref.projection = merge_projection(vd.projection, resolved_ref.projection);
    if (debug_ref_ancestor) {
        std::fprintf(stderr,
                     "[ref_ancestor] in_path=%s ref_path=%s out_path=%s out_value_data=%p\n",
                     vd.path.to_string().c_str(),
                     ref_view.path.to_string().c_str(),
                     resolved_ref.path.to_string().c_str(),
                     resolved_ref.value_data);
    }
    return resolved_ref;
}

void refresh_dynamic_ref_binding(const ViewData& vd, engine_time_t current_time) {
    if (!vd.uses_link_target) {
        return;
    }

    LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
    if (link_target == nullptr || !link_target->is_linked) {
        return;
    }
    refresh_dynamic_ref_binding_for_link_target(link_target, vd.sampled, current_time);
}

bool resolve_rebind_bridge_views(const ViewData& vd,
                                 const TSMeta* self_meta,
                                 engine_time_t current_time,
                                 ViewData& previous_resolved,
                                 ViewData& current_resolved) {
    if (!vd.uses_link_target) {
        return false;
    }

    LinkTarget* link_target = resolve_link_target(vd, vd.path.indices);
    if (link_target == nullptr ||
        !link_target->has_previous_target ||
        link_target->last_rebind_time != current_time ||
        !link_target->is_linked) {
        return false;
    }

    ViewData previous_view = link_target->previous_view_data(vd.sampled);
    ViewData current_view = link_target->has_resolved_target ? link_target->resolved_target : link_target->as_view_data(vd.sampled);
    current_view.sampled = current_view.sampled || vd.sampled;

    const auto resolve_or_empty_ref = [&](const ViewData& bridge_view, ViewData& out) {
        if (resolve_read_view_data(bridge_view, self_meta, out)) {
            return true;
        }

        // Bridge transitions to/from empty REF wrappers should still be surfaced
        // to container adapters so they can emit removal/addition deltas.
        const TSMeta* bridge_meta = meta_at_path(bridge_view.meta, bridge_view.path.indices);
        if (!dispatch_meta_is_ref(bridge_meta)) {
            return false;
        }

        auto local = resolve_value_slot_const(bridge_view);
        if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
            return false;
        }

        TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(local->data());
        if (!ref.is_empty()) {
            return false;
        }

        out = bridge_view;
        out.sampled = out.sampled || vd.sampled;
        return true;
    };

    if (!resolve_or_empty_ref(previous_view, previous_resolved)) {
        return false;
    }
    if (!resolve_or_empty_ref(current_view, current_resolved)) {
        return false;
    }
    return true;
}




}  // namespace hgraph
