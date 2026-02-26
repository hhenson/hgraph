#include "ts_ops_internal.h"

namespace hgraph {

bool is_same_view_data(const ViewData& lhs, const ViewData& rhs) {
    return lhs.value_data == rhs.value_data &&
           lhs.time_data == rhs.time_data &&
           lhs.observer_data == rhs.observer_data &&
           lhs.delta_data == rhs.delta_data &&
           lhs.link_data == rhs.link_data &&
           lhs.projection == rhs.projection &&
           lhs.path.indices == rhs.path.indices;
}

// Resolve the view used for reads. For non-REF consumers, this transparently
// dereferences REF bindings so TS adapters observe concrete target values.
bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out) {
    out = vd;
    out.sampled = out.sampled || vd.sampled;

    for (size_t depth = 0; depth < 64; ++depth) {
        if (auto rebound = resolve_bound_view_data(out); rebound.has_value()) {
            const ViewData next = std::move(rebound.value());
            if (is_same_view_data(next, out)) {
                return false;
            }
            out = next;
            out.sampled = out.sampled || vd.sampled;

            // REF views expose the reference object itself. For REF consumers we
            // stop after resolving the direct bind chain.
            if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
                return true;
            }
            continue;
        }

        if (self_meta != nullptr && self_meta->kind == TSKind::REF) {
            return true;
        }

        const TSMeta* current = meta_at_path(out.meta, out.path.indices);
        if (current == nullptr || current->kind != TSKind::REF) {
            if (auto through_ref_ancestor = resolve_ref_ancestor_descendant_view_data(out);
                through_ref_ancestor.has_value()) {
                const ViewData next = std::move(*through_ref_ancestor);
                if (is_same_view_data(next, out)) {
                    return false;
                }
                out = next;
                out.sampled = out.sampled || vd.sampled;
                continue;
            }
            return true;
        }

        if (auto ref_value = resolve_value_slot_const(out);
            ref_value.has_value() && ref_value->valid() && ref_value->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_value->to_python());
            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                if (std::getenv("HGRAPH_DEBUG_REF_ANCESTOR") != nullptr) {
                    std::string target_repr{"<none>"};
                    if (target->ops != nullptr && target->ops->to_python != nullptr) {
                        try {
                            target_repr = nb::cast<std::string>(nb::repr(target->ops->to_python(*target)));
                        } catch (...) {
                            target_repr = "<repr_error>";
                        }
                    }
                    std::fprintf(stderr,
                                 "[resolve_read_ref] path=%s ref_value_data=%p target_value_data=%p target_path=%s target=%s\n",
                                 out.path.to_string().c_str(),
                                 ref_value->data(),
                                 target->value_data,
                                 target->path.to_string().c_str(),
                                 target_repr.c_str());
                }
                out = *target;
                out.sampled = out.sampled || vd.sampled;
                out.projection = merge_projection(vd.projection, out.projection);
                continue;
            }

            // Empty local REF values are placeholders used by REF->REF bind.
            // Fall through to binding resolution if a link exists.
            if (!ref.is_empty()) {
                const bool self_static_container =
                    self_meta != nullptr &&
                    (self_meta->kind == TSKind::TSB ||
                     (self_meta->kind == TSKind::TSL && self_meta->fixed_size() > 0));
                if (self_static_container) {
                    // Static container consumers can be driven by unbound REF
                    // payloads (for example switch-style REF[TSB] wrappers).
                    // Keep reads on the local container view so child paths can
                    // resolve through REF descendants.
                    out = vd;
                    out.sampled = out.sampled || vd.sampled;
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    return false;
}

void stamp_time_paths(ViewData& vd, engine_time_t current_time) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr) {
        return;
    }

    if (!time_root->has_value()) {
        time_root->emplace();
    }

    auto stamp_one_time_path = [&](const std::vector<size_t>& path) {
        std::optional<ValueView> slot;
        if (path.empty()) {
            slot = time_root->view();
        } else {
            slot = navigate_mut(time_root->view(), path);
        }
        if (!slot.has_value()) {
            return;
        }
        if (engine_time_t* et_ptr = extract_time_ptr(*slot); et_ptr != nullptr) {
            *et_ptr = current_time;
        }
    };

    for (const auto& path : time_stamp_paths_for_ts_path(vd.meta, vd.path.indices)) {
        stamp_one_time_path(path);
    }

    // For static containers (TSB, fixed-size TSL), writing a parent value should also
    // advance child timestamps so child.modified mirrors Python semantics.
    const TSMeta* target_meta = meta_at_path(vd.meta, vd.path.indices);
    if (target_meta != nullptr && (target_meta->kind == TSKind::TSB ||
                                   (target_meta->kind == TSKind::TSL && target_meta->fixed_size() > 0))) {
        // TSD child writes can be partial (for example only one bundle field present in
        // a dict delta). In that case, stamping all descendants would incorrectly mark
        // absent fields as valid/modified.
        bool parent_is_tsd = false;
        if (!vd.path.indices.empty()) {
            std::vector<size_t> parent_path(vd.path.indices.begin(), vd.path.indices.end() - 1);
            const TSMeta* parent_meta = meta_at_path(vd.meta, parent_path);
            parent_is_tsd = parent_meta != nullptr && parent_meta->kind == TSKind::TSD;
        }
        if (parent_is_tsd) {
            return;
        }

        std::vector<std::vector<size_t>> descendant_ts_paths;
        std::vector<size_t>              current_ts_path = vd.path.indices;
        collect_static_descendant_ts_paths(target_meta, current_ts_path, descendant_ts_paths);
        for (const auto& descendant_ts_path : descendant_ts_paths) {
            for (const auto& time_path : time_stamp_paths_for_ts_path(vd.meta, descendant_ts_path)) {
                stamp_one_time_path(time_path);
            }
        }
    }
}

void set_leaf_time_path(ViewData& vd, engine_time_t time_value) {
    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr || !time_root->has_value()) {
        return;
    }

    const auto time_path = ts_path_to_time_path(vd.meta, vd.path.indices);
    std::optional<ValueView> slot;
    if (time_path.empty()) {
        slot = time_root->view();
    } else {
        slot = navigate_mut(time_root->view(), time_path);
    }
    if (!slot.has_value()) {
        return;
    }
    if (engine_time_t* et_ptr = extract_time_ptr(*slot); et_ptr != nullptr) {
        *et_ptr = time_value;
    }
}

std::optional<ValueView> resolve_value_slot_mut(ViewData& vd) {
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr || value_root->schema() == nullptr) {
        return std::nullopt;
    }
    if (!value_root->has_value()) {
        value_root->emplace();
    }
    if (vd.path.indices.empty()) {
        return value_root->view();
    }
    return navigate_mut(value_root->view(), vd.path.indices);
}

std::optional<View> resolve_value_slot_const(const ViewData& vd) {
    auto* value_root = static_cast<const Value*>(vd.value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return std::nullopt;
    }
    if (vd.path.indices.empty()) {
        return value_root->view();
    }
    return navigate_const(value_root->view(), vd.path.indices);
}

bool has_local_ref_wrapper_value(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::REF) {
        return false;
    }

    auto local = resolve_value_slot_const(vd);
    if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
    return !ref.is_empty();
}

bool has_bound_ref_static_children(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::REF) {
        return false;
    }

    const TSMeta* element = current->element_ts();
    if (element == nullptr) {
        return false;
    }

    if (element->kind == TSKind::TSB && element->fields() != nullptr) {
        for (size_t i = 0; i < element->field_count(); ++i) {
            std::vector<size_t> child_path = vd.path.indices;
            child_path.push_back(i);
            if (LinkTarget* child = resolve_link_target(vd, child_path); child != nullptr && child->is_linked) {
                return true;
            }
        }
    } else if (element->kind == TSKind::TSL && element->fixed_size() > 0) {
        for (size_t i = 0; i < element->fixed_size(); ++i) {
            std::vector<size_t> child_path = vd.path.indices;
            child_path.push_back(i);
            if (LinkTarget* child = resolve_link_target(vd, child_path); child != nullptr && child->is_linked) {
                return true;
            }
        }
    }

    return false;
}

bool assign_ref_value_from_bound_static_children(ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || current->kind != TSKind::REF) {
        return false;
    }

    const TSMeta* element = current->element_ts();
    if (element == nullptr) {
        return false;
    }

    size_t child_count = 0;
    if (element->kind == TSKind::TSB && element->fields() != nullptr) {
        child_count = element->field_count();
    } else if (element->kind == TSKind::TSL && element->fixed_size() > 0) {
        child_count = element->fixed_size();
    } else {
        return false;
    }

    std::vector<TimeSeriesReference> child_refs;
    child_refs.reserve(child_count);
    bool has_non_empty_child = false;

    for (size_t i = 0; i < child_count; ++i) {
        ViewData child = vd;
        child.path.indices.push_back(i);

        TimeSeriesReference child_ref = TimeSeriesReference::make();
        if (auto bound = resolve_bound_view_data(child); bound.has_value()) {
            const TSMeta* bound_meta = meta_at_path(bound->meta, bound->path.indices);
            if (bound_meta != nullptr && bound_meta->kind == TSKind::REF) {
                bool resolved_from_local = false;
                if (auto local_ref_value = resolve_value_slot_const(*bound);
                    local_ref_value.has_value() &&
                    local_ref_value->valid() &&
                    local_ref_value->schema() == ts_reference_meta()) {
                    child_ref = nb::cast<TimeSeriesReference>(local_ref_value->to_python());
                    resolved_from_local = true;
                }

                View bound_value = op_value(*bound);
                if (!resolved_from_local &&
                    bound_value.valid() &&
                    bound_value.schema() == ts_reference_meta()) {
                    child_ref = nb::cast<TimeSeriesReference>(bound_value.to_python());
                } else if (!resolved_from_local && op_valid(*bound)) {
                    child_ref = TimeSeriesReference::make(*bound);
                }
            } else {
                child_ref = TimeSeriesReference::make(*bound);
            }
        } else {
            const TSMeta* child_meta = meta_at_path(child.meta, child.path.indices);
            bool resolved_from_local = false;
            if (child_meta != nullptr && child_meta->kind == TSKind::REF) {
                if (auto local_ref_value = resolve_value_slot_const(child);
                    local_ref_value.has_value() &&
                    local_ref_value->valid() &&
                    local_ref_value->schema() == ts_reference_meta()) {
                    child_ref = nb::cast<TimeSeriesReference>(local_ref_value->to_python());
                    resolved_from_local = true;
                }
            }

            View child_value = op_value(child);
            if (!resolved_from_local && child_value.valid()) {
                if (child_value.schema() == ts_reference_meta()) {
                    child_ref = nb::cast<TimeSeriesReference>(child_value.to_python());
                } else {
                    child_ref = TimeSeriesReference::make(child);
                }
            } else if (!resolved_from_local && op_valid(child)) {
                child_ref = TimeSeriesReference::make(child);
            }
        }

        has_non_empty_child = has_non_empty_child || !child_ref.is_empty();
        child_refs.push_back(std::move(child_ref));
    }

    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return false;
    }

    if (has_non_empty_child) {
        maybe_dst->from_python(nb::cast(TimeSeriesReference::make(std::move(child_refs))));
    } else {
        maybe_dst->from_python(nb::cast(TimeSeriesReference::make()));
    }
    return true;
}

bool assign_ref_value_from_target(ViewData& vd, const ViewData& target) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return false;
    }
    maybe_dst->from_python(nb::cast(TimeSeriesReference::make(target)));
    return true;
}

void clear_ref_value(ViewData& vd) {
    if (std::getenv("HGRAPH_DEBUG_REF_BIND_PATH") != nullptr) {
        std::fprintf(stderr,
                     "[clear_ref_value] path=%s root_reset=%d\n",
                     vd.path.to_string().c_str(),
                     vd.path.indices.empty() ? 1 : 0);
    }
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }
    if (vd.path.indices.empty()) {
        value_root->reset();
        set_leaf_time_path(vd, MIN_DT);
        return;
    }
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return;
    }
    maybe_dst->from_python(nb::cast(TimeSeriesReference::make()));
    set_leaf_time_path(vd, MIN_DT);
}

void clear_ref_container_ancestor_cache(ViewData& vd) {
    if (vd.path.indices.empty()) {
        return;
    }

    for (size_t depth = 0; depth < vd.path.indices.size(); ++depth) {
        std::vector<size_t> ancestor_path(vd.path.indices.begin(),
                                          vd.path.indices.begin() + static_cast<std::ptrdiff_t>(depth));
        const TSMeta* ancestor_meta = meta_at_path(vd.meta, ancestor_path);
        if (ancestor_meta == nullptr || ancestor_meta->kind != TSKind::REF) {
            continue;
        }

        const TSMeta* element_meta = ancestor_meta->element_ts();
        const bool static_ref_container =
            element_meta != nullptr &&
            (element_meta->kind == TSKind::TSB ||
             (element_meta->kind == TSKind::TSL && element_meta->fixed_size() > 0));
        if (!static_ref_container) {
            break;
        }

        ViewData ancestor = vd;
        ancestor.path.indices = std::move(ancestor_path);
        clear_ref_value(ancestor);
        break;
    }
}



}  // namespace hgraph

