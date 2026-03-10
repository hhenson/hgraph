#include "ts_ops_internal.h"

namespace hgraph {

bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
    return lhs.same_identity(rhs);
}

bool is_same_view_data(const ViewData& lhs, const ViewData& rhs) {
    return lhs.same_identity(rhs);
}

// Navigate the level pointer from root to match the ViewData's path depth.
// After link resolution, level points to the target TSValue's root but the
// path may be deeper (e.g., TSL child at index 1). This walks the level tree
// so that level points to the correct child entry.
void navigate_level_to_path(ViewData& vd) {
    if (vd.level == nullptr) {
        return;
    }
    const auto indices = vd.path_indices();
    // Only navigate indices beyond current level depth
    for (size_t i = vd.level_depth; i < indices.size(); ++i) {
        vd.level = vd.level->ensure_child(indices[i]);
    }
    vd.level_depth = static_cast<uint16_t>(indices.size());
}

// Resolve the view used for reads. For non-REF consumers, this transparently
// dereferences REF bindings so TS adapters observe concrete target values.
bool resolve_read_view_data(const ViewData& vd, ViewData& out) {
    return resolve_read_view_data(vd, vd.meta, out);
}

bool resolve_read_view_data(const ViewData& vd, const TSMeta* self_meta, ViewData& out) {
    const bool self_is_ref = dispatch_meta_is_ref(self_meta);
    const bool self_static_container = dispatch_meta_is_static_container(self_meta);
    const bool debug_rrvd = HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_RRVD");
    out = vd;
    out.sampled = out.sampled || vd.sampled;
    bind_view_data_ops(out);
    if (debug_rrvd) {
        std::fprintf(stderr, "[rrvd] START path=%s self_is_ref=%d self_static=%d meta_kind=%d\n",
            vd.to_short_path().to_string().c_str(), self_is_ref?1:0, self_static_container?1:0,
            self_meta != nullptr ? static_cast<int>(self_meta->kind) : -1);
    }

    for (size_t depth = 0; depth < 64; ++depth) {
        if (auto rebound = resolve_bound_view_data(out); rebound.has_value()) {
            const ViewData next = std::move(rebound.value());
            if (is_same_view_data(next, out)) {
                if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu bound_same -> false\n", depth);
                return false;
            }
            out = next;
            out.sampled = out.sampled || vd.sampled;
            bind_view_data_ops(out);
            if (debug_rrvd) {
                std::fprintf(stderr, "[rrvd] depth=%zu BOUND -> %s meta_kind=%d\n", depth,
                    out.to_short_path().to_string().c_str(),
                    out.meta != nullptr ? static_cast<int>(out.meta->kind) : -1);
            }

            // REF views expose the reference object itself. For REF consumers we
            // stop after resolving the direct bind chain.
            if (self_is_ref) {
                navigate_level_to_path(out);
                return true;
            }
            continue;
        }

        if (self_is_ref) {
            navigate_level_to_path(out);
            if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu self_is_ref -> true at %s\n", depth, out.to_short_path().to_string().c_str());
            return true;
        }

        const TSMeta* current = out.meta;
        if (!dispatch_meta_is_ref(current)) {
            if (auto through_ref_ancestor = resolve_ref_ancestor_descendant_view_data(out);
                through_ref_ancestor.has_value()) {
                const ViewData next = std::move(*through_ref_ancestor);
                if (is_same_view_data(next, out)) {
                    if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu ref_ancestor_same -> false\n", depth);
                    return false;
                }
                out = next;
                out.sampled = out.sampled || vd.sampled;
                bind_view_data_ops(out);
                if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu REF_ANCESTOR -> %s\n", depth, out.to_short_path().to_string().c_str());
                continue;
            }
            navigate_level_to_path(out);
            if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu non-ref DONE at %s\n", depth, out.to_short_path().to_string().c_str());
            return true;
        }

        if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu current_is_ref at %s, checking value slot\n", depth, out.to_short_path().to_string().c_str());
        if (auto ref_value = resolve_value_slot_const(out);
            ref_value.has_value() && ref_value->valid() && ref_value->schema() == ts_reference_meta()) {
            TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(ref_value->data());
            if (debug_rrvd) {
                std::fprintf(stderr, "[rrvd] depth=%zu ref is_empty=%d is_bound=%d is_unbound=%d has_bound=%d\n",
                    depth, ref.is_empty()?1:0, ref.is_bound()?1:0, ref.is_unbound()?1:0,
                    ref.bound_view()!=nullptr?1:0);
            }
            if (const ViewData* target = ref.bound_view(); target != nullptr) {
                if (debug_rrvd) {
                    engine_time_t target_dlmt = direct_last_modified_time(*target);
                    std::fprintf(stderr, "[rrvd] depth=%zu REF_TARGET -> %s meta_kind=%d target_dlmt=%lld\n",
                        depth, target->to_short_path().to_string().c_str(),
                        target->meta != nullptr ? static_cast<int>(target->meta->kind) : -1,
                        static_cast<long long>(target_dlmt.time_since_epoch().count()));
                }
                if (HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_ANCESTOR")) {
                    std::string target_repr{"<none>"};
                    if (target->ops != nullptr && target->ops->to_python != nullptr) {
                        try {
                            target_repr = nb::cast<std::string>(nb::repr(op_to_python(*target)));
                        } catch (...) {
                            target_repr = "<repr_error>";
                        }
                    }
                    std::fprintf(stderr,
                                 "[resolve_read_ref] path=%s ref_value_data=%p target_value_data=%p target_path=%s target=%s\n",
                                 out.to_short_path().to_string().c_str(),
                                 ref_value->data(),
                                 target->value_data,
                                 target->to_short_path().to_string().c_str(),
                                 target_repr.c_str());
                }
                out = *target;
                out.sampled = out.sampled || vd.sampled;
                out.projection = merge_projection(vd.projection, out.projection);
                bind_view_data_ops(out);
                continue;
            }

            // Empty local REF values are placeholders used by REF->REF bind.
            // Fall through to binding resolution if a link exists.
            if (!ref.is_empty()) {
                if (self_static_container) {
                    // Static container consumers can be driven by unbound REF
                    // payloads (for example switch-style REF[TSB] wrappers).
                    // Keep reads on the local container view so child paths can
                    // resolve through REF descendants.
                    out = vd;
                    out.sampled = out.sampled || vd.sampled;
                    navigate_level_to_path(out);
                    return true;
                }
                return false;
            }
        }
        if (debug_rrvd) std::fprintf(stderr, "[rrvd] depth=%zu -> false (no ref value or empty ref)\n", depth);
        return false;
    }

    if (debug_rrvd) std::fprintf(stderr, "[rrvd] -> false (max depth)\n");
    return false;
}

void stamp_time_paths(ViewData& vd, engine_time_t current_time) {
    // Update TSLevelEntry time for this level and all ancestors
    if (vd.level != nullptr) {
        vd.level->last_modified_time = current_time;
        // Also stamp ancestor levels so parent containers appear modified
        if (vd.root_level != nullptr && vd.root_level != vd.level) {
            TSLevelEntry* ancestor = vd.root_level;
            ancestor->last_modified_time = current_time;
            const auto indices = vd.path_indices();
            for (size_t i = 0; i + 1 < indices.size(); ++i) {
                ancestor = ancestor->child_at(indices[i]);
                if (ancestor == nullptr) break;
                ancestor->last_modified_time = current_time;
            }
        }
    }

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

    for (const auto& path : time_stamp_paths_for_ts_path(vd.root_meta, vd.path_indices())) {
        stamp_one_time_path(path);
    }

    // For static containers (TSB, fixed-size TSL), writing a parent value should also
    // advance child timestamps so child.modified mirrors Python semantics.
    const TSMeta* target_meta = vd.meta;
    if (dispatch_meta_is_static_container(target_meta)) {
        // TSD child writes can be partial (for example only one bundle field present in
        // a dict delta). In that case, stamping all descendants would incorrectly mark
        // absent fields as valid/modified.
        bool parent_is_tsd = false;
        if (vd.path_depth() > 0) {
            const auto _indices = vd.path_indices();
            std::vector<size_t> parent_path(_indices.begin(), _indices.end() - 1);
            const TSMeta* parent_meta = meta_at_path(vd.root_meta, parent_path);
            parent_is_tsd = dispatch_meta_is_tsd(parent_meta);
        }
        if (parent_is_tsd) {
            return;
        }

        std::vector<std::vector<size_t>> descendant_ts_paths;
        std::vector<size_t>              current_ts_path = vd.path_indices();
        collect_static_descendant_ts_paths(target_meta, current_ts_path, descendant_ts_paths);
        for (const auto& descendant_ts_path : descendant_ts_paths) {
            // Also stamp level entry for descendant
            if (vd.level != nullptr) {
                // Navigate from root level to descendant using the relative path
                // (descendant_ts_path starts from root, but we need relative to vd)
                const size_t base_depth = vd.path_depth();
                TSLevelEntry* descendant_level = vd.level;
                for (size_t i = base_depth; i < descendant_ts_path.size() && descendant_level != nullptr; ++i) {
                    descendant_level = descendant_level->ensure_child(descendant_ts_path[i]);
                }
                if (descendant_level != nullptr) {
                    descendant_level->last_modified_time = current_time;
                }
            }
            for (const auto& time_path : time_stamp_paths_for_ts_path(vd.root_meta, descendant_ts_path)) {
                stamp_one_time_path(time_path);
            }
        }
    }
}

void set_leaf_time_path(ViewData& vd, engine_time_t time_value) {
    // Update TSLevelEntry time
    if (vd.level != nullptr) {
        vd.level->last_modified_time = time_value;
    }

    auto* time_root = static_cast<Value*>(vd.time_data);
    if (time_root == nullptr || time_root->schema() == nullptr || !time_root->has_value()) {
        return;
    }

    const auto time_path = ts_path_to_time_path(vd.root_meta, vd.path_indices());
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
    if (vd.path_depth() == 0) {
        return value_root->view();
    }
    return navigate_mut(value_root->view(), vd.path_indices());
}

std::optional<View> resolve_value_slot_const(const ViewData& vd) {
    auto* value_root = static_cast<const Value*>(vd.value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return std::nullopt;
    }
    if (vd.path_depth() == 0) {
        return value_root->view();
    }
    return navigate_const(value_root->view(), vd.path_indices());
}

namespace {

nb::object* resolve_python_value_cache_slot_impl(PythonValueCacheNode* root,
                                                 const std::vector<size_t>& path,
                                                 bool create) {
    if (root == nullptr) {
        return nullptr;
    }

    if (path.empty()) {
        return root->scalar_value();
    }

    PythonValueCacheNode* node = root;
    for (size_t depth = 0; depth < path.size(); ++depth) {
        const size_t index = path[depth];
        nb::object* slot = node->slot_value(index, create);
        if (slot == nullptr) {
            return nullptr;
        }
        if (depth + 1 == path.size()) {
            return slot;
        }

        node = node->child_node(index, create);
        if (node == nullptr) {
            return nullptr;
        }
    }

    return nullptr;
}

const nb::object* resolve_python_value_cache_slot_impl(const PythonValueCacheNode* root,
                                                       const std::vector<size_t>& path) {
    if (root == nullptr) {
        return nullptr;
    }

    if (path.empty()) {
        return root->scalar_value();
    }

    const PythonValueCacheNode* node = root;
    for (size_t depth = 0; depth < path.size(); ++depth) {
        const size_t index = path[depth];
        const nb::object* slot = node->slot_value(index);
        if (slot == nullptr) {
            return nullptr;
        }
        if (depth + 1 == path.size()) {
            return slot;
        }

        node = node->child_node(index);
        if (node == nullptr) {
            return nullptr;
        }
    }

    return nullptr;
}

PythonDeltaCacheEntry* resolve_python_delta_cache_slot_impl(PythonValueCacheNode* root,
                                                            const std::vector<size_t>& path,
                                                            bool create) {
    if (root == nullptr) {
        return nullptr;
    }

    if (path.empty()) {
        return root->delta_root_value();
    }

    PythonValueCacheNode* node = root;
    for (size_t depth = 0; depth < path.size(); ++depth) {
        const size_t index = path[depth];
        PythonDeltaCacheEntry* slot = node->delta_slot_value(index, create);
        if (slot == nullptr) {
            return nullptr;
        }
        if (depth + 1 == path.size()) {
            return slot;
        }

        node = node->child_node(index, create);
        if (node == nullptr) {
            return nullptr;
        }
    }

    return nullptr;
}

const PythonDeltaCacheEntry* resolve_python_delta_cache_slot_impl(const PythonValueCacheNode* root,
                                                                  const std::vector<size_t>& path) {
    if (root == nullptr) {
        return nullptr;
    }

    if (path.empty()) {
        return root->delta_root_value();
    }

    const PythonValueCacheNode* node = root;
    for (size_t depth = 0; depth < path.size(); ++depth) {
        const size_t index = path[depth];
        const PythonDeltaCacheEntry* slot = node->delta_slot_value(index);
        if (slot == nullptr) {
            return nullptr;
        }
        if (depth + 1 == path.size()) {
            return slot;
        }

        node = node->child_node(index);
        if (node == nullptr) {
            return nullptr;
        }
    }

    return nullptr;
}

}  // namespace

nb::object* resolve_python_value_cache_slot(ViewData& vd, bool create) {
    auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
    auto* slot = resolve_python_value_cache_slot_impl(root, vd.path_indices(), create);
    vd.python_value_cache_slot = slot;
    return slot;
}

const nb::object* resolve_python_value_cache_slot(const ViewData& vd) {
    auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
    return resolve_python_value_cache_slot_impl(root, vd.path_indices());
}

PythonDeltaCacheEntry* resolve_python_delta_cache_slot(ViewData& vd, bool create) {
    auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
    return resolve_python_delta_cache_slot_impl(root, vd.path_indices(), create);
}

const PythonDeltaCacheEntry* resolve_python_delta_cache_slot(const ViewData& vd) {
    auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
    return resolve_python_delta_cache_slot_impl(root, vd.path_indices());
}

void seed_python_value_cache_slot(ViewData& vd, const nb::object& value) {
    nb::object* slot = vd.python_value_cache_slot != nullptr
                           ? static_cast<nb::object*>(vd.python_value_cache_slot)
                           : resolve_python_value_cache_slot(vd, true);
    if (slot == nullptr) {
        return;
    }
    *slot = value;
    vd.python_value_cache_slot = slot;
}

void seed_python_value_cache_slot_from_view(ViewData& vd, const View& value) {
    if (!value.valid()) {
        seed_python_value_cache_slot(vd, nb::none());
        return;
    }
    seed_python_value_cache_slot(vd, value.to_python());
}

void invalidate_python_value_cache(ViewData& vd) {
    HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_CALLS();
    auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
    if (root == nullptr || root->empty()) {
        return;
    }
    HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_EFFECTIVE();
    root->keyed_delta_lookup_cache()->clear();
    root->tsd_key_set_delta_cache()->clear();

    if (Py_IsInitialized() == 0) {
        root->abandon_subtree();
        vd.python_value_cache_slot = nullptr;
        return;
    }

    nb::gil_scoped_acquire gil;
    if (vd.path_depth() == 0) {
        root->clear_subtree();
        vd.python_value_cache_slot = root->scalar_value();
        return;
    }

    // Invalidate root delta cache for non-root writes to keep container-level
    // delta reads coherent without per-parent cache traversal.
    root->delta_root_value()->clear();

    PythonValueCacheNode* node = root;
    const auto indices = vd.path_indices();
    const size_t total_depth = indices.size();
    for (size_t depth = 0; depth < total_depth; ++depth) {
        const size_t index = indices[depth];
        nb::object* slot = node->slot_value(index, false);
        if (slot == nullptr) {
            vd.python_value_cache_slot = nullptr;
            return;
        }
        *slot = nb::object();
        if (PythonDeltaCacheEntry* delta_slot = node->delta_slot_value(index, false); delta_slot != nullptr) {
            delta_slot->clear();
        }

        if (depth + 1 == total_depth) {
            if (PythonValueCacheNode* child = node->child_node(index, false); child != nullptr) {
                child->clear_subtree();
            }
            vd.python_value_cache_slot = slot;
            return;
        }

        node = node->child_node(index, false);
        if (node == nullptr) {
            vd.python_value_cache_slot = nullptr;
            return;
        }
    }

    vd.python_value_cache_slot = nullptr;
}

void invalidate_python_delta_cache(ViewData& vd) {
    HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_CALLS();
    auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
    if (root == nullptr || root->empty()) {
        return;
    }
    HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_EFFECTIVE();
    root->keyed_delta_lookup_cache()->clear();
    root->tsd_key_set_delta_cache()->clear();

    if (Py_IsInitialized() == 0) {
        root->abandon_delta_subtree();
        return;
    }

    nb::gil_scoped_acquire gil;
    if (vd.path_depth() == 0) {
        root->clear_delta_subtree();
        return;
    }

    // Keep container-level delta reads coherent for descendant invalidations.
    root->delta_root_value()->clear();

    PythonValueCacheNode* node = root;
    const auto indices = vd.path_indices();
    const size_t total_depth = indices.size();
    for (size_t depth = 0; depth < total_depth; ++depth) {
        const size_t index = indices[depth];
        PythonDeltaCacheEntry* delta_slot = node->delta_slot_value(index, false);
        if (delta_slot == nullptr) {
            return;
        }
        delta_slot->clear();

        if (depth + 1 == total_depth) {
            if (PythonValueCacheNode* child = node->child_node(index, false); child != nullptr) {
                child->clear_delta_subtree();
            }
            return;
        }

        node = node->child_node(index, false);
        if (node == nullptr) {
            return;
        }
    }
}

bool has_local_ref_wrapper_value(const ViewData& vd) {
    const TSMeta* current = vd.meta;
    if (!dispatch_meta_is_ref(current)) {
        return false;
    }

    auto local = resolve_value_slot_const(vd);
    if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = *static_cast<const TimeSeriesReference*>(local->data());
    return !ref.is_empty();
}

bool has_bound_ref_static_children(const ViewData& vd) {
    const TSMeta* current = vd.meta;
    if (!dispatch_meta_is_ref(current)) {
        return false;
    }

    const TSMeta* element = current->element_ts();
    if (element == nullptr) {
        return false;
    }

    if (dispatch_meta_is_tsb(element) && element->fields() != nullptr) {
        for (size_t i = 0; i < element->field_count(); ++i) {
            std::vector<size_t> child_path = vd.path_indices();
            child_path.push_back(i);
            if (LinkTarget* child = resolve_link_target(vd, child_path); child != nullptr && child->is_linked) {
                return true;
            }
        }
    } else if (dispatch_meta_is_fixed_tsl(element)) {
        for (size_t i = 0; i < element->fixed_size(); ++i) {
            std::vector<size_t> child_path = vd.path_indices();
            child_path.push_back(i);
            if (LinkTarget* child = resolve_link_target(vd, child_path); child != nullptr && child->is_linked) {
                return true;
            }
        }
    }

    return false;
}

bool assign_ref_value_from_bound_static_children(ViewData& vd) {
    const TSMeta* current = vd.meta;
    if (!dispatch_meta_is_ref(current)) {
        return false;
    }

    const TSMeta* element = current->element_ts();
    if (element == nullptr) {
        return false;
    }

    size_t child_count = 0;
    if (dispatch_meta_is_tsb(element) && element->fields() != nullptr) {
        child_count = element->field_count();
    } else if (dispatch_meta_is_fixed_tsl(element)) {
        child_count = element->fixed_size();
    } else {
        return false;
    }

    std::vector<TimeSeriesReference> child_refs;
    child_refs.reserve(child_count);
    bool has_non_empty_child = false;

    for (size_t i = 0; i < child_count; ++i) {
        ViewData child = make_child_view_data(vd, i);

        TimeSeriesReference child_ref = TimeSeriesReference::make();
        if (auto bound = resolve_bound_view_data(child); bound.has_value()) {
            const TSMeta* bound_meta = bound->meta;
            if (dispatch_meta_is_ref(bound_meta)) {
                bool resolved_from_local = false;
                if (auto local_ref_value = resolve_value_slot_const(*bound);
                    local_ref_value.has_value() &&
                    local_ref_value->valid() &&
                    local_ref_value->schema() == ts_reference_meta()) {
                    child_ref = *static_cast<const TimeSeriesReference*>(local_ref_value->data());
                    resolved_from_local = true;
                }

                View bound_value = op_value(*bound);
                if (!resolved_from_local &&
                    bound_value.valid() &&
                    bound_value.schema() == ts_reference_meta()) {
                    child_ref = *static_cast<const TimeSeriesReference*>(bound_value.data());
                } else if (!resolved_from_local && op_valid(*bound)) {
                    child_ref = TimeSeriesReference::make(*bound);
                }
            } else {
                child_ref = TimeSeriesReference::make(*bound);
            }
        } else {
            const TSMeta* child_meta = child.meta;
            bool resolved_from_local = false;
            if (dispatch_meta_is_ref(child_meta)) {
                if (auto local_ref_value = resolve_value_slot_const(child);
                    local_ref_value.has_value() &&
                    local_ref_value->valid() &&
                    local_ref_value->schema() == ts_reference_meta()) {
                    child_ref = *static_cast<const TimeSeriesReference*>(local_ref_value->data());
                    resolved_from_local = true;
                }
            }

            View child_value = op_value(child);
            if (!resolved_from_local && child_value.valid()) {
                if (child_value.schema() == ts_reference_meta()) {
                    child_ref = *static_cast<const TimeSeriesReference*>(child_value.data());
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

    auto* dst_ref = static_cast<TimeSeriesReference*>(maybe_dst->data());
    if (has_non_empty_child) {
        *dst_ref = TimeSeriesReference::make(std::move(child_refs));
    } else {
        *dst_ref = TimeSeriesReference::make();
    }
    return true;
}

bool assign_ref_value_from_target(ViewData& vd, const ViewData& target) {
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return false;
    }
    *static_cast<TimeSeriesReference*>(maybe_dst->data()) = TimeSeriesReference::make(target);
    return true;
}

void clear_ref_value(ViewData& vd) {
    if (HGRAPH_DEBUG_ENV_ENABLED("HGRAPH_DEBUG_REF_BIND_PATH")) {
        std::fprintf(stderr,
                     "[clear_ref_value] path=%s root_reset=%d\n",
                     vd.to_short_path().to_string().c_str(),
                     vd.path_depth() == 0 ? 1 : 0);
    }
    auto* value_root = static_cast<Value*>(vd.value_data);
    if (value_root == nullptr) {
        return;
    }
    if (vd.path_depth() == 0) {
        value_root->reset();
        set_leaf_time_path(vd, MIN_DT);
        return;
    }
    auto maybe_dst = resolve_value_slot_mut(vd);
    if (!maybe_dst.has_value() || !maybe_dst->valid() || maybe_dst->schema() != ts_reference_meta()) {
        return;
    }
    *static_cast<TimeSeriesReference*>(maybe_dst->data()) = TimeSeriesReference::make();
    set_leaf_time_path(vd, MIN_DT);
}

void clear_ref_container_ancestor_cache(ViewData& vd) {
    if (vd.path_depth() == 0) {
        return;
    }

    const auto indices = vd.path_indices();
    for (size_t depth = 0; depth < indices.size(); ++depth) {
        std::vector<size_t> ancestor_path(indices.begin(),
                                          indices.begin() + static_cast<std::ptrdiff_t>(depth));
        const TSMeta* ancestor_meta = meta_at_path(vd.root_meta, ancestor_path);
        if (!dispatch_meta_is_ref(ancestor_meta)) {
            continue;
        }

        const TSMeta* element_meta = ancestor_meta->element_ts();
        const bool static_ref_container = dispatch_meta_is_static_container(element_meta);
        if (!static_ref_container) {
            break;
        }

        // Walk up the PathNode chain to reach the ancestor at `depth`
        PathNode* ancestor_node = vd.path.get();
        for (size_t i = indices.size(); i > depth && ancestor_node != nullptr; --i) {
            ancestor_node = ancestor_node->parent;
        }
        ViewData ancestor = vd;
        if (ancestor_node != nullptr) {
            ancestor_node->retain();
            ancestor.path = PathHandle(ancestor_node);
        } else {
            ancestor.path = PathHandle{};
        }
        sync_level_to_path(ancestor);
        clear_ref_value(ancestor);
        break;
    }
}



}  // namespace hgraph
