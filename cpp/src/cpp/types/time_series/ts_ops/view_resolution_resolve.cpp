#include "ts_ops_internal.h"

namespace hgraph {

ViewProjection merge_projection(ViewProjection requested, ViewProjection resolved) {
    return requested != ViewProjection::NONE ? requested : resolved;
}

std::optional<std::vector<size_t>> remap_residual_indices_for_bound_view(
    const ViewData& local_view,
    const ViewData& bound_view,
    const std::vector<size_t>& residual_indices) {
    if (residual_indices.empty()) {
        return std::vector<size_t>{};
    }

    const TSMeta* current = meta_at_path(local_view.meta, local_view.path.indices);
    std::vector<size_t> local_path = local_view.path.indices;
    std::vector<size_t> bound_path = bound_view.path.indices;
    std::vector<size_t> mapped;
    mapped.reserve(residual_indices.size());

    for (size_t index : residual_indices) {
        while (dispatch_meta_is_ref(current)) {
            current = current->element_ts();
        }
        if (current == nullptr) {
            return std::nullopt;
        }

        switch (dispatch_meta_path_kind(current)) {
            case DispatchMetaPathKind::TSB:
                if (current->fields() == nullptr || index >= current->field_count()) {
                    return std::nullopt;
                }
                mapped.push_back(index);
                local_path.push_back(index);
                bound_path.push_back(index);
                current = current->fields()[index].ts_type;
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                mapped.push_back(index);
                local_path.push_back(index);
                bound_path.push_back(index);
                current = current->element_ts();
                continue;
            case DispatchMetaPathKind::TSD: {
                ViewData local_container = local_view;
                local_container.path.indices = local_path;
                ViewData bound_container = bound_view;
                bound_container.path.indices = bound_path;

                auto local_value = resolve_value_slot_const(local_container);
                auto bound_value = resolve_value_slot_const(bound_container);
                if (!local_value.has_value() || !bound_value.has_value() ||
                    !local_value->valid() || !bound_value->valid() ||
                    !local_value->is_map() || !bound_value->is_map()) {
                    return std::nullopt;
                }

                auto local_map = local_value->as_map();
                auto bound_map = bound_value->as_map();

                std::optional<View> key_for_local_slot;
                for (View key : local_map.keys()) {
                    auto slot = map_slot_for_key(local_map, key);
                    if (slot.has_value() && *slot == index) {
                        key_for_local_slot = key;
                        break;
                    }
                }
                if (!key_for_local_slot.has_value()) {
                    return std::nullopt;
                }

                auto bound_slot = map_slot_for_key(bound_map, *key_for_local_slot);
                if (!bound_slot.has_value()) {
                    return std::nullopt;
                }

                mapped.push_back(*bound_slot);
                local_path.push_back(index);
                bound_path.push_back(*bound_slot);
                current = current->element_ts();
                continue;
            }
            default:
                break;
        }

        mapped.push_back(index);
        local_path.push_back(index);
        bound_path.push_back(index);
        current = nullptr;
    }

    return mapped;
}

std::optional<ViewData> resolve_bound_view_data(const ViewData& vd) {
    const bool debug_resolve = std::getenv("HGRAPH_DEBUG_RESOLVE") != nullptr;
    const auto target_can_accept_residual = [](const TSMeta* meta) {
        while (dispatch_meta_is_ref(meta)) {
            meta = meta->element_ts();
        }
        if (meta == nullptr) {
            return false;
        }
        const DispatchMetaPathKind kind = dispatch_meta_path_kind(meta);
        return kind == DispatchMetaPathKind::TSB ||
               kind == DispatchMetaPathKind::TSLFixed ||
               kind == DispatchMetaPathKind::TSLDynamic ||
               kind == DispatchMetaPathKind::TSD;
    };
    const auto log_resolve = [&](const char* tag, const ViewData& out) {
        if (!debug_resolve) {
            return;
        }
        const TSMeta* in_meta = vd.meta != nullptr ? meta_at_path(vd.meta, vd.path.indices) : nullptr;
        const TSMeta* out_meta = out.meta != nullptr ? meta_at_path(out.meta, out.path.indices) : nullptr;
        std::fprintf(stderr,
                     "[resolve] %s in=%s uses_lt=%d kind=%d -> out=%s out_uses_lt=%d out_kind=%d\n",
                     tag,
                     vd.path.to_string().c_str(),
                     vd.uses_link_target ? 1 : 0,
                     in_meta != nullptr ? static_cast<int>(in_meta->kind) : -1,
                     out.path.to_string().c_str(),
                     out.uses_link_target ? 1 : 0,
                     out_meta != nullptr ? static_cast<int>(out_meta->kind) : -1);
    };

    if (vd.uses_link_target) {
        if (LinkTarget* target = resolve_link_target(vd, vd.path.indices);
            target != nullptr && target->is_linked) {
            ViewData resolved = target->as_view_data(vd.sampled);
            resolved.projection = merge_projection(vd.projection, resolved.projection);
            size_t residual_len = 0;
            const TSMeta* resolved_meta = meta_at_path(resolved.meta, resolved.path.indices);
            if (resolved.path.indices.size() < vd.path.indices.size() &&
                target_can_accept_residual(resolved_meta)) {
                const auto residual = link_residual_ts_path(vd.meta, vd.path.indices);
                residual_len = residual.size();
                if (!residual.empty()) {
                    if (auto mapped = remap_residual_indices_for_bound_view(vd, resolved, residual); mapped.has_value()) {
                        resolved.path.indices.insert(resolved.path.indices.end(), mapped->begin(), mapped->end());
                    } else {
                        resolved.path.indices.insert(resolved.path.indices.end(), residual.begin(), residual.end());
                    }
                }
            }
            bind_view_data_ops(resolved);
            if (std::getenv("HGRAPH_DEBUG_REF_ANCESTOR") != nullptr) {
                std::fprintf(stderr,
                             "[resolve_lt] in_path=%s link_target_path=%s residual_len=%zu out_path=%s out_value_data=%p\n",
                             vd.path.to_string().c_str(),
                             target->target_path.to_string().c_str(),
                             residual_len,
                             resolved.path.to_string().c_str(),
                             resolved.value_data);
            }
            log_resolve("lt-direct", resolved);
            return resolved;
        }

        // Child links in containers can remain unbound while an ancestor link is
        // bound at the container root. Resolve through the nearest linked
        // ancestor and append the residual child path.
        if (!vd.path.indices.empty()) {
            for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                const std::vector<size_t> parent_path(vd.path.indices.begin(), vd.path.indices.begin() + depth - 1);
                LinkTarget* parent = resolve_link_target(vd, parent_path);
                if (parent == nullptr || !parent->is_linked) {
                    continue;
                }

                ViewData resolved = parent->as_view_data(vd.sampled);
                resolved.projection = merge_projection(vd.projection, resolved.projection);
                const std::vector<size_t> residual(
                    vd.path.indices.begin() + static_cast<std::ptrdiff_t>(parent_path.size()),
                    vd.path.indices.end());
                ViewData local_parent = vd;
                local_parent.path.indices = parent_path;
                if (auto mapped = remap_residual_indices_for_bound_view(local_parent, resolved, residual); mapped.has_value()) {
                    resolved.path.indices.insert(resolved.path.indices.end(), mapped->begin(), mapped->end());
                } else {
                    resolved.path.indices.insert(resolved.path.indices.end(), residual.begin(), residual.end());
                }
                bind_view_data_ops(resolved);
                log_resolve("lt-ancestor", resolved);
                return resolved;
            }
        }
    } else {
        const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
        if (dispatch_meta_is_ref(current)) {
            // For per-slot REF values (notably TSD dynamic children), prefer the
            // local reference payload over shared REFLink indirection.
            if (auto local = resolve_value_slot_const(vd);
                local.has_value() && local->valid() && local->schema() == ts_reference_meta()) {
                TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
                if (ref.is_empty()) {
                    return std::nullopt;
                }
            }
        }

        // Prefer local REF wrapper payloads on ancestor paths (e.g. TSD slot
        // descendants) before consulting REFLink alternative storage.
        if (auto resolved_ref_ancestor = resolve_ref_ancestor_descendant_view_data(vd);
            resolved_ref_ancestor.has_value()) {
            ViewData resolved = std::move(*resolved_ref_ancestor);
            bind_view_data_ops(resolved);
            log_resolve("ref-value-ancestor", resolved);
            return resolved;
        }

        // REFLink dereference is alternative storage for REF paths only.
        // Non-REF container roots (e.g. TSD of REF values) must not resolve via
        // REFLink, otherwise reads collapse to a child scalar target.
        const auto path_traverses_ref = [&vd]() -> bool {
            const TSMeta* cursor = vd.meta;
            for (size_t index : vd.path.indices) {
                if (dispatch_meta_is_ref(cursor)) {
                    return true;
                }
                if (cursor == nullptr) {
                    return false;
                }

                switch (dispatch_meta_path_kind(cursor)) {
                    case DispatchMetaPathKind::TSB:
                        if (cursor->fields() == nullptr || index >= cursor->field_count()) {
                            return false;
                        }
                        cursor = cursor->fields()[index].ts_type;
                        continue;
                    case DispatchMetaPathKind::TSLFixed:
                    case DispatchMetaPathKind::TSLDynamic:
                    case DispatchMetaPathKind::TSD:
                        cursor = cursor->element_ts();
                        continue;
                    default:
                        return false;
                }
            }
            return dispatch_meta_is_ref(cursor);
        };

        if (!path_traverses_ref()) {
            return std::nullopt;
        }

        REFLink* direct_ref_link = resolve_ref_link(vd, vd.path.indices);
        if (debug_resolve) {
            std::fprintf(stderr,
                         "[resolve-ref] direct path=%s link=%p linked=%d\n",
                         vd.path.to_string().c_str(),
                         static_cast<void*>(direct_ref_link),
                         (direct_ref_link != nullptr && direct_ref_link->is_linked) ? 1 : 0);
        }

        if (direct_ref_link != nullptr && direct_ref_link->is_linked) {
            REFLink* ref_link = direct_ref_link;
            ViewData resolved = ref_link->resolved_view_data();
            // Direct REF links already represent the fully resolved target for
            // this exact path. Appending residual indices here would duplicate
            // keyed TSD slots (for example out/1 -> out/1/1) and collapse
            // nested REF[TSD] reads to scalar children.
            resolved.sampled = resolved.sampled || vd.sampled;
            resolved.projection = merge_projection(vd.projection, resolved.projection);
            bind_view_data_ops(resolved);
            log_resolve("ref-direct", resolved);
            return resolved;
        }

        // Like LinkTarget, REF-linked container roots can service descendant
        // reads/writes by carrying the unresolved child suffix.
        if (!vd.path.indices.empty()) {
            for (size_t depth = vd.path.indices.size(); depth > 0; --depth) {
                const std::vector<size_t> parent_path(vd.path.indices.begin(), vd.path.indices.begin() + depth - 1);
                REFLink* parent = resolve_ref_link(vd, parent_path);
                if (debug_resolve) {
                    ViewData parent_vd = vd;
                    parent_vd.path.indices = parent_path;
                    std::fprintf(stderr,
                                 "[resolve-ref] ancestor path=%s link=%p linked=%d\n",
                                 parent_vd.path.to_string().c_str(),
                                 static_cast<void*>(parent),
                                 (parent != nullptr && parent->is_linked) ? 1 : 0);
                }
                if (parent == nullptr || !parent->is_linked) {
                    continue;
                }

                ViewData resolved = parent->resolved_view_data();
                resolved.path.indices.insert(
                    resolved.path.indices.end(),
                    vd.path.indices.begin() + static_cast<std::ptrdiff_t>(parent_path.size()),
                    vd.path.indices.end());
                resolved.sampled = resolved.sampled || vd.sampled;
                resolved.projection = merge_projection(vd.projection, resolved.projection);
                bind_view_data_ops(resolved);
                log_resolve("ref-ancestor", resolved);
                return resolved;
            }
        }
    }

    return std::nullopt;
}


}  // namespace hgraph
