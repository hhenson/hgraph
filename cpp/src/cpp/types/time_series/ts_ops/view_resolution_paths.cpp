#include "ts_ops_internal.h"

namespace hgraph {

namespace {

struct PathVectorCacheEntry {
    const TSMeta* root_meta{nullptr};
    std::vector<size_t> ts_path;
    std::vector<size_t> out_path;
    bool valid{false};
};

struct PathVectorListCacheEntry {
    const TSMeta* root_meta{nullptr};
    std::vector<size_t> ts_path;
    std::vector<std::vector<size_t>> out_paths;
    bool valid{false};
};

std::vector<size_t> ts_path_to_link_path_uncached(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;
    bool crossed_dynamic_boundary = false;

    if (meta == nullptr) {
        return out;
    }

    if (ts_path.empty()) {
        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::Ref:
            case DispatchMetaPathKind::TSB:
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSD:
                out.push_back(0);  // root/container link slot.
                break;
            default:
                break;
        }
        return out;
    }

    for (size_t index : ts_path) {
        while (dispatch_meta_path_kind(meta) == DispatchMetaPathKind::Ref) {
            out.push_back(1);  // descend into referred link tree.
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        if (crossed_dynamic_boundary) {
            switch (dispatch_meta_path_kind(meta)) {
                case DispatchMetaPathKind::TSB:
                    if (meta->fields() == nullptr || index >= meta->field_count()) {
                        return out;
                    }
                    meta = meta->fields()[index].ts_type;
                    continue;
                case DispatchMetaPathKind::TSLFixed:
                case DispatchMetaPathKind::TSLDynamic:
                case DispatchMetaPathKind::TSD:
                    meta = meta->element_ts();
                    continue;
                default:
                    return out;
            }
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                out.push_back(index + 1);  // slot 0 reserved for container link.
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                continue;
            case DispatchMetaPathKind::TSLFixed:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                continue;
            case DispatchMetaPathKind::TSLDynamic:
                crossed_dynamic_boundary = true;
                meta = meta->element_ts();
                continue;
            case DispatchMetaPathKind::TSD:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                continue;
            default:
                return out;
        }
    }

    // Container/REF nodes have a root link slot at 0.
    if (!crossed_dynamic_boundary && meta != nullptr) {
        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::Ref:
            case DispatchMetaPathKind::TSB:
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSD:
                out.push_back(0);
                break;
            default:
                break;
        }
    }

    return out;
}

std::vector<size_t> ts_path_to_time_path_uncached(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;

    if (meta == nullptr) {
        return out;
    }

    if (ts_path.empty()) {
        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
            case DispatchMetaPathKind::TSD:
            case DispatchMetaPathKind::TSW:
                out.push_back(0);  // container time slot
                break;
            default:
                break;
        }
        return out;
    }

    for (size_t index : ts_path) {
        while (dispatch_meta_path_kind(meta) == DispatchMetaPathKind::Ref) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                out.push_back(index + 1);
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                continue;
            case DispatchMetaPathKind::TSD:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                continue;
            default:
                return out;
        }
    }

    return out;
}

std::vector<size_t> ts_path_to_observer_path_uncached(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;

    if (meta == nullptr) {
        return out;
    }

    if (ts_path.empty()) {
        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
            case DispatchMetaPathKind::TSD:
                out.push_back(0);  // container observer slot
                break;
            default:
                break;
        }
        return out;
    }

    for (size_t index : ts_path) {
        while (dispatch_meta_path_kind(meta) == DispatchMetaPathKind::Ref) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                out.push_back(index + 1);
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                continue;
            case DispatchMetaPathKind::TSD:
                out.push_back(1);
                out.push_back(index);
                meta = meta->element_ts();
                continue;
            default:
                return out;
        }
    }

    return out;
}

std::vector<std::vector<size_t>> time_stamp_paths_for_ts_path_uncached(const TSMeta* root_meta,
                                                                        const std::vector<size_t>& ts_path) {
    std::vector<std::vector<size_t>> out;
    if (root_meta == nullptr) {
        return out;
    }

    switch (dispatch_meta_path_kind(root_meta)) {
        case DispatchMetaPathKind::TSB:
        case DispatchMetaPathKind::TSLFixed:
        case DispatchMetaPathKind::TSLDynamic:
        case DispatchMetaPathKind::TSD:
        case DispatchMetaPathKind::TSW:
            out.push_back({0});  // root container timestamp
            break;
        default:
            out.push_back({});
            break;
    }

    const TSMeta* meta = root_meta;
    std::vector<size_t> current_path;
    for (size_t index : ts_path) {
        while (dispatch_meta_path_kind(meta) == DispatchMetaPathKind::Ref) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                current_path.push_back(index + 1);
                out.push_back(current_path);
                if (index >= meta->field_count() || meta->fields() == nullptr) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                current_path.push_back(1);
                current_path.push_back(index);
                out.push_back(current_path);
                meta = meta->element_ts();
                continue;
            case DispatchMetaPathKind::TSD:
                current_path.push_back(1);
                current_path.push_back(index);
                out.push_back(current_path);
                meta = meta->element_ts();
                continue;
            default:
                return out;
        }
    }

    return out;
}

}  // namespace

std::vector<size_t> ts_path_to_link_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    static thread_local PathVectorCacheEntry cache{};
    if (cache.valid &&
        cache.root_meta == root_meta &&
        cache.ts_path == ts_path) {
        return cache.out_path;
    }

    std::vector<size_t> out = ts_path_to_link_path_uncached(root_meta, ts_path);
    cache.root_meta = root_meta;
    cache.ts_path = ts_path;
    cache.out_path = out;
    cache.valid = true;
    return out;
}

std::vector<size_t> ts_path_to_time_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    static thread_local PathVectorCacheEntry cache{};
    if (cache.valid &&
        cache.root_meta == root_meta &&
        cache.ts_path == ts_path) {
        return cache.out_path;
    }

    std::vector<size_t> out = ts_path_to_time_path_uncached(root_meta, ts_path);
    cache.root_meta = root_meta;
    cache.ts_path = ts_path;
    cache.out_path = out;
    cache.valid = true;
    return out;
}

std::vector<size_t> ts_path_to_observer_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    static thread_local PathVectorCacheEntry cache{};
    if (cache.valid &&
        cache.root_meta == root_meta &&
        cache.ts_path == ts_path) {
        return cache.out_path;
    }

    std::vector<size_t> out = ts_path_to_observer_path_uncached(root_meta, ts_path);
    cache.root_meta = root_meta;
    cache.ts_path = ts_path;
    cache.out_path = out;
    cache.valid = true;
    return out;
}

std::vector<std::vector<size_t>> time_stamp_paths_for_ts_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    static thread_local PathVectorListCacheEntry cache{};
    if (cache.valid &&
        cache.root_meta == root_meta &&
        cache.ts_path == ts_path) {
        return cache.out_paths;
    }

    std::vector<std::vector<size_t>> out = time_stamp_paths_for_ts_path_uncached(root_meta, ts_path);
    cache.root_meta = root_meta;
    cache.ts_path = ts_path;
    cache.out_paths = out;
    cache.valid = true;
    return out;
}

size_t static_container_child_count(const TSMeta* meta) {
    if (meta == nullptr) {
        return 0;
    }

    if (dispatch_meta_is_tsb(meta)) {
        return meta->field_count();
    }
    if (dispatch_meta_is_tsl(meta)) {
        return meta->fixed_size();
    }
    return 0;
}

bool link_target_points_to_unbound_ref_composite(const ViewData& vd, const LinkTarget* payload) {
    if (payload == nullptr || !payload->is_linked) {
        return false;
    }

    ViewData target = payload->as_view_data(vd.sampled);
    const TSMeta* target_meta = meta_at_path(target.meta, target.path.indices);
    if (!dispatch_meta_is_ref(target_meta)) {
        return false;
    }

    auto local = resolve_value_slot_const(target);
    if (!local.has_value() || !local->valid() || local->schema() != ts_reference_meta()) {
        return false;
    }

    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(local->to_python());
    return ref.is_unbound() && !ref.is_empty();
}

bool is_unpeered_static_container_view(const ViewData& vd, const TSMeta* current) {
    if (!vd.uses_link_target || current == nullptr) {
        return false;
    }

    if (!dispatch_meta_is_static_container(current)) {
        return false;
    }

    if (LinkTarget* payload = resolve_link_target(vd, vd.path.indices); payload != nullptr) {
        return !payload->is_linked || link_target_points_to_unbound_ref_composite(vd, payload);
    }
    return false;
}

void collect_static_descendant_ts_paths(const TSMeta* node_meta,
                                        std::vector<size_t>& current_ts_path,
                                        std::vector<std::vector<size_t>>& out) {
    if (node_meta == nullptr) {
        return;
    }

    if (dispatch_meta_is_tsb(node_meta)) {
        if (node_meta->fields() == nullptr) {
            return;
        }
        for (size_t i = 0; i < node_meta->field_count(); ++i) {
            current_ts_path.push_back(i);
            out.push_back(current_ts_path);
            collect_static_descendant_ts_paths(node_meta->fields()[i].ts_type, current_ts_path, out);
            current_ts_path.pop_back();
        }
        return;
    }

    if (dispatch_meta_is_fixed_tsl(node_meta)) {
        for (size_t i = 0; i < node_meta->fixed_size(); ++i) {
            current_ts_path.push_back(i);
            out.push_back(current_ts_path);
            collect_static_descendant_ts_paths(node_meta->element_ts(), current_ts_path, out);
            current_ts_path.pop_back();
        }
    }
}

engine_time_t extract_time_value(const View& time_view) {
    if (!time_view.valid()) {
        return MIN_DT;
    }

    if (time_view.is_scalar_type<engine_time_t>()) {
        return time_view.as<engine_time_t>();
    }

    if (time_view.is_tuple()) {
        auto tuple = time_view.as_tuple();
        if (tuple.size() > 0) {
            View head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<engine_time_t>()) {
                return head.as<engine_time_t>();
            }
        }
    }

    return MIN_DT;
}

engine_time_t* extract_time_ptr(ValueView time_view) {
    if (!time_view.valid()) {
        return nullptr;
    }

    if (time_view.is_scalar_type<engine_time_t>()) {
        return &time_view.as<engine_time_t>();
    }

    if (time_view.is_tuple()) {
        auto tuple = time_view.as_tuple();
        if (tuple.size() > 0) {
            ValueView head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<engine_time_t>()) {
                return &head.as<engine_time_t>();
            }
        }
    }

    return nullptr;
}


}  // namespace hgraph
