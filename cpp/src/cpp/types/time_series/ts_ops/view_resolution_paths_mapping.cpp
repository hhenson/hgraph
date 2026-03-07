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
        if (dispatch_meta_is_ref(meta)) {
            out.push_back(0);  // root/container link slot.
            return out;
        }
        switch (dispatch_meta_path_kind(meta)) {
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
        while (dispatch_meta_is_ref(meta)) {
            out.push_back(1);  // descend into referred link tree.
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        if (crossed_dynamic_boundary) {
            if (!dispatch_meta_step_child_no_ref(meta, index)) {
                return out;
            }
            continue;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                out.push_back(index + 1);  // slot 0 reserved for container link.
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSLFixed:
                out.push_back(1);
                out.push_back(index);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSLDynamic:
                crossed_dynamic_boundary = true;
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSD:
                out.push_back(1);
                out.push_back(index);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            default:
                return out;
        }
    }

    // Container/REF nodes have a root link slot at 0.
    if (!crossed_dynamic_boundary && meta != nullptr) {
        if (dispatch_meta_is_ref(meta)) {
            out.push_back(0);
            return out;
        }
        switch (dispatch_meta_path_kind(meta)) {
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
        while (dispatch_meta_is_ref(meta)) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                out.push_back(index + 1);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                out.push_back(1);
                out.push_back(index);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSD:
                out.push_back(1);
                out.push_back(index);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
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
        while (dispatch_meta_is_ref(meta)) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                out.push_back(index + 1);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                out.push_back(1);
                out.push_back(index);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSD:
                out.push_back(1);
                out.push_back(index);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
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
        while (dispatch_meta_is_ref(meta)) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            break;
        }

        switch (dispatch_meta_path_kind(meta)) {
            case DispatchMetaPathKind::TSB:
                current_path.push_back(index + 1);
                out.push_back(current_path);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSLFixed:
            case DispatchMetaPathKind::TSLDynamic:
                current_path.push_back(1);
                current_path.push_back(index);
                out.push_back(current_path);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
                continue;
            case DispatchMetaPathKind::TSD:
                current_path.push_back(1);
                current_path.push_back(index);
                out.push_back(current_path);
                if (!dispatch_meta_step_child_no_ref(meta, index)) {
                    return out;
                }
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

}  // namespace hgraph
