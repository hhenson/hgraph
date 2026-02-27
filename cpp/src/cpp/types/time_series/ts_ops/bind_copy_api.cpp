#include "ts_ops_internal.h"

namespace hgraph {

TSView op_child_at(const ViewData& vd, size_t index, engine_time_t current_time) {
    (void)current_time;
    ViewData child = vd;
    child.path.indices.push_back(index);
    child.ops = get_ts_ops(meta_at_path(child.meta, child.path.indices));
    child.engine_time_ptr = vd.engine_time_ptr;
    return TSView(child, child.engine_time_ptr);
}

TSView op_child_by_name(const ViewData& vd, std::string_view name, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || !dispatch_meta_is_tsb(current) || current->fields() == nullptr) {
        return {};
    }

    for (size_t i = 0; i < current->field_count(); ++i) {
        if (name == current->fields()[i].name) {
            return op_child_at(vd, i, current_time);
        }
    }
    return {};
}

TSView op_child_by_key(const ViewData& vd, const View& key, engine_time_t current_time) {
    View v = op_value(vd);
    if (!v.valid() || !v.is_map()) {
        return {};
    }

    auto map = v.as_map();
    auto slot = map_slot_for_key(map, key);
    if (slot.has_value()) {
        return op_child_at(vd, *slot, current_time);
    }

    return {};
}

size_t op_list_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || !dispatch_meta_is_tsl(current)) {
        return 0;
    }

    if (current->fixed_size() > 0) {
        return current->fixed_size();
    }

    View v = op_value(vd);
    return (v.valid() && v.is_list()) ? v.as_list().size() : 0;
}

size_t op_bundle_size(const ViewData& vd) {
    const TSMeta* current = meta_at_path(vd.meta, vd.path.indices);
    if (current == nullptr || !dispatch_meta_is_tsb(current)) {
        return 0;
    }
    return current->field_count();
}
View op_observer(const ViewData& vd) {
    auto* observer_root = static_cast<const Value*>(vd.observer_data);
    if (observer_root == nullptr || !observer_root->has_value()) {
        return {};
    }

    const auto observer_path = ts_path_to_observer_path(vd.meta, vd.path.indices);
    std::optional<View> maybe_observer;
    if (observer_path.empty()) {
        maybe_observer = observer_root->view();
    } else {
        maybe_observer = navigate_const(observer_root->view(), observer_path);
    }
    if (!maybe_observer.has_value() || !maybe_observer->valid()) {
        return {};
    }

    // Observer nodes for containers are tuple[container_observer, children...].
    if (maybe_observer->is_tuple()) {
        auto tuple = maybe_observer->as_tuple();
        if (tuple.size() > 0) {
            return tuple.at(0);
        }
    }

    return *maybe_observer;
}

void op_notify_observers(ViewData& vd, engine_time_t current_time) {
    notify_link_target_observers(vd, current_time);
}

void store_to_link_target(LinkTarget& target, const ViewData& source) {
    target.bind(source);
}

void store_to_ref_link(REFLink& target, const ViewData& source) {
    const TSMeta* source_meta = meta_at_path(source.meta, source.path.indices);
    if (dispatch_meta_is_ref(source_meta)) {
        target.bind_to_ref(source);
    } else {
        target.bind(source);
    }
}

bool resolve_direct_bound_view_data(const ViewData& source, ViewData& out) {
    if (auto rebound = resolve_bound_view_data(source); rebound.has_value()) {
        out = std::move(*rebound);
        bind_view_data_ops(out);
        return true;
    }
    out = source;
    bind_view_data_ops(out);
    return false;
}

bool resolve_bound_target_view_data(const ViewData& source, ViewData& out) {
    out = source;
    bind_view_data_ops(out);
    bool followed = false;

    for (size_t depth = 0; depth < 64; ++depth) {
        auto rebound = resolve_bound_view_data(out);
        if (!rebound.has_value()) {
            return followed;
        }

        const ViewData next = std::move(rebound.value());
        if (is_same_view_data(next, out)) {
            return false;
        }

        out = next;
        bind_view_data_ops(out);
        followed = true;
    }

    return false;
}

bool resolve_previous_bound_target_view_data(const ViewData& source, ViewData& out) {
    out = source;
    bind_view_data_ops(out);
    if (!source.uses_link_target) {
        return false;
    }

    if (LinkTarget* target = resolve_link_target(source, source.path.indices);
        target != nullptr && target->has_previous_target) {
        out = target->previous_view_data(source.sampled);
        out.projection = merge_projection(source.projection, out.projection);
        bind_view_data_ops(out);
        return true;
    }

    if (!source.path.indices.empty()) {
        for (size_t depth = source.path.indices.size(); depth > 0; --depth) {
            const std::vector<size_t> parent_path(
                source.path.indices.begin(),
                source.path.indices.begin() + static_cast<std::ptrdiff_t>(depth - 1));
            LinkTarget* parent = resolve_link_target(source, parent_path);
            if (parent == nullptr || !parent->has_previous_target) {
                continue;
            }

            ViewData previous = parent->previous_view_data(source.sampled);
            previous.projection = merge_projection(source.projection, previous.projection);

            const std::vector<size_t> residual(
                source.path.indices.begin() + static_cast<std::ptrdiff_t>(parent_path.size()),
                source.path.indices.end());
            if (!residual.empty()) {
                ViewData local_parent = source;
                local_parent.path.indices = parent_path;
                if (auto mapped = remap_residual_indices_for_bound_view(local_parent, previous, residual); mapped.has_value()) {
                    previous.path.indices.insert(previous.path.indices.end(), mapped->begin(), mapped->end());
                } else {
                    previous.path.indices.insert(previous.path.indices.end(), residual.begin(), residual.end());
                }
            }

            out = std::move(previous);
            bind_view_data_ops(out);
            return true;
        }
    }

    return false;
}

std::optional<TSView> resolve_tsd_removed_child_snapshot(const ViewData& parent_view,
                                                         const value::View& key,
                                                         engine_time_t current_time) {
    if (!key.valid() ||
        current_time == MIN_DT ||
        parent_view.value_data == nullptr) {
        return std::nullopt;
    }

    std::vector<ViewData> lookup_views;
    lookup_views.push_back(parent_view);

    const TSMeta* expected_parent_meta = meta_at_path(parent_view.meta, parent_view.path.indices);
    const TSMeta* expected_child_meta =
        dispatch_meta_is_tsd(expected_parent_meta)
            ? expected_parent_meta->element_ts()
            : nullptr;

    ViewData bound_parent{};
    if (resolve_bound_target_view_data(parent_view, bound_parent) &&
        !same_view_identity(bound_parent, parent_view)) {
        lookup_views.push_back(bound_parent);
    }

    for (const ViewData& lookup_view : lookup_views) {
        if (lookup_view.link_observer_registry == nullptr) {
            continue;
        }

        std::shared_ptr<void> existing =
            lookup_view.link_observer_registry->feature_state(k_tsd_removed_snapshot_state_key);
        if (!existing) {
            continue;
        }

        auto state = std::static_pointer_cast<TsdRemovedChildSnapshotState>(std::move(existing));
        auto by_parent = state->entries.find(lookup_view.value_data);
        if (by_parent == state->entries.end()) {
            continue;
        }

        auto& records = by_parent->second;
        records.erase(
            std::remove_if(
                records.begin(),
                records.end(),
                [current_time](const TsdRemovedChildSnapshotRecord& record) {
                    return record.time < current_time;
                }),
            records.end());
        if (records.empty()) {
            state->entries.erase(by_parent);
            continue;
        }

        for (const auto& record : records) {
            if (record.time != current_time ||
                record.parent_path != lookup_view.path.indices ||
                !key_matches_relaxed(record.key.view(), key) ||
                !record.snapshot) {
                continue;
            }

            TSView view = record.snapshot->ts_view(parent_view.engine_time_ptr);
            if (expected_child_meta != nullptr) {
                const TSMeta* snapshot_meta = view.ts_meta();
                if (snapshot_meta == nullptr || dispatch_meta_ops(snapshot_meta) != dispatch_meta_ops(expected_child_meta)) {
                    if (dispatch_meta_is_ref(snapshot_meta)) {
                        ViewData resolved_snapshot{};
                        if (resolve_bound_target_view_data(view.view_data(), resolved_snapshot)) {
                            const TSMeta* resolved_meta =
                                meta_at_path(resolved_snapshot.meta, resolved_snapshot.path.indices);
                            if (resolved_meta != nullptr && dispatch_meta_ops(resolved_meta) == dispatch_meta_ops(expected_child_meta)) {
                                TSView resolved_view(resolved_snapshot, parent_view.engine_time_ptr);
                                resolved_view.view_data().sampled = true;
                                return resolved_view;
                            }
                        }
                    }
                    continue;
                }
            }

            view.view_data().sampled = true;
            return view;
        }
    }

    return std::nullopt;
}

void copy_view_data_value(ViewData& dst, const ViewData& src, engine_time_t current_time) {
    copy_view_data_value_impl(dst, src, current_time);
}

void notify_ts_link_observers(const ViewData& target_view, engine_time_t current_time) {
    notify_link_target_observers(target_view, current_time);
}

void register_ts_link_observer(LinkTarget& observer) {
    register_link_target_observer(observer);
}

void unregister_ts_link_observer(LinkTarget& observer) {
    unregister_link_target_observer(observer);
}

void register_ts_ref_link_observer(REFLink& observer) {
    register_ref_link_observer(observer);
}

void unregister_ts_ref_link_observer(REFLink& observer) {
    unregister_ref_link_observer(observer);
}

void register_ts_active_link_observer(LinkTarget& observer) {
    register_active_link_target_observer(observer);
}

void unregister_ts_active_link_observer(LinkTarget& observer) {
    unregister_active_link_target_observer(observer);
}

void register_ts_active_ref_link_observer(REFLink& observer) {
    register_active_ref_link_observer(observer);
}

void unregister_ts_active_ref_link_observer(REFLink& observer) {
    unregister_active_ref_link_observer(observer);
}

void reset_ts_link_observers() {
    // Registries are endpoint-owned and do not require process-global cleanup.
}




}  // namespace hgraph
