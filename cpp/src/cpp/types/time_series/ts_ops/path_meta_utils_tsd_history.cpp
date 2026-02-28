#include "ts_ops_internal.h"

namespace hgraph {

std::shared_ptr<TsdRemovedChildSnapshotState> ensure_tsd_removed_snapshot_state(TSLinkObserverRegistry* registry) {
    if (registry == nullptr) {
        return {};
    }

    std::shared_ptr<void> existing = registry->feature_state(k_tsd_removed_snapshot_state_key);
    if (existing) {
        return std::static_pointer_cast<TsdRemovedChildSnapshotState>(std::move(existing));
    }

    auto state = std::make_shared<TsdRemovedChildSnapshotState>();
    registry->set_feature_state(std::string{k_tsd_removed_snapshot_state_key}, state);
    return state;
}

std::shared_ptr<TsdVisibleKeyHistoryState> ensure_tsd_visible_key_history_state(TSLinkObserverRegistry* registry) {
    if (registry == nullptr) {
        return {};
    }

    std::shared_ptr<void> existing = registry->feature_state(k_tsd_visible_key_history_state_key);
    if (existing) {
        return std::static_pointer_cast<TsdVisibleKeyHistoryState>(std::move(existing));
    }

    auto state = std::make_shared<TsdVisibleKeyHistoryState>();
    registry->set_feature_state(std::string{k_tsd_visible_key_history_state_key}, state);
    return state;
}

bool key_matches_relaxed(const value::View& lhs, const value::View& rhs) {
    if (!lhs.valid() || !rhs.valid()) {
        return false;
    }
    if (lhs.schema() != rhs.schema()) {
        return false;
    }
    return lhs.equals(rhs);
}

void record_tsd_removed_child_snapshot(const ViewData& parent_view,
                                       const View& key,
                                       const ViewData& child_view,
                                       engine_time_t current_time) {
    if (!key.valid() ||
        current_time == MIN_DT ||
        parent_view.link_observer_registry == nullptr ||
        parent_view.value_data == nullptr) {
        return;
    }

    const TSMeta* child_meta = meta_at_path(child_view.meta, child_view.path.indices);
    if (child_meta == nullptr) {
        return;
    }

    auto state = ensure_tsd_removed_snapshot_state(parent_view.link_observer_registry);
    if (!state) {
        return;
    }

    auto& records = state->entries[parent_view.value_data];
    records.erase(
        std::remove_if(
            records.begin(),
            records.end(),
            [&](const TsdRemovedChildSnapshotRecord& record) {
                if (record.time != current_time) {
                    return true;
                }
                return record.parent_path == parent_view.path.indices &&
                       key_matches_relaxed(record.key.view(), key);
            }),
        records.end());

    auto snapshot = std::make_shared<TSValue>(child_meta);
    ViewData snapshot_view = snapshot->make_view_data({}, parent_view.engine_time_ptr);
    copy_view_data_value(snapshot_view, child_view, current_time);

    TsdRemovedChildSnapshotRecord record{};
    record.parent_path = parent_view.path.indices;
    record.time = current_time;
    record.key = key.clone();
    record.snapshot = std::move(snapshot);
    records.push_back(std::move(record));
}

void mark_tsd_visible_key_history(const ViewData& parent_view, const value::View& key, engine_time_t current_time) {
    if (!key.valid() ||
        current_time == MIN_DT ||
        parent_view.link_observer_registry == nullptr ||
        parent_view.value_data == nullptr) {
        return;
    }

    auto state = ensure_tsd_visible_key_history_state(parent_view.link_observer_registry);
    if (!state) {
        return;
    }

    auto& records = state->entries[parent_view.value_data];
    for (auto& record : records) {
        if (record.parent_path == parent_view.path.indices &&
            key_matches_relaxed(record.key.view(), key)) {
            record.last_seen = current_time;
            return;
        }
    }

    TsdVisibleKeyHistoryRecord record{};
    record.parent_path = parent_view.path.indices;
    record.key = key.clone();
    record.last_seen = current_time;
    records.push_back(std::move(record));
}

bool has_tsd_visible_key_history(const ViewData& parent_view, const value::View& key) {
    if (!key.valid() || parent_view.value_data == nullptr) {
        return false;
    }

    std::vector<ViewData> lookup_views;
    lookup_views.push_back(parent_view);

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
            lookup_view.link_observer_registry->feature_state(k_tsd_visible_key_history_state_key);
        if (!existing) {
            continue;
        }

        auto state = std::static_pointer_cast<TsdVisibleKeyHistoryState>(std::move(existing));
        auto it = state->entries.find(lookup_view.value_data);
        if (it == state->entries.end()) {
            continue;
        }

        for (const auto& record : it->second) {
            if (record.parent_path == lookup_view.path.indices &&
                key_matches_relaxed(record.key.view(), key) &&
                record.last_seen > MIN_DT) {
                return true;
            }
        }
    }

    return false;
}

void clear_tsd_visible_key_history(const ViewData& parent_view, const value::View& key) {
    if (!key.valid() || parent_view.value_data == nullptr) {
        return;
    }

    std::vector<ViewData> lookup_views;
    lookup_views.push_back(parent_view);

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
            lookup_view.link_observer_registry->feature_state(k_tsd_visible_key_history_state_key);
        if (!existing) {
            continue;
        }

        auto state = std::static_pointer_cast<TsdVisibleKeyHistoryState>(std::move(existing));
        auto it = state->entries.find(lookup_view.value_data);
        if (it == state->entries.end()) {
            continue;
        }

        auto& records = it->second;
        records.erase(
            std::remove_if(
                records.begin(),
                records.end(),
                [&](const TsdVisibleKeyHistoryRecord& record) {
                    return record.parent_path == lookup_view.path.indices &&
                           key_matches_relaxed(record.key.view(), key);
                }),
            records.end());
        if (records.empty()) {
            state->entries.erase(it);
        }
    }
}



}  // namespace hgraph
