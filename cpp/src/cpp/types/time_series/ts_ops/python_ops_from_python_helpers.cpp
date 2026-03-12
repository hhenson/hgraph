#include "ts_ops_internal.h"

namespace hgraph {

void notify_if_static_container_children_changed(bool changed,
                                                 const ViewData& vd,
                                                 engine_time_t current_time) {
    if (!changed) {
        return;
    }

    // Child writes already stamp their own paths (and ancestors). Re-stamping
    // the static container root would mark untouched siblings modified.
    notify_link_target_observers(vd, current_time);
}
void prune_ref_unbound_item_change_state(RefUnboundItemChangeState& state, engine_time_t current_time) {
    if (current_time == MIN_DT) {
        return;
    }

    for (auto it = state.entries.begin(); it != state.entries.end();) {
        auto& records = it->second;
        records.erase(
            std::remove_if(
                records.begin(),
                records.end(),
                [current_time](const RefUnboundItemChangeRecord& record) {
                    return record.time < current_time;
                }),
            records.end());

        if (records.empty()) {
            it = state.entries.erase(it);
        } else {
            ++it;
        }
    }
}

std::shared_ptr<RefUnboundItemChangeState> ensure_ref_unbound_item_change_state(TSLinkObserverRegistry* registry) {
    if (registry == nullptr) {
        return {};
    }

    std::shared_ptr<void> existing = registry->feature_state(k_ref_unbound_item_change_state_key);
    if (existing) {
        return std::static_pointer_cast<RefUnboundItemChangeState>(std::move(existing));
    }

    auto state = std::make_shared<RefUnboundItemChangeState>();
    registry->set_feature_state(std::string{k_ref_unbound_item_change_state_key}, state);
    return state;
}

void record_unbound_ref_item_changes(const ViewData& source,
                                     const std::vector<size_t>& changed_indices,
                                     engine_time_t current_time) {
    if (changed_indices.empty() ||
        source.link_observer_registry == nullptr ||
        source.value_data == nullptr ||
        current_time == MIN_DT) {
        return;
    }

    auto state = ensure_ref_unbound_item_change_state(source.link_observer_registry);
    if (!state) {
        return;
    }
    prune_ref_unbound_item_change_state(*state, current_time);

    auto& records = state->entries[source.value_data];
    const auto existing = std::find_if(
        records.begin(),
        records.end(),
        [&source](const RefUnboundItemChangeRecord& record) {
            return record.path == source.path_indices();
        });

    if (existing != records.end()) {
        existing->time = current_time;
        existing->changed_indices = changed_indices;
        return;
    }

    RefUnboundItemChangeRecord record{};
    record.path = source.path_indices();
    record.time = current_time;
    record.changed_indices = changed_indices;
    records.push_back(std::move(record));
}

bool unbound_ref_item_changed_this_tick(const ViewData& item_view, size_t item_index, engine_time_t current_time) {
    if (item_view.path_depth() == 0 ||
        item_view.link_observer_registry == nullptr ||
        item_view.value_data == nullptr ||
        current_time == MIN_DT) {
        return false;
    }

    std::shared_ptr<void> existing = item_view.link_observer_registry->feature_state(k_ref_unbound_item_change_state_key);
    if (!existing) {
        return false;
    }

    auto state = std::static_pointer_cast<RefUnboundItemChangeState>(std::move(existing));
    prune_ref_unbound_item_change_state(*state, current_time);
    const auto by_value = state->entries.find(item_view.value_data);
    if (by_value == state->entries.end()) {
        return false;
    }

    std::vector<size_t> parent_path = item_view.path_indices();
    parent_path.pop_back();
    const auto record = std::find_if(
        by_value->second.begin(),
        by_value->second.end(),
        [&parent_path, current_time](const RefUnboundItemChangeRecord& value) {
            return value.time == current_time && value.path == parent_path;
        });
    if (record == by_value->second.end()) {
        return false;
    }

    return std::find(record->changed_indices.begin(), record->changed_indices.end(), item_index) !=
           record->changed_indices.end();
}

}  // namespace hgraph
