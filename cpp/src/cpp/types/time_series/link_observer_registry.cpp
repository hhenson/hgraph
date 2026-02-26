#include <hgraph/types/time_series/link_observer_registry.h>

#include <unordered_set>

namespace hgraph {
namespace {

std::unordered_set<const TSLinkObserverRegistry*>& live_registries() {
    static std::unordered_set<const TSLinkObserverRegistry*> pointers;
    return pointers;
}

void track_live_registry(const TSLinkObserverRegistry* registry, bool live) {
    if (live) {
        live_registries().insert(registry);
    } else {
        live_registries().erase(registry);
    }
}

}  // namespace

TSLinkObserverRegistry::TSLinkObserverRegistry() {
    track_live_registry(this, true);
}

TSLinkObserverRegistry::~TSLinkObserverRegistry() {
    track_live_registry(this, false);
}

std::shared_ptr<void>* TSLinkObserverRegistry::known_state_slot(std::string_view key) noexcept {
    if (key == kTsdRemovedChildSnapshotsKey) {
        return &tsd_removed_child_snapshots_state_;
    }
    if (key == kTsdVisibleKeyHistoryKey) {
        return &tsd_visible_key_history_state_;
    }
    if (key == kRefUnboundItemChangesKey) {
        return &ref_unbound_item_changes_state_;
    }
    return nullptr;
}

const std::shared_ptr<void>* TSLinkObserverRegistry::known_state_slot(std::string_view key) const noexcept {
    if (key == kTsdRemovedChildSnapshotsKey) {
        return &tsd_removed_child_snapshots_state_;
    }
    if (key == kTsdVisibleKeyHistoryKey) {
        return &tsd_visible_key_history_state_;
    }
    if (key == kRefUnboundItemChangesKey) {
        return &ref_unbound_item_changes_state_;
    }
    return nullptr;
}

std::shared_ptr<void> TSLinkObserverRegistry::feature_state(std::string_view key) const {
    if (const auto* slot = known_state_slot(key); slot != nullptr) {
        return *slot;
    }
    if (extra_feature_states_ == nullptr) {
        return {};
    }
    const auto it = extra_feature_states_->find(std::string{key});
    return it == extra_feature_states_->end() ? std::shared_ptr<void>{} : it->second;
}

void TSLinkObserverRegistry::set_feature_state(std::string key, std::shared_ptr<void> state) {
    if (auto* slot = known_state_slot(key); slot != nullptr) {
        *slot = std::move(state);
        return;
    }

    if (!state) {
        if (extra_feature_states_ == nullptr) {
            return;
        }
        extra_feature_states_->erase(key);
        if (extra_feature_states_->empty()) {
            extra_feature_states_.reset();
        }
        return;
    }

    if (extra_feature_states_ == nullptr) {
        extra_feature_states_ = std::make_unique<std::unordered_map<std::string, std::shared_ptr<void>>>();
    }
    (*extra_feature_states_)[std::move(key)] = std::move(state);
}

void TSLinkObserverRegistry::clear_feature_state(std::string_view key) {
    if (auto* slot = known_state_slot(key); slot != nullptr) {
        slot->reset();
        return;
    }

    if (extra_feature_states_ == nullptr) {
        return;
    }
    extra_feature_states_->erase(std::string{key});
    if (extra_feature_states_->empty()) {
        extra_feature_states_.reset();
    }
}

void TSLinkObserverRegistry::clear() {
    tsd_removed_child_snapshots_state_.reset();
    tsd_visible_key_history_state_.reset();
    ref_unbound_item_changes_state_.reset();
    extra_feature_states_.reset();
}

bool is_live_link_observer_registry(const TSLinkObserverRegistry* registry) noexcept {
    if (registry == nullptr) {
        return false;
    }
    return live_registries().contains(registry);
}

}  // namespace hgraph
