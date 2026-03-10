#pragma once

#include <hgraph/hgraph_base.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hgraph {

struct LinkTarget;
struct REFLink;

/**
 * Endpoint-owned registry used by TS link observers.
 *
 * This registry is intentionally not process-global; it is owned by TSInput /
 * TSOutput instances and accessed through ViewData.
 */
struct HGRAPH_EXPORT TSLinkObserverRegistry {
    static constexpr std::string_view kTsdRemovedChildSnapshotsKey{"tsd_removed_child_snapshots"};
    static constexpr std::string_view kTsdVisibleKeyHistoryKey{"tsd_visible_key_history"};
    static constexpr std::string_view kRefUnboundItemChangesKey{"ref_unbound_item_changes"};
    static constexpr std::string_view kTsdDeltaTickStateKey{"tsd_delta_tick_state"};

    TSLinkObserverRegistry();
    TSLinkObserverRegistry(const TSLinkObserverRegistry&) = delete;
    TSLinkObserverRegistry& operator=(const TSLinkObserverRegistry&) = delete;
    TSLinkObserverRegistry(TSLinkObserverRegistry&&) = delete;
    TSLinkObserverRegistry& operator=(TSLinkObserverRegistry&&) = delete;
    ~TSLinkObserverRegistry();

    [[nodiscard]] std::shared_ptr<void> feature_state(std::string_view key) const;
    void set_feature_state(std::string key, std::shared_ptr<void> state);
    void clear_feature_state(std::string_view key);
    void clear();

private:
    [[nodiscard]] std::shared_ptr<void>* known_state_slot(std::string_view key) noexcept;
    [[nodiscard]] const std::shared_ptr<void>* known_state_slot(std::string_view key) const noexcept;

    std::shared_ptr<void> tsd_removed_child_snapshots_state_{};
    std::shared_ptr<void> tsd_visible_key_history_state_{};
    std::shared_ptr<void> ref_unbound_item_changes_state_{};
    std::shared_ptr<void> tsd_delta_tick_state_{};
    std::unique_ptr<std::unordered_map<std::string, std::shared_ptr<void>>> extra_feature_states_{};
};

HGRAPH_EXPORT bool is_live_link_observer_registry(const TSLinkObserverRegistry* registry) noexcept;

}  // namespace hgraph
