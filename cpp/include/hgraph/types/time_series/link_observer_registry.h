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
    TSLinkObserverRegistry();
    TSLinkObserverRegistry(const TSLinkObserverRegistry&) = delete;
    TSLinkObserverRegistry& operator=(const TSLinkObserverRegistry&) = delete;
    TSLinkObserverRegistry(TSLinkObserverRegistry&&) = delete;
    TSLinkObserverRegistry& operator=(TSLinkObserverRegistry&&) = delete;
    ~TSLinkObserverRegistry();

    std::unordered_map<std::string, std::shared_ptr<void>> feature_states;

    [[nodiscard]] std::shared_ptr<void> feature_state(std::string_view key) const {
        const auto it = feature_states.find(std::string{key});
        if (it == feature_states.end()) {
            return {};
        }
        return it->second;
    }

    void set_feature_state(std::string key, std::shared_ptr<void> state) {
        if (!state) {
            feature_states.erase(key);
            return;
        }
        feature_states[std::move(key)] = std::move(state);
    }

    void clear_feature_state(std::string_view key) {
        feature_states.erase(std::string{key});
    }

    void clear() {
        feature_states.clear();
    }
};

HGRAPH_EXPORT bool is_live_link_observer_registry(const TSLinkObserverRegistry* registry) noexcept;

}  // namespace hgraph
