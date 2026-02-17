#pragma once

#include <hgraph/hgraph_base.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace hgraph {

struct LinkTarget;
struct REFLink;

struct LinkObserverRegistration {
    std::vector<size_t> path;
    LinkTarget* link_target{nullptr};
};

struct REFLinkObserverRegistration {
    std::vector<size_t> path;
    REFLink* ref_link{nullptr};
};

/**
 * Endpoint-owned registry used by TS link observers.
 *
 * This registry is intentionally not process-global; it is owned by TSInput /
 * TSOutput instances and accessed through ViewData.
 */
struct HGRAPH_EXPORT TSLinkObserverRegistry {
    std::unordered_map<void*, std::vector<LinkObserverRegistration>> entries;
    std::unordered_map<void*, std::vector<REFLinkObserverRegistration>> ref_entries;
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
        entries.clear();
        ref_entries.clear();
        feature_states.clear();
    }
};

}  // namespace hgraph
