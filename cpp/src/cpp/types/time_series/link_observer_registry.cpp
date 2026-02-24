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

bool is_live_link_observer_registry(const TSLinkObserverRegistry* registry) noexcept {
    if (registry == nullptr) {
        return false;
    }
    return live_registries().contains(registry);
}

}  // namespace hgraph
