#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>

#include <algorithm>
#include <vector>

namespace hgraph {

/**
 * Lightweight observer container used by TS observer schema leaves.
 *
 * Stores non-owning Notifiable pointers and enforces uniqueness.
 */
struct HGRAPH_EXPORT ObserverList {
    std::vector<Notifiable*> observers{};

    void subscribe(Notifiable* observer) {
        if (observer == nullptr) {
            return;
        }
        if (std::find(observers.begin(), observers.end(), observer) == observers.end()) {
            observers.push_back(observer);
        }
    }

    void unsubscribe(Notifiable* observer) {
        observers.erase(
            std::remove(observers.begin(), observers.end(), observer),
            observers.end());
    }

    [[nodiscard]] bool contains(Notifiable* observer) const {
        return std::find(observers.begin(), observers.end(), observer) != observers.end();
    }
};

}  // namespace hgraph
