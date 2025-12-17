//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_OBSERVER_STORAGE_H
#define HGRAPH_VALUE_OBSERVER_STORAGE_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>
#include <unordered_set>
#include <vector>
#include <memory>

namespace hgraph::value {

    /**
     * ObserverStorage - Hierarchical observer storage for TimeSeriesValue
     *
     * Mirrors the type structure to allow subscriptions at any level:
     * - Root level: notified for any change
     * - Field/element/entry level: notified for changes at that specific location
     *
     * Notifications propagate upward: a change at a leaf notifies all ancestors.
     *
     * Memory is lazily allocated:
     * - Children vector only grows when child subscriptions are made
     * - No allocation until first subscribe() call
     */
    class ObserverStorage {
    public:
        ObserverStorage() = default;
        explicit ObserverStorage(const TypeMeta* meta) : _meta(meta) {}

        // Non-copyable, movable
        ObserverStorage(const ObserverStorage&) = delete;
        ObserverStorage& operator=(const ObserverStorage&) = delete;
        ObserverStorage(ObserverStorage&&) noexcept = default;
        ObserverStorage& operator=(ObserverStorage&&) noexcept = default;

        // Schema access
        [[nodiscard]] const TypeMeta* meta() const { return _meta; }
        [[nodiscard]] bool valid() const { return _meta != nullptr; }

        // Parent linkage for upward notification propagation
        void set_parent(ObserverStorage* parent) { _parent = parent; }
        [[nodiscard]] ObserverStorage* parent() const { return _parent; }

        // Subscription management at this level
        void subscribe(hgraph::Notifiable* notifiable) {
            if (notifiable) {
                _subscribers.insert(notifiable);
            }
        }

        void unsubscribe(hgraph::Notifiable* notifiable) {
            _subscribers.erase(notifiable);
        }

        [[nodiscard]] bool has_subscribers() const {
            return !_subscribers.empty();
        }

        [[nodiscard]] size_t subscriber_count() const {
            return _subscribers.size();
        }

        // Notification - notifies this level's subscribers and propagates to parent
        void notify(engine_time_t time) {
            // Notify all subscribers at this level
            for (auto* subscriber : _subscribers) {
                subscriber->notify(time);
            }
            // Propagate to parent (upward notification)
            if (_parent) {
                _parent->notify(time);
            }
        }

        // Child observer storage access (for hierarchical subscriptions)
        // Uses unified index-based approach for all container types:
        // - Bundles: field index
        // - Lists: element index
        // - Dicts: entry index from DictStorage

        [[nodiscard]] ObserverStorage* child(size_t index) {
            if (index >= _children.size()) {
                return nullptr;
            }
            return _children[index].get();
        }

        [[nodiscard]] const ObserverStorage* child(size_t index) const {
            if (index >= _children.size()) {
                return nullptr;
            }
            return _children[index].get();
        }

        // Ensure a child observer storage exists at the given index
        // Creates the storage lazily if it doesn't exist
        ObserverStorage* ensure_child(size_t index, const TypeMeta* child_meta = nullptr) {
            // Grow the children vector if needed
            if (index >= _children.size()) {
                _children.resize(index + 1);
            }

            // Create the child storage if it doesn't exist
            if (!_children[index]) {
                _children[index] = std::make_unique<ObserverStorage>(child_meta);
                _children[index]->set_parent(this);
            }

            return _children[index].get();
        }

        // Get child count (for testing/debugging)
        [[nodiscard]] size_t children_capacity() const {
            return _children.size();
        }

        // Count non-null children (for testing/debugging)
        [[nodiscard]] size_t children_count() const {
            size_t count = 0;
            for (const auto& child : _children) {
                if (child) ++count;
            }
            return count;
        }

    private:
        const TypeMeta* _meta{nullptr};
        ObserverStorage* _parent{nullptr};  // Non-owning parent pointer for upward propagation
        std::unordered_set<hgraph::Notifiable*> _subscribers;
        std::vector<std::unique_ptr<ObserverStorage>> _children;
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_OBSERVER_STORAGE_H
