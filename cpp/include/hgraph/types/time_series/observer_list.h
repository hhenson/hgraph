#pragma once

/**
 * @file observer_list.h
 * @brief ObserverList - List of observers for time-series element notifications.
 *
 * ObserverList is the atomic unit for the observer schema. Each time-series
 * element/field can have its own ObserverList for fine-grained subscription.
 * Observers are notified when the element is modified.
 *
 * Use cases:
 * - Scalar time-series: Single ObserverList for value changes
 * - Bundle fields: Per-field ObserverList for fine-grained subscriptions
 * - Collection elements: Per-slot ObserverList for element-level notifications
 */

#include <hgraph/types/notifiable.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <vector>

namespace hgraph {

/**
 * @brief List of observers for a time-series element.
 *
 * ObserverList is the atomic unit for the observer schema. Each time-series
 * element/field can have its own ObserverList for fine-grained subscription.
 *
 * Key characteristics:
 * - Maintains a list of Notifiable pointers (non-owning)
 * - Supports add/remove of observers
 * - Notifies all observers on modification
 * - Safe to notify on empty list (no-op)
 */
class ObserverList {
public:
    ObserverList() = default;

    // Copyable and movable
    ObserverList(const ObserverList&) = default;
    ObserverList(ObserverList&&) noexcept = default;
    ObserverList& operator=(const ObserverList&) = default;
    ObserverList& operator=(ObserverList&&) noexcept = default;

    ~ObserverList() = default;

    // ========== Observer Management ==========

    /**
     * @brief Add an observer to the list.
     * @param obs The observer to add (caller retains ownership)
     * @note Adding the same observer multiple times results in multiple notifications
     */
    void add_observer(Notifiable* obs) {
        if (obs) {
            observers_.push_back(obs);
        }
    }

    /**
     * @brief Remove an observer from the list.
     * @param obs The observer to remove
     * @note If the observer was added multiple times, only the first instance is removed
     */
    void remove_observer(Notifiable* obs) {
        auto it = std::find(observers_.begin(), observers_.end(), obs);
        if (it != observers_.end()) {
            observers_.erase(it);
        }
    }

    // ========== Notification ==========

    /**
     * @brief Notify all observers of a modification.
     * @param current_time The time at which the modification occurred
     */
    void notify_modified(engine_time_t current_time) {
        for (auto* obs : observers_) {
            obs->notify(current_time);
        }
    }

    // ========== State Management ==========

    /**
     * @brief Clear all observers from the list.
     */
    void clear() {
        observers_.clear();
    }

    // ========== Accessors ==========

    /**
     * @brief Check if the list has no observers.
     */
    [[nodiscard]] bool empty() const { return observers_.empty(); }

    /**
     * @brief Get the number of observers.
     */
    [[nodiscard]] size_t size() const { return observers_.size(); }

private:
    std::vector<Notifiable*> observers_;
};

} // namespace hgraph
