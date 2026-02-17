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
#include <cstdio>
#include <typeinfo>
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
    static constexpr uint32_t ALIVE_SENTINEL = 0xFEED'FACE;

    ObserverList() = default;

    // Copyable and movable (preserve sentinel)
    ObserverList(const ObserverList& o) : observers_(o.observers_), trace_(o.trace_) {}
    ObserverList(ObserverList&& o) noexcept : observers_(std::move(o.observers_)), trace_(o.trace_) {}
    ObserverList& operator=(const ObserverList& o) { observers_ = o.observers_; trace_ = o.trace_; return *this; }
    ObserverList& operator=(ObserverList&& o) noexcept { observers_ = std::move(o.observers_); trace_ = o.trace_; return *this; }

    ~ObserverList() { sentinel_ = 0xDEAD'0B5E; }

    [[nodiscard]] bool is_alive() const { return sentinel_ == ALIVE_SENTINEL; }

    // ========== Observer Management ==========

    /**
     * @brief Add an observer to the list.
     * @param obs The observer to add (caller retains ownership)
     * @note Adding the same observer multiple times results in multiple notifications
     */
    void add_observer(Notifiable* obs) {
        if (obs) {
            if (!is_alive()) {
                fprintf(stderr, "[OBSERVER] add_observer to DEAD ObserverList %p sentinel=0x%08X\n",
                        (void*)this, sentinel_);
                return;  // Skip subscription to freed observer list
            }
            observers_.push_back(obs);
            if (trace_) {
                fprintf(stderr, "[OBS_TRACE] add_observer(%p) to list %p now size=%zu\n",
                        (void*)obs, (void*)this, observers_.size());
            }
        }
    }

    void set_trace(bool enable) { trace_ = enable; }

    /**
     * @brief Remove an observer from the list.
     * @param obs The observer to remove
     * @note If the observer was added multiple times, only the first instance is removed
     */
    void remove_observer(Notifiable* obs) {
        if (!is_alive()) {
            return;  // Skip removal from freed observer list
        }
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
        for (size_t i = 0; i < observers_.size(); ++i) {
            auto* obs = observers_[i];
            if (!obs || !obs->is_alive()) {
                // Remove dangling/null observer to prevent future issues
                observers_.erase(observers_.begin() + static_cast<ptrdiff_t>(i));
                --i;
                continue;
            }
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
    uint32_t sentinel_ = ALIVE_SENTINEL;
    std::vector<Notifiable*> observers_;
    bool trace_ = false;
};

} // namespace hgraph
