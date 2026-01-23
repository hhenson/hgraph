#pragma once

/**
 * @file observer_array.h
 * @brief ObserverArray - Parallel observer lists synchronized with KeySet.
 *
 * ObserverArray provides parallel observer list storage for time-series
 * collections (TSD). It implements the SlotObserver protocol to stay
 * synchronized with a KeySet, maintaining per-slot observer lists.
 *
 * Key design principles:
 * - Implements SlotObserver for automatic synchronization
 * - Each slot has its own independent ObserverList
 * - On erase, observers are notified of removal before clearing
 * - Enables fine-grained per-element subscription
 */

#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/value/slot_observer.h>

#include <cstddef>
#include <vector>

namespace hgraph {

/**
 * @brief Parallel observer lists synchronized with KeySet.
 *
 * ObserverArray maintains a vector of ObserverLists parallel to a KeySet's
 * slot storage. Each slot has an associated ObserverList for fine-grained
 * subscription to element modifications.
 *
 * SlotObserver protocol:
 * - on_capacity: Resizes storage, new slots get empty ObserverLists
 * - on_insert: Clears the slot's ObserverList (fresh start)
 * - on_erase: Notifies observers of removal, then clears the list
 * - on_update: No-op (notifications done via ObserverList directly)
 * - on_clear: Notifies all observers of removal, then clears all lists
 */
class ObserverArray : public value::SlotObserver {
public:
    ObserverArray() = default;

    // Non-copyable, movable
    ObserverArray(const ObserverArray&) = delete;
    ObserverArray& operator=(const ObserverArray&) = delete;
    ObserverArray(ObserverArray&&) noexcept = default;
    ObserverArray& operator=(ObserverArray&&) noexcept = default;

    ~ObserverArray() override = default;

    // ========== SlotObserver Implementation ==========

    /**
     * @brief Called when KeySet capacity changes.
     *
     * Resizes the observer storage to match the new capacity.
     * New slots get empty ObserverLists.
     *
     * @param old_cap Previous capacity
     * @param new_cap New capacity
     */
    void on_capacity(size_t old_cap, size_t new_cap) override {
        (void)old_cap;  // Unused
        observers_.resize(new_cap);
    }

    /**
     * @brief Called after a new key is inserted at a slot.
     *
     * Clears the slot's ObserverList to ensure a fresh start.
     *
     * @param slot The slot index where insertion occurred
     */
    void on_insert(size_t slot) override {
        if (slot < observers_.size()) {
            observers_[slot].clear();
        }
        ++size_;
    }

    /**
     * @brief Called before a key is erased from a slot.
     *
     * Notifies all observers of the removal, then clears the list.
     *
     * @param slot The slot index being erased
     */
    void on_erase(size_t slot) override {
        if (slot < observers_.size()) {
            observers_[slot].notify_removed();
            observers_[slot].clear();
        }
        --size_;
    }

    /**
     * @brief Called when a value is updated at a slot.
     *
     * No-op for ObserverArray - notifications are done via the
     * ObserverList directly when values change.
     *
     * @param slot The slot index where the value was updated
     */
    void on_update(size_t slot) override {
        (void)slot;  // No-op
    }

    /**
     * @brief Called when all keys are cleared.
     *
     * Notifies all observers of removal, then clears all lists.
     */
    void on_clear() override {
        for (auto& obs_list : observers_) {
            obs_list.notify_removed();
            obs_list.clear();
        }
        size_ = 0;
    }

    // ========== Observer Access ==========

    /**
     * @brief Get the ObserverList at a slot.
     * @param slot The slot index
     * @return Reference to the ObserverList at the slot
     */
    [[nodiscard]] ObserverList& at(size_t slot) {
        return observers_[slot];
    }

    /**
     * @brief Get the const ObserverList at a slot.
     * @param slot The slot index
     * @return Const reference to the ObserverList at the slot
     */
    [[nodiscard]] const ObserverList& at(size_t slot) const {
        return observers_[slot];
    }

    /**
     * @brief Get the number of active slots (not capacity).
     */
    [[nodiscard]] size_t size() const {
        return size_;
    }

    /**
     * @brief Get the capacity (total slots including inactive).
     */
    [[nodiscard]] size_t capacity() const {
        return observers_.size();
    }

private:
    std::vector<ObserverList> observers_;  // Parallel observer list storage
    size_t size_{0};                       // Number of active slots
};

} // namespace hgraph
