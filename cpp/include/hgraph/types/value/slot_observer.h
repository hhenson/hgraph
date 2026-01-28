#pragma once

/**
 * @file slot_observer.h
 * @brief SlotObserver protocol for parallel arrays synchronized with KeySet.
 *
 * SlotObserver provides an extension point for data structures that need to
 * maintain parallel storage alongside a KeySet. Observers are notified of
 * capacity changes, insertions, erasures, and updates, allowing them to keep
 * their own storage in sync.
 *
 * Use cases:
 * - ValueArray: Maintains parallel value storage for Map types
 * - DeltaTracker: Tracks add/remove/update operations for delta propagation
 */

#include <cstddef>
#include <vector>

namespace hgraph::value {

/**
 * @brief Observer interface for KeySet slot operations.
 *
 * Implementers receive notifications when:
 * - Capacity changes (needs to resize parallel storage)
 * - A slot is inserted (needs to construct/initialize parallel data)
 * - A slot is erased (needs to destroy parallel data)
 * - A slot's value is updated (for maps - value changed for existing key)
 * - All slots are cleared (needs to reset parallel storage)
 */
struct SlotObserver {
    virtual ~SlotObserver() = default;

    /**
     * @brief Called when KeySet capacity changes.
     *
     * Observers should resize their parallel storage to match.
     * Note: This is called BEFORE elements are moved during reallocation.
     *
     * @param old_cap Previous capacity
     * @param new_cap New capacity
     */
    virtual void on_capacity(size_t old_cap, size_t new_cap) = 0;

    /**
     * @brief Called after a new key is inserted at a slot.
     *
     * Observers should construct/initialize their parallel data at this slot.
     * The slot is guaranteed to be valid and within current capacity.
     *
     * @param slot The slot index where insertion occurred
     */
    virtual void on_insert(size_t slot) = 0;

    /**
     * @brief Called before a key is erased from a slot.
     *
     * Observers should destroy their parallel data at this slot.
     * Note: With stable slot storage, keys never move - this is a simple erase.
     *
     * @param slot The slot index being erased
     */
    virtual void on_erase(size_t slot) = 0;

    /**
     * @brief Called when a value is updated at a slot (map-specific).
     *
     * For sets, this is never called. For maps, this is called when
     * set_item() updates the value for an existing key.
     * Default implementation does nothing.
     *
     * @param slot The slot index where the value was updated
     */
    virtual void on_update(size_t slot) { (void)slot; }

    /**
     * @brief Called when all keys are cleared.
     *
     * Observers should destroy all parallel data and reset state.
     * Capacity may or may not change after this call.
     */
    virtual void on_clear() = 0;
};

/**
 * @brief Lightweight wrapper for observer list with dispatch methods.
 *
 * ObserverDispatcher provides a convenient way to manage and notify
 * SlotObservers. It can be used by both KeySet (for sets) and MapStorage
 * (for maps with value update notifications).
 */
class ObserverDispatcher {
public:
    ObserverDispatcher() = default;

    // Non-copyable, movable
    ObserverDispatcher(const ObserverDispatcher&) = delete;
    ObserverDispatcher& operator=(const ObserverDispatcher&) = delete;
    ObserverDispatcher(ObserverDispatcher&&) noexcept = default;
    ObserverDispatcher& operator=(ObserverDispatcher&&) noexcept = default;

    /**
     * @brief Register an observer.
     */
    void add_observer(SlotObserver* observer) {
        if (observer) {
            observers_.push_back(observer);
        }
    }

    /**
     * @brief Unregister an observer.
     */
    void remove_observer(SlotObserver* observer) {
        observers_.erase(
            std::remove(observers_.begin(), observers_.end(), observer),
            observers_.end());
    }

    /**
     * @brief Get the observer list (for iteration/access).
     */
    [[nodiscard]] const std::vector<SlotObserver*>& observers() const {
        return observers_;
    }

    // ========== Dispatch Methods ==========

    void notify_capacity(size_t old_cap, size_t new_cap) {
        for (auto* obs : observers_) {
            obs->on_capacity(old_cap, new_cap);
        }
    }

    void notify_insert(size_t slot) {
        for (auto* obs : observers_) {
            obs->on_insert(slot);
        }
    }

    void notify_erase(size_t slot) {
        for (auto* obs : observers_) {
            obs->on_erase(slot);
        }
    }

    void notify_update(size_t slot) {
        for (auto* obs : observers_) {
            obs->on_update(slot);
        }
    }

    void notify_clear() {
        for (auto* obs : observers_) {
            obs->on_clear();
        }
    }

private:
    std::vector<SlotObserver*> observers_;
};

} // namespace hgraph::value
