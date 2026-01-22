#pragma once

/**
 * @file slot_observer.h
 * @brief SlotObserver protocol for parallel arrays synchronized with KeySet.
 *
 * SlotObserver provides an extension point for data structures that need to
 * maintain parallel storage alongside a KeySet. Observers are notified of
 * capacity changes, insertions, and erasures, allowing them to keep their
 * own storage in sync.
 *
 * Use cases:
 * - ValueArray: Maintains parallel value storage for Map types
 * - DeltaTracker: Tracks add/remove operations for delta propagation
 */

#include <cstddef>

namespace hgraph::value {

/**
 * @brief Observer interface for KeySet slot operations.
 *
 * Implementers receive notifications when the KeySet:
 * - Changes capacity (needs to resize parallel storage)
 * - Inserts at a slot (needs to construct/initialize parallel data)
 * - Erases a slot (needs to destroy parallel data)
 * - Clears all slots (needs to reset parallel storage)
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
     * Note: For swap-with-last erasure, the observer should also handle
     * moving data from the last slot to this slot if needed.
     *
     * @param slot The slot index being erased
     * @param last_slot The last valid slot (for swap-with-last, or same as slot if last)
     */
    virtual void on_erase(size_t slot, size_t last_slot) = 0;

    /**
     * @brief Called when all keys are cleared.
     *
     * Observers should destroy all parallel data and reset state.
     * Capacity may or may not change after this call.
     */
    virtual void on_clear() = 0;
};

} // namespace hgraph::value
