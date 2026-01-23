#pragma once

/**
 * @file set_delta.h
 * @brief SetDelta - Slot-based delta tracking for TSS (Time-Series Set).
 *
 * SetDelta tracks add/remove operations using slot indices for zero-copy
 * delta propagation. It implements the SlotObserver protocol to receive
 * notifications from the underlying KeySet.
 *
 * Key design principles:
 * - Tracks added/removed slot indices (not element copies)
 * - Handles add/remove cancellation within the same tick
 * - Erase then insert records both (slot reuse scenario)
 * - on_clear() sets a cleared flag
 * - clear() resets all state including the cleared flag
 */

#include <hgraph/types/value/slot_observer.h>

#include <algorithm>
#include <cstddef>
#include <vector>

namespace hgraph {

/**
 * @brief Slot-based delta tracking for TSS.
 *
 * SetDelta maintains vectors of added and removed slot indices, allowing
 * efficient delta propagation without copying elements. It implements
 * add/remove cancellation: if a slot is inserted then erased in the same
 * tick, neither operation appears in the delta.
 *
 * SlotObserver protocol:
 * - on_capacity: No-op (delta doesn't need to track capacity)
 * - on_insert: Tracks slot as added (or cancels prior removal)
 * - on_erase: Tracks slot as removed (or cancels prior addition)
 * - on_update: No-op (sets don't have value updates)
 * - on_clear: Sets the cleared flag
 */
class SetDelta : public value::SlotObserver {
public:
    SetDelta() = default;

    // Non-copyable, movable
    SetDelta(const SetDelta&) = delete;
    SetDelta& operator=(const SetDelta&) = delete;
    SetDelta(SetDelta&&) noexcept = default;
    SetDelta& operator=(SetDelta&&) noexcept = default;

    ~SetDelta() override = default;

    // ========== SlotObserver Implementation ==========

    /**
     * @brief Called when KeySet capacity changes.
     *
     * No-op for SetDelta - we only track actual operations, not capacity.
     *
     * @param old_cap Previous capacity
     * @param new_cap New capacity
     */
    void on_capacity(size_t old_cap, size_t new_cap) override {
        (void)old_cap;
        (void)new_cap;
        // No-op - delta doesn't need to track capacity
    }

    /**
     * @brief Called after a new key is inserted at a slot.
     *
     * If the slot was previously in the removed list (erase then insert),
     * both are recorded. If the slot was not in removed, it's added to
     * the added list.
     *
     * @param slot The slot index where insertion occurred
     */
    void on_insert(size_t slot) override {
        // Check if this slot was removed earlier in the same tick
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            // Erase then insert scenario: keep both records
            // The slot was removed (old element gone) and something new was added
            added_.push_back(slot);
        } else {
            // Fresh insert
            added_.push_back(slot);
        }
    }

    /**
     * @brief Called before a key is erased from a slot.
     *
     * If the slot is in the added list (insert then erase), they cancel
     * out - remove from added and don't add to removed. Otherwise, add
     * to the removed list.
     *
     * @param slot The slot index being erased
     */
    void on_erase(size_t slot) override {
        // Check if this slot was added earlier in the same tick
        auto it = std::find(added_.begin(), added_.end(), slot);
        if (it != added_.end()) {
            // Insert then erase: they cancel out
            added_.erase(it);
            // Don't add to removed_
        } else {
            // Removing a pre-existing element
            removed_.push_back(slot);
        }
    }

    /**
     * @brief Called when a value is updated at a slot.
     *
     * No-op for SetDelta - sets don't track value updates (elements are
     * either present or not, there's no "value" to update).
     *
     * @param slot The slot index where the value was updated
     */
    void on_update(size_t slot) override {
        (void)slot;
        // No-op - sets don't track updates
    }

    /**
     * @brief Called when all keys are cleared.
     *
     * Sets the cleared flag. The added/removed lists are not cleared here
     * since they may still contain relevant information about what happened
     * before the clear.
     */
    void on_clear() override {
        cleared_ = true;
    }

    // ========== Delta Accessors ==========

    /**
     * @brief Get the list of added slot indices.
     * @return Const reference to the added slots vector
     */
    [[nodiscard]] const std::vector<size_t>& added() const {
        return added_;
    }

    /**
     * @brief Get the list of removed slot indices.
     * @return Const reference to the removed slots vector
     */
    [[nodiscard]] const std::vector<size_t>& removed() const {
        return removed_;
    }

    /**
     * @brief Check if on_clear() was called this tick.
     * @return true if the underlying set was cleared
     */
    [[nodiscard]] bool was_cleared() const {
        return cleared_;
    }

    /**
     * @brief Check if there are no delta changes.
     *
     * Empty means no additions, no removals, and not cleared.
     *
     * @return true if delta is empty (no changes to report)
     */
    [[nodiscard]] bool empty() const {
        return added_.empty() && removed_.empty() && !cleared_;
    }

    // ========== State Management ==========

    /**
     * @brief Reset all delta state.
     *
     * Called at the start of each tick to clear accumulated delta.
     * Resets added, removed, and the cleared flag.
     */
    void clear() {
        added_.clear();
        removed_.clear();
        cleared_ = false;
    }

private:
    std::vector<size_t> added_;    // Slots added this tick
    std::vector<size_t> removed_;  // Slots removed this tick
    bool cleared_{false};          // Whether on_clear() was called this tick
};

} // namespace hgraph
