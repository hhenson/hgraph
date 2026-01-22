#pragma once

/**
 * @file delta_tracker.h
 * @brief DeltaTracker - SlotObserver for tracking add/remove operations.
 *
 * DeltaTracker observes a KeySet and records which slots were added or removed
 * during a processing cycle. It implements add/remove cancellation:
 * - If a slot is added then removed in the same cycle, neither is recorded
 * - If a slot is removed then added in the same cycle, neither is recorded
 *
 * This is used by TrackedSetStorage for delta propagation in TimeSeriesSet.
 * For maps with value update tracking, see MapDeltaTracker.
 */

#include <hgraph/types/value/slot_observer.h>

#include <algorithm>
#include <cstddef>
#include <vector>

namespace hgraph::value {

/**
 * @brief SlotObserver that tracks add/remove deltas with cancellation.
 *
 * Tracks which slots were added or removed during a tick/cycle.
 * Implements the cancellation logic:
 * - Add then remove in same tick = no delta
 * - Remove then add in same tick = no delta (cancels out)
 */
class DeltaTracker : public SlotObserver {
public:
    DeltaTracker() = default;

    // Non-copyable, movable
    DeltaTracker(const DeltaTracker&) = delete;
    DeltaTracker& operator=(const DeltaTracker&) = delete;
    DeltaTracker(DeltaTracker&&) noexcept = default;
    DeltaTracker& operator=(DeltaTracker&&) noexcept = default;

    // ========== SlotObserver Implementation ==========

    void on_capacity(size_t /*old_cap*/, size_t /*new_cap*/) override {
        // Capacity changes don't affect delta tracking
    }

    void on_insert(size_t slot) override {
        // Check if this slot was previously removed this tick
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            // Cancel: was removed, now added back = no net change
            swap_erase(removed_, it);
        } else {
            // Track as newly added
            added_.push_back(slot);
        }
    }

    void on_erase(size_t slot) override {
        // Check if this slot was added this tick
        auto it = std::find(added_.begin(), added_.end(), slot);
        if (it != added_.end()) {
            // Cancel: was added, now removed = no net change
            swap_erase(added_, it);
        } else {
            // Track as removed
            removed_.push_back(slot);
        }
    }

protected:
    /**
     * @brief O(1) erasure by swapping with last element.
     * Order doesn't matter for delta tracking, so this is safe.
     */
    static void swap_erase(std::vector<size_t>& vec,
                           typename std::vector<size_t>::iterator it) {
        if (it != vec.end() - 1) {
            std::swap(*it, vec.back());
        }
        vec.pop_back();
    }

    void on_clear() override {
        // When clearing, all previously existing items are "removed"
        // But we can't know which existed before - caller handles this
        // by iterating the set before clear and recording removals
        // For the observer pattern, clear just resets our tracking
        added_.clear();
        removed_.clear();
    }

    // ========== Delta Access ==========

    /**
     * @brief Get slots that were added this tick.
     */
    [[nodiscard]] const std::vector<size_t>& added_slots() const { return added_; }

    /**
     * @brief Get slots that were removed this tick.
     */
    [[nodiscard]] const std::vector<size_t>& removed_slots() const { return removed_; }

    /**
     * @brief Check if a slot was added this tick.
     */
    [[nodiscard]] bool was_added(size_t slot) const {
        return std::find(added_.begin(), added_.end(), slot) != added_.end();
    }

    /**
     * @brief Check if a slot was removed this tick.
     */
    [[nodiscard]] bool was_removed(size_t slot) const {
        return std::find(removed_.begin(), removed_.end(), slot) != removed_.end();
    }

    /**
     * @brief Check if there are any deltas.
     */
    [[nodiscard]] bool has_delta() const {
        return !added_.empty() || !removed_.empty();
    }

    // ========== Preparation ==========

    /**
     * @brief Sort delta vectors for ordered iteration.
     *
     * Call this before iterating if you need slots in ascending order.
     * Sorting enables better cache locality when accessing data at slots.
     */
    void prepare() {
        std::sort(added_.begin(), added_.end());
        std::sort(removed_.begin(), removed_.end());
    }

    // ========== Tick Management ==========

    /**
     * @brief Clear delta tracking for a new tick/cycle.
     *
     * Call this at the beginning of each processing cycle to reset
     * the add/remove tracking.
     */
    void begin_tick() {
        added_.clear();
        removed_.clear();
    }

    /**
     * @brief Alias for begin_tick() - clear delta tracking.
     */
    void clear_deltas() {
        begin_tick();
    }

private:
    std::vector<size_t> added_;    // Slots added this tick
    std::vector<size_t> removed_;  // Slots removed this tick
};

} // namespace hgraph::value
