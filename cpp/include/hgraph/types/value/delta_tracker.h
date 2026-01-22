#pragma once

/**
 * @file delta_tracker.h
 * @brief DeltaTracker - SlotObserver for tracking add/remove/update operations.
 *
 * DeltaTracker observes a KeySet and records which slots were added, removed,
 * or updated during a processing cycle. It implements add/remove cancellation:
 * - If a slot is added then removed in the same cycle, neither is recorded
 * - If a slot is removed then added in the same cycle, tracked as update
 *
 * For maps, on_update() is called when a value changes for an existing key.
 * For sets, on_update() is never called (sets have no values).
 *
 * This is used by TrackedSetStorage for delta propagation in TimeSeriesSet
 * and can be used by TrackedMapStorage for TimeSeriesDict.
 */

#include <hgraph/types/value/slot_observer.h>

#include <algorithm>
#include <cstddef>
#include <vector>

namespace hgraph::value {

/**
 * @brief SlotObserver that tracks add/remove/update deltas with cancellation.
 *
 * Tracks which slots were added, removed, or updated during a tick/cycle.
 * Implements the cancellation logic:
 * - Add then remove in same tick = no delta
 * - Remove then add in same tick = tracked as update (value may differ)
 * - Add then update = only add recorded (add implies new value)
 * - Update multiple times = recorded once
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
            // Cancel: was removed, now added back
            // Track as update since value may have changed
            removed_.erase(it);
            if (std::find(updated_.begin(), updated_.end(), slot) == updated_.end()) {
                updated_.push_back(slot);
            }
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
            added_.erase(it);
            // Also remove from updated if present
            auto upd_it = std::find(updated_.begin(), updated_.end(), slot);
            if (upd_it != updated_.end()) {
                updated_.erase(upd_it);
            }
        } else {
            // Track as removed
            removed_.push_back(slot);
            // Remove from updated if present (removal supersedes update)
            auto upd_it = std::find(updated_.begin(), updated_.end(), slot);
            if (upd_it != updated_.end()) {
                updated_.erase(upd_it);
            }
        }
    }

    void on_update(size_t slot) override {
        // If the slot was added this tick, don't record as update
        // (the "add" already implies a new value was set)
        if (std::find(added_.begin(), added_.end(), slot) != added_.end()) {
            return;
        }

        // Only record once per tick
        if (std::find(updated_.begin(), updated_.end(), slot) == updated_.end()) {
            updated_.push_back(slot);
        }
    }

    void on_clear() override {
        // When clearing, all previously existing items are "removed"
        // But we can't know which existed before - caller handles this
        // by iterating the set before clear and recording removals
        // For the observer pattern, clear just resets our tracking
        added_.clear();
        removed_.clear();
        updated_.clear();
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
     * @brief Get slots that were updated this tick (map-specific).
     */
    [[nodiscard]] const std::vector<size_t>& updated_slots() const { return updated_; }

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
     * @brief Check if a slot was updated this tick.
     */
    [[nodiscard]] bool was_updated(size_t slot) const {
        return std::find(updated_.begin(), updated_.end(), slot) != updated_.end();
    }

    /**
     * @brief Check if there are any deltas.
     */
    [[nodiscard]] bool has_delta() const {
        return !added_.empty() || !removed_.empty() || !updated_.empty();
    }

    /**
     * @brief Check if there are any key deltas (add or remove, not update).
     */
    [[nodiscard]] bool has_key_delta() const {
        return !added_.empty() || !removed_.empty();
    }

    /**
     * @brief Check if there are any value updates.
     */
    [[nodiscard]] bool has_value_updates() const {
        return !updated_.empty();
    }

    // ========== Tick Management ==========

    /**
     * @brief Clear delta tracking for a new tick/cycle.
     *
     * Call this at the beginning of each processing cycle to reset
     * the add/remove/update tracking.
     */
    void begin_tick() {
        added_.clear();
        removed_.clear();
        updated_.clear();
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
    std::vector<size_t> updated_;  // Slots updated this tick (map-specific)
};

} // namespace hgraph::value
