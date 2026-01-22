#pragma once

/**
 * @file map_delta_tracker.h
 * @brief MapDeltaTracker - SlotObserver for tracking map add/remove/update operations.
 *
 * MapDeltaTracker extends delta tracking to include value updates:
 * - Added: New keys inserted
 * - Removed: Keys deleted
 * - Updated: Existing keys whose values changed
 *
 * Implements add/remove/update cancellation:
 * - If a key is added then removed in the same cycle, neither is recorded
 * - If a key is removed then added in the same cycle, neither is recorded
 * - If a key is updated multiple times, it appears once in updated
 * - If a key is added then updated, only added is recorded (add implies new value)
 *
 * This is used by TrackedMapStorage for delta propagation in TimeSeriesDict.
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
 * Implements the cancellation logic for maps where value updates are
 * distinct from key additions.
 */
class MapDeltaTracker : public SlotObserver {
public:
    MapDeltaTracker() = default;

    // Non-copyable, movable
    MapDeltaTracker(const MapDeltaTracker&) = delete;
    MapDeltaTracker& operator=(const MapDeltaTracker&) = delete;
    MapDeltaTracker(MapDeltaTracker&&) noexcept = default;
    MapDeltaTracker& operator=(MapDeltaTracker&&) noexcept = default;

    // ========== SlotObserver Implementation ==========

    void on_capacity(size_t /*old_cap*/, size_t /*new_cap*/) override {
        // Capacity changes don't affect delta tracking
    }

    void on_insert(size_t slot) override {
        // Check if this slot was previously removed this tick
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            // Cancel: was removed, now added back = no net change for key
            // But the value might be different, so track as update
            removed_.erase(it);
            // Mark as updated since it was re-added (value may have changed)
            if (std::find(updated_.begin(), updated_.end(), slot) == updated_.end()) {
                updated_.push_back(slot);
            }
        } else {
            // Track as newly added
            added_.push_back(slot);
        }
    }

    void on_erase(size_t slot, size_t /*last_slot*/) override {
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

    void on_clear() override {
        // When clearing, all previously existing items are "removed"
        // Caller handles this by iterating before clear and recording removals
        // For the observer pattern, clear just resets our tracking
        added_.clear();
        removed_.clear();
        updated_.clear();
    }

    // ========== Update Notification (Map-specific) ==========

    /**
     * @brief Called when a value is updated for an existing key.
     *
     * This is not part of SlotObserver since it's map-specific.
     * MapStorage calls this when set_item updates an existing key's value.
     *
     * @param slot The slot index where the value was updated
     */
    void on_value_update(size_t slot) {
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

    // ========== Delta Access ==========

    /**
     * @brief Get slots that were added this tick (new keys).
     */
    [[nodiscard]] const std::vector<size_t>& added_slots() const { return added_; }

    /**
     * @brief Get slots that were removed this tick (deleted keys).
     */
    [[nodiscard]] const std::vector<size_t>& removed_slots() const { return removed_; }

    /**
     * @brief Get slots that were updated this tick (value changes for existing keys).
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
     * @brief Check if a slot's value was updated this tick.
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
    std::vector<size_t> added_;    // Slots added this tick (new keys)
    std::vector<size_t> removed_;  // Slots removed this tick (deleted keys)
    std::vector<size_t> updated_;  // Slots updated this tick (value changes)
};

} // namespace hgraph::value
