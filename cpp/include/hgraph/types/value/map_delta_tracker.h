#pragma once

/**
 * @file map_delta_tracker.h
 * @brief MapDeltaTracker - DeltaTracker extension for tracking value updates in maps.
 *
 * MapDeltaTracker extends DeltaTracker with value update tracking. While DeltaTracker
 * tracks only add/remove operations (suitable for sets), MapDeltaTracker additionally
 * tracks when existing keys have their values changed.
 *
 * This supports the design where Map = Set + ValueArray, and a set view can be
 * taken from a map using just the base DeltaTracker for key changes only.
 */

#include <hgraph/types/value/delta_tracker.h>

#include <algorithm>
#include <vector>

namespace hgraph::value {

/**
 * @brief DeltaTracker extension that also tracks value updates for maps.
 *
 * Inherits add/remove tracking from DeltaTracker and adds:
 * - on_update() tracking for value changes on existing keys
 * - updated_slots() to retrieve which slots had value updates
 *
 * Cancellation logic for updates:
 * - If a slot is added this tick, subsequent updates are not recorded
 *   (the add already implies a new value)
 * - If a slot is removed after being updated, the update is discarded
 */
class MapDeltaTracker : public DeltaTracker {
public:
    MapDeltaTracker() = default;

    // Non-copyable, movable
    MapDeltaTracker(const MapDeltaTracker&) = delete;
    MapDeltaTracker& operator=(const MapDeltaTracker&) = delete;
    MapDeltaTracker(MapDeltaTracker&&) noexcept = default;
    MapDeltaTracker& operator=(MapDeltaTracker&&) noexcept = default;

    // ========== SlotObserver Override for Updates ==========

    void on_update(size_t slot) override {
        // If the slot was added this tick, don't record as update
        // (the "add" already implies a new value was set)
        if (was_added(slot)) {
            return;
        }

        // Only record once per tick
        if (std::find(updated_.begin(), updated_.end(), slot) == updated_.end()) {
            updated_.push_back(slot);
        }
    }

    void on_erase(size_t slot) override {
        // Let base class handle add/remove tracking
        DeltaTracker::on_erase(slot);

        // Remove from updated if present (removal supersedes update)
        auto upd_it = std::find(updated_.begin(), updated_.end(), slot);
        if (upd_it != updated_.end()) {
            updated_.erase(upd_it);
        }
    }

    void on_clear() override {
        DeltaTracker::on_clear();
        updated_.clear();
    }

    // ========== Update-Specific Access ==========

    /**
     * @brief Get slots that were updated this tick.
     */
    [[nodiscard]] const std::vector<size_t>& updated_slots() const { return updated_; }

    /**
     * @brief Check if a slot was updated this tick.
     */
    [[nodiscard]] bool was_updated(size_t slot) const {
        return std::find(updated_.begin(), updated_.end(), slot) != updated_.end();
    }

    /**
     * @brief Check if there are any value updates.
     */
    [[nodiscard]] bool has_value_updates() const {
        return !updated_.empty();
    }

    /**
     * @brief Check if there are any deltas (including updates).
     */
    [[nodiscard]] bool has_delta() const {
        return DeltaTracker::has_delta() || !updated_.empty();
    }

    // ========== Tick Management ==========

    /**
     * @brief Clear delta tracking for a new tick/cycle.
     */
    void begin_tick() {
        DeltaTracker::begin_tick();
        updated_.clear();
    }

    /**
     * @brief Alias for begin_tick() - clear delta tracking.
     */
    void clear_deltas() {
        begin_tick();
    }

private:
    std::vector<size_t> updated_;  // Slots with value updates this tick
};

} // namespace hgraph::value
