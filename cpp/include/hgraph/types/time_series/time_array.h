#pragma once

/**
 * @file time_array.h
 * @brief TimeArray - Parallel timestamp array synchronized with KeySet.
 *
 * TimeArray provides parallel timestamp storage for time-series collections
 * (TSD, TSS). It implements the SlotObserver protocol to stay synchronized
 * with a KeySet, maintaining per-slot modification timestamps.
 *
 * Key design principles:
 * - Implements SlotObserver for automatic synchronization
 * - MIN_DT indicates "not valid" (never been set)
 * - Modified check uses >= comparison (modified if time >= current_time)
 * - Provides direct data() access for zero-copy Arrow/numpy integration
 */

#include <hgraph/types/value/slot_observer.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <vector>

namespace hgraph {

/**
 * @brief Parallel timestamp array synchronized with KeySet.
 *
 * TimeArray maintains a vector of timestamps parallel to a KeySet's slot storage.
 * Each slot has an associated timestamp indicating when it was last modified.
 *
 * SlotObserver protocol:
 * - on_capacity: Resizes storage, new slots initialized to MIN_DT
 * - on_insert: Initializes slot timestamp to MIN_DT (invalid until set)
 * - on_erase: Preserves timestamp (may be queried for delta purposes)
 * - on_update: No-op (timestamp set explicitly via set())
 * - on_clear: Resets all timestamps to MIN_DT
 */
class TimeArray : public value::SlotObserver {
public:
    TimeArray() = default;

    // Non-copyable, movable
    TimeArray(const TimeArray&) = delete;
    TimeArray& operator=(const TimeArray&) = delete;
    TimeArray(TimeArray&&) noexcept = default;
    TimeArray& operator=(TimeArray&&) noexcept = default;

    ~TimeArray() override = default;

    // ========== SlotObserver Implementation ==========

    /**
     * @brief Called when KeySet capacity changes.
     *
     * Resizes the timestamp storage to match the new capacity.
     * New slots are initialized to MIN_DT (invalid).
     *
     * @param old_cap Previous capacity
     * @param new_cap New capacity
     */
    void on_capacity(size_t old_cap, size_t new_cap) override {
        (void)old_cap;  // Unused
        times_.resize(new_cap, MIN_DT);
    }

    /**
     * @brief Called after a new key is inserted at a slot.
     *
     * Initializes the slot's timestamp to MIN_DT (invalid until explicitly set).
     *
     * @param slot The slot index where insertion occurred
     */
    void on_insert(size_t slot) override {
        if (slot < times_.size()) {
            times_[slot] = MIN_DT;
        }
        ++size_;
    }

    /**
     * @brief Called before a key is erased from a slot.
     *
     * Preserves the timestamp - it may be queried for delta purposes
     * until the slot is reused.
     *
     * @param slot The slot index being erased
     */
    void on_erase(size_t slot) override {
        (void)slot;  // Preserve timestamp
        --size_;
    }

    /**
     * @brief Called when a value is updated at a slot.
     *
     * No-op for TimeArray - timestamps are set explicitly via set().
     *
     * @param slot The slot index where the value was updated
     */
    void on_update(size_t slot) override {
        (void)slot;  // No-op - timestamps set explicitly
    }

    /**
     * @brief Called when all keys are cleared.
     *
     * Resets all timestamps to MIN_DT (invalid).
     */
    void on_clear() override {
        std::fill(times_.begin(), times_.end(), MIN_DT);
        size_ = 0;
    }

    // ========== Time Access ==========

    /**
     * @brief Get the timestamp at a slot.
     * @param slot The slot index
     * @return The timestamp at the slot
     */
    [[nodiscard]] engine_time_t at(size_t slot) const {
        return times_[slot];
    }

    /**
     * @brief Set the timestamp at a slot.
     * @param slot The slot index
     * @param t The timestamp to set
     */
    void set(size_t slot, engine_time_t t) {
        times_[slot] = t;
    }

    /**
     * @brief Check if a slot was modified at or after current_time.
     *
     * Uses >= comparison: modified if last_modified_time >= current_time.
     * This means the time-series was modified during this tick (equal)
     * or potentially in a future tick (greater than, for out-of-order processing).
     *
     * @param slot The slot index
     * @param current The current engine time
     * @return true if the slot's timestamp >= current
     */
    [[nodiscard]] bool modified(size_t slot, engine_time_t current) const {
        return times_[slot] >= current;
    }

    /**
     * @brief Check if a slot has ever been set (is valid).
     *
     * A slot is valid if its timestamp is not MIN_DT.
     *
     * @param slot The slot index
     * @return true if the slot has been set at least once
     */
    [[nodiscard]] bool valid(size_t slot) const {
        return times_[slot] != MIN_DT;
    }

    // ========== Raw Access ==========

    /**
     * @brief Get direct pointer to timestamp storage.
     *
     * Provides zero-copy access for Arrow/numpy integration.
     *
     * @return Pointer to the beginning of timestamp storage
     */
    [[nodiscard]] engine_time_t* data() {
        return times_.data();
    }

    /**
     * @brief Get direct const pointer to timestamp storage.
     */
    [[nodiscard]] const engine_time_t* data() const {
        return times_.data();
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
        return times_.size();
    }

private:
    std::vector<engine_time_t> times_;  // Parallel timestamp storage
    size_t size_{0};                    // Number of active slots
};

} // namespace hgraph
