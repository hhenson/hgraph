#pragma once

/**
 * @file map_delta.h
 * @brief MapDelta - Slot-based delta tracking for TSD (Time-Series Dictionary).
 *
 * MapDelta tracks add/remove/update operations using slot indices for zero-copy
 * delta propagation. It implements the SlotObserver protocol to receive
 * notifications from the underlying KeySet/MapStorage.
 *
 * Key design principles:
 * - Tracks added/removed/updated slot indices (not element copies)
 * - Handles add/remove cancellation within the same tick
 * - Maintains children vector for nested time-series delta navigation
 * - on_clear() sets a cleared flag
 * - clear() resets all state including the cleared flag
 *
 * This file also defines DeltaVariant for type-safe child delta storage.
 */

#include <hgraph/types/value/slot_observer.h>

#include <algorithm>
#include <cstddef>
#include <variant>
#include <vector>

namespace hgraph {

// Forward declarations for DeltaVariant
class SetDelta;
class MapDelta;
struct BundleDeltaNav;
struct ListDeltaNav;

/**
 * @brief Type-safe variant for child delta storage.
 *
 * DeltaVariant can hold a pointer to any delta structure type, enabling
 * type-safe navigation of nested time-series deltas. The monostate
 * alternative represents "no delta" (for scalar or non-delta types).
 */
using DeltaVariant = std::variant<
    std::monostate,      // No delta (scalar, or type without delta)
    SetDelta*,           // TSS delta
    MapDelta*,           // TSD delta
    BundleDeltaNav*,     // TSB delta navigation
    ListDeltaNav*        // TSL delta navigation
>;

/**
 * @brief Slot-based delta tracking for TSD.
 *
 * MapDelta maintains vectors of added, removed, and updated slot indices,
 * allowing efficient delta propagation without copying elements. It implements
 * add/remove cancellation: if a slot is inserted then erased in the same
 * tick, neither operation appears in the delta.
 *
 * Additionally, MapDelta maintains a children vector for accessing nested
 * time-series delta information when the TSD's value type is itself a
 * time-series type.
 *
 * SlotObserver protocol:
 * - on_capacity: Resizes children vector to match
 * - on_insert: Tracks slot as added (or cancels prior removal)
 * - on_erase: Tracks slot as removed (or cancels prior addition)
 * - on_update: Tracks slot as updated
 * - on_clear: Sets the cleared flag
 */
class MapDelta : public value::SlotObserver {
public:
    MapDelta() = default;

    // Non-copyable, movable
    MapDelta(const MapDelta&) = delete;
    MapDelta& operator=(const MapDelta&) = delete;
    MapDelta(MapDelta&&) noexcept = default;
    MapDelta& operator=(MapDelta&&) noexcept = default;

    ~MapDelta() override = default;

    // ========== SlotObserver Implementation ==========

    /**
     * @brief Called when KeySet capacity changes.
     *
     * Resizes the children vector to match the new capacity.
     * New slots get monostate (no delta).
     *
     * @param old_cap Previous capacity
     * @param new_cap New capacity
     */
    void on_capacity(size_t old_cap, size_t new_cap) override {
        (void)old_cap;
        children_.resize(new_cap);
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
            // Also remove from updated if present
            auto updated_it = std::find(updated_.begin(), updated_.end(), slot);
            if (updated_it != updated_.end()) {
                updated_.erase(updated_it);
            }
            // Don't add to removed_
        } else {
            // Removing a pre-existing element
            removed_.push_back(slot);
            // Remove from updated if it was there
            auto updated_it = std::find(updated_.begin(), updated_.end(), slot);
            if (updated_it != updated_.end()) {
                updated_.erase(updated_it);
            }
        }
    }

    /**
     * @brief Called when a value is updated at a slot.
     *
     * Adds the slot to the updated list if not already present and
     * not in the added list (new slots don't need "updated" tracking).
     *
     * @param slot The slot index where the value was updated
     */
    void on_update(size_t slot) override {
        // Don't track updates for slots that were just added
        if (std::find(added_.begin(), added_.end(), slot) != added_.end()) {
            return;
        }
        // Only add if not already in updated list
        if (std::find(updated_.begin(), updated_.end(), slot) == updated_.end()) {
            updated_.push_back(slot);
        }
    }

    /**
     * @brief Called when all keys are cleared.
     *
     * Sets the cleared flag. The operation lists are not cleared here
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
     * @brief Get the list of updated slot indices.
     * @return Const reference to the updated slots vector
     */
    [[nodiscard]] const std::vector<size_t>& updated() const {
        return updated_;
    }

    /**
     * @brief Get mutable access to the children delta vector.
     *
     * Allows setting child delta pointers for nested time-series types.
     *
     * @return Mutable reference to the children vector
     */
    [[nodiscard]] std::vector<DeltaVariant>& children() {
        return children_;
    }

    /**
     * @brief Get const access to the children delta vector.
     * @return Const reference to the children vector
     */
    [[nodiscard]] const std::vector<DeltaVariant>& children() const {
        return children_;
    }

    /**
     * @brief Check if on_clear() was called this tick.
     * @return true if the underlying map was cleared
     */
    [[nodiscard]] bool was_cleared() const {
        return cleared_;
    }

    /**
     * @brief Check if there are no delta changes.
     *
     * Empty means no additions, no removals, no updates, and not cleared.
     *
     * @return true if delta is empty (no changes to report)
     */
    [[nodiscard]] bool empty() const {
        return added_.empty() && removed_.empty() && updated_.empty() && !cleared_;
    }

    // ========== State Management ==========

    /**
     * @brief Reset all delta state.
     *
     * Called at the start of each tick to clear accumulated delta.
     * Resets added, removed, updated, children, and the cleared flag.
     */
    void clear() {
        added_.clear();
        removed_.clear();
        updated_.clear();
        // Reset children to monostate
        for (auto& child : children_) {
            child = std::monostate{};
        }
        cleared_ = false;
    }

private:
    std::vector<size_t> added_;             // Slots added this tick
    std::vector<size_t> removed_;           // Slots removed this tick
    std::vector<size_t> updated_;           // Slots updated this tick
    std::vector<DeltaVariant> children_;    // Child deltas for nested TS types
    bool cleared_{false};                   // Whether on_clear() was called this tick
};

} // namespace hgraph
