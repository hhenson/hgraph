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

#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/value/key_set.h>
#include <hgraph/types/value/slot_observer.h>

#include <variant>
#include <vector>

namespace hgraph {

/// Type alias for hash-based key tracking in deltas
using KeyHashSet = ankerl::unordered_dense::set<size_t>;

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

    /**
     * @brief Construct with a KeySet binding for key hash tracking.
     * @param key_set The KeySet to track (caller retains ownership)
     */
    explicit MapDelta(const value::KeySet* key_set)
        : key_set_(key_set)
    {}

    // Non-copyable, movable
    MapDelta(const MapDelta&) = delete;
    MapDelta& operator=(const MapDelta&) = delete;
    MapDelta(MapDelta&&) noexcept = default;
    MapDelta& operator=(MapDelta&&) noexcept = default;

    ~MapDelta() override = default;

    // ========== KeySet Binding ==========

    /**
     * @brief Bind to a KeySet for key hash tracking.
     *
     * When bound, on_erase captures the key's hash before destruction,
     * enabling O(1) was_key_removed() queries.
     *
     * @param key_set The KeySet to track (caller retains ownership)
     */
    void bind(const value::KeySet* key_set) {
        key_set_ = key_set;
    }

    /**
     * @brief Get the bound KeySet.
     * @return Pointer to the bound KeySet, or nullptr if not bound
     */
    [[nodiscard]] const value::KeySet* key_set() const {
        return key_set_;
    }

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
     * Always records the insertion - even if same slot was removed earlier
     * (erase then insert scenario: slot was removed and something new was added).
     *
     * @param slot The slot index where insertion occurred
     */
    void on_insert(size_t slot) override {
        // Always record the insertion
        added_.insert(slot);
    }

    /**
     * @brief Called before a key is erased from a slot.
     *
     * If the slot is in the added set (insert then erase), they cancel
     * out - remove from added and don't add to removed. Otherwise, add
     * to the removed set and capture the key's hash for O(1) lookup.
     *
     * @param slot The slot index being erased
     */
    void on_erase(size_t slot) override {
        // Check if this slot was added earlier in the same tick
        auto it = added_.find(slot);
        if (it != added_.end()) {
            // Insert then erase: they cancel out
            added_.erase(it);
            // Also remove from updated if present
            updated_.erase(slot);
            // Don't add to removed_
        } else {
            // Removing a pre-existing element
            removed_.insert(slot);
            // Remove from updated if it was there
            updated_.erase(slot);

            // Capture the key's hash before it's destroyed (if bound to KeySet)
            if (key_set_) {
                const void* key_ptr = key_set_->key_at_slot(slot);
                const value::TypeMeta* key_type = key_set_->key_type();
                if (key_type && key_type->ops && key_type->ops->hash) {
                    size_t key_hash = key_type->ops->hash(key_ptr, key_type);
                    removed_key_hashes_.insert(key_hash);
                }
            }
        }
    }

    /**
     * @brief Called when a value is updated at a slot.
     *
     * Adds the slot to the updated set if not in the added set
     * (new slots don't need "updated" tracking - they're already "added").
     *
     * @param slot The slot index where the value was updated
     */
    void on_update(size_t slot) override {
        // Don't track updates for slots that were just added
        if (added_.contains(slot)) {
            return;
        }
        // Add to updated set (set handles deduplication)
        updated_.insert(slot);
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
     * @brief Get the set of added slot indices.
     * @return Const reference to the added slots set
     */
    [[nodiscard]] const SlotSet& added() const {
        return added_;
    }

    /**
     * @brief Get the set of removed slot indices.
     * @return Const reference to the removed slots set
     */
    [[nodiscard]] const SlotSet& removed() const {
        return removed_;
    }

    /**
     * @brief Get the set of updated slot indices.
     * @return Const reference to the updated slots set
     */
    [[nodiscard]] const SlotSet& updated() const {
        return updated_;
    }

    /**
     * @brief Check if a specific slot was added this tick.
     * @param slot The slot index to check
     * @return true if the slot was added
     */
    [[nodiscard]] bool was_slot_added(size_t slot) const {
        return added_.contains(slot);
    }

    /**
     * @brief Check if a specific slot was removed this tick.
     * @param slot The slot index to check
     * @return true if the slot was removed
     */
    [[nodiscard]] bool was_slot_removed(size_t slot) const {
        return removed_.contains(slot);
    }

    /**
     * @brief Check if a specific slot was updated this tick.
     * @param slot The slot index to check
     * @return true if the slot was updated
     */
    [[nodiscard]] bool was_slot_updated(size_t slot) const {
        return updated_.contains(slot);
    }

    /**
     * @brief Check if a specific slot was modified (added or updated) this tick.
     * @param slot The slot index to check
     * @return true if the slot was added or updated
     */
    [[nodiscard]] bool was_slot_modified(size_t slot) const {
        return added_.contains(slot) || updated_.contains(slot);
    }

    /**
     * @brief Check if a key with the given hash was removed this tick.
     *
     * This is O(1) lookup in the removed key hashes set.
     * Requires that the delta was bound to a KeySet.
     *
     * @param key_hash The hash of the key to check
     * @return true if a key with this hash was removed
     */
    [[nodiscard]] bool was_key_hash_removed(size_t key_hash) const {
        return removed_key_hashes_.contains(key_hash);
    }

    /**
     * @brief Check if a specific key was removed this tick.
     *
     * This is O(1) lookup using the key's hash.
     * Requires that the delta was bound to a KeySet.
     *
     * @param key_ptr Pointer to the key data
     * @param key_type TypeMeta for the key type
     * @return true if the key was removed
     */
    [[nodiscard]] bool was_key_removed(const void* key_ptr, const value::TypeMeta* key_type) const {
        if (!key_type || !key_type->ops || !key_type->ops->hash) {
            return false;
        }
        size_t key_hash = key_type->ops->hash(key_ptr, key_type);
        return removed_key_hashes_.contains(key_hash);
    }

    /**
     * @brief Get the set of removed key hashes.
     * @return Const reference to the removed key hashes set
     */
    [[nodiscard]] const KeyHashSet& removed_key_hashes() const {
        return removed_key_hashes_;
    }

    /**
     * @brief Get the set of modified slot indices (added + updated).
     *
     * This returns a lazily-computed union of added and updated slots.
     * The result is cached and invalidated when clear() is called.
     *
     * @return Const reference to the modified slots set
     */
    [[nodiscard]] const SlotSet& modified() const {
        if (!modified_valid_) {
            modified_.clear();
            modified_.insert(added_.begin(), added_.end());
            modified_.insert(updated_.begin(), updated_.end());
            modified_valid_ = true;
        }
        return modified_;
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
     * Resets added, removed, updated, removed key hashes, children, and the cleared flag.
     */
    void clear() {
        added_.clear();
        removed_.clear();
        updated_.clear();
        removed_key_hashes_.clear();
        // Invalidate cached modified set
        modified_.clear();
        modified_valid_ = false;
        // Reset children to monostate
        for (auto& child : children_) {
            child = std::monostate{};
        }
        cleared_ = false;
    }

private:
    const value::KeySet* key_set_{nullptr};  // Bound KeySet for key hash tracking
    SlotSet added_;    // Slots added this tick
    SlotSet removed_;  // Slots removed this tick
    SlotSet updated_;  // Slots updated this tick
    KeyHashSet removed_key_hashes_;  // Hashes of removed keys for O(1) lookup
    std::vector<DeltaVariant> children_;  // Child deltas for nested TS types
    bool cleared_{false};  // Whether on_clear() was called this tick

    // Cached combined modified set (lazily computed)
    mutable SlotSet modified_;
    mutable bool modified_valid_{false};
};

} // namespace hgraph
