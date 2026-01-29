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
 * - Removed elements remain accessible by slot during the current tick
 *   (they go to a free list only used in the next engine cycle)
 * - Tracks removed key hashes for O(1) was_key_removed() queries
 * - Handles add/remove cancellation within the same tick
 * - Erase then insert records both (slot reuse scenario)
 * - on_clear() sets a cleared flag
 * - clear() resets all state including the cleared flag
 */

#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/value/key_set.h>
#include <hgraph/types/value/slot_observer.h>

namespace hgraph {

/// Type alias for hash-based key tracking in deltas
using KeyHashSet = ankerl::unordered_dense::set<size_t>;

/**
 * @brief Slot-based delta tracking for TSS.
 *
 * SetDelta maintains sets of added and removed slot indices, allowing
 * efficient delta propagation without copying elements. Using sets enables
 * O(1) membership queries (was_slot_added, was_slot_removed). It also tracks
 * removed key hashes for O(1) was_key_removed() queries.
 *
 * It implements add/remove cancellation: if a slot is inserted then erased
 * in the same tick, neither operation appears in the delta.
 *
 * SlotObserver protocol:
 * - on_capacity: No-op (delta doesn't need to track capacity)
 * - on_insert: Tracks slot as added (or cancels prior removal)
 * - on_erase: Tracks slot as removed (or cancels prior addition), captures key hash
 * - on_update: No-op (sets don't have value updates)
 * - on_clear: Sets the cleared flag
 */
class SetDelta : public value::SlotObserver {
public:
    SetDelta() = default;

    /**
     * @brief Construct with a KeySet binding for key hash tracking.
     * @param key_set The KeySet to track (caller retains ownership)
     */
    explicit SetDelta(const value::KeySet* key_set)
        : key_set_(key_set)
    {}

    // Non-copyable, movable
    SetDelta(const SetDelta&) = delete;
    SetDelta& operator=(const SetDelta&) = delete;
    SetDelta(SetDelta&&) noexcept = default;
    SetDelta& operator=(SetDelta&&) noexcept = default;

    ~SetDelta() override = default;

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
     * If the slot was previously in the removed set (erase then insert),
     * both are recorded. If the slot was not in removed, it's added to
     * the added set.
     *
     * @param slot The slot index where insertion occurred
     */
    void on_insert(size_t slot) override {
        // Always record the insertion - even if same slot was removed earlier
        // (erase then insert scenario: slot was removed and something new was added)
        added_.insert(slot);
    }

    /**
     * @brief Called before a key is erased from a slot.
     *
     * If the slot is in the added set (insert then erase), they cancel
     * out - remove from added and don't add to removed. Otherwise, add
     * to the removed set and capture the key's hash for O(1) lookup.
     *
     * Note: The key data remains accessible at the slot during the current tick
     * because removed slots go to a free list only used in the next engine cycle.
     *
     * @param slot The slot index being erased
     */
    void on_erase(size_t slot) override {
        // Check if this slot was added earlier in the same tick
        auto it = added_.find(slot);
        if (it != added_.end()) {
            // Insert then erase: they cancel out
            added_.erase(it);
            // Don't add to removed_
        } else {
            // Removing a pre-existing element
            removed_.insert(slot);

            // Capture the key's hash for O(1) was_key_removed() queries
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
     * Resets added, removed, removed key hashes, and the cleared flag.
     */
    void clear() {
        added_.clear();
        removed_.clear();
        removed_key_hashes_.clear();
        cleared_ = false;
    }

private:
    const value::KeySet* key_set_{nullptr};  // Bound KeySet for key hash tracking
    SlotSet added_;                          // Slots added this tick
    SlotSet removed_;                        // Slots removed this tick
    KeyHashSet removed_key_hashes_;          // Hashes of removed keys for O(1) lookup
    bool cleared_{false};                    // Whether on_clear() was called this tick
};

} // namespace hgraph
