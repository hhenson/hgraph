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
 * - Uses composition: contains a SetDelta for key add/remove tracking
 * - Adds updated slot tracking for value modifications
 * - Maintains children vector for nested time-series delta navigation
 * - The embedded key_delta_ can be used directly by TSSView for key_set() access
 *
 * This file also defines DeltaVariant for type-safe child delta storage.
 */

#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/value/key_set.h>
#include <hgraph/types/value/slot_observer.h>

#include <variant>
#include <vector>

namespace hgraph {

// Forward declarations for DeltaVariant
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
 * MapDelta uses composition to extend SetDelta with value update tracking:
 * - key_delta_: Embedded SetDelta for key add/remove (composition)
 * - updated_: Slot indices where values were updated
 * - children_: Child deltas for nested time-series types
 *
 * The key_delta_ member can be accessed directly via key_delta() for use
 * by TSDView::key_set(), which returns a TSSView pointing to it.
 *
 * SlotObserver protocol:
 * - on_capacity: Resizes children vector to match
 * - on_insert: Forwards to key_delta_
 * - on_erase: Forwards to key_delta_, clears from updated_
 * - on_update: Tracks slot as updated (if not newly added)
 * - on_clear: Forwards to key_delta_
 */
class MapDelta : public value::SlotObserver {
public:
    MapDelta() = default;

    /**
     * @brief Construct with a KeySet binding for key hash tracking.
     * @param key_set The KeySet to track (caller retains ownership)
     */
    explicit MapDelta(const value::KeySet* key_set)
        : key_delta_(key_set)
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
     * Forwards to the embedded key_delta_.
     *
     * @param key_set The KeySet to track (caller retains ownership)
     */
    void bind(const value::KeySet* key_set) {
        key_delta_.bind(key_set);
    }

    /**
     * @brief Get the bound KeySet.
     * @return Pointer to the bound KeySet, or nullptr if not bound
     */
    [[nodiscard]] const value::KeySet* key_set() const {
        return key_delta_.key_set();
    }

    // ========== Composition Access ==========

    /**
     * @brief Get the embedded SetDelta for key tracking.
     *
     * This allows TSDView::key_set() to return a TSSView that uses
     * the embedded SetDelta directly.
     *
     * @return Reference to the embedded SetDelta
     */
    [[nodiscard]] SetDelta& key_delta() {
        return key_delta_;
    }

    /**
     * @brief Get const access to the embedded SetDelta.
     * @return Const reference to the embedded SetDelta
     */
    [[nodiscard]] const SetDelta& key_delta() const {
        return key_delta_;
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
        key_delta_.on_capacity(old_cap, new_cap);
        children_.resize(new_cap);
    }

    /**
     * @brief Called after a new key is inserted at a slot.
     *
     * Forwards to key_delta_ for add/remove tracking.
     *
     * @param slot The slot index where insertion occurred
     */
    void on_insert(size_t slot) override {
        key_delta_.on_insert(slot);
    }

    /**
     * @brief Called before a key is erased from a slot.
     *
     * Forwards to key_delta_ for add/remove tracking.
     * Also removes from updated_ if present.
     *
     * @param slot The slot index being erased
     */
    void on_erase(size_t slot) override {
        // Remove from updated if it was there (before key_delta_ processes)
        updated_.erase(slot);
        // Forward to key_delta_ for add/remove cancellation logic
        key_delta_.on_erase(slot);
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
        if (key_delta_.was_slot_added(slot)) {
            return;
        }
        // Add to updated set (set handles deduplication)
        updated_.insert(slot);
    }

    /**
     * @brief Called when all keys are cleared.
     *
     * Forwards to key_delta_.
     */
    void on_clear() override {
        key_delta_.on_clear();
    }

    // ========== Key Delta Accessors (delegate to key_delta_) ==========

    /**
     * @brief Get the set of added slot indices.
     * @return Const reference to the added slots set
     */
    [[nodiscard]] const SlotSet& added() const {
        return key_delta_.added();
    }

    /**
     * @brief Get the set of removed slot indices.
     * @return Const reference to the removed slots set
     */
    [[nodiscard]] const SlotSet& removed() const {
        return key_delta_.removed();
    }

    /**
     * @brief Check if a specific slot was added this tick.
     * @param slot The slot index to check
     * @return true if the slot was added
     */
    [[nodiscard]] bool was_slot_added(size_t slot) const {
        return key_delta_.was_slot_added(slot);
    }

    /**
     * @brief Check if a specific slot was removed this tick.
     * @param slot The slot index to check
     * @return true if the slot was removed
     */
    [[nodiscard]] bool was_slot_removed(size_t slot) const {
        return key_delta_.was_slot_removed(slot);
    }

    /**
     * @brief Check if a key with the given hash was removed this tick.
     * @param key_hash The hash of the key to check
     * @return true if a key with this hash was removed
     */
    [[nodiscard]] bool was_key_hash_removed(size_t key_hash) const {
        return key_delta_.was_key_hash_removed(key_hash);
    }

    /**
     * @brief Check if a specific key was removed this tick.
     * @param key_ptr Pointer to the key data
     * @param key_type TypeMeta for the key type
     * @return true if the key was removed
     */
    [[nodiscard]] bool was_key_removed(const void* key_ptr, const value::TypeMeta* key_type) const {
        return key_delta_.was_key_removed(key_ptr, key_type);
    }

    /**
     * @brief Get the set of removed key hashes.
     * @return Const reference to the removed key hashes set
     */
    [[nodiscard]] const KeyHashSet& removed_key_hashes() const {
        return key_delta_.removed_key_hashes();
    }

    /**
     * @brief Check if on_clear() was called this tick.
     * @return true if the underlying map was cleared
     */
    [[nodiscard]] bool was_cleared() const {
        return key_delta_.was_cleared();
    }

    // ========== Map-Specific Delta Accessors ==========

    /**
     * @brief Get the set of updated slot indices.
     * @return Const reference to the updated slots set
     */
    [[nodiscard]] const SlotSet& updated() const {
        return updated_;
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
        return key_delta_.was_slot_added(slot) || updated_.contains(slot);
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
            modified_.insert(key_delta_.added().begin(), key_delta_.added().end());
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
     * @brief Check if there are no delta changes.
     *
     * Empty means no additions, no removals, no updates, and not cleared.
     *
     * @return true if delta is empty (no changes to report)
     */
    [[nodiscard]] bool empty() const {
        return key_delta_.empty() && updated_.empty();
    }

    // ========== State Management ==========

    /**
     * @brief Reset all delta state.
     *
     * Called at the start of each tick to clear accumulated delta.
     * Resets key_delta_, updated, children, and the cleared flag.
     */
    void clear() {
        key_delta_.clear();
        updated_.clear();
        // Invalidate cached modified set
        modified_.clear();
        modified_valid_ = false;
        // Reset children to monostate
        for (auto& child : children_) {
            child = std::monostate{};
        }
    }

private:
    SetDelta key_delta_;  // Embedded SetDelta for key add/remove (COMPOSITION)
    SlotSet updated_;     // Slots updated this tick (MapDelta-specific)
    std::vector<DeltaVariant> children_;  // Child deltas for nested TS types

    // Cached combined modified set (lazily computed)
    mutable SlotSet modified_;
    mutable bool modified_valid_{false};
};

} // namespace hgraph
