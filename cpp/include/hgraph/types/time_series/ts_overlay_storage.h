#pragma once

/**
 * @file ts_overlay_storage.h
 * @brief TS overlay storage for hierarchical modification tracking and observers.
 *
 * The TS overlay system provides parallel per-element metadata (timestamps + observers)
 * that mirrors the structure of the Value data tree. This enables hierarchical
 * modification tracking where changes propagate upward to parents.
 *
 * Key concepts:
 * - Delta is NOT stored - computed dynamically from timestamps
 * - Observers are lazily allocated (no cost until first subscription)
 * - Timestamps propagate upward to parents
 * - No backward compatibility needed for C++ internals
 *
 * Phase 2 Task 1: Base interface and scalar implementation
 */

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/container_hooks.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/feature_extension.h>

#include <any>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace hgraph {

// Forward declarations
struct TSOverlayStorage;

/**
 * @brief Lazy observer list for a specific TS level.
 *
 * Observers are allocated on first subscription to minimize memory overhead.
 * Notifications propagate upward to parent observers.
 */
class ObserverList {
public:
    ObserverList() = default;
    ~ObserverList() = default;

    // No copying - unique ownership of observer sets
    ObserverList(const ObserverList&) = delete;
    ObserverList& operator=(const ObserverList&) = delete;

    // Move semantics
    ObserverList(ObserverList&&) noexcept = default;
    ObserverList& operator=(ObserverList&&) noexcept = default;

    /**
     * @brief Add an observer to this level.
     * @param observer The notifiable observer to add
     */
    void subscribe(Notifiable* observer);

    /**
     * @brief Remove an observer from this level.
     * @param observer The notifiable observer to remove
     */
    void unsubscribe(Notifiable* observer);

    /**
     * @brief Check if an observer is subscribed at this level.
     * @param observer The notifiable observer to check
     * @return True if the observer is subscribed
     */
    [[nodiscard]] bool is_subscribed(Notifiable* observer) const noexcept {
        return observer && _observers.find(observer) != _observers.end();
    }

    /**
     * @brief Check if there are any observers at this level.
     * @return True if at least one observer is subscribed
     */
    [[nodiscard]] bool has_observers() const noexcept {
        return !_observers.empty();
    }

    /**
     * @brief Notify all observers at this level.
     * @param time The engine time of the modification
     */
    void notify(engine_time_t time);

private:
    std::unordered_set<Notifiable*> _observers;
};

/**
 * @brief Base class for TS overlay storage.
 *
 * Provides the fundamental interface for hierarchical modification tracking
 * and observer management. Each TS overlay storage node tracks:
 * - Last modification time (for delta computation)
 * - Parent link (for upward propagation)
 * - Optional observers (lazy allocation)
 *
 * Modifications propagate upward: when a child is modified, the parent's
 * timestamp is also updated. This ensures that querying modification state
 * at any level reflects changes in descendants.
 */
struct TSOverlayStorage {
    /**
     * @brief Virtual destructor for proper cleanup in derived classes.
     */
    virtual ~TSOverlayStorage() = default;

    // ========== Modification Time Queries ==========

    /**
     * @brief Get the last modification time.
     * @return The engine time when this overlay was last modified, or MIN_DT if never modified
     */
    [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

    /**
     * @brief Check if this overlay was modified at a specific time.
     * @param time The time to check against
     * @return True if last_modified_time() == time
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return last_modified_time() == time;
    }

    /**
     * @brief Check if this overlay has valid data (has been set at least once).
     * @return True if last_modified_time() > MIN_DT
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() > MIN_DT;
    }

    // ========== Modification State Management ==========

    /**
     * @brief Mark this overlay as modified at a specific time.
     *
     * Updates the local timestamp and propagates the change to the parent.
     * This is the core operation that maintains hierarchical consistency.
     *
     * @param time The engine time of the modification (must be monotonically increasing)
     */
    virtual void mark_modified(engine_time_t time) = 0;

    /**
     * @brief Mark this overlay as invalid (never been set).
     *
     * Sets the timestamp to MIN_DT and notifies observers.
     * Note: Does NOT propagate to parent (invalidation is local).
     */
    virtual void mark_invalid() = 0;

    // ========== Parent Chain Management ==========

    /**
     * @brief Set the parent overlay for upward propagation.
     * @param parent Pointer to the parent overlay (can be nullptr for root)
     */
    void set_parent(TSOverlayStorage* parent) noexcept {
        _parent = parent;
    }

    /**
     * @brief Get the parent overlay.
     * @return Pointer to parent, or nullptr if this is the root
     */
    [[nodiscard]] TSOverlayStorage* parent() const noexcept {
        return _parent;
    }

    /**
     * @brief Propagate a modification timestamp to the parent.
     *
     * Called by mark_modified() to ensure parent timestamps reflect child changes.
     *
     * @param time The modification time to propagate
     */
    void propagate_modified_to_parent(engine_time_t time) {
        if (_parent) {
            _parent->mark_modified(time);
        }
    }

    // ========== Observer Management ==========

    /**
     * @brief Get the observer list (may be null if no observers).
     * @return Pointer to observer list, or nullptr if not yet allocated
     */
    [[nodiscard]] ObserverList* observers() const noexcept {
        return _observers.get();
    }

    /**
     * @brief Ensure the observer list exists and return a reference.
     *
     * Lazily allocates the observer list on first access.
     *
     * @return Reference to the observer list
     */
    ObserverList& ensure_observers() {
        if (!_observers) {
            _observers = std::make_unique<ObserverList>();
        }
        return *_observers;
    }

    /**
     * @brief Subscribe an observer to this overlay (convenience method).
     *
     * This is a convenience wrapper around ensure_observers().subscribe().
     * Lazily allocates the observer list if needed.
     *
     * @param observer The notifiable observer to add
     */
    void subscribe(Notifiable* observer) {
        ensure_observers().subscribe(observer);
    }

    /**
     * @brief Unsubscribe an observer from this overlay (convenience method).
     *
     * This is a convenience wrapper. Does nothing if observer list is not allocated.
     *
     * @param observer The notifiable observer to remove
     */
    void unsubscribe(Notifiable* observer) {
        if (_observers) {
            _observers->unsubscribe(observer);
        }
    }

    /**
     * @brief Check if an observer is subscribed to this overlay (convenience method).
     *
     * This is a convenience wrapper. Returns false if observer list is not allocated.
     *
     * @param observer The notifiable observer to check
     * @return True if the observer is subscribed at this level
     */
    [[nodiscard]] bool is_subscribed(Notifiable* observer) const noexcept {
        return _observers && _observers->is_subscribed(observer);
    }

    // ========== Bound Output Storage (for TSD REF elements) ==========

    /**
     * @brief Set a bound Python output for this overlay.
     *
     * Used by TSD REF elements when bind_output() is called. The binding is stored
     * in the overlay so it persists across Python wrapper instances.
     *
     * @param output The bound Python output wrapped in std::any
     */
    void set_bound_output(std::any output) {
        _bound_output = std::move(output);
    }

    /**
     * @brief Get the bound Python output for this overlay.
     * @return The stored bound output, or empty std::any if not set
     */
    [[nodiscard]] const std::any& bound_output() const noexcept {
        return _bound_output;
    }

    /**
     * @brief Check if this overlay has a bound output.
     * @return True if a bound output has been set
     */
    [[nodiscard]] bool has_bound_output() const noexcept {
        return _bound_output.has_value();
    }

    /**
     * @brief Clear the bound output.
     */
    void clear_bound_output() {
        _bound_output.reset();
    }

protected:
    TSOverlayStorage* _parent{nullptr};               ///< Parent overlay for upward propagation
    std::unique_ptr<ObserverList> _observers{nullptr}; ///< Lazy observer list
    std::any _bound_output;                           ///< Bound Python output for TSD REF elements
};

/**
 * @brief Scalar TS overlay storage.
 *
 * Simplest overlay implementation for scalar TS types (TS<int>, TS<float>, etc.).
 * Stores a single timestamp and optional observer list.
 *
 * Usage:
 * @code
 * ScalarTSOverlay overlay;
 * overlay.mark_modified(current_time);
 * if (overlay.modified_at(current_time)) {
 *     // Value changed this cycle
 * }
 * @endcode
 */
class ScalarTSOverlay : public TSOverlayStorage {
public:
    /**
     * @brief Construct a scalar overlay with invalid initial state.
     */
    ScalarTSOverlay() = default;

    /**
     * @brief Virtual destructor.
     */
    ~ScalarTSOverlay() override = default;

    // No copying - overlays have unique identity
    ScalarTSOverlay(const ScalarTSOverlay&) = delete;
    ScalarTSOverlay& operator=(const ScalarTSOverlay&) = delete;

    // Move semantics (must be explicitly defined due to base class unique_ptr)
    ScalarTSOverlay(ScalarTSOverlay&&) noexcept;
    ScalarTSOverlay& operator=(ScalarTSOverlay&&) noexcept;

    // ========== Modification Time Queries ==========

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _last_modified_time;
    }

    // ========== Modification State Management ==========

    void mark_modified(engine_time_t time) override;
    void mark_invalid() override;

private:
    engine_time_t _last_modified_time{MIN_DT}; ///< Timestamp of last modification
};

// Forward declarations
struct TSMeta;
struct TSBTypeMeta;
struct TSLTypeMeta;

/**
 * @brief Composite TS overlay storage for Bundle (TSB) and Tuple types.
 *
 * Manages per-field child overlays with a fixed number of children.
 * This overlay provides hierarchical modification tracking where:
 * - Each child field has its own overlay
 * - Parent timestamp is updated when any child is modified
 * - Observer notifications propagate from children to parent
 *
 * The number of children is determined by the TSMeta schema provided
 * during construction (for bundles, this is the field count).
 *
 * Usage:
 * @code
 * // Create composite overlay for a bundle with 3 fields
 * CompositeTSOverlay overlay(bundle_ts_meta);
 *
 * // Access child by index
 * auto* field_overlay = overlay.child(0);
 * field_overlay->mark_modified(current_time);
 *
 * // Parent is automatically updated
 * assert(overlay.modified_at(current_time));
 *
 * // Access child by name (for bundles)
 * auto* named_field = overlay.child("field_name");
 * @endcode
 */
class CompositeTSOverlay : public TSOverlayStorage {
public:
    /**
     * @brief Construct a composite overlay with children based on the TS schema.
     *
     * Creates child overlays recursively based on the schema structure.
     * For TSB types, creates one child overlay per field.
     *
     * @param ts_meta The time-series metadata defining the structure
     */
    explicit CompositeTSOverlay(const TSMeta* ts_meta);

    /**
     * @brief Virtual destructor.
     */
    ~CompositeTSOverlay() override = default;

    // No copying - overlays have unique identity
    CompositeTSOverlay(const CompositeTSOverlay&) = delete;
    CompositeTSOverlay& operator=(const CompositeTSOverlay&) = delete;

    // Move semantics
    CompositeTSOverlay(CompositeTSOverlay&&) noexcept;
    CompositeTSOverlay& operator=(CompositeTSOverlay&&) noexcept;

    // ========== Modification Time Queries ==========

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _last_modified_time;
    }

    // ========== Modification State Management ==========

    void mark_modified(engine_time_t time) override;
    void mark_invalid() override;

    // ========== Child Navigation ==========

    /**
     * @brief Get the number of child overlays.
     * @return The number of children (field count for bundles)
     */
    [[nodiscard]] size_t child_count() const noexcept {
        return _children.size();
    }

    /**
     * @brief Get a child overlay by index.
     * @param index The child index (0-based)
     * @return Pointer to the child overlay, or nullptr if index is out of range
     */
    [[nodiscard]] TSOverlayStorage* child(size_t index) noexcept {
        return index < _children.size() ? _children[index].get() : nullptr;
    }

    /**
     * @brief Get a child overlay by index (const version).
     * @param index The child index (0-based)
     * @return Pointer to the child overlay, or nullptr if index is out of range
     */
    [[nodiscard]] const TSOverlayStorage* child(size_t index) const noexcept {
        return index < _children.size() ? _children[index].get() : nullptr;
    }

    /**
     * @brief Get a child overlay by name (for bundles).
     * @param name The field name
     * @return Pointer to the child overlay, or nullptr if field not found
     */
    [[nodiscard]] TSOverlayStorage* child(std::string_view name) noexcept;

    /**
     * @brief Get a child overlay by name (const version, for bundles).
     * @param name The field name
     * @return Pointer to the child overlay, or nullptr if field not found
     */
    [[nodiscard]] const TSOverlayStorage* child(std::string_view name) const noexcept;

    // ========== Delta Query ==========

    /**
     * @brief Get indices of fields whose values were modified at the given time.
     *
     * Computes modified indices by checking which child overlays have
     * last_modified_time == time.
     *
     * @param time The current engine time
     * @return Vector of modified field indices
     */
    [[nodiscard]] std::vector<size_t> modified_indices(engine_time_t time) const;

    /**
     * @brief Check if any fields were modified at the given time.
     *
     * @param time The current engine time
     * @return True if at least one field was modified
     */
    [[nodiscard]] bool has_modified(engine_time_t time) const;

private:
    /**
     * @brief Create a child overlay for a given TS type.
     *
     * Factory method for recursive overlay creation. Delegates to the
     * unified make_ts_overlay() factory function.
     *
     * @param child_ts_meta The time-series metadata for the child
     * @return Unique pointer to the created overlay
     */
    static std::unique_ptr<TSOverlayStorage> create_child_overlay(const TSMeta* child_ts_meta);

    engine_time_t _last_modified_time{MIN_DT};                     ///< Timestamp of last modification
    std::vector<std::unique_ptr<TSOverlayStorage>> _children;      ///< Child overlays (one per field)
    const TSBTypeMeta* _bundle_meta{nullptr};                      ///< Bundle metadata (for name lookup, nullptr for non-bundles)
};

/**
 * @brief List TS overlay storage for TSL (time-series list) with dynamic children.
 *
 * Manages per-element child overlays with a variable number of children.
 * This overlay provides hierarchical modification tracking where:
 * - Each list element has its own overlay
 * - Parent timestamp is updated when any child is modified
 * - Observer notifications propagate from children to parent
 * - Children are created/destroyed as the list size changes
 *
 * The element type is determined by the TSLTypeMeta schema provided
 * during construction.
 *
 * Usage:
 * @code
 * // Create list overlay for a TSL[TS[int], 0] (dynamic list)
 * ListTSOverlay overlay(tsl_ts_meta);
 *
 * // Add a new element
 * auto* new_element = overlay.push_back();
 * new_element->mark_modified(current_time);
 *
 * // Parent is automatically updated
 * assert(overlay.modified_at(current_time));
 *
 * // Access element by index
 * auto* elem = overlay.child(0);
 *
 * // Resize the list
 * overlay.resize(5);  // Creates 5 elements total
 *
 * // Remove last element
 * overlay.pop_back();
 * @endcode
 */
class ListTSOverlay : public TSOverlayStorage {
public:
    /**
     * @brief Construct a list overlay with element type from the TS schema.
     *
     * Creates an empty list overlay. Elements are added via push_back() or resize().
     *
     * @param ts_meta The time-series metadata defining the list structure (must be TSLTypeMeta)
     */
    explicit ListTSOverlay(const TSMeta* ts_meta);

    /**
     * @brief Virtual destructor.
     */
    ~ListTSOverlay() override = default;

    // No copying - overlays have unique identity
    ListTSOverlay(const ListTSOverlay&) = delete;
    ListTSOverlay& operator=(const ListTSOverlay&) = delete;

    // Move semantics
    ListTSOverlay(ListTSOverlay&&) noexcept;
    ListTSOverlay& operator=(ListTSOverlay&&) noexcept;

    // ========== Modification Time Queries ==========

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _last_modified_time;
    }

    // ========== Modification State Management ==========

    void mark_modified(engine_time_t time) override;
    void mark_invalid() override;

    // ========== Dynamic Child Management ==========

    /**
     * @brief Resize the list to a new size.
     *
     * If new_size > current size: creates new child overlays at the end
     * If new_size < current size: removes child overlays from the end
     * If new_size == current size: no-op
     *
     * All new children have their parent pointer set to this overlay.
     *
     * @param new_size The new number of elements
     */
    void resize(size_t new_size);

    /**
     * @brief Add a new child overlay at the end of the list.
     *
     * Creates a new child overlay based on the element type schema.
     * The new child has its parent pointer set to this overlay.
     *
     * @return Pointer to the newly created child overlay
     */
    TSOverlayStorage* push_back();

    /**
     * @brief Remove the last child overlay.
     *
     * Does nothing if the list is already empty.
     */
    void pop_back();

    /**
     * @brief Remove all child overlays.
     *
     * After this call, child_count() will return 0.
     */
    void clear();

    // ========== Child Navigation ==========

    /**
     * @brief Get the number of child overlays.
     * @return The current number of elements in the list
     */
    [[nodiscard]] size_t child_count() const noexcept {
        return _children.size();
    }

    /**
     * @brief Get a child overlay by index.
     * @param index The child index (0-based)
     * @return Pointer to the child overlay, or nullptr if index is out of range
     */
    [[nodiscard]] TSOverlayStorage* child(size_t index) noexcept {
        return index < _children.size() ? _children[index].get() : nullptr;
    }

    /**
     * @brief Get a child overlay by index (const version).
     * @param index The child index (0-based)
     * @return Pointer to the child overlay, or nullptr if index is out of range
     */
    [[nodiscard]] const TSOverlayStorage* child(size_t index) const noexcept {
        return index < _children.size() ? _children[index].get() : nullptr;
    }

    // ========== Delta Query ==========

    /**
     * @brief Get indices of elements whose values were modified at the given time.
     *
     * Computes modified indices by checking which child overlays have
     * last_modified_time == time.
     *
     * @param time The current engine time
     * @return Vector of modified element indices
     */
    [[nodiscard]] std::vector<size_t> modified_indices(engine_time_t time) const;

    /**
     * @brief Check if any elements were modified at the given time.
     *
     * @param time The current engine time
     * @return True if at least one element was modified
     */
    [[nodiscard]] bool has_modified(engine_time_t time) const;

private:
    /**
     * @brief Create a new child overlay for a list element.
     *
     * Uses the element type schema to create the appropriate overlay type.
     * Delegates to the unified make_ts_overlay() factory function.
     *
     * @return Unique pointer to the created overlay
     */
    std::unique_ptr<TSOverlayStorage> create_child_overlay();

    engine_time_t _last_modified_time{MIN_DT};                     ///< Timestamp of last modification
    std::vector<std::unique_ptr<TSOverlayStorage>> _children;      ///< Child overlays (one per element)
    const TSMeta* _element_type{nullptr};                          ///< Element type schema for creating new children
};

/**
 * @brief Set TS overlay storage for TSS (time-series set) with added/removed buffers.
 *
 * TSS contains scalar values (not time-series), so instead of per-element timestamps,
 * we track:
 * - Container-level modification timestamp
 * - Added indices buffer: slots that had elements added this tick
 * - Removed indices buffer: slots that had elements removed this tick
 * - Removed values buffer: actual values that were removed (for delta access)
 *
 * Delta tracking features:
 * - **Lazy cleanup**: Buffers are automatically cleared when a modification occurs
 *   at a different time than the last modification (no explicit clear_delta() needed)
 * - **Time-checked queries**: Query methods check if current time matches last_modified_time
 * - **Removed value buffering**: Removed values are stored until next tick, allowing
 *   downstream consumers to access the removed values during delta processing
 *
 * Usage:
 * @code
 * // Create set overlay for a TSS[int]
 * SetTSOverlay overlay(tss_ts_meta);
 *
 * // Get hooks for the backing store
 * auto hooks = overlay.make_hooks();
 *
 * // When inserting into the backing store:
 * overlay.record_added(idx, current_time);
 * // Buffers auto-clear if time changed since last modification
 *
 * // When erasing from the backing store (pass value to buffer it):
 * overlay.record_removed(idx, current_time, removed_value);
 *
 * // Query delta - returns true if time matches and there's delta to process
 * if (overlay.has_delta_at(current_time)) {
 *     const auto& added = overlay.added_indices();
 *     const auto& removed = overlay.removed_indices();
 *     const auto& removed_vals = overlay.removed_values();  // Access removed values
 * }
 * @endcode
 */
class SetTSOverlay : public TSOverlayStorage {
public:
    /**
     * @brief Construct a set overlay with element type from the TS schema.
     *
     * Creates an empty overlay with empty added/removed buffers.
     *
     * @param ts_meta The time-series metadata defining the set structure (must be TSSTypeMeta)
     */
    explicit SetTSOverlay(const TSMeta* ts_meta);

    /**
     * @brief Virtual destructor.
     */
    ~SetTSOverlay() override = default;

    // No copying - overlays have unique identity
    SetTSOverlay(const SetTSOverlay&) = delete;
    SetTSOverlay& operator=(const SetTSOverlay&) = delete;

    // Move semantics
    SetTSOverlay(SetTSOverlay&&) noexcept;
    SetTSOverlay& operator=(SetTSOverlay&&) noexcept;

    // ========== Modification Time Queries ==========

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _last_modified_time;
    }

    // ========== Modification State Management ==========

    void mark_modified(engine_time_t time) override;
    void mark_invalid() override;

    // ========== Delta Query with Time Check ==========

    /**
     * @brief Check if there is delta at the given time.
     *
     * Returns true if:
     * - The given time matches _last_modified_time AND
     * - There is at least one added or removed element
     *
     * If the time doesn't match, this clears the delta buffers and returns false.
     * This provides lazy cleanup - no explicit clear_delta() call needed.
     *
     * @param time The current engine time to check against
     * @return True if there is valid delta at this time
     */
    [[nodiscard]] bool has_delta_at(engine_time_t time) {
        if (time != _last_modified_time) {
            clear_delta_buffers();
            return false;
        }
        return !_added_indices.empty() || !_removed_indices.empty();
    }

    /**
     * @brief Get indices of elements added this tick (if time matches).
     *
     * Caller should first call has_delta_at() to verify the time matches.
     *
     * @return Reference to the added indices vector
     */
    [[nodiscard]] const std::vector<size_t>& added_indices() const noexcept {
        return _added_indices;
    }

    /**
     * @brief Get indices of elements removed this tick (if time matches).
     *
     * Caller should first call has_delta_at() to verify the time matches.
     *
     * @return Reference to the removed indices vector
     */
    [[nodiscard]] const std::vector<size_t>& removed_indices() const noexcept {
        return _removed_indices;
    }

    /**
     * @brief Get values of elements removed this tick (if time matches).
     *
     * The removed values correspond to the indices in removed_indices().
     * Caller should first call has_delta_at() to verify the time matches.
     *
     * @return Reference to the removed values vector
     */
    [[nodiscard]] const std::vector<value::PlainValue>& removed_values() const noexcept {
        return _removed_values;
    }

    /**
     * @brief Check if there are any added elements (without time check).
     * @return True if at least one element was added
     */
    [[nodiscard]] bool has_added() const noexcept {
        return !_added_indices.empty();
    }

    /**
     * @brief Check if there are any removed elements (without time check).
     * @return True if at least one element was removed
     */
    [[nodiscard]] bool has_removed() const noexcept {
        return !_removed_indices.empty();
    }

    // ========== O(1) Element Lookup ==========

    /**
     * @brief Check if a specific element was added this tick (O(1) lookup).
     *
     * Uses hash set for O(1) containment check.
     *
     * @param element The element to check
     * @return True if the element was added this tick
     */
    [[nodiscard]] bool was_added_element(const value::ConstValueView& element) const;

    /**
     * @brief Check if a specific element was removed this tick (O(1) lookup).
     *
     * Uses hash set for O(1) containment check.
     *
     * @param element The element to check
     * @return True if the element was removed this tick
     */
    [[nodiscard]] bool was_removed_element(const value::ConstValueView& element) const;

    /**
     * @brief Record an element as added at a specific index.
     *
     * If the time differs from _last_modified_time, the delta buffers are
     * cleared first (lazy cleanup).
     *
     * @param index The backing store slot index
     * @param time The engine time when the element was added
     * @param added_value The value being added (optional, for O(1) lookup support)
     */
    void record_added(size_t index, engine_time_t time, value::PlainValue added_value = {});

    /**
     * @brief Record an element as removed at a specific index.
     *
     * If the time differs from _last_modified_time, the delta buffers are
     * cleared first (lazy cleanup). The removed value is buffered so it can
     * be accessed until the delta is cleared.
     *
     * @param index The backing store slot index being removed
     * @param time The engine time when the element was removed
     * @param removed_value The value being removed (will be moved into buffer)
     */
    void record_removed(size_t index, engine_time_t time, value::PlainValue removed_value);

    // ========== Container Hook Integration ==========

    /**
     * @brief Create container hooks for this overlay.
     *
     * Returns a ContainerHooks structure that the backing store can use
     * to notify this overlay of insert/swap/erase operations.
     *
     * Note: The hooks handle swap operations. The caller must call
     * record_added()/record_removed() with the time for add/remove tracking.
     *
     * @return ContainerHooks structure with callbacks pointing to this overlay
     */
    [[nodiscard]] value::ContainerHooks make_hooks() noexcept {
        value::ContainerHooks hooks;
        hooks.ctx = this;
        hooks.on_insert = &SetTSOverlay::hook_on_insert;
        hooks.on_swap = &SetTSOverlay::hook_on_swap;
        hooks.on_erase = &SetTSOverlay::hook_on_erase;
        return hooks;
    }

private:
    // ========== Internal Methods ==========

    /**
     * @brief Clear delta buffers and removed values (internal use).
     */
    void clear_delta_buffers() {
        _added_indices.clear();
        _removed_indices.clear();
        _removed_values.clear();
        _added_values.clear();
        // Clear the lookup sets
        if (_added_values_set.valid()) {
            _added_values_set.view().as_set().clear();
        }
        if (_removed_values_set.valid()) {
            _removed_values_set.view().as_set().clear();
        }
    }

    /**
     * @brief Check and reset delta buffers if time changed.
     *
     * Called before recording new delta to ensure buffers are fresh.
     *
     * @param time The current modification time
     */
    void maybe_reset_delta(engine_time_t time) {
        if (time != _last_modified_time && _last_modified_time != MIN_DT) {
            clear_delta_buffers();
        }
    }

    // ========== Hook Callbacks (Static) ==========

    static void hook_on_insert(void* ctx, size_t index);
    static void hook_on_swap(void* ctx, size_t index_a, size_t index_b);
    static void hook_on_erase(void* ctx, size_t index);

    engine_time_t _last_modified_time{MIN_DT};       ///< Timestamp of last modification
    std::vector<size_t> _added_indices;               ///< Indices of elements added this tick
    std::vector<size_t> _removed_indices;             ///< Indices of elements removed this tick
    std::vector<value::PlainValue> _removed_values;    ///< Buffered removed values
    std::vector<value::PlainValue> _added_values;      ///< Buffered added values
    value::PlainValue _added_values_set;               ///< Hash set for O(1) added lookup
    value::PlainValue _removed_values_set;             ///< Hash set for O(1) removed lookup
    const TSMeta* _element_type{nullptr};             ///< Element type schema
};

// Forward declaration for KeySetOverlayView
class KeySetOverlayView;

/**
 * @brief Map TS overlay storage for TSD (time-series dict/map) with added/removed key buffers.
 *
 * Manages per-entry modification tracking aligned with the backing store slots.
 * Since TSD[K, V] maps scalar keys to time-series values:
 * - Keys are scalars: tracked via added/removed buffers (like TSS)
 * - Values are time-series: tracked via child overlays
 *
 * This design separates key tracking from value tracking:
 * - Added key indices buffer: slots that had keys added this tick
 * - Removed key indices buffer: slots that had keys removed this tick
 * - Per-entry value_overlay: child TSOverlayStorage for each TS value
 *
 * Delta computation:
 * - added_keys: indices in _added_key_indices buffer
 * - removed_keys: indices in _removed_key_indices buffer
 * - modified_values: entries where value_overlay->modified_at(current_time)
 *
 * Usage:
 * @code
 * // Create map overlay for TSD[str, TS[int]]
 * MapTSOverlay overlay(tsd_ts_meta);
 *
 * // Get hooks for the backing store
 * auto hooks = overlay.make_hooks();
 *
 * // When inserting a new key-value pair:
 * auto result = map_view.set_with_index(key, value, hooks);
 * if (result.inserted) {
 *     overlay.record_key_added(result.index, current_time);
 * }
 *
 * // Get the value's overlay for modification tracking
 * auto* value_overlay = overlay.value_overlay(result.index);
 * value_overlay->mark_modified(current_time);
 *
 * // Query delta for current tick
 * const auto& added_keys = overlay.added_key_indices();
 * const auto& removed_keys = overlay.removed_key_indices();
 *
 * // Clear buffers for next tick
 * overlay.clear_delta();
 * @endcode
 */
class MapTSOverlay : public TSOverlayStorage {
public:
    /**
     * @brief Construct a map overlay with value type from the TS schema.
     *
     * Creates an empty overlay with empty added/removed buffers.
     *
     * @param ts_meta The time-series metadata defining the map structure (must be TSDTypeMeta)
     */
    explicit MapTSOverlay(const TSMeta* ts_meta);

    /**
     * @brief Virtual destructor.
     * Clears Python references in _get_ref_outputs before destruction to prevent
     * access to invalidated C++ objects during Python GC.
     */
    ~MapTSOverlay() override {
        // Clear Python references before C++ objects are destroyed
        // This prevents crashes during Python GC when nb::object tries to access
        // C++ objects that have been deallocated
        _get_ref_outputs.clear();
    }

    // No copying - overlays have unique identity
    MapTSOverlay(const MapTSOverlay&) = delete;
    MapTSOverlay& operator=(const MapTSOverlay&) = delete;

    // Move semantics
    MapTSOverlay(MapTSOverlay&&) noexcept;
    MapTSOverlay& operator=(MapTSOverlay&&) noexcept;

    // ========== Modification Time Queries ==========

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _last_modified_time;
    }

    // ========== Modification State Management ==========

    void mark_modified(engine_time_t time) override;
    void mark_invalid() override;

    // ========== Delta Query with Time Check ==========

    /**
     * @brief Check if there is delta at the given time.
     *
     * Returns true if:
     * - The given time matches _last_delta_time AND
     * - There is at least one added or removed key
     *
     * If the time doesn't match, this clears the delta buffers and returns false.
     * This provides lazy cleanup - no explicit clear_delta() call needed.
     *
     * Note: Uses _last_delta_time instead of _last_modified_time because
     * child value modifications can propagate up and change the container's
     * _last_modified_time without being structural changes.
     *
     * @param time The current engine time to check against
     * @return True if there is valid delta at this time
     */
    [[nodiscard]] bool has_delta_at(engine_time_t time) {
        if (time != _last_delta_time) {
            clear_delta_buffers();
            _last_delta_time = MIN_DT;  // Reset delta time
            return false;
        }
        return !_added_key_indices.empty() || !_removed_key_indices.empty();
    }

    // ========== Delta Buffers for Keys ==========

    /**
     * @brief Get indices of keys added this tick.
     * @return Reference to the added key indices vector
     */
    [[nodiscard]] const std::vector<size_t>& added_key_indices() const noexcept {
        return _added_key_indices;
    }

    /**
     * @brief Get indices of keys removed this tick.
     * @return Reference to the removed key indices vector
     */
    [[nodiscard]] const std::vector<size_t>& removed_key_indices() const noexcept {
        return _removed_key_indices;
    }

    /**
     * @brief Get the buffered removed key values.
     *
     * When a key is removed, its value is stored here so it can be accessed
     * until the delta is cleared at the next tick. Corresponds 1:1 with
     * removed_key_indices().
     *
     * @return Reference to the removed key values vector
     */
    [[nodiscard]] const std::vector<value::PlainValue>& removed_key_values() const noexcept {
        return _removed_key_values;
    }

    /**
     * @brief Get the buffered removed value overlays.
     *
     * When a key is removed, its value overlay is moved here so the value
     * can still be accessed until the delta is cleared at the next tick.
     * Corresponds 1:1 with removed_key_indices().
     *
     * @return Reference to the removed value overlays vector
     */
    [[nodiscard]] const std::vector<std::unique_ptr<TSOverlayStorage>>& removed_value_overlays() const noexcept {
        return _removed_value_overlays;
    }

    /**
     * @brief Check if there are any added keys this tick.
     * @return True if at least one key was added
     */
    [[nodiscard]] bool has_added_keys() const noexcept {
        return !_added_key_indices.empty();
    }

    /**
     * @brief Check if there are any removed keys this tick.
     * @return True if at least one key was removed
     */
    [[nodiscard]] bool has_removed_keys() const noexcept {
        return !_removed_key_indices.empty();
    }

    /**
     * @brief Get indices of keys whose values were modified this tick.
     *
     * Computes modified keys by checking which value overlays have
     * last_modified_time == time and are not in added_key_indices.
     *
     * @param time The current engine time
     * @return Vector of modified key indices
     */
    [[nodiscard]] std::vector<size_t> modified_key_indices(engine_time_t time) const;

    /**
     * @brief Check if there are any modified keys this tick.
     *
     * A key is considered modified if its value overlay's last_modified_time
     * equals the given time and the key is not newly added.
     *
     * @param time The current engine time
     * @return True if at least one existing key's value was modified
     */
    [[nodiscard]] bool has_modified_keys(engine_time_t time) const;

    /**
     * @brief Record a key as added at a specific index.
     *
     * Called after inserting a new key into the backing store.
     * Also creates the child overlay for the value if it doesn't exist.
     * If the time differs from _last_modified_time, the delta buffers are
     * cleared first (lazy cleanup).
     *
     * @param index The backing store slot index
     * @param time The engine time when the key was added
     */
    void record_key_added(size_t index, engine_time_t time);

    /**
     * @brief Record a key as removed at a specific index.
     *
     * Called before erasing a key from the backing store.
     * Both the key value and its value overlay are buffered so they can
     * still be accessed until the delta is cleared.
     * If the time differs from _last_delta_time, the delta buffers are
     * cleared first (lazy cleanup).
     *
     * @param index The backing store slot index being removed
     * @param time The engine time when the key was removed
     * @param removed_key The key value being removed (will be moved into buffer)
     */
    void record_key_removed(size_t index, engine_time_t time, value::PlainValue removed_key);

    // ========== Per-Entry Value Overlay Access ==========

    /**
     * @brief Get the value overlay for a specific entry.
     *
     * Returns the child TSOverlayStorage for the entry's time-series value.
     * The caller can use this to track modifications to the value.
     *
     * @param index The backing store slot index
     * @return Pointer to the value's overlay, or nullptr if slot is empty
     */
    [[nodiscard]] TSOverlayStorage* value_overlay(size_t index) noexcept {
        return index < _value_overlays.size() ? _value_overlays[index].get() : nullptr;
    }

    [[nodiscard]] const TSOverlayStorage* value_overlay(size_t index) const noexcept {
        return index < _value_overlays.size() ? _value_overlays[index].get() : nullptr;
    }

    /**
     * @brief Ensure a value overlay exists for a specific entry.
     *
     * Creates the child overlay if it doesn't exist.
     *
     * @param index The backing store slot index
     * @return Pointer to the value's overlay
     */
    TSOverlayStorage* ensure_value_overlay(size_t index);

    // ========== Entry Count ==========

    /**
     * @brief Get the number of allocated value overlay slots.
     *
     * @return The number of value overlay slots (may include empty slots)
     */
    [[nodiscard]] size_t entry_count() const noexcept {
        return _value_overlays.size();
    }

    /**
     * @brief Pre-allocate value overlay slots.
     *
     * Reserves capacity in the value overlay vector to avoid reallocations.
     *
     * @param n The number of slots to reserve
     */
    void reserve(size_t n) {
        _value_overlays.reserve(n);
    }

    // ========== Container Hook Integration ==========

    /**
     * @brief Create container hooks for this overlay.
     *
     * Returns a ContainerHooks structure that the backing store can use
     * to notify this overlay of insert/swap/erase operations.
     *
     * Note: The hooks handle slot management. The caller must call
     * record_key_added() with the time after insertion.
     *
     * @return ContainerHooks structure with callbacks pointing to this overlay
     */
    [[nodiscard]] value::ContainerHooks make_hooks() noexcept {
        value::ContainerHooks hooks;
        hooks.ctx = this;
        hooks.on_insert = &MapTSOverlay::hook_on_insert;
        hooks.on_swap = &MapTSOverlay::hook_on_swap;
        hooks.on_erase = &MapTSOverlay::hook_on_erase;
        return hooks;
    }

    /**
     * @brief Get the value type schema for creating child overlays.
     */
    [[nodiscard]] const TSMeta* value_type() const noexcept { return _value_type; }

    /**
     * @brief Get a key set view for SetTSOverlay-compatible key tracking.
     *
     * This mirrors how TSD exposes key_set() on the value side. The returned
     * view provides the same interface as SetTSOverlay for key modification
     * tracking (added_indices, removed_indices, removed_values).
     *
     * @return KeySetOverlayView wrapping this map's key tracking
     */
    [[nodiscard]] KeySetOverlayView key_set_view() noexcept;

    // ========== Is-Empty Tracking ==========

    /**
     * @brief Get the is_empty overlay for this map.
     *
     * This overlay tracks when the map's empty state changes.
     * It's marked as modified when the map transitions from empty to non-empty
     * or vice versa.
     *
     * @return Reference to the is_empty overlay
     */
    [[nodiscard]] ScalarTSOverlay& is_empty_overlay() noexcept { return _is_empty_overlay; }
    [[nodiscard]] const ScalarTSOverlay& is_empty_overlay() const noexcept { return _is_empty_overlay; }

    /**
     * @brief Get the current is_empty value.
     *
     * @return True if the map is considered empty based on the last update
     */
    [[nodiscard]] bool is_empty_value() const noexcept { return _is_empty_value; }

    /**
     * @brief Update the is_empty state based on current map size.
     *
     * Call this after keys are added or removed. If the is_empty state
     * changed (map went from empty to non-empty or vice versa), the
     * is_empty overlay is marked as modified.
     *
     * @param time The current engine time
     * @param current_size The current number of keys in the map
     */
    void update_is_empty_state(engine_time_t time, size_t current_size);

private:
    // ========== Internal Methods ==========

    /**
     * @brief Clear delta buffers, removed key values, and removed value overlays (internal use).
     */
    void clear_delta_buffers() {
        _added_key_indices.clear();
        _removed_key_indices.clear();
        _removed_key_values.clear();
        _removed_value_overlays.clear();
    }

    /**
     * @brief Check and reset delta buffers if time changed.
     *
     * Called before recording new delta to ensure buffers are fresh.
     * Tracks delta time separately from modification time because
     * child value modifications can propagate up and change
     * _last_modified_time without being structural changes.
     *
     * @param time The current modification time
     */
    void maybe_reset_delta(engine_time_t time) {
        if (time != _last_delta_time && _last_delta_time != MIN_DT) {
            clear_delta_buffers();
        }
        _last_delta_time = time;
    }

    // ========== Hook Callbacks (Static) ==========

    /**
     * @brief Hook callback for entry insertion.
     *
     * Note: Does NOT record to added buffer here - caller must call record_key_added()
     * with the engine time after the insert completes.
     */
    static void hook_on_insert(void* ctx, size_t index);

    /**
     * @brief Hook callback for swap-with-last.
     *
     * Updates the value overlays to reflect the swap.
     */
    static void hook_on_swap(void* ctx, size_t index_a, size_t index_b);

    /**
     * @brief Hook callback for entry erasure.
     *
     * Note: Does NOT record to removed buffer here - caller must call record_key_removed()
     * with the engine time before the erase completes.
     */
    static void hook_on_erase(void* ctx, size_t index);

    /**
     * @brief Create a child overlay for a value entry.
     */
    std::unique_ptr<TSOverlayStorage> create_value_overlay();

    engine_time_t _last_modified_time{MIN_DT};                               ///< Timestamp of last modification (includes child propagation)
    engine_time_t _last_delta_time{MIN_DT};                                  ///< Timestamp of last delta recording (for lazy cleanup)
    std::vector<size_t> _added_key_indices;                                   ///< Indices of keys added this tick
    std::vector<size_t> _removed_key_indices;                                 ///< Indices of keys removed this tick
    std::vector<value::PlainValue> _removed_key_values;                       ///< Buffered removed key values
    std::vector<std::unique_ptr<TSOverlayStorage>> _value_overlays;           ///< Per-entry value overlays
    std::vector<std::unique_ptr<TSOverlayStorage>> _removed_value_overlays;   ///< Buffered removed value overlays
    const TSMeta* _value_type{nullptr};                                       ///< Value TS type for creating child overlays
    ScalarTSOverlay _is_empty_overlay;                                        ///< Overlay tracking is_empty state changes
    bool _is_empty_value{true};                                               ///< Current is_empty value (starts true for empty map)
    mutable std::unordered_map<size_t, std::any> _ref_caches;                 ///< Per-index REF cache for TSD[K, REF[V]] elements

    // ========== Feature Output Extension Storage (for get_ref tracking) ==========
    mutable std::unordered_map<value::PlainValue, std::any, PlainValueHash, PlainValueEqual> _get_ref_outputs;  ///< Tracked ref outputs from get_ref()

public:
    // ========== Feature Output Extension Methods (for get_ref tracking) ==========

    /**
     * @brief Get tracked ref output for a key, if it exists.
     * @param key The key to look up
     * @return Pointer to the stored std::any, or nullptr if not tracked
     */
    [[nodiscard]] const std::any* get_ref_output(const value::ConstValueView& key) const {
        auto it = _get_ref_outputs.find(value::PlainValue(key));
        return it != _get_ref_outputs.end() ? &it->second : nullptr;
    }

    /**
     * @brief Store a ref output for a key (for get_ref tracking).
     * @param key The key to store under
     * @param output The Python object to store (should be nb::object)
     */
    void set_ref_output(const value::ConstValueView& key, std::any output) const {
        value::PlainValue key_copy(key);
        _get_ref_outputs[std::move(key_copy)] = std::move(output);
    }

    /**
     * @brief Check if there's a tracked ref output for a key.
     * @param key The key to check
     * @return True if there's a tracked output
     */
    [[nodiscard]] bool has_ref_output(const value::ConstValueView& key) const {
        return _get_ref_outputs.find(value::PlainValue(key)) != _get_ref_outputs.end();
    }

    /**
     * @brief Get all tracked ref outputs (for iteration during updates).
     * @return Reference to the internal map
     */
    [[nodiscard]] const std::unordered_map<value::PlainValue, std::any, PlainValueHash, PlainValueEqual>&
    get_ref_outputs() const noexcept {
        return _get_ref_outputs;
    }

    /**
     * @brief Get mutable access to all tracked ref outputs (for updates).
     * @return Reference to the internal map
     */
    [[nodiscard]] std::unordered_map<value::PlainValue, std::any, PlainValueHash, PlainValueEqual>&
    get_ref_outputs_mut() const noexcept {
        return _get_ref_outputs;
    }

    // ========== REF Cache Methods (for TSD[K, REF[V]]) ==========

    /**
     * @brief Set the REF cache value for a specific index.
     * @param index The backing store slot index
     * @param value The Python object to cache (should be nb::object)
     */
    void set_ref_cache(size_t index, std::any value) const {
        _ref_caches[index] = std::move(value);
    }

    /**
     * @brief Get the REF cache value for a specific index.
     * @param index The backing store slot index
     * @return Reference to the cached any, or empty any if not set
     */
    [[nodiscard]] const std::any& ref_cache(size_t index) const {
        static const std::any empty;
        auto it = _ref_caches.find(index);
        return it != _ref_caches.end() ? it->second : empty;
    }

    /**
     * @brief Check if REF cache has a value for a specific index.
     * @param index The backing store slot index
     * @return True if the index has a cached REF value
     */
    [[nodiscard]] bool has_ref_cache(size_t index) const noexcept {
        auto it = _ref_caches.find(index);
        return it != _ref_caches.end() && it->second.has_value();
    }

    /**
     * @brief Clear the REF cache for a specific index.
     * @param index The backing store slot index
     */
    void clear_ref_cache(size_t index) const {
        _ref_caches.erase(index);
    }

    /**
     * @brief Update tracked ref outputs when a key is removed.
     *
     * When a key is removed from the TSD, any tracked ref outputs (from get_ref)
     * need to be updated to point to an empty reference. This matches Python's
     * _ref_ts_feature.update(k) behavior.
     *
     * @param key The key being removed
     */
    void update_ref_output_for_removed_key(const value::ConstValueView& key);
};

// ============================================================================
// KeySetOverlayView - Read-only Set View Over Map Keys
// ============================================================================

/**
 * @brief Read-only set view over a MapTSOverlay's key tracking.
 *
 * This view provides a SetTSOverlay-compatible interface for accessing
 * the key modification tracking of a MapTSOverlay. It allows TSD to expose
 * its key_set tracking in the same way that SetTSOverlay tracks a TSS.
 *
 * The view does not own the underlying MapTSOverlay - it's a lightweight
 * wrapper that forwards to the map's key tracking methods.
 *
 * This mirrors how ConstKeySetView provides set-like access to map keys
 * on the value side.
 *
 * Usage:
 * @code
 * MapTSOverlay map_overlay(tsd_meta);
 *
 * // Get a key set view for delta tracking
 * auto key_view = map_overlay.key_set_view();
 *
 * // Query delta using SetTSOverlay-compatible interface
 * if (key_view.has_delta_at(current_time)) {
 *     const auto& added = key_view.added_indices();
 *     const auto& removed = key_view.removed_indices();
 *     const auto& removed_vals = key_view.removed_values();
 * }
 * @endcode
 */
class KeySetOverlayView {
public:
    /**
     * @brief Construct a key set view from a MapTSOverlay.
     * @param map_overlay The map overlay to view keys of (must not be null)
     */
    explicit KeySetOverlayView(MapTSOverlay* map_overlay) noexcept
        : _map(map_overlay) {}

    // ========== Delta Query ==========

    /**
     * @brief Check if there is key delta at the given time.
     * @param time The current engine time
     * @return True if there are added or removed keys at this time
     */
    [[nodiscard]] bool has_delta_at(engine_time_t time) {
        return _map->has_delta_at(time);
    }

    /**
     * @brief Check if there are any added keys (without time check).
     */
    [[nodiscard]] bool has_added() const noexcept {
        return _map->has_added_keys();
    }

    /**
     * @brief Check if there are any removed keys (without time check).
     */
    [[nodiscard]] bool has_removed() const noexcept {
        return _map->has_removed_keys();
    }

    // ========== Delta Buffers ==========

    /**
     * @brief Get indices of keys added this tick.
     * @return Reference to the added indices vector
     */
    [[nodiscard]] const std::vector<size_t>& added_indices() const noexcept {
        return _map->added_key_indices();
    }

    /**
     * @brief Get indices of keys removed this tick.
     * @return Reference to the removed indices vector
     */
    [[nodiscard]] const std::vector<size_t>& removed_indices() const noexcept {
        return _map->removed_key_indices();
    }

    /**
     * @brief Get the buffered removed key values.
     *
     * The removed values correspond to the indices in removed_indices().
     *
     * @return Reference to the removed key values vector
     */
    [[nodiscard]] const std::vector<value::PlainValue>& removed_values() const noexcept {
        return _map->removed_key_values();
    }

    // ========== Underlying Map Access ==========

    /**
     * @brief Get the underlying MapTSOverlay.
     */
    [[nodiscard]] MapTSOverlay* map_overlay() const noexcept { return _map; }

private:
    MapTSOverlay* _map;
};

// ============================================================================
// Factory Function
// ============================================================================

/**
 * @brief Factory function to create the appropriate overlay type from TSMeta.
 *
 * Creates a TSOverlayStorage instance based on the TSMeta kind:
 * - TSTypeKind::TS → ScalarTSOverlay
 * - TSTypeKind::TSB → CompositeTSOverlay (recursive for fields)
 * - TSTypeKind::TSL → ListTSOverlay (recursive for elements)
 * - TSTypeKind::TSS → SetTSOverlay
 * - TSTypeKind::TSD → MapTSOverlay
 * - TSTypeKind::REF → ScalarTSOverlay (references behave like scalars)
 * - TSTypeKind::SIGNAL → ScalarTSOverlay (signals behave like scalars)
 * - TSTypeKind::TSW → ListTSOverlay (windows use cyclic buffer behavior)
 *
 * For composite types (TSB, TSL), this function recursively creates child
 * overlays and sets up parent-child relationships during construction.
 *
 * @param ts_meta The time-series metadata defining the overlay structure
 * @return Unique pointer to the created overlay, or nullptr if ts_meta is null
 */
[[nodiscard]] std::unique_ptr<TSOverlayStorage> make_ts_overlay(const TSMeta* ts_meta);

}  // namespace hgraph
