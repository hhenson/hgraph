#pragma once

/**
 * @file signal_view.h
 * @brief SignalView - View for SIGNAL time-series.
 *
 * SignalView provides presence-only semantics - it tracks when something
 * ticks without caring about the actual value. Key behaviors:
 *
 * 1. **No value data**: SIGNAL has no semantic data, only modification state.
 *    The value() method returns the modification state (bool).
 *
 * 2. **Reference dereferencing**: When binding to sources with REF types,
 *    SignalView works with the dereferenced schema. This ensures the signal
 *    monitors actual data sources rather than reference wrappers.
 *
 * 3. **Child signals**: Supports lazy creation of child signals for binding
 *    to composite types (TSB/TSL). Children aggregate modification state.
 *
 * Usage:
 * @code
 * // Basic usage - check if signal ticked
 * SignalView heartbeat = ...;
 * if (heartbeat.modified()) {
 *     // React to the tick
 * }
 *
 * // Child signal access (for composite binding)
 * SignalView child = heartbeat[0];  // Lazily creates child signal
 *
 * // Output signal - tick it
 * SignalView output = ...;
 * output.tick();  // Marks as modified at current time
 * @endcode
 */

#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/util/date_time.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace hgraph {

// Forward declarations
class TSView;
class TSTypeRegistry;

/**
 * @brief View for SIGNAL time-series.
 *
 * SignalView provides presence-only semantics. Unlike other time-series views
 * that carry values, SignalView only tracks modification state ("did something tick?").
 *
 * Special behaviors:
 * - value() returns modification state (bool), not actual data
 * - When binding to sources with REF types, uses dereferenced schema
 * - Child signals aggregate modified/valid state from all children
 * - Active/passive state propagates to children
 */
class SignalView {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an unbound SignalView.
     */
    SignalView() noexcept = default;

    // Non-copyable (children_ uses unique_ptr for reference stability)
    SignalView(const SignalView&) = delete;
    SignalView& operator=(const SignalView&) = delete;

    // Moveable
    SignalView(SignalView&&) noexcept = default;
    SignalView& operator=(SignalView&&) noexcept = default;

    /**
     * @brief Construct from ViewData and current time.
     *
     * @param view_data The ViewData (should be for SIGNAL kind)
     * @param current_time The current engine time
     */
    SignalView(ViewData view_data, engine_time_t current_time) noexcept;

    /**
     * @brief Construct and bind to a source TSView.
     *
     * If the source contains REF types, automatically dereferences
     * the schema and binds to the dereferenced view.
     *
     * @param source The source TSView to bind to
     * @param current_time The current engine time
     */
    SignalView(const TSView& source, engine_time_t current_time);

    // ========== Core Signal Methods ==========

    /**
     * @brief Check if the signal ticked (modified at current time).
     *
     * If child signals exist, returns true if ANY child is modified.
     * Otherwise, delegates to the bound source's modified state.
     *
     * @return true if the signal ticked
     */
    [[nodiscard]] bool modified() const;

    /**
     * @brief Check if the signal has ever ticked (is valid).
     *
     * If child signals exist, returns true if ANY child is valid.
     * Otherwise, delegates to the bound source's valid state.
     *
     * @return true if the signal has ever ticked
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Get the last modification time.
     *
     * If child signals exist, returns the MAX of all children.
     * Otherwise, delegates to the bound source.
     *
     * @return The last modification time
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    /**
     * @brief Get the current engine time.
     * @return The time captured at construction
     */
    [[nodiscard]] engine_time_t current_time() const noexcept {
        return current_time_;
    }

    // ========== Value Access (Uniform API) ==========

    /**
     * @brief Get the signal's "value" - its modification state.
     *
     * For API uniformity with other TS types, SIGNAL exposes value()
     * which returns the modification state as a bool.
     *
     * @return true if modified (same as modified())
     */
    [[nodiscard]] bool value() const {
        return modified();
    }

    /**
     * @brief Get the delta value - same as value for SIGNAL.
     *
     * SIGNAL has no delta tracking; delta_value returns the same
     * as value (modification state).
     *
     * @return true if modified (same as value())
     */
    [[nodiscard]] bool delta_value() const {
        return value();
    }

    // ========== Child Signal Access ==========

    /**
     * @brief Access child signal by index.
     *
     * Lazily creates child signals when accessed. Used for binding
     * to composite time-series (TSL elements, TSB fields).
     *
     * If bound to a source, the child binds to the corresponding
     * source child (with dereferencing if needed).
     *
     * @param index The child index
     * @return Reference to the child SignalView
     */
    SignalView& operator[](size_t index);

    /**
     * @brief Access child signal by index (const version).
     *
     * Returns invalid SignalView if index doesn't exist.
     *
     * @param index The child index
     * @return The child SignalView, or invalid if not found
     */
    [[nodiscard]] const SignalView& at(size_t index) const;

    /**
     * @brief Access child signal by field name.
     *
     * Only valid when bound to a TSB (bundle) source.
     * Lazily creates the child signal for the named field.
     *
     * @param name The field name
     * @return Reference to the child SignalView for that field
     * @throws std::invalid_argument if not bound to a TSB or field not found
     */
    SignalView& field(const std::string& name);

    /**
     * @brief Check if child signals have been created.
     * @return true if any children exist
     */
    [[nodiscard]] bool has_children() const noexcept {
        return !children_.empty();
    }

    /**
     * @brief Get the number of child signals.
     * @return Number of children (0 if none created)
     */
    [[nodiscard]] size_t child_count() const noexcept {
        return children_.size();
    }

    // ========== Binding ==========

    /**
     * @brief Check if bound to a source.
     *
     * A signal is bound if it has a source ViewData OR if it has
     * child signals (for free-standing composite binding).
     *
     * @return true if bound
     */
    [[nodiscard]] bool bound() const noexcept;

    /**
     * @brief Bind to a source TSView.
     *
     * If the source contains REF types, dereferences the schema
     * and binds to the dereferenced view data.
     *
     * @param source The source TSView to bind to
     */
    void bind(const TSView& source);

    /**
     * @brief Unbind from the current source.
     *
     * Also clears all child signals.
     */
    void unbind();

    // ========== Active/Passive State ==========

    /**
     * @brief Check if this signal is active (subscribed to notifications).
     * @return true if active
     */
    [[nodiscard]] bool active() const noexcept { return active_; }

    /**
     * @brief Make this signal active (subscribe to notifications).
     *
     * Also activates all existing child signals.
     */
    void make_active();

    /**
     * @brief Make this signal passive (unsubscribe from notifications).
     *
     * Also deactivates all existing child signals.
     */
    void make_passive();

    // ========== Output Operations (Tick) ==========

    /**
     * @brief Tick the signal (for output signals).
     *
     * Updates modification time to current_time and notifies observers.
     * Only valid for output signals with local storage.
     */
    void tick();

    // ========== Metadata ==========

    /**
     * @brief Get the SIGNAL metadata.
     *
     * Returns the SIGNALMeta singleton.
     *
     * @return The TSMeta for SIGNAL kind
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept;

    /**
     * @brief Get the dereferenced source schema.
     *
     * This is the schema the signal is actually monitoring
     * (after REF dereferencing).
     *
     * @return The dereferenced source TSMeta, or nullptr if unbound
     */
    [[nodiscard]] const TSMeta* source_meta() const noexcept {
        return source_meta_;
    }

    // ========== Underlying Access ==========

    /**
     * @brief Get the underlying ViewData.
     *
     * For advanced use cases.
     */
    [[nodiscard]] const ViewData& view_data() const noexcept {
        return view_data_;
    }

    /**
     * @brief Check if this SignalView has valid data.
     * @return true if ViewData is structurally valid
     */
    explicit operator bool() const noexcept {
        return view_data_.valid() || has_children();
    }

private:
    ViewData view_data_;                                  ///< View data (to dereferenced source or local)
    const TSMeta* source_meta_{nullptr};                  ///< Dereferenced source schema
    engine_time_t current_time_{MIN_DT};                  ///< Current engine time
    std::vector<std::unique_ptr<SignalView>> children_;   ///< Child signals (unique_ptr for stable references to Python)
    bool active_{false};                                  ///< Active (subscribed) state

    /// Stored TSView for child navigation (TSView child navigation works correctly)
    std::optional<TSView> source_view_;

    /// Static invalid signal for returning from const accessors
    static const SignalView invalid_signal_;

    /// Get or create child at index
    SignalView& get_or_create_child(size_t index);

    /// Bind a child signal to the corresponding source child
    void bind_child(SignalView& child, size_t index);
};

} // namespace hgraph
