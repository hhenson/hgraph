#pragma once

/**
 * @file ts_view.h
 * @brief TSView - Non-owning time-series view.
 *
 * TSView provides coordinated access to time-series data with temporal
 * semantics. It wraps ViewData and adds the current engine time, enabling
 * proper modified() checks and delta operations.
 *
 * Key design points:
 * - TSView = ViewData + current_time (as per user guide)
 * - ViewData contains ShortPath for graph navigation
 * - ts_ops provides polymorphic operations
 * - Kind-specific views (TSBView, TSLView, etc.) are wrappers around TSView
 *
 * TSView is lightweight and designed to be passed by value.
 */

#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <string>

namespace hgraph {

// Forward declarations
class TSValue;
class TSBView;
class TSLView;
class TSSView;
class TSDView;

/**
 * @brief Non-owning view of a time-series value.
 *
 * TSView provides access to time-series data with temporal semantics.
 * It is constructed from a ViewData (containing path, data pointers, ops)
 * plus the current engine time.
 *
 * The current_time is used for:
 * - modified() checks (compares last_modified_time >= current_time)
 * - Lazy delta clearing (when current_time > last_delta_clear_time)
 * - Binding views to a specific point in time
 *
 * Usage:
 * @code
 * TSView view = ts_value.ts_view(current_time);
 *
 * // Check modification status
 * if (view.modified()) {
 *     // Access value
 *     double val = view.value<double>();
 *
 *     // Access delta if available
 *     if (view.has_delta()) {
 *         auto delta = view.delta_value();
 *     }
 * }
 *
 * // Navigation returns TSView (not value::View)
 * TSView child = view[0];           // By index
 * TSView field = view.field("bid"); // By name (for bundles)
 * @endcode
 */
class TSView {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid TSView.
     */
    TSView() noexcept = default;

    /**
     * @brief Construct from ViewData and current time.
     *
     * @param view_data The ViewData containing path, data, and ops
     * @param current_time The current engine time
     */
    TSView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data))
        , current_time_(current_time) {}

    /**
     * @brief Construct from a TSValue.
     *
     * @param ts_value The owning TSValue
     * @param current_time The current engine time
     */
    TSView(TSValue& ts_value, engine_time_t current_time);

    // ========== Validity ==========

    /**
     * @brief Boolean conversion - returns true if ViewData is structurally valid.
     *
     * This checks if the view has valid data pointers and ops table.
     * Use valid() to check if the time-series has ever been set.
     */
    explicit operator bool() const noexcept {
        return view_data_.valid();
    }

    // ========== Metadata ==========

    /**
     * @brief Get the time-series metadata.
     * @return The TSMeta, or nullptr if invalid
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept {
        return view_data_.meta;
    }

    /**
     * @brief Get the current engine time.
     * @return The time captured at construction
     */
    [[nodiscard]] engine_time_t current_time() const noexcept {
        return current_time_;
    }

    // ========== Time-Series Semantics ==========

    /**
     * @brief Get the last modification time.
     *
     * For atomic TS types, this is the direct timestamp.
     * For composite types (TSB/TSL/TSD), this is the container's timestamp.
     *
     * @return The last modification time, or MIN_ST if never modified
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    /**
     * @brief Check if modified at or after current_time.
     *
     * Uses >= comparison: something is modified at current_time if
     * last_modified_time >= current_time.
     *
     * @return true if last_modified_time >= current_time
     */
    [[nodiscard]] bool modified() const;

    /**
     * @brief Check if the value has ever been set.
     *
     * A value is valid if last_modified_time != MIN_ST.
     *
     * @return true if the value has been set at least once
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Check if this AND all children are valid.
     *
     * For scalars, same as is_valid(). For containers, recursively
     * checks all children.
     *
     * @return true if this and all descendants are valid
     */
    [[nodiscard]] bool all_valid() const;

    /**
     * @brief Check if this view was obtained through a modified REF.
     *
     * When a REF changes target, views obtained through it are "sampled"
     * and report modified=true regardless of target modification.
     *
     * @return true if sampled (obtained through modified REF)
     */
    [[nodiscard]] bool sampled() const;

    /**
     * @brief Check if delta tracking is enabled for this kind.
     * @return true if delta tracking is available
     */
    [[nodiscard]] bool has_delta() const;

    // ========== Value Access ==========

    /**
     * @brief Get the value as a type-erased View.
     * @return View of the underlying value data
     */
    [[nodiscard]] value::View value() const;

    /**
     * @brief Get the value as a specific type.
     *
     * Shorthand for value().as<T>().
     *
     * @tparam T The expected type
     * @return The typed value
     */
    template<typename T>
    [[nodiscard]] T value() const {
        return value().template as<T>();
    }

    /**
     * @brief Get the delta value as a type-erased View.
     *
     * Returns an invalid View if no delta tracking for this kind.
     *
     * @return View of the delta data
     */
    [[nodiscard]] value::View delta_value() const;

    // ========== Mutation (for outputs) ==========

    /**
     * @brief Set the value from a View.
     *
     * Updates modification time and notifies observers.
     *
     * @param src The source value
     */
    void set_value(const value::View& src);

    /**
     * @brief Apply a delta to the value.
     *
     * Updates modification time and notifies observers.
     *
     * @param delta The delta to apply
     */
    void apply_delta(const value::View& delta);

    /**
     * @brief Invalidate the value (reset to never-set state).
     */
    void invalidate();

    // ========== Python Interop ==========

    /**
     * @brief Convert the value to a Python object.
     * @return Python object representation of the value
     */
    [[nodiscard]] nb::object to_python() const;

    /**
     * @brief Convert the delta to a Python object.
     * @return Python object representation of the delta
     */
    [[nodiscard]] nb::object delta_to_python() const;

    /**
     * @brief Set the value from a Python object.
     * @param src The Python object
     */
    void from_python(const nb::object& src);

    // ========== Navigation ==========

    /**
     * @brief Access child by index.
     *
     * For TSB: field by index
     * For TSL: element by index
     * For TSD: value at slot index
     *
     * @param index The child index
     * @return TSView for the child
     */
    [[nodiscard]] TSView operator[](size_t index) const;

    /**
     * @brief Access field by name.
     *
     * Only valid for TSB (bundle) types.
     *
     * @param name The field name
     * @return TSView for the field
     */
    [[nodiscard]] TSView field(const std::string& name) const;

    /**
     * @brief Get the number of children.
     *
     * For TSB: number of fields
     * For TSL: number of elements
     * For TSD: number of key-value pairs
     * For TSS: number of set elements
     * For scalars: 0
     */
    [[nodiscard]] size_t size() const;

    // ========== Path Access ==========

    /**
     * @brief Get the graph-aware path to this view.
     * @return The ShortPath
     */
    [[nodiscard]] const ShortPath& short_path() const noexcept {
        return view_data_.path;
    }

    /**
     * @brief Get the fully-qualified path as a string.
     * @return String representation of the path
     */
    [[nodiscard]] std::string fq_path() const {
        return view_data_.path.to_string();
    }

    // ========== Observer Access ==========

    /**
     * @brief Get the observer list for this time-series.
     * @return View of the observer data
     */
    [[nodiscard]] value::View observer() const;

    // ========== Binding (Link Management) ==========

    /**
     * @brief Bind this position to a target TSView.
     *
     * Creates a link from the current position to the target.
     * After binding, access to this position will redirect to the target.
     *
     * @param target The target TSView to bind to
     */
    void bind(const TSView& target);

    /**
     * @brief Remove the link at this position.
     *
     * After unbinding, the position reverts to local storage.
     */
    void unbind();

    /**
     * @brief Check if this position is bound (linked).
     *
     * @return true if the position is linked to another target
     */
    [[nodiscard]] bool is_bound() const;

    // ========== Kind-Specific View Conversions ==========

    /**
     * @brief Get as a bundle view.
     *
     * Valid for: TSB
     *
     * @return TSBView with field access and modification bubbling
     * @throws std::runtime_error if not a bundle
     */
    [[nodiscard]] TSBView as_bundle() const;

    /**
     * @brief Get as a list view.
     *
     * Valid for: TSL
     *
     * @return TSLView with element access and modification bubbling
     * @throws std::runtime_error if not a list
     */
    [[nodiscard]] TSLView as_list() const;

    /**
     * @brief Get as a set view.
     *
     * Valid for: TSS
     *
     * @return TSSView with delta tracking
     * @throws std::runtime_error if not a set
     */
    [[nodiscard]] TSSView as_set() const;

    /**
     * @brief Get as a dict view.
     *
     * Valid for: TSD
     *
     * @return TSDView with delta tracking and modification bubbling
     * @throws std::runtime_error if not a dict
     */
    [[nodiscard]] TSDView as_dict() const;

    // ========== Raw Access ==========

    /**
     * @brief Get the underlying ViewData.
     *
     * For advanced use cases only.
     */
    [[nodiscard]] const ViewData& view_data() const noexcept {
        return view_data_;
    }

    /**
     * @brief Get mutable access to ViewData.
     *
     * For advanced use cases only (mutation through views).
     */
    [[nodiscard]] ViewData& view_data() noexcept {
        return view_data_;
    }

private:
    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph

// Include kind-specific view headers after TSView is defined
#include <hgraph/types/time_series/ts_bundle_view.h>
#include <hgraph/types/time_series/ts_list_view.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/ts_dict_view.h>
