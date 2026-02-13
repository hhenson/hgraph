#pragma once

/**
 * @file ts_output_view.h
 * @brief TSOutputView - View wrapper for TSOutput with mutation support.
 *
 * TSOutputView wraps TSView and adds output-specific operations:
 * - Value mutation (set_value, apply_delta)
 * - Observer subscription management
 * - Navigation that returns TSOutputView
 *
 * @see design/05_TSOUTPUT_TSINPUT.md §TSOutputView
 * @see user_guide/05_TSOUTPUT_TSINPUT.md §TSOutputView
 */

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/fq_path.h>
#include <hgraph/types/notifiable.h>

namespace hgraph {

// Forward declarations
class TSOutput;

/**
 * @brief View wrapper for TSOutput, adds output-specific operations.
 *
 * Wraps TSView and provides:
 * - Value mutation (set_value, apply_delta)
 * - Observer subscription management
 * - Navigation that returns TSOutputView
 *
 * TSOutputView is lightweight and designed to be passed by value.
 *
 * Usage:
 * @code
 * TSOutputView view = output.view(current_time);
 *
 * // Set value
 * view.set_value(value::View::from(42));
 *
 * // Subscribe for notifications
 * view.subscribe(my_input);
 *
 * // Navigate to child (returns TSOutputView)
 * TSOutputView child = view[0];
 * @endcode
 */
class TSOutputView {
public:
    // ========== Construction ==========

    /**
     * @brief Construct from TSView and owning output.
     *
     * @param ts_view The underlying TSView
     * @param output The owning TSOutput (for context)
     */
    TSOutputView(TSView ts_view, TSOutput* output) noexcept
        : ts_view_(std::move(ts_view))
        , output_(output) {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSOutputView() noexcept = default;

    // ========== Data Access (delegated to TSView) ==========

    /**
     * @brief Get the value as a View.
     */
    [[nodiscard]] value::View value() const { return ts_view_.value(); }

    /**
     * @brief Get the delta value as a View.
     */
    [[nodiscard]] value::View delta_value() const { return ts_view_.delta_value(); }

    /**
     * @brief Check if modified at current time.
     */
    [[nodiscard]] bool modified() const { return ts_view_.modified(); }

    /**
     * @brief Check if the value has ever been set.
     */
    [[nodiscard]] bool valid() const { return ts_view_.valid(); }

    /**
     * @brief Get the current engine time.
     */
    [[nodiscard]] engine_time_t current_time() const noexcept { return ts_view_.current_time(); }

    /**
     * @brief Get the time-series metadata.
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return ts_view_.ts_meta(); }

    // ========== Output-Specific Mutation ==========

    /**
     * @brief Set the value at this position.
     *
     * Updates modification time and notifies observers.
     *
     * @param v The value to set
     */
    void set_value(const value::View& v) {
        ts_view_.set_value(v);
    }

    /**
     * @brief Apply delta at this position.
     *
     * Updates modification time and notifies observers.
     *
     * @param dv The delta to apply
     */
    void apply_delta(const value::View& dv) {
        ts_view_.apply_delta(dv);
    }

    /**
     * @brief Invalidate the value.
     */
    void invalidate() {
        ts_view_.invalidate();
    }

    // ========== Python Interop ==========

    /**
     * @brief Set the value from a Python object.
     */
    void from_python(const nb::object& src) {
        ts_view_.from_python(src);
    }

    /**
     * @brief Convert the value to a Python object.
     */
    [[nodiscard]] nb::object to_python() const {
        return ts_view_.to_python();
    }

    // ========== Observer Management ==========

    /**
     * @brief Subscribe observer for notifications.
     *
     * The observer will be notified when this position is modified.
     *
     * @param observer The Notifiable to subscribe
     */
    void subscribe(Notifiable* observer);

    /**
     * @brief Unsubscribe observer.
     *
     * @param observer The Notifiable to unsubscribe
     */
    void unsubscribe(Notifiable* observer);

    // ========== Navigation ==========

    /**
     * @brief Navigate to field by name.
     *
     * Only valid for TSB (bundle) types.
     *
     * @param name The field name
     * @return TSOutputView for the field
     */
    [[nodiscard]] TSOutputView field(const std::string& name) const;

    /**
     * @brief Navigate to child by index.
     *
     * For TSB: field by index
     * For TSL: element by index
     * For TSD: value at slot index
     *
     * @param index The child index
     * @return TSOutputView for the child
     */
    [[nodiscard]] TSOutputView operator[](size_t index) const;

    /**
     * @brief Navigate to child by key.
     *
     * Only valid for TSD (dict) types.
     *
     * @param key The key (as a value::View)
     * @return TSOutputView for the value at that key
     */
    [[nodiscard]] TSOutputView operator[](const value::View& key) const;

    /**
     * @brief Get the number of children.
     */
    [[nodiscard]] size_t size() const { return ts_view_.size(); }

    // ========== Path Access ==========

    /**
     * @brief Get the graph-aware path to this view.
     */
    [[nodiscard]] const ShortPath& short_path() const noexcept {
        return ts_view_.short_path();
    }

    /**
     * @brief Get the fully-qualified path.
     * @return FQPath with semantic path elements
     */
    [[nodiscard]] FQPath fq_path() const;

    // ========== Internal Access ==========

    /**
     * @brief Get the underlying TSView.
     */
    [[nodiscard]] TSView& ts_view() noexcept { return ts_view_; }

    /**
     * @brief Get the underlying TSView (const).
     */
    [[nodiscard]] const TSView& ts_view() const noexcept { return ts_view_; }

    /**
     * @brief Get the owning TSOutput.
     */
    [[nodiscard]] TSOutput* output() noexcept { return output_; }

    /**
     * @brief Get the owning TSOutput (const).
     */
    [[nodiscard]] const TSOutput* output() const noexcept { return output_; }

    /**
     * @brief Get the underlying ViewData.
     */
    [[nodiscard]] const ViewData& view_data() const noexcept {
        return ts_view_.view_data();
    }

    /**
     * @brief Check if this view is valid.
     */
    explicit operator bool() const noexcept {
        return static_cast<bool>(ts_view_);
    }

private:
    TSView ts_view_;        ///< Core view (ViewData + current_time)
    TSOutput* output_{nullptr};  ///< For context
};

} // namespace hgraph
