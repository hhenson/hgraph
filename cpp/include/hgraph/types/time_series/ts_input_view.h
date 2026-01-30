#pragma once

/**
 * @file ts_input_view.h
 * @brief TSInputView - View wrapper for TSInput with binding support.
 *
 * TSInputView wraps a link position and adds input-specific operations:
 * - Binding to TSOutputView
 * - Active/passive subscription control
 * - Navigation that returns TSInputView
 *
 * @see design/05_TSOUTPUT_TSINPUT.md §TSInputView
 * @see user_guide/05_TSOUTPUT_TSINPUT.md §TSInputView
 */

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/short_path.h>

namespace hgraph {

// Forward declarations
class TSInput;
class TSOutput;
class TSOutputView;

/**
 * @brief View wrapper for TSInput, adds input-specific operations.
 *
 * TSInputView provides:
 * - Binding to TSOutputView (creates links, manages subscriptions)
 * - Active/passive subscription control
 * - Navigation that returns TSInputView
 * - Value access (reads through links to bound outputs)
 *
 * TSInputView is lightweight and designed to be passed by value.
 *
 * Usage:
 * @code
 * TSInputView input_view = input.view(current_time);
 *
 * // Bind to output
 * input_view.bind(output_view);
 *
 * // Make active (subscribe to notifications)
 * input_view.make_active();
 *
 * // Access value (reads from linked output)
 * value::View val = input_view.value();
 *
 * // Check modification status
 * if (input_view.modified()) {
 *     // Process the value
 * }
 * @endcode
 */
class TSInputView {
public:
    // ========== Construction ==========

    /**
     * @brief Construct from TSView and owning input.
     *
     * @param ts_view The underlying TSView
     * @param input The owning TSInput (for context)
     * @param active_view View into the active state hierarchy (optional)
     */
    TSInputView(TSView ts_view, TSInput* input, value::View active_view = value::View()) noexcept
        : ts_view_(std::move(ts_view))
        , input_(input)
        , bound_output_(nullptr)
        , active_view_(active_view) {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSInputView() noexcept = default;

    // ========== TSView Access ==========

    /**
     * @brief Get the underlying TSView.
     *
     * The TSView provides access to the linked data.
     */
    [[nodiscard]] TSView& ts_view() noexcept { return ts_view_; }

    /**
     * @brief Get the underlying TSView (const).
     */
    [[nodiscard]] const TSView& ts_view() const noexcept { return ts_view_; }

    // ========== Data Access (via linked data) ==========

    /**
     * @brief Get value view at this position.
     *
     * Reads through the link to the bound output's data.
     */
    [[nodiscard]] value::View value() const { return ts_view_.value(); }

    /**
     * @brief Get the delta value as a View.
     */
    [[nodiscard]] value::View delta_value() const { return ts_view_.delta_value(); }

    /**
     * @brief Check if modified at current time.
     *
     * Returns true if the linked output was modified at current_time.
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

    // ========== Python Interop ==========

    /**
     * @brief Convert the value to a Python object.
     */
    [[nodiscard]] nb::object to_python() const {
        return ts_view_.to_python();
    }

    // ========== Input-Specific Binding ==========

    /**
     * @brief Bind this input position to an output.
     *
     * Creates a link from this position to the output's data.
     * If active, also subscribes to notifications from the output.
     *
     * @param output The output view to bind to
     */
    void bind(TSOutputView& output);

    /**
     * @brief Unbind from current source.
     *
     * Removes the link and unsubscribes from notifications.
     */
    void unbind();

    /**
     * @brief Check if bound to an output.
     */
    [[nodiscard]] bool is_bound() const { return ts_view_.is_bound(); }

    // ========== Subscription Control ==========

    /**
     * @brief Make this position active (subscribe to notifications).
     *
     * When active, the owning TSInput receives notifications when
     * the bound output is modified.
     */
    void make_active();

    /**
     * @brief Make this position passive (unsubscribe from notifications).
     */
    void make_passive();

    /**
     * @brief Check if this position is active.
     */
    [[nodiscard]] bool active() const;

    // ========== Navigation ==========

    /**
     * @brief Navigate to field by name.
     *
     * Only valid for TSB (bundle) types.
     *
     * @param name The field name
     * @return TSInputView for the field
     */
    [[nodiscard]] TSInputView field(const std::string& name) const;

    /**
     * @brief Navigate to child by index.
     *
     * For TSB: field by index
     * For TSL: element by index
     * For TSD: value at slot index
     *
     * @param index The child index
     * @return TSInputView for the child
     */
    [[nodiscard]] TSInputView operator[](size_t index) const;

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

    // ========== Internal Access ==========

    /**
     * @brief Get the owning TSInput.
     */
    [[nodiscard]] TSInput* input() noexcept { return input_; }

    /**
     * @brief Get the owning TSInput (const).
     */
    [[nodiscard]] const TSInput* input() const noexcept { return input_; }

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

    /**
     * @brief Set the bound output pointer (used internally during binding).
     * @param output The TSOutput pointer this view is bound to
     */
    void set_bound_output(TSOutput* output) noexcept { bound_output_ = output; }

    /**
     * @brief Get the bound output pointer.
     * @return The TSOutput pointer this view is bound to, or nullptr if unbound
     */
    [[nodiscard]] TSOutput* bound_output() const noexcept { return bound_output_; }

private:
    TSView ts_view_;            ///< Core view (ViewData + current_time)
    TSInput* input_{nullptr};   ///< For context and subscription management
    TSOutput* bound_output_{nullptr};  ///< The output this view is bound to (for subscription management)
    value::View active_view_;   ///< View into the active state hierarchy at this position
};

} // namespace hgraph
