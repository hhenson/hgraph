#pragma once

/**
 * @file ts_input.h
 * @brief TSInput - Consumer of time-series values.
 *
 * TSInput subscribes to TSOutput(s) and provides access to linked values.
 * Owns a TSValue containing Links at its leaves that point to bound output values.
 *
 * @see design/05_TSOUTPUT_TSINPUT.md
 * @see user_guide/05_TSOUTPUT_TSINPUT.md
 */

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/short_path.h>
#include <hgraph/types/time_series/fq_path.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/hgraph_forward_declarations.h>

#include <memory>
#include <vector>

namespace hgraph {

// Forward declarations
class TSInputView;
class TSOutput;
class TSOutputView;
class ObserverList;

/// Lightweight Notifiable for non-peered SIGNAL binding.
/// When a non-peered TSB output binds to a SIGNAL input, each field
/// output subscribes via a SignalSubscription that updates the SIGNAL's
/// time_data and schedules the owning node.
struct SignalSubscription : Notifiable {
    engine_time_t* signal_time_data{nullptr};
    ObserverList* output_observers{nullptr};
    node_ptr owning_node{nullptr};
    bool subscribed{false};

    void notify(engine_time_t et) override;
    void subscribe();
    void unsubscribe();
};

/// Proxy for REF output → non-REF input binding.
/// When a REF output is connected to a non-REF input (e.g., REF[TSL] → TSL),
/// this proxy observes the REF output's observer list. When the REF changes,
/// it resolves the TSReference and re-binds the input to the actual target.
/// This is needed because non-scalar ops (TSL, TSS, TSD) use resolve_delegation_target
/// (not resolve_delegation_target_with_ref) and cannot resolve REF data directly.
struct RefBindingProxy : Notifiable {
    ViewData ref_output_vd{};              ///< REF output's ViewData (to read TSReference)
    ViewData input_vd{};                   ///< Non-REF input field's ViewData (to update LinkTarget)
    TSInput* input{nullptr};               ///< The input for subscription/notification
    ObserverList* ref_observers{nullptr};   ///< REF output's observer list (for subscribe/unsubscribe)
    bool subscribed{false};

    void notify(engine_time_t et) override;
    void subscribe();
    void unsubscribe();
};

/**
 * @brief Consumer of time-series values.
 *
 * TSInput subscribes to TSOutput(s) and provides access to linked values.
 * It owns a TSValue containing Links at its leaves that point to bound output values.
 *
 * Key responsibilities:
 * - Owns TSValue with link storage at leaves
 * - Manages active/passive subscription state
 * - Implements Notifiable to receive notifications from outputs
 * - Provides TSInputView for access
 *
 * Usage:
 * @code
 * // Create input with schema
 * TSInput input(ts_meta, owning_node);
 *
 * // Get view for binding
 * TSInputView input_view = input.view(current_time);
 * TSOutputView output_view = output.view(current_time);
 *
 * // Bind input to output
 * input_view.bind(output_view);
 *
 * // Make active to receive notifications
 * input_view.make_active();
 *
 * // Access value (reads from linked output)
 * value::View val = input_view.value();
 * @endcode
 */
class TSInput : public Notifiable {
public:
    // ========== Construction ==========

    /**
     * @brief Construct TSInput with schema and owning node.
     *
     * @param ts_meta Schema for this input
     * @param owner The Node that owns this input
     */
    TSInput(const TSMeta* ts_meta, node_ptr owner);

    /**
     * @brief Default constructor - creates invalid TSInput.
     */
    TSInput() noexcept = default;

    // Non-copyable, movable
    TSInput(const TSInput&) = delete;
    TSInput& operator=(const TSInput&) = delete;
    TSInput(TSInput&&) noexcept = default;
    TSInput& operator=(TSInput&&) noexcept = default;

    ~TSInput() override;

    // ========== View Access ==========

    /**
     * @brief Get view for this input at current time.
     *
     * @param current_time The current engine time
     * @return TSInputView for access
     */
    TSInputView view(engine_time_t current_time);

    /**
     * @brief Get view for this input at current time with specific schema.
     *
     * The schema parameter allows requesting a different view of the bound data.
     *
     * @param current_time The current engine time
     * @param schema The requested schema
     * @return TSInputView for the requested schema
     */
    TSInputView view(engine_time_t current_time, const TSMeta* schema);

    // ========== Subscription Control ==========

    /**
     * @brief Set active/passive state for entire input.
     *
     * When active, receives notifications from bound outputs.
     * When passive, does not receive notifications (polling mode).
     *
     * @param active true to activate, false to deactivate
     */
    void set_active(bool active);

    /**
     * @brief Set active/passive state for specific field (TSB).
     *
     * Only valid for bundle inputs.
     *
     * @param field The field name
     * @param active true to activate, false to deactivate
     */
    void set_active(const std::string& field, bool active);

    /**
     * @brief Check if this input (root level) is active.
     */
    [[nodiscard]] bool active() const noexcept;

    /**
     * @brief Get a mutable view of the active state data.
     * @return Mutable view of active_
     */
    [[nodiscard]] value::View active_view();

    /**
     * @brief Get a const view of the active state data.
     * @return Const view of active_
     */
    [[nodiscard]] value::View active_view() const;

    // ========== Notifiable Interface ==========

    /**
     * @brief Called when source output changes.
     *
     * Schedules owning node for execution.
     *
     * @param et The time at which the modification occurred
     */
    void notify(engine_time_t et) override;

    // ========== Accessors ==========

    /**
     * @brief Get the owning node.
     */
    [[nodiscard]] node_ptr owning_node() const noexcept { return owning_node_; }

    /**
     * @brief Get the input schema.
     */
    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }

    /**
     * @brief Get mutable reference to the value (contains links).
     */
    [[nodiscard]] TSValue& value() noexcept { return value_; }

    /**
     * @brief Get const reference to the value.
     */
    [[nodiscard]] const TSValue& value() const noexcept { return value_; }

    /**
     * @brief Get the root ShortPath for this input.
     */
    [[nodiscard]] ShortPath root_path() const {
        return ShortPath(owning_node_, PortType::INPUT, {});
    }

    /**
     * @brief Get the root ViewData for this input's value.
     *
     * This is used for FQPath conversion - navigation starts from this root.
     */
    [[nodiscard]] ViewData root_view_data() const {
        ViewData vd = const_cast<TSValue&>(value_).make_view_data();
        vd.path = ShortPath(owning_node_, PortType::INPUT);
        vd.uses_link_target = true;  // TSInput uses LinkTarget (not REFLink)
        return vd;
    }

    /**
     * @brief Convert a TSView's path to a fully-qualified FQPath.
     *
     * This navigates through the input's value structure to convert
     * slot indices (used by ShortPath) to semantic elements (field names,
     * actual TSD keys).
     *
     * @param view The TSView whose path to convert
     * @return FQPath with semantic path elements
     */
    [[nodiscard]] FQPath to_fq_path(const TSView& view) const {
        return view.view_data().path.to_fq(root_view_data());
    }

    /**
     * @brief Check if valid (has schema).
     */
    [[nodiscard]] bool valid() const noexcept { return meta_ != nullptr; }

    // ========== Bound Output Tracking ==========

    /**
     * @brief Set the bound output (called during binding phase).
     */
    void set_bound_output(TSOutput* output) noexcept { bound_output_ = output; }

    /**
     * @brief Get the bound output.
     */
    [[nodiscard]] TSOutput* bound_output() const noexcept { return bound_output_; }

    // ========== Signal Multi-Bind Support ==========

    /**
     * @brief Register a signal subscription for non-peered SIGNAL binding.
     *
     * When a non-peered TSB output binds to a SIGNAL input, each field's
     * output needs its own subscription that updates the SIGNAL's time_data.
     *
     * @param signal_time_data Pointer to the SIGNAL child's time_data
     * @param output_observers Pointer to the source output's ObserverList
     */
    void add_signal_subscription(engine_time_t* signal_time_data, ObserverList* output_observers);

    /**
     * @brief Register a REF binding proxy for REF output → non-REF input.
     *
     * When a REF output is connected to a non-REF input, the proxy observes
     * the REF output and, when it changes, resolves the reference and binds
     * the input to the actual target.
     */
    void add_ref_binding_proxy(ViewData ref_output_vd, ViewData input_vd, ObserverList* ref_observers);

private:
    TSValue value_;                     ///< Contains Links at leaves pointing to outputs
    value::Value<> active_;             ///< Hierarchical active state (mirrors schema structure)
    const TSMeta* meta_{nullptr};       ///< Schema
    node_ptr owning_node_{nullptr};     ///< For scheduling
    TSOutput* bound_output_{nullptr};   ///< Persistent bound output reference
    std::vector<std::unique_ptr<SignalSubscription>> signal_subscriptions_;  ///< Non-peered SIGNAL subscriptions
    std::vector<std::unique_ptr<RefBindingProxy>> ref_binding_proxies_;     ///< REF→non-REF binding proxies
};

} // namespace hgraph
