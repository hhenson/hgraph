#pragma once

/**
 * @file ts_link.h
 * @brief TSLink - Symbolic link binding an input position to an external output.
 *
 * TSLink represents the "peered" binding in the input hierarchy. It:
 * - Holds a reference to an external output's TSValue
 * - Manages subscription (active/passive state) to the output's overlay
 * - Delegates notifications directly to the owning node
 * - Provides view access to the linked output's data
 *
 * When navigation in an input encounters a TSLink, it transparently returns
 * a view into the linked output's data rather than local data.
 *
 * See: ts_design_docs/TSInput_DESIGN.md
 */

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>

namespace hgraph {

// Forward declarations
struct TSView;
struct TSValue;

/**
 * @brief Link to an external output - the "symbolic link" in the input hierarchy.
 *
 * TSLink implements Notifiable so it can be registered with an output's overlay.
 * When the linked output is modified, the overlay notifies this TSLink, which
 * then delegates directly to the owning node.
 *
 * Key behaviors:
 * - Active state is preserved across bind/unbind operations
 * - When active, automatically subscribes to bound output's overlay
 * - Notifications go directly to node (no bubble-up through parent inputs)
 * - Notify-time deduplication prevents redundant node notifications
 *
 * Usage:
 * @code
 * TSLink link;
 * link.set_node(owning_node);
 *
 * // Bind to an output
 * link.bind(output->ts_value());
 *
 * // Make active to receive notifications
 * link.make_active();
 *
 * // Get view into linked data
 * TSView view = link.view();
 * float price = view.as<float>();
 *
 * // Unbind (active state preserved)
 * link.unbind();
 *
 * // Rebind to different output (auto-subscribes if still active)
 * link.bind(other_output->ts_value());
 * @endcode
 */
struct TSLink : Notifiable {
    // ========== Construction ==========

    TSLink() noexcept = default;

    /**
     * @brief Construct with owning node.
     * @param node The node to notify on modifications
     */
    explicit TSLink(Node* node) noexcept : _node(node) {}

    // Non-copyable (registered as observer)
    TSLink(const TSLink&) = delete;
    TSLink& operator=(const TSLink&) = delete;

    // Movable
    TSLink(TSLink&& other) noexcept;
    TSLink& operator=(TSLink&& other) noexcept;

    ~TSLink() override;

    // ========== Node Association ==========

    /**
     * @brief Set the owning node (for notification delegation).
     * @param node The node to notify on modifications
     */
    void set_node(Node* node) noexcept { _node = node; }

    /**
     * @brief Get the owning node.
     */
    [[nodiscard]] Node* node() const noexcept { return _node; }

    // ========== Binding ==========

    /**
     * @brief Bind to an external TSValue (from an output).
     *
     * If currently active, unsubscribes from old output and subscribes to new.
     * Active state is preserved across rebinding.
     *
     * @param output The TSValue to link to (typically from an output)
     */
    void bind(const TSValue* output);

    /**
     * @brief Unbind from current output.
     *
     * Active state is preserved - will auto-subscribe when rebound.
     */
    void unbind();

    /**
     * @brief Check if currently bound to an output.
     */
    [[nodiscard]] bool bound() const noexcept { return _output != nullptr; }

    /**
     * @brief Get the bound output.
     */
    [[nodiscard]] const TSValue* output() const noexcept { return _output; }

    // ========== Subscription Control ==========

    /**
     * @brief Make this link active (subscribe to output's overlay).
     *
     * When active, modifications to the bound output trigger notifications
     * to the owning node.
     */
    void make_active();

    /**
     * @brief Make this link passive (unsubscribe from output's overlay).
     *
     * When passive, modifications to the bound output are not notified.
     */
    void make_passive();

    /**
     * @brief Check if this link is active.
     */
    [[nodiscard]] bool active() const noexcept { return _active; }

    // ========== Notifiable Implementation ==========

    /**
     * @brief Called when the bound output is modified.
     *
     * Delegates to owning node with deduplication.
     *
     * @param time The engine time of the modification
     */
    void notify(engine_time_t time) override;

    // ========== View Access ==========

    /**
     * @brief Get a view into the linked output's data.
     *
     * This is what navigation returns when it encounters this link.
     *
     * @return TSView into the bound output, or invalid view if unbound
     */
    [[nodiscard]] TSView view() const;

    // ========== State Queries ==========

    /**
     * @brief Check if the linked output is valid (has been set).
     */
    [[nodiscard]] bool valid() const;

    /**
     * @brief Check if the linked output was modified at the given time.
     * @param time The time to check against
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const;

    /**
     * @brief Get the last modification time of the linked output.
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    // ========== Sample Time ==========
    // NOTE: Sample time tracking is provided for REF type support (Phase 6.75).
    // For non-REF inputs, bindings are established during wiring and remain stable,
    // so sample_time is not automatically set during bind(). When REF support is
    // implemented, bind() should set _sample_time to current evaluation time on
    // rebinding, and modified_at() should incorporate sampled_at() in its check.

    /**
     * @brief Set the sample time (when this link was bound).
     *
     * Used for detecting rebinding within an evaluation cycle.
     * This is primarily needed for REF type support where dynamic rebinding
     * can occur at runtime.
     *
     * @param time The sample time
     */
    void set_sample_time(engine_time_t time) noexcept { _sample_time = time; }

    /**
     * @brief Get the sample time.
     */
    [[nodiscard]] engine_time_t sample_time() const noexcept { return _sample_time; }

    /**
     * @brief Check if this link was sampled (bound) at the given time.
     */
    [[nodiscard]] bool sampled_at(engine_time_t time) const noexcept {
        return _sample_time == time;
    }

    // ========== REF Support ==========

    /**
     * @brief Set whether this link only notifies once (for REF inputs).
     *
     * REF inputs bound to non-REF outputs should only notify on the first
     * tick (when the binding takes effect), not on subsequent ticks when
     * underlying values change. This matches Python's _sampled semantics.
     *
     * @param notify_once If true, only notify on first notification
     */
    void set_notify_once(bool notify_once) noexcept { _notify_once = notify_once; }

    /**
     * @brief Check if this link only notifies once.
     */
    [[nodiscard]] bool notify_once() const noexcept { return _notify_once; }

    // ========== Element Index Support (for TSL->TS binding) ==========

    /**
     * @brief Set the element index within the linked container.
     *
     * When binding to a TSL element (e.g., TSL output to TS input),
     * the element index indicates which element within the container
     * this link refers to. -1 means the whole container.
     *
     * If already bound to a TSL and active, this will switch the subscription
     * from the whole TSL overlay to the specific element's overlay.
     *
     * @param idx Element index, or -1 for whole container
     */
    void set_element_index(int idx);

    /**
     * @brief Get the element index.
     * @return Element index, or -1 if linking to whole container
     */
    [[nodiscard]] int element_index() const noexcept { return _element_index; }

    /**
     * @brief Check if this link refers to a specific element.
     */
    [[nodiscard]] bool is_element_binding() const noexcept { return _element_index >= 0; }

private:
    // ========== Binding State ==========
    const TSValue* _output{nullptr};
    TSOverlayStorage* _output_overlay{nullptr};

    // ========== Notification ==========
    Node* _node{nullptr};
    bool _active{false};
    bool _notify_once{false};          ///< For REF: only notify on first tick
    engine_time_t _sample_time{MIN_DT};
    engine_time_t _notify_time{MIN_DT};
    int _element_index{-1};            ///< Element index for TSL->TS binding (-1 = whole container)

    // ========== Helpers ==========

    /**
     * @brief Subscribe to the output's overlay if active and bound.
     */
    void subscribe_if_needed();

    /**
     * @brief Unsubscribe from the output's overlay if subscribed.
     */
    void unsubscribe_if_needed();

    /**
     * @brief Check if the owning node's graph is stopping.
     */
    [[nodiscard]] bool is_graph_stopping() const;
};

}  // namespace hgraph
