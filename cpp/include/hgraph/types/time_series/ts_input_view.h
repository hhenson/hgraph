#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

struct TSOutputView;

/**
 * Input-specialized instantiation of the generic time-series view surface.
 *
 * `TSInputView` is intended to expose input-only behavior, especially binding
 * and activation control, while reusing the shared time-series navigation
 * contract.
 *
 * Activation is path-local by design. A parent input may remain passive while
 * one of its descendants is active, so this view works against the exact
 * active-state payload for the represented path rather than inferring activity
 * from the surrounding subtree.
 */
struct HGRAPH_EXPORT TSInputView : TSView<TSInputView> {
    /**
     * Construct a navigable input view over TS storage.
     *
     * This is the wiring-time/runtime navigation surface used for collection
     * access and binding.
     */
    TSInputView(TSViewContext context,
                TSViewContext parent = TSViewContext::none(),
                engine_time_t evaluation_time = MIN_DT) noexcept;

    /**
     * Construct an input view from the path-local active-state payload.
     *
     * The supplied active-state view is expected to represent the activation
     * flag for this exact input position. The supplied state pointer is a
     * non-owning reference to the time-series state node represented by this
     * view. The supplied scheduling notifier is the non-null registration
     * identity to use when this view requests node scheduling from a bound
     * output.
     *
     * From the input root down to the first target link, this is intended to
     * be the owning node itself. For views below a target link, this is
     * intended to switch to the target link state's `scheduling_notifier` so
     * each linked branch schedules independently.
     */
    explicit TSInputView(View active_state, BaseState *state,
                         Notifiable *scheduling_notifier) noexcept;

    virtual ~TSInputView() = default;

    /**
     * Bind this collection-selected input slot to an output.
     *
     * This is intended to be called on a child view reached through `TSL` or
     * `TSB` navigation, where the current view identifies the slot to replace
     * with a target link.
     */
    void bind_output(const TSOutputView &output);

    /**
     * Mark the input view as active.
     *
     * This is intended to enable active observation for the represented input
     * position so upstream notifications reach the owning input endpoint.
     * Activity is local to this path and does not imply parent activation.
     */
    virtual void make_active();

    /**
     * Mark the input view as passive.
     *
     * This is intended to disable active observation for the represented input
     * position so upstream notifications are no longer requested.
     */
    virtual void make_passive();

    /**
     * Return whether the input view is currently active.
     *
     * This is intended to report whether the represented input position is
     * currently participating in active observation.
     */
    [[nodiscard]] virtual bool active() const;

protected:
    void subscribe_scheduling_notifier() noexcept;
    void unsubscribe_scheduling_notifier() noexcept;

    View m_active_state{View::invalid_for(nullptr)};
    /**
     * Non-owning reference to the represented time-series state node.
     */
    BaseState         *m_state{nullptr};
    /**
     * Non-owning notifier used only for scheduling the owning node when this
     * input view becomes active against a bound output.
     */
    Notifiable        *m_scheduling_notifier{nullptr};
};

/**
 * Input view base for collection time-series positions.
 *
 * Collection inputs use the collection-local active flag stored at the head of
 * the active-state tuple. Child active payloads are carried alongside that
 * flag in the remaining collection-specific slots.
 */
struct HGRAPH_EXPORT TSInputCollectionView : TSInputView {
    explicit TSInputCollectionView(View active_state, BaseState *state,
                                   Notifiable *scheduling_notifier) noexcept;

    void make_active() override;
    void make_passive() override;
    [[nodiscard]] bool active() const override;

};

}  // namespace hgraph
