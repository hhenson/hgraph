#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/value/value_view.h>

namespace hgraph {

/**
 * Input-specialized instantiation of the generic time-series view surface.
 *
 * `TSInputView` is intended to expose input-only behavior, especially binding
 * and activation control, while reusing the shared time-series navigation
 * contract.
 */
struct HGRAPH_EXPORT TSInputView : TSView<TSInputView> {
    /**
     * Construct an input view.
     *
     * Future constructors are expected to bind this view to a concrete input
     * endpoint and navigation position.
     */
    TSInputView() = default;

    /**
     * Construct an input view from the path-local active-state payload.
     *
     * The supplied active-state view is expected to represent the activation
     * flag for this exact input position.
     */
    explicit TSInputView(value::ValueView active_state) noexcept;

    virtual ~TSInputView() = default;

    /**
     * Mark the input view as active.
     *
     * This is intended to enable active observation for the represented input
     * position so upstream notifications reach the owning input endpoint.
     */
    virtual void make_active() noexcept;

    /**
     * Mark the input view as passive.
     *
     * This is intended to disable active observation for the represented input
     * position so upstream notifications are no longer requested.
     */
    virtual void make_passive() noexcept;

    /**
     * Return whether the input view is currently active.
     *
     * This is intended to report whether the represented input position is
     * currently participating in active observation.
     */
    [[nodiscard]] virtual bool active() const noexcept;

protected:
    value::ValueView m_active_state;
};

/**
 * Input view base for collection time-series positions.
 *
 * Collection inputs use the collection-local active flag stored at the head of
 * the active-state tuple. Child active payloads are carried alongside that
 * flag in the remaining collection-specific slots.
 */
struct HGRAPH_EXPORT TSInputCollectionView : TSInputView {
    using TSInputView::TSInputView;

    void make_active() noexcept override;
    void make_passive() noexcept override;
    [[nodiscard]] bool active() const noexcept override;
};

}  // namespace hgraph
