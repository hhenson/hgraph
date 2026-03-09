#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

/**
 * Non-owning input-specialized wrapper around `TSView`.
 *
 * `TSInputView` is intended to expose input-only behavior, especially binding
 * and activation control, while reusing the generic navigation surface of
 * `TSView`.
 */
struct HGRAPH_EXPORT TSInputView {
    /**
     * Construct an input view.
     *
     * Future constructors are expected to bind this view to a concrete input
     * endpoint and navigation position.
     */
    TSInputView() = default;

    /**
     * Mark the input view as active.
     *
     * This is intended to enable active observation for the represented input
     * position so upstream notifications reach the owning input endpoint.
     */
    void make_active() noexcept;

    /**
     * Mark the input view as passive.
     *
     * This is intended to disable active observation for the represented input
     * position so upstream notifications are no longer requested.
     */
    void make_passive() noexcept;

    /**
     * Return whether the input view is currently active.
     *
     * This is intended to report whether the represented input position is
     * currently participating in active observation.
     */
    [[nodiscard]] bool active() const noexcept;

private:
    TSView *m_view{nullptr};
    bool m_active{false};
};

}  // namespace hgraph
