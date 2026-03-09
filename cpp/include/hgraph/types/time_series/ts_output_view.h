#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

/**
 * Non-owning view over an output endpoint.
 *
 * `TSOutputView` is intended to expose output-facing operations without
 * transferring ownership of the underlying `TSOutput` state.
 */
struct HGRAPH_EXPORT TSOutputView {
    /**
     * Construct an output view.
     *
     * Future constructors are expected to bind this view to a concrete output
     * endpoint and navigation position.
     */
    TSOutputView() = default;

    /**
     * Construct an output view from the generic time-series view state.
     */
    explicit TSOutputView(TSView *view) noexcept;

    /**
     * Return the underlying generic time-series view.
     */
    [[nodiscard]] TSView *ts_view() noexcept { return m_view; }

    /**
     * Return the underlying generic time-series view.
     */
    [[nodiscard]] const TSView *ts_view() const noexcept { return m_view; }

private:
    TSView *m_view{nullptr};
};

}  // namespace hgraph
