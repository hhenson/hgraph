#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

/**
 * Output-specialized instantiation of the generic time-series view surface.
 *
 * `TSOutputView` is intended to expose output-facing operations without
 * transferring ownership of the underlying `TSOutput` state.
 */
struct HGRAPH_EXPORT TSOutputView : TSView<TSOutputView> {
    using TSView<TSOutputView>::TSView;

    /**
     * Construct an output view.
     *
     * Future constructors are expected to bind this view to a concrete output
     * endpoint and navigation position.
     */
    TSOutputView() = default;

    /**
     * Return the binding handle for the represented output position.
     */
    [[nodiscard]] LinkedTSContext linked_context() const noexcept
    {
        if (this->m_context.output_view_ops != nullptr) {
            return this->m_context.output_view_ops->linked_context(this->m_context);
        }

        const TSViewContext resolved = this->m_context.resolved();
        return LinkedTSContext{
            resolved.schema,
            resolved.value_dispatch,
            resolved.ts_dispatch,
            resolved.value_data,
            this->m_context.ts_state,
        };
    }
};

}  // namespace hgraph
