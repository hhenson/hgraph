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

    TSOutputView(ParentValue owner, ViewContext context, ViewContext parent = ViewContext::none()) noexcept :
        TSView<TSOutputView>(owner, context, parent)
    {}

    /**
     * Construct an output view.
     *
     * Future constructors are expected to bind this view to a concrete output
     * endpoint and navigation position.
     */
    TSOutputView() = default;
};

}  // namespace hgraph
