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
    /**
     * Construct an output view.
     *
     * Future constructors are expected to bind this view to a concrete output
     * endpoint and navigation position.
     */
    TSOutputView() = default;
};

}  // namespace hgraph
