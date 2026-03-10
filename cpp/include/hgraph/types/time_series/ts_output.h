#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_value.h>

namespace hgraph {

/**
 * Owning output-side time-series endpoint.
 *
 * `TSOutput` is intended to represent the producer-facing endpoint on a node.
 * It is the authoritative owner of the output-side `TSValue` payload and the
 * source of truth for leaf values published by that output, together with any
 * output-local runtime state exposed through the Python time-series output
 * API.
 *
 * `TSOutput` is intended to own the bulk of the time-series data. Leaf values
 * are stored here, and reference-oriented representations of the same logical
 * output are also owned here so they can be surfaced through the public API
 * without changing the underlying endpoint identity.
 *
 * Collection outputs are intended to hold their native structure directly. Any
 * peered input observing that structure is expected to do so through
 * `TargetLinkState`, rather than by taking ownership of duplicated output leaf
 * storage.
 */
struct HGRAPH_EXPORT TSOutput {
    /**
     * Construct an output endpoint.
     *
     * Future constructors are expected to establish the runtime identity and
     * storage owned by this output.
     */
    TSOutput() = default;

    [[nodiscard]] TSOutputView view();

private:
    [[nodiscard]] TimeSeriesStatePtr state_ptr() noexcept;

    TimeSeriesStateV state;
    TSValue value_storage;
};

}  // namespace hgraph
