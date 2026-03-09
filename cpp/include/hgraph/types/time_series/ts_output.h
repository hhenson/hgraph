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
 * It will own the value published by that output together with any output-local
 * runtime state.
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
