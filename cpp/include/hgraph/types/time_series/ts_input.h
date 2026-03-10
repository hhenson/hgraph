#pragma once

#include <hgraph/hgraph_base.h>

namespace hgraph {

/**
 * Owning input-side time-series endpoint.
 *
 * `TSInput` is intended to represent the consumer-facing endpoint on a node.
 * It owns the input-local runtime state needed for binding, activation, and
 * input-specific navigation as exposed through the Python time-series input
 * API.
 *
 * Native input-owned collection storage is intended to be used only for
 * non-peered `TSL` and `TSB` shapes. In that case the input owns the
 * collection structure and the child state directly.
 *
 * When an input is peered to an output, child positions are intended to be
 * represented through `TargetLinkState` rather than by duplicating leaf value
 * storage inside the input. The output-side endpoint remains the source of
 * truth for peered leaf values, while the input tracks binding and activation
 * state for its local view of that structure.
 */
struct HGRAPH_EXPORT TSInput {
    /**
     * Construct an input endpoint.
     *
     * Future constructors are expected to establish the runtime identity and
     * storage owned by this input.
     */
    TSInput() = default;
};

}  // namespace hgraph
