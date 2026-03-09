#pragma once

#include <hgraph/hgraph_base.h>

namespace hgraph {

/**
 * Owning input-side time-series endpoint.
 *
 * `TSInput` is intended to represent the consumer-facing endpoint on a node.
 * It will own the input-local runtime state needed for binding, activation,
 * and input-specific navigation.
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
