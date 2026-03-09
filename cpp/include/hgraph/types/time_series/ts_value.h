#pragma once

#include <hgraph/types/value/value.h>

#include <hgraph/hgraph_base.h>

namespace hgraph {

/**
 * Owning time-series value storage.
 *
 * `TSValue` is intended to hold the runtime-owned representation of a logical
 * time-series value, including payload data and any value-scoped state required
 * by the runtime.
 */
struct HGRAPH_EXPORT TSValue {
    /**
     * Construct an owned time-series value.
     *
     * Future constructors are expected to establish the value schema and any
     * initial runtime state.
     */
    TSValue() = default;
private:
    value::Value m_value;
};

}  // namespace hgraph
