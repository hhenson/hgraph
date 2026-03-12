#pragma once

#include <hgraph/hgraph_base.h>

#include <cstdint>

namespace hgraph
{

    /**
     * Selects whether a value storage builder should retain mutation deltas.
     *
     * `Plain` is intended for compact ordinary value storage.
     * `Delta` retains the extra mutation bookkeeping required by the
     * time-series layer.
     */
    enum class MutationTracking : std::uint8_t
    {
        Plain,
        Delta,
    };

}  // namespace hgraph
