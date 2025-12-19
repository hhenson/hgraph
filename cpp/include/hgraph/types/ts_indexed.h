// STUB: Old ts_indexed.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy indexed types - stubs for migration
    struct IndexedTimeSeriesOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct IndexedTimeSeriesInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };
}
