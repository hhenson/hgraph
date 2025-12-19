// STUB: Old ts.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy TimeSeriesValueInput template - stub for migration
    // TODO: Code using this should migrate to ts::TSInput with views
    template<typename T>
    struct TimeSeriesValueInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    // Legacy TimeSeriesValueOutput template - stub for migration
    // TODO: Code using this should migrate to ts::TSOutput with views
    template<typename T>
    struct TimeSeriesValueOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };
}
