// STUB: Old tss.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy set types - stubs for migration
    struct TimeSeriesSetOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesSetInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    template<typename T>
    struct TimeSeriesSetOutput_T : TimeSeriesSetOutput {
        using TimeSeriesSetOutput::TimeSeriesSetOutput;
    };

    template<typename T>
    struct TimeSeriesSetInput_T : TimeSeriesSetInput {
        using TimeSeriesSetInput::TimeSeriesSetInput;
    };
}
