// STUB: Old tsl.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy list types - stubs for migration
    struct TimeSeriesListOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesListInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    template<typename T>
    struct TimeSeriesListOutput_T : TimeSeriesListOutput {
        using TimeSeriesListOutput::TimeSeriesListOutput;
    };

    template<typename T>
    struct TimeSeriesListInput_T : TimeSeriesListInput {
        using TimeSeriesListInput::TimeSeriesListInput;
    };
}
