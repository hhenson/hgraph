// STUB: Old tsw.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy window types - stubs for migration
    struct TimeSeriesWindowOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesWindowInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    template<typename T>
    struct TimeSeriesWindowOutput_T : TimeSeriesWindowOutput {
        using TimeSeriesWindowOutput::TimeSeriesWindowOutput;
    };

    template<typename T>
    struct TimeSeriesWindowInput_T : TimeSeriesWindowInput {
        using TimeSeriesWindowInput::TimeSeriesWindowInput;
    };

    template<typename T>
    struct TimeSeriesFixedWindowOutput : TimeSeriesWindowOutput {
        using TimeSeriesWindowOutput::TimeSeriesWindowOutput;
    };

    template<typename T>
    struct TimeSeriesTimeWindowOutput : TimeSeriesWindowOutput {
        using TimeSeriesWindowOutput::TimeSeriesWindowOutput;
    };
}
