// STUB: Old base_time_series.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy base types - stubs for migration
    // These inherit from the new types so code can compile

    struct TimeSeriesOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    struct BaseTimeSeriesOutput : TimeSeriesOutput {
        using TimeSeriesOutput::TimeSeriesOutput;
    };

    struct BaseTimeSeriesInput : TimeSeriesInput {
        using TimeSeriesInput::TimeSeriesInput;
    };

    // Old TimeSeriesType base class - stub
    struct TimeSeriesType {
        virtual ~TimeSeriesType() = default;
    };
}
