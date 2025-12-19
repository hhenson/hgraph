// STUB: Old ts_signal.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy signal types - stubs for migration
    struct TimeSeriesSignalOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesSignalInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };
}
