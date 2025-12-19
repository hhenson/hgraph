// STUB: Old tsb.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy bundle types - stubs for migration
    struct TimeSeriesBundleOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesBundleInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    template<typename... Ts>
    struct TimeSeriesBundleOutput_T : TimeSeriesBundleOutput {
        using TimeSeriesBundleOutput::TimeSeriesBundleOutput;
    };

    template<typename... Ts>
    struct TimeSeriesBundleInput_T : TimeSeriesBundleInput {
        using TimeSeriesBundleInput::TimeSeriesBundleInput;
    };
}
