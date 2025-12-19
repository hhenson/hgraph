// STUB: Old tsd.h removed - types migrated to new system
// This stub exists only to allow incremental migration
#pragma once

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph {
    // Legacy dict types - stubs for migration
    struct TimeSeriesDictOutput : ts::TSOutput {
        using ts::TSOutput::TSOutput;
    };

    struct TimeSeriesDictInput : ts::TSInput {
        using ts::TSInput::TSInput;
    };

    template<typename K, typename V = ts::TSOutput>
    struct TimeSeriesDictOutput_T : TimeSeriesDictOutput {
        using TimeSeriesDictOutput::TimeSeriesDictOutput;
    };

    template<typename K, typename V = ts::TSInput>
    struct TimeSeriesDictInput_T : TimeSeriesDictInput {
        using TimeSeriesDictInput::TimeSeriesDictInput;
    };
}
