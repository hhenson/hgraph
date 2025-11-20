#pragma once
#include "py_time_series.h"

namespace hgraph
{
    struct PyTimeSeriesSignal : PyTimeSeriesInput
    {
        explicit PyTimeSeriesSignal(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesSignal(TimeSeriesType *ts);
    };

    void signal_register_with_nanobind(nb::module_ &m);
}
