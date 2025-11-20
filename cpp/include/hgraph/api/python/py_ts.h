#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesValueOutput : PyTimeSeriesOutput
    {
        explicit PyTimeSeriesValueOutput(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesValueOutput(TimeSeriesType *ts);
    };

    struct PyTimeSeriesValueInput : PyTimeSeriesInput
    {
        explicit PyTimeSeriesValueInput(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesValueInput(TimeSeriesType *ts);
    };

    void ts_register_with_nanobind(nb::module_ &m);
}
