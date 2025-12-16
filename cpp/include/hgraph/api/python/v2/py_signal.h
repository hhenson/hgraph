#pragma once
#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesSignalInput : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesType>;
        explicit PyTimeSeriesSignalInput(api_ptr impl);
        explicit PyTimeSeriesSignalInput(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesSignalInput(TimeSeriesType *ts);
    };

    void signal_register_with_nanobind(nb::module_ &m);
}
