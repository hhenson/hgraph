#include <hgraph/api/python/py_ts.h>

#include <utility>

namespace hgraph
{

    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TimeSeriesType *ts, control_block_ptr control_block)
        : PyTimeSeriesOutput(ts, std::move(control_block)) {}

    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TimeSeriesType *ts) : PyTimeSeriesOutput(ts) {}

    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TimeSeriesType *ts, control_block_ptr control_block)
        : PyTimeSeriesInput(ts, std::move(control_block)) {}

    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TimeSeriesType *ts) : PyTimeSeriesInput(ts) {}

    void ts_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph