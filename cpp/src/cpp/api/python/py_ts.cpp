#include <hgraph/api/python/py_ts.h>

namespace hgraph
{
    // View-based constructor (the only supported mode)
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    // View-based constructor (the only supported mode)
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TSView view)
        : PyTimeSeriesInput(view) {}

    void ts_register_with_nanobind(nb::module_ &m) {
        // No need to re-register value property - base class handles it via TSView
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph
