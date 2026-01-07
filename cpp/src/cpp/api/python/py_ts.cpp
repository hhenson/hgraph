#include <hgraph/api/python/py_ts.h>

#include <utility>

namespace hgraph
{
    // ApiPtr-based constructor (to be removed once wrapping uses views)
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    // View-based constructor
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TSMutableView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    // ApiPtr-based constructor (to be removed once wrapping uses views)
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(api_ptr impl)
        : PyTimeSeriesInput(std::move(impl)) {}

    // View-based constructor
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TSView view)
        : PyTimeSeriesInput(std::move(view)) {}

    void ts_register_with_nanobind(nb::module_ &m) {
        // No need to re-register value property - base class handles it via TSView
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph
