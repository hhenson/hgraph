#include <hgraph/api/python/py_ts.h>

#include <utility>

namespace hgraph
{

    // Legacy constructor - uses ApiPtr
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    // New view-based constructor
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}


    // Legacy constructor - uses ApiPtr
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(api_ptr impl)
        : PyTimeSeriesInput(std::move(impl)) {}

    // New view-based constructor
    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}


    void ts_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph