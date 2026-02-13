#include <hgraph/api/python/py_ts.h>

#include <utility>

namespace hgraph
{

    // View-based constructors
    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    PyTimeSeriesValueInput::PyTimeSeriesValueInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    void ts_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph
