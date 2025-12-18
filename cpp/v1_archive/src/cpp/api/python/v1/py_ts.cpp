#include <hgraph/api/python/py_ts.h>

#include <utility>

namespace hgraph
{

    PyTimeSeriesValueOutput::PyTimeSeriesValueOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}


    PyTimeSeriesValueInput::PyTimeSeriesValueInput(api_ptr impl)
        : PyTimeSeriesInput(std::move(impl)) {}


    void ts_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph