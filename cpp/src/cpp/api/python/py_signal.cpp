#include <hgraph/api/python/py_signal.h>

namespace hgraph
{

    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(api_ptr impl) : PyTimeSeriesInput(std::move(impl)) {}

    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(TSInputView view) : PyTimeSeriesInput(std::move(view)) {}

    void signal_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TS_Signal");
    }

}  // namespace hgraph
