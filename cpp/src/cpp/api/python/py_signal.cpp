#include <hgraph/api/python/py_signal.h>

namespace hgraph
{
    // View-based constructor (the only supported mode)
    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(TSView view) : PyTimeSeriesInput(view) {}

    void signal_register_with_nanobind(nb::module_ &m) {
        // No need to re-register value property - base class handles it via TSView
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TS_Signal");
    }

}  // namespace hgraph
