#include <hgraph/api/python/py_signal.h>

namespace hgraph
{
    // Constructor is inherited from base class via "using" declaration

    void signal_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TS_Signal");
    }

}  // namespace hgraph
