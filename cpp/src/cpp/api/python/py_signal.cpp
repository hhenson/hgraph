#include <hgraph/api/python/py_signal.h>

namespace hgraph
{

    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(api_ptr impl) : PyTimeSeriesInput(std::move(impl)) {}

    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(TimeSeriesType *ts, control_block_ptr control_block) : PyTimeSeriesInput(ts, std::move(control_block)) {}

    PyTimeSeriesSignalInput::PyTimeSeriesSignalInput(TimeSeriesType *ts) : PyTimeSeriesInput(ts) {}

    void signal_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSignalInput, PyTimeSeriesInput>(m, "TS_Signal");
    }

}  // namespace hgraph
