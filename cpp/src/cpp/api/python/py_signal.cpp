#include <hgraph/api/python/py_signal.h>

namespace hgraph
{

    PyTimeSeriesSignal::PyTimeSeriesSignal(TimeSeriesType *ts, control_block_ptr control_block) : PyTimeSeriesInput(ts, std::move(control_block)) {}

    PyTimeSeriesSignal::PyTimeSeriesSignal(TimeSeriesType *ts) : PyTimeSeriesInput(ts) {}

    void signal_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSignal, PyTimeSeriesInput>(m, "TS_Signal");
    }

}  // namespace hgraph
