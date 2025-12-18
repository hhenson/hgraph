#include <hgraph/api/python/py_ts.h>

namespace hgraph
{
    // Constructors are inherited from base classes via "using" declarations

    void ts_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueOutput, PyTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<PyTimeSeriesValueInput, PyTimeSeriesInput>(m, "TimeSeriesValueInput");
    }

}  // namespace hgraph
