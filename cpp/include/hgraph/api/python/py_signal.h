#pragma once
#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesSignalInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesSignalInput(TSView view);

        // Move constructor/assignment use defaults
        PyTimeSeriesSignalInput(PyTimeSeriesSignalInput&& other) noexcept = default;
        PyTimeSeriesSignalInput& operator=(PyTimeSeriesSignalInput&& other) noexcept = default;

        // Delete copy constructor and assignment
        PyTimeSeriesSignalInput(const PyTimeSeriesSignalInput&) = delete;
        PyTimeSeriesSignalInput& operator=(const PyTimeSeriesSignalInput&) = delete;
    };

    void signal_register_with_nanobind(nb::module_ &m);
}
