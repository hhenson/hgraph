#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesValueOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesValueOutput(TSMutableView view);

        // Move constructor/assignment use defaults
        PyTimeSeriesValueOutput(PyTimeSeriesValueOutput&& other) noexcept = default;
        PyTimeSeriesValueOutput& operator=(PyTimeSeriesValueOutput&& other) noexcept = default;

        // Delete copy constructor and assignment
        PyTimeSeriesValueOutput(const PyTimeSeriesValueOutput&) = delete;
        PyTimeSeriesValueOutput& operator=(const PyTimeSeriesValueOutput&) = delete;
    };

    struct PyTimeSeriesValueInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesValueInput(TSView view);

        // Move constructor/assignment use defaults
        PyTimeSeriesValueInput(PyTimeSeriesValueInput&& other) noexcept = default;
        PyTimeSeriesValueInput& operator=(PyTimeSeriesValueInput&& other) noexcept = default;

        // Delete copy constructor and assignment
        PyTimeSeriesValueInput(const PyTimeSeriesValueInput&) = delete;
        PyTimeSeriesValueInput& operator=(const PyTimeSeriesValueInput&) = delete;
    };

    void ts_register_with_nanobind(nb::module_ &m);
}
