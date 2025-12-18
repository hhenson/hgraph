#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * PyTimeSeriesValueOutput - Wrapper for simple value outputs (TS[T])
     *
     * Inherits all functionality from base class.
     * No additional methods needed for simple scalar values.
     */
    struct PyTimeSeriesValueOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesValueOutput(PyTimeSeriesValueOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        PyTimeSeriesValueOutput& operator=(PyTimeSeriesValueOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesValueOutput(const PyTimeSeriesValueOutput&) = delete;
        PyTimeSeriesValueOutput& operator=(const PyTimeSeriesValueOutput&) = delete;
    };

    /**
     * PyTimeSeriesValueInput - Wrapper for simple value inputs (TS[T])
     *
     * Inherits all functionality from base class.
     * No additional methods needed for simple scalar values.
     */
    struct PyTimeSeriesValueInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesValueInput(PyTimeSeriesValueInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesValueInput& operator=(PyTimeSeriesValueInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesValueInput(const PyTimeSeriesValueInput&) = delete;
        PyTimeSeriesValueInput& operator=(const PyTimeSeriesValueInput&) = delete;
    };

    void ts_register_with_nanobind(nb::module_ &m);
}
