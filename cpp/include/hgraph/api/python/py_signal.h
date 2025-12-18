#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * PyTimeSeriesSignalInput - Wrapper for signal inputs
     *
     * Signal is a special input type that carries no value, just modification state.
     * Inherits all functionality from base class.
     */
    struct PyTimeSeriesSignalInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesSignalInput(PyTimeSeriesSignalInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesSignalInput& operator=(PyTimeSeriesSignalInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesSignalInput(const PyTimeSeriesSignalInput&) = delete;
        PyTimeSeriesSignalInput& operator=(const PyTimeSeriesSignalInput&) = delete;
    };

    void signal_register_with_nanobind(nb::module_ &m);
}
