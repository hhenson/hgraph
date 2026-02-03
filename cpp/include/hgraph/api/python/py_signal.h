#pragma once
#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesSignalInput : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesType>;

        // Legacy constructors - uses ApiPtr
        explicit PyTimeSeriesSignalInput(api_ptr impl);
        explicit PyTimeSeriesSignalInput(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesSignalInput(TimeSeriesType *ts);

        // New view-based constructor
        explicit PyTimeSeriesSignalInput(TSInputView view);

        // Move constructor
        PyTimeSeriesSignalInput(PyTimeSeriesSignalInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesSignalInput& operator=(PyTimeSeriesSignalInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesSignalInput(const PyTimeSeriesSignalInput&) = delete;
        PyTimeSeriesSignalInput& operator=(const PyTimeSeriesSignalInput&) = delete;
    };

    void signal_register_with_nanobind(nb::module_ &m);
}
