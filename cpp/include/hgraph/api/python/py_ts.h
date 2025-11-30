#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesValueOutput : PyTimeSeriesOutput
    {
        explicit PyTimeSeriesValueOutput(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesValueOutput(TimeSeriesType *ts);

        // Move constructor
        PyTimeSeriesValueOutput(PyTimeSeriesValueOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesValueOutput& operator=(PyTimeSeriesValueOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesValueOutput(const PyTimeSeriesValueOutput&) = delete;
        PyTimeSeriesValueOutput& operator=(const PyTimeSeriesValueOutput&) = delete;
    };

    struct PyTimeSeriesValueInput : PyTimeSeriesInput
    {
        explicit PyTimeSeriesValueInput(TimeSeriesType *ts, control_block_ptr control_block);
        explicit PyTimeSeriesValueInput(TimeSeriesType *ts);

        // Move constructor
        PyTimeSeriesValueInput(PyTimeSeriesValueInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesValueInput& operator=(PyTimeSeriesValueInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesValueInput(const PyTimeSeriesValueInput&) = delete;
        PyTimeSeriesValueInput& operator=(const PyTimeSeriesValueInput&) = delete;
    };

    void ts_register_with_nanobind(nb::module_ &m);
}
