#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * PyTimeSeriesWindowOutput - Wrapper for window outputs (TSW)
     *
     * Provides window operations via view.
     */
    struct PyTimeSeriesWindowOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesWindowOutput(PyTimeSeriesWindowOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        PyTimeSeriesWindowOutput& operator=(PyTimeSeriesWindowOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesWindowOutput(const PyTimeSeriesWindowOutput&) = delete;
        PyTimeSeriesWindowOutput& operator=(const PyTimeSeriesWindowOutput&) = delete;

        // Window operations
        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;

        [[nodiscard]] nb::object window_size() const;  // can be int or time-delta
        [[nodiscard]] nb::object min_size() const;     // can be int or time-delta

        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;

        [[nodiscard]] nb::int_ len() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    /**
     * PyTimeSeriesWindowInput - Wrapper for window inputs (TSW)
     *
     * Provides window operations via view.
     */
    struct PyTimeSeriesWindowInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesWindowInput(PyTimeSeriesWindowInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesWindowInput& operator=(PyTimeSeriesWindowInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesWindowInput(const PyTimeSeriesWindowInput&) = delete;
        PyTimeSeriesWindowInput& operator=(const PyTimeSeriesWindowInput&) = delete;

        // Window operations
        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;

        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;

        // Override all_valid to check min_size
        [[nodiscard]] nb::bool_ all_valid() const;

        [[nodiscard]] nb::int_ len() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tsw_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
