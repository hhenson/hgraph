#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    struct PyTimeSeriesFixedWindowOutput : PyTimeSeriesOutput
    {
        // View-based constructor
        explicit PyTimeSeriesFixedWindowOutput(TSOutputView view);

        // Move constructor
        PyTimeSeriesFixedWindowOutput(PyTimeSeriesFixedWindowOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesFixedWindowOutput& operator=(PyTimeSeriesFixedWindowOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy
        PyTimeSeriesFixedWindowOutput(const PyTimeSeriesFixedWindowOutput&) = delete;
        PyTimeSeriesFixedWindowOutput& operator=(const PyTimeSeriesFixedWindowOutput&) = delete;

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::int_ size() const;
        [[nodiscard]] nb::int_ min_size() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;
    };

    struct PyTimeSeriesTimeWindowOutput : PyTimeSeriesOutput
    {
        // View-based constructor
        explicit PyTimeSeriesTimeWindowOutput(TSOutputView view);

        // Move constructor
        PyTimeSeriesTimeWindowOutput(PyTimeSeriesTimeWindowOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesTimeWindowOutput& operator=(PyTimeSeriesTimeWindowOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy
        PyTimeSeriesTimeWindowOutput(const PyTimeSeriesTimeWindowOutput&) = delete;
        PyTimeSeriesTimeWindowOutput& operator=(const PyTimeSeriesTimeWindowOutput&) = delete;

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::object size() const;
        [[nodiscard]] nb::object min_size() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;
    };

    struct PyTimeSeriesWindowInput : PyTimeSeriesInput
    {
        // View-based constructor
        explicit PyTimeSeriesWindowInput(TSInputView view);

        // Move constructor
        PyTimeSeriesWindowInput(PyTimeSeriesWindowInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesWindowInput& operator=(PyTimeSeriesWindowInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy
        PyTimeSeriesWindowInput(const PyTimeSeriesWindowInput&) = delete;
        PyTimeSeriesWindowInput& operator=(const PyTimeSeriesWindowInput&) = delete;

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;
    };

    void tsw_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
