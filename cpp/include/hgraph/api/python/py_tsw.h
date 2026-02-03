#pragma once

#include "hgraph/types/tsw.h"
#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * @brief Non-templated Python wrapper for TimeSeriesFixedWindowOutput.
     */
    struct PyTimeSeriesFixedWindowOutput : PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesFixedWindowOutput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesFixedWindowOutput(api_ptr impl);

        // New view-based constructor
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

        // Delete copy constructor and assignment
        PyTimeSeriesFixedWindowOutput(const PyTimeSeriesFixedWindowOutput&) = delete;
        PyTimeSeriesFixedWindowOutput& operator=(const PyTimeSeriesFixedWindowOutput&) = delete;

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::int_ size() const;
        [[nodiscard]] nb::int_ min_size() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;

    private:
        [[nodiscard]] TimeSeriesFixedWindowOutput* impl() const;
    };

    /**
     * @brief Non-templated Python wrapper for TimeSeriesTimeWindowOutput.
     */
    struct PyTimeSeriesTimeWindowOutput : PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesTimeWindowOutput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesTimeWindowOutput(api_ptr impl);

        // New view-based constructor
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

        // Delete copy constructor and assignment
        PyTimeSeriesTimeWindowOutput(const PyTimeSeriesTimeWindowOutput&) = delete;
        PyTimeSeriesTimeWindowOutput& operator=(const PyTimeSeriesTimeWindowOutput&) = delete;

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::object size() const;
        [[nodiscard]] nb::object min_size() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;

    private:
        [[nodiscard]] TimeSeriesTimeWindowOutput* impl() const;
    };

    /**
     * @brief Non-templated Python wrapper for TimeSeriesWindowInput.
     */
    struct PyTimeSeriesWindowInput : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesWindowInput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesWindowInput(api_ptr impl);

        // New view-based constructor
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

        // Delete copy constructor and assignment
        PyTimeSeriesWindowInput(const PyTimeSeriesWindowInput&) = delete;
        PyTimeSeriesWindowInput& operator=(const PyTimeSeriesWindowInput&) = delete;

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;

    private:
        [[nodiscard]] TimeSeriesWindowInput* impl() const;
    };

    // Registration
    void tsw_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
