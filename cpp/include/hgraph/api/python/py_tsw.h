#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesFixedWindowOutput.
     *
     * Note: value(), delta_value(), all_valid() are inherited from PyTimeSeriesOutput
     * and work correctly through the view/ops layer (WindowStorageOps).
     */
    struct PyTimeSeriesFixedWindowOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesFixedWindowOutput(TSMutableView view);

        // Move constructor/assignment
        PyTimeSeriesFixedWindowOutput(PyTimeSeriesFixedWindowOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}
        PyTimeSeriesFixedWindowOutput& operator=(PyTimeSeriesFixedWindowOutput&& other) noexcept = default;
        PyTimeSeriesFixedWindowOutput(const PyTimeSeriesFixedWindowOutput&) = delete;
        PyTimeSeriesFixedWindowOutput& operator=(const PyTimeSeriesFixedWindowOutput&) = delete;

        // Window-specific accessors (not in base class)
        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::int_ size() const;
        [[nodiscard]] nb::int_ min_size() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;
    };

    /**
     * @brief Python wrapper for TimeSeriesTimeWindowOutput.
     */
    struct PyTimeSeriesTimeWindowOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesTimeWindowOutput(TSMutableView view);

        // Move constructor/assignment
        PyTimeSeriesTimeWindowOutput(PyTimeSeriesTimeWindowOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}
        PyTimeSeriesTimeWindowOutput& operator=(PyTimeSeriesTimeWindowOutput&& other) noexcept = default;
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

    /**
     * @brief Python wrapper for TimeSeriesWindowInput.
     *
     * Note: value(), delta_value(), all_valid() are inherited from PyTimeSeriesInput
     * and work correctly through the view/ops layer (WindowStorageOps).
     */
    struct PyTimeSeriesWindowInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesWindowInput(TSView view);

        // Move constructor/assignment
        PyTimeSeriesWindowInput(PyTimeSeriesWindowInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}
        PyTimeSeriesWindowInput& operator=(PyTimeSeriesWindowInput&& other) noexcept = default;
        PyTimeSeriesWindowInput(const PyTimeSeriesWindowInput&) = delete;
        PyTimeSeriesWindowInput& operator=(const PyTimeSeriesWindowInput&) = delete;

        // Window-specific accessors (not in base class)
        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;
        [[nodiscard]] nb::bool_ has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;
        [[nodiscard]] nb::int_ len() const;
    };

    // Registration
    void tsw_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
