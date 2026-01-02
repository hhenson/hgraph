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

        explicit PyTimeSeriesFixedWindowOutput(api_ptr impl);

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

        explicit PyTimeSeriesTimeWindowOutput(api_ptr impl);

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

        explicit PyTimeSeriesWindowInput(api_ptr impl);

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
