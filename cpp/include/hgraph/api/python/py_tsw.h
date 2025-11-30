#pragma once

#include "hgraph/types/tsw.h"

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{

    //TODO: This underlying code was largely AI generated and is a bit of a mess. This needs a good
    //      hard review with some corrections.

    template <typename T_U>
    concept is_tsw_output = std::same_as<T_U, TimeSeriesFixedWindowOutput<typename T_U::value_type>> ||
                 std::same_as<T_U, TimeSeriesTimeWindowOutput<typename T_U::value_type>>;

    template <typename T_U>
        requires is_tsw_output<T_U>
    struct PyTimeSeriesWindowOutput : PyTimeSeriesOutput
    {
        using underlying_type = T_U;
        using underlying_type_ptr = std::shared_ptr<underlying_type>;

        explicit PyTimeSeriesWindowOutput(T_U *impl, const control_block_ptr &cb);
        explicit PyTimeSeriesWindowOutput(T_U *impl);

        [[nodiscard]] nb::object value_times() const;
        [[nodiscard]] engine_time_t first_modified_time() const;

        // Extra API to mirror Python TSW
        [[nodiscard]] nb::object   size() const;  // can be int of time-delta
        [[nodiscard]] nb::object  min_size() const; // can be int or time-delta
        [[nodiscard]] nb::bool_  has_removed_value() const;
        [[nodiscard]] nb::object removed_value() const;

        [[nodiscard]] nb::int_ len() const;

    private:
        [[nodiscard]] underlying_type_ptr impl() const;
    };

    // Unified window input that works with both fixed-size and timedelta outputs
    template <typename T> struct PyTimeSeriesWindowInput : PyTimeSeriesInput
    {
        using underlying_type = TimeSeriesWindowInput<T>;
        using underlying_type_ptr = std::shared_ptr<underlying_type>;

        explicit PyTimeSeriesWindowInput(TimeSeriesWindowInput<T> *impl, const control_block_ptr &cb);
        explicit PyTimeSeriesWindowInput(TimeSeriesWindowInput<T> *impl);
        
        [[nodiscard]] nb::object value_times() const;

        [[nodiscard]] engine_time_t first_modified_time() const;
        
        [[nodiscard]] nb::bool_ has_removed_value() const;
        
        [[nodiscard]] nb::object removed_value() const;

        [[nodiscard]] nb::int_ len() const;
    private:
        [[nodiscard]] underlying_type_ptr impl() const;
    };

    // Registration
    void tsw_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
