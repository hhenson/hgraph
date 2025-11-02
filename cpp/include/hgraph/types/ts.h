//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2/ts_value_helpers.h>

namespace hgraph
{
    template <typename T>
    struct TimeSeriesValueOutput : TimeSeriesOutput
    {
        using value_type = T;
        using ptr = nb::ref<TimeSeriesValueOutput<T>>;

        // Constructor - delegates to base and creates TSOutput
        explicit TimeSeriesValueOutput(const node_ptr &parent);
        explicit TimeSeriesValueOutput(const TimeSeriesType::ptr &parent);

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(nb::object value) override;

        void apply_result(nb::object value) override;

        const T& value() const;

        void set_value(const T& value);

        void set_value(T&& value);

        void mark_invalid() override;

        void copy_from_output(const TimeSeriesOutput& output) override;

        void copy_from_input(const TimeSeriesInput& input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

        void reset_value();

    private:
        TSOutput _ts_output;
    };

    template <typename T>
    struct TimeSeriesValueInput : TimeSeriesInput
    {
        using value_type = T;
        using ptr = nb::ref<TimeSeriesValueInput<T>>;

        // Constructor - delegates to base and creates TSInput
        explicit TimeSeriesValueInput(const node_ptr &parent);
        explicit TimeSeriesValueInput(const TimeSeriesType::ptr &parent);

        [[nodiscard]] TimeSeriesValueOutput<T>& value_output();
        [[nodiscard]] const TimeSeriesValueOutput<T>& value_output() const;

        [[nodiscard]] const T& value() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

    private:
        TSInput _ts_input;
    };

    void register_ts_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TS_H