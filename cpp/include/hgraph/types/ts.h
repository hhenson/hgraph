//
// Created by Howard Henson on 03/01/2025.
//

#ifndef TS_H
#define TS_H

#include <hgraph/types/base_time_series.h>

namespace hgraph {

    struct TimeSeriesValueOutputBase : BaseTimeSeriesOutput {
        using BaseTimeSeriesOutput::BaseTimeSeriesOutput;

        VISITOR_SUPPORT()
    };

    template<typename T>
    struct TimeSeriesValueOutput final : TimeSeriesValueOutputBase {
        using value_type = T;
        using s_ptr = std::shared_ptr<TimeSeriesValueOutput<T>>;

        using TimeSeriesValueOutputBase::TimeSeriesValueOutputBase;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void py_set_value(const nb::object& value) override;

        void apply_result(const nb::object& value) override;

        const T &value() const { return _value; }

        void set_value(const T &value);

        void set_value(T &&value);

        void mark_invalid() override;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        void reset_value();

        VISITOR_SUPPORT()

    private:
        T _value{};
    };

    struct TimeSeriesValueInputBase : BaseTimeSeriesInput {
        using BaseTimeSeriesInput::BaseTimeSeriesInput;

        VISITOR_SUPPORT()
    };

    template<typename T>
    struct TimeSeriesValueInput final : TimeSeriesValueInputBase {
        using value_type = T;
        using ptr = TimeSeriesValueInput<T>*;

        using TimeSeriesValueInputBase::TimeSeriesValueInputBase;

        [[nodiscard]] TimeSeriesValueOutput<T> &value_output();

        [[nodiscard]] const TimeSeriesValueOutput<T> &value_output() const;

        [[nodiscard]] const T &value() const;

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        VISITOR_SUPPORT()
    };

    void register_ts_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TS_H