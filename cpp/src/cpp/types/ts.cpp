#include <hgraph/types/ts.h>
#include <hgraph/util/string_utils.h>

namespace hgraph {
    template<typename T>
    nb::object TimeSeriesValueOutput<T>::py_value() const { return valid() ? nb::cast(_value) : nb::none(); }

    template<typename T>
    nb::object TimeSeriesValueOutput<T>::py_delta_value() const { return py_value(); }

    template<typename T>
    void TimeSeriesValueOutput<T>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        set_value(nb::cast<T>(value));
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) { return; }
        try {
            py_set_value(value);
        } catch (std::exception &e) {
            std::string msg = "Cannot apply node output " + to_string(value) + " of type " +
                              std::string(typeid(T).name()) + " to TimeSeriesValueOutput: " + e.what();
            throw nb::type_error(msg.c_str());
        }
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::set_value(const T &value) {
        _value = value;
        mark_modified();
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::set_value(T &&value) {
        _value = std::move(value);
        mark_modified();
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::mark_invalid() {
        _value = {}; // Set to the equivalent of none
        BaseTimeSeriesOutput::mark_invalid();
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_t = dynamic_cast<const TimeSeriesValueOutput<T> &>(output);
        if (output_t.valid()){
            set_value(output_t._value);
        } else {
            mark_invalid();
        }
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        const auto &input_t = dynamic_cast<const TimeSeriesValueInput<T> &>(input);
        if (input_t.valid()) {
            set_value(input_t.value());
        } else {
            mark_invalid();
        }
    }

    template<typename T>
    bool TimeSeriesValueOutput<T>::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueOutput<T> *>(other) != nullptr;
    }

    template<typename T>
    void TimeSeriesValueOutput<T>::reset_value() {
        _value = {};
    }

    template<typename T>
    TimeSeriesValueOutput<T> &TimeSeriesValueInput<T>::value_output() {
        return dynamic_cast<TimeSeriesValueOutput<T> &>(*output());
    }

    template<typename T>
    const TimeSeriesValueOutput<T> &TimeSeriesValueInput<T>::value_output() const {
        return dynamic_cast<TimeSeriesValueOutput<T> &>(*output());
    }

    template<typename T>
    const T &TimeSeriesValueInput<T>::value() const { return value_output().value(); }

    template<typename T>
    bool TimeSeriesValueInput<T>::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueInput<T> *>(other) != nullptr;
    }

    // TODO: How to better track the types we have registered as there is a corresponding item to deal with in the output and input
    // builders.
    template struct TimeSeriesValueInput<bool>;
    template struct TimeSeriesValueInput<int64_t>;
    template struct TimeSeriesValueInput<double>;
    template struct TimeSeriesValueInput<engine_date_t>;
    template struct TimeSeriesValueInput<engine_time_t>;
    template struct TimeSeriesValueInput<engine_time_delta_t>;
    template struct TimeSeriesValueInput<nb::object>;

    template struct TimeSeriesValueOutput<bool>;
    template struct TimeSeriesValueOutput<int64_t>;
    template struct TimeSeriesValueOutput<double>;
    template struct TimeSeriesValueOutput<engine_date_t>;
    template struct TimeSeriesValueOutput<engine_time_t>;
    template struct TimeSeriesValueOutput<engine_time_delta_t>;
    template struct TimeSeriesValueOutput<nb::object>;

    using TS_Bool = TimeSeriesValueInput<bool>;
    using TS_Out_Bool = TimeSeriesValueOutput<bool>;
    using TS_Int = TimeSeriesValueInput<int64_t>;
    using TS_Out_Int = TimeSeriesValueOutput<int64_t>;
    using TS_Float = TimeSeriesValueInput<double>;
    using TS_Out_Float = TimeSeriesValueOutput<double>;
    using TS_Date = TimeSeriesValueInput<engine_date_t>;
    using TS_Out_Date = TimeSeriesValueOutput<engine_date_t>;
    using TS_DateTime = TimeSeriesValueInput<engine_time_t>;
    using TS_Out_DateTime = TimeSeriesValueOutput<engine_time_t>;
    using TS_TimeDelta = TimeSeriesValueInput<engine_time_delta_t>;
    using TS_Out_TimeDelta = TimeSeriesValueOutput<engine_time_delta_t>;
    using TS_Object = TimeSeriesValueInput<nb::object>;
    using TS_Out_Object = TimeSeriesValueOutput<nb::object>;

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TS_Out_Bool, BaseTimeSeriesOutput>(m, "TS_Out_Bool");
        nb::class_<TS_Bool, BaseTimeSeriesInput>(m, "TS_Bool");
        nb::class_<TS_Out_Int, BaseTimeSeriesOutput>(m, "TS_Out_Int");
        nb::class_<TS_Int, BaseTimeSeriesInput>(m, "TS_Int");
        nb::class_<TS_Out_Float, BaseTimeSeriesOutput>(m, "TS_Out_Float");
        nb::class_<TS_Float, BaseTimeSeriesInput>(m, "TS_Float");
        nb::class_<TS_Out_Date, BaseTimeSeriesOutput>(m, "TS_Out_Date");
        nb::class_<TS_Date, BaseTimeSeriesInput>(m, "TS_Date");
        nb::class_<TS_Out_DateTime, BaseTimeSeriesOutput>(m, "TS_Out_DateTime");
        nb::class_<TS_DateTime, BaseTimeSeriesInput>(m, "TS_DateTime");
        nb::class_<TS_Out_TimeDelta, BaseTimeSeriesOutput>(m, "TS_Out_TimeDelta");
        nb::class_<TS_TimeDelta, BaseTimeSeriesInput>(m, "TS_TimeDelta");
        nb::class_<TS_Out_Object, BaseTimeSeriesOutput>(m, "TS_Out_Object");
        nb::class_<TS_Object, BaseTimeSeriesInput>(m, "TS_Object");
    }
} // namespace hgraph