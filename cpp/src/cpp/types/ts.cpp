#include <hgraph/types/ts.h>
#include <hgraph/types/node.h>
#include <hgraph/util/string_utils.h>

namespace hgraph
{
    // TimeSeriesValueOutput<T> implementation with delegation to TSOutput

    template <typename T>
    TimeSeriesValueOutput<T>::TimeSeriesValueOutput(const node_ptr &parent)
        : TimeSeriesOutput(parent)
          , _ts_output(static_cast<Notifiable *>(const_cast<Node *>(parent.get())), typeid(T)) {}

    template <typename T>
    TimeSeriesValueOutput<T>::TimeSeriesValueOutput(const TimeSeriesType::ptr &parent)
        : TimeSeriesOutput(parent)
          , _ts_output(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(parent.get())), typeid(T)) {}

    template <typename T>
    nb::object TimeSeriesValueOutput<T>::py_value() const { return valid() ? nb::cast(value()) : nb::none(); }

    template <typename T>
    nb::object TimeSeriesValueOutput<T>::py_delta_value() const { return py_value(); }

    template <typename T>
    void TimeSeriesValueOutput<T>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        set_value(nb::cast<T>(value));
    }

    template <typename T>
    void TimeSeriesValueOutput<T>::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) { return; }
        try { py_set_value(value); } catch (std::exception &e) {
            std::string msg = "Cannot apply node output " + to_string(value) + " of type " +
                              std::string(typeid(T).name()) + " to TimeSeriesValueOutput: " + e.what();
            throw nb::type_error(msg.c_str());
        }
    }

    template <typename T>
    const T &TimeSeriesValueOutput<T>::value() const { return get_from_any<T>(_ts_output.value()); }

    template <typename T>
    void TimeSeriesValueOutput<T>::set_value(const T &value) {
        _ts_output.set_value(make_any_value(value));
        mark_modified();
    }

    template <typename T>
    void TimeSeriesValueOutput<T>::set_value(T &&value) {
        _ts_output.set_value(make_any_value(std::forward<T>(value)));
        mark_modified();
    }

    template <typename T>
    void TimeSeriesValueOutput<T>::mark_invalid() {
        _ts_output.invalidate();
        TimeSeriesOutput::mark_invalid();
    }

    template <typename T>
    void TimeSeriesValueOutput<T>::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_t = dynamic_cast<const TimeSeriesValueOutput<T> &>(output);
        set_value(output_t.value());
    }

    template <typename T>
    void TimeSeriesValueOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        const auto &input_t = dynamic_cast<const TimeSeriesValueInput<T> &>(input);
        set_value(input_t.value());
    }

    template <typename T>
    bool TimeSeriesValueOutput<T>::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueOutput<T> *>(other) != nullptr;
    }

    template <typename T>
    void TimeSeriesValueOutput<T>::reset_value() { _ts_output.invalidate(); }

    // TimeSeriesValueInput<T> implementation with delegation to TSInput

    template <typename T>
    TimeSeriesValueInput<T>::TimeSeriesValueInput(const node_ptr &parent)
        : TimeSeriesInput(parent)
          , _ts_input(static_cast<Notifiable *>(const_cast<Node *>(parent.get())), typeid(T)) {}

    template <typename T>
    TimeSeriesValueInput<T>::TimeSeriesValueInput(const TimeSeriesType::ptr &parent)
        : TimeSeriesInput(parent)
          , _ts_input(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(parent.get())), typeid(T)) {}

    template <typename T>
    TimeSeriesValueOutput<T> &TimeSeriesValueInput<T>::value_output() {
        return dynamic_cast<TimeSeriesValueOutput<T> &>(*output());
    }

    template <typename T>
    const TimeSeriesValueOutput<T> &TimeSeriesValueInput<T>::value_output() const {
        return dynamic_cast<TimeSeriesValueOutput<T> &>(*output());
    }

    template <typename T>
    const T &TimeSeriesValueInput<T>::value() const { return get_from_any<T>(_ts_input.value()); }

    template <typename T>
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

    using TS_Bool          = TimeSeriesValueInput<bool>;
    using TS_Out_Bool      = TimeSeriesValueOutput<bool>;
    using TS_Int           = TimeSeriesValueInput<int64_t>;
    using TS_Out_Int       = TimeSeriesValueOutput<int64_t>;
    using TS_Float         = TimeSeriesValueInput<double>;
    using TS_Out_Float     = TimeSeriesValueOutput<double>;
    using TS_Date          = TimeSeriesValueInput<engine_date_t>;
    using TS_Out_Date      = TimeSeriesValueOutput<engine_date_t>;
    using TS_DateTime      = TimeSeriesValueInput<engine_time_t>;
    using TS_Out_DateTime  = TimeSeriesValueOutput<engine_time_t>;
    using TS_TimeDelta     = TimeSeriesValueInput<engine_time_delta_t>;
    using TS_Out_TimeDelta = TimeSeriesValueOutput<engine_time_delta_t>;
    using TS_Object        = TimeSeriesValueInput<nb::object>;
    using TS_Out_Object    = TimeSeriesValueOutput<nb::object>;

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TS_Out_Bool, TimeSeriesOutput>(m, "TS_Out_Bool");
        nb::class_<TS_Bool, TimeSeriesInput>(m, "TS_Bool");
        nb::class_<TS_Out_Int, TimeSeriesOutput>(m, "TS_Out_Int");
        nb::class_<TS_Int, TimeSeriesInput>(m, "TS_Int");
        nb::class_<TS_Out_Float, TimeSeriesOutput>(m, "TS_Out_Float");
        nb::class_<TS_Float, TimeSeriesInput>(m, "TS_Float");
        nb::class_<TS_Out_Date, TimeSeriesOutput>(m, "TS_Out_Date");
        nb::class_<TS_Date, TimeSeriesInput>(m, "TS_Date");
        nb::class_<TS_Out_DateTime, TimeSeriesOutput>(m, "TS_Out_DateTime");
        nb::class_<TS_DateTime, TimeSeriesInput>(m, "TS_DateTime");
        nb::class_<TS_Out_TimeDelta, TimeSeriesOutput>(m, "TS_Out_TimeDelta");
        nb::class_<TS_TimeDelta, TimeSeriesInput>(m, "TS_TimeDelta");
        nb::class_<TS_Out_Object, TimeSeriesOutput>(m, "TS_Out_Object");
        nb::class_<TS_Object, TimeSeriesInput>(m, "TS_Object");
    }
} // namespace hgraph