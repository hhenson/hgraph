#include <hgraph/types/ts.h>
#include <hgraph/util/string_utils.h>

namespace hgraph {

    // TimeSeriesValueOutput implementation

    TimeSeriesValueOutput::TimeSeriesValueOutput(node_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesOutput(parent), _value_type(&tp) {}

    TimeSeriesValueOutput::TimeSeriesValueOutput(time_series_output_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesOutput(parent), _value_type(&tp) {}

    nb::object TimeSeriesValueOutput::py_value() const {
        if (!valid() || !_value.has_value()) return nb::none();

        // Check if stored as nb::object first
        if (auto *obj = _value.get_if<nb::object>()) return *obj;

        // Cast specific types
        if (auto *b = _value.get_if<bool>()) return nb::cast(*b);
        if (auto *i = _value.get_if<int64_t>()) return nb::cast(*i);
        if (auto *d = _value.get_if<double>()) return nb::cast(*d);
        if (auto *dt = _value.get_if<engine_date_t>()) return nb::cast(*dt);
        if (auto *t = _value.get_if<engine_time_t>()) return nb::cast(*t);
        if (auto *td = _value.get_if<engine_time_delta_t>()) return nb::cast(*td);

        return nb::none();
    }

    nb::object TimeSeriesValueOutput::py_delta_value() const {
        return py_value();
    }

    void TimeSeriesValueOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }

        // Store based on expected type
        if (*_value_type == typeid(bool)) {
            _value.emplace<bool>(nb::cast<bool>(value));
        } else if (*_value_type == typeid(int64_t)) {
            _value.emplace<int64_t>(nb::cast<int64_t>(value));
        } else if (*_value_type == typeid(double)) {
            _value.emplace<double>(nb::cast<double>(value));
        } else if (*_value_type == typeid(engine_date_t)) {
            _value.emplace<engine_date_t>(nb::cast<engine_date_t>(value));
        } else if (*_value_type == typeid(engine_time_t)) {
            _value.emplace<engine_time_t>(nb::cast<engine_time_t>(value));
        } else if (*_value_type == typeid(engine_time_delta_t)) {
            _value.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(value));
        } else {
            _value.emplace<nb::object>(value);
        }
        mark_modified();
    }

    bool TimeSeriesValueOutput::can_apply_result(const nb::object &value) {
        return !modified();
    }

    void TimeSeriesValueOutput::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        try {
            py_set_value(value);
        } catch (std::exception &e) {
            std::string msg = "Cannot apply node output " + to_string(value) + " of type " +
                              std::string(_value_type->name()) + " to TimeSeriesValueOutput: " + e.what();
            throw nb::type_error(msg.c_str());
        }
    }

    void TimeSeriesValueOutput::mark_invalid() {
        _value.reset();
        BaseTimeSeriesOutput::mark_invalid();
    }

    void TimeSeriesValueOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_t = dynamic_cast<const TimeSeriesValueOutput &>(output);
        if (output_t.valid() && output_t._value.has_value()) {
            _value = output_t._value;
            mark_modified();
        } else {
            mark_invalid();
        }
    }

    void TimeSeriesValueOutput::copy_from_input(const TimeSeriesInput &input) {
        const auto &input_t = dynamic_cast<const TimeSeriesValueInput &>(input);
        if (input_t.valid()) {
            // Get value from bound output
            _value = input_t.value_output()._value;
            mark_modified();
        } else {
            mark_invalid();
        }
    }

    bool TimeSeriesValueOutput::is_same_type(const TimeSeriesType *other) const {
        auto *other_out = dynamic_cast<const TimeSeriesValueOutput *>(other);
        return other_out != nullptr && *other_out->_value_type == *_value_type;
    }

    // TimeSeriesValueInput implementation

    TimeSeriesValueInput::TimeSeriesValueInput(node_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesInput(parent), _value_type(&tp) {}

    TimeSeriesValueInput::TimeSeriesValueInput(time_series_input_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesInput(parent), _value_type(&tp) {}

    TimeSeriesValueOutput& TimeSeriesValueInput::value_output() {
        return dynamic_cast<TimeSeriesValueOutput &>(*output());
    }

    const TimeSeriesValueOutput& TimeSeriesValueInput::value_output() const {
        return dynamic_cast<const TimeSeriesValueOutput &>(*output());
    }

    nb::object TimeSeriesValueInput::py_value() const {
        if (!valid()) return nb::none();
        return value_output().py_value();
    }

    nb::object TimeSeriesValueInput::py_delta_value() const {
        return py_value();
    }

    bool TimeSeriesValueInput::is_same_type(const TimeSeriesType *other) const {
        auto *other_in = dynamic_cast<const TimeSeriesValueInput *>(other);
        return other_in != nullptr && *other_in->_value_type == *_value_type;
    }

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueOutput, BaseTimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<TimeSeriesValueInput, BaseTimeSeriesInput>(m, "TimeSeriesValueInput");
    }
} // namespace hgraph
