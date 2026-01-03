#include <hgraph/types/ts.h>
#include <hgraph/util/string_utils.h>

namespace hgraph {

    // ============================================================================
    // TimeSeriesValueOutput Implementation
    // ============================================================================

    TimeSeriesValueOutput::TimeSeriesValueOutput(Node* owning_node, const value::TypeMeta* schema)
        : TimeSeriesValueOutputBase(owning_node), _value(schema) {}

    TimeSeriesValueOutput::TimeSeriesValueOutput(TimeSeriesOutput* parent, const value::TypeMeta* schema)
        : TimeSeriesValueOutputBase(parent), _value(schema) {}

    nb::object TimeSeriesValueOutput::py_value() const {
        return valid() ? _value.to_python() : nb::none();
    }

    nb::object TimeSeriesValueOutput::py_delta_value() const {
        return py_value();
    }

    void TimeSeriesValueOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        _value.from_python(value);
        mark_modified();
    }

    void TimeSeriesValueOutput::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) {
            return;
        }
        try {
            py_set_value(value);
        } catch (std::exception& e) {
            std::string msg = "Cannot apply node output " + to_string(value) +
                              " to TimeSeriesValueOutput: " + e.what();
            throw nb::type_error(msg.c_str());
        }
    }

    void TimeSeriesValueOutput::mark_invalid() {
        _value = value::CachedValue(schema());  // Reset to default
        BaseTimeSeriesOutput::mark_invalid();
    }

    void TimeSeriesValueOutput::copy_from_output(const TimeSeriesOutput& output) {
        auto* other = dynamic_cast<const TimeSeriesValueOutput*>(&output);
        if (!other) {
            throw std::runtime_error("TimeSeriesValueOutput::copy_from_output: type mismatch");
        }
        if (other->valid()) {
            _value.view().copy_from(other->_value.const_view());
            mark_modified();
        } else {
            mark_invalid();
        }
    }

    void TimeSeriesValueOutput::copy_from_input(const TimeSeriesInput& input) {
        auto* other = dynamic_cast<const TimeSeriesValueInput*>(&input);
        if (!other) {
            throw std::runtime_error("TimeSeriesValueOutput::copy_from_input: type mismatch");
        }
        if (other->valid()) {
            _value.view().copy_from(other->value());
            mark_modified();
        } else {
            mark_invalid();
        }
    }

    bool TimeSeriesValueOutput::is_same_type(const TimeSeriesType* other) const {
        auto* ts = dynamic_cast<const TimeSeriesValueOutput*>(other);
        return ts && ts->schema() == schema();
    }

    void TimeSeriesValueOutput::reset_value() {
        _value = value::CachedValue(schema());
    }

    // ============================================================================
    // TimeSeriesValueInput Implementation
    // ============================================================================

    TimeSeriesValueOutput& TimeSeriesValueInput::value_output() {
        return dynamic_cast<TimeSeriesValueOutput&>(*output());
    }

    const TimeSeriesValueOutput& TimeSeriesValueInput::value_output() const {
        return dynamic_cast<const TimeSeriesValueOutput&>(*output());
    }

    value::ConstValueView TimeSeriesValueInput::value() const {
        return value_output().value();
    }

    const value::TypeMeta* TimeSeriesValueInput::schema() const {
        return value_output().schema();
    }

    bool TimeSeriesValueInput::is_same_type(const TimeSeriesType* other) const {
        auto* ts = dynamic_cast<const TimeSeriesValueInput*>(other);
        return ts != nullptr;
    }

    // ============================================================================
    // Python Bindings
    // ============================================================================

    void register_ts_with_nanobind(nb::module_& m) {
        // TimeSeriesValueOutputBase - intermediate class for visitor pattern
        nb::class_<TimeSeriesValueOutputBase, BaseTimeSeriesOutput>(m, "TimeSeriesValueOutputBase");

        // TimeSeriesValueOutput - created via builders, not directly from Python
        nb::class_<TimeSeriesValueOutput, TimeSeriesValueOutputBase>(m, "TimeSeriesValueOutput")
            .def_prop_ro("schema", &TimeSeriesValueOutput::schema);

        // TimeSeriesValueInputBase - intermediate class for visitor pattern
        nb::class_<TimeSeriesValueInputBase, BaseTimeSeriesInput>(m, "TimeSeriesValueInputBase");

        // TimeSeriesValueInput - created via builders
        nb::class_<TimeSeriesValueInput, TimeSeriesValueInputBase>(m, "TimeSeriesValueInput")
            .def_prop_ro("schema", &TimeSeriesValueInput::schema);
    }

} // namespace hgraph
