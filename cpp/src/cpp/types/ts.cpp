#include <hgraph/types/ts.h>
#include <hgraph/types/node.h>
#include <hgraph/util/string_utils.h>

namespace hgraph {

    // Helper to get NotifiableContext from node_ptr
    static NotifiableContext* get_context(node_ptr node) {
        auto* ctx = dynamic_cast<NotifiableContext*>(node);
        if (!ctx) {
            throw std::runtime_error("Node does not implement NotifiableContext");
        }
        return ctx;
    }

    // Helper to get NotifiableContext from time_series_output_ptr by traversing to owning node
    static NotifiableContext* get_context(time_series_output_ptr ts_output) {
        auto* node = ts_output->owning_node();
        if (!node) {
            throw std::runtime_error("TimeSeriesOutput has no owning node");
        }
        return get_context(node);
    }

    // Helper to get NotifiableContext from time_series_input_ptr by traversing to owning node
    static NotifiableContext* get_context(time_series_input_ptr ts_input) {
        auto* node = ts_input->owning_node();
        if (!node) {
            throw std::runtime_error("TimeSeriesInput has no owning node");
        }
        return get_context(node);
    }

    // TimeSeriesValueOutput implementation

    TimeSeriesValueOutput::TimeSeriesValueOutput(node_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesOutput(parent), _ts_output(get_context(parent), tp) {}

    TimeSeriesValueOutput::TimeSeriesValueOutput(time_series_output_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesOutput(parent), _ts_output(get_context(parent), tp) {}

    nb::object TimeSeriesValueOutput::py_value() const {
        if (!valid()) return nb::none();

        const auto& av = _ts_output.value();
        if (!av.has_value()) return nb::none();

        // Check if stored as nb::object first
        if (auto *obj = av.get_if<nb::object>()) return *obj;

        // Cast specific types
        if (auto *b = av.get_if<bool>()) return nb::cast(*b);
        if (auto *i = av.get_if<int64_t>()) return nb::cast(*i);
        if (auto *d = av.get_if<double>()) return nb::cast(*d);
        if (auto *dt = av.get_if<engine_date_t>()) return nb::cast(*dt);
        if (auto *t = av.get_if<engine_time_t>()) return nb::cast(*t);
        if (auto *td = av.get_if<engine_time_delta_t>()) return nb::cast(*td);

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

        const auto& tp = _ts_output.value_type();
        AnyValue<> av;

        // Store based on expected type
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(value));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(value));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(value));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(value));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(value));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(value));
        } else {
            av.emplace<nb::object>(value);
        }

        _ts_output.set_value(std::move(av));
        // TSOutput::set_value already notifies via its parent mechanism,
        // but we still need to trigger BaseTimeSeriesOutput notification
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
                              std::string(_ts_output.value_type().name()) + " to TimeSeriesValueOutput: " + e.what();
            throw nb::type_error(msg.c_str());
        }
    }

    void TimeSeriesValueOutput::mark_invalid() {
        _ts_output.invalidate();
        BaseTimeSeriesOutput::mark_invalid();
    }

    void TimeSeriesValueOutput::invalidate() {
        _ts_output.invalidate();
        BaseTimeSeriesOutput::invalidate();
    }

    // Use BaseTimeSeriesOutput's modified/valid/last_modified_time which integrate with the existing
    // notification infrastructure. The _ts_output is just for value storage.
    // Note: These are inherited from BaseTimeSeriesOutput, no need to override.

    void TimeSeriesValueOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_t = dynamic_cast<const TimeSeriesValueOutput &>(output);
        if (output_t.valid()) {
            _ts_output.set_value(output_t._ts_output.value());
            mark_modified();
        } else {
            mark_invalid();
        }
    }

    void TimeSeriesValueOutput::copy_from_input(const TimeSeriesInput &input) {
        const auto &input_t = dynamic_cast<const TimeSeriesValueInput &>(input);
        if (input_t.valid()) {
            _ts_output.set_value(input_t._ts_input.value());
            mark_modified();
        } else {
            mark_invalid();
        }
    }

    bool TimeSeriesValueOutput::is_same_type(const TimeSeriesType *other) const {
        auto *other_out = dynamic_cast<const TimeSeriesValueOutput *>(other);
        return other_out != nullptr && other_out->_ts_output.value_type() == _ts_output.value_type();
    }

    // TimeSeriesValueInput implementation

    TimeSeriesValueInput::TimeSeriesValueInput(node_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesInput(parent), _ts_input(get_context(parent), tp) {}

    TimeSeriesValueInput::TimeSeriesValueInput(time_series_input_ptr parent, const std::type_info &tp)
        : BaseTimeSeriesInput(parent), _ts_input(get_context(parent), tp) {}

    TimeSeriesValueOutput& TimeSeriesValueInput::value_output() {
        return dynamic_cast<TimeSeriesValueOutput &>(*output());
    }

    const TimeSeriesValueOutput& TimeSeriesValueInput::value_output() const {
        return dynamic_cast<const TimeSeriesValueOutput &>(*output());
    }

    nb::object TimeSeriesValueInput::py_value() const {
        if (!valid()) return nb::none();

        // If _ts_input is bound and has a value, use it
        if (_ts_input.bound()) {
            const auto& av = _ts_input.value();
            if (av.has_value()) {
                // Check if stored as nb::object first
                if (auto *obj = av.get_if<nb::object>()) return *obj;

                // Cast specific types
                if (auto *b = av.get_if<bool>()) return nb::cast(*b);
                if (auto *i = av.get_if<int64_t>()) return nb::cast(*i);
                if (auto *d = av.get_if<double>()) return nb::cast(*d);
                if (auto *dt = av.get_if<engine_date_t>()) return nb::cast(*dt);
                if (auto *t = av.get_if<engine_time_t>()) return nb::cast(*t);
                if (auto *td = av.get_if<engine_time_delta_t>()) return nb::cast(*td);

                return nb::none();
            }
        }

        // Fall back to the bound output's py_value() for non-TimeSeriesValueOutput cases
        // (e.g., when bound through a reference)
        if (bound() && output()) {
            return output()->py_value();
        }

        return nb::none();
    }

    nb::object TimeSeriesValueInput::py_delta_value() const {
        return py_value();
    }

    bool TimeSeriesValueInput::is_same_type(const TimeSeriesType *other) const {
        auto *other_in = dynamic_cast<const TimeSeriesValueInput *>(other);
        return other_in != nullptr && other_in->_ts_input.value_type() == _ts_input.value_type();
    }

    bool TimeSeriesValueInput::bind_output(const time_series_output_s_ptr& output_) {
        // First do the base binding to set up parent tracking
        bool peer = BaseTimeSeriesInput::bind_output(output_);

        // Then bind the TSInput to the TSOutput for value sharing
        // We need to bind to the actual output we're connected to (which may differ from output_
        // if we went through a reference)
        if (bound()) {
            auto* ts_value_output = dynamic_cast<TimeSeriesValueOutput*>(output().get());
            if (ts_value_output) {
                _ts_input.bind_output(ts_value_output->ts_output());
            }
        }

        return peer;
    }

    void TimeSeriesValueInput::un_bind_output(bool unbind_refs) {
        _ts_input.un_bind();
        BaseTimeSeriesInput::un_bind_output(unbind_refs);
    }

    // NOTE: modified/valid/last_modified_time/active/make_active/make_passive
    // are inherited from BaseTimeSeriesInput - no need to override.

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueOutput, TimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<TimeSeriesValueInput, TimeSeriesInput>(m, "TimeSeriesValueInput");
    }
} // namespace hgraph
