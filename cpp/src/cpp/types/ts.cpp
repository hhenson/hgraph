#include <hgraph/types/ts.h>
#include <hgraph/types/node.h>
#include <hgraph/util/string_utils.h>

namespace hgraph {

    // TimeSeriesValueOutput implementation

    TimeSeriesValueOutput::TimeSeriesValueOutput(node_ptr parent, const std::type_info &tp)
        : _parent_adapter{parent}, _ts_output{this, tp} {}

    TimeSeriesValueOutput::TimeSeriesValueOutput(time_series_output_ptr parent, const std::type_info &tp)
        : _parent_adapter{parent}, _ts_output{this, tp} {}

    void TimeSeriesValueOutput::notify(engine_time_t et) {
        // Outputs notify their parent (which may be a collection output or node)
        if (_parent_adapter.has_parent_output()) {
            _parent_adapter.parent_output()->mark_child_modified(*this, et);
        }
        // Note: Unlike inputs, outputs don't typically notify the node directly
        // The node is notified through the subscription mechanism
    }

    engine_time_t TimeSeriesValueOutput::current_engine_time() const { return owning_node()->current_engine_time(); }

    void TimeSeriesValueOutput::add_before_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_before_evaluation_notification(std::move(fn));
    }

    void TimeSeriesValueOutput::add_after_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_after_evaluation_notification(std::move(fn));
    }

    node_ptr TimeSeriesValueOutput::owning_node() { return _parent_adapter.owning_node(); }

    node_ptr TimeSeriesValueOutput::owning_node() const { return _parent_adapter.owning_node(); }

    graph_ptr TimeSeriesValueOutput::owning_graph() { return _parent_adapter.owning_graph(); }

    graph_ptr TimeSeriesValueOutput::owning_graph() const { return _parent_adapter.owning_graph(); }

    bool TimeSeriesValueOutput::has_parent_or_node() const { return _parent_adapter.has_parent_or_node(); }

    bool TimeSeriesValueOutput::has_owning_node() const { return _parent_adapter.has_owning_node(); }

    nb::object TimeSeriesValueOutput::py_value() const { return _ts_output.value().as_python(); }

    nb::object TimeSeriesValueOutput::py_delta_value() const {
        auto event = _ts_output.delta_value();
        return event.value.as_python();
    }

    engine_time_t TimeSeriesValueOutput::last_modified_time() const { return _ts_output.last_modified_time(); }

    bool TimeSeriesValueOutput::modified() const { return _ts_output.modified(); }

    bool TimeSeriesValueOutput::valid() const { return _ts_output.valid(); }

    bool TimeSeriesValueOutput::all_valid() const { return _ts_output.valid(); }

    void TimeSeriesValueOutput::re_parent(node_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesValueOutput::re_parent(const time_series_type_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesValueOutput::reset_parent_or_node() { _parent_adapter.reset_parent_or_node(); }

    void TimeSeriesValueOutput::builder_release_cleanup() {
        // Think about what may be required to be done here
    }

    bool TimeSeriesValueOutput::is_same_type(const TimeSeriesType *other) const {
        auto *other_out = dynamic_cast<const TimeSeriesValueOutput *>(other);
        return other_out != nullptr && other_out->_ts_output.value_type() == _ts_output.value_type();
    }

    bool TimeSeriesValueOutput::is_reference() const { return false; }

    bool TimeSeriesValueOutput::has_reference() const { return false; }

    TimeSeriesOutput::s_ptr TimeSeriesValueOutput::parent_output() const {
        auto p{_parent_adapter.parent_output()};
        return p != nullptr ? p->shared_from_this() : s_ptr{};
    }

    TimeSeriesOutput::s_ptr TimeSeriesValueOutput::parent_output() {
        auto p{_parent_adapter.parent_output()};
        return p != nullptr ? p->shared_from_this() : s_ptr{};
    }

    bool TimeSeriesValueOutput::has_parent_output() const { return _parent_adapter.has_parent_output(); }

    void TimeSeriesValueOutput::subscribe(Notifiable *node) { _ts_output.subscribe(node); }

    void TimeSeriesValueOutput::un_subscribe(Notifiable *node) { _ts_output.unsubscribe(node); }

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
    }

    bool TimeSeriesValueOutput::can_apply_result(const nb::object &value) { return !modified(); }

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

    void TimeSeriesValueOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesValueOutput *>(&output);
        if (output_t) {
            if (output_t->valid()) {
                _ts_output.set_value(output_t->_ts_output.value());
            }
        } else {
            throw std::runtime_error("TimeSeriesValueOutput::copy_from_output: Expected TimeSeriesValueOutput");
        }
    }

    void TimeSeriesValueOutput::copy_from_input(const TimeSeriesInput &input) {
        auto input_t = dynamic_cast<const TimeSeriesValueInput *>(&input);
        if (input_t) {
            if (input_t->valid()) {
                _ts_output.set_value(input_t->_ts_input.value());
            }
        } else {
            throw std::runtime_error("TimeSeriesValueOutput::copy_from_input: Expected TimeSeriesValueInput");
        }
    }

    void TimeSeriesValueOutput::clear() { _ts_output.invalidate(); }

    void TimeSeriesValueOutput::invalidate() { _ts_output.invalidate(); }

    void TimeSeriesValueOutput::mark_invalid() { _ts_output.invalidate(); }

    void TimeSeriesValueOutput::mark_modified() {
        // Trigger notification by setting the current value again
        // This will update the last_modified_time and notify subscribers
        if (valid()) {
            auto current = _ts_output.value();
            _ts_output.set_value(current);
        }
    }

    void TimeSeriesValueOutput::mark_modified(engine_time_t modified_time) {
        // The TSOutput handles modified time internally based on events
        mark_modified();
    }

    void TimeSeriesValueOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        // For a value output, we don't have children in the traditional sense
        // But we propagate the modification notification upward
        notify(modified_time);
    }

    // TimeSeriesValueInput implementation

    TimeSeriesValueInput::TimeSeriesValueInput(node_ptr parent, const std::type_info &tp)
        : _parent_adapter{parent}, _ts_input{this, tp} {}

    TimeSeriesValueInput::TimeSeriesValueInput(time_series_input_ptr parent, const std::type_info &tp)
        : _parent_adapter{parent}, _ts_input{this, tp} {}

    void TimeSeriesValueInput::notify(engine_time_t et) {
        // Notify through the parent adapter
        _parent_adapter.notify_modified(this, et);
    }

    engine_time_t TimeSeriesValueInput::current_engine_time() const { return owning_node()->current_engine_time(); }

    void TimeSeriesValueInput::add_before_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_before_evaluation_notification(std::move(fn));
    }

    void TimeSeriesValueInput::add_after_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_after_evaluation_notification(std::move(fn));
    }

    node_ptr TimeSeriesValueInput::owning_node() { return _parent_adapter.owning_node(); }

    node_ptr TimeSeriesValueInput::owning_node() const { return _parent_adapter.owning_node(); }

    graph_ptr TimeSeriesValueInput::owning_graph() { return _parent_adapter.owning_graph(); }

    graph_ptr TimeSeriesValueInput::owning_graph() const { return _parent_adapter.owning_graph(); }

    bool TimeSeriesValueInput::has_parent_or_node() const { return _parent_adapter.has_parent_or_node(); }

    bool TimeSeriesValueInput::has_owning_node() const { return _parent_adapter.has_owning_node(); }

    TimeSeriesValueOutput& TimeSeriesValueInput::value_output() {
        return dynamic_cast<TimeSeriesValueOutput &>(*output());
    }

    const TimeSeriesValueOutput& TimeSeriesValueInput::value_output() const {
        return dynamic_cast<const TimeSeriesValueOutput &>(*output());
    }

    nb::object TimeSeriesValueInput::py_value() const { return _ts_input.value().as_python(); }

    nb::object TimeSeriesValueInput::py_delta_value() const { return py_value(); }

    engine_time_t TimeSeriesValueInput::last_modified_time() const { return _ts_input.last_modified_time(); }

    bool TimeSeriesValueInput::modified() const { return _ts_input.modified(); }

    bool TimeSeriesValueInput::valid() const { return _ts_input.valid(); }

    bool TimeSeriesValueInput::all_valid() const { return _ts_input.valid(); }

    void TimeSeriesValueInput::re_parent(node_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesValueInput::re_parent(const time_series_type_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesValueInput::reset_parent_or_node() { _parent_adapter.reset_parent_or_node(); }

    void TimeSeriesValueInput::builder_release_cleanup() {
        // Think about what may be required to be done here
    }

    bool TimeSeriesValueInput::is_same_type(const TimeSeriesType *other) const {
        auto *other_in = dynamic_cast<const TimeSeriesValueInput *>(other);
        return other_in != nullptr && other_in->_ts_input.value_type() == _ts_input.value_type();
    }

    bool TimeSeriesValueInput::is_reference() const { return false; }

    bool TimeSeriesValueInput::has_reference() const { return false; }

    TimeSeriesInput::s_ptr TimeSeriesValueInput::parent_input() const {
        auto p{_parent_adapter.parent_input()};
        return p != nullptr ? p->shared_from_this() : TimeSeriesInput::s_ptr{};
    }

    bool TimeSeriesValueInput::has_parent_input() const { return _parent_adapter.has_parent_input(); }

    bool TimeSeriesValueInput::active() const { return _ts_input.active(); }

    void TimeSeriesValueInput::make_active() { _ts_input.make_active(); }

    void TimeSeriesValueInput::make_passive() { _ts_input.make_passive(); }

    bool TimeSeriesValueInput::bound() const { return _ts_input.bound(); }

    bool TimeSeriesValueInput::has_peer() const { return _ts_input.bound(); }

    time_series_output_s_ptr TimeSeriesValueInput::output() const { return _bound_output; }

    bool TimeSeriesValueInput::has_output() const { return _bound_output != nullptr; }

    time_series_reference_output_s_ptr TimeSeriesValueInput::reference_output() const {
        throw std::runtime_error("TimeSeriesValueInput does not support reference_output");
    }

    TimeSeriesInput::s_ptr TimeSeriesValueInput::get_input(size_t index) {
        throw std::runtime_error("TimeSeriesValueInput does not support get_input");
    }

    void TimeSeriesValueInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        // Since this is a leaf we should not have this method called.
        throw std::runtime_error("TimeSeriesValueInput does not support notify_parent");
    }

    bool TimeSeriesValueInput::bind_output(const time_series_output_s_ptr& output_) {
        auto* ts_value_output = dynamic_cast<TimeSeriesValueOutput*>(output_.get());
        if (ts_value_output) {
            _bound_output = output_;
            _ts_input.bind_output(ts_value_output->ts_output());
            return true;
        }
        return false;
    }

    void TimeSeriesValueInput::un_bind_output(bool unbind_refs) {
        _bound_output = nullptr;
        _ts_input.un_bind();
    }

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueOutput, TimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<TimeSeriesValueInput, TimeSeriesInput>(m, "TimeSeriesValueInput");
    }
} // namespace hgraph
