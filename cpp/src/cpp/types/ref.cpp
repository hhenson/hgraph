#include <hgraph/builders/output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tss.h>

#include <algorithm>

namespace hgraph {

    bool TimeSeriesReferenceOutput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesReferenceOutput *>(other) != nullptr;
    }

    const TimeSeriesReference::ptr &TimeSeriesReferenceOutput::value() const { return _value; }

    TimeSeriesReference::ptr &TimeSeriesReferenceOutput::value() { return _value; }

    void TimeSeriesReferenceOutput::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        auto v{nb::cast<TimeSeriesReference::ptr>(value)};
        set_value(v);
    }

    void TimeSeriesReferenceOutput::set_value(TimeSeriesReference::ptr value) {
        _value = value;
        mark_modified();
        for (auto input: _reference_observers) { _value->bind_input(*input); }
    }

    void TimeSeriesReferenceOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    bool TimeSeriesReferenceOutput::can_apply_result(nb::object value) { return !modified(); }

    void TimeSeriesReferenceOutput::observe_reference(TimeSeriesInput::ptr input_) {
        _reference_observers.emplace(input_);
    }

    void TimeSeriesReferenceOutput::stop_observing_reference(TimeSeriesInput::ptr input_) {
        _reference_observers.erase(input_);
    }

    void TimeSeriesReferenceOutput::clear() { set_value(TimeSeriesReference::make()); }

    nb::object TimeSeriesReferenceOutput::py_value() const { return has_value() ? nb::cast(_value) : nb::none(); }

    nb::object TimeSeriesReferenceOutput::py_delta_value() const { return py_value(); }

    void TimeSeriesReferenceOutput::invalidate() {
        reset_value();
        mark_invalid();
    }

    void TimeSeriesReferenceOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesReferenceOutput *>(&output);
        if (output_t) {
            set_value(output_t->_value);
        } else {
            throw std::runtime_error("TimeSeriesReferenceOutput::copy_from_output: Expected TimeSeriesReferenceOutput");
        }
    }

    void TimeSeriesReferenceOutput::copy_from_input(const TimeSeriesInput &input) {
        auto input_t = dynamic_cast<const TimeSeriesReferenceInput *>(&input);
        if (input_t) {
            set_value(input_t->value());
        } else {
            throw std::runtime_error("TimeSeriesReferenceOutput::copy_from_input: Expected TimeSeriesReferenceInput");
        }
    }

    bool TimeSeriesReferenceOutput::is_reference() const { return true; }

    bool TimeSeriesReferenceOutput::has_reference() const { return true; }

    void TimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReferenceOutput, TimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("observe_reference", &TimeSeriesReferenceOutput::observe_reference, "input"_a,
                 "Register an input as observing this reference value")
            .def("stop_observing_reference", &TimeSeriesReferenceOutput::stop_observing_reference, "input"_a,
                 "Unregister an input from observing this reference value")
                .def_prop_ro(
                    "reference_observers_count", [](const TimeSeriesReferenceOutput &self) {
                        return self._reference_observers.size();
                    },
                    "Number of inputs observing this reference value")
            .def("clear", &TimeSeriesReferenceOutput::clear)
            .def("__str__",
                 [](const TimeSeriesReferenceOutput &self) {
                     return fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(&self),
                                       self.has_value() ? self._value->to_string() : "None");
                 })
            .def("__repr__", [](const TimeSeriesReferenceOutput &self) {
                return fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(&self),
                                       self.has_value() ? self._value->to_string() : "None");
            });
    }

    bool TimeSeriesReferenceOutput::has_value() const { return _value != nullptr; }

    void TimeSeriesReferenceOutput::reset_value() { _value = nullptr; }

    void TimeSeriesReferenceInput::start() {
        set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
        notify(sample_time());
    }

    nb::object TimeSeriesReferenceInput::py_value() const {
        auto v{value()};
        if (v == nullptr) { return nb::none(); }
        return nb::cast(v);
    }

    nb::object TimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    TimeSeriesReference::ptr TimeSeriesReferenceInput::value() const {
        if (has_output()) { return output_t()->value(); }
        if (has_value()) { return _value; }
        if (_items.has_value()) {
            _value = TimeSeriesReference::make(*_items);
            return _value;
        }
        return nullptr;
    }

    bool TimeSeriesReferenceInput::bound() const { return BaseTimeSeriesInput::bound() || !_items.has_value(); }

    bool TimeSeriesReferenceInput::modified() const {
        if (sampled()) { return true; }
        if (has_output()) { return output()->modified(); }
        if (_items.has_value()) {
            return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->modified(); });
        }
        return false;
    }

    bool TimeSeriesReferenceInput::valid() const {
        return has_value() ||
               (_items.has_value() && !_items->empty() &&
                std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); })) ||
               (has_output() && BaseTimeSeriesInput::valid());
    }

    bool TimeSeriesReferenceInput::all_valid() const {
        return (_items.has_value() && !_items->empty() &&
                std::all_of(_items->begin(), _items->end(), [](const auto &item) { return item->all_valid(); })) ||
               has_value() || BaseTimeSeriesInput::all_valid();
    }

    engine_time_t TimeSeriesReferenceInput::last_modified_time() const {
            std::vector<engine_time_t> times;

        if (_items.has_value()) {
            for (const auto &item : *_items) { times.push_back(item->last_modified_time()); }
        }

        if (has_output()) { times.push_back(output()->last_modified_time()); }

        return times.empty() ? sample_time() : *std::max_element(times.begin(), times.end());
    }

    void TimeSeriesReferenceInput::clone_binding(const TimeSeriesReferenceInput::ptr &other) {
        un_bind_output(false);
        if (other->has_output()) {
            bind_output(other->output());
        } else if (other->_items.has_value()) {
            for (size_t i = 0; i < other->_items->size(); ++i) {
                this->get_ref_input(i)->clone_binding((*other->_items)[i]);
            }
        } else if (other->has_value()) {
            _value = other->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    bool TimeSeriesReferenceInput::bind_output(time_series_output_ptr output_) {
        auto peer = do_bind_output(output_);

        if (owning_node()->is_started() && has_output() && output()->valid()) {
            set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
            if (active()) { notify(sample_time()); }
        }

        return peer;
    }

    void TimeSeriesReferenceInput::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();
        do_un_bind_output(unbind_refs);

        if (has_owning_node() && owning_node()->is_started() && was_valid) {
            set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
            if (active()) {
                // Notify as the state of the node has changed from bound to unbound
                owning_node()->notify(sample_time());
            }
        }
    }

    void TimeSeriesReferenceInput::make_active() {
        if (has_output()) {
            BaseTimeSeriesInput::make_active();
        } else {
            set_active(true);
        }

        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_active(); }
        }

        if (valid()) {
            set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
            notify(last_modified_time());
        }
    }

    void TimeSeriesReferenceInput::make_passive() {
        if (has_output()) {
            BaseTimeSeriesInput::make_passive();
        } else {
            set_active(false);
        }

        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_passive(); }
        }
    }

    TimeSeriesInput *TimeSeriesReferenceInput::get_input(size_t index) { return get_ref_input(index); }

    TimeSeriesReferenceInput *TimeSeriesReferenceInput::get_ref_input(size_t index) {
        if (!_items.has_value()) { _items = std::vector<TimeSeriesReferenceInput::ptr>{}; }
        _items->reserve(index + 1);
        auto sz{_items->size()};
        while (index >= sz) {
            auto new_item = new TimeSeriesReferenceInput(this);
            if (active()) { new_item->make_active(); }
            _items->push_back(new_item);
            ++sz;
        }
        return (*_items)[index].get();
    }

    bool TimeSeriesReferenceInput::do_bind_output(time_series_output_ptr &output_) {
        if (dynamic_cast<const TimeSeriesReferenceOutput *>(output_.get()) != nullptr) {
            // Match Python behavior: bind to a TimeSeriesReferenceOutput as a normal peer
            reset_value();
            return BaseTimeSeriesInput::do_bind_output(output_);
        }
        // We are binding directly to a concrete output: wrap it as a reference value
        _value = TimeSeriesReference::make(std::move(output_));
        output().reset();
        if (owning_node()->is_started()) {
            set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
            notify(sample_time());
        } else {
            owning_node()->add_start_input(this);
        }
        return false;
    }

    void TimeSeriesReferenceInput::do_un_bind_output(bool unbind_refs) {
        if (has_output()) { BaseTimeSeriesInput::do_un_bind_output(unbind_refs); }
        if (has_value()) {
            reset_value();
            // TODO: Do we need to notify here? Should we notify only if the input is active?
            set_sample_time(
                owning_node()->is_started() ? owning_graph()->evaluation_clock()->evaluation_time() : MIN_ST);
        }
        if (_items.has_value()) {
            for (auto &item: *_items) { item->un_bind_output(unbind_refs); }
            _items.reset();
        }
    }

    TimeSeriesReferenceOutput *TimeSeriesReferenceInput::output_t() const {
        return const_cast<TimeSeriesReferenceInput *>(this)->output_t();
    }

    TimeSeriesReferenceOutput *TimeSeriesReferenceInput::output_t() {
        auto _output{output().get()};
        auto _result{dynamic_cast<TimeSeriesReferenceOutput *>(_output)};
        if (_result == nullptr) {
            throw std::runtime_error("TimeSeriesReferenceInput::output_t: Expected TimeSeriesReferenceOutput*");
    }
        return _result;
    }

    void TimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReferenceInput, TimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def("bind_output", &TimeSeriesReferenceInput::bind_output, "output"_a,
                 "Bind this reference input to an output or wrap a concrete output as a reference")
            .def("un_bind_output", &TimeSeriesReferenceInput::un_bind_output, "unbind_refs"_a = false,
                 "Unbind this reference input; optionally unbind nested references")
            .def("__getitem__",
                 [](TimeSeriesReferenceInput &self, size_t index) -> TimeSeriesInput::ptr {
                     return TimeSeriesInput::ptr{self.get_input(index)};
                 })
            .def("__str__",
                 [](const TimeSeriesReferenceInput &self) {
                     std::string value_str = "None";
                    if (self.has_value()) {
                        value_str = self._value->to_string();
                     } else if (self.has_output()) {
                         value_str = "bound";
                     } else if (self._items.has_value()) {
                         value_str = fmt::format("{} items", self._items->size());
                     }
                     return fmt::format("TimeSeriesReferenceInput@{:p}[{}]", static_cast<const void *>(&self), value_str);
                 })
            .def("__repr__", [](const TimeSeriesReferenceInput &self) {
                std::string value_str = "None";
                    if (self.has_value()) {
                        value_str = self._value->to_string();
                } else if (self.has_output()) {
                    value_str = "bound";
                } else if (self._items.has_value()) {
                    value_str = fmt::format("{} items", self._items->size());
                }
                return fmt::format("TimeSeriesReferenceInput@{:p}[{}]", static_cast<const void *>(&self), value_str);
            });
    }

    bool TimeSeriesReferenceInput::is_reference() const { return true; }

    bool TimeSeriesReferenceInput::has_reference() const { return true; }

    void TimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        reset_value();
        set_sample_time(modified_time);
        if (active()) { BaseTimeSeriesInput::notify_parent(this, modified_time); }
       }

    std::vector<TimeSeriesReferenceInput::ptr> &TimeSeriesReferenceInput::items() {
        return _items.has_value() ? *_items : empty_items;
    }

    const std::vector<TimeSeriesReferenceInput::ptr> &TimeSeriesReferenceInput::items() const {
        return _items.has_value() ? *_items : empty_items;
    }

    bool TimeSeriesReferenceInput::has_value() const { return _value != nullptr; }

    void TimeSeriesReferenceInput::reset_value() { _value = nullptr; }
}  // namespace hgraph