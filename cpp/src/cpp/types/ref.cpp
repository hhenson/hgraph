#include "hgraph/types/v2/ts_value_helpers.h"

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

namespace hgraph
{

    // ORIGINAL: Inherited from BaseTimeSeriesOutput, initialized _value member
    // NEW: Initialize TSOutput with ref_value_tp type
    TimeSeriesReferenceOutput::TimeSeriesReferenceOutput(const node_ptr &parent)
        : _ts(static_cast<NotifiableContext *>(const_cast<Node *>(parent.get())), typeid(ref_value_tp)) {}

    TimeSeriesReferenceOutput::TimeSeriesReferenceOutput(const TimeSeriesType::ptr &parent)
        : _ts(static_cast<NotifiableContext *>(const_cast<TimeSeriesType *>(parent.get())), typeid(ref_value_tp)) {}

    // TimeSeriesType interface - delegate to _ts or implement directly
    node_ptr TimeSeriesReferenceOutput::owning_node() {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return node_ptr{node};
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->owning_node(); }
        return nullptr;
    }

    node_ptr TimeSeriesReferenceOutput::owning_node() const { return const_cast<TimeSeriesReferenceOutput *>(this)->owning_node(); }

    graph_ptr TimeSeriesReferenceOutput::owning_graph() {
        auto node{owning_node()};
        if (node != nullptr) return node->graph();
        return nullptr;
    }

    graph_ptr TimeSeriesReferenceOutput::owning_graph() const {
        return const_cast<TimeSeriesReferenceOutput *>(this)->owning_graph();
    }

    void TimeSeriesReferenceOutput::re_parent(const node_ptr &node) {
        _ts.set_parent(static_cast<NotifiableContext *>(const_cast<Node *>(node.get())));
    }

    void TimeSeriesReferenceOutput::re_parent(const TimeSeriesType::ptr &ts) {
        _ts.set_parent(static_cast<NotifiableContext *>(const_cast<TimeSeriesType *>(ts.get())));
    }

    bool TimeSeriesReferenceOutput::has_owning_node() const {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return true;
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->has_owning_node(); }
        return false;
    }

    bool TimeSeriesReferenceOutput::is_reference() const { return true; }

    bool TimeSeriesReferenceOutput::has_reference() const { return true; }

    void TimeSeriesReferenceOutput::reset_parent_or_node() { _ts.set_parent(nullptr); }

    bool TimeSeriesReferenceOutput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesReferenceOutput *>(other) != nullptr;
    }

    ref_value_tp TimeSeriesReferenceOutput::value() const { return const_cast<TimeSeriesReferenceOutput *>(this)->value(); }

    ref_value_tp TimeSeriesReferenceOutput::value() {
        if (!valid()) { return TimeSeriesReference::make(); }
        // If we are valid, then we must have a ref_value_tp instance.
        return *_ts.value().template get_if<ref_value_tp>();
    }

    void TimeSeriesReferenceOutput::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        auto v{nb::cast<ref_value_tp>(value)};
        set_value(v);
    }

    // ORIGINAL: set_value() assigned to _value then called mark_modified()
    // NEW: Store to _ts.set_value() which handles modification internally
    // REF-SPECIFIC: Still notify reference observers after setting
    void TimeSeriesReferenceOutput::set_value(ref_value_tp value) {
        AnyValue<> any;
        any.emplace<ref_value_tp>(std::move(value));
        _ts.set_value(any);
    }

    void TimeSeriesReferenceOutput::apply_result(nb::object value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    bool TimeSeriesReferenceOutput::can_apply_result(nb::object value) { return !_ts.modified(); }

    void TimeSeriesReferenceOutput::observe_reference(TimeSeriesInput::ptr input_) { _ts.subscribe(input_.get()); }

    void TimeSeriesReferenceOutput::stop_observing_reference(TimeSeriesInput::ptr input_) { _ts.unsubscribe(input_.get()); }

    void TimeSeriesReferenceOutput::clear() { _ts.reset(); }

    // ORIGINAL: py_value() checked has_value() and returned _value
    // NEW: Extract from _ts.value() AnyValue
    nb::object TimeSeriesReferenceOutput::py_value() const {
        if (valid()) { return nb::cast(value()); }
        return nb::none();
    }

    nb::object TimeSeriesReferenceOutput::py_delta_value() const { return py_value(); }

    void TimeSeriesReferenceOutput::invalidate() { _ts.invalidate(); }

    // State and operation methods - delegate to _ts
    void TimeSeriesReferenceOutput::mark_invalid() { _ts.invalidate(); }

    void TimeSeriesReferenceOutput::mark_modified() {
        // This is a hack to deal with the possibility that we get called
        // From some external source.
        if (!_ts.modified()) {
            auto v{_ts.value()};
            _ts.set_value(v);
        }
    }

    void TimeSeriesReferenceOutput::mark_child_modified(TimeSeriesOutput &, engine_time_t) {
        throw std::runtime_error("mark_child_modified() unavailable: no children");
    }

    bool TimeSeriesReferenceOutput::modified() const { return _ts.modified(); }

    bool TimeSeriesReferenceOutput::valid() const { return _ts.valid(); }

    bool TimeSeriesReferenceOutput::all_valid() const { return _ts.valid(); }

    engine_time_t TimeSeriesReferenceOutput::last_modified_time() const { return _ts.last_modified_time(); }

    TimeSeriesOutput::ptr TimeSeriesReferenceOutput::parent_output() const {
        return const_cast<TimeSeriesReferenceOutput *>(this)->parent_output();
    }

    TimeSeriesOutput::ptr TimeSeriesReferenceOutput::parent_output() { return dynamic_cast<TimeSeriesOutput *>(_ts.parent()); }

    bool TimeSeriesReferenceOutput::has_parent_output() const { return dynamic_cast<TimeSeriesOutput *>(_ts.parent()) != nullptr; }

    void TimeSeriesReferenceOutput::subscribe(Notifiable *node) { _ts.subscribe(node); }

    void TimeSeriesReferenceOutput::un_subscribe(Notifiable *node) { _ts.unsubscribe(node); }

    void TimeSeriesReferenceOutput::builder_release_cleanup() {}

    void TimeSeriesReferenceOutput::mark_modified(engine_time_t) {
        // This variation was created to improve performance if we already have engine time,
        // For now we will just delegate to the default.
        mark_modified();
    }

    void TimeSeriesReferenceOutput::notify(engine_time_t) {
        // Since we have no children, we will not be marked as modified
        throw std::runtime_error("notify() unavailable: TSOutput handles notification");
    }

    TSOutput &TimeSeriesReferenceOutput::ts() { return _ts; }

    const TSOutput &TimeSeriesReferenceOutput::ts() const { return _ts; }

    void TimeSeriesReferenceOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesReferenceOutput *>(&output);
        if (output_t) {
            set_value(output_t->value());
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

    void TimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReferenceOutput, TimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("observe_reference", &TimeSeriesReferenceOutput::observe_reference, "input"_a,
                 "Register an input as observing this reference value")
            .def("stop_observing_reference", &TimeSeriesReferenceOutput::stop_observing_reference, "input"_a,
                 "Unregister an input from observing this reference value")
            .def("clear", &TimeSeriesReferenceOutput::clear)
            .def("__str__",
                 [](const TimeSeriesReferenceOutput &self) {
                     return fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(&self),
                                        self._ts.valid() ? self.value()->to_string() : "None");
                 })
            .def("__repr__", [](const TimeSeriesReferenceOutput &self) {
                return fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(&self),
                                   self._ts.valid() ? self.value()->to_string() : "None");
            });
    }

    // ========== TimeSeriesReferenceInput Implementation ==========

    // ORIGINAL: Inherited constructors from BaseTimeSeriesInput
    // NEW: Initialize TSInput with ref_value_tp type
    TimeSeriesReferenceInput::TimeSeriesReferenceInput(const node_ptr &parent)
        : _ts(static_cast<NotifiableContext *>(const_cast<Node *>(parent.get())), typeid(ref_value_tp)) {}

    TimeSeriesReferenceInput::TimeSeriesReferenceInput(const TimeSeriesType::ptr &parent)
        : _ts(static_cast<NotifiableContext *>(const_cast<TimeSeriesType *>(parent.get())), typeid(ref_value_tp)) {}

    // TimeSeriesType interface - delegate to _ts or implement directly
    node_ptr TimeSeriesReferenceInput::owning_node() {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return node_ptr{node};
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->owning_node(); }
        return nullptr;
    }

    node_ptr TimeSeriesReferenceInput::owning_node() const { return const_cast<TimeSeriesReferenceInput *>(this)->owning_node(); }

    graph_ptr TimeSeriesReferenceInput::owning_graph() {
        auto node{owning_node()};
        if (node != nullptr) return node->graph();
        return nullptr;
    }

    graph_ptr TimeSeriesReferenceInput::owning_graph() const {
        return const_cast<TimeSeriesReferenceInput *>(this)->owning_graph();
    }

    void TimeSeriesReferenceInput::re_parent(const node_ptr &node) {
        _ts.set_parent(static_cast<NotifiableContext *>(const_cast<Node *>(node.get())));
    }

    void TimeSeriesReferenceInput::re_parent(const TimeSeriesType::ptr &ts) {
        _ts.set_parent(static_cast<NotifiableContext *>(const_cast<TimeSeriesType *>(ts.get())));
    }

    bool TimeSeriesReferenceInput::has_owning_node() const {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return true;
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->has_owning_node(); }
        return false;
    }

    bool TimeSeriesReferenceInput::is_reference() const { return true; }

    bool TimeSeriesReferenceInput::has_reference() const { return true; }

    void TimeSeriesReferenceInput::reset_parent_or_node() { _ts.set_parent(nullptr); }

    bool TimeSeriesReferenceInput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesReferenceInput *>(other) != nullptr;
    }

    void TimeSeriesReferenceInput::start() {
        // The current logic is designed to for an evaluation of this input and
        // to ensure that it marked as "modified" so that if there is an existing
        // reference, it will propagate.
        // In the world we are working with, we should use a specialised impl for this
        _ts.mark_sampled();
    }

    nb::object TimeSeriesReferenceInput::py_value() const {
        auto v{value()};
        if (v == nullptr) { return nb::none(); }
        return nb::cast(v);
    }

    nb::object TimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    // If we are
    ref_value_tp TimeSeriesReferenceInput::value() const {
        // When bound to a REF output, extract from _ts
        if (_ts.valid()) {
            return get_from_any<ref_value_tp>(_ts.value());
        }

        // If there was no value, then we may need to build it.
        if (_items.has_value() && !_items->empty()) {
            const_cast<TSInput &>(_ts).set_value(make_any_value(TimeSeriesReference::make(*_items)));
            return get_from_any<ref_value_tp>(_ts.value());
        }

        // The current logic is to return a nullptr if there is no value.
        return nullptr;
    }

    // OK, so either we are bound directly, or we have _items that imply bound.
    bool TimeSeriesReferenceInput::bound() const { return _ts.bound() || !_items->empty(); }

    bool TimeSeriesReferenceInput::modified() const {
        if (_items.has_value()) {
            return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->modified(); });
        }
        // If there are no items, we default to standard model.
        return _ts.modified();
    }

    // OK, following the model of if there are items then use those, otherwise default to std model.
    bool TimeSeriesReferenceInput::valid() const {
        // This is a bit strange, since if we have _items, surely we are valid?
        // If not, if we construct the UnBoundReference and cache it, we will deem ourselves valid,
        // without this check.
        // To deal with this, I have attempted to invert the logic a bit
        if (_items.has_value() && !_items->empty()) {
            return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); });
        }
        return _ts.valid();
    }

    bool TimeSeriesReferenceInput::all_valid() const {
        if (_items.has_value() && !_items->empty()) {
            return std::all_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); });
        }
        return _ts.valid();
    }

    // Keeping _items logic in place, adjusted to look neater
    engine_time_t TimeSeriesReferenceInput::last_modified_time() const {
        auto tm{_ts.last_modified_time()};
        if (_items.has_value() && !_items->empty()) {
            std::vector<engine_time_t> times;
            for (const auto &item : *_items) { times.push_back(item->last_modified_time()); }
            times.push_back(tm);
            return *std::max_element(times.begin(), times.end());
        }
        return tm;
    }

    void TimeSeriesReferenceInput::clone_binding(const TimeSeriesReferenceInput::ptr &other) {
        un_bind_output(false);
        // There are two possible scenarios, either the other is bound or it is non-bound.
        // If it is non-bound, that implies it either has lists or is empty.
        if (other->bound()) {
            _ts.copy_from_input(const_cast<TSInput &>(other->ts()));
        } else if (other->_items.has_value()) {
            // In this case we are running unbound
            _ts.un_bind();  // Just in case we were bound
            // This will is even better than marking sampled as it treat this as being
            // set with a new value.
            _ts.set_value(make_any_value(TimeSeriesReference::make(*_items)));
        }
    }

    // There are two key scenarios:
    // 1. We get a TimeSeriesReferenceOutput to bind to.
    // 2. We get a normal (non-reference output to bind to)
    // In case 1. we are good and off we go.
    // In case 2. we need to convert the output to a reference and tick it out as value.
    bool TimeSeriesReferenceInput::bind_output(time_series_output_ptr output_) {
        if (auto out{dynamic_cast<TimeSeriesReferenceOutput *>(output_.get())}; out != nullptr) {
            _ts.bind_output(out->ts());
            return true;
        }

        // Now handle the non-peered case
        if (_ts.bound()) {
            _ts.un_bind();  // This should leave us with a non-bound wrapper
        }
        _ts.set_value(make_any_value(TimeSeriesReference::make(std::move(output_))));
        if (!owning_node()->is_started()) {
            // Since we can't schedule yet (we are in the middle of wiring the graph)
            // And since we are not an output, we use this hack to ensure we get scheduled
            // on the first tick.
            owning_node()->add_start_input(this);
        }
        return false;
    }

    void TimeSeriesReferenceInput::un_bind_output(bool unbind_refs) {
        _ts.un_bind();
        if (_items.has_value() && !_items->empty()) {
            for (auto &item : *_items) { item->un_bind_output(unbind_refs); }
            _items.reset();
        }
    }

    void TimeSeriesReferenceInput::make_active() {
        if (_items.has_value() && !_items->empty()) {
            for (auto &item : *_items) { item->make_active(); }
        }
        _ts.make_active();

        // I guess this makes sure the last value is processed, but on an input making it active
        // should not do this. Perhaps there is a good use-case, once it is all working again,
        // this needs to be reviewed.
        if (valid()) { _ts.mark_sampled(); }
    }

    void TimeSeriesReferenceInput::make_passive() {
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_passive(); }
        }
        _ts.make_passive();
    }

    TimeSeriesInput *TimeSeriesReferenceInput::get_input(size_t index) { return get_ref_input(index); }

    const TimeSeriesInput *TimeSeriesReferenceInput::get_input(size_t index) const {
        if (!_items.has_value() || index >= _items->size()) { return nullptr; }
        return (*_items)[index].get();
    }

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

    // Additional interface methods
    bool TimeSeriesReferenceInput::has_peer() const { return _ts.bound(); }

    // We should not be calling this anymore, if we do we can figure it out.
    time_series_output_ptr TimeSeriesReferenceInput::output() const {
        throw std::runtime_error(
            "TimeSeriesReferenceInput::output() - not available in new architecture, use output_t() if bound to REF output");
    }

    TimeSeriesInput::ptr TimeSeriesReferenceInput::parent_input() const { return dynamic_cast<TimeSeriesInput *>(_ts.parent()); }

    bool TimeSeriesReferenceInput::has_parent_input() const { return dynamic_cast<TimeSeriesInput *>(_ts.parent()) != nullptr; }

    time_series_reference_output_ptr TimeSeriesReferenceInput::reference_output() const {
        // This method appears to be for special handling - return null for now
        return {};
    }

    void TimeSeriesReferenceInput::builder_release_cleanup() { _ts.un_bind(); }

    void TimeSeriesReferenceInput::notify(engine_time_t et) {
        // Forward notification to the internal TSInput which handles the actual notification logic
        // This can happen when the TimeSeriesReferenceInput is used in contexts where it's treated
        // as a regular TimeSeriesInput (e.g., in collections or as a parent input)
        _ts.notify(et);
    }

    bool TimeSeriesReferenceInput::active() const { return _ts.active(); }

    bool TimeSeriesReferenceInput::has_output() const { return _ts.bound(); }

    TSInput &TimeSeriesReferenceInput::ts() { return _ts; }

    const TSInput &TimeSeriesReferenceInput::ts() const { return _ts; }

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
                     if (self._ts.valid()) {
                         value_str = self.value()->to_string();
                     } else if (self.has_output()) {
                         value_str = "bound";
                     } else if (self._items.has_value()) {
                         value_str = fmt::format("{} items", self._items->size());
                     }
                     return fmt::format("TimeSeriesReferenceInput@{:p}[{}]", static_cast<const void *>(&self), value_str);
                 })
            .def("__repr__", [](const TimeSeriesReferenceInput &self) {
                std::string value_str = "None";
                if (self._ts.valid()) {
                    value_str = self.value()->to_string();
                } else if (self.has_output()) {
                    value_str = "bound";
                } else if (self._items.has_value()) {
                    value_str = fmt::format("{} items", self._items->size());
                }
                return fmt::format("TimeSeriesReferenceInput@{:p}[{}]", static_cast<const void *>(&self), value_str);
            });
    }

    void TimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
       if (_items.has_value() && !_items->empty()) {
           // I'm assuming the items have their reference changes, so this will reset that state
           // Since it is a normal setting of value, we should not need to do anything else.
           _ts.set_value(make_any_value(TimeSeriesReference::make(*_items)));
       }
    }

}  // namespace hgraph