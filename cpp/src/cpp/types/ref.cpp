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
    // ============================================================
    // TimeSeriesReference Implementation
    // ============================================================

    // Private constructors
    TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {
        // _storage.empty is already initialized
    }

    TimeSeriesReference::TimeSeriesReference(time_series_output_ptr output) : _kind(Kind::BOUND) {
        new (&_storage.bound) TimeSeriesOutput::ptr(std::move(output));
    }

    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items) : _kind(Kind::UNBOUND) {
        new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(items));
    }

    // Copy constructor
    TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference &other) : _kind(other._kind) {
        copy_from(other);
    }

    // Move constructor
    TimeSeriesReference::TimeSeriesReference(TimeSeriesReference &&other) noexcept : _kind(other._kind) {
        move_from(std::move(other));
    }

    // Copy assignment
    TimeSeriesReference &TimeSeriesReference::operator=(const TimeSeriesReference &other) {
        if (this != &other) {
            destroy();
            _kind = other._kind;
            copy_from(other);
        }
        return *this;
    }

    // Move assignment
    TimeSeriesReference &TimeSeriesReference::operator=(TimeSeriesReference &&other) noexcept {
        if (this != &other) {
            destroy();
            _kind = other._kind;
            move_from(std::move(other));
        }
        return *this;
    }

    // Destructor
    TimeSeriesReference::~TimeSeriesReference() { destroy(); }

    void TimeSeriesReference::destroy() noexcept {
        switch (_kind) {
            case Kind::EMPTY:
                break;
            case Kind::BOUND:
                _storage.bound.~ref();
                break;
            case Kind::UNBOUND:
                _storage.unbound.~vector();
                break;
        }
    }

    void TimeSeriesReference::copy_from(const TimeSeriesReference &other) {
        switch (other._kind) {
            case Kind::EMPTY:
                break;
            case Kind::BOUND:
                new (&_storage.bound) TimeSeriesOutput::ptr(other._storage.bound);
                break;
            case Kind::UNBOUND:
                new (&_storage.unbound) std::vector<TimeSeriesReference>(other._storage.unbound);
                break;
        }
    }

    void TimeSeriesReference::move_from(TimeSeriesReference &&other) noexcept {
        switch (other._kind) {
            case Kind::EMPTY:
                break;
            case Kind::BOUND:
                new (&_storage.bound) TimeSeriesOutput::ptr(std::move(other._storage.bound));
                break;
            case Kind::UNBOUND:
                new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(other._storage.unbound));
                break;
        }
    }

    // Accessors with validation
    const TimeSeriesOutput::ptr &TimeSeriesReference::output() const {
        if (_kind != Kind::BOUND) {
            throw std::runtime_error("TimeSeriesReference::output() called on non-bound reference");
        }
        return _storage.bound;
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const {
        if (_kind != Kind::UNBOUND) {
            throw std::runtime_error("TimeSeriesReference::items() called on non-unbound reference");
        }
        return _storage.unbound;
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t ndx) const {
        return items()[ndx];
    }

    // Operations delegated by kind
    void TimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        switch (_kind) {
            case Kind::EMPTY:
                try {
                    ts_input.un_bind_output(false);
                } catch (const std::exception &e) {
                    throw std::runtime_error(std::string("Error in EmptyTimeSeriesReference::bind_input: ") + e.what());
                } catch (...) { throw std::runtime_error("Unknown error in EmptyTimeSeriesReference::bind_input"); }
                break;
            case Kind::BOUND: {
                bool reactivate = false;
                // Treat inputs previously bound via a reference as bound, so we unbind to generate correct deltas
                if (ts_input.bound() && !ts_input.has_peer()) {
                    reactivate = ts_input.active();
                    ts_input.un_bind_output(false);
                }
                ts_input.bind_output(_storage.bound);
                if (reactivate) { ts_input.make_active(); }
                break;
            }
            case Kind::UNBOUND: {
                bool reactivate = false;
                if (ts_input.bound() && ts_input.has_peer()) {
                    reactivate = ts_input.active();
                    ts_input.un_bind_output(false);
                }

                for (size_t i = 0; i < _storage.unbound.size(); ++i) {
                    // Get the child input (from REF, Indexed, or Signal input)
                    TimeSeriesInput *item{ts_input.get_input(i)};
                    _storage.unbound[i].bind_input(*item);
                }

                if (reactivate) { ts_input.make_active(); }
                break;
            }
        }
    }

    bool TimeSeriesReference::has_output() const {
        switch (_kind) {
            case Kind::EMPTY:
                return false;
            case Kind::BOUND:
                return true;
            case Kind::UNBOUND:
                return false;
        }
        return false;
    }

    bool TimeSeriesReference::is_valid() const {
        switch (_kind) {
            case Kind::EMPTY:
                return false;
            case Kind::BOUND:
                return _storage.bound && _storage.bound->valid();
            case Kind::UNBOUND:
                return std::any_of(_storage.unbound.begin(), _storage.unbound.end(),
                                   [](const auto &item) { return item.is_valid(); });
        }
        return false;
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const {
        if (_kind != other._kind) return false;

        switch (_kind) {
            case Kind::EMPTY:
                return true;
            case Kind::BOUND:
                return _storage.bound == other._storage.bound;
            case Kind::UNBOUND:
                return _storage.unbound == other._storage.unbound;
        }
        return false;
    }

    std::string TimeSeriesReference::to_string() const {
        switch (_kind) {
            case Kind::EMPTY:
                return "REF[<UnSet>]";
            case Kind::BOUND:
                return fmt::format("REF[{}<{}>.output@{:p}]", _storage.bound->owning_node()->signature().name,
                                   fmt::join(_storage.bound->owning_node()->node_id(), ", "),
                                   const_cast<void *>(static_cast<const void *>(_storage.bound.get())));
            case Kind::UNBOUND: {
                std::vector<std::string> string_items;
                string_items.reserve(_storage.unbound.size());
                for (const auto &item : _storage.unbound) { string_items.push_back(item.to_string()); }
                return fmt::format("REF[{}]", fmt::join(string_items, ", "));
            }
        }
        return "REF[?]";
    }

    // Factory methods
    TimeSeriesReference TimeSeriesReference::make() { return TimeSeriesReference(); }

    TimeSeriesReference TimeSeriesReference::make(time_series_output_ptr output) {
        if (output.get() == nullptr) {
            return make();
        } else {
            return TimeSeriesReference(std::move(output));
        }
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items) {
        if (items.empty()) { return make(); }
        return TimeSeriesReference(std::move(items));
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<nb::ref<TimeSeriesReferenceInput>> items) {
        if (items.empty()) { return make(); }
        std::vector<TimeSeriesReference> refs;
        refs.reserve(items.size());
        for (auto item : items) {
            // Call value() instead of accessing _value directly, so bound items return their output's value
            refs.emplace_back(item->value());
        }
        return TimeSeriesReference(std::move(refs));
    }

    void TimeSeriesReference::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
                .def("__str__", &TimeSeriesReference::to_string)
                .def("__repr__", &TimeSeriesReference::to_string)
                .def("bind_input", &TimeSeriesReference::bind_input)
                .def_prop_ro("has_output", &TimeSeriesReference::has_output)
                .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
                .def_prop_ro("is_bound", &TimeSeriesReference::is_bound)
                .def_prop_ro("is_unbound", &TimeSeriesReference::is_unbound)
                .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
                .def_prop_ro("output", &TimeSeriesReference::output)
                .def_prop_ro("items", &TimeSeriesReference::items)
                .def("__getitem__", &TimeSeriesReference::operator[])
                .def_static("make", nb::overload_cast<>(&TimeSeriesReference::make))
                .def_static("make", nb::overload_cast<time_series_output_ptr>(&TimeSeriesReference::make))
                .def_static("make", nb::overload_cast<std::vector<TimeSeriesReference>>(&TimeSeriesReference::make))
                .def_static(
                    "make",
                    [](nb::object ts, nb::object items) -> TimeSeriesReference {
                        if (!ts.is_none()) {
                            if (nb::isinstance<TimeSeriesOutput>(ts)) return make(nb::cast<TimeSeriesOutput::ptr>(ts));
                            if (nb::isinstance<TimeSeriesReferenceInput>(ts))
                                return nb::cast<TimeSeriesReferenceInput::ptr>(ts)->value();
                            if (nb::isinstance<TimeSeriesInput>(ts)) {
                                auto ts_input = nb::cast<TimeSeriesInput::ptr>(ts);
                                if (ts_input->has_peer()) return make(ts_input->output());
                                // Deal with list of inputs
                                std::vector<TimeSeriesReference> items_list;
                                auto ts_ndx{dynamic_cast<IndexedTimeSeriesInput *>(ts_input.get())};
                                items_list.reserve(ts_ndx->size());
                                for (auto &ts_ptr : ts_ndx->values()) {
                                    auto ref_input{dynamic_cast<TimeSeriesReferenceInput *>(ts_ptr.get())};
                                    items_list.emplace_back(ref_input ? ref_input->value() : TimeSeriesReference::make());
                                }
                                return make(items_list);
                            }
                            // We may wish to raise an exception here?
                        } else if (!items.is_none()) {
                            auto items_list = nb::cast<std::vector<TimeSeriesReference>>(items);
                            return make(items_list);
                        }
                        return make();
                    },
                    "ts"_a = nb::none(), "from_items"_a = nb::none());
    }

    // ============================================================
    // TimeSeriesReferenceOutput Implementation
    // ============================================================

    bool TimeSeriesReferenceOutput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesReferenceOutput *>(other) != nullptr;
    }

    const TimeSeriesReference &TimeSeriesReferenceOutput::value() const {
        if (!_value.has_value()) {
            throw std::runtime_error("TimeSeriesReferenceOutput::value() called when no value present");
        }
        return *_value;
    }

    TimeSeriesReference &TimeSeriesReferenceOutput::value() {
        if (!_value.has_value()) {
            throw std::runtime_error("TimeSeriesReferenceOutput::value() called when no value present");
        }
        return *_value;
    }

    TimeSeriesReference TimeSeriesReferenceOutput::py_value_or_empty() const {
        return _value.has_value() ? *_value : TimeSeriesReference::make();
    }

    void TimeSeriesReferenceOutput::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        auto v{nb::cast<TimeSeriesReference>(value)};
        set_value(std::move(v));
    }

    void TimeSeriesReferenceOutput::set_value(TimeSeriesReference value) {
        _value = std::move(value);
        mark_modified();
        for (auto input : _reference_observers) { _value->bind_input(*input); }
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

    nb::object TimeSeriesReferenceOutput::py_value() const { return has_value() ? nb::cast(*_value) : nb::none(); }

    nb::object TimeSeriesReferenceOutput::py_delta_value() const { return py_value(); }

    void TimeSeriesReferenceOutput::invalidate() {
        reset_value();
        mark_invalid();
    }

    void TimeSeriesReferenceOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesReferenceOutput *>(&output);
        if (output_t) {
            if (output_t->_value.has_value()) {
                set_value(*output_t->_value);
            }
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
        nb::class_<TimeSeriesReferenceOutput, BaseTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
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
                .def("__str__", [](const TimeSeriesReferenceOutput &self) {
                    return fmt::format("TimeSeriesReferenceOutput@{:p}[{}]",
                                       static_cast<const void *>(&self),
                                       self.has_value() ? self._value->to_string() : "None");
                })
                .def("__repr__", [](const TimeSeriesReferenceOutput &self) {
                    return fmt::format("TimeSeriesReferenceOutput@{:p}[{}]",
                                       static_cast<const void *>(&self),
                                       self.has_value() ? self._value->to_string() : "None");
                });
    }

    bool TimeSeriesReferenceOutput::has_value() const { return _value.has_value(); }

    void TimeSeriesReferenceOutput::reset_value() { _value.reset(); }

    // ============================================================
    // TimeSeriesReferenceInput Implementation
    // ============================================================

    void TimeSeriesReferenceInput::start() {
        set_sample_time(owning_graph()->evaluation_clock()->evaluation_time());
        notify(sample_time());
    }

    nb::object TimeSeriesReferenceInput::py_value() const {
        auto v{value()};
        return nb::cast(v);
    }

    nb::object TimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    TimeSeriesReference TimeSeriesReferenceInput::value() const {
        if (has_output()) { return output_t()->py_value_or_empty(); }
        if (has_value()) { return *_value; }
        if (_items.has_value()) {
            _value = TimeSeriesReference::make(*_items);
            return *_value;
        }
        return TimeSeriesReference::make();
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
            for (auto &item : *_items) { item->un_bind_output(unbind_refs); }
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
        nb::class_<TimeSeriesReferenceInput, BaseTimeSeriesInput>(m, "TimeSeriesReferenceInput")
                .def("bind_output", &TimeSeriesReferenceInput::bind_output, "output"_a,
                     "Bind this reference input to an output or wrap a concrete output as a reference")
                .def("un_bind_output", &TimeSeriesReferenceInput::un_bind_output, "unbind_refs"_a = false,
                     "Unbind this reference input; optionally unbind nested references")
                .def("__getitem__", [](TimeSeriesReferenceInput &self, size_t index) -> TimeSeriesInput::ptr {
                    return TimeSeriesInput::ptr{self.get_input(index)};
                })
                .def("__str__", [](const TimeSeriesReferenceInput &self) {
                    std::string value_str = "None";
                    if (self.has_value()) {
                        value_str = self._value->to_string();
                    } else if (self.has_output()) {
                        value_str = "bound";
                    } else if (self._items.has_value()) {
                        value_str = fmt::format("{} items", self._items->size());
                    }
                    return fmt::format("TimeSeriesReferenceInput@{:p}[{}]",
                                       static_cast<const void *>(&self), value_str);
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
                    return fmt::format("TimeSeriesReferenceInput@{:p}[{}]",
                                       static_cast<const void *>(&self), value_str);
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

    bool TimeSeriesReferenceInput::has_value() const { return _value.has_value(); }

    void TimeSeriesReferenceInput::reset_value() { _value.reset(); }

    // ============================================================
    // Specialized Reference Input Implementations
    // ============================================================

    // TimeSeriesValueReferenceInput - REF[TS[...]]
    void TimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueReferenceInput, TimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // TimeSeriesListReferenceInput - REF[TSL[...]]
    TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(Node *owning_node, size_t size)
        : TimeSeriesReferenceInput(owning_node), _size(size) {}

    TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(TimeSeriesType *parent_input, size_t size)
        : TimeSeriesReferenceInput(parent_input), _size(size) {}

    TimeSeriesInput *TimeSeriesListReferenceInput::get_input(size_t index) {
        // Delegate to parent for now - batch creation can be added later
        return TimeSeriesReferenceInput::get_input(index);
    }

    void TimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListReferenceInput, TimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput")
                .def(nb::init<Node *>(), "owning_node"_a)
                .def(nb::init<Node *, size_t>(), "owning_node"_a, "size"_a);
    }

    // TimeSeriesBundleReferenceInput - REF[TSB[...]]
    TimeSeriesBundleReferenceInput::TimeSeriesBundleReferenceInput(Node *owning_node, size_t size)
        : TimeSeriesReferenceInput(owning_node), _size(size) {}

    TimeSeriesBundleReferenceInput::TimeSeriesBundleReferenceInput(TimeSeriesType *parent_input, size_t size)
        : TimeSeriesReferenceInput(parent_input), _size(size) {}

    void TimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleReferenceInput, TimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput")
                .def(nb::init<Node *>(), "owning_node"_a)
                .def(nb::init<Node *, size_t>(), "owning_node"_a, "size"_a);
    }

    // TimeSeriesDictReferenceInput - REF[TSD[...]]
    void TimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictReferenceInput, TimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // TimeSeriesSetReferenceInput - REF[TSS[...]]
    void TimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetReferenceInput, TimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // TimeSeriesWindowReferenceInput - REF[TSW[...]]
    void TimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowReferenceInput, TimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // ============================================================
    // Specialized Reference Output Implementations
    // ============================================================

    // TimeSeriesValueReferenceOutput - REF[TS[...]]
    void TimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueReferenceOutput, TimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // TimeSeriesListReferenceOutput - REF[TSL[...]]
    TimeSeriesListReferenceOutput::TimeSeriesListReferenceOutput(Node *owning_node, size_t size)
        : TimeSeriesReferenceOutput(owning_node), _size(size) {}

    TimeSeriesListReferenceOutput::TimeSeriesListReferenceOutput(TimeSeriesType *parent_output, size_t size)
        : TimeSeriesReferenceOutput(parent_output), _size(size) {}

    void TimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesListReferenceOutput, TimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput")
                .def(nb::init<Node *>(), "owning_node"_a)
                .def(nb::init<Node *, size_t>(), "owning_node"_a, "size"_a);
    }

    // TimeSeriesBundleReferenceOutput - REF[TSB[...]]
    TimeSeriesBundleReferenceOutput::TimeSeriesBundleReferenceOutput(Node *owning_node, size_t size)
        : TimeSeriesReferenceOutput(owning_node), _size(size) {}

    TimeSeriesBundleReferenceOutput::TimeSeriesBundleReferenceOutput(TimeSeriesType *parent_output, size_t size)
        : TimeSeriesReferenceOutput(parent_output), _size(size) {}

    void TimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesBundleReferenceOutput, TimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput")
                .def(nb::init<Node *>(), "owning_node"_a)
                .def(nb::init<Node *, size_t>(), "owning_node"_a, "size"_a);
    }

    // TimeSeriesDictReferenceOutput - REF[TSD[...]]
    void TimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictReferenceOutput, TimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // TimeSeriesSetReferenceOutput - REF[TSS[...]]
    void TimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSetReferenceOutput, TimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

    // TimeSeriesWindowReferenceOutput - REF[TSW[...]]
    void TimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesWindowReferenceOutput, TimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput")
                .def(nb::init<Node *>(), "owning_node"_a);
    }

} // namespace hgraph
