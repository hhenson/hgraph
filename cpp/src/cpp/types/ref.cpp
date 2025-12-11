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
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/types/v2/ts_value_helpers.h>

#include <algorithm>

namespace hgraph
{
    // ============================================================
    // TimeSeriesReference Implementation
    // ============================================================

    // Private constructors
    TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {
        // _storage.empty is already initialized
    }

    TimeSeriesReference::TimeSeriesReference(time_series_output_s_ptr output) : _kind(Kind::BOUND) {
        new (&_storage.bound) time_series_output_s_ptr(std::move(output));
    }

    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items) : _kind(Kind::UNBOUND) {
        new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(items));
    }

    // Copy constructor
    TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference &other) : _kind(other._kind) { copy_from(other); }

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
            case Kind::EMPTY: break;
            case Kind::BOUND: _storage.bound.~shared_ptr(); break;  // Call shared_ptr destructor
            case Kind::UNBOUND: _storage.unbound.~vector(); break;
        }
    }

    void TimeSeriesReference::copy_from(const TimeSeriesReference &other) {
        switch (other._kind) {
            case Kind::EMPTY: break;
            case Kind::BOUND: new (&_storage.bound) time_series_output_s_ptr(other._storage.bound); break;
            case Kind::UNBOUND: new (&_storage.unbound) std::vector<TimeSeriesReference>(other._storage.unbound); break;
        }
    }

    void TimeSeriesReference::move_from(TimeSeriesReference &&other) noexcept {
        switch (other._kind) {
            case Kind::EMPTY: break;
            case Kind::BOUND: new (&_storage.bound) time_series_output_s_ptr(std::move(other._storage.bound)); break;
            case Kind::UNBOUND: new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(other._storage.unbound)); break;
        }
    }

    // Accessors with validation
    const time_series_output_s_ptr &TimeSeriesReference::output() const {
        if (_kind != Kind::BOUND) { throw std::runtime_error("TimeSeriesReference::output() called on non-bound reference"); }
        return _storage.bound;
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const {
        if (_kind != Kind::UNBOUND) { throw std::runtime_error("TimeSeriesReference::items() called on non-unbound reference"); }
        return _storage.unbound;
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t ndx) const { return items()[ndx]; }

    // Operations delegated by kind
    void TimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        switch (_kind) {
            case Kind::EMPTY: try { ts_input.un_bind_output(false);
                } catch (const std::exception &e) {
                    throw std::runtime_error(std::string("Error in EmptyTimeSeriesReference::bind_input: ") + e.what());
                } catch (...) { throw std::runtime_error("Unknown error in EmptyTimeSeriesReference::bind_input"); }
                break;
            case Kind::BOUND:
                {
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
            case Kind::UNBOUND:
                {
                    bool reactivate = false;
                    if (ts_input.bound() && ts_input.has_peer()) {
                        reactivate = ts_input.active();
                        ts_input.un_bind_output(false);
                    }

                    for (size_t i = 0; i < _storage.unbound.size(); ++i) {
                        // Get the child input (from REF, Indexed, or Signal input)
                        auto item = ts_input.get_input(i);
                        _storage.unbound[i].bind_input(*item);
                    }

                    if (reactivate) { ts_input.make_active(); }
                    break;
                }
        }
    }

    bool TimeSeriesReference::has_output() const {
        switch (_kind) {
            case Kind::EMPTY: return false;
            case Kind::BOUND: return true;
            case Kind::UNBOUND: return false;
        }
        return false;
    }

    bool TimeSeriesReference::is_valid() const {
        switch (_kind) {
            case Kind::EMPTY: return false;
            case Kind::BOUND: return _storage.bound && _storage.bound->valid();
            case Kind::UNBOUND:
                return std::any_of(_storage.unbound.begin(), _storage.unbound.end(),
                                   [](const auto &item) { return item.is_valid(); });
        }
        return false;
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const {
        if (_kind != other._kind) return false;

        switch (_kind) {
            case Kind::EMPTY: return true;
            case Kind::BOUND: return _storage.bound == other._storage.bound;
            case Kind::UNBOUND: return _storage.unbound == other._storage.unbound;
        }
        return false;
    }

    std::string TimeSeriesReference::to_string() const {
        switch (_kind) {
            case Kind::EMPTY: return "REF[<UnSet>]";
            case Kind::BOUND:
                return fmt::format("REF[{}<{}>.output@{:p}]", _storage.bound->owning_node()->signature().name,
                                   fmt::join(_storage.bound->owning_node()->node_id(), ", "),
                                   const_cast<void *>(static_cast<const void *>(_storage.bound.get())));
            case Kind::UNBOUND:
                {
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

    TimeSeriesReference TimeSeriesReference::make(time_series_output_s_ptr output) {
        if (output == nullptr) {
            return make();
        } else {
            return TimeSeriesReference(std::move(output));
        }
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items) {
        if (items.empty()) { return make(); }
        return TimeSeriesReference(std::move(items));
    }


    TimeSeriesReference TimeSeriesReference::make(const std::vector<time_series_input_s_ptr>& items) {
        if (items.empty()) { return make(); }
        std::vector<TimeSeriesReference> refs;
        refs.reserve(items.size());
        for (const auto& item : items) {
            // Try to cast to TimeSeriesReferenceInput to get value() - this handles
            // both BaseTimeSeriesReferenceInput and TimeSeriesValueReferenceInput
            auto ref_item = dynamic_cast<TimeSeriesReferenceInput*>(item.get());
            if (ref_item) {
                refs.emplace_back(ref_item->value());
            } else if (item->has_peer()) {
                // Child has a peer output - create bound reference to it
                refs.emplace_back(TimeSeriesReference::make(item->output()));
            } else if (item->has_output()) {
                refs.emplace_back(TimeSeriesReference::make(item->output()));
            } else {
                refs.emplace_back(TimeSeriesReference::make());
            }
        }
        return TimeSeriesReference(std::move(refs));
    }

    // ============================================================
    // TimeSeriesReferenceOutput Implementation
    // ============================================================

    bool TimeSeriesReferenceOutput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesReferenceOutput *>(other) != nullptr;
    }

    const TimeSeriesReference &TimeSeriesReferenceOutput::value() const {
        if (!_value.has_value()) { throw std::runtime_error("TimeSeriesReferenceOutput::value() called when no value present"); }
        return *_value;
    }

    TimeSeriesReference &TimeSeriesReferenceOutput::value() {
        if (!_value.has_value()) { throw std::runtime_error("TimeSeriesReferenceOutput::value() called when no value present"); }
        return *_value;
    }

    TimeSeriesReference TimeSeriesReferenceOutput::py_value_or_empty() const {
        return _value.has_value() ? *_value : TimeSeriesReference::make();
    }

    void TimeSeriesReferenceOutput::py_set_value(const nb::object& value) {
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

    void TimeSeriesReferenceOutput::apply_result(const nb::object& value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    bool TimeSeriesReferenceOutput::can_apply_result(const nb::object& value) { return !modified(); }

    void TimeSeriesReferenceOutput::observe_reference(TimeSeriesInput::ptr input_) { _reference_observers.emplace(input_); }

    void TimeSeriesReferenceOutput::stop_observing_reference(TimeSeriesInput::ptr input_) { _reference_observers.erase(input_); }

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
            if (output_t->_value.has_value()) { set_value(*output_t->_value); }
        } else {
            throw std::runtime_error("TimeSeriesReferenceOutput::copy_from_output: Expected TimeSeriesReferenceOutput");
        }
    }

    void TimeSeriesReferenceOutput::copy_from_input(const TimeSeriesInput &input) {
        auto input_t = dynamic_cast<const BaseTimeSeriesReferenceInput *>(&input);
        if (input_t) {
            set_value(input_t->value());
        } else {
            throw std::runtime_error("TimeSeriesReferenceOutput::copy_from_input: Expected TimeSeriesReferenceInput");
        }
    }

    bool TimeSeriesReferenceOutput::is_reference() const { return true; }

    bool TimeSeriesReferenceOutput::has_reference() const { return true; }

    bool TimeSeriesReferenceOutput::has_value() const { return _value.has_value(); }

    void TimeSeriesReferenceOutput::reset_value() { _value.reset(); }

    // ============================================================
    // TimeSeriesReferenceInput Implementation
    // ============================================================


    void BaseTimeSeriesReferenceInput::start() {
        set_sample_time(owning_graph()->evaluation_time());
        notify(sample_time());
    }

    nb::object BaseTimeSeriesReferenceInput::py_value() const {
        return nb::cast(value());
    }

    nb::object BaseTimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    TimeSeriesReference BaseTimeSeriesReferenceInput::value() const {
        if (has_output()) { return output_t()->py_value_or_empty(); }
        if (has_value()) { return *_value; }
        return TimeSeriesReference::make();
    }

    bool BaseTimeSeriesReferenceInput::bound() const { return BaseTimeSeriesInput::bound(); }

    bool BaseTimeSeriesReferenceInput::modified() const {
        if (sampled()) { return true; }
        if (has_output()) { return output()->modified(); }
        return false;
    }

    bool BaseTimeSeriesReferenceInput::valid() const { return has_value() || (has_output() && BaseTimeSeriesInput::valid()); }

    bool BaseTimeSeriesReferenceInput::all_valid() const { return has_value() || BaseTimeSeriesInput::all_valid(); }

    engine_time_t BaseTimeSeriesReferenceInput::last_modified_time() const {
        return has_output() ? output()->last_modified_time() : sample_time();
    }

    void BaseTimeSeriesReferenceInput::clone_binding(TimeSeriesReferenceInput::ptr other) {
        un_bind_output(false);
        auto base_other = dynamic_cast<BaseTimeSeriesReferenceInput*>(other);
        if (base_other == nullptr) {
            throw std::runtime_error("BaseTimeSeriesReferenceInput::clone_binding: Expected BaseTimeSeriesReferenceInput*");
        }
        if (base_other->has_output()) {
            bind_output(base_other->output());
        } else if (base_other->has_value()) {
            _value = base_other->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    bool BaseTimeSeriesReferenceInput::bind_output(const time_series_output_s_ptr &output_) {
        auto peer = do_bind_output(output_);

        if (owning_node()->is_started() && has_output() && output()->valid()) {
            set_sample_time(owning_graph()->evaluation_time());
            if (active()) { notify(sample_time()); }
        }

        return peer;
    }

    void BaseTimeSeriesReferenceInput::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();
        do_un_bind_output(unbind_refs);

        if (has_owning_node() && owning_node()->is_started() && was_valid) {
            set_sample_time(owning_graph()->evaluation_time());
            if (active()) {
                // Notify as the state of the node has changed from bound to unbound
                owning_node()->notify(sample_time());
            }
        }
    }

    void BaseTimeSeriesReferenceInput::make_active() {
        if (has_output()) {
            BaseTimeSeriesInput::make_active();
        } else {
            set_active(true);
        }

        if (valid()) {
            set_sample_time(owning_graph()->evaluation_time());
            notify(last_modified_time());
        }
    }

    void BaseTimeSeriesReferenceInput::make_passive() {
        if (has_output()) {
            BaseTimeSeriesInput::make_passive();
        } else {
            set_active(false);
        }
    }

    TimeSeriesInput::s_ptr BaseTimeSeriesReferenceInput::get_input(size_t index) {
        auto *ref = get_ref_input(index);
        return ref ? ref->shared_from_this() : time_series_input_s_ptr{};
    }

    TimeSeriesReferenceInput *BaseTimeSeriesReferenceInput::get_ref_input(size_t index) {
        throw std::runtime_error("BaseTimeSeriesReferenceInput::get_ref_input: Not implemented on this type");
    }

    bool BaseTimeSeriesReferenceInput::do_bind_output(time_series_output_s_ptr output_) {
        if (std::dynamic_pointer_cast<TimeSeriesReferenceOutput>(output_) != nullptr) {
            // Match Python behavior: bind to a TimeSeriesReferenceOutput as a normal peer
            reset_value();
            return BaseTimeSeriesInput::do_bind_output(output_);
        }
        // We are binding directly to a concrete output (not via a TimeSeriesReferenceOutput):
        // Match Python behavior - set _output = None (don't call BaseTimeSeriesInput::do_bind_output)
        // and store the reference value directly
        _value = TimeSeriesReference::make(output_);
        if (owning_node()->is_started()) {
            set_sample_time(owning_graph()->evaluation_time());
            notify(sample_time());
        } else {
            owning_node()->add_start_input(std::dynamic_pointer_cast<BaseTimeSeriesReferenceInput>(shared_from_this()));
        }
        return false;
    }

    void BaseTimeSeriesReferenceInput::do_un_bind_output(bool unbind_refs) {
        if (has_output()) { BaseTimeSeriesInput::do_un_bind_output(unbind_refs); }
        if (has_value()) {
            reset_value();
            // TODO: Do we need to notify here? Should we notify only if the input is active?
            set_sample_time(owning_node()->is_started() ? owning_graph()->evaluation_time() : MIN_ST);
        }
    }

    TimeSeriesReferenceOutput *BaseTimeSeriesReferenceInput::output_t() const {
        return const_cast<BaseTimeSeriesReferenceInput *>(this)->output_t();
    }

    TimeSeriesReferenceOutput *BaseTimeSeriesReferenceInput::output_t() {
        auto _output{output()};
        auto _result{dynamic_cast<TimeSeriesReferenceOutput *>(_output.get())};
        if (_result == nullptr) {
            throw std::runtime_error("BaseTimeSeriesReferenceInput::output_t: Expected TimeSeriesReferenceOutput*");
        }
        return _result;
    }

    void BaseTimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        reset_value();
        set_sample_time(modified_time);
        if (active()) { BaseTimeSeriesInput::notify_parent(this, modified_time); }
    }

    bool BaseTimeSeriesReferenceInput::has_value() const { return _value.has_value(); }

    void BaseTimeSeriesReferenceInput::reset_value() { _value.reset(); }

    std::optional<TimeSeriesReference> BaseTimeSeriesReferenceInput::raw_value() { return _value; }

    TimeSeriesValueReferenceInput::TimeSeriesValueReferenceInput(node_ptr parent)
        : _parent_adapter{parent}, _ts_input{this, typeid(TimeSeriesReference)} {}
    TimeSeriesValueReferenceInput::TimeSeriesValueReferenceInput(time_series_input_ptr parent)
        : _parent_adapter{parent}, _ts_input{this, typeid(TimeSeriesReference)} {}
    void TimeSeriesValueReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        // Since this is a leaf we should not have this method called.
        throw std::runtime_error("TimeSeriesValueReferenceInput does not support notify_parent");
    }
    void TimeSeriesValueReferenceInput::notify(engine_time_t et) {
        // This is the big change for now, to make things work with less pain.
        // We set notification to this input and then perform the parent delegation
        _parent_adapter.notify_modified(this, et);
    }
    engine_time_t TimeSeriesValueReferenceInput::current_engine_time() const { return owning_node()->current_engine_time(); }
    void          TimeSeriesValueReferenceInput::add_before_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_before_evaluation_notification(std::move(fn));
    }
    void TimeSeriesValueReferenceInput::add_after_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_after_evaluation_notification(std::move(fn));
    }
    node_ptr TimeSeriesValueReferenceInput::owning_node() { return _parent_adapter.owning_node(); }
    node_ptr TimeSeriesValueReferenceInput::owning_node() const { return _parent_adapter.owning_node(); }
    graph_ptr TimeSeriesValueReferenceInput::owning_graph() { return _parent_adapter.owning_graph(); }
    graph_ptr TimeSeriesValueReferenceInput::owning_graph() const { return _parent_adapter.owning_graph(); }
    bool      TimeSeriesValueReferenceInput::has_parent_or_node() const { return _parent_adapter.has_parent_or_node(); }
    bool      TimeSeriesValueReferenceInput::has_owning_node() const { return _parent_adapter.has_owning_node(); }
    nb::object TimeSeriesValueReferenceInput::py_value() const {
        auto ref_ptr = _ts_input.value().get_if<TimeSeriesReference>();
        return nb::cast(ref_ptr ? *ref_ptr : TimeSeriesReference::make());
    }
    nb::object TimeSeriesValueReferenceInput::py_delta_value() const { return py_value(); }
    engine_time_t TimeSeriesValueReferenceInput::last_modified_time() const { return _ts_input.last_modified_time(); }
    bool          TimeSeriesValueReferenceInput::modified() const { return _ts_input.modified(); }
    bool          TimeSeriesValueReferenceInput::valid() const { return _ts_input.valid(); }
    bool          TimeSeriesValueReferenceInput::all_valid() const { return _ts_input.valid(); }
    TimeSeriesReference TimeSeriesValueReferenceInput::value() const {
        auto ref_ptr = _ts_input.value().get_if<TimeSeriesReference>();
        return ref_ptr ? *ref_ptr : TimeSeriesReference::make();
    }
    void          TimeSeriesValueReferenceInput::re_parent(node_ptr parent) { _parent_adapter.re_parent(parent); }
    void          TimeSeriesValueReferenceInput::re_parent(const time_series_type_ptr parent) { _parent_adapter.re_parent(parent); }
    void          TimeSeriesValueReferenceInput::reset_parent_or_node() { _parent_adapter.reset_parent_or_node(); }
    bool          TimeSeriesValueReferenceInput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueReferenceInput *>(other) != nullptr;
    }
    TimeSeriesInput::s_ptr TimeSeriesValueReferenceInput::parent_input() const {
        auto p{_parent_adapter.parent_input()};
        return p != nullptr ? p->shared_from_this() : TimeSeriesInput::s_ptr{};
    }
    bool TimeSeriesValueReferenceInput::has_parent_input() const { return _parent_adapter.has_parent_input(); }
    bool TimeSeriesValueReferenceInput::active() const { return _ts_input.active(); }
    void TimeSeriesValueReferenceInput::make_active() { _ts_input.make_active(); }
    void TimeSeriesValueReferenceInput::make_passive() { _ts_input.make_passive(); }
    bool TimeSeriesValueReferenceInput::bound() const { return _ts_input.bound(); }
    bool TimeSeriesValueReferenceInput::has_peer() const { return _ts_input.bound(); }

    void TimeSeriesValueReferenceInput::start() {
        _ts_input.mark_sampled();
        // Match Python: notify the owning node that this input has changed
        notify(owning_graph()->evaluation_time());
    }

    // ============================================================
    // Specialized Reference Input Implementations
    // ============================================================

    // time_series_input_s_ptr TimeSeriesValueReferenceInput::clone_blank_ref_instance() {
    //     return arena_make_shared_as<TimeSeriesValueReferenceInput, TimeSeriesInput>(owning_node());
    // }

    bool TimeSeriesValueReferenceInput::bind_output(const time_series_output_s_ptr &output_) {
        auto out{dynamic_cast<TimeSeriesValueReferenceOutput *>(output_.get())};
        if (out == nullptr) {
            // We are adapting from a normal time-series value to a reference time-series
            _bound_reference_output = nullptr;  // Clear any previous reference output binding
            auto value{TimeSeriesReference::make(output_)};
            _ts_input.set_value(make_any_value(std::move(value)));
            // Notify the owning node that this input has been bound with a value
            // This is needed because NonBoundTSValue::notify_subscribers is a no-op
            if (has_owning_node()) {
                if (owning_node()->is_started() && valid()) {
                    notify(owning_graph()->evaluation_time());
                } else {
                    // Register this input to have start() called during node startup
                    owning_node()->add_start_input(std::dynamic_pointer_cast<TimeSeriesReferenceInput>(shared_from_this()));
                }
            }
        } else {
            // Binding to a TimeSeriesValueReferenceOutput - track it for output() access
            _bound_reference_output = output_;
            _ts_input.bind_output(out->_ts_output);
        }
        return true;  // Always returns true as we're binding to a concrete output
    }

    void TimeSeriesValueReferenceInput::un_bind_output(bool unbind_refs) {
        _bound_reference_output = nullptr;  // Clear reference output tracking
        _ts_input.un_bind();
    }

    void TimeSeriesValueReferenceInput::clone_binding(TimeSeriesReferenceInput::ptr other) {
        // Match Python: first check if other has an output we should bind to
        if (other->has_output()) {
            bind_output(other->output());
            return;
        }

        // For TimeSeriesValueReferenceInput, clone_binding copies the value from the other reference
        if (auto other_val = dynamic_cast<TimeSeriesValueReferenceInput*>(other)) {
            // Copy from another TimeSeriesValueReferenceInput - copy the underlying value
            if (other_val->valid()) {
                AnyValue<> any_v;
                any_v.emplace<TimeSeriesReference>(other_val->value());
                _ts_input.set_value(std::move(any_v));
                // Notify if node is started and input is active (match Python behavior)
                if (has_owning_node() && owning_node()->is_started() && active()) {
                    notify(owning_graph()->evaluation_time());
                }
            }
        } else if (auto base_other = dynamic_cast<BaseTimeSeriesReferenceInput*>(other)) {
            // Copy from BaseTimeSeriesReferenceInput
            if (base_other->valid()) {
                AnyValue<> any_v;
                any_v.emplace<TimeSeriesReference>(base_other->value());
                _ts_input.set_value(std::move(any_v));
                // Notify if node is started and input is active (match Python behavior)
                if (has_owning_node() && owning_node()->is_started() && active()) {
                    notify(owning_graph()->evaluation_time());
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesValueReferenceInput::clone_binding: Unsupported reference input type");
        }
    }

    // TimeSeriesListReferenceInput - REF[TSL[...]]
    TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(Node *owning_node, InputBuilder::ptr value_builder, size_t size)
        : BaseTimeSeriesReferenceInput(owning_node), _value_builder(std::move(value_builder)), _size(size) {}

    TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(TimeSeriesInput *parent_input, InputBuilder::ptr value_builder,
                                                               size_t size)
        : BaseTimeSeriesReferenceInput(parent_input), _value_builder(std::move(value_builder)), _size(size) {}

    TimeSeriesInput::s_ptr TimeSeriesListReferenceInput::get_input(size_t index) { return get_ref_input(index)->shared_from_this(); }

    TimeSeriesReference TimeSeriesListReferenceInput::value() const {
        if (has_output()) { return output_t()->py_value_or_empty(); }
        if (has_value()) { return *_value; }
        if (_items.has_value()) {
            _value = TimeSeriesReference::make(*_items);
            return *_value;
        }
        return TimeSeriesReference::make();
    }

    bool TimeSeriesListReferenceInput::bound() const { return BaseTimeSeriesReferenceInput::bound() || !_items.has_value(); }

    bool TimeSeriesListReferenceInput::modified() const {
        if (!(BaseTimeSeriesReferenceInput::modified())) {
            if (_items.has_value()) {
                return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->modified(); });
            }
            return false;
        }
        return true;
    }

    bool TimeSeriesListReferenceInput::valid() const {
        return BaseTimeSeriesReferenceInput::valid() ||
               (_items.has_value() && !_items->empty() &&
                std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); })) ||
               (has_output() && BaseTimeSeriesInput::valid());
    }

    bool TimeSeriesListReferenceInput::all_valid() const {
        return (_items.has_value() && !_items->empty() &&
                std::all_of(_items->begin(), _items->end(), [](const auto &item) { return item->all_valid(); })) ||
               BaseTimeSeriesReferenceInput::all_valid();
    }

    engine_time_t TimeSeriesListReferenceInput::last_modified_time() const {
        std::vector<engine_time_t> times;
        if (_items.has_value()) {
            for (const auto &item : *_items) { times.push_back(item->last_modified_time()); }
        }

        if (has_output()) { times.push_back(output()->last_modified_time()); }

        return times.empty() ? sample_time() : *std::max_element(times.begin(), times.end());
    }

    void TimeSeriesListReferenceInput::clone_binding(TimeSeriesReferenceInput::ptr other) {
        auto other_{dynamic_cast<TimeSeriesListReferenceInput *>(other)};
        if (other_ == nullptr) {
            throw std::runtime_error(
                "TimeSeriesListReferenceInput::clone_binding: Expected TimeSeriesListReferenceInput*");
        }
        un_bind_output(false);
        if (other_->has_output()) {
            bind_output(other_->output());
        } else if (other_->_items.has_value()) {
            for (size_t i = 0; i < other_->_items->size(); ++i) {
                auto other_item = dynamic_cast<TimeSeriesReferenceInput *>((*other_->_items)[i].get());
                this->get_ref_input(i)->clone_binding(other_item);
            }
        } else if (other_->has_value()) {
            _value = other_->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    std::vector<time_series_input_s_ptr> &TimeSeriesListReferenceInput::items() {
        return _items.has_value() ? *_items : empty_items;
    }

    const std::vector<time_series_input_s_ptr> &TimeSeriesListReferenceInput::items() const {
        return _items.has_value() ? *_items : empty_items;
    }

    void TimeSeriesListReferenceInput::make_active() {
        BaseTimeSeriesReferenceInput::make_active();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_active(); }
        }
    }

    void TimeSeriesListReferenceInput::make_passive() {
        BaseTimeSeriesReferenceInput::make_passive();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_passive(); }
        }
    }

    time_series_input_s_ptr TimeSeriesListReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesListReferenceInput, TimeSeriesInput>(owning_node(), _value_builder, _size);
    }

    TimeSeriesReferenceInput *TimeSeriesListReferenceInput::get_ref_input(size_t index) {
        if (!_items.has_value()) {
            _items = std::vector<time_series_input_s_ptr>{};
            _items->reserve(_size);
            for (size_t i = 0; i < _size; ++i) {
                auto new_item = _value_builder->make_instance(this);
                if (active()) { new_item->make_active(); }
                _items->push_back(std::dynamic_pointer_cast<TimeSeriesInput>(new_item));
            }
        }
        return dynamic_cast<TimeSeriesReferenceInput *>((*_items)[index].get());
    }

    // TimeSeriesBundleReferenceInput - REF[TSB[...]]
    TimeSeriesBundleReferenceInput::TimeSeriesBundleReferenceInput(Node *owning_node, std::vector<InputBuilder::ptr> value_builders,
                                                                   size_t size)
        : BaseTimeSeriesReferenceInput(owning_node), _value_builders(std::move(value_builders)), _size(size), _items{} {}

    TimeSeriesBundleReferenceInput::TimeSeriesBundleReferenceInput(TimeSeriesInput                *parent_input,
                                                                   std::vector<InputBuilder::ptr> value_builders, size_t size)
        : BaseTimeSeriesReferenceInput(parent_input), _value_builders(std::move(value_builders)), _size(size) {}

    bool TimeSeriesBundleReferenceInput::bound() const { return BaseTimeSeriesReferenceInput::bound() || !_items.has_value(); }

    bool TimeSeriesBundleReferenceInput::modified() const {
        if (!(BaseTimeSeriesReferenceInput::modified())) {
            if (_items.has_value()) {
                return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->modified(); });
            }
            return false;
        }
        return true;
    }

    bool TimeSeriesBundleReferenceInput::valid() const {
        return BaseTimeSeriesReferenceInput::valid() ||
               (_items.has_value() && !_items->empty() &&
                std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); })) ||
               (has_output() && BaseTimeSeriesInput::valid());
    }

    bool TimeSeriesBundleReferenceInput::all_valid() const {
        return (_items.has_value() && !_items->empty() &&
                std::all_of(_items->begin(), _items->end(), [](const auto &item) { return item->all_valid(); })) ||
               BaseTimeSeriesReferenceInput::all_valid();
    }

    engine_time_t TimeSeriesBundleReferenceInput::last_modified_time() const {
        std::vector<engine_time_t> times;
        if (_items.has_value()) {
            for (const auto &item : *_items) { times.push_back(item->last_modified_time()); }
        }

        if (has_output()) { times.push_back(output()->last_modified_time()); }

        return times.empty() ? sample_time() : *std::max_element(times.begin(), times.end());
    }

    void TimeSeriesBundleReferenceInput::clone_binding(TimeSeriesReferenceInput::ptr other) {
        auto other_{dynamic_cast<TimeSeriesBundleReferenceInput *>(other)};
        if (other_ == nullptr) {
            throw std::runtime_error(
                "TimeSeriesBundleReferenceInput::clone_binding: Expected TimeSeriesBundleReferenceInput*");
        }
        un_bind_output(false);
        if (other_->has_output()) {
            bind_output(other_->output());
        } else if (other_->_items.has_value()) {
            for (size_t i = 0; i < other_->_items->size(); ++i) {
                auto other_item = dynamic_cast<TimeSeriesReferenceInput *>((*other_->_items)[i].get());
                this->get_ref_input(i)->clone_binding(other_item);
            }
        } else if (other_->has_value()) {
            _value = other_->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    std::vector<time_series_input_s_ptr> &TimeSeriesBundleReferenceInput::items() {
        return _items.has_value() ? *_items : empty_items;
    }

    const std::vector<time_series_input_s_ptr> &TimeSeriesBundleReferenceInput::items() const {
        return _items.has_value() ? *_items : empty_items;
    }

    TimeSeriesReference TimeSeriesBundleReferenceInput::value() const {
        if (has_output()) { return output_t()->py_value_or_empty(); }
        if (has_value()) { return *_value; }
        if (_items.has_value()) {
            _value = TimeSeriesReference::make(*_items);
            return *_value;
        }
        return TimeSeriesReference::make();
    }

    void TimeSeriesBundleReferenceInput::make_active() {
        BaseTimeSeriesReferenceInput::make_active();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_active(); }
        }
    }

    void TimeSeriesBundleReferenceInput::make_passive() {
        BaseTimeSeriesReferenceInput::make_passive();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_passive(); }
        }
    }

    time_series_input_s_ptr TimeSeriesBundleReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesBundleReferenceInput, TimeSeriesInput>(owning_node(), _value_builders, _size);
    }

    TimeSeriesReferenceInput *TimeSeriesBundleReferenceInput::get_ref_input(size_t index) {
        if (!_items.has_value()) {
            _items = std::vector<time_series_input_s_ptr>{};
            _items->reserve(_size);
            for (size_t i = 0; i < _size; ++i) {
                auto new_item = _value_builders[i]->make_instance(this);
                if (active()) { new_item->make_active(); }
                _items->push_back(std::dynamic_pointer_cast<TimeSeriesInput>(new_item));
            }
        }
        return dynamic_cast<TimeSeriesReferenceInput *>((*_items)[index].get());
    }

    time_series_input_s_ptr TimeSeriesDictReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesDictReferenceInput, TimeSeriesInput>(owning_node());
    }

    time_series_input_s_ptr TimeSeriesSetReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesSetReferenceInput, TimeSeriesInput>(owning_node());
    }

    time_series_input_s_ptr TimeSeriesWindowReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesWindowReferenceInput, TimeSeriesInput>(owning_node());
    }

    // ============================================================
    // TimeSeriesValueReferenceOutput Implementation
    // ============================================================

    TimeSeriesValueReferenceOutput::TimeSeriesValueReferenceOutput(node_ptr parent)
        : _parent_adapter{parent}, _ts_output{this, typeid(TimeSeriesReference)} {}

    TimeSeriesValueReferenceOutput::TimeSeriesValueReferenceOutput(time_series_output_ptr parent)
        : _parent_adapter{parent}, _ts_output{this, typeid(TimeSeriesReference)} {}

    void TimeSeriesValueReferenceOutput::notify(engine_time_t et) {
        // Outputs notify their parent (which may be a collection output or node)
        if (_parent_adapter.has_parent_output()) {
            _parent_adapter.parent_output()->mark_child_modified(*this, et);
        }
        // Note: Unlike inputs, outputs don't typically notify the node directly
        // The node is notified through the subscription mechanism
    }

    engine_time_t TimeSeriesValueReferenceOutput::current_engine_time() const { return owning_node()->current_engine_time(); }

    void TimeSeriesValueReferenceOutput::add_before_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_before_evaluation_notification(std::move(fn));
    }

    void TimeSeriesValueReferenceOutput::add_after_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_after_evaluation_notification(std::move(fn));
    }

    node_ptr TimeSeriesValueReferenceOutput::owning_node() { return _parent_adapter.owning_node(); }

    node_ptr TimeSeriesValueReferenceOutput::owning_node() const { return _parent_adapter.owning_node(); }

    graph_ptr TimeSeriesValueReferenceOutput::owning_graph() { return _parent_adapter.owning_graph(); }

    graph_ptr TimeSeriesValueReferenceOutput::owning_graph() const { return _parent_adapter.owning_graph(); }

    bool TimeSeriesValueReferenceOutput::has_parent_or_node() const { return _parent_adapter.has_parent_or_node(); }

    bool TimeSeriesValueReferenceOutput::has_owning_node() const { return _parent_adapter.has_owning_node(); }

    nb::object TimeSeriesValueReferenceOutput::py_value() const { return _ts_output.value().as_python(); }

    nb::object TimeSeriesValueReferenceOutput::py_delta_value() const {
        auto event = _ts_output.delta_value();
        return event.value.as_python();
    }

    engine_time_t TimeSeriesValueReferenceOutput::last_modified_time() const { return _ts_output.last_modified_time(); }

    bool TimeSeriesValueReferenceOutput::modified() const { return _ts_output.modified(); }

    bool TimeSeriesValueReferenceOutput::valid() const { return _ts_output.valid(); }

    bool TimeSeriesValueReferenceOutput::all_valid() const { return _ts_output.valid(); }

    void TimeSeriesValueReferenceOutput::re_parent(node_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesValueReferenceOutput::re_parent(const time_series_type_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesValueReferenceOutput::reset_parent_or_node() { _parent_adapter.reset_parent_or_node(); }

    void TimeSeriesValueReferenceOutput::builder_release_cleanup() {
        // Think about what may be required to be done here
    }

    bool TimeSeriesValueReferenceOutput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueReferenceOutput *>(other) != nullptr;
    }

    bool TimeSeriesValueReferenceOutput::is_reference() const { return true; }

    bool TimeSeriesValueReferenceOutput::has_reference() const { return true; }

    TimeSeriesOutput::s_ptr TimeSeriesValueReferenceOutput::parent_output() const {
        auto p{_parent_adapter.parent_output()};
        return p != nullptr ? p->shared_from_this() : s_ptr{};
    }

    TimeSeriesOutput::s_ptr TimeSeriesValueReferenceOutput::parent_output() {
        auto p{_parent_adapter.parent_output()};
        return p != nullptr ? p->shared_from_this() : s_ptr{};
    }

    bool TimeSeriesValueReferenceOutput::has_parent_output() const { return _parent_adapter.has_parent_output(); }

    void TimeSeriesValueReferenceOutput::subscribe(Notifiable *node) { _ts_output.subscribe(node); }

    void TimeSeriesValueReferenceOutput::un_subscribe(Notifiable *node) { _ts_output.unsubscribe(node); }

    void TimeSeriesValueReferenceOutput::apply_result(const nb::object &value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    void TimeSeriesValueReferenceOutput::py_set_value(const nb::object &value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        auto v{nb::cast<TimeSeriesReference>(value)};
        AnyValue<> any_v;
        any_v.emplace<TimeSeriesReference>(std::move(v));
        _ts_output.set_value(std::move(any_v));
        notify_reference_observers();
    }

    void TimeSeriesValueReferenceOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto output_t = dynamic_cast<const TimeSeriesValueReferenceOutput *>(&output);
        if (output_t) {
            if (output_t->valid()) {
                AnyValue<> any_v;
                any_v.emplace<TimeSeriesReference>(*output_t->_ts_output.value().get_if<TimeSeriesReference>());
                _ts_output.set_value(std::move(any_v));
                notify_reference_observers();
            }
        } else {
            throw std::runtime_error("TimeSeriesValueReferenceOutput::copy_from_output: Expected TimeSeriesValueReferenceOutput");
        }
    }

    void TimeSeriesValueReferenceOutput::copy_from_input(const TimeSeriesInput &input) {
        auto input_t = dynamic_cast<const TimeSeriesValueReferenceInput *>(&input);
        if (input_t) {
            if (input_t->valid()) {
                auto ref_value = input_t->_ts_input.value().get_if<TimeSeriesReference>();
                if (ref_value) {
                    AnyValue<> any_v;
                    any_v.emplace<TimeSeriesReference>(*ref_value);
                    _ts_output.set_value(std::move(any_v));
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesValueReferenceOutput::copy_from_input: Expected TimeSeriesValueReferenceInput");
        }
    }

    void TimeSeriesValueReferenceOutput::clear() {
        AnyValue<> any_v;
        any_v.emplace<TimeSeriesReference>(TimeSeriesReference::make());
        _ts_output.set_value(std::move(any_v));
    }

    void TimeSeriesValueReferenceOutput::invalidate() { _ts_output.invalidate(); }

    void TimeSeriesValueReferenceOutput::mark_invalid() { _ts_output.invalidate(); }

    void TimeSeriesValueReferenceOutput::mark_modified() {
        // Trigger notification by setting the current value again
        // This will update the last_modified_time and notify subscribers
        if (valid()) {
            auto current = _ts_output.value();
            _ts_output.set_value(current);
        }
    }

    void TimeSeriesValueReferenceOutput::mark_modified(engine_time_t modified_time) {
        // The TSOutput handles modified time internally based on events
        mark_modified();
    }

    void TimeSeriesValueReferenceOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        // For a value output, we don't have children in the traditional sense
        // But we propagate the modification notification upward
        notify(modified_time);
    }

    bool TimeSeriesValueReferenceOutput::can_apply_result(const nb::object &value) { return !modified(); }

    std::optional<TimeSeriesReference> TimeSeriesValueReferenceOutput::reference_value() const {
        if (!valid()) { return std::nullopt; }
        auto ref_ptr = _ts_output.value().get_if<TimeSeriesReference>();
        if (ref_ptr) { return *ref_ptr; }
        return std::nullopt;
    }

    void TimeSeriesValueReferenceOutput::observe_reference(TimeSeriesInput::ptr input_) {
        _reference_observers.emplace(input_);
    }

    void TimeSeriesValueReferenceOutput::stop_observing_reference(TimeSeriesInput::ptr input_) {
        _reference_observers.erase(input_);
    }

    void TimeSeriesValueReferenceOutput::notify_reference_observers() {
        auto ref_opt = reference_value();
        if (ref_opt) {
            for (auto input : _reference_observers) {
                ref_opt->bind_input(*input);
            }
        }
    }

    // ============================================================
    // Specialized Reference Output Implementations
    // ============================================================

    // TimeSeriesListReferenceOutput - REF[TSL[...]]
    TimeSeriesListReferenceOutput::TimeSeriesListReferenceOutput(Node *owning_node, OutputBuilder::ptr value_builder, size_t size)
        : TimeSeriesReferenceOutput(owning_node), _value_builder(std::move(value_builder)), _size(size) {}

    TimeSeriesListReferenceOutput::TimeSeriesListReferenceOutput(TimeSeriesOutput *parent_output, OutputBuilder::ptr value_builder,
                                                                 size_t size)
        : TimeSeriesReferenceOutput(parent_output), _value_builder(std::move(value_builder)), _size(size) {}

    // TimeSeriesBundleReferenceOutput - REF[TSB[...]]
    TimeSeriesBundleReferenceOutput::TimeSeriesBundleReferenceOutput(Node                           *owning_node,
                                                                     std::vector<OutputBuilder::ptr> value_builder, size_t size)
        : TimeSeriesReferenceOutput(owning_node), _value_builder(std::move(value_builder)), _size(size) {}

    TimeSeriesBundleReferenceOutput::TimeSeriesBundleReferenceOutput(TimeSeriesOutput                 *parent_output,
                                                                     std::vector<OutputBuilder::ptr> value_builder, size_t size)
        : TimeSeriesReferenceOutput(parent_output), _value_builder(std::move(value_builder)), _size(size) {}

}  // namespace hgraph
