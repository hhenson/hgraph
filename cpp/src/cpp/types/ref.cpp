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

    const TimeSeriesReference &TimeSeriesReference::empty() {
        static const TimeSeriesReference empty_ref;
        return empty_ref;
    }

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


    TimeSeriesReference TimeSeriesReference::make(const std::vector<TimeSeriesReferenceInput*>& items) {
        if (items.empty()) { return make(); }
        std::vector<TimeSeriesReference> refs;
        refs.reserve(items.size());
        for (auto item : items) {
            refs.emplace_back(item->value());
        }
        return TimeSeriesReference(std::move(refs));
    }

    TimeSeriesReference TimeSeriesReference::make(const std::vector<std::shared_ptr<TimeSeriesReferenceInput>>& items) {
        if (items.empty()) { return make(); }
        std::vector<TimeSeriesReference> refs;
        refs.reserve(items.size());
        for (const auto& item : items) {
            refs.emplace_back(item->value());
        }
        return TimeSeriesReference(std::move(refs));
    }

    // ============================================================
    // TimeSeriesReferenceOutput Implementation
    // ============================================================

    bool TimeSeriesReferenceOutput::is_same_type(const TimeSeriesType *other) const {
        // Single comparison checks both type (Reference) and direction (Output)
        return other->kind() == kind();
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
        auto input_t = dynamic_cast<const TimeSeriesReferenceInput *>(&input);
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

    void TimeSeriesReferenceInput::start() {
        set_sample_time(owning_graph()->evaluation_time());
        notify(sample_time());
    }

    nb::object TimeSeriesReferenceInput::py_value() const {
        return nb::cast(value());
    }

    nb::object TimeSeriesReferenceInput::py_delta_value() const { return py_value(); }

    const TimeSeriesReference& TimeSeriesReferenceInput::value() const {
        if (has_output()) { 
            auto ref_output = static_cast<TimeSeriesReferenceOutput*>(output());
            if (ref_output->has_value()) {
                return ref_output->value();
            } else {
                return TimeSeriesReference::empty();
            }
        }
        if (has_value()) { return *_value; }
        return TimeSeriesReference::empty();
    }

    bool TimeSeriesReferenceInput::bound() const { return BaseTimeSeriesInput::bound(); }

    bool TimeSeriesReferenceInput::modified() const {
        if (sampled()) { return true; }
        if (has_output()) { return output()->modified(); }
        return false;
    }

    bool TimeSeriesReferenceInput::valid() const { return has_value() || (has_output() && BaseTimeSeriesInput::valid()); }

    bool TimeSeriesReferenceInput::all_valid() const { return has_value() || BaseTimeSeriesInput::all_valid(); }

    engine_time_t TimeSeriesReferenceInput::last_modified_time() const {
        return has_output() ? output()->last_modified_time() : sample_time();
    }

    void TimeSeriesReferenceInput::clone_binding(const TimeSeriesReferenceInput::ptr other) {
        un_bind_output(false);
        if (other->has_output()) {
            bind_output(other->output()->shared_from_this());
        } else if (other->has_value()) {
            _value = other->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    bool TimeSeriesReferenceInput::bind_output(time_series_output_s_ptr output_) {
        auto peer = do_bind_output(output_);

        if (owning_node()->is_started() && has_output() && output()->valid()) {
            set_sample_time(owning_graph()->evaluation_time());
            if (active()) { notify(sample_time()); }
        }

        return peer;
    }

    void TimeSeriesReferenceInput::un_bind_output(bool unbind_refs) {
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

    void TimeSeriesReferenceInput::make_active() {
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

    void TimeSeriesReferenceInput::make_passive() {
        if (has_output()) {
            BaseTimeSeriesInput::make_passive();
        } else {
            set_active(false);
        }
    }

    TimeSeriesInput::s_ptr TimeSeriesReferenceInput::get_input(size_t index) {
        auto *ref = get_ref_input(index);
        return ref ? ref->shared_from_this() : time_series_input_s_ptr{};
    }

    TimeSeriesReferenceInput *TimeSeriesReferenceInput::get_ref_input(size_t index) {
        throw std::runtime_error("TimeSeriesReferenceInput::get_ref_input: Not implemented on this type");
    }

    bool TimeSeriesReferenceInput::do_bind_output(time_series_output_s_ptr output_) {
        if (std::dynamic_pointer_cast<TimeSeriesReferenceOutput>(output_) != nullptr) {
            // Match Python behavior: bind to a TimeSeriesReferenceOutput as a normal peer
            reset_value();
            return BaseTimeSeriesInput::do_bind_output(output_);
        }
        // We are binding directly to a concrete output: wrap it as a reference value
        // Get shared_ptr to keep the output alive while this reference holds it
        _value = TimeSeriesReference::make(std::move(output_));
        if (owning_node()->is_started()) {
            set_sample_time(owning_graph()->evaluation_time());
            notify(sample_time());
        } else {
            owning_node()->add_start_input(std::dynamic_pointer_cast<TimeSeriesReferenceInput>(shared_from_this()));
        }
        return false;
    }

    void TimeSeriesReferenceInput::do_un_bind_output(bool unbind_refs) {
        if (has_output()) { BaseTimeSeriesInput::do_un_bind_output(unbind_refs); }
        if (has_value()) {
            reset_value();
            // TODO: Do we need to notify here? Should we notify only if the input is active?
            set_sample_time(owning_node()->is_started() ? owning_graph()->evaluation_time() : MIN_ST);
        }
    }

    TimeSeriesReferenceOutput *TimeSeriesReferenceInput::output_t() const {
        return const_cast<TimeSeriesReferenceInput *>(this)->output_t();
    }

    TimeSeriesReferenceOutput *TimeSeriesReferenceInput::output_t() {
        auto _output{output()};
        auto _result{dynamic_cast<TimeSeriesReferenceOutput *>(_output)};
        if (_result == nullptr) {
            throw std::runtime_error("TimeSeriesReferenceInput::output_t: Expected TimeSeriesReferenceOutput*");
        }
        return _result;
    }

    bool TimeSeriesReferenceInput::is_reference() const { return true; }

    bool TimeSeriesReferenceInput::has_reference() const { return true; }

    void TimeSeriesReferenceInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        reset_value();
        set_sample_time(modified_time);
        if (active()) { BaseTimeSeriesInput::notify_parent(this, modified_time); }
    }

    bool TimeSeriesReferenceInput::has_value() const { return _value.has_value(); }

    void TimeSeriesReferenceInput::reset_value() { _value.reset(); }

    std::optional<TimeSeriesReference> &TimeSeriesReferenceInput::raw_value() { return _value; }

    // ============================================================
    // Specialized Reference Input Implementations
    // ============================================================

    time_series_input_s_ptr TimeSeriesValueReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesValueReferenceInput, TimeSeriesInput>(owning_node());
    }

    // TimeSeriesListReferenceInput - REF[TSL[...]]
    TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(Node *owning_node, InputBuilder::ptr value_builder, size_t size)
        : TimeSeriesReferenceInput(owning_node), _value_builder(std::move(value_builder)), _size(size) {}

    TimeSeriesListReferenceInput::TimeSeriesListReferenceInput(TimeSeriesInput *parent_input, InputBuilder::ptr value_builder,
                                                               size_t size)
        : TimeSeriesReferenceInput(parent_input), _value_builder(std::move(value_builder)), _size(size) {}

    TimeSeriesInput::s_ptr TimeSeriesListReferenceInput::get_input(size_t index) { return get_ref_input(index)->shared_from_this(); }

    const TimeSeriesReference& TimeSeriesListReferenceInput::value() const {
        if (has_output()) { 
            auto ref_output = static_cast<TimeSeriesReferenceOutput*>(output());
            if (ref_output->has_value()) {
                return ref_output->value();
            } else {
                return TimeSeriesReference::empty();
            }
        }
        if (has_value()) { return *_value; }
        if (_items.has_value()) {
            _value = TimeSeriesReference::make(*_items);
            return *_value;
        }
        return TimeSeriesReference::empty();
    }

    bool TimeSeriesListReferenceInput::bound() const { return TimeSeriesReferenceInput::bound() || _items.has_value(); }

    bool TimeSeriesListReferenceInput::modified() const {
        if (!(TimeSeriesReferenceInput::modified())) {
            if (_items.has_value()) {
                return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->modified(); });
            }
            return false;
        }
        return true;
    }

    bool TimeSeriesListReferenceInput::valid() const {
        return TimeSeriesReferenceInput::valid() ||
               (_items.has_value() && !_items->empty() &&
                std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); })) ||
               (has_output() && BaseTimeSeriesInput::valid());
    }

    bool TimeSeriesListReferenceInput::all_valid() const {
        return (_items.has_value() && !_items->empty() &&
                std::all_of(_items->begin(), _items->end(), [](const auto &item) { return item->all_valid(); })) ||
               TimeSeriesReferenceInput::all_valid();
    }

    engine_time_t TimeSeriesListReferenceInput::last_modified_time() const {
        std::vector<engine_time_t> times;
        if (_items.has_value()) {
            for (const auto &item : *_items) { times.push_back(item->last_modified_time()); }
        }

        if (has_output()) { times.push_back(output()->last_modified_time()); }

        return times.empty() ? sample_time() : *std::max_element(times.begin(), times.end());
    }

    void TimeSeriesListReferenceInput::clone_binding(const TimeSeriesReferenceInput::ptr other) {
        auto other_{dynamic_cast<TimeSeriesListReferenceInput const *>(other)};
        if (other_ == nullptr) {
            throw std::runtime_error(
                "TimeSeriesBundleReferenceInput::clone_binding: Expected TimeSeriesBundleReferenceInput const*");
        }
        un_bind_output(false);
        if (other_->has_output()) {
            bind_output(other_->output()->shared_from_this());
        } else if (other_->_items.has_value()) {
            for (size_t i = 0; i < other_->_items->size(); ++i) { this->get_ref_input(i)->clone_binding((*other_->_items)[i].get()); }
        } else if (other_->has_value()) {
            _value = other_->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    void TimeSeriesListReferenceInput::un_bind_output(bool unbind_refs) {
        TimeSeriesReferenceInput::un_bind_output(unbind_refs);
        if (_items.has_value()) {
            for (auto &item : *_items) { item->un_bind_output(unbind_refs); }
        }
    }

    std::vector<TimeSeriesReferenceInput::s_ptr> &TimeSeriesListReferenceInput::items() {
        return _items.has_value() ? *_items : empty_items;
    }

    const std::vector<TimeSeriesReferenceInput::s_ptr> &TimeSeriesListReferenceInput::items() const {
        return _items.has_value() ? *_items : empty_items;
    }

    void TimeSeriesListReferenceInput::make_active() {
        TimeSeriesReferenceInput::make_active();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_active(); }
        }
    }

    void TimeSeriesListReferenceInput::make_passive() {
        TimeSeriesReferenceInput::make_passive();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_passive(); }
        }
    }

    time_series_input_s_ptr TimeSeriesListReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesListReferenceInput, TimeSeriesInput>(owning_node(), _value_builder, _size);
    }

    TimeSeriesReferenceInput *TimeSeriesListReferenceInput::get_ref_input(size_t index) {
        if (!_items.has_value()) {
            _items = std::vector<TimeSeriesReferenceInput::s_ptr>{};
            _items->reserve(_size);
            for (size_t i = 0; i < _size; ++i) {
                auto new_item = _value_builder->make_instance(this);
                if (active()) { new_item->make_active(); }
                _items->push_back(std::dynamic_pointer_cast<TimeSeriesReferenceInput>(new_item));
            }
        }
        return (*_items)[index].get();
    }

    // TimeSeriesBundleReferenceInput - REF[TSB[...]]
    TimeSeriesBundleReferenceInput::TimeSeriesBundleReferenceInput(Node *owning_node, std::vector<InputBuilder::ptr> value_builders,
                                                                   size_t size)
        : TimeSeriesReferenceInput(owning_node), _value_builders(std::move(value_builders)), _size(size), _items{} {}

    TimeSeriesBundleReferenceInput::TimeSeriesBundleReferenceInput(TimeSeriesInput                *parent_input,
                                                                   std::vector<InputBuilder::ptr> value_builders, size_t size)
        : TimeSeriesReferenceInput(parent_input), _value_builders(std::move(value_builders)), _size(size) {}

    bool TimeSeriesBundleReferenceInput::bound() const { return TimeSeriesReferenceInput::bound() || _items.has_value(); }

    bool TimeSeriesBundleReferenceInput::modified() const {
        if (!(TimeSeriesReferenceInput::modified())) {
            if (_items.has_value()) {
                return std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->modified(); });
            }
            return false;
        }
        return true;
    }

    bool TimeSeriesBundleReferenceInput::valid() const {
        return TimeSeriesReferenceInput::valid() ||
               (_items.has_value() && !_items->empty() &&
                std::any_of(_items->begin(), _items->end(), [](const auto &item) { return item->valid(); })) ||
               (has_output() && BaseTimeSeriesInput::valid());
    }

    bool TimeSeriesBundleReferenceInput::all_valid() const {
        return (_items.has_value() && !_items->empty() &&
                std::all_of(_items->begin(), _items->end(), [](const auto &item) { return item->all_valid(); })) ||
               TimeSeriesReferenceInput::all_valid();
    }

    engine_time_t TimeSeriesBundleReferenceInput::last_modified_time() const {
        std::vector<engine_time_t> times;
        if (_items.has_value()) {
            for (const auto &item : *_items) { times.push_back(item->last_modified_time()); }
        }

        if (has_output()) { times.push_back(output()->last_modified_time()); }

        return times.empty() ? sample_time() : *std::max_element(times.begin(), times.end());
    }

    void TimeSeriesBundleReferenceInput::clone_binding(const TimeSeriesReferenceInput::ptr other) {
        auto other_{dynamic_cast<TimeSeriesBundleReferenceInput const *>(other)};
        if (other_ == nullptr) {
            throw std::runtime_error(
                "TimeSeriesBundleReferenceInput::clone_binding: Expected TimeSeriesBundleReferenceInput const*");
        }
        un_bind_output(false);
        if (other_->has_output()) {
            bind_output(other_->output()->shared_from_this());
        } else if (other_->_items.has_value()) {
            for (size_t i = 0; i < other_->_items->size(); ++i) { this->get_ref_input(i)->clone_binding((*other_->_items)[i].get()); }
        } else if (other_->has_value()) {
            _value = other_->_value;
            if (owning_node()->is_started()) {
                set_sample_time(owning_graph()->evaluation_time());
                if (active()) { notify(sample_time()); }
            }
        }
    }

    void TimeSeriesBundleReferenceInput::un_bind_output(bool unbind_refs) {
        TimeSeriesReferenceInput::un_bind_output(unbind_refs);
        if (_items.has_value()) {
            for (auto &item : *_items) { item->un_bind_output(unbind_refs); }
        }
    }

    std::vector<TimeSeriesReferenceInput::s_ptr> &TimeSeriesBundleReferenceInput::items() {
        return _items.has_value() ? *_items : empty_items;
    }

    const std::vector<TimeSeriesReferenceInput::s_ptr> &TimeSeriesBundleReferenceInput::items() const {
        return _items.has_value() ? *_items : empty_items;
    }

    const TimeSeriesReference& TimeSeriesBundleReferenceInput::value() const {
        if (has_output()) { 
            auto ref_output = static_cast<TimeSeriesReferenceOutput*>(output());
            if (ref_output->has_value()) {
                return ref_output->value();
            } else {
                return TimeSeriesReference::empty();
            }
        }
        if (has_value()) { return *_value; }
        if (_items.has_value()) {
            _value = TimeSeriesReference::make(*_items);
            return *_value;
        }
        return TimeSeriesReference::empty();
    }

    void TimeSeriesBundleReferenceInput::make_active() {
        TimeSeriesReferenceInput::make_active();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_active(); }
        }
    }

    void TimeSeriesBundleReferenceInput::make_passive() {
        TimeSeriesReferenceInput::make_passive();
        if (_items.has_value()) {
            for (auto &item : *_items) { item->make_passive(); }
        }
    }

    time_series_input_s_ptr TimeSeriesBundleReferenceInput::clone_blank_ref_instance() {
        return arena_make_shared_as<TimeSeriesBundleReferenceInput, TimeSeriesInput>(owning_node(), _value_builders, _size);
    }

    TimeSeriesReferenceInput *TimeSeriesBundleReferenceInput::get_ref_input(size_t index) {
        if (!_items.has_value()) {
            _items = std::vector<TimeSeriesReferenceInput::s_ptr>{};
            _items->reserve(_size);
            for (size_t i = 0; i < _size; ++i) {
                auto new_item = _value_builders[i]->make_instance(this);
                if (active()) { new_item->make_active(); }
                _items->push_back(std::dynamic_pointer_cast<TimeSeriesReferenceInput>(new_item));
            }
        }
        return (*_items)[index].get();
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
