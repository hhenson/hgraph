#include "hgraph/types/graph.h"

#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/string_utils.h>

namespace hgraph
{

    TimeSeriesValueOutput::TimeSeriesValueOutput(const node_ptr &parent, const std::type_info &tp)
        : _ts(static_cast<Notifiable *>(const_cast<Node *>(parent.get())), tp) {}

    TimeSeriesValueOutput::TimeSeriesValueOutput(const TimeSeriesType::ptr &parent, const std::type_info &tp)
        : _ts(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(parent.get())), tp) {}

    node_ptr TimeSeriesValueOutput::owning_node() {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return node_ptr{node};
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->owning_node(); }
        return nullptr;
    }

    node_ptr TimeSeriesValueOutput::owning_node() const { return const_cast<TimeSeriesValueOutput *>(this)->owning_node(); }

    graph_ptr TimeSeriesValueOutput::owning_graph() {
        auto node{owning_node()};
        if (node != nullptr) return node->graph();
        return nullptr;
    }

    graph_ptr TimeSeriesValueOutput::owning_graph() const { return const_cast<TimeSeriesValueOutput *>(this)->owning_graph(); }

    void TimeSeriesValueOutput::re_parent(const node_ptr &node) {
        // This effectively changes the notification route for this output or input.
        _ts.set_parent(static_cast<Notifiable *>(const_cast<Node *>(node.get())));
    }

    void TimeSeriesValueOutput::re_parent(const TimeSeriesType::ptr &ts) {
        _ts.set_parent(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(ts.get())));
    }

    bool TimeSeriesValueOutput::has_owning_node() const {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return true;
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->has_owning_node(); }
        return false;
    }

    bool TimeSeriesValueOutput::is_reference() const { return false; }

    bool TimeSeriesValueOutput::has_reference() const { return false; }

    void TimeSeriesValueOutput::reset_parent_or_node() { _ts.set_parent(nullptr); }

    nb::object TimeSeriesValueOutput::py_value() const {
        if (auto &av = _ts.value(); av.has_value()) {
            // Check if it's stored as nb::object
            if (auto *obj = av.get_if<nb::object>()) { return *obj; }
            // Otherwise try to cast specific types
            if (auto *b = av.get_if<bool>()) return nb::cast(*b);
            if (auto *i = av.get_if<int64_t>()) return nb::cast(*i);
            if (auto *d = av.get_if<double>()) return nb::cast(*d);
            if (auto *dt = av.get_if<engine_date_t>()) return nb::cast(*dt);
            if (auto *t = av.get_if<engine_time_t>()) return nb::cast(*t);
            if (auto *td = av.get_if<engine_time_delta_t>()) return nb::cast(*td);
        }
        return nb::none();
    }

    nb::object TimeSeriesValueOutput::py_delta_value() const { return py_value(); }

    void TimeSeriesValueOutput::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }

        AnyValue<>  any;
        const auto &type = _ts.value_type();

        if (type == typeid(bool)) {
            any.emplace<bool>(cast<bool>(value));
        } else if (type == typeid(int64_t)) {
            any.emplace<int64_t>(cast<int64_t>(value));
        } else if (type == typeid(double)) {
            any.emplace<double>(cast<double>(value));
        } else if (type == typeid(engine_date_t)) {
            any.emplace<engine_date_t>(cast<engine_date_t>(value));
        } else if (type == typeid(engine_time_t)) {
            any.emplace<engine_time_t>(cast<engine_time_t>(value));
        } else if (type == typeid(engine_time_delta_t)) {
            any.emplace<engine_time_delta_t>(cast<engine_time_delta_t>(value));
        } else {
            any.emplace<nb::object>(value);
        }

        _ts.set_value(std::move(any));
    }

    bool TimeSeriesValueOutput::can_apply_result(nb::object value) { return !_ts.modified(); }

    void TimeSeriesValueOutput::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    void TimeSeriesValueOutput::mark_invalid() { _ts.invalidate(); }

    void TimeSeriesValueOutput::mark_modified() {
        // This can be used to mark an output as being modified without actually being modified.
        // This feels like something that needs to go away (i.e. private / implementation detail)
        if (!_ts.modified()) {
            // Re-apply the last value, this will force the correct notification behaviour.
            auto v{_ts.value()};
            _ts.set_value(v);
        }
    }

    void TimeSeriesValueOutput::mark_child_modified(TimeSeriesOutput &, engine_time_t modified_time) {
        // This has no children
        throw std::runtime_error("mark_child_modified() unavailable: no children");
    }

    bool TimeSeriesValueOutput::modified() const { return _ts.modified(); }

    bool TimeSeriesValueOutput::valid() const { return _ts.valid(); }

    bool TimeSeriesValueOutput::all_valid() const {
        // The TS[] does not have any children so all_valid is the same as valid.
        return _ts.valid();
    }

    engine_time_t TimeSeriesValueOutput::last_modified_time() const { return _ts.last_modified_time(); }

    TimeSeriesOutput::ptr TimeSeriesValueOutput::parent_output() const {
        return const_cast<TimeSeriesValueOutput *>(this)->parent_output();
    }

    TimeSeriesOutput::ptr TimeSeriesValueOutput::parent_output() { return dynamic_cast<TimeSeriesOutput *>(_ts.parent()); }

    bool TimeSeriesValueOutput::has_parent_output() const { return dynamic_cast<TimeSeriesOutput *>(_ts.parent()) != nullptr; }

    void TimeSeriesValueOutput::subscribe(Notifiable *node) {
        // The only subscribers should be from the bound TSInput, this does not manage
        // Subscriptions this way
        // This is probably another method that can go away
        _ts.subscribe(node);
    }

    void TimeSeriesValueOutput::un_subscribe(Notifiable *node) {
        // The only subscribers should be from the bound TSInput, this does not manage
        // Subscriptions this way
        // This is probably another method that can go away
        _ts.unsubscribe(node);
    }

    void TimeSeriesValueOutput::builder_release_cleanup() {
        // We could just invalidate but that may cause cascading cleanup calls.
    }

    void TimeSeriesValueOutput::clear() { _ts.reset(); }

    void TimeSeriesValueOutput::invalidate() { _ts.invalidate(); }

    void TimeSeriesValueOutput::mark_modified(engine_time_t modified_time) {
        // This was a performance enhancement to reduce the need to fetch the engine_time.
        // So will just delegate to mark modified without the time for now.
        // I would prefer to see if we can improve the retrieval of the time. We can always put
        // this back if necessary.
        mark_modified();
    }

    void TimeSeriesValueOutput::notify(engine_time_t) {
        // Since this is always going to be a leaf, it is not going to have it's notify
        // called (since we are an output).
        throw std::runtime_error("notify() unavailable: the TS output is a leaf and not part of a notify cycle");
    }

    void TimeSeriesValueOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_t = dynamic_cast<const TimeSeriesValueOutput &>(output);
        _ts.set_value(output_t._ts.value());
    }

    inline void TimeSeriesValueOutput::copy_from_input(const TimeSeriesInput &input) {
        const auto &input_t = dynamic_cast<const TimeSeriesValueInput &>(input);
        _ts.set_value(input_t.ts().value());
    }

    bool TimeSeriesValueOutput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueOutput *>(other) != nullptr;
    }

    TSOutput       &TimeSeriesValueOutput::ts() { return _ts; }
    const TSOutput &TimeSeriesValueOutput::ts() const { return _ts; }

    TimeSeriesValueInput::TimeSeriesValueInput(const node_ptr &parent, const std::type_info &tp)
        : _ts(static_cast<Notifiable *>(const_cast<Node *>(parent.get())), tp) {}

    TimeSeriesValueInput::TimeSeriesValueInput(const TimeSeriesType::ptr &parent, const std::type_info &tp)
        : _ts(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(parent.get())), tp) {}

    node_ptr TimeSeriesValueInput::owning_node() {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return node_ptr(node);
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->owning_node(); }
        return {};
    }

    node_ptr TimeSeriesValueInput::owning_node() const { return const_cast<TimeSeriesValueInput *>(this)->owning_node(); }

    graph_ptr TimeSeriesValueInput::owning_graph() {
        auto node{owning_node()};
        if (node != nullptr) return node->graph();
        return {};
    }

    graph_ptr TimeSeriesValueInput::owning_graph() const { return const_cast<TimeSeriesValueInput *>(this)->owning_graph(); }

    void TimeSeriesValueInput::re_parent(const node_ptr &node) {
        _ts.set_parent(static_cast<Notifiable *>(const_cast<Node *>(node.get())));
    }

    void TimeSeriesValueInput::re_parent(const TimeSeriesType::ptr &ts) {
        _ts.set_parent(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(ts.get())));
    }

    bool TimeSeriesValueInput::has_owning_node() const { return owning_node() != nullptr; }

    bool TimeSeriesValueInput::is_reference() const { return false; }

    bool TimeSeriesValueInput::has_reference() const { return false; }

    void TimeSeriesValueInput::reset_parent_or_node() {
        // This may be a bad idea
        _ts.set_parent(nullptr);
    }

    TimeSeriesInput::ptr TimeSeriesValueInput::parent_input() const { return dynamic_cast<TimeSeriesInput *>(_ts.parent()); }

    bool TimeSeriesValueInput::has_parent_input() const { return dynamic_cast<TimeSeriesInput *>(_ts.parent()) != nullptr; }

    bool TimeSeriesValueInput::bound() const {
        // This is asking if we have an output bound
        return _ts.bound();
    }

    bool TimeSeriesValueInput::has_peer() const {
        // For the simple TS, we have a peer always, but the logic is currently delegated
        // to bound. So to keep this consistent, we will return:
        return TimeSeriesValueInput::bound();
    }

    time_series_output_ptr TimeSeriesValueInput::output() const {
        // OK, don't know what will be asking for this, but for now we will raise to see,
        // This is another thing that needs to go away
        throw std::runtime_error("output() unavailable: no output");
        // return nullptr;
    }

    bool TimeSeriesValueInput::bind_output(time_series_output_ptr output_) {
        if (output_ == nullptr) return false;
        auto *ts_ref = dynamic_cast<TimeSeriesReferenceOutput *>(output_.get());
        if (ts_ref != nullptr) {
            // Since this is a TS instance, we will not experience the list
            // variation. This means we can just rely on standard binding
            _ts.bind_output(ts_ref->ts());
            return false;
        }
        auto *ts_out = dynamic_cast<TimeSeriesValueOutput *>(output_.get());
        if (!ts_out) return false;
        _ts.bind_output(ts_out->ts());
        return true;
    }

    void TimeSeriesValueInput::un_bind_output(bool) {
        // Reinitialize TSInput to a fresh, unbound state
        _ts.un_bind();
    }

    bool TimeSeriesValueInput::active() const { return _ts.active(); }

    void TimeSeriesValueInput::make_active() { _ts.make_active(); }

    void TimeSeriesValueInput::make_passive() { _ts.make_passive(); }

    bool TimeSeriesValueInput::has_output() const { return bound(); }

    void TimeSeriesValueInput::builder_release_cleanup() { _ts.un_bind(); }

    nb::object TimeSeriesValueInput::py_value() const {
        // TODO: Make this better
        if (auto &av = _ts.value(); av.has_value()) {
            // Check if it's stored as nb::object
            if (auto *obj = av.get_if<nb::object>()) { return *obj; }
            // Otherwise try to cast specific types
            if (auto *b = av.get_if<bool>()) return nb::cast(*b);
            if (auto *i = av.get_if<int64_t>()) return nb::cast(*i);
            if (auto *d = av.get_if<double>()) return nb::cast(*d);
            if (auto *dt = av.get_if<engine_date_t>()) return nb::cast(*dt);
            if (auto *t = av.get_if<engine_time_t>()) return nb::cast(*t);
            if (auto *td = av.get_if<engine_time_delta_t>()) return nb::cast(*td);
        }
        return nb::none();
    }

    nb::object TimeSeriesValueInput::py_delta_value() const { return py_value(); }

    bool TimeSeriesValueInput::modified() const { return _ts.modified(); }

    bool TimeSeriesValueInput::valid() const { return _ts.valid(); }

    bool TimeSeriesValueInput::all_valid() const { return _ts.valid(); }

    engine_time_t TimeSeriesValueInput::last_modified_time() const { return _ts.last_modified_time(); }

    time_series_reference_output_ptr TimeSeriesValueInput::reference_output() const {
        // The only usages of this that I can see are regarding printing (BackTrace) and in a nano-bind property exposure
        // So am not going to implement and am going to leave as null-ptr
        return {};
    }

    const TimeSeriesInput *TimeSeriesValueInput::get_input(size_t) const {
        throw std::runtime_error("get_input() no supported on TS");
    }

    TimeSeriesInput *TimeSeriesValueInput::get_input(size_t) { throw std::runtime_error("get_input() no supported on TS"); }

    bool TimeSeriesValueInput::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueInput *>(other) != nullptr;
    }

    TSInput       &TimeSeriesValueInput::ts() { return _ts; }
    const TSInput &TimeSeriesValueInput::ts() const { return _ts; }

    void TimeSeriesValueInput::notify(engine_time_t et) {
        // We pass in the parent of this node as the notifable object since there is
        // Nothing the wrapper needs to track. The only risk I can see at the moment is
        // if this was held by a TimeSeriesRefereceInput, the other place this is used
        // is in the TSD, but the check there is find the key that is change, this could be
        // a problem ...
        throw std::runtime_error("notify() unavailable: we directly notify parent");
    }

    void TimeSeriesValueInput::notify_parent(TimeSeriesInput *child, engine_time_t et) {
        // This should not happen as the notification is from a child to it's parent
        // This should be cleaned up as it's just another formation of notification and
        // only has specialization on REF, we should not be called by this as we have no children
        throw std::runtime_error("notify_parent() unavailable: no children");
    }

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesValueOutput, TimeSeriesOutput>(m, "TimeSeriesValueOutput");
        nb::class_<TimeSeriesValueInput, TimeSeriesInput>(m, "TimeSeriesValueInput");
    }
}  // namespace hgraph