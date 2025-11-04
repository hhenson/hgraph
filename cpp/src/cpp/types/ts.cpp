#include "hgraph/types/graph.h"

#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/util/string_utils.h>

namespace hgraph
{

    template <typename T>
    TimeSeriesValueOutput<T>::TimeSeriesValueOutput(const node_ptr &parent) : _ts(
        static_cast<Notifiable *>(const_cast<Node *>(parent.get())), typeid(T)) {}

    template <typename T>
    TimeSeriesValueOutput<T>::TimeSeriesValueOutput(const TimeSeriesType::ptr &parent)
        : _ts(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(parent.get())), typeid(T)) {}

    template <typename T> node_ptr TimeSeriesValueOutput<T>::owning_node() {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return node_ptr{node};
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->owning_node(); }
        return nullptr;
    }

    template <typename T> node_ptr TimeSeriesValueOutput<T>::owning_node() const {
        return const_cast<TimeSeriesValueOutput *>(this)->owning_node();
    }

    template <typename T> graph_ptr TimeSeriesValueOutput<T>::owning_graph() {
        auto node{owning_node()};
        if (node != nullptr) return node->graph();
        return nullptr;
    }

    template <typename T> graph_ptr TimeSeriesValueOutput<T>::owning_graph() const {
        return const_cast<TimeSeriesValueOutput *>(this)->owning_graph();
    }

    template <typename T> void TimeSeriesValueOutput<T>::re_parent(const node_ptr &node) {
        // This effectively changes the notification route for this output or input.
        _ts.set_parent(static_cast<Notifiable *>(const_cast<Node *>(node.get())));
    }

    template <typename T> void TimeSeriesValueOutput<T>::re_parent(const TimeSeriesType::ptr &ts) {
        _ts.set_parent(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(ts.get())));
    }

    template <typename T> bool TimeSeriesValueOutput<T>::has_owning_node() const {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return true;
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->has_owning_node(); }
        return false;
    }

    template <typename T> bool TimeSeriesValueOutput<T>::is_reference() const { return false; }

    template <typename T> bool TimeSeriesValueOutput<T>::has_reference() const { return false; }

    template <typename T> void TimeSeriesValueOutput<T>::reset_parent_or_node() { _ts.set_parent(nullptr); }

    template <typename T> nb::object TimeSeriesValueOutput<T>::py_value() const {
        if (auto &av = _ts.value(); av.has_value()) { return nb::cast(*av.template get_if<T>()); }
        return nb::none();
    }

    template <typename T> nb::object TimeSeriesValueOutput<T>::py_delta_value() const { return py_value(); }

    template <typename T> void TimeSeriesValueOutput<T>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        set_value(nb::cast<T>(value));
    }

    template <typename T> bool TimeSeriesValueOutput<T>::can_apply_result(nb::object value) { return !_ts.modified(); }

    template <typename T> void TimeSeriesValueOutput<T>::apply_result(nb::object value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    template <typename T> const T &TimeSeriesValueOutput<T>::value() const {
        const auto &av = _ts.value();
        const T    *pv = av.template get_if<T>();
        if (!pv) throw std::bad_cast();
        return *pv;
    }

    template <typename T> void TimeSeriesValueOutput<T>::set_value(const T &v) {
        AnyValue<> any;
        any.emplace<T>(v);
        _ts.set_value(any);
    }

    template <typename T> void TimeSeriesValueOutput<T>::set_value(T &&v) {
        AnyValue<> any;
        any.emplace<T>(std::move(v));
        _ts.set_value(std::move(any));
    }

    template <typename T> void TimeSeriesValueOutput<T>::mark_invalid() { _ts.invalidate(); }

    template <typename T> void TimeSeriesValueOutput<T>::mark_modified() {
        // This can be used to mark an output as being modified without actually being modified.
        // This feels like something that needs to go away (i.e. private / implementation detail)
        if (!_ts.modified()) {
            // Re-apply the last value, this will force the correct notification behaviour.
            auto v{_ts.value()};
            _ts.set_value(v);
        }
    }

    template <typename T> void TimeSeriesValueOutput<T>::mark_child_modified(TimeSeriesOutput &, engine_time_t modified_time) {
        // This has no children
        throw std::runtime_error("mark_child_modified() unavailable: no children");
    }

    template <typename T> bool TimeSeriesValueOutput<T>::modified() const { return _ts.modified(); }

    template <typename T> bool TimeSeriesValueOutput<T>::valid() const { return _ts.valid(); }

    template <typename T> bool TimeSeriesValueOutput<T>::all_valid() const {
        // The TS[] does not have any children so all_valid is the same as valid.
        return _ts.valid();
    }

    template <typename T> engine_time_t TimeSeriesValueOutput<T>::last_modified_time() const { return _ts.last_modified_time(); }

    template <typename T> TimeSeriesOutput::ptr TimeSeriesValueOutput<T>::parent_output() const {
        return const_cast<TimeSeriesValueOutput *>(this)->parent_output();
    }

    template <typename T> TimeSeriesOutput::ptr TimeSeriesValueOutput<T>::parent_output() {
        return dynamic_cast<TimeSeriesOutput *>(_ts.parent());
    }

    template <typename T> bool TimeSeriesValueOutput<T>::has_parent_output() const {
        return dynamic_cast<TimeSeriesOutput *>(_ts.parent()) != nullptr;
    }

    template <typename T> void TimeSeriesValueOutput<T>::subscribe(Notifiable *node) {
        // The only subscribers should be from the bound TSInput, this does not manage
        // Subscriptions this way
        // This is probably another method that can go away
        _ts.subscribe(node);
    }

    template <typename T> void TimeSeriesValueOutput<T>::un_subscribe(Notifiable *node) {
        // The only subscribers should be from the bound TSInput, this does not manage
        // Subscriptions this way
        // This is probably another method that can go away
        _ts.un_subscribe(node);
    }

    template <typename T> void TimeSeriesValueOutput<T>::builder_release_cleanup() {
        // We could just invalidate but that may cause cascading cleanup calls.
    }

    template <typename T> void TimeSeriesValueOutput<T>::clear() { _ts.reset(); }

    template <typename T> void TimeSeriesValueOutput<T>::invalidate() { _ts.invalidate(); }

    template <typename T> void TimeSeriesValueOutput<T>::mark_modified(engine_time_t modified_time) {
        // This was a performance enhancement to reduce the need to fetch the engine_time.
        // So will just delegate to mark modified without the time for now.
        // I would prefer to see if we can improve the retrieval of the time. We can always put
        // this back if necessary.
        mark_modified();
    }

    template <typename T> void TimeSeriesValueOutput<T>::notify(engine_time_t) {
        // Since this is always going to be a leaf, it is not going to have it's notify
        // called (since we are an output).
        throw std::runtime_error("notify() unavailable: the TS output is a leaf and not part of a notify cycle");
    }

    template <typename T> void TimeSeriesValueOutput<T>::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_t = dynamic_cast<const TimeSeriesValueOutput<T> &>(output);
        set_value(output_t.value());
    }

    template <typename T> inline void TimeSeriesValueOutput<T>::copy_from_input(const TimeSeriesInput &input) {
        const auto &input_t = dynamic_cast<const TimeSeriesValueInput<T> &>(input);
        set_value(input_t.value());
    }

    template <typename T> bool TimeSeriesValueOutput<T>::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueOutput<T> *>(other) != nullptr;
    }

    template <typename T> engine_time_t TimeSeriesValueOutput<T>::current_engine_time() const {
        return owning_graph()->evaluation_clock()->evaluation_time();
    }
    template <typename T> TSOutput       &TimeSeriesValueOutput<T>::ts() { return _ts; }
    template <typename T> const TSOutput &TimeSeriesValueOutput<T>::ts() const { return _ts; }

    template <typename T>
    TimeSeriesValueInput<T>::TimeSeriesValueInput(const node_ptr &parent) : _ts(static_cast<Notifiable *>(const_cast<Node *>(parent.get())), typeid(T)) {}

    template <typename T>
    TimeSeriesValueInput<T>::TimeSeriesValueInput(const TimeSeriesType::ptr &parent)
        : _ts(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(parent.get())), typeid(T)) {}

    template <typename T> node_ptr TimeSeriesValueInput<T>::owning_node() {
        auto parent{_ts.parent()};
        if (auto node{dynamic_cast<Node *>(parent)}; node != nullptr) return node_ptr(node);
        if (auto ts{dynamic_cast<TimeSeriesType *>(parent)}; ts != nullptr) { return ts->owning_node(); }
        return {};
    }

    template <typename T> node_ptr TimeSeriesValueInput<T>::owning_node() const {
        return const_cast<TimeSeriesValueInput *>(this)->owning_node();
    }

    template <typename T> graph_ptr TimeSeriesValueInput<T>::owning_graph() {
        auto node{owning_node()};
        if (node != nullptr) return node->graph();
        return {};
    }

    template <typename T> graph_ptr TimeSeriesValueInput<T>::owning_graph() const {
        return const_cast<TimeSeriesValueInput *>(this)->owning_graph();
    }

    template <typename T> void TimeSeriesValueInput<T>::re_parent(const node_ptr &node) {
        _ts.set_parent(static_cast<Notifiable *>(const_cast<Node *>(node.get())));
    }

    template <typename T> void TimeSeriesValueInput<T>::re_parent(const TimeSeriesType::ptr &ts) {
        _ts.set_parent(static_cast<Notifiable *>(const_cast<TimeSeriesType *>(ts.get())));
    }

    template <typename T> bool TimeSeriesValueInput<T>::has_owning_node() const { return owning_node() != nullptr; }

    template <typename T> bool TimeSeriesValueInput<T>::is_reference() const { return false; }

    template <typename T> bool TimeSeriesValueInput<T>::has_reference() const { return false; }

    template <typename T> void TimeSeriesValueInput<T>::reset_parent_or_node() {
        // This may be a bad idea
        _ts.set_parent(nullptr);
    }

    template <typename T> TimeSeriesInput::ptr TimeSeriesValueInput<T>::parent_input() const {
        return dynamic_cast<TimeSeriesInput *>(_ts.parent());
    }

    template <typename T> bool TimeSeriesValueInput<T>::has_parent_input() const {
        return dynamic_cast<TimeSeriesInput *>(_ts.parent()) != nullptr;
    }

    template <typename T> bool TimeSeriesValueInput<T>::bound() const {
        // This is asking if we have an output bound
        return _ts.bound();
    }

    template <typename T> bool TimeSeriesValueInput<T>::has_peer() const {
        // For the simple TS, we have a peer always, but the logic is currently delegated
        // to bound. So to keep this consistent, we will return:
        return TimeSeriesValueInput<T>::bound();
    }

    template <typename T> time_series_output_ptr TimeSeriesValueInput<T>::output() const {
        // OK, don't know what will be asking for this, but for now we will raise to see,
        // This is another thing that needs to go away
        throw std::runtime_error("output() unavailable: no output");
        // return nullptr;
    }

    template <typename T> bool TimeSeriesValueInput<T>::bind_output(time_series_output_ptr output_) {
        if (output_ == nullptr) return false;
        auto *ts_out = dynamic_cast<TimeSeriesValueOutput<T> *>(output_.get());
        if (!ts_out) return false;
        _ts.bind_output(ts_out->ts());
        return true;
    }

    template <typename T> void TimeSeriesValueInput<T>::un_bind_output(bool) {
        // Reinitialize TSInput to a fresh, unbound state
        _ts.un_bind();
    }

    template <typename T> bool TimeSeriesValueInput<T>::active() const { return _ts.active(); }

    template <typename T> void TimeSeriesValueInput<T>::make_active() { _ts.make_active(); }

    template <typename T> void TimeSeriesValueInput<T>::make_passive() { _ts.make_passive(); }

    template <typename T> bool TimeSeriesValueInput<T>::has_output() const { return bound(); }

    template <typename T> void TimeSeriesValueInput<T>::builder_release_cleanup() { _ts.un_bind(); }

    template <typename T> nb::object TimeSeriesValueInput<T>::py_value() const {
        if (auto &av = _ts.value(); av.has_value()) { return nb::cast(*av.template get_if<T>()); }
        return nb::none();
    }

    template <typename T> nb::object TimeSeriesValueInput<T>::py_delta_value() const { return py_value(); }

    template <typename T> bool TimeSeriesValueInput<T>::modified() const { return _ts.modified(); }

    template <typename T> bool TimeSeriesValueInput<T>::valid() const { return _ts.valid(); }

    template <typename T> bool TimeSeriesValueInput<T>::all_valid() const { return _ts.valid(); }

    template <typename T> engine_time_t TimeSeriesValueInput<T>::last_modified_time() const { return _ts.last_modified_time(); }

    template <typename T> time_series_reference_output_ptr TimeSeriesValueInput<T>::reference_output() const {
        // The only usages of this that I can see are regarding printing (BackTrace) and in a nano-bind property exposure
        // So am not going to implement and am going to leave as null-ptr
        return {};
    }

    template <typename T> const TimeSeriesInput *TimeSeriesValueInput<T>::get_input(size_t) const {
        throw std::runtime_error("get_input() no supported on TS");
    }

    template <typename T> TimeSeriesInput *TimeSeriesValueInput<T>::get_input(size_t) {
        throw std::runtime_error("get_input() no supported on TS");
    }

    template <typename T> const T &TimeSeriesValueInput<T>::value() const {
        const auto &av = _ts.value();
        const T    *pv = av.template get_if<T>();
        if (!pv) throw std::bad_cast();
        return *pv;
    }

    template <typename T> TimeSeriesValueOutput<T> &TimeSeriesValueInput<T>::value_output() {
        throw std::runtime_error("value_output() unavailable: no stored bound output");
    }

    template <typename T> const TimeSeriesValueOutput<T> &TimeSeriesValueInput<T>::value_output() const {
        throw std::runtime_error("value_output() unavailable: no stored bound output");
    }

    template <typename T> bool TimeSeriesValueInput<T>::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesValueInput<T> *>(other) != nullptr;
    }

    template <typename T> void TimeSeriesValueInput<T>::notify(engine_time_t et) {
        // We pass in the parent of this node as the notifable object since there is
        // Nothing the wrapper needs to track. The only risk I can see at the moment is
        // if this was held by a TimeSeriesRefereceInput, the other place this is used
        // is in the TSD, but the check there is find the key that is change, this could be
        // a problem ...
        throw std::runtime_error("notify() unavailable: we directly notify parent");
    }

    template <typename T> engine_time_t TimeSeriesValueInput<T>::current_engine_time() const {
        return owning_graph()->evaluation_clock()->evaluation_time();
    }

    template <typename T> void TimeSeriesValueInput<T>::notify_parent(TimeSeriesInput *child, engine_time_t et) {
        // This should not happen as the notification is from a child to it's parent
        // This should be cleaned up as it's just another formation of notification and
        // only has specialization on REF, we should not be called by this as we have no children
        throw std::runtime_error("notify_parent() unavailable: no children");
    }

    // Explicit template instantiations for known types
    template struct TimeSeriesValueInput<bool>;
    template struct TimeSeriesValueInput<int64_t>;
    template struct TimeSeriesValueInput<double>;
    template struct TimeSeriesValueInput<engine_date_t>;
    template struct TimeSeriesValueInput<engine_time_t>;
    template struct TimeSeriesValueInput<engine_time_delta_t>;
    template struct TimeSeriesValueInput<nb::object>;

    template struct TimeSeriesValueOutput<bool>;
    template struct TimeSeriesValueOutput<int64_t>;
    template struct TimeSeriesValueOutput<double>;
    template struct TimeSeriesValueOutput<engine_date_t>;
    template struct TimeSeriesValueOutput<engine_time_t>;
    template struct TimeSeriesValueOutput<engine_time_delta_t>;
    template struct TimeSeriesValueOutput<nb::object>;

    using TS_Bool          = TimeSeriesValueInput<bool>;
    using TS_Out_Bool      = TimeSeriesValueOutput<bool>;
    using TS_Int           = TimeSeriesValueInput<int64_t>;
    using TS_Out_Int       = TimeSeriesValueOutput<int64_t>;
    using TS_Float         = TimeSeriesValueInput<double>;
    using TS_Out_Float     = TimeSeriesValueOutput<double>;
    using TS_Date          = TimeSeriesValueInput<engine_date_t>;
    using TS_Out_Date      = TimeSeriesValueOutput<engine_date_t>;
    using TS_DateTime      = TimeSeriesValueInput<engine_time_t>;
    using TS_Out_DateTime  = TimeSeriesValueOutput<engine_time_t>;
    using TS_TimeDelta     = TimeSeriesValueInput<engine_time_delta_t>;
    using TS_Out_TimeDelta = TimeSeriesValueOutput<engine_time_delta_t>;
    using TS_Object        = TimeSeriesValueInput<nb::object>;
    using TS_Out_Object    = TimeSeriesValueOutput<nb::object>;

    void register_ts_with_nanobind(nb::module_ &m) {
        nb::class_<TS_Out_Bool, TimeSeriesOutput>(m, "TS_Out_Bool");
        nb::class_<TS_Bool, TimeSeriesInput>(m, "TS_Bool");
        nb::class_<TS_Out_Int, TimeSeriesOutput>(m, "TS_Out_Int");
        nb::class_<TS_Int, TimeSeriesInput>(m, "TS_Int");
        nb::class_<TS_Out_Float, TimeSeriesOutput>(m, "TS_Out_Float");
        nb::class_<TS_Float, TimeSeriesInput>(m, "TS_Float");
        nb::class_<TS_Out_Date, TimeSeriesOutput>(m, "TS_Out_Date");
        nb::class_<TS_Date, TimeSeriesInput>(m, "TS_Date");
        nb::class_<TS_Out_DateTime, TimeSeriesOutput>(m, "TS_Out_DateTime");
        nb::class_<TS_DateTime, TimeSeriesInput>(m, "TS_DateTime");
        nb::class_<TS_Out_TimeDelta, TimeSeriesOutput>(m, "TS_Out_TimeDelta");
        nb::class_<TS_TimeDelta, TimeSeriesInput>(m, "TS_TimeDelta");
        nb::class_<TS_Out_Object, TimeSeriesOutput>(m, "TS_Out_Object");
        nb::class_<TS_Object, TimeSeriesInput>(m, "TS_Object");
    }
}  // namespace hgraph