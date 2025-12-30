#include "hgraph/types/node.h"

#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tss.h>

namespace hgraph
{
    template <typename T>
    template <typename U>
        requires(!std::is_same_v<U, nb::object>)
    SetDelta_T<T>::SetDelta_T(collection_type added, collection_type removed)
        : _added(std::move(added)), _removed(std::move(removed)) {}

    template <typename T>
    template <typename U>
        requires(std::is_same_v<U, nb::object>)
    SetDelta_T<T>::SetDelta_T(collection_type added, collection_type removed, nb::object tp)
        : _added(std::move(added)), _removed(std::move(removed)), _tp(std::move(tp)) {}

    template <typename T> const typename SetDelta_T<T>::collection_type &SetDelta_T<T>::added() const { return _added; }

    template <typename T> const typename SetDelta_T<T>::collection_type &SetDelta_T<T>::removed() const { return _removed; }

    template <typename T> bool SetDelta_T<T>::operator==(const SetDelta_T<T> &other) const {
        auto added{_added == other.added()};
        auto removed{_removed == other.removed()};
        return added && removed;
    }

    template <typename T> size_t SetDelta_T<T>::hash() const {
        size_t seed = 0;
        for (const auto &item : _added) { seed ^= std::hash<T>{}(item) + 0x9e3779b9 + (seed << 6) + (seed >> 2); }
        for (const auto &item : _removed) { seed ^= std::hash<T>{}(item) + 0x9e3779b9 + (seed << 6) + (seed >> 2); }
        return seed;
    }

    template <typename T> SetDelta_T<T> SetDelta_T<T>::operator+(const SetDelta_T<T> &other) const {
        collection_type added{};
        added.insert(_added.begin(), _added.end());
        for (auto it = other._removed.begin(); it != other._removed.end(); ++it) added.erase(*it);
        for (auto it = other._added.begin(); it != other._added.end(); ++it) added.insert(*it);

        collection_type removed{};
        removed.insert(other._removed.begin(), other._removed.end());
        for (auto it = _added.begin(); it != _added.end(); ++it) removed.erase(*it);

        collection_type removed2{};
        removed2.insert(_removed.begin(), _removed.end());
        for (auto it = other._added.begin(); it != other._added.end(); ++it) removed2.erase(*it);
        for (auto it = removed2.begin(); it != removed2.end(); ++it) removed.insert(*it);

        if constexpr (std::is_same_v<T, nb::object>) {
            return SetDelta_T<nb::object>(std::move(added), std::move(removed), _tp);
        } else {
            return SetDelta_T<T>(std::move(added), std::move(removed));
        }
    }

    template <typename T> nb::object SetDelta_T<T>::py_type() const {
        if constexpr (std::is_same_v<T, bool>) {
            return nb::borrow(nb::cast(true).type());
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return nb::borrow(nb::cast((int64_t)1).type());
        } else if constexpr (std::is_same_v<T, double>) {
            return nb::borrow(nb::cast((double)1.0).type());
        } else if constexpr (std::is_same_v<T, engine_date_t>) {
            return nb::module_::import_("datetime").attr("date");
        } else if constexpr (std::is_same_v<T, engine_time_t>) {
            return nb::module_::import_("datetime").attr("datetime");
        } else if constexpr (std::is_same_v<T, engine_time_delta_t>) {
            return nb::module_::import_("datetime").attr("timedelta");
        } else if constexpr (std::is_same_v<T, nb::object>) {
            return _tp;
        } else {
            throw std::runtime_error("Unknown tp");
        }
    }

    template struct SetDelta_T<bool>;
    template struct SetDelta_T<int64_t>;
    template struct SetDelta_T<double>;
    template struct SetDelta_T<engine_date_t>;
    template struct SetDelta_T<engine_time_t>;
    template struct SetDelta_T<engine_time_delta_t>;
    template struct SetDelta_T<nb::object>;

    template <typename T> bool eq(const SetDelta_T<T> &self, const nb::handle &other) {
        // Support direct comparison against another SetDelta instance
        if (nb::isinstance<SetDelta_T<T>>(other)) {
            const auto &rhs = nb::cast<const SetDelta_T<T> &>(other);
            return self == rhs;
        }

        // Fallback: comparison against an iterable of items/Removed wrappers (Python semantics)
        if (!nb::isinstance<nb::iterable>(other)) { return false; }
        auto added   = self.added();
        auto removed = self.removed();
        if (nb::len(other) != added.size() + removed.size()) { return false; }
        auto REMOVED = get_removed();
        for (auto i : nb::iter(other)) {
            if (nb::isinstance(i, REMOVED)) {
                if (!removed.contains(nb::cast<T>(i.attr("item")))) return false;
            } else {
                if (!added.contains(nb::cast<T>(i))) return false;
            }
        }
        return true;
    }

    template <typename T> void _register_with_nanobind(nb::module_ &m, const char *name) {
        using SetDelta = SetDelta_T<T>;

        // Bind by-value constructors to match the actual C++ signatures and avoid dangling references
        auto set_delta_tp = nb::class_<SetDelta>(m, name);
        if constexpr (std::is_same_v<T, nb::object>) {
            set_delta_tp.def(nb::init<std::unordered_set<nb::object>, std::unordered_set<nb::object>, nb::object>(), "added"_a,
                             "removed"_a, "tp"_a);
        } else {
            set_delta_tp.def(nb::init<std::unordered_set<T>, std::unordered_set<T>>(), "added"_a, "removed"_a);
        }
        set_delta_tp.def_prop_ro("added", [](const SetDelta &self) { return nb::frozenset(nb::cast(self.added())); })
            .def_prop_ro("removed", [](const SetDelta &self) { return nb::frozenset(nb::cast(self.removed())); })
            .def_prop_ro("tp", &SetDelta::py_type)
            .def(
                "__str__",
                [](const SetDelta &self) { return nb::str("SetDelta(added={}, removed={})").format(self.added(), self.removed()); })
            .def(
                "__repr__",
                [](const SetDelta &self) {
                    return nb::str("SetDelta[{}](added={}, removed={})").format(self.py_type(), self.added(), self.removed());
                })
            .def("__eq__", &eq<T>)
            .def("__hash__", &SetDelta::hash)
            .def("__add__", &SetDelta::operator+);
    }

    void register_set_delta_with_nanobind(nb::module_ &m) {
        _register_with_nanobind<bool>(m, "SetDelta_bool");
        _register_with_nanobind<int64_t>(m, "SetDelta_int");
        _register_with_nanobind<double>(m, "SetDelta_float");
        _register_with_nanobind<engine_date_t>(m, "SetDelta_date");
        _register_with_nanobind<engine_time_t>(m, "SetDelta_date_time");
        _register_with_nanobind<engine_time_delta_t>(m, "SetDelta_time_delta");
        _register_with_nanobind<nb::object>(m, "SetDelta_object");
    }

    TimeSeriesSetOutput::TimeSeriesSetOutput(const node_ptr &parent)
        : TimeSeriesSet<BaseTimeSeriesOutput>(parent), _is_empty_ref_output{std::dynamic_pointer_cast<TimeSeriesValueOutput>(
                                                           TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()).make_instance(this))} {}

    TimeSeriesSetOutput::TimeSeriesSetOutput(time_series_output_ptr parent)
        : TimeSeriesSet<BaseTimeSeriesOutput>(parent), _is_empty_ref_output{std::dynamic_pointer_cast<TimeSeriesValueOutput>(
                                                           TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()).make_instance(this))} {}

    time_series_value_output_s_ptr &TimeSeriesSetOutput::is_empty_output() {
        if (!_is_empty_ref_output->valid()) {
            // Set the bool value via Python interop
            _is_empty_ref_output->py_set_value(nb::cast(empty()));
        }
        return _is_empty_ref_output;
    }

    void TimeSeriesSetOutput::invalidate() {
        clear();
        _reset_last_modified_time();
    }

    TimeSeriesSetOutput &TimeSeriesSetInput::set_output() const { return dynamic_cast<TimeSeriesSetOutput &>(*output()); }

    template <typename T_Key>
    TimeSeriesSetOutput_T<T_Key>::TimeSeriesSetOutput_T(const node_ptr &parent)
        : TimeSeriesSetOutput(parent),
          _contains_ref_outputs{this,
                                // Note: naked new is correct here - output_builder_s_ptr is nb::ref<OutputBuilder>
                                // which uses intrusive reference counting and accepts raw pointers from new
                                new TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()),
                                [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const element_type &key) {
                                    auto& ts_val = dynamic_cast<TimeSeriesValueOutput&>(ref);
                                    auto& ts_set = dynamic_cast<const TimeSeriesSetOutput_T<element_type>&>(ts);
                                    ts_val.py_set_value(nb::cast(ts_set.contains(key)));
                                },
                                {}} {}

    template <typename T_Key>
    TimeSeriesSetOutput_T<T_Key>::TimeSeriesSetOutput_T(time_series_output_ptr parent)
        : TimeSeriesSetOutput(parent),
          _contains_ref_outputs{this,
                                // Note: naked new is correct here - output_builder_s_ptr is nb::ref<OutputBuilder>
                                // which uses intrusive reference counting and accepts raw pointers from new
                                new TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()),
                                [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const element_type &key) {
                                    auto& ts_val = dynamic_cast<TimeSeriesValueOutput&>(ref);
                                    auto& ts_set = dynamic_cast<const TimeSeriesSetOutput_T<element_type>&>(ts);
                                    ts_val.py_set_value(nb::cast(ts_set.contains(key)));
                                },
                                {}} {}

    template <typename T_Key> nb::object TimeSeriesSetOutput_T<T_Key>::py_value() const {
        if (!_py_value.is_valid() || _py_value.is_none()) {
            nb::set v{};
            for (const T_Key &item : _value) { v.add(nb::cast(item)); }
            _py_value = nb::frozenset(v);
        }
        return _py_value;
    }

    template <typename T_Key>
    const typename TimeSeriesSetOutput_T<T_Key>::collection_type &TimeSeriesSetOutput_T<T_Key>::value() const {
        return _value;
    }

    // This form of the set_value requires:
    // added is a disjoint set versus the current value
    // removed is sub-set of the current value
    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::set_value(collection_type added, collection_type removed) {
        for (const auto &item : removed) { _value.erase(item); }
        for (const auto &item : added) { _value.emplace(item); }
        _added   = std::move(added);
        _removed = std::move(removed);
        _post_modify();
    }

    // This is to deal with an object value, there are two scenarios, one is getting a setdelta,
    // the other is an iterable.
    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::set_value(const nb::object &value) {
        if (nb::isinstance<SetDelta_T<T_Key>>(value)) {
            set_value(nb::cast<SetDelta_T<T_Key>>(value));
        } else if (nb::isinstance<nb::frozenset>(value)) {
            auto            v = nb::frozenset(value);
            collection_type added;
            collection_type to_remove;
            for (const auto &e : nb::iter(v)) {
                auto k = nb::cast<T_Key>(e);
                if (!_value.contains(k)) { added.insert(k); }
            }
            for (const auto &k : _value) {
                if (!v.contains(k)) { to_remove.insert(k); }
            }
            set_value(std::move(added), std::move(to_remove));
        } else {
            auto removed = get_removed();
            auto v       = nb::iter(value);

            collection_type added;
            collection_type to_remove;

            for (const auto &r : v) {
                if (!nb::isinstance(r, removed)) {
                    auto k = nb::cast<T_Key>(r);
                    if (!_value.contains(k)) { added.insert(k); }
                } else {
                    auto item = nb::cast<T_Key>(r.attr("item"));
                    if (_value.contains(item)) {
                        if (added.contains(item)) { throw std::runtime_error("Cannot remove and add the same element"); }
                        to_remove.insert(item);
                    }
                }
            }
            set_value(std::move(added), std::move(to_remove));
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::set_value(const SetDelta_T<T_Key> &delta) {
        collection_type added;
        collection_type removed;
        added.reserve(delta.added().size());
        removed.reserve(delta.removed().size());
        for (const auto &item : delta.added()) {
            if (!_value.contains(item)) { added.insert(item); }
        }
        for (const auto &item : delta.removed()) {
            if (_value.contains(item)) { removed.insert(item); }
        }
        set_value(std::move(added), std::move(removed));
    }

    template <typename T_Key> nb::object TimeSeriesSetOutput_T<T_Key>::py_delta_value() const {
        if (!modified()) {
            // For consistency with Python, this should return empty if requested and not modified.
            collection_type empty_set;
            return nb::cast(make_set_delta<T_Key>(empty_set, empty_set));
        }
        return nb::cast(make_set_delta<T_Key>(_added, _removed));
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
        } else {
            set_value(value);
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::apply_result(const nb::object& value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::mark_modified(engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            // Make sure we only do this once
            TimeSeriesSetOutput::mark_modified(modified_time);
            if (has_parent_or_node()) {
                auto weak_self = weak_from_this();
                owning_node()->graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                    if (auto self = weak_self.lock()) {
                        static_cast<TimeSeriesSetOutput_T *>(self.get())->_reset();
                    }
                });
            }
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_add(const element_type &item) {
        _value.emplace(item);
        _added.emplace(item);
        _removed.erase(item);
        // Reset Python-side caches so py_added/py_removed reflect current tick modifications
        _py_added.reset();
        _py_removed.reset();
        _py_value.reset();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_remove(const element_type &item) {
        _value.erase(item);
        _removed.emplace(item);
        // Reset Python-side caches so py_added/py_removed reflect current tick modifications
        _py_added.reset();
        _py_removed.reset();
        _py_value.reset();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_post_modify() {
        // We get here after setting the value, so we can overload this to reset the caches.
        _py_value.reset();
        _py_removed.reset();
        _py_added.reset();

        bool has_changes{_added.size() > 0 || _removed.size() > 0};
        bool needs_validation{!valid()};
        bool is_current_cycle = (last_modified_time() < owning_graph()->evaluation_time());
        if ((has_changes || needs_validation) && is_current_cycle) {
            mark_modified();
            if (_added.size() > 0 && is_empty_output()->valid() && is_empty_output()->value().template as<bool>()) {
                is_empty_output()->py_set_value(nb::cast(false));
            } else if (_removed.size() > 0 && empty()) {
                is_empty_output()->py_set_value(nb::cast(true));
            }
            _contains_ref_outputs.update_all(_added.begin(), _added.end());
            _contains_ref_outputs.update_all(_removed.begin(), _removed.end());
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_reset() {
        _added.clear();
        _removed.clear();
        _py_added.reset();
        _py_removed.reset();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::_reset_value() {
        _reset();
        auto a{collection_type{}};
        std::swap(a, _value);
        auto b{collection_type{}};
        std::swap(b, _added);
        auto c{collection_type{}};
        std::swap(c, _removed);
    }

    template <typename T> nb::object TimeSeriesSetInput_T<T>::py_value() const { return nb::cast(value()); }

    template <typename T> nb::object TimeSeriesSetInput_T<T>::py_delta_value() const { return nb::cast(delta_value()); }

    template <typename T> size_t TimeSeriesSetInput_T<T>::size() const { return has_output() ? set_output_t().size() : 0; }

    template <typename T> bool TimeSeriesSetInput_T<T>::empty() const { return value().empty(); }

    template <typename T> const typename TimeSeriesSetInput_T<T>::collection_type &TimeSeriesSetInput_T<T>::value() const {
        return bound() ? set_output_t().value() : _empty;
    }

    template <typename T> typename TimeSeriesSetInput_T<T>::set_delta TimeSeriesSetInput_T<T>::delta_value() const {
        return make_set_delta<element_type>(added(), removed());
    }

    template <typename T> bool TimeSeriesSetInput_T<T>::contains(const element_type &item) const {
        return has_output() ? set_output_t().contains(item) : false;
    }

    template <typename T> const typename TimeSeriesSetInput_T<T>::collection_type &TimeSeriesSetInput_T<T>::values() const {
        return value();
    }

    template <typename T> const typename TimeSeriesSetInput_T<T>::collection_type &TimeSeriesSetInput_T<T>::added() const {
        // The added results are cached, we will clear out the results at the end of the cycle.
        if (!_added.empty()) { return _added; }

        // If we have a previous output, then we need to do some work to compute the effect _added
        if (has_prev_output()) {
            // Get all elements from current values
            auto &prev         = prev_output_t().value();
            auto &prev_added   = prev_output_t().added();
            auto &prev_removed = prev_output_t().removed();
            for (const auto &item : values()) {
                // Only add if not in previous state
                // (prev values + removed - added)
                bool was_in_prev = (prev.contains(item) || prev_removed.contains(item)) && !prev_added.contains(item);
                if (!was_in_prev) { _added.insert(item); }
            }
            if (!_added.empty()) { _add_reset_prev(); }
            return _added;
        }

        if (has_output()) { return sampled() ? values() : set_output_t().added(); }

        _added.clear();
        return _added;
    }

    template <typename T> bool TimeSeriesSetInput_T<T>::was_added(const element_type &item) const {
        if (has_prev_output()) { return set_output_t().was_added(item) && !prev_output_t().contains(item); }
        if (sampled()) { return contains(item); }
        return set_output_t().was_added(item);
    }

    template <typename T> const typename TimeSeriesSetInput_T<T>::collection_type &TimeSeriesSetInput_T<T>::removed() const {
        if (!_removed.empty()) { return _removed; }

        if (has_prev_output()) {
            auto &prev         = prev_output_t().value();
            auto &prev_added   = prev_output_t().added();
            auto &prev_removed = prev_output_t().removed();
            auto &value        = values();
            // Calculate removed elements as:
            // (previous_values union previous_removed) minus previous_added minus current_values
            collection_type prev_state;
            prev_state.insert(prev.begin(), prev.end());
            prev_state.insert(prev_removed.begin(), prev_removed.end());
            for (const auto &item : prev_added) { prev_state.erase(item); }
            for (const auto &item : prev_state) {
                if (!value.contains(item)) { _removed.insert(item); }
            }
            if (!_removed.empty()) { _add_reset_prev(); }
            return _removed;
        }

        if (has_output()) { return set_output_t().removed(); }

        return _empty;
    }

    template <typename T> bool TimeSeriesSetInput_T<T>::was_removed(const element_type &item) const {
        if (has_prev_output()) { return prev_output_t().contains(item) && !contains(item); }
        if (sampled()) { return false; }
        return has_output() ? set_output_t().was_removed(item) : false;
    }

    template <typename T> bool TimeSeriesSetInput_T<T>::is_same_type(const TimeSeriesType *other) const {
        return dynamic_cast<const TimeSeriesSetInput_T<T> *>(other) != nullptr;
    }

    template <typename T>
    const TimeSeriesSetOutput_T<typename TimeSeriesSetInput_T<T>::element_type> &TimeSeriesSetInput_T<T>::prev_output_t() const {
        return reinterpret_cast<const TimeSeriesSetOutput_T<element_type> &>(prev_output());
    }

    template <typename T>
    const TimeSeriesSetOutput_T<typename TimeSeriesSetInput_T<T>::element_type> &TimeSeriesSetInput_T<T>::set_output_t() const {
        return reinterpret_cast<const TimeSeriesSetOutput_T<element_type> &>(*output());
    }

    template <typename T> void TimeSeriesSetInput_T<T>::reset_prev() {
        TimeSeriesSetInput::reset_prev();
        _added.clear();
        _removed.clear();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::clear() {
        _removed.clear();
        _removed.reserve(_value.size());
        for (const auto &item : _value) {
            if (!_added.contains(item)) { _removed.emplace(item); }
        }
        _added.clear();
        _contains_ref_outputs.update_all(_value.begin(), _value.end());
        _value.clear();
        is_empty_output()->py_set_value(nb::cast(true));
        // Clear the caches
        _py_value.reset();
        _py_added.reset();
        _py_removed.reset();
        mark_modified();
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::copy_from_output(const TimeSeriesOutput &output) {
        auto &output_obj = dynamic_cast<const TimeSeriesSetOutput_T<T_Key> &>(output);

        _added.clear();
        _removed.clear();

        // Make a copy of _value to avoid issues if we're copying from ourselves
        collection_type old_value = _value;

        // Calculate added elements (elements in output but not in current value)
        for (const auto &item : output_obj._value) {
            if (!old_value.contains(item)) { _added.insert(item); }
        }

        // Calculate removed elements (elements in current value but not in output)
        for (const auto &item : old_value) {
            if (!output_obj._value.contains(item)) { _removed.insert(item); }
        }

        if (_added.size() > 0 || _removed.size() > 0 || !valid()) {
            _value = output_obj._value;
            // Reset Python-side caches so py_added/py_removed reflect current tick modifications
            _py_added.reset();
            _py_removed.reset();
            _py_value.reset();
            is_empty_output()->py_set_value(nb::cast(empty()));
            _contains_ref_outputs.update_all(_added.begin(), _added.end());
            _contains_ref_outputs.update_all(_removed.begin(), _removed.end());
            mark_modified();
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::copy_from_input(const TimeSeriesInput &input) {
        auto &input_obj = dynamic_cast<const TimeSeriesSetInput_T<T_Key> &>(input);

        _added.clear();
        _removed.clear();

        // Calculate added elements (elements in input but not in current value)
        const auto &input_value = input_obj.value();

        // Make a copy of _value to avoid issues if input_value references _value
        collection_type old_value = _value;

        for (const auto &item : input_value) {
            if (!old_value.contains(item)) { _added.insert(item); }
        }

        // Calculate removed elements (elements in current value but not in input)
        for (const auto &item : old_value) {
            if (!input_value.contains(item)) { _removed.insert(item); }
        }

        if (_added.size() > 0 || _removed.size() > 0 || !valid()) {
            _value = input_value;
            // Reset Python-side caches so py_added/py_removed reflect current tick modifications
            _py_added.reset();
            _py_removed.reset();
            _py_value.reset();
            is_empty_output()->py_set_value(nb::cast(empty()));
            _contains_ref_outputs.update_all(_added.begin(), _added.end());
            _contains_ref_outputs.update_all(_removed.begin(), _removed.end());
            mark_modified();
        }
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::contains(const element_type &item) const {
        return _value.contains(item);
    }

    template <typename T_Key> size_t TimeSeriesSetOutput_T<T_Key>::size() const { return _value.size(); }

    template <typename T_Key>
    const typename TimeSeriesSetOutput_T<T_Key>::collection_type &TimeSeriesSetOutput_T<T_Key>::added() const {
        return _added;
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::has_added() const { return !_added.empty(); }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::was_added(const element_type &item) const {
        return _added.contains(item);
    }

    template <typename T_Key>
    const typename TimeSeriesSetOutput_T<T_Key>::collection_type &TimeSeriesSetOutput_T<T_Key>::removed() const {
        return _removed;
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::has_removed() const { return !_removed.empty(); }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::was_removed(const element_type &item) const {
        return _removed.contains(item);
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::remove(const element_type &key) {
        if (contains(key)) {
            bool was_added = false;
            if (_added.contains(key)) {
                _added.erase(key);
                was_added = true;
            }

            if (was_added) {
                _value.erase(key);
            } else {
                _remove(key);
            }

            _contains_ref_outputs.update(key);

            if (empty()) { is_empty_output()->py_set_value(nb::cast(true)); }

            mark_modified();
        }
    }

    template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::add(const element_type &key) {
        if (!contains(key)) {
            if (empty()) { is_empty_output()->py_set_value(nb::cast(false)); }
            _add(key);
            _contains_ref_outputs.update(key);
            mark_modified();
        }
    }

    template <typename T_Key> bool TimeSeriesSetOutput_T<T_Key>::empty() const { return _value.empty(); }

    template <typename T_Key>
    time_series_value_output_s_ptr TimeSeriesSetOutput_T<T_Key>::get_contains_output(const nb::object &item,
                                                                                       const nb::object &requester) {
        return std::dynamic_pointer_cast<TimeSeriesValueOutput>(
            _contains_ref_outputs.create_or_increment(nb::cast<element_type>(item), static_cast<void *>(requester.ptr())));
    }

    template <typename T_Key>
    void TimeSeriesSetOutput_T<T_Key>::release_contains_output(const nb::object &item, const nb::object &requester) {
        _contains_ref_outputs.release(nb::cast<element_type>(item), static_cast<void *>(requester.ptr()));
    }

    // template <typename T_Key> void TimeSeriesSetOutput_T<T_Key>::post_modify() { _post_modify(); }

    // nb::object TimeSeriesSetInput::py_added() const {
    //     if (has_prev_output()) {
    //         // Get current values as a set
    //         auto current_values = nb::set(py_values());
    //         // Get previous state (old values + removed - added)
    //         auto prev_values  = nb::set(prev_output().py_values());
    //         auto prev_removed = nb::set(prev_output().py_removed());
    //         auto prev_added   = nb::set(prev_output().py_added());
    //         auto old_state    = (prev_values | prev_removed) - prev_added;
    //         // Added items are current values minus old_state
    //         return current_values - old_state;
    //     } else {
    //         return sampled() ? py_values() : set_output().py_added();
    //     }
    // }
    //
    // nb::object TimeSeriesSetInput::py_removed() const {
    //     if (has_prev_output()) {
    //         auto prev_values    = nb::set(prev_output().py_values());
    //         auto prev_removed   = nb::set(prev_output().py_removed());
    //         auto prev_added     = nb::set(prev_output().py_added());
    //         auto current_values = nb::set(py_values());
    //
    //         return ((prev_values | prev_removed) - prev_added) - current_values;
    //     } else if (sampled()) {
    //         return nb::set();
    //     } else if (has_output()) {
    //         return set_output().py_removed();
    //     } else {
    //         return nb::set();
    //     }
    // }
    //
    // bool TimeSeriesSetInput::py_was_removed(const nb::object &item) const {
    //     if (has_prev_output()) {
    //         return prev_output().py_contains(item) && !py_contains(item);
    //     } else if (sampled()) {
    //         return false;
    //     } else {
    //         return set_output().py_was_removed(item);
    //     }
    // }

    const TimeSeriesSetOutput &TimeSeriesSetInput::prev_output() const { return *_prev_output; }

    bool TimeSeriesSetInput::has_prev_output() const { return _prev_output != nullptr; }

    void TimeSeriesSetInput::reset_prev() {
        _pending_reset_prev = false;
        _prev_output.reset();
    }

    void TimeSeriesSetInput::_add_reset_prev() const {
        // Capture weak_ptr to avoid preventing destruction, but skip callback if already destroyed
        if (_pending_reset_prev) { return; }
        _pending_reset_prev = true;
        // Need non-const pointer since reset_prev() is not const
        auto weak_self      = std::weak_ptr(std::const_pointer_cast<TimeSeriesInput>(shared_from_this()));
        owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
            if (auto self = weak_self.lock()) {
                static_cast<TimeSeriesSetInput *>(self.get())->reset_prev();
            }
        });
    }

    bool TimeSeriesSetInput::do_bind_output(time_series_output_s_ptr output) {
        if (has_output()) {
            _prev_output = std::dynamic_pointer_cast<TimeSeriesSetOutput>(this->output());
            // Clean up after the engine cycle is complete
            _add_reset_prev();
        }
        return BaseTimeSeriesInput::do_bind_output(std::move(output));
    }

    void TimeSeriesSetInput::do_un_bind_output(bool unbind_refs) {
        if (has_output()) {
            // Get shared_ptr via shared_from_this() since output() returns raw pointer
            _prev_output = std::dynamic_pointer_cast<TimeSeriesSetOutput>(this->output()->shared_from_this());
            _add_reset_prev();
        }
        BaseTimeSeriesInput::do_un_bind_output(unbind_refs);
    }

    template struct TimeSeriesSetInput_T<bool>;
    template struct TimeSeriesSetInput_T<int64_t>;
    template struct TimeSeriesSetInput_T<double>;
    template struct TimeSeriesSetInput_T<engine_date_t>;
    template struct TimeSeriesSetInput_T<engine_time_t>;
    template struct TimeSeriesSetInput_T<engine_time_delta_t>;
    template struct TimeSeriesSetInput_T<nb::object>;

    template struct TimeSeriesSetOutput_T<bool>;
    template struct TimeSeriesSetOutput_T<int64_t>;
    template struct TimeSeriesSetOutput_T<double>;
    template struct TimeSeriesSetOutput_T<engine_date_t>;
    template struct TimeSeriesSetOutput_T<engine_time_t>;
    template struct TimeSeriesSetOutput_T<engine_time_delta_t>;
    template struct TimeSeriesSetOutput_T<nb::object>;

    // ============================================================================
    // TimeSeriesSetValueOutput Implementation
    // ============================================================================

    // Helper to create set schema from element schema
    static const value::TypeMeta* make_set_schema(const value::TypeMeta* element_schema) {
        return value::TypeRegistry::instance().set(element_schema).build();
    }

    TimeSeriesSetValueOutput::TimeSeriesSetValueOutput(Node* owning_node, const value::TypeMeta* element_schema)
        : TimeSeriesSetOutput(owning_node),
          _element_schema(element_schema),
          _value(make_set_schema(element_schema)),
          _added(make_set_schema(element_schema)),
          _removed(make_set_schema(element_schema)),
          _contains_ref_outputs{this,
                                new TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()),
                                [](const TimeSeriesOutput& ts, TimeSeriesOutput& ref, const nb::object& key) {
                                    auto& ts_val = dynamic_cast<TimeSeriesValueOutput&>(ref);
                                    auto& ts_set = dynamic_cast<const TimeSeriesSetValueOutput&>(ts);
                                    ts_val.py_set_value(nb::cast(ts_set.contains(key)));
                                },
                                {}} {}

    TimeSeriesSetValueOutput::TimeSeriesSetValueOutput(TimeSeriesOutput* parent, const value::TypeMeta* element_schema)
        : TimeSeriesSetOutput(parent),
          _element_schema(element_schema),
          _value(make_set_schema(element_schema)),
          _added(make_set_schema(element_schema)),
          _removed(make_set_schema(element_schema)),
          _contains_ref_outputs{this,
                                new TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()),
                                [](const TimeSeriesOutput& ts, TimeSeriesOutput& ref, const nb::object& key) {
                                    auto& ts_val = dynamic_cast<TimeSeriesValueOutput&>(ref);
                                    auto& ts_set = dynamic_cast<const TimeSeriesSetValueOutput&>(ts);
                                    ts_val.py_set_value(nb::cast(ts_set.contains(key)));
                                },
                                {}} {}

    nb::object TimeSeriesSetValueOutput::py_value() const {
        return valid() ? _value.to_python() : nb::none();
    }

    nb::object TimeSeriesSetValueOutput::py_delta_value() const {
        // Return SetDelta_object for consistency with templated version
        if (!modified()) {
            return nb::cast(make_set_delta<nb::object>(
                std::unordered_set<nb::object>{},
                std::unordered_set<nb::object>{}
            ));
        }

        // Convert added/removed to Python sets
        std::unordered_set<nb::object> added_objs;
        std::unordered_set<nb::object> removed_objs;

        nb::object py_added = _added.to_python();
        nb::object py_removed = _removed.to_python();

        if (nb::isinstance<nb::frozenset>(py_added)) {
            for (auto item : nb::frozenset(py_added)) {
                added_objs.insert(nb::borrow<nb::object>(item));
            }
        }
        if (nb::isinstance<nb::frozenset>(py_removed)) {
            for (auto item : nb::frozenset(py_removed)) {
                removed_objs.insert(nb::borrow<nb::object>(item));
            }
        }

        return nb::cast(make_set_delta<nb::object>(std::move(added_objs), std::move(removed_objs)));
    }

    void TimeSeriesSetValueOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }

        auto removed_type = get_removed();
        auto set_view = _value.view().as_set();
        auto added_view = _added.view().as_set();
        auto removed_view = _removed.view().as_set();

        // Clear added/removed for this cycle
        added_view.clear();
        removed_view.clear();

        if (nb::isinstance<SetDelta_T<nb::object>>(value)) {
            // Handle SetDelta
            auto delta = nb::cast<SetDelta_T<nb::object>>(value);
            for (const auto& item : delta.added()) {
                if (!set_view.contains_py(item)) {
                    set_view.insert_py(item);
                    added_view.insert_py(item);
                }
            }
            for (const auto& item : delta.removed()) {
                if (set_view.contains_py(item)) {
                    set_view.erase_py(item);
                    removed_view.insert_py(item);
                }
            }
        } else if (nb::isinstance<nb::frozenset>(value)) {
            // Handle frozenset - replace value
            auto v = nb::frozenset(value);
            // Calculate added (in new, not in old)
            for (auto item : v) {
                nb::object obj = nb::borrow<nb::object>(item);
                if (!set_view.contains_py(obj)) {
                    added_view.insert_py(obj);
                }
            }
            // Calculate removed (in old, not in new)
            nb::object current_py = _value.to_python();
            if (nb::isinstance<nb::frozenset>(current_py)) {
                for (auto item : nb::frozenset(current_py)) {
                    nb::object obj = nb::borrow<nb::object>(item);
                    if (!v.contains(obj)) {
                        removed_view.insert_py(obj);
                    }
                }
            }
            // Now update the set
            set_view.clear();
            for (auto item : v) {
                set_view.insert_py(nb::borrow<nb::object>(item));
            }
        } else {
            // Handle iterable with possible Removed markers
            for (auto item : nb::iter(value)) {
                nb::object obj = nb::borrow<nb::object>(item);
                if (nb::isinstance(obj, removed_type)) {
                    // It's a Removed marker
                    nb::object inner_item = obj.attr("item");
                    if (set_view.contains_py(inner_item)) {
                        if (added_view.contains_py(inner_item)) {
                            throw std::runtime_error("Cannot remove and add the same element");
                        }
                        set_view.erase_py(inner_item);
                        removed_view.insert_py(inner_item);
                    }
                } else {
                    // Regular item to add
                    if (!set_view.contains_py(obj)) {
                        set_view.insert_py(obj);
                        added_view.insert_py(obj);
                    }
                }
            }
        }

        _post_modify();
    }

    void TimeSeriesSetValueOutput::apply_result(const nb::object& value) {
        if (value.is_none()) {
            return;
        }
        py_set_value(value);
    }

    size_t TimeSeriesSetValueOutput::size() const {
        return value().size();
    }

    bool TimeSeriesSetValueOutput::empty() const {
        return size() == 0;
    }

    bool TimeSeriesSetValueOutput::contains(const nb::object& item) const {
        return _value.const_view().as_set().contains_py(item);
    }

    void TimeSeriesSetValueOutput::add(const nb::object& item) {
        if (!contains(item)) {
            if (empty()) {
                is_empty_output()->py_set_value(nb::cast(false));
            }
            _value.view().as_set().insert_py(item);
            _added.view().as_set().insert_py(item);
            _contains_ref_outputs.update(item);
            mark_modified();
        }
    }

    void TimeSeriesSetValueOutput::remove(const nb::object& item) {
        if (contains(item)) {
            auto added_view = _added.view().as_set();
            if (added_view.contains_py(item)) {
                added_view.erase_py(item);
            } else {
                _removed.view().as_set().insert_py(item);
            }
            _value.view().as_set().erase_py(item);
            _contains_ref_outputs.update(item);

            if (empty()) {
                is_empty_output()->py_set_value(nb::cast(true));
            }
            mark_modified();
        }
    }

    void TimeSeriesSetValueOutput::clear() {
        // Move current non-added elements to removed
        auto removed_view = _removed.view().as_set();
        removed_view.clear();

        nb::object current_py = _value.to_python();
        nb::object added_py = _added.to_python();

        if (nb::isinstance<nb::frozenset>(current_py)) {
            for (auto item : nb::frozenset(current_py)) {
                nb::object obj = nb::borrow<nb::object>(item);
                // Only add to removed if not in added
                if (!nb::isinstance<nb::frozenset>(added_py) ||
                    !nb::frozenset(added_py).contains(obj)) {
                    removed_view.insert_py(obj);
                }
                _contains_ref_outputs.update(obj);
            }
        }

        _added.view().as_set().clear();
        _value.view().as_set().clear();
        is_empty_output()->py_set_value(nb::cast(true));
        mark_modified();
    }

    void TimeSeriesSetValueOutput::copy_from_output(const TimeSeriesOutput& output) {
        auto* other = dynamic_cast<const TimeSeriesSetValueOutput*>(&output);
        if (!other) {
            throw std::runtime_error("TimeSeriesSetValueOutput::copy_from_output: type mismatch");
        }

        auto added_view = _added.view().as_set();
        auto removed_view = _removed.view().as_set();
        added_view.clear();
        removed_view.clear();

        nb::object current_py = _value.to_python();
        nb::object other_py = other->_value.to_python();

        if (nb::isinstance<nb::frozenset>(other_py)) {
            // Calculate added
            for (auto item : nb::frozenset(other_py)) {
                nb::object obj = nb::borrow<nb::object>(item);
                if (!nb::isinstance<nb::frozenset>(current_py) ||
                    !nb::frozenset(current_py).contains(obj)) {
                    added_view.insert_py(obj);
                }
            }
        }

        if (nb::isinstance<nb::frozenset>(current_py)) {
            // Calculate removed
            for (auto item : nb::frozenset(current_py)) {
                nb::object obj = nb::borrow<nb::object>(item);
                if (!nb::isinstance<nb::frozenset>(other_py) ||
                    !nb::frozenset(other_py).contains(obj)) {
                    removed_view.insert_py(obj);
                }
            }
        }

        if (added_view.size() > 0 || removed_view.size() > 0 || !valid()) {
            _value.view().copy_from(other->_value.const_view());
            is_empty_output()->py_set_value(nb::cast(empty()));

            // Update contains outputs
            nb::object added_py = _added.to_python();
            nb::object removed_py = _removed.to_python();
            if (nb::isinstance<nb::frozenset>(added_py)) {
                for (auto item : nb::frozenset(added_py)) {
                    _contains_ref_outputs.update(nb::borrow<nb::object>(item));
                }
            }
            if (nb::isinstance<nb::frozenset>(removed_py)) {
                for (auto item : nb::frozenset(removed_py)) {
                    _contains_ref_outputs.update(nb::borrow<nb::object>(item));
                }
            }
            mark_modified();
        }
    }

    void TimeSeriesSetValueOutput::copy_from_input(const TimeSeriesInput& input) {
        auto* other = dynamic_cast<const TimeSeriesSetValueInput*>(&input);
        if (!other) {
            throw std::runtime_error("TimeSeriesSetValueOutput::copy_from_input: type mismatch");
        }

        auto added_view = _added.view().as_set();
        auto removed_view = _removed.view().as_set();
        added_view.clear();
        removed_view.clear();

        nb::object current_py = _value.to_python();
        nb::object input_py = other->set_value_output()._value.to_python();

        if (nb::isinstance<nb::frozenset>(input_py)) {
            // Calculate added
            for (auto item : nb::frozenset(input_py)) {
                nb::object obj = nb::borrow<nb::object>(item);
                if (!nb::isinstance<nb::frozenset>(current_py) ||
                    !nb::frozenset(current_py).contains(obj)) {
                    added_view.insert_py(obj);
                }
            }
        }

        if (nb::isinstance<nb::frozenset>(current_py)) {
            // Calculate removed
            for (auto item : nb::frozenset(current_py)) {
                nb::object obj = nb::borrow<nb::object>(item);
                if (!nb::isinstance<nb::frozenset>(input_py) ||
                    !nb::frozenset(input_py).contains(obj)) {
                    removed_view.insert_py(obj);
                }
            }
        }

        if (added_view.size() > 0 || removed_view.size() > 0 || !valid()) {
            _value.view().copy_from(other->set_value_output()._value.const_view());
            is_empty_output()->py_set_value(nb::cast(empty()));

            nb::object added_py = _added.to_python();
            nb::object removed_py = _removed.to_python();
            if (nb::isinstance<nb::frozenset>(added_py)) {
                for (auto item : nb::frozenset(added_py)) {
                    _contains_ref_outputs.update(nb::borrow<nb::object>(item));
                }
            }
            if (nb::isinstance<nb::frozenset>(removed_py)) {
                for (auto item : nb::frozenset(removed_py)) {
                    _contains_ref_outputs.update(nb::borrow<nb::object>(item));
                }
            }
            mark_modified();
        }
    }

    bool TimeSeriesSetValueOutput::is_same_type(const TimeSeriesType* other) const {
        auto* ts = dynamic_cast<const TimeSeriesSetValueOutput*>(other);
        return ts && ts->element_schema() == element_schema();
    }

    time_series_value_output_s_ptr TimeSeriesSetValueOutput::get_contains_output(
        const nb::object& item, const nb::object& requester) {
        return std::dynamic_pointer_cast<TimeSeriesValueOutput>(
            _contains_ref_outputs.create_or_increment(item, static_cast<void*>(requester.ptr())));
    }

    void TimeSeriesSetValueOutput::release_contains_output(
        const nb::object& item, const nb::object& requester) {
        _contains_ref_outputs.release(item, static_cast<void*>(requester.ptr()));
    }

    void TimeSeriesSetValueOutput::mark_modified(engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            TimeSeriesSetOutput::mark_modified(modified_time);
            if (has_parent_or_node()) {
                auto weak_self = weak_from_this();
                owning_node()->graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                    if (auto self = weak_self.lock()) {
                        static_cast<TimeSeriesSetValueOutput*>(self.get())->_reset();
                    }
                });
            }
        }
    }

    void TimeSeriesSetValueOutput::_reset_value() {
        _reset();
        _value.view().as_set().clear();
        _added.view().as_set().clear();
        _removed.view().as_set().clear();
    }

    void TimeSeriesSetValueOutput::_post_modify() {
        auto added_view = added_set();
        auto removed_view = removed_set();

        bool has_changes = added_view.size() > 0 || removed_view.size() > 0;
        bool needs_validation = !valid();
        bool is_current_cycle = (last_modified_time() < owning_graph()->evaluation_time());

        if ((has_changes || needs_validation) && is_current_cycle) {
            mark_modified();
            if (added_view.size() > 0 && is_empty_output()->valid() &&
                is_empty_output()->value().template as<bool>()) {
                is_empty_output()->py_set_value(nb::cast(false));
            } else if (removed_view.size() > 0 && empty()) {
                is_empty_output()->py_set_value(nb::cast(true));
            }

            // Update contains outputs
            nb::object added_py = _added.to_python();
            nb::object removed_py = _removed.to_python();
            if (nb::isinstance<nb::frozenset>(added_py)) {
                for (auto item : nb::frozenset(added_py)) {
                    _contains_ref_outputs.update(nb::borrow<nb::object>(item));
                }
            }
            if (nb::isinstance<nb::frozenset>(removed_py)) {
                for (auto item : nb::frozenset(removed_py)) {
                    _contains_ref_outputs.update(nb::borrow<nb::object>(item));
                }
            }
        }
    }

    void TimeSeriesSetValueOutput::_reset() {
        _added.view().as_set().clear();
        _removed.view().as_set().clear();
    }

    // ============================================================================
    // TimeSeriesSetValueInput Implementation
    // ============================================================================

    TimeSeriesSetValueOutput& TimeSeriesSetValueInput::set_value_output() {
        return dynamic_cast<TimeSeriesSetValueOutput&>(*output());
    }

    const TimeSeriesSetValueOutput& TimeSeriesSetValueInput::set_value_output() const {
        return dynamic_cast<const TimeSeriesSetValueOutput&>(*output());
    }

    nb::object TimeSeriesSetValueInput::py_value() const {
        return bound() ? set_value_output().py_value() : nb::none();
    }

    nb::object TimeSeriesSetValueInput::py_delta_value() const {
        // TODO: Handle rebinding case like templated version
        return bound() ? set_value_output().py_delta_value() : nb::none();
    }

    size_t TimeSeriesSetValueInput::size() const {
        return has_output() ? set_value_output().size() : 0;
    }

    bool TimeSeriesSetValueInput::empty() const {
        return size() == 0;
    }

    value::ConstSetView TimeSeriesSetValueInput::value() const {
        return set_value_output().value();
    }

    value::ConstSetView TimeSeriesSetValueInput::added() const {
        // TODO: Handle rebinding case
        return set_value_output().added_set();
    }

    value::ConstSetView TimeSeriesSetValueInput::removed() const {
        // TODO: Handle rebinding case
        return set_value_output().removed_set();
    }

    bool TimeSeriesSetValueInput::contains(const nb::object& item) const {
        return has_output() ? set_value_output().contains(item) : false;
    }

    const value::TypeMeta* TimeSeriesSetValueInput::element_schema() const {
        return set_value_output().element_schema();
    }

    bool TimeSeriesSetValueInput::is_same_type(const TimeSeriesType* other) const {
        return dynamic_cast<const TimeSeriesSetValueInput*>(other) != nullptr;
    }

    const TimeSeriesSetValueOutput& TimeSeriesSetValueInput::prev_value_output() const {
        return dynamic_cast<const TimeSeriesSetValueOutput&>(prev_output());
    }

    void TimeSeriesSetValueInput::reset_prev() {
        TimeSeriesSetInput::reset_prev();
        // Clear cached computations
        _cached_added = value::PlainValue();
        _cached_removed = value::PlainValue();
    }

    void register_tss_with_nanobind(nb::module_& m) {
        // TimeSeriesSetValueOutput - created via builders, not directly from Python
        nb::class_<TimeSeriesSetValueOutput, TimeSeriesSetOutput>(m, "TimeSeriesSetValueOutput")
            .def_prop_ro("element_schema", &TimeSeriesSetValueOutput::element_schema);

        // TimeSeriesSetValueInput - created via builders
        nb::class_<TimeSeriesSetValueInput, TimeSeriesSetInput>(m, "TimeSeriesSetValueInput")
            .def_prop_ro("element_schema", &TimeSeriesSetValueInput::element_schema);
    }

}  // namespace hgraph