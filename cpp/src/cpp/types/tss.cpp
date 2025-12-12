#include "hgraph/types/node.h"

#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tss.h>

namespace hgraph
{
    // ============================================================================
    // SetDelta_T implementation (kept for Python bindings)
    // ============================================================================

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

    // ============================================================================
    // TimeSeriesSetOutput implementation
    // ============================================================================

    TimeSeriesSetOutput::TimeSeriesSetOutput(node_ptr parent, const std::type_info &element_tp)
        : _parent_adapter{parent},
          _tss_output{this, element_tp},
          _is_empty_ref_output{std::dynamic_pointer_cast<TimeSeriesValueOutput>(
              TimeSeriesValueOutputBuilder(typeid(bool)).make_instance(this))} {}

    TimeSeriesSetOutput::TimeSeriesSetOutput(time_series_output_ptr parent, const std::type_info &element_tp)
        : _parent_adapter{parent},
          _tss_output{this, element_tp},
          _is_empty_ref_output{std::dynamic_pointer_cast<TimeSeriesValueOutput>(
              TimeSeriesValueOutputBuilder(typeid(bool)).make_instance(this))} {}

    void TimeSeriesSetOutput::notify(engine_time_t et) {
        // Outputs notify their parent (which may be a collection output or node)
        if (_parent_adapter.has_parent_output()) {
            _parent_adapter.parent_output()->mark_child_modified(*this, et);
        }
    }

    engine_time_t TimeSeriesSetOutput::current_engine_time() const { return owning_node()->current_engine_time(); }

    void TimeSeriesSetOutput::add_before_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_before_evaluation_notification(std::move(fn));
    }

    void TimeSeriesSetOutput::add_after_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_after_evaluation_notification(std::move(fn));
    }

    node_ptr TimeSeriesSetOutput::owning_node() { return _parent_adapter.owning_node(); }

    node_ptr TimeSeriesSetOutput::owning_node() const { return _parent_adapter.owning_node(); }

    graph_ptr TimeSeriesSetOutput::owning_graph() { return _parent_adapter.owning_graph(); }

    graph_ptr TimeSeriesSetOutput::owning_graph() const { return _parent_adapter.owning_graph(); }

    bool TimeSeriesSetOutput::has_parent_or_node() const { return _parent_adapter.has_parent_or_node(); }

    bool TimeSeriesSetOutput::has_owning_node() const { return _parent_adapter.has_owning_node(); }

    nb::object TimeSeriesSetOutput::py_value() const {
        if (!_py_value.is_valid() || _py_value.is_none()) {
            nb::set v{};
            for (const auto& av : _tss_output.values()) {
                v.add(av.as_python());
            }
            _py_value = nb::frozenset(v);
        }
        return _py_value;
    }

    nb::object TimeSeriesSetOutput::py_delta_value() const {
        if (!modified()) {
            // For consistency with Python, return empty delta if not modified
            const auto& tp = element_type();
            if (tp == typeid(bool)) {
                return nb::cast(make_set_delta<bool>({}, {}));
            } else if (tp == typeid(int64_t)) {
                return nb::cast(make_set_delta<int64_t>({}, {}));
            } else if (tp == typeid(double)) {
                return nb::cast(make_set_delta<double>({}, {}));
            } else if (tp == typeid(engine_date_t)) {
                return nb::cast(make_set_delta<engine_date_t>({}, {}));
            } else if (tp == typeid(engine_time_t)) {
                return nb::cast(make_set_delta<engine_time_t>({}, {}));
            } else if (tp == typeid(engine_time_delta_t)) {
                return nb::cast(make_set_delta<engine_time_delta_t>({}, {}));
            } else {
                return nb::cast(make_set_delta<nb::object>({}, {}));
            }
        }

        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            return nb::cast(make_set_delta<bool>(added<bool>(), removed<bool>()));
        } else if (tp == typeid(int64_t)) {
            return nb::cast(make_set_delta<int64_t>(added<int64_t>(), removed<int64_t>()));
        } else if (tp == typeid(double)) {
            return nb::cast(make_set_delta<double>(added<double>(), removed<double>()));
        } else if (tp == typeid(engine_date_t)) {
            return nb::cast(make_set_delta<engine_date_t>(added<engine_date_t>(), removed<engine_date_t>()));
        } else if (tp == typeid(engine_time_t)) {
            return nb::cast(make_set_delta<engine_time_t>(added<engine_time_t>(), removed<engine_time_t>()));
        } else if (tp == typeid(engine_time_delta_t)) {
            return nb::cast(make_set_delta<engine_time_delta_t>(added<engine_time_delta_t>(), removed<engine_time_delta_t>()));
        } else {
            return nb::cast(make_set_delta<nb::object>(added<nb::object>(), removed<nb::object>()));
        }
    }

    void TimeSeriesSetOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }

        const auto& tp = element_type();

        // Helper to convert Python iterable to AnyValue vectors
        auto convert_to_any = [&tp](const nb::handle& items) {
            std::vector<AnyValue<>> result;
            for (const auto& item : nb::iter(items)) {
                AnyValue<> av;
                if (tp == typeid(bool)) {
                    av.emplace<bool>(nb::cast<bool>(item));
                } else if (tp == typeid(int64_t)) {
                    av.emplace<int64_t>(nb::cast<int64_t>(item));
                } else if (tp == typeid(double)) {
                    av.emplace<double>(nb::cast<double>(item));
                } else if (tp == typeid(engine_date_t)) {
                    av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
                } else if (tp == typeid(engine_time_t)) {
                    av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
                } else if (tp == typeid(engine_time_delta_t)) {
                    av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
                } else {
                    av.emplace<nb::object>(nb::borrow(item));
                }
                result.push_back(std::move(av));
            }
            return result;
        };

        // Check if it's a SetDelta
        if (nb::hasattr(value, "added") && nb::hasattr(value, "removed")) {
            auto added_items = convert_to_any(value.attr("added"));
            auto removed_items = convert_to_any(value.attr("removed"));
            _tss_output.set_delta(added_items, removed_items);
            _post_modify();
            return;
        }

        // Check if it's a frozenset - compute delta
        if (nb::isinstance<nb::frozenset>(value)) {
            // Compute delta: added = new - current, removed = current - new
            std::vector<AnyValue<>> added_av, removed_av;
            auto new_values = convert_to_any(value);
            auto current_values = _tss_output.values();

            // Build set of new value hashes for comparison
            std::unordered_set<size_t> new_hashes;
            for (const auto& av : new_values) {
                new_hashes.insert(av.hash_code());
            }

            std::unordered_set<size_t> current_hashes;
            for (const auto& av : current_values) {
                current_hashes.insert(av.hash_code());
            }

            // Added: in new but not in current
            for (const auto& av : new_values) {
                if (!current_hashes.contains(av.hash_code())) {
                    added_av.push_back(av);
                }
            }

            // Removed: in current but not in new
            for (const auto& av : current_values) {
                if (!new_hashes.contains(av.hash_code())) {
                    removed_av.push_back(av);
                }
            }

            _tss_output.set_delta(added_av, removed_av);
            _post_modify();
            return;
        }

        // Assume iterable with potential Removed() markers
        auto REMOVED = get_removed();
        std::vector<AnyValue<>> added_av, removed_av;
        auto current_values = _tss_output.values();
        std::unordered_set<size_t> current_hashes;
        for (const auto& av : current_values) {
            current_hashes.insert(av.hash_code());
        }

        for (const auto& item : nb::iter(value)) {
            if (nb::isinstance(item, REMOVED)) {
                // This is a removal
                AnyValue<> av;
                auto inner = item.attr("item");
                if (tp == typeid(bool)) {
                    av.emplace<bool>(nb::cast<bool>(inner));
                } else if (tp == typeid(int64_t)) {
                    av.emplace<int64_t>(nb::cast<int64_t>(inner));
                } else if (tp == typeid(double)) {
                    av.emplace<double>(nb::cast<double>(inner));
                } else if (tp == typeid(engine_date_t)) {
                    av.emplace<engine_date_t>(nb::cast<engine_date_t>(inner));
                } else if (tp == typeid(engine_time_t)) {
                    av.emplace<engine_time_t>(nb::cast<engine_time_t>(inner));
                } else if (tp == typeid(engine_time_delta_t)) {
                    av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(inner));
                } else {
                    av.emplace<nb::object>(nb::borrow(inner));
                }
                if (current_hashes.contains(av.hash_code())) {
                    removed_av.push_back(std::move(av));
                }
            } else {
                // This is an addition
                AnyValue<> av;
                if (tp == typeid(bool)) {
                    av.emplace<bool>(nb::cast<bool>(item));
                } else if (tp == typeid(int64_t)) {
                    av.emplace<int64_t>(nb::cast<int64_t>(item));
                } else if (tp == typeid(double)) {
                    av.emplace<double>(nb::cast<double>(item));
                } else if (tp == typeid(engine_date_t)) {
                    av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
                } else if (tp == typeid(engine_time_t)) {
                    av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
                } else if (tp == typeid(engine_time_delta_t)) {
                    av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
                } else {
                    av.emplace<nb::object>(nb::borrow(item));
                }
                if (!current_hashes.contains(av.hash_code())) {
                    added_av.push_back(std::move(av));
                }
            }
        }

        _tss_output.set_delta(added_av, removed_av);
        _post_modify();
    }

    bool TimeSeriesSetOutput::can_apply_result(const nb::object &value) { return !modified(); }

    nb::object TimeSeriesSetOutput::py_added() const {
        nb::set result{};
        for (const auto& av : _tss_output.added()) {
            result.add(av.as_python());
        }
        return nb::frozenset(result);
    }

    nb::object TimeSeriesSetOutput::py_removed() const {
        nb::set result{};
        for (const auto& av : _tss_output.removed()) {
            result.add(av.as_python());
        }
        return nb::frozenset(result);
    }

    bool TimeSeriesSetOutput::py_contains(const nb::object& item) const {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        return _tss_output.contains(av);
    }

    bool TimeSeriesSetOutput::py_was_added(const nb::object& item) const {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        return _tss_output.was_added(av);
    }

    bool TimeSeriesSetOutput::py_was_removed(const nb::object& item) const {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        return _tss_output.was_removed(av);
    }

    void TimeSeriesSetOutput::py_add(const nb::object& item) {
        if (item.is_none()) return;
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        _tss_output.add(av);
        _post_modify();
    }

    void TimeSeriesSetOutput::py_remove(const nb::object& item) {
        if (item.is_none()) return;
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        _tss_output.remove(av);
        _post_modify();
    }

    void TimeSeriesSetOutput::apply_result(const nb::object& value) {
        if (!value.is_valid() || value.is_none()) return;
        py_set_value(value);
    }

    size_t TimeSeriesSetOutput::size() const { return _tss_output.size(); }

    bool TimeSeriesSetOutput::empty() const { return _tss_output.empty(); }

    bool TimeSeriesSetOutput::has_added() const { return !_tss_output.added().empty(); }

    bool TimeSeriesSetOutput::has_removed() const { return !_tss_output.removed().empty(); }

    TimeSeriesValueOutput::s_ptr TimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) {
        // Convert item to AnyValue for hashing
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }

        size_t hash = av.hash_code();
        auto it = _contains_ref_outputs.find(hash);

        if (it != _contains_ref_outputs.end()) {
            it->second.requesters.insert(static_cast<void*>(requester.ptr()));
            return it->second.output;
        }

        // Create new contains output
        auto output = std::dynamic_pointer_cast<TimeSeriesValueOutput>(
            TimeSeriesValueOutputBuilder(typeid(bool)).make_instance(this));
        output->set_value<bool>(_tss_output.contains(av));

        ContainsRefEntry entry{output, {static_cast<void*>(requester.ptr())}};
        _contains_ref_outputs[hash] = std::move(entry);
        return output;
    }

    void TimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }

        size_t hash = av.hash_code();
        auto it = _contains_ref_outputs.find(hash);
        if (it != _contains_ref_outputs.end()) {
            it->second.requesters.erase(static_cast<void*>(requester.ptr()));
            if (it->second.requesters.empty()) {
                _contains_ref_outputs.erase(it);
            }
        }
    }

    TimeSeriesValueOutput::s_ptr& TimeSeriesSetOutput::is_empty_output() {
        if (!_is_empty_ref_output->valid()) {
            _is_empty_ref_output->set_value<bool>(empty());
        }
        return _is_empty_ref_output;
    }

    engine_time_t TimeSeriesSetOutput::last_modified_time() const { return _tss_output.last_modified_time(); }

    bool TimeSeriesSetOutput::modified() const { return _tss_output.modified(); }

    bool TimeSeriesSetOutput::valid() const { return _tss_output.valid(); }

    bool TimeSeriesSetOutput::all_valid() const { return _tss_output.valid(); }

    void TimeSeriesSetOutput::re_parent(node_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesSetOutput::re_parent(const time_series_type_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesSetOutput::reset_parent_or_node() { _parent_adapter.reset_parent_or_node(); }

    void TimeSeriesSetOutput::builder_release_cleanup() {}

    bool TimeSeriesSetOutput::is_same_type(const TimeSeriesType *other) const {
        auto *other_out = dynamic_cast<const TimeSeriesSetOutput *>(other);
        return other_out != nullptr && other_out->element_type() == element_type();
    }

    bool TimeSeriesSetOutput::is_reference() const { return false; }

    bool TimeSeriesSetOutput::has_reference() const { return false; }

    TimeSeriesOutput::s_ptr TimeSeriesSetOutput::parent_output() const {
        auto p{_parent_adapter.parent_output()};
        return p != nullptr ? p->shared_from_this() : s_ptr{};
    }

    TimeSeriesOutput::s_ptr TimeSeriesSetOutput::parent_output() {
        auto p{_parent_adapter.parent_output()};
        return p != nullptr ? p->shared_from_this() : s_ptr{};
    }

    bool TimeSeriesSetOutput::has_parent_output() const { return _parent_adapter.has_parent_output(); }

    void TimeSeriesSetOutput::subscribe(Notifiable *node) { _tss_output.subscribe(node); }

    void TimeSeriesSetOutput::un_subscribe(Notifiable *node) { _tss_output.unsubscribe(node); }

    void TimeSeriesSetOutput::mark_invalid() { _tss_output.invalidate(); }

    void TimeSeriesSetOutput::invalidate() {
        _tss_output.invalidate();
        _py_value.reset();
        _py_added.reset();
        _py_removed.reset();
    }

    void TimeSeriesSetOutput::clear() {
        _tss_output.clear();
        _post_modify();
    }

    void TimeSeriesSetOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto* output_t = dynamic_cast<const TimeSeriesSetOutput *>(&output);
        if (output_t) {
            if (output_t->valid()) {
                // Compute delta between current and other
                auto other_values = output_t->_tss_output.values();
                auto current_values = _tss_output.values();

                std::unordered_set<size_t> current_hashes;
                for (const auto& av : current_values) {
                    current_hashes.insert(av.hash_code());
                }

                std::unordered_set<size_t> other_hashes;
                for (const auto& av : other_values) {
                    other_hashes.insert(av.hash_code());
                }

                std::vector<AnyValue<>> added_av, removed_av;

                // Added: in other but not in current
                for (const auto& av : other_values) {
                    if (!current_hashes.contains(av.hash_code())) {
                        added_av.push_back(av);
                    }
                }

                // Removed: in current but not in other
                for (const auto& av : current_values) {
                    if (!other_hashes.contains(av.hash_code())) {
                        removed_av.push_back(av);
                    }
                }

                if (!added_av.empty() || !removed_av.empty() || !valid()) {
                    _tss_output.set_delta(added_av, removed_av);
                    _post_modify();
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesSetOutput::copy_from_output: Expected TimeSeriesSetOutput");
        }
    }

    void TimeSeriesSetOutput::copy_from_input(const TimeSeriesInput &input) {
        auto* input_t = dynamic_cast<const TimeSeriesSetInput *>(&input);
        if (input_t) {
            if (input_t->valid()) {
                auto other_values = input_t->_tss_input.values();
                auto current_values = _tss_output.values();

                std::unordered_set<size_t> current_hashes;
                for (const auto& av : current_values) {
                    current_hashes.insert(av.hash_code());
                }

                std::unordered_set<size_t> other_hashes;
                for (const auto& av : other_values) {
                    other_hashes.insert(av.hash_code());
                }

                std::vector<AnyValue<>> added_av, removed_av;

                for (const auto& av : other_values) {
                    if (!current_hashes.contains(av.hash_code())) {
                        added_av.push_back(av);
                    }
                }

                for (const auto& av : current_values) {
                    if (!other_hashes.contains(av.hash_code())) {
                        removed_av.push_back(av);
                    }
                }

                if (!added_av.empty() || !removed_av.empty() || !valid()) {
                    _tss_output.set_delta(added_av, removed_av);
                    _post_modify();
                }
            }
        } else {
            throw std::runtime_error("TimeSeriesSetOutput::copy_from_input: Expected TimeSeriesSetInput");
        }
    }

    void TimeSeriesSetOutput::mark_modified() {
        // Reset caches
        _py_value.reset();
        _py_added.reset();
        _py_removed.reset();

        // Register reset callback
        if (has_parent_or_node()) {
            auto weak_self = weak_from_this();
            owning_node()->graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesSetOutput *>(self.get())->_reset();
                }
            });
        }
    }

    void TimeSeriesSetOutput::mark_modified(engine_time_t modified_time) {
        mark_modified();
    }

    void TimeSeriesSetOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        notify(modified_time);
    }

    void TimeSeriesSetOutput::_post_modify() {
        _py_value.reset();
        _py_added.reset();
        _py_removed.reset();

        bool has_changes = has_added() || has_removed();
        bool needs_validation = !valid();

        if (has_changes || needs_validation) {
            mark_modified();

            // Update is_empty output
            if (has_added() && is_empty_output()->valid() && is_empty_output()->value<bool>()) {
                is_empty_output()->set_value<bool>(false);
            } else if (has_removed() && empty()) {
                is_empty_output()->set_value<bool>(true);
            }

            // Update contains outputs
            for (auto& [hash, entry] : _contains_ref_outputs) {
                // Check if this item was added or removed
                for (const auto& av : _tss_output.added()) {
                    if (av.hash_code() == hash) {
                        entry.output->set_value<bool>(true);
                        break;
                    }
                }
                for (const auto& av : _tss_output.removed()) {
                    if (av.hash_code() == hash) {
                        entry.output->set_value<bool>(false);
                        break;
                    }
                }
            }
        }
    }

    void TimeSeriesSetOutput::_reset() {
        _tss_output.reset();
        _py_added.reset();
        _py_removed.reset();
    }

    // ============================================================================
    // TimeSeriesSetInput implementation
    // ============================================================================

    TimeSeriesSetInput::TimeSeriesSetInput(node_ptr parent, const std::type_info &element_tp)
        : _parent_adapter{parent}, _tss_input{this, element_tp} {}

    TimeSeriesSetInput::TimeSeriesSetInput(time_series_input_ptr parent, const std::type_info &element_tp)
        : _parent_adapter{parent}, _tss_input{this, element_tp} {}

    void TimeSeriesSetInput::notify(engine_time_t et) {
        _parent_adapter.notify_modified(this, et);
    }

    engine_time_t TimeSeriesSetInput::current_engine_time() const { return owning_node()->current_engine_time(); }

    void TimeSeriesSetInput::add_before_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_before_evaluation_notification(std::move(fn));
    }

    void TimeSeriesSetInput::add_after_evaluation_notification(std::function<void()> &&fn) {
        owning_node()->add_after_evaluation_notification(std::move(fn));
    }

    TimeSeriesSetOutput& TimeSeriesSetInput::set_output() {
        return dynamic_cast<TimeSeriesSetOutput &>(*output());
    }

    const TimeSeriesSetOutput& TimeSeriesSetInput::set_output() const {
        return dynamic_cast<const TimeSeriesSetOutput &>(*output());
    }

    node_ptr TimeSeriesSetInput::owning_node() { return _parent_adapter.owning_node(); }

    node_ptr TimeSeriesSetInput::owning_node() const { return _parent_adapter.owning_node(); }

    graph_ptr TimeSeriesSetInput::owning_graph() { return _parent_adapter.owning_graph(); }

    graph_ptr TimeSeriesSetInput::owning_graph() const { return _parent_adapter.owning_graph(); }

    bool TimeSeriesSetInput::has_parent_or_node() const { return _parent_adapter.has_parent_or_node(); }

    bool TimeSeriesSetInput::has_owning_node() const { return _parent_adapter.has_owning_node(); }

    nb::object TimeSeriesSetInput::py_value() const {
        nb::set v{};
        for (const auto& av : _tss_input.values()) {
            v.add(av.as_python());
        }
        return nb::frozenset(v);
    }

    nb::object TimeSeriesSetInput::py_delta_value() const {
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            return nb::cast(delta_value<bool>());
        } else if (tp == typeid(int64_t)) {
            return nb::cast(delta_value<int64_t>());
        } else if (tp == typeid(double)) {
            return nb::cast(delta_value<double>());
        } else if (tp == typeid(engine_date_t)) {
            return nb::cast(delta_value<engine_date_t>());
        } else if (tp == typeid(engine_time_t)) {
            return nb::cast(delta_value<engine_time_t>());
        } else if (tp == typeid(engine_time_delta_t)) {
            return nb::cast(delta_value<engine_time_delta_t>());
        } else {
            return nb::cast(delta_value<nb::object>());
        }
    }

    nb::object TimeSeriesSetInput::py_added() const {
        nb::set result{};
        for (const auto& av : _tss_input.added()) {
            result.add(av.as_python());
        }
        return nb::frozenset(result);
    }

    nb::object TimeSeriesSetInput::py_removed() const {
        nb::set result{};
        for (const auto& av : _tss_input.removed()) {
            result.add(av.as_python());
        }
        return nb::frozenset(result);
    }

    bool TimeSeriesSetInput::py_contains(const nb::object& item) const {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        return _tss_input.contains(av);
    }

    bool TimeSeriesSetInput::py_was_added(const nb::object& item) const {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        return _tss_input.was_added(av);
    }

    bool TimeSeriesSetInput::py_was_removed(const nb::object& item) const {
        AnyValue<> av;
        const auto& tp = element_type();
        if (tp == typeid(bool)) {
            av.emplace<bool>(nb::cast<bool>(item));
        } else if (tp == typeid(int64_t)) {
            av.emplace<int64_t>(nb::cast<int64_t>(item));
        } else if (tp == typeid(double)) {
            av.emplace<double>(nb::cast<double>(item));
        } else if (tp == typeid(engine_date_t)) {
            av.emplace<engine_date_t>(nb::cast<engine_date_t>(item));
        } else if (tp == typeid(engine_time_t)) {
            av.emplace<engine_time_t>(nb::cast<engine_time_t>(item));
        } else if (tp == typeid(engine_time_delta_t)) {
            av.emplace<engine_time_delta_t>(nb::cast<engine_time_delta_t>(item));
        } else {
            av.emplace<nb::object>(item);
        }
        return _tss_input.was_removed(av);
    }

    size_t TimeSeriesSetInput::size() const { return _tss_input.size(); }

    bool TimeSeriesSetInput::empty() const { return _tss_input.empty(); }

    engine_time_t TimeSeriesSetInput::last_modified_time() const { return _tss_input.last_modified_time(); }

    bool TimeSeriesSetInput::modified() const { return _tss_input.modified(); }

    bool TimeSeriesSetInput::valid() const { return _tss_input.valid(); }

    bool TimeSeriesSetInput::all_valid() const { return _tss_input.valid(); }

    void TimeSeriesSetInput::re_parent(node_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesSetInput::re_parent(const time_series_type_ptr parent) { _parent_adapter.re_parent(parent); }

    void TimeSeriesSetInput::reset_parent_or_node() { _parent_adapter.reset_parent_or_node(); }

    void TimeSeriesSetInput::builder_release_cleanup() {}

    bool TimeSeriesSetInput::is_same_type(const TimeSeriesType *other) const {
        auto *other_in = dynamic_cast<const TimeSeriesSetInput *>(other);
        return other_in != nullptr && other_in->element_type() == element_type();
    }

    bool TimeSeriesSetInput::is_reference() const { return false; }

    bool TimeSeriesSetInput::has_reference() const { return false; }

    TimeSeriesInput::s_ptr TimeSeriesSetInput::parent_input() const {
        auto p{_parent_adapter.parent_input()};
        return p != nullptr ? p->shared_from_this() : TimeSeriesInput::s_ptr{};
    }

    bool TimeSeriesSetInput::has_parent_input() const { return _parent_adapter.has_parent_input(); }

    bool TimeSeriesSetInput::active() const { return _tss_input.active(); }

    void TimeSeriesSetInput::make_active() { _tss_input.make_active(); }

    void TimeSeriesSetInput::make_passive() { _tss_input.make_passive(); }

    bool TimeSeriesSetInput::bound() const { return _tss_input.bound(); }

    bool TimeSeriesSetInput::has_peer() const { return _tss_input.bound(); }

    time_series_output_s_ptr TimeSeriesSetInput::output() const { return _bound_output; }

    bool TimeSeriesSetInput::has_output() const { return _bound_output != nullptr; }

    time_series_reference_output_s_ptr TimeSeriesSetInput::reference_output() const {
        throw std::runtime_error("TimeSeriesSetInput does not support reference_output");
    }

    TimeSeriesInput::s_ptr TimeSeriesSetInput::get_input(size_t index) {
        throw std::runtime_error("TimeSeriesSetInput does not support get_input");
    }

    void TimeSeriesSetInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        throw std::runtime_error("TimeSeriesSetInput does not support notify_parent");
    }

    bool TimeSeriesSetInput::bind_output(const time_series_output_s_ptr& output_) {
        auto* tss_output = dynamic_cast<TimeSeriesSetOutput*>(output_.get());
        if (tss_output) {
            _bound_output = output_;
            _tss_input.bind_output(tss_output->tss_output());
            return true;
        }
        return false;
    }

    void TimeSeriesSetInput::un_bind_output(bool unbind_refs) {
        _bound_output = nullptr;
        _tss_input.unbind();
    }

}  // namespace hgraph
