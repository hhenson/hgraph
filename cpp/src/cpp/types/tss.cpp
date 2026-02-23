#include "hgraph/types/node.h"

#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/value/value.h>

namespace hgraph
{
    // ========== TimeSeriesSetOutput Implementation ==========

    TimeSeriesSetOutput::TimeSeriesSetOutput(const node_ptr &parent, const value::TypeMeta* element_type)
        : TimeSeriesSet<BaseTimeSeriesOutput>(parent),
          _storage(element_type),
          _element_type(element_type),
          _is_empty_ref_output{std::dynamic_pointer_cast<TimeSeriesValueOutput>(
              TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()).make_instance(this))},
          _contains_ref_outputs{this,
                                new TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()),
                                element_type,
                                [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const value::View &key) {
                                    auto& ts_val = dynamic_cast<TimeSeriesValueOutput&>(ref);
                                    auto& ts_set = dynamic_cast<const TimeSeriesSetOutput&>(ts);
                                    ts_val.py_set_value(nb::cast(ts_set.contains(key)));
                                },
                                {}} {}

    TimeSeriesSetOutput::TimeSeriesSetOutput(time_series_output_ptr parent, const value::TypeMeta* element_type)
        : TimeSeriesSet<BaseTimeSeriesOutput>(parent),
          _storage(element_type),
          _element_type(element_type),
          _is_empty_ref_output{std::dynamic_pointer_cast<TimeSeriesValueOutput>(
              TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()).make_instance(this))},
          _contains_ref_outputs{this,
                                new TimeSeriesValueOutputBuilder(value::scalar_type_meta<bool>()),
                                element_type,
                                [](const TimeSeriesOutput &ts, TimeSeriesOutput &ref, const value::View &key) {
                                    auto& ts_val = dynamic_cast<TimeSeriesValueOutput&>(ref);
                                    auto& ts_set = dynamic_cast<const TimeSeriesSetOutput&>(ts);
                                    ts_val.py_set_value(nb::cast(ts_set.contains(key)));
                                },
                                {}} {}

    time_series_value_output_s_ptr &TimeSeriesSetOutput::is_empty_output() {
        if (!_is_empty_ref_output->valid()) {
            _is_empty_ref_output->py_set_value(nb::cast(empty()));
        }
        return _is_empty_ref_output;
    }

    void TimeSeriesSetOutput::invalidate() {
        clear();
        _reset_last_modified_time();
    }

    void TimeSeriesSetOutput::add(const value::View& elem) {
        if (_storage.add(elem)) {
            if (_storage.size() == 1) {
                is_empty_output()->py_set_value(nb::cast(false));
            }
            _contains_ref_outputs.update(elem);
            mark_modified();
        }
    }

    void TimeSeriesSetOutput::remove(const value::View& elem) {
        if (_storage.remove(elem)) {
            _contains_ref_outputs.update(elem);
            if (_storage.empty()) {
                is_empty_output()->py_set_value(nb::cast(true));
            }
            mark_modified();
        }
    }

    // ========== Python Interop Methods for TimeSeriesSetOutput ==========

    bool TimeSeriesSetOutput::py_contains(const nb::object& item) const {
        if (!_element_type || item.is_none()) return false;
        value::Value temp(_element_type);
        temp.from_python(item);
        return _storage.contains(value::View(temp.view()));
    }

    bool TimeSeriesSetOutput::py_was_added(const nb::object& item) const {
        if (!_element_type || item.is_none()) return false;
        value::Value temp(_element_type);
        temp.from_python(item);
        return _storage.was_added(value::View(temp.view()));
    }

    bool TimeSeriesSetOutput::py_was_removed(const nb::object& item) const {
        if (!_element_type || item.is_none()) return false;
        value::Value temp(_element_type);
        temp.from_python(item);
        return _storage.was_removed(value::View(temp.view()));
    }

    void TimeSeriesSetOutput::py_add(const nb::object& item) {
        if (!_element_type || item.is_none()) return;
        value::Value temp(_element_type);
        temp.from_python(item);
        if (_storage.add(value::View(temp.view()))) {
            if (_storage.size() == 1) {
                is_empty_output()->py_set_value(nb::cast(false));
            }
            _contains_ref_outputs.update(item);
            mark_modified();
        }
    }

    void TimeSeriesSetOutput::py_remove(const nb::object& item) {
        if (!_element_type || item.is_none()) return;
        value::Value temp(_element_type);
        temp.from_python(item);
        if (_storage.remove(value::View(temp.view()))) {
            _contains_ref_outputs.update(item);
            if (_storage.empty()) {
                is_empty_output()->py_set_value(nb::cast(true));
            }
            mark_modified();
        }
    }

    nb::object TimeSeriesSetOutput::py_added() const {
        return _storage.added().to_python();
    }

    nb::object TimeSeriesSetOutput::py_removed() const {
        return _storage.removed().to_python();
    }

    nb::object TimeSeriesSetOutput::py_value() const {
        return _storage.value().to_python();
    }

    nb::object TimeSeriesSetOutput::py_delta_value() const {
        // Return PythonSetDelta for proper comparison in tests
        auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");
        if (!modified()) {
            return PythonSetDelta(nb::frozenset(), nb::frozenset());
        }
        return PythonSetDelta(py_added(), py_removed());
    }

    void TimeSeriesSetOutput::py_set_value(const nb::object& value) {
        if (value.is_none()) {
            invalidate();
            return;
        }

        // Handle objects with .added and .removed attributes (like PythonSetDelta)
        if (nb::hasattr(value, "added") && nb::hasattr(value, "removed")) {
            bool was_invalid = !valid();
            auto added_iter = value.attr("added");
            auto removed_iter = value.attr("removed");

            // Filter to match Python behavior:
            // - Only remove elements that ARE in current value
            // - Only add elements that are NOT in current value
            nb::list to_remove;
            for (auto item : nb::iter(removed_iter)) {
                auto obj = nb::cast<nb::object>(item);
                if (py_contains(obj)) {
                    to_remove.append(obj);
                }
            }
            nb::list to_add;
            for (auto item : nb::iter(added_iter)) {
                auto obj = nb::cast<nb::object>(item);
                if (!py_contains(obj)) {
                    to_add.append(obj);
                }
            }

            for (auto item : to_remove) {
                py_remove(nb::cast<nb::object>(item));
            }
            for (auto item : to_add) {
                py_add(nb::cast<nb::object>(item));
            }

            // Handle empty set on first tick marking modified
            if (was_invalid && !modified()) {
                mark_modified();
            }
            return;
        }

        // Handle dict with added/removed (delta format)
        if (nb::isinstance<nb::dict>(value)) {
            auto d = nb::cast<nb::dict>(value);
            if (d.contains("added") && d.contains("removed")) {
                bool was_invalid = !valid();
                auto added = d["added"];
                auto removed = d["removed"];
                for (auto item : nb::iter(removed)) {
                    py_remove(nb::cast<nb::object>(item));
                }
                for (auto item : nb::iter(added)) {
                    py_add(nb::cast<nb::object>(item));
                }
                // Handle empty set on first tick marking modified
                if (was_invalid && !modified()) {
                    mark_modified();
                }
                return;
            }
        }

        // Handle frozenset (replace entire set)
        if (nb::isinstance<nb::frozenset>(value)) {
            bool was_invalid = !valid();
            auto fs = nb::cast<nb::frozenset>(value);
            // Build list of items to add and remove
            nb::list to_add;
            nb::list to_remove;

            // Items in new set but not in current - add them
            for (auto item : fs) {
                if (!py_contains(nb::cast<nb::object>(item))) {
                    to_add.append(item);
                }
            }

            // Items in current set but not in new - remove them
            for (auto elem : _storage.value()) {
                auto py_item = elem.to_python();
                if (!fs.contains(py_item)) {
                    to_remove.append(py_item);
                }
            }

            for (auto item : to_remove) {
                py_remove(nb::cast<nb::object>(item));
            }
            for (auto item : to_add) {
                py_add(nb::cast<nb::object>(item));
            }

            // Handle empty set on first tick marking modified
            if (was_invalid && !modified()) {
                mark_modified();
            }
            return;
        }

        // Handle Removed wrapper and iterable
        bool was_invalid = !valid();
        auto removed_class = get_removed();
        for (auto r : nb::iter(value)) {
            if (nb::isinstance(r, removed_class)) {
                auto item = nb::cast<nb::object>(r.attr("item"));
                if (py_contains(item)) {
                    py_remove(item);
                }
            } else {
                auto item = nb::cast<nb::object>(r);
                if (!py_contains(item)) {
                    py_add(item);
                }
            }
        }

        // Handle empty set on first tick marking modified
        if (was_invalid && !modified()) {
            mark_modified();
        }
    }

    void TimeSeriesSetOutput::apply_result(const nb::object& value) {
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    void TimeSeriesSetOutput::clear() {
        // Update contains outputs for all elements before clearing
        if (_contains_ref_outputs) {
            for (auto elem : _storage.value()) {
                _contains_ref_outputs.update(elem.to_python());
            }
        }
        _storage.clear();
        is_empty_output()->py_set_value(nb::cast(true));
        mark_modified();
    }

    void TimeSeriesSetOutput::copy_from_output(const TimeSeriesOutput &output) {
        auto &other = dynamic_cast<const TimeSeriesSetOutput&>(output);

        // Build lists of items to add and remove
        nb::list to_add;
        nb::list to_remove;

        // Elements in other but not in this - add them
        for (auto elem : other._storage.value()) {
            if (!_storage.contains(elem)) {
                to_add.append(elem.to_python());
            }
        }

        // Elements in this but not in other - remove them
        for (auto elem : _storage.value()) {
            if (!other._storage.contains(elem)) {
                to_remove.append(elem.to_python());
            }
        }

        if (nb::len(to_add) > 0 || nb::len(to_remove) > 0 || !valid()) {
            for (auto item : to_remove) {
                py_remove(nb::cast<nb::object>(item));
            }
            for (auto item : to_add) {
                py_add(nb::cast<nb::object>(item));
            }
        }
    }

    void TimeSeriesSetOutput::copy_from_input(const TimeSeriesInput &input) {
        auto &other = dynamic_cast<const TimeSeriesSetInput&>(input);

        // Build lists of items to add and remove
        nb::list to_add;
        nb::list to_remove;

        // Elements in other but not in this - add them
        for (auto elem : other.value_view()) {
            if (!_storage.contains(elem)) {
                to_add.append(elem.to_python());
            }
        }

        // Elements in this but not in other - remove them
        for (auto elem : _storage.value()) {
            if (!other.contains(elem)) {
                to_remove.append(elem.to_python());
            }
        }

        if (nb::len(to_add) > 0 || nb::len(to_remove) > 0 || !valid()) {
            for (auto item : to_remove) {
                py_remove(nb::cast<nb::object>(item));
            }
            for (auto item : to_add) {
                py_add(nb::cast<nb::object>(item));
            }
        }
    }

    void TimeSeriesSetOutput::mark_modified(engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            TimeSeriesSet<BaseTimeSeriesOutput>::mark_modified(modified_time);
            if (has_parent_or_node()) {
                auto weak_self = weak_from_this();
                owning_node()->graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                    if (auto self = weak_self.lock()) {
                        static_cast<TimeSeriesSetOutput*>(self.get())->_storage.clear_deltas();
                    }
                });
            }
        }
    }

    void TimeSeriesSetOutput::_reset_value() {
        _storage.clear();
        _storage.clear_deltas();
    }

    time_series_value_output_s_ptr TimeSeriesSetOutput::get_contains_output(const nb::object &item,
                                                                             const nb::object &requester) {
        // Convert Python object to Value for the lookup
        value::Value key_val(_element_type);
        key_val.from_python(item);
        return std::dynamic_pointer_cast<TimeSeriesValueOutput>(
            _contains_ref_outputs.create_or_increment(key_val.view(), static_cast<void*>(requester.ptr())));
    }

    void TimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) {
        // Convert Python object to Value for the lookup
        value::Value key_val(_element_type);
        key_val.from_python(item);
        _contains_ref_outputs.release(key_val.view(), static_cast<void*>(requester.ptr()));
    }

    void TimeSeriesSetOutput::_post_modify() {
        bool has_changes = _storage.has_delta();
        bool needs_validation = !valid();
        bool is_current_cycle = (last_modified_time() < owning_graph()->evaluation_time());
        if ((has_changes || needs_validation) && is_current_cycle) {
            mark_modified();
            if (!_storage.added().empty() && is_empty_output()->valid() &&
                is_empty_output()->value().template as<bool>()) {
                is_empty_output()->py_set_value(nb::cast(false));
            } else if (!_storage.removed().empty() && empty()) {
                is_empty_output()->py_set_value(nb::cast(true));
            }
            // Update feature outputs for added/removed elements
            _update_contains_refs();
        }
    }

    void TimeSeriesSetOutput::_update_contains_refs() {
        if (_contains_ref_outputs) {
            for (auto elem : _storage.added()) {
                _contains_ref_outputs.update(elem.to_python());
            }
            for (auto elem : _storage.removed()) {
                _contains_ref_outputs.update(elem.to_python());
            }
        }
    }

    // ========== TimeSeriesSetInput Implementation ==========

    TimeSeriesSetInput::TimeSeriesSetInput(const node_ptr &parent, const value::TypeMeta* element_type)
        : TimeSeriesSet<BaseTimeSeriesInput>(parent), _element_type(element_type) {}

    TimeSeriesSetInput::TimeSeriesSetInput(time_series_input_ptr parent, const value::TypeMeta* element_type)
        : TimeSeriesSet<BaseTimeSeriesInput>(parent), _element_type(element_type) {}

    TimeSeriesSetOutput &TimeSeriesSetInput::set_output() const {
        return dynamic_cast<TimeSeriesSetOutput &>(*output());
    }

    const value::TypeMeta* TimeSeriesSetInput::element_type() const {
        // First check local element type (set during construction)
        if (_element_type) return _element_type;
        // Fall back to output's element type if available
        return has_output() ? set_output().element_type() : nullptr;
    }

    value::SetView TimeSeriesSetInput::value_view() const {
        if (has_output()) {
            return set_output().value_view();
        }
        // Return an invalid view when no output is bound
        // Callers should check valid() or handle empty iteration gracefully
        return value::SetView{};
    }

    bool TimeSeriesSetInput::contains(const value::View& elem) const {
        return has_output() ? set_output().contains(elem) : false;
    }

    bool TimeSeriesSetInput::was_added(const value::View& elem) const {
        if (!has_output()) return false;

        if (has_prev_output()) {
            // Element is "added" if it's in current set but wasn't in prev state
            // prev state = (prev_values + prev_removed - prev_added)
            if (!set_output().contains(elem)) return false;
            bool was_in_prev = (prev_output().contains(elem) || prev_output().was_removed(elem))
                               && !prev_output().was_added(elem);
            return !was_in_prev;
        }

        if (sampled()) return set_output().contains(elem);
        return set_output().was_added(elem);
    }

    bool TimeSeriesSetInput::was_removed(const value::View& elem) const {
        if (!has_output()) return false;

        if (has_prev_output()) {
            // Element is "removed" if it was in prev state but not in current
            bool was_in_prev = (prev_output().contains(elem) || prev_output().was_removed(elem))
                               && !prev_output().was_added(elem);
            return was_in_prev && !set_output().contains(elem);
        }

        if (sampled()) return false;
        return set_output().was_removed(elem);
    }

    std::vector<value::View> TimeSeriesSetInput::collect_added() const {
        std::vector<value::View> result;

        if (!has_output()) return result;

        if (has_prev_output()) {
            // Calculate added: items in current that weren't in prev state
            // prev state = (prev_values + prev_removed - prev_added)
            for (auto elem : set_output().value_view()) {
                bool was_in_prev = (prev_output().contains(elem) || prev_output().was_removed(elem))
                                   && !prev_output().was_added(elem);
                if (!was_in_prev) {
                    result.push_back(elem);
                }
            }
            return result;
        }

        if (sampled()) {
            for (auto elem : set_output().value_view()) {
                result.push_back(elem);
            }
            return result;
        }

        for (auto elem : set_output().added_view()) {
            result.push_back(elem);
        }
        return result;
    }

    std::vector<value::View> TimeSeriesSetInput::collect_removed() const {
        std::vector<value::View> result;

        // Check prev_output FIRST (matching Python order)
        if (has_prev_output()) {
            // Calculate removed: items in prev state that aren't in current
            // prev state = (prev_values + prev_removed - prev_added)
            // Collect views into prev_output storage (no copying needed!)
            std::vector<value::View> prev_state;
            for (auto elem : prev_output().value_view()) {
                prev_state.push_back(elem);
            }
            for (auto elem : prev_output().removed_view()) {
                // Only add if not already in prev_state
                bool found = false;
                for (const auto& existing : prev_state) {
                    if (existing.equals(elem)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    prev_state.push_back(elem);
                }
            }
            // Remove items that were only added in the previous cycle
            for (auto elem : prev_output().added_view()) {
                prev_state.erase(
                    std::remove_if(prev_state.begin(), prev_state.end(),
                        [&](const value::View& v) { return v.equals(elem); }),
                    prev_state.end());
            }

            // Now filter: items in prev_state that aren't in current values
            for (const auto& elem : prev_state) {
                bool in_current = has_output() && set_output().contains(elem);
                if (!in_current) {
                    result.push_back(elem);
                }
            }
            return result;
        }

        if (sampled()) return result;  // Return empty

        if (has_output()) {
            for (auto elem : set_output().removed_view()) {
                result.push_back(elem);
            }
        }
        return result;
    }

    // ========== Python Interop Methods for TimeSeriesSetInput ==========

    bool TimeSeriesSetInput::py_contains(const nb::object& item) const {
        return has_output() ? set_output().py_contains(item) : false;
    }

    bool TimeSeriesSetInput::py_was_added(const nb::object& item) const {
        if (!has_output()) return false;

        if (has_prev_output()) {
            // Element is "added" if it's in current set but wasn't in prev state
            if (!set_output().py_contains(item)) return false;
            bool was_in_prev = (prev_output().py_contains(item) || prev_output().py_was_removed(item))
                               && !prev_output().py_was_added(item);
            return !was_in_prev;
        }

        if (sampled()) return set_output().py_contains(item);
        return set_output().py_was_added(item);
    }

    bool TimeSeriesSetInput::py_was_removed(const nb::object& item) const {
        if (!has_output()) return false;

        if (has_prev_output()) {
            bool was_in_prev = (prev_output().py_contains(item) || prev_output().py_was_removed(item))
                               && !prev_output().py_was_added(item);
            return was_in_prev && !set_output().py_contains(item);
        }

        if (sampled()) return false;
        return set_output().py_was_removed(item);
    }

    nb::object TimeSeriesSetInput::py_added() const {
        if (!has_output()) return nb::frozenset();

        if (has_prev_output()) {
            // Calculate added: items in current that weren't in prev state
            nb::set result;
            for (auto elem : set_output().value_view()) {
                auto py_item = elem.to_python();
                bool was_in_prev = (prev_output().py_contains(py_item) || prev_output().py_was_removed(py_item))
                                   && !prev_output().py_was_added(py_item);
                if (!was_in_prev) {
                    result.add(py_item);
                }
            }
            if (!result.is_none() && nb::len(result) > 0) {
                _add_reset_prev();
            }
            return nb::frozenset(result);
        }

        if (sampled()) return set_output().py_value();
        return set_output().py_added();
    }

    nb::object TimeSeriesSetInput::py_removed() const {
        // Check prev_output FIRST (matching Python order)
        if (has_prev_output()) {
            // Calculate removed: items in prev state that aren't in current
            // prev state = (prev_values + prev_removed - prev_added)
            nb::set prev_state;
            for (auto elem : prev_output().value_view()) {
                prev_state.add(elem.to_python());
            }
            for (auto elem : prev_output().removed_view()) {
                prev_state.add(elem.to_python());
            }
            for (auto elem : prev_output().added_view()) {
                auto py_item = elem.to_python();
                if (prev_state.contains(py_item)) {
                    prev_state.discard(py_item);
                }
            }

            // Get current values (empty if no output)
            nb::set current_values;
            if (has_output()) {
                for (auto elem : set_output().value_view()) {
                    current_values.add(elem.to_python());
                }
            }

            nb::set result;
            for (auto item : prev_state) {
                if (!current_values.contains(item)) {
                    result.add(item);
                }
            }
            if (!result.is_none() && nb::len(result) > 0) {
                _add_reset_prev();
            }
            return nb::frozenset(result);
        }

        if (!has_output()) return nb::frozenset();
        if (sampled()) return nb::frozenset();
        return set_output().py_removed();
    }

    nb::object TimeSeriesSetInput::py_value() const {
        return has_output() ? set_output().py_value() : nb::frozenset();
    }

    nb::object TimeSeriesSetInput::py_delta_value() const {
        // Return PythonSetDelta for proper comparison in tests
        auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");
        return PythonSetDelta(py_added(), py_removed());
    }

    bool TimeSeriesSetInput::valid() const {
        // Match Python behavior: TSS input is valid if it was ever sampled (_sample_time > MIN_DT),
        // which allows reading delta values from _prev_output even when unbound.
        // Python: return self._sample_time > MIN_DT or (self.bound and self.output.valid)
        return sample_time() > MIN_DT || BaseTimeSeriesInput::valid();
    }

    size_t TimeSeriesSetInput::size() const {
        return has_output() ? set_output().size() : 0;
    }

    bool TimeSeriesSetInput::empty() const {
        return !has_output() || set_output().empty();
    }

    const TimeSeriesSetOutput &TimeSeriesSetInput::prev_output() const {
        return *_prev_output;
    }

    bool TimeSeriesSetInput::has_prev_output() const {
        return _prev_output != nullptr;
    }

    void TimeSeriesSetInput::reset_prev() {
        _pending_reset_prev = false;
        _prev_output.reset();
    }

    void TimeSeriesSetInput::_add_reset_prev() const {
        if (_pending_reset_prev) { return; }
        _pending_reset_prev = true;
        auto weak_self = std::weak_ptr(std::const_pointer_cast<TimeSeriesInput>(shared_from_this()));
        owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
            if (auto self = weak_self.lock()) {
                static_cast<TimeSeriesSetInput*>(self.get())->reset_prev();
            }
        });
    }

    bool TimeSeriesSetInput::do_bind_output(time_series_output_s_ptr output) {
        if (has_output()) {
            _prev_output = std::dynamic_pointer_cast<TimeSeriesSetOutput>(this->output()->shared_from_this());
            _add_reset_prev();
        }
        return BaseTimeSeriesInput::do_bind_output(std::move(output));
    }

    void TimeSeriesSetInput::do_un_bind_output(bool unbind_refs) {
        if (has_output()) {
            _prev_output = std::dynamic_pointer_cast<TimeSeriesSetOutput>(this->output()->shared_from_this());
            _add_reset_prev();
        }
        BaseTimeSeriesInput::do_un_bind_output(unbind_refs);
    }

}  // namespace hgraph
