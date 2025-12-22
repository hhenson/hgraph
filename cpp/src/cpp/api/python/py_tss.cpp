#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/ts_python_helpers.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/builders/time_series_types/cpp_time_series_builder.h>
#include <hgraph/types/type_api.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph
{
    // PyTimeSeriesSetOutput implementations
    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        nb::object val = value();
        if (val.is_none()) return false;
        // Use Python's __contains__ on the value
        return nb::cast<bool>(val.attr("__contains__")(item));
    }

    size_t PyTimeSeriesSetOutput::size() const {
        return view().set_size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(view().set_size() == 0);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &item) {
        // Get current value and add the item
        nb::object current = value();
        nb::set new_set;
        if (!current.is_none()) {
            new_set = nb::set(current);
        }

        // Check if already in set
        bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item));
        if (in_set) return;  // Already in set, no change

        new_set.add(item);

        // Store via base class
        PyTimeSeriesOutput::set_value(nb::frozenset(new_set));

        // Create SetDelta and cache it
        nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
        nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");

        if (_output) {
            // Get or create cached delta and add to it
            auto cached = ts::get_cached_delta(_output);
            nb::object delta;
            if (!cached.is_none()) {
                // Add the item to the existing delta's added set
                nb::object existing_added = cached.attr("added");
                nb::set new_added(existing_added);
                new_added.add(item);
                delta = PythonSetDelta(nb::frozenset(new_added), cached.attr("removed"));
            } else {
                nb::set single;
                single.add(item);
                delta = PythonSetDelta(nb::frozenset(single), nb::frozenset());
            }
            ts::cache_delta(_output, delta);
            _output->register_delta_reset_callback();
        }

        // Update contains extension for the added item
        nb::set single_item;
        single_item.add(item);
        update_contains_for_keys(single_item);
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &item) {
        // Get current value and remove the item
        nb::object current = value();
        if (current.is_none()) return;

        nb::set new_set(current);

        // Check if in set
        bool in_set = nb::cast<bool>(new_set.attr("__contains__")(item));
        if (!in_set) return;  // Not in set, no change

        new_set.discard(item);

        // Create SetDelta and apply it
        nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
        nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");

        // Store via base class and cache delta
        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        PyTimeSeriesOutput::set_value(nb::frozenset(new_set));

        if (_output) {
            // Get or create cached delta
            auto cached = ts::get_cached_delta(_output);
            nb::object delta;
            if (!cached.is_none()) {
                // Check if item was in the added set - if so, remove from added instead of adding to removed
                nb::object existing_added = cached.attr("added");
                nb::object existing_removed = cached.attr("removed");
                nb::set new_added(existing_added);
                nb::set new_removed(existing_removed);

                bool was_added = nb::cast<bool>(new_added.attr("__contains__")(item));
                if (was_added) {
                    new_added.discard(item);
                } else {
                    new_removed.add(item);
                }
                delta = PythonSetDelta(nb::frozenset(new_added), nb::frozenset(new_removed));
            } else {
                nb::set single;
                single.add(item);
                delta = PythonSetDelta(nb::frozenset(), nb::frozenset(single));
            }
            ts::cache_delta(_output, delta);
            _output->register_delta_reset_callback();
        }

        // Update contains extension for the removed item
        nb::set single_item;
        single_item.add(item);
        update_contains_for_keys(single_item);
    }

    void PyTimeSeriesSetOutput::set_value(nb::object py_value) {
        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        // Get evaluation time
        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // Get the current value as a Python set
        nb::object current_value = value();
        nb::set old_set;
        if (!current_value.is_none()) {
            old_set = nb::set(current_value);
        }

        // Check if this is a SetDelta object
        if (ts::is_set_delta(py_value)) {
            nb::set new_set;
            if (!current_value.is_none()) {
                new_set = nb::set(current_value);
            }

            // Get added and removed from the delta
            nb::object added = py_value.attr("added");
            nb::object removed = py_value.attr("removed");

            // Build filtered added set (only elements not already in set)
            nb::set filtered_added;
            for (auto item : added) {
                nb::object item_obj = nb::cast<nb::object>(item);
                bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                if (!in_set) {
                    filtered_added.add(item_obj);
                    new_set.add(item_obj);
                }
            }

            // Build filtered removed set (only elements that are in set)
            nb::set filtered_removed;
            for (auto item : removed) {
                nb::object item_obj = nb::cast<nb::object>(item);
                bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                if (in_set) {
                    filtered_removed.add(item_obj);
                    new_set.discard(item_obj);
                }
            }

            // Only mark as modified if there were actual changes
            if (len(filtered_added) > 0 || len(filtered_removed) > 0 || !_view.has_value()) {
                // Store the new set value using the base class
                PyTimeSeriesOutput::set_value(nb::frozenset(new_set));

                // Import PythonSetDelta to create the filtered delta for caching
                nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
                nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
                nb::object filtered_delta = PythonSetDelta(
                    nb::frozenset(filtered_added),
                    nb::frozenset(filtered_removed)
                );

                // Cache the filtered delta for delta_value() to return
                if (_output) {
                    ts::cache_delta(_output, filtered_delta);
                    _output->register_delta_reset_callback();
                }

                // Update contains extension for changed keys
                update_contains_for_keys(filtered_added);
                update_contains_for_keys(filtered_removed);
            }
            return;
        }

        // Handle plain set/frozenset - may contain Removed markers
        if (ts::is_python_set(py_value)) {
            nb::set added_set;
            nb::set removed_set;
            nb::set new_set;  // The resulting set value

            // Check if set contains Removed markers
            bool has_removed = ts::set_contains_removed_markers(py_value);

            if (has_removed) {
                // Set with Removed markers - apply delta to current value
                // Start with current value
                if (!current_value.is_none()) {
                    new_set = nb::set(current_value);
                }

                // Process each element
                for (auto item : py_value) {
                    nb::object item_obj = nb::cast<nb::object>(item);

                    if (ts::is_removed_marker(item_obj)) {
                        // Extract the actual item from Removed(item)
                        nb::object actual_item = item_obj.attr("item");
                        // Only add to removed if it's currently in the set
                        bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(actual_item));
                        if (in_set) {
                            removed_set.add(actual_item);
                            new_set.discard(actual_item);
                        }
                    } else {
                        // Regular element - add if not already in set
                        bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                        if (!in_set) {
                            added_set.add(item_obj);
                            new_set.add(item_obj);
                        }
                    }
                }
            } else if (ts::is_python_frozenset(py_value)) {
                // frozenset without Removed markers - treat as REPLACEMENT
                // Python logic for frozenset: self._value = v, added = v - old, removed = old - v
                new_set = nb::set(py_value);  // The new value IS the input set

                // Compute added = input - old (elements in input but not in old)
                for (auto item : py_value) {
                    nb::object item_obj = nb::cast<nb::object>(item);
                    bool in_old = !current_value.is_none() &&
                                  nb::cast<bool>(nb::borrow<nb::object>(current_value).attr("__contains__")(item_obj));
                    if (!in_old) {
                        added_set.add(item_obj);
                    }
                }

                // Compute removed = old - input (elements in old but not in input)
                if (!current_value.is_none()) {
                    for (auto item : current_value) {
                        nb::object item_obj = nb::cast<nb::object>(item);
                        bool in_new = nb::cast<bool>(py_value.attr("__contains__")(item_obj));
                        if (!in_new) {
                            removed_set.add(item_obj);
                        }
                    }
                }
            } else {
                // Regular set without Removed markers - treat as ADDITIONS ONLY
                // Python logic: added = elements not in current value, no removals
                // Start with current value
                if (!current_value.is_none()) {
                    new_set = nb::set(current_value);
                }

                // Add all elements that are not already in the set
                for (auto item : py_value) {
                    nb::object item_obj = nb::cast<nb::object>(item);
                    bool in_set = len(new_set) > 0 && nb::cast<bool>(new_set.attr("__contains__")(item_obj));
                    if (!in_set) {
                        added_set.add(item_obj);
                        new_set.add(item_obj);
                    }
                }
                // removed_set stays empty - no removals for regular sets
            }

            // Only mark as modified if there were actual changes or this is first tick
            if (len(added_set) > 0 || len(removed_set) > 0 || !_view.has_value()) {
                // Store the new set value using the base class
                PyTimeSeriesOutput::set_value(nb::frozenset(new_set));

                // Import PythonSetDelta to create the delta for caching
                nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
                nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
                nb::object delta = PythonSetDelta(
                    nb::frozenset(added_set),
                    nb::frozenset(removed_set)
                );

                // Cache the delta for delta_value() to return
                if (_output) {
                    ts::cache_delta(_output, delta);
                    _output->register_delta_reset_callback();
                }

                // Update contains extension for changed keys
                update_contains_for_keys(added_set);
                update_contains_for_keys(removed_set);
            }
            return;
        }

        // Fall back to base class behavior for other types
        PyTimeSeriesOutput::set_value(std::move(py_value));
    }

    void PyTimeSeriesSetOutput::apply_result(nb::object value) {
        if (value.is_none()) return;
        set_value(std::move(value));
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        // Get from cached delta
        if (!_output) return nb::frozenset();
        auto cached = ts::get_cached_delta(_output);
        if (cached.is_none()) return nb::frozenset();
        return cached.attr("added");
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        // Get from cached delta
        if (!_output) return nb::frozenset();
        auto cached = ts::get_cached_delta(_output);
        if (cached.is_none()) return nb::frozenset();
        return cached.attr("removed");
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        nb::object added_set = added();
        if (added_set.is_none()) return nb::bool_(false);
        return nb::bool_(nb::cast<bool>(added_set.attr("__contains__")(item)));
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        nb::object removed_set = removed();
        if (removed_set.is_none()) return nb::bool_(false);
        return nb::bool_(nb::cast<bool>(removed_set.attr("__contains__")(item)));
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        return nb::str("TSS{...}");
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const {
        return py_str();
    }

    // Feature extension helper: get singleton TSMeta for TS[bool]
    static const TSMeta* get_bool_ts_meta() {
        static const TSMeta* bool_ts_meta = types::ts_type<types::TS<bool>>();
        return bool_ts_meta;
    }

    // Feature extension helper: get singleton OutputBuilder for TS[bool]
    static output_builder_s_ptr get_bool_output_builder() {
        // Use nb::ref with new, matching the pattern in make_output_builder
        static output_builder_s_ptr bool_builder(new CppTimeSeriesOutputBuilder(get_bool_ts_meta()));
        return bool_builder;
    }

    void PyTimeSeriesSetOutput::ensure_contains_extension() {
        if (_contains_extension) return;

        if (!_output) return;  // Need output to create extension

        // Create the value getter function: sets output to True if key is in set
        // We use the ts_python_helpers to safely get the value from the owning output
        auto value_getter = [](const ts::TSOutput& owning, ts::TSOutput& result, const nb::object& key) {
            // Get the set value from the underlying owning output
            // Access the value view directly to avoid observer issues
            auto& owning_ts_value = const_cast<ts::TSOutput&>(owning).underlying();
            auto owning_view = owning_ts_value.view();
            auto vv = owning_view.value_view();
            nb::object set_value = value::value_to_python(vv.data(), vv.schema());

            bool in_set = false;
            if (!set_value.is_none()) {
                in_set = nb::cast<bool>(set_value.attr("__contains__")(key));
            }

            // Set the result output to the boolean value
            // Access the underlying TSValue directly to avoid observer issues
            auto& result_ts_value = result.underlying();
            auto result_view = result_ts_value.view();
            result_view.set<bool>(in_set, owning.last_modified_time());
        };

        _contains_extension = std::make_unique<FeatureOutputExtension<nb::object>>(
            _output,
            get_bool_output_builder(),
            value_getter,
            std::nullopt  // No separate initial value getter
        );
    }

    void PyTimeSeriesSetOutput::update_contains_for_keys(const nb::handle &keys) {
        if (!_contains_extension || !(*_contains_extension)) return;
        if (keys.is_none()) return;

        // Iterate over the keys and update any that are being tracked
        for (auto key : keys) {
            _contains_extension->update(nb::borrow<nb::object>(key));
        }
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) {
        ensure_contains_extension();
        if (!_contains_extension) {
            return nb::none();
        }

        // Get raw pointer from the requester object for tracking
        const void* requester_ptr = requester.ptr();

        auto& output = _contains_extension->create_or_increment(item, requester_ptr);
        if (!output) {
            return nb::none();
        }

        // Wrap the output for Python
        return wrap_output(output.get(), _node);
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) {
        if (!_contains_extension) return;

        const void* requester_ptr = requester.ptr();
        _contains_extension->release(item, requester_ptr);
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() {
        if (!_is_empty_output) {
            if (!_output || !_node) {
                return nb::none();
            }

            // Create a TS[bool] output for the empty state
            _is_empty_output = get_bool_ts_meta()->make_output(_node.get());

            // Initialize with current empty state
            bool is_empty = size() == 0;
            _is_empty_output->view().set<bool>(is_empty, _node->graph() ? _node->graph()->evaluation_time() : MIN_DT);
        }

        return wrap_output(_is_empty_output.get(), _node);
    }

    // PyTimeSeriesSetInput implementations
    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        nb::object val = value();
        if (val.is_none()) return false;
        // Use Python's __contains__ on the value
        return nb::cast<bool>(val.attr("__contains__")(item));
    }

    size_t PyTimeSeriesSetInput::size() const {
        return view().set_size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        return nb::bool_(view().set_size() == 0);
    }

    nb::object PyTimeSeriesSetInput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetInput::added() const {
        // Get from delta_value
        nb::object delta = delta_value();
        if (delta.is_none()) return nb::frozenset();
        if (nb::hasattr(delta, "added")) {
            return delta.attr("added");
        }
        return nb::frozenset();
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        // Get from delta_value
        nb::object delta = delta_value();
        if (delta.is_none()) return nb::frozenset();
        if (nb::hasattr(delta, "removed")) {
            return delta.attr("removed");
        }
        return nb::frozenset();
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        nb::object added_set = added();
        if (added_set.is_none()) return nb::bool_(false);
        return nb::bool_(nb::cast<bool>(added_set.attr("__contains__")(item)));
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        nb::object removed_set = removed();
        if (removed_set.is_none()) return nb::bool_(false);
        return nb::bool_(nb::cast<bool>(removed_set.attr("__contains__")(item)));
    }

    nb::str PyTimeSeriesSetInput::py_str() const {
        return nb::str("TSS{...}");
    }

    nb::str PyTimeSeriesSetInput::py_repr() const {
        return py_str();
    }

    void tss_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput")
            .def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", [](const PyTimeSeriesSetOutput& self) { return self.size(); })
            .def_prop_ro("empty", &PyTimeSeriesSetOutput::empty)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove)
            // Override value setter and apply_result to handle SetDelta objects
            .def_prop_rw("value",
                         [](const PyTimeSeriesSetOutput& self) { return self.value(); },
                         &PyTimeSeriesSetOutput::set_value,
                         nb::arg("value").none())
            .def("apply_result", &PyTimeSeriesSetOutput::apply_result, nb::arg("value").none())
            // These are methods, not properties - match Python API
            .def("values", &PyTimeSeriesSetOutput::values)
            .def("added", &PyTimeSeriesSetOutput::added)
            .def("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed)
            // Feature extension methods
            .def("get_contains_output", &PyTimeSeriesSetOutput::get_contains_output)
            .def("release_contains_output", &PyTimeSeriesSetOutput::release_contains_output)
            .def("is_empty_output", &PyTimeSeriesSetOutput::is_empty_output)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr);

        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", [](const PyTimeSeriesSetInput& self) { return self.size(); })
            .def_prop_ro("empty", &PyTimeSeriesSetInput::empty)
            // These are methods, not properties - match Python API
            .def("values", &PyTimeSeriesSetInput::values)
            .def("added", &PyTimeSeriesSetInput::added)
            .def("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);
    }

}  // namespace hgraph
