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

    // value() returns current set value (removed elements are already removed from storage)
    nb::object PyTimeSeriesSetOutput::value() const {
        // Use base class implementation - elements are removed immediately now
        return PyTimeSeriesOutput::value();
    }

    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        nb::object val = value();
        if (val.is_none()) return false;
        // Use Python's __contains__ on the value
        return nb::cast<bool>(val.attr("__contains__")(item));
    }

    size_t PyTimeSeriesSetOutput::size() const {
        // Elements are removed immediately now, just return storage size
        return _view.set_size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(size() == 0);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &item) {
        if (!_view.valid() || _view.kind() != value::TypeKind::Set) return;

        // Get element schema from set type
        auto* set_meta = static_cast<const value::SetTypeMeta*>(_view.schema());
        auto* elem_schema = set_meta->element_type;

        // Create typed Value for element and convert from Python
        value::Value elem_value(elem_schema);
        if (elem_schema->ops && elem_schema->ops->from_python) {
            elem_schema->ops->from_python(elem_value.data(), item.ptr(), elem_schema);
        }

        // Add to set storage - core handles storage update
        auto* set_storage = static_cast<value::SetStorage*>(_view.value_view().data());
        auto [added, index] = set_storage->add_with_index(elem_value.data());

        if (added) {
            // Use core modification tracking
            auto time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
            _view.tracker().mark_set_element_added(index, time);
            _view.mark_modified(time);

            if (_output) {
                _output->register_delta_reset_callback();

                // Update Python delta cache - add to 'added' set
                nb::set added_set;
                nb::set removed_set;

                auto cached = ts::get_cached_delta(_output);
                if (!cached.is_none()) {
                    // Create mutable sets from the frozensets in the cached delta
                    for (auto elem : cached.attr("added")) {
                        added_set.add(nb::borrow<nb::object>(elem));
                    }
                    for (auto elem : cached.attr("removed")) {
                        removed_set.add(nb::borrow<nb::object>(elem));
                    }
                }

                added_set.add(item);

                // Create updated delta and cache it
                nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
                nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
                nb::object delta = PythonSetDelta(
                    nb::frozenset(added_set),
                    nb::frozenset(removed_set)
                );
                ts::cache_delta(_output, delta);
            }

            // Update contains extension (wrapper feature for Python API)
            update_contains_for_item(item, true);
        }
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &item) {
        if (!_view.valid() || _view.kind() != value::TypeKind::Set) return;

        // Get element schema from set type
        auto* set_meta = static_cast<const value::SetTypeMeta*>(_view.schema());
        auto* elem_schema = set_meta->element_type;

        // Create typed Value for element and convert from Python
        value::Value elem_value(elem_schema);
        if (elem_schema->ops && elem_schema->ops->from_python) {
            elem_schema->ops->from_python(elem_value.data(), item.ptr(), elem_schema);
        }

        // Find element's index before removal
        auto* set_storage = static_cast<value::SetStorage*>(_view.value_view().data());
        auto opt_index = set_storage->find_index(elem_value.data());
        if (!opt_index) return;  // Not in set

        size_t index = *opt_index;
        auto time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // Check if element was added this tick - core handles add-then-remove cancellation
        bool was_added_this_tick = _view.tracker().set_element_added_at(index, time);

        if (was_added_this_tick) {
            // Add-then-remove same tick: cancel out, don't record as removed
            _view.tracker().remove_set_element_tracking(index);
        } else {
            // Element existed before tick: record for delta access
            _view.tracker().record_set_removal(elem_value.data(), time);
        }

        // Remove from storage
        set_storage->remove(elem_value.data());

        _view.mark_modified(time);
        if (_output) {
            _output->register_delta_reset_callback();

            // Update Python delta cache
            nb::set added_set;
            nb::set removed_set;

            auto cached = ts::get_cached_delta(_output);
            if (!cached.is_none()) {
                // Create mutable sets from the frozensets in the cached delta
                for (auto elem : cached.attr("added")) {
                    added_set.add(nb::borrow<nb::object>(elem));
                }
                for (auto elem : cached.attr("removed")) {
                    removed_set.add(nb::borrow<nb::object>(elem));
                }
            }

            if (was_added_this_tick) {
                // Add-then-remove same tick: remove from 'added', don't add to 'removed'
                // Use Python's discard to handle the case where item might not be in set
                bool in_added = len(added_set) > 0 && nb::cast<bool>(added_set.attr("__contains__")(item));
                if (in_added) {
                    added_set.discard(item);
                }
            } else {
                // Element existed before tick: add to 'removed'
                removed_set.add(item);
            }

            // Create updated delta and cache it
            nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
            nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
            nb::object delta = PythonSetDelta(
                nb::frozenset(added_set),
                nb::frozenset(removed_set)
            );
            ts::cache_delta(_output, delta);
        }

        // Update contains extension (wrapper feature for Python API)
        update_contains_for_item(item, false);
    }

    void PyTimeSeriesSetOutput::set_value(nb::object py_value) {
        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        if (!_view.valid() || _view.kind() != value::TypeKind::Set) {
            // Fall back to base class for non-set types
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        // Get element schema for Python-to-C++ conversion
        auto* set_meta = static_cast<const value::SetTypeMeta*>(_view.schema());
        auto* elem_schema = set_meta->element_type;
        auto* set_storage = static_cast<value::SetStorage*>(_view.value_view().data());

        auto time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // Get current value as Python set for delta computation
        nb::object current_value = value();

        // Track what was added and removed for Python delta
        nb::set py_added;
        nb::set py_removed;
        bool had_changes = false;

        // Helper lambda to add element using core storage
        auto add_element = [&](const nb::object& item) -> bool {
            value::Value elem_value(elem_schema);
            if (elem_schema->ops && elem_schema->ops->from_python) {
                elem_schema->ops->from_python(elem_value.data(), item.ptr(), elem_schema);
            }
            auto [added, index] = set_storage->add_with_index(elem_value.data());
            if (added) {
                _view.tracker().mark_set_element_added(index, time);
                py_added.add(item);
                return true;
            }
            return false;
        };

        // Helper lambda to remove element using core storage
        auto remove_element = [&](const nb::object& item) -> bool {
            value::Value elem_value(elem_schema);
            if (elem_schema->ops && elem_schema->ops->from_python) {
                elem_schema->ops->from_python(elem_value.data(), item.ptr(), elem_schema);
            }
            auto opt_index = set_storage->find_index(elem_value.data());
            if (!opt_index) return false;

            size_t index = *opt_index;
            bool was_added_this_tick = _view.tracker().set_element_added_at(index, time);

            if (was_added_this_tick) {
                _view.tracker().remove_set_element_tracking(index);
                // Remove from py_added if it was added this tick
                bool in_added = len(py_added) > 0 && nb::cast<bool>(py_added.attr("__contains__")(item));
                if (in_added) {
                    py_added.discard(item);
                }
            } else {
                _view.tracker().record_set_removal(elem_value.data(), time);
                py_removed.add(item);
            }

            set_storage->remove(elem_value.data());
            return true;
        };

        // Handle SetDelta object
        if (ts::is_set_delta(py_value)) {
            nb::object added = py_value.attr("added");
            nb::object removed = py_value.attr("removed");

            // Process added elements
            for (auto item : added) {
                nb::object item_obj = nb::borrow<nb::object>(item);
                if (add_element(item_obj)) {
                    had_changes = true;
                }
            }

            // Process removed elements
            for (auto item : removed) {
                nb::object item_obj = nb::borrow<nb::object>(item);
                if (remove_element(item_obj)) {
                    had_changes = true;
                }
            }
        }
        // Handle plain set/frozenset
        else if (ts::is_python_set(py_value)) {
            bool has_removed = ts::set_contains_removed_markers(py_value);

            if (has_removed) {
                // Set with Removed markers - apply as delta
                for (auto item : py_value) {
                    nb::object item_obj = nb::borrow<nb::object>(item);

                    if (ts::is_removed_marker(item_obj)) {
                        nb::object actual_item = item_obj.attr("item");
                        if (remove_element(actual_item)) {
                            had_changes = true;
                        }
                    } else {
                        if (add_element(item_obj)) {
                            had_changes = true;
                        }
                    }
                }
            } else if (ts::is_python_frozenset(py_value)) {
                // frozenset - REPLACEMENT semantics
                // First, remove elements not in new set
                if (!current_value.is_none()) {
                    // Collect elements to remove (can't modify while iterating)
                    std::vector<nb::object> to_remove;
                    for (auto item : current_value) {
                        nb::object item_obj = nb::borrow<nb::object>(item);
                        bool in_new = nb::cast<bool>(py_value.attr("__contains__")(item_obj));
                        if (!in_new) {
                            to_remove.push_back(item_obj);
                        }
                    }
                    for (const auto& item : to_remove) {
                        if (remove_element(item)) {
                            had_changes = true;
                        }
                    }
                }

                // Then add new elements
                for (auto item : py_value) {
                    nb::object item_obj = nb::borrow<nb::object>(item);
                    if (add_element(item_obj)) {
                        had_changes = true;
                    }
                }
            } else {
                // Regular set - ADDITIONS ONLY
                for (auto item : py_value) {
                    nb::object item_obj = nb::borrow<nb::object>(item);
                    if (add_element(item_obj)) {
                        had_changes = true;
                    }
                }
            }
        } else {
            // Unknown type - fall back to base class
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        // Mark as modified if there were changes or this is first value
        if (had_changes || !_view.has_value()) {
            _view.mark_modified(time);

            if (_output) {
                _output->register_delta_reset_callback();

                // Cache Python delta for delta_value()
                nb::module_ tss_module = nb::module_::import_("hgraph._impl._types._tss");
                nb::object PythonSetDelta = tss_module.attr("PythonSetDelta");
                nb::object delta = PythonSetDelta(
                    nb::frozenset(py_added),
                    nb::frozenset(py_removed)
                );
                ts::cache_delta(_output, delta);
            }

            // Update contains extension for changed keys
            update_contains_for_keys(py_added);
            update_contains_for_keys(py_removed);
        }
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
    // Uses compile-time type API which goes through the registry for deduplication.
    // With HGRAPH_TYPE_API_WITH_PYTHON defined (via CMake), type_of<T>() returns
    // Python-aware TypeMeta, ensuring consistency with runtime APIs.
    static const TSMeta* get_bool_ts_meta() {
        return types::ts_type<types::TS<bool>>();
    }

    // Feature extension helper: get singleton OutputBuilder for TS[bool]
    static output_builder_s_ptr get_bool_output_builder() {
        // Use nb::ref with new, matching the pattern in make_output_builder
        static output_builder_s_ptr bool_builder(new CppTimeSeriesOutputBuilder(get_bool_ts_meta()));
        return bool_builder;
    }

    void PyTimeSeriesSetOutput::ensure_contains_extension() {
        if (!_output) return;  // Need output to create extension

        // Check if extension already exists (either on wrapper or in cache)
        auto* cache = _output->python_cache();
        if (cache->tss_contains_extension) {
            // Extension already exists in cache - retrieve it for wrapper if needed
            if (!_contains_extension) {
                _contains_extension.reset(static_cast<FeatureOutputExtension<nb::object>*>(
                    cache->tss_contains_extension.get()));
                // Don't take ownership - the cache owns it
                _contains_extension.release();
            }
            return;
        }

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

            // Get the underlying TSValue from the result output directly
            // Use the modification time as the source of truth for whether there's a current value
            auto& result_value = result.underlying();
            bool has_val = result_value.has_value();

            // Read current value from the last known state if available
            // Note: Due to an issue with value storage pointer changes, we track
            // the value state through the modification tracker instead
            bool old_value = false;
            if (has_val) {
                // If we have a previous value, try to read it
                auto result_view = result_value.view();
                auto result_vv = result_view.value_view();
                if (result_vv.valid() && result_vv.data()) {
                    old_value = *static_cast<bool*>(result_vv.data());
                }
            }

            bool value_changed = (old_value != in_set) || !has_val;

            if (value_changed) {
                // Get a fresh view for writing
                auto result_view = result_value.view();
                auto result_vv = result_view.value_view();

                if (result_vv.valid() && result_vv.data()) {
                    // Write value directly
                    *static_cast<bool*>(result_vv.data()) = in_set;
                    // Mark modified via the view's tracker
                    result_view.mark_modified(owning.last_modified_time());
                }
            }
        };

        // Create the extension as a shared_ptr so it can be stored in the cache
        auto ext = std::make_shared<FeatureOutputExtension<nb::object>>(
            _output,
            get_bool_output_builder(),
            value_getter,
            std::nullopt  // No separate initial value getter
        );

        // Store in cache for set_python_value to access
        cache->tss_contains_extension = ext;

        // Create callback for set_python_value to use
        cache->tss_update_contains_for_keys = [ext](const nb::handle& keys) {
            if (!ext || keys.is_none()) return;
            for (auto key : keys) {
                ext->update(nb::borrow<nb::object>(key));
            }
        };

        // Store raw pointer for wrapper (doesn't own)
        _contains_extension = std::make_unique<FeatureOutputExtension<nb::object>>(
            _output,
            get_bool_output_builder(),
            value_getter,
            std::nullopt
        );
        // Actually, just use the shared one directly - we'll access through the cache
        _contains_extension.reset();
    }

    void PyTimeSeriesSetOutput::update_contains_for_keys(const nb::handle &keys) {
        if (!_output) return;

        // Use the callback stored in the cache
        auto* cache = _output->python_cache();

        if (!cache->tss_update_contains_for_keys) {
            return;
        }
        if (keys.is_none()) {
            return;
        }

        // Use the cached callback to update
        cache->tss_update_contains_for_keys(keys);
    }

    void PyTimeSeriesSetOutput::update_contains_for_item(const nb::object &item, bool added) {
        if (!_output) return;

        // Get the extension from the cache
        auto* cache = _output->python_cache();
        if (!cache->tss_contains_extension) return;

        auto* ext = static_cast<FeatureOutputExtension<nb::object>*>(cache->tss_contains_extension.get());
        ext->update(item);
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) {
        ensure_contains_extension();

        if (!_output) return nb::none();

        // Get the extension from the cache
        auto* cache = _output->python_cache();
        if (!cache->tss_contains_extension) {
            return nb::none();
        }

        auto* ext = static_cast<FeatureOutputExtension<nb::object>*>(cache->tss_contains_extension.get());

        // Get raw pointer from the requester object for tracking
        const void* requester_ptr = requester.ptr();

        auto& output = ext->create_or_increment(item, requester_ptr);
        if (!output) {
            return nb::none();
        }

        // Wrap the output for Python
        return wrap_output(output.get(), _node);
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) {
        if (!_output) return;

        // Get the extension from the cache
        auto* cache = _output->python_cache();
        if (!cache->tss_contains_extension) return;

        auto* ext = static_cast<FeatureOutputExtension<nb::object>*>(cache->tss_contains_extension.get());

        const void* requester_ptr = requester.ptr();
        ext->release(item, requester_ptr);
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
