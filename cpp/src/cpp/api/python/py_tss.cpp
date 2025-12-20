#include <hgraph/api/python/py_tss.h>
#include <hgraph/types/time_series/ts_python_helpers.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph
{
    // Helper to check if an object is a SetDelta (has 'added' and 'removed' attributes)
    static bool is_set_delta(const nb::object& obj) {
        return nb::hasattr(obj, "added") && nb::hasattr(obj, "removed");
    }

    // PyTimeSeriesSetOutput implementations
    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        // TODO: Implement via view
        return false;
    }

    size_t PyTimeSeriesSetOutput::size() const {
        return view().set_size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(view().set_size() == 0);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &item) {
        // TODO: Implement via view with proper type conversion
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &item) {
        // TODO: Implement via view with proper type conversion
    }

    // Helper to check if an object is a set or frozenset
    static bool is_python_set(const nb::object& obj) {
        return nb::isinstance<nb::set>(obj) || nb::isinstance<nb::frozenset>(obj);
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
        if (is_set_delta(py_value)) {
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
            }
            return;
        }

        // Handle plain set/frozenset - treat as full replacement
        if (is_python_set(py_value)) {
            nb::set new_set_obj(py_value);

            // Compute delta: added = new - old, removed = old - new
            nb::set added_set;
            nb::set removed_set;

            // Find added elements (in new but not in old)
            for (auto item : new_set_obj) {
                nb::object item_obj = nb::cast<nb::object>(item);
                bool in_old = len(old_set) > 0 && nb::cast<bool>(old_set.attr("__contains__")(item_obj));
                if (!in_old) {
                    added_set.add(item_obj);
                }
            }

            // Find removed elements (in old but not in new)
            for (auto item : old_set) {
                nb::object item_obj = nb::cast<nb::object>(item);
                bool in_new = nb::cast<bool>(new_set_obj.attr("__contains__")(item_obj));
                if (!in_new) {
                    removed_set.add(item_obj);
                }
            }

            // Only mark as modified if there were actual changes or this is first tick
            if (len(added_set) > 0 || len(removed_set) > 0 || !_view.has_value()) {
                // Store the new set value using the base class
                PyTimeSeriesOutput::set_value(nb::frozenset(new_set_obj));

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
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        return nb::str("TSS{...}");
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesSetInput implementations
    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        // TODO: Implement via view
        return false;
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
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        // TODO: Implement delta tracking
        return nb::frozenset();
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        return nb::bool_(false);
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
            .def_prop_ro("values", &PyTimeSeriesSetOutput::values)
            .def_prop_ro("added", &PyTimeSeriesSetOutput::added)
            .def_prop_ro("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr);

        nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput")
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", [](const PyTimeSeriesSetInput& self) { return self.size(); })
            .def_prop_ro("empty", &PyTimeSeriesSetInput::empty)
            .def_prop_ro("values", &PyTimeSeriesSetInput::values)
            .def_prop_ro("added", &PyTimeSeriesSetInput::added)
            .def_prop_ro("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);
    }

}  // namespace hgraph
