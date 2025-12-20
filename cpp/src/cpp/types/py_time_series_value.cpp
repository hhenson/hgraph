//
// Created by Claude on 20/12/2025.
//
// Implementation of Python bindings for HgTimeSeriesValue.
//

#include <hgraph/types/value/py_time_series_value.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::value {

void register_py_time_series_value_with_nanobind(nb::module_& m) {
    // Note: CallableNotifiable is an internal implementation detail, not exposed to Python.
    // Users interact with subscribe/unsubscribe using Python callables directly.

    // Register HgTimeSeriesValue
    nb::class_<PyHgTimeSeriesValue>(m, "HgTimeSeriesValue")
        // Constructor from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
             "Create a time-series value with the given schema, default-constructed.")

        // =====================================================================
        // Basic properties
        // =====================================================================
        .def_prop_ro("valid", &PyHgTimeSeriesValue::valid,
                     "True if this value has a valid schema and storage.")

        .def_prop_ro("schema", &PyHgTimeSeriesValue::schema, nb::rv_policy::reference,
                     "The TypeMeta schema for this value.")

        .def_prop_ro("kind", &PyHgTimeSeriesValue::kind,
                     "The TypeKind of this value (Scalar, List, Set, Dict, Bundle, etc.).")

        .def_prop_ro("type_name", &PyHgTimeSeriesValue::type_name,
                     "The type name string for this value's schema.")

        // =====================================================================
        // Modification tracking
        // =====================================================================
        .def("modified_at", &PyHgTimeSeriesValue::modified_at, "time"_a,
             "Returns True if this value was modified at the given time.")

        .def_prop_ro("last_modified_time", &PyHgTimeSeriesValue::last_modified_time,
                     "The engine time when this value was last modified.")

        .def_prop_ro("has_value", &PyHgTimeSeriesValue::has_value,
                     "True if this value has been set (modified at least once).")

        .def("mark_invalid", &PyHgTimeSeriesValue::mark_invalid,
             "Mark this value as invalid (reset modification tracking).")

        // =====================================================================
        // Value access
        // =====================================================================
        .def_prop_ro("py_value", &PyHgTimeSeriesValue::py_value,
                     "Get the current value as a Python object.")

        .def("set_value", &PyHgTimeSeriesValue::set_value, "value"_a, "time"_a,
             "Set the value from a Python object at the given time.")

        // =====================================================================
        // Bundle field operations
        // =====================================================================
        .def_prop_ro("field_count", &PyHgTimeSeriesValue::field_count,
                     "Number of fields in a Bundle type (0 for non-bundles).")

        .def("field_modified_at", &PyHgTimeSeriesValue::field_modified_at,
             "index"_a, "time"_a,
             "Returns True if the field at index was modified at the given time.")

        .def("get_field", &PyHgTimeSeriesValue::get_field, "index"_a,
             "Get the value of a field by index.")

        .def("get_field_by_name", &PyHgTimeSeriesValue::get_field_by_name, "name"_a,
             "Get the value of a field by name.")

        .def("set_field", &PyHgTimeSeriesValue::set_field,
             "index"_a, "value"_a, "time"_a,
             "Set the value of a field by index at the given time.")

        .def("set_field_by_name", &PyHgTimeSeriesValue::set_field_by_name,
             "name"_a, "value"_a, "time"_a,
             "Set the value of a field by name at the given time.")

        // =====================================================================
        // List element operations
        // =====================================================================
        .def_prop_ro("list_size", &PyHgTimeSeriesValue::list_size,
                     "Number of elements in a List type (0 for non-lists).")

        .def("element_modified_at", &PyHgTimeSeriesValue::element_modified_at,
             "index"_a, "time"_a,
             "Returns True if the element at index was modified at the given time.")

        .def("get_element", &PyHgTimeSeriesValue::get_element, "index"_a,
             "Get the value of an element by index.")

        .def("set_element", &PyHgTimeSeriesValue::set_element,
             "index"_a, "value"_a, "time"_a,
             "Set the value of an element by index at the given time.")

        // =====================================================================
        // Set operations
        // =====================================================================
        .def_prop_ro("set_size", &PyHgTimeSeriesValue::set_size,
                     "Number of elements in a Set type (0 for non-sets).")

        // =====================================================================
        // Dict operations
        // =====================================================================
        .def_prop_ro("dict_size", &PyHgTimeSeriesValue::dict_size,
                     "Number of entries in a Dict type (0 for non-dicts).")

        // =====================================================================
        // String representation
        // =====================================================================
        .def("__str__", &PyHgTimeSeriesValue::to_string)
        .def("__repr__", [](const PyHgTimeSeriesValue& self) {
            return "HgTimeSeriesValue(" + self.type_name() + ", " + self.to_string() + ")";
        })
        .def("to_debug_string", &PyHgTimeSeriesValue::to_debug_string, "time"_a,
             "Get a debug string including modification status at the given time.")

        // =====================================================================
        // Observer/Subscription API
        // =====================================================================
        .def("subscribe", &PyHgTimeSeriesValue::subscribe, "callback"_a,
             "Subscribe a callable to receive notifications when this value is modified.\n\n"
             "The callable receives a single argument: the engine time (datetime) of the modification.\n\n"
             "Example:\n"
             "    notifications = []\n"
             "    ts_value.subscribe(lambda t: notifications.append(t))\n"
             "    ts_value.set_value(42, time=some_time)\n"
             "    assert len(notifications) == 1")

        .def("unsubscribe", &PyHgTimeSeriesValue::unsubscribe, "callback"_a,
             "Unsubscribe a callable from receiving notifications.\n\n"
             "The callable must be the same object that was passed to subscribe().")

        .def_prop_ro("has_subscribers", &PyHgTimeSeriesValue::has_subscribers,
                     "True if this value has any subscribers registered.")

        .def_prop_ro("subscriber_count", &PyHgTimeSeriesValue::subscriber_count,
                     "Number of subscribers currently registered.");
}

} // namespace hgraph::value
