//
// Created by Claude on 20/12/2025.
//
// Implementation of Python bindings for HgTSValue and HgTSView.
//

#include <hgraph/types/value/py_time_series_value.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::value {

void register_py_time_series_value_with_nanobind(nb::module_& m) {
    // Register HgTSView first (since HgTSValue returns it)
    nb::class_<PyHgTSView>(m, "HgTSView")
        // =====================================================================
        // Basic properties
        // =====================================================================
        .def_prop_ro("valid", &PyHgTSView::valid,
                     "True if this view has a valid schema and storage.")

        .def_prop_ro("schema", &PyHgTSView::schema, nb::rv_policy::reference,
                     "The TypeMeta schema for this view.")

        .def_prop_ro("kind", &PyHgTSView::kind,
                     "The TypeKind of this view (Scalar, List, Set, Dict, Bundle, etc.).")

        .def_prop_ro("type_name", &PyHgTSView::type_name,
                     "The type name string for this view's schema.")

        // =====================================================================
        // Modification tracking
        // =====================================================================
        .def("modified_at", &PyHgTSView::modified_at, "time"_a,
             "Returns True if this view was modified at the given time.")

        .def_prop_ro("last_modified_time", &PyHgTSView::last_modified_time,
                     "The engine time when this view was last modified.")

        .def_prop_ro("has_value", &PyHgTSView::has_value,
                     "True if this view has been set (modified at least once).")

        // =====================================================================
        // Value access
        // =====================================================================
        .def_prop_ro("py_value", &PyHgTSView::py_value,
                     "Get the current value as a Python object.")

        .def("set_value", &PyHgTSView::set_value, "value"_a, "time"_a,
             "Set the value from a Python object at the given time.")

        // =====================================================================
        // Navigation - Bundle fields (fluent API)
        // =====================================================================
        .def("field", nb::overload_cast<size_t>(&PyHgTSView::field),
             "index"_a,
             "Navigate to a bundle field by index. Returns a view for that field.\n\n"
             "Example:\n"
             "    field_view = ts_value.view().field(0)\n"
             "    field_view.set_value(42, time=T100)")

        .def("field", nb::overload_cast<const std::string&>(&PyHgTSView::field),
             "name"_a,
             "Navigate to a bundle field by name. Returns a view for that field.\n\n"
             "Example:\n"
             "    field_view = ts_value.view().field('x')\n"
             "    field_view.subscribe(callback)")

        // =====================================================================
        // Navigation - List elements (fluent API)
        // =====================================================================
        .def("element", &PyHgTSView::element, "index"_a,
             "Navigate to a list element by index. Returns a view for that element.\n\n"
             "Example:\n"
             "    elem_view = ts_value.view().element(0)\n"
             "    elem_view.subscribe(callback)")

        // =====================================================================
        // Navigation - Dict entries (fluent API)
        // =====================================================================
        .def("key", &PyHgTSView::key, "key"_a,
             "Navigate to a dict entry by key. Returns a view for that entry.\n\n"
             "The key can be any Python value matching the dict's key type.\n\n"
             "Example:\n"
             "    entry_view = ts_value.view().key('a')\n"
             "    entry_view.subscribe(callback)")

        // =====================================================================
        // Subscription (fluent API)
        // =====================================================================
        .def("subscribe", &PyHgTSView::subscribe, "callback"_a,
             nb::rv_policy::reference,
             "Subscribe a callable to receive notifications when this view is modified.\n\n"
             "Returns self for fluent chaining.\n\n"
             "Example:\n"
             "    ts_value.view().field(0).subscribe(callback).set_value(42, time=T100)")

        .def("unsubscribe", &PyHgTSView::unsubscribe, "callback"_a,
             nb::rv_policy::reference,
             "Unsubscribe a callable from receiving notifications.\n\n"
             "Returns self for fluent chaining.")

        // =====================================================================
        // Size queries
        // =====================================================================
        .def_prop_ro("field_count", &PyHgTSView::field_count,
                     "Number of fields in a Bundle type (0 for non-bundles).")

        .def_prop_ro("list_size", &PyHgTSView::list_size,
                     "Number of elements in a List type (0 for non-lists).")

        .def_prop_ro("dict_size", &PyHgTSView::dict_size,
                     "Number of entries in a Dict type (0 for non-dicts).")

        .def_prop_ro("set_size", &PyHgTSView::set_size,
                     "Number of elements in a Set type (0 for non-sets).")

        // =====================================================================
        // String representation
        // =====================================================================
        .def("__str__", &PyHgTSView::to_string)
        .def("__repr__", [](const PyHgTSView& self) {
            return "HgTSView(" + self.type_name() + ", " + self.to_string() + ")";
        })
        .def("to_debug_string", &PyHgTSView::to_debug_string, "time"_a,
             "Get a debug string including modification status at the given time.");

    // Register HgTSValue
    nb::class_<PyHgTSValue>(m, "HgTSValue")
        // Constructor from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
             "Create a time-series value with the given schema, default-constructed.")

        // =====================================================================
        // Basic properties
        // =====================================================================
        .def_prop_ro("valid", &PyHgTSValue::valid,
                     "True if this value has a valid schema and storage.")

        .def_prop_ro("schema", &PyHgTSValue::schema, nb::rv_policy::reference,
                     "The TypeMeta schema for this value.")

        .def_prop_ro("kind", &PyHgTSValue::kind,
                     "The TypeKind of this value (Scalar, List, Set, Dict, Bundle, etc.).")

        .def_prop_ro("type_name", &PyHgTSValue::type_name,
                     "The type name string for this value's schema.")

        // =====================================================================
        // Modification tracking
        // =====================================================================
        .def("modified_at", &PyHgTSValue::modified_at, "time"_a,
             "Returns True if this value was modified at the given time.")

        .def_prop_ro("last_modified_time", &PyHgTSValue::last_modified_time,
                     "The engine time when this value was last modified.")

        .def_prop_ro("has_value", &PyHgTSValue::has_value,
                     "True if this value has been set (modified at least once).")

        .def("mark_invalid", &PyHgTSValue::mark_invalid,
             "Mark this value as invalid (reset modification tracking).")

        // =====================================================================
        // Value access
        // =====================================================================
        .def_prop_ro("py_value", &PyHgTSValue::py_value,
                     "Get the current value as a Python object.")

        .def("set_value", &PyHgTSValue::set_value, "value"_a, "time"_a,
             "Set the value from a Python object at the given time.")

        // =====================================================================
        // Fluent View API
        // =====================================================================
        .def("view", &PyHgTSValue::view,
             "Get a view for fluent navigation and subscription.\n\n"
             "Example:\n"
             "    ts_value.view().subscribe(callback)  # Root subscription\n"
             "    ts_value.view().field(0).subscribe(callback)  # Field subscription\n"
             "    ts_value.view().field('x').set_value(42, time=T100)")

        // =====================================================================
        // Bundle field operations (legacy API)
        // =====================================================================
        .def_prop_ro("field_count", &PyHgTSValue::field_count,
                     "Number of fields in a Bundle type (0 for non-bundles).")

        .def("field_modified_at", &PyHgTSValue::field_modified_at,
             "index"_a, "time"_a,
             "Returns True if the field at index was modified at the given time.")

        .def("get_field", &PyHgTSValue::get_field, "index"_a,
             "Get the value of a field by index.")

        .def("get_field_by_name", &PyHgTSValue::get_field_by_name, "name"_a,
             "Get the value of a field by name.")

        .def("set_field", &PyHgTSValue::set_field,
             "index"_a, "value"_a, "time"_a,
             "Set the value of a field by index at the given time.")

        .def("set_field_by_name", &PyHgTSValue::set_field_by_name,
             "name"_a, "value"_a, "time"_a,
             "Set the value of a field by name at the given time.")

        // =====================================================================
        // List element operations (legacy API)
        // =====================================================================
        .def_prop_ro("list_size", &PyHgTSValue::list_size,
                     "Number of elements in a List type (0 for non-lists).")

        .def("element_modified_at", &PyHgTSValue::element_modified_at,
             "index"_a, "time"_a,
             "Returns True if the element at index was modified at the given time.")

        .def("get_element", &PyHgTSValue::get_element, "index"_a,
             "Get the value of an element by index.")

        .def("set_element", &PyHgTSValue::set_element,
             "index"_a, "value"_a, "time"_a,
             "Set the value of an element by index at the given time.")

        // =====================================================================
        // Set operations
        // =====================================================================
        .def_prop_ro("set_size", &PyHgTSValue::set_size,
                     "Number of elements in a Set type (0 for non-sets).")

        // =====================================================================
        // Dict operations
        // =====================================================================
        .def_prop_ro("dict_size", &PyHgTSValue::dict_size,
                     "Number of entries in a Dict type (0 for non-dicts).")

        // =====================================================================
        // String representation
        // =====================================================================
        .def("__str__", &PyHgTSValue::to_string)
        .def("__repr__", [](const PyHgTSValue& self) {
            return "HgTSValue(" + self.type_name() + ", " + self.to_string() + ")";
        })
        .def("to_debug_string", &PyHgTSValue::to_debug_string, "time"_a,
             "Get a debug string including modification status at the given time.")

        // =====================================================================
        // Observer/Subscription API (legacy, for backwards compatibility)
        // =====================================================================
        .def("subscribe", &PyHgTSValue::subscribe, "callback"_a,
             "Subscribe a callable to receive notifications when this value is modified.\n\n"
             "Equivalent to: ts_value.view().subscribe(callback)")

        .def("unsubscribe", &PyHgTSValue::unsubscribe, "callback"_a,
             "Unsubscribe a callable from receiving notifications.")

        .def_prop_ro("has_subscribers", &PyHgTSValue::has_subscribers,
                     "True if this value has any subscribers registered.")

        .def_prop_ro("subscriber_count", &PyHgTSValue::subscriber_count,
                     "Number of subscribers currently registered.");
}

} // namespace hgraph::value
