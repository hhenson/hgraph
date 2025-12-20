//
// Created by Claude on 20/12/2025.
//
// Implementation of Python bindings for TSOutput and TSOutputView.
//

#include <hgraph/types/time_series/py_ts_output.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::ts {

void register_py_ts_output_with_nanobind(nb::module_& m) {
    // =========================================================================
    // TSOutputView
    // =========================================================================
    nb::class_<PyTSOutputView>(m, "TSOutputView")
        // Basic properties
        .def_prop_ro("valid", &PyTSOutputView::valid,
                     "True if this view has valid storage.")

        .def_prop_ro("meta", &PyTSOutputView::meta, nb::rv_policy::reference,
                     "The TimeSeriesTypeMeta for this view.")

        .def_prop_ro("value_schema", &PyTSOutputView::value_schema, nb::rv_policy::reference,
                     "The underlying value TypeMeta schema.")

        .def_prop_ro("kind", &PyTSOutputView::kind,
                     "The TypeKind of this view (Scalar, List, Set, Dict, Bundle, etc.).")

        .def_prop_ro("ts_kind", &PyTSOutputView::ts_kind,
                     "The TimeSeriesKind of this view (TS, TSS, TSD, TSL, TSB, TSW, REF).")

        .def_prop_ro("type_name", &PyTSOutputView::type_name,
                     "The type name string for this view.")

        // Path tracking
        .def_prop_ro("path", &PyTSOutputView::path_string,
                     "The navigation path from root to this view.")

        // Modification tracking
        .def("modified_at", &PyTSOutputView::modified_at, "time"_a,
             "Returns True if this view was modified at the given time.")

        .def_prop_ro("last_modified_time", &PyTSOutputView::last_modified_time,
                     "The engine time when this view was last modified.")

        .def_prop_ro("has_value", &PyTSOutputView::has_value,
                     "True if this view has been set (modified at least once).")

        .def("mark_modified", &PyTSOutputView::mark_modified, "time"_a,
             "Mark this view as modified at the given time.")

        .def("mark_invalid", &PyTSOutputView::mark_invalid,
             "Mark this view as invalid (reset modification tracking).")

        // Value access
        .def_prop_ro("py_value", &PyTSOutputView::py_value,
                     "Get the current value as a Python object.")

        .def("set_value", &PyTSOutputView::set_value, "value"_a, "time"_a,
             "Set the value from a Python object at the given time.")

        // Bundle field navigation
        .def("field", &PyTSOutputView::field, "index"_a,
             "Navigate to a bundle field by index. Returns a view for that field.")

        .def("field_by_name", &PyTSOutputView::field_by_name, "name"_a,
             "Navigate to a bundle field by name. Returns a view for that field.")

        .def("field_modified_at", &PyTSOutputView::field_modified_at, "index"_a, "time"_a,
             "Returns True if the field at index was modified at the given time.")

        .def_prop_ro("field_count", &PyTSOutputView::field_count,
                     "Number of fields in a Bundle type (0 for non-bundles).")

        // List element navigation
        .def("element", &PyTSOutputView::element, "index"_a,
             "Navigate to a list element by index. Returns a view for that element.")

        .def("element_modified_at", &PyTSOutputView::element_modified_at, "index"_a, "time"_a,
             "Returns True if the element at index was modified at the given time.")

        .def_prop_ro("list_size", &PyTSOutputView::list_size,
                     "Number of elements in a List type (0 for non-lists).")

        // Set operations
        .def_prop_ro("set_size", &PyTSOutputView::set_size,
                     "Number of elements in a Set type (0 for non-sets).")

        // Dict operations
        .def_prop_ro("dict_size", &PyTSOutputView::dict_size,
                     "Number of entries in a Dict type (0 for non-dicts).")

        // Window operations
        .def_prop_ro("window_size", &PyTSOutputView::window_size,
                     "Number of elements in a Window type (0 for non-windows).")

        .def_prop_ro("window_empty", &PyTSOutputView::window_empty,
                     "True if the window is empty.")

        .def_prop_ro("window_full", &PyTSOutputView::window_full,
                     "True if the window is at capacity.")

        .def("window_get", &PyTSOutputView::window_get, "index"_a,
             "Get the value at the given window index.")

        .def("window_timestamp", &PyTSOutputView::window_timestamp, "index"_a,
             "Get the timestamp at the given window index.")

        .def("window_clear", &PyTSOutputView::window_clear, "time"_a,
             "Clear the window at the given time.")

        // Ref operations
        .def_prop_ro("ref_is_empty", &PyTSOutputView::ref_is_empty,
                     "True if this REF is empty (uninitialized).")

        .def_prop_ro("ref_is_bound", &PyTSOutputView::ref_is_bound,
                     "True if this REF is bound to a target.")

        .def_prop_ro("ref_is_valid", &PyTSOutputView::ref_is_valid,
                     "True if this REF is valid (bound and target exists).")

        .def("ref_clear", &PyTSOutputView::ref_clear, "time"_a,
             "Clear the REF binding at the given time.")

        // String representation
        .def("__str__", &PyTSOutputView::to_string)
        .def("__repr__", [](const PyTSOutputView& self) {
            return "TSOutputView(" + self.type_name() + ", path=" + self.path_string() + ")";
        })
        .def("to_debug_string", &PyTSOutputView::to_debug_string, "time"_a,
             "Get a debug string including modification status at the given time.");

    // =========================================================================
    // TSOutput
    // =========================================================================
    nb::class_<PyTSOutput>(m, "TSOutput")
        // Constructor
        .def(nb::init<const TimeSeriesTypeMeta*>(), "meta"_a,
             "Create a TSOutput with the given TimeSeriesTypeMeta. Node is nullptr for testing.")

        // Basic properties
        .def_prop_ro("valid", &PyTSOutput::valid,
                     "True if this output has valid storage.")

        .def_prop_ro("meta", &PyTSOutput::meta, nb::rv_policy::reference,
                     "The TimeSeriesTypeMeta for this output.")

        .def_prop_ro("value_schema", &PyTSOutput::value_schema, nb::rv_policy::reference,
                     "The underlying value TypeMeta schema.")

        .def_prop_ro("kind", &PyTSOutput::kind,
                     "The TypeKind of this output (Scalar, List, Set, Dict, Bundle, etc.).")

        .def_prop_ro("ts_kind", &PyTSOutput::ts_kind,
                     "The TimeSeriesKind of this output (TS, TSS, TSD, TSL, TSB, TSW, REF).")

        .def_prop_ro("type_name", &PyTSOutput::type_name,
                     "The type name string for this output.")

        // View creation
        .def("view", &PyTSOutput::view,
             "Get a view for fluent navigation and mutation.")

        // Modification tracking
        .def("modified_at", &PyTSOutput::modified_at, "time"_a,
             "Returns True if this output was modified at the given time.")

        .def_prop_ro("last_modified_time", &PyTSOutput::last_modified_time,
                     "The engine time when this output was last modified.")

        .def_prop_ro("has_value", &PyTSOutput::has_value,
                     "True if this output has been set (modified at least once).")

        .def("mark_invalid", &PyTSOutput::mark_invalid,
             "Mark this output as invalid (reset modification tracking).")

        // Direct value access
        .def_prop_ro("py_value", &PyTSOutput::py_value,
                     "Get the current value as a Python object.")

        .def("set_value", &PyTSOutput::set_value, "value"_a, "time"_a,
             "Set the value from a Python object at the given time.")

        // Observer support
        .def_prop_ro("has_observers", &PyTSOutput::has_observers,
                     "True if this output has any observers registered.")

        // String representation
        .def("__str__", &PyTSOutput::to_string)
        .def("__repr__", [](const PyTSOutput& self) {
            return "TSOutput(" + self.type_name() + ")";
        })
        .def("to_debug_string", &PyTSOutput::to_debug_string, "time"_a,
             "Get a debug string including modification status at the given time.");
}

} // namespace hgraph::ts
