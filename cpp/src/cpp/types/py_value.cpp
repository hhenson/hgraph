//
// Created by Claude on 19/12/2024.
//
// Implementation of Python bindings for HgValue.
//

#include <hgraph/types/value/py_value.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::value {

void register_py_value_with_nanobind(nb::module_& m) {
    nb::class_<PyHgValue>(m, "HgValue")
        // Constructor from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
             "Create a value with the given schema, default-constructed.")

        // Validity
        .def_prop_ro("valid", &PyHgValue::valid,
                     "True if this value has a valid schema and storage.")

        // Schema access
        .def_prop_ro("schema", &PyHgValue::schema, nb::rv_policy::reference,
                     "The TypeMeta schema for this value.")

        // Type kind
        .def_prop_ro("kind", &PyHgValue::kind,
                     "The TypeKind of this value (Scalar, List, Set, Dict, Bundle, etc.).")

        // Python value get/set
        .def_prop_rw("py_value", &PyHgValue::py_value, &PyHgValue::set_py_value,
                     "Get or set the value as a Python object.")

        // Type name
        .def_prop_ro("type_name", &PyHgValue::type_name,
                     "The type name string for this value's schema.")

        // Copy
        .def("copy", &PyHgValue::copy,
             "Create a deep copy of this value.")

        // String representation
        .def("__str__", &PyHgValue::to_string)
        .def("__repr__", [](const PyHgValue& self) {
            return "HgValue(" + self.type_name() + ", " + self.to_string() + ")";
        })

        // Equality
        .def("__eq__", &PyHgValue::equals, "other"_a)
        .def("__ne__", [](const PyHgValue& self, const PyHgValue& other) {
            return !self.equals(other);
        }, "other"_a)

        // Hash
        .def("__hash__", &PyHgValue::hash)

        // Static factory
        .def_static("from_python", &PyHgValue::from_python,
                    "schema"_a, "value"_a,
                    "Create a value from a Python object with the given schema.");
}

} // namespace hgraph::value
