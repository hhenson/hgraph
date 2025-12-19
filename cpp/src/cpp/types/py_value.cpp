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

        // =====================================================================
        // Comparison operators
        // =====================================================================
        .def("__eq__", &PyHgValue::equals, "other"_a)
        .def("__ne__", [](const PyHgValue& self, const PyHgValue& other) {
            return !self.equals(other);
        }, "other"_a)
        .def("__lt__", &PyHgValue::less_than, "other"_a)
        .def("__le__", &PyHgValue::less_equal, "other"_a)
        .def("__gt__", &PyHgValue::greater_than, "other"_a)
        .def("__ge__", &PyHgValue::greater_equal, "other"_a)

        // =====================================================================
        // Arithmetic operators (binary)
        // Note: __add__ and __sub__ are defined in collection operators section
        // because they handle both arithmetic and collection operations
        // =====================================================================
        .def("__mul__", &PyHgValue::multiply, "other"_a)
        .def("__rmul__", &PyHgValue::multiply, "other"_a)
        .def("__truediv__", &PyHgValue::divide, "other"_a)
        .def("__rtruediv__", [](const PyHgValue& self, const PyHgValue& other) {
            return other.divide(self);
        }, "other"_a)
        .def("__floordiv__", &PyHgValue::floor_divide, "other"_a)
        .def("__rfloordiv__", [](const PyHgValue& self, const PyHgValue& other) {
            return other.floor_divide(self);
        }, "other"_a)
        .def("__mod__", &PyHgValue::modulo, "other"_a)
        .def("__rmod__", [](const PyHgValue& self, const PyHgValue& other) {
            return other.modulo(self);
        }, "other"_a)
        .def("__pow__", &PyHgValue::power, "other"_a)
        .def("__rpow__", [](const PyHgValue& self, const PyHgValue& other) {
            return other.power(self);
        }, "other"_a)

        // =====================================================================
        // Collection operators (set algebra, dict merge, list concat)
        // =====================================================================
        // Union / merge: |
        .def("__or__", &PyHgValue::or_op, "other"_a)
        .def("__ror__", &PyHgValue::or_op, "other"_a)
        .def("__ior__", [](PyHgValue& self, const PyHgValue& other) -> PyHgValue& {
            self.ior_op(other);
            return self;
        }, "other"_a, nb::rv_policy::none)

        // Intersection: &
        .def("__and__", &PyHgValue::and_op, "other"_a)
        .def("__rand__", &PyHgValue::and_op, "other"_a)
        .def("__iand__", [](PyHgValue& self, const PyHgValue& other) -> PyHgValue& {
            self.iand_op(other);
            return self;
        }, "other"_a, nb::rv_policy::none)

        // Difference: - (note: for scalars this falls back to arithmetic subtraction)
        // sub_op handles both set difference and scalar subtraction
        .def("__sub__", &PyHgValue::sub_op, "other"_a)
        .def("__rsub__", [](const PyHgValue& self, const PyHgValue& other) {
            // For rsub, we need other - self
            // This only makes sense for scalars, not sets
            return other.subtract(self);
        }, "other"_a)
        .def("__isub__", [](PyHgValue& self, const PyHgValue& other) -> PyHgValue& {
            self.isub_op(other);
            return self;
        }, "other"_a, nb::rv_policy::none)

        // Symmetric difference: ^
        .def("__xor__", &PyHgValue::xor_op, "other"_a)
        .def("__rxor__", &PyHgValue::xor_op, "other"_a)
        .def("__ixor__", [](PyHgValue& self, const PyHgValue& other) -> PyHgValue& {
            self.ixor_op(other);
            return self;
        }, "other"_a, nb::rv_policy::none)

        // Addition / concatenation: +
        // add_op handles both arithmetic addition and list concatenation
        .def("__add__", &PyHgValue::add_op, "other"_a)
        .def("__radd__", &PyHgValue::add_op, "other"_a)

        // In-place addition / extend: +=
        .def("__iadd__", [](PyHgValue& self, const PyHgValue& other) -> PyHgValue& {
            self.iadd_op(other);
            return self;
        }, "other"_a, nb::rv_policy::none)

        // =====================================================================
        // Unary operators
        // =====================================================================
        .def("__neg__", &PyHgValue::negate)
        .def("__pos__", &PyHgValue::positive)
        .def("__abs__", &PyHgValue::absolute)
        .def("__invert__", &PyHgValue::invert)

        // =====================================================================
        // Boolean conversion
        // =====================================================================
        .def("__bool__", &PyHgValue::to_bool)

        // =====================================================================
        // Container operations
        // =====================================================================
        .def("__len__", &PyHgValue::length)
        .def("__contains__", &PyHgValue::contains, "item"_a)
        .def("__getitem__", &PyHgValue::getitem, "key"_a)
        .def("__setitem__", &PyHgValue::setitem, "key"_a, "value"_a)
        .def("__iter__", &PyHgValue::iter)

        // =====================================================================
        // Hash
        // =====================================================================
        .def("__hash__", &PyHgValue::hash)

        // =====================================================================
        // Static factory
        // =====================================================================
        .def_static("from_python", &PyHgValue::from_python,
                    "schema"_a, "value"_a,
                    "Create a value from a Python object with the given schema.");
}

} // namespace hgraph::value
