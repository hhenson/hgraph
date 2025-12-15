//
// Created by Claude on 15/12/2025.
//
// Python bindings for TypeMeta and related types from the value type system.

#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/util/date_time.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::value {

void register_type_meta_with_nanobind(nb::module_ &m) {
    // TypeKind enum
    nb::enum_<TypeKind>(m, "TypeKind")
        .value("Scalar", TypeKind::Scalar)
        .value("List", TypeKind::List)
        .value("Set", TypeKind::Set)
        .value("Dict", TypeKind::Dict)
        .value("Bundle", TypeKind::Bundle)
        .value("Ref", TypeKind::Ref)
        .value("Window", TypeKind::Window)
        .export_values();

    // TypeMeta class (read-only properties)
    nb::class_<TypeMeta>(m, "TypeMeta")
        .def_prop_ro("size", [](const TypeMeta& meta) { return meta.size; })
        .def_prop_ro("alignment", [](const TypeMeta& meta) { return meta.alignment; })
        .def_prop_ro("kind", [](const TypeMeta& meta) { return meta.kind; })
        .def_prop_ro("name", [](const TypeMeta& meta) { return meta.name ? meta.name : ""; })
        .def("type_name_str", &TypeMeta::type_name_str)
        .def("is_hashable", &TypeMeta::is_hashable)
        .def("is_comparable", &TypeMeta::is_comparable)
        .def("is_buffer_compatible", &TypeMeta::is_buffer_compatible)
        .def("__repr__", [](const TypeMeta& meta) {
            return "TypeMeta(" + meta.type_name_str() + ")";
        });

    // Factory function for scalar types
    // Maps Python types to C++ TypeMeta:
    //   bool -> bool, int -> int64_t, float -> double
    //   date -> engine_date_t, datetime -> engine_time_t, timedelta -> engine_time_delta_t
    //   everything else -> nb::object (including str)
    m.def("get_scalar_type_meta", [](nb::handle py_type) -> const TypeMeta* {
        // Import Python builtins and datetime modules for type checks
        auto builtins = nb::module_::import_("builtins");
        auto py_bool_type = builtins.attr("bool");
        auto py_int_type = builtins.attr("int");
        auto py_float_type = builtins.attr("float");

        auto datetime_mod = nb::module_::import_("datetime");
        auto date_type = datetime_mod.attr("date");
        auto datetime_type = datetime_mod.attr("datetime");
        auto timedelta_type = datetime_mod.attr("timedelta");

        // Check bool first (before int, since bool is subclass of int in Python)
        if (py_type.is(py_bool_type)) {
            return ScalarTypeMetaWithPython<bool>::get();
        }
        if (py_type.is(py_int_type)) {
            return ScalarTypeMetaWithPython<int64_t>::get();
        }
        if (py_type.is(py_float_type)) {
            return ScalarTypeMetaWithPython<double>::get();
        }
        // datetime must be checked before date (datetime is subclass of date)
        if (py_type.is(datetime_type)) {
            return ScalarTypeMetaWithPython<engine_time_t>::get();
        }
        if (py_type.is(date_type)) {
            return ScalarTypeMetaWithPython<engine_date_t>::get();
        }
        if (py_type.is(timedelta_type)) {
            return ScalarTypeMetaWithPython<engine_time_delta_t>::get();
        }
        // Everything else (str, custom objects, etc.) -> nb::object
        return ScalarTypeMetaWithPython<nb::object>::get();
    }, nb::rv_policy::reference, "py_type"_a,
       "Get the C++ TypeMeta for a Python scalar type.");
}

} // namespace hgraph::value
