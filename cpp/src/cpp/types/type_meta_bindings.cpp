//
// Created by Claude on 15/12/2025.
//
// Python bindings for TypeMeta and related types from the value type system.

#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/util/date_time.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph::value {

namespace {
    // Hash helper for combining type keys
    constexpr size_t DICT_SEED = 0x444943540000;   // "DICT"
    constexpr size_t SET_SEED = 0x5345540000;      // "SET"
    constexpr size_t DYNLIST_SEED = 0x44594E4C;    // "DYNL"
    constexpr size_t BUNDLE_SEED = 0x42554E444C;   // "BUNDL"
}

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

    // ========================================================================
    // Composite Type Factory Functions
    // ========================================================================

    // Dict factory: get_dict_type_meta(key_meta, value_meta) -> TypeMeta*
    m.def("get_dict_type_meta", [](const TypeMeta* key_type, const TypeMeta* value_type) -> const TypeMeta* {
        // Generate cache key from key+value type pointers
        size_t key = hash_combine(DICT_SEED, reinterpret_cast<size_t>(key_type));
        key = hash_combine(key, reinterpret_cast<size_t>(value_type));

        auto& registry = TypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = DictTypeBuilderWithPython()
            .key_type(key_type)
            .value_type(value_type)
            .build();

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "key_type"_a, "value_type"_a,
       "Get or create a Dict TypeMeta for the given key and value types.");

    // Set factory: get_set_type_meta(element_meta) -> TypeMeta*
    m.def("get_set_type_meta", [](const TypeMeta* element_type) -> const TypeMeta* {
        size_t key = hash_combine(SET_SEED, reinterpret_cast<size_t>(element_type));

        auto& registry = TypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = SetTypeBuilderWithPython()
            .element_type(element_type)
            .build();

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "element_type"_a,
       "Get or create a Set TypeMeta for the given element type.");

    // DynamicList factory: get_dynamic_list_type_meta(element_meta) -> TypeMeta*
    // Used for Python's tuple[T, ...] - variable length sequences
    m.def("get_dynamic_list_type_meta", [](const TypeMeta* element_type) -> const TypeMeta* {
        size_t key = hash_combine(DYNLIST_SEED, reinterpret_cast<size_t>(element_type));

        auto& registry = TypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = DynamicListTypeBuilderWithPython()
            .element_type(element_type)
            .build();

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "element_type"_a,
       "Get or create a DynamicList TypeMeta for variable-length sequences (tuple[T, ...]).");

    // Bundle factory: get_bundle_type_meta(fields, type_name) -> TypeMeta*
    // fields is a list of (name, TypeMeta*) tuples
    m.def("get_bundle_type_meta", [](nb::list fields, nb::object type_name) -> const TypeMeta* {
        // Build cache key from field names and types
        size_t key = BUNDLE_SEED;
        std::vector<std::pair<std::string, const TypeMeta*>> field_pairs;

        for (auto item : fields) {
            auto tuple = nb::cast<nb::tuple>(item);
            auto name = nb::cast<std::string>(tuple[0]);
            auto* field_type = nb::cast<const TypeMeta*>(tuple[1]);
            key = hash_combine(key, hash_string(name));
            key = hash_combine(key, reinterpret_cast<size_t>(field_type));
            field_pairs.emplace_back(name, field_type);
        }

        auto& registry = TypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        BundleTypeBuilderWithPython builder;
        for (const auto& [name, field_type] : field_pairs) {
            builder.add_field(name, field_type);
        }

        // Handle type_name - need to store the string persistently
        const char* name_str = nullptr;
        static std::vector<std::string> stored_names;  // Keep names alive
        if (!type_name.is_none()) {
            stored_names.push_back(nb::cast<std::string>(type_name));
            name_str = stored_names.back().c_str();
        }

        auto meta = builder.build(name_str);
        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "fields"_a, "type_name"_a = nb::none(),
       "Get or create a Bundle TypeMeta for the given fields. "
       "Fields should be a list of (name, TypeMeta) tuples.");
}

} // namespace hgraph::value
