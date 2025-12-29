#include <hgraph/api/python/py_value.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/vector.h>

namespace hgraph {

using namespace hgraph::value;
using namespace nanobind::literals;

// ============================================================================
// TypeKind Enum Binding
// ============================================================================

static void register_type_kind(nb::module_& m) {
    nb::enum_<TypeKind>(m, "TypeKind", "Categories of types in the Value system")
        .value("Scalar", TypeKind::Scalar, "Atomic values: int, double, bool, string, datetime, etc.")
        .value("Tuple", TypeKind::Tuple, "Indexed heterogeneous collection (unnamed, positional access only)")
        .value("Bundle", TypeKind::Bundle, "Named field collection (struct-like, index + name access)")
        .value("List", TypeKind::List, "Indexed homogeneous collection (dynamic size)")
        .value("Set", TypeKind::Set, "Unordered unique elements")
        .value("Map", TypeKind::Map, "Key-value pairs")
        .value("Window", TypeKind::Window, "Time-based sliding window")
        .value("Ref", TypeKind::Ref, "Reference to another time-series");
}

// ============================================================================
// BundleFieldInfo Binding (User Guide Section 5.2)
// ============================================================================

static void register_bundle_field_info(nb::module_& m) {
    nb::class_<BundleFieldInfo>(m, "BundleFieldInfo",
        "Metadata for a single field in a Bundle type")
        .def_prop_ro("name", [](const BundleFieldInfo& self) {
            return self.name ? std::string(self.name) : std::string();
        }, "Get the field name")
        .def_ro("index", &BundleFieldInfo::index, "Get the field index (0-based)")
        .def_ro("offset", &BundleFieldInfo::offset, "Get the byte offset from bundle start")
        .def_prop_ro("type", [](const BundleFieldInfo& self) { return self.type; },
            nb::rv_policy::reference, "Get the field type schema")
        .def("__repr__", [](const BundleFieldInfo& self) {
            return "BundleFieldInfo(name='" + (self.name ? std::string(self.name) : "<null>") +
                   "', index=" + std::to_string(self.index) +
                   ", offset=" + std::to_string(self.offset) + ")";
        });
}

// ============================================================================
// TypeMeta Binding (Read-only schema descriptor)
// ============================================================================

static void register_type_meta(nb::module_& m) {
    nb::class_<TypeMeta>(m, "TypeMeta", "Type metadata describing a value's schema")
        .def_ro("size", &TypeMeta::size, "Size in bytes")
        .def_ro("alignment", &TypeMeta::alignment, "Alignment requirement")
        .def_ro("kind", &TypeMeta::kind, "Type category")
        .def_ro("field_count", &TypeMeta::field_count, "Number of fields (Bundle/Tuple)")
        .def_ro("fixed_size", &TypeMeta::fixed_size, "Fixed size (0 = dynamic)")
        .def("is_fixed_size", &TypeMeta::is_fixed_size, "Check if this is a fixed-size collection")
        .def("is_hashable", &TypeMeta::is_hashable, "Check if this type is hashable")
        .def("is_comparable", &TypeMeta::is_comparable, "Check if this type is comparable")
        .def("is_equatable", &TypeMeta::is_equatable, "Check if this type is equatable")
        .def("is_trivially_copyable", &TypeMeta::is_trivially_copyable, "Check if this type is trivially copyable")
        // Element and key type access for composite types
        .def_prop_ro("element_type", [](const TypeMeta& self) { return self.element_type; },
            nb::rv_policy::reference, "Get the element type (for List/Set/Map value)")
        .def_prop_ro("key_type", [](const TypeMeta& self) { return self.key_type; },
            nb::rv_policy::reference, "Get the key type (for Map)")
        // Fields property for Bundle/Tuple types (User Guide Section 5.2)
        .def_prop_ro("fields", [](const TypeMeta& self) {
            std::vector<const BundleFieldInfo*> result;
            if (self.fields && self.field_count > 0) {
                result.reserve(self.field_count);
                for (size_t i = 0; i < self.field_count; ++i) {
                    result.push_back(&self.fields[i]);
                }
            }
            return result;
        }, nb::rv_policy::reference_internal, "Get the field metadata (for Bundle/Tuple)");
}

// ============================================================================
// Free Functions for Scalar Type Metadata (Design Doc Section 3.2)
// ============================================================================

static void register_scalar_type_meta_functions(nb::module_& m) {
    // Free functions matching Design Doc API: scalar_type_meta<T>()
    m.def("scalar_type_meta_int64", []() { return scalar_type_meta<int64_t>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for int64 scalar type");

    m.def("scalar_type_meta_double", []() { return scalar_type_meta<double>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for double scalar type");

    m.def("scalar_type_meta_bool", []() { return scalar_type_meta<bool>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for bool scalar type");

    m.def("scalar_type_meta_string", []() { return scalar_type_meta<std::string>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for string scalar type");

    m.def("scalar_type_meta_date", []() { return scalar_type_meta<engine_date_t>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for date scalar type");

    m.def("scalar_type_meta_datetime", []() { return scalar_type_meta<engine_time_t>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for datetime scalar type");

    m.def("scalar_type_meta_timedelta", []() { return scalar_type_meta<engine_time_delta_t>(); },
        nb::rv_policy::reference,
        "Get the TypeMeta for timedelta scalar type");
}

// ============================================================================
// Type Builders Binding (Design Doc Section 3.2)
// ============================================================================

static void register_type_builders(nb::module_& m) {
    // TupleTypeBuilder
    nb::class_<TupleTypeBuilder>(m, "TupleTypeBuilder",
        "Builder for tuple types (heterogeneous, index-only access)")
        .def("element", &TupleTypeBuilder::element, "type"_a, nb::rv_policy::reference,
            "Add an element type to the tuple (returns self for chaining)")
        .def("build", &TupleTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the tuple type");

    // BundleTypeBuilder
    nb::class_<BundleTypeBuilder>(m, "BundleTypeBuilder",
        "Builder for bundle types (struct-like, index + name access)")
        .def("field", &BundleTypeBuilder::field, "name"_a, "type"_a, nb::rv_policy::reference,
            "Add a field to the bundle (returns self for chaining)")
        .def("build", &BundleTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the bundle type");

    // ListTypeBuilder
    nb::class_<ListTypeBuilder>(m, "ListTypeBuilder",
        "Builder for list types (homogeneous indexed collections)")
        .def("build", &ListTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the list type");

    // SetTypeBuilder
    nb::class_<SetTypeBuilder>(m, "SetTypeBuilder",
        "Builder for set types (unique elements)")
        .def("build", &SetTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the set type");

    // MapTypeBuilder
    nb::class_<MapTypeBuilder>(m, "MapTypeBuilder",
        "Builder for map types (key-value pairs)")
        .def("build", &MapTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the map type");
}

// ============================================================================
// TypeRegistry Binding (Design Doc Section 3.2)
// ============================================================================

static void register_type_registry(nb::module_& m) {
    nb::class_<TypeRegistry>(m, "TypeRegistry", "Central registry for all type metadata")
        // Singleton accessor
        .def_static("instance", &TypeRegistry::instance, nb::rv_policy::reference,
            "Get the singleton TypeRegistry instance")

        // Named bundle lookup
        .def("get_bundle_by_name", &TypeRegistry::get_bundle_by_name, nb::rv_policy::reference,
            "name"_a, "Get a named bundle type by name (returns None if not found)")
        .def("has_bundle", &TypeRegistry::has_bundle, "name"_a, "Check if a named bundle exists")

        // Scalar type accessors (kept for backward compatibility)
        .def("get_int_type", [](TypeRegistry& self) { return self.get_scalar<int64_t>(); },
            nb::rv_policy::reference, "Get the int64 type metadata")
        .def("get_double_type", [](TypeRegistry& self) { return self.get_scalar<double>(); },
            nb::rv_policy::reference, "Get the double type metadata")
        .def("get_bool_type", [](TypeRegistry& self) { return self.get_scalar<bool>(); },
            nb::rv_policy::reference, "Get the bool type metadata")
        .def("get_string_type", [](TypeRegistry& self) { return self.get_scalar<std::string>(); },
            nb::rv_policy::reference, "Get the string type metadata")

        // Composite type builders (Design Doc API)
        .def("tuple", &TypeRegistry::tuple,
            "Create a tuple type builder (heterogeneous, unnamed)")
        .def("bundle", static_cast<BundleTypeBuilder (TypeRegistry::*)()>(&TypeRegistry::bundle),
            "Create an anonymous bundle type builder")
        .def("bundle", static_cast<BundleTypeBuilder (TypeRegistry::*)(const std::string&)>(&TypeRegistry::bundle),
            "name"_a, "Create a named bundle type builder")
        .def("list", &TypeRegistry::list, "element_type"_a,
            "Create a dynamic list type builder")
        .def("fixed_list", &TypeRegistry::fixed_list, "element_type"_a, "size"_a,
            "Create a fixed-size list type builder")
        .def("set", &TypeRegistry::set, "element_type"_a,
            "Create a set type builder")
        .def("map", &TypeRegistry::map, "key_type"_a, "value_type"_a,
            "Create a map type builder");
}

// ============================================================================
// ConstValueView Binding
// ============================================================================

static void register_const_value_view(nb::module_& m) {
    nb::class_<ConstValueView>(m, "ConstValueView",
        "Non-owning const view into a Value, providing read-only access")
        // Validity
        .def("valid", &ConstValueView::valid, "Check if the view is valid")
        .def("__bool__", &ConstValueView::valid, "Boolean conversion (validity)")
        .def_prop_ro("schema", &ConstValueView::schema, nb::rv_policy::reference,
            "Get the type schema")

        // Type kind queries
        .def("is_scalar", &ConstValueView::is_scalar, "Check if this is a scalar type")
        .def("is_tuple", &ConstValueView::is_tuple, "Check if this is a tuple type")
        .def("is_bundle", &ConstValueView::is_bundle, "Check if this is a bundle type")
        .def("is_list", &ConstValueView::is_list, "Check if this is a list type")
        .def("is_fixed_list", &ConstValueView::is_fixed_list, "Check if this is a fixed-size list")
        .def("is_set", &ConstValueView::is_set, "Check if this is a set type")
        .def("is_map", &ConstValueView::is_map, "Check if this is a map type")

        // Type checking
        .def("is_type", &ConstValueView::is_type, "schema"_a,
            "Check if this view has a specific schema (pointer equality)")

        // Typed scalar access - explicit getters since Python can't do as<T>()
        .def("as_int", [](const ConstValueView& self) { return self.checked_as<int64_t>(); },
            "Get the value as int64 (throws if type mismatch)")
        .def("as_double", [](const ConstValueView& self) { return self.checked_as<double>(); },
            "Get the value as double (throws if type mismatch)")
        .def("as_bool", [](const ConstValueView& self) { return self.checked_as<bool>(); },
            "Get the value as bool (throws if type mismatch)")
        .def("as_string", [](const ConstValueView& self) -> std::string {
            return self.checked_as<std::string>();
        }, "Get the value as string (throws if type mismatch)")

        // Safe typed access (returns None on type mismatch)
        .def("try_as_int", [](const ConstValueView& self) -> std::optional<int64_t> {
            auto* p = self.try_as<int64_t>();
            return p ? std::optional<int64_t>(*p) : std::nullopt;
        }, "Try to get the value as int64 (returns None if type mismatch)")
        .def("try_as_double", [](const ConstValueView& self) -> std::optional<double> {
            auto* p = self.try_as<double>();
            return p ? std::optional<double>(*p) : std::nullopt;
        }, "Try to get the value as double (returns None if type mismatch)")
        .def("try_as_bool", [](const ConstValueView& self) -> std::optional<bool> {
            auto* p = self.try_as<bool>();
            return p ? std::optional<bool>(*p) : std::nullopt;
        }, "Try to get the value as bool (returns None if type mismatch)")
        .def("try_as_string", [](const ConstValueView& self) -> std::optional<std::string> {
            auto* p = self.try_as<std::string>();
            return p ? std::optional<std::string>(*p) : std::nullopt;
        }, "Try to get the value as string (returns None if type mismatch)")

        // Type-specific scalar checks
        .def("is_int", [](const ConstValueView& self) { return self.is_scalar_type<int64_t>(); },
            "Check if this is an int64 scalar")
        .def("is_double", [](const ConstValueView& self) { return self.is_scalar_type<double>(); },
            "Check if this is a double scalar")
        .def("is_bool", [](const ConstValueView& self) { return self.is_scalar_type<bool>(); },
            "Check if this is a bool scalar")
        .def("is_string", [](const ConstValueView& self) { return self.is_scalar_type<std::string>(); },
            "Check if this is a string scalar")

        // Specialized view access (Design Doc Section 6.2 - as_bundle(), as_list(), etc.)
        .def("as_tuple", &ConstValueView::as_tuple, "Get as a const tuple view (throws if not a tuple)")
        .def("as_bundle", &ConstValueView::as_bundle, "Get as a const bundle view (throws if not a bundle)")
        .def("as_list", &ConstValueView::as_list, "Get as a const list view (throws if not a list)")
        .def("as_set", &ConstValueView::as_set, "Get as a const set view (throws if not a set)")
        .def("as_map", &ConstValueView::as_map, "Get as a const map view (throws if not a map)")

        // Safe composite type access (returns None if type mismatch)
        .def("try_as_tuple", &ConstValueView::try_as_tuple,
            "Try to get as a const tuple view (returns None if not a tuple)")
        .def("try_as_bundle", &ConstValueView::try_as_bundle,
            "Try to get as a const bundle view (returns None if not a bundle)")
        .def("try_as_list", &ConstValueView::try_as_list,
            "Try to get as a const list view (returns None if not a list)")
        .def("try_as_set", &ConstValueView::try_as_set,
            "Try to get as a const set view (returns None if not a set)")
        .def("try_as_map", &ConstValueView::try_as_map,
            "Try to get as a const map view (returns None if not a map)")

        // Operations
        .def("equals", &ConstValueView::equals, "other"_a, "Check equality with another view")
        .def("hash", &ConstValueView::hash, "Compute the hash of the value")
        .def("to_string", &ConstValueView::to_string, "Convert the value to a string")
        .def("to_python", &ConstValueView::to_python, "Convert the value to a Python object")
        .def("clone", [](const ConstValueView& self) -> PlainValue {
            return self.clone<NoCache>();
        }, "Create a deep copy of this value")

        // Python special methods
        .def("__eq__", [](const ConstValueView& self, const ConstValueView& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &ConstValueView::hash)
        .def("__str__", &ConstValueView::to_string)
        .def("__repr__", [](const ConstValueView& self) {
            if (!self.valid()) return std::string("ConstValueView(invalid)");
            return "ConstValueView(" + self.to_string() + ")";
        });
}

// ============================================================================
// ValueView Binding
// ============================================================================

static void register_value_view(nb::module_& m) {
    nb::class_<ValueView, ConstValueView>(m, "ValueView",
        "Non-owning mutable view into a Value")
        // Mutable scalar setters
        .def("set_int", [](ValueView& self, int64_t value) {
            self.checked_as<int64_t>() = value;
        }, "value"_a, "Set the value as int64 (throws if type mismatch)")
        .def("set_double", [](ValueView& self, double value) {
            self.checked_as<double>() = value;
        }, "value"_a, "Set the value as double (throws if type mismatch)")
        .def("set_bool", [](ValueView& self, bool value) {
            self.checked_as<bool>() = value;
        }, "value"_a, "Set the value as bool (throws if type mismatch)")
        .def("set_string", [](ValueView& self, const std::string& value) {
            self.checked_as<std::string>() = value;
        }, "value"_a, "Set the value as string (throws if type mismatch)")

        // Specialized view access (mutable)
        .def("as_tuple", &ValueView::as_tuple, "Get as a mutable tuple view (throws if not a tuple)")
        .def("as_bundle", &ValueView::as_bundle, "Get as a mutable bundle view (throws if not a bundle)")
        .def("as_list", &ValueView::as_list, "Get as a mutable list view (throws if not a list)")
        .def("as_set", &ValueView::as_set, "Get as a mutable set view (throws if not a set)")
        .def("as_map", &ValueView::as_map, "Get as a mutable map view (throws if not a map)")

        // Mutation from another view
        .def("copy_from", &ValueView::copy_from, "other"_a,
            "Copy data from another view (schemas must match)")

        // Python interop
        .def("from_python", &ValueView::from_python, "src"_a,
            "Set the value from a Python object")

        // Raw data access (returns pointer as integer for debugging/FFI)
        .def("data", [](ValueView& self) -> uintptr_t {
            return reinterpret_cast<uintptr_t>(self.data());
        }, "Get raw data pointer as integer (for debugging/FFI)")

        // Repr
        .def("__repr__", [](const ValueView& self) {
            if (!self.valid()) return std::string("ValueView(invalid)");
            return "ValueView(" + self.to_string() + ")";
        });
}

// ============================================================================
// ConstIndexedView Binding
// ============================================================================

static void register_const_indexed_view(nb::module_& m) {
    nb::class_<ConstIndexedView, ConstValueView>(m, "ConstIndexedView",
        "Base class for types supporting const index-based access")
        .def("size", &ConstIndexedView::size, "Get the number of elements")
        .def("empty", &ConstIndexedView::empty, "Check if empty")
        .def("at", &ConstIndexedView::at, "index"_a, "Get element at index (const)")
        .def("__getitem__", [](const ConstIndexedView& self, int64_t index) {
            int64_t size = static_cast<int64_t>(self.size());
            if (index < 0) {
                index += size;  // Convert negative index to positive
            }
            if (index < 0 || index >= size) {
                throw nb::index_error("index out of range");
            }
            return self[static_cast<size_t>(index)];
        }, "index"_a)
        .def("__len__", &ConstIndexedView::size)
        .def("__iter__", [](const ConstIndexedView& self) {
            nb::list result;
            for (size_t i = 0; i < self.size(); ++i) {
                result.append(nb::cast(self[i]));
            }
            return nb::iter(result);
        }, "Iterate over elements");
}

// ============================================================================
// IndexedView Binding
// ============================================================================

static void register_indexed_view(nb::module_& m) {
    nb::class_<IndexedView, ValueView>(m, "IndexedView",
        "Base class for types supporting mutable index-based access")
        .def("size", &IndexedView::size, "Get the number of elements")
        .def("empty", &IndexedView::empty, "Check if empty")
        .def("at", static_cast<ValueView (IndexedView::*)(size_t)>(&IndexedView::at),
            "index"_a, "Get element at index (mutable)")
        .def("__getitem__", [](IndexedView& self, int64_t index) {
            int64_t size = static_cast<int64_t>(self.size());
            if (index < 0) {
                index += size;  // Convert negative index to positive
            }
            if (index < 0 || index >= size) {
                throw nb::index_error("index out of range");
            }
            return self[static_cast<size_t>(index)];
        }, "index"_a)
        .def("__len__", &IndexedView::size)
        .def("set", static_cast<void (IndexedView::*)(size_t, const ConstValueView&)>(&IndexedView::set),
            "index"_a, "value"_a, "Set element at index from a view")
        // Overload: set from PlainValue (auto-extract const_view)
        .def("set", [](IndexedView& self, size_t index, const PlainValue& value) {
            self.set(index, value.const_view());
        }, "index"_a, "value"_a, "Set element at index from a PlainValue")
        .def("__iter__", [](IndexedView& self) {
            nb::list result;
            for (size_t i = 0; i < self.size(); ++i) {
                result.append(nb::cast(self[i]));
            }
            return nb::iter(result);
        }, "Iterate over elements");
}

// ============================================================================
// Tuple Views Binding
// ============================================================================

static void register_tuple_views(nb::module_& m) {
    nb::class_<ConstTupleView, ConstIndexedView>(m, "ConstTupleView",
        "Const view for tuple types (heterogeneous index-only access)")
        .def("element_type", &ConstTupleView::element_type, "index"_a, nb::rv_policy::reference,
            "Get the type of element at index");

    nb::class_<TupleView, IndexedView>(m, "TupleView",
        "Mutable view for tuple types")
        .def("element_type", &TupleView::element_type, "index"_a, nb::rv_policy::reference,
            "Get the type of element at index")
        // Overload: set from PlainValue (must come before generic object version)
        .def("set", [](TupleView& self, size_t index, const PlainValue& value) {
            if (index >= self.size()) {
                throw std::out_of_range("Tuple index out of range");
            }
            // Verify type matches the expected element type
            const TypeMeta* elem_type = self.element_type(index);
            if (value.schema() != elem_type) {
                throw std::runtime_error("Type mismatch: value type doesn't match tuple element type");
            }
            self.set(index, value.const_view());
        }, "index"_a, "value"_a, "Set element at index from PlainValue (with type checking)")
        // Auto-wrapping set from Python native object with type checking
        .def("set", [](TupleView& self, size_t index, const nb::object& py_value) {
            if (index >= self.size()) {
                throw std::out_of_range("Tuple index out of range");
            }
            // Get the expected element type at this index
            const TypeMeta* elem_type = self.element_type(index);
            // Create a temporary Value of the correct type
            PlainValue temp(elem_type);
            // Convert from Python - this will throw if types are incompatible
            temp.from_python(py_value);
            // Set using the view
            self.set(index, temp.const_view());
        }, "index"_a, "value"_a, "Set element at index from Python object (with type checking)");
}

// ============================================================================
// Bundle Views Binding
// ============================================================================

static void register_bundle_views(nb::module_& m) {
    nb::class_<ConstBundleView, ConstIndexedView>(m, "ConstBundleView",
        "Const view for bundle types (struct-like access)")
        // Named field access - use lambdas for std::string to std::string_view conversion
        .def("at_name", [](const ConstBundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a, "Get field by name")
        .def("__getitem__", [](const ConstBundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a)
        // Field metadata
        .def("field_count", &ConstBundleView::field_count, "Get the number of fields")
        .def("has_field", [](const ConstBundleView& self, const std::string& name) {
            return self.has_field(name);
        }, "name"_a, "Check if a field exists")
        .def("field_index", [](const ConstBundleView& self, const std::string& name) {
            return self.field_index(name);
        }, "name"_a, "Get field index by name (returns size() if not found)");

    nb::class_<BundleView, IndexedView>(m, "BundleView",
        "Mutable view for bundle types")
        // Named field access (const)
        .def("at_name", [](const BundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a, "Get field by name (const)")
        // Named field access (mutable)
        .def("at_name_mut", [](BundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a, "Get field by name (mutable)")
        // Set by name with ConstValueView
        .def("set_name", [](BundleView& self, const std::string& name, const ConstValueView& value) {
            self.set(name, value);
        }, "name"_a, "value"_a, "Set field by name from ConstValueView")
        // Set by name with auto-wrap from Python object
        .def("set", [](BundleView& self, const std::string& name, const nb::object& py_value) {
            size_t idx = self.field_index(name);
            if (idx >= self.field_count()) {
                throw std::runtime_error("Field not found: " + name);
            }
            const TypeMeta* field_type = self.schema()->fields[idx].type;
            PlainValue temp(field_type);
            temp.from_python(py_value);
            self.set(name, temp.const_view());
        }, "name"_a, "value"_a, "Set field by name from Python object (auto-wrap)")
        // __getitem__ for indexing
        .def("__getitem__", [](BundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a)
        // Field metadata
        .def("field_count", &BundleView::field_count, "Get the number of fields")
        .def("has_field", [](const BundleView& self, const std::string& name) {
            return self.has_field(name);
        }, "name"_a, "Check if a field exists")
        .def("field_index", [](const BundleView& self, const std::string& name) {
            return self.field_index(name);
        }, "name"_a, "Get field index by name (returns size() if not found)");
}

// ============================================================================
// List Views Binding
// ============================================================================

static void register_list_views(nb::module_& m) {
    nb::class_<ConstListView, ConstIndexedView>(m, "ConstListView",
        "Const view for list types")
        .def("front", &ConstListView::front, "Get the first element")
        .def("back", &ConstListView::back, "Get the last element")
        .def("element_type", &ConstListView::element_type, nb::rv_policy::reference,
            "Get the element type")
        .def("is_fixed", &ConstListView::is_fixed, "Check if this is a fixed-size list");

    nb::class_<ListView, IndexedView>(m, "ListView",
        "Mutable view for list types")
        .def("front", &ListView::front, "Get the first element (mutable)")
        .def("back", &ListView::back, "Get the last element (mutable)")
        .def("element_type", &ListView::element_type, nb::rv_policy::reference,
            "Get the element type")
        .def("is_fixed", &ListView::is_fixed, "Check if this is a fixed-size list")
        // Dynamic list operations with type checking
        .def("push_back", [](ListView& self, const ConstValueView& value) {
            // Type check: value schema must match element type
            if (value.schema() != self.element_type()) {
                throw std::runtime_error("Type mismatch: cannot push_back value of different type");
            }
            self.push_back(value);
        }, "value"_a, "Append an element (throws if fixed-size or type mismatch)")
        // Override set with type checking for lists
        .def("set", [](ListView& self, size_t index, const ConstValueView& value) {
            if (value.schema() != self.element_type()) {
                throw std::runtime_error("Type mismatch: cannot set value of different type");
            }
            self.set(index, value);
        }, "index"_a, "value"_a, "Set element at index (with type checking)")
        .def("pop_back", &ListView::pop_back, "Remove the last element (throws if fixed-size)")
        .def("clear", &ListView::clear, "Clear all elements (throws if fixed-size)")
        .def("resize", &ListView::resize, "new_size"_a, "Resize the list (throws if fixed-size)")
        .def("reset", static_cast<void (ListView::*)(const ConstValueView&)>(&ListView::reset),
            "sentinel"_a, "Reset all elements to a sentinel value");
}

// ============================================================================
// Set Views Binding
// ============================================================================

static void register_set_views(nb::module_& m) {
    nb::class_<ConstSetView, ConstValueView>(m, "ConstSetView",
        "Const view for set types")
        .def("size", &ConstSetView::size, "Get the number of elements")
        .def("empty", &ConstSetView::empty, "Check if empty")
        .def("__len__", &ConstSetView::size)
        .def("contains", static_cast<bool (ConstSetView::*)(const ConstValueView&) const>(
            &ConstSetView::contains), "value"_a, "Check if an element is in the set")
        .def("__contains__", static_cast<bool (ConstSetView::*)(const ConstValueView&) const>(
            &ConstSetView::contains), "value"_a)
        .def("element_type", &ConstSetView::element_type, nb::rv_policy::reference,
            "Get the element type")
        // Index-based element access (set elements can be accessed by index)
        .def("__getitem__", [](const ConstSetView& self, size_t index) {
            if (index >= self.size()) {
                throw nb::index_error("set index out of range");
            }
            const void* elem = self.schema()->ops->get_at(self.data(), index, self.schema());
            return ConstValueView(elem, self.schema()->element_type);
        }, "index"_a, "Get element at index")
        // Iteration support using index-based access
        .def("__iter__", [](const ConstSetView& self) {
            nb::list result;
            for (size_t i = 0; i < self.size(); ++i) {
                const void* elem = self.schema()->ops->get_at(self.data(), i, self.schema());
                result.append(nb::cast(ConstValueView(elem, self.schema()->element_type)));
            }
            return nb::iter(result);
        }, "Iterate over elements");

    nb::class_<SetView, ValueView>(m, "SetView",
        "Mutable view for set types")
        .def("size", &SetView::size, "Get the number of elements")
        .def("empty", &SetView::empty, "Check if empty")
        .def("__len__", &SetView::size)
        .def("contains", static_cast<bool (SetView::*)(const ConstValueView&) const>(
            &SetView::contains), "value"_a, "Check if an element is in the set")
        .def("__contains__", static_cast<bool (SetView::*)(const ConstValueView&) const>(
            &SetView::contains), "value"_a)
        .def("insert", static_cast<bool (SetView::*)(const ConstValueView&)>(&SetView::insert),
            "value"_a, "Insert an element (returns true if inserted)")
        .def("erase", static_cast<bool (SetView::*)(const ConstValueView&)>(&SetView::erase),
            "value"_a, "Remove an element (returns true if removed)")
        .def("clear", &SetView::clear, "Clear all elements")
        .def("element_type", &SetView::element_type, nb::rv_policy::reference,
            "Get the element type")
        // Index-based element access
        .def("__getitem__", [](SetView& self, size_t index) {
            if (index >= self.size()) {
                throw nb::index_error("set index out of range");
            }
            const void* elem = self.schema()->ops->get_at(self.data(), index, self.schema());
            return ConstValueView(elem, self.schema()->element_type);
        }, "index"_a, "Get element at index")
        // Iteration support
        .def("__iter__", [](SetView& self) {
            nb::list result;
            for (size_t i = 0; i < self.size(); ++i) {
                const void* elem = self.schema()->ops->get_at(self.data(), i, self.schema());
                result.append(nb::cast(ConstValueView(elem, self.schema()->element_type)));
            }
            return nb::iter(result);
        }, "Iterate over elements");
}

// ============================================================================
// Map Views Binding
// ============================================================================

static void register_map_views(nb::module_& m) {
    nb::class_<ConstMapView, ConstValueView>(m, "ConstMapView",
        "Const view for map types")
        .def("size", &ConstMapView::size, "Get the number of entries")
        .def("empty", &ConstMapView::empty, "Check if empty")
        .def("__len__", &ConstMapView::size)
        .def("at", static_cast<ConstValueView (ConstMapView::*)(const ConstValueView&) const>(
            &ConstMapView::at), "key"_a, "Get value by key")
        .def("__getitem__", static_cast<ConstValueView (ConstMapView::*)(const ConstValueView&) const>(
            &ConstMapView::operator[]), "key"_a)
        .def("contains", static_cast<bool (ConstMapView::*)(const ConstValueView&) const>(
            &ConstMapView::contains), "key"_a, "Check if a key exists")
        .def("__contains__", static_cast<bool (ConstMapView::*)(const ConstValueView&) const>(
            &ConstMapView::contains), "key"_a)
        .def("key_type", &ConstMapView::key_type, nb::rv_policy::reference, "Get the key type")
        .def("value_type", &ConstMapView::value_type, nb::rv_policy::reference, "Get the value type")
        // Iteration support - iterate over keys (like Python dict)
        .def("__iter__", [](const ConstMapView& self) {
            // Use to_python() to get dict, then iterate over keys
            nb::object py_dict = self.to_python();
            return nb::iter(py_dict);
        }, "Iterate over keys (like dict)")
        .def("keys", [](const ConstMapView& self) {
            nb::object py_dict = self.to_python();
            return py_dict.attr("keys")();
        }, "Get view of keys")
        .def("values", [](const ConstMapView& self) {
            nb::object py_dict = self.to_python();
            return py_dict.attr("values")();
        }, "Get view of values")
        .def("items", [](const ConstMapView& self) {
            nb::object py_dict = self.to_python();
            return py_dict.attr("items")();
        }, "Get view of (key, value) pairs");

    nb::class_<MapView, ValueView>(m, "MapView",
        "Mutable view for map types")
        .def("size", &MapView::size, "Get the number of entries")
        .def("empty", &MapView::empty, "Check if empty")
        .def("__len__", &MapView::size)
        .def("at", static_cast<ValueView (MapView::*)(const ConstValueView&)>(&MapView::at),
            "key"_a, "Get value by key (mutable, throws if not found)")
        .def("at_const", static_cast<ConstValueView (MapView::*)(const ConstValueView&) const>(
            &MapView::at), "key"_a, "Get value by key (const, throws if not found)")
        // __getitem__ with auto-insert behavior (like C++ std::map::operator[])
        .def("__getitem__", [](MapView& self, const ConstValueView& key) -> ValueView {
            // If key doesn't exist, insert default value
            if (!self.contains(key)) {
                // Create a default-constructed value of the value type
                const TypeMeta* val_type = self.value_type();
                PlainValue default_val(val_type);
                self.set(key, default_val.const_view());
            }
            return self.at(key);
        }, "key"_a, "Get value by key (auto-inserts default if missing)")
        .def("contains", static_cast<bool (MapView::*)(const ConstValueView&) const>(
            &MapView::contains), "key"_a, "Check if a key exists")
        .def("__contains__", static_cast<bool (MapView::*)(const ConstValueView&) const>(
            &MapView::contains), "key"_a)
        .def("set", static_cast<void (MapView::*)(const ConstValueView&, const ConstValueView&)>(
            &MapView::set), "key"_a, "value"_a, "Set value for key")
        .def("insert", static_cast<bool (MapView::*)(const ConstValueView&, const ConstValueView&)>(
            &MapView::insert), "key"_a, "value"_a, "Insert key-value pair (returns true if inserted)")
        .def("erase", static_cast<bool (MapView::*)(const ConstValueView&)>(&MapView::erase),
            "key"_a, "Remove entry by key (returns true if removed)")
        .def("clear", &MapView::clear, "Clear all entries")
        .def("key_type", &MapView::key_type, nb::rv_policy::reference, "Get the key type")
        .def("value_type", &MapView::value_type, nb::rv_policy::reference, "Get the value type")
        // Iteration support - iterate over keys (like Python dict)
        .def("__iter__", [](MapView& self) {
            nb::object py_dict = self.to_python();
            return nb::iter(py_dict);
        }, "Iterate over keys (like dict)")
        .def("keys", [](MapView& self) {
            nb::object py_dict = self.to_python();
            return py_dict.attr("keys")();
        }, "Get view of keys")
        .def("values", [](MapView& self) {
            nb::object py_dict = self.to_python();
            return py_dict.attr("values")();
        }, "Get view of values")
        .def("items", [](MapView& self) {
            nb::object py_dict = self.to_python();
            return py_dict.attr("items")();
        }, "Get view of (key, value) pairs");
}

// ============================================================================
// PlainValue (Value<NoCache>) Binding
// ============================================================================

static void register_plain_value(nb::module_& m) {
    nb::class_<PlainValue>(m, "PlainValue",
        "Owning type-erased value storage without caching")
        // Constructors from scalars
        .def(nb::init<int64_t>(), "value"_a, "Construct from int64")
        .def(nb::init<double>(), "value"_a, "Construct from double")
        .def(nb::init<bool>(), "value"_a, "Construct from bool")
        .def(nb::init<const std::string&>(), "value"_a, "Construct from string")
        .def(nb::init<engine_date_t>(), "value"_a, "Construct from date")
        .def(nb::init<engine_time_t>(), "value"_a, "Construct from datetime")
        .def(nb::init<engine_time_delta_t>(), "value"_a, "Construct from timedelta")
        // Construct from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
            "Construct from type schema (default value)")
        // Construct from view (copy)
        .def(nb::init<const ConstValueView&>(), "view"_a,
            "Construct by copying from a view")

        // Validity
        .def("valid", &PlainValue::valid, "Check if the Value contains data")
        .def("__bool__", &PlainValue::valid, "Boolean conversion (validity)")
        .def_prop_ro("schema", &PlainValue::schema, nb::rv_policy::reference,
            "Get the type schema")

        // View access (Design Doc Section 6.2)
        .def("view", static_cast<ValueView (PlainValue::*)()>(&PlainValue::view),
            "Get a mutable view of the data")
        .def("const_view", &PlainValue::const_view, "Get a const view of the data")

        // Specialized view access (Design Doc Section 6.2)
        .def("as_tuple", static_cast<TupleView (PlainValue::*)()>(&PlainValue::as_tuple),
            "Get as a tuple view (mutable)")
        .def("as_bundle", static_cast<BundleView (PlainValue::*)()>(&PlainValue::as_bundle),
            "Get as a bundle view (mutable)")
        .def("as_list", static_cast<ListView (PlainValue::*)()>(&PlainValue::as_list),
            "Get as a list view (mutable)")
        .def("as_set", static_cast<SetView (PlainValue::*)()>(&PlainValue::as_set),
            "Get as a set view (mutable)")
        .def("as_map", static_cast<MapView (PlainValue::*)()>(&PlainValue::as_map),
            "Get as a map view (mutable)")

        // Typed access - explicit getters (Design Doc Section 7)
        .def("as_int", [](PlainValue& self) { return self.checked_as<int64_t>(); },
            "Get the value as int64 (throws if type mismatch)")
        .def("as_double", [](PlainValue& self) { return self.checked_as<double>(); },
            "Get the value as double (throws if type mismatch)")
        .def("as_bool", [](PlainValue& self) { return self.checked_as<bool>(); },
            "Get the value as bool (throws if type mismatch)")
        .def("as_string", [](PlainValue& self) -> std::string {
            return self.checked_as<std::string>();
        }, "Get the value as string (throws if type mismatch)")

        // Typed setters
        .def("set_int", [](PlainValue& self, int64_t value) {
            self.as<int64_t>() = value;
        }, "value"_a, "Set the value as int64")
        .def("set_double", [](PlainValue& self, double value) {
            self.as<double>() = value;
        }, "value"_a, "Set the value as double")
        .def("set_bool", [](PlainValue& self, bool value) {
            self.as<bool>() = value;
        }, "value"_a, "Set the value as bool")
        .def("set_string", [](PlainValue& self, const std::string& value) {
            self.as<std::string>() = value;
        }, "value"_a, "Set the value as string")

        // Operations
        .def("equals", static_cast<bool (PlainValue::*)(const ConstValueView&) const>(
            &PlainValue::equals), "other"_a, "Check equality with a view")
        .def("hash", &PlainValue::hash, "Compute the hash")
        .def("to_string", &PlainValue::to_string, "Convert to string")

        // Python interop (Design Doc Section 6.2)
        .def("to_python", &PlainValue::to_python, "Convert to a Python object")
        .def("from_python", &PlainValue::from_python, "src"_a,
            "Set the value from a Python object")

        // Static copy method
        .def_static("copy", static_cast<PlainValue (*)(const PlainValue&)>(&PlainValue::copy),
            "other"_a, "Create a copy of a Value")
        .def_static("copy_view", static_cast<PlainValue (*)(const ConstValueView&)>(
            &PlainValue::copy), "view"_a, "Create a copy from a view")

        // Python special methods
        .def("__eq__", [](const PlainValue& self, const ConstValueView& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &PlainValue::hash)
        .def("__str__", &PlainValue::to_string)
        .def("__repr__", [](const PlainValue& self) {
            if (!self.valid()) return std::string("PlainValue(invalid)");
            return "PlainValue(" + self.to_string() + ")";
        });
}

// ============================================================================
// CachedValue (Value<WithPythonCache>) Binding
// ============================================================================

static void register_cached_value(nb::module_& m) {
    nb::class_<CachedValue>(m, "CachedValue",
        "Owning type-erased value storage with Python object caching")
        // Constructors from scalars
        .def(nb::init<int64_t>(), "value"_a, "Construct from int64")
        .def(nb::init<double>(), "value"_a, "Construct from double")
        .def(nb::init<bool>(), "value"_a, "Construct from bool")
        .def(nb::init<const std::string&>(), "value"_a, "Construct from string")
        .def(nb::init<engine_date_t>(), "value"_a, "Construct from date")
        .def(nb::init<engine_time_t>(), "value"_a, "Construct from datetime")
        .def(nb::init<engine_time_delta_t>(), "value"_a, "Construct from timedelta")
        // Construct from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
            "Construct from type schema (default value)")
        // Construct from view (copy)
        .def(nb::init<const ConstValueView&>(), "view"_a,
            "Construct by copying from a view")

        // Validity
        .def("valid", &CachedValue::valid, "Check if the Value contains data")
        .def("__bool__", &CachedValue::valid, "Boolean conversion (validity)")
        .def_prop_ro("schema", &CachedValue::schema, nb::rv_policy::reference,
            "Get the type schema")

        // View access (Design Doc Section 6.2)
        .def("view", static_cast<ValueView (CachedValue::*)()>(&CachedValue::view),
            "Get a mutable view of the data (invalidates cache)")
        .def("const_view", &CachedValue::const_view, "Get a const view of the data")

        // Specialized view access (Design Doc Section 6.2)
        .def("as_tuple", static_cast<TupleView (CachedValue::*)()>(&CachedValue::as_tuple),
            "Get as a tuple view (mutable, invalidates cache)")
        .def("as_bundle", static_cast<BundleView (CachedValue::*)()>(&CachedValue::as_bundle),
            "Get as a bundle view (mutable, invalidates cache)")
        .def("as_list", static_cast<ListView (CachedValue::*)()>(&CachedValue::as_list),
            "Get as a list view (mutable, invalidates cache)")
        .def("as_set", static_cast<SetView (CachedValue::*)()>(&CachedValue::as_set),
            "Get as a set view (mutable, invalidates cache)")
        .def("as_map", static_cast<MapView (CachedValue::*)()>(&CachedValue::as_map),
            "Get as a map view (mutable, invalidates cache)")

        // Typed access - explicit getters (Design Doc Section 7)
        .def("as_int", [](CachedValue& self) { return self.checked_as<int64_t>(); },
            "Get the value as int64 (throws if type mismatch)")
        .def("as_double", [](CachedValue& self) { return self.checked_as<double>(); },
            "Get the value as double (throws if type mismatch)")
        .def("as_bool", [](CachedValue& self) { return self.checked_as<bool>(); },
            "Get the value as bool (throws if type mismatch)")
        .def("as_string", [](CachedValue& self) -> std::string {
            return self.checked_as<std::string>();
        }, "Get the value as string (throws if type mismatch)")

        // Typed setters
        .def("set_int", [](CachedValue& self, int64_t value) {
            self.as<int64_t>() = value;
        }, "value"_a, "Set the value as int64 (invalidates cache)")
        .def("set_double", [](CachedValue& self, double value) {
            self.as<double>() = value;
        }, "value"_a, "Set the value as double (invalidates cache)")
        .def("set_bool", [](CachedValue& self, bool value) {
            self.as<bool>() = value;
        }, "value"_a, "Set the value as bool (invalidates cache)")
        .def("set_string", [](CachedValue& self, const std::string& value) {
            self.as<std::string>() = value;
        }, "value"_a, "Set the value as string (invalidates cache)")

        // Operations
        .def("equals", static_cast<bool (CachedValue::*)(const ConstValueView&) const>(
            &CachedValue::equals), "other"_a, "Check equality with a view")
        .def("hash", &CachedValue::hash, "Compute the hash")
        .def("to_string", &CachedValue::to_string, "Convert to string")

        // Python interop (Design Doc Section 6.2)
        .def("to_python", &CachedValue::to_python,
            "Convert to a Python object (cached)")
        .def("from_python", &CachedValue::from_python, "src"_a,
            "Set the value from a Python object (updates cache)")

        // Static copy method
        .def_static("copy", static_cast<CachedValue (*)(const CachedValue&)>(&CachedValue::copy),
            "other"_a, "Create a copy of a Value")
        .def_static("copy_view", static_cast<CachedValue (*)(const ConstValueView&)>(
            &CachedValue::copy), "view"_a, "Create a copy from a view")

        // Python special methods
        .def("__eq__", [](const CachedValue& self, const ConstValueView& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &CachedValue::hash)
        .def("__str__", &CachedValue::to_string)
        .def("__repr__", [](const CachedValue& self) {
            if (!self.valid()) return std::string("CachedValue(invalid)");
            return "CachedValue(" + self.to_string() + ")";
        });
}

// ============================================================================
// TSValue Registration (Caching + Modification Tracking)
// ============================================================================

static void register_ts_value(nb::module_& m) {
    nb::class_<TSValue>(m, "TSValue",
        "Value with caching and modification tracking (for time-series)")
        // Constructors from scalars
        .def(nb::init<int64_t>(), "value"_a, "Construct from int64")
        .def(nb::init<double>(), "value"_a, "Construct from double")
        .def(nb::init<bool>(), "value"_a, "Construct from bool")
        .def(nb::init<const std::string&>(), "value"_a, "Construct from string")
        .def(nb::init<engine_date_t>(), "value"_a, "Construct from date")
        .def(nb::init<engine_time_t>(), "value"_a, "Construct from datetime")
        .def(nb::init<engine_time_delta_t>(), "value"_a, "Construct from timedelta")
        // Construct from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
            "Construct from type schema (default value)")
        // Construct from view (copy)
        .def(nb::init<const ConstValueView&>(), "view"_a,
            "Construct by copying from a view")

        // Validity
        .def("valid", &TSValue::valid, "Check if the Value contains data")
        .def("__bool__", &TSValue::valid, "Boolean conversion (validity)")
        .def_prop_ro("schema", &TSValue::schema, nb::rv_policy::reference,
            "Get the type schema")

        // View access
        .def("view", static_cast<ValueView (TSValue::*)()>(&TSValue::view),
            "Get a mutable view of the data (invalidates cache)")
        .def("const_view", &TSValue::const_view, "Get a const view of the data")

        // Specialized view access
        .def("as_tuple", static_cast<TupleView (TSValue::*)()>(&TSValue::as_tuple),
            "Get as a tuple view (mutable, invalidates cache)")
        .def("as_bundle", static_cast<BundleView (TSValue::*)()>(&TSValue::as_bundle),
            "Get as a bundle view (mutable, invalidates cache)")
        .def("as_list", static_cast<ListView (TSValue::*)()>(&TSValue::as_list),
            "Get as a list view (mutable, invalidates cache)")
        .def("as_set", static_cast<SetView (TSValue::*)()>(&TSValue::as_set),
            "Get as a set view (mutable, invalidates cache)")
        .def("as_map", static_cast<MapView (TSValue::*)()>(&TSValue::as_map),
            "Get as a map view (mutable, invalidates cache)")

        // Python interop
        .def("to_python", &TSValue::to_python,
            "Convert to Python object (uses cache)")
        .def("from_python", &TSValue::from_python, "src"_a,
            "Set value from Python object (notifies callbacks)")

        // Modification tracking
        .def("on_modified", [](TSValue& self, nb::object callback) {
            self.on_modified([callback]() {
                callback();
            });
        }, "callback"_a, "Register a callback for modification events")

        // Equality and comparison
        .def("equals", static_cast<bool (TSValue::*)(const ConstValueView&) const>(&TSValue::equals),
            "other"_a, "Check equality with a view")

        // Python special methods
        .def("__eq__", [](const TSValue& self, const ConstValueView& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &TSValue::hash)
        .def("__str__", &TSValue::to_string)
        .def("__repr__", [](const TSValue& self) {
            if (!self.valid()) return std::string("TSValue(invalid)");
            return "TSValue(" + self.to_string() + ")";
        });
}

// ============================================================================
// ValidatedValue Registration (Rejects None)
// ============================================================================

static void register_validated_value(nb::module_& m) {
    nb::class_<ValidatedValue>(m, "ValidatedValue",
        "Value that validates input (rejects None)")
        // Constructors from scalars
        .def(nb::init<int64_t>(), "value"_a, "Construct from int64")
        .def(nb::init<double>(), "value"_a, "Construct from double")
        .def(nb::init<bool>(), "value"_a, "Construct from bool")
        .def(nb::init<const std::string&>(), "value"_a, "Construct from string")
        .def(nb::init<engine_date_t>(), "value"_a, "Construct from date")
        .def(nb::init<engine_time_t>(), "value"_a, "Construct from datetime")
        .def(nb::init<engine_time_delta_t>(), "value"_a, "Construct from timedelta")
        // Construct from schema
        .def(nb::init<const TypeMeta*>(), "schema"_a,
            "Construct from type schema (default value)")
        // Construct from view (copy)
        .def(nb::init<const ConstValueView&>(), "view"_a,
            "Construct by copying from a view")

        // Validity
        .def("valid", &ValidatedValue::valid, "Check if the Value contains data")
        .def("__bool__", &ValidatedValue::valid, "Boolean conversion (validity)")
        .def_prop_ro("schema", &ValidatedValue::schema, nb::rv_policy::reference,
            "Get the type schema")

        // View access
        .def("view", static_cast<ValueView (ValidatedValue::*)()>(&ValidatedValue::view),
            "Get a mutable view of the data")
        .def("const_view", &ValidatedValue::const_view, "Get a const view of the data")

        // Python interop
        .def("to_python", &ValidatedValue::to_python, "Convert to Python object")
        .def("from_python", &ValidatedValue::from_python, "src"_a,
            "Set value from Python object (throws if None)")

        // Equality and comparison
        .def("equals", static_cast<bool (ValidatedValue::*)(const ConstValueView&) const>(&ValidatedValue::equals),
            "other"_a, "Check equality with a view")

        // Python special methods
        .def("__eq__", [](const ValidatedValue& self, const ConstValueView& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &ValidatedValue::hash)
        .def("__str__", &ValidatedValue::to_string)
        .def("__repr__", [](const ValidatedValue& self) {
            if (!self.valid()) return std::string("ValidatedValue(invalid)");
            return "ValidatedValue(" + self.to_string() + ")";
        });
}

// ============================================================================
// Main Registration Function
// ============================================================================

void value_register_with_nanobind(nb::module_& m) {
    // Create a submodule for the value type system
    auto value_mod = m.def_submodule("value", "Value type system bindings");

    // Register enums first
    register_type_kind(value_mod);

    // Register BundleFieldInfo before TypeMeta (TypeMeta references it)
    register_bundle_field_info(value_mod);

    // Register TypeMeta and TypeRegistry
    register_type_meta(value_mod);
    register_type_builders(value_mod);
    register_type_registry(value_mod);

    // Register free functions for scalar type metadata (Design Doc Section 3.2)
    register_scalar_type_meta_functions(value_mod);

    // Register base view classes (order matters for inheritance)
    register_const_value_view(value_mod);
    register_value_view(value_mod);

    // Register indexed view base classes
    register_const_indexed_view(value_mod);
    register_indexed_view(value_mod);

    // Register specialized views
    register_tuple_views(value_mod);
    register_bundle_views(value_mod);
    register_list_views(value_mod);
    register_set_views(value_mod);
    register_map_views(value_mod);

    // Register Value classes
    register_plain_value(value_mod);
    register_cached_value(value_mod);
    register_ts_value(value_mod);
    register_validated_value(value_mod);

    // Also export the main types at the module level for convenience
    m.attr("PlainValue") = value_mod.attr("PlainValue");
    m.attr("CachedValue") = value_mod.attr("CachedValue");
    m.attr("TSValue") = value_mod.attr("TSValue");
    m.attr("ValidatedValue") = value_mod.attr("ValidatedValue");
    m.attr("ConstValueView") = value_mod.attr("ConstValueView");
    m.attr("ValueView") = value_mod.attr("ValueView");
    m.attr("TypeRegistry") = value_mod.attr("TypeRegistry");
    m.attr("TypeMeta") = value_mod.attr("TypeMeta");
}

} // namespace hgraph
