#include <hgraph/api/python/py_value.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/composite_ops.h>
#include <hgraph/types/value/path.h>
#include <hgraph/types/value/traversal.h>
#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/value/tracked_set_storage.h>
#include <hgraph/types/value/tracked_set_view.h>
#include <hgraph/types/value/set_delta_value.h>
#include <hgraph/types/value/view_range.h>
#include <hgraph/types/value/delta_storage.h>
#include <hgraph/types/value/delta_view.h>
#include <hgraph/types/value/delta_value.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/vector.h>
#include <Python.h>  // For Py_buffer

namespace hgraph {

using namespace hgraph::value;
using namespace nanobind::literals;

// ============================================================================
// TypeKind Enum Binding
// ============================================================================

static void register_type_kind(nb::module_& m) {
    nb::enum_<TypeKind>(m, "TypeKind", "Categories of types in the Value system")
        .value("Atomic", TypeKind::Atomic, "Atomic values: int, double, bool, string, datetime, etc.")
        .value("Tuple", TypeKind::Tuple, "Indexed heterogeneous collection (unnamed, positional access only)")
        .value("Bundle", TypeKind::Bundle, "Named field collection (struct-like, index + name access)")
        .value("List", TypeKind::List, "Indexed homogeneous collection (dynamic size)")
        .value("Set", TypeKind::Set, "Unordered unique elements")
        .value("Map", TypeKind::Map, "Key-value pairs")
        .value("CyclicBuffer", TypeKind::CyclicBuffer, "Fixed-size circular buffer (re-centers on read)")
        .value("Queue", TypeKind::Queue, "FIFO queue with optional max capacity");
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
        // Name-based lookup (Phase 1)
        .def_prop_ro("name", [](const TypeMeta& self) -> nb::object {
            return self.name ? nb::str(self.name) : nb::none();
        }, "Human-readable type name (or None)")
        .def_static("get", [](const std::string& name) {
            return TypeMeta::get(name);
        }, nb::rv_policy::reference, "Look up a TypeMeta by name")
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
            "Create a map type builder")
        .def("cyclic_buffer", &TypeRegistry::cyclic_buffer, "element_type"_a, "capacity"_a,
            "Create a cyclic buffer type builder")
        .def("queue", &TypeRegistry::queue, "element_type"_a,
            "Create a queue type builder");

    // CyclicBuffer type builder
    nb::class_<CyclicBufferTypeBuilder>(m, "CyclicBufferTypeBuilder",
        "Builder for cyclic buffer types")
        .def("build", &CyclicBufferTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the cyclic buffer type");

    // Queue type builder
    nb::class_<QueueTypeBuilder>(m, "QueueTypeBuilder",
        "Builder for queue types")
        .def("max_capacity", &QueueTypeBuilder::max_capacity, "max"_a,
            "Set the maximum capacity (0 = unbounded)")
        .def("build", &QueueTypeBuilder::build, nb::rv_policy::reference,
            "Build and register the queue type");
}

// ============================================================================
// View Binding
// ============================================================================

static void register_const_value_view(nb::module_& m) {
    nb::class_<View>(m, "View",
        "Non-owning const view into a Value, providing read-only access")
        // Validity
        .def("valid", &View::valid, "Check if the view is valid")
        .def("__bool__", &View::valid, "Boolean conversion (validity)")
        .def_prop_ro("schema", &View::schema, nb::rv_policy::reference,
            "Get the type schema")

        // Type kind queries
        .def("is_scalar", &View::is_scalar, "Check if this is a scalar type")
        .def("is_tuple", &View::is_tuple, "Check if this is a tuple type")
        .def("is_bundle", &View::is_bundle, "Check if this is a bundle type")
        .def("is_list", &View::is_list, "Check if this is a list type")
        .def("is_fixed_list", &View::is_fixed_list, "Check if this is a fixed-size list")
        .def("is_set", &View::is_set, "Check if this is a set type")
        .def("is_map", &View::is_map, "Check if this is a map type")
        .def("is_cyclic_buffer", &View::is_cyclic_buffer, "Check if this is a cyclic buffer type")
        .def("is_queue", &View::is_queue, "Check if this is a queue type")

        // Type checking
        .def("is_type", &View::is_type, "schema"_a,
            "Check if this view has a specific schema (pointer equality)")

        // Typed scalar access - explicit getters since Python can't do as<T>()
        .def("as_int", [](const View& self) { return self.checked_as<int64_t>(); },
            "Get the value as int64 (throws if type mismatch)")
        .def("as_double", [](const View& self) { return self.checked_as<double>(); },
            "Get the value as double (throws if type mismatch)")
        .def("as_bool", [](const View& self) { return self.checked_as<bool>(); },
            "Get the value as bool (throws if type mismatch)")
        .def("as_string", [](const View& self) -> std::string {
            return self.checked_as<std::string>();
        }, "Get the value as string (throws if type mismatch)")

        // Safe typed access (returns None on type mismatch)
        .def("try_as_int", [](const View& self) -> std::optional<int64_t> {
            auto* p = self.try_as<int64_t>();
            return p ? std::optional<int64_t>(*p) : std::nullopt;
        }, "Try to get the value as int64 (returns None if type mismatch)")
        .def("try_as_double", [](const View& self) -> std::optional<double> {
            auto* p = self.try_as<double>();
            return p ? std::optional<double>(*p) : std::nullopt;
        }, "Try to get the value as double (returns None if type mismatch)")
        .def("try_as_bool", [](const View& self) -> std::optional<bool> {
            auto* p = self.try_as<bool>();
            return p ? std::optional<bool>(*p) : std::nullopt;
        }, "Try to get the value as bool (returns None if type mismatch)")
        .def("try_as_string", [](const View& self) -> std::optional<std::string> {
            auto* p = self.try_as<std::string>();
            return p ? std::optional<std::string>(*p) : std::nullopt;
        }, "Try to get the value as string (returns None if type mismatch)")

        // Type-specific scalar checks
        .def("is_int", [](const View& self) { return self.is_scalar_type<int64_t>(); },
            "Check if this is an int64 scalar")
        .def("is_double", [](const View& self) { return self.is_scalar_type<double>(); },
            "Check if this is a double scalar")
        .def("is_bool", [](const View& self) { return self.is_scalar_type<bool>(); },
            "Check if this is a bool scalar")
        .def("is_string", [](const View& self) { return self.is_scalar_type<std::string>(); },
            "Check if this is a string scalar")

        // Specialized view access (Design Doc Section 6.2 - as_bundle(), as_list(), etc.)
        // Use mutable versions since Python doesn't have const
        .def("as_tuple", static_cast<TupleView (View::*)()>(&View::as_tuple),
            "Get as a tuple view (throws if not a tuple)")
        .def("as_bundle", static_cast<BundleView (View::*)()>(&View::as_bundle),
            "Get as a bundle view (throws if not a bundle)")
        .def("as_list", static_cast<ListView (View::*)()>(&View::as_list),
            "Get as a list view (throws if not a list)")
        .def("as_set", static_cast<SetView (View::*)()>(&View::as_set),
            "Get as a set view (throws if not a set)")
        .def("as_map", static_cast<MapView (View::*)()>(&View::as_map),
            "Get as a map view (throws if not a map)")
        .def("as_cyclic_buffer", static_cast<CyclicBufferView (View::*)()>(&View::as_cyclic_buffer),
            "Get as a cyclic buffer view (throws if not a cyclic buffer)")
        .def("as_queue", static_cast<QueueView (View::*)()>(&View::as_queue),
            "Get as a queue view (throws if not a queue)")

        // Safe composite type access (returns None if type mismatch)
        .def("try_as_tuple", static_cast<std::optional<TupleView> (View::*)()>(&View::try_as_tuple),
            "Try to get as a tuple view (returns None if not a tuple)")
        .def("try_as_bundle", static_cast<std::optional<BundleView> (View::*)()>(&View::try_as_bundle),
            "Try to get as a bundle view (returns None if not a bundle)")
        .def("try_as_list", static_cast<std::optional<ListView> (View::*)()>(&View::try_as_list),
            "Try to get as a list view (returns None if not a list)")
        .def("try_as_set", static_cast<std::optional<SetView> (View::*)()>(&View::try_as_set),
            "Try to get as a set view (returns None if not a set)")
        .def("try_as_map", static_cast<std::optional<MapView> (View::*)()>(&View::try_as_map),
            "Try to get as a map view (returns None if not a map)")
        .def("try_as_cyclic_buffer", static_cast<std::optional<CyclicBufferView> (View::*)()>(&View::try_as_cyclic_buffer),
            "Try to get as a cyclic buffer view (returns None if not a cyclic buffer)")
        .def("try_as_queue", static_cast<std::optional<QueueView> (View::*)()>(&View::try_as_queue),
            "Try to get as a queue view (returns None if not a queue)")

        // Operations
        .def("equals", &View::equals, "other"_a, "Check equality with another view")
        .def("hash", &View::hash, "Compute the hash of the value")
        .def("to_string", &View::to_string, "Convert the value to a string")
        .def("to_python", &View::to_python, "Convert the value to a Python object")
        .def("clone", [](const View& self) -> PlainValue {
            return self.clone<NoCache>();
        }, "Create a deep copy of this value")
        // Path tracking
        .def("path_string", &View::path_string, "Get the navigation path as a string")
        .def("path_depth", [](const View& self) {
            return self.path().depth();
        }, "Get the depth of the navigation path")

        // Python special methods
        .def("__eq__", [](const View& self, const View& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &View::hash)
        .def("__str__", &View::to_string)
        .def("__repr__", [](const View& self) {
            if (!self.valid()) return std::string("View(invalid)");
            return "View(" + self.to_string() + ")";
        })

        // Visitor Pattern (User Guide Section 8)
        .def("visit", [](const View& self, nb::callable handler) {
            // Convert value to Python and call the handler
            nb::object py_value = self.to_python();
            return handler(py_value);
        }, "handler"_a,
            "Visit the value with a callable handler.\n\n"
            "The handler is called with the Python-converted value.\n"
            "Returns whatever the handler returns.")

        .def("visit_void", [](const View& self, nb::callable handler) {
            // Convert value to Python and call the handler (discard result)
            nb::object py_value = self.to_python();
            handler(py_value);
        }, "handler"_a,
            "Visit the value with a callable handler for side effects only.\n\n"
            "The handler is called with the Python-converted value.\n"
            "The return value is ignored.")

        .def("match", [](const View& self, nb::args type_handler_pairs) {
            // Pattern matching: pairs of (type, handler)
            // Last pair can have None as type for default handler
            nb::object py_value = self.to_python();

            for (size_t i = 0; i < type_handler_pairs.size(); ++i) {
                nb::tuple pair = nb::cast<nb::tuple>(type_handler_pairs[i]);
                if (pair.size() != 2) {
                    throw std::runtime_error("match: each argument must be a (type, handler) tuple");
                }

                nb::object type_or_none = pair[0];
                nb::callable handler = nb::cast<nb::callable>(pair[1]);

                if (type_or_none.is_none()) {
                    // Default handler
                    return handler(py_value);
                }

                // Check if py_value is an instance of the type
                if (nb::isinstance(py_value, type_or_none)) {
                    return handler(py_value);
                }
            }

            throw std::runtime_error("match: no handler matched for value type");
        },
            "Pattern match on the value with (type, handler) pairs.\n\n"
            "Each argument should be a tuple of (type, callable).\n"
            "Use (None, handler) as the last pair for a default handler.\n"
            "Returns the result of the first matching handler.\n\n"
            "Example:\n"
            "    result = view.match(\n"
            "        (int, lambda x: f'int:{x}'),\n"
            "        (float, lambda x: f'float:{x}'),\n"
            "        (str, lambda x: f'str:{x}'),\n"
            "        (None, lambda x: 'default')\n"
            "    )")

        // Path-based Navigation (User Guide Section 10)
        .def("navigate", [](const View& self, const std::string& path_str) {
            return navigate(self, path_str);
        }, "path"_a,
            "Navigate through the value using a path string.\n\n"
            "Returns the View at the path destination.\n"
            "Throws if navigation fails.")
        .def("try_navigate", [](const View& self, const std::string& path_str)
            -> std::optional<View> {
            return try_navigate(self, path_str);
        }, "path"_a,
            "Try to navigate through the value using a path string.\n\n"
            "Returns the View at the destination, or None on failure.")

        // ========== Mutable Operations (merged from ValueView) ==========

        // Mutable scalar setters
        .def("set_int", [](View& self, int64_t value) {
            self.checked_as<int64_t>() = value;
        }, "value"_a, "Set the value as int64 (throws if type mismatch)")
        .def("set_double", [](View& self, double value) {
            self.checked_as<double>() = value;
        }, "value"_a, "Set the value as double (throws if type mismatch)")
        .def("set_bool", [](View& self, bool value) {
            self.checked_as<bool>() = value;
        }, "value"_a, "Set the value as bool (throws if type mismatch)")
        .def("set_string", [](View& self, const std::string& value) {
            self.checked_as<std::string>() = value;
        }, "value"_a, "Set the value as string (throws if type mismatch)")

        // Mutation from another view
        .def("copy_from", &View::copy_from, "other"_a,
            "Copy data from another view (schemas must match)")

        // Python interop (mutable)
        .def("from_python", &View::from_python, "src"_a,
            "Set the value from a Python object")

        // Raw data access (returns pointer as integer for debugging/FFI)
        .def("data_ptr", [](View& self) -> uintptr_t {
            return reinterpret_cast<uintptr_t>(self.data());
        }, "Get raw data pointer as integer (for debugging/FFI)")

        // Mutable Visitor Pattern (User Guide Section 8)
        .def("visit_mut", [](View& self, nb::callable handler) {
            nb::object py_value = self.to_python();
            nb::object result = handler(py_value);
            if (!result.is_none()) {
                self.from_python(result);
            }
        }, "handler"_a,
            "Visit the value with a mutable callable handler.\n\n"
            "The handler is called with the Python-converted value.\n"
            "If the handler returns a value (not None), the view is updated with it.")

        // Path-based Navigation (mutable)
        .def("navigate_mut", [](View& self, const std::string& path_str) {
            return navigate_mut(self, path_str);
        }, "path"_a,
            "Navigate through the mutable value using a path string.\n\n"
            "Returns the View at the path destination.\n"
            "Throws if navigation fails.")
        .def("try_navigate_mut", [](View& self, const std::string& path_str)
            -> std::optional<View> {
            return try_navigate_mut(self, path_str);
        }, "path"_a,
            "Try to navigate through the mutable value using a path string.\n\n"
            "Returns the View at the destination, or None on failure.");
}

// ============================================================================
// IndexedView Binding (merged const/mutable)
// ============================================================================

static void register_indexed_view(nb::module_& m) {
    nb::class_<IndexedView, View>(m, "IndexedView",
        "Base class for types supporting index-based access")
        .def("size", &IndexedView::size, "Get the number of elements")
        .def("empty", &IndexedView::empty, "Check if empty")
        .def("at", static_cast<View (IndexedView::*)(size_t)>(&IndexedView::at),
            "index"_a, "Get element at index")
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
        .def("set", static_cast<void (IndexedView::*)(size_t, const View&)>(&IndexedView::set),
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
// TupleView Binding (merged const/mutable)
// ============================================================================

static void register_tuple_views(nb::module_& m) {
    nb::class_<TupleView, IndexedView>(m, "TupleView",
        "View for tuple types (heterogeneous index-only access)")
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
// BundleView Binding (merged const/mutable)
// ============================================================================

static void register_bundle_views(nb::module_& m) {
    nb::class_<BundleView, IndexedView>(m, "BundleView",
        "View for bundle types (struct-like access)")
        // Named field access
        .def("at_name", [](BundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a, "Get field by name")
        // Alias for at_name for backwards compatibility
        .def("at_name_mut", [](BundleView& self, const std::string& name) {
            return self.at(name);
        }, "name"_a, "Get field by name (alias for at_name)")
        // Set by name with View
        .def("set_name", [](BundleView& self, const std::string& name, const View& value) {
            self.set(name, value);
        }, "name"_a, "value"_a, "Set field by name from View")
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
        }, "name"_a, "Get field index by name (returns size() if not found)")
        // items() iteration - yields (name, value) pairs as Python objects
        .def("items", [](const BundleView& self) {
            nb::list result;
            for (auto [name, field] : self.items()) {
                result.append(nb::make_tuple(nb::cast(name), field.to_python()));
            }
            return result;
        }, "Iterate over (name, value) pairs")
        // items_views() - yields (name, View) pairs without conversion
        .def("items_views", [](const BundleView& self) {
            nb::list result;
            for (auto [name, field] : self.items()) {
                result.append(nb::make_tuple(nb::cast(name), nb::cast(field)));
            }
            return result;
        }, "Iterate over (name, view) pairs as View objects");
}

// ============================================================================
// ListView Binding (merged const/mutable)
// ============================================================================

// Helper function to check if element type is buffer compatible
static bool is_list_buffer_compatible(const ListView& list) {
    const TypeMeta* elem = list.element_type();
    if (!elem || elem->kind != TypeKind::Atomic) return false;

    // Use the BufferCompatible flag from TypeMeta
    return elem->is_buffer_compatible();
}


static void register_list_views(nb::module_& m) {
    nb::class_<ListView, IndexedView>(m, "ListView",
        "View for list types")
        .def("front", static_cast<View (ListView::*)()>(&ListView::front), "Get the first element")
        .def("back", static_cast<View (ListView::*)()>(&ListView::back), "Get the last element")
        .def("element_type", &ListView::element_type, nb::rv_policy::reference,
            "Get the element type")
        .def("is_fixed", &ListView::is_fixed, "Check if this is a fixed-size list")
        .def("is_buffer_compatible", [](const ListView& self) {
            // Check if element type supports buffer protocol
            const TypeMeta* elem = self.element_type();
            if (!elem || elem->kind != TypeKind::Atomic) return false;
            return elem->is_buffer_compatible();
        }, "Check if this list supports the buffer protocol (numpy compatibility)")
        // Zero-copy numpy conversion for mutable list view
        .def("to_numpy", [](ListView& self) -> nb::object {
            const TypeMeta* elem = self.element_type();
            if (!elem || elem->kind != TypeKind::Atomic || !elem->is_buffer_compatible()) {
                throw std::runtime_error("List element type not buffer compatible for numpy");
            }

            // Import numpy
            nb::module_ np = nb::module_::import_("numpy");

            size_t n = self.size();
            if (n == 0) {
                // Return empty array for empty list
                const char* dtype = "int64";
                if (elem == scalar_type_meta<double>()) dtype = "float64";
                else if (elem == scalar_type_meta<bool>()) dtype = "bool";
                return np.attr("array")(nb::list(), "dtype"_a = dtype);
            }

            // Get pointer to data
            void* data_ptr = nullptr;
            const TypeMeta* schema = self.schema();
            if (schema->is_fixed_size()) {
                // Fixed list: data stored inline
                data_ptr = const_cast<void*>(self.data());
            } else {
                // Dynamic list: data in DynamicListStorage
                auto* storage = static_cast<DynamicListStorage*>(const_cast<void*>(self.data()));
                data_ptr = storage->data_ptr();
            }

            if (!data_ptr) {
                throw std::runtime_error("Cannot get data pointer for list");
            }

            // Create numpy array using ctypes to directly reference the memory
            // This creates a zero-copy view into the Value's storage
            // Use numpy.ctypeslib.as_array for zero-copy
            // But we need to ensure the Value stays alive - use numpy.ndarray directly
            nb::module_ ctypes = nb::module_::import_("ctypes");

            // Create a ctypes pointer to the data
            nb::object c_void_p = ctypes.attr("c_void_p")(reinterpret_cast<uintptr_t>(data_ptr));

            // Use numpy.ctypeslib.as_array with shape
            nb::object np_ctypeslib = np.attr("ctypeslib");

            // Create the appropriate ctypes array type
            nb::object c_type;
            if (elem == scalar_type_meta<int64_t>()) {
                c_type = ctypes.attr("c_int64");
            } else if (elem == scalar_type_meta<double>()) {
                c_type = ctypes.attr("c_double");
            } else if (elem == scalar_type_meta<bool>()) {
                c_type = ctypes.attr("c_bool");
            } else {
                throw std::runtime_error("Unsupported element type for zero-copy numpy");
            }

            // Create pointer type and cast
            // Use Python's __mul__ to create array type (e.g., c_int64 * n)
            nb::object array_type = c_type.attr("__mul__")(n);
            nb::object ptr = ctypes.attr("cast")(c_void_p, ctypes.attr("POINTER")(array_type));

            // Use np.ctypeslib.as_array to create zero-copy view
            nb::object arr = np_ctypeslib.attr("as_array")(ptr.attr("contents"));

            return arr;
        }, "Convert to a numpy array (zero-copy, shares memory with Value)")
        // Dynamic list operations with type checking
        .def("append", [](ListView& self, const View& value) {
            // Type check: value schema must match element type
            if (value.schema() != self.element_type()) {
                throw std::runtime_error("Type mismatch: cannot append value of different type");
            }
            self.append(value);
        }, "value"_a, "Append an element (throws if fixed-size or type mismatch)")
        // Override set with type checking for lists
        .def("set", [](ListView& self, size_t index, const View& value) {
            if (value.schema() != self.element_type()) {
                throw std::runtime_error("Type mismatch: cannot set value of different type");
            }
            self.set(index, value);
        }, "index"_a, "value"_a, "Set element at index (with type checking)")
        .def("pop_back", &ListView::pop_back, "Remove the last element (throws if fixed-size)")
        .def("clear", &ListView::clear, "Clear all elements (throws if fixed-size)")
        .def("resize", &ListView::resize, "new_size"_a, "Resize the list (throws if fixed-size)")
        .def("reset", static_cast<void (ListView::*)(const View&)>(&ListView::reset),
            "sentinel"_a, "Reset all elements to a sentinel value")
        // items() iteration - yields (index, value) pairs as Python objects
        .def("items", [](const ListView& self) {
            nb::list result;
            for (auto [idx, elem] : self.items()) {
                result.append(nb::make_tuple(nb::cast(idx), elem.to_python()));
            }
            return result;
        }, "Iterate over (index, value) pairs")
        // items_views() - yields (index, View) pairs without conversion
        .def("items_views", [](const ListView& self) {
            nb::list result;
            for (auto [idx, elem] : self.items()) {
                result.append(nb::make_tuple(nb::cast(idx), nb::cast(elem)));
            }
            return result;
        }, "Iterate over (index, view) pairs as View objects");
}

// ============================================================================
// SetView Binding (merged const/mutable)
// ============================================================================

static void register_set_views(nb::module_& m) {
    nb::class_<SetView, View>(m, "SetView",
        "View for set types")
        .def("size", &SetView::size, "Get the number of elements")
        .def("empty", &SetView::empty, "Check if empty")
        .def("__len__", &SetView::size)
        .def("contains", static_cast<bool (SetView::*)(const View&) const>(
            &SetView::contains), "value"_a, "Check if an element is in the set")
        .def("__contains__", static_cast<bool (SetView::*)(const View&) const>(
            &SetView::contains), "value"_a)
        .def("add", static_cast<bool (SetView::*)(const View&)>(&SetView::add),
            "value"_a, "Add an element (returns true if added)")
        .def("remove", static_cast<bool (SetView::*)(const View&)>(&SetView::remove),
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
            return View(elem, self.schema()->element_type);
        }, "index"_a, "Get element at index")
        // Iteration support
        .def("__iter__", [](SetView& self) {
            nb::list result;
            for (size_t i = 0; i < self.size(); ++i) {
                const void* elem = self.schema()->ops->get_at(self.data(), i, self.schema());
                result.append(nb::cast(View(elem, self.schema()->element_type)));
            }
            return nb::iter(result);
        }, "Iterate over elements");
}

// ============================================================================
// TrackedSetStorage Binding
// ============================================================================

static void register_tracked_set(nb::module_& m) {
    // TrackedSetStorage - owning storage with delta tracking
    nb::class_<TrackedSetStorage>(m, "TrackedSetStorage",
        "Storage for sets with delta tracking (added/removed elements)")
        .def(nb::init<const TypeMeta*>(), "element_type"_a,
            "Create TrackedSetStorage with element type")
        .def("size", &TrackedSetStorage::size, "Get current set size")
        .def("empty", &TrackedSetStorage::empty, "Check if empty")
        .def("has_delta", &TrackedSetStorage::has_delta, "Check if there are pending changes")
        .def("__len__", &TrackedSetStorage::size)
        .def("value", static_cast<const SetView (TrackedSetStorage::*)() const>(&TrackedSetStorage::value),
            "Get view of current set")
        .def("added", &TrackedSetStorage::added, "Get const view of added elements")
        .def("removed", &TrackedSetStorage::removed, "Get const view of removed elements")
        .def("contains", static_cast<bool (TrackedSetStorage::*)(const View&) const>(
            &TrackedSetStorage::contains), "elem"_a, "Check if element is in set")
        .def("__contains__", static_cast<bool (TrackedSetStorage::*)(const View&) const>(
            &TrackedSetStorage::contains), "elem"_a)
        .def("was_added", static_cast<bool (TrackedSetStorage::*)(const View&) const>(
            &TrackedSetStorage::was_added), "elem"_a, "Check if element was added this cycle")
        .def("was_removed", static_cast<bool (TrackedSetStorage::*)(const View&) const>(
            &TrackedSetStorage::was_removed), "elem"_a, "Check if element was removed this cycle")
        .def("add", static_cast<bool (TrackedSetStorage::*)(const View&)>(
            &TrackedSetStorage::add), "elem"_a, "Add element with delta tracking")
        .def("remove", static_cast<bool (TrackedSetStorage::*)(const View&)>(
            &TrackedSetStorage::remove), "elem"_a, "Remove element with delta tracking")
        .def("clear_deltas", &TrackedSetStorage::clear_deltas, "Clear delta tracking")
        .def("clear", &TrackedSetStorage::clear, "Clear all elements");

    // TrackedSetView (merged const/mutable)
    nb::class_<TrackedSetView>(m, "TrackedSetView",
        "View for TrackedSetStorage")
        .def(nb::init<TrackedSetStorage*>(), "storage"_a)
        .def(nb::init<const TrackedSetStorage*>(), "storage"_a)
        .def("size", &TrackedSetView::size, "Get set size")
        .def("empty", &TrackedSetView::empty, "Check if empty")
        .def("has_delta", &TrackedSetView::has_delta, "Check for pending changes")
        .def("__len__", &TrackedSetView::size)
        .def("value", &TrackedSetView::value, "Get view of current set")
        .def("added", &TrackedSetView::added, "Get view of added elements")
        .def("removed", &TrackedSetView::removed, "Get view of removed elements")
        .def("contains", static_cast<bool (TrackedSetView::*)(const View&) const>(
            &TrackedSetView::contains), "elem"_a)
        .def("__contains__", static_cast<bool (TrackedSetView::*)(const View&) const>(
            &TrackedSetView::contains), "elem"_a)
        .def("was_added", &TrackedSetView::was_added, "elem"_a)
        .def("was_removed", &TrackedSetView::was_removed, "elem"_a)
        .def("element_type", &TrackedSetView::element_type, nb::rv_policy::reference)
        .def("add", static_cast<bool (TrackedSetView::*)(const View&)>(
            &TrackedSetView::add), "elem"_a, "Add element with delta tracking")
        .def("remove", static_cast<bool (TrackedSetView::*)(const View&)>(
            &TrackedSetView::remove), "elem"_a, "Remove element with delta tracking")
        .def("clear_deltas", &TrackedSetView::clear_deltas)
        .def("clear", &TrackedSetView::clear)
        .def("__iter__", [](const TrackedSetView& self) {
            nb::list result;
            for (auto elem : self) {
                result.append(nb::cast(elem));
            }
            return nb::iter(result);
        }, "Iterate over current set elements");

    // SetDeltaValue - snapshot of delta changes
    nb::class_<SetDeltaValue>(m, "SetDeltaValue",
        "Value class representing set delta changes")
        .def(nb::init<const TypeMeta*>(), "element_type"_a,
            "Create empty delta with element type")
        .def(nb::init<SetView, SetView, const TypeMeta*>(),
            "added"_a, "removed"_a, "element_type"_a,
            "Create delta from added/removed sets")
        .def("added", &SetDeltaValue::added, "Get const view of added elements")
        .def("removed", &SetDeltaValue::removed, "Get const view of removed elements")
        .def("empty", &SetDeltaValue::empty, "Check if delta is empty")
        .def("added_count", &SetDeltaValue::added_count, "Get number of added elements")
        .def("removed_count", &SetDeltaValue::removed_count, "Get number of removed elements")
        .def("element_type", &SetDeltaValue::element_type, nb::rv_policy::reference)
        .def("to_python", &SetDeltaValue::to_python, "Convert to Python dict with added/removed");

    // ========== DeltaView Classes ==========

    // SetDeltaView - Non-owning view into set delta data
    nb::class_<SetDeltaView>(m, "SetDeltaView",
        "Non-owning view into set delta data (added/removed elements)")
        .def(nb::init<>(), "Create empty view")
        .def("empty", &SetDeltaView::empty, "Check if delta is empty")
        .def("change_count", &SetDeltaView::change_count, "Get total number of changes")
        .def("valid", &SetDeltaView::valid, "Check if view is valid")
        .def("added", [](const SetDeltaView& self) {
            nb::list result;
            for (auto elem : self.added()) {
                result.append(elem.to_python());
            }
            return result;
        }, "Get list of added elements")
        .def("removed", [](const SetDeltaView& self) {
            nb::list result;
            for (auto elem : self.removed()) {
                result.append(elem.to_python());
            }
            return result;
        }, "Get list of removed elements")
        .def("added_count", &SetDeltaView::added_count, "Get number of added elements")
        .def("removed_count", &SetDeltaView::removed_count, "Get number of removed elements")
        .def("element_type", &SetDeltaView::element_type, nb::rv_policy::reference);

    // MapDeltaView - Non-owning view into map delta data
    nb::class_<MapDeltaView>(m, "MapDeltaView",
        "Non-owning view into map delta data (added/updated/removed entries)")
        .def(nb::init<>(), "Create empty view")
        .def("empty", &MapDeltaView::empty, "Check if delta is empty")
        .def("change_count", &MapDeltaView::change_count, "Get total number of changes")
        .def("valid", &MapDeltaView::valid, "Check if view is valid")
        .def("added_keys", [](const MapDeltaView& self) {
            nb::list result;
            for (auto key : self.added_keys()) {
                result.append(key.to_python());
            }
            return result;
        }, "Get list of added keys")
        .def("added_items", [](const MapDeltaView& self) {
            nb::dict result;
            for (auto [key, value] : self.added_items()) {
                result[key.to_python()] = value.to_python();
            }
            return result;
        }, "Get dict of added entries")
        .def("updated_keys", [](const MapDeltaView& self) {
            nb::list result;
            for (auto key : self.updated_keys()) {
                result.append(key.to_python());
            }
            return result;
        }, "Get list of updated keys")
        .def("updated_items", [](const MapDeltaView& self) {
            nb::dict result;
            for (auto [key, value] : self.updated_items()) {
                result[key.to_python()] = value.to_python();
            }
            return result;
        }, "Get dict of updated entries")
        .def("removed_keys", [](const MapDeltaView& self) {
            nb::list result;
            for (auto key : self.removed_keys()) {
                result.append(key.to_python());
            }
            return result;
        }, "Get list of removed keys")
        .def("added_count", &MapDeltaView::added_count, "Get number of added entries")
        .def("updated_count", &MapDeltaView::updated_count, "Get number of updated entries")
        .def("removed_count", &MapDeltaView::removed_count, "Get number of removed entries")
        .def("key_type", &MapDeltaView::key_type, nb::rv_policy::reference)
        .def("value_type", &MapDeltaView::value_type, nb::rv_policy::reference);

    // ListDeltaView - Non-owning view into list delta data
    nb::class_<ListDeltaView>(m, "ListDeltaView",
        "Non-owning view into list delta data (updated indices/values)")
        .def(nb::init<>(), "Create empty view")
        .def("empty", &ListDeltaView::empty, "Check if delta is empty")
        .def("change_count", &ListDeltaView::change_count, "Get total number of changes")
        .def("valid", &ListDeltaView::valid, "Check if view is valid")
        .def("updated_items", [](const ListDeltaView& self) {
            nb::dict result;
            for (auto [idx_view, value] : self.updated_items()) {
                size_t idx = idx_view.as<size_t>();
                result[nb::int_(idx)] = value.to_python();
            }
            return result;
        }, "Get dict mapping updated indices to new values")
        .def("updated_indices", [](const ListDeltaView& self) {
            const auto& indices = self.updated_indices();
            nb::list result;
            for (size_t idx : indices) {
                result.append(nb::int_(idx));
            }
            return result;
        }, "Get list of updated indices")
        .def("updated_count", &ListDeltaView::updated_count, "Get number of updated elements")
        .def("element_type", &ListDeltaView::element_type, nb::rv_policy::reference);

    // ========== DeltaValue Class ==========

    nb::class_<DeltaValue>(m, "DeltaValue",
        "Owning storage for delta changes to collections")
        .def(nb::init<>(), "Create invalid/empty delta")
        .def(nb::init<const TypeMeta*>(), "value_schema"_a,
            "Create delta for a given value schema (Set, Map, or List)")
        .def("value_schema", &DeltaValue::value_schema, nb::rv_policy::reference,
            "Get the schema of the value this delta applies to")
        .def("kind", &DeltaValue::kind, "Get the kind of collection this delta tracks")
        .def("valid", &DeltaValue::valid, "Check if this is a valid delta")
        .def("empty", &DeltaValue::empty, "Check if delta is empty")
        .def("change_count", &DeltaValue::change_count, "Get total number of changes")
        .def("clear", &DeltaValue::clear, "Clear all recorded changes")
        .def("is_set_delta", &DeltaValue::is_set_delta, "Check if this is a set delta")
        .def("is_map_delta", &DeltaValue::is_map_delta, "Check if this is a map delta")
        .def("is_list_delta", &DeltaValue::is_list_delta, "Check if this is a list delta")
        .def("set_view", &DeltaValue::set_view, "Get const view for set delta")
        .def("map_view", &DeltaValue::map_view, "Get const view for map delta")
        .def("list_view", &DeltaValue::list_view, "Get const view for list delta")
        .def("to_python", &DeltaValue::to_python, "Convert to Python representation");
}

// ============================================================================
// KeySetView Binding - Set View Over Map Keys
// ============================================================================

static void register_key_set_view(nb::module_& m) {
    nb::class_<KeySetView, View>(m, "KeySetView",
        "Read-only set view over map keys (same interface as SetView)")
        .def("size", &KeySetView::size, "Get the number of keys")
        .def("empty", &KeySetView::empty, "Check if empty")
        .def("__len__", &KeySetView::size)
        .def("contains", static_cast<bool (KeySetView::*)(const View&) const>(
            &KeySetView::contains), "key"_a, "Check if a key is in the set")
        .def("__contains__", static_cast<bool (KeySetView::*)(const View&) const>(
            &KeySetView::contains), "key"_a)
        .def("element_type", &KeySetView::element_type, nb::rv_policy::reference,
            "Get the key/element type")
        // Iteration support using KeySetView::const_iterator
        .def("__iter__", [](const KeySetView& self) {
            nb::list result;
            for (auto it = self.begin(); it != self.end(); ++it) {
                result.append(nb::cast(*it));
            }
            return nb::iter(result);
        }, "Iterate over keys");
}

// ============================================================================
// MapView Binding (merged const/mutable)
// ============================================================================

static void register_map_views(nb::module_& m) {
    nb::class_<MapView, View>(m, "MapView",
        "View for map types")
        .def("size", &MapView::size, "Get the number of entries")
        .def("empty", &MapView::empty, "Check if empty")
        .def("__len__", &MapView::size)
        .def("at", static_cast<View (MapView::*)(const View&)>(&MapView::at),
            "key"_a, "Get value by key (throws if not found)")
        .def("at_const", static_cast<View (MapView::*)(const View&) const>(
            &MapView::at), "key"_a, "Get value by key (const, throws if not found)")
        // __getitem__ with auto-insert behavior (like C++ std::map::operator[])
        .def("__getitem__", [](MapView& self, const View& key) -> View {
            // If key doesn't exist, insert default value
            if (!self.contains(key)) {
                // Create a default-constructed value of the value type
                const TypeMeta* val_type = self.value_type();
                PlainValue default_val(val_type);
                self.set_item(key, default_val.const_view());
            }
            return self.at(key);
        }, "key"_a, "Get value by key (auto-inserts default if missing)")
        .def("contains", static_cast<bool (MapView::*)(const View&) const>(
            &MapView::contains), "key"_a, "Check if a key exists")
        .def("__contains__", static_cast<bool (MapView::*)(const View&) const>(
            &MapView::contains), "key"_a)
        .def("set_item", static_cast<void (MapView::*)(const View&, const View&)>(
            &MapView::set_item), "key"_a, "value"_a, "Set value for key")
        .def("add", static_cast<bool (MapView::*)(const View&, const View&)>(
            &MapView::add), "key"_a, "value"_a, "Add key-value pair (returns true if added)")
        .def("remove", static_cast<bool (MapView::*)(const View&)>(&MapView::remove),
            "key"_a, "Remove entry by key (returns true if removed)")
        .def("clear", &MapView::clear, "Clear all entries")
        .def("key_type", &MapView::key_type, nb::rv_policy::reference, "Get the key type")
        .def("value_type", &MapView::value_type, nb::rv_policy::reference, "Get the value type")
        // Iteration support - iterate over keys (like Python dict)
        .def("__iter__", [](MapView& self) {
            // Use keys() to get KeySetView, then iterate
            auto keys = self.keys();
            nb::list result;
            for (auto it = keys.begin(); it != keys.end(); ++it) {
                result.append(nb::cast(*it));
            }
            return nb::iter(result);
        }, "Iterate over keys (like dict)")
        .def("keys", &MapView::keys,
            "Get KeySetView over map keys (same interface as SetView)")
        .def("values", [](MapView& self) {
            nb::list result;
            for (auto [key, val] : self.items()) {
                result.append(val.to_python());
            }
            return result;
        }, "Get list of values")
        // items() iteration - yields (key, value) pairs as Python objects
        .def("items", [](const MapView& self) {
            nb::list result;
            for (auto [key, val] : self.items()) {
                result.append(nb::make_tuple(key.to_python(), val.to_python()));
            }
            return result;
        }, "Iterate over (key, value) pairs")
        // items_views() - yields (View, View) pairs without conversion
        .def("items_views", [](const MapView& self) {
            nb::list result;
            for (auto [key, val] : self.items()) {
                result.append(nb::make_tuple(nb::cast(key), nb::cast(val)));
            }
            return result;
        }, "Iterate over (key, value) pairs as View objects");
}

// ============================================================================
// CyclicBufferView Binding (merged const/mutable)
// ============================================================================

// Helper function to check if element type is buffer compatible
static bool is_cyclic_buffer_compatible(const CyclicBufferView& buf) {
    const TypeMeta* elem = buf.element_type();
    if (!elem || elem->kind != TypeKind::Atomic) return false;
    return elem->is_buffer_compatible();
}

static void register_cyclic_buffer_views(nb::module_& m) {
    nb::class_<CyclicBufferView, IndexedView>(m, "CyclicBufferView",
        "View for cyclic buffer types (fixed-size circular buffer)")
        .def("front", static_cast<View (CyclicBufferView::*)()>(&CyclicBufferView::front), "Get the oldest element")
        .def("back", static_cast<View (CyclicBufferView::*)()>(&CyclicBufferView::back), "Get the newest element")
        .def("element_type", &CyclicBufferView::element_type, nb::rv_policy::reference,
            "Get the element type")
        .def("capacity", &CyclicBufferView::capacity, "Get the fixed capacity")
        .def("full", &CyclicBufferView::full, "Check if the buffer is full")
        .def("append", static_cast<void (CyclicBufferView::*)(const View&)>(
            &CyclicBufferView::append), "value"_a,
            "Append a value (evicts oldest if full)")
        .def("clear", &CyclicBufferView::clear, "Clear all elements")
        .def("is_buffer_compatible", [](const CyclicBufferView& self) {
            const TypeMeta* elem = self.element_type();
            if (!elem || elem->kind != TypeKind::Atomic) return false;
            return elem->is_buffer_compatible();
        }, "Check if this buffer supports numpy conversion")
        .def("to_numpy", [](const CyclicBufferView& self) -> nb::object {
            const TypeMeta* elem = self.element_type();
            if (!elem || elem->kind != TypeKind::Atomic || !elem->is_buffer_compatible()) {
                throw std::runtime_error("CyclicBuffer element type not buffer compatible for numpy");
            }

            nb::module_ np = nb::module_::import_("numpy");
            size_t n = self.size();

            // Always copy (cannot be zero-copy due to re-centering)
            if (elem == scalar_type_meta<int64_t>()) {
                auto arr = np.attr("empty")(n, "dtype"_a = "int64");
                int64_t* ptr = reinterpret_cast<int64_t*>(
                    nb::cast<intptr_t>(arr.attr("ctypes").attr("data")));
                for (size_t i = 0; i < n; ++i) {
                    ptr[i] = self[i].as<int64_t>();
                }
                return arr;
            } else if (elem == scalar_type_meta<double>()) {
                auto arr = np.attr("empty")(n, "dtype"_a = "float64");
                double* ptr = reinterpret_cast<double*>(
                    nb::cast<intptr_t>(arr.attr("ctypes").attr("data")));
                for (size_t i = 0; i < n; ++i) {
                    ptr[i] = self[i].as<double>();
                }
                return arr;
            } else if (elem == scalar_type_meta<bool>()) {
                auto arr = np.attr("empty")(n, "dtype"_a = "bool");
                bool* ptr = reinterpret_cast<bool*>(
                    nb::cast<intptr_t>(arr.attr("ctypes").attr("data")));
                for (size_t i = 0; i < n; ++i) {
                    ptr[i] = self[i].as<bool>();
                }
                return arr;
            }
            throw std::runtime_error("Unsupported element type for numpy conversion");
        }, "Convert to a numpy array (copies data in logical order)");
}

// ============================================================================
// QueueView Binding (merged const/mutable)
// ============================================================================

static void register_queue_views(nb::module_& m) {
    nb::class_<QueueView, IndexedView>(m, "QueueView",
        "View for queue types (FIFO with optional max capacity)")
        .def("front", static_cast<View (QueueView::*)()>(&QueueView::front), "Get the front element")
        .def("back", static_cast<View (QueueView::*)()>(&QueueView::back), "Get the back element")
        .def("element_type", &QueueView::element_type, nb::rv_policy::reference,
            "Get the element type")
        .def("max_capacity", &QueueView::max_capacity,
            "Get the max capacity (0 = unbounded)")
        .def("has_max_capacity", &QueueView::has_max_capacity,
            "Check if the queue has a max capacity")
        .def("append", [](QueueView& self, const View& value) {
            self.append(value);
        }, "value"_a, "Append a value to the back of the queue")
        .def("pop_front", &QueueView::pop_front,
            "Remove and discard the front element")
        .def("clear", &QueueView::clear, "Clear all elements");
}

// ============================================================================
// PathElement Binding (User Guide Section 10)
// ============================================================================

static void register_path_element(nb::module_& m) {
    nb::class_<PathElement>(m, "PathElement",
        "Represents a single element in a navigation path")
        // Factory methods - use different names to avoid conflict with properties
        .def_static("field", &PathElement::field, "name"_a,
            "Create a field access element (for bundles)")
        .def_static("index", &PathElement::index, "idx"_a,
            "Create an index access element (for lists/tuples)")
        .def_static("key", &PathElement::key, "view"_a,
            "Create a value key element (for maps with arbitrary key types)")

        // Type queries
        .def("is_field", &PathElement::is_field,
            "Check if this is a field access element")
        .def("is_index", &PathElement::is_index,
            "Check if this is an index access element")
        .def("is_string", &PathElement::is_string,
            "Check if this uses a string for access")
        .def("is_value", &PathElement::is_value,
            "Check if this contains an arbitrary value key")

        // Accessors - use get_ prefix to avoid conflict with static factory methods
        .def_prop_ro("name", [](const PathElement& self) -> std::string {
            if (!self.is_string()) {
                throw std::runtime_error("PathElement is not a field element");
            }
            return self.name();
        }, "Get the field name (throws if not a field element)")
        .def("get_index", &PathElement::get_index,
            "Get the index value (throws if not an index element)")
        .def("get_value", &PathElement::get_value,
            "Get the value key as a View (throws if not a value element)")

        // String representation
        .def("to_string", &PathElement::to_string, "Convert to string representation")
        .def("__repr__", [](const PathElement& self) {
            return "PathElement(" + self.to_string() + ")";
        });
}

// ============================================================================
// Path Functions Binding (User Guide Section 10)
// ============================================================================

static void register_path_functions(nb::module_& m) {
    // parse_path function
    m.def("parse_path", [](const std::string& path_str) {
        return parse_path(path_str);
    }, "path_str"_a,
        "Parse a path string into a list of PathElements.\n\n"
        "Supports syntax:\n"
        "  - Field access: 'name', 'user.address'\n"
        "  - Index access: '[0]', 'items[0]'\n"
        "  - Mixed: 'users[0].addresses[1].city'");

    // path_to_string function
    m.def("path_to_string", [](const ValuePath& path) {
        return path_to_string(path);
    }, "path"_a,
        "Convert a path to string representation");

    // navigate function (const)
    m.def("navigate", [](View view, const std::string& path_str) {
        return navigate(view, path_str);
    }, "view"_a, "path"_a,
        "Navigate through a value using a path string.\n\n"
        "Returns the View at the path destination.\n"
        "Throws if navigation fails.");

    // navigate with ValuePath
    m.def("navigate", [](View view, const ValuePath& path) {
        return navigate(view, path);
    }, "view"_a, "path"_a,
        "Navigate through a value using a ValuePath.");

    // try_navigate function (const)
    m.def("try_navigate", [](View view, const std::string& path_str)
        -> std::optional<View> {
        return try_navigate(view, path_str);
    }, "view"_a, "path"_a,
        "Try to navigate through a value using a path string.\n\n"
        "Returns the View at the path destination, or None on failure.");

    // try_navigate with ValuePath
    m.def("try_navigate", [](View view, const ValuePath& path)
        -> std::optional<View> {
        return try_navigate(view, path);
    }, "view"_a, "path"_a,
        "Try to navigate through a value using a ValuePath.");

    // navigate_mut function (mutable)
    m.def("navigate_mut", [](View view, const std::string& path_str) {
        return navigate_mut(view, path_str);
    }, "view"_a, "path"_a,
        "Navigate through a mutable value using a path string.\n\n"
        "Returns the View at the path destination.\n"
        "Throws if navigation fails.");

    // navigate_mut with ValuePath
    m.def("navigate_mut", [](View view, const ValuePath& path) {
        return navigate_mut(view, path);
    }, "view"_a, "path"_a,
        "Navigate through a mutable value using a ValuePath.");

    // try_navigate_mut function
    m.def("try_navigate_mut", [](View view, const std::string& path_str)
        -> std::optional<View> {
        return try_navigate_mut(view, path_str);
    }, "view"_a, "path"_a,
        "Try to navigate through a mutable value using a path string.\n\n"
        "Returns the View at the path destination, or None on failure.");

    // try_navigate_mut with ValuePath
    m.def("try_navigate_mut", [](View view, const ValuePath& path)
        -> std::optional<View> {
        return try_navigate_mut(view, path);
    }, "view"_a, "path"_a,
        "Try to navigate through a mutable value using a ValuePath.");
}

// ============================================================================
// Traversal Functions Binding (User Guide Section 11)
// ============================================================================

static void register_traversal_functions(nb::module_& m) {
    // deep_visit function
    m.def("deep_visit", [](View view, nb::object callback) {
        deep_visit(view, [&callback](View leaf, const TraversalPath& path) {
            // Convert TraversalPath to Python list
            nb::list py_path;
            for (const auto& elem : path) {
                if (std::holds_alternative<std::string>(elem)) {
                    py_path.append(nb::cast(std::get<std::string>(elem)));
                } else {
                    py_path.append(nb::cast(std::get<size_t>(elem)));
                }
            }
            callback(leaf, py_path);
        });
    }, "view"_a, "callback"_a,
        "Visit all leaf (scalar) values in a nested structure.\n\n"
        "Calls callback(leaf_view, path) for each scalar value.");

    // count_leaves function
    m.def("count_leaves", &count_leaves, "view"_a,
        "Count all leaf (scalar) values in a nested structure.");

    // collect_leaf_paths function
    m.def("collect_leaf_paths", [](View view) {
        auto paths = collect_leaf_paths(view);
        nb::list result;
        for (const auto& path : paths) {
            nb::list py_path;
            for (const auto& elem : path) {
                if (std::holds_alternative<std::string>(elem)) {
                    py_path.append(nb::cast(std::get<std::string>(elem)));
                } else {
                    py_path.append(nb::cast(std::get<size_t>(elem)));
                }
            }
            result.append(py_path);
        }
        return result;
    }, "view"_a,
        "Collect the paths to all leaf values.");

    // sum_numeric function
    m.def("sum_numeric", &sum_numeric, "view"_a,
        "Sum all numeric leaf values (int64 and double).");

    // max_numeric function
    m.def("max_numeric", &max_numeric, "view"_a,
        "Find maximum numeric leaf value (returns None if no numeric leaves).");

    // min_numeric function
    m.def("min_numeric", &min_numeric, "view"_a,
        "Find minimum numeric leaf value (returns None if no numeric leaves).");

    // path_to_string for TraversalPath (using overload)
    m.def("traversal_path_to_string", [](const nb::list& py_path) {
        TraversalPath path;
        for (size_t i = 0; i < nb::len(py_path); ++i) {
            nb::object elem = py_path[i];
            if (nb::isinstance<nb::str>(elem)) {
                path.push_back(nb::cast<std::string>(elem));
            } else {
                path.push_back(nb::cast<size_t>(elem));
            }
        }
        return path_to_string(path);
    }, "path"_a,
        "Convert a traversal path (list of strings/ints) to string representation.");
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
        .def(nb::init<const View&>(), "view"_a,
            "Construct by copying from a view")
        // Construct from schema and Python object
        .def("__init__", [](PlainValue* self, const TypeMeta* schema, nb::object py_obj) {
            new (self) PlainValue(schema);
            if (self->valid()) {
                self->from_python(py_obj);
            }
        }, "schema"_a, "py_obj"_a,
            "Construct from schema and Python object")

        // Validity and null semantics
        .def("valid", &PlainValue::valid, "Check if the Value contains data")
        .def("__bool__", &PlainValue::valid, "Boolean conversion (validity)")
        .def("is_null", &PlainValue::is_null, "Check if the Value is in null state (has schema but no value)")
        .def("has_value", &PlainValue::has_value, "Check if the Value is present (not null, not invalid)")
        .def("reset", &PlainValue::reset, "Reset to null state (keeps schema)")
        .def("emplace", &PlainValue::emplace, "Construct value in place (from null to valid)")
        .def_prop_ro("schema", &PlainValue::schema, nb::rv_policy::reference,
            "Get the type schema")
        .def_static("make_null", &PlainValue::make_null, "schema"_a,
            "Create a null Value with the given schema")

        // View access (Design Doc Section 6.2)
        .def("view", static_cast<View (PlainValue::*)()>(&PlainValue::view),
            "Get a mutable view of the data")
        .def("const_view", &PlainValue::const_view, nb::keep_alive<0, 1>(), "Get a const view of the data")

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
        .def("equals", static_cast<bool (PlainValue::*)(const View&) const>(
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
        .def_static("copy_view", static_cast<PlainValue (*)(const View&)>(
            &PlainValue::copy), "view"_a, "Create a copy from a view")

        // Python special methods
        .def("__eq__", [](const PlainValue& self, const View& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &PlainValue::hash)
        .def("__str__", &PlainValue::to_string)
        .def("__repr__", [](const PlainValue& self) {
            if (!self.valid()) return std::string("PlainValue(invalid)");
            return "PlainValue(" + self.to_string() + ")";
        })

        // Path-based Navigation (User Guide Section 10)
        .def("navigate", [](const PlainValue& self, const std::string& path_str) {
            return navigate(self.const_view(), path_str);
        }, "path"_a,
            "Navigate through the value using a path string.\n\n"
            "Returns the View at the path destination.\n"
            "Throws if navigation fails.")
        .def("try_navigate", [](const PlainValue& self, const std::string& path_str)
            -> std::optional<View> {
            return try_navigate(self.const_view(), path_str);
        }, "path"_a,
            "Try to navigate through the value using a path string.\n\n"
            "Returns the View at the destination, or None on failure.")
        .def("navigate_mut", [](PlainValue& self, const std::string& path_str) {
            return navigate_mut(self.view(), path_str);
        }, "path"_a,
            "Navigate through the mutable value using a path string.\n\n"
            "Returns the View at the path destination.\n"
            "Throws if navigation fails.")
        .def("try_navigate_mut", [](PlainValue& self, const std::string& path_str)
            -> std::optional<View> {
            return try_navigate_mut(self.view(), path_str);
        }, "path"_a,
            "Try to navigate through the mutable value using a path string.\n\n"
            "Returns the View at the destination, or None on failure.")

        // Delta Operations
        .def("apply_delta", static_cast<void (PlainValue::*)(const SetDeltaView&)>(
            &PlainValue::apply_delta), "delta"_a,
            "Apply a set delta to this value (add/remove elements)")
        .def("apply_delta", static_cast<void (PlainValue::*)(const MapDeltaView&)>(
            &PlainValue::apply_delta), "delta"_a,
            "Apply a map delta to this value (add/update/remove entries)")
        .def("apply_delta", static_cast<void (PlainValue::*)(const ListDeltaView&)>(
            &PlainValue::apply_delta), "delta"_a,
            "Apply a list delta to this value (update elements at indices)");
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
        .def(nb::init<const View&>(), "view"_a,
            "Construct by copying from a view")
        // Construct from schema and Python object
        .def("__init__", [](CachedValue* self, const TypeMeta* schema, nb::object py_obj) {
            new (self) CachedValue(schema);
            if (self->valid()) {
                self->from_python(py_obj);
            }
        }, "schema"_a, "py_obj"_a,
            "Construct from schema and Python object")

        // Validity and null semantics
        .def("valid", &CachedValue::valid, "Check if the Value contains data")
        .def("__bool__", &CachedValue::valid, "Boolean conversion (validity)")
        .def("is_null", &CachedValue::is_null, "Check if the Value is in null state")
        .def("has_value", &CachedValue::has_value, "Check if the Value is present")
        .def("reset", &CachedValue::reset, "Reset to null state (keeps schema)")
        .def("emplace", &CachedValue::emplace, "Construct value in place")
        .def_prop_ro("schema", &CachedValue::schema, nb::rv_policy::reference,
            "Get the type schema")
        .def_static("make_null", &CachedValue::make_null, "schema"_a,
            "Create a null Value with the given schema")

        // View access (Design Doc Section 6.2)
        .def("view", static_cast<View (CachedValue::*)()>(&CachedValue::view),
            "Get a mutable view of the data (invalidates cache)")
        .def("const_view", &CachedValue::const_view, nb::keep_alive<0, 1>(), "Get a const view of the data")

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
        .def("equals", static_cast<bool (CachedValue::*)(const View&) const>(
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
        .def_static("copy_view", static_cast<CachedValue (*)(const View&)>(
            &CachedValue::copy), "view"_a, "Create a copy from a view")

        // Python special methods
        .def("__eq__", [](const CachedValue& self, const View& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &CachedValue::hash)
        .def("__str__", &CachedValue::to_string)
        .def("__repr__", [](const CachedValue& self) {
            if (!self.valid()) return std::string("CachedValue(invalid)");
            return "CachedValue(" + self.to_string() + ")";
        })

        // Delta Operations
        .def("apply_delta", static_cast<void (CachedValue::*)(const SetDeltaView&)>(
            &CachedValue::apply_delta), "delta"_a,
            "Apply a set delta to this value")
        .def("apply_delta", static_cast<void (CachedValue::*)(const MapDeltaView&)>(
            &CachedValue::apply_delta), "delta"_a,
            "Apply a map delta to this value")
        .def("apply_delta", static_cast<void (CachedValue::*)(const ListDeltaView&)>(
            &CachedValue::apply_delta), "delta"_a,
            "Apply a list delta to this value");
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
        .def(nb::init<const View&>(), "view"_a,
            "Construct by copying from a view")
        // Construct from schema and Python object
        .def("__init__", [](TSValue* self, const TypeMeta* schema, nb::object py_obj) {
            new (self) TSValue(schema);
            if (self->valid()) {
                self->from_python(py_obj);
            }
        }, "schema"_a, "py_obj"_a,
            "Construct from schema and Python object")

        // Validity and null semantics
        .def("valid", &TSValue::valid, "Check if the Value contains data")
        .def("__bool__", &TSValue::valid, "Boolean conversion (validity)")
        .def("is_null", &TSValue::is_null, "Check if the Value is in null state")
        .def("has_value", &TSValue::has_value, "Check if the Value is present")
        .def("reset", &TSValue::reset, "Reset to null state (keeps schema)")
        .def("emplace", &TSValue::emplace, "Construct value in place")
        .def_prop_ro("schema", &TSValue::schema, nb::rv_policy::reference,
            "Get the type schema")
        .def_static("make_null", &TSValue::make_null, "schema"_a,
            "Create a null Value with the given schema")

        // View access
        .def("view", static_cast<View (TSValue::*)()>(&TSValue::view),
            "Get a mutable view of the data (invalidates cache)")
        .def("const_view", &TSValue::const_view, nb::keep_alive<0, 1>(), "Get a const view of the data")

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
        .def("equals", static_cast<bool (TSValue::*)(const View&) const>(&TSValue::equals),
            "other"_a, "Check equality with a view")

        // Python special methods
        .def("__eq__", [](const TSValue& self, const View& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &TSValue::hash)
        .def("__str__", &TSValue::to_string)
        .def("__repr__", [](const TSValue& self) {
            if (!self.valid()) return std::string("TSValue(invalid)");
            return "TSValue(" + self.to_string() + ")";
        })

        // Delta Operations
        .def("apply_delta", static_cast<void (TSValue::*)(const SetDeltaView&)>(
            &TSValue::apply_delta), "delta"_a,
            "Apply a set delta to this value")
        .def("apply_delta", static_cast<void (TSValue::*)(const MapDeltaView&)>(
            &TSValue::apply_delta), "delta"_a,
            "Apply a map delta to this value")
        .def("apply_delta", static_cast<void (TSValue::*)(const ListDeltaView&)>(
            &TSValue::apply_delta), "delta"_a,
            "Apply a list delta to this value");
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
        .def(nb::init<const View&>(), "view"_a,
            "Construct by copying from a view")
        // Construct from schema and Python object
        .def("__init__", [](ValidatedValue* self, const TypeMeta* schema, nb::object py_obj) {
            new (self) ValidatedValue(schema);
            if (self->valid()) {
                self->from_python(py_obj);
            }
        }, "schema"_a, "py_obj"_a,
            "Construct from schema and Python object")

        // Validity and null semantics
        .def("valid", &ValidatedValue::valid, "Check if the Value contains data")
        .def("__bool__", &ValidatedValue::valid, "Boolean conversion (validity)")
        .def("is_null", &ValidatedValue::is_null, "Check if the Value is in null state")
        .def("has_value", &ValidatedValue::has_value, "Check if the Value is present")
        .def("reset", &ValidatedValue::reset, "Reset to null state (keeps schema)")
        .def("emplace", &ValidatedValue::emplace, "Construct value in place")
        .def_prop_ro("schema", &ValidatedValue::schema, nb::rv_policy::reference,
            "Get the type schema")
        .def_static("make_null", &ValidatedValue::make_null, "schema"_a,
            "Create a null Value with the given schema")

        // View access
        .def("view", static_cast<View (ValidatedValue::*)()>(&ValidatedValue::view),
            "Get a mutable view of the data")
        .def("const_view", &ValidatedValue::const_view, nb::keep_alive<0, 1>(), "Get a const view of the data")

        // Python interop
        .def("to_python", &ValidatedValue::to_python, "Convert to Python object")
        .def("from_python", &ValidatedValue::from_python, "src"_a,
            "Set value from Python object (throws if None)")

        // Equality and comparison
        .def("equals", static_cast<bool (ValidatedValue::*)(const View&) const>(&ValidatedValue::equals),
            "other"_a, "Check equality with a view")

        // Python special methods
        .def("__eq__", [](const ValidatedValue& self, const View& other) {
            return self.equals(other);
        }, nb::is_operator())
        .def("__hash__", &ValidatedValue::hash)
        .def("__str__", &ValidatedValue::to_string)
        .def("__repr__", [](const ValidatedValue& self) {
            if (!self.valid()) return std::string("ValidatedValue(invalid)");
            return "ValidatedValue(" + self.to_string() + ")";
        })

        // Delta Operations
        .def("apply_delta", static_cast<void (ValidatedValue::*)(const SetDeltaView&)>(
            &ValidatedValue::apply_delta), "delta"_a,
            "Apply a set delta to this value")
        .def("apply_delta", static_cast<void (ValidatedValue::*)(const MapDeltaView&)>(
            &ValidatedValue::apply_delta), "delta"_a,
            "Apply a map delta to this value")
        .def("apply_delta", static_cast<void (ValidatedValue::*)(const ListDeltaView&)>(
            &ValidatedValue::apply_delta), "delta"_a,
            "Apply a list delta to this value");
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

    // Register type meta binding functions for Python-to-C++ type mapping
    register_type_meta_bindings(value_mod);

    // Register base view class (merged View with both const and mutable operations)
    register_const_value_view(value_mod);

    // Register indexed view base class (merged const/mutable)
    register_indexed_view(value_mod);

    // Register specialized views (all merged const/mutable)
    register_tuple_views(value_mod);
    register_bundle_views(value_mod);
    register_list_views(value_mod);
    register_set_views(value_mod);
    register_tracked_set(value_mod);  // TrackedSetStorage and TrackedSetView
    register_key_set_view(value_mod);  // Before map_views - KeySetView returned by map.keys()
    register_map_views(value_mod);
    register_cyclic_buffer_views(value_mod);
    register_queue_views(value_mod);

    // Register Value classes
    register_plain_value(value_mod);
    register_cached_value(value_mod);
    register_ts_value(value_mod);
    register_validated_value(value_mod);

    // Register path-based access utilities
    register_path_element(value_mod);
    register_path_functions(value_mod);
    register_traversal_functions(value_mod);

    // Also export the main types at the module level for convenience
    m.attr("PlainValue") = value_mod.attr("PlainValue");
    m.attr("CachedValue") = value_mod.attr("CachedValue");
    m.attr("TSValue") = value_mod.attr("TSValue");
    m.attr("ValidatedValue") = value_mod.attr("ValidatedValue");
    m.attr("View") = value_mod.attr("View");
    m.attr("TypeRegistry") = value_mod.attr("TypeRegistry");
    m.attr("TypeMeta") = value_mod.attr("TypeMeta");
    // TrackedSet types for TSS
    m.attr("TrackedSetStorage") = value_mod.attr("TrackedSetStorage");
    m.attr("TrackedSetView") = value_mod.attr("TrackedSetView");
    m.attr("SetDeltaValue") = value_mod.attr("SetDeltaValue");
}

} // namespace hgraph
