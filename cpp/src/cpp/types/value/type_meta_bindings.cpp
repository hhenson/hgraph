/**
 * @file type_meta_bindings.cpp
 * @brief Python bindings for type metadata mapping.
 *
 * This file implements the binding functions that map Python types to C++ TypeMeta*.
 * It provides factory functions for creating TypeMeta for scalars, collections, and bundles.
 */

#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/composite_ops.h>
#include <hgraph/util/date_time.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/optional.h>

#include <unordered_map>
#include <functional>

namespace hgraph::value {

using namespace nanobind::literals;

// ============================================================================
// nb::object Scalar Type Operations
// ============================================================================

/**
 * @brief ScalarOps specialization for nb::object.
 *
 * This provides type-erased storage for arbitrary Python objects.
 * All operations delegate to the Python runtime.
 */
template<>
struct ScalarOps<nb::object> {
    static void construct(void* dst, const TypeMeta*) {
        new (dst) nb::object{};
    }

    static void destruct(void* obj, const TypeMeta*) {
        static_cast<nb::object*>(obj)->~object();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta*) {
        *static_cast<nb::object*>(dst) = *static_cast<const nb::object*>(src);
    }

    static void move_assign(void* dst, void* src, const TypeMeta*) {
        *static_cast<nb::object*>(dst) = std::move(*static_cast<nb::object*>(src));
    }

    static void move_construct(void* dst, void* src, const TypeMeta*) {
        new (dst) nb::object(std::move(*static_cast<nb::object*>(src)));
    }

    static bool equals(const void* a, const void* b, const TypeMeta*) {
        const auto& obj_a = *static_cast<const nb::object*>(a);
        const auto& obj_b = *static_cast<const nb::object*>(b);
        if (!obj_a.is_valid() && !obj_b.is_valid()) return true;
        if (!obj_a.is_valid() || !obj_b.is_valid()) return false;
        try {
            return obj_a.equal(obj_b);
        } catch (...) {
            return false;
        }
    }

    static size_t hash(const void* obj, const TypeMeta*) {
        const auto& py_obj = *static_cast<const nb::object*>(obj);
        if (!py_obj.is_valid()) return 0;
        try {
            return nb::hash(py_obj);
        } catch (...) {
            return 0;
        }
    }

    static bool less_than(const void* a, const void* b, const TypeMeta*) {
        const auto& obj_a = *static_cast<const nb::object*>(a);
        const auto& obj_b = *static_cast<const nb::object*>(b);
        if (!obj_a.is_valid() || !obj_b.is_valid()) return false;
        try {
            return obj_a < obj_b;
        } catch (...) {
            return false;
        }
    }

    static std::string to_string(const void* obj, const TypeMeta*) {
        const auto& py_obj = *static_cast<const nb::object*>(obj);
        if (!py_obj.is_valid()) return "None";
        try {
            return nb::str(py_obj).c_str();
        } catch (...) {
            return "<object>";
        }
    }

    static nb::object to_python(const void* obj, const TypeMeta*) {
        const auto& py_obj = *static_cast<const nb::object*>(obj);
        return py_obj.is_valid() ? nb::object(py_obj) : nb::none();
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta*) {
        *static_cast<nb::object*>(dst) = src;
    }

    static constexpr TypeOps make_ops() {
        return TypeOps{
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            &less_than,
            nullptr,  // size
            nullptr,  // get_at
            nullptr,  // set_at
            nullptr,  // get_field
            nullptr,  // set_field
            nullptr,  // contains
            nullptr,  // insert
            nullptr,  // erase
            nullptr,  // map_get
            nullptr,  // map_set
            nullptr,  // resize
            nullptr,  // clear
        };
    }
};

// Specialization for nb::object flags - not trivially anything since it holds Python refs
template<>
constexpr TypeFlags compute_scalar_flags<nb::object>() {
    // nb::object needs proper construction/destruction for refcounting
    // It is hashable and equatable via Python runtime
    return TypeFlags::Hashable | TypeFlags::Equatable;
}

// ============================================================================
// Type Caching for Composite Types
// ============================================================================

namespace {

// Cache key for composite types based on structural identity
struct CompositeTypeKey {
    TypeKind kind;
    const TypeMeta* element_type;
    const TypeMeta* key_type;
    std::vector<std::pair<std::string, const TypeMeta*>> fields;

    bool operator==(const CompositeTypeKey& other) const {
        if (kind != other.kind) return false;
        if (element_type != other.element_type) return false;
        if (key_type != other.key_type) return false;
        if (fields.size() != other.fields.size()) return false;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first != other.fields[i].first) return false;
            if (fields[i].second != other.fields[i].second) return false;
        }
        return true;
    }
};

struct CompositeTypeKeyHash {
    size_t operator()(const CompositeTypeKey& key) const {
        size_t h = std::hash<int>{}(static_cast<int>(key.kind));
        h ^= std::hash<const void*>{}(key.element_type) << 1;
        h ^= std::hash<const void*>{}(key.key_type) << 2;
        for (const auto& field : key.fields) {
            h ^= std::hash<std::string>{}(field.first) << 3;
            h ^= std::hash<const void*>{}(field.second) << 4;
        }
        return h;
    }
};

// Cache for composite types
std::unordered_map<CompositeTypeKey, const TypeMeta*, CompositeTypeKeyHash> g_composite_cache;

// Registry mapping TypeMeta* to Python class for CompoundScalar reconstruction
std::unordered_map<const TypeMeta*, nb::object> g_compound_scalar_registry;

} // anonymous namespace

// ============================================================================
// CompoundScalar Operations (Bundle with Python class reconstruction)
// ============================================================================

/**
 * @brief Get the Python class associated with a TypeMeta (for CompoundScalar).
 *
 * @param meta The TypeMeta to look up
 * @return The Python class, or an invalid nb::object if not found
 */
nb::object get_compound_scalar_class(const TypeMeta* meta) {
    auto it = g_compound_scalar_registry.find(meta);
    if (it != g_compound_scalar_registry.end()) {
        return it->second;
    }
    return nb::object();
}

/**
 * @brief Register a Python class for a TypeMeta (for CompoundScalar).
 *
 * @param meta The TypeMeta to register
 * @param py_class The Python class to associate
 */
void register_compound_scalar_class(const TypeMeta* meta, nb::object py_class) {
    g_compound_scalar_registry[meta] = std::move(py_class);
}

/**
 * @brief Operations for CompoundScalar types (Bundle with Python class reconstruction).
 *
 * This is like BundleOps but to_python() reconstructs the original Python
 * CompoundScalar class instead of returning a dict. This preserves hashability
 * when CompoundScalar is used as keys in TSD/TSS/mesh operations.
 */
struct CompoundScalarOps {
    // Most operations delegate to BundleOps
    static void construct(void* dst, const TypeMeta* schema) {
        BundleOps::construct(dst, schema);
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        BundleOps::destruct(obj, schema);
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        BundleOps::copy_assign(dst, src, schema);
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        BundleOps::move_assign(dst, src, schema);
    }

    static void move_construct(void* dst, void* src, const TypeMeta* schema) {
        BundleOps::move_construct(dst, src, schema);
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        return BundleOps::equals(a, b, schema);
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        return BundleOps::to_string(obj, schema);
    }

    // ========== Python Interop - Reconstruct CompoundScalar ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        // Check if we have a registered Python class for this type
        nb::object py_class = get_compound_scalar_class(schema);
        if (py_class.is_valid()) {
            // Build kwargs dict from field values
            nb::dict kwargs;
            for (size_t i = 0; i < schema->field_count; ++i) {
                const BundleFieldInfo& field = schema->fields[i];
                const void* field_ptr = static_cast<const char*>(obj) + field.offset;
                if (field.type && field.type->ops && field.type->ops->to_python && field.name) {
                    kwargs[field.name] = field.type->ops->to_python(field_ptr, field.type);
                }
            }
            // Construct the Python class with **kwargs
            return py_class(**kwargs);
        }
        // Fallback to dict if no class registered
        return BundleOps::to_python(obj, schema);
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        BundleOps::from_python(dst, src, schema);
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        return BundleOps::hash(obj, schema);
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* schema) {
        return BundleOps::size(obj, schema);
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        return BundleOps::get_at(obj, index, schema);
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        BundleOps::set_at(obj, index, value, schema);
    }

    // ========== Bundle-specific Operations ==========

    static const void* get_field(const void* obj, const char* name, const TypeMeta* schema) {
        return BundleOps::get_field(obj, name, schema);
    }

    static void set_field(void* obj, const char* name, const void* value, const TypeMeta* schema) {
        BundleOps::set_field(obj, name, value, schema);
    }

    /// Get the operations vtable for CompoundScalar
    static const TypeOps* ops() {
        static const TypeOps compound_scalar_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &move_construct,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (bundles not ordered)
            &size,
            &get_at,
            &set_at,
            &get_field,
            &set_field,
            nullptr,   // contains (not a set)
            nullptr,   // insert (not a set)
            nullptr,   // erase (not a set)
            nullptr,   // map_get (not a map)
            nullptr,   // map_set (not a map)
            nullptr,   // resize (not resizable)
            nullptr,   // clear (not clearable)
        };
        return &compound_scalar_ops;
    }
};

// ============================================================================
// Scalar Type Mapping
// ============================================================================

/**
 * @brief Get the TypeMeta for a Python scalar type.
 *
 * Maps Python types to their corresponding C++ TypeMeta:
 * - bool -> bool
 * - int -> int64_t
 * - float -> double
 * - datetime.date -> engine_date_t
 * - datetime.datetime -> engine_time_t
 * - datetime.timedelta -> engine_time_delta_t
 * - Everything else (str, bytes, Enum, etc.) -> nb::object
 *
 * @param py_type The Python type object
 * @return TypeMeta* for the corresponding C++ type
 */
static const TypeMeta* get_scalar_type_meta(nb::object py_type) {
    // Import datetime module for type checks
    nb::module_ datetime_mod = nb::module_::import_("datetime");
    nb::object date_type = datetime_mod.attr("date");
    nb::object datetime_type = datetime_mod.attr("datetime");
    nb::object timedelta_type = datetime_mod.attr("timedelta");

    // Get built-in types
    nb::module_ builtins = nb::module_::import_("builtins");
    nb::object bool_type = builtins.attr("bool");
    nb::object int_type = builtins.attr("int");
    nb::object float_type = builtins.attr("float");

    // Check each type - order matters (datetime before date since datetime is subclass of date)
    if (py_type.is(bool_type)) {
        return scalar_type_meta<bool>();
    }
    if (py_type.is(int_type)) {
        return scalar_type_meta<int64_t>();
    }
    if (py_type.is(float_type)) {
        return scalar_type_meta<double>();
    }
    // Check datetime BEFORE date since datetime is a subclass of date
    if (py_type.is(datetime_type)) {
        return scalar_type_meta<engine_time_t>();
    }
    if (py_type.is(date_type)) {
        return scalar_type_meta<engine_date_t>();
    }
    if (py_type.is(timedelta_type)) {
        return scalar_type_meta<engine_time_delta_t>();
    }

    // Fallback: use nb::object for everything else (str, bytes, Enum, etc.)
    return scalar_type_meta<nb::object>();
}

// ============================================================================
// Composite Type Factories
// ============================================================================

/**
 * @brief Get the TypeMeta for a Dict type.
 *
 * @param key_meta TypeMeta for the key type
 * @param value_meta TypeMeta for the value type
 * @return TypeMeta* for the dict type
 */
static const TypeMeta* get_dict_type_meta(const TypeMeta* key_meta, const TypeMeta* value_meta) {
    if (!key_meta || !value_meta) {
        throw std::invalid_argument("key_meta and value_meta must not be null");
    }

    // Check cache
    CompositeTypeKey cache_key{TypeKind::Map, value_meta, key_meta, {}};
    auto it = g_composite_cache.find(cache_key);
    if (it != g_composite_cache.end()) {
        return it->second;
    }

    // Build new type
    auto& registry = TypeRegistry::instance();
    const TypeMeta* result = registry.map(key_meta, value_meta).build();
    g_composite_cache[cache_key] = result;
    return result;
}

/**
 * @brief Get the TypeMeta for a Set type.
 *
 * @param element_meta TypeMeta for the element type
 * @return TypeMeta* for the set type
 */
static const TypeMeta* get_set_type_meta(const TypeMeta* element_meta) {
    if (!element_meta) {
        throw std::invalid_argument("element_meta must not be null");
    }

    // Check cache
    CompositeTypeKey cache_key{TypeKind::Set, element_meta, nullptr, {}};
    auto it = g_composite_cache.find(cache_key);
    if (it != g_composite_cache.end()) {
        return it->second;
    }

    // Build new type
    auto& registry = TypeRegistry::instance();
    const TypeMeta* result = registry.set(element_meta).build();
    g_composite_cache[cache_key] = result;
    return result;
}

/**
 * @brief Get the TypeMeta for a dynamic list type (tuple[T, ...]).
 *
 * @param element_meta TypeMeta for the element type
 * @return TypeMeta* for the list type
 */
static const TypeMeta* get_dynamic_list_type_meta(const TypeMeta* element_meta) {
    if (!element_meta) {
        throw std::invalid_argument("element_meta must not be null");
    }

    // Check cache
    CompositeTypeKey cache_key{TypeKind::List, element_meta, nullptr, {}};
    auto it = g_composite_cache.find(cache_key);
    if (it != g_composite_cache.end()) {
        return it->second;
    }

    // Build new type - mark as variadic tuple since this is tuple[T, ...]
    auto& registry = TypeRegistry::instance();
    const TypeMeta* result = registry.list(element_meta).as_variadic_tuple().build();
    g_composite_cache[cache_key] = result;
    return result;
}

/**
 * @brief Get the TypeMeta for a fixed tuple type (Tuple[T1, T2, ...]).
 *
 * Tuples are indexed by position, not by name. Uses TupleOps which returns
 * a Python tuple (hashable) from to_python(), not a dict.
 *
 * @param element_types Vector of element type TypeMeta pointers
 * @return TypeMeta* for the tuple type
 */
static const TypeMeta* get_tuple_type_meta(
    std::vector<const TypeMeta*> element_types
) {
    // Validate all elements have valid TypeMeta
    for (const auto* elem : element_types) {
        if (!elem) {
            throw std::invalid_argument("All element types must not be null");
        }
    }

    // Check cache - create cache key with synthetic field names
    std::vector<std::pair<std::string, const TypeMeta*>> fields;
    for (size_t i = 0; i < element_types.size(); ++i) {
        fields.emplace_back("$" + std::to_string(i), element_types[i]);
    }
    CompositeTypeKey cache_key{TypeKind::Tuple, nullptr, nullptr, fields};
    auto it = g_composite_cache.find(cache_key);
    if (it != g_composite_cache.end()) {
        return it->second;
    }

    // Build new type using TupleTypeBuilder
    auto& registry = TypeRegistry::instance();
    TupleTypeBuilder builder = registry.tuple();

    for (const auto* elem : element_types) {
        builder.element(elem);
    }

    const TypeMeta* result = builder.build();
    g_composite_cache[cache_key] = result;
    return result;
}

/**
 * @brief Get the TypeMeta for a bundle type (CompoundScalar).
 *
 * Bundle type equivalence is based on field names + types + order (not bundle name).
 * Two bundles with identical fields in the same order return the same TypeMeta*.
 *
 * @param fields Vector of (field_name, field_meta) pairs
 * @param type_name Optional name for display/lookup (does not affect type identity)
 * @return TypeMeta* for the bundle type
 */
static const TypeMeta* get_bundle_type_meta(
    std::vector<std::pair<std::string, const TypeMeta*>> fields,
    std::optional<std::string> type_name
) {
    // Validate all fields have valid TypeMeta
    for (const auto& field : fields) {
        if (!field.second) {
            throw std::invalid_argument("All field types must not be null");
        }
    }

    // Check cache - key is based on fields, not name
    CompositeTypeKey cache_key{TypeKind::Bundle, nullptr, nullptr, fields};
    auto it = g_composite_cache.find(cache_key);
    if (it != g_composite_cache.end()) {
        return it->second;
    }

    // Build new type
    auto& registry = TypeRegistry::instance();
    BundleTypeBuilder builder = type_name.has_value()
        ? registry.bundle(type_name.value())
        : registry.bundle();

    for (const auto& field : fields) {
        builder.field(field.first.c_str(), field.second);
    }

    const TypeMeta* result = builder.build();
    g_composite_cache[cache_key] = result;
    return result;
}

/**
 * @brief Get the TypeMeta for a CompoundScalar type.
 *
 * This creates a Bundle-like TypeMeta but uses CompoundScalarOps which
 * reconstructs the original Python class in to_python() instead of returning a dict.
 * This preserves hashability when CompoundScalar is used as keys in TSD/TSS/mesh.
 *
 * @param fields Vector of (field_name, field_meta) pairs
 * @param py_class The Python CompoundScalar class to reconstruct
 * @param type_name Optional name for display/lookup (does not affect type identity)
 * @return TypeMeta* for the CompoundScalar type
 */
static const TypeMeta* get_compound_scalar_type_meta(
    std::vector<std::pair<std::string, const TypeMeta*>> fields,
    nb::object py_class,
    std::optional<std::string> type_name
) {
    // Validate all fields have valid TypeMeta
    for (const auto& field : fields) {
        if (!field.second) {
            throw std::invalid_argument("All field types must not be null");
        }
    }

    // Check cache - key is based on fields, not name
    CompositeTypeKey cache_key{TypeKind::Bundle, nullptr, nullptr, fields};
    auto it = g_composite_cache.find(cache_key);
    if (it != g_composite_cache.end()) {
        // Already exists - just ensure the py_class is registered
        register_compound_scalar_class(it->second, py_class);
        return it->second;
    }

    // Build new type manually (similar to BundleTypeBuilder but with CompoundScalarOps)
    auto& registry = TypeRegistry::instance();
    const size_t count = fields.size();

    // Calculate total size and alignment
    size_t total_size = 0;
    size_t max_alignment = 1;

    // Allocate field info array
    auto field_info = std::make_unique<BundleFieldInfo[]>(count);

    for (size_t i = 0; i < count; ++i) {
        const char* name = fields[i].first.c_str();
        const TypeMeta* type = fields[i].second;

        // Align offset for this field
        size_t alignment = type ? type->alignment : 1;
        total_size = (total_size + alignment - 1) & ~(alignment - 1);

        // Store the name in registry (to ensure stable pointer)
        const char* stored_name = registry.store_name(name ? name : "");

        // Store field info
        field_info[i].name = stored_name;
        field_info[i].index = i;
        field_info[i].offset = total_size;
        field_info[i].type = type;

        // Update totals
        total_size += type ? type->size : 0;
        if (alignment > max_alignment) max_alignment = alignment;
    }

    // Align final size
    total_size = (total_size + max_alignment - 1) & ~(max_alignment - 1);

    // Store fields in registry and get pointer
    BundleFieldInfo* fields_ptr = count > 0 ? registry.store_field_info(std::move(field_info)) : nullptr;

    // Create TypeMeta with CompoundScalarOps
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::Bundle;
    meta->flags = TypeFlags::Hashable | TypeFlags::Equatable;  // CompoundScalar is hashable
    meta->field_count = count;
    meta->size = total_size;
    meta->alignment = max_alignment;
    meta->ops = CompoundScalarOps::ops();  // Use CompoundScalarOps for to_python reconstruction
    meta->element_type = nullptr;
    meta->key_type = nullptr;
    meta->fields = fields_ptr;
    meta->fixed_size = 0;

    const TypeMeta* result = registry.register_composite(std::move(meta));

    // Register the Python class for reconstruction
    register_compound_scalar_class(result, py_class);

    // Register as named bundle if name was provided
    if (type_name.has_value() && !type_name.value().empty()) {
        registry.register_named_bundle(type_name.value(), result);
    }

    g_composite_cache[cache_key] = result;
    return result;
}

// ============================================================================
// Python Bindings Registration
// ============================================================================

void register_type_meta_bindings(nb::module_& m) {
    // Register nb::object as a scalar type
    TypeRegistry::instance().register_scalar<nb::object>();

    // get_scalar_type_meta(py_type) - Maps Python type to TypeMeta*
    m.def("get_scalar_type_meta", &get_scalar_type_meta,
        "py_type"_a,
        nb::rv_policy::reference,
        "Get the TypeMeta for a Python scalar type.\n\n"
        "Maps Python types to their corresponding C++ TypeMeta:\n"
        "- bool -> bool\n"
        "- int -> int64_t\n"
        "- float -> double\n"
        "- datetime.date -> engine_date_t\n"
        "- datetime.datetime -> engine_time_t\n"
        "- datetime.timedelta -> engine_time_delta_t\n"
        "- Everything else (str, bytes, Enum, etc.) -> nb::object fallback");

    // get_dict_type_meta(key_meta, value_meta) - Creates Dict TypeMeta*
    m.def("get_dict_type_meta", &get_dict_type_meta,
        "key_meta"_a, "value_meta"_a,
        nb::rv_policy::reference,
        "Get the TypeMeta for a Dict type with the specified key and value types.");

    // get_set_type_meta(element_meta) - Creates Set TypeMeta*
    m.def("get_set_type_meta", &get_set_type_meta,
        "element_meta"_a,
        nb::rv_policy::reference,
        "Get the TypeMeta for a Set type with the specified element type.");

    // get_dynamic_list_type_meta(element_meta) - Creates List TypeMeta* for tuple[T, ...]
    m.def("get_dynamic_list_type_meta", &get_dynamic_list_type_meta,
        "element_meta"_a,
        nb::rv_policy::reference,
        "Get the TypeMeta for a dynamic list type (tuple[T, ...]).");

    // get_tuple_type_meta(element_types) - Creates Tuple TypeMeta* for Tuple[T1, T2, ...]
    m.def("get_tuple_type_meta", &get_tuple_type_meta,
        "element_types"_a,
        nb::rv_policy::reference,
        "Get the TypeMeta for a fixed tuple type (Tuple[T1, T2, ...]).\n\n"
        "Tuples are indexed by position, not by name. Returns a TypeMeta using TupleOps\n"
        "which converts to Python tuple (hashable) via to_python().\n\n"
        "Args:\n"
        "    element_types: List of element type TypeMeta pointers");

    // get_bundle_type_meta(fields, type_name) - Creates Bundle TypeMeta* for CompoundScalar
    m.def("get_bundle_type_meta", &get_bundle_type_meta,
        "fields"_a, "type_name"_a = nb::none(),
        nb::rv_policy::reference,
        "Get the TypeMeta for a bundle type (CompoundScalar).\n\n"
        "Bundle type equivalence is based on field names + types + order (not bundle name).\n"
        "Two bundles with identical fields in the same order return the same TypeMeta*.\n\n"
        "Args:\n"
        "    fields: List of (field_name, field_meta) tuples\n"
        "    type_name: Optional name for display/lookup (does not affect type identity)");

    // get_compound_scalar_type_meta(fields, py_class, type_name) - Creates CompoundScalar TypeMeta*
    m.def("get_compound_scalar_type_meta", &get_compound_scalar_type_meta,
        "fields"_a, "py_class"_a, "type_name"_a = nb::none(),
        nb::rv_policy::reference,
        "Get the TypeMeta for a CompoundScalar type.\n\n"
        "This creates a Bundle-like TypeMeta but uses CompoundScalarOps which\n"
        "reconstructs the original Python class in to_python() instead of returning a dict.\n"
        "This preserves hashability when CompoundScalar is used as keys in TSD/TSS/mesh.\n\n"
        "Args:\n"
        "    fields: List of (field_name, field_meta) tuples\n"
        "    py_class: The Python CompoundScalar class to reconstruct in to_python()\n"
        "    type_name: Optional name for display/lookup (does not affect type identity)");
}

} // namespace hgraph::value
