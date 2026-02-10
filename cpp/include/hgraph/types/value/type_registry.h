#pragma once

/**
 * @file type_registry.h
 * @brief Central registry for type metadata.
 *
 * The TypeRegistry is the single source of truth for TypeMeta pointers.
 * All types must be registered before use. Registration provides the
 * TypeMeta pointer used for type identity comparisons.
 */

#include <hgraph/types/value/type_meta.h>

#include <Python.h>

#include <memory>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace hgraph::value {

// ============================================================================
// Type Builders (Forward Declarations)
// ============================================================================

class TupleBuilder;
class BundleBuilder;
class ListBuilder;
class SetBuilder;
class MapBuilder;
class CyclicBufferBuilder;
class QueueBuilder;

// ============================================================================
// Type Registry
// ============================================================================

/**
 * @brief Central registry for all type metadata.
 *
 * The TypeRegistry maintains ownership of all TypeMeta instances and provides
 * lookup functionality. Types are registered once and their metadata pointers
 * remain stable for the lifetime of the registry.
 *
 * Thread Safety:
 * - Read operations (get_scalar, get_bundle_by_name, has_*) are thread-safe
 *   for concurrent reads when no registration is in progress.
 * - Registration operations (register_type, register_composite, builder.build())
 *   are NOT thread-safe and must be synchronized externally if called from
 *   multiple threads.
 * - In hgraph, registration happens during graph construction under Python's GIL,
 *   and graph execution is single-threaded, so no additional synchronization
 *   is needed in practice.
 * - For other use cases, ensure all type registration completes before
 *   concurrent access begins.
 *
 * Usage:
 * @code
 * auto& registry = TypeRegistry::instance();
 *
 * // Register/get scalar types
 * const TypeMeta* int_type = registry.get_scalar<int64_t>();
 *
 * // Build composite types
 * const TypeMeta* person = registry.bundle("Person")
 *     .field("name", registry.get_scalar<std::string>())
 *     .field("age", registry.get_scalar<int64_t>())
 *     .build();
 * @endcode
 */
class TypeRegistry {
public:
    /// Get the singleton instance
    static TypeRegistry& instance();

    // Deleted copy/move
    TypeRegistry(const TypeRegistry&) = delete;
    TypeRegistry& operator=(const TypeRegistry&) = delete;
    TypeRegistry(TypeRegistry&&) = delete;
    TypeRegistry& operator=(TypeRegistry&&) = delete;

    // ========== Type Registration ==========

    /**
     * @brief Register a type with auto-generated operations.
     *
     * If the type is already registered, returns the existing TypeMeta.
     * Thread-safe for concurrent reads; registration should happen at startup.
     *
     * @tparam T The type to register
     * @return Pointer to the registered TypeMeta
     */
    template<typename T>
    const TypeMeta* register_type();

    /**
     * @brief Register a type with a human-readable name.
     *
     * If the type is already registered, updates the name and returns the existing TypeMeta.
     * The name is stored in the registry's string pool and the type can be looked up
     * via get_by_name().
     *
     * @tparam T The type to register
     * @param name The human-readable name (e.g., "int", "str", "bool")
     * @return Pointer to the registered TypeMeta
     */
    template<typename T>
    const TypeMeta* register_type(const std::string& name);

    /**
     * @brief Register a type with a name and custom operations.
     *
     * Allows registering types with user-provided operations rather than
     * auto-generated ones. The custom_ops pointer must remain valid for
     * the lifetime of the registry.
     *
     * @tparam T The type to register
     * @param name The human-readable name
     * @param custom_ops Custom operations vtable (must outlive the registry)
     * @return Pointer to the registered TypeMeta
     */
    template<typename T>
    const TypeMeta* register_type(const std::string& name, const TypeOps* custom_ops);

    /**
     * @brief Register a type by name only with custom operations (no C++ type binding).
     *
     * Used for types that only exist in the Python layer or have no C++ counterpart.
     *
     * @param name The human-readable name
     * @param custom_ops Custom operations vtable (must outlive the registry)
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* register_type(const std::string& name, const TypeOps* custom_ops);

    /**
     * @brief Get the TypeMeta for a registered scalar type.
     *
     * @tparam T The scalar type
     * @return Pointer to the TypeMeta, or nullptr if not registered
     */
    template<typename T>
    [[nodiscard]] const TypeMeta* get_scalar() const;

    /**
     * @brief Check if a scalar type is registered.
     *
     * @tparam T The scalar type
     * @return true if registered
     */
    template<typename T>
    [[nodiscard]] bool has_scalar() const;

    // ========== Composite Type Builders ==========

    /**
     * @brief Create a tuple type builder (heterogeneous, unnamed).
     * @return A builder for constructing the tuple type
     */
    TupleBuilder tuple();

    /**
     * @brief Create an anonymous bundle type builder.
     * @return A builder for constructing the bundle type
     */
    BundleBuilder bundle();

    /**
     * @brief Create a named bundle type builder.
     *
     * Named bundles can be retrieved later by name.
     *
     * @param name The name for this bundle type
     * @return A builder for constructing the bundle type
     */
    BundleBuilder bundle(const std::string& name);

    /**
     * @brief Create a dynamic list type builder.
     *
     * @param element_type The type of list elements
     * @return A builder for constructing the list type
     */
    ListBuilder list(const TypeMeta* element_type);

    /**
     * @brief Create a fixed-size list type builder.
     *
     * @param element_type The type of list elements
     * @param size The fixed size of the list
     * @return A builder for constructing the list type
     */
    ListBuilder fixed_list(const TypeMeta* element_type, size_t size);

    /**
     * @brief Create a set type builder.
     *
     * @param element_type The type of set elements (must be hashable)
     * @return A builder for constructing the set type
     */
    SetBuilder set(const TypeMeta* element_type);

    /**
     * @brief Create a map type builder.
     *
     * @param key_type The type of map keys (must be hashable)
     * @param value_type The type of map values
     * @return A builder for constructing the map type
     */
    MapBuilder map(const TypeMeta* key_type, const TypeMeta* value_type);

    /**
     * @brief Create a cyclic buffer type builder.
     *
     * @param element_type The type of buffer elements
     * @param capacity The fixed capacity of the buffer
     * @return A builder for constructing the cyclic buffer type
     */
    CyclicBufferBuilder cyclic_buffer(const TypeMeta* element_type, size_t capacity);

    /**
     * @brief Create a queue type builder.
     *
     * @param element_type The type of queue elements
     * @return A builder for constructing the queue type
     */
    QueueBuilder queue(const TypeMeta* element_type);

    // ========== Named Bundle Lookup ==========

    /**
     * @brief Get a named bundle type by name.
     *
     * @param name The bundle name
     * @return Pointer to the TypeMeta, or nullptr if not found
     */
    [[nodiscard]] const TypeMeta* get_bundle_by_name(const std::string& name) const;

    /**
     * @brief Check if a named bundle exists.
     *
     * @param name The bundle name
     * @return true if the bundle exists
     */
    [[nodiscard]] bool has_bundle(const std::string& name) const;

    // ========== Name-Based Type Lookup ==========

    /**
     * @brief Get a TypeMeta by its human-readable name.
     *
     * Looks up types registered with names like "int", "str", "bool", "float",
     * "date", "datetime", "timedelta".
     *
     * @param name The type name
     * @return Pointer to the TypeMeta, or nullptr if not found
     */
    [[nodiscard]] const TypeMeta* get_by_name(const std::string& name) const;

    /**
     * @brief Check if a type with the given name exists.
     *
     * @param name The type name
     * @return true if the type exists
     */
    [[nodiscard]] bool has_by_name(const std::string& name) const;

    // ========== Python Type Lookup ==========

    /**
     * @brief Get a TypeMeta from a Python type object.
     *
     * Requires GIL to be held by caller.
     *
     * @param py_type The Python type object
     * @return Pointer to the TypeMeta, or nullptr if not found
     */
    [[nodiscard]] const TypeMeta* from_python_type(nb::handle py_type) const;

    /**
     * @brief Register a Python type mapping.
     *
     * Associates a Python type with a TypeMeta for lookup via from_python_type().
     * Requires GIL to be held by caller.
     *
     * @param py_type The Python type object
     * @param meta The TypeMeta to associate
     */
    void register_python_type(nb::handle py_type, const TypeMeta* meta);

    // ========== Internal Registration ==========

    /**
     * @brief Register a composite type (called by builders).
     *
     * Takes ownership of the TypeMeta.
     *
     * @param meta The TypeMeta to register
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* register_composite(std::unique_ptr<TypeMeta> meta);

    /**
     * @brief Register a named bundle (called by builders).
     *
     * @param name The bundle name
     * @param meta Pointer to the already-registered TypeMeta
     */
    void register_named_bundle(const std::string& name, const TypeMeta* meta);

    /**
     * @brief Store a field info array (called by builders).
     *
     * Takes ownership of the field info array.
     *
     * @param fields The field info array
     * @return Pointer to the stored array
     */
    BundleFieldInfo* store_field_info(std::unique_ptr<BundleFieldInfo[]> fields);

    /**
     * @brief Store a field name string (called by builders).
     *
     * Takes ownership of the string.
     *
     * @param name The name to store
     * @return Pointer to the stored string
     */
    const char* store_name(std::string name);

private:
    TypeRegistry() = default;
    ~TypeRegistry() = default;

    /// Scalar types indexed by type_index
    std::unordered_map<std::type_index, std::unique_ptr<TypeMeta>> _scalar_types;

    /// Operations storage for scalar types (need stable addresses)
    std::unordered_map<std::type_index, std::unique_ptr<TypeOps>> _scalar_ops;

    /// Named bundles for lookup by name
    std::unordered_map<std::string, const TypeMeta*> _named_bundles;

    /// All composite types (ownership)
    std::vector<std::unique_ptr<TypeMeta>> _composite_types;

    /// Field info storage for bundles (ownership)
    std::vector<std::unique_ptr<BundleFieldInfo[]>> _field_storage;

    /// Name storage for bundles/fields (ownership)
    std::vector<std::unique_ptr<std::string>> _name_storage;

    // ========== Name-based lookup caches ==========

    /// Name-based lookup cache (name -> TypeMeta*)
    std::unordered_map<std::string, const TypeMeta*> _name_cache;

    /// Python type lookup cache (PyObject* -> TypeMeta*)
    /// Note: Uses raw PyObject* pointers; GIL must be held during access
    std::unordered_map<PyObject*, const TypeMeta*> _python_type_cache;

    /// Internal helper: store a name in the string pool with deduplication
    const char* store_name_interned(const std::string& name);
};

// ============================================================================
// Convenience Function
// ============================================================================

/**
 * @brief Get the TypeMeta for a scalar type.
 *
 * Convenience function that accesses the singleton registry.
 * Registers the type if not already registered.
 *
 * Thread Safety Warning:
 * This function is NOT thread-safe if the type has not been previously
 * registered. The get-then-register pattern can race with concurrent calls.
 * For thread-safe usage, either:
 * 1. Pre-register all needed types during startup, or
 * 2. Ensure external synchronization (e.g., Python's GIL during graph construction)
 *
 * @tparam T The scalar type
 * @return Pointer to the TypeMeta
 */
template<typename T>
const TypeMeta* scalar_type_meta() {
    auto& registry = TypeRegistry::instance();
    auto* meta = registry.get_scalar<T>();
    if (!meta) {
        meta = registry.register_type<T>();
    }
    return meta;
}

// ============================================================================
// Template Implementations
// ============================================================================

template<typename T>
const TypeMeta* TypeRegistry::register_type() {
    std::type_index idx(typeid(T));

    // Check if already registered
    auto it = _scalar_types.find(idx);
    if (it != _scalar_types.end()) {
        return it->second.get();
    }

    // Create operations vtable
    auto ops = std::make_unique<TypeOps>(ScalarOps<T>::make_ops());
    TypeOps* ops_ptr = ops.get();
    _scalar_ops[idx] = std::move(ops);

    // Create TypeMeta
    auto meta = std::make_unique<TypeMeta>();
    meta->size = sizeof(T);
    meta->alignment = alignof(T);
    meta->kind = TypeKind::Atomic;
    meta->flags = compute_scalar_flags<T>();
    meta->ops = ops_ptr;
    meta->name = nullptr;
    meta->element_type = nullptr;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->field_count = 0;
    meta->fixed_size = 0;

    TypeMeta* meta_ptr = meta.get();
    _scalar_types[idx] = std::move(meta);

    return meta_ptr;
}

template<typename T>
const TypeMeta* TypeRegistry::get_scalar() const {
    std::type_index idx(typeid(T));
    auto it = _scalar_types.find(idx);
    return (it != _scalar_types.end()) ? it->second.get() : nullptr;
}

template<typename T>
bool TypeRegistry::has_scalar() const {
    return _scalar_types.count(std::type_index(typeid(T))) > 0;
}

template<typename T>
const TypeMeta* TypeRegistry::register_type(const std::string& name) {
    // First, ensure the type is registered
    TypeMeta* meta_ptr = const_cast<TypeMeta*>(register_type<T>());

    // Store the name in the string pool (with deduplication)
    const char* stored_name = store_name_interned(name);

    // Update the TypeMeta's name field
    meta_ptr->name = stored_name;

    // Add to name cache
    _name_cache[name] = meta_ptr;

    return meta_ptr;
}

template<typename T>
const TypeMeta* TypeRegistry::register_type(const std::string& name, const TypeOps* custom_ops) {
    std::type_index idx(typeid(T));

    // Check if already registered
    auto it = _scalar_types.find(idx);
    if (it != _scalar_types.end()) {
        // Update name and ops if provided
        TypeMeta* meta_ptr = it->second.get();
        if (!name.empty()) {
            const char* stored_name = store_name_interned(name);
            meta_ptr->name = stored_name;
            _name_cache[name] = meta_ptr;
        }
        if (custom_ops) {
            meta_ptr->ops = custom_ops;
        }
        return meta_ptr;
    }

    // Create TypeMeta with custom ops
    auto meta = std::make_unique<TypeMeta>();
    meta->size = sizeof(T);
    meta->alignment = alignof(T);
    meta->kind = TypeKind::Atomic;
    meta->flags = compute_scalar_flags<T>();
    meta->ops = custom_ops;
    meta->name = nullptr;
    meta->element_type = nullptr;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->field_count = 0;
    meta->fixed_size = 0;

    TypeMeta* meta_ptr = meta.get();
    _scalar_types[idx] = std::move(meta);

    if (!name.empty()) {
        const char* stored_name = store_name_interned(name);
        meta_ptr->name = stored_name;
        _name_cache[name] = meta_ptr;
    }

    return meta_ptr;
}

// ============================================================================
// Type Builders
// ============================================================================

/**
 * @brief Builder for tuple types.
 *
 * Tuples are heterogeneous collections with positional (index) access only.
 * They are always unnamed.
 */
class TupleBuilder {
public:
    TupleBuilder() : _registry(TypeRegistry::instance()) {}
    explicit TupleBuilder(TypeRegistry& registry) : _registry(registry) {}

    /// Add an element type (user guide API name)
    TupleBuilder& add_element(const TypeMeta* type) {
        _element_types.push_back(type);
        return *this;
    }

    /// Add an element type (legacy name, kept for backward compat)
    TupleBuilder& element(const TypeMeta* type) {
        return add_element(type);
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    std::vector<const TypeMeta*> _element_types;
};

/**
 * @brief Builder for bundle types.
 *
 * Bundles are struct-like types with both named and indexed field access.
 */
class BundleBuilder {
public:
    BundleBuilder() : _registry(TypeRegistry::instance()) {}
    BundleBuilder(TypeRegistry& registry, std::string name = "")
        : _registry(registry), _name(std::move(name)) {}

    /// Set the bundle name
    BundleBuilder& set_name(const std::string& name) {
        _name = name;
        return *this;
    }

    /// Add a field (user guide API name)
    BundleBuilder& add_field(const char* name, const TypeMeta* type) {
        _fields.push_back({name, type});
        return *this;
    }

    /// Add a field (legacy name, kept for backward compat)
    BundleBuilder& field(const char* name, const TypeMeta* type) {
        return add_field(name, type);
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    std::string _name;
    std::vector<std::pair<const char*, const TypeMeta*>> _fields;
};

/**
 * @brief Builder for list types.
 */
class ListBuilder {
public:
    ListBuilder() : _registry(TypeRegistry::instance()) {}
    ListBuilder(TypeRegistry& registry, const TypeMeta* element_type, size_t fixed_size = 0)
        : _registry(registry), _element_type(element_type), _fixed_size(fixed_size) {}

    /// Set the element type
    ListBuilder& set_element_type(const TypeMeta* type) {
        _element_type = type;
        return *this;
    }

    /// Set the fixed size (0 = dynamic)
    ListBuilder& set_size(size_t size) {
        _fixed_size = size;
        return *this;
    }

    /// Mark as variadic tuple (tuple[T, ...])
    ListBuilder& as_variadic_tuple() {
        _is_variadic_tuple = true;
        return *this;
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type{nullptr};
    size_t _fixed_size{0};
    bool _is_variadic_tuple{false};
};

/**
 * @brief Builder for set types.
 */
class SetBuilder {
public:
    SetBuilder() : _registry(TypeRegistry::instance()) {}
    SetBuilder(TypeRegistry& registry, const TypeMeta* element_type)
        : _registry(registry), _element_type(element_type) {}

    /// Set the element type
    SetBuilder& set_element_type(const TypeMeta* type) {
        _element_type = type;
        return *this;
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type{nullptr};
};

/**
 * @brief Builder for map types.
 */
class MapBuilder {
public:
    MapBuilder() : _registry(TypeRegistry::instance()) {}
    MapBuilder(TypeRegistry& registry, const TypeMeta* key_type, const TypeMeta* value_type)
        : _registry(registry), _key_type(key_type), _value_type(value_type) {}

    /// Set the key type
    MapBuilder& set_key_type(const TypeMeta* type) {
        _key_type = type;
        return *this;
    }

    /// Set the value type
    MapBuilder& set_value_type(const TypeMeta* type) {
        _value_type = type;
        return *this;
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _key_type{nullptr};
    const TypeMeta* _value_type{nullptr};
};

/**
 * @brief Builder for cyclic buffer types.
 *
 * Cyclic buffers are fixed-size circular buffers that re-center on read.
 * When full, the oldest element is overwritten.
 */
class CyclicBufferBuilder {
public:
    CyclicBufferBuilder() : _registry(TypeRegistry::instance()) {}
    CyclicBufferBuilder(TypeRegistry& registry, const TypeMeta* element_type, size_t capacity)
        : _registry(registry), _element_type(element_type), _capacity(capacity) {}

    /// Set the element type
    CyclicBufferBuilder& set_element_type(const TypeMeta* type) {
        _element_type = type;
        return *this;
    }

    /// Set the buffer capacity
    CyclicBufferBuilder& set_capacity(size_t capacity) {
        _capacity = capacity;
        return *this;
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type{nullptr};
    size_t _capacity{0};
};

/**
 * @brief Builder for queue types.
 *
 * Queues are FIFO data structures with optional max capacity.
 */
class QueueBuilder {
public:
    QueueBuilder() : _registry(TypeRegistry::instance()) {}
    QueueBuilder(TypeRegistry& registry, const TypeMeta* element_type)
        : _registry(registry), _element_type(element_type) {}

    /// Set the element type
    QueueBuilder& set_element_type(const TypeMeta* type) {
        _element_type = type;
        return *this;
    }

    /// Set the maximum capacity (0 = unbounded)
    QueueBuilder& max_capacity(size_t max) {
        _max_capacity = max;
        return *this;
    }

    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type{nullptr};
    size_t _max_capacity{0};
};

// ============================================================================
// TypeMeta Static Method Implementations
// ============================================================================

inline const TypeMeta* TypeMeta::get(const std::string& type_name) {
    return TypeRegistry::instance().get_by_name(type_name);
}

template<typename T>
const TypeMeta* TypeMeta::get() {
    return TypeRegistry::instance().get_scalar<T>();
}

inline const TypeMeta* TypeMeta::from_python_type(nb::handle py_type) {
    return TypeRegistry::instance().from_python_type(py_type);
}

} // namespace hgraph::value
