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

#include <memory>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace hgraph::value {

// ============================================================================
// Type Builders (Forward Declarations)
// ============================================================================

class TupleTypeBuilder;
class BundleTypeBuilder;
class ListTypeBuilder;
class SetTypeBuilder;
class MapTypeBuilder;
class CyclicBufferTypeBuilder;
class QueueTypeBuilder;

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
 * - Registration operations (register_scalar, register_composite, builder.build())
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

    // ========== Scalar Type Registration ==========

    /**
     * @brief Register a scalar type.
     *
     * If the type is already registered, returns the existing TypeMeta.
     * Thread-safe for concurrent reads; registration should happen at startup.
     *
     * @tparam T The scalar type to register
     * @return Pointer to the registered TypeMeta
     */
    template<typename T>
    const TypeMeta* register_scalar();

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
    TupleTypeBuilder tuple();

    /**
     * @brief Create an anonymous bundle type builder.
     * @return A builder for constructing the bundle type
     */
    BundleTypeBuilder bundle();

    /**
     * @brief Create a named bundle type builder.
     *
     * Named bundles can be retrieved later by name.
     *
     * @param name The name for this bundle type
     * @return A builder for constructing the bundle type
     */
    BundleTypeBuilder bundle(const std::string& name);

    /**
     * @brief Create a dynamic list type builder.
     *
     * @param element_type The type of list elements
     * @return A builder for constructing the list type
     */
    ListTypeBuilder list(const TypeMeta* element_type);

    /**
     * @brief Create a fixed-size list type builder.
     *
     * @param element_type The type of list elements
     * @param size The fixed size of the list
     * @return A builder for constructing the list type
     */
    ListTypeBuilder fixed_list(const TypeMeta* element_type, size_t size);

    /**
     * @brief Create a set type builder.
     *
     * @param element_type The type of set elements (must be hashable)
     * @return A builder for constructing the set type
     */
    SetTypeBuilder set(const TypeMeta* element_type);

    /**
     * @brief Create a map type builder.
     *
     * @param key_type The type of map keys (must be hashable)
     * @param value_type The type of map values
     * @return A builder for constructing the map type
     */
    MapTypeBuilder map(const TypeMeta* key_type, const TypeMeta* value_type);

    /**
     * @brief Create a cyclic buffer type builder.
     *
     * @param element_type The type of buffer elements
     * @param capacity The fixed capacity of the buffer
     * @return A builder for constructing the cyclic buffer type
     */
    CyclicBufferTypeBuilder cyclic_buffer(const TypeMeta* element_type, size_t capacity);

    /**
     * @brief Create a queue type builder.
     *
     * @param element_type The type of queue elements
     * @return A builder for constructing the queue type
     */
    QueueTypeBuilder queue(const TypeMeta* element_type);

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
        meta = registry.register_scalar<T>();
    }
    return meta;
}

// ============================================================================
// Template Implementations
// ============================================================================

template<typename T>
const TypeMeta* TypeRegistry::register_scalar() {
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
    meta->kind = TypeKind::Scalar;
    meta->flags = compute_scalar_flags<T>();
    meta->ops = ops_ptr;
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

// ============================================================================
// Type Builders
// ============================================================================

/**
 * @brief Builder for tuple types.
 *
 * Tuples are heterogeneous collections with positional (index) access only.
 * They are always unnamed.
 */
class TupleTypeBuilder {
public:
    explicit TupleTypeBuilder(TypeRegistry& registry) : _registry(registry) {}

    /**
     * @brief Add an element type to the tuple.
     * @param type The type of this element
     * @return Reference to this builder for chaining
     */
    TupleTypeBuilder& element(const TypeMeta* type) {
        _element_types.push_back(type);
        return *this;
    }

    /**
     * @brief Build and register the tuple type.
     * @return Pointer to the registered TypeMeta
     */
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
class BundleTypeBuilder {
public:
    BundleTypeBuilder(TypeRegistry& registry, std::string name = "")
        : _registry(registry), _name(std::move(name)) {}

    /**
     * @brief Add a field to the bundle.
     * @param name The field name
     * @param type The field type
     * @return Reference to this builder for chaining
     */
    BundleTypeBuilder& field(const char* name, const TypeMeta* type) {
        _fields.push_back({name, type});
        return *this;
    }

    /**
     * @brief Build and register the bundle type.
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    std::string _name;
    std::vector<std::pair<const char*, const TypeMeta*>> _fields;
};

/**
 * @brief Builder for list types.
 */
class ListTypeBuilder {
public:
    ListTypeBuilder(TypeRegistry& registry, const TypeMeta* element_type, size_t fixed_size = 0)
        : _registry(registry), _element_type(element_type), _fixed_size(fixed_size) {}

    /**
     * @brief Mark this list as a variadic tuple (tuple[T, ...]).
     *
     * When set, to_python will return a Python tuple instead of a list,
     * making the type hashable.
     *
     * @return Reference to this builder for chaining
     */
    ListTypeBuilder& as_variadic_tuple() {
        _is_variadic_tuple = true;
        return *this;
    }

    /**
     * @brief Build and register the list type.
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type;
    size_t _fixed_size;
    bool _is_variadic_tuple{false};
};

/**
 * @brief Builder for set types.
 */
class SetTypeBuilder {
public:
    SetTypeBuilder(TypeRegistry& registry, const TypeMeta* element_type)
        : _registry(registry), _element_type(element_type) {}

    /**
     * @brief Build and register the set type.
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type;
};

/**
 * @brief Builder for map types.
 */
class MapTypeBuilder {
public:
    MapTypeBuilder(TypeRegistry& registry, const TypeMeta* key_type, const TypeMeta* value_type)
        : _registry(registry), _key_type(key_type), _value_type(value_type) {}

    /**
     * @brief Build and register the map type.
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _key_type;
    const TypeMeta* _value_type;
};

/**
 * @brief Builder for cyclic buffer types.
 *
 * Cyclic buffers are fixed-size circular buffers that re-center on read.
 * When full, the oldest element is overwritten.
 */
class CyclicBufferTypeBuilder {
public:
    CyclicBufferTypeBuilder(TypeRegistry& registry, const TypeMeta* element_type, size_t capacity)
        : _registry(registry), _element_type(element_type), _capacity(capacity) {}

    /**
     * @brief Build and register the cyclic buffer type.
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type;
    size_t _capacity;
};

/**
 * @brief Builder for queue types.
 *
 * Queues are FIFO data structures with optional max capacity.
 */
class QueueTypeBuilder {
public:
    QueueTypeBuilder(TypeRegistry& registry, const TypeMeta* element_type)
        : _registry(registry), _element_type(element_type), _max_capacity(0) {}

    /**
     * @brief Set the maximum capacity for the queue.
     *
     * @param max The maximum capacity (0 = unbounded)
     * @return Reference to this builder for chaining
     */
    QueueTypeBuilder& max_capacity(size_t max) {
        _max_capacity = max;
        return *this;
    }

    /**
     * @brief Build and register the queue type.
     * @return Pointer to the registered TypeMeta
     */
    const TypeMeta* build();

private:
    TypeRegistry& _registry;
    const TypeMeta* _element_type;
    size_t _max_capacity;
};

} // namespace hgraph::value
