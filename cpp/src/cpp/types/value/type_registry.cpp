#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/composite_ops.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>
#include <hgraph/types/value/queue_ops.h>

namespace hgraph::value {

// ============================================================================
// Helper: Compute composite type flags from element/field types
// ============================================================================

/**
 * @brief Compute TypeFlags for a composite type from its elements.
 *
 * Composite types (Tuple, Bundle) inherit flags that require ALL elements
 * to have that flag. For example, a tuple is only Hashable if all its
 * elements are Hashable.
 */
static TypeFlags compute_composite_flags(const TypeMeta* const* types, size_t count) {
    if (count == 0) {
        // Empty composite is trivial and hashable
        return TypeFlags::TriviallyConstructible | TypeFlags::TriviallyDestructible |
               TypeFlags::TriviallyCopyable | TypeFlags::Hashable |
               TypeFlags::Equatable | TypeFlags::Comparable;
    }

    // Start with all flags set, then intersect with each element's flags
    TypeFlags result = TypeFlags::TriviallyConstructible | TypeFlags::TriviallyDestructible |
                       TypeFlags::TriviallyCopyable | TypeFlags::Hashable |
                       TypeFlags::Equatable | TypeFlags::Comparable | TypeFlags::BufferCompatible;

    for (size_t i = 0; i < count; ++i) {
        const TypeMeta* elem = types[i];
        if (!elem) continue;

        // Intersect flags - composite only has a flag if ALL elements have it
        if (!elem->is_trivially_constructible()) {
            result = result & ~TypeFlags::TriviallyConstructible;
        }
        if (!elem->is_trivially_destructible()) {
            result = result & ~TypeFlags::TriviallyDestructible;
        }
        if (!elem->is_trivially_copyable()) {
            result = result & ~TypeFlags::TriviallyCopyable;
        }
        if (!elem->is_hashable()) {
            result = result & ~TypeFlags::Hashable;
        }
        if (!elem->is_equatable()) {
            result = result & ~TypeFlags::Equatable;
        }
        if (!elem->is_comparable()) {
            result = result & ~TypeFlags::Comparable;
        }
        if (!elem->is_buffer_compatible()) {
            result = result & ~TypeFlags::BufferCompatible;
        }
    }

    return result;
}

/**
 * @brief Compute TypeFlags for a Bundle from its field info.
 */
static TypeFlags compute_bundle_flags(const BundleFieldInfo* fields, size_t count) {
    if (count == 0) {
        return TypeFlags::TriviallyConstructible | TypeFlags::TriviallyDestructible |
               TypeFlags::TriviallyCopyable | TypeFlags::Hashable |
               TypeFlags::Equatable | TypeFlags::Comparable;
    }

    TypeFlags result = TypeFlags::TriviallyConstructible | TypeFlags::TriviallyDestructible |
                       TypeFlags::TriviallyCopyable | TypeFlags::Hashable |
                       TypeFlags::Equatable | TypeFlags::Comparable | TypeFlags::BufferCompatible;

    for (size_t i = 0; i < count; ++i) {
        const TypeMeta* elem = fields[i].type;
        if (!elem) continue;

        if (!elem->is_trivially_constructible()) {
            result = result & ~TypeFlags::TriviallyConstructible;
        }
        if (!elem->is_trivially_destructible()) {
            result = result & ~TypeFlags::TriviallyDestructible;
        }
        if (!elem->is_trivially_copyable()) {
            result = result & ~TypeFlags::TriviallyCopyable;
        }
        if (!elem->is_hashable()) {
            result = result & ~TypeFlags::Hashable;
        }
        if (!elem->is_equatable()) {
            result = result & ~TypeFlags::Equatable;
        }
        if (!elem->is_comparable()) {
            result = result & ~TypeFlags::Comparable;
        }
        if (!elem->is_buffer_compatible()) {
            result = result & ~TypeFlags::BufferCompatible;
        }
    }

    return result;
}

// ============================================================================
// TypeRegistry Singleton
// ============================================================================

TypeRegistry& TypeRegistry::instance() {
    static TypeRegistry registry;
    return registry;
}

// ============================================================================
// Named Bundle Operations
// ============================================================================

const TypeMeta* TypeRegistry::get_bundle_by_name(const std::string& name) const {
    auto it = _named_bundles.find(name);
    return (it != _named_bundles.end()) ? it->second : nullptr;
}

bool TypeRegistry::has_bundle(const std::string& name) const {
    return _named_bundles.count(name) > 0;
}

// ============================================================================
// Internal Registration
// ============================================================================

const TypeMeta* TypeRegistry::register_composite(std::unique_ptr<TypeMeta> meta) {
    TypeMeta* ptr = meta.get();
    _composite_types.push_back(std::move(meta));
    return ptr;
}

void TypeRegistry::register_named_bundle(const std::string& name, const TypeMeta* meta) {
    _named_bundles[name] = meta;
}

BundleFieldInfo* TypeRegistry::store_field_info(std::unique_ptr<BundleFieldInfo[]> fields) {
    BundleFieldInfo* ptr = fields.get();
    _field_storage.push_back(std::move(fields));
    return ptr;
}

const char* TypeRegistry::store_name(std::string name) {
    auto stored = std::make_unique<std::string>(std::move(name));
    const char* ptr = stored->c_str();
    _name_storage.push_back(std::move(stored));
    return ptr;
}

// ============================================================================
// Non-template register_type (name + custom ops, no C++ type binding)
// ============================================================================

const TypeMeta* TypeRegistry::register_type(const std::string& name, const type_ops& custom_ops) {
    // Check name cache first
    auto it = _name_cache.find(name);
    if (it != _name_cache.end()) {
        return it->second;
    }

    // Create TypeMeta with custom ops
    auto meta = std::make_unique<TypeMeta>();
    meta->size = 0;
    meta->alignment = 1;
    meta->kind = TypeKind::Atomic;
    meta->flags = TypeFlags::None;
    meta->ops_ = custom_ops;
    meta->element_type = nullptr;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->field_count = 0;
    meta->fixed_size = 0;

    const char* stored_name = store_name_interned(name);
    meta->name = stored_name;

    TypeMeta* meta_ptr = meta.get();
    _composite_types.push_back(std::move(meta));
    _name_cache[name] = meta_ptr;

    return meta_ptr;
}

// ============================================================================
// Name-Based Type Lookup
// ============================================================================

const TypeMeta* TypeRegistry::get_by_name(const std::string& name) const {
    auto it = _name_cache.find(name);
    return (it != _name_cache.end()) ? it->second : nullptr;
}

bool TypeRegistry::has_by_name(const std::string& name) const {
    return _name_cache.count(name) > 0;
}

// ============================================================================
// Python Type Lookup
// ============================================================================

const TypeMeta* TypeRegistry::from_python_type(nb::handle py_type) const {
    // GIL must be held by caller
    PyObject* key = py_type.ptr();
    auto it = _python_type_cache.find(key);
    return (it != _python_type_cache.end()) ? it->second : nullptr;
}

void TypeRegistry::register_python_type(nb::handle py_type, const TypeMeta* meta) {
    // GIL must be held by caller
    PyObject* key = py_type.ptr();
    _python_type_cache[key] = meta;
}

const char* TypeRegistry::store_name_interned(const std::string& name) {
    // Check if already stored (deduplication)
    for (const auto& stored : _name_storage) {
        if (*stored == name) {
            return stored->c_str();
        }
    }
    // Add new string to pool
    return store_name(name);
}

// ============================================================================
// Type Builder Methods
// ============================================================================

TupleBuilder TypeRegistry::tuple() {
    return TupleBuilder(*this);
}

BundleBuilder TypeRegistry::bundle() {
    return BundleBuilder(*this);
}

BundleBuilder TypeRegistry::bundle(const std::string& name) {
    return BundleBuilder(*this, name);
}

ListBuilder TypeRegistry::list(const TypeMeta* element_type) {
    return ListBuilder(*this, element_type, 0);
}

ListBuilder TypeRegistry::fixed_list(const TypeMeta* element_type, size_t size) {
    return ListBuilder(*this, element_type, size);
}

SetBuilder TypeRegistry::set(const TypeMeta* element_type) {
    return SetBuilder(*this, element_type);
}

MapBuilder TypeRegistry::map(const TypeMeta* key_type, const TypeMeta* value_type) {
    return MapBuilder(*this, key_type, value_type);
}

CyclicBufferBuilder TypeRegistry::cyclic_buffer(const TypeMeta* element_type, size_t capacity) {
    return CyclicBufferBuilder(*this, element_type, capacity);
}

QueueBuilder TypeRegistry::queue(const TypeMeta* element_type) {
    return QueueBuilder(*this, element_type);
}

// ============================================================================
// TupleBuilder::build()
// ============================================================================

const TypeMeta* TupleBuilder::build() {
    const size_t count = _element_types.size();

    // Calculate total size and alignment
    size_t total_size = 0;
    size_t max_alignment = 1;

    // Allocate field info array (tuples use BundleFieldInfo with nullptr names)
    auto fields = std::make_unique<BundleFieldInfo[]>(count);

    for (size_t i = 0; i < count; ++i) {
        const TypeMeta* elem = _element_types[i];

        // Align offset for this element
        size_t alignment = elem ? elem->alignment : 1;
        total_size = (total_size + alignment - 1) & ~(alignment - 1);

        // Store field info (name is nullptr for tuple elements)
        fields[i].name = nullptr;
        fields[i].index = i;
        fields[i].offset = total_size;
        fields[i].type = elem;

        // Update totals
        total_size += elem ? elem->size : 0;
        if (alignment > max_alignment) max_alignment = alignment;
    }

    // Align final size
    total_size = (total_size + max_alignment - 1) & ~(max_alignment - 1);

    // Store fields in registry and get pointer
    BundleFieldInfo* fields_ptr = count > 0 ? _registry.store_field_info(std::move(fields)) : nullptr;

    // Compute flags from element types
    TypeFlags flags = compute_composite_flags(_element_types.data(), count);

    // Create TypeMeta
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::Tuple;
    meta->flags = flags;
    meta->field_count = count;
    meta->size = total_size;
    meta->alignment = max_alignment;
    meta->ops_ = TupleOps::make_ops();
    meta->name = nullptr;
    meta->element_type = nullptr;
    meta->key_type = nullptr;
    meta->fields = fields_ptr;
    meta->fixed_size = 0;

    return _registry.register_composite(std::move(meta));
}

// ============================================================================
// BundleBuilder::build()
// ============================================================================

const TypeMeta* BundleBuilder::build() {
    const size_t count = _fields.size();

    // Calculate total size and alignment
    size_t total_size = 0;
    size_t max_alignment = 1;

    // Allocate field info array
    auto fields = std::make_unique<BundleFieldInfo[]>(count);

    for (size_t i = 0; i < count; ++i) {
        const char* name = _fields[i].first;
        const TypeMeta* type = _fields[i].second;

        // Align offset for this field
        size_t alignment = type ? type->alignment : 1;
        total_size = (total_size + alignment - 1) & ~(alignment - 1);

        // Store the name in registry (to ensure stable pointer)
        const char* stored_name = _registry.store_name(name ? name : "");

        // Store field info
        fields[i].name = stored_name;
        fields[i].index = i;
        fields[i].offset = total_size;
        fields[i].type = type;

        // Update totals
        total_size += type ? type->size : 0;
        if (alignment > max_alignment) max_alignment = alignment;
    }

    // Align final size
    total_size = (total_size + max_alignment - 1) & ~(max_alignment - 1);

    // Store fields in registry and get pointer
    BundleFieldInfo* fields_ptr = count > 0 ? _registry.store_field_info(std::move(fields)) : nullptr;

    // Compute flags from field types
    TypeFlags flags = fields_ptr ? compute_bundle_flags(fields_ptr, count) : TypeFlags::None;

    // Create TypeMeta
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::Bundle;
    meta->flags = flags;
    meta->field_count = count;
    meta->size = total_size;
    meta->alignment = max_alignment;
    meta->ops_ = BundleOps::make_ops();
    meta->name = nullptr;
    meta->element_type = nullptr;
    meta->key_type = nullptr;
    meta->fields = fields_ptr;
    meta->fixed_size = 0;

    const TypeMeta* result = _registry.register_composite(std::move(meta));

    // Register as named bundle if name was provided
    if (!_name.empty()) {
        _registry.register_named_bundle(_name, result);
    }

    return result;
}

// ============================================================================
// ListBuilder::build()
// ============================================================================

const TypeMeta* ListBuilder::build() {
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::List;
    meta->flags = _is_variadic_tuple ? TypeFlags::VariadicTuple : TypeFlags::None;
    meta->field_count = 0;

    if (_fixed_size > 0) {
        // Fixed-size list: elements stored inline
        size_t elem_size = _element_type ? _element_type->size : 0;
        size_t elem_align = _element_type ? _element_type->alignment : 1;
        meta->size = elem_size * _fixed_size;
        meta->alignment = elem_align;
    } else {
        // Dynamic list: uses DynamicListStorage
        meta->size = sizeof(DynamicListStorage);
        meta->alignment = alignof(DynamicListStorage);
    }

    meta->ops_ = ListOps::make_ops();
    meta->name = nullptr;
    meta->element_type = _element_type;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->fixed_size = _fixed_size;

    return _registry.register_composite(std::move(meta));
}

// ============================================================================
// SetBuilder::build()
// ============================================================================

const TypeMeta* SetBuilder::build() {
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::Set;
    meta->flags = TypeFlags::None;
    meta->field_count = 0;
    meta->size = sizeof(SetStorage);
    meta->alignment = alignof(SetStorage);
    meta->ops_ = SetOps::make_ops();
    meta->name = nullptr;
    meta->element_type = _element_type;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->fixed_size = 0;

    return _registry.register_composite(std::move(meta));
}

// ============================================================================
// MapBuilder::build()
// ============================================================================

const TypeMeta* MapBuilder::build() {
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::Map;
    meta->flags = TypeFlags::None;
    meta->field_count = 0;
    meta->size = sizeof(MapStorage);
    meta->alignment = alignof(MapStorage);
    meta->ops_ = MapOps::make_ops();
    meta->name = nullptr;
    meta->element_type = _value_type;  // Map uses element_type for values
    meta->key_type = _key_type;
    meta->fields = nullptr;
    meta->fixed_size = 0;

    return _registry.register_composite(std::move(meta));
}

// ============================================================================
// CyclicBufferBuilder::build()
// ============================================================================

const TypeMeta* CyclicBufferBuilder::build() {
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::CyclicBuffer;
    meta->flags = TypeFlags::None;
    meta->field_count = 0;
    meta->size = sizeof(CyclicBufferStorage);
    meta->alignment = alignof(CyclicBufferStorage);
    meta->ops_ = CyclicBufferOps::make_ops();
    meta->name = nullptr;
    meta->element_type = _element_type;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->fixed_size = _capacity;  // Store capacity in fixed_size

    return _registry.register_composite(std::move(meta));
}

// ============================================================================
// QueueBuilder::build()
// ============================================================================

const TypeMeta* QueueBuilder::build() {
    auto meta = std::make_unique<TypeMeta>();
    meta->kind = TypeKind::Queue;
    meta->flags = TypeFlags::None;
    meta->field_count = 0;
    meta->size = sizeof(QueueStorage);
    meta->alignment = alignof(QueueStorage);
    meta->ops_ = QueueOps::make_ops();
    meta->name = nullptr;
    meta->element_type = _element_type;
    meta->key_type = nullptr;
    meta->fields = nullptr;
    meta->fixed_size = _max_capacity;  // 0 = unbounded, >0 = max capacity

    return _registry.register_composite(std::move(meta));
}

} // namespace hgraph::value
