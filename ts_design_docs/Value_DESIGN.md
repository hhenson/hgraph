# Value Type System - Comprehensive Design Document

**Version**: 1.1
**Date**: 2025-12-29
**Branch**: `ts_value_25`
**Status**: Design

---

## Table of Contents

1. [Overview](#1-overview)
2. [Design Principles](#2-design-principles)
3. [Schema System](#3-schema-system)
4. [Storage Strategy](#4-storage-strategy)
5. [Trait System](#5-trait-system)
6. [Value and View Classes](#6-value-and-view-classes)
7. [Type Access API](#7-type-access-api)
8. [Visiting and Iteration](#8-visiting-and-iteration)
9. [Memory Layout](#9-memory-layout)
10. [Implementation References](#10-implementation-references)
11. [Testing Strategy](#11-testing-strategy)
12. [Future Work](#12-future-work)
13. [Extension Mechanism](#13-extension-mechanism)

**Appendices:**
- [Appendix A: Comparison with Alternatives](#appendix-a-comparison-with-alternatives)
- [Appendix B: Quick Reference](#appendix-b-quick-reference)

**Related Documents:**
- [Value_USER_GUIDE.md](Value_USER_GUIDE.md) - Practical usage guide with examples
- [Value_EXAMPLES.md](Value_EXAMPLES.md) - Extended examples and patterns

---

## 1. Overview

The Value type system provides type-erased data storage controlled by a schema. It is the foundation for representing
runtime values in the hgraph C++ runtime, supporting:

- Atomic values (scalars)
- Bundle/struct values (named fields)
- Lists (indexed collections)
- Maps (key-value pairs)
- Sets (unique elements)
- CyclicBuffers (fixed-size circular buffers)
- Queues (FIFO queues with optional max capacity)

The schema defines the type, operations, and memory layout. Values can be accessed at various levels of nesting through
the View abstraction.

### 1.1 Key Requirements (from Value.md)

1. Type erased data storage
2. Type controlled by type schema
3. Schema supports resolved HgTypeMetaData (Python integration)
4. Pre-allocation of memory based on schema
5. Nested schema support
6. Type registration and lookup
7. Multi-level access through views

---

## 2. Design Principles

| Principle | Description |
|-----------|-------------|
| **Type Erasure** | Store any type without compile-time knowledge |
| **Type Safety** | Runtime type checking via schema |
| **Memory Co-location** | Keep related data together for cache efficiency |
| **Composition over Inheritance** | Build complex types from simple traits |
| **Reduce Memory Churn** | Minimize allocations through SBO and pre-allocation |

---

## 3. Schema System

The schema (TypeMeta) describes the type, its size, alignment, and available operations.

### 3.1 TypeMeta Structure

```cpp
namespace hgraph::value {

enum class TypeKind : uint8_t {
    Scalar,       // Atomic values: int, double, bool, string, datetime
    Tuple,        // Indexed heterogeneous collection (unnamed, positional access only)
    Bundle,       // Named field collection (struct-like, index + name access)
    List,         // Indexed homogeneous collection (dynamic size)
    Set,          // Unordered unique elements
    Map,          // Key-value pairs
    CyclicBuffer, // Fixed-size circular buffer (re-centers on read)
    Queue,        // FIFO queue with optional max capacity
    Ref           // Reference to another time-series (future)
};

enum class TypeFlags : uint32_t {
    None                   = 0,
    TriviallyConstructible = 1 << 0,
    TriviallyDestructible  = 1 << 1,
    TriviallyCopyable      = 1 << 2,
    Hashable               = 1 << 3,
    Comparable             = 1 << 4,
    Equatable              = 1 << 5,
    BufferCompatible       = 1 << 6,  // numpy/Arrow compatible
};

struct TypeMeta {
    size_t size;              // Size in bytes
    size_t alignment;         // Alignment requirement
    TypeKind kind;            // Type category
    TypeFlags flags;          // Capability flags
    const TypeOps* ops;       // Type-erased operations vtable

    // For composite types
    const TypeMeta* element_type;      // List/Set element, Map value
    const TypeMeta* key_type;          // Map key type
    const BundleFieldInfo* fields;     // Bundle/Tuple field metadata (nullptr for homogeneous types)
    size_t field_count;                // Number of fields (Bundle/Tuple), or fixed_size (fixed List)

    // For fixed-size collections
    size_t fixed_size;                 // 0 = dynamic, >0 = fixed capacity (List)
    bool is_fixed_size() const { return fixed_size > 0; }
};

} // namespace hgraph::value
```

**Reference**: See `../hgraph/.claude/agents/value-rewrite/prototypes/prototype-003-entt/include/type_meta_stub.h`
for a working TypeMeta implementation.

### 3.2 Type Registration

Types must be registered before use. Registration provides the TypeMeta pointer used for type identity.

```cpp
namespace hgraph::value {

class TypeRegistry {
public:
    static TypeRegistry& instance();

    // Scalar type registration (compile-time)
    template<typename T>
    const TypeMeta* register_scalar();

    // Get registered scalar type
    template<typename T>
    const TypeMeta* get_scalar() const;

    // Composite type builders - anonymous (unnamed)
    TupleTypeBuilder tuple();  // Heterogeneous, index-only access (always fixed size)
    BundleTypeBuilder bundle();
    ListTypeBuilder list(const TypeMeta* element_type);           // Variable size (dynamic)
    ListTypeBuilder fixed_list(const TypeMeta* element_type, size_t size);  // Fixed size (pre-allocated)
    SetTypeBuilder set(const TypeMeta* element_type);
    MapTypeBuilder map(const TypeMeta* key_type, const TypeMeta* value_type);
    CyclicBufferTypeBuilder cyclic_buffer(const TypeMeta* element_type, size_t capacity);  // Fixed-size circular buffer
    QueueTypeBuilder queue(const TypeMeta* element_type);  // FIFO queue with optional max capacity

    // Named bundle support (tuples are always unnamed)
    // Named bundles can be retrieved by name after registration
    BundleTypeBuilder bundle(const std::string& name);
    const TypeMeta* get_bundle_by_name(const std::string& name) const;
    bool has_bundle(const std::string& name) const;

private:
    std::unordered_map<std::type_index, const TypeMeta*> _scalar_types;
    std::unordered_map<std::string, const TypeMeta*> _named_bundles;
    std::vector<std::unique_ptr<TypeMeta>> _composite_types;
};

// Convenience function
template<typename T>
const TypeMeta* scalar_type_meta() {
    return TypeRegistry::instance().get_scalar<T>();
}

} // namespace hgraph::value
```

**Named vs Unnamed Bundles:**
- **Unnamed bundles**: Created with `bundle()`, schema lifetime managed by registry
- **Named bundles**: Created with `bundle("Name")`, can be retrieved later by name
- Useful for schema reuse and Python interop where type names matter

**Reference**: EnTT uses `entt::type_id<T>()` for compile-time type hashing. See EnTT documentation at
https://skypjack.github.io/entt/core.html

### 3.3 Schema Comparison

Schema identity uses pointer equality (fast, nominal typing):

```cpp
bool same_type(const TypeMeta* a, const TypeMeta* b) {
    return a == b;  // O(1) identity check
}
```

For structural compatibility (e.g., Python interop), implement a separate comparison:

```cpp
bool structurally_compatible(const TypeMeta* a, const TypeMeta* b);
```

### 3.4 Supported Scalar Types

The following types are currently supported in the C++ runtime (see `cpp/include/hgraph/hgraph_forward_declarations.h:189`):

```cpp
using ts_payload_types = tp::tpack<bool, int64_t, double, engine_date_t, engine_time_t, engine_time_delta_t, nb::object>;
```

**Currently Implemented in C++:**

| C++ Type | Python Type | Size | Hashable | Comparable | Notes |
|----------|-------------|------|----------|------------|-------|
| `bool` | `bool` | 1 | Yes | Yes | |
| `int64_t` | `int` | 8 | Yes | Yes | Python int maps to int64_t |
| `double` | `float` | 8 | Yes | Yes | Python float maps to double |
| `engine_date_t` | `date` | 4 | Yes | Yes | `std::chrono::year_month_day` |
| `engine_time_t` | `datetime` | 8 | Yes | Yes | `time_point<system_clock, microseconds>` |
| `engine_time_delta_t` | `timedelta` | 8 | Yes | Yes | `std::chrono::microseconds` |
| `nb::object` | any | 16 | Via Python | Via Python | Fallback for unsupported types |

**Python Atomic Types (from `hgraph/_types/_scalar_type_meta_data.py:256-275`):**

| Python Type | C++ Mapping | Status |
|-------------|-------------|--------|
| `bool` | `bool` | Native |
| `int` | `int64_t` | Native |
| `float` | `double` | Native |
| `date` | `engine_date_t` | Native |
| `datetime` | `engine_time_t` | Native |
| `time` | `nb::object` | Via Python object |
| `timedelta` | `engine_time_delta_t` | Native |
| `str` | `std::string` / `nb::object` | Planned native |
| `bytes` | `nb::object` | Via Python object |
| `Enum` | `nb::object` | Via Python object |
| `Size` | Special marker | Type system only |
| `WindowSize` | Special marker | Type system only |
| `ScalarValue` | `nb::object` | Type-erased scalar |

**Time Types (from `cpp/include/hgraph/util/date_time.h`):**

```cpp
using engine_clock = std::chrono::system_clock;
using engine_time_t = std::chrono::time_point<engine_clock, std::chrono::microseconds>;
using engine_time_delta_t = std::chrono::microseconds;
using engine_date_t = std::chrono::year_month_day;
```

---

## 4. Storage Strategy

### 4.1 Small Buffer Optimization (SBO)

To avoid heap allocations for small types, use a fixed inline buffer with fallback to heap.

**Primary Approach: EnTT basic_any**

EnTT's `basic_any<24>` provides production-ready SBO:

```cpp
#include <entt/core/any.hpp>

namespace hgraph::value {
    // 24-byte inline buffer covers all scalar types
    using ValueStorage = entt::basic_any<24>;
}
```

**Key EnTT Features**:
- Built-in SBO with configurable buffer size
- `data()` method provides raw pointer access (critical for `as<T>()`)
- Type-safe construction via `std::in_place_type<T>`
- Mature, battle-tested (used in Minecraft)

**Reference**: `../hgraph/.claude/agents/value-rewrite/prototypes/prototype-003-entt/include/entt_value.h:43`

**CMake Integration**:
```cmake
include(FetchContent)
FetchContent_Declare(
    entt
    GIT_REPOSITORY https://github.com/skypjack/entt.git
    GIT_TAG v3.13.2
)
FetchContent_MakeAvailable(entt)
target_link_libraries(hgraph PRIVATE EnTT::EnTT)
```

**Reference**: `../hgraph/.claude/agents/value-rewrite/output/value-redesign-final.md:379-393`

### 4.2 Fallback: Custom SBO Storage

If external dependencies are prohibited, implement custom SBO:

```cpp
template<size_t InlineSize = 24, size_t Alignment = 8>
class SboStorage {
    static_assert(InlineSize >= sizeof(void*));

public:
    [[nodiscard]] bool is_inline() const noexcept { return _is_inline; }
    [[nodiscard]] void* data() noexcept {
        return _is_inline ? &_storage.inline_buffer : _storage.heap_ptr;
    }
    [[nodiscard]] static constexpr bool fits_inline(size_t size, size_t align) noexcept {
        return size <= InlineSize && align <= Alignment;
    }

private:
    union Storage {
        alignas(Alignment) unsigned char inline_buffer[InlineSize];
        void* heap_ptr;
    } _storage;
    bool _is_inline{true};
    size_t _size{0};
    size_t _alignment{0};
};
```

**Reference**: `../hgraph/.claude/agents/value-rewrite/prototypes/prototype-001-sbo-value/include/sbo_storage.h:49-233`

### 4.3 Storage Mode Decision

```
If sizeof(T) <= 24 AND alignof(T) <= 8 AND nothrow_move_constructible<T>:
    Use inline buffer (SBO)
Else:
    Allocate on heap
```

Types fitting in SBO (24 bytes):
- `bool` (1 byte)
- `int64_t` (8 bytes)
- `double` (8 bytes)
- `engine_date_t` / `year_month_day` (4 bytes)
- `engine_time_t` (8 bytes)
- `engine_time_delta_t` (8 bytes)
- `std::string` on most platforms (24 bytes with SSO)
- `nb::object` (16 bytes - Python object handle)

### 4.4 Composite Type Storage

**Bundle**: Contiguous memory with field offsets calculated from schema.

```
+--------+--------+--------+--------+
| Field0 | Field1 | Field2 | ...    |
+--------+--------+--------+--------+
         ^        ^
         |        └── offset from BundleFieldInfo
         └── base pointer
```

**List/Set/Map**: Dynamic storage with contiguous element arrays.

```cpp
// List storage: header + contiguous elements
struct ListStorage {
    size_t size;
    size_t capacity;
    // elements follow in-place or via separate allocation
};

// Set storage: uses ankerl::unordered_dense for O(1) lookup
struct SetStorage {
    ankerl::unordered_dense::set<...> elements;
};

// Map storage: keys and values in parallel arrays
struct MapStorage {
    ankerl::unordered_dense::map<...> entries;
};
```

---

## 5. Trait System

Traits define operations on values. Following the composition principle, traits are function pointer tables
in the TypeOps structure.

### 5.1 Trait Hierarchy (from Value.md)

```
Core Traits (all types):
├── construct(void* dst, const TypeMeta* schema)
├── destruct(void* obj, const TypeMeta* schema)
├── copy_assign(void* dst, const void* src, const TypeMeta* schema)
├── equals(const void* a, const void* b, const TypeMeta* schema) -> bool
├── to_string(const void* obj, const TypeMeta* schema) -> std::string
├── to_python(const void* obj, const TypeMeta* schema) -> nb::object
└── from_python(void* dst, const nb::object& src, const TypeMeta* schema)

Hashable Trait:
└── hash(const void* obj, const TypeMeta* schema) -> size_t

Comparable Trait (extends Hashable):
└── less_than(const void* a, const void* b, const TypeMeta* schema) -> bool

Iterable Trait:
├── size(const void* obj, const TypeMeta* schema) -> size_t
└── iterate(const void* obj, const TypeMeta* schema) -> Iterator

Indexable Trait (extends Iterable):
├── get_at(const void* obj, size_t index, const TypeMeta* schema) -> ConstValueView
└── set_at(void* obj, size_t index, const Value& value, const TypeMeta* schema)

Bundle Trait (extends Indexable):
├── get_at(size_t index) -> ConstValueView  [inherited from Indexable]
├── set_at(size_t index, value) -> void     [inherited from Indexable]
├── get_field(const char* name) -> ConstValueView  [by name]
└── set_field(const char* name, value) -> void     [by name]

NOTE: Bundle fields are accessible by BOTH index position AND field name.
      Field order is significant - the order fields are added determines their index.
      get_at(0) returns the first field, get_at(1) the second, etc.

Set Trait (extends Iterable):
├── contains(const void* obj, const Value& element, const TypeMeta* schema) -> bool
├── insert(void* obj, const Value& element, const TypeMeta* schema)
└── erase(void* obj, const Value& element, const TypeMeta* schema)

Map Trait (extends Set):
├── get(const void* obj, const Value& key, const TypeMeta* schema) -> ConstValueView
└── set(void* obj, const Value& key, const Value& value, const TypeMeta* schema)
```

### 5.2 TypeOps Structure

```cpp
struct TypeOps {
    // Core (required for all types)
    void (*construct)(void* dst, const TypeMeta* schema);
    void (*destruct)(void* obj, const TypeMeta* schema);
    void (*copy_assign)(void* dst, const void* src, const TypeMeta* schema);
    bool (*equals)(const void* a, const void* b, const TypeMeta* schema);
    std::string (*to_string)(const void* obj, const TypeMeta* schema);

    // Python interop
    nb::object (*to_python)(const void* obj, const TypeMeta* schema);
    void (*from_python)(void* dst, const nb::object& src, const TypeMeta* schema);

    // Hashable (optional, nullptr if not supported)
    size_t (*hash)(const void* obj, const TypeMeta* schema);

    // Comparable (optional)
    bool (*less_than)(const void* a, const void* b, const TypeMeta* schema);

    // Iterable (optional)
    size_t (*size)(const void* obj, const TypeMeta* schema);
    // Note: Iterator design TBD

    // Indexable (optional)
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);

    // Bundle (optional)
    const void* (*get_field)(const void* obj, const char* name, const TypeMeta* schema);
    void (*set_field)(void* obj, const char* name, const void* value, const TypeMeta* schema);

    // Set (optional)
    bool (*contains)(const void* obj, const void* element, const TypeMeta* schema);
    void (*insert)(void* obj, const void* element, const TypeMeta* schema);
    void (*erase)(void* obj, const void* element, const TypeMeta* schema);

    // Map (optional)
    const void* (*map_get)(const void* obj, const void* key, const TypeMeta* schema);
    void (*map_set)(void* obj, const void* key, const void* value, const TypeMeta* schema);
};
```

### 5.3 Trait Implementation for Scalars

```cpp
template<typename T>
struct ScalarOps {
    static void construct(void* dst, const TypeMeta*) {
        new (dst) T{};
    }

    static void destruct(void* obj, const TypeMeta*) {
        static_cast<T*>(obj)->~T();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta*) {
        *static_cast<T*>(dst) = *static_cast<const T*>(src);
    }

    static bool equals(const void* a, const void* b, const TypeMeta*) {
        return *static_cast<const T*>(a) == *static_cast<const T*>(b);
    }

    static size_t hash(const void* obj, const TypeMeta*) {
        return std::hash<T>{}(*static_cast<const T*>(obj));
    }

    static bool less_than(const void* a, const void* b, const TypeMeta*) {
        return *static_cast<const T*>(a) < *static_cast<const T*>(b);
    }

    static std::string to_string(const void* obj, const TypeMeta*) {
        if constexpr (std::is_same_v<T, std::string>) {
            return *static_cast<const T*>(obj);
        } else {
            return std::to_string(*static_cast<const T*>(obj));
        }
    }

    static constexpr TypeOps ops = {
        &construct, &destruct, &copy_assign, &equals, &to_string,
        nullptr, nullptr,  // Python (filled separately)
        &hash, &less_than,
        nullptr, nullptr, nullptr,  // Iterable/Indexable
        nullptr, nullptr,           // Bundle
        nullptr, nullptr, nullptr,  // Set
        nullptr, nullptr            // Map
    };
};
```

### 5.4 Extending Trait Operations

Trait operations can be extended without modifying TypeOps using the composition-based extension mechanism.
See [Section 13: Extension Mechanism](#13-extension-mechanism) for details on:
- Pre/post hooks for operations like `to_python` and `from_python`
- `OperatorContext` for carrying hooks and state
- Built-in extensions like `PythonCache`

---

## 6. Value and View Classes

The view system provides a hierarchy of specialized views based on type characteristics. Users can query
the type kind and obtain specialized views that expose type-appropriate operations.

### 6.1 View Type Hierarchy

```
ConstValueView (base)
├── Type queries: is_scalar(), is_tuple(), is_bundle(), is_list(), is_set(), is_map(),
│                 is_cyclic_buffer(), is_queue()
├── Conversion: as_tuple(), as_bundle(), as_list(), as_set(), as_map(),
│               as_cyclic_buffer(), as_queue()
│
├── ConstIndexedView (for positional access)
│   ├── ConstTupleView (heterogeneous, index-only access, always unnamed)
│   ├── ConstBundleView (struct-like, named + indexed fields)
│   ├── ConstListView (homogeneous indexed collection, fixed or dynamic size)
│   ├── ConstCyclicBufferView (fixed-size circular buffer, re-centers on read)
│   └── ConstQueueView (FIFO queue with optional max capacity)
│
├── ConstSetView (unique elements)
│
└── ConstMapView (key-value pairs)

ValueView (mutable base, extends ConstValueView)
├── IndexedView, TupleView, BundleView, ListView, SetView, MapView,
│   CyclicBufferView, QueueView (mutable versions)
```

### 6.2 Value (Owning Storage)

The Value class is a template with an optional policy parameter for zero-overhead extensions.
See [Section 13: Extension Mechanism](#13-extension-mechanism) for the full policy system.

```cpp
// Forward declarations for policy system (see Section 13)
struct NoCache {};
struct WithPythonCache {};

template<typename Policy = NoCache>
class Value : private PolicyStorage<Policy> {
public:
    Value() = default;
    explicit Value(const TypeMeta* schema);

    template<typename T>
    explicit Value(const T& val);

    // Construct by copying from a view (cloning)
    explicit Value(const ConstValueView& view);

    // Move semantics
    Value(Value&&) noexcept = default;
    Value& operator=(Value&&) noexcept = default;

    // Copy via explicit method (alternative to constructor)
    static Value copy(const Value& other);
    static Value copy(const ConstValueView& view);

    // Validity
    [[nodiscard]] bool valid() const;
    [[nodiscard]] const TypeMeta* schema() const;

    // Base views - mutable view invalidates cache if policy has caching
    [[nodiscard]] ValueView view() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return ValueView(data(), _schema);
    }
    [[nodiscard]] ConstValueView view() const;
    [[nodiscard]] ConstValueView const_view() const;

    // Specialized views (convenience methods)
    [[nodiscard]] TupleView as_tuple();
    [[nodiscard]] ConstTupleView as_tuple() const;
    [[nodiscard]] BundleView as_bundle();
    [[nodiscard]] ConstBundleView as_bundle() const;
    [[nodiscard]] ListView as_list();
    [[nodiscard]] ConstListView as_list() const;
    [[nodiscard]] SetView as_set();
    [[nodiscard]] ConstSetView as_set() const;
    [[nodiscard]] MapView as_map();
    [[nodiscard]] ConstMapView as_map() const;

    // Type access (see Section 7)
    template<typename T> [[nodiscard]] T& as();
    template<typename T> [[nodiscard]] const T& as() const;
    template<typename T> [[nodiscard]] T* try_as();
    template<typename T> [[nodiscard]] const T* try_as() const;
    template<typename T> [[nodiscard]] T& checked_as();
    template<typename T> [[nodiscard]] const T& checked_as() const;

    // Raw access
    [[nodiscard]] void* data();
    [[nodiscard]] const void* data() const;

    // Operations via TypeOps
    [[nodiscard]] bool equals(const Value& other) const;
    [[nodiscard]] bool equals(const ConstValueView& other) const;
    [[nodiscard]] size_t hash() const;
    [[nodiscard]] std::string to_string() const;

    // Python interop - uses policy for caching behavior (see Section 13)
    [[nodiscard]] nb::object to_python() const {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            if (this->_cached_python) {
                return *this->_cached_python;
            }
            auto result = _schema->ops->to_python(data(), _schema);
            this->_cached_python = result;
            return result;
        } else {
            return _schema->ops->to_python(data(), _schema);
        }
    }

    void from_python(const nb::object& src) {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        _schema->ops->from_python(data(), src, _schema);
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->_cached_python = src;
        }
    }

private:
    ValueStorage _storage;           // EnTT basic_any or custom SBO
    const TypeMeta* _schema{nullptr};
};

// Type aliases for common configurations
using PlainValue = Value<NoCache>;        // Default, no extensions
using CachedValue = Value<WithPythonCache>;  // Python object caching
```

### 6.3 ConstValueView (Base Non-owning Const View)

```cpp
class ConstValueView {
public:
    ConstValueView() = default;
    ConstValueView(const void* data, const TypeMeta* schema);

    // Validity
    [[nodiscard]] bool valid() const { return _data && _schema; }
    [[nodiscard]] const TypeMeta* schema() const { return _schema; }

    // ===== Type Kind Queries =====
    [[nodiscard]] bool is_scalar() const {
        return valid() && _schema->kind == TypeKind::Scalar;
    }
    [[nodiscard]] bool is_tuple() const {
        return valid() && _schema->kind == TypeKind::Tuple;
    }
    [[nodiscard]] bool is_bundle() const {
        return valid() && _schema->kind == TypeKind::Bundle;
    }
    [[nodiscard]] bool is_list() const {
        return valid() && _schema->kind == TypeKind::List;
    }
    [[nodiscard]] bool is_fixed_list() const {
        return is_list() && _schema->is_fixed_size();
    }
    [[nodiscard]] bool is_set() const {
        return valid() && _schema->kind == TypeKind::Set;
    }
    [[nodiscard]] bool is_map() const {
        return valid() && _schema->kind == TypeKind::Map;
    }

    // ===== Specialized View Conversions =====
    // Safe conversions - return std::optional (nullopt if wrong type)
    [[nodiscard]] std::optional<ConstTupleView> try_as_tuple() const;
    [[nodiscard]] std::optional<ConstBundleView> try_as_bundle() const;
    [[nodiscard]] std::optional<ConstListView> try_as_list() const;
    [[nodiscard]] std::optional<ConstSetView> try_as_set() const;
    [[nodiscard]] std::optional<ConstMapView> try_as_map() const;

    // Throwing conversions - throw if wrong type
    [[nodiscard]] ConstTupleView as_tuple() const;
    [[nodiscard]] ConstBundleView as_bundle() const;
    [[nodiscard]] ConstListView as_list() const;
    [[nodiscard]] ConstSetView as_set() const;
    [[nodiscard]] ConstMapView as_map() const;

    // ===== Scalar Type Checking =====
    [[nodiscard]] bool is_type(const TypeMeta* other) const { return _schema == other; }

    template<typename T>
    [[nodiscard]] bool is_scalar_type() const {
        return valid() && _schema == scalar_type_meta<T>();
    }

    // ===== Scalar Type Access =====
    template<typename T>
    [[nodiscard]] const T& as() const {
        assert(valid() && is_scalar_type<T>());
        return *static_cast<const T*>(_data);
    }

    template<typename T>
    [[nodiscard]] const T* try_as() const {
        return is_scalar_type<T>() ? static_cast<const T*>(_data) : nullptr;
    }

    template<typename T>
    [[nodiscard]] const T& checked_as() const {
        if (!valid()) throw std::runtime_error("invalid view");
        if (!is_scalar_type<T>()) throw std::runtime_error("type mismatch");
        return *static_cast<const T*>(_data);
    }

    // Raw access
    [[nodiscard]] const void* data() const { return _data; }

    // Operations
    [[nodiscard]] bool equals(const ConstValueView& other) const;
    [[nodiscard]] size_t hash() const;
    [[nodiscard]] std::string to_string() const;

    // Python interop (see Section 13 for extended operations with hooks)
    [[nodiscard]] nb::object to_python() const;
    [[nodiscard]] nb::object to_python(const OperatorContext& ctx) const;

    // Clone: create an owning Value copy of this view's data
    [[nodiscard]] Value clone() const;

protected:
    const void* _data{nullptr};
    const TypeMeta* _schema{nullptr};
};
```

### 6.4 ValueView (Base Non-owning Mutable View)

```cpp
class ValueView : public ConstValueView {
public:
    ValueView() = default;
    ValueView(void* data, const TypeMeta* schema);

    // Mutable data access
    [[nodiscard]] void* data() { return _mutable_data; }

    // ===== Specialized Mutable View Conversions =====
    [[nodiscard]] std::optional<TupleView> try_as_tuple();
    [[nodiscard]] std::optional<BundleView> try_as_bundle();
    [[nodiscard]] std::optional<ListView> try_as_list();
    [[nodiscard]] std::optional<SetView> try_as_set();
    [[nodiscard]] std::optional<MapView> try_as_map();

    [[nodiscard]] TupleView as_tuple();
    [[nodiscard]] BundleView as_bundle();
    [[nodiscard]] ListView as_list();
    [[nodiscard]] SetView as_set();
    [[nodiscard]] MapView as_map();

    // ===== Scalar Mutable Access =====
    template<typename T>
    [[nodiscard]] T& as() {
        assert(valid() && is_scalar_type<T>());
        return *static_cast<T*>(_mutable_data);
    }

    template<typename T>
    [[nodiscard]] T* try_as() {
        return is_scalar_type<T>() ? static_cast<T*>(_mutable_data) : nullptr;
    }

    template<typename T>
    [[nodiscard]] T& checked_as() {
        if (!valid()) throw std::runtime_error("invalid view");
        if (!is_scalar_type<T>()) throw std::runtime_error("type mismatch");
        return *static_cast<T*>(_mutable_data);
    }

    // Copy from another view
    void copy_from(const ConstValueView& other);

    // Python interop (see Section 13 for extended operations with hooks)
    void from_python(const nb::object& src);
    void from_python(const nb::object& src, const OperatorContext& ctx);

    // Root tracking (for notification chains - TSValue use case)
    void set_root(Value* root) { _root = root; }
    [[nodiscard]] Value* root() { return _root; }

private:
    void* _mutable_data{nullptr};
    Value* _root{nullptr};  // Optional, for notification
};
```

### 6.5 ConstIndexedView (Positional Access)

Base class for types supporting index-based access (bundles, lists).

```cpp
class ConstIndexedView : public ConstValueView {
public:
    using ConstValueView::ConstValueView;

    // Size
    [[nodiscard]] size_t size() const;

    // ===== Unified at() / operator[] for index access =====
    [[nodiscard]] ConstValueView at(size_t index) const;
    [[nodiscard]] ConstValueView operator[](size_t index) const { return at(index); }

    // Iteration
    class const_iterator;
    [[nodiscard]] const_iterator begin() const;
    [[nodiscard]] const_iterator end() const;
};
```

### 6.6 IndexedView (Mutable Positional Access)

```cpp
class IndexedView : public ValueView {
public:
    using ValueView::ValueView;

    // Size
    [[nodiscard]] size_t size() const;

    // ===== Unified at() / operator[] for index access =====
    [[nodiscard]] ConstValueView at(size_t index) const;
    [[nodiscard]] ValueView at(size_t index);
    [[nodiscard]] ConstValueView operator[](size_t index) const { return at(index); }
    [[nodiscard]] ValueView operator[](size_t index) { return at(index); }

    // Mutation by index
    void set(size_t index, const Value& value);
    void set(size_t index, const ConstValueView& value);

    // Templated mutation - automatically wraps T if schema-compatible
    // Validates that T matches the element type at compile-time or runtime
    template<typename T>
    void set(size_t index, const T& value) {
        // Runtime check: schema->element_type == scalar_type_meta<T>()
        set(index, Value(value));
    }
};
```

### 6.7 ConstTupleView (Heterogeneous Index-Only Access)

Tuples are like bundles but without named fields - only positional (index) access is supported.
Tuples are always unnamed (no named tuple schemas).

```cpp
class ConstTupleView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    // Inherits from ConstIndexedView:
    // - size()
    // - at(size_t index) / operator[](size_t index)
    // - begin() / end()

    // Element type access (heterogeneous - each position can have different type)
    [[nodiscard]] const TypeMeta* element_type(size_t index) const;
};
```

### 6.8 TupleView (Mutable Heterogeneous Index-Only Access)

```cpp
class TupleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    // Inherits from IndexedView:
    // - at(size_t index) / operator[](size_t index)
    // - set(size_t index, value)

    // Element type access
    [[nodiscard]] const TypeMeta* element_type(size_t index) const;

    // Templated mutation - validates T against element_type(index)
    template<typename T>
    void set(size_t index, const T& value) {
        // Runtime check: element_type(index) == scalar_type_meta<T>()
        IndexedView::set(index, Value(value));
    }
};
```

### 6.9 ConstBundleView (Struct-like Access)

Bundles support BOTH index-based and name-based access. Field order is significant.

```cpp
class ConstBundleView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    // Inherit index access from ConstIndexedView:
    // - at(size_t index)
    // - operator[](size_t index)

    // ===== Named field access (overloads) =====
    [[nodiscard]] ConstValueView at(std::string_view name) const;
    [[nodiscard]] ConstValueView operator[](std::string_view name) const { return at(name); }

    // Field metadata
    [[nodiscard]] size_t field_count() const { return size(); }
    [[nodiscard]] const BundleFieldInfo* field_info(size_t index) const;
    [[nodiscard]] const BundleFieldInfo* field_info(std::string_view name) const;

    // Check if field exists
    [[nodiscard]] bool has_field(std::string_view name) const;

    // Get field index by name (returns size() if not found)
    [[nodiscard]] size_t field_index(std::string_view name) const;
};
```

### 6.10 BundleView (Mutable Struct-like Access)

```cpp
class BundleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    // Inherit from IndexedView:
    // - at(size_t index) / operator[](size_t index)
    // - set(size_t index, value)

    // ===== Named field access (overloads) =====
    [[nodiscard]] ConstValueView at(std::string_view name) const;
    [[nodiscard]] ValueView at(std::string_view name);
    [[nodiscard]] ConstValueView operator[](std::string_view name) const { return at(name); }
    [[nodiscard]] ValueView operator[](std::string_view name) { return at(name); }

    // ===== Named field mutation (overloads) =====
    void set(std::string_view name, const Value& value);
    void set(std::string_view name, const ConstValueView& value);

    // ===== Templated mutation - auto-wrap if schema-compatible =====
    template<typename T>
    void set(size_t index, const T& value) {
        // Runtime check: field_info(index)->type == scalar_type_meta<T>()
        IndexedView::set(index, Value(value));
    }

    template<typename T>
    void set(std::string_view name, const T& value) {
        // Runtime check: field_info(name)->type == scalar_type_meta<T>()
        set(name, Value(value));
    }

    // Field metadata (same as const version)
    [[nodiscard]] size_t field_count() const;
    [[nodiscard]] const BundleFieldInfo* field_info(size_t index) const;
    [[nodiscard]] const BundleFieldInfo* field_info(std::string_view name) const;
    [[nodiscard]] bool has_field(std::string_view name) const;
    [[nodiscard]] size_t field_index(std::string_view name) const;
};
```

### 6.11 ConstListView (Indexed Collection Access)

```cpp
class ConstListView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    // Inherits from ConstIndexedView:
    // - size()
    // - at(size_t index) / operator[](size_t index)
    // - begin() / end()

    // List-specific
    [[nodiscard]] bool empty() const { return size() == 0; }
    [[nodiscard]] ConstValueView front() const { return at(0); }
    [[nodiscard]] ConstValueView back() const { return at(size() - 1); }
};
```

### 6.12 ListView (Mutable Indexed Collection)

```cpp
class ListView : public IndexedView {
public:
    using IndexedView::IndexedView;

    // Inherits from IndexedView:
    // - at(size_t) / operator[](size_t)
    // - set(size_t, value)

    // List-specific mutations (dynamic lists only - throw on fixed lists)
    void push_back(const Value& value);
    void push_back(const ConstValueView& value);
    void pop_back();
    void clear();
    void resize(size_t new_size);

    // Reset all elements to a sentinel value (works on both fixed and dynamic lists)
    void reset(const Value& sentinel);
    void reset(const ConstValueView& sentinel);

    // ===== Templated mutations - auto-wrap if schema-compatible =====
    template<typename T>
    void push_back(const T& value) {
        // Runtime check: schema->element_type == scalar_type_meta<T>()
        push_back(Value(value));
    }

    template<typename T>
    void reset(const T& sentinel) {
        // Runtime check: schema->element_type == scalar_type_meta<T>()
        reset(Value(sentinel));
    }

    // Convenience
    [[nodiscard]] bool empty() const { return size() == 0; }
    [[nodiscard]] ValueView front() { return at(0); }
    [[nodiscard]] ValueView back() { return at(size() - 1); }
};
```

### 6.13 ConstSetView (Unique Element Access)

```cpp
class ConstSetView : public ConstValueView {
public:
    using ConstValueView::ConstValueView;

    // Size
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const { return size() == 0; }

    // ===== Membership test =====
    [[nodiscard]] bool contains(const ConstValueView& value) const;

    // Templated membership - auto-wrap if schema-compatible
    template<typename T>
    [[nodiscard]] bool contains(const T& value) const {
        return contains(Value(value).const_view());
    }

    // Iteration
    class const_iterator;
    [[nodiscard]] const_iterator begin() const;
    [[nodiscard]] const_iterator end() const;
};
```

### 6.14 SetView (Mutable Set Operations)

```cpp
class SetView : public ValueView {
public:
    using ValueView::ValueView;

    // Size
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const { return size() == 0; }

    // Membership
    [[nodiscard]] bool contains(const ConstValueView& value) const;

    // Mutations
    bool insert(const Value& value);       // Returns true if inserted
    bool insert(const ConstValueView& value);
    bool erase(const ConstValueView& value);  // Returns true if erased
    void clear();

    // ===== Templated mutations - auto-wrap if schema-compatible =====
    template<typename T>
    [[nodiscard]] bool contains(const T& value) const {
        return contains(Value(value).const_view());
    }

    template<typename T>
    bool insert(const T& value) {
        // Runtime check: schema->element_type == scalar_type_meta<T>()
        return insert(Value(value));
    }

    template<typename T>
    bool erase(const T& value) {
        return erase(Value(value).const_view());
    }
};
```

### 6.15 ConstMapView (Key-Value Access)

```cpp
class ConstMapView : public ConstValueView {
public:
    using ConstValueView::ConstValueView;

    // Size
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const { return size() == 0; }

    // ===== Key-based access =====
    [[nodiscard]] ConstValueView at(const ConstValueView& key) const;
    [[nodiscard]] ConstValueView operator[](const ConstValueView& key) const { return at(key); }

    // Membership
    [[nodiscard]] bool contains(const ConstValueView& key) const;

    // ===== Templated access - auto-wrap key if schema-compatible =====
    template<typename K>
    [[nodiscard]] ConstValueView at(const K& key) const {
        return at(Value(key).const_view());
    }

    template<typename K>
    [[nodiscard]] bool contains(const K& key) const {
        return contains(Value(key).const_view());
    }

    // Iteration (over key-value pairs)
    class const_iterator;  // Dereferences to pair<ConstValueView, ConstValueView>
    [[nodiscard]] const_iterator begin() const;
    [[nodiscard]] const_iterator end() const;

    // Key/value iteration
    class const_key_iterator;
    class const_value_iterator;
    [[nodiscard]] auto keys() const;    // Returns range of keys
    [[nodiscard]] auto values() const;  // Returns range of values
};
```

### 6.16 MapView (Mutable Key-Value Operations)

```cpp
class MapView : public ValueView {
public:
    using ValueView::ValueView;

    // Size
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const { return size() == 0; }

    // ===== Key-based access =====
    [[nodiscard]] ConstValueView at(const ConstValueView& key) const;
    [[nodiscard]] ValueView at(const ConstValueView& key);
    [[nodiscard]] ConstValueView operator[](const ConstValueView& key) const { return at(key); }
    [[nodiscard]] ValueView operator[](const ConstValueView& key) { return at(key); }

    // Membership
    [[nodiscard]] bool contains(const ConstValueView& key) const;

    // ===== Mutation =====
    void set(const ConstValueView& key, const Value& value);
    void set(const ConstValueView& key, const ConstValueView& value);

    // Insert (returns true if new key inserted)
    bool insert(const ConstValueView& key, const Value& value);
    bool insert(const ConstValueView& key, const ConstValueView& value);

    // Erase (returns true if key existed)
    bool erase(const ConstValueView& key);
    void clear();

    // Get or insert default
    ValueView operator[](const ConstValueView& key);  // Inserts default if missing

    // ===== Templated mutations - auto-wrap key/value if schema-compatible =====
    template<typename K>
    [[nodiscard]] ConstValueView at(const K& key) const {
        return at(Value(key).const_view());
    }

    template<typename K>
    [[nodiscard]] ValueView at(const K& key) {
        return at(Value(key).const_view());
    }

    template<typename K>
    [[nodiscard]] bool contains(const K& key) const {
        return contains(Value(key).const_view());
    }

    template<typename K, typename V>
    void set(const K& key, const V& value) {
        // Runtime check: key_type matches K, value_type matches V
        set(Value(key).const_view(), Value(value));
    }

    template<typename K, typename V>
    bool insert(const K& key, const V& value) {
        return insert(Value(key).const_view(), Value(value));
    }

    template<typename K>
    bool erase(const K& key) {
        return erase(Value(key).const_view());
    }
};
```

### 6.17 Unified Access Pattern Summary

All specialized views use consistent `at()` / `operator[]` overloads:

| View Type | `at(size_t)` | `at(string_view)` | `at(ConstValueView)` |
|-----------|--------------|-------------------|----------------------|
| ConstIndexedView | ✓ | - | - |
| ConstTupleView | ✓ (inherited) | - | - |
| ConstBundleView | ✓ (inherited) | ✓ | - |
| ConstListView | ✓ (inherited) | - | - |
| ConstSetView | - | - | - (use contains) |
| ConstMapView | - | - | ✓ |

**Usage Examples:**
```cpp
// Tuple: access by index only (heterogeneous, unnamed)
TupleView tuple = value.as_tuple();
tuple[0].as<int64_t>() = 42;
tuple[1].as<std::string>() = "hello";
tuple[2].as<double>() = 3.14;

// Bundle: access by index or name
BundleView bundle = value.as_bundle();
bundle[0] = Value(42);              // By index
bundle["name"] = Value("Alice");    // By name

// List: access by index (homogeneous, fixed or dynamic size)
ListView list = value.as_list();
list[0] = Value(100);
list.push_back(Value(200));  // Only for dynamic lists

// Map: access by key
MapView map = value.as_map();
Value key("price");
map[key.const_view()] = Value(9.99);

// Set: membership test
SetView set = value.as_set();
set.insert(Value(42));
bool has_it = set.contains(Value(42).const_view());
```

---

## 7. Type Access API

Three tiers of typed access, matching different use cases:

### 7.1 `as<T>()` - Debug Assertion

Zero overhead in release builds. Use when type is guaranteed correct.

```cpp
template<typename T>
T& Value::as() {
    assert(valid() && "as<T>() on invalid Value");
    assert(is_scalar_type<T>() && "as<T>() type mismatch");
    return *static_cast<T*>(_storage.data());
}
```

### 7.2 `try_as<T>()` - Safe Access

Returns nullptr on type mismatch. Use for optional type handling.

```cpp
template<typename T>
T* Value::try_as() {
    if (!valid() || !is_scalar_type<T>()) return nullptr;
    return static_cast<T*>(_storage.data());
}
```

### 7.3 `checked_as<T>()` - Throwing Access

Throws on mismatch. Use at API boundaries.

```cpp
template<typename T>
T& Value::checked_as() {
    if (!valid()) throw std::runtime_error("checked_as<T>() on invalid Value");
    if (!is_scalar_type<T>()) throw std::runtime_error("checked_as<T>() type mismatch");
    return *static_cast<T*>(_storage.data());
}
```

### 7.4 Why EnTT's `data()` is Critical

EnTT's `basic_any::data()` returns a raw `void*` to the stored value, enabling the `as<T>()` pattern.
This was the deciding factor against Microsoft Proxy, which doesn't provide pointer access.

**From EnTT source** (entt/core/any.hpp):
```cpp
[[nodiscard]] const void *data() const noexcept {
    return storage.data();  // Returns pointer to inline buffer or heap
}
```

**Reference**: `../hgraph/.claude/agents/value-rewrite/output/value-redesign-final.md:208-237`

---

## 8. Visiting and Iteration

This section addresses the requirement from `Value.md`:
> "We need some way to access the value in a type-safe way, something like the visit approach in std::any.
> We need to be able to do nested structure visiting."

### 8.1 Design Influences

**Flux Library** (https://github.com/tcbrindle/flux):
- Uses **cursors** instead of iterators - cursors are like indices, requiring both sequence and position
- Enables bounds checking and prevents dangling references
- Four basis operations: `first`, `is_last`, `inc`, `read_at`

**ddvisitor** (https://github.com/uentity/ddvisitor):
- Sequential callable matching against arguments
- Automatic unpacking of `std::optional` and `std::variant`
- Supports dynamic matches (returns `std::optional`) and static matches (terminal)

**tpack** (https://github.com/uentity/tpack):
- Value-based type pack manipulation
- Algorithms as `constexpr` functions rather than meta-functions

### 8.2 Value Visitor Pattern

The visitor pattern enables type-safe operations on type-erased values without knowing the concrete type at compile time.

```cpp
namespace hgraph::value {

// Result type for visitors that may fail
template<typename T>
using VisitResult = std::optional<T>;

// Visitor interface
template<typename R = void>
struct ValueVisitor {
    // Called for scalar types
    virtual R visit_bool(bool& value) { return unhandled(); }
    virtual R visit_int64(int64_t& value) { return unhandled(); }
    virtual R visit_double(double& value) { return unhandled(); }
    virtual R visit_date(engine_date_t& value) { return unhandled(); }
    virtual R visit_datetime(engine_time_t& value) { return unhandled(); }
    virtual R visit_timedelta(engine_time_delta_t& value) { return unhandled(); }
    virtual R visit_string(std::string& value) { return unhandled(); }
    virtual R visit_object(nb::object& value) { return unhandled(); }

    // Called for composite types
    virtual R visit_bundle(ValueView bundle, const TypeMeta* schema) { return unhandled(); }
    virtual R visit_list(ValueView list, const TypeMeta* schema) { return unhandled(); }
    virtual R visit_set(ValueView set, const TypeMeta* schema) { return unhandled(); }
    virtual R visit_map(ValueView map, const TypeMeta* schema) { return unhandled(); }
    virtual R visit_cyclic_buffer(ValueView buf, const TypeMeta* schema) { return unhandled(); }
    virtual R visit_queue(ValueView queue, const TypeMeta* schema) { return unhandled(); }

protected:
    virtual R unhandled() {
        if constexpr (std::is_void_v<R>) return;
        else throw std::runtime_error("Unhandled type in visitor");
    }
};

// Visit dispatch function
template<typename R, typename Visitor>
R visit(ValueView view, Visitor&& visitor) {
    const TypeMeta* schema = view.schema();
    switch (schema->kind) {
        case TypeKind::Scalar:
            return dispatch_scalar<R>(view, schema, std::forward<Visitor>(visitor));
        case TypeKind::Bundle:
            return visitor.visit_bundle(view, schema);
        case TypeKind::List:
            return visitor.visit_list(view, schema);
        case TypeKind::Set:
            return visitor.visit_set(view, schema);
        case TypeKind::Map:
            return visitor.visit_map(view, schema);
        case TypeKind::CyclicBuffer:
            return visitor.visit_cyclic_buffer(view, schema);
        case TypeKind::Queue:
            return visitor.visit_queue(view, schema);
        default:
            throw std::runtime_error("Unknown type kind");
    }
}

} // namespace hgraph::value
```

### 8.3 Nested Structure Visiting

For recursive visitation of nested structures (bundles containing bundles, lists of bundles, etc.):

```cpp
namespace hgraph::value {

// Recursive visitor that descends into nested structures
template<typename LeafVisitor>
class DeepVisitor : public ValueVisitor<void> {
public:
    explicit DeepVisitor(LeafVisitor leaf) : _leaf(std::move(leaf)) {}

    // Scalars are leaf nodes - delegate to leaf visitor
    void visit_bool(bool& v) override { _leaf(v); }
    void visit_int64(int64_t& v) override { _leaf(v); }
    void visit_double(double& v) override { _leaf(v); }
    void visit_date(engine_date_t& v) override { _leaf(v); }
    void visit_datetime(engine_time_t& v) override { _leaf(v); }
    void visit_timedelta(engine_time_delta_t& v) override { _leaf(v); }
    void visit_string(std::string& v) override { _leaf(v); }

    // Composites recurse
    void visit_bundle(ValueView bundle, const TypeMeta* schema) override {
        for (size_t i = 0; i < schema->field_count; ++i) {
            visit(bundle.get_at(i), *this);
        }
    }

    void visit_list(ValueView list, const TypeMeta* schema) override {
        size_t n = schema->ops->size(list.data(), schema);
        for (size_t i = 0; i < n; ++i) {
            visit(list.get_at(i), *this);
        }
    }

    void visit_cyclic_buffer(ValueView buf, const TypeMeta* schema) override {
        size_t n = schema->ops->size(buf.data(), schema);
        for (size_t i = 0; i < n; ++i) {
            visit(buf.get_at(i), *this);
        }
    }

    void visit_queue(ValueView queue, const TypeMeta* schema) override {
        size_t n = schema->ops->size(queue.data(), schema);
        for (size_t i = 0; i < n; ++i) {
            visit(queue.get_at(i), *this);
        }
    }

private:
    LeafVisitor _leaf;
};

// Helper to create deep visitor
template<typename F>
auto make_deep_visitor(F&& leaf_func) {
    return DeepVisitor<std::decay_t<F>>(std::forward<F>(leaf_func));
}

} // namespace hgraph::value
```

### 8.4 Cursor-Based Iteration (Flux-Inspired)

Following Flux's cursor model, iteration separates the sequence from the position:

```cpp
namespace hgraph::value {

// Cursor for iterating over composite values
struct ValueCursor {
    size_t index{0};
};

// Sequence operations (Flux-style)
struct IterableOps {
    // Get first cursor position
    static ValueCursor first(ConstValueView seq) {
        return ValueCursor{0};
    }

    // Check if cursor is at end
    static bool is_last(ConstValueView seq, ValueCursor cur) {
        return cur.index >= seq.size();
    }

    // Advance cursor
    static void inc(ConstValueView seq, ValueCursor& cur) {
        ++cur.index;
    }

    // Read element at cursor
    static ConstValueView read_at(ConstValueView seq, ValueCursor cur) {
        return seq.get_at(cur.index);
    }

    // For mutable access
    static ValueView read_at(ValueView seq, ValueCursor cur) {
        return seq.get_at(cur.index);
    }
};

// Range-based for loop support
class ValueIterator {
public:
    using value_type = ConstValueView;
    using difference_type = std::ptrdiff_t;

    ValueIterator(ConstValueView seq, ValueCursor cur)
        : _seq(seq), _cursor(cur) {}

    ConstValueView operator*() const {
        return IterableOps::read_at(_seq, _cursor);
    }

    ValueIterator& operator++() {
        IterableOps::inc(_seq, _cursor);
        return *this;
    }

    bool operator==(const ValueIterator& other) const {
        return _cursor.index == other._cursor.index;
    }

    bool operator!=(const ValueIterator& other) const {
        return !(*this == other);
    }

private:
    ConstValueView _seq;
    ValueCursor _cursor;
};

// Enable range-based for
inline ValueIterator begin(ConstValueView v) {
    return ValueIterator(v, IterableOps::first(v));
}

inline ValueIterator end(ConstValueView v) {
    return ValueIterator(v, ValueCursor{v.size()});
}

} // namespace hgraph::value
```

### 8.5 Callable-Based Visiting (ddvisitor-Inspired)

For ad-hoc visitation without defining visitor classes:

```cpp
namespace hgraph::value {

// Visit with overloaded lambdas
template<typename... Fs>
struct Overloaded : Fs... {
    using Fs::operator()...;
};
template<typename... Fs> Overloaded(Fs...) -> Overloaded<Fs...>;

// Apply overloaded callable to value
template<typename... Fs>
auto visit_with(ValueView view, Fs&&... handlers) {
    auto visitor = Overloaded{std::forward<Fs>(handlers)...};

    switch (view.schema()->kind) {
        case TypeKind::Scalar:
            // Dispatch to appropriate handler based on scalar type
            if (view.is_scalar_type<int64_t>())
                return visitor(view.as<int64_t>());
            if (view.is_scalar_type<double>())
                return visitor(view.as<double>());
            if (view.is_scalar_type<bool>())
                return visitor(view.as<bool>());
            // ... etc
            break;
        case TypeKind::Bundle:
            return visitor(view, view.schema());  // Pass as bundle
        // ... etc
    }
}

} // namespace hgraph::value
```

**Usage Example:**

```cpp
Value v = /* some value */;
visit_with(v.view(),
    [](int64_t& i) { std::cout << "int: " << i << "\n"; },
    [](double& d) { std::cout << "double: " << d << "\n"; },
    [](auto& x) { std::cout << "other type\n"; }  // Catch-all
);
```

### 8.6 Path-Based Access

For accessing deeply nested values by path:

```cpp
namespace hgraph::value {

// Path element: either field name or index
struct PathElement {
    std::variant<std::string, size_t> element;

    static PathElement field(std::string name) { return {std::move(name)}; }
    static PathElement index(size_t idx) { return {idx}; }
};

using ValuePath = std::vector<PathElement>;

// Navigate to nested value
ConstValueView navigate(ConstValueView root, const ValuePath& path) {
    ConstValueView current = root;
    for (const auto& elem : path) {
        if (std::holds_alternative<std::string>(elem.element)) {
            current = current.get_field(std::get<std::string>(elem.element).c_str());
        } else {
            current = current.get_at(std::get<size_t>(elem.element));
        }
    }
    return current;
}

// Parse path string: "bundle.field[0].nested"
ValuePath parse_path(std::string_view path_str);

} // namespace hgraph::value
```

---

## 9. Memory Layout

### 9.1 Value Object Size

With EnTT `basic_any<24>`:

```
Value object: ~48 bytes
├── EnttAny _storage
│   ├── buffer[24]     <- Inline data for SBO types
│   ├── type_info      <- 8 bytes
│   └── vtable*        <- 8 bytes
└── TypeMeta* _schema  <- 8 bytes
```

### 9.2 Bundle Field Layout

Fields are laid out contiguously with proper alignment. Each field has both a name and an index
for dual access modes:

```cpp
struct BundleFieldInfo {
    const char* name;        // Field name for name-based access
    size_t index;            // Field position (0-based) for index-based access
    size_t offset;           // Byte offset from bundle start
    const TypeMeta* type;    // Field type schema
};

// Field order is significant - the order fields are added to the schema
// determines their index. Fields can be accessed by BOTH name AND index.
//
// Example: struct { int x; double y; bool z; }
//   Field "x": index=0, offset=0
//   Field "y": index=1, offset=8 (after padding)
//   Field "z": index=2, offset=16
// Layout: [x:4][pad:4][y:8][z:1][pad:7] = 24 bytes
```

### 9.3 Cache Line Considerations

- x86-64 cache line: 64 bytes
- Value object (48 bytes) fits in single cache line
- Bundle fields should be ordered by access frequency
- Consider `[[gnu::packed]]` for network/file formats only

### 9.4 Arrow Format Compatibility (Future)

For columnar processing, consider Arrow-compatible memory layout:
- Validity bitmap for nullable values
- Offset arrays for variable-length types
- Data arrays with natural alignment

**Reference**: https://arrow.apache.org/docs/format/Columnar.html

---

## 10. Implementation References

### 10.1 EnTT Prototype (Recommended)

| Component | File | Lines | What to Copy |
|-----------|------|-------|--------------|
| Storage type | `prototype-003-entt/include/entt_value.h` | 43 | `using EnttAny = entt::basic_any<24>` |
| Value class | `prototype-003-entt/include/entt_value.h` | 191-402 | Full class structure |
| View classes | `prototype-003-entt/include/entt_value.h` | 51-180 | ConstValueView, ValueView |
| TypeMeta | `prototype-003-entt/include/type_meta_stub.h` | All | TypeMeta/TypeOps interface |
| Tests | `prototype-003-entt/tests/test_standalone.cpp` | All | 35 unit tests |

### 10.2 Custom SBO Fallback

| Component | File | Lines | What to Copy |
|-----------|------|-------|--------------|
| SboStorage | `prototype-001-sbo-value/include/sbo_storage.h` | 49-233 | Union storage pattern |
| fits_inline | `prototype-001-sbo-value/include/sbo_storage.h` | 217-219 | Compile-time size check |

### 10.3 CMake Integration

```cmake
# From value-redesign-final.md:379-393
include(FetchContent)
FetchContent_Declare(
    entt
    GIT_REPOSITORY https://github.com/skypjack/entt.git
    GIT_TAG v3.13.2
)
FetchContent_MakeAvailable(entt)
target_link_libraries(hgraph PRIVATE EnTT::EnTT)
```

### 10.4 Full Path References

```
../hgraph/.claude/agents/value-rewrite/
├── output/
│   └── value-redesign-final.md          <- EnTT recommendation, CMake setup
├── prototypes/
│   ├── prototype-001-sbo-value/
│   │   └── include/sbo_storage.h        <- Custom SBO implementation
│   └── prototype-003-entt/
│       ├── include/entt_value.h         <- EnTT-based Value class
│       ├── include/type_meta_stub.h     <- TypeMeta interface
│       └── tests/test_standalone.cpp    <- Unit tests
└── state/
    ├── architecture-requirements.md     <- Full requirements
    └── research-findings.md             <- Technology research
```

---

## 11. Testing Strategy

### 11.1 Unit Test Categories

| Category | Tests | Coverage |
|----------|-------|----------|
| TypeMeta construction | 5 | Scalar, Bundle, List, Set, Map, CyclicBuffer, Queue |
| Value construction | 6 | Default, scalar, composite |
| `as<T>()` access | 7 | Read, write, type mismatch |
| View creation | 4 | Value->View, const correctness |
| Copy/move semantics | 4 | Value copying, move optimization |
| Comparison ops | 4 | equals, less_than, hash |
| Composite navigation | 6 | Bundle fields, List indexing |
| Python interop | 4 | to_python, from_python |
| CyclicBuffer | 28 | Creation, push, eviction, iteration, numpy |
| Queue | TBD | Creation, push, pop, iteration |

### 11.2 Test File Location

```
hgraph_unit_tests/_types/_value/
├── test_type_meta.py
├── test_value.py
├── test_view.py
├── test_bundle.py
├── test_list.py
├── test_set.py
├── test_map.py
├── test_value_cyclic_buffer.py
└── test_value_queue.py
```

### 11.3 Example Test Cases

```python
# test_value.py
def test_scalar_value_construction():
    """Value can hold scalar types."""
    v = Value(42)
    assert v.valid()
    assert v.as_int() == 42

def test_as_type_mismatch_raises():
    """checked_as raises on type mismatch."""
    v = Value(42)  # int
    with pytest.raises(RuntimeError, match="type mismatch"):
        v.checked_as_double()

def test_bundle_field_access():
    """Bundle values support field access."""
    schema = BundleType([("x", INT), ("y", DOUBLE)])
    v = Value(schema)
    v.set_field("x", 10)
    v.set_field("y", 3.14)
    assert v.get_field("x").as_int() == 10
    assert v.get_field("y").as_double() == 3.14
```

---

## 12. Future Work

### 12.1 TSValue Integration

The Value system will be extended with modification tracking for time-series:

```cpp
class TSValue : public Value {
    ModificationTracker _tracker;
    ObserverStorage* _observers{nullptr};  // Lazy allocation
};
```

### 12.2 DynamicList

Variable-length lists with growth semantics:

```cpp
class DynamicList {
    std::vector<Value> _elements;
    const TypeMeta* _element_type;
};
```

### 12.3 Python Buffer Protocol

For numpy interop, implement Python buffer protocol on BufferCompatible types.

### 12.4 Arrow Integration

Native Arrow format support for zero-copy data exchange.

---

## 13. Extension Mechanism

The extension mechanism provides **zero-overhead composition** of value behaviors using compile-time
dispatch. Two complementary patterns are supported:

1. **Policy-Based** - Simple single-concern extensions via template parameter
2. **CRTP Mixin** - Flexible multi-extension compositions via template chaining

### 13.1 Design Principles

| Principle | Description |
|-----------|-------------|
| **Zero Overhead** | `if constexpr` eliminates unused code paths at compile time |
| **Same API** | `v.to_python()` works identically regardless of extensions |
| **Composition over Inheritance** | Extensions are template parameters, not base classes |
| **Gradual Complexity** | Simple cases stay simple; complex cases are flexible |

### 13.2 Policy-Based Extensions (Simple Cases)

For single-concern extensions, use a template policy parameter:

```cpp
namespace hgraph::value {

// Policy tag types (empty - zero size via EBO)
struct NoCache {};
struct WithPythonCache {};

// Policy traits - detect capabilities at compile time
template<typename Policy>
struct policy_traits {
    static constexpr bool has_python_cache = false;
    static constexpr bool has_modification_tracking = false;
};

template<>
struct policy_traits<WithPythonCache> {
    static constexpr bool has_python_cache = true;
    static constexpr bool has_modification_tracking = false;
};

// Conditional storage - zero size when not needed (EBO)
template<typename Policy, typename = void>
struct PolicyStorage {};

template<typename Policy>
struct PolicyStorage<Policy, std::enable_if_t<policy_traits<Policy>::has_python_cache>> {
    mutable std::optional<nb::object> _cached_python;

    void invalidate_cache() { _cached_python = std::nullopt; }
    bool has_cache() const { return _cached_python.has_value(); }
};

} // namespace hgraph::value
```

### 13.3 Value Template with Policies

```cpp
namespace hgraph::value {

template<typename Policy = NoCache>
class Value : private PolicyStorage<Policy> {
public:
    // ===== Constructors (unchanged) =====
    Value() = default;
    explicit Value(const TypeMeta* schema);
    template<typename T> explicit Value(const T& val);

    // ===== API is identical regardless of Policy =====

    nb::object to_python() const {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            if (this->_cached_python) {
                return *this->_cached_python;
            }
            auto result = do_to_python();
            this->_cached_python = result;
            return result;
        } else {
            return do_to_python();
        }
    }

    void from_python(const nb::object& src) {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        do_from_python(src);
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->_cached_python = src;
        }
    }

    [[nodiscard]] ValueView view() {
        if constexpr (policy_traits<Policy>::has_python_cache) {
            this->invalidate_cache();
        }
        return ValueView(data(), _schema);
    }

    // ... rest of API unchanged ...

private:
    nb::object do_to_python() const {
        return _schema->ops->to_python(data(), _schema);
    }

    void do_from_python(const nb::object& src) {
        _schema->ops->from_python(data(), src, _schema);
    }

    ValueStorage _storage;
    const TypeMeta* _schema{nullptr};
};

// Type aliases for convenience
using PlainValue = Value<NoCache>;
using CachedValue = Value<WithPythonCache>;

} // namespace hgraph::value
```

### 13.4 CRTP Mixin Extensions (Complex Cases)

For multiple extensions or custom behavior, use CRTP mixin chaining:

```cpp
namespace hgraph::value {

// Core value data and operations
class ValueCore {
public:
    void* data();
    const void* data() const;
    const TypeMeta* schema() const;
protected:
    ValueStorage _storage;
    const TypeMeta* _schema{nullptr};
};

// Base operations via CRTP
template<typename Derived>
class ValueOps : public ValueCore {
public:
    nb::object to_python() const {
        return static_cast<const Derived*>(this)->impl_to_python();
    }

    void from_python(const nb::object& src) {
        static_cast<Derived*>(this)->impl_from_python(src);
    }

protected:
    nb::object base_to_python() const {
        return _schema->ops->to_python(data(), _schema);
    }

    void base_from_python(const nb::object& src) {
        _schema->ops->from_python(data(), src, _schema);
    }
};

// No-op extension (default)
template<typename Base>
class NoExtension : public Base {
public:
    using Base::Base;
protected:
    nb::object impl_to_python() const { return this->base_to_python(); }
    void impl_from_python(const nb::object& src) { this->base_from_python(src); }
};

// Python cache mixin
template<typename Base>
class WithCache : public Base {
public:
    using Base::Base;

protected:
    nb::object impl_to_python() const {
        if (_cache) return *_cache;
        _cache = this->base_to_python();
        return *_cache;
    }

    void impl_from_python(const nb::object& src) {
        invalidate();
        this->base_from_python(src);
        _cache = src;
    }

public:
    void invalidate() { _cache = std::nullopt; }

private:
    mutable std::optional<nb::object> _cache;
};

// Modification tracking mixin
template<typename Base>
class WithModTracking : public Base {
public:
    using Base::Base;

    void impl_from_python(const nb::object& src) {
        Base::impl_from_python(src);
        notify_modified();
    }

    void on_modified(std::function<void()> callback) {
        _callbacks.push_back(std::move(callback));
    }

private:
    void notify_modified() {
        for (auto& cb : _callbacks) cb();
    }
    std::vector<std::function<void()>> _callbacks;
};

// Compose via nesting (read right-to-left)
using Value = NoExtension<ValueOps<Value>>;
using CachedValue = WithCache<ValueOps<CachedValue>>;
using TSValue = WithModTracking<WithCache<ValueOps<TSValue>>>;

} // namespace hgraph::value
```

### 13.5 Zero-Overhead Verification

```cpp
// No extension: same size as base
static_assert(sizeof(Value<NoCache>) == sizeof(ValueStorage) + sizeof(TypeMeta*));

// With cache: adds only the optional
static_assert(sizeof(Value<WithPythonCache>) ==
              sizeof(ValueStorage) + sizeof(TypeMeta*) + sizeof(std::optional<nb::object>));
```

### 13.6 Usage Examples

**Simple Policy-Based:**
```cpp
// Default - no overhead
Value<> v1(123456789);
v1.to_python();  // Direct call

// With caching - same API
// Note: Use large integers (>256) to avoid Python's small integer cache
Value<WithPythonCache> v2(123456789);
v2.to_python();  // First: convert + cache
v2.to_python();  // Second: return cached

// Using type alias
CachedValue v3(123456789);
v3.to_python();
```

**CRTP Mixin Chaining:**
```cpp
// Multiple extensions (read right-to-left)
using TSValue = WithModTracking<WithCache<ValueOps<TSValue>>>;

TSValue v(123456789);
v.to_python();  // Uses cache
v.on_modified([]{ std::cout << "Value changed!\n"; });
v.from_python(nb::int_(987654321));  // Invalidates cache, triggers callback
```

### 13.7 When to Use Each Pattern

| Use Case | Pattern | Example |
|----------|---------|---------|
| Single built-in extension | Policy | `Value<WithPythonCache>` |
| Multiple extensions | CRTP Mixin | `WithMod<WithCache<...>>` |
| Custom behavior/hooks | CRTP Mixin | `WithValidation<ValueOps<...>>` |
| No extensions | Either | `Value<>` or `PlainValue` |

---

## Appendix A: Comparison with Alternatives

### A.1 Why EnTT over Microsoft Proxy?

| Feature | EnTT | MS Proxy |
|---------|------|----------|
| `data()` access | Yes | No |
| SBO built-in | Yes | No |
| Maturity | High (Minecraft) | Medium |
| C++ standard | C++17 | C++20 |

MS Proxy cannot provide `as<T>()` because it doesn't expose raw storage pointers.

**Reference**: `../hgraph/.claude/agents/value-rewrite/output/value-redesign-final.md:598-656`

### A.2 Why EnTT over std::any?

| Feature | EnTT basic_any | std::any |
|---------|----------------|----------|
| SBO size | Configurable | Implementation-defined |
| `data()` access | Yes | No |
| Type hash | Compile-time | Runtime (typeid) |

---

## Appendix B: Quick Reference

### B.1 Value Creation

```cpp
// Scalar (use large integers >256 for cache identity tests)
Value v1(123456789);
Value v2(3.14);
Value v3(std::string("hello"));

// From schema
Value v4(bundle_schema);
v4.view().set_field("x", Value(10));

// Copy
Value v5 = Value::copy(v1);
```

### B.2 Type Access

```cpp
int& x = v1.as<int>();              // Debug assert
int* p = v1.try_as<int>();          // nullptr if wrong type
int& y = v1.checked_as<int>();      // Throws on mismatch
```

### B.3 View Usage

```cpp
ConstValueView cv = v1.const_view();
ValueView mv = v1.view();

mv.as<int>() = 100;  // Modifies v1
assert(cv.as<int>() == 100);
```

### B.4 Cloning Values

```cpp
// Clone from view to create owning Value
ConstValueView cv = some_value.const_view();
Value copy = cv.clone();

// Alternative: constructor from view
Value copy2(cv);
```

---

**End of Document**
