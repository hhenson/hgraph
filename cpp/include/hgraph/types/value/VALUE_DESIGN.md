# hgraph::value Type System - Design Document

## Overview

The `hgraph::value` type system provides a type-erased, metadata-driven approach to representing and manipulating values in C++. It enables runtime construction of complex types (scalars, bundles, lists, sets, dictionaries) and provides type-safe access through schema validation.

## Design Goals

1. **Type Erasure with Safety**: Store any type's data without templates at runtime, while maintaining type safety through schema validation.

2. **Composability**: Build complex nested types from simpler ones (bundles containing lists of bundles, etc.).

3. **Zero-Copy Navigation**: Navigate into nested structures without copying data.

4. **Python Interoperability**: Support buffer protocol and eventual Python bindings.

5. **Performance**: Minimize overhead for type-erased operations through function pointers and direct memory access.

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                        TypeMeta                             │
│  (size, alignment, flags, kind, ops, type_info, name)       │
└─────────────────┬───────────────────────────────────────────┘
                  │
    ┌─────────────┼─────────────┬─────────────┬───────────────┐
    │             │             │             │               │
    ▼             ▼             ▼             ▼               ▼
┌────────┐  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
│ Scalar │  │BundleType- │ │ ListType-  │ │ SetType-   │ │ DictType-  │
│TypeMeta│  │   Meta     │ │   Meta     │ │   Meta     │ │   Meta     │
└────────┘  └────────────┘ └────────────┘ └────────────┘ └────────────┘
```

### TypeMeta

The central type descriptor containing:

- `size`: Storage size in bytes
- `alignment`: Required memory alignment
- `flags`: Type capabilities (hashable, comparable, etc.)
- `kind`: Type classification (Scalar, Bundle, List, Set, Dict)
- `ops`: Pointer to TypeOps vtable
- `type_info`: Optional C++ RTTI for debugging
- `name`: Optional human-readable name

### TypeOps

Function pointer vtable for type-erased operations:

```cpp
struct TypeOps {
    void (*construct)(void* dest, const TypeMeta* meta);
    void (*destruct)(void* dest, const TypeMeta* meta);
    void (*copy_construct)(void* dest, const void* src, const TypeMeta* meta);
    void (*move_construct)(void* dest, void* src, const TypeMeta* meta);
    void (*copy_assign)(void* dest, const void* src, const TypeMeta* meta);
    void (*move_assign)(void* dest, void* src, const TypeMeta* meta);
    bool (*equals)(const void* a, const void* b, const TypeMeta* meta);
    bool (*less_than)(const void* a, const void* b, const TypeMeta* meta);
    size_t (*hash)(const void* v, const TypeMeta* meta);
    void* (*to_python)(const void* v, const TypeMeta* meta);
    void (*from_python)(void* dest, void* py_obj, const TypeMeta* meta);
};
```

### TypeFlags

Bitfield describing type properties:

- `TriviallyConstructible`: Can default-construct without initialization
- `TriviallyDestructible`: No destructor needed
- `TriviallyCopyable`: Can memcpy instead of copy-construct
- `BufferCompatible`: Can expose via buffer protocol (e.g., numpy)
- `Hashable`: Has valid hash function
- `Comparable`: Supports < and ==
- `Equatable`: Supports ==

## Type Kinds

### Scalar Types

Primitive types (int, double, bool, etc.) with statically generated metadata:

```cpp
const TypeMeta* int_meta = scalar_type_meta<int>();
```

Generated at compile time using `ScalarTypeMeta<T>` template.

### Bundle Types (Structs)

Named field collections with computed layout:

```cpp
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");
```

Memory layout follows C struct rules with proper alignment padding.

### List Types (Fixed Arrays)

Contiguous arrays with fixed element count:

```cpp
auto list_meta = ListTypeBuilder()
    .element<double>()
    .count(10)
    .build("DoubleArray10");
```

Buffer-compatible for numpy interop when element type is trivial.

### Set Types

Dynamic hash sets with type-erased elements:

```cpp
auto set_meta = SetTypeBuilder()
    .element<int>()
    .build("IntSet");
```

Uses `SetStorage` class internally (vector + index set).

### Dict Types

Dynamic hash maps with type-erased keys and values:

```cpp
auto dict_meta = DictTypeBuilder()
    .key<int>()
    .value<double>()
    .build("IntDoubleMap");
```

Uses `DictStorage` class internally (parallel vectors + index list).

## Value Access Pattern

Three-tier access pattern for safety and flexibility:

### Value (Owner)

Owns storage and schema reference. Responsible for lifetime management.

```cpp
Value val(point_meta);
val.view().field("x").as<int>() = 10;
```

### ValueView (Mutable View)

Non-owning mutable view with full type information. Enables navigation.

```cpp
ValueView view = val.view();
ValueView x_view = view.field("x");
x_view.as<int>() = 42;
```

### ConstValueView (Const View)

Non-owning const view for read-only access.

```cpp
ConstValueView cv = val.const_view();
int x = cv.field("x").as<int>();
```

## Type Checking

### Schema Pointer Comparison

Type identity is determined by pointer equality:

```cpp
bool is_correct = val.is_type(point_meta);  // O(1)
```

This is intentional - types with identical structure but different schemas are considered different (nominal typing, not structural).

### Safe Access Methods

Three levels of type-safe access:

1. **`as<T>()`**: Debug assertion, zero overhead in release
2. **`try_as<T>()`**: Returns nullptr on mismatch
3. **`checked_as<T>()`**: Throws on mismatch

```cpp
// Best performance, asserts in debug
int& x = view.as<int>();

// Safe, check return value
if (int* p = view.try_as<int>()) {
    use(*p);
}

// Safe, exception on error
try {
    int& x = view.checked_as<int>();
} catch (std::runtime_error&) { }
```

## Type Registry

Central registry for named type lookup:

```cpp
TypeRegistry registry;

// Register type (takes ownership)
registry.register_type("Point", std::move(point_meta));

// Lookup
const TypeMeta* type = registry.get("Point");
const TypeMeta* type = registry.require("Point");  // throws if missing
```

Built-in scalars are pre-registered:
- `bool`, `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- Aliases: `int`, `long`, `float`, `double`, `size_t`

## Memory Layout

### Bundle Layout

Fields are laid out with proper alignment:

```cpp
struct Example {
    int8_t a;      // offset 0
    // 7 bytes padding
    double b;      // offset 8
    int32_t c;     // offset 16
    // 4 bytes padding
};                 // size 24, align 8
```

### Dynamic Collections

Sets and dicts use dynamic storage classes (`SetStorage`, `DictStorage`) that manage their own memory.

#### Hash Table Implementation

`SetStorage` and `DictStorage` use `ankerl::unordered_dense` (robin-hood hashing with backward shift deletion) for O(1) operations. The implementation stores:

- **Element data**: Contiguous `std::vector<char>` for cache-efficient storage
- **Index set**: `ankerl::unordered_dense::set<size_t>` mapping to element indices
- **Transparent functors**: Custom hash/equal functors enabling heterogeneous lookup by `const void*`

```
┌─────────────────────────────────────────────────────────┐
│  SetStorage                                             │
├─────────────────────────────────────────────────────────┤
│  _elements: [elem0|elem1|elem2|elem3|...]  (contiguous) │
│  _index_set: {0, 2, 3, ...}  (robin-hood hash set)      │
│                                                         │
│  Lookup: hash(key) → find in _index_set → _elements[idx]│
└─────────────────────────────────────────────────────────┘
```

#### Iteration Order

**Current behavior**: Iteration order is NOT guaranteed to be insertion order. The `ankerl::unordered_dense` library uses backward shift deletion, which swaps the deleted element with the last element to avoid shifting. This breaks insertion order after any deletion.

**If insertion order is needed**, the implementation can be modified:

1. Add an `_active` bitmap: `std::vector<bool> _active`
2. Set `_active[idx] = true` on insert, `_active[idx] = false` on remove
3. Change iteration to walk indices 0 → `_entry_count` sequentially, skipping where `!_active[idx]`

```cpp
// Insertion-order iterator (not currently implemented)
class InsertionOrderIterator {
    void advance_to_active() {
        while (_idx < _storage->_entry_count && !_storage->_active[_idx]) {
            ++_idx;
        }
    }
    // ...
};
```

**Tradeoffs**:
- Pro: Guaranteed insertion order iteration
- Con: Slightly slower iteration (must skip tombstones)
- Con: Additional memory for bitmap (~1 bit per element)
- Con: Memory fragmentation over time (tombstones accumulate)

A compaction strategy could be added to periodically rebuild the storage and reclaim tombstone space when fragmentation exceeds a threshold.

## Thread Safety

- Type metadata is immutable after construction (thread-safe to read)
- Values are not thread-safe (caller must synchronize)
- Registry registration is not thread-safe (register during init)

## Type Composability

The type system is fully composable - any type can be used as elements/keys/values with appropriate constraints:

| Container | Element/Key/Value Requirements |
|-----------|-------------------------------|
| **List** | Any type (no restrictions) |
| **Set** | Element must be Hashable + Equatable |
| **Dict Key** | Must be Hashable + Equatable |
| **Dict Value** | Any type (no restrictions) |

### Flag Propagation

Flags propagate correctly through composition:

- **Bundle**: Hashable if all fields are Hashable; Equatable if all fields are Equatable
- **List**: Inherits flags from element type
- **Set**: Always Hashable (uses XOR of element hashes for order-independence)
- **Dict**: Hashable only if both key AND value types are Hashable

### Composability Examples

```cpp
// 1. Set of Bundles
//    Bundles are hashable/equatable if all their fields are
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

auto point_set_meta = SetTypeBuilder()
    .element_type(point_meta.get())  // Bundle as set element
    .build("PointSet");

// 2. Dict with Bundle Keys and List Values
auto int_list_meta = ListTypeBuilder()
    .element<int>()
    .count(5)
    .build("IntList5");

auto point_to_list_meta = DictTypeBuilder()
    .key_type(point_meta.get())      // Bundle as dict key
    .value_type(int_list_meta.get()) // List as dict value
    .build("PointToListMap");

// 3. List of Sets
auto set_list_meta = ListTypeBuilder()
    .element_type(point_set_meta.get())  // Set as list element
    .count(10)
    .build("PointSetList10");

// 4. Deeply Nested: Dict of String -> List of Set of Points
auto point_set_list_meta = ListTypeBuilder()
    .element_type(point_set_meta.get())
    .count(5)
    .build();

auto string_to_nested_meta = DictTypeBuilder()
    .key<int>()  // Using int as key (string not yet implemented)
    .value_type(point_set_list_meta.get())
    .build("NestedContainer");
```

## Future Extensions

1. **Python Bindings**: Implement `to_python`/`from_python` ops
2. **Ref Type**: Reference counting for shared values
3. **Variant Type**: Union-like discriminated types
4. **String Type**: Type-erased string with SSO
5. **Serialization**: Binary and JSON serialization

## File Structure

```
cpp/include/
├── ankerl/
│   ├── unordered_dense.h  # Robin-hood hash map/set (v4.8.1, MIT license)
│   └── stl.h              # STL includes for unordered_dense
└── hgraph/types/value/
    ├── all.h              # Convenience header (includes all)
    ├── type_meta.h        # Core TypeMeta, TypeOps, TypeFlags
    ├── scalar_type.h      # ScalarTypeMeta<T>, TypedValue
    ├── bundle_type.h      # BundleTypeMeta, BundleTypeBuilder
    ├── list_type.h        # ListTypeMeta, ListTypeBuilder
    ├── set_type.h         # SetTypeMeta, SetTypeBuilder, SetStorage
    ├── dict_type.h        # DictTypeMeta, DictTypeBuilder, DictStorage
    ├── type_registry.h    # TypeRegistry
    ├── value.h            # Value, ValueView, ConstValueView
    ├── VALUE_DESIGN.md    # This document
    └── VALUE_USER_GUIDE.md # User guide
```

## Testing

Tests are in `cpp/tests/`:
- `test_value.cpp`: Unit tests (54 test cases, 262 assertions)
- `value_examples.cpp`: Comprehensive examples
