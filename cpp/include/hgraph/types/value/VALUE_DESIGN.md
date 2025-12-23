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
    ┌─────────────┼─────────────┬─────────────┬─────────────┬───────────────┐
    │             │             │             │             │               │
    ▼             ▼             ▼             ▼             ▼               ▼
┌────────┐  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
│ Scalar │  │BundleType- │ │ ListType-  │ │ SetType-   │ │ DictType-  │ │ RefType-   │
│TypeMeta│  │   Meta     │ │   Meta     │ │   Meta     │ │   Meta     │ │   Meta     │
└────────┘  └────────────┘ └────────────┘ └────────────┘ └────────────┘ └────────────┘
```

### TypeMeta

The central type descriptor containing:

- `size`: Storage size in bytes
- `alignment`: Required memory alignment
- `flags`: Type capabilities (hashable, comparable, etc.)
- `kind`: Type classification (Scalar, Bundle, List, Set, Dict, Window, Ref)
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
    std::string (*to_string)(const void* v, const TypeMeta* meta);
    nb::object (*to_python)(const void* v, const TypeMeta* meta);
    bool (*from_python)(void* dest, const TypeMeta* meta, nb::handle py_obj);
};
```

### String Representation

All types implement `to_string` for logging and debugging. The `TypeMeta` provides a convenience wrapper:

```cpp
// Via TypeMeta
std::string s = meta->to_string_at(data_ptr);

// Via Value/ValueView
std::string s = value.to_string();
```

### Type Names (Schema Description)

All types implement `type_name` which returns a Python-style type description:

```cpp
// Via TypeMeta
std::string type_desc = meta->type_name_str();
```

Type name formats use Python naming conventions:

| TypeKind | Format | Example |
|----------|--------|---------|
| Scalar (bool) | `bool` | `bool` |
| Scalar (int*) | `int` | `int` |
| Scalar (float/double) | `float` | `float` |
| Scalar (string) | `str` | `str` |
| Scalar (other) | Uses `name` field | `datetime`, `date`, `timedelta` |
| Bundle (named) | Uses `name` field | `Point` |
| Bundle (anon) | `{field: type, ...}` | `{x: int, y: float}` |
| List | `Tuple[elem, Size[n]]` | `Tuple[int, Size[5]]` |
| Set | `Set[elem]` | `Set[int]` |
| Dict | `Dict[key, value]` | `Dict[str, float]` |
| Window (fixed) | `Window[elem, Size[n]]` | `Window[float, Size[10]]` |
| Window (time) | `Window[elem, timedelta[...]]` | `Window[float, timedelta[seconds=60]]` |
| Ref | `REF[target]` | `REF[int]` |

Type-specific value output formats:

| TypeKind | Format | Example |
|----------|--------|---------|
| Scalar (bool) | `true`/`false` | `true` |
| Scalar (int) | `<number>` | `42` |
| Scalar (float) | `<number>` | `3.14159` |
| Scalar (string) | `"<value>"` | `"hello"` |
| Bundle | `{field=val, ...}` | `{x=1, y=2.5}` |
| List | `[elem, ...]` | `[1, 2, 3]` |
| Set | `{elem, ...}` | `{1, 2, 3}` |
| Dict | `{key: val, ...}` | `{a: 1, b: 2}` |
| Window | `Window[size=N, newest=val]` | `Window[size=3, newest=42]` |
| Ref (empty) | `REF[empty]` | `REF[empty]` |
| Ref (bound) | `REF[bound: val]` | `REF[bound: 42]` |
| Ref (unbound) | `REF[unbound: N items]` | `REF[unbound: 3 items]` |

For time-series values, additional debug formatting is available:

```cpp
// Basic: just the value
ts_value.to_string();  // "42"

// Debug: with modification status
ts_value.to_debug_string(current_time);
// "TS[int64_t]@0x7fff5fbff8c0(value=\"42\", modified=true, last_modified=2025-01-01 12:00:00.000000)"

// TSView uses stored current_time
ts_view.to_debug_string();
// "TS[int64_t]@0x7fff5fbff8c0(value=\"42\", modified=true)"
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

`DictStorage` composes `SetStorage` for keys with parallel value storage. This
enables direct access to keys as a Set and shared modification tracking logic.
`DictTypeMeta` embeds a `SetTypeMeta` for the key set type.

### Window Types (Time-Series History)

Time-windowed collections of timestamped values. Two modes:

**Fixed-length (cyclic buffer):**
```cpp
auto window_meta = WindowTypeBuilder()
    .element<double>()
    .fixed_count(100)
    .build("DoubleWindow100");
```

Stores up to N entries in a cyclic buffer. Oldest entry overwritten when full.

**Variable-length (time-based queue):**
```cpp
auto window_meta = WindowTypeBuilder()
    .element<double>()
    .time_duration(std::chrono::minutes(5))
    .build("DoubleWindow5min");
```

Stores entries within a time window. Entries older than `current_time - duration` are expired.

Two specialized storage implementations:
- `CyclicWindowStorage`: Fixed-length cyclic buffer with `_head` and `_count`
- `QueueWindowStorage`: Variable-length queue with automatic eviction on push
- `WindowStorage`: Union wrapper that delegates to the appropriate implementation
- Values stored separately from timestamps
- `compact()` optimizes for reading (resets cyclic buffer to start at 0, removes expired entries)
- Atomic modification tracking (single timestamp, like Set)

### Ref Types (References)

Non-owning references to other values. REF types enable pointer-like semantics within the value system.

**Two structural forms:**

1. **Atomic refs** (REF[TS], REF[TSS], REF[TSW], REF[TSD]):
   - Always a single pointer (bound reference)
   - `item_count == 0`

2. **Composite refs** (REF[TSL], REF[TSB]):
   - Can be bound (single pointer) OR unbound (collection of references)
   - `item_count > 0` indicates potential unbound structure

```cpp
// Atomic ref - references a single int time-series
auto ref_int_meta = RefTypeBuilder()
    .value_type(int_meta)
    .build("RefInt");

// Composite ref - can reference a list or be unbound with 3 items
auto ref_list_meta = RefTypeBuilder()
    .value_type(list_meta)
    .item_count(3)
    .build("RefList3");
```

**Core Components:**

- `ValueRef`: Non-owning view containing `data*`, `tracker*`, and `schema*`
- `RefStorage`: Union type with three variants:
  - `EMPTY`: No reference (null)
  - `BOUND`: Single `ValueRef` pointing to a value
  - `UNBOUND`: `vector<RefStorage>` for composite refs
- `RefTypeMeta`: Extended TypeMeta with `value_type` and `item_count`

**RefStorage States:**

```
┌─────────────────────────────────────────────────────────────┐
│  RefStorage                                                  │
├─────────────────────────────────────────────────────────────┤
│  Kind::EMPTY   → No reference (is_valid() = false)          │
│  Kind::BOUND   → ValueRef { data*, tracker*, schema* }      │
│  Kind::UNBOUND → vector<RefStorage> (composite refs)         │
└─────────────────────────────────────────────────────────────┘
```

**Usage Example:**

```cpp
#include <hgraph/types/value/ref_type.h>
using namespace hgraph::value;

// Create ref type
const TypeMeta* int_meta = scalar_type_meta<int>();
auto ref_meta = RefTypeBuilder()
    .value_type(int_meta)
    .build("RefInt");

// Create ref value
Value ref_val(ref_meta.get());
ValueView rv = ref_val.view();

// Initially empty
assert(rv.ref_is_empty());

// Bind to target
int target = 42;
rv.ref_bind(ValueRef{&target, nullptr, int_meta});

// Access target
assert(rv.ref_is_bound());
assert(*static_cast<int*>(rv.ref_target()->data) == 42);

// Clear
rv.ref_clear();
assert(rv.ref_is_empty());
```

**Composite Ref Example:**

```cpp
// Composite ref with 3 items
auto ref_meta = RefTypeBuilder()
    .value_type(int_meta)
    .item_count(3)
    .build("RefList3");

Value ref_val(ref_meta.get());
ValueView rv = ref_val.view();

// Create unbound structure
rv.ref_make_unbound(3);

// Set individual items
int val1 = 10, val2 = 20, val3 = 30;
rv.ref_set_item(0, ValueRef{&val1, nullptr, int_meta});
rv.ref_set_item(1, ValueRef{&val2, nullptr, int_meta});
rv.ref_set_item(2, ValueRef{&val3, nullptr, int_meta});

// Navigate to items
auto item0 = rv.ref_item(0);
assert(item0.ref_is_bound());
```

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

#### Compaction Strategy

`SetStorage` and `DictStorage` include built-in compaction support to manage memory fragmentation:

```cpp
// Check fragmentation level (0.0 = no waste, 1.0 = all tombstones)
double ratio = storage.fragmentation_ratio();

// Compact when fragmentation exceeds threshold
if (ratio > 0.5) {
    storage.compact();  // Rebuilds storage, reclaims tombstones
}
```

**`fragmentation_ratio()`**: Returns the proportion of allocated slots that are tombstones (removed elements). Calculated as `(capacity - active_count) / capacity`.

**`compact()`**: Rebuilds the internal storage to eliminate tombstones:
1. Creates new contiguous element storage
2. Copies only active elements
3. Rebuilds the index set
4. Resets capacity to match active count

This is useful after bulk deletions or when memory efficiency is critical. The operation is O(n) where n is the number of active elements.

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

## Modification Tracking

The `ModificationTracker` provides a parallel data structure for tracking when value elements were last modified. This is essential for implementing time-series values where you need to know "was this modified at the current evaluation time?"

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 ModificationTrackerStorage                       │
│  (Owns tracking storage, allocates based on TypeKind)            │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ModificationTracker                            │
│  (Non-owning view, enables hierarchical propagation)             │
└─────────────────────────────────────────────────────────────────┘
```

### Storage Layout by Type

| Type | Storage | Notes |
|------|---------|-------|
| **Scalar** | Single `engine_time_t` | 8 bytes |
| **Bundle** | `[bundle_time][field0_time][field1_time]...` | Per-field tracking, accessible by index or name |
| **List** | `[list_time][elem0_time][elem1_time]...` | Per-element tracking with propagation |
| **Set** | `SetModificationStorage` | Per-element tracking + delta (added/removed) |
| **Dict** | `DictModificationStorage` | Composes `SetModificationStorage` for keys + value tracking |
| **Window** | Single `engine_time_t` | Atomic tracking |
| **Ref (atomic)** | Single `engine_time_t` | Single timestamp for ref binding changes |
| **Ref (composite)** | `[ref_time][item0_time][item1_time]...` | Per-item tracking with propagation |

**Bundle Field Order**: Field indices correspond to schema creation order in `BundleTypeBuilder`. Fields are accessible by both index and name:

```cpp
auto meta = BundleTypeBuilder()
    .add_field<int>("x")      // index 0
    .add_field<int>("y")      // index 1
    .add_field<double>("z")   // index 2
    .build();

// Both equivalent:
tracker.field(0).mark_modified(time);    // by index
tracker.field("x").mark_modified(time);  // by name
```

### Hierarchical Propagation

When a child element is marked modified, the modification propagates to the parent:

```cpp
// Bundle { x: int, y: int }
tracker.field("x").mark_modified(current_time);
// Result: field "x" is modified AND bundle is modified

// Nested bundles work recursively
// Outer { id: int, point: Inner { x: int, y: int } }
tracker.field("point").mark_modified(current_time);
// Result: point field modified → outer bundle modified
```

### Time Monotonicity

Modification times only move forward:

```cpp
tracker.mark_modified(make_time(200));  // Sets to 200
tracker.mark_modified(make_time(100));  // Ignored (earlier)
tracker.mark_modified(make_time(300));  // Updates to 300
```

### Usage Example

```cpp
#include <hgraph/types/value/modification_tracker.h>
using namespace hgraph::value;

// Create tracker for a bundle type
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

ModificationTrackerStorage storage(point_meta.get());
ModificationTracker tracker = storage.tracker();

engine_time_t current_time = /* from evaluation clock */;

// Mark field modified (propagates to bundle)
tracker.field("x").mark_modified(current_time);

// Query modification state
if (tracker.modified_at(current_time)) {
    // Bundle was modified at this time
}

if (tracker.field_modified_at(0, current_time)) {
    // Field "x" was modified
}

// Reset to unmodified
tracker.mark_invalid();
```

### Set vs Dict Tracking

- **Set**: Tracked atomically - single timestamp for structural changes (add/remove). Per-element tracking is not needed because sets will track added/removed elements separately for delta management.

- **Dict**: Tracks both structural changes (key add/remove) AND per-entry modifications (value changes on existing keys). This is needed because TSD (Time-Series Dict) must distinguish between "key was added" vs "existing key's value changed".

```cpp
// Dict tracking
tracker.mark_modified(current_time);           // Structural change (key added)
tracker.mark_dict_entry_modified(0, time);     // Entry 0 value changed
tracker.structurally_modified_at(time);        // Was a key added/removed?
tracker.dict_entry_modified_at(0, time);       // Was entry 0's value changed?
```

## Time-Series Value

The `TSValue` combines `Value` (data storage) and `ModificationTrackerStorage` (modification tracking) into a unified time-series container. This provides the foundation for implementing TS, TSB, TSL, TSS, TSD using the type-erased value system.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      TSValue                             │
│  ┌─────────────────┐  ┌──────────────────────────────────────┐  │
│  │     Value       │  │   ModificationTrackerStorage         │  │
│  │  (owns data)    │  │   (owns tracking)                    │  │
│  └─────────────────┘  └──────────────────────────────────────┘  │
│                                                                  │
│  Methods: value(), set_value(), modified_at(), mark_invalid()    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TSView                           │
│  (Non-owning view into TSValue)                          │
│  - Provides field()/element() navigation with auto-tracking      │
│  - Automatically marks modifications when values are set         │
└─────────────────────────────────────────────────────────────────┘
```

### TSValue (Owner)

The owning container that manages both value storage and modification tracking:

```cpp
class TSValue {
public:
    explicit TSValue(const TypeMeta* schema);

    // Schema access
    [[nodiscard]] const TypeMeta* schema() const;
    [[nodiscard]] TypeKind kind() const;
    [[nodiscard]] bool valid() const;

    // Value access (read-only)
    [[nodiscard]] ConstValueView value() const;

    // Modification state
    [[nodiscard]] bool modified_at(engine_time_t time) const;
    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] bool has_value() const;
    void mark_invalid();

    // Mutable access with tracking
    [[nodiscard]] TSView view(engine_time_t current_time);

    // Direct scalar access (convenience)
    template<typename T> void set_value(const T& val, engine_time_t time);
    template<typename T> [[nodiscard]] const T& as() const;
};
```

### TSView (Auto-Tracking View)

Unlike raw `ValueView`, this view automatically marks modifications when values are changed:

```cpp
class TSView {
public:
    // Navigation (returns sub-views that track to parent)
    [[nodiscard]] TSView field(size_t index);
    [[nodiscard]] TSView field(const std::string& name);
    [[nodiscard]] TSView element(size_t index);

    // Value access - auto-marks modified on set
    template<typename T> [[nodiscard]] T& as();
    template<typename T> void set(const T& val);

    // Modification queries
    [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const;
    [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const;

    // Set operations - atomic tracking
    template<typename T> bool add(const T& element);
    template<typename T> bool remove(const T& element);
    template<typename T> [[nodiscard]] bool contains(const T& element) const;

    // Dict operations - structural + entry tracking
    template<typename K, typename V> void insert(const K& key, const V& value);
    template<typename K> [[nodiscard]] bool dict_contains(const K& key) const;
    template<typename K> [[nodiscard]] ConstValueView dict_get(const K& key) const;
    template<typename K> bool dict_remove(const K& key);
};
```

### Type-Specific Behavior

| Type | Tracking Granularity | Auto-Propagation |
|------|---------------------|------------------|
| **Scalar** | Single value, single timestamp | N/A |
| **Bundle** | Per-field timestamps | Field → Bundle |
| **List** | Per-element timestamps | Element → List |
| **Set** | Atomic timestamp | N/A |
| **Dict** | Structural + per-entry | Entry → Dict |
| **Window** | Atomic timestamp | N/A |
| **Ref (atomic)** | Single timestamp | N/A |
| **Ref (composite)** | Per-item timestamps | Item → Ref |

### Usage Example

```cpp
#include <hgraph/types/value/time_series_value.h>
using namespace hgraph::value;

// Create a time-series bundle
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

TSValue ts_point(point_meta.get());

// Initial state
assert(!ts_point.modified_at(t1));
assert(!ts_point.has_value());

// Modify via view (auto-tracking)
auto view = ts_point.view(t1);
view.field("x").set(10);  // Marks field "x" AND bundle modified at t1
view.field("y").set(20);

// Check state
assert(ts_point.modified_at(t1));
assert(ts_point.value().field("x").as<int>() == 10);
assert(view.field_modified_at(0, t1));  // field "x" by index

// Next cycle - not modified at t2
assert(!ts_point.modified_at(t2));

// Invalidate value
ts_point.mark_invalid();
assert(!ts_point.has_value());
```

### Key Design Decisions

1. **View holds current_time**: The view needs the current evaluation time to mark modifications correctly. This is passed when creating the view via `view(current_time)`.

2. **Auto-tracking on set()**: Unlike raw `ValueView`, `TSView` automatically marks modifications when values are changed through `set()` or container operations.

3. **Hierarchical propagation**: Child modifications (field, element) automatically propagate to parent containers.

4. **Move-only ownership**: `TSValue` owns both storages and uses move-only semantics.

5. **Const safety**: Read-only operations (`value()`, `modified_at()`, etc.) are const-correct.

## Observer/Notification System

The `TSValue` includes an observer pattern for change notification. Observers can subscribe to any level of the value hierarchy and receive notifications when values are modified.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      TSValue                             │
│  ┌─────────────────┐  ┌──────────────────────────────────────┐  │
│  │     Value       │  │   ModificationTrackerStorage         │  │
│  │  (owns data)    │  │   (owns tracking)                    │  │
│  └─────────────────┘  └──────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │  std::unique_ptr<ObserverStorage> (lazy, nullptr until use)  ││
│  └──────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Design Goals

1. **Per-Level Subscription**: Subscribe at root, field, element, or dict entry level
2. **Lazy Allocation**: Zero overhead when observers not used (nullptr until first subscribe)
3. **Upward Propagation**: Child modifications notify all ancestor observers
4. **Unified Index-Based Children**: All container types use same index-based observer lookup

### ObserverStorage Class

```cpp
class ObserverStorage {
public:
    explicit ObserverStorage(const TypeMeta* meta);

    // Subscription at this level
    void subscribe(Notifiable* notifiable);
    void unsubscribe(Notifiable* notifiable);
    bool has_subscribers() const;

    // Child observer storage (unified for bundle/list/dict)
    ObserverStorage* child(size_t index);
    ObserverStorage* ensure_child(size_t index, const TypeMeta* child_meta = nullptr);

    // Notification with upward propagation
    void notify(engine_time_t time);
    void set_parent(ObserverStorage* parent);

private:
    const TypeMeta* _meta{nullptr};
    ObserverStorage* _parent{nullptr};
    std::unordered_set<Notifiable*> _subscribers;
    std::vector<std::unique_ptr<ObserverStorage>> _children;
};
```

### Notifiable Interface

```cpp
namespace hgraph::value {
    struct Notifiable {
        virtual ~Notifiable() = default;
        virtual void notify(engine_time_t time) = 0;
    };
}
```

### Parallel Structure Example

The observer storage mirrors the type structure. For a nested bundle:

```
Schema: Trade { symbol: string, orders: List<Order> }
        Order { price: double, qty: int }

Data Structure (Value)          Observer Structure (ObserverStorage)
═══════════════════════         ════════════════════════════════════

Trade                           ObserverStorage (for Trade)
├─ symbol: "AAPL"               ├─ subscribers: {nodeA}  ← subscribes to whole Trade
├─ orders: [...]                └─ children[1]: ObserverStorage (for orders)
                                    ├─ subscribers: {nodeB}  ← subscribes to orders list
                                    └─ children[0]: ObserverStorage (for orders[0])
                                        └─ subscribers: {nodeC}  ← subscribes to first order
```

### Notification Propagation

When a nested value changes, notifications propagate upward:

```
User modifies: orders[0].price = 99.5

Step 1: Notify orders[0] subscribers → nodeC.notify(time)
Step 2: Propagate to orders list    → nodeB.notify(time)
Step 3: Propagate to Trade root     → nodeA.notify(time)
```

### Dict Observer Storage

Dictionaries use entry indices from `DictStorage` for observer lookup:

```
Dict<string, Setting>
DictStorage internal: "theme"→idx:0, "font"→idx:1, "size"→idx:2

Dict                             ObserverStorage (for Dict)
├─ "theme" → Setting (idx:0)    ├─ subscribers: {structWatcher}  ← structural changes
├─ "font"  → Setting (idx:1)    └─ _children: vector<ObserverStorage*>
└─ "size"  → Setting (idx:2)        ├─ [0] → ObserverStorage { subscribers: {themeWatcher} }
                                    └─ [1] → ObserverStorage { subscribers: {fontWatcher} }
```

**Key → Observer Mapping**:
1. Look up key in DictStorage → get entry index via `find_index()`
2. Use that index in observer storage's `_children` vector
3. If entry is removed, its observer slot becomes orphaned

### Lazy Allocation Flow

```
Initial state:
  TSValue
  └─ _observers: nullptr  ← no heap allocation

After trade.subscribe(nodeA):
  TSValue
  └─ _observers ──→ ObserverStorage
                    ├─ subscribers: {nodeA}
                    └─ children: []  ← still lazy

After subscribing to a field:
  TSValue
  └─ _observers ──→ ObserverStorage
                    ├─ subscribers: {nodeA}
                    └─ children: [nullptr, ptr ──→ ObserverStorage
                                                   └─ subscribers: {nodeB}]
```

### TSValue Observer API

```cpp
class TSValue {
public:
    // Observer/subscription API (lazy allocation)
    void subscribe(Notifiable* notifiable);
    void unsubscribe(Notifiable* notifiable);
    [[nodiscard]] bool has_observers() const;
};
```

### Usage Example

```cpp
#include <hgraph/types/value/time_series_value.h>
using namespace hgraph::value;

// Custom observer
struct MyObserver : Notifiable {
    int count = 0;
    engine_time_t last_time{MIN_DT};

    void notify(engine_time_t time) override {
        ++count;
        last_time = time;
    }
};

// Create a time-series bundle
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

TSValue ts_point(point_meta.get());
MyObserver observer;

// Subscribe at root level
ts_point.subscribe(&observer);

// Modifications trigger notification
engine_time_t t1 = make_time(100);
ts_point.view(t1).field("x").set(42);

assert(observer.count == 1);
assert(observer.last_time == t1);

// Unsubscribe
ts_point.unsubscribe(&observer);
```

### Key Design Decisions

1. **Notifiable* Interface**: Simple interface in `hgraph::value` namespace to avoid external dependencies.

2. **Lazy Observer Storage**: `_observers` is `nullptr` until first `subscribe()` call - zero overhead when not used.

3. **Parent Fallback**: When navigating to a child view without explicit child observers, the parent observer is used so notifications still propagate.

4. **Unified Index-Based Children**: All container types (Bundle, List, Dict) use the same `_children` vector with index-based lookup.

5. **Entry Index for Dict**: Dict entries use `DictStorage::find_index()` to map keys to stable entry indices for observer lookup.

## Python Conversion Support

The `python_conversion.h` header provides full bidirectional conversion between C++ values and Python objects using nanobind. Python support is implemented via:

### Named Type Instances

Standard hgraph scalar types with Python conversion support:

| Function | C++ Type | Python Type |
|----------|----------|-------------|
| `bool_type()` | `bool` | `bool` |
| `int_type()` | `int64_t` | `int` |
| `float_type()` | `double` | `float` |
| `date_type()` | `engine_date_t` | `datetime.date` |
| `date_time_type()` | `engine_time_t` | `datetime.datetime` |
| `time_delta_type()` | `engine_time_delta_t` | `datetime.timedelta` |
| `object_type()` | `nb::object` | any Python object |

Helper function `scalar_type_by_name(name)` provides lookup by string name.

### Python-Enabled Type Builders

Composite type builders with Python conversion ops:
- `BundleTypeBuilderWithPython` - converts to/from Python `dict`
- `ListTypeBuilderWithPython` - converts to/from Python `list`
- `SetTypeBuilderWithPython` - converts to/from Python `set`
- `DictTypeBuilderWithPython` - converts to/from Python `dict`
- `WindowTypeBuilderWithPython` - converts to/from Python `list` of `(timestamp_ns, value)` tuples

### Conversion Functions

```cpp
// C++ to Python
nb::object value_to_python(const void* v, const TypeMeta* meta);

// Python to C++
void value_from_python(void* dest, nb::handle py_obj, const TypeMeta* meta);
```

### Custom Scalar Types

Use `scalar_type_meta_with_python<T>()` to get TypeMeta with Python support for any type that has a nanobind type caster.

## Schema-Driven Binding

The binding system (`bind.h`, `bound_value.h`, `deref_time_series_value.h`) enables automatic dereferencing of REF types during wiring.

### Schema Matching

`match_schemas()` compares input and output schemas to determine binding requirements:

| Match Result | Input Schema | Output Schema | Action |
|--------------|--------------|---------------|--------|
| `Peer` | `TS[X]` | `TS[X]` | Direct match, no transformation |
| `Deref` | `TS[X]` | `REF[TS[X]]` | Dereference the REF |
| `Composite` | `TSB[a:X,b:Y]` | `TSB[a:REF[X],b:Y]` | Per-field binding |
| `Mismatch` | - | - | Incompatible types |

### BoundValue

Result of binding, representing the relationship between input expectation and output provider:

```cpp
enum class BoundValueKind {
    Peer,       // Direct match: output type equals input type
    Deref,      // Output is REF[X], input expects X
    Composite   // TSB/TSL with per-field/element bindings
};

class BoundValue {
    // Factory methods
    static BoundValue make_peer(TSValue* source);
    static BoundValue make_deref(std::unique_ptr<DerefTSValue> deref, const TypeMeta* schema);
    static BoundValue make_composite(const TypeMeta* schema, std::vector<BoundValue> children);

    // Unified access
    ConstValueView value() const;
    bool modified_at(engine_time_t time) const;
    bool has_value() const;

    // Lifecycle
    void begin_evaluation(engine_time_t time);
    void end_evaluation();
};
```

### DerefTSValue

Wrapper that transparently dereferences REF values:

- Tracks current and previous targets (for delta computation)
- Unified modification tracking (ref changed OR underlying value modified)
- Lifecycle: `begin_evaluation()` → use → `end_evaluation()`

```cpp
DerefTSValue deref(ref_view, target_schema);

deref.begin_evaluation(current_time);
if (deref.modified_at(current_time)) {
    auto value = deref.target_value();
    if (deref.has_previous()) {
        auto prev = deref.previous_target();
        // Compute delta...
    }
}
deref.end_evaluation();
```

### Usage

```cpp
// During wiring
BoundValue binding = bind(input_schema, output_value, current_time);

// During evaluation
binding.begin_evaluation(time);
if (binding.modified_at(time)) {
    ConstValueView value = binding.value();
    // Process value...
}
binding.end_evaluation();
```

## Delta Tracking

The `bind.h` header provides delta computation for Set and Dict types, enabling efficient change detection between evaluation cycles.

### SetDelta

Tracks elements added to or removed from a set:

```cpp
struct SetDelta {
    std::vector<ConstTypedPtr> added;    // Elements in new but not old
    std::vector<ConstTypedPtr> removed;  // Elements in old but not new

    [[nodiscard]] bool empty() const;
    [[nodiscard]] size_t total_changes() const;
};

// Compute delta between two set values
SetDelta delta = compute_set_delta(old_set_view, new_set_view);

// Or compute full delta (treats old as defining removed, new as added)
SetDelta full = compute_set_full_delta(old_set_view, new_set_view);
```

### DictDelta

Tracks key-value pairs added, removed, or modified in a dictionary:

```cpp
struct DictDelta {
    std::vector<DictStorage::ConstKeyValuePair> added;     // New keys
    std::vector<DictStorage::ConstKeyValuePair> removed;   // Removed keys
    std::vector<DictStorage::ConstKeyValuePair> modified;  // Existing keys with changed values

    [[nodiscard]] bool empty() const;
    [[nodiscard]] size_t total_changes() const;
};

// Compute delta between two dict values
DictDelta delta = compute_dict_delta(old_dict_view, new_dict_view);

// Or compute full delta
DictDelta full = compute_dict_full_delta(old_dict_view, new_dict_view);
```

### Usage in Time-Series

Delta tracking is essential for TSS (Time-Series Set) and TSD (Time-Series Dict) to implement `delta_value` semantics:

```cpp
// In TSS evaluation
if (tss.modified_at(current_time)) {
    SetDelta delta = compute_set_delta(previous_value, current_value);
    for (const auto& elem : delta.added) {
        // Process newly added elements
    }
    for (const auto& elem : delta.removed) {
        // Process removed elements
    }
}
```

## Future Extensions

1. **Variant Type**: Union-like discriminated types
2. **String Type**: Type-erased string with SSO
3. **Serialization**: Binary and JSON serialization
4. **Through-Reference Tracking**: Track modifications through to the target value (REF[TS] → TS)

## File Structure

```
cpp/include/
├── ankerl/
│   ├── unordered_dense.h  # Robin-hood hash map/set (v4.8.1, MIT license)
│   └── stl.h              # STL includes for unordered_dense
└── hgraph/
    ├── util/
    │   └── date_time.h    # engine_time_t, MIN_DT, MAX_DT
    └── types/value/
        ├── all.h                      # Convenience header (includes all)
        ├── type_meta.h                # Core TypeMeta, TypeOps, TypeFlags
        ├── scalar_type.h              # ScalarTypeMeta<T>, TypedValue
        ├── bundle_type.h              # BundleTypeMeta, BundleTypeBuilder
        ├── list_type.h                # ListTypeMeta, ListTypeBuilder
        ├── set_type.h                 # SetTypeMeta, SetTypeBuilder, SetStorage
        ├── dict_type.h                # DictTypeMeta, DictTypeBuilder, DictStorage
        ├── window_type.h              # WindowTypeMeta, WindowTypeBuilder, CyclicWindowStorage, QueueWindowStorage
        ├── ref_type.h                 # RefTypeMeta, RefTypeBuilder, ValueRef, RefStorage
        ├── type_registry.h            # TypeRegistry
        ├── value.h                    # Value, ValueView, ConstValueView
        ├── modification_tracker.h     # ModificationTrackerStorage, ModificationTracker
        ├── observer_storage.h         # ObserverStorage, Notifiable interface
        ├── time_series_value.h        # TSValue, TSView
        ├── deref_time_series_value.h  # DerefTSValue (REF dereferencing wrapper)
        ├── bound_value.h              # BoundValue (schema-driven binding result)
        ├── bind.h                     # Schema matching and binding functions
        ├── python_conversion.h        # Python conversion ops, named type instances
        ├── VALUE_DESIGN.md            # This document
        └── VALUE_USER_GUIDE.md        # User guide
```

## Testing

Tests are in `cpp/tests/`:
- `test_value.cpp`: Unit tests (174 test cases, 7,377 assertions)
- `value_examples.cpp`: Comprehensive examples

Test categories include:
- Scalar, Bundle, List, Set, Dict, Window type operations
- Value/View creation and navigation
- Type composability (nested types)
- Copy/move semantics
- Edge cases and stress tests
- **Modification tracking** (12 test cases)
- **Time-series values** (25 test cases)
- **Observer/notification** (12 test cases)
- **Window types** (17 test cases)
- **Ref types** (18 test cases)
- **Delta tracking** (Set/Dict delta computation)
- **Compaction** (SetStorage/DictStorage fragmentation and compaction)
