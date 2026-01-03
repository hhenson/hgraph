# Value Type System - User Guide

**Version**: 1.1
**Date**: 2025-12-29
**Related**: [Value_DESIGN.md](Value_DESIGN.md) - Design Document

---

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Creating Scalar Values](#2-creating-scalar-values)
3. [Accessing Scalar Values](#3-accessing-scalar-values)
4. [Working with Tuple Types](#4-working-with-tuple-types)
5. [Working with Bundle Types](#5-working-with-bundle-types)
6. [Working with List Types](#6-working-with-list-types)
7. [Working with Set and Map Types](#7-working-with-set-and-map-types)
   - [7.1 Sets](#71-sets)
   - [7.2 Maps](#72-maps)
   - [7.3 KeySet - Read-Only View Over Map Keys](#73-keyset---read-only-view-over-map-keys)
   - [7.4 Set vs KeySet Comparison](#74-set-vs-keyset-comparison)
   - [7.5 CyclicBuffer and Queue Types](#75-cyclicbuffer-and-queue-types)
8. [Visiting Values](#8-visiting-values)
9. [Deep Traversal of Nested Structures](#9-deep-traversal-of-nested-structures)
10. [Path-Based Access](#10-path-based-access)
11. [Comparison and Hashing](#11-comparison-and-hashing)
12. [Cloning Values](#12-cloning-values)
13. [Python Interop](#13-python-interop)
14. [Performance Tips](#14-performance-tips)
    - [14.1 Set and Map Performance Characteristics](#141-set-and-map-performance-characteristics)
15. [Error Handling](#15-error-handling)
16. [Extending Value Operations](#16-extending-value-operations)

---

## 1. Getting Started

The Value type system provides type-erased storage for runtime values. Values are controlled by schemas
(TypeMeta) that define their type, size, and available operations.

**Basic Concepts:**
- **Value**: Owns storage for a single value of any supported type
- **ValueView**: Non-owning mutable reference to a value
- **ConstValueView**: Non-owning read-only reference to a value
- **TypeMeta**: Describes the type, including size, alignment, and operations

**Specialized Views:**

The view system provides specialized views based on type characteristics:

| View Type | Purpose | Access Pattern |
|-----------|---------|----------------|
| `TupleView` | Heterogeneous indexed (unnamed) | `at(index)`, `operator[]` |
| `BundleView` | Struct-like types (named) | `at(index)`, `at(name)`, `operator[]` |
| `ListView` | Homogeneous indexed (fixed/dynamic) | `at(index)`, `operator[]`, `push_back()` |
| `SetView` | Unique elements | `contains()`, `insert()`, `erase()` |
| `MapView` | Key-value pairs | `at(key)`, `operator[]`, `contains()`, `keys()` |
| `ConstKeySetView` | Read-only view of map keys | `contains()`, `size()`, iteration |
| `CyclicBufferView` | Fixed-size circular buffer | `at(index)`, `push_back()`, `front()`, `back()` |
| `QueueView` | FIFO queue with optional max | `at(index)`, `push_back()`, `pop_front()` |

```cpp
// Query type and get specialized view
if (value.const_view().is_bundle()) {
    ConstBundleView bv = value.as_bundle();
    // Use bundle-specific operations
}

// Or use try_as for safe conversion
if (auto bv = value.const_view().try_as_bundle()) {
    // Use *bv as ConstBundleView
}
```

---

## 2. Creating Scalar Values

```cpp
#include <hgraph/types/value/value.h>

using namespace hgraph::value;

// Create scalar values with automatic type detection
Value v_int(42);                    // int64_t
Value v_double(3.14);               // double
Value v_bool(true);                 // bool
Value v_string(std::string("hello")); // string

// Create from datetime types
Value v_date(engine_date_t{2025y/December/27d});
Value v_time(engine_time_t{...});
Value v_delta(engine_time_delta_t{std::chrono::seconds(60)});
```

---

## 3. Accessing Scalar Values

```cpp
// Fast access (debug assertion only)
int64_t x = v_int.as<int64_t>();

// Safe access (returns nullptr on type mismatch)
int64_t* p = v_int.try_as<int64_t>();
if (p) {
    std::cout << "Value: " << *p << "\n";
}

// Checked access (throws on type mismatch)
try {
    double d = v_int.checked_as<double>();  // Throws!
} catch (const std::runtime_error& e) {
    std::cout << "Type mismatch: " << e.what() << "\n";
}
```

---

## 4. Working with Tuple Types

Tuples are heterogeneous indexed collections - each position can hold a different type.
Unlike bundles, tuples have **no named fields** and are always unnamed (anonymous).
Access is by **index position only**.

### 4.1 Creating Tuple Schemas

```cpp
// Create a tuple schema with heterogeneous element types
auto tuple_schema = TypeRegistry::instance().tuple()
    .element(scalar_type_meta<int64_t>())      // index 0
    .element(scalar_type_meta<std::string>())  // index 1
    .element(scalar_type_meta<double>())       // index 2
    .build();
```

### 4.2 Creating and Accessing Tuple Values

```cpp
// Create a tuple value
Value record(tuple_schema);

// Get specialized TupleView for tuple operations
TupleView tv = record.as_tuple();

// Templated set - automatically wraps native types (validates against schema)
tv.set(0, 42);                    // int64_t - auto-wrapped
tv.set(1, std::string("hello"));  // string - auto-wrapped
tv.set(2, 3.14);                  // double - auto-wrapped

// Explicit Value wrapping still works
tv.set(0, Value(100));

// Or use operator[] for direct access
tv[0].as<int64_t>() = 200;
tv[1].as<std::string>() = "world";

// Read elements - using ConstTupleView
ConstTupleView ctv = record.as_tuple();

// By index using at() or operator[]
int64_t id = ctv.at(0).as<int64_t>();
std::string name = ctv[1].as<std::string>();
double value = ctv[2].as<double>();

// Get element type at position
const TypeMeta* elem_type = ctv.element_type(1);  // string type

// Iterate over elements
for (size_t i = 0; i < ctv.size(); ++i) {
    std::cout << ctv[i].to_string() << " ";
}
```

### 4.3 Tuple vs Bundle

| Feature | Tuple | Bundle |
|---------|-------|--------|
| Named fields | No | Yes |
| Index access | Yes | Yes |
| Named schema | No (always anonymous) | Optional |
| Use case | Lightweight records | Named struct-like data |

---

## 5. Working with Bundle Types

Bundles are struct-like containers with ordered, named fields. Fields can be accessed by
**both index position and name**.

### 5.1 Creating Bundle Schemas

```cpp
// Anonymous bundle schema
auto schema = TypeRegistry::instance().bundle()
    .field("x", scalar_type_meta<int64_t>())      // index 0
    .field("y", scalar_type_meta<double>())       // index 1
    .field("name", scalar_type_meta<std::string>()) // index 2
    .build();

// Named bundle schema (can be retrieved by name later)
auto point_schema = TypeRegistry::instance().bundle("Point")
    .field("x", scalar_type_meta<int64_t>())
    .field("y", scalar_type_meta<int64_t>())
    .build();

// Retrieve named schema later
const TypeMeta* retrieved = TypeRegistry::instance().get_bundle_by_name("Point");
```

### 5.2 Creating and Accessing Bundle Values

```cpp
// Create a bundle value
Value point(schema);

// Get specialized BundleView for bundle operations
BundleView bv = point.as_bundle();

// Templated set - automatically wraps native types (validates against schema)
bv.set("x", 10);                      // int64_t - auto-wrapped
bv.set("y", 20.5);                    // double - auto-wrapped
bv.set("name", std::string("origin")); // string - auto-wrapped

// By index with templated set
bv.set(0, 100);     // "x" = 100
bv.set(1, 25.5);    // "y" = 25.5

// Explicit Value wrapping still works
bv.set("x", Value(10));

// Or use operator[] for direct access
bv["x"].as<int64_t>() = 10;
bv["y"].as<double>() = 20.5;
bv[0].as<int64_t>() = 10;

// Read fields - using ConstBundleView
ConstBundleView cbv = point.as_bundle();  // const version

// By name using at() or operator[]
int64_t x = cbv.at("x").as<int64_t>();
double y = cbv["y"].as<double>();
std::string name = cbv["name"].as<std::string>();

// By index using at() or operator[]
int64_t x2 = cbv.at(0).as<int64_t>();
double y2 = cbv[1].as<double>();

// Check field metadata
bool has_z = cbv.has_field("z");           // false
size_t idx = cbv.field_index("y");         // 1
const BundleFieldInfo* info = cbv.field_info("x");
```

---

## 6. Working with List Types

Lists are homogeneous indexed collections. They come in two variants:
- **Dynamic lists**: Can grow/shrink at runtime (default)
- **Fixed-size lists**: Pre-allocated with a fixed capacity

### 6.1 Dynamic Lists

```cpp
// Create a dynamic list schema
auto list_schema = TypeRegistry::instance()
    .list(scalar_type_meta<int64_t>())
    .build();

// Create and populate a list
Value numbers(list_schema);

// Get specialized ListView for list operations
ListView lv = numbers.as_list();

// Templated push_back - automatically wraps native types
lv.push_back(10);   // int64_t - auto-wrapped
lv.push_back(20);
lv.push_back(30);

// Explicit Value wrapping still works
lv.push_back(Value(40));

// Access by index using at() or operator[]
int64_t first = lv[0].as<int64_t>();
lv[1].as<int64_t>() = 25;  // Modify element

// Use set() for assignment
lv.set(2, Value(35));

// List operations
size_t count = lv.size();           // 3
bool empty = lv.empty();            // false
ValueView front_elem = lv.front();  // First element
ValueView back_elem = lv.back();    // Last element

lv.pop_back();      // Remove last element
lv.clear();         // Remove all elements
lv.resize(5);       // Resize to 5 elements (default-initialized)

// Iterate using ConstListView
ConstListView clv = numbers.as_list();
for (ConstValueView elem : clv) {
    std::cout << elem.as<int64_t>() << " ";
}
```

### 6.2 Fixed-Size Lists

```cpp
// Create a fixed-size list schema (pre-allocates storage for 10 elements)
auto fixed_schema = TypeRegistry::instance()
    .fixed_list(scalar_type_meta<double>(), 10)
    .build();

// Create a fixed-size list
Value fixed_list(fixed_schema);
ListView flv = fixed_list.as_list();

// Check if fixed size
bool is_fixed = fixed_list.const_view().is_fixed_list();  // true

// Access by index (elements are default-initialized)
flv[0].as<double>() = 1.0;
flv[1].as<double>() = 2.0;

// set() works on fixed lists
flv.set(5, Value(5.0));

// Size is fixed at creation
size_t capacity = flv.size();  // 10

// Templated reset - automatically wraps sentinel value
flv.reset(0.0);  // All 10 elements are now 0.0

// reset() is the primary way to "clear" a fixed-size list
flv.reset(std::numeric_limits<double>::quiet_NaN());  // Mark all as "empty" using NaN

// Explicit Value wrapping still works
flv.reset(Value(-1.0));

// NOTE: push_back(), pop_back(), resize(), clear() are NOT available
// for fixed-size lists - they will throw
```

### 6.3 Dynamic vs Fixed-Size Lists

| Feature | Dynamic List | Fixed-Size List |
|---------|--------------|-----------------|
| Pre-allocated | No | Yes |
| `push_back()` | Yes | No |
| `pop_back()` | Yes | No |
| `resize()` | Yes | No |
| `clear()` | Yes | No |
| `reset(sentinel)` | Yes | Yes |
| Memory efficiency | Variable | Optimal (no reallocation) |
| Use case | Unknown size | Known size, performance-critical |

---

## 7. Working with Set and Map Types

Sets and Maps use **robin-hood hashing** internally, providing **O(1) average-case** performance
for `contains()`, `insert()`, `erase()`, and key lookup operations.

### 7.1 Sets

```cpp
// Set of integers
auto set_schema = TypeRegistry::instance()
    .set(scalar_type_meta<int64_t>())
    .build();

Value int_set(set_schema);

// Get specialized SetView
SetView sv = int_set.as_set();

// Templated insert - automatically wraps native types
// O(1) amortized - uses robin-hood hashing
sv.insert(1);   // int64_t - auto-wrapped
sv.insert(2);
sv.insert(3);
bool inserted = sv.insert(2);  // false - already exists

// Templated membership test - O(1) average
bool has_two = sv.contains(2);   // true - auto-wrapped
bool has_ten = sv.contains(10);  // false

// Templated erase - O(1) average
sv.erase(2);  // Remove element

// Explicit Value wrapping still works
sv.insert(Value(100));
bool has_100 = sv.contains(Value(100).const_view());

// Size and operations
size_t count = sv.size();
sv.clear();  // Remove all

// Iterate using ConstSetView
ConstSetView csv = int_set.as_set();
for (ConstValueView elem : csv) {
    std::cout << elem.as<int64_t>() << " ";
}
```

### 7.2 Maps

```cpp
// Map from string to double
auto map_schema = TypeRegistry::instance()
    .map(scalar_type_meta<std::string>(), scalar_type_meta<double>())
    .build();

Value prices(map_schema);

// Get specialized MapView
MapView mv = prices.as_map();

// Templated set - automatically wraps key and value
// O(1) amortized - uses robin-hood hashing
mv.set(std::string("apple"), 1.50);   // Both auto-wrapped
mv.set(std::string("banana"), 0.75);

// Templated access - O(1) average
double apple_price = mv.at(std::string("apple")).as<double>();

// Templated membership test - O(1) average
bool has_apple = mv.contains(std::string("apple"));  // true

// Templated insert (returns false if key exists) - O(1) amortized
bool inserted = mv.insert(std::string("apple"), 1.75);  // false

// Templated erase - O(1) average
mv.erase(std::string("banana"));

// Explicit Value wrapping still works
Value orange_key(std::string("orange"));
mv.set(orange_key.const_view(), Value(2.00));
double orange_price = mv.at(orange_key.const_view()).as<double>();

// operator[] inserts default if key missing (requires ConstValueView key)
mv[orange_key.const_view()].as<double>() = 2.50;

// Iterate over key-value pairs using ConstMapView
ConstMapView cmv = prices.as_map();
for (auto [key, val] : cmv) {
    std::cout << key.as<std::string>() << ": " << val.as<double>() << "\n";
}

// Templated contains on const view
bool has_orange = cmv.contains(std::string("orange"));  // true
```

### 7.3 KeySet - Read-Only View Over Map Keys

The `keys()` method returns a `ConstKeySetView` - a read-only set view over the map's keys.
This view has the **same interface as `ConstSetView`**, making it easy to work with map keys
using familiar set operations.

```cpp
// Get a read-only set view of the keys
ConstMapView cmv = prices.as_map();
ConstKeySetView key_set = cmv.keys();

// Same interface as ConstSetView
size_t num_keys = key_set.size();        // Number of keys
bool is_empty = key_set.empty();         // Check if map is empty
const TypeMeta* kt = key_set.element_type();  // Key type (same as map's key_type)

// Membership test - O(1) average (uses map's hash index)
Value apple_key(std::string("apple"));
bool has_apple = key_set.contains(apple_key.const_view());

// Iterate over keys only
for (ConstValueView key : key_set) {
    std::cout << key.as<std::string>() << "\n";
}

// Works on mutable MapView too
MapView mv = prices.as_map();
ConstKeySetView mv_keys = mv.keys();  // Still read-only
```

**Python Usage:**

```python
# Get map view
cmv = value.const_view().as_map()

# Get key set view - same interface as ConstSetView
key_set = cmv.keys()

# Use familiar set operations
print(f"Number of keys: {key_set.size()}")
print(f"Is empty: {key_set.empty()}")

# Membership test
apple_key = PlainValue("apple")
if key_set.contains(apple_key.const_view()):
    print("Has apple!")

# Iterate over keys
for key in key_set:
    print(f"Key: {key.as_string()}")

# Works with 'in' operator
if apple_key.const_view() in key_set:
    print("Apple is in the key set")
```

### 7.4 Set vs KeySet Comparison

| Feature | ConstSetView | ConstKeySetView |
|---------|--------------|-----------------|
| `size()` | ✓ | ✓ |
| `empty()` | ✓ | ✓ |
| `contains()` | ✓ | ✓ |
| `element_type()` | ✓ | ✓ (returns key type) |
| `__len__` | ✓ | ✓ |
| `__iter__` | ✓ | ✓ |
| `__contains__` | ✓ | ✓ |
| `insert()` | ✓ (via SetView) | ✗ (read-only) |
| `erase()` | ✓ (via SetView) | ✗ (read-only) |

### 7.5 CyclicBuffer and Queue Types

**Implementation**: `cpp/include/hgraph/types/value/cyclic_buffer_ops.h`, `indexed_view.h`

CyclicBuffer and Queue are window-like data structures for managing collections with special access patterns:

| Type | Description | Capacity | When Full |
|------|-------------|----------|-----------|
| **CyclicBuffer** | Fixed-size circular buffer | Fixed at creation | Oldest element evicted |
| **Queue** | FIFO queue | Optional max capacity | Acts like CyclicBuffer if bounded |

#### CyclicBuffer

A fixed-size circular buffer that re-centers on read. Logical index 0 always refers to the oldest element.

```cpp
// Create a cyclic buffer of integers with capacity 5
auto cb_schema = TypeRegistry::instance()
    .cyclic_buffer(scalar_type_meta<int64_t>(), 5)
    .build();

Value buffer(cb_schema);
CyclicBufferView cbv = buffer.as_cyclic_buffer();

// Push elements
cbv.push_back(Value(10));  // Buffer: [10]
cbv.push_back(Value(20));  // Buffer: [10, 20]
cbv.push_back(Value(30));  // Buffer: [10, 20, 30]

// Access elements (logical order: oldest first)
ConstValueView oldest = cbv.front();  // 10
ConstValueView newest = cbv.back();   // 30
ConstValueView second = cbv[1];       // 20

// Check capacity
size_t cap = cbv.capacity();  // 5
bool is_full = cbv.full();    // false (size=3, capacity=5)

// When full, oldest element is evicted
cbv.push_back(Value(40));
cbv.push_back(Value(50));  // Buffer now full: [10, 20, 30, 40, 50]
cbv.push_back(Value(60));  // Evicts 10: [20, 30, 40, 50, 60]

// Logical indexing always gives oldest-first order
cbv[0].as<int64_t>();  // 20 (oldest)
cbv[4].as<int64_t>();  // 60 (newest)

// Iterate in logical order
for (ConstValueView elem : cbv) {
    std::cout << elem.as<int64_t>() << " ";  // Prints: 20 30 40 50 60
}

// Clear the buffer
cbv.clear();
```

#### Queue (Partial Implementation)

A FIFO queue with optional maximum capacity. When unbounded, it can grow without limit.
When bounded, it behaves like a CyclicBuffer when full.

```cpp
// Create an unbounded queue
auto queue_schema = TypeRegistry::instance()
    .queue(scalar_type_meta<int64_t>())
    .build();

// Create a bounded queue with max capacity 100
auto bounded_schema = TypeRegistry::instance()
    .queue(scalar_type_meta<int64_t>())
    .max_capacity(100)
    .build();

Value q(queue_schema);
QueueView qv = q.as_queue();

// Check capacity
size_t max = qv.max_capacity();      // 0 = unbounded
bool bounded = qv.has_max_capacity(); // false

// Note: push_back() and pop_front() are planned but not yet implemented
// Currently Queue uses CyclicBuffer operations as a placeholder
```

#### Python Usage

```python
from hgraph._hgraph import value

# Create a cyclic buffer
int_schema = value.scalar_type_meta_int64()
cb_schema = value.TypeRegistry.instance().cyclic_buffer(int_schema, 5).build()

buf = value.PlainValue(cb_schema)
cbv = buf.view().as_cyclic_buffer()

# Push values
for i in range(7):
    elem = value.PlainValue(int_schema)
    elem.set_int(i * 10)
    cbv.push_back(elem.const_view())

# Buffer contains [20, 30, 40, 50, 60] (evicted 0, 10)
print(f"Size: {len(cbv)}, Capacity: {cbv.capacity()}, Full: {cbv.full()}")

# Access elements
print(f"Oldest: {cbv.front().as_int()}")  # 20
print(f"Newest: {cbv.back().as_int()}")   # 60

# Convert to numpy (copies in logical order)
import numpy as np
arr = cbv.to_numpy()  # numpy array: [20, 30, 40, 50, 60]
print(f"Type: {arr.dtype}, Values: {arr}")

# Convert to Python list
py_list = buf.const_view().to_python()  # [20, 30, 40, 50, 60]

# Iterate
for elem in buf.const_view().as_cyclic_buffer():
    print(elem.as_int())
```

#### CyclicBuffer vs List Comparison

| Feature | CyclicBuffer | List (Dynamic) | List (Fixed) |
|---------|--------------|----------------|--------------|
| Size | Fixed capacity | Grows dynamically | Fixed at creation |
| `push_back()` | Evicts oldest if full | Always appends | Not supported |
| `pop_back()` | Not supported | Removes last | Not supported |
| Index 0 | Oldest element | First inserted | First position |
| Memory | Pre-allocated | May reallocate | Pre-allocated |
| `to_numpy()` | Returns copy (re-centered) | Zero-copy possible | Zero-copy possible |

---

## 8. Visiting Values

**Implementation**: `cpp/include/hgraph/types/value/visitor.h`

The visitor pattern provides flexible runtime dispatch based on TypeKind without static dependencies on specific scalar types.

### Visit with Overloaded Handlers

The `visit` function combines handlers using the overloaded pattern and dispatches based on TypeKind:

```cpp
#include <hgraph/types/value/visitor.h>

using namespace hgraph::value;

// Visit with type-specific handlers
std::string result = visit(value.const_view(),
    [](ConstValueView v) { return "scalar: " + v.to_string(); },
    [](ConstTupleView t) { return "tuple[" + std::to_string(t.size()) + "]"; },
    [](ConstBundleView b) { return "bundle"; },
    [](ConstListView l) { return "list"; },
    [](ConstSetView s) { return "set"; },
    [](ConstMapView m) { return "map"; }
);

// Partial handlers with catch-all (ConstValueView matches any type)
std::string result = visit(value.const_view(),
    [](ConstListView l) { return "list[" + std::to_string(l.size()) + "]"; },
    [](ConstMapView m) { return "map"; },
    [](ConstValueView v) { return "other: " + v.to_string(); }  // catch-all
);
```

For scalar values, the handler receives `ConstValueView`. Use `is_scalar_type<T>()` to check the type:

```cpp
int64_t doubled = visit(value.const_view(),
    [](ConstValueView v) {
        if (v.is_scalar_type<int64_t>()) return v.as<int64_t>() * 2;
        if (v.is_scalar_type<double>()) return static_cast<int64_t>(v.as<double>() * 2);
        return int64_t{0};
    },
    [](ConstTupleView) { return int64_t{0}; },
    [](ConstBundleView) { return int64_t{0}; },
    [](ConstListView) { return int64_t{0}; },
    [](ConstSetView) { return int64_t{0}; },
    [](ConstMapView) { return int64_t{0}; }
);
```

### Mutable Visiting

For mutable access, pass `ValueView` instead:

```cpp
visit(value.view(),
    [](ValueView v) { v.as<int64_t>() *= 2; },
    [](TupleView t) { /* modify tuple */ },
    [](BundleView b) { /* modify bundle */ },
    [](ListView l) { /* modify list */ },
    [](SetView s) { /* modify set */ },
    [](MapView m) { /* modify map */ }
);
```

### Pattern Matching with When/Otherwise

For declarative pattern matching on TypeKind:

```cpp
std::string result = match<std::string>(value.const_view(),
    when<TypeKind::Scalar>([](ConstValueView v) { return v.to_string(); }),
    when<TypeKind::List>([](ConstListView l) {
        return "list of " + std::to_string(l.size());
    }),
    otherwise([](ConstValueView) { return "other"; })
);
```

### Python Bindings

The Python bindings provide simple visitor methods on `ConstValueView` and `ValueView`:

```python
# Visit with a handler (converts to Python, calls handler)
result = value.const_view().visit(lambda x: f"value: {x}")

# Void visitor for side effects
value.const_view().visit_void(lambda x: print(x))

# Pattern matching on Python types
result = value.const_view().match(
    (int, lambda x: f"int:{x}"),
    (float, lambda x: f"float:{x}"),
    (str, lambda x: f"str:{x}"),
    (None, lambda x: "default"),  # None = default handler
)

# Mutable visitor (returns new value to update)
value.view().visit_mut(lambda x: x * 2)  # Doubles numeric value
```

---

## 9. Deep Traversal of Nested Structures

**Implementation**: `cpp/include/hgraph/types/value/traversal.h`

Deep traversal visits all leaf (scalar) values in a nested structure, tracking the path to each:

```cpp
#include <hgraph/types/value/traversal.h>

using namespace hgraph::value;

// Count all leaf values
size_t count = count_leaves(value.const_view());
std::cout << "Total leaves: " << count << "\n";

// Visit each leaf with its path
deep_visit(value.const_view(), [](ConstValueView leaf, const TraversalPath& path) {
    std::cout << "At " << path_to_string(path) << ": " << leaf.to_string() << "\n";
});

// Collect all leaf paths
std::vector<TraversalPath> paths = collect_leaf_paths(value.const_view());

// Collect leaves with their paths
auto leaves = collect_leaves(value.const_view());
for (const auto& [path, leaf] : leaves) {
    std::cout << path_to_string(path) << " = " << leaf.to_string() << "\n";
}

// Transform all numeric values (mutable traversal)
deep_visit_mut(value.view(), [](ValueView leaf, const TraversalPath&) {
    if (leaf.is_scalar_type<int64_t>()) {
        leaf.as<int64_t>() *= 2;
    }
});

// Utility transformations
transform_int64(value.view(), [](int64_t v) { return v * 2; });
transform_double(value.view(), [](double v) { return v + 1.0; });
transform_string(value.view(), [](const std::string& s) { return s + "_suffix"; });

// Aggregations
double total = sum_numeric(value.const_view());
auto max_val = max_numeric(value.const_view());  // returns std::optional<double>
auto min_val = min_numeric(value.const_view());
```

---

## 10. Path-Based Access

**Implementation**: `cpp/include/hgraph/types/value/path.h`

Navigate through nested structures using path expressions:

```cpp
#include <hgraph/types/value/path.h>

using namespace hgraph::value;

// Build a path programmatically
ValuePath path;
path.push_back(PathElement::field("user"));      // Bundle field access
path.push_back(PathElement::field("addresses")); // Another field
path.push_back(PathElement::index(0));           // List/tuple index
path.push_back(PathElement::field("city"));      // Nested field

// For maps with arbitrary key types, use PathElement::key() with a ConstValueView
Value<> map_key("config");
path.push_back(PathElement::key(map_key.const_view()));  // Map key lookup

// Navigate to nested value
ConstValueView city = navigate(root.const_view(), path);
std::cout << "City: " << city.as<std::string>() << "\n";

// Parse path from string (supports dot notation, bracket indexing, and map keys)
ValuePath p = parse_path("user.addresses[0].city");
ValuePath p2 = parse_path("items[0][1]");  // Multi-dimensional access
ValuePath p3 = parse_path("data[\"key\"].value");  // Map string key access
ValuePath p4 = parse_path("map['name']");  // Single-quoted key also works

// Safe navigation (returns std::optional)
auto maybe_city = try_navigate(root.const_view(), path);
if (maybe_city) {
    std::cout << "Found: " << maybe_city->as<std::string>() << "\n";
}

// String-based navigation
auto city2 = navigate(root.const_view(), "user.addresses[0].city");

// Mutable navigation
ValueView mut_city = navigate_mut(root.view(), path);
mut_city.as<std::string>() = "New City";

// Convert path back to string
std::string path_str = path_to_string(path);  // "user.addresses[0].city"

// Map navigation example
Value<> config = /* map with structure: {"database": {"host": "localhost", "port": 5432}} */;
auto host = navigate(config.const_view(), R"(["database"].host)");
std::cout << "Host: " << host.as<std::string>() << "\n";  // "localhost"

// Map with integer keys
Value<> lookup = /* map with structure: {1: "one", 2: "two"} */;
auto val = navigate(lookup.const_view(), "[2]");
std::cout << "Value: " << val.as<std::string>() << "\n";  // "two"
```

**Path Syntax (string parsing):**
- Field access: `user`, `address.city` (for bundles or maps with string keys)
- Index access: `[0]`, `items[1]` (for lists, tuples, or maps with integer keys)
- String key access: `["key"]` or `['key']` (alternative syntax for string keys)
- Mixed: `users[0].addresses[1].city`, `data["config"].settings[0].value`

**Programmatic Path Building:**
For maps with arbitrary key types (not just string or integer), use `PathElement::key()`:
```cpp
// Map with tuple keys
auto tuple_schema = registry.tuple().element(int_schema).element(string_schema).build();
Value<> key = /* tuple value */;
path.push_back(PathElement::key(key.const_view()));  // Arbitrary value key
```

**Type-Dependent Navigation:**
- String elements on bundles → field access by name
- String elements on maps with string keys → string key lookup
- String elements on maps with non-string keys → error
- Index elements on lists/tuples/bundles → element access by position
- Index elements on maps with integer keys → integer key lookup
- Value elements on maps → key lookup (key type must match)

---

## 11. Comparison and Hashing

```cpp
Value a(42);
Value b(42);
Value c(100);

// Equality
if (a.equals(b)) {
    std::cout << "a equals b\n";
}

// Use in unordered containers
std::unordered_set<Value, ValueHash, ValueEqual> value_set;
value_set.insert(a);
value_set.insert(c);

// Use in ordered containers
std::set<Value, ValueLess> value_ordered;
value_ordered.insert(a);
value_ordered.insert(c);
```

---

## 12. Cloning Values

Views are non-owning references. To create an owning copy of a viewed value, use `clone()`:

```cpp
// From a ConstValueView
ConstValueView cv = some_value.const_view();
Value copy = cv.clone();  // Creates owning copy

// From a ValueView
ValueView mv = some_value.view();
Value copy2 = mv.clone();  // Also works

// Alternative: explicit Value constructor from view
Value copy3(cv);  // Equivalent to clone()
```

**When to use clone:**
- When you need to store a value beyond the lifetime of the original
- When passing values to functions that take ownership
- When building collections of values from views

---

## 13. Python Interop

```cpp
#include <nanobind/nanobind.h>
namespace nb = nanobind;

// Convert C++ Value to Python object
Value cpp_value(42);
nb::object py_obj = cpp_value.to_python();

// Convert Python object to C++ Value
nb::object py_list = nb::eval("[[1, 2], [3, 4]]");
Value cpp_list = Value::from_python(py_list, list_of_lists_schema);
```

### NumPy Integration

Lists of numeric types can be converted to NumPy arrays:

```python
from hgraph._hgraph import value

# Create a list of integers
int_list = value.PlainValue(int_list_schema)
list_view = int_list.as_list()
for i in range(10):
    list_view.push_back(value.PlainValue(i).const_view())

# Convert to numpy array
clv = int_list.const_view().as_list()
if clv.is_buffer_compatible():
    arr = clv.to_numpy()  # Returns numpy array
    print(arr.sum())      # NumPy operations work

# Supported element types:
# - int64 -> np.int64
# - double -> np.float64
# - bool -> np.bool_
```

**Note:** `to_numpy()` creates a copy of the data. For true zero-copy buffer protocol support,
future versions may implement the Python buffer protocol directly.

---

## 14. Performance Tips

1. **Use views for temporary access** - Views avoid copying and are lightweight.

2. **Prefer `as<T>()` over `checked_as<T>()`** in performance-critical code when type
   is guaranteed correct.

3. **Pre-allocate bundles** - Bundle memory is allocated upfront based on schema.

4. **Use cursors for iteration** - Cursor-based iteration is bounds-checked and efficient.

5. **Minimize Python interop** in hot paths - `to_python`/`from_python` involve GIL acquisition.

6. **Use `clone()` sparingly** - Only clone when you need ownership; prefer views otherwise.

7. **Set and Map operations are O(1)** - Sets and Maps use robin-hood hashing internally
   via `ankerl::unordered_dense`. All key operations (`contains()`, `insert()`, `erase()`,
   `at()`, `set()`) are O(1) average-case, making them efficient for large collections.

```cpp
// Good: Single view for multiple accesses
ValueView v = value.view();
v.set_field("a", Value(1));
v.set_field("b", Value(2));
v.set_field("c", Value(3));

// Avoid: Creating new view for each access
value.view().set_field("a", Value(1));  // New view
value.view().set_field("b", Value(2));  // New view
value.view().set_field("c", Value(3));  // New view
```

### 14.1 Set and Map Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| `contains()` | O(1) average | Hash lookup |
| `insert()` | O(1) amortized | May trigger rehash |
| `erase()` | O(1) average | Uses tombstone pattern |
| `at()` / `set()` | O(1) average | Hash lookup |
| `size()` | O(1) | Stored count |
| Iteration | O(n) | Linear scan of elements |

**Memory Layout:**
- Elements are stored contiguously in a vector for cache efficiency
- A separate hash index maps keys to element indices
- This provides both fast random access and efficient iteration

```cpp
// Large set - still O(1) lookup
SetView sv = large_set.as_set();
for (int i = 0; i < 10000; ++i) {
    sv.insert(i);  // O(1) amortized each
}
bool found = sv.contains(9999);  // O(1) - instant lookup

// Large map - same O(1) performance
MapView mv = large_map.as_map();
for (int i = 0; i < 10000; ++i) {
    mv.set(std::to_string(i), double(i));  // O(1) amortized each
}
double val = mv.at(std::string("9999")).as<double>();  // O(1) lookup
```

---

## 15. Error Handling

```cpp
// Type mismatch
Value v(42);
try {
    double d = v.checked_as<double>();
} catch (const std::runtime_error& e) {
    // Handle type mismatch
}

// Invalid value access
Value empty;  // Default constructed, invalid
if (!empty.valid()) {
    // Handle invalid value
}

// Safe pattern
if (auto* p = v.try_as<double>()) {
    // Use *p
} else {
    // Not a double
}
```

---

## 16. Extending Value Operations

Value operations can be extended using **zero-overhead composition** at compile time. Two complementary
patterns are available:

1. **Policy-Based** - Simple template parameter for single-concern extensions
2. **CRTP Mixin** - Template chaining for multiple extensions

Both patterns provide the **same API** regardless of extensions - `v.to_python()` works identically.

### 16.1 Policy-Based Extensions (Simple)

For single extensions like caching, use a template policy parameter:

```cpp
#include <hgraph/value/value.h>

using namespace hgraph::value;

// Default - no extensions, no overhead
Value<> v1(123456789);
v1.to_python();  // Direct call to TypeOps

// With Python caching - same API
// Note: Use large integers (>256) to avoid Python's small integer cache
Value<WithPythonCache> v2(123456789);
v2.to_python();  // First call: convert + cache
v2.to_python();  // Second call: return cached object

// Using type alias for convenience
CachedValue v3(123456789);  // Same as Value<WithPythonCache>
nb::object py1 = v3.to_python();
nb::object py2 = v3.to_python();
assert(py1.is(py2));  // Same Python object!
```

**Cache Invalidation:**

The cache is automatically invalidated when you request a mutable view:

```cpp
CachedValue v(123456789);
nb::object py1 = v.to_python();  // Cached

// Get mutable view - automatically invalidates cache
ValueView view = v.view();
view.as<int64_t>() = 987654321;

nb::object py2 = v.to_python();  // Re-converts (cache was invalidated)
assert(!py1.is(py2));  // Different objects
```

### 16.2 Zero-Overhead Guarantee

Policy storage uses Empty Base Optimization (EBO) - unused extensions add zero size:

```cpp
// No extension: minimal size
static_assert(sizeof(Value<NoCache>) == sizeof(ValueStorage) + sizeof(TypeMeta*));

// With cache: adds only the optional
static_assert(sizeof(Value<WithPythonCache>) ==
              sizeof(ValueStorage) + sizeof(TypeMeta*) + sizeof(std::optional<nb::object>));
```

The `if constexpr` dispatch eliminates unused code paths at compile time.

### 16.3 CRTP Mixin Extensions (Flexible)

For multiple extensions or custom behavior, use CRTP mixin chaining.
Read the chain right-to-left: `WithModTracking<WithCache<ValueOps<...>>>` means:
1. Start with `ValueOps` (base operations)
2. Add `WithCache` (Python object caching)
3. Add `WithModTracking` (modification callbacks)

```cpp
// TSValue: caching + modification tracking
using TSValue = WithModTracking<WithCache<ValueOps<TSValue>>>;

// Note: Use large integers (>256) to avoid Python's small integer cache
TSValue v(123456789);

// Register modification callback
v.on_modified([]{ std::cout << "Value changed!\n"; });

// Uses cache from WithCache
nb::object py1 = v.to_python();
nb::object py2 = v.to_python();
assert(py1.is(py2));  // Cached

// Modification invalidates cache and triggers callback
v.from_python(nb::int_(987654321));
// Output: "Value changed!"

nb::object py3 = v.to_python();  // Re-converts
assert(!py1.is(py3));  // Different object
```

### 16.4 Defining Custom Extensions

Use CRTP mixins to define custom extension behavior:

```cpp
// Mixin that adds validation on from_python
template<typename Base>
class WithValidation : public Base {
public:
    using Base::Base;

    void impl_from_python(const nb::object& src) {
        // Reject None values
        if (src.is_none()) {
            throw std::runtime_error("Cannot convert None to Value");
        }
        Base::impl_from_python(src);
    }
};

// Use the mixin
using ValidatedValue = WithValidation<ValueOps<ValidatedValue>>;

ValidatedValue v(0);
try {
    v.from_python(nb::none());  // Throws!
} catch (const std::runtime_error& e) {
    // "Cannot convert None to Value"
}
```

### 16.5 Composing Multiple Mixins

Chain multiple mixins for combined behavior:

```cpp
// Order matters: outer mixins wrap inner ones
// Read right-to-left: ValueOps -> WithCache -> WithValidation -> WithModTracking
using FullFeaturedValue = WithModTracking<
                            WithValidation<
                              WithCache<
                                ValueOps<FullFeaturedValue>>>>;

FullFeaturedValue v(123456789);

// Has all features:
v.on_modified([&]{ /* notify observers */ });    // From WithModTracking
v.from_python(some_obj);                          // Validates (WithValidation)
                                                  // Invalidates cache (WithCache)
                                                  // Triggers callback (WithModTracking)
nb::object py = v.to_python();                    // Uses cache (WithCache)
```

### 16.6 When to Use Each Pattern

| Use Case | Pattern | Example |
|----------|---------|---------|
| Single built-in extension | Policy | `Value<WithPythonCache>` |
| Multiple extensions | CRTP Mixin | `WithA<WithB<ValueOps<...>>>` |
| Custom behavior/hooks | CRTP Mixin | Override `impl_to_python()` |
| No extensions (default) | Either | `Value<>` or `PlainValue` |

### 16.7 Built-in Type Aliases

```cpp
// Convenience aliases for common configurations
using PlainValue = Value<NoCache>;           // No extensions
using CachedValue = Value<WithPythonCache>;  // Python object caching

// For time-series (via CRTP)
using TSValue = WithModTracking<WithCache<ValueOps<TSValue>>>;
```

---

**End of User Guide**
