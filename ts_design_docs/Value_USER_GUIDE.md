# Value Type System - User Guide

**Version**: 1.0
**Date**: 2025-12-28
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
8. [Visiting Values](#8-visiting-values)
9. [Deep Traversal of Nested Structures](#9-deep-traversal-of-nested-structures)
10. [Path-Based Access](#10-path-based-access)
11. [Comparison and Hashing](#11-comparison-and-hashing)
12. [Cloning Values](#12-cloning-values)
13. [Python Interop](#13-python-interop)
14. [Performance Tips](#14-performance-tips)
15. [Error Handling](#15-error-handling)

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
| `MapView` | Key-value pairs | `at(key)`, `operator[]`, `contains()` |

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
sv.insert(1);   // int64_t - auto-wrapped
sv.insert(2);
sv.insert(3);
bool inserted = sv.insert(2);  // false - already exists

// Templated membership test
bool has_two = sv.contains(2);   // true - auto-wrapped
bool has_ten = sv.contains(10);  // false

// Templated erase
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
mv.set(std::string("apple"), 1.50);   // Both auto-wrapped
mv.set(std::string("banana"), 0.75);

// Templated access
double apple_price = mv.at(std::string("apple")).as<double>();

// Templated membership test
bool has_apple = mv.contains(std::string("apple"));  // true

// Templated insert (returns false if key exists)
bool inserted = mv.insert(std::string("apple"), 1.75);  // false

// Templated erase
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

// Iterate over just keys or values
for (auto key : cmv.keys()) {
    std::cout << key.as<std::string>() << "\n";
}
```

---

## 8. Visiting Values

Use the visitor pattern to handle type-erased values:

```cpp
// Using overloaded lambdas
visit_with(some_value.view(),
    [](int64_t& i) {
        std::cout << "Integer: " << i << "\n";
    },
    [](double& d) {
        std::cout << "Double: " << d << "\n";
    },
    [](std::string& s) {
        std::cout << "String: " << s << "\n";
    },
    [](auto& other) {
        std::cout << "Other type\n";
    }
);

// Using a visitor class
class PrintVisitor : public ValueVisitor<void> {
public:
    void visit_int64(int64_t& v) override {
        std::cout << v;
    }
    void visit_double(double& v) override {
        std::cout << v;
    }
    void visit_bundle(ValueView bundle, const TypeMeta* schema) override {
        std::cout << "{";
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << schema->fields[i].name << ": ";
            visit(bundle.get_at(i), *this);
        }
        std::cout << "}";
    }
};

PrintVisitor printer;
visit(value.view(), printer);
```

---

## 9. Deep Traversal of Nested Structures

```cpp
// Count all leaf values in a nested structure
int count = 0;
auto counter = make_deep_visitor([&count](auto&) {
    ++count;
});
visit(complex_value.view(), counter);
std::cout << "Total leaf values: " << count << "\n";

// Transform all numeric values
auto doubler = make_deep_visitor([](auto& v) {
    if constexpr (std::is_arithmetic_v<std::decay_t<decltype(v)>>) {
        v *= 2;
    }
});
visit(value.view(), doubler);
```

---

## 10. Path-Based Access

```cpp
// Navigate to deeply nested values
ValuePath path = {
    PathElement::field("user"),
    PathElement::field("addresses"),
    PathElement::index(0),
    PathElement::field("city")
};

ConstValueView city = navigate(root.const_view(), path);
std::cout << "City: " << city.as<std::string>() << "\n";

// Parse path from string
ValuePath p = parse_path("user.addresses[0].city");
```

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

---

## 14. Performance Tips

1. **Use views for temporary access** - Views avoid copying and are lightweight.

2. **Prefer `as<T>()` over `checked_as<T>()`** in performance-critical code when type
   is guaranteed correct.

3. **Pre-allocate bundles** - Bundle memory is allocated upfront based on schema.

4. **Use cursors for iteration** - Cursor-based iteration is bounds-checked and efficient.

5. **Minimize Python interop** in hot paths - `to_python`/`from_python` involve GIL acquisition.

6. **Use `clone()` sparingly** - Only clone when you need ownership; prefer views otherwise.

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

**End of User Guide**
