# hgraph::value Type System - User Guide

## Quick Start

Include the convenience header:

```cpp
#include <hgraph/types/value/all.h>
using namespace hgraph::value;
```

## Creating Values

### Scalar Values

```cpp
// Using type registry
TypeRegistry registry;
const TypeMeta* int_type = registry.get("int");
Value val(int_type);
val.as<int>() = 42;

// Direct scalar creation
Value direct(scalar_type_meta<double>());
direct.as<double>() = 3.14;

// Helper function
Value quick = make_scalar(100);  // int value = 100
```

### Bundle Values (Structs)

```cpp
// Define the type
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

// Create a value
Value point(point_meta.get());

// Access via view
ValueView pv = point.view();
pv.field("x").as<int>() = 10;
pv.field("y").as<int>() = 20;

// Read via const view
ConstValueView cpv = point.const_view();
int x = cpv.field("x").as<int>();  // 10
int y = cpv.field("y").as<int>();  // 20
```

### List Values (Fixed Arrays)

```cpp
// Define list of 5 doubles
auto list_meta = ListTypeBuilder()
    .element<double>()
    .count(5)
    .build("DoubleArray5");

// Create and populate
Value list(list_meta.get());
ValueView lv = list.view();
for (size_t i = 0; i < lv.list_size(); ++i) {
    lv.element(i).as<double>() = i * 1.5;
}

// Read
ConstValueView clv = list.const_view();
double val = clv.element(2).as<double>();  // 3.0
```

### Set Values

```cpp
// Define int set
auto set_meta = SetTypeBuilder()
    .element<int>()
    .build("IntSet");

// Create and use
Value set(set_meta.get());
ValueView sv = set.view();

sv.set_add(10);
sv.set_add(20);
sv.set_add(10);  // returns false (duplicate)

// Check contents
ConstValueView csv = set.const_view();
bool has10 = csv.set_contains(10);  // true
size_t count = csv.set_size();      // 2
```

### Dict Values

```cpp
// Define int -> double dict
auto dict_meta = DictTypeBuilder()
    .key<int>()
    .value<double>()
    .build("IntDoubleDict");

// Create and populate
Value dict(dict_meta.get());
ValueView dv = dict.view();

dv.dict_insert(1, 1.1);
dv.dict_insert(2, 2.2);
dv.dict_insert(3, 3.3);

// Access
ConstValueView cdv = dict.const_view();
bool has2 = cdv.dict_contains(2);          // true
ConstValueView v = cdv.dict_get(1);        // view of value 1.1
double val = v.as<double>();               // 1.1
```

## Working with Views

### Mutable Access (ValueView)

```cpp
Value bundle(bundle_meta);
ValueView view = bundle.view();

// Navigate and modify
view.field("x").as<int>() = 42;
view.field("nested").field("value").as<double>() = 3.14;
view.element(0).as<int>() = 100;

// Collection modifications
view.set_add(42);
view.set_remove(10);
view.dict_insert(1, 2.0);
view.dict_remove(3);
```

### Read-Only Access (ConstValueView)

```cpp
ConstValueView cv = bundle.const_view();

// Navigate and read
int x = cv.field("x").as<int>();
double val = cv.field("nested").field("value").as<double>();
int elem = cv.element(0).as<int>();

// Collection queries
size_t size = cv.set_size();
bool has = cv.set_contains(42);
bool exists = cv.dict_contains(key);
ConstValueView entry = cv.dict_get(key);
```

### Type Introspection

```cpp
ConstValueView view = val.const_view();

// Kind checks
if (view.is_scalar()) { ... }
if (view.is_bundle()) { ... }
if (view.is_list()) { ... }
if (view.is_set()) { ... }
if (view.is_dict()) { ... }

// Schema comparison
if (view.is_type(expected_meta)) { ... }
if (view.same_type_as(other_view)) { ... }

// Field/element information
size_t n_fields = view.field_count();
size_t n_elements = view.list_size();
const TypeMeta* elem_type = view.element_type();
const TypeMeta* key_type = view.key_type();
const TypeMeta* val_type = view.value_type();
```

## Type-Safe Access

### Three Access Levels

```cpp
// 1. as<T>() - Debug assertion, zero overhead in release
//    Use when you're confident about the type
int x = view.as<int>();

// 2. try_as<T>() - Returns nullptr on mismatch
//    Use when type might be wrong
if (int* p = view.try_as<int>()) {
    process(*p);
} else {
    handle_type_error();
}

// 3. checked_as<T>() - Throws on mismatch
//    Use when you want exception on error
try {
    int x = view.checked_as<int>();
} catch (const std::runtime_error& e) {
    log_error(e.what());
}
```

### Schema Validation

```cpp
// Check if view matches expected type
if (view.is_scalar_type<int>()) {
    // Safe to use as<int>()
    int x = view.as<int>();
}

// Compare with schema
const TypeMeta* expected = registry.get("Point");
if (view.is_type(expected)) {
    // Process as Point
}
```

## Type Registry

### Registration

```cpp
TypeRegistry registry;

// Register custom type (takes ownership)
auto meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");
const BundleTypeMeta* point_type =
    registry.register_type("Point", std::move(meta));

// Register scalar with external ownership
registry.register_type("MyAlias", scalar_type_meta<int>());
```

### Lookup

```cpp
// Get (returns nullptr if not found)
const TypeMeta* type = registry.get("Point");

// Require (throws if not found)
const TypeMeta* type = registry.require("Point");

// Check existence
if (registry.contains("Point")) { ... }

// List all types
std::vector<std::string> names = registry.type_names();
```

### Pre-registered Types

Built-in scalars available in every registry:
- `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `int`, `long`, `float`, `double`, `size_t`

## Complex Nested Types

### Building Nested Types

```cpp
TypeRegistry registry;

// Level 1: Point
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");
auto* point_type = registry.register_type("Point", std::move(point_meta));

// Level 2: Rectangle using Point
auto rect_meta = BundleTypeBuilder()
    .add_field("top_left", point_type)
    .add_field("bottom_right", point_type)
    .build("Rectangle");
auto* rect_type = registry.register_type("Rectangle", std::move(rect_meta));

// Level 3: List of Rectangles
auto rect_list_meta = ListTypeBuilder()
    .element_type(rect_type)
    .count(10)
    .build("RectangleList10");
auto* rect_list_type = registry.register_type("RectangleList10", std::move(rect_list_meta));
```

### Navigating Nested Data

```cpp
Value canvas(canvas_type);
ValueView cv = canvas.view();

// Deep navigation with chained calls
cv.field("rectangles")
  .element(0)
  .field("top_left")
  .field("x")
  .as<int>() = 100;

// Or step by step
ValueView rects = cv.field("rectangles");
ValueView rect0 = rects.element(0);
ValueView tl = rect0.field("top_left");
tl.field("x").as<int>() = 100;
tl.field("y").as<int>() = 50;
```

## Value Copying

```cpp
// Copy a value
Value original(point_meta);
// ... populate original ...

Value copy = Value::copy(original);
// copy is independent, modifying it doesn't affect original

// Copy from view
ConstValueView view = get_some_view();
Value from_view = Value::copy(view);
```

## Comparison and Hashing

```cpp
Value a(point_meta);
Value b(point_meta);
// ... populate ...

// Equality
bool equal = a.equals(b);
bool same = a.view().equals(b.view());

// Hashing
size_t h1 = a.hash();
size_t h2 = a.view().hash();
```

## String Representation

All value types support `to_string()` for logging and debugging:

### Basic Usage

```cpp
Value val(int_meta);
val.view().as<int>() = 42;

std::string s = val.to_string();  // "42"
```

### Type-Specific Formats

```cpp
// Scalar types
Value bool_val(bool_meta);
bool_val.view().as<bool>() = true;
bool_val.to_string();  // "true"

Value str_val(string_meta);
str_val.view().as<std::string>() = "hello";
str_val.to_string();  // "\"hello\""

// Bundle
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");
Value point(point_meta.get());
point.view().field("x").as<int>() = 10;
point.view().field("y").as<int>() = 20;
point.to_string();  // "{x=10, y=20}"

// List
auto list_meta = ListTypeBuilder().element<int>().count(3).build();
Value list(list_meta.get());
list.view().element(0).as<int>() = 1;
list.view().element(1).as<int>() = 2;
list.view().element(2).as<int>() = 3;
list.to_string();  // "[1, 2, 3]"

// Set
auto set_meta = SetTypeBuilder().element<int>().build();
Value set(set_meta.get());
set.view().set_add(10);
set.view().set_add(20);
set.to_string();  // "{10, 20}"

// Dict
auto dict_meta = DictTypeBuilder().key<int>().value<double>().build();
Value dict(dict_meta.get());
dict.view().dict_insert(1, 1.5);
dict.view().dict_insert(2, 2.5);
dict.to_string();  // "{1: 1.5, 2: 2.5}"

// Window
window.to_string();  // "Window[size=3, newest=42]"

// Ref
ref.to_string();  // "REF[bound: 42]" or "REF[empty]" or "REF[unbound: 3 items]"
```

### TSValue String Representation

Time-series values support both simple and debug formats:

```cpp
TSValue ts(int_meta);
ts.set_value(42, current_time);

// Simple: just the value
ts.to_string();  // "42"

// Debug: includes modification status
ts.to_debug_string(current_time);
// "TS[int64_t]@0x7fff5fbff8c0(value=\"42\", modified=true, last_modified=2025-01-01 12:00:00.000000)"

// TSView uses stored current_time
auto view = ts.view(current_time);
view.to_string();        // "42"
view.to_debug_string();  // "TS[int64_t]@0x...(value=\"42\", modified=true)"
```

### Using TypeMeta Directly

```cpp
// Access via TypeMeta
const TypeMeta* meta = value.schema();
std::string s = meta->to_string_at(value.data());
```

## Type Names (Schema Description)

Get a Python-style description of a type's schema:

```cpp
// Get type description
const TypeMeta* meta = value.schema();
std::string type_desc = meta->type_name_str();
```

### Type Name Formats

Type names use Python naming conventions (e.g., `int64_t` → `int`):

```cpp
// Scalars
scalar_type_meta<bool>()->type_name_str();      // "bool"
scalar_type_meta<int64_t>()->type_name_str();   // "int"
scalar_type_meta<double>()->type_name_str();    // "float"
scalar_type_meta<std::string>()->type_name_str(); // "str"

// Named bundle
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");
point_meta->type_name_str();  // "Point"

// Anonymous bundle
auto anon_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<double>("y")
    .build();  // No name
anon_meta->type_name_str();  // "{x: int, y: float}"

// List (fixed-size tuple)
auto list_meta = ListTypeBuilder()
    .element<int>()
    .count(5)
    .build();
list_meta->type_name_str();  // "Tuple[int, Size[5]]"

// Set
auto set_meta = SetTypeBuilder()
    .element<int>()
    .build();
set_meta->type_name_str();  // "Set[int]"

// Dict
auto dict_meta = DictTypeBuilder()
    .key<std::string>()
    .value<double>()
    .build();
dict_meta->type_name_str();  // "Dict[str, float]"

// Window (fixed)
auto window_meta = WindowTypeBuilder()
    .element<double>()
    .max_count(10)
    .build();
window_meta->type_name_str();  // "Window[float, Size[10]]"

// Window (time-based)
auto time_window_meta = WindowTypeBuilder()
    .element<double>()
    .time_duration(std::chrono::seconds(60))
    .build();
time_window_meta->type_name_str();  // "Window[float, timedelta[seconds=60]]"

// Ref
auto ref_meta = RefTypeBuilder()
    .value_type(int_meta)
    .build();
ref_meta->type_name_str();  // "REF[int]"
```

## Best Practices

### 1. Store Views Carefully

Views are non-owning - ensure the underlying Value outlives them:

```cpp
// GOOD
Value val(meta);
ValueView view = val.view();
process(view);  // val still alive

// BAD - dangling view
ValueView get_view() {
    Value temp(meta);
    return temp.view();  // temp destroyed, view dangles!
}
```

### 2. Use Appropriate Access Method

```cpp
// Known type, performance critical
value.as<int>() = 42;

// Uncertain type, need to handle gracefully
if (auto* p = view.try_as<int>()) {
    use(*p);
}

// Uncertain type, want exception
int x = view.checked_as<int>();
```

### 3. Register Types Early

```cpp
// At program start
TypeRegistry& registry = TypeRegistry::global();
register_all_types(registry);

// During runtime
const TypeMeta* type = registry.require("MyType");
Value val(type);
```

### 4. Use TypeBuilder Fluent API

```cpp
// Clear, readable type definitions
auto complex = BundleTypeBuilder()
    .add_field<int>("id")
    .add_field<double>("value")
    .add_field("nested", inner_type)
    .add_field("items", list_type)
    .build("Complex");
```

## Type Composability

The type system is fully composable. Any type can be used as container elements, keys, or values:

| Container | Element/Key/Value Requirements |
|-----------|-------------------------------|
| **List** | Any type (no restrictions) |
| **Set** | Element must be Hashable + Equatable |
| **Dict Key** | Must be Hashable + Equatable |
| **Dict Value** | Any type (no restrictions) |

### Flag Propagation

Flags propagate through composition:

- **Bundle**: Hashable/Equatable if all fields are
- **List**: Inherits flags from element type
- **Set**: Always Hashable (XOR of element hashes)
- **Dict**: Hashable only if both key AND value are Hashable

### Set of Bundles

```cpp
// Bundles are hashable/equatable if all their fields are
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

auto point_set_meta = SetTypeBuilder()
    .element_type(point_meta.get())  // Bundle as set element
    .build("PointSet");

// Use it
Value point_set(point_set_meta.get());
ValueView psv = point_set.view();

// Add points to the set
Value p1(point_meta.get());
p1.view().field("x").as<int>() = 10;
p1.view().field("y").as<int>() = 20;
psv.set_add(p1.data());  // Add via raw pointer

// Or use SetView directly for typed access
SetView sv(static_cast<const SetTypeMeta*>(point_set_meta.get()));
// ... populate ...
```

### Dict with Bundle Keys and List Values

```cpp
auto int_list_meta = ListTypeBuilder()
    .element<int>()
    .count(5)
    .build("IntList5");

auto point_to_list_meta = DictTypeBuilder()
    .key_type(point_meta.get())      // Bundle as dict key
    .value_type(int_list_meta.get()) // List as dict value
    .build("PointToListMap");

Value map(point_to_list_meta.get());
ValueView mv = map.view();

// Insert: point -> list of ints
// (Use DictStorage directly for complex key types)
```

### List of Sets

```cpp
auto point_set_meta = SetTypeBuilder()
    .element_type(point_meta.get())
    .build("PointSet");

auto set_list_meta = ListTypeBuilder()
    .element_type(point_set_meta.get())  // Set as list element
    .count(10)
    .build("PointSetList10");

Value set_list(set_list_meta.get());
ValueView slv = set_list.view();

// Access individual sets
ValueView first_set = slv.element(0);
// first_set is now a view of a SetStorage
```

### Deeply Nested Types

```cpp
// Dict mapping int -> List of Set of Points
auto point_set_list_meta = ListTypeBuilder()
    .element_type(point_set_meta.get())
    .count(5)
    .build();

auto nested_meta = DictTypeBuilder()
    .key<int>()
    .value_type(point_set_list_meta.get())
    .build("NestedContainer");

Value nested(nested_meta.get());
ValueView nv = nested.view();

// Insert entry: int key -> list of point sets
nv.dict_insert(1, /* list value */);

// Navigate deeply
ConstValueView cv = nested.const_view();
ConstValueView list_val = cv.dict_get(1);        // Get list
ConstValueView set_val = list_val.element(0);    // Get first set
size_t set_size = set_val.set_size();            // Query set
```

## Common Patterns

### Factory Pattern

```cpp
Value create_default_point(const TypeMeta* point_type) {
    Value p(point_type);
    p.view().field("x").as<int>() = 0;
    p.view().field("y").as<int>() = 0;
    return p;
}
```

### Visitor Pattern

```cpp
void print_value(ConstValueView view) {
    switch (view.kind()) {
        case TypeKind::Scalar:
            if (view.is_scalar_type<int>()) {
                std::cout << view.as<int>();
            } else if (view.is_scalar_type<double>()) {
                std::cout << view.as<double>();
            }
            break;
        case TypeKind::Bundle:
            std::cout << "{";
            for (size_t i = 0; i < view.field_count(); ++i) {
                if (i > 0) std::cout << ", ";
                print_value(view.field(i));
            }
            std::cout << "}";
            break;
        // ... etc
    }
}
```

### Generic Processing

```cpp
void copy_if_same_type(ValueView dest, ConstValueView src) {
    if (dest.same_type_as(src)) {
        dest.copy_from(src);
    }
}
```

## Modification Tracking

The `ModificationTracker` provides a parallel data structure for tracking when value elements were last modified. This is essential for implementing time-series values.

### Basic Usage

```cpp
#include <hgraph/types/value/modification_tracker.h>
using namespace hgraph;
using namespace hgraph::value;

// Create tracker for a type
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

ModificationTrackerStorage storage(point_meta.get());
ModificationTracker tracker = storage.tracker();

// Get current time from evaluation clock
engine_time_t current_time = /* from evaluation clock */;

// Mark as modified
tracker.mark_modified(current_time);

// Query modification state
if (tracker.modified_at(current_time)) {
    // Value was modified at this time
}

if (tracker.valid_value()) {
    // Value has been modified at least once
}

// Reset to unmodified
tracker.mark_invalid();
```

### Bundle Field Tracking

Fields are accessible by both index and name. **Field indices correspond to schema creation order**:

```cpp
// Schema defines field order:
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")    // index 0
    .add_field<int>("y")    // index 1
    .build();

ModificationTrackerStorage storage(point_meta.get());
ModificationTracker tracker = storage.tracker();

// Access by name
tracker.field("x").mark_modified(current_time);

// Access by index (equivalent to above)
tracker.field(0).mark_modified(current_time);

// Query by index
if (tracker.field_modified_at(0, current_time)) {
    // Field "x" (index 0) was modified
}

// Bundle is also marked modified (hierarchical propagation)
if (tracker.modified_at(current_time)) {
    // Bundle was modified (because field was modified)
}
```

### List Element Tracking

```cpp
auto list_meta = ListTypeBuilder()
    .element<int>()
    .count(5)
    .build();

ModificationTrackerStorage storage(list_meta.get());
ModificationTracker tracker = storage.tracker();

// Mark element modified
tracker.element(2).mark_modified(current_time);

// Query element modification
if (tracker.element_modified_at(2, current_time)) {
    // Element 2 was modified
}

// List itself is marked modified
if (tracker.modified_at(current_time)) {
    // List was modified
}
```

### Set Tracking (Atomic)

Sets are tracked atomically - one timestamp for the entire set:

```cpp
auto set_meta = SetTypeBuilder()
    .element<int>()
    .build();

ModificationTrackerStorage storage(set_meta.get());
ModificationTracker tracker = storage.tracker();

// Mark set as modified (add/remove occurred)
tracker.mark_modified(current_time);

if (tracker.modified_at(current_time)) {
    // Set was modified at this time
}
```

### Dict Tracking (Structural + Entry)

Dicts track both structural changes (key add/remove) and per-entry modifications:

```cpp
auto dict_meta = DictTypeBuilder()
    .key<int>()
    .value<double>()
    .build();

ModificationTrackerStorage storage(dict_meta.get());
ModificationTracker tracker = storage.tracker();

// Structural modification (key added)
tracker.mark_modified(current_time);

// Entry value modification (key already exists)
tracker.mark_dict_entry_modified(0, current_time);  // Entry at index 0

// Query structural vs entry modification
if (tracker.structurally_modified_at(current_time)) {
    // A key was added or removed
}

if (tracker.dict_entry_modified_at(0, current_time)) {
    // Entry 0's value was modified
}

// Get entry's last modification time
engine_time_t entry_time = tracker.dict_entry_last_modified(0);

// Remove tracking for deleted entry
tracker.remove_dict_entry_tracking(0);
```

### Hierarchical Propagation

When a child element is modified, the parent is automatically marked modified:

```cpp
// Nested bundle: Outer { id: int, point: Inner { x: int, y: int } }
auto inner_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Inner");

auto outer_meta = BundleTypeBuilder()
    .add_field<int>("id")
    .add_field("point", inner_meta.get())
    .build("Outer");

ModificationTrackerStorage storage(outer_meta.get());
ModificationTracker tracker = storage.tracker();

// Modify inner field
tracker.field("point").mark_modified(current_time);

// Outer bundle is also marked modified
assert(tracker.modified_at(current_time));
```

### Time Monotonicity

Modification times only advance forward:

```cpp
tracker.mark_modified(make_time(200));  // Sets to 200
tracker.mark_modified(make_time(100));  // Ignored (earlier time)
tracker.mark_modified(make_time(300));  // Updates to 300

assert(tracker.last_modified_time() == make_time(300));
```

### Move Semantics

```cpp
// Storage is move-only
ModificationTrackerStorage storage1(point_meta.get());
storage1.tracker().mark_modified(current_time);

ModificationTrackerStorage storage2 = std::move(storage1);
// storage1 is now invalid
// storage2 has the tracked modification
```

## Time-Series Values

The `TSValue` combines value storage and modification tracking into a unified container with automatic modification propagation. This is the foundation for implementing TS, TSB, TSL, TSS, TSD.

### Basic Usage

```cpp
#include <hgraph/types/value/time_series_value.h>
using namespace hgraph;
using namespace hgraph::value;

// Create a time-series scalar
const TypeMeta* int_meta = scalar_type_meta<int>();
TSValue ts(int_meta);

// Initial state - no value, never modified
assert(!ts.has_value());
assert(!ts.modified_at(current_time));

// Set value at current time
ts.set_value(42, current_time);

// Now has value and is modified
assert(ts.has_value());
assert(ts.modified_at(current_time));
assert(ts.as<int>() == 42);

// Read-only access
ConstValueView val = ts.value();
int x = val.as<int>();  // 42
```

### Using TSView

The `TSView` provides auto-tracking - modifications are automatically recorded:

```cpp
TSValue ts(int_meta);
auto t1 = make_time(100);

// Get view with current time
auto view = ts.view(t1);

// set() automatically marks modified at t1
view.set(42);

assert(ts.modified_at(t1));
assert(ts.as<int>() == 42);
```

### Bundle Time-Series (TSB Pattern)

Track modifications at field granularity:

```cpp
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

TSValue ts_point(point_meta.get());

auto t1 = make_time(100);
auto t2 = make_time(200);

// At t1, modify field "x"
auto view1 = ts_point.view(t1);
view1.field("x").set(10);

// Field "x" is modified, bundle is modified (propagation)
assert(view1.field_modified_at(0, t1));  // by index
assert(ts_point.modified_at(t1));

// Field "y" is NOT modified at t1
assert(!view1.field_modified_at(1, t1));

// At t2, modify field "y"
auto view2 = ts_point.view(t2);
view2.field("y").set(20);

// Only field "y" modified at t2
assert(!view2.field_modified_at(0, t2));  // x not modified at t2
assert(view2.field_modified_at(1, t2));    // y modified at t2

// Read values
assert(ts_point.value().field("x").as<int>() == 10);
assert(ts_point.value().field("y").as<int>() == 20);
```

### Access by Index and Name

Bundle fields support access by both index and name. **Field indices match schema creation order**:

```cpp
auto meta = BundleTypeBuilder()
    .add_field<int>("first")     // index 0
    .add_field<double>("second") // index 1
    .add_field<std::string>("third") // index 2
    .build();

TSValue ts(meta.get());
auto view = ts.view(current_time);

// By name
view.field("first").set(100);

// By index (equivalent)
view.field(0).set(100);

// Mix and match
view.field(1).set(2.5);        // index
view.field("third").set(std::string("hello")); // name
```

### List Time-Series (TSL Pattern)

Track modifications at element granularity:

```cpp
auto list_meta = ListTypeBuilder()
    .element<int>()
    .count(5)
    .build();

TSValue ts_list(list_meta.get());

auto t1 = make_time(100);
auto view = ts_list.view(t1);

// Modify element 2
view.element(2).set(42);

// Element 2 modified, list modified
assert(view.element_modified_at(2, t1));
assert(ts_list.modified_at(t1));

// Other elements NOT modified
assert(!view.element_modified_at(0, t1));
assert(!view.element_modified_at(1, t1));
```

### Set Time-Series (TSS Pattern)

Sets use atomic tracking - one timestamp for the entire set:

```cpp
auto set_meta = SetTypeBuilder()
    .element<int>()
    .build();

TSValue ts_set(set_meta.get());
auto view = ts_set.view(current_time);

// add() returns true if element was added
bool added = view.add(10);  // true
assert(ts_set.modified_at(current_time));

view.add(20);
view.add(30);

// Duplicate returns false, set NOT modified again
added = view.add(10);  // false (already exists)

// Check contents
assert(view.contains(10));
assert(view.contains(20));
assert(!view.contains(99));
assert(view.set_size() == 3);

// remove() returns true if element was removed
bool removed = view.remove(20);  // true
assert(view.set_size() == 2);
```

### Dict Time-Series (TSD Pattern)

Dicts track structural changes and per-entry modifications:

```cpp
auto dict_meta = DictTypeBuilder()
    .key<std::string>()
    .value<int>()
    .build();

TSValue ts_dict(dict_meta.get());
auto view = ts_dict.view(current_time);

// Insert new key - structural modification
view.insert(std::string("a"), 100);

assert(ts_dict.modified_at(current_time));
assert(view.dict_contains(std::string("a")));
assert(view.dict_get(std::string("a")).as<int>() == 100);

// Insert more
view.insert(std::string("b"), 200);
view.insert(std::string("c"), 300);

// Update existing key
view.insert(std::string("a"), 150);  // Updates value

// Read
assert(view.dict_get(std::string("a")).as<int>() == 150);
assert(view.dict_size() == 3);

// Remove key
bool removed = view.dict_remove(std::string("b"));  // true
assert(view.dict_size() == 2);
assert(!view.dict_contains(std::string("b")));
```

### Nested Bundles

Navigation works hierarchically with propagation:

```cpp
auto inner_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Inner");

auto outer_meta = BundleTypeBuilder()
    .add_field<std::string>("name")
    .add_field("point", inner_meta.get())
    .build("Outer");

TSValue ts(outer_meta.get());
auto view = ts.view(current_time);

// Set outer field
view.field("name").set(std::string("test"));

// Navigate to nested field
view.field("point").field("x").set(10);
view.field("point").field("y").set(20);

// All are modified
assert(ts.modified_at(current_time));
assert(view.field_modified_at(0, current_time));  // name
assert(view.field_modified_at(1, current_time));  // point (propagated)

// Read nested values
assert(ts.value().field("name").as<std::string>() == "test");
assert(ts.value().field("point").field("x").as<int>() == 10);
```

### Invalidating Values

Mark a value as invalid (no longer has a valid value):

```cpp
TSValue ts(int_meta);
ts.set_value(42, current_time);
assert(ts.has_value());

ts.mark_invalid();
assert(!ts.has_value());
```

### Move Semantics

`TSValue` is move-only:

```cpp
TSValue ts1(int_meta);
ts1.set_value(42, current_time);

// Move construct
TSValue ts2 = std::move(ts1);
// ts1 is now invalid
// ts2 has the value and modification history

assert(!ts1.valid());
assert(ts2.valid());
assert(ts2.as<int>() == 42);
```

### Raw Access

For advanced use cases, access underlying components:

```cpp
TSValue ts(point_meta.get());
auto view = ts.view(current_time);

// View internals
ValueView raw_value = view.value_view();
ModificationTracker raw_tracker = view.tracker();
engine_time_t time = view.current_time();

// TSValue internals
Value& underlying_val = ts.underlying_value();
ModificationTrackerStorage& underlying_tracker = ts.underlying_tracker();
```

### Best Practices

1. **Always pass current time to view()**: The view needs the evaluation time for correct modification tracking.

```cpp
// GOOD
auto view = ts.view(current_time);
view.set(42);

// BAD - no time context
// (TSValue::set_value() exists for convenience)
ts.set_value(42, current_time);
```

2. **Check has_value() before reading**: A time-series may not have been set yet.

```cpp
if (ts.has_value()) {
    int x = ts.as<int>();
}
```

3. **Use appropriate granularity**:
   - Scalar: Single value tracking
   - Bundle: Per-field when you need to know which fields changed
   - List: Per-element when you need to know which indices changed
   - Set: Atomic when you track structural changes only
   - Dict: Structural + entry when you need both

4. **Views are non-owning**: Ensure the `TSValue` outlives any views:

```cpp
// GOOD
TSValue ts(meta);
auto view = ts.view(current_time);
process(view);  // ts still alive

// BAD - dangling view
auto get_view() {
    TSValue temp(meta);
    return temp.view(current_time);  // temp destroyed!
}
```

## Observer/Notification System

The `TSValue` supports an observer pattern for change notification. When values are modified, subscribed observers are notified.

### Creating an Observer

Implement the `Notifiable` interface:

```cpp
#include <hgraph/types/value/observer_storage.h>
using namespace hgraph::value;

struct MyObserver : Notifiable {
    int notification_count = 0;
    engine_time_t last_notification_time{MIN_DT};

    void notify(engine_time_t time) override {
        ++notification_count;
        last_notification_time = time;
    }
};
```

### Basic Subscription

Subscribe an observer to a `TSValue`:

```cpp
TSValue ts(int_meta);
MyObserver observer;

// Subscribe at root level
ts.subscribe(&observer);

// Modifications trigger notification
auto t1 = make_time(100);
ts.set_value(42, t1);

assert(observer.notification_count == 1);
assert(observer.last_notification_time == t1);

// Another modification at same time - still notified
ts.set_value(43, t1);
assert(observer.notification_count == 2);

// Unsubscribe
ts.unsubscribe(&observer);

// No longer notified
ts.set_value(44, t1);
assert(observer.notification_count == 2);  // Unchanged
```

### Using View for Notifications

Notifications are also triggered through `TSView`:

```cpp
TSValue ts(int_meta);
MyObserver observer;
ts.subscribe(&observer);

auto t1 = make_time(100);
auto view = ts.view(t1);

// set() on view triggers notification
view.set(42);
assert(observer.notification_count == 1);
```

### Multiple Observers

Multiple observers can subscribe to the same value:

```cpp
TSValue ts(int_meta);
MyObserver observer1, observer2;

ts.subscribe(&observer1);
ts.subscribe(&observer2);

ts.set_value(42, current_time);

// Both notified
assert(observer1.notification_count == 1);
assert(observer2.notification_count == 1);
```

### Bundle Field Notifications

When a bundle field is modified, the bundle's observer is notified:

```cpp
auto point_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Point");

TSValue ts(point_meta.get());
MyObserver observer;
ts.subscribe(&observer);

auto view = ts.view(current_time);

// Modifying any field notifies the bundle observer
view.field("x").set(10);
assert(observer.notification_count == 1);

view.field("y").set(20);
assert(observer.notification_count == 2);
```

### List Element Notifications

List element modifications notify the list observer:

```cpp
auto list_meta = ListTypeBuilder()
    .element<int>()
    .count(5)
    .build();

TSValue ts(list_meta.get());
MyObserver observer;
ts.subscribe(&observer);

auto view = ts.view(current_time);

view.element(0).set(100);
assert(observer.notification_count == 1);

view.element(2).set(200);
assert(observer.notification_count == 2);
```

### Set Operation Notifications

Set mutations (add/remove) notify when the set changes:

```cpp
auto set_meta = SetTypeBuilder()
    .element<int>()
    .build();

TSValue ts(set_meta.get());
MyObserver observer;
ts.subscribe(&observer);

auto view = ts.view(current_time);

// Adding new element notifies
view.add(10);
assert(observer.notification_count == 1);

// Adding duplicate does NOT notify (set unchanged)
view.add(10);
assert(observer.notification_count == 1);

// Removing existing element notifies
view.remove(10);
assert(observer.notification_count == 2);

// Removing non-existent does NOT notify
view.remove(999);
assert(observer.notification_count == 2);
```

### Dict Operation Notifications

Dict mutations notify for structural changes:

```cpp
auto dict_meta = DictTypeBuilder()
    .key<int>()
    .value<double>()
    .build();

TSValue ts(dict_meta.get());
MyObserver observer;
ts.subscribe(&observer);

auto view = ts.view(current_time);

// Insert new key - structural change
view.insert(1, 10.5);
assert(observer.notification_count == 1);

// Update existing key - also notifies (value changed)
view.insert(1, 20.5);
assert(observer.notification_count == 2);

// Remove existing key
view.dict_remove(1);
assert(observer.notification_count == 3);
```

### Nested Structure Propagation

Modifications at any level propagate upward to all ancestor observers:

```cpp
auto inner_meta = BundleTypeBuilder()
    .add_field<int>("x")
    .add_field<int>("y")
    .build("Inner");

auto outer_meta = BundleTypeBuilder()
    .add_field<int>("id")
    .add_field("point", inner_meta.get())
    .build("Outer");

TSValue ts(outer_meta.get());
MyObserver observer;
ts.subscribe(&observer);

auto view = ts.view(current_time);

// Direct field modification
view.field("id").set(1);
assert(observer.notification_count == 1);

// Nested modification - propagates to root
view.field("point").field("x").set(10);
assert(observer.notification_count == 2);

view.field("point").field("y").set(20);
assert(observer.notification_count == 3);
```

### Lazy Allocation

Observer storage is only allocated when first subscription is made:

```cpp
TSValue ts(int_meta);

// No overhead - _observers is nullptr
assert(!ts.has_observers());

MyObserver observer;
ts.subscribe(&observer);

// Now allocated
assert(ts.has_observers());
```

### Observer Best Practices

1. **Subscribe before modifications**: Ensure observers are subscribed before values are modified to receive all notifications.

2. **Unsubscribe when done**: Always unsubscribe observers to prevent memory leaks and unwanted notifications.

```cpp
void process(TSValue& ts) {
    MyObserver observer;
    ts.subscribe(&observer);

    // ... do work ...

    ts.unsubscribe(&observer);  // Don't forget!
}
```

3. **Observer lifetime**: Ensure observers outlive their subscription period. Unsubscribe before destroying the observer.

```cpp
// GOOD
{
    MyObserver observer;
    ts.subscribe(&observer);
    // ... use ...
    ts.unsubscribe(&observer);
}  // observer destroyed safely

// BAD
{
    MyObserver* observer = new MyObserver();
    ts.subscribe(observer);
    delete observer;  // Still subscribed!
    // ts may crash when notifying
}
```

4. **Avoid re-entrancy**: Don't modify the same `TSValue` from within a `notify()` callback, as this may cause recursive notifications.

---

## Window Types (TSW)

Windows store a history of values with associated timestamps. There are two types:

- **Fixed-length window**: Cyclic buffer that stores the N most recent values
- **Variable-length window**: Time-based queue that stores values within a time duration

### Creating Window Types

```cpp
using namespace hgraph::value;

// Fixed-length window: stores last 5 values
auto fixed_window_meta = WindowTypeBuilder()
    .element_type(double_meta)    // Element type
    .max_count(5)                 // Keep last 5 values
    .build();

// Variable-length window: stores values from last 60 seconds
auto variable_window_meta = WindowTypeBuilder()
    .element_type(double_meta)
    .window_duration(std::chrono::seconds(60))
    .build();
```

### Using Windows with Value

```cpp
// Create a fixed-length window value
Value window_val(fixed_window_meta.get());
auto view = window_val.view();

// Push values with timestamps
engine_time_t t1 = /* some time */;
view.window_push(1.0, t1);
view.window_push(2.0, t1 + std::chrono::seconds(1));
view.window_push(3.0, t1 + std::chrono::seconds(2));

// Read values (index 0 = oldest, size-1 = newest)
assert(view.window_size() == 3);
assert(view.window_get(0).as<double>() == 1.0);  // Oldest
assert(view.window_get(2).as<double>() == 3.0);  // Newest

// Get timestamps
auto oldest_ts = view.window_oldest_timestamp();
auto newest_ts = view.window_newest_timestamp();
```

### Fixed-Length Window (Cyclic Buffer)

When a fixed-length window reaches capacity, new values overwrite the oldest:

```cpp
auto meta = WindowTypeBuilder()
    .element_type(int_meta)
    .max_count(3)
    .build();

Value val(meta.get());
auto view = val.view();

// Fill the window
view.window_push(1, t1);
view.window_push(2, t2);
view.window_push(3, t3);
assert(view.window_size() == 3);
assert(view.window_full());

// Push more - oldest is overwritten
view.window_push(4, t4);
assert(view.window_size() == 3);  // Still 3
assert(view.window_get(0).as<int>() == 2);  // 1 was evicted
assert(view.window_get(2).as<int>() == 4);  // Newest
```

### Variable-Length Window (Time-Based)

Variable-length windows automatically evict values older than the window duration:

```cpp
auto meta = WindowTypeBuilder()
    .element_type(int_meta)
    .window_duration(std::chrono::seconds(10))  // 10-second window
    .build();

Value val(meta.get());
auto view = val.view();

engine_time_t base = /* start time */;
view.window_push(1, base);
view.window_push(2, base + std::chrono::seconds(5));
view.window_push(3, base + std::chrono::seconds(12));  // Evicts value at 'base'

assert(view.window_size() == 2);  // Only values within 10s of newest
assert(view.window_get(0).as<int>() == 2);  // base+5s is within range
```

### Window Compaction

For optimal read performance, use `compact()` to reorganize internal storage:

```cpp
// After many push operations on a fixed-length window,
// internal indices may wrap around
view.window_compact(current_time);  // Reorganizes to linear layout

// For variable windows, compact also removes expired entries
view.window_evict_expired(current_time);  // Explicit expiration
```

### Window with TSValue

Windows integrate with the time-series system for modification tracking and observers:

```cpp
TSValue ts(fixed_window_meta.get());

// Subscribe to changes
MyObserver observer;
ts.subscribe(&observer);

auto view = ts.view(current_time);

// Push triggers modification tracking and notification
view.window_push(42.0, timestamp);
assert(ts.modified_at(current_time));
assert(observer.notification_count == 1);

// Access window state
assert(view.window_size() == 1);
assert(view.window_get(0).as<double>() == 42.0);

// Clear the window
view.window_clear();
assert(view.window_empty());
assert(observer.notification_count == 2);
```

### Window Operations Summary

| Operation | Description |
|-----------|-------------|
| `window_push(val, ts)` | Add value with timestamp |
| `window_get(index)` | Get value at index (0 = oldest) |
| `window_timestamp(index)` | Get timestamp at index |
| `window_size()` | Current number of entries |
| `window_empty()` | True if no entries |
| `window_full()` | True if at capacity (fixed only) |
| `window_oldest_timestamp()` | Timestamp of oldest entry |
| `window_newest_timestamp()` | Timestamp of newest entry |
| `window_compact(time)` | Optimize for reading |
| `window_evict_expired(time)` | Remove expired entries (variable) |
| `window_clear()` | Remove all entries |
| `window_is_fixed_length()` | True if fixed-length type |
| `window_is_variable_length()` | True if variable-length type |

### Window Type Queries

```cpp
const auto* meta = value.schema();
if (meta->kind == TypeKind::Window) {
    const auto* window_meta = static_cast<const WindowTypeMeta*>(meta);

    if (window_meta->is_fixed_length()) {
        std::cout << "Fixed window, max " << window_meta->max_count << " entries\n";
    } else {
        std::cout << "Variable window, duration "
                  << window_meta->window_duration.count() << "ns\n";
    }

    std::cout << "Element type: " << window_meta->element_type->name << "\n";
}
```

## Python Conversion Support

The `python_conversion.h` header provides bidirectional conversion between C++ values and Python objects using nanobind.

### Including Python Support

```cpp
#include <hgraph/types/value/python_conversion.h>
using namespace hgraph::value;
```

### Standard Scalar Types

Named accessor functions provide TypeMeta for standard hgraph scalar types with Python conversion:

| Function | C++ Type | Python Type |
|----------|----------|-------------|
| `bool_type()` | `bool` | `bool` |
| `int_type()` | `int64_t` | `int` |
| `float_type()` | `double` | `float` |
| `date_type()` | `engine_date_t` | `datetime.date` |
| `date_time_type()` | `engine_time_t` | `datetime.datetime` |
| `time_delta_type()` | `engine_time_delta_t` | `datetime.timedelta` |
| `object_type()` | `nb::object` | any Python object |

```cpp
// Get type metadata with Python support
const TypeMeta* int_meta = int_type();
const TypeMeta* dt_meta = date_time_type();

// Or lookup by name
const TypeMeta* meta = scalar_type_by_name("float");  // Returns float_type()
```

### Converting Values to/from Python

Use `value_to_python()` and `value_from_python()` for conversion:

```cpp
// C++ to Python
Value val(int_type());
val.view().as<int64_t>() = 42;
nb::object py_obj = value_to_python(val.data(), val.schema());
// py_obj is now Python int 42

// Python to C++
nb::object py_int = nb::cast(123);
Value result(int_type());
value_from_python(result.data(), py_int, result.schema());
// result now contains 123
```

### Building Types with Python Support

Use the `*WithPython` builder variants for composite types:

```cpp
// Bundle with Python support
auto point_meta = BundleTypeBuilderWithPython()
    .add_field<int64_t>("x")
    .add_field<int64_t>("y")
    .build("Point");

// Converts to/from Python dict: {"x": 10, "y": 20}

// List with Python support
auto list_meta = ListTypeBuilderWithPython()
    .element<double>()
    .count(5)
    .build();

// Converts to/from Python list: [1.0, 2.0, 3.0, 4.0, 5.0]

// Set with Python support
auto set_meta = SetTypeBuilderWithPython()
    .element<int64_t>()
    .build();

// Converts to/from Python set: {1, 2, 3}

// Dict with Python support
auto dict_meta = DictTypeBuilderWithPython()
    .key<int64_t>()
    .value<double>()
    .build();

// Converts to/from Python dict: {1: 1.5, 2: 2.5}

// Window with Python support
auto window_meta = WindowTypeBuilderWithPython()
    .element<double>()
    .fixed_count(10)
    .build();

// Converts to/from Python list of tuples: [(timestamp_ns, value), ...]
```

### Custom Scalar Types

To add Python support for a custom scalar type:

```cpp
// Use the templated accessor
const TypeMeta* my_type = scalar_type_meta_with_python<MyType>();

// Requires that nb::cast<MyType>() works (nanobind type caster registered)
```

### Conversion Mappings

| C++ Type Kind | Python Type |
|---------------|-------------|
| Scalar | Uses `nb::cast<T>()` |
| Bundle | `dict` (field names as keys) |
| List | `list` |
| Set | `set` |
| Dict | `dict` |
| Window | `list` of `(timestamp_ns, value)` tuples |
| Ref | Dereferenced value, or `list` for unbound refs |

### Type Registry Integration

```cpp
TypeRegistry registry;

// Register types with Python support
registry.register_type("Point", BundleTypeBuilderWithPython()
    .add_field<int64_t>("x")
    .add_field<int64_t>("y")
    .build("Point"));

// Use for conversion
const TypeMeta* point_type = registry.require("Point");
Value point(point_type);
// ... populate ...
nb::object py_dict = value_to_python(point.data(), point_type);
```
