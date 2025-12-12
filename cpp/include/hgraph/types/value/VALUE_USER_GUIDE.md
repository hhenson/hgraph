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
