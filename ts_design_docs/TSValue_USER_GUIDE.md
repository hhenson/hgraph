# Time-Series Value (TSValue) - User Guide

**Version**: 0.1 (Draft)
**Date**: 2025-01-05
**Related**: [TSValue_DESIGN.md](TSValue_DESIGN.md) - Design Document

---

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Time-Series Schema System](#2-time-series-schema-system)
3. [Creating TSValues](#3-creating-tsvalues)
4. [TSViews - Reading Values](#4-tsviews---reading-values)
5. [Node Storage and View Creation](#5-node-storage-and-view-creation)
6. [Binding and Notification](#6-binding-and-notification)
7. [Working with Bundles (TSB)](#7-working-with-bundles-tsb)
8. [Path-Based Navigation](#8-path-based-navigation)
9. [Python Interop](#9-python-interop)
10. [Policies and Customization](#10-policies-and-customization)
11. [Migration from Current Types](#11-migration-from-current-types)

---

## 1. Getting Started

The TSValue type system provides type-erased time-series values for the hgraph runtime. It builds on the Value type system (using the existing policy infrastructure), adding time-series semantics (modification tracking, notification, binding).

**Basic Concepts:**

| Concept | Description |
|---------|-------------|
| **TSValue** | Extends Value with policies, owns time-series storage |
| **TSMutableView** | Mutable view for outputs - allows setting values |
| **TSView** | Read-only view for inputs - base view with `as_xxx()` casting |
| **TSBView** | Bundle-specific view with field navigation |

**Key Difference from Value:**
- `Value` is just data with a schema
- `TSValue` extends `Value<CombinedPolicy<WithPythonCache, WithModificationTracking>>` adding:
  - Time-series schema (`TSMeta*`)
  - Node ownership tracking (`Node*`)
  - View creation methods (`view()`, `mutable_view()`, `bundle_view()`)

**Relationship to Existing Types:**

| Current Type | New Equivalent |
|--------------|----------------|
| `TimeSeriesValueOutput` | `TSValue` stored in Node, accessed via `TSMutableView` |
| `TimeSeriesValueInput` (peered) | `TSView` bound to external output |
| `TimeSeriesBundleInput` (non-peered) | `TSValue` stored in Node, accessed via `TSBView` |

**Node storage pattern:**
- **Single input**: `std::optional<TSValue>` - always TSB, accessed via `bundle_view()`
- **Single output**: `std::optional<TSValue>` - any TS type, accessed via `mutable_view()`
- **Error/State outputs**: Optional additional TSValue members
- **Peered inputs**: Just `TSView` bound to upstream output

---

## 2. Time-Series Schema System

The time-series system uses a **dual-schema** approach:

1. **Scalar Schema** (`TypeMeta`) - Describes atomic data types (int, double, str, etc.)
2. **Time-Series Schema** (`TSTypeMeta`) - Describes time-series structure and modification tracking

### 2.1 Time-Series Hierarchy

| Type | Description | Element Type |
|------|-------------|--------------|
| `TS<T>` | **Base case** - time-series of atomic type T | Scalar (atomic) |
| `TSB[...]` | Bundle with named time-series fields | Each field is a TS |
| `TSL<TS<T>>` | List of time-series values | Elements are TS |
| `TSD<K, TS<V>>` | Dict mapping scalar keys to time-series values | Values are TS |
| `TSS<T>` | Set time-series (set of scalar values) | Scalars (atomic) |
| `TSW<TS<T>>` | Window over a time-series (fixed or time-based) | Underlying TS |
| `REF<ts>` | Reference to another time-series | Any TS schema |

**Key Point:** `TSB[price: TS[float]]` contains a **time-series** `price`, not a scalar float. Each time-series element can tick independently with its own modification tracking.

**Note on TSS vs TSL:**
- `TSS<T>` holds a **set of scalar values** - the set itself is the time-series
- `TSL<TS<T>>` holds a **list of time-series** - each element is independently tracked

### 2.2 Why Two Schemas?

Each TS schema node represents a point where:
- Modification can be independently tracked
- Observers can subscribe for notifications

For `TSB[price: TS[float], volume: TS[int]]`:
- The bundle itself tracks when any field was modified
- `price` tracks when the price was modified
- `volume` tracks when the volume was modified

The scalar schema describes atomic data storage; the TS schema describes the time-series overlay.

### 2.3 Creating Time-Series Schemas - Compile-Time API

The compile-time API uses C++20 templates for type-safe schema construction:

```cpp
#include <hgraph/types/type_api.h>

using namespace hgraph::types;

// TS<T> - scalar time-series
auto* ts_int = ts_type<TS<int>>();
auto* ts_double = ts_type<TS<double>>();
auto* ts_str = ts_type<TS<std::string>>();

// TSB[...] - bundle with named fields (C++20 string literal template params)
auto* point = ts_type<TSB<
    Field<"x", TS<int>>,
    Field<"y", TS<float>>,
    Name<"Point">
>>();

// Nested bundles
auto* trade = ts_type<TSB<
    Field<"quote", TSB<Field<"price", TS<float>>, Field<"volume", TS<int>>>>,
    Field<"symbol", TS<std::string>>
>>();

// Collection types
auto* tsl = ts_type<TSL<TS<int>, 3>>();       // List of 3 TS<int>
auto* tsd = ts_type<TSD<std::string, TS<int>>>();  // Dict with string keys
auto* tss = ts_type<TSS<int>>();              // Set of ints

// Windows
auto* fixed_window = ts_type<TSW<float, 10>>();                // Last 10 values
auto* time_window = ts_type<TSW_Time<float, Seconds<60>>>();   // 60-second window

// References
auto* ref = ts_type<REF<TS<int>>>();
```

### 2.4 Creating Time-Series Schemas - Runtime API

For dynamic construction (e.g., from Python type info), use the runtime API:

```cpp
#include <hgraph/types/type_api.h>

using namespace hgraph::types;

// TS<T> - from scalar TypeMeta
auto* ts_int = runtime::ts(value::int_type());
auto* ts_float = runtime::ts(value::float_type());

// TSB[...] - from field definitions
auto* point = runtime::tsb({
    {"x", ts_int},
    {"y", ts_float}
}, "Point");  // Optional name

// Collection types
auto* tsl = runtime::tsl(ts_int, 3);                    // TSL<TS<int>, 3>
auto* tsd = runtime::tsd(value::str_type(), ts_int);   // TSD<str, TS<int>>
auto* tss = runtime::tss(value::int_type());           // TSS<int>

// Windows
auto* fixed_window = runtime::tsw(value::float_type(), 10, 0);          // Count-based
auto* time_window = runtime::tsw_time(value::float_type(), 60'000'000); // 60s in microseconds

// References and signal
auto* ref = runtime::ref(ts_int);
auto* signal = runtime::signal();
```

### 2.5 Extracting the Scalar Schema

Every TS schema (TSMeta) provides its underlying scalar schema for Value storage:

```cpp
// Get the scalar schema for data storage
const value::TypeMeta* scalar_schema = ts_int->value_schema();

// For bundles, this returns the corresponding bundle TypeMeta
const value::TypeMeta* bundle_scalar = point->value_schema();

// Navigate to nested types
const TSMeta* x_field = point->field_meta("x");  // TSMeta for TS<int>
const TSMeta* element = tsl->element_meta();     // TSMeta for element type
```

### 2.6 Schema Interning

Schemas are automatically interned - same type = same pointer:

```cpp
auto* ts1 = ts_type<TS<int>>();
auto* ts2 = ts_type<TS<int>>();
assert(ts1 == ts2);  // Same pointer - interned!

auto* ts3 = runtime::ts(value::int_type());
assert(ts1 == ts3);  // Both APIs return same interned type
```

### 2.7 Schema Hierarchy

| Time-Series Type | Compile-Time API | Runtime API | Value Schema |
|------------------|------------------|-------------|--------------|
| `TS<int>` | `ts_type<TS<int>>()` | `runtime::ts(int_meta)` | `scalar<int>` |
| `TSB[a: TS<int>]` | `ts_type<TSB<Field<"a", TS<int>>>>()` | `runtime::tsb({...})` | `bundle{...}` |
| `TSL<TS<int>, 3>` | `ts_type<TSL<TS<int>, 3>>()` | `runtime::tsl(ts, 3)` | `list<...>` |
| `TSD<str, TS<int>>` | `ts_type<TSD<str, TS<int>>>()` | `runtime::tsd(k, v)` | `dict<...>` |
| `TSS<int>` | `ts_type<TSS<int>>()` | `runtime::tss(meta)` | `set<int>` |
| `TSW<float, 10>` | `ts_type<TSW<float, 10>>()` | `runtime::tsw(...)` | window storage |
| `REF<TS<int>>` | `ts_type<REF<TS<int>>>()` | `runtime::ref(ts)` | ref storage |
| `SIGNAL` | N/A | `runtime::signal()` | `nullptr` |

---

## 3. Creating TSValues

TSValue is typically created by the node infrastructure, but can be created directly for testing. **TSValue takes a TSMeta schema** (from the APIs in Section 2).

### 3.1 Using Compile-Time API

```cpp
#include <hgraph/types/tsvalue.h>
#include <hgraph/types/type_api.h>

using namespace hgraph;
using namespace hgraph::types;

// Create a scalar TSValue using compile-time API
const TSMeta* ts_int_meta = ts_type<TS<int>>();
TSValue ts_int(ts_int_meta);

// Create with owning node (main output, output_id = 0)
TSValue ts_int_owned(ts_int_meta, owning_node);

// Create with explicit output_id
TSValue error_output(ts_int_meta, owning_node, ERROR_PATH);    // Error output (-1)
TSValue state_output(ts_int_meta, owning_node, STATE_PATH);    // State output (-2)

// Create a bundle TSValue
const TSMeta* order_meta = ts_type<TSB<
    Field<"price", TS<double>>,
    Field<"quantity", TS<int64_t>>,
    Name<"Order">
>>();
TSValue ts_order(order_meta, owning_node);
```

### 3.2 Output ID Constants

TSValues are identified by their owning node and output ID:

| Constant | Value | Purpose |
|----------|-------|---------|
| `OUTPUT_MAIN` | `0` | Main output of the node |
| `ERROR_PATH` | `-1` | Error output for exceptions |
| `STATE_PATH` | `-2` | Recordable state for checkpointing |

```cpp
// Access the identification info
Node* node = ts_value.owning_node();
int id = ts_value.output_id();

// Full path for REF binding
TimeSeriesPath path = ts_value.full_path();
```

### 3.3 Using Runtime API

```cpp
#include <hgraph/types/tsvalue.h>
#include <hgraph/types/type_api.h>

using namespace hgraph;
using namespace hgraph::types;

// Create a scalar TSValue using runtime API
const TSMeta* ts_int_meta = runtime::ts(value::int_type());
TSValue ts_int(ts_int_meta);

// Create with owning node
TSValue ts_int_owned(ts_int_meta, owning_node);

// Create a bundle TSValue
const TSMeta* order_meta = runtime::tsb({
    {"price", runtime::ts(value::float_type())},
    {"quantity", runtime::ts(value::int_type())}
}, "Order");
TSValue ts_order(order_meta);

// The TSValue internally uses the value schema for data storage
// but tracks modifications according to the TS schema structure
```

---

## 4. TSViews - Reading Values

TSView provides non-mutable (read-only) access to a TSValue. This is the "input" behavior.

**Important:** Views require an observer (typically `Node*`) for `make_active()`/`make_passive()` to work.

```cpp
// Create a view with an observer for notification support
TSView view(owning_node);  // Node* implements Notifiable
view.bind(&ts_int);

// Check state
if (view.valid()) {
    // Read the value
    value::ConstValueView v = view.value();
    int64_t x = v.as<int64_t>();
}

// Check for modifications
if (view.modified()) {
    // Value was modified this cycle
}

// Unbind when done
view.unbind();
```

### 4.1 Observer Requirement

Views are tied to a `Notifiable*` observer for notification propagation:

```cpp
// Observer is required for make_active/make_passive to work
TSView view(owning_node);  // Pass Node* which implements Notifiable

// Or set observer later
TSView view;
view.set_observer(owning_node);
view.bind(&ts_value);

// Observer receives notifications when value changes
// Typically: observer->notify(engine_time) is called
```

### 4.2 Active vs Passive Views

Views can be active (receiving notifications) or passive (not notified).

```cpp
// Make view active - will receive modification notifications
// REQUIRES: observer must be set
view.make_active();

// Make view passive - stops receiving notifications
view.make_passive();

// Check state
bool is_active = view.active();
```

---

## 5. Node Storage and View Creation

Nodes store TSValue directly using `std::optional`. Views are created on demand via methods on TSValue.

### 5.1 Node Storage Pattern

A Node has **at most one** of each named storage location:

```cpp
class Node {
    std::optional<TSValue> _input;           // Always TSB, exposed as TSBView
    std::optional<TSValue> _output;          // Any TS type, exposed as TSMutableView
    std::optional<TSValue> _error_output;    // For error handling
    std::optional<TSValue> _recorded_state;  // For state persistence

public:
    // Set the single input/output
    void set_input(TSValue input);   // Must be TSB
    void set_output(TSValue output);
    void set_error_output(TSValue err);
    void set_recorded_state(TSValue state);

    // Input is always TSB - return TSBView
    TSBView input_view() { return _input->bundle_view(); }

    // Output returns TSMutableView (use as_xxx() for specific types)
    TSMutableView output_view() { return _output->mutable_view(); }
    TSMutableView error_output_view();
    TSMutableView recorded_state_view();

    bool has_input() const { return _input.has_value(); }
    bool has_output() const { return _output.has_value(); }
};
```

### 5.2 View Creation Methods

TSValue provides methods to create appropriate views:

```cpp
class TSValue : public Value<CombinedPolicy<WithPythonCache, WithModificationTracking>> {
    const TSMeta* _ts_meta{nullptr};
    Node* _owning_node{nullptr};

public:
    // View creation methods
    TSView view();                    // Read-only view
    TSMutableView mutable_view();     // Mutable view for outputs
    TSBView bundle_view();            // Bundle-specific view (for TSB inputs)

    // Schema access
    const TSMeta* ts_meta() const { return _ts_meta; }
    Node* owning_node() const { return _owning_node; }
};
```

### 5.3 View Casting

TSView provides `as_xxx()` methods for type-specific access:

```cpp
class TSView {
    // Casting methods for specific types
    TSBView as_bundle();      // For TSB types
    TSLView as_list();        // For TSL types
    TSDView as_dict();        // For TSD types
    TSSView as_set();         // For TSS types

    // Direct scalar access (for TS[T])
    template<typename T>
    const T& as() const;
};

class TSMutableView : public TSView {
    // Mutable access
    template<typename T>
    void set(const T& val, engine_time_t time);

    // Copy from another view
    void copy_from(const TSView& source);
};
```

### 5.4 Usage Example

```cpp
class MyNode : public Node {
public:
    void setup(const TSMeta* input_schema, const TSMeta* output_schema) {
        // Create TSValues via TSMeta factory methods
        set_input(input_schema->make_input(this));
        set_output(output_schema->make_output(this));
    }

    void do_evaluate(engine_time_t time) {
        // Input is always TSB - use bundle_view
        TSBView in = input_view();
        float price = in.field("price").as<float>();

        // Output returns TSMutableView
        TSMutableView out = output_view();
        out.set(static_cast<int>(price * 2), time);
    }
};
```

### 5.5 Type Summary

| Type | What It Is | Use When |
|------|------------|----------|
| `TSValue` | Value with policies + TS metadata | Stored in Node |
| `TSMutableView` | Mutable view | Accessing outputs |
| `TSView` | Read-only base view | Generic TS access with `as_xxx()` |
| `TSBView` | Bundle-specific view | Accessing TSB inputs |

### 5.6 Copy Operations

```cpp
// Copy from another view
TSView source = input_view().field("data");
output_view().copy_from(source);
```

---

## 6. Binding and Notification

Views are bound to TSValues. When the TSValue is modified, active views are notified through a **hierarchical observer system**.

### 6.1 The Binding Flow

```cpp
// 1. Create TSOutput (typically by builder) - owns value + view
TSOutput ts_output(schema, owning_node);

// 2. Create input view and bind to output's value
TSView input(owning_node);  // Node* is the observer
input.bind(&ts_output.owned_value());

// 3. Make active to receive notifications
input.make_active();

// 4. When output is modified, input's observer is notified
// Notifications propagate through hierarchical ObserverStorage

// 5. Unbind when done
input.unbind();
```

### 6.2 Hierarchical Observer Storage

TSValue uses **hierarchical observers** that mirror the TS schema structure:

```cpp
// Subscribe to all changes at root level
ts_output.subscribe(my_observer);  // Notified on ANY change

// Subscribe to specific field only
TSView price_view = ts_output.view().field("price");
price_view.subscribe(price_observer);  // Only on price changes

// Notifications propagate UPWARD:
// - Change to price → notifies price observers → notifies root observers
// - Change to volume → notifies volume observers → notifies root observers
```

### 6.3 Time as Parameter

Time is passed to mutation methods, **not stored** in views:

```cpp
// Time passed when modifying
engine_time_t now = current_engine_time();
view.set(42, now);  // Time as parameter

// Avoids stale time issues in views
```

### 6.4 Lazy Observer Allocation

No memory cost until someone actually subscribes:

```cpp
// ObserverStorage is only allocated when needed
TSOutput output(schema, node);
// No ObserverStorage allocated yet

output.subscribe(my_node);  // NOW it's allocated
```

---

## 7. Working with Bundles (TSB)

Bundles are struct-like containers with named fields.

### 7.1 Creating Bundle TSValues

Bundle TSValues require a TSMeta bundle schema:

```cpp
// Using compile-time API
const TSMeta* quote_meta = ts_type<TSB<
    Field<"symbol", TS<std::string>>,
    Field<"price", TS<double>>,
    Field<"volume", TS<int64_t>>,
    Name<"Quote">
>>();
TSValue ts_quote(quote_meta, owning_node);

// Using runtime API
const TSMeta* quote_meta_rt = runtime::tsb({
    {"symbol", runtime::ts(value::str_type())},
    {"price", runtime::ts(value::float_type())},
    {"volume", runtime::ts(value::int_type())}
}, "Quote");
TSValue ts_quote_rt(quote_meta_rt, owning_node);

// The TSMeta schema provides:
// - Per-field modification tracking (each field is a TS)
// - The corresponding value schema for data storage via value_schema()
```

### 7.2 Bundle Views

```cpp
// Get a bundle view
TSBView bundle_view;
bundle_view.bind(&ts_bundle);

// Access fields by name
TSView price_view = bundle_view.at("price");
TSView symbol_view = bundle_view.at("symbol");

// Access fields by index
TSView first_field = bundle_view[0];

// Iterate over fields
for (const auto& key : bundle_view.keys()) {
    TSView field = bundle_view.at(key);
    // ...
}
```

### 7.3 Mutable Bundle Views

```cpp
TSBMutableView bundle_output;
bundle_output.bind(&ts_bundle);

// Get mutable child views
TSMutableView price_output = bundle_output.at_mut("price");
price_output.set_value(value::Value(123.45));
```

---

## 8. Path-Based Navigation

For deep access into nested structures, use navigation paths.

```cpp
#include <hgraph/types/value/path.h>

// Parse a path
value::ValuePath path = value::parse_path("orders[0].price");

// Navigate to the location
TSView root_view;
root_view.bind(&ts_value);

value::ConstValueView nested = value::navigate(root_view.value(), path);
double price = nested.as<double>();

// Mutable navigation
TSMutableView output;
output.bind(&ts_value);

value::ValueView nested_mut = value::navigate_mut(output.value_mut(), path);
nested_mut.as<double>() = 99.99;
```

### 8.1 View Parent Chain

Views track their position in the hierarchy via navigation paths:

```cpp
// Create a view at a specific path
TSView nested_view;
nested_view.bind(&ts_value, value::parse_path("orders[0].price"));

// The view's path is used for navigation
value::ConstValueView v = nested_view.value();  // Automatically navigates
```

### 8.2 Two Path Types

The system uses two distinct path representations:

| Path Type | Lifetime | Use Case |
|-----------|----------|----------|
| **Lightweight** | One engine cycle | Views, internal navigation |
| **Stored** | Persistent | REF types, global state, serialization |

### 8.3 Lightweight Paths (Views)

Views use lightweight paths with **ordinal integers only** - no field names:

```cpp
// Lightweight path - all integers
struct LightweightPath {
    std::vector<size_t> elements;  // Ordinal positions only
};

// TSB: field ordinal (0, 1, 2...), NOT field name
// TSL: index (0, 1, 2...)
// TSD: map index (internal order - may change!)

// Example: 2nd field, then 3rd element
LightweightPath path{{1, 2}};

// Views use lightweight paths for efficiency
TSView view(observer);
view.bind(&ts_value, lightweight_path);
```

**Characteristics:**
- Zero allocation for small paths (SBO)
- No string comparisons
- Valid only for current engine cycle

### 8.4 Stored Paths (REF Types)

REF types and global state use stored paths - **completely pointer-free** for serialization:

```cpp
// Stored path - fully serializable, no pointers
struct StoredPath {
    tuple<int, ...> graph_id;    // Graph ID (not pointer!)
    size_t node_ndx;             // Node index in graph (not full node_id)
    int output_id;               // 0=main, -1=error, -2=state
    std::vector<PathElement> elements;
};

// Path element types by container:
// - TSL: size_t index
// - TSB: string field name
// - TSD: Value (raw key)

// Example: Reference to "price" field in node 5
StoredPath path{
    {0, 1},              // graph_id
    5,                   // node_ndx (index in graph's node list)
    OUTPUT_MAIN,
    {{PathElement::Field, "price"}}
};
```

**Key benefit:** Using `graph_id` + `node_ndx` instead of pointers makes paths fully serializable for checkpointing and replay.

### 8.5 Working with REF Types

REF types store **StoredPaths** for persistence:

```cpp
// Get stored path from TSValue
StoredPath ref_path = ts_value.stored_path();

// Store in TimeSeriesReference
TimeSeriesReference ref;
ref.set_path(ref_path);

// Later, resolve and bind
if (TSValue* target = ref.resolve()) {
    input_view.bind(target);
}
```

### 8.6 String Path Limitations

String paths (e.g., `"orders[0].price"`) are only supported when:
1. No TSD in path, OR
2. TSD key type has string bijection (to_string/from_string)

```cpp
// May fail for TSD with complex key types
auto str = path_to_string(stored_path);  // Returns nullopt if not stringifiable

// For now, prefer structured StoredPath over string representation
```

---

## 9. Python Interop

TSValue provides Python interoperability through the caching policy.

### 9.1 Reading Values

```cpp
// Python-friendly value access
nb::object py_val = view.py_value();

// Delta value (for change tracking)
nb::object py_delta = view.py_delta_value();
```

### 9.2 Writing Values

```cpp
// Set from Python object
output.py_set_value(py_obj);

// Apply result (handles None correctly)
output.apply_result(py_obj);
```

### 9.3 Python Caching

The `WithPythonCache` policy caches Python object conversions:

```cpp
// First call converts C++ to Python
nb::object py1 = view.py_value();  // Converts

// Second call returns cached object (if value unchanged)
nb::object py2 = view.py_value();  // Returns cached

// Modification invalidates cache
output.set_value(value::Value(100));
nb::object py3 = view.py_value();  // Converts again
```

---

## 10. Hierarchical Tracking and Policies

TSValue uses **hierarchical modification tracking** and **hierarchical observers** that mirror the TS schema structure.

### 10.1 Hierarchical Modification Tracking

Each level of the TS schema has its own modification timestamp:

```cpp
TSBOutput bundle(ts_type<TSB<
    Field<"price", TS<float>>,
    Field<"volume", TS<int>>
>>(), node);

engine_time_t now = current_engine_time();

// Set price - updates price timestamp AND bundle timestamp
bundle.view().field("price").set(99.5f, now);

// Query modification at different levels
bool price_modified = bundle.view().field("price").modified_at(now);  // true
bool bundle_modified = bundle.view().modified_at(now);  // true (propagated)
bool volume_modified = bundle.view().field("volume").modified_at(now);  // false
```

### 10.2 Collection Tracking

Collections have specialized modification storage:

```cpp
// TSS (Set) - tracks element additions/removals
SetModificationStorage:
  - structural_modified: when set structure changed
  - element_modified_at: per-element timestamps
  - removed_elements_data: for delta access

// TSD (Dict) - tracks key/value modifications
DictModificationStorage:
  - structural_modified: when keys added/removed
  - key_added_at_map: when each key was added
  - value_modified_at: per-value timestamps
  - old_values: for delta_value access
```

### 10.3 Python Caching Policy

The `Value` inside TSValue uses Python caching:

```cpp
// First call converts C++ to Python
nb::object py1 = view.py_value();  // Converts

// Second call returns cached object (if value unchanged)
nb::object py2 = view.py_value();  // Returns cached

// Modification invalidates cache
output.set_value(100, now);
nb::object py3 = view.py_value();  // Converts again
```

### 10.4 Memory Efficiency

Both trackers and observers use lazy allocation:

```cpp
// No observer storage until first subscription
TSOutput output(schema, node);
// _observers is nullptr

output.subscribe(my_node);
// NOW ObserverStorage is allocated

// Same for child trackers in composites
// Children allocated on first access
```

---

## 11. Migration from Current Types

### 11.1 TimeSeriesValueOutput -> TSValue in Node

**Before:**
```cpp
// Current implementation
TimeSeriesValueOutput output(owning_node, schema);
output.py_set_value(value);
output.mark_modified();
```

**After:**
```cpp
// New implementation - TSValue stored in Node
class MyNode : public Node {
    void setup(const TSMeta* output_schema) {
        set_output(output_schema->make_output(this));
    }

    void do_evaluate(engine_time_t time) {
        TSMutableView out = output_view();
        out.py_set_value(value);
        // Modification tracking is automatic via policy
    }
};
```

### 11.2 TimeSeriesValueInput -> TSView

**Before:**
```cpp
// Current implementation
TimeSeriesValueInput input(owning_node);
input.bind_output(output_ptr);
input.make_active();
nb::object val = input.py_value();
```

**After:**
```cpp
// New implementation - TSView with observer
TSView input(owning_node);  // Observer for notifications
input.bind(&ts_value);
input.make_active();
nb::object val = input.py_value();
```

### 11.3 TimeSeriesBundleInput (Non-Peered) -> TSValue with TSBView

**Before:**
```cpp
// Current implementation - non-peered bundle input
TimeSeriesBundleInput input(owning_node, ts_schema);
```

**After:**
```cpp
// Store TSValue in Node, access via bundle_view()
class MyNode : public Node {
    void setup(const TSMeta* input_schema) {
        set_input(input_schema->make_input(this));  // Must be TSB
    }

    void do_evaluate(engine_time_t time) {
        TSBView in = input_view();  // Returns TSBView for TSB inputs
        float price = in.field("price").as<float>();
    }
};
```

### 11.4 Migration Summary

| Old Type | New Equivalent |
|----------|----------------|
| `TimeSeriesValueOutput` | `TSValue` in Node → `output_view()` returns `TSMutableView` |
| `TimeSeriesBundleInput` (non-peered) | `TSValue` in Node → `input_view()` returns `TSBView` |
| `TimeSeriesValueInput` (peered) | `TSView` bound to external output |

**Key change:** Nodes store `std::optional<TSValue>`, views created on demand via methods.

---

## Next Steps

1. See [TSValue_DESIGN.md](TSValue_DESIGN.md) for implementation details
2. Review test cases in `hgraph_unit_tests/ts_tests/`
3. Check Python wrapper integration in `cpp/include/hgraph/api/python/`
