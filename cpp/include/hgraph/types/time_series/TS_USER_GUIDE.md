# hgraph::ts Time-Series Type System - User Guide

## Quick Start

Include the necessary headers:

```cpp
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/access_strategy.h>
#include <hgraph/types/time_series/ts_type_meta.h>
using namespace hgraph::ts;
```

## Core Concepts

The time-series type system provides:
- **TSOutput** - Owns time-series data with modification tracking
- **TSInput** - Binds to outputs, provides read-only access
- **Views** - Lightweight navigation into nested structures with path tracking
- **AccessStrategy** - Handles type transformations during binding

## Creating TSOutput

### Simple Scalar Output

```cpp
// Create type metadata for TS[int]
struct MyTSIntMeta : TimeSeriesTypeMeta {
    MyTSIntMeta() {
        ts_kind = TimeSeriesKind::TS;
        name = "TS[int]";
        value_schema = scalar_type_meta<int64_t>();
    }
    // ... implement virtual methods
};

MyTSIntMeta ts_int_meta;

// Create output
TSOutput output(&ts_int_meta, nullptr);  // nullptr = no owning node

// Set value with explicit time
engine_time_t now = /* current engine time */;
output.view().set(42, now);

// Read value
int value = output.as<int>();
```

### Using Views for Navigation

Views provide chainable navigation with path tracking:

```cpp
// For bundle types (TSB)
TSOutputView view = output.view();
view.field("price").set(100.0, time);
view.field("quantity").set(10, time);

// For list types (TSL)
view.element(0).set(1, time);
view.element(1).set(2, time);

// Chain navigation (move-efficient)
output.view().field("nested").element(0).set(42, time);
```

### Path Tracking

Views remember their navigation path for debugging:

```cpp
TSOutputView nested = output.view().field("data").element(0);
std::string path = nested.path_string();  // "root.field(\"data\").element(0)"
```

## Creating TSInput

### Basic Input Binding

```cpp
TSInput input(&ts_int_meta, nullptr);

// Bind to an output
input.bind_output(&output);

// Activate to receive notifications
input.make_active();

// Read value (delegates to bound output)
int value = input.as<int>();
bool modified = input.modified_at(current_time);

// Deactivate when done
input.make_passive();

// Unbind
input.unbind_output();
```

### Using Input Views

```cpp
TSInputView view = input.view();

// Navigation (read-only)
int price = view.field("price").as<int>();
int elem0 = view.element(0).as<int>();

// Query modification
bool changed = view.modified_at(time);
bool hasVal = view.has_value();
```

## Access Strategies

When binding input to output, an AccessStrategy tree is built automatically based on type comparison.

### Strategy Types

| Strategy | Use Case | Example |
|----------|----------|---------|
| `DirectAccess` | Types match exactly | `TS[int]` to `TS[int]` |
| `CollectionAccess` | Collection types | `TSL[...]` to `TSL[...]` |
| `RefObserver` | Non-REF input to REF output | `TS[int]` input to `REF[TS[int]]` output |
| `RefWrapper` | REF input to non-REF output | `REF[TS[int]]` input to `TS[int]` output |

### Manual Strategy Building

Usually strategies are built automatically, but you can build them manually:

```cpp
auto strategy = build_access_strategy(
    input_meta,   // Input's type metadata
    output_meta,  // Output's type metadata
    &input        // Owner TSInput
);
```

### Checking Strategy Type

```cpp
if (is_direct_access(strategy.get())) {
    // Simple delegation, no transformation
}

auto* collection = dynamic_cast<CollectionAccessStrategy*>(strategy.get());
if (collection) {
    size_t count = collection->child_count();
    for (size_t i = 0; i < count; ++i) {
        AccessStrategy* child = collection->child(i);
        // Process child strategy
    }
}
```

## Modification Tracking

### Checking Modification

```cpp
// On output
bool modified = output.modified_at(time);
engine_time_t last = output.last_modified_time();
bool hasVal = output.has_value();

// On input (delegates to strategy)
bool modified = input.modified_at(time);

// On views
bool fieldMod = view.field_modified_at(0, time);
bool elemMod = view.element_modified_at(0, time);
```

### Marking Modified

```cpp
// Direct marking (for when value changes externally)
output.view().mark_modified(time);

// Setting value automatically marks modified
output.view().set(newValue, time);
```

## Subscription and Notification

### Subscribe to Output Changes

```cpp
class MyNotifiable : public Notifiable {
public:
    void notify(engine_time_t time) override {
        // Called when output is modified
    }
};

MyNotifiable notifiable;
output.subscribe(&notifiable);

// Later
output.unsubscribe(&notifiable);
```

### TSInput as Notifiable

TSInput implements Notifiable and propagates notifications to its owning node:

```cpp
// When active, TSInput subscribes to bound output(s)
input.make_active();

// When output changes, TSInput::notify() is called
// TSInput propagates to owning node
```

## Complex Type Binding

### REF Type Handling

When binding across REF boundaries, strategies handle the transformation:

```cpp
// Output: REF[TS[int]] - reference to a time-series
// Input: TS[int] - expects the dereferenced value

// Build strategy automatically handles this:
// Creates RefObserverAccessStrategy that:
// 1. Subscribes to the REF output
// 2. Tracks when reference changes
// 3. Rebinds child strategy to new target
```

### Collection with REF Elements

```cpp
// Output: TSL[REF[TS[int]], Size[2]] - list of references
// Input: TSL[TS[int], Size[2]] - list of values

// Strategy tree:
// CollectionAccessStrategy (TSL)
//   [0] -> RefObserverAccessStrategy -> DirectAccess
//   [1] -> RefObserverAccessStrategy -> DirectAccess
```

### REF Redistribution

The most complex case - REF at different positions:

```cpp
// Output: TSD[str, REF[TSL[TS[int]]]] - dict with REF values
// Input: TSD[str, TSL[REF[TS[int]]]] - dict with REF elements

// Strategy tree handles the mismatch:
// CollectionAccess (TSD)
//   -> RefObserver (dereference outer REF)
//     -> CollectionAccess (TSL)
//       -> RefWrapper (wrap each element as REF)
```

## String Representation

### Debug Strings

```cpp
engine_time_t time = /* current time */;

// TSOutput
std::string str = output.to_string();         // Value only: "42"
std::string debug = output.to_debug_string(time);  // Full debug info

// TSOutputView
std::string viewDebug = view.to_debug_string(time);
// "TSOutputView{path=root.field(0), value=\"42\", modified=true}"

// TSInput
std::string inputStr = input.to_string();
std::string inputDebug = input.to_debug_string(time);
// "TSInput{type=TS[int], bound=true, value=\"42\", modified=true}"
```

## Best Practices

### 1. Always Pass Time Explicitly

```cpp
// Good - time is explicit
output.view().set(42, current_time);

// The time parameter ensures correct modification tracking
```

### 2. Use Move-Efficient View Chaining

```cpp
// Good - uses rvalue overloads, moves path
output.view().field("x").element(0).set(val, time);

// Less efficient - copies path at each step
TSOutputView v1 = output.view();
TSOutputView v2 = v1.field("x");
TSOutputView v3 = v2.element(0);
```

### 3. Activate Inputs Only When Needed

```cpp
// Activate when you need notifications
input.make_active();

// ... use input ...

// Deactivate when done to avoid unnecessary notifications
input.make_passive();
```

### 4. Check Validity Before Access

```cpp
TSInputView view = input.view();
if (view.valid() && view.has_value()) {
    int val = view.as<int>();
}
```

## Type Metadata

To use TSOutput/TSInput, you need TimeSeriesTypeMeta implementations. See `ts_type_meta.h` for the base classes:

- `TimeSeriesTypeMeta` - Base for all TS types
- `TSLTypeMeta` - For TSL (list) types
- `TSBTypeMeta` - For TSB (bundle) types
- `TSDTypeMeta` - For TSD (dict) types
- `REFTypeMeta` - For REF types

Example implementation:

```cpp
struct MyTSLMeta : TSLTypeMeta {
    MyElementMeta element_meta;

    MyTSLMeta() {
        ts_kind = TimeSeriesKind::TSL;
        name = "TSL[TS[int], Size[3]]";
        element_ts_type = &element_meta;
        size = 3;
    }

    std::string type_name_str() const override {
        return "TSL[TS[int], Size[3]]";
    }

    // ... other virtual methods
};
```

## See Also

- `TS_DESIGN.md` - Detailed architecture and design documentation
- `VALUE_USER_GUIDE.md` - User guide for the underlying value system
- `test_ts_input.cpp` - Comprehensive test examples
