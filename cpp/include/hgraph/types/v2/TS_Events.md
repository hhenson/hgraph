# Time Series Events

**Type-erased event structures for HGraph time series values**

Time series events represent timestamped changes in HGraph's reactive graph system. They capture state transitions as
values flow through time, enabling event-driven computation and recovery/replay functionality.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [TsEventAny - Scalar Events](#tseventany---scalar-events)
4. [TsCollectionEventAny - Collection Events](#tscollectioneventany---collection-events)
5. [Visitor Patterns](#visitor-patterns)
6. [Use Cases](#use-cases)
7. [API Reference](#api-reference)

---

## Overview

HGraph uses two primary event types to represent changes in time series values:

- **`TsEventAny`**: Represents a change to a scalar time series value (e.g., `TS[int]`, `TS[str]`)
- **`TsCollectionEventAny`**: Represents batch changes to collection time series (e.g., `TSD[str, int]`, `TSS[int]`)

Both types use **type erasure** via `AnyValue<>` to allow generic handling of values while maintaining type safety
through visitor patterns.

### Event Semantics

Events represent **deltas** (changes), not complete state. The time series value maintains the current state, while
events describe what changed:

```
Time:   t0      t1        t2         t3
State:  10  →   25    →   25     →   42
Event:  None    Modify    None       Modify
              (value=25)            (value=42)
```

---

## Architecture

### Memory Layout

**TsEventAny**:

```
┌────────────────────────────────────────────────────┐
│ engine_time_t time       (8 bytes)                 │
├────────────────────────────────────────────────────┤
│ TsEventKind kind         (1 byte)                  │
├────────────────────────────────────────────────────┤
│ AnyValue<> value         (40 bytes typical)        │
│   • Inline buffer or heap pointer                  │
│   • vtable pointer for operations                  │
└────────────────────────────────────────────────────┘
Total: ~56 bytes (stack-allocated)
```

**TsCollectionEventAny**:

```
┌────────────────────────────────────────────────────┐
│ engine_time_t time       (8 bytes)                 │
├────────────────────────────────────────────────────┤
│ TsEventKind kind         (1 byte)                  │
├────────────────────────────────────────────────────┤
│ std::vector<CollectionItem> items                  │
│   ├─ AnyKey key                                    │
│   ├─ ColItemKind kind                              │
│   └─ AnyValue<> value                              │
└────────────────────────────────────────────────────┘
Each CollectionItem: ~88 bytes (2× AnyValue + metadata)
```

---

## TsEventAny - Scalar Events

Represents a timestamped change to a scalar time series value.

### Event Kinds

```cpp
enum class TsEventKind {
    None = 0,       // No event occurred (query result)
    Recover = 1,    // Initialize or replay value (recovery/query result)
    Invalidate = 2, // Value became invalid
    Modify = 3      // Value changed
};
```

| Kind           | Has Value? | Propagates? | Semantics                                                      |
|----------------|------------|-------------|----------------------------------------------------------------|
| **None**       | No         | No          | Query returned no event at this time                           |
| **Recover**    | Optional   | No          | Initialize or replay time series value (recovery/query result) |
| **Invalidate** | No         | Yes         | Value is now invalid/unavailable                               |
| **Modify**     | Yes        | Yes         | Value changed to new value                                     |

### Structure

```cpp
struct TsEventAny {
    engine_time_t time;      // Timestamp
    TsEventKind kind;        // Event type
    AnyValue<> value;        // Payload (type-erased)
};
```

### Factory Methods

```cpp
// Create events (zero-cost, inline)
auto e1 = TsEventAny::none(t);                    // No event
auto e2 = TsEventAny::invalidate(t);              // Invalidation
auto e3 = TsEventAny::modify(t, 42);              // Modify with value
auto e4 = TsEventAny::recover(t);                 // Recover without value
auto e5 = TsEventAny::recover(t, "state");        // Recover with value
```

### Validation

Events can be validated to ensure value presence matches the event kind:

```cpp
auto event = TsEventAny::modify(t, 42);
if (event.is_valid()) {
    // Process valid event
}

// Invalid examples (for testing):
TsEventAny bad1{t, TsEventKind::None, make_any(42)};  // None should have no value
assert(!bad1.is_valid());

TsEventAny bad2{t, TsEventKind::Modify, {}};          // Modify must have value
assert(!bad2.is_valid());
```

### Visitor Pattern

Type-safe access to event values:

```cpp
auto event = TsEventAny::modify(t, 42);

// Visit value with type checking
bool found = event.visit_value_as<int>([](int value) {
    std::cout << "Value: " << value << "\n";
});
// found == true

// Wrong type returns false without calling visitor
found = event.visit_value_as<double>([](double) {
    // Never called
});
// found == false

// Mutable visitor (modify in place)
event.visit_value_as<int>([](int& value) {
    value *= 2;  // Modify the value
});
```

---

## TsCollectionEventAny - Collection Events

Represents batch changes to collection time series (TSD, TSS, TSL, TSB).

### Collection Item Operations

```cpp
enum class ColItemKind {
    Reset = 0,   // Clear key's value (set to default)
    Modify = 1,  // Set key to value
    Remove = 2   // Delete key from collection
};
```

### Structure

```cpp
struct CollectionItem {
    AnyKey key;           // The key being modified
    ColItemKind kind;     // Operation type
    AnyValue<> value;     // New value (only for Modify)
};

struct TsCollectionEventAny {
    engine_time_t time;              // Timestamp
    TsEventKind kind;                // Event kind (None/Invalidate/Modify/Recover)
    std::vector<CollectionItem> items; // Batch of changes
};
```

### Fluent Builder API

```cpp
auto event = TsCollectionEventAny::modify(t);

event.add_modify(make_any("key1"), make_any(100))   // Set key1 = 100
     .add_modify(make_any("key2"), make_any(200))   // Set key2 = 200
     .add_reset(make_any("key3"))                    // Reset key3
     .remove(make_any("key4"))                       // Delete key4
     .add_modify(make_any("key5"), make_any(300));  // Set key5 = 300
```

---

## Visitor Patterns

### TsEventAny Visitor

Access scalar event values with type safety:

```cpp
auto event = TsEventAny::modify(t, std::string("hello"));

// Const visitor
event.visit_value_as<std::string>([](const std::string& s) {
    std::cout << s << "\n";
});

// Mutable visitor
event.visit_value_as<std::string>([](std::string& s) {
    s += " world";
});
```

### CollectionItem Visitors

Access individual collection items:

```cpp
for (const auto& item : event) {
    // Visit key
    item.visit_key_as<std::string>([](const std::string& key) {
        std::cout << "Key: " << key << "\n";
    });

    // Visit value (only for Modify operations)
    item.visit_value_as<int>([](int value) {
        std::cout << "Value: " << value << "\n";
    });
}
```

### TsCollectionEventAny Visitor (Recommended)

**Separate handlers per operation** - the most ergonomic approach:

```cpp
std::map<std::string, int> my_map;

event.visit_items_as<std::string, int>(
    // on_modify: set key to value
    [&](const std::string& key, int value) {
        my_map[key] = value;
    },
    // on_reset: clear key's value
    [&](const std::string& key) {
        my_map[key] = 0;  // or some default
    },
    // on_remove: delete key
    [&](const std::string& key) {
        my_map.erase(key);
    }
);
```

**Benefits:**

- **Type-safe**: Values only accessed for Modify operations
- **Ergonomic**: Handlers match natural operation semantics
- **Efficient**: No unnecessary checks or default construction
- **Both const and mutable versions** available

---

## Use Cases

### 1. Apply Collection Changes to std::map

```cpp
// Event with multiple operations
auto event = TsCollectionEventAny::modify(t);
event.add_modify(make_any(1), make_any("one"))
     .add_modify(make_any(2), make_any("two"))
     .remove(make_any(3));

// Apply to existing map
std::map<int, std::string> data = {{3, "three"}, {4, "four"}};

event.visit_items_as<int, std::string>(
    [&](int key, const std::string& value) { data[key] = value; },
    [&](int key) { data[key] = ""; },
    [&](int key) { data.erase(key); }
);

// Result: {1: "one", 2: "two", 4: "four"}
```

### 2. Type Filtering

Handle only items with specific key/value types:

```cpp
// Event with mixed types
event.add_modify(make_any(1), make_any(100))           // int -> int
     .add_modify(make_any("str"), make_any(200))       // string -> int
     .add_modify(make_any(2), make_any(300));          // int -> int

// Only process int keys
std::vector<int> int_values;

event.visit_items_as<int, int>(
    [&](int key, int value) { int_values.push_back(value); },
    [](int) {},
    [](int) {}
);

// Result: int_values = {100, 300}  (string key skipped)
```

### 3. Accumulation

```cpp
// Financial transaction event
event.add_modify(make_any("sales"), make_any(1000.0))
     .add_modify(make_any("expenses"), make_any(500.0))
     .add_reset(make_any("pending"));

double total = 0.0;
int active_accounts = 0;

event.visit_items_as<std::string, double>(
    [&](const std::string&, double value) {
        total += value;
        active_accounts++;
    },
    [&](const std::string&) {
        active_accounts++;  // Reset counts as active
    },
    [](const std::string&) {}
);

// Result: total = 1500.0, active_accounts = 3
```

### 4. Mutable Transformation

```cpp
// Double all values in place
event.visit_items_as<int, int>(
    [](int, int& value) { value *= 2; },  // Mutable reference
    [](int&) {},
    [](int&) {}
);
```

### 5. Range-based Iteration

For custom iteration logic:

```cpp
for (const auto& item : event) {
    std::cout << "Operation: ";
    switch (item.kind) {
        case ColItemKind::Modify:
            std::cout << "Modify\n";
            break;
        case ColItemKind::Reset:
            std::cout << "Reset\n";
            break;
        case ColItemKind::Remove:
            std::cout << "Remove\n";
            break;
    }
}
```

### 6. Event Chaining

Build events incrementally with fluent API:

```cpp
TsCollectionEventAny build_update(const std::vector<int>& ids) {
    auto event = TsCollectionEventAny::modify(current_time());

    for (int id : ids) {
        if (should_update(id)) {
            event.add_modify(make_any(id), make_any(compute_value(id)));
        } else if (should_reset(id)) {
            event.add_reset(make_any(id));
        }
    }

    return event;
}
```

---

## API Reference

### TsEventAny

| Method           | Signature                                               | Description                             |
|------------------|---------------------------------------------------------|-----------------------------------------|
| `none`           | `static TsEventAny none(engine_time_t t)`               | Create None event                       |
| `invalidate`     | `static TsEventAny invalidate(engine_time_t t)`         | Create Invalidate event                 |
| `modify`         | `template<T> static TsEventAny modify(t, T&& v)`        | Create Modify event with value          |
| `recover`        | `static TsEventAny recover(engine_time_t t)`            | Create Recover event without value      |
| `recover`        | `template<T> static TsEventAny recover(t, T&& v)`       | Create Recover event with value         |
| `is_valid`       | `bool is_valid() const`                                 | Check value presence matches event kind |
| `visit_value_as` | `template<T, V> bool visit_value_as(V&& visitor) const` | Visit value with type (const)           |
| `visit_value_as` | `template<T, V> bool visit_value_as(V&& visitor)`       | Visit value with type (mutable)         |
| `operator==`     | `friend bool operator==(a, b)`                          | Equality comparison                     |
| `operator!=`     | `friend bool operator!=(a, b)`                          | Inequality comparison                   |

### CollectionItem

| Method           | Signature                                               | Description                                  |
|------------------|---------------------------------------------------------|----------------------------------------------|
| `visit_key_as`   | `template<T, V> bool visit_key_as(V&& visitor) const`   | Visit key with type (const)                  |
| `visit_key_as`   | `template<T, V> bool visit_key_as(V&& visitor)`         | Visit key with type (mutable)                |
| `visit_value_as` | `template<T, V> bool visit_value_as(V&& visitor) const` | Visit value with type (const, Modify only)   |
| `visit_value_as` | `template<T, V> bool visit_value_as(V&& visitor)`       | Visit value with type (mutable, Modify only) |

### TsCollectionEventAny

| Method           | Signature                                                 | Description                             |
|------------------|-----------------------------------------------------------|-----------------------------------------|
| `none`           | `static TsCollectionEventAny none(engine_time_t t)`       | Create None event                       |
| `invalidate`     | `static TsCollectionEventAny invalidate(engine_time_t t)` | Create Invalidate event                 |
| `modify`         | `static TsCollectionEventAny modify(engine_time_t t)`     | Create empty Modify event               |
| `recover`        | `static TsCollectionEventAny recover(engine_time_t t)`    | Create Recover event                    |
| `add_modify`     | `TsCollectionEventAny& add_modify(AnyKey, AnyValue<>)`    | Add Modify operation (fluent)           |
| `add_reset`      | `TsCollectionEventAny& add_reset(AnyKey)`                 | Add Reset operation (fluent)            |
| `remove`         | `TsCollectionEventAny& remove(AnyKey)`                    | Add Remove operation (fluent)           |
| `begin/end`      | `auto begin() const / auto end() const`                   | Range-based iteration (const)           |
| `begin/end`      | `auto begin() / auto end()`                               | Range-based iteration (mutable)         |
| `visit_items_as` | `template<K,V,M,R,D> void visit_items_as(M, R, D) const`  | Visit all items with handlers (const)   |
| `visit_items_as` | `template<K,V,M,R,D> void visit_items_as(M, R, D)`        | Visit all items with handlers (mutable) |

---

## Best Practices

1. **Use visitor patterns** instead of direct value access - provides type safety and handles type mismatches gracefully

2. **Prefer `visit_items_as`** over manual iteration for collections - more ergonomic and type-safe

3. **Use fluent builder API** for constructing collection events - cleaner and more readable

4. **Validate events in tests** using `is_valid()` to catch construction errors early

5. **Use const visitors by default** - only use mutable visitors when in-place modification is necessary

6. **Type filtering is automatic** - visitors only called for matching types, non-matching items silently skipped

7. **Check visitor return values** when you need to know if type matched:
   ```cpp
   if (event.visit_value_as<int>([](int v) { process(v); })) {
       // Value was int and processed
   } else {
       // Value was not int or not present
   }
   ```

---

## Implementation Notes

- Events are **value types** (stack-allocated, copyable)
- `AnyValue<>` uses **small buffer optimization** - small values stored inline, large values on heap
- **Zero-cost factory methods** - optimized away by compiler (RVO/copy elision)
- **Type erasure overhead** - single vtable pointer per value, minimal runtime cost
- **Visitor dispatch** - single virtual call per visit operation
- **Thread safety** - not thread-safe, use external synchronization if shared between threads

---

## See Also

- [AnyValue.md](AnyValue.md) - Type erasure implementation details
- [HGraph Documentation](https://docs.hgraph.io) - Time series concepts and graph semantics
