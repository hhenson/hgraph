# Delta and Change Tracking: Incremental Processing

**Parent**: [Overview](00_OVERVIEW.md)

---

## Overview

Efficient reactive systems process **only what changed**. The time-series system provides rich delta information so you can:
- Know *that* something changed (`.modified()`)
- Know *what* changed (`.delta_value()`, `.modified_keys()`, etc.)
- Know *how* it changed (added, removed, updated)

---

## Modification Tracking Basics

### `.modified()`

True if the time-series changed **this tick**:

```cpp
if (price.modified()) {
    // React to change
    double new_price = price.value();
}
```

For composites, `modified()` is true if **any descendant** changed.

### `.last_modified_time()`

The exact engine time of the last modification:

```cpp
// Usually you use .modified() instead
if (price.last_modified_time() == engine.current_time()) {
    // Equivalent to price.modified()
}
```

### Hierarchical Propagation

Changes propagate **upward**:

```
TSB[a: TS[int], b: TS[float]]

If b changes at time T:
├── b.last_modified_time() = T
├── b.modified() = true
├── bundle.last_modified_time() = T  (propagated up)
└── bundle.modified() = true         (because child changed)
```

You can drill down to find exactly which children changed.

---

## Delta for Scalar TS[T]

Scalars have simple delta semantics:

```cpp
TSView<double> price = ...;

// Current value
double current = price.value();  // → 42.0

// Delta value (the change itself)
double delta = price.delta_value();  // → 42.0 (same as value for scalars)
```

For scalars, `value()` and `delta_value()` are the same - the "change" is the new value.

---

## Delta for Bundle TSB

### Which Fields Changed?

```cpp
TSBView quote = ...;  // TSB[bid: TS[float], ask: TS[float], time: TS[datetime]]

// Check specific field
if (quote.field("bid").modified()) {
    std::cout << "Bid changed to " << quote.field("bid").value<double>() << "\n";
}

// Iterate over modified fields
for (auto [name, ts] : quote.modified_items()) {
    std::cout << name << " changed to " << ts.to_string() << "\n";
}
```

### Value vs Delta Value

```cpp
// Full value - returns Python object (dataclass or dict depending on schema)
nb::object full = quote.to_python();

// Delta value - returns Python dict (only modified fields)
nb::object delta = quote.delta_to_python();

// Or access fields directly without Python conversion
for (auto [name, ts] : quote.modified_items()) {
    // Direct C++ access to modified field values
}
```

**Note**: `to_python()` returns a dataclass instance when the schema has an associated `scalar_type`, otherwise a dict. `delta_to_python()` always returns a dict containing only the modified fields.

---

## Delta for List TSL

### Which Elements Changed?

```cpp
TSLView<TSView<double>, 10> prices = ...;

// Check specific element
if (prices[3].modified()) {
    std::cout << "Element 3 changed to " << prices[3].value() << "\n";
}

// Iterate over modified elements (keys are indices for TSL)
for (auto idx : prices.modified_keys()) {
    std::cout << "prices[" << idx << "] = " << prices[idx].value() << "\n";
}

for (auto [idx, ts] : prices.modified_items()) {
    std::cout << "prices[" << idx << "] = " << ts.value() << "\n";
}
```

### Delta Value

```cpp
// Full value - Python list
nb::object full = prices.to_python();  // → [1.0, 2.0, 3.0, 4.0, ...]

// Delta value - Python dict mapping modified indices to values
nb::object delta = prices.delta_to_python();  // → {3: 4.5, 7: 8.2}
```

---

## Delta for Set TSS

Sets track **membership changes**:

```cpp
TSSView active_ids = ...;  // TSS[int]

// Elements added this tick
for (int64_t id : active_ids.added()) {
    std::cout << "ID added: " << id << "\n";
}

// Elements removed this tick
for (int64_t id : active_ids.removed()) {
    std::cout << "ID removed: " << id << "\n";
}

// Check specific element
if (active_ids.was_added(42)) {
    std::cout << "ID 42 was just added\n";
}

// Check for any structural change
if (active_ids.modified()) {
    std::cout << "Membership changed\n";
}
```

### Value vs Delta Value

```cpp
// Full value - Python frozenset
nb::object full = active_ids.to_python();

// Delta value - Python SetDelta object
nb::object delta = active_ids.delta_to_python();

// Or direct C++ access (avoids Python conversion)
const auto& added = active_ids.added();      // Range of added elements
const auto& removed = active_ids.removed();  // Range of removed elements
```

**Note**: `delta_to_python()` returns a `SetDelta` object, not a dict. Use `.added` and `.removed` properties to access the changes.

---

## Delta for Dict TSD

Dicts track **key changes** and **value changes** separately:

### Key Changes (Structural)

```cpp
TSDView stock_prices = ...;  // TSD[int, TS[float]]

// Keys added this tick
for (int64_t key : stock_prices.added_keys()) {
    std::cout << "New key: " << key << "\n";
}

// Keys removed this tick
for (int64_t key : stock_prices.removed_keys()) {
    std::cout << "Removed key: " << key << "\n";
}

// Added/removed with their values
for (auto [key, ts] : stock_prices.added_items()) {
    std::cout << "New: " << key << " = " << ts.value() << "\n";
}
```

### Value Changes (Non-Structural)

```cpp
// Keys whose TS values changed (excludes newly added keys)
for (int64_t key : stock_prices.modified_keys()) {
    std::cout << key << " updated to " << stock_prices[key].value() << "\n";
}

for (auto [key, ts] : stock_prices.modified_items()) {
    std::cout << key << ": " << ts.value() << "\n";
}
```

### Key Set Access

```cpp
// Access keys as a TSS-like view
auto key_set = stock_prices.key_set();

// Key set has TSS semantics
for (int64_t key : key_set.added()) {
    std::cout << "New key: " << key << "\n";
}
```

### Value vs Delta Value

```cpp
// Full value - Python frozendict
nb::object full = stock_prices.to_python();

// Delta value - Python frozendict
nb::object delta = stock_prices.delta_to_python();

// Direct C++ access preferred for performance
for (auto [key, ts] : stock_prices.items()) {
    // Access all current entries
}
```

---

## Validity and Delta

### Valid vs Invalid

```cpp
// Time-series that was never set
if (!price.valid()) {
    // price.delta_value() is undefined or sentinel
}

// Time-series that was set then invalidated
if (!price.valid() && price.modified()) {
    // Was valid, now invalid - this is a change
    std::cout << "Price was invalidated\n";
}
```

### all_valid for Composites

```cpp
TSBView quote = ...;  // TSB[bid: TS[float], ask: TS[float]]

// Individual validity
quote.field("bid").valid();  // Is bid set?
quote.field("ask").valid();  // Is ask set?

// Composite validity
quote.valid();      // Is bundle itself valid?
quote.all_valid();  // Are ALL fields valid?
```

---

## Delta Clearing

Deltas are **ephemeral** - they're valid only for the current tick.

```
At tick T=1:
  active_ids.add(42);
  // active_ids.added() = {42}

At tick T=2 (no changes):
  // active_ids.added() = {}  (automatically cleared)
  // active_ids contains 42   (still present)
```

You don't need to clear deltas manually - the system handles it.

---

## Value-Layer Delta (DeltaValue)

Time-series delta tracking is built on top of the **DeltaValue** concept from the [Value](02_VALUE.md) layer. Understanding this relationship helps when working with deltas programmatically.

### Two Layers of Delta

```
┌─────────────────────────────────────────────┐
│         Time-Series Layer                   │
│  .modified(), .added(), .removed()          │
│  .delta_to_python(), .modified_keys()       │
│  ┌───────────────────────────────────────┐  │
│  │        Value Layer (DeltaValue)       │  │
│  │  Represents the actual change data    │  │
│  │  Added elements, removed elements,    │  │
│  │  updated entries                      │  │
│  └───────────────────────────────────────┘  │
│  + modification time tracking               │
│  + observer notifications                   │
└─────────────────────────────────────────────┘
```

- **Value layer**: `DeltaValue` holds the raw change data (what was added, removed, updated)
- **Time-series layer**: Adds temporal tracking and provides convenient query APIs

### Extracting DeltaValue from Time-Series

You can extract the underlying `DeltaValue` for use elsewhere:

```cpp
TSSView active_ids = ...;  // TSS[int]

if (active_ids.modified()) {
    // Extract the delta as a DeltaValue
    DeltaValue delta = active_ids.delta();

    // Apply to another value
    Value other_set(set_schema);
    other_set.apply_delta(delta);
}
```

### Creating Time-Series Changes from DeltaValue

When writing to a time-series output, you can apply a `DeltaValue`:

```cpp
TSSOutput active_ids_out = ...;

// Build a delta
DeltaValue delta(set_schema);
delta.mark_added(42);
delta.mark_removed(99);

// Apply to time-series (marks modified, notifies observers)
active_ids_out.apply_delta(delta);
```

This is more efficient than individual `add()`/`remove()` calls when making multiple changes.

### DeltaValue for Each Collection Type

| TS Type | DeltaValue Contents |
|---------|---------------------|
| **TSL** | Map of index → new value |
| **TSS** | Set of added + set of removed |
| **TSD** | Added entries + updated entries + removed keys |

See [Value: Bulk Operations](02_VALUE.md#bulk-operations-on-collections) for full `DeltaValue` API.

---

## Delta and Links

### Linked Inputs

When an input is linked to an output, delta flows through:

```
Output modifies value
        │
        ▼
Output computes delta
        │
        ▼
Link notifies input
        │
        ▼
Input sees same delta (via link)
```

The input's `.modified()`, `.delta_value()`, etc. reflect the output's state.

### Non-Peered Inputs

Non-peered inputs have their own delta tracking:

```cpp
// Non-peered input with local storage
TSOutput<double> local_cache = ...;  // Not linked

// When you write to it
local_cache.set_value(42.0);

// It tracks its own modification
local_cache.modified();  // True this tick
```

---

## Performance: Delta vs Full Processing

| Approach | When to Use |
|----------|-------------|
| Full recompute | Small data, infrequent changes, complex dependencies |
| Delta processing | Large data, frequent small changes, independent elements |

```cpp
// Delta approach - O(changes)
for (int64_t key : stock_prices.modified_keys()) {
    process(key, stock_prices[key].value());
}

// Full approach - O(total size)
for (auto [key, ts] : stock_prices.items()) {
    process(key, ts.value());
}
```

---

## Summary

| Type | Delta Query |
|------|-------------|
| **TS[T]** | `.modified()`, `.delta_value()` |
| **TSB** | `.modified_items()`, `.delta_to_python()` |
| **TSL** | `.modified_keys()`, `.modified_items()`, `.delta_to_python()` |
| **TSS** | `.added()`, `.removed()`, `.delta_to_python()` |
| **TSD** | `.added_keys()`, `.removed_keys()`, `.modified_keys()`, `.delta_to_python()` |

Use delta information to build efficient incremental algorithms.
