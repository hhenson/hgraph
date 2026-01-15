# Time-Series: Values That Change Over Time

**Parent**: [Overview](00_OVERVIEW.md) | **Prerequisite**: [Value](02_VALUE.md)

---

## What Is a Time-Series?

A **time-series** is a value with temporal semantics:
- It tracks **when** it was last modified
- It can notify **observers** when it changes
- It knows whether it has **valid** data

Time-series are the primary abstraction users work with in graph code.

---

## Time-Series Kinds

### TS[T] - Scalar Time-Series

The most common type. Wraps a single scalar value.

```cpp
TSView<double> price = ...;

// Reading
double current_price = price.value();     // Get current value
bool did_change = price.modified();       // True if changed this tick
bool is_set = price.valid();              // True if ever been set

// Writing (outputs only)
TSOutput<double> price_out = ...;
price_out.set_value(42.0);                // Set new value (marks modified)
```

### TSB[Schema] - Bundle Time-Series

A bundle where **each field is independently tracked**:

```cpp
// Schema defined via TSMeta
TSBView quote = ...;  // TSB[bid: TS[float], ask: TS[float], timestamp: TS[datetime]]

// Field access - each field is a time-series
double bid = quote.field("bid").value<double>();   // Current bid price
bool bid_changed = quote.field("bid").modified();  // Did bid change?
bool ask_changed = quote.field("ask").modified();  // Did ask change?

// Or via index
double bid = quote[0].value<double>();             // By position

// Container-level queries
bool any_changed = quote.modified();               // Did ANY field change?
bool is_valid = quote.valid();                     // Is the bundle itself valid?
bool all_valid = quote.all_valid();                // Are ALL fields valid?
```

Key insight: `quote.modified()` is true if **any** child changed. You can drill down to see exactly which fields changed.

### TSL[TS[T], Size] - List Time-Series

A list of **independent time-series elements**:

```cpp
// Fixed-size list of 10 floats
TSLView<TSView<double>, 10> prices = ...;

// Dynamic-size list
TSLView<TSView<double>, 0> prices_dyn = ...;

// Element access - each element is a time-series
double first = prices[0].value();         // Value of first element
bool elem_changed = prices[0].modified(); // Did element 0 change?

// Container queries
size_t count = prices.size();             // Number of elements
bool any_changed = prices.modified();     // Did ANY element change?

// Iteration
for (auto ts_elem : prices) {
    if (ts_elem.modified()) {
        std::cout << ts_elem.value() << "\n";
    }
}
```

### TSD[K, TS[V]] - Dict Time-Series

Maps **scalar keys** to **time-series values**:

```cpp
// Int keys to float time-series
TSDView stock_prices = ...;  // TSD[int, TS[float]]

// Key-based access
double price = stock_prices[123].value();     // Current price for key 123
bool changed = stock_prices[123].modified();  // Did this key change?

// Key existence
bool has_key = stock_prices.contains(123);    // Is key present?

// Key iteration
for (int64_t key : stock_prices.keys()) {
    if (stock_prices[key].modified()) {
        std::cout << key << ": " << stock_prices[key].value() << "\n";
    }
}

// Key set access (TSS-like view)
auto key_set = stock_prices.key_set();        // View of keys as a set
```

Keys are scalars (not time-series). Values are time-series. The **key set** can change over time (keys added/removed), and this is tracked.

### TSS[T] - Set Time-Series

A **set of scalar values** that changes over time:

```cpp
TSSView active_ids = ...;  // TSS[int]

// Membership
bool is_active = active_ids.contains(42);   // Is 42 in the set?
size_t count = active_ids.size();           // Number of elements

// Change tracking (set-level delta)
for (int64_t id : active_ids.added()) {     // Elements added this tick
    std::cout << "Added: " << id << "\n";
}
for (int64_t id : active_ids.removed()) {   // Elements removed this tick
    std::cout << "Removed: " << id << "\n";
}

// Check specific element changes
bool was_added = active_ids.was_added(42);
bool was_removed = active_ids.was_removed(99);

// Iteration over current values
for (int64_t id : active_ids.values()) {
    std::cout << id << "\n";
}
```

Unlike TSD, TSS contains **scalars** not time-series. The delta tracks which elements were added/removed.

### REF[TS[T]] - Reference Time-Series

REF holds a **TimeSeriesReference** as its value. Conceptually, `REF[TS[T]]` behaves like `TS[TimeSeriesReference]` - it's a time-series containing a reference value.

```cpp
RefView<TSView<double>> price_ref = ...;

// The value is a TimeSeriesReference
auto ref_value = price_ref.value();   // Returns TimeSeriesReference

// State queries
bool changed = price_ref.modified();  // Did the reference change this tick?
bool has_ref = price_ref.valid();     // Does it contain a valid reference?

// Writing (outputs only)
RefOutput<TSView<double>> price_ref_out = ...;
price_ref_out.set_value(some_ts.ref());  // Set reference to point to some_ts
```

### REF Binding Semantics

In the following, "time-series" refers to any time-series type (TS, TSB, TSL, TSD, TSS, SIGNAL), not just `TS[T]`.

**REF → REF**: Normal scalar binding. The TimeSeriesReference value is copied like any other scalar. No special behavior.

**REF → time-series**: Special conversion. When a REF output is linked to a time-series input:
- The runtime extracts the target time-series from the reference
- A **dynamic link** is established to the target
- When the reference changes, the link is updated to follow the new target
- The input sees the referenced time-series's value and modifications

**time-series → REF**: Special conversion. When a time-series output is linked to a REF input:
- A TimeSeriesReference pointing to the time-series is created
- The REF input's value is this reference

```cpp
// Example: Dynamic routing with REF → TS binding
void select_source(
    TSView<bool> use_primary,
    TSView<double> primary,
    TSView<double> secondary,
    RefOutput<TSView<double>>& output
) {
    // Returns a reference to the selected source
    if (use_primary.value()) {
        output.set_value(primary.ref());    // Reference to primary
    } else {
        output.set_value(secondary.ref());  // Reference to secondary
    }
}

// Consumer with TS input - dynamic link from REF→TS binding
void consumer(TSView<double> price, TSOutput<std::string>& output) {
    // When wired: consumer(price=select_source(...))
    // The REF→TS binding creates a dynamic link
    // price.value() follows whichever source is selected
    output.set_value("Price: " + std::to_string(price.value()));
}
```

See [Links and Binding](04_LINKS_AND_BINDING.md) for more details on dynamic routing.

### SIGNAL - Event Without Value

A time-series that **ticks** but carries no data:

```cpp
SignalView heartbeat = ...;

// Only meaningful query
bool ticked = heartbeat.modified();    // Did it tick?

// No value() method - signals have no data
```

Use signals for pure event notification.

---

## Input vs Output

### Inputs (Read-Only)

When a time-series appears as a node **input**, you get a read-only view:

```cpp
// Input view - read-only
void my_node(TSView<double> price) {
    // Can read
    double v = price.value();
    bool m = price.modified();

    // Cannot write - TSView has no set_value()
}
```

Inputs are **linked** to outputs. See [Links and Binding](04_LINKS_AND_BINDING.md).

### Outputs (Read-Write)

When a time-series is a node **output**, you own it and can write:

```cpp
// Output - writable
void my_node(TSOutput<double>& output) {
    // Set value explicitly
    output.set_value(42.0);
}
```

Writing to an output:
1. Updates the value
2. Sets `last_modified_time` to current engine time
3. Notifies observers (linked inputs)

---

## Key Properties

### `.value()` / `.value<T>()`

The current data. For composite types, returns a view into the structure.

```cpp
// Scalar
double v = price.value();              // Returns double

// Bundle
nb::object val = quote.to_python();    // Python object (dict or dataclass)
double bid = quote.field("bid").value<double>();  // Field's value

// List
auto list = prices.to_python();        // Python list
double first = prices[0].value();      // Element's value
```

### `.modified()`

True if the time-series was modified **this tick** (current engine time).

```cpp
if (price.modified()) {
    // React to change
    double new_value = price.value();
}
```

For composite types, `modified()` is true if **any** descendant was modified.

### `.valid()`

True if the time-series has **ever been set** (has meaningful data).

```cpp
if (!price.valid()) {
    // No data yet, skip processing
    return;
}

// Safe to access value
process(price.value());
```

### `.all_valid()` (Composites Only)

True if the container **and all children** are valid:

```cpp
// Bundle: all fields must be valid
if (quote.all_valid()) {
    // Safe to access all fields
    double spread = quote.field("ask").value<double>() - quote.field("bid").value<double>();
}
```

### `.last_modified_time()`

The engine time when this was last modified. Used internally for `modified()` check.

```cpp
// Usually you use .modified() instead
if (price.last_modified_time() == current_engine_time) {
    // Same as price.modified()
}
```

---

## Hierarchical Change Tracking

Changes propagate **upward**:

```
TSB[a: TS[int], b: TS[float]]
         │
         ▼
┌─────────────────────────────────┐
│  TSB (bundle)                   │
│  modified_time: T2              │  ◄── Updated when child changes
│                                 │
│  ┌──────────┐  ┌──────────┐    │
│  │ field a  │  │ field b  │    │
│  │ time: T1 │  │ time: T2 │    │  ◄── Field b changed at T2
│  └──────────┘  └──────────┘    │
└─────────────────────────────────┘
```

When `b` is modified:
1. `b.modified()` → true
2. `b.last_modified_time()` → T2
3. Parent bundle's `last_modified_time()` → T2
4. `bundle.modified()` → true

This continues up to the root.

---

## Delta Access

For composites, you can query **what changed**:

```cpp
// Bundle: which fields changed?
for (auto [name, ts] : quote.modified_items()) {
    std::cout << name << " changed to " << ts.to_string() << "\n";
}

// List: which indices changed?
for (auto [idx, ts] : prices.modified_items()) {
    std::cout << "prices[" << idx << "] = " << ts.value() << "\n";
}

// Set: what was added/removed?
for (const auto& elem : active_ids.added()) {
    std::cout << "Added: " << elem << "\n";
}
for (const auto& elem : active_ids.removed()) {
    std::cout << "Removed: " << elem << "\n";
}

// Dict: key changes + value changes
for (const auto& key : stock_prices.added_keys()) {
    std::cout << "New key: " << key << "\n";
}
for (auto [key, ts] : stock_prices.modified_items()) {
    std::cout << "Price update: " << key << " = " << ts.value() << "\n";
}
```

See [Delta and Change Tracking](06_DELTA.md) for details.

---

## Next

- [Links and Binding](04_LINKS_AND_BINDING.md) - How inputs connect to outputs
- [Access Patterns](05_ACCESS_PATTERNS.md) - Reading, writing, iteration
- [Delta and Change Tracking](06_DELTA.md) - Incremental processing
