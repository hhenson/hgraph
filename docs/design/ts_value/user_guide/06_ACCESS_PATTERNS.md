# Access Patterns: Reading, Writing, and Iteration

**Parent**: [Overview](00_OVERVIEW.md)

---

## Overview

This document describes how users interact with time-series data:
- Reading values
- Writing to outputs
- Navigating composite structures
- Iterating over collections

---

## Reading Values

### Scalar TS[T]

```cpp
TSView price = ...;  // Input

// Get current value (type-erased)
double current = price.value().as<double>();

// Check state
if (price.modified()) {
    std::cout << "Price changed to " << price.value().as<double>() << "\n";
}

if (!price.valid()) {
    std::cout << "No price yet\n";
}
```

### Bundle TSB

```cpp
TSBView quote = ...;  // TSB[bid: TS[float], ask: TS[float]]

// Access by field name
double bid_price = quote.field("bid").value<double>();
double ask_price = quote.field("ask").value<double>();

// Access by index - two equivalent ways:
double first_field = quote[0].value<double>();       // [] operator syntax
double first_alt = quote.field(0).value<double>();   // field() method syntax

// Check individual field modification
if (quote.field("bid").modified()) {
    std::cout << "Bid updated\n";
}

// Check any field modification
if (quote.modified()) {
    std::cout << "Something in quote changed\n";
}

// Get all values as Python object (dict or dataclass)
nb::object data = quote.to_python();
```

### List TSL

```cpp
TSLView prices = ...;

// Access by index - two equivalent ways:
double first = prices[0].value().as<double>();       // [] operator syntax
double first_alt = prices.at(0).value().as<double>(); // at() method syntax
double last = prices[prices.size() - 1].value().as<double>();  // No negative indexing

// Bounds
size_t count = prices.size();

// Check element modification
if (prices[3].modified()) {
    std::cout << "Element 3 changed: " << prices[3].value().as<double>() << "\n";
}

// Get all as Python list
nb::object all_values = prices.to_python();
```

### Dict TSD

```cpp
TSDView stock_prices = ...;  // TSD[int, TS[float]]

// Access by key - two equivalent ways:
value::View key = value::make_scalar<int64_t>(123);
double price_123 = stock_prices[key].value<double>();      // [] operator syntax
double price_alt = stock_prices.at(key).value<double>();   // at() method syntax

// Check key existence
value::View key456 = value::make_scalar<int64_t>(456);
if (stock_prices.contains(key456)) {
    double price_456 = stock_prices[key456].value<double>();
}

// Key iteration
for (auto key : stock_prices.keys()) {
    std::cout << key.as<int64_t>() << ": " << stock_prices[key].value<double>() << "\n";
}

// Key-value iteration
for (auto [key, ts] : stock_prices.items()) {
    if (ts.modified()) {
        std::cout << key.as<int64_t>() << " updated to " << ts.value<double>() << "\n";
    }
}
```

### Set TSS

```cpp
TSSView active_ids = ...;  // TSS[int]

// Membership test
if (active_ids.contains(42)) {
    std::cout << "ID 42 is active\n";
}

// Count
size_t count = active_ids.size();

// Iteration over current values
for (auto id : active_ids.values()) {
    std::cout << id.as<int64_t>() << "\n";
}
```

---

## Writing to Outputs

### Scalar Output

```cpp
void node_impl(const TSView& data, TSView& output) {
    double val = data.value().as<double>() * 2.0;
    output.set_value(value_from(val));  // Set new value (type-erased)
}
```

### Bundle Output

```cpp
// Write entire bundle (from Python dict)
output.set_from_python(nb::dict("x"_a=1.0, "y"_a=2.0));

// Write individual fields
output.field("x").set_value(1.0);
output.field("y").set_value(2.0);
```

### List Output

```cpp
// Write individual elements
output[0].set_value(1.0);

// Resize (dynamic lists only)
output.resize(5);
output.append(6.0);
```

### Dict Output

```cpp
// Add/update entry (int keys)
output[123].set_value(150.0);

// Remove entry
output.remove(456);

// Bulk update (from Python dict)
output.set_from_python(nb::dict(nb::arg(123)=150.0, nb::arg(456)=140.0));
```

### Set Output

```cpp
// Add element (int values)
output.add(42);

// Remove element
output.remove(99);

// Bulk update (from Python set)
output.set_from_python(nb::set(nb::int_(42), nb::int_(100)));
```

---

## Invalidation

Outputs can be invalidated (marked as having no valid data):

```cpp
void maybe_value(const TSView& condition, const TSView& value, TSView& output) {
    if (condition.value().as<bool>()) {
        output.set_value(value.value());  // Copy type-erased value
    } else {
        output.invalidate();  // Mark as having no valid data
    }
}
```

Invalidated outputs:
- Have `valid() == false`
- Have `modified() == true` (invalidation is a change)
- Notify observers

---

## Path Navigation

For deeply nested structures, use path navigation:

```cpp
// Navigate to nested element
int alice_score_3 = data.field("users")[42].field("scores")[3].value<int>();

// Path as string (for dynamic access)
auto ts = data.navigate("users.42.scores.3");
int value = ts.value<int>();
```

### Safe Navigation

```cpp
// Check existence at each level
if (data.field("users").contains(42)) {
    auto user = data.field("users")[42];
    if (user.field("scores").valid()) {
        int score = user.field("scores")[3].value<int>();
    }
}
```

---

## Iteration Patterns

### Iterate Over Bundle Fields

```cpp
TSBView quote = ...;

// By name
for (const auto& field_name : quote.keys()) {
    std::cout << field_name << ": " << quote.field(field_name).to_string() << "\n";
}

// Key-value iteration
for (auto [name, ts] : quote.items()) {
    std::cout << name << ": " << ts.to_string() << "\n";
}
```

### Iterate Over List Elements

```cpp
TSLView prices = ...;

// Simple iteration
for (auto ts_elem : prices.values()) {
    std::cout << ts_elem.value().as<double>() << "\n";
}

// With index
for (auto [idx, ts_elem] : prices.items()) {
    if (ts_elem.modified()) {
        std::cout << "Element " << idx << " changed to " << ts_elem.value().as<double>() << "\n";
    }
}

// Only modified elements (values only)
for (auto ts_elem : prices.modified_values()) {
    std::cout << "Changed: " << ts_elem.value().as<double>() << "\n";
}

// Modified elements with indices
for (auto [idx, ts_elem] : prices.modified_items()) {
    std::cout << "Element " << idx << " changed to " << ts_elem.value().as<double>() << "\n";
}
```

### Iterate Over Dict Entries

```cpp
TSDView stock_prices = ...;  // TSD[int, TS[float]]

// Keys only
for (auto key : stock_prices.keys()) {
    // key is a View of the key value
}

// Key-value pairs
for (auto [key, ts_value] : stock_prices.items()) {
    std::cout << key.as<int64_t>() << ": " << ts_value.value() << "\n";
}

// Filter for modified entries
for (auto [key, ts_value] : stock_prices.items()) {
    if (ts_value.modified()) {
        std::cout << key.as<int64_t>() << " was modified\n";
    }
}

// Modified entries (keys only)
for (auto key : stock_prices.modified_keys()) {
    std::cout << "Updated: " << key.as<int64_t>() << "\n";
}

// Modified entries (key-value pairs)
for (auto [key, ts_value] : stock_prices.modified_items()) {
    std::cout << key.as<int64_t>() << " = " << ts_value.value() << "\n";
}
```

### Iterate Over Set Elements

```cpp
TSSView active_ids = ...;  // TSS[int]

// All current elements
for (auto id : active_ids.values()) {
    std::cout << id.as<int64_t>() << "\n";
}

// Delta iteration
for (auto id : active_ids.added()) {
    std::cout << "Added: " << id.as<int64_t>() << "\n";
}

for (auto id : active_ids.removed()) {
    std::cout << "Removed: " << id.as<int64_t>() << "\n";
}
```

---

## Bulk Operations

### Copy Between Values

```cpp
// Copy entire value
output.copy_from(input);

// Copy with transformation
output.set_value(transform(input.value()));
```

### Slice Access (Lists)

```cpp
TSLView prices = ...;

// Read slice (returns a view)
auto subset = prices.slice(10, 20);  // View from index 10 to 19
```

### Buffer Access (Advanced)

For high-performance scenarios, access underlying buffer:

```cpp
// Direct buffer access (for contiguous numeric data)
std::span<double> buffer = prices.as_span();  // Zero-copy view

// Bulk operations
double total = std::accumulate(buffer.begin(), buffer.end(), 0.0);
```

This requires:
- Contiguous memory layout
- Compatible element type (numeric scalars)

---

## Performance Considerations

### Prefer Field Access Over `.to_python()`

```cpp
// Less efficient - converts to Python dict
nb::object data = quote.to_python();  // Python conversion overhead

// More efficient - direct field access
double bid = quote.field("bid").value<double>();  // No Python conversion
```

### Check `modified` Before Processing

```cpp
// Efficient - skip unchanged data
if (prices.modified()) {
    for (auto ts : prices.modified_values()) {
        process(ts.value());
    }
}

// Less efficient - process everything
for (auto ts : prices.values()) {
    process(ts.value());  // Even if nothing changed
}
```

### Use Bulk Operations for Large Data

```cpp
// Fast - direct buffer access
auto buffer = prices.as_span();
double total = std::accumulate(buffer.begin(), buffer.end(), 0.0);
```

---

## Error Handling

### Invalid Access

```cpp
// Accessing invalid time-series (never set or invalidated)
if (!price.valid()) {
    // value() is safe - returns None/sentinel
    auto v = price.value();       // Returns None/sentinel, does not throw
    auto typed = price.value<double>();  // Also returns None/sentinel

    // Other operations throw
    // price.delta_value();       // Throws exception
    // price.field("x");          // Throws exception
}
```

| Method | Behavior on Invalid |
|--------|---------------------|
| `.value()` | Returns `None` (Python) / sentinel |
| `.value<T>()` | Returns `None` (Python) / sentinel |
| `.delta_value()` | Throws exception |
| Navigation | Throws exception |

### Non-existent Keys/Indices

```cpp
// Accessing non-existent key
if (!stock_prices.contains(456)) {
    // stock_prices[456] throws std::out_of_range or similar
}

// Out of bounds
if (i >= prices.size()) {
    // prices[i] throws std::out_of_range
}
```

### Type Mismatches

```cpp
// Type-erased API performs runtime type checking
// Mismatched types throw exceptions
TSView output = ...;  // Schema is TS[float]
output.set_value(value_from("not a float"));  // Runtime error: schema mismatch
```

---

## Next

- [Delta and Change Tracking](07_DELTA.md) - Incremental processing patterns
