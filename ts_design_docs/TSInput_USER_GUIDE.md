# TSInput User Guide

**Date:** 2026-01-09
**Related:** TSInput_DESIGN.md, TSValue_USER_GUIDE.md

---

## 1. Introduction

TSInput provides the input side of the time-series binding model in hgraph. While outputs own their data, inputs create **symbolic links** to external outputs, transparently redirecting navigation to the linked data.

### 1.1 Key Concepts

- **Peered**: An input position bound to an external output. Navigation returns views into the output's data.
- **Non-peered**: An input position with local storage. Only applies to collection types (TSB, TSL) that contain peered children.
- **Link**: A binding that connects an input position to an output's TSValue.
- **Transparent Navigation**: Users navigate inputs the same way as outputs - links are followed automatically.

---

## 2. Quick Start

### 2.1 Basic Usage

```cpp
#include <hgraph/types/time_series/ts_input_root.h>

// Create an input from a bundle schema
TSInputRoot input(bundle_meta, owning_node);

// Bind fields to outputs
input.bind_field("price", &price_output_tsvalue);
input.bind_field("volume", &volume_output_tsvalue);

// Make active to receive notifications
input.make_active();

// Navigate - links are followed transparently
TSView price = input.field("price");
float val = price.as<float>();

// Check if any field was modified
if (input.modified_at(current_time)) {
    // Handle modifications
}
```

### 2.2 Header Files

```cpp
#include <hgraph/types/time_series/ts_input_root.h>  // TSInputRoot
#include <hgraph/types/time_series/ts_link.h>        // TSLink (for advanced use)
#include <hgraph/types/time_series/ts_value.h>       // TSValue with link support
#include <hgraph/types/time_series/ts_view.h>        // TSView, TSBView, etc.
```

---

## 3. TSInputRoot

`TSInputRoot` is the primary user-facing class for working with inputs.

### 3.1 Construction

```cpp
// From a bundle schema
TSInputRoot input(bundle_meta, owning_node);

// From a generic TSMeta (must be TSB type)
TSInputRoot input(ts_meta, owning_node);

// Default construction (invalid until initialized)
TSInputRoot input;
```

### 3.2 Navigation

Navigation methods return views. If a field is linked, the view points into the linked output's data.

```cpp
// By index
TSView field_view = input.field(0);

// By name
TSView price_view = input.field("price");

// Element access (alias for field by index)
TSView elem = input.element(0);

// Get a bundle view for bulk operations
TSBView bundle = input.bundle_view();

// Get field count
size_t count = input.size();
```

### 3.3 Binding

Bind fields to external outputs to make them peered:

```cpp
// Bind by index
input.bind_field(0, &output_tsvalue);

// Bind by name
input.bind_field("price", &output_tsvalue);

// Unbind (disconnect from output)
input.unbind_field(0);
input.unbind_field("price");

// Check if bound
if (input.is_field_bound("price")) {
    // Field is linked to an output
}
```

### 3.4 Active Control

Active inputs receive notifications when linked outputs are modified:

```cpp
// Make active - subscribes all links
input.make_active();

// Make passive - unsubscribes all links
input.make_passive();

// Check state
if (input.active()) {
    // Currently receiving notifications
}
```

### 3.5 State Queries

```cpp
// Check if any field was modified at the current time
if (input.modified_at(engine_time)) {
    // At least one linked field was modified
}

// Check if all linked fields are valid (have been set)
if (input.all_valid()) {
    // All fields have data
}

// Get the most recent modification time
engine_time_t last = input.last_modified_time();
```

---

## 4. Working with Views

Views returned from input navigation are the same `TSView` types used with outputs. However, when navigating through an input, they may be pointing into linked output data.

### 4.1 Transparent Navigation

```cpp
// This works the same whether "orders" is linked or not:
TSView orders = input.field("orders");

// For a linked field, this returns the output's data:
if (orders.valid()) {
    TSLView list = orders.as_list();
    for (size_t i = 0; i < list.size(); ++i) {
        TSView elem = list.element(i);
        // Process element...
    }
}
```

### 4.2 Path Behavior When Crossing Links

When navigation crosses a link boundary, the view's path resets to the linked output:

```cpp
// Input: TSB[price: TS[float], orders: TSL[TS[Order], 3]]
// orders[0] is linked to OutputA

TSView orders = input.field("orders");
// Path is relative to input root

TSView order0 = orders.element(0);
// CROSSES LINK: path now relative to OutputA root
// stored_path() returns OutputA's node info

// Further navigation within OutputA
TSView order_id = order0.field("id");
// Path builds within OutputA's structure
```

This is correct because the linked output owns its data - paths should identify the output location, not the input path.

### 4.3 Checking Modification State

```cpp
TSView price = input.field("price");

// Check if this specific field was modified
if (price.modified_at(current_time)) {
    float new_price = price.as<float>();
}

// Get last modification time
engine_time_t last = price.last_modified_time();
```

---

## 5. Advanced: Direct Link Access

For advanced use cases, you can access links directly through the underlying TSValue.

### 5.1 Accessing Links

```cpp
TSValue& root = input.value();

// Check if link support is enabled
if (root.has_link_support()) {
    // Get the number of child slots
    size_t count = root.child_count();

    // Check if a specific index is linked
    if (root.is_linked(0)) {
        // Get the link
        TSLink* link = root.link_at(0);

        // Access link state
        bool is_active = link->active();
        bool is_bound = link->bound();
        const TSValue* output = link->output();
    }
}
```

### 5.2 Creating Links Programmatically

```cpp
TSValue& root = input.value();

// Enable link support if not already done
root.enable_link_support();

// Create a link at index 0
// NOTE: Schema validation is performed - output must match expected schema
root.create_link(0, &output_tsvalue);

// Remove a link
root.remove_link(0);
```

**Schema Validation:** When calling `create_link()`, the output's TSMeta is validated against the expected schema at that position. If there's a mismatch (e.g., linking a `TS[int]` to a position expecting `TS[float]`), an exception is thrown.

### 5.3 Active Control at Link Level

```cpp
TSLink* link = root.link_at(0);
if (link) {
    // Control individual link activation
    link->make_active();
    link->make_passive();
}
```

---

## 6. Nested Non-Peered Structures

When a collection (TSB or TSL) is non-peered but contains peered children, the input creates nested TSValues with their own link support. This works recursively to arbitrary depth (TSB -> TSL -> TSB -> ...).

### 6.1 Recursive Structure Support

When `enable_link_support()` is called, it recursively creates nested TSValues for all composite children:

```
Input: TSB[
    level1: TSL[TSB[
        inner: TS[int]
    ], 2]
]

Internal structure created:
TSValue (root TSB)
├── _child_values[0] = TSValue (TSL, link support)
│   ├── _child_values[0] = TSValue (TSB, link support)
│   │   └── _child_links[0] = nullptr (inner: ready for link)
│   └── _child_values[1] = TSValue (TSB, link support)
│       └── _child_links[0] = nullptr (inner: ready for link)
└── _child_links[0] = nullptr (level1: not linked at top level)
```

### 6.2 Simple Example Structure

```
Input: TSB[
    price: TS[float],         // peered
    orders: TSL[TS[Order], 3] // non-peered list, children peered individually
]
```

```cpp
// Access the non-peered list
TSView orders_view = input.field("orders");
TSLView orders = orders_view.as_list();

// Navigate to individual elements (these follow links)
TSView order0 = orders.element(0);  // Follows link to Output B
TSView order1 = orders.element(1);  // Follows link to Output C
```

### 6.2 Accessing Nested TSValues

```cpp
TSValue& root = input.value();

// Get the nested TSValue for a non-peered child
TSValue* orders_tsv = root.child_value(1);  // Index of "orders"

if (orders_tsv && orders_tsv->has_link_support()) {
    // Access the nested links
    for (size_t i = 0; i < orders_tsv->child_count(); ++i) {
        if (orders_tsv->is_linked(i)) {
            // This element is linked
        }
    }
}
```

---

## 7. Notification Flow

### 7.1 How Notifications Work

1. Output is modified → overlay marks modified
2. Overlay notifies subscribers
3. TSLink::notify() is called
4. TSLink delegates directly to Node::notify()

**Key Point**: Notifications go directly from link to node. There's no bubble-up through parent inputs.

### 7.2 Deduplication

If multiple links notify the same node in the same tick, the node is only scheduled once:

```cpp
// TSLink internally tracks _notify_time
void TSLink::notify(engine_time_t time) {
    if (_notify_time != time) {
        _notify_time = time;
        _node->notify(time);  // Only once per tick
    }
}
```

---

## 8. Common Patterns

### 8.1 Processing Modified Fields

```cpp
void on_tick(engine_time_t time) {
    // Check each field for modifications
    for (size_t i = 0; i < input.size(); ++i) {
        TSView field_view = input.field(i);
        if (field_view.modified_at(time)) {
            process_field(i, field_view);
        }
    }
}
```

### 8.2 Waiting for All Fields Valid

```cpp
void start() {
    input.make_active();
}

void on_tick(engine_time_t time) {
    if (!input.all_valid()) {
        return;  // Wait for all fields to be set
    }

    // All fields have data, process...
}
```

### 8.3 Dynamic Rebinding

```cpp
void rebind_to_new_source(const TSValue* new_output) {
    // Active state is preserved - no need to deactivate/reactivate
    input.unbind_field("price");
    input.bind_field("price", new_output);
    // If active, automatically subscribes to new output
}
```

---

## 9. Comparison: Input vs Output Navigation

| Aspect | Output | Input |
|--------|--------|-------|
| Data ownership | Owns data | Links to external data |
| `view()` returns | View into local storage | View into linked output (or local for non-peered) |
| `modified_at()` | Checks local overlay | Checks linked output's overlay |
| `make_active()` | N/A | Subscribes links to outputs |

---

## 10. Best Practices

1. **Call make_active() after binding**: Ensure all fields are bound before activating.

2. **Check validity before access**: Use `all_valid()` or check individual fields.

3. **Don't hold views across ticks**: Views are transient and may become invalid.

4. **Use field names when possible**: More readable and robust against schema changes.

5. **Let active state be managed by TSInputRoot**: Use `make_active()`/`make_passive()` on the root, not individual links.

---

## 11. API Reference Summary

### TSInputRoot

| Method | Description |
|--------|-------------|
| `TSInputRoot(meta, node)` | Construct from schema and owning node |
| `valid()` | Check if input is valid |
| `field(index)` | Get view of field by index |
| `field(name)` | Get view of field by name |
| `element(index)` | Alias for `field(index)` |
| `size()` | Get number of fields |
| `bundle_view()` | Get TSBView with link support |
| `bind_field(index, output)` | Bind field to output |
| `unbind_field(index)` | Unbind field |
| `is_field_bound(index)` | Check if field is bound |
| `make_active()` | Subscribe all links |
| `make_passive()` | Unsubscribe all links |
| `active()` | Check if active |
| `modified_at(time)` | Check if any field modified |
| `all_valid()` | Check if all fields valid |
| `last_modified_time()` | Get most recent modification |
| `value()` | Get underlying TSValue |
| `owning_node()` | Get owning node |

### TSLink

| Method | Description |
|--------|-------------|
| `TSLink(node)` | Construct with owning node |
| `bind(output)` | Bind to output TSValue |
| `unbind()` | Unbind from output |
| `bound()` | Check if bound |
| `output()` | Get bound output |
| `make_active()` | Subscribe to output |
| `make_passive()` | Unsubscribe from output |
| `active()` | Check if active |
| `view()` | Get view into linked output |
| `valid()` | Check if linked output is valid |
| `modified_at(time)` | Check modification |
| `last_modified_time()` | Get last modification time |

### TSValue Link Support

| Method | Description |
|--------|-------------|
| `enable_link_support()` | Enable links for this TSValue |
| `has_link_support()` | Check if links enabled |
| `is_linked(index)` | Check if child is linked |
| `link_at(index)` | Get link at index |
| `child_value(index)` | Get nested TSValue |
| `child_count()` | Get number of child slots |
| `create_link(index, output)` | Create link at index |
| `remove_link(index)` | Remove link at index |
| `make_links_active()` | Activate all links |
| `make_links_passive()` | Deactivate all links |
