# TSInput Design Document

**Date:** 2026-01-09
**Status:** Implemented
**Related:** TSValue_DESIGN.md, Phase5_Hierarchical_Subscriptions_DESIGN.md

---

## 1. Overview

TSInput provides the input side of the time-series binding model. Unlike outputs which own their data, inputs are a hybrid of:
- **Local storage** for non-peered portions of the schema
- **Links** to external outputs for peered portions

From the user's perspective, navigating an input is transparent - they get views regardless of whether the underlying data is local or linked to an external output.

### 1.1 Key Insight: Symbolic Links

The core abstraction is treating peered bindings as **symbolic links**:
- The input has a TSValue structure (owns the hierarchy)
- At peered positions, instead of local data, there's a "link" to an external output
- Navigation transparently follows links, returning views into the linked output
- Users don't know or care whether they're viewing local or linked data

---

## 2. Architecture

### 2.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        TSInputRoot                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    TSValue (root TSB)                        ││
│  │                   with link support enabled                  ││
│  │  ┌─────────────────────────────────────────────────────────┐││
│  │  │  _child_links: vector<unique_ptr<TSLink>>               │││
│  │  │                                                          │││
│  │  │  [0] TSLink ──────► Output A (peered)                   │││
│  │  │  [1] nullptr (non-peered, local TSValue with links)     │││
│  │  │  [2] TSLink ──────► Output B (peered)                   │││
│  │  └─────────────────────────────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Implementation Files

| Component | Header | Implementation |
|-----------|--------|----------------|
| TSLink | `cpp/include/hgraph/types/time_series/ts_link.h` | `cpp/src/cpp/types/time_series/ts_link.cpp` |
| TSValue (extended) | `cpp/include/hgraph/types/time_series/ts_value.h` | `cpp/src/cpp/types/time_series/ts_value.cpp` |
| TSView (updated) | `cpp/include/hgraph/types/time_series/ts_view.h` | `cpp/src/cpp/types/time_series/ts_view.cpp` |
| TSInputRoot | `cpp/include/hgraph/types/time_series/ts_input_root.h` | `cpp/src/cpp/types/time_series/ts_input_root.cpp` |

### 2.3 Peering Model

| Position State | Storage | Navigation Result |
|----------------|---------|-------------------|
| **Peered** | TSLink pointing to output | View into linked output |
| **Non-peered collection** | Local TSValue with its own links | View into local structure, recurse |
| **Non-peered leaf** | N/A - scalars are always peered or inside non-peered parent |

**Key Rules:**
1. Top-level input is always a non-peered TSB (bundle)
2. Only collection types (TSB, TSL, TSD) can be non-peered
3. Scalars (TS[T]), sets (TSS), windows (TSW), and refs (REF) are always peered (leaf nodes)
4. Once a position is peered, everything below it comes from the linked output

---

## 3. TSLink Structure

TSLink is the binding point that connects an input position to an external output's TSValue.

**File:** `cpp/include/hgraph/types/time_series/ts_link.h`

```cpp
/**
 * @brief Link to an external output - the "symbolic link" in the input hierarchy.
 *
 * TSLink implements Notifiable so it can be registered with an output's overlay.
 * When the linked output is modified, the overlay notifies this TSLink, which
 * then delegates directly to the owning node.
 *
 * Key behaviors:
 * - Active state is preserved across bind/unbind operations
 * - When active, automatically subscribes to bound output's overlay
 * - Notifications go directly to node (no bubble-up through parent inputs)
 * - Notify-time deduplication prevents redundant node notifications
 */
struct TSLink : Notifiable {
    // ========== Construction ==========

    TSLink() noexcept = default;
    explicit TSLink(Node* node) noexcept : _node(node) {}

    // Non-copyable, movable
    TSLink(const TSLink&) = delete;
    TSLink& operator=(const TSLink&) = delete;
    TSLink(TSLink&& other) noexcept;
    TSLink& operator=(TSLink&& other) noexcept;
    ~TSLink() override;

    // ========== Node Association ==========

    void set_node(Node* node) noexcept { _node = node; }
    [[nodiscard]] Node* node() const noexcept { return _node; }

    // ========== Binding ==========

    /**
     * @brief Bind to an external TSValue (from an output).
     *
     * If currently active, unsubscribes from old output and subscribes to new.
     * Active state is preserved across rebinding.
     *
     * @param output The TSValue to link to (typically from an output)
     */
    void bind(const TSValue* output);

    /**
     * @brief Unbind from current output.
     *
     * Active state is preserved - will auto-subscribe when rebound.
     */
    void unbind();

    [[nodiscard]] bool bound() const noexcept { return _output != nullptr; }
    [[nodiscard]] const TSValue* output() const noexcept { return _output; }

    // ========== Subscription Control ==========

    void make_active();
    void make_passive();
    [[nodiscard]] bool active() const noexcept { return _active; }

    // ========== Notifiable Implementation ==========

    void notify(engine_time_t time) override;

    // ========== View Access ==========

    [[nodiscard]] TSView view() const;

    // ========== State Queries ==========

    [[nodiscard]] bool valid() const;
    [[nodiscard]] bool modified_at(engine_time_t time) const;
    [[nodiscard]] engine_time_t last_modified_time() const;

    // ========== Sample Time ==========

    void set_sample_time(engine_time_t time) noexcept { _sample_time = time; }
    [[nodiscard]] engine_time_t sample_time() const noexcept { return _sample_time; }
    [[nodiscard]] bool sampled_at(engine_time_t time) const noexcept;

private:
    const TSValue* _output{nullptr};
    TSOverlayStorage* _output_overlay{nullptr};
    Node* _node{nullptr};
    bool _active{false};
    engine_time_t _sample_time{MIN_DT};
    engine_time_t _notify_time{MIN_DT};

    void subscribe_if_needed();
    void unsubscribe_if_needed();
    [[nodiscard]] bool is_graph_stopping() const;
};
```

### 3.1 Notification Implementation

```cpp
void TSLink::notify(engine_time_t time) {
    if (!_active) {
        return;  // Ignore notifications when passive
    }

    if (_notify_time != time) {
        _notify_time = time;

        // Delegate to owning node
        if (_node && !is_graph_stopping()) {
            _node->notify(time);
        }
    }
}
```

---

## 4. TSValue Extension for Link Support

TSValue is extended to optionally support links at child positions.

**File:** `cpp/include/hgraph/types/time_series/ts_value.h`

### 4.1 New Members

```cpp
struct TSValue {
    // ========== Existing Members ==========
    base_value_type _value;
    std::unique_ptr<TSOverlayStorage> _overlay;
    const TSMeta* _ts_meta{nullptr};
    Node* _owning_node{nullptr};
    int _output_id{OUTPUT_MAIN};

    // ========== Link Support (for inputs) ==========

    /**
     * @brief Per-child link tracking for composite types.
     *
     * Indexed by child position (field index for TSB, element index for TSL).
     * - nullptr = local data (non-peered at this position)
     * - non-null = linked to external output (peered)
     */
    std::vector<std::unique_ptr<TSLink>> _child_links;

    /**
     * @brief Per-child TSValue storage for non-peered composite children.
     *
     * When a child position is non-peered but is itself a composite type
     * (TSB or TSL) that may have peered grandchildren, we need nested
     * TSValues with their own link support.
     */
    std::vector<std::unique_ptr<TSValue>> _child_values;

    // ...
};
```

### 4.2 Link Support API

```cpp
// ========== Query Methods ==========

[[nodiscard]] bool has_link_support() const noexcept {
    return !_child_links.empty();
}

[[nodiscard]] bool is_linked(size_t index) const noexcept {
    return index < _child_links.size() &&
           _child_links[index] != nullptr &&
           _child_links[index]->bound();
}

[[nodiscard]] TSLink* link_at(size_t index) noexcept;
[[nodiscard]] const TSLink* link_at(size_t index) const noexcept;

[[nodiscard]] TSValue* child_value(size_t index) noexcept;
[[nodiscard]] const TSValue* child_value(size_t index) const noexcept;

[[nodiscard]] size_t child_count() const noexcept {
    return _child_links.size();
}

// ========== Modification Methods ==========

void enable_link_support();
void create_link(size_t index, const TSValue* output);
void remove_link(size_t index);
TSValue* get_or_create_child_value(size_t index);

// ========== Active Control ==========

void make_links_active();
void make_links_passive();
```

### 4.3 Implementation Details

**enable_link_support() - Recursive for Nested Structures:**

Link support is enabled recursively to handle arbitrary nesting depth (TSB -> TSL -> TSB -> ...):

```cpp
void TSValue::enable_link_support() {
    if (!_ts_meta) return;

    size_t child_count = 0;
    std::vector<const TSMeta*> child_metas;

    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB: {
            auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
            child_count = bundle_meta->field_count();
            // Collect child schemas
            for (size_t i = 0; i < child_count; ++i) {
                child_metas.push_back(bundle_meta->field_meta(i));
            }
            break;
        }
        case TSTypeKind::TSL: {
            auto* list_meta = static_cast<const TSLTypeMeta*>(_ts_meta);
            child_count = list_meta->fixed_size();
            const TSMeta* elem_type = list_meta->element_type();
            for (size_t i = 0; i < child_count; ++i) {
                child_metas.push_back(elem_type);
            }
            break;
        }
        default:
            return;  // Scalars, sets, etc. don't have child links
    }

    _child_links.resize(child_count);
    _child_values.resize(child_count);

    // RECURSIVE: Create child_values for all composite children
    // This ensures the entire nested structure is ready for navigation
    for (size_t i = 0; i < child_count; ++i) {
        const TSMeta* child_meta = child_metas[i];
        if (child_meta &&
            (child_meta->kind() == TSTypeKind::TSB ||
             child_meta->kind() == TSTypeKind::TSL)) {
            // This child is a composite type - create its TSValue with link support
            _child_values[i] = std::make_unique<TSValue>(child_meta, _owning_node);
            _child_values[i]->enable_link_support();  // Recursive call
        }
        // For leaf types (TS, TSS, TSW, etc.), child_values[i] stays nullptr
    }
}
```

**create_link() - With Schema Validation:**

When binding a link, the output schema is validated against the expected schema:

```cpp
void TSValue::create_link(size_t index, const TSValue* output) {
    if (index >= _child_links.size()) {
        throw std::out_of_range("TSValue::create_link: index out of bounds");
    }

    // Get expected schema for this child position
    const TSMeta* expected_schema = nullptr;
    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB: {
            auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
            if (index < bundle_meta->field_count()) {
                expected_schema = bundle_meta->field_meta(index);
            }
            break;
        }
        case TSTypeKind::TSL: {
            auto* list_meta = static_cast<const TSLTypeMeta*>(_ts_meta);
            expected_schema = list_meta->element_type();
            break;
        }
        default:
            throw std::runtime_error("TSValue::create_link: not a composite type");
    }

    // Validate output schema matches expected
    if (output && expected_schema) {
        const TSMeta* output_schema = output->ts_meta();
        if (!output_schema) {
            throw std::runtime_error("TSValue::create_link: output has no schema");
        }

        // Schema validation: check kind matches
        if (output_schema->kind() != expected_schema->kind()) {
            throw std::runtime_error(
                "TSValue::create_link: schema mismatch - expected kind " +
                std::to_string(static_cast<int>(expected_schema->kind())) +
                " but got " + std::to_string(static_cast<int>(output_schema->kind())));
        }

        // Deeper validation: check value schemas match
        const value::TypeMeta* expected_value = expected_schema->value_schema();
        const value::TypeMeta* output_value = output_schema->value_schema();
        if (expected_value != output_value) {
            if (expected_value && output_value) {
                if (expected_value->kind != output_value->kind ||
                    expected_value->size != output_value->size) {
                    throw std::runtime_error(
                        "TSValue::create_link: value schema mismatch");
                }
            } else if (expected_value || output_value) {
                throw std::runtime_error(
                    "TSValue::create_link: value schema mismatch");
            }
        }
    }

    // Create link if it doesn't exist
    if (!_child_links[index]) {
        _child_links[index] = std::make_unique<TSLink>(_owning_node);
    }

    // Bind to the output
    _child_links[index]->bind(output);

    // Clear any child value at this position (it's now peered)
    if (index < _child_values.size()) {
        _child_values[index].reset();
    }
}
```

---

## 5. View Navigation with Links

TSView navigation is updated to check for links and follow them transparently.

**File:** `cpp/include/hgraph/types/time_series/ts_view.h`

### 5.1 Link Source Tracking

TSView adds a `_link_source` member to track the TSValue with link support at the current navigation level:

```cpp
struct TSView {
protected:
    value::ConstValueView _view;
    const TSMeta* _ts_meta{nullptr};
    const TSValue* _container{nullptr};       // Container for state access
    TSOverlayStorage* _overlay{nullptr};      // Overlay for modification tracking
    const TSValue* _root{nullptr};            // Root TSValue for path tracking
    LightweightPath _path;                    // Path from root to this view
    const TSValue* _link_source{nullptr};     // TSValue with link support for navigation

public:
    [[nodiscard]] bool has_link_source() const noexcept { return _link_source != nullptr; }
    [[nodiscard]] const TSValue* link_source() const noexcept { return _link_source; }
    void set_link_source(const TSValue* source) noexcept { _link_source = source; }
};
```

### 5.2 TSBView::field() with Link Support

```cpp
TSView TSBView::field(size_t index) const {
    if (!valid()) {
        throw std::runtime_error("TSBView::field() called on invalid view");
    }

    const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    if (index >= bundle_meta->field_count()) {
        throw std::out_of_range("TSBView::field(): index out of range");
    }

    // ===== Link Support: Check if this child is linked =====
    if (_link_source && _link_source->is_linked(index)) {
        // Follow the link - return view into the linked output
        TSLink* link = const_cast<TSValue*>(_link_source)->link_at(index);
        return link->view();
    }

    const TSBFieldInfo& field_info = bundle_meta->field(index);

    // Navigate to the field data using the bundle view by name
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(field_info.name);

    // Extend path with field ordinal
    LightweightPath child_path = _path.with(index);

    // ===== Link Support: Check for nested TSValue with links =====
    const TSValue* child_link_source = nullptr;
    if (_link_source) {
        // Check if there's a nested non-peered child with link support
        child_link_source = const_cast<TSValue*>(_link_source)->child_value(index);
    }

    // Use overlay-based child navigation with path
    if (auto* composite = composite_overlay()) {
        TSOverlayStorage* child_overlay = composite->child(index);
        TSView result(field_value.data(), field_info.type, child_overlay, _root, std::move(child_path));
        result.set_link_source(child_link_source);
        return result;
    }

    // No overlay - return view with path but without tracking
    TSView result(field_value.data(), field_info.type, nullptr, _root, std::move(child_path));
    result.set_link_source(child_link_source);
    return result;
}
```

### 5.3 Path Behavior When Crossing Links

**Important:** When navigation crosses a link boundary, the path resets to the linked output's root.

```cpp
TSView TSLink::view() const {
    if (!_output) {
        return TSView{};  // Invalid view if unbound
    }
    return _output->view();  // Returns view rooted at output
}
```

When `link->view()` is called:
- `_root` becomes the linked output's TSValue
- `_path` becomes empty (root of output)
- Further navigation builds path within the output's structure

This is the **correct behavior** because:
1. The linked output owns its data - paths should be relative to its structure
2. `stored_path()` should return the output's node/output info, not the input's
3. The linked output doesn't have links (outputs own their data), so `_link_source` is not set

**Example:**
```cpp
// Input schema: TSB[price: TS[float], orders: TSL[TS[Order], 3]]
// orders[0] is linked to OutputA

TSView orders = input.field("orders");
// _root = input._value (local)
// _path = [1]  (orders field)
// _link_source = input._value.child_value(1) (nested TSValue)

TSView order0 = orders.element(0);
// CROSSES LINK: returns OutputA.view()
// _root = OutputA
// _path = []  (root of OutputA)
// _link_source = nullptr (outputs don't have links)

// Further navigation is within OutputA's data
TSView order_id = order0.field("id");
// _root = OutputA
// _path = [0]  (id field within OutputA's structure)
```

### 5.4 Link Source Propagation

Link source is propagated through type casting methods:

```cpp
TSBView TSView::as_bundle() const {
    // ... validation ...

    auto* composite = _overlay ? static_cast<CompositeTSOverlay*>(_overlay) : nullptr;
    TSBView result(_view.data(), static_cast<const TSBTypeMeta*>(_ts_meta), composite);
    result._root = _root;
    result._path = _path;
    result._link_source = _link_source;  // Propagate link source
    return result;
}
```

Similarly for `as_list()`, `as_dict()`, and `as_set()`.

---

## 6. TSInputRoot

TSInputRoot is the user-facing wrapper providing the top-level input interface.

**File:** `cpp/include/hgraph/types/time_series/ts_input_root.h`

```cpp
/**
 * @brief Top-level input container with link support.
 *
 * Wraps a TSValue (always a TSB) to provide:
 * - Transparent navigation through links
 * - Field binding to external outputs
 * - Active/passive subscription control
 * - Aggregated state queries
 */
struct TSInputRoot {
    // ========== Construction ==========

    TSInputRoot() noexcept = default;
    TSInputRoot(const TSBTypeMeta* meta, Node* node);
    TSInputRoot(const TSMeta* meta, Node* node);

    // Move operations
    TSInputRoot(TSInputRoot&& other) noexcept = default;
    TSInputRoot& operator=(TSInputRoot&& other) noexcept = default;

    // Non-copyable
    TSInputRoot(const TSInputRoot&) = delete;
    TSInputRoot& operator=(const TSInputRoot&) = delete;

    // ========== Validity ==========

    [[nodiscard]] bool valid() const noexcept;
    explicit operator bool() const noexcept { return valid(); }

    // ========== Navigation ==========

    [[nodiscard]] TSView field(size_t index) const;
    [[nodiscard]] TSView field(const std::string& name) const;
    [[nodiscard]] TSView element(size_t index) const { return field(index); }
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] const TSBTypeMeta* bundle_meta() const noexcept;
    [[nodiscard]] TSBView bundle_view() const;

    // ========== Binding ==========

    void bind_field(size_t index, const TSValue* output);
    void bind_field(const std::string& name, const TSValue* output);
    void unbind_field(size_t index);
    void unbind_field(const std::string& name);
    [[nodiscard]] bool is_field_bound(size_t index) const noexcept;
    [[nodiscard]] bool is_field_bound(const std::string& name) const noexcept;

    // ========== Active Control ==========

    void make_active();
    void make_passive();
    [[nodiscard]] bool active() const noexcept { return _active; }

    // ========== State Queries ==========

    [[nodiscard]] bool modified_at(engine_time_t time) const;
    [[nodiscard]] bool all_valid() const;
    [[nodiscard]] engine_time_t last_modified_time() const;

    // ========== Direct Access ==========

    [[nodiscard]] TSValue& value() noexcept { return _value; }
    [[nodiscard]] const TSValue& value() const noexcept { return _value; }
    [[nodiscard]] Node* owning_node() const noexcept { return _node; }

private:
    TSValue _value;
    Node* _node{nullptr};
    bool _active{false};

    size_t field_index(const std::string& name) const;
};
```

### 6.1 Key Implementation Details

**bundle_view() with link source:**
```cpp
TSBView TSInputRoot::bundle_view() const {
    if (!valid()) {
        throw std::runtime_error("TSInputRoot::bundle_view() called on invalid input");
    }

    // Get the base view
    TSView base_view = _value.view();

    // Set the link source for transparent navigation
    base_view.set_link_source(&_value);

    // Convert to bundle view (link source is propagated)
    return base_view.as_bundle();
}
```

**bind_field() with auto-activation:**
```cpp
void TSInputRoot::bind_field(size_t index, const TSValue* output) {
    if (!valid()) {
        throw std::runtime_error("TSInputRoot::bind_field() called on invalid input");
    }

    _value.create_link(index, output);

    // If active, the link should auto-subscribe
    if (_active) {
        TSLink* link = _value.link_at(index);
        if (link) {
            link->make_active();
        }
    }
}
```

---

## 7. Notification Model

### 7.1 Direct to Node (No Bubble-Up)

Notifications go directly from TSLink to Node:

```
Output modified
    → output overlay marks modified
        → overlay notifies subscribers
            → TSLink::notify(time)
                → Node::notify(time)
```

**No parent input involvement.** This simplifies the notification path significantly.

### 7.2 Deduplication

Each TSLink tracks `_notify_time` to prevent duplicate notifications within a tick:

```cpp
void TSLink::notify(engine_time_t time) {
    if (!_active) return;

    if (_notify_time != time) {
        _notify_time = time;
        if (_node && !is_graph_stopping()) {
            _node->notify(time);
        }
    }
}
```

### 7.3 Active State

- `make_active()`: Subscribes link to output's overlay
- `make_passive()`: Unsubscribes
- Active state is preserved across bind/unbind operations

---

## 8. Example: Mixed Peered/Non-Peered Structure

```
Input Schema: TSB[
    price: TS[float],
    orders: TSL[TS[Order], 3],
    metadata: TSB[id: TS[int], name: TS[str]]
]

Wiring:
    - price → Output A (single scalar)
    - orders[0] → Output B
    - orders[1] → Output C
    - orders[2] → Output D
    - metadata → Output E (entire bundle)

Resulting Structure:

TSInputRoot
└── _value: TSValue (TSB, link support enabled)
    └── _child_links[3]:
        [0] TSLink → Output A          // price: peered
        [1] nullptr                     // orders: non-peered (children peered individually)
        [2] TSLink → Output E          // metadata: peered (entire bundle)

    └── _child_values[3]:              // For non-peered children needing nested structure
        [0] nullptr                     // price: peered, no local value
        [1] TSValue (TSL, link support) // orders: non-peered list
            └── _child_links[3]:
                [0] TSLink → Output B
                [1] TSLink → Output C
                [2] TSLink → Output D
        [2] nullptr                     // metadata: peered, no local value
```

Navigation:
- `input.field("price")` → follows link → view into Output A
- `input.field("orders")` → not linked → view into local TSL
- `input.field("orders").element(0)` → follows link → view into Output B
- `input.field("metadata")` → follows link → view into Output E
- `input.field("metadata").field("id")` → navigates within Output E's data

---

## 9. Usage Example

```cpp
// Create schemas
const TSMeta* float_ts = TSTypeRegistry::instance().ts(value::float_type());
const TSBTypeMeta* bundle_meta = TSTypeRegistry::instance().tsb({
    {"price", float_ts},
    {"volume", float_ts},
});

// Create output TSValues (simulating outputs)
TSValue price_output(float_ts, some_node, OUTPUT_MAIN);
TSValue volume_output(float_ts, some_node, OUTPUT_MAIN);

// Create input
TSInputRoot input(bundle_meta, owning_node);

// Bind fields to outputs
input.bind_field("price", &price_output);
input.bind_field("volume", &volume_output);

// Make active to receive notifications
input.make_active();

// Navigate through input - links are followed transparently
TSView price_view = input.field("price");  // Returns view into price_output
float price = price_view.as<float>();

// Check state
if (input.modified_at(current_time)) {
    // At least one field was modified
}
```

---

## 10. Open Questions

1. **TSD Support**: Deferred. TSD with dynamic keys needs additional design for key observer pattern integration.

2. **Delta Access**: How do non-peered collections report deltas? Aggregate from children?

3. **REF Integration**: When REF resolves dynamically, how does rebinding work?

---

## Appendix A: Comparison with Legacy Implementation

| Aspect | Legacy (BaseTimeSeriesInput) | New (TSLink + TSValue) |
|--------|------------------------------|------------------------|
| Notification path | Bubbles up through parent inputs | Direct to node |
| Active tracking | Per-input `_active` flag | Per-link `_active` flag |
| Peered/non-peered | Separate code paths | Unified via link presence |
| Storage | Complex variant | TSValue with optional links |
| Navigation | Type-specific input classes | Unified TSView with link source |

---

## Appendix B: Design Decisions

1. **Links bind to TSValue, not TSOutput**: The binding is at the TSValue level (data storage), not the TSOutput level (node interface). This keeps the implementation simpler and more focused.

2. **TSLink as Notifiable**: Each link is independently subscribable, allowing fine-grained active control.

3. **No bubble-up**: Simplifies notification, removes need for `notify_parent()` chain.

4. **Symbolic link analogy**: Clean mental model - peered positions are transparent redirects.

5. **Active state preserved across rebind**: Consistent with legacy behavior, simplifies rebinding logic.

6. **_link_source in TSView**: Rather than using `_container` (which is for state access), we use a separate `_link_source` member specifically for link-aware navigation. This keeps concerns separated.
