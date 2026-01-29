# TSOutput and TSInput Design

## Overview

TSOutput and TSInput are the graph endpoints:
- **TSOutput**: Producer of time-series values
- **TSInput**: Consumer of time-series values

## TSOutput

### Purpose
Owns native time-series value and manages cast alternatives. Provides views to consumers.

### Structure

```cpp
class TSOutput {
    TSValue native_value_;                              // Native representation
    robin_hood::unordered_map<const TSMeta*, TSValue> alternatives_;  // Cast/peer representations
    const TSMeta* meta_;                                // Schema
    ShortPath path_;                                    // Location in graph
    ObserverList observers_;                            // Subscribed inputs

public:
    // View access (always returns fresh view, never materializes)
    TSView view(engine_time_t current_time) const {
        return native_value_.view(current_time);
    }

    // Alternative view (for casts/peering)
    TSView view(const TSMeta* target_meta, engine_time_t current_time);

    // Modification
    void set_value(const View& value, engine_time_t time);
    void mark_modified(engine_time_t time);

    // State queries
    bool modified(engine_time_t current_time) const;
    bool valid() const;

    // Observer management
    void add_observer(Notifiable* observer);
    void remove_observer(Notifiable* observer);
    void notify_observers();

    // Schema access
    const TSMeta* meta() const { return meta_; }
};
```

### Native vs Alternatives

TSOutput maintains the native representation plus alternative views for different schemas:

```
TSOutput
├── native_value_: TSValue[int]        ← Original type (always present)
└── alternatives_
    ├── TSMeta[float]* → TSValue[float]      ← Cast to float
    ├── TSMeta[string]* → TSValue[string]    ← Cast to string
    └── TSMeta[TSB[x:int]]* → TSValue[...]   ← Peered bundle view
```

### Alternative Management

Alternatives are created on-demand when a consumer requests a different schema:

```cpp
TSView TSOutput::view(const TSMeta* target_meta, engine_time_t current_time) {
    // Native schema - return directly
    if (target_meta == meta_) {
        return native_value_.view(current_time);
    }

    // Check for existing alternative
    auto it = alternatives_.find(target_meta);
    if (it != alternatives_.end()) {
        // Sync alternative with native if native was modified
        if (native_value_.last_modified_time() > it->second.last_modified_time()) {
            sync_alternative(it->second, native_value_);
        }
        return it->second.view(current_time);
    }

    // Create new alternative
    TSValue alt = create_alternative(native_value_, target_meta);
    auto [inserted_it, _] = alternatives_.emplace(target_meta, std::move(alt));
    return inserted_it->second.view(current_time);
}
```

### Alternative Sync

When native value is modified, alternatives are lazily synced on next access:

```cpp
void TSOutput::set_value(const View& value, engine_time_t time) {
    native_value_.set_value(value, time);
    // Alternatives NOT immediately updated - lazy sync on access
    notify_observers();
}
```

### Python Value Cache

For Python interop, TSOutput can cache the Python conversion:

```cpp
class TSOutput {
    // ...
    mutable std::optional<nb::object> python_cache_;
    mutable engine_time_t python_cache_time_ = MIN_TIME;

public:
    nb::object to_python(engine_time_t current_time) const {
        if (python_cache_time_ < native_value_.last_modified_time()) {
            python_cache_ = native_value_.to_python();
            python_cache_time_ = native_value_.last_modified_time();
        }
        return *python_cache_;
    }
};
```

## TSInput

### Purpose
Subscribes to TSOutput(s) and provides access to linked values. TSInput owns a TSValue containing Links at its leaves that point to bound output values.

### Structure

```cpp
class TSInput : public Notifiable {
    TSValue value_;                             // Contains Links at leaves pointing to outputs
    Value active_value_;                        // Subscription state (mirrors TS schema)
    const TSMeta* meta_;                        // Schema
    Node* owning_node_;                         // For scheduling

public:
    // View access - returns TSInputView
    TSInputView view(engine_time_t time, const TSMeta& schema) {
        return TSInputView(&value_, &active_value_, this, time, schema);
    }

    // Subscription control
    void set_active(bool active);
    void set_active(std::string_view field, bool active);  // For TSB

    // Notifiable interface - called when source changes
    void notify() override;

    // Schema access
    const TSMeta* meta() const { return meta_; }
};
```

### Value Structure with Links

TSInput's value_ has a mixed structure:
- **Non-peered nodes**: Internal structure (bundles, lists) owned locally
- **Link leaves**: Terminal nodes that point to output values via ViewData

```
TSInput.value_:
├── TSB (non-peered, local)
│   ├── a: Link → Output1.native_value_.a
│   └── b: TSL (non-peered, local)
│       ├── [0]: Link → Output2.native_value_
│       └── [1]: Link → Output3.native_value_
```

### TSInputView

TSInputView wraps a Link for O(1) data access without runtime navigation:

```cpp
class TSInputView {
    Link* link_;                    // ViewData: path + data + ops
    Value* active_value_;           // Active state at this path
    engine_time_t current_time_;
    TSInput* input_;

public:
    // Convert Link to TSView (just adds current_time)
    TSView ts_view() const {
        return TSView{link_->view_data, current_time_};
    }

    // Delegate to Link's ViewData for data access
    View value() const {
        return View{link_->view_data.data, link_->view_data.ops};
    }

    bool modified() const;
    bool valid() const;

    // Input-specific binding
    void bind(TSOutputView& output);
    void unbind();

    // Subscription control
    void make_active();
    void make_passive();
    bool active() const;

    // Navigation - returns new views with child Links
    TSInputView field(std::string_view name);
    TSInputView operator[](size_t index);
};
```

### Binding Process

Binding establishes Links from input leaves to output values:

```cpp
void TSInputView::bind(TSOutputView& output) {
    // 1. Populate Link's ViewData from output
    link_->view_data.path = output.short_path();
    link_->view_data.data = output.data_ptr();
    link_->view_data.ops = output.ops();

    // 2. Subscribe if active
    if (active()) {
        output.subscribe(input_);
    }
}

void TSInputView::unbind() {
    // Unsubscribe and clear link
    // ...
}
```

### Active State

The `active_value_` mirrors the TS schema structure, tracking which parts are subscribed:

```
TSInput[TSB[a: TS[int], b: TS[float]]]

active_value_ (Value with bool at each TS leaf):
  ├── a: true   ← Receiving updates for 'a'
  └── b: false  ← NOT receiving updates for 'b'
```

### Partial Binding (Un-Peered TSB)

For un-peered bundles, each field has its own Link:

```cpp
// Each field in TSInput.value_ has a Link at its leaf
// Binding happens per-field:
input_view.field("a").bind(output1_view);
input_view.field("b")[0].bind(output2_view);
input_view.field("b")[1].bind(output3_view);
```

### Observer Notification

When source TSOutput is modified, it notifies all observers:

```cpp
void TSInput::notify() override {
    // Input is notified that source changed
    // Scheduler can now consider this node for execution
    // No data copying needed - view() will get fresh data
}
```

## Paths

### ShortPath (Runtime)

```cpp
struct ShortPath {
    Node* node;           // Graph node
    PortType port_type;   // INPUT or OUTPUT
    SmallVector<uint32_t, 4> indices;  // Path through compound types
};
```

### FQPath (Serialization)

```cpp
struct FQPath {
    uint64_t node_id;              // Stable node identifier
    std::vector<PathElement> path; // Path elements
};

struct PathElement {
    std::variant<
        std::string,    // Field name
        size_t,         // Index
        Key             // Dict key
    > element;
};
```

### Path Resolution

```cpp
TSView resolve_path(const ShortPath& path, engine_time_t time) {
    // 1. Find the node
    // 2. Get the port (input or output)
    // 3. Navigate through indices
    // 4. Return TSView
}
```

## ViewData

### Structure

```cpp
struct ViewData {
    ShortPath path;     // Location in graph
    void* data;         // Pointer to data
    ts_ops* ops;        // Operations vtable
};
```

### Relationships

```
ViewData (base)
    │
    ├── Link (no time) - stored in TSInput.value_
    │
    └── TSView (+ time) - returned from access methods
```

## Graph Topology

### Node Structure

```cpp
struct Node {
    std::vector<TSInput> inputs_;
    std::vector<TSOutput> outputs_;
    // ... other node data
};
```

### Port Enumeration

```cpp
enum class PortType {
    INPUT,
    OUTPUT
};
```

## Open Questions

- TODO: How to handle output aliasing?
- TODO: Error propagation through the graph?
- TODO: Lazy binding for dynamic graphs?

## References

- User Guide: `05_TSOUTPUT_TSINPUT.md`
- Research: `01_BASE_TIME_SERIES.md`
