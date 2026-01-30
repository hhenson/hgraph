# TSOutput and TSInput Design

## Overview

TSOutput and TSInput are the graph endpoints:
- **TSOutput**: Producer of time-series values
- **TSInput**: Consumer of time-series values

## TSOutput

### Purpose
Owns native time-series value and manages cast alternatives. Provides views to consumers.
Observer management is delegated to TSValue's `observer_value_` component.

### Structure

```cpp
class TSOutput {
    TSValue native_value_;                              // Native representation (includes observer_value_)
    robin_hood::unordered_map<const TSMeta*, TSValue> alternatives_;     // Cast/peer representations
    Node* owning_node_;                                 // For graph context

public:
    TSOutput(const TSMeta& ts_meta, Node* owner)
        : native_value_(ts_meta), owning_node_(owner) {}

    // View access - returns TSOutputView, schema determines native vs alternative
    TSOutputView view(engine_time_t current_time, const TSMeta& schema) {
        if (&schema == &native_value_.ts_meta()) {
            return TSOutputView(&native_value_, this, current_time);
        }
        // Get or create alternative for this schema
        TSValue& alt = get_or_create_alternative(schema);
        return TSOutputView(&alt, this, current_time);
    }

    // Bulk mutation via delta
    void apply_value(const DeltaValue& delta) {
        native_value_.apply_delta(delta);
        // Alternatives subscribe to native (or its components) and sync via notifications
    }

    // Graph context
    Node* owning_node() const { return owning_node_; }

    // Schema access (delegates to native_value_)
    const TSMeta& ts_meta() const { return native_value_.ts_meta(); }

private:
    TSValue& get_or_create_alternative(const TSMeta& schema);
};

// TSOutputView wraps TSView, adds output-specific operations
class TSOutputView {
    TSView ts_view_;                // Core view (ViewData + current_time)
    TSOutput* output_;              // For context

public:
    // Delegates to TSView for data access
    View value() { return ts_view_.value(); }
    DeltaView delta_value() { return ts_view_.delta_value(); }
    bool modified() { return ts_view_.modified(); }
    bool valid() { return ts_view_.valid(); }

    // Output-specific mutation
    void set_value(View v) { ts_view_.set_value(v); }
    void apply_delta(DeltaView dv) { ts_view_.apply_delta(dv); }

    // Observer management (delegates to TSValue's observer_value_)
    void subscribe(Notifiable* observer);
    void unsubscribe(Notifiable* observer);

    // Navigation - wraps TSView navigation
    TSOutputView field(std::string_view name);
    TSOutputView operator[](size_t index);
    TSOutputView operator[](View key);

    // Path access
    const ShortPath& short_path() const { return ts_view_.short_path(); }
    FQPath fq_path() const { return ts_view_.fq_path(); }
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

Alternatives are created on-demand when a consumer requests a different schema.
The `view()` method on TSOutput handles this transparently:

```cpp
TSValue& TSOutput::get_or_create_alternative(const TSMeta& schema) {
    auto it = alternatives_.find(&schema);
    if (it != alternatives_.end()) {
        // Sync alternative with native if native was modified
        if (native_value_.last_modified_time() > it->second.last_modified_time()) {
            sync_alternative(it->second, native_value_);
        }
        return it->second;
    }

    // Create new alternative
    auto& alt = alternatives_.emplace(&schema, TSValue(schema)).first->second;
    establish_sync(native_value_, alt);
    return alt;
}
```

### Alternative Sync

Alternatives subscribe to the native value (or its components) and sync via notifications.
When an alternative is created, it establishes subscriptions to the relevant parts of the native value.
This is handled by `establish_sync()` during alternative creation.

Observers of the output are notified through TSValue's `observer_value_` component.

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

    // Path access (via Link's ViewData)
    const ShortPath& short_path() const { return link_->view_data.path; }
    FQPath fq_path() const { return link_->view_data.path.to_fq(); }
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
