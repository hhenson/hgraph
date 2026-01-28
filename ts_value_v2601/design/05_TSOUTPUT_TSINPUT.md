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
Subscribes to TSOutput(s) and provides access to linked values. **Never materializes** - always delegates to source via AccessStrategy.

### Structure

```cpp
class TSInput : public Notifiable {
    std::unique_ptr<AccessStrategy> strategy_;  // Delegates all access to source
    Value active_value_;                        // Subscription state (mirrors TS schema)
    const TSMeta* meta_;                        // Schema
    ShortPath path_;                            // Location in graph
    bool bound_ = false;                        // Whether binding is complete

public:
    // State queries - delegate to strategy
    bool modified(engine_time_t current_time) const {
        return active() && strategy_->modified(current_time);
    }

    bool valid() const {
        return strategy_->valid();
    }

    bool active() const;

    // Value access - NEVER materializes, always fresh from source
    TSView view(engine_time_t current_time) const {
        return strategy_->view(current_time);
    }

    // Delta access
    DeltaView delta(engine_time_t current_time) const {
        return strategy_->delta(current_time);
    }

    // Subscription control
    void set_active(bool active);
    void set_active(std::string_view field, bool active);  // For TSB

    // Binding - creates appropriate AccessStrategy
    void bind(TSOutput& source);
    void unbind();

    // Notifiable interface - called when source changes
    void notify() override;

    // Schema access
    const TSMeta* meta() const { return meta_; }
};
```

### Never-Materialized Pattern

**Critical Design Decision**: TSInput never stores a copy of the source value. Every access goes through the AccessStrategy to the source TSOutput.

```cpp
// WRONG - materializing the value
class BadTSInput {
    TSValue cached_value_;  // NO! Don't do this

    TSView view(engine_time_t t) const {
        return cached_value_.view(t);  // Stale data!
    }
};

// CORRECT - always delegate to source
class TSInput {
    AccessStrategy* strategy_;

    TSView view(engine_time_t t) const {
        return strategy_->view(t);  // Always fresh
    }
};
```

**Why this matters**:
1. **REF correctness**: REF targets can change at runtime. Cached values would be stale.
2. **Memory efficiency**: No duplicate storage of values.
3. **Consistency**: Single source of truth for each value.

### Binding Process

```cpp
void TSInput::bind(TSOutput& source) {
    // 1. Select appropriate strategy based on types
    strategy_ = select_strategy(this, &source);

    // 2. Register as observer
    source.add_observer(this);

    // 3. Initialize active state
    initialize_active_state();

    bound_ = true;
}

void TSInput::unbind() {
    if (strategy_) {
        // Unregister from source
        strategy_->source()->remove_observer(this);
        strategy_.reset();
    }
    bound_ = false;
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

For un-peered bundles, each field can have its own AccessStrategy:

```cpp
class TSBInput : public TSInput {
    std::vector<std::unique_ptr<AccessStrategy>> field_strategies_;

public:
    void bind_field(size_t index, TSOutput& source) {
        field_strategies_[index] = select_strategy(field_input(index), &source);
        source.add_observer(this);
    }

    TSView field_view(size_t index, engine_time_t t) const {
        return field_strategies_[index]->view(t);
    }
};
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
