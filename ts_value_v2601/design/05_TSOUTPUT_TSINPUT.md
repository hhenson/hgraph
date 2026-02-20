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
    size_t port_index_;                                 // Port index on owning node

public:
    TSOutput(const TSMeta& ts_meta, Node* owner, size_t port_index = 0)
        : native_value_(ts_meta), owning_node_(owner), port_index_(port_index) {}

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

### Port Index

The `port_index_` field identifies which output port this TSOutput represents on its owning node.
Nodes can have multiple outputs (e.g., a node returning a tuple), and each output needs a unique
identifier for path construction.

**Purpose:**
- Distinguishes between multiple outputs on the same node
- Used as the first index in ShortPath construction
- Essential for resolving references and navigation

**ShortPath Construction:**

When creating the root path for a TSOutput, `port_index_` becomes the first element in the
path's index vector:

```cpp
ShortPath TSOutput::root_path() const {
    return ShortPath(owning_node_, PortType::OUTPUT, {port_index_});
}
```

For a node with multiple outputs:
```
Node (e.g., returning TSB[a: TS[int], b: TS[float]])
├── outputs_[0]: TSOutput with port_index_ = 0
│   └── root_path: ShortPath(node, OUTPUT, {0})
│       └── field "a": ShortPath(node, OUTPUT, {0, 0})
│       └── field "b": ShortPath(node, OUTPUT, {0, 1})
└── outputs_[1]: TSOutput with port_index_ = 1  (if separate outputs)
    └── root_path: ShortPath(node, OUTPUT, {1})
```

**Multi-Output Nodes:**

When a node produces multiple independent time-series outputs:
1. Each output is stored in `Node::outputs_[i]`
2. Each TSOutput is constructed with `port_index = i`
3. Path resolution uses `port_index` to locate the correct output

```cpp
// Node construction with multiple outputs
Node::Node(...) {
    for (size_t i = 0; i < num_outputs; ++i) {
        outputs_.emplace_back(output_metas[i], this, i);  // port_index = i
    }
}
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

### AlternativeStructuralObserver

When TSOutput creates an alternative for TSD/TSL types where the element types differ
(e.g., native `TSD[str, TS[int]]` with alternative `TSD[str, REF[TS[int]]]`), a simple
bind at the collection level is insufficient. Each element needs individual link
establishment, and structural changes (inserts/erases) in the native must be reflected
in the alternative.

`AlternativeStructuralObserver` implements `SlotObserver` to keep alternatives synchronized:

```cpp
class AlternativeStructuralObserver : public value::SlotObserver {
    TSOutput* output_;              // Owning TSOutput
    TSValue* alt_;                  // Alternative TSValue
    const TSMeta* native_meta_;     // Native element schema
    const TSMeta* target_meta_;     // Target element schema
    value::KeySet* registered_key_set_; // KeySet we're registered with

public:
    void on_insert(size_t slot) override;  // Element added to native
    void on_erase(size_t slot) override;   // Element removed from native
    void on_update(size_t slot) override;  // Value update (no-op, links handle this)
    void on_clear() override;              // All elements cleared
    void on_capacity(size_t, size_t) override; // Capacity change (no-op)

    void register_with(value::KeySet* key_set);
};
```

#### When It's Created

An `AlternativeStructuralObserver` is created during `establish_links_recursive()` when:

1. Both native and target are the same collection kind (TSD or TSL)
2. Their element types differ (requiring per-element conversion)

```cpp
// In establish_links_recursive(), Case 5: Both are lists/dicts
if (target_elem != native_elem) {
    auto observer = std::make_unique<AlternativeStructuralObserver>(
        this, &alt, native_meta, target_meta
    );
    subscribe_structural_observer(native_view, observer.get());
    structural_observers_.push_back(std::move(observer));
}
```

#### How It Syncs Structural Changes

**on_insert(slot)**: When a new element is inserted into the native TSD/TSL at a slot:
1. Gets TSView for the new native element at that slot
2. Gets the corresponding alternative element view
3. Calls `establish_links_recursive()` to set up the appropriate link (e.g., REFLink for REF conversion)

**on_erase(slot)**: When an element is removed from the native:
1. Gets the alternative element at that slot
2. Calls `unbind()` to clean up the link

**on_clear()**: When all elements are cleared:
1. Unbinds the entire alternative view

**on_update(slot)**: No action needed - the established link already points to the native,
so value updates are visible through the existing link.

#### KeySet Registration

For TSD types, the observer registers with the native's `KeySet`:

```cpp
void TSOutput::subscribe_structural_observer(TSView native_view,
                                              AlternativeStructuralObserver* observer) {
    if (meta->kind == TSKind::TSD) {
        auto* map_storage = static_cast<value::MapStorage*>(vd.value_data);
        observer->register_with(&map_storage->key_set());
    }
}
```

The `register_with()` method:
1. Unregisters from any previous KeySet (if re-registering)
2. Stores the KeySet pointer for cleanup on destruction
3. Calls `key_set->add_observer(this)`

The destructor automatically unregisters from the KeySet, ensuring proper cleanup
when the TSOutput (and its structural observers) are destroyed.

#### Lifecycle Management

- Observers are owned by `TSOutput::structural_observers_` (vector of unique_ptr)
- Non-copyable, non-movable (registered with KeySet by raw pointer)
- Destructor auto-unregisters from KeySet if still registered

This design ensures alternatives stay synchronized with native structural changes
without requiring polling or manual synchronization.

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

Binding establishes Links from input leaves to output values. The actual implementation delegates to TSView for core binding and separately manages subscription lifecycle:

```cpp
void TSInputView::bind(TSOutputView& output) {
    // 1. Delegate link creation to TSView layer
    //    TSView::bind() handles populating the Link's ViewData from output
    ts_view_.bind(output.ts_view());

    // 2. Track the bound output for subscription management
    bound_output_ = output.output();

    // 3. Subscribe for notifications if active
    if (input_ && input_->active()) {
        output.subscribe(input_);
    }
}

void TSInputView::unbind() {
    // 1. Unsubscribe if we were active and bound
    if (bound_output_ && input_ && input_->active()) {
        TSOutputView output_view = bound_output_->view(ts_view_.current_time());
        output_view.unsubscribe(input_);
    }

    // 2. Clear the bound output reference
    bound_output_ = nullptr;

    // 3. Delegate link removal to TSView layer
    ts_view_.unbind();
}
```

### SIGNAL Bind-Time Resolution

For `TSInputView` / `TSInput` binding of SIGNAL inputs, target selection is resolved at bind-time (not on each read):

1. `SIGNAL[impl]`: bind directly to implementation target.
2. Plain `SIGNAL` + `TSD[...]` source: bind to `key_set` projection (`ViewProjection::TSD_KEY_SET`).
3. Otherwise: bind directly to source view.

This keeps SIGNAL behavior in the binding layer and avoids runtime fallback branches in `modified`, `valid`, and `last_modified_time`.

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

## Cross-Graph Wiring

Nested graph nodes (NestedGraphNode, ComponentNode, TryExceptNode) contain inner graphs whose boundary nodes must be wired to the outer component's TSInput/TSOutput. TSInput and TSOutput objects are fixed once created, so all wiring happens through links and forwarding.

### Input Wiring: Inner Stub Reads Outer's Input Data

The inner graph's stub source node has its own TSInput with an unpopulated LinkTarget. The outer component's TSInput has per-field LinkTargets already populated during normal graph wiring (pointing to the upstream output's data).

When we navigate the outer input view to a field, the returned view's ViewData contains **resolved** pointers (LinkTarget indirection already followed). We bind the inner node's TSInput to these resolved pointers:

```cpp
auto outer_field = outer_input_view.field("a");
auto inner_input_view = inner_node->ts_input()->view(time);
inner_input_view.ts_view().bind(outer_field.ts_view());
```

This calls `ts_ops::bind` which:
1. Copies the resolved upstream data pointers into the inner's LinkTarget via `store_to_link_target`
2. Sets `owner_time_ptr` to the inner's own time slot (time-accounting chain)
3. Subscribes inner's LinkTarget to upstream's observer list

After this, the inner stub node reads upstream data via its LinkTarget and gets time-accounting via `LinkTarget::notify()`. Node-scheduling is set up later via `set_active(true)` during `_initialise_inputs()` at inner graph start.

**Ordering**: `bind` during `wire_graph()` (in `initialise()`), before inner graph `start()`.

### Output Wiring: Inner Sink Writes to Outer's Output Data

The inner sink node's TSOutput needs to redirect all writes to the outer component's TSOutput storage. This is achieved through a `forwarded_target_` LinkTarget on TSOutput.

**Why inner→outer direction?** During outer graph wiring (before `initialise()`), downstream TSInput LinkTargets are populated with raw pointers to the outer TSOutput's storage. These pointers are fixed. An outer→inner link would require downstream reads to follow an additional indirection they don't support. So the forwarding redirects inner **writes** to outer's storage.

```cpp
class TSOutput {
    LinkTarget forwarded_target_;  // Populated during cross-graph wiring
    // ...
    TSOutputView view(engine_time_t current_time) {
        if (forwarded_target_.is_linked) {
            // All 7 fields from outer's storage
            ViewData vd = make_view_data_from(forwarded_target_);
            return TSOutputView(TSView(vd, current_time), this);
        }
        // Normal path: use own native_value_
        return TSOutputView(native_value_.ts_view(current_time), this);
    }
};
```

`store_to_link_target` copies all 7 target-data fields including `ops` and `meta`, so every operation through the forwarded view uses the outer's ts_ops vtable, outer's time storage, and outer's observer list. Mutations land directly in outer storage and notify downstream nodes.

During `wire_graph()`:
```cpp
ViewData outer_data = ts_output()->native_value().make_view_data();
store_to_link_target(inner_node->ts_output()->forwarded_target(), outer_data);
```

### TryExceptNode Special Case

TryExceptNode's output is a bundle with "out" and "exception" fields. The inner graph's sink maps to the "out" sub-field:

```cpp
auto out_field_view = ts_output()->view(time).field("out");
store_to_link_target(inner->ts_output()->forwarded_target(), out_field_view.view_data());
```

### Graph Lifecycle Ordering

1. **Outer graph wiring** (before `initialise()`): Downstream TSInput LinkTargets populated with pointers to outer TSOutput storage
2. **`initialise()`**: Creates inner graph, calls `wire_graph()`
3. **`wire_graph()`**: Binds inner inputs + sets up output forwarding
4. **`start()`**: Calls `_initialise_inputs()` on inner nodes, which calls `set_active(true)` to subscribe for node-scheduling

## Open Questions

- TODO: Error propagation through the graph?
- TODO: Lazy binding for dynamic graphs?

## References

- User Guide: `05_TSOUTPUT_TSINPUT.md`
- Research: `01_BASE_TIME_SERIES.md`
