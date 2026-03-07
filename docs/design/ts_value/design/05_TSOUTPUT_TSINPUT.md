# TSOutput and TSInput Design

## Status Note (2026-03-07)

This document still describes the original endpoint direction, including alternative-heavy output handling.

That approach proved too complex in the previous branch. The next implementation should prefer a smaller model centered on one native runtime representation plus explicit projection/adaptation points.

See `09_SIMPLIFIED_RUNTIME.md` for the current clean implementation direction.

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

    // Endpoint views use owning_node_->cached_evaluation_time_ptr() internally.
    TSView view();                                       // Native schema
    TSView view_for_input(const TSInput& input);         // Input schema (native or alternative)
    TSOutputView output_view();
    TSOutputView output_view_for_input(const TSInput& input);

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

    // Observer wiring is owned by binding/activation in TSInput; TSOutputView
    // does not expose subscribe/unsubscribe APIs.

    // Navigation - wraps TSView navigation
    TSOutputView field(std::string_view name);
    TSOutputView operator[](size_t index);
    TSOutputView operator[](View key);

    // Path access
    ShortPath short_path() const;
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

Alternatives are created on-demand when a bound input requests a different schema.
The endpoint APIs handle this transparently:

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

TSView TSOutput::view_for_input(const TSInput& input) {
    const TSMeta* input_meta = input.meta();
    return input_meta == native_value_.meta() ? view() : view_for_schema(input_meta);
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
    TSValue value_;
    Value active_value_;
    const TSMeta* meta_;
    Node* owning_node_;
    size_t port_index_;                         // Endpoint id on owning node

public:
    TSView view();                              // Uses owning_node cached time pointer
    TSInputView input_view();

    // Subscription control
    void set_active(bool active);
    void set_active(const TSView& ts_view, bool active);

    // Notifiable interface - called when source changes
    void notify(engine_time_t et) override;

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

TSInputView wraps TSView and delegates bind/activation behavior back to TSInput:

```cpp
class TSInputView {
    TSInput* owner_;
    TSView ts_view_;

public:
    View value() const { return ts_view_.value(); }
    DeltaView delta_value() const { return ts_view_.delta_value(); }
    bool modified() const { return ts_view_.modified(); }
    bool valid() const { return ts_view_.valid(); }

    void bind(const TSOutputView& output);
    void unbind();

    void make_active();
    void make_passive();
    bool active() const;
    ShortPath short_path() const;
    FQPath fq_path() const;
};
```

### Binding Process

Binding establishes Links from input leaves to output values. The implementation delegates link setup/teardown to TSView and activation state to TSInput owner logic:

```cpp
void TSInputView::bind(const TSOutputView& output) {
    ts_view_.bind(output.as_ts_view());
    if (owner_ && owner_->active(ts_view_)) {
        // Active inputs that bind later must re-attach notifier wiring immediately.
        owner_->set_active(ts_view_, true);
    }
}

void TSInputView::unbind() {
    ts_view_.unbind();
}
```

### SIGNAL Bind-Time Resolution

For `TSInputView` / `TSInput` binding of SIGNAL inputs, target selection is resolved at bind-time (not on each read):

1. `SIGNAL[impl]`: bind directly to implementation target.
2. Plain `SIGNAL` + `TSD[...]` source: bind to `key_set` projection (`ViewProjection::TSD_KEY_SET`).
3. Otherwise: bind directly to source view.

Bind-time resolution is the primary mechanism. Runtime read paths still include explicit SIGNAL semantics for link rebind transitions and direct SIGNAL-source handling to preserve Python parity during dynamic rewires.

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
void TSInput::notify(engine_time_t et) override {
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
    └── TSView (+ engine time reference) - returned from access methods
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
auto inner_input_view = inner_node->ts_input()->input_view();
inner_input_view.set_current_time_ptr(&time);
inner_input_view.ts_view().bind(outer_field.ts_view());
```

This calls `ts_ops::bind` which:
1. Copies the resolved upstream data pointers into the inner's LinkTarget via `store_to_link_target`
2. Sets `owner_time_ptr` to the inner's own time slot (time-accounting chain)
3. Subscribes inner's LinkTarget to upstream's observer list

After this, the inner stub node reads upstream data via its LinkTarget and gets time-accounting via `LinkTarget::notify()`. Node-scheduling is set up later via `set_active(true)` during `_initialise_inputs()` at inner graph start.

**Ordering**: `bind` during `wire_graph()` (in `initialise()`), before inner graph `start()`.

### Output Wiring: Inner Sink Writes to Outer's Output Data

The inner sink node's TSOutput needs to redirect all writes to the outer component's TSOutput storage. This is achieved through a **Node-level output override** mechanism.

**Why inner→outer direction?** During outer graph wiring (before `initialise()`), downstream TSInput LinkTargets are populated with raw pointers to the outer TSOutput's storage. These pointers are fixed. An outer→inner link would require downstream reads to follow an additional indirection they don't support. So the forwarding redirects inner **writes** to outer's storage.

**Mechanism**: `Node::_output_override_node` — a raw pointer on Node that, when set, causes `Node::output()` to delegate to the override node's output view:

```cpp
class Node {
    node_ptr _output_override_node{nullptr};  // Set during cross-graph wiring

public:
    TSOutputView output() const {
        if (_output_override_node != nullptr && _output_override_node != this) {
            return _output_override_node->output();  // Delegate to outer node
        }
        // Normal path: use own TSOutput
        return _output.output_view();
    }

    void set_output_override(node_ptr source_node) noexcept;
    void clear_output_override() noexcept;
};
```

When the inner sink node calls `output()`, it receives the **outer node's** TSOutputView. All writes from inner code land directly in the outer TSOutput's storage, using the outer's ts_ops vtable, time storage, and observer list. Mutations notify downstream nodes through the outer's observer infrastructure.

During `wire_graph()`:
```cpp
// NestedGraphNode / ComponentNode
auto inner_sink_node = m_active_graph_->nodes()[m_output_node_id_];
inner_sink_node->set_output_override(this);  // "this" = outer node
```

**Design rationale**: The output override lives at the Node level rather than inside TSOutput. This keeps TSOutput's API clean (no forwarding concept) and makes the redirection a graph-wiring concern rather than a time-series concern. The inner sink node's own TSOutput is effectively unused — all output operations go through the override.

**Cleanup**: On `dispose()`, the override is cleared to prevent dangling pointers:
```cpp
inner_sink_node->clear_output_override();
```

### TryExceptNode Special Case

TryExceptNode uses a **hybrid approach** because its output is a bundle with "out" and "exception" fields, where only the "out" sub-field maps to the inner graph's sink:

1. **Binding**: During `wire_outputs()`, the outer "out" field is bound to the inner output view for observer notification setup:
```cpp
auto outer_bundle = outer_view.try_as_bundle();
auto out_ts = outer_bundle->field("out");
out_ts.as_ts_view().bind(inner_view.as_ts_view());
```

2. **Explicit copy**: During `do_eval()`, when the inner output is modified, values are explicitly copied to the outer "out" field:
```cpp
if (inner_view.modified() && inner_view.valid()) {
    out_ts.copy_from_output(inner_view);
}
```

This differs from the override pattern because the inner sink writes to its own TSOutput (not the outer's), and the outer node copies the result after evaluation. The "exception" field is written directly by the outer TryExceptNode when an exception is caught.

### Graph Lifecycle Ordering

1. **Outer graph wiring** (before `initialise()`): Downstream TSInput LinkTargets populated with pointers to outer TSOutput storage
2. **`initialise()`**: Creates inner graph, calls `wire_graph()`
3. **`wire_graph()`**: Binds inner inputs + sets up output override (`set_output_override`)
4. **`start()`**: Calls `_initialise_inputs()` on inner nodes, which calls `set_active(true)` to subscribe for node-scheduling
5. **`dispose()`**: Clears output override (`clear_output_override`) before releasing inner graph

## Open Questions

- TODO: Error propagation through the graph?
- TODO: Lazy binding for dynamic graphs?

## References

- User Guide: `05_TSOUTPUT_TSINPUT.md`
- Research: `01_BASE_TIME_SERIES.md`
