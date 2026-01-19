# TSOutput and TSInput: Graph Endpoints

**Parent**: [Overview](00_OVERVIEW.md)

---

## Overview

TSOutput and TSInput are the **graph endpoints** - dedicated objects that connect nodes to the data flow network. They are **not** lightweight TSValue wrappers but rather specialized objects with distinct responsibilities, composed from TSValue while exposing their own API.

- **TSOutput**: The data source - owns and publishes values, manages alternative representations
- **TSInput**: The data consumer - binds to outputs, controls notification subscription

Both utilize [Links](04_LINKS_AND_BINDING.md) internally for their binding behavior.

---

## TSOutput: The Data Source

### Structure

TSOutput owns and manages **multiple representations** of its data:

```
┌─────────────────────────────────────────────────────────────┐
│  TSOutput                                                    │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  native_value_: TSValue                              │   │
│  │  (native schema of the output)                       │   │
│  │                                                       │   │
│  │  Contains:                                           │   │
│  │  ├── data_value_                                     │   │
│  │  ├── time_value_                                     │   │
│  │  └── observer_value_ ← Manages observer list         │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  alternatives_: map<TSMeta*, TSValue>                │   │
│  │                                                       │   │
│  │  schema_A → TSValue (cast representation)            │   │
│  │  schema_B → TSValue (cast representation)            │   │
│  │  ...                                                  │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  owning_node_: Node*                                 │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Core API:                                                   │
│    view(engine_time_t, TSMeta&) → TSOutputView              │
│    apply_value(DeltaValue)                                  │
│                                                              │
│  TSOutputView API (type-erased):                            │
│    value(), delta(), modified(), valid()                    │
│    set_value(), apply_delta()                               │
│    subscribe(), unsubscribe()                               │
│    Navigation: field(), operator[], navigate()              │
│    (does NOT support: bind, make_active/passive)            │
└─────────────────────────────────────────────────────────────┘
```

**Note**: TSOutput follows the view pattern. Call `view(current_time, schema)` to get a type-erased `TSOutputView`. If the schema matches the native schema, returns a view of `native_value_`. If the schema differs (but is compatible), creates or returns an existing alternative representation. Observer management is handled by the TSValue's `observer_value_` component.

### Responsibilities

1. **Own the native value**: The primary TSValue in the output's declared schema
2. **Manage alternative representations**: Created on-demand when inputs require different schemas (cast)
3. **Keep alternatives in sync**: When native value changes, propagate to all alternatives
4. **Notify observers**: When modified, notify all subscribed inputs

### Native Value

The native value is always present and represents the output's declared type:

```cpp
class TSOutput {
    TSValue native_value_;  // Always exists, schema matches output's declared type
    std::map<const TSMeta*, TSValue> alternatives_;
    Node* owning_node_;

public:
    TSOutput(const TSMeta& ts_meta, Node* owner)
        : native_value_(ts_meta), owning_node_(owner) {}

    // View access - returns type-erased TSOutputView
    // If schema matches native, returns view of native_value_
    // If schema differs (but compatible), creates/returns alternative
    TSOutputView view(engine_time_t time, const TSMeta& schema) {
        if (&schema == &native_value_.ts_meta()) {
            return TSOutputView(&native_value_, this, time);
        }
        // Get or create alternative for this schema
        TSValue& alt = get_or_create_alternative(schema);
        return TSOutputView(&alt, this, time);
    }

    // Bulk mutation via delta (applies to native, syncs to alternatives)
    void apply_value(const DeltaValue& delta) {
        native_value_.apply_delta(delta);
        sync_alternatives();
    }

    Node* owning_node() const { return owning_node_; }

private:
    TSValue& get_or_create_alternative(const TSMeta& schema) {
        auto it = alternatives_.find(&schema);
        if (it != alternatives_.end()) {
            return it->second;
        }
        auto& alt = alternatives_.emplace(&schema, TSValue(schema)).first->second;
        establish_sync(native_value_, alt);
        return alt;
    }
};

// TSOutputView is type-erased, tracks navigation path for composite access
class TSOutputView {
    void* data_;
    ts_output_ops* ops_;
    // Path tracking for navigation (field access, indexing)
    // Enables tracing back to owning node/output

public:
    // TSView-like accessors
    View value();
    DeltaView delta();
    bool modified();
    bool valid();

    // Output-specific mutation
    void set_value(View v);
    void apply_delta(DeltaView dv);

    // Observer management (delegates to observer_value_)
    void subscribe(TSInput* input);
    void unsubscribe(TSInput* input);

    // Navigation - returns new views with updated path
    TSOutputView field(std::string_view name);
    TSOutputView operator[](size_t index);
    TSOutputView operator[](View key);
    TSOutputView navigate(std::string_view path);
};
```

### Alternative Representations (Cast)

When an input's schema differs from the output's native schema, the `view(time, schema)` method automatically creates and returns a view of an alternative representation:

```cpp
// Input needs TSD[str, REF[TS[int]]] but output is TSD[str, TS[int]]
TSOutputView out_view = output.view(current_time, ref_schema);
// Internally creates alternative if needed, returns view of it

// Multiple inputs with same cast requirement share the alternative
TSOutputView view1 = output.view(current_time, ref_schema);  // Same alternative
TSOutputView view2 = output.view(current_time, ref_schema);  // Same alternative
```

Alternatives are indexed by schema pointer, allowing multiple inputs with the same cast requirement to share a single alternative representation. The `view()` method encapsulates the get-or-create logic.

### Observer Management

Observer management is handled by TSValue's `observer_value_` component. The `TSOutputView` exposes subscription methods:

```cpp
// Usage via view
TSOutput output(ts_meta);
TSOutputView view = output.view(current_time, native_schema);

// Subscribe/unsubscribe through the view
view.subscribe(input_ptr);
view.unsubscribe(input_ptr);

// Internally, TSOutputView delegates to observer_value_
class TSOutputView {
public:
    void subscribe(TSInput* input) {
        // Delegates to TSValue's observer_value_
        get_ts_value()->subscribe(input);
    }

    void unsubscribe(TSInput* input) {
        get_ts_value()->unsubscribe(input);
    }
};
```

---

## TSInput: The Data Consumer

### Structure

TSInput owns a **single TSValue** representing its view of bound data:

```
┌─────────────────────────────────────────────────────────────┐
│  TSInput                                                     │
│                                                              │
│  ┌──────────────────────────────────┐                       │
│  │  value_: TSValue                 │                       │
│  │  (input's schema)                │                       │
│  │                                  │                       │
│  │  Structure:                      │                       │
│  │  ├── non-peered nodes (local)    │                       │
│  │  └── LINK leaves → output values │                       │
│  └──────────────────────────────────┘                       │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  active_value_: Value                                │   │
│  │  (schema mirrors TS schema structure)                │   │
│  │                                                       │   │
│  │  Tracks active/passive state at each level:          │   │
│  │  ├── TSB[a, b] → Bundle[a: bool, b: ...]            │   │
│  │  ├── TSL[TS[T], N] → List[bool, N]                  │   │
│  │  └── TS[T] → bool (leaf)                            │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────┐                       │
│  │  owning_node_: Node*             │  ← For scheduling     │
│  └──────────────────────────────────┘                       │
│                                                              │
│  Core API:                                                   │
│    view(engine_time_t, TSMeta&) → TSInputView               │
│                                                              │
│  TSInputView API (type-erased):                             │
│    value(), delta(), modified(), valid()                    │
│    bind(), unbind()                                         │
│    make_active(), make_passive(), active()                  │
│    Navigation: field(), operator[], navigate()              │
│    (does NOT support: set_value, subscribe/unsubscribe)     │
└─────────────────────────────────────────────────────────────┘
```

**Note**: The `active_value_` mirrors the TS schema structure, allowing active/passive state to be tracked at each level of a composite input. This is analogous to how `time_value_` and `observer_value_` track modification time and observers at each level in TSValue.

### Responsibilities

1. **Own local structure**: The TSValue contains non-peered nodes for the input's schema
2. **Maintain LINKs**: Leaf nodes are LINKs pointing to bound output values
3. **Control subscription**: Active/passive state determines notification behavior
4. **Schedule owning node**: When notified, schedule the node for evaluation

### Value Structure

The input's TSValue has a mixed structure:
- **Non-peered nodes**: Internal structure (bundles, lists, etc.) owned locally
- **LINK leaves**: Terminal nodes that point to output values

```
Input Schema: TSB[a: TS[int], b: TSL[TS[float], 2]]

TSInput.value_:
├── TSB (non-peered, local)
│   ├── a: LINK → Output1.native_value_.a
│   └── b: TSL (non-peered, local)
│       ├── [0]: LINK → Output2.native_value_
│       └── [1]: LINK → Output3.native_value_
```

### Binding

When an input binds to an output, it establishes LINKs from its leaf nodes to the appropriate output values. Binding is done through the `TSInputView`:

```cpp
class TSInput {
    TSValue value_;
    Value active_value_;  // Mirrors TS schema, tracks active state at each level
    Node* owning_node_;

public:
    TSInput(const TSMeta& ts_meta, Node* owner)
        : value_(ts_meta)
        , active_value_(derive_active_schema(ts_meta))  // bool at each TS level
        , owning_node_(owner) {}

    // View access - returns type-erased TSInputView
    TSInputView view(engine_time_t time, const TSMeta& schema) {
        return TSInputView(&value_, &active_value_, this, time, schema);
    }
};

// TSInputView is type-erased, tracks navigation path for composite access
class TSInputView {
    void* data_;
    ts_input_ops* ops_;
    // Path tracking for navigation (field access, indexing)
    // Enables tracing back to owning node/input and active_value_ position

public:
    // TSView-like accessors (read-only)
    View value();
    DeltaView delta();
    bool modified();
    bool valid();

    // Input-specific binding
    void bind(TSOutputView& output);
    void unbind();

    // Subscription control (operates on active_value_ at current path)
    void make_active();
    void make_passive();
    bool active();        // Active at current path
    bool any_active();    // Any child active (for composites)
    bool all_active();    // All children active (for composites)

    // Navigation - returns new views with updated path
    TSInputView field(std::string_view name);
    TSInputView operator[](size_t index);
    TSInputView operator[](View key);
    TSInputView navigate(std::string_view path);
};

// Usage - input and output may have different schemas
TSInput input(input_schema, owning_node);
TSInputView in_view = input.view(current_time, input_schema);

TSOutput output(output_schema);
// Output provides view with input's schema - creates alternative if needed
TSOutputView out_view = output.view(current_time, input_schema);

in_view.bind(out_view);  // Establishes LINK, subscribes if active
```

### Subscription Control

The active state controls whether the input receives notifications. Because `active_value_` mirrors the TS schema structure, active/passive can be controlled at any level of a composite input:

```cpp
// Usage via view - active state is per-path
TSInputView view = input.view(current_time, input_schema);

// Make entire input active
view.make_active();

// Or control at finer granularity
view.field("prices").make_active();      // Only prices field active
view.field("metadata").make_passive();   // Metadata passive

// Check active state at current navigation path
bool is_active = view.active();

// For composites, can check if any/all children active
bool any = view.any_active();
bool all = view.all_active();
```

The `active_value_` structure enables fine-grained subscription control:

```cpp
// Example: TSB[prices: TSL[TS[float], 10], metadata: TS[str]]
//
// active_value_ structure:
// Bundle {
//   prices: List[bool, 10]   // Each element can be active/passive
//   metadata: bool           // Leaf active state
// }

// Making prices[3] active subscribes only to that element's output
view.field("prices")[3].make_active();
```

Internally, `make_active()` and `make_passive()` update the corresponding position in `active_value_` and subscribe/unsubscribe from the bound output's `observer_value_`.

---

## API Comparison

| Aspect | TSOutput / TSOutputView | TSInput / TSInputView |
|--------|-------------------------|----------------------|
| **Core owns** | Native TSValue + alternatives map | Single TSValue |
| **View access** | `view(time, schema)` → TSOutputView | `view(time, schema)` → TSInputView |
| **Read** | `value()`, `delta()`, `modified()` | `value()`, `delta()`, `modified()` |
| **Mutation** | `set_value()`, `apply_delta()` | Read-only (no mutation methods) |
| **Bind** | Not applicable (sources) | `bind()`, `unbind()` |
| **Active/Passive** | Not applicable | `make_active()`, `make_passive()` |
| **Observers** | `subscribe()`, `unsubscribe()` | Is an observer, receives notifications |
| **Cast support** | `view(time, schema)` creates alt if needed | Passes schema to output's `view()` |

---

## Composition Model

Both TSOutput and TSInput are **composed from** TSValue and follow the **view pattern** - access is through type-erased views with schema parameter:

```cpp
class TSOutput {
    TSValue native_value_;  // Contains data, time, AND observer management
    std::map<const TSMeta*, TSValue> alternatives_;
    Node* owning_node_;

public:
    // View access - schema determines native vs alternative
    TSOutputView view(engine_time_t time, const TSMeta& schema);

    // Bulk mutation (applies to native, syncs alternatives)
    void apply_value(const DeltaValue& delta);

    Node* owning_node() const;
};

class TSInput {
    TSValue value_;
    Value active_value_;  // Mirrors TS schema, bool at each level
    Node* owning_node_;

public:
    // View access
    TSInputView view(engine_time_t time, const TSMeta& schema);

    // Notification callback
    void on_peer_modified();
};

// Type-erased views provide the actual API and track navigation paths
class TSOutputView {
    void* data_;
    ts_output_ops* ops_;
    // Path tracking enables tracing back to owning node
    // Methods: value(), delta(), modified(), set_value(), subscribe()
    // Navigation: field(), operator[], navigate()
};

class TSInputView {
    void* data_;
    ts_input_ops* ops_;
    // Path tracking enables tracing back to owning node
    // Methods: value(), delta(), modified(), bind(), make_active()
    // Navigation: field(), operator[], navigate()
};
```

This composition approach:
- Encapsulates the TSValue implementation details
- Allows TSOutput and TSInput to have different APIs appropriate to their roles
- Enables future implementation changes without affecting the external interface

---

## UML Diagrams

### Class Structure

```mermaid
classDiagram
    class TSOutput {
        -TSValue native_value_
        -map~TSMeta*, TSValue~ alternatives_
        -Node* owning_node_
        +view(time, schema) TSOutputView
        +apply_value(delta: DeltaValue) void
        +owning_node() Node*
    }

    class TSOutputView {
        <<type-erased>>
        -void* data_
        -ts_output_ops* ops_
        +value() View
        +delta() DeltaView
        +modified() bool
        +valid() bool
        +set_value(v: View) void
        +apply_delta(dv: DeltaView) void
        +subscribe(input: TSInput*) void
        +unsubscribe(input: TSInput*) void
        +field(name) TSOutputView
        +operator[](index) TSOutputView
        +navigate(path) TSOutputView
    }

    class TSInput {
        -TSValue value_
        -Value active_value_
        -Node* owning_node_
        +view(time, schema) TSInputView
        +on_peer_modified() void
    }

    class TSInputView {
        <<type-erased>>
        -void* data_
        -ts_input_ops* ops_
        +value() View
        +delta() DeltaView
        +modified() bool
        +valid() bool
        +bind(output: TSOutputView&) void
        +unbind() void
        +make_active() void
        +make_passive() void
        +active() bool
        +any_active() bool
        +all_active() bool
        +field(name) TSInputView
        +operator[](index) TSInputView
        +navigate(path) TSInputView
    }

    class TSValue {
        -Value data_value_
        -Value time_value_
        -Value observer_value_
        +subscribe(observer) void
        +unsubscribe(observer) void
        +notify_observers() void
    }

    class Node {
        -vector~TSInput*~ inputs_
        -vector~TSOutput*~ outputs_
        +schedule() void
        +evaluate() void
    }

    TSOutput "1" *-- "1" TSValue : native_value_
    TSOutput "1" *-- "*" TSValue : alternatives_
    TSOutput ..> TSOutputView : creates
    TSInput "1" *-- "1" TSValue : value_
    TSInput ..> TSInputView : creates
    TSValue "1" --> "*" TSInput : observer_value_ notifies
    TSInput "*" --> "1" Node : owned by
    TSOutput "*" --> "1" Node : owned by

    note for TSOutput "view(time, schema) returns\nnative or alternative view"
    note for TSOutputView "Type-erased view with\npath tracking. Navigation\nreturns new views."
    note for TSInput "active_value_ mirrors TS\nschema for per-level\nactive/passive control."
    note for TSInputView "Type-erased view with\npath tracking. Navigation\nreturns new views."
    note for TSValue "observer_value_ manages\nthe list of subscribed\nobservers."
```

### Input Binding Flow

```mermaid
sequenceDiagram
    participant IV as TSInputView
    participant I as TSInput
    participant OV as TSOutputView
    participant O as TSOutput
    participant N as Node

    Note over IV,N: Get views with schema
    I->>IV: view(time, input_schema)
    O->>OV: view(time, input_schema)
    Note over O: Creates alternative if schema differs

    Note over IV,N: Binding
    IV->>OV: bind(output_view)
    IV->>I: establish_link to TSValue behind view
    IV->>OV: subscribe(input)
    OV->>O: delegate to observer_value_

    Note over IV,N: Runtime notification
    OV->>OV: set_value(new_data)
    O->>O: sync_alternatives()
    O->>I: notify via observer_value_
    I->>N: schedule()
```

### Output Alternative Management

```mermaid
flowchart TD
    subgraph TSOutput
        subgraph "native_value_: TSValue"
            NV_DATA[data_value_<br/>TSD str TS int]
            NV_TIME[time_value_]
            NV_OBS[observer_value_<br/>manages observers]
        end
        subgraph "alternatives_"
            A1[schema_A<br/>TSD str REF TS int]
            A2[schema_B<br/>...]
        end
    end

    subgraph "Input A (needs REF)"
        IA[TSInput.value_]
    end

    subgraph "Input B (needs REF)"
        IB[TSInput.value_]
    end

    subgraph "Input C (native schema)"
        IC[TSInput.value_]
    end

    NV_DATA -->|sync| A1
    NV_DATA -->|sync| A2
    A1 -.->|LINK| IA
    A1 -.->|LINK| IB
    NV_DATA -.->|LINK| IC

    NV_OBS -->|notify| IA
    NV_OBS -->|notify| IB
    NV_OBS -->|notify| IC

    style NV_DATA fill:#e3f2fd
    style NV_OBS fill:#e8f5e9
    style A1 fill:#fff3e0
    style A2 fill:#fff3e0
```

### Input Value Structure

```mermaid
flowchart TD
    subgraph "TSInput.value_ (TSB schema)"
        ROOT[TSB root<br/>non-peered]
        FA[field_a<br/>LINK]
        FB[field_b: TSL<br/>non-peered]
        FB0[elem 0<br/>LINK]
        FB1[elem 1<br/>LINK]

        ROOT --> FA
        ROOT --> FB
        FB --> FB0
        FB --> FB1
    end

    subgraph "Bound Outputs"
        O1[Output1.value_]
        O2[Output2.value_]
        O3[Output3.value_]
    end

    FA -.->|LINK| O1
    FB0 -.->|LINK| O2
    FB1 -.->|LINK| O3

    style ROOT fill:#e8f5e9
    style FB fill:#e8f5e9
    style FA fill:#fff9c4
    style FB0 fill:#fff9c4
    style FB1 fill:#fff9c4
```

---

## Relationship to Other Concepts

### TSValue
TSOutput and TSInput are **composed from** TSValue. See [Time-Series](03_TIME_SERIES.md) for TSValue details.

### Links
The binding mechanism uses [Links](04_LINKS_AND_BINDING.md) - inputs establish LINKs to output values. The LINK provides zero-copy access and notification capability.

### Cast
When input and output schemas differ, [Cast](04_LINKS_AND_BINDING.md#cast-logic-schema-conversion-at-bind-time) creates alternative representations. The output owns and syncs these alternatives.

### Memory Stability
Both native values and alternatives must maintain [memory stability](04_LINKS_AND_BINDING.md#memory-stability-requirements) so that LINKs remain valid under mutation.

---

## Best Practices

### For Output Usage

1. **Prefer native schema**: Design outputs with schemas that most consumers need directly
2. **Minimize alternatives**: Each alternative adds sync overhead
3. **Batch mutations**: Multiple set_value() calls trigger multiple syncs; batch when possible

### For Input Usage

1. **Use passive when appropriate**: If an input shouldn't trigger evaluation, make it passive
2. **Match schemas when possible**: Direct binding is more efficient than cast
3. **Bind at appropriate granularity**: Bind whole structures when you need the whole thing

---

## Next

- [Access Patterns](06_ACCESS_PATTERNS.md) - Reading and writing through views
- [Delta and Change Tracking](07_DELTA.md) - Incremental processing
