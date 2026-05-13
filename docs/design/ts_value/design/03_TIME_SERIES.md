# Time Series Design

## Purpose

Time-series infrastructure adds temporal behavior to the existing value system without replacing it.

The core separation is:

- `Value/View` for user-visible payload data
- `TSState` for runtime metadata
- `TSValue` as the owner of both

This is the authoritative direction for the next implementation.

## Design Position (2026-03-07)

The earlier design split runtime metadata into separate time, observer, delta, and link trees.

That approach is now retired.

The preferred model is:

- keep payloads in the existing `Value/View` logic
- give every time-series level exactly one runtime-state node
- make that state-node shape follow the schema kind at that level
- keep endpoint-local and node-local retained state outside the generic `TSValue`

This produces a simpler and more understandable runtime:

- one payload tree
- one runtime-state tree
- explicit endpoint and node state where required

## Core Data Separation

### Payload data

Payload data is the data the user thinks they are working with:

- scalar values
- bundle fields
- list elements
- dict key/value content
- set membership
- window contents

Payload data continues to use the existing `Value/View` ownership, nullability, and navigation rules.

### Runtime state

Runtime state is everything needed to make the payload behave as a time series:

- modification time
- validity and sampled semantics where applicable
- observer registration
- binding/rebinding state
- delta bookkeeping
- REF resolution state
- recursively embedded child state for composite types

Runtime state is not exposed as a second user payload. It is internal runtime structure.

### State kept out of `TSValue`

The generic time-series runtime should not absorb all runtime concerns.

These remain separate:

- `TSInput` activation/subscription state
- `TSInput` endpoint-local collections of non-peered child elements and leaf `LINK`s
- `TSOutput` projection/adaptation caches
- node-local retained keyed state for nested graphs
- operator-specific fallback or reconciliation buffers

That boundary is important. If a piece of state exists only because of a particular endpoint or operator lifecycle, it should not be hidden inside generic per-value runtime storage.

In particular, `TSInput` is still allowed to be special:

- it may hold a collection of non-peered child inputs
- those child structures may terminate in leaf `LINK`s
- that endpoint structure is not the same thing as the generic `TSValue` runtime-state tree

## TSMeta Schema Generation

Each `TSMeta` generates two primary schemas:

1. `value_schema_`: the user-visible payload shape
2. `state_schema_`: the runtime-state shape

The key rule is:

- `value_schema_` follows the payload type system
- `state_schema_` follows the time-series schema structure

### Runtime schema catalogue

| TS kind | Payload shape | Runtime-state shape |
|---------|---------------|---------------------|
| `TS[T]` | `T` | `ScalarState` |
| `TSB[...]` | bundle | `BundleState` |
| `TSL[T]` | list of payloads | `ListState` |
| `TSS[T]` | set payload | `SetState` |
| `TSD[K,V]` | map payload | `DictState` |
| `TSW[T]` | window payload | `WindowState` |
| `REF[T]` | reference payload | `RefState` |
| `SIGNAL` | no payload | `SignalState` |

The runtime state for a kind is a single schema-defined structure for that level. We do not generate separate side-car schemas for time, observer, delta, and links.

## TSValue

`TSValue` owns the payload tree and the runtime-state tree.

Conceptually:

```cpp
class TSValue {
    Value data_value_;
    Value state_value_;
    const TSMeta* meta_;
};
```

### Invariants

1. `data_value_` and `state_value_` always represent the same logical path.
2. Every schema level owns exactly one state node.
3. All per-value runtime behavior is expressed through that state node.
4. Operator-specific retained behavior is not stored in `state_value_`.

## TSState Model

### Common header

Every runtime-state node begins with the same conceptual header:

```cpp
struct TSStateHeader {
    engine_time_t last_modified_time;
    ObserverList observers;
};
```

Concrete kinds extend that header with the fields they require.

### Kind-specific state

```cpp
struct ScalarState {
    TSStateHeader header;
    BindingState binding;
};

struct BundleState {
    TSStateHeader header;
    std::array<StateNode, field_count> children;
};

struct ListState {
    TSStateHeader header;
    BindingState binding;
    ListDeltaState delta;
    DynamicChildStates children;
};

struct SetState {
    TSStateHeader header;
    BindingState binding;
    SetDeltaState delta;
};

struct DictState {
    TSStateHeader header;
    BindingState binding;
    DictDeltaState delta;
    DynamicChildStates value_children;
};

struct WindowState {
    TSStateHeader header;
    BindingState binding;
    WindowRuntimeState window;
};

struct RefState {
    TSStateHeader header;
    BindingState binding;
    RefResolutionState resolution;
    OptionalChildState resolved_child;
};

struct SignalState {
    TSStateHeader header;
};
```

The exact low-level storage layout can still evolve, but the state responsibilities should not.

## Binding State Lives In The State Node

Binding is not a separate tree. It is part of the runtime state for the level that is bound.

Conceptually:

```cpp
struct BindingState {
    BindingMode mode;                 // unbound, direct, ref-driven, projected
    TargetIdentity source_identity;   // nominal source
    TargetIdentity effective_target;  // current resolved target
};
```

This matters because:

- rebinding is a state transition on the level
- nested graphs need a stable concept of effective target identity
- notification and modification stamping belong to the same level state that describes the binding

The implementation may still use helper objects such as `LinkTarget` or `REFLink`, but they should be understood as payloads inside the per-level state node, not as justification for a separate link tree.

## Delta State Lives In The Same Node

Delta tracking is also part of the per-level runtime state.

Examples:

- `SetState` owns membership delta
- `DictState` owns added/removed/updated key delta
- `ListState` owns element-update bookkeeping when list semantics need it
- `WindowState` owns whatever runtime state is required for window expiration and removal tracking

This keeps all "what changed at this level" information attached to the level that changed.

User-facing `DeltaView` and persistent `DeltaValue` remain useful concepts, but the runtime bookkeeping that produces them belongs in `TSState`.

## TSView

`TSView` is the non-owning view over a logical time-series position.

It navigates payload and runtime state in lockstep.

Conceptually:

```cpp
class TSView {
    View data_view_;
    View state_view_;
    const engine_time_t* engine_time_ptr_;
};
```

Where:

- `data_view_` follows the existing `Value/View` logic for payload access
- `state_view_` gives access to the per-level runtime state
- kind-specific wrappers are built on top of those two views

### State-side operations

The time-series operations for a `TSView` should be driven from the state side.

That means:

- data reads and writes operate through `data_view_`
- time, modified, valid, observer, binding, and delta queries operate through `state_view_`
- kind-specific state dispatch should come from the state-view side because that is where the time-series runtime contract lives
- the runtime should be able to look up the appropriate state operations from `TSMeta` plus a small runtime role context rather than manually threading vtables through endpoint code
- the resolved state ops for a level should expose whether that level is currently peered/non-peered and bound/unbound

This keeps the payload model simple while making runtime semantics explicit.

### Navigation rule

When a `TSView` navigates to a child:

1. navigate `data_view_` to the child payload
2. navigate `state_view_` to the matching child state
3. construct the child `TSView` from the paired result

That rule is what preserves the data/state separation while still making the runtime coherent.

## Worked Examples

### `TS[int]`

```cpp
TSValue {
    data_value_: int
    state_value_: ScalarState {
        header,
        binding
    }
}
```

### `TSB[a: TS[int], b: TSL[TS[float]]]`

```cpp
TSValue {
    data_value_: bundle {
        a: int,
        b: list<float>
    }
    state_value_: BundleState {
        header,
        children: [
            ScalarState {
                header,
                binding
            },
            ListState {
                header,
                binding,
                delta,
                children: [
                    ScalarState { header, binding },
                    ...
                ]
            }
        ]
    }
}
```

### `TSD[str, TS[int]]`

```cpp
TSValue {
    data_value_: map<string, int>
    state_value_: DictState {
        header,
        binding,
        delta,
        value_children: [
            ScalarState { header, binding },
            ...
        ]
    }
}
```

### `REF[TSL[TS[int]]]`

```cpp
TSValue {
    data_value_: reference_payload
    state_value_: RefState {
        header,
        binding,
        resolution,
        resolved_child: ListState { ... }
    }
}
```

## Why This Model Is Better

This separation is cleaner for implementation and debugging:

1. The user payload model stays unchanged.
2. There is only one place to look for runtime metadata at a given level.
3. Binding, delta, and modification tracking no longer need independent mirrored schema walkers.
4. Nested-graph rebinding can compare effective target identity directly from the relevant state node.
5. The generic runtime is smaller because endpoint and node-specific state stays outside it.

## Consequences For The Rest Of The Design

This model changes how the surrounding docs should be read:

- `01_SCHEMA.md`: runtime schema generation should produce payload schema plus runtime-state schema
- `04_LINKS_AND_BINDING.md`: link payloads are part of binding state, not a side-car tree
- `05_TSOUTPUT_TSINPUT.md`: endpoint-local active/subscription state remains outside `TSValue`
- `07_DELTA.md`: delta surfaces remain important, but runtime delta bookkeeping belongs in `TSState`
- `09_SIMPLIFIED_RUNTIME.md`: this document is the detailed runtime-storage expression of that simplified direction
