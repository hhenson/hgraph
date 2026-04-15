# RFC: v2 Nested Graph Runtime and Boundary Binding

Date: 2026-04-14  
Status: Draft for implementation  
Scope: Define the `v2` runtime model for nested graphs, keyed nested operators, graph-boundary binding, and the future of nested stubs.

## 1. Context

The repository is currently split across two execution worlds:

1. The default C++ execution path for ordinary Python-backed nodes now prefers the `v2` builder/runtime surface.
2. The nested-graph operators still depend on the legacy nested-node/builder stack.
3. The bridge between them rejects `v2.GraphBuilder` child graphs.

As a result:

1. `nested_graph`, `map_`, `reduce`, `switch_`, `try_except`, and `component` do not currently execute as proper nested graphs in the C++ path.
2. `mesh_` additionally hits mixed-graph failures because it still depends on non-`v2` support nodes.
3. context-sensitive graphs are also blocked because `v2` Python-backed nodes do not yet support `context_inputs`.

This RFC assumes the desired outcome is the "new world":

1. nested graphs are first-class `v2` runtime capability
2. no tactical compatibility layer is added as the long-term design
3. no mixed legacy / `v2` graph execution is part of the target state

Python remains the semantic reference implementation.

## 2. Goals

1. Make nested graphs a first-class `v2` runtime feature.
2. Preserve the `v2` philosophy:
   - efficient memory usage
   - erased runtime types
   - reusable `NodeBuilder` / `NodeImpl` / `type_ops` patterns
   - explicit state ownership
3. Avoid reproducing the legacy nested class hierarchy inside `v2`.
4. Introduce explicit reusable graph-boundary binding operations.
5. Keep keyed retained state inside the operator that owns the keyed semantics.
6. Improve readability by reducing implicit runtime conventions and name-based recovery.
7. Leave Python as the behavioral source of truth for parity tests.

## 3. Non-Goals

1. Preserving the legacy nested runtime architecture.
2. Supporting mixed legacy / `v2` graphs as a permanent execution mode.
3. Adding ad hoc one-off runtime fixes per nested operator.
4. Encoding operator-specific keyed semantics inside generic graph infrastructure.
5. Freezing every internal type name in this RFC.

## 4. Design Principles

## 4.1 One graph family

The target system should have one nested-graph execution family:

1. top-level `v2` graphs
2. child `v2` graphs
3. support nodes used inside nested graphs also on `v2`

There should be no "legacy nested island" inside an otherwise `v2` graph.

## 4.2 Reuse through substrate, not inheritance

The legacy system shares nested behavior through a `NestedNode` inheritance family.

That is not the desired `v2` shape.

The `v2` design should instead share behavior through a small set of reusable building blocks:

1. child-graph template
2. child-graph instance
3. nested evaluation clock delegate
4. boundary binding plan
5. boundary binding executor

Each nested operator should remain a normal `v2` node implementation with operator-owned state.

## 4.3 Operator-owned retained state

The generic nested substrate should own:

1. child graph construction and destruction
2. graph-local evaluation clock delegation
3. boundary bind / rebind / unbind operations
4. traits inheritance and inspection identity

The nested operator should own:

1. active keyed graph map
2. rank ordering
3. dependency tracking
4. reduction tree structure
5. branch switching state
6. operator-specific output semantics

This keeps the generic runtime explicit and small.

## 4.4 Compile away pure plumbing where possible

The current nested-stub model introduces runtime nodes that exist only to represent graph boundaries.

The `v2` target should prefer compiling that plumbing into boundary plans rather than carrying it as runtime nodes.

## 4.5 Readability over cleverness

The final nested runtime should be explainable in a small number of concepts.

If a behavior depends on:

1. string prefixes such as `"stub:"`
2. mutating Python callable objects to smuggle runtime state
3. hidden graph-family fallback rules

then the design is too implicit for the target architecture.

## 5. Problem Statement

The current wiring and runtime model for nested graphs has three structural problems.

### 5.1 Nested child graphs do not fit the `v2` graph model

The current `v2.GraphBuilder` is a top-level execution artifact. The legacy nested builders expect a legacy `GraphBuilder` child graph and reject `v2.GraphBuilder`.

That means the current `v2` path has no first-class child-graph substrate.

### 5.2 Boundary plumbing is modeled as runtime graph nodes

Nested graphs are currently wired through input/output stub nodes such as:

1. `stub:<arg>`
2. `stub:__out__`

These nodes are discovered later and rewritten at runtime.

This is convenient for wiring but poor for the final runtime because it:

1. adds pure-plumbing nodes to child graphs
2. consumes memory and endpoint state
3. relies on name conventions
4. leaks binding mechanics into operator implementations

### 5.3 Context and support plumbing are not yet `v2`-complete

The `v2` path still lacks:

1. context-manager input support for Python-backed nodes
2. a `v2`-native context output / lookup path usable inside nested graphs
3. a uniform way to keep support nodes in the same builder family as the rest of the child graph

`mesh_` is the clearest example because it depends on both nested graphs and context-shaped support wiring.

## 6. Proposed Runtime Model

The proposed model introduces a `v2` nested-graph substrate with four main pieces:

1. `ChildGraphTemplate`
2. `ChildGraphInstance`
3. `BoundaryBindingPlan`
4. `NestedGraphClockDelegate`

### 6.1 ChildGraphTemplate

`ChildGraphTemplate` is the compile-time artifact stored in node-builder type state.

It represents:

1. the child graph's compiled `v2.GraphBuilder`
2. the boundary binding plan for its imports and exports
3. any extra child-graph metadata needed for runtime creation

Suggested shape:

```text
ChildGraphTemplate
  graph_builder: v2::GraphBuilder
  boundary_plan: BoundaryBindingPlan
  default_label: string
  flags:
    has_output
    captures_exception
    requires_context
```

This is shared immutable data.

It should live in `NodeBuilder` type state for the nested operator rather than in ad hoc runtime objects.

### 6.2 ChildGraphInstance

`ChildGraphInstance` is the runtime handle owned by a nested operator's state.

It represents one active child graph and the runtime objects needed to manage it.

Suggested shape:

```text
ChildGraphInstance
  graph: v2::Graph
  boundary_runtime: BoundaryBindingRuntime
  instance_id: graph lineage token
  label: string
```

It should expose:

1. `initialise`
2. `start`
3. `stop`
4. `evaluate`
5. `bind`
6. `unbind`
7. `next_scheduled_time`

The nested operator may own:

1. one instance
2. a keyed map of instances
3. a tree or ranked structure pointing at instances

### 6.3 BoundaryBindingPlan

`BoundaryBindingPlan` replaces runtime stub nodes as the primary execution model.

It is an explicit plan describing how a child graph connects to its parent node.

Suggested shape:

```text
BoundaryBindingPlan
  inputs: list<InputBindingSpec>
  outputs: list<OutputBindingSpec>
  context_bindings: list<ContextBindingSpec>

InputBindingSpec
  arg_name: string
  binding_mode: enum
  child_node_index: int
  child_input_path: Path
  ts_schema: TSMeta / equivalent erased schema handle

OutputBindingSpec
  binding_mode: enum
  child_node_index: int
  child_output_path: Path
  parent_output_path: Path or implicit root

ContextBindingSpec
  mode: enum
  context_key: string
  child_node_index: int
  child_path: Path
```

The critical property is that this plan is explicit and runtime-ready.

The runtime should not need to rediscover boundary structure by scanning child nodes by name.

### 6.4 BoundaryBindingRuntime

The runtime executor for the plan performs:

1. initial bind
2. keyed bind for multiplexed arguments
3. rebind on target change
4. unbind / detach during child removal
5. output aliasing or export handoff

It should be shared reusable code, not reimplemented in each nested operator.

### 6.5 NestedGraphClockDelegate

The current nested-evaluation-clock behavior in Python and legacy C++ is the right semantic reference:

1. child graph has its own delegated clock
2. it schedules through the parent node
3. it respects parent node last-evaluation semantics

The `v2` version should be implemented as a clock delegate owned by `ChildGraphInstance`, not by introducing a second graph family.

## 7. Binding Modes

The generic boundary binder should support a small number of explicit modes.

At minimum:

1. `clone_ref_binding`
   - bind child REF input to the same upstream target as the parent input
2. `bind_multiplexed_element`
   - bind child input to a keyed/list element selected by the operator
3. `bind_key_value`
   - bind the child key input from operator-owned scalar/value state
4. `alias_child_output`
   - expose child output directly as parent output
5. `bind_bundle_member_output`
   - support output bundles such as `try_except(...).out`
6. `detach_restore_blank`
   - detach child input and restore an inert local input when a child graph is removed
7. `context_import`
   - import required context from the parent graph boundary
8. `context_export`
   - export child-owned context where required

The goal is not to add many modes.

The goal is to make the real modes small, explicit, and shared.

## 8. Integration With v2 NodeBuilder And type_ops

The nested runtime should reuse the existing `v2` builder pattern.

### 8.1 Builder state

Each nested operator's builder should store immutable nested-graph data in type state:

```text
MapNodeBuilderState
  child_template: ChildGraphTemplate
  multiplexed_args: ...
  key_arg: ...

SwitchNodeBuilderState
  child_templates_by_key: ...
  default_template: ...

ReduceNodeBuilderState
  child_template: ChildGraphTemplate
  lhs_binding: ...
  rhs_binding: ...
```

This fits the current erased builder design:

1. compile once
2. store immutable shared type state
3. construct lean runtime instances

### 8.2 Runtime node data

The node runtime payload should store only operator runtime state.

Example:

```text
MapNodeRuntimeData
  active_graphs: keyed container<ChildGraphInstance>
  scheduled_keys: keyed schedule map
  recordable_id_prefix: optional string
```

The child template is not copied per node.

### 8.3 No new nested-node hierarchy

The recommendation is:

1. do not introduce a `v2::NestedNode` base class mirroring the legacy hierarchy
2. introduce reusable helpers used by normal `NodeImpl<Spec>` implementations

That keeps the `v2` surface consistent and avoids a second object model inside `v2`.

## 9. Nested Stub Strategy

The current stub system is a wiring convenience, not a good final runtime representation.

### 9.1 Current behavior

Nested wiring currently:

1. creates real placeholder input/output nodes
2. emits them into the child graph builder
3. identifies them later by `stub:` names
4. rewires or mutates them at runtime

This is acceptable as a transitional semantic reference but not as the target `v2` design.

### 9.2 Options

#### Option A: Keep runtime stub nodes

Pros:

1. smallest wiring change
2. simplest migration from existing logic

Cons:

1. extra runtime nodes and endpoint state
2. extra slab memory
3. hidden coupling via names and conventions
4. awkward key handling through mutable Python callable objects
5. poorer readability

#### Option B: Keep stubs only at wiring time, then compile them away

Pros:

1. preserves current wiring convenience
2. removes pure-plumbing runtime nodes
3. converts boundary structure into an explicit reusable plan
4. fits the `v2` compiled-builder philosophy

Cons:

1. requires a compilation pass from stub shape to boundary plan
2. still carries some stub concepts in the wiring layer

#### Option C: Replace stubs entirely with dedicated boundary placeholders

Pros:

1. cleanest end state
2. no stub node concept at all
3. strongest readability

Cons:

1. highest initial wiring rewrite cost
2. more invasive change to nested-graph wiring

### 9.3 Recommendation

The recommended path is Option B.

Rationale:

1. It delivers the runtime shape we want without forcing an immediate full rewrite of nested wiring.
2. It removes the runtime cost and indirection of stub nodes.
3. It gives a clean intermediate architecture that can later evolve to Option C if justified.

### 9.4 Additional recommendation for key stubs

The current key-stub mechanism based on mutating a Python callable object should not survive into the `v2` design.

Key injection should become an explicit boundary binding mode:

1. child key input location is recorded in the boundary plan
2. the runtime binder writes the key value directly through erased schema-aware binding
3. no Python eval object mutation is involved

This is both more efficient and more readable.

## 10. Context Support

Context support is required nested-graph infrastructure, not an optional follow-on.

The `v2` design must support:

1. context-manager inputs on Python-backed nodes
2. context export/import across graph boundaries
3. `mesh` context access without mixed builder families

The recommendation is:

1. port context-manager handling into `v2` Python-backed node execution
2. replace the remaining legacy context support path with `v2` builder/runtime support
3. treat context bindings as part of `BoundaryBindingPlan`

This keeps `mesh`, `switch`-inside-context, and `map`-inside-context on the same substrate as the rest of nested execution.

## 11. Operator Mapping Onto The Substrate

Each operator should become a thin layer over the shared child-graph and boundary infrastructure.

### 11.1 nested_graph

Uses:

1. one `ChildGraphTemplate`
2. one `ChildGraphInstance`
3. direct input and output boundary bindings

### 11.2 try_except

Same as `nested_graph`, plus:

1. exception capture policy
2. output routing to `out` vs `exception`

### 11.3 component

Same as `nested_graph`, plus:

1. delayed activation until `recordable_id` is fully ready
2. global-state uniqueness checks
3. trait propagation

### 11.4 switch

Uses:

1. child template selection by key
2. at most one live child graph
3. branch reset output semantics

### 11.5 map

Uses:

1. one child template
2. many keyed child instances
3. keyed boundary bind / detach semantics
4. keyed scheduling state

### 11.6 reduce

Uses:

1. one child template
2. one internal child graph grown as a tree or chain
3. explicit boundary binds between internal nodes

This part should correct existing parity drift instead of copying it.

### 11.7 mesh

Uses:

1. one child template
2. many keyed child instances
3. explicit dependency tracking and re-ranking in operator state
4. context export/import through the shared context binding substrate

## 12. Memory And Performance Expectations

The design should outperform the legacy nested approach in the following ways.

### 12.1 Better steady-state memory shape

By compiling away boundary stubs:

1. fewer runtime nodes per child graph
2. fewer inputs/outputs allocated only for plumbing
3. less scheduler and bookkeeping state

This matters most for:

1. `map`
2. `mesh`
3. reduction trees

### 12.2 Better key handling

By replacing mutable Python key stubs with explicit boundary-key binding:

1. less Python object traffic
2. less runtime mutation of callable state
3. clearer ownership of key values

### 12.3 Better code locality

By storing immutable child templates in builder type state:

1. compile-once data is shared
2. runtime node payloads stay small
3. the operator runtime works mostly with local state plus child instances

## 13. Readability Rules

The implementation should follow these rules.

1. No runtime behavior depends on scanning child nodes by `"stub:"` name.
2. No key propagation depends on mutating Python eval objects.
3. No nested operator should implement bespoke bind/unbind logic if a shared boundary binder can express it.
4. Shared nested-runtime types should have names that reflect responsibility:
   - template
   - instance
   - boundary plan
   - boundary runtime
   - clock delegate
5. Comments should explain graph-boundary semantics where they are not obvious from code.

## 14. Implementation Phases

## Phase 1: RFC Finalization

1. agree on the child-graph substrate
2. agree on boundary binding modes
3. agree on the stub strategy

Exit criteria:

1. this RFC is accepted or revised into the implementation contract

## Phase 2: v2 Child Graph Substrate

1. extend `v2` graph/runtime to support child graph construction and lifecycle
2. add delegated nested evaluation clock behavior
3. add parent-node, graph-id lineage, label, and traits inheritance

Exit criteria:

1. a `v2` child graph can be created, started, evaluated, stopped, and inspected without legacy graph classes

## Phase 3: Boundary Binding Infrastructure

1. define `BoundaryBindingPlan`
2. compile current nested wiring into that plan
3. implement reusable runtime bind / rebind / detach helpers

Exit criteria:

1. `nested_graph` can run without runtime stub nodes

## Phase 4: Context Support

1. add `context_inputs` support to `v2` Python-backed nodes
2. port context output/import to `v2`
3. remove mixed-builder context dependencies from nested graph wiring

Exit criteria:

1. the standalone context suite passes under `HGRAPH_USE_CPP=1`

## Phase 5: Nested Operator Bring-up

Recommended order:

1. `nested_graph`
2. `try_except`
3. `component`
4. `switch_`
5. `map_`
6. associative `reduce`
7. non-associative `reduce`
8. `mesh_`

Exit criteria:

1. each operator family passes focused parity tests in both Python and C++ runtime modes

## Phase 6: Cleanup

1. remove legacy nested-graph dependency from the `v2` path
2. remove runtime reliance on stub-node scanning
3. reassess whether the wiring layer should move from Option B to Option C for boundaries

## 15. Acceptance Criteria

The design is considered complete when all of the following are true.

1. No nested-graph path in `HGRAPH_USE_CPP=1` depends on legacy `GraphBuilder` or legacy `Graph`.
2. No supported nested workflow requires mixed legacy / `v2` builders.
3. Nested operators use shared child-graph and boundary-binding infrastructure rather than bespoke ad hoc plumbing.
4. Runtime boundary stubs have been compiled away.
5. `context`, `nested_graph`, `map`, `reduce`, `switch`, `try_except`, `component`, and `mesh` pass focused parity tests under both runtime modes.
6. Full graph lineage uses the proper nested `node_id()` semantics rather than local index shortcuts.
7. The implementation remains explainable in terms of the shared substrate described in this RFC.

## 16. Open Questions

These should be resolved before implementation if possible, but they do not change the core architecture.

1. Should `ChildGraphInstance` own a dedicated boundary-runtime object, or should the operator runtime own boundary state directly?
2. Should context import/export be represented as dedicated binding modes or a small parallel plan object?
3. Should the long-term end state move from Option B to Option C for boundary representation once parity is reached?

## 17. Summary Decision

The recommended design is:

1. implement nested graphs as first-class `v2` child graphs
2. share behavior through reusable substrate objects, not a new nested-node hierarchy
3. keep operator semantics in operator-owned runtime state
4. compile nested stubs into explicit boundary binding plans
5. treat context support as required nested-graph infrastructure
6. keep Python as the semantic reference while building the `v2`-native execution model
