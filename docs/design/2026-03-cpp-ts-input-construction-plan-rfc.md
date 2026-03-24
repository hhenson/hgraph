# C++ TSInput Construction Plan RFC

## Problem

The new `TSInput` design should not build its nested storage incrementally at bind time.

This design assumes a clean break in the C++ runtime.

- no legacy compatibility path is required
- no transitional builder layering is required
- the new builder contract should directly model the desired runtime construction behavior

For node inputs we already know two important facts:

- the runtime root is always a non-peered `TSB` of the node's named inputs
- the final wiring paths are known once graph wiring is complete

What is missing today is a way to carry that final wiring shape from graph building into `TSInput` construction.

The current builder flow still assumes:

- `InputBuilder` only knows the declared schema
- `GraphBuilder` creates nodes first
- edges then walk runtime input/output paths and call `bind_output(...)`

That forces `TSInput` to create missing child structure dynamically when binding happens.

The redesign goal is to remove that model entirely.

## What The Current Wiring Flow Already Knows

### Wiring paths already encode static non-peered descent

`WiringPort` paths are the compile-time selection path used during graph wiring.

For `TSB` and fixed `TSL`, the non-peered case is resolved during wiring, not at runtime:

- `TSBWiringPort._wiring_port_for(...)` returns `self.node_instance.inputs[arg]` when `has_peer == false`
- `TSLWiringPort.__getitem__(...)` returns the child input port directly when `has_peer == false`
- `TSBWiringPort.edges_for(...)` and `TSLWiringPort.edges_for(...)` recursively expand non-peered structure into destination paths

That means the final edge set already contains the exact destination leaf paths that must exist in the runtime input tree.

### Graph building already has the full edge set

`hgraph/_wiring/_graph_builder.py:create_graph_builder(...)` is the first place where both of these are true:

- all `NodeBuilder` instances exist
- all final `Edge(src_node, output_path, dst_node, input_path)` values exist

This is the right point to compile a construction plan.

### Runtime builders do not receive this information

Today:

- `NodeBuilder` only carries `input_builder`
- `InputBuilder.make_instance(...)` only receives the owning node or parent input
- `PythonGraphBuilder.make_and_connect_nodes(...)` and the C++ `GraphBuilder::make_and_connect_nodes(...)` navigate runtime paths and bind after construction

So the runtime input tree shape is not available when the input is built.

## Desired Shape

For the new TS runtime, `TSInput` should be constructed from a precompiled plan:

- the root `TSB` is always allocated
- only the required non-peered `TSB` and fixed `TSL` prefixes are allocated below it
- terminal positions are precreated as link-backed slots
- binding later only resolves those precreated link terminals to concrete output views

This separates:

- construction shape
- runtime binding target

## Decision

The chosen direction is:

- `TSInputBuilder` becomes a construction-plan-driven builder
- `TSInputBuilder(construction_plan)` is the target API shape
- the plan is compiled during graph building
- runtime input construction consumes that plan directly
- runtime binding only binds prebuilt link terminals

There is no fallback schema-only input-builder path for the new TS runtime.

## Proposed Data Model

Introduce a node-local input construction plan.

Suggested shape:

```text
InputConstructionPlan
  root: BundlePlan

BundlePlan
  children: vector<SlotPlan>

ListPlan
  size: int
  children: vector<SlotPlan>

SlotPlan
  kind:
    Empty
    NativeBundle
    NativeList
    LinkTerminal

LinkTerminal
  schema: represented TS schema
  binding_ref: BindingRef

BindingRef
  src_node: int
  output_path: tuple[int, ...]
```

Important detail:

- the plan describes the storage shape to build
- the binding reference describes what that terminal should later bind to

This should be sparse:

- unused branches do not appear
- shared prefixes are stored once

## How To Compile The Plan

Compile the plan in `create_graph_builder(...)` after all `node_builders` and `edges` have been collected, but before `GraphBuilderFactory.make(...)` is called.

Algorithm:

1. Group edges by `dst_node`.
2. For each destination node, group edges by top-level input arg using `edge.input_path[0]`.
3. For each edge, walk the destination input schema using `edge.input_path`.
4. For every prefix entered through `TSB` or fixed `TSL`, mark that prefix as a native collection node in the plan.
5. Mark the terminal slot as a `LinkTerminal` with the edge's `(src_node, output_path)`.
6. Merge all edges for the same input tree into one sparse trie.
7. Reject conflicts:
   - same slot used as both terminal and collection
   - schema mismatch at a shared prefix
   - unsupported dynamic container descent in the static plan

For the current scope, this only needs to support static `TSB` and fixed `TSL` descent.

## Builder Contract

`TSInputBuilder` should be redefined around the construction plan.

Target shape:

```text
TSInputBuilder(construction_plan)
```

The root schema can still be derivable from the plan root, so the plan is the authoritative runtime construction contract.

This means:

- the builder identity is the runtime input shape
- the builder is no longer just "declared schema plus runtime binding later"
- node builders carry a fully planned input builder

## Where The Plan Should Live

The cleanest ownership is:

- graph builder compiles the plan
- node builder carries the plan for its input root
- input builder consumes the plan to construct the runtime `TSInput`

This should be implemented directly, not as a wrapper around the existing builder hierarchy.

So the flow should be:

- compile `InputConstructionPlan`
- create `TSInputBuilder(construction_plan)`
- store that builder on the node builder
- construct `TSInput` from the plan

## Runtime Construction Flow

With a compiled plan, input construction becomes:

1. Build the root node input `TSB`.
2. Walk the construction plan.
3. For every native `TSB`/`TSL` node in the plan, allocate the corresponding value/state storage immediately.
4. For every `LinkTerminal`, allocate the link state immediately, but leave it unbound.
5. Store a stable lookup from `input_path` to the constructed terminal link node.

This removes dynamic child creation from the binding step.

The intended result is that `TSInputView::bind_output(...)` only resolves a prebuilt terminal and binds it. It should not allocate or replace structure.

## Runtime Binding Flow

After all nodes are constructed, `GraphBuilder` still performs the final bind pass because only then do concrete source outputs exist.

But instead of:

- walking runtime input structure
- creating/replacing slots dynamically

it should:

- resolve the prebuilt terminal by `edge.input_path`
- resolve the output view by `edge.output_path`
- bind the existing terminal link to that output view

So the edge pass becomes:

- `lookup prebuilt terminal`
- `bind existing link`

not:

- `discover or build structure`
- `replace slot`

## Why This Is Better

- `TSInput` storage shape becomes deterministic at construction time
- memory allocation can be sized up front
- binding becomes a pure resolution step
- non-peered collection structure is built exactly once
- runtime behavior matches the wiring graph more directly

## Scope Boundaries

For the first implementation, keep the plan compiler limited to:

- root node input `TSB`
- static `TSB` descent
- fixed `TSL` descent
- terminal target links

Do not include yet:

- dynamic `TSD` descent
- active-path planning
- ref rebinding policies
- output alternative planning

Those can layer on later.

## Concrete API Direction

The clean target shape is:

```text
GraphBuilder
  -> compiles InputConstructionPlan per destination node
  -> builds NodeBuilder(input_builder=TSInputBuilder(plan))

TSInputBuilder(plan)
  -> allocates root non-peered TSB
  -> allocates required native TSB/TSL prefixes
  -> allocates unbound link terminals
  -> records stable input-path-to-terminal lookup

Graph bind pass
  -> resolves TSOutputView by output_path
  -> resolves prebuilt terminal by input_path
  -> binds terminal
```

## Recommended Next Steps

1. Add `InputConstructionPlan` types in Python and C++.
2. Change `create_graph_builder(...)` to compile per-destination-node plans from the final edge set.
3. Redefine the new TS-side `TSInputBuilder` contract around `construction_plan`.
4. Update node builders to carry the plan-driven input builder directly.
5. Change TS input construction to build native prefixes and unbound link terminals from the plan.
6. Reduce runtime binding to prebuilt-terminal resolution plus `TSOutputView` binding.

## Practical Conclusion

The information needed by `TSInput` already exists.

It does not need to be rediscovered during runtime binding.

The missing step is to extract the destination-side path trie from the final wiring edges, compile it into `InputConstructionPlan`, and feed that directly into `TSInputBuilder(construction_plan)` before node instances are created.
