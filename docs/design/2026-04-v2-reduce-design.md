# Design: v2 Reduce Runtime

Date: 2026-04-18  
Status: Proposed implementation design  
Scope: `reduce` on the C++ runtime, with emphasis on dynamic `TSD` reduction and reuse of the new nested graph substrate.

## 1. Context

`reduce` is the next nested operator that needs a native C++ implementation.

At the moment:

1. `map_`, `switch_`, `try_except`, and the nested substrate now exist on the v2 runtime.
2. `reduce` is still explicitly unsupported in `_use_cpp_runtime.py`.
3. The Python implementation is the semantic reference, but it is not the shape we should copy mechanically.

This is the first nested operator where the runtime data structure matters as much as the child-graph substrate. A poor design here will either:

1. waste memory by materializing too many nested graphs
2. lose memory stability when the source `TSD` changes shape
3. bake in Python quirks we already know we do not want long-term

The design below is intended to be the implementation target before starting the C++ work.

## 2. Requirements

## 2.1 Semantic requirements

The native implementation must satisfy the existing reduce tests and preserve Python-visible behavior unless there is an explicit reason to improve the semantics.

This includes:

1. associative `TSD[K, TS[T]] -> TS[T]`
2. associative `TSL[TS[T], N] -> TS[T]`
3. nested results, for example `TSD -> TSD` reductions
4. reductions where the reduced item type is itself produced by `map_` or `switch_`
5. reductions over bundle outputs
6. `zero=None` / invalid-zero cases
7. non-associative ordered reduction over `TSD[int, ...]`
8. tuple reduction, which lowers to the ordered `TSD[int, ...]` path

## 2.2 Runtime requirements

The C++ design should:

1. use memory-stable storage for dynamically created reduction state
2. avoid routing output or reset semantics through Python
3. integrate with the new `ChildGraphTemplate` / `ChildGraphInstance` substrate
4. avoid rebuilding key bookkeeping that already exists in the value layer
5. avoid the Python implementation's unnecessary zero-padding behavior for associative reduction

## 2.3 Non-goals

This design does not try to:

1. preserve the old Python nested runtime shape
2. force associative and non-associative reduction through the same runtime kernel
3. make `reduce` depend on legacy nested builders or stub execution

## 3. Problems In The Python Shape

The Python `PythonReduceNodeImpl` is a useful semantic reference, but it is not the right runtime shape for v2.

Its main weaknesses are:

1. it materializes reduction nodes at the leaves as well as the internal tree
2. it pads odd branches with `zero`, which causes known spurious zero ticks
3. it keeps its own key-to-node bookkeeping at the node layer
4. it grows and shrinks by graph chunk count rather than by a reusable stable-slot store
5. it does not cleanly separate:
   - source slot lifecycle
   - leaf placement
   - internal combine-node storage

The stream-layer FIXME documents the key semantic smell:

1. associative reduce currently uses `zero` as padding for odd branches
2. that can emit values that are not really reductions of the live inputs

That behavior should not be carried into the v2 implementation.

## 4. Core Design Decisions

## 4.1 Source `TSD` slot lifecycle is authoritative

For dynamic `TSD` reduction, the authoritative slot lifecycle already exists in the value layer:

1. `MapViewDispatch::add_slot_observer(...)`
2. `on_insert`
3. `on_remove`
4. `on_erase`
5. `on_capacity`

The reduce runtime should mirror that lifecycle rather than recreate key tracking from scratch.

This means:

1. source slot ids come from the bound source `TSD`
2. reduce keeps a source-slot mirror
3. leaf placement is a secondary mapping on top of source slots

## 4.2 Leaves are not child graphs

This is the most important design choice.

For associative reduction:

1. leaves are direct aliases to source item outputs
2. only internal combine points are child graphs

This is better than the Python design because it avoids creating a reduction child graph just to combine:

1. `item + zero`
2. `zero + item`
3. `item + item` at the bottom layer when only one input is live

Instead:

1. a leaf position either references one live source output or is empty
2. an internal node either:
   - aliases a single live child aggregate
   - combines two live child aggregates via one child graph
   - is empty

This gives the minimal number of active combine graphs:

1. `0` live inputs -> `0` combine graphs
2. `1` live input -> `0` combine graphs
3. `n` live inputs -> at most `n - 1` live combine graphs

## 4.3 `zero` is for the empty result, not odd-branch padding

For associative reduction:

1. `zero` should define the result when the whole collection is empty
2. `zero` should not be injected just because one subtree has an odd count

Root result semantics should be:

1. no live inputs:
   - alias `zero` if `zero` is valid
   - otherwise invalidate the output
2. one live input:
   - alias that input directly
3. two or more live inputs:
   - reduce by internal combine nodes

This removes the spurious zero tick behavior without changing the expected mathematical result for associative reductions.

## 4.4 Dense leaf positions are separate from source slots

Source `TSD` slots are authoritative for lifecycle, but they are not appropriate as tree positions because:

1. they may be sparse
2. they may remain occupied during remove-before-erase
3. a balanced tree wants dense positions `0..live_count-1`

The runtime should therefore maintain:

1. `source_slot -> dense_leaf`
2. `dense_leaf -> source_slot`

When a source slot is erased, the runtime compacts the dense leaf set by moving the last live leaf into the vacated position.

This keeps:

1. the tree dense
2. rebalancing bounded
3. updates to `O(log n)` ancestors

## 4.5 Internal combine nodes use stable slot storage

Internal combine nodes are dynamic runtime objects and must not move when capacity grows.

They should use a stable-slot payload store backed by chained storage blocks, the same core approach already extracted for keyed storage.

The desired utility shape is:

1. `StablePayloadStore<T>`
2. `KeyedPayloadStore<T>` remains a keyed convenience wrapper over the same storage idea

`reduce` then uses:

1. `StablePayloadStore<ReduceOpRuntime>` for internal combine nodes
2. a second stable storage region for each child graph's slab memory

This gives:

1. stable addresses for per-node runtime payloads
2. stable addresses for each nested child graph memory slab
3. append-only capacity growth

## 4.6 Child graphs live in reserved per-node slabs

Each internal combine node uses one `ChildGraphInstance`.

Its storage should come from a pre-reserved stable slab, not per-node heap allocation. The runtime already supports this via `GraphStorageReservation`.

For each internal node slot:

1. reserve `child_template.graph_builder.memory_size()` bytes
2. aligned to `child_template.graph_builder.alignment()`
3. pass that block to `ChildGraphInstance::initialise(...)`

This keeps:

1. graph memory local and stable
2. growth append-only
3. destruction cheap and explicit

## 5. Associative TSD Runtime Shape

## 5.1 Builder state

Add a new builder state for associative reduce:

```text
ReduceNodeBuilderState
  child_template: const ChildGraphTemplate *
  mode: AssociativeTsd
  source_arg: "ts"
  zero_arg: "zero"
```

The child template is the compiled binary reduction graph with:

1. two input bindings, one for each reducer argument
2. one output binding

The builder/export layer should compile it exactly as with other nested operators, but without any Python runtime dependency.

## 5.2 Runtime data

Suggested runtime payload:

```text
ReduceNodeRuntimeData
  input: TSInput *
  output: TSOutput *
  error_output: TSOutput *
  recordable_state: TSOutput *
  child_template: const ChildGraphTemplate *
  next_child_graph_id: int64_t

  source_binding:
    linked_source: LinkedTSContext
    map_dispatch: const MapViewDispatch *
    map_value_data: void *
    observer_registered: bool

  source_slots: KeyedPayloadStore<ReduceSourceSlotRuntime>
  dense_to_source: std::vector<size_t>
  live_leaf_count: size_t
  leaf_capacity: size_t

  internal_nodes: StablePayloadStore<ReduceOpRuntime>
  child_graph_storage: StableSlotStorage

  dirty_internal_nodes: dynamic_bitset
  pending_schedule: engine_time_t
  bound_root_source: AggregateRef
```

Where:

```text
ReduceSourceSlotRuntime
  key: Value
  dense_leaf: size_t or npos
  live: bool
  removed: bool

ReduceOpRuntime
  child_instance: ChildGraphInstance
  bound: bool
  last_source_left: AggregateRef
  last_source_right: AggregateRef
  next_scheduled: engine_time_t

AggregateRef
  kind: Empty | Zero | Leaf | Node
  index: size_t
```

Notes:

1. source slots are keyed by authoritative source slot id
2. internal nodes are keyed by tree position
3. `AggregateRef` identifies where an aggregate currently comes from without materializing it

## 5.3 Tree layout

Use a standard complete binary tree over `leaf_capacity`.

Definitions:

1. `leaf_capacity` is always a power of two, with minimum `1`
2. dense leaves occupy positions `0..live_leaf_count-1`
3. tree leaves are at logical indices `leaf_base + dense_leaf`
4. internal tree nodes occupy `0..leaf_capacity-2`

Only internal positions can own child graphs.

Leaf positions never own child graphs; they reference source outputs.

## 5.4 Aggregate resolution

For any tree position, define the aggregate source recursively:

1. empty leaf -> `Empty`
2. live leaf -> `Leaf(dense_leaf)`
3. internal node:
   - left empty, right empty -> `Empty`
   - exactly one non-empty child -> alias that child aggregate
   - both non-empty -> `Node(node_index)`

This means an internal combine graph exists only when both sides are non-empty.

## 5.5 Binding rules for internal nodes

For a dirty internal node:

1. resolve left child aggregate
2. resolve right child aggregate
3. if either side changed, rebind child inputs
4. if both sides are non-empty:
   - ensure the child graph exists
   - bind lhs input to the resolved left source
   - bind rhs input to the resolved right source
   - evaluate child graph
5. if only one side is non-empty:
   - stop and unbind any previously active child graph for this node
   - aggregate becomes a pure alias
6. if both sides are empty:
   - stop and unbind any previously active child graph for this node
   - aggregate becomes empty

The source for a binding may be:

1. the parent node's `zero` output
2. a live source `TSD` child output
3. another internal combine node's output

This requires a small helper to resolve `AggregateRef -> TSOutputView`.

This should use the normal `TSInputView::bind_output(...)` path, not boundary binding, because these are runtime links within the reduce node's own internal structure.

## 5.6 Root output publication

The reduce node's public output should be an output link to the root aggregate source.

Publication rules:

1. root aggregate = `Empty`
   - if `zero` is valid, bind the public output to `zero`
   - otherwise clear link and invalidate the output
2. root aggregate = `Leaf`
   - bind public output directly to that source item output
3. root aggregate = `Node`
   - bind public output directly to that internal node's child output

This reuses `OutputLink` and keeps output semantics the same as other nested operators.

## 6. Source Slot Lifecycle

## 6.1 Observer attachment

When the `ts` input is bound to a live `TSD` output:

1. capture the bound `LinkedTSContext`
2. require `schema->kind == TSD`
3. resolve the `MapViewDispatch`
4. register the reduce runtime as a slot observer on the source map storage

If the `ts` input rebinds:

1. detach from the old source
2. clear mirrored source state
3. attach to the new source
4. rebuild dense leaf placement from the new source's live slots

## 6.2 Insert

On `on_insert(source_slot)`:

1. ensure `source_slots` capacity
2. materialize the source slot runtime if needed
3. allocate `dense_leaf = live_leaf_count++`
4. append `dense_to_source[dense_leaf] = source_slot`
5. update `source_slot -> dense_leaf`
6. grow `leaf_capacity` if required
7. mark the leaf's ancestor path dirty

## 6.3 Remove

On `on_remove(source_slot)`:

1. mark the slot not live for aggregation purposes immediately
2. keep the source slot runtime alive until erase
3. mark the old dense leaf path dirty

This matches the two-phase remove/erase lifecycle:

1. contribution disappears on remove
2. structural storage is released on erase

## 6.4 Erase

On `on_erase(source_slot)`:

1. find its dense leaf
2. if it is not already the last live dense leaf:
   - move the last live dense leaf into its position
   - update both mappings
   - mark both affected ancestor paths dirty
3. decrement `live_leaf_count`
4. clear the dead source slot runtime
5. shrink `leaf_capacity` if the dense leaf count is now far below capacity

The shrink rule should be conservative, for example:

1. only shrink when `leaf_capacity >= 8`
2. and `live_leaf_count <= leaf_capacity / 4`

## 7. Evaluation Protocol

Associative TSD reduce should evaluate in four stages.

## 7.1 Process structural changes

Apply any pending insert/remove/erase operations and update dense leaf placement.

## 7.2 Recompute dirty paths bottom-up

Dirty work should be bounded to the paths affected by:

1. source slot lifecycle changes
2. modified source leaf values
3. modified internal child outputs

For each dirty internal node, process in descending depth order so children are current before parents.

## 7.3 Evaluate dirty internal child graphs

For internal nodes that actually require a combine graph:

1. rebind inputs if their aggregate sources changed
2. evaluate the child graph
3. capture `next_scheduled_time`

## 7.4 Publish root output

After the internal recompute pass:

1. resolve the root aggregate
2. rebind the public output link if needed
3. mark output modified if the bound target changed or the bound root source modified
4. schedule the reduce node if any internal node reported a future `next_scheduled_time`

## 8. Memory Efficiency Compared With Python

The proposed design is intentionally more efficient than the Python reference:

1. leaves are aliases, not child graphs
2. odd branches do not allocate or tick zero-padding nodes
3. internal combine nodes only exist when both sides are non-empty
4. internal node payloads live in stable-slot slabs
5. child graph slabs live in stable-slot slabs

For `n` live inputs:

1. Python-style full tree shape tends toward capacity-based over-allocation
2. this design tends toward exactly the active internal structure needed for the live leaves

This is the correct direction for v2.

## 9. Non-Associative Reduce

Non-associative reduce is a different kernel and should not be forced into the associative tree.

Required semantics:

1. preserve order
2. `zero` is a real lhs seed, not just an empty-result default
3. currently only `TSD[int, ...]` is supported dynamically

The recommended design is a stable linear chain:

```text
zero -> node[0](zero, item[0]) -> node[1](prev, item[1]) -> ... -> output
```

Suggested runtime shape:

1. one stable payload store of chain nodes
2. slot index equals integer position
3. extending the chain appends nodes
4. shrinking the chain truncates tail nodes

This mirrors the Python semantics but still uses:

1. `ChildGraphTemplate`
2. `ChildGraphInstance`
3. stable per-node graph slabs
4. direct output-to-input bindings

Because ordered remove semantics are constrained to tail removal, this remains straightforward.

## 10. TSL Reduce

`TSL` reduce is statically shaped.

Recommendation:

1. do not force the first C++ delivery to unify `TSL` with dynamic `TSD`
2. build a fixed reduction tree at construction time
3. reuse the same internal combine-node storage and aggregate-resolution logic where possible

This can be implemented after associative `TSD` reduce with much lower risk.

## 11. Export / Builder Integration

Add two new exported builders in `_hgraph_nodes.cpp`:

1. `build_reduce_node`
2. `build_non_associative_reduce_node`

Their job is:

1. compile the nested binary reducer graph into a `ChildGraphTemplate`
2. build the boundary plan for lhs/rhs inputs and output
3. configure the correct `NodeBuilder` type state

This should mirror the style already used by:

1. `build_map_node`
2. `build_switch_node`

Then `_use_cpp_runtime.py` can replace the current unsupported stubs with those builders.

## 12. Recommended Delivery Order

Implementation should be phased in this order:

1. associative `TSD` reduce
   - this unblocks current switch tests and most dynamic use
2. `TSL` associative reduce
3. non-associative `TSD[int, ...]` reduce
4. tuple reduction through the non-associative path

This gives the highest-value path first while keeping the runtime design clean.

## 13. Concrete Implementation Tasks

## Phase 1: storage helpers

1. extract a generic `StablePayloadStore<T>` from the keyed storage helper
2. keep `KeyedPayloadStore<T>` as the keyed wrapper
3. add a stable slab helper for child graph storage if needed

## Phase 2: associative TSD reduce runtime

1. add `ReduceNodeBuilderState`
2. add `ReduceNodeRuntimeData`
3. add source slot observer attachment/detachment
4. add dense leaf placement and compaction
5. add internal node stable storage
6. add aggregate resolution and root output publication
7. expose the builder in `_hgraph_nodes.cpp`

## Phase 3: tests

1. enable `test_reduce.py` associative `TSD` cases
2. enable switch tests that depend on reduce
3. add a regression that proves odd-branch associative reduce does not tick padding zero spuriously

## Phase 4: ordered reduce

1. add non-associative builder/runtime
2. enable tuple and ordered-`TSD[int]` tests

## 14. Open Questions

These need to be settled while implementing, but they do not block the design direction.

1. Should the reduce runtime use `SlotObserver` callbacks directly as the sole source of structural truth, or also rescan source delta on rebind for safety?
2. Do we want a reusable `AggregateRef` helper that can later be shared by mesh-style dependency trees?
3. Should `TSL` static reduce reuse the same aggregate-resolution kernel immediately, or land as a simpler dedicated path first?

## 15. Recommendation

Proceed with associative `TSD` reduce using:

1. source-slot observation from the input `TSD`
2. dense leaf compaction
3. internal combine graphs only
4. stable-slot payload storage
5. root output publication via `OutputLink`

This is the right v2 shape:

1. less memory than Python
2. stable under dynamic growth/shrink
3. aligned with the new nested-graph substrate
4. free of the zero-padding wart
