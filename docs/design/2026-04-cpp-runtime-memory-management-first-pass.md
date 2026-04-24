# RFC: C++ Runtime Memory Management First Pass

Date: 2026-04-21  
Status: First pass for discussion  
Scope: Runtime object lifetime for graph/node/time-series instances in the legacy C++ runtime.

## 1. Context

The current C++ runtime already has a partial arena story:

1. `GraphBuilder::make_instance(..., use_arena=true)` allocates one raw buffer for a graph build and seeds an `ArenaAllocationContext`.
2. Runtime objects are then created with `arena_make_shared()` / `arena_make_shared_as()` so they can still participate in `shared_from_this()`.
3. Builders expose `memory_size()` and `type_alignment()` so the graph buffer can be sized ahead of construction.

That gives us contiguous allocation for the object shells, but it is not yet a complete memory-management solution.

## 2. Current State Review

### 2.1 What already exists

1. Arena allocation helpers live in `cpp/include/hgraph/util/arena_enable_shared_from_this.h`.
2. Graph-level pre-sizing lives in `cpp/src/cpp/builders/graph_builder.cpp`.
3. Builder sizing contracts live in `cpp/include/hgraph/builders/builder.h` and `cpp/src/cpp/builders/node_builder.cpp`.
4. Nested input/output builders recursively include child object sizes, for example `TimeSeriesBundleOutputBuilder::memory_size()`.
5. Semantic teardown exists today through `release_instance()` and `dispose_component()`.

### 2.2 The main gap

The arena path currently uses aliasing `shared_ptr` ownership over a raw buffer:

1. Placement-new constructs an object inside the arena buffer.
2. A `shared_ptr<T>` is created with the arena buffer as the owning control block.
3. When the last `shared_ptr` dies, the buffer is freed.

That does not automatically run the destructor for each arena-allocated object.

This matters because arena-allocated objects are not trivial:

1. `Node` owns `nb::dict` and other RAII members.
2. `Graph` owns vectors and traits state.
3. Many runtime objects own STL containers, `shared_ptr`, or Python handles.
4. Python-backed node types own `nb::callable` and other refcounted objects.

`release_instance()` currently performs semantic cleanup, but it is not a replacement for destructor execution.

## 3. Goals

1. Guarantee correct destruction of every arena-allocated runtime object exactly once.
2. Preserve existing `shared_ptr`-based APIs and `arena_enable_shared_from_this`.
3. Keep heap and arena construction semantically identical.
4. Keep the first pass scoped to runtime object shells, not full container/value-payload pooling.
5. Make teardown safe for objects that own Python references.

## 4. Non-Goals

This first pass should not attempt to:

1. Move all STL container storage into arenas.
2. Replace the `Value` storage model with PMR/custom allocators.
3. Eliminate every heap allocation inside runtime objects.
4. Rework the v2 runtime at the same time.
5. Solve operator-specific retention structures beyond defining their lifetime boundary.

## 5. Required Invariants

Any implementation should preserve these invariants:

1. Arena-allocated objects must have their destructors called in reverse construction order.
2. Destructor registration must happen only after successful construction.
3. Teardown must be idempotent at the arena-owner level.
4. `release_instance()` remains the place for semantic teardown and unbinding, not raw-memory reclamation.
5. Heap and arena paths must produce the same externally visible lifecycle behavior.
6. If an arena object owns `nb::object`-like state, destructor execution must happen while the GIL is held or under an explicitly defined GIL policy.
7. Nested arena allocations during construction must remain safe and non-overlapping.
8. Arena overflow must fail deterministically with actionable diagnostics.

## 6. First-Pass Design

### 6.1 Introduce an arena owner, not just a raw buffer

Replace the current "buffer plus aliasing `shared_ptr<void>`" model with an arena owner object, for example:

1. raw storage pointer
2. capacity
3. current offset
4. a destructor stack
5. debug/canary metadata
6. a destroyed flag

This owner becomes the true lifetime root for a graph instance.

### 6.2 Register destructors at allocation time

`arena_make_shared()` / `arena_make_shared_as()` should:

1. reserve aligned storage
2. placement-new the object
3. register `{void* ptr, destroy_fn}` with the arena owner
4. then return the aliasing `shared_ptr`

The destroy callback can be a small type-erased function pointer:

```cpp
using destroy_fn_t = void(*)(void*);
```

with registration like:

```cpp
arena->register_destructor(obj_ptr, [](void* p) {
    static_cast<T*>(p)->~T();
});
```

### 6.3 Arena destruction walks the destructor stack

When the owning arena object is destroyed:

1. acquire the required Python/GIL context if needed
2. walk destructors in reverse order
3. destroy each constructed object once
4. free the backing buffer

This is the core correctness fix.

### 6.4 Keep `release_instance()` for semantic teardown

The current builder-side `release_instance()` and `dispose_component()` flow is still useful and should remain responsible for:

1. unbinding inputs/outputs
2. releasing nested graphs
3. disposing operator-specific retained state
4. stopping lifecycle participants

But it should no longer be the thing we implicitly rely on for C++ destructor correctness.

### 6.5 Unify the allocation entry point

There are currently two arena construction paths:

1. `_arena_construct_shared()` in `arena_enable_shared_from_this.h`
2. `make_instance_impl()` in `builder.h`

For the first pass, these should share one implementation path so:

1. canary logic is not duplicated
2. destructor registration is not duplicated
3. overflow checks are consistent
4. `shared_from_this()` initialization is handled identically everywhere

The simplest version is to make `make_instance_impl()` delegate to the arena helper.

### 6.6 Keep arena scope limited

First pass arena coverage should be:

1. `Graph`
2. `Node` subclasses
3. `TimeSeriesInput` / `TimeSeriesOutput` shells and children built during graph construction

Explicitly out of scope for this pass:

1. `Value` payload buffers
2. container internals like `std::vector` backing storage
3. map/set/list storage owned by the value system
4. long-lived dynamic collections created after graph construction unless they already participate in the builder arena

### 6.7 Nested graphs should own their own arena roots

Dynamic or nested graphs should not share a single monolithic parent arena in the first pass.

Preferred first-pass rule:

1. each built graph instance owns one arena root
2. nested graphs built later get their own arena root
3. releasing a nested graph tears down only that graph's arena

That keeps ownership boundaries clear and avoids partial-destruction complexity inside a shared arena.

## 7. What Needs To Be Implemented

### 7.1 Core runtime changes

1. Add an `ArenaOwner` / `ArenaStorage` type with destructor registration.
2. Extend `ArenaAllocationContext` to point at that owner, not just raw buffer state.
3. Update `arena_make_shared()` / `arena_make_shared_as()` to register destructors.
4. Remove duplicated raw-buffer construction logic from `make_instance_impl()`.
5. Update `GraphBuilder::make_instance()` to allocate and retain the new arena owner.

### 7.2 GIL policy

We need an explicit rule for arena destruction when destructors may touch Python refs.

First-pass requirement:

1. graph teardown must happen under the GIL, or
2. arena owner destruction must explicitly acquire the GIL before invoking registered destructors

This needs to be decided and then documented in code.

### 7.3 Debugging and diagnostics

1. Keep canary support.
2. Add assertion/logging when destructor stack underflows or double-destroys.
3. Record peak offset and final object count for debugging.
4. Make overflow messages identify the type being allocated when possible.

### 7.4 Test coverage

We need dedicated tests for:

1. heap vs arena lifecycle parity
2. destructor execution order
3. exactly-once destruction
4. nested arena allocations during construction
5. graph release for nested/dynamic graphs
6. Python-ref ownership teardown under GIL
7. canary failure/overflow diagnostics in debug mode

## 8. Work That Can Wait Until Phase 2

After correctness is in place, later work can address:

1. reducing or eliminating manual `memory_size()` bookkeeping
2. arena-backed PMR allocators for selected STL containers
3. pooling of dynamic keyed runtime state
4. value-layer allocator customization
5. performance instrumentation comparing heap vs arena builds

## 9. Open Questions

1. Should arena teardown always acquire the GIL, or should callers be required to hold it?
2. Do we want arena overflow retry logic long-term, or should a bad `memory_size()` estimate be treated as a bug?
3. Should `extend_graph()` use the same graph arena, or always allocate a fresh subgraph arena?
4. Which dynamic runtime objects are intentionally allowed to remain heap-only after graph start?

## 10. Recommendation

The first pass should focus on correctness, not allocator breadth:

1. introduce an explicit arena owner
2. register destructors for every arena-allocated object
3. run destructor teardown in reverse order with a defined GIL policy
4. keep existing builder sizing for now
5. defer PMR/value-storage work until after this path is correct and testable

That is the minimum viable memory-management solution for the current runtime.
