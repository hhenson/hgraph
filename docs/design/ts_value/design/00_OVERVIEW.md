# Design Overview

## Purpose

This document provides the high-level design overview for the new time-series infrastructure implementation.

## Design Goals

1. **Two-Layer Schema System**: TypeMeta for value schemas, TSMeta for time-series schemas
2. **Type-Erased Views**: Uniform access patterns through View, TSView, DeltaView
3. **Vtable-Based Polymorphism**: type_ops and ts_ops for runtime polymorphism
4. **Memory Stability**: Stable addresses for linked data structures
5. **Efficient Delta Tracking**: Support for both stored and computed deltas
6. **Explicit Runtime State**: No hidden fallback state machines in the core runtime
7. **Collapsed Runtime State Tree**: Keep user data in `Value/View`, but collapse time/observer/link/delta metadata into one schema-shaped runtime state tree
8. **Nested-Graph Compatibility**: Binding and rebind semantics must work cleanly for keyed and nested graphs
9. **Python Parity Without Python-Driven Core Complexity**: Python remains a hard compatibility target, but wrapper behavior must not dictate a convoluted core architecture

## Document Structure

| Document | Description |
|----------|-------------|
| 01_SCHEMA.md | TypeMeta, TSMeta, and vtable design |
| 02_VALUE.md | Value, View, and type-erased access |
| 03_TIME_SERIES.md | TSValue, TSView, and temporal tracking |
| 04_LINKS_AND_BINDING.md | Links, REF, binding mechanics |
| 05_TSOUTPUT_TSINPUT.md | TSOutput, TSInput, paths, ViewData |
| 06_ACCESS_PATTERNS.md | Reading, writing, iteration patterns |
| 07_DELTA.md | DeltaView, DeltaValue, change tracking |
| 08_IMPLEMENTATION_REVIEW.md | Review of the previous branch implementation and where it diverged from the design |
| 09_SIMPLIFIED_RUNTIME.md | Clean implementation direction informed by real runtime complexity but designed to avoid the previous branch's failure modes |

## Key Data Structures

```
┌─────────────────────────────────────────────────────────────┐
│                      Schema Layer                           │
├─────────────────────────────────────────────────────────────┤
│  TypeMeta (value schema)    TSMeta (time-series schema)     │
│  - type_ops vtable          - ts_ops vtable                 │
│  - size, alignment          - value_schema (TypeMeta)       │
│  - child schemas            - state_schema (TypeMeta)       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Value Layer                            │
├─────────────────────────────────────────────────────────────┤
│  Value (owns memory)        View (borrows memory)           │
│  - TypeMeta* meta           - void* data                    │
│  - void* data               - type_ops* ops                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Time-Series Layer                        │
├─────────────────────────────────────────────────────────────┤
│  TSValue                    TSView                          │
│  - data_value_              - data_view_                    │
│  - state_value_             - state_view_                   │
│  - schema-shaped state      - Kind-specific wrappers        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Graph Endpoint Layer                     │
├─────────────────────────────────────────────────────────────┤
│  TSOutput                   TSInput                         │
│  - native_value_ (TSValue)  - input_value_                  │
│  - explicit adapters        - active_state_                 │
│                            - non-peered elems + leaf LINKs  │
└─────────────────────────────────────────────────────────────┘
```

## Reality Constraints

The implementation review of the previous branch showed that the base design was directionally correct but under-specified in a few important areas:

1. **Target identity is a first-class runtime concern**
   - It is not enough to know that an input is "bound".
   - The runtime must be able to compare the current effective target with a previous effective target without layering ad hoc fallback logic everywhere.

2. **Dynamic containers and nested graphs change the problem**
   - `TSD` and `TSS` do not just need delta support.
   - They also need a clean strategy for keyed graph lifecycle, rebinding, and removal semantics.

3. **REF semantics cannot be treated as a small extension**
   - REF rebinding, sampled semantics, and composite references need an explicit state model.
   - They should not be spread across many unrelated helpers.

4. **Python compatibility is a hard requirement**
   - However, the C++ runtime should expose explicit primitives for Python rather than accumulating Python-specific fallback behavior inside the core graph/value layers.

5. **The parallel side-car model should be collapsed**
   - The previous "time tree + observer tree + delta tree + link tree" split made the model harder to reason about.
   - The preferred direction is to keep data separate but collapse the rest into one runtime state tree whose shape follows the time-series schema.

## Simplification Direction

The next implementation direction is:

1. **Prefer one native runtime representation plus explicit adapters**
   - Avoid eager alternative trees that recursively mirror the whole output state.

2. **Use one schema-shaped runtime state tree**
   - Keep value payloads in `Value/View`.
   - Represent time, observers, binding state, and delta state in one per-level runtime state structure.

3. **Keep runtime state explicit and local**
   - If a node needs fallback or retained state, that state should usually live in the node, not in generic time-series infrastructure.

4. **Promote hidden feature-extension behavior into the design**
   - If a behavior is required for correctness, it must appear in the design as a named concept with lifecycle and invariants.

5. **Treat nested-graph rebinding as a primary use case**
   - The design should be easy to reason about for `reduce`, `map`, and other keyed nested-graph operators, not only for simple one-hop bindings.

6. **Optimize for understandability first**
   - The previous branch demonstrated that "generic" fallback-heavy behavior quickly becomes unmaintainable.
   - The new design must favor fewer concepts, sharper boundaries, and explicit invariants.

### Memory Management

**User Perspective**: Value semantics throughout. Users work with values as simple data - no manual memory management, no pointers, just values.

**Internal Implementation**:
1. **Typed null by default**: `Value(schema)` preserves schema while payload is absent until `emplace()` / `from_python(...)`.
2. **SBO + heap fallback**: `ValueStorage` stores small payloads inline (`24` bytes, align `8`), otherwise heap-allocates with schema alignment.
3. **Vtable lifecycle**: construction/copy/move/destroy are delegated to `type_ops`.
4. **Nested nullability**: composite/container child null state is tracked with validity bitmaps (bundle/tuple/list/map-values).
5. **RAII cleanup**: payload storage and container-owned buffers are cleaned up through normal C++ lifetime rules.

**Decision (2026-02-13)**: optimize internal compactness and algorithm efficiency first; maintain Arrow-compatible validity semantics, but do not require Arrow-native in-memory layout for every structure.

**Stable Addresses**: For linked structures (TSD elements, TSL elements), addresses must remain stable after creation. See 04_LINKS_AND_BINDING.md for details.

### Thread Safety

**Single-threaded execution**: All graph evaluation runs on a single thread. No thread safety concerns for the value/time-series infrastructure - no locks, atomics, or synchronization needed.

### Performance

**Design Philosophy**: Optimize for performance with minimal overhead while preserving required abstractions.

**Allocation Strategy**:
- Primitives: inline storage (zero allocation)
- Dynamic structures: allocate on demand
- Leverage standard components: `std::vector`, ankerl::unordered_dense

**Container Choices**:
- **Maps/Sets**: `ankerl::unordered_dense` (robin-hood backward shift deletion)
  - Dense, cache-efficient storage with vector-backed elements
  - High-quality hashing with better distribution than std::unordered_map
  - Supports transparent (heterogeneous) lookup
- **Sequences**: `std::vector<std::byte>` for type-erased element storage
- **Stable storage**: Index-based indirection when stable addresses required

**Guidelines**:
- Avoid unnecessary indirection
- Prefer contiguous memory where possible
- Use transparent lookup functors to avoid temporary object creation

## Open Questions

1. How much of schema adaptation should be handled by persistent endpoint-owned adapters versus transient view-level projections?
2. Should input-side and output-side link payloads remain separate runtime types, or can they share a common structural base without reintroducing complexity?
3. Which Python-observable behaviors belong in the core runtime contract, and which should be implemented as wrapper-layer composition?

## References

- User Guide: `docs/design/ts_value/user_guide/`
- Research: `docs/design/ts_value/research/`
