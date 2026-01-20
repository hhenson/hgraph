# Design Overview

## Purpose

This document provides the high-level design overview for the new time-series infrastructure implementation.

## Design Goals

1. **Two-Layer Schema System**: TypeMeta for value schemas, TSMeta for time-series schemas
2. **Type-Erased Views**: Uniform access patterns through View, TSView, DeltaView
3. **Vtable-Based Polymorphism**: type_ops and ts_ops for runtime polymorphism
4. **Memory Stability**: Stable addresses for linked data structures
5. **Efficient Delta Tracking**: Support for both stored and computed deltas

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

## Key Data Structures

```
┌─────────────────────────────────────────────────────────────┐
│                      Schema Layer                           │
├─────────────────────────────────────────────────────────────┤
│  TypeMeta (value schema)    TSMeta (time-series schema)     │
│  - type_ops vtable          - ts_ops vtable                 │
│  - size, alignment          - value_schema (TypeMeta)       │
│  - child schemas            - time_meta, observer_meta      │
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
│  - data_value_              - ViewData + current_time       │
│  - time_value_              - Kind-specific wrappers        │
│  - observer_value_          - (TSBView, TSLView, etc.)      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Graph Endpoint Layer                     │
├─────────────────────────────────────────────────────────────┤
│  TSOutput                   TSInput                         │
│  - native_value_ (TSValue)  - value_ (with LINKs)           │
│  - alternatives_ (casts)    - active_value_ (subscriptions) │
└─────────────────────────────────────────────────────────────┘
```

## Cross-Cutting Concerns

### Memory Management

**User Perspective**: Value semantics throughout. Users work with values as simple data - no manual memory management, no pointers, just values.

**Internal Implementation**:
1. **Schema provides size**: TypeMeta contains the static size and alignment required for the value's memory
2. **Lightweight SBO**: Primitives (bool, int64_t, double, nb::object, etc.) stored inline in data_ pointer - no allocation
3. **Heap for compounds**: Non-primitive types get heap-allocated memory
4. **Vtable initializes**: The type_ops vtable methods (default_construct, copy_construct, etc.) initialize the memory
5. **RAII cleanup**: Value destructor calls type_ops::destruct, then frees heap memory if allocated

```
Construction:
  Value(TypeMeta* meta)
    if meta->is_primitive():
      inline_data_ = 0
      meta->ops()->default_construct(&inline_data_, meta)
    else:
      heap_data_ = allocate(meta->size(), meta->alignment())
      meta->ops()->default_construct(heap_data_, meta)

Destruction:
  ~Value()
    if is_inline():
      meta_->ops()->destruct(&inline_data_, meta_)
    else:
      meta_->ops()->destruct(heap_data_, meta_)
      deallocate(heap_data_)
```

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

- TODO: List unresolved design questions

## References

- User Guide: `ts_value_v2601/user_guide/`
- Research: `ts_value_v2601/research/`
