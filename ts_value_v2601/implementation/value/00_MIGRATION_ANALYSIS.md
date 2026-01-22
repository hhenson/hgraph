# Migration Analysis: hgraph Value Type System

## Document Overview

This document analyzes the current C++ implementation of the hgraph Value type system against the design specifications found in `ts_value_v2601/design/` and `ts_value_v2601/user_guide/`. The analysis identifies matches, gaps, divergences, and recommended migration tasks.

---

## Section 1: Design Summary

### 1.1 Schema Layer (TypeMeta)

The design specifies a `TypeMeta` structure that describes value types with:

**Specified Structure (from design/01_SCHEMA.md):**
- `name_`: Human-readable name (owned string)
- `size_`: Static size in bytes
- `alignment_`: Required alignment
- `ops_`: Operations vtable stored **by value** (not pointer) to reduce pointer chasing
- `kind_`: TypeKind enumeration
- Child type information for composites

**Specified TypeKind:**
- Atomic, Bundle, Tuple, List, Map, Set

**Specified type_ops Structure (design/01_SCHEMA.md lines 169-202):**
- Uses **tagged union** design with common operations plus kind-specific extension ops
- Common: construct, destroy, copy, move, equals, hash, to_string, to_python, from_python
- Kind-specific unions: atomic_ops, bundle_ops, tuple_ops, list_ops, set_ops, map_ops

### 1.2 Value Layer

The design specifies:

**Value Class (design/02_VALUE.md):**
- Type-erased owning container
- Lightweight SBO (Small Buffer Optimization) for primitives that fit within a pointer (8 bytes)
- Nullable values using **pointer tagging** on the TypeMeta pointer (low bit as null flag)
- Methods: construct, destruct, copy/move semantics, to_python/from_python

**View Class (design/02_VALUE.md):**
- Type-erased non-owning reference
- Contains: data pointer, schema pointer, owner pointer, and **path** to track position
- Kind-specific wrappers: BundleView, ListView, MapView, SetView

### 1.3 Set/Map Architecture (design/01_SCHEMA.md lines 217-433)

Design specifies a **layered protocol-based architecture**:
- **KeySet (Core)**: Slot-based storage with generation tracking, hash indexing via `ankerl::unordered_dense::map`
- **SetStorage**: Wraps KeySet, implements set_ops
- **ValueArray**: Observer of KeySet, maintains parallel value array
- **MapStorage**: Composes SetStorage + ValueArray
- **SlotObserver Protocol**: For extensions to track slot lifecycle
- **SlotHandle**: (slot, generation) pairs for stale reference detection

---

## Section 2: Current Implementation Analysis

### 2.1 TypeMeta Structure

**Current Implementation (type_meta.h lines 224-293):**
```cpp
struct TypeMeta {
    size_t size;              // Size in bytes
    size_t alignment;         // Alignment requirement
    TypeKind kind;            // Type category
    TypeFlags flags;          // Capability flags
    const TypeOps* ops;       // Operations vtable (POINTER, not inline)

    const TypeMeta* element_type;   // List/Set element, Map value type
    const TypeMeta* key_type;       // Map key type
    const BundleFieldInfo* fields;  // Bundle/Tuple field metadata
    size_t field_count;             // Number of fields
    size_t fixed_size;              // 0 = dynamic, >0 = fixed
};
```

**Observations:**
- **DIVERGENCE**: `ops` is stored as a **pointer**, not inline as specified in design
- **MISSING**: `name_` field for human-readable type name
- **ADDITION**: `TypeFlags` enum for capability flags (TriviallyConstructible, Hashable, etc.)
- **ADDITION**: `fixed_size` field for fixed-capacity containers
- **ADDITION**: Additional TypeKind values: CyclicBuffer, Queue

### 2.2 TypeOps Structure

**Current Implementation (type_meta.h lines 121-211):**
```cpp
struct TypeOps {
    void (*construct)(void* dst, const TypeMeta* schema);
    void (*destruct)(void* obj, const TypeMeta* schema);
    void (*copy_assign)(void* dst, const void* src, const TypeMeta* schema);
    void (*move_assign)(void* dst, void* src, const TypeMeta* schema);
    void (*move_construct)(void* dst, void* src, const TypeMeta* schema);
    bool (*equals)(const void* a, const void* b, const TypeMeta* schema);
    std::string (*to_string)(const void* obj, const TypeMeta* schema);
    nb::object (*to_python)(const void* obj, const TypeMeta* schema);
    void (*from_python)(void* dst, const nb::object& src, const TypeMeta* schema);
    size_t (*hash)(const void* obj, const TypeMeta* schema);
    bool (*less_than)(const void* a, const void* b, const TypeMeta* schema);
    size_t (*size)(const void* obj, const TypeMeta* schema);
    const void* (*get_at)(const void* obj, size_t index, const TypeMeta* schema);
    void (*set_at)(void* obj, size_t index, const void* value, const TypeMeta* schema);
    const void* (*get_field)(const void* obj, const char* name, const TypeMeta* schema);
    void (*set_field)(void* obj, const char* name, const void* value, const TypeMeta* schema);
    bool (*contains)(const void* obj, const void* element, const TypeMeta* schema);
    void (*insert)(void* obj, const void* element, const TypeMeta* schema);
    void (*erase)(void* obj, const void* element, const TypeMeta* schema);
    const void* (*map_get)(const void* obj, const void* key, const TypeMeta* schema);
    void (*map_set)(void* obj, const void* key, const void* value, const TypeMeta* schema);
    void (*resize)(void* obj, size_t new_size, const TypeMeta* schema);
    void (*clear)(void* obj, const TypeMeta* schema);
};
```

**Observations:**
- **DIVERGENCE**: Uses **flat structure** with all operations as optional function pointers, NOT tagged union design
- **MATCH**: All specified common operations are present
- **ADDITION**: `move_construct` operation for placement new with move semantics
- **ADDITION**: `less_than` for ordered comparison
- **MISSING**: No kind-specific extension ops unions (atomic_ops, bundle_ops, etc.)

### 2.3 Value Class

**Current Implementation (value.h):**
```cpp
template<typename Policy>
class Value : private PolicyStorage<Policy> {
    ValueStorage _storage;
    const TypeMeta* _schema{nullptr};
};
```

**Observations:**
- **MATCH**: Uses template-based policy pattern for extensions (NoCache, WithPythonCache, etc.)
- **MATCH**: Provides type-safe access via `as<T>()`, `try_as<T>()`, `checked_as<T>()`
- **MATCH**: Python interop via `to_python()`, `from_python()`
- **DIVERGENCE**: No pointer tagging for null state - uses separate validity check on `_storage.has_value()`
- **ADDITION**: Policy-based extensions for caching and modification tracking

### 2.4 ValueStorage (SBO Implementation)

**Current Implementation (value_storage.h lines 32-347):**
```cpp
inline constexpr size_t SBO_BUFFER_SIZE = 24;
inline constexpr size_t SBO_ALIGNMENT = 8;

class ValueStorage {
    union Storage {
        alignas(SBO_ALIGNMENT) unsigned char inline_buffer[SBO_BUFFER_SIZE];
        void* heap_ptr;
    };
    Storage _storage;
    const TypeMeta* _schema{nullptr};
    bool _is_inline{true};
};
```

**Observations:**
- **DIVERGENCE**: SBO buffer is 24 bytes, design specifies 8 bytes (sizeof(void*)) for primitives only
- **DIVERGENCE**: Design specifies storing value directly in `data_` field using union with `uint64_t inline_data_`
- **MATCH**: Provides `fits_inline()` static methods for compile-time/runtime SBO decisions

### 2.5 View Classes

**Current Implementation (value_view.h, indexed_view.h):**
- `ConstValueView`: data pointer + schema pointer (2 pointers)
- `ValueView`: inherits ConstValueView, adds mutable data pointer + optional root pointer
- Specialized views: ConstTupleView, TupleView, ConstBundleView, BundleView, ConstListView, ListView, ConstSetView, SetView, ConstMapView, MapView, ConstCyclicBufferView, CyclicBufferView, ConstQueueView, QueueView

**Observations:**
- **GAP**: No `owner_` pointer to owning Value (design specifies `Value* owner_`)
- **GAP**: No `path_` field for tracking position within Value structure
- **ADDITION**: `_root` pointer in ValueView for notification chains (TSValue support)
- **ADDITION**: Separate const/mutable view class hierarchy

### 2.6 Set/Map Storage

**Current Implementation (composite_ops.h lines 1022-2143):**

**SetStorage:**
```cpp
struct SetStorage {
    using IndexSet = ankerl::unordered_dense::set<size_t, SetIndexHash, SetIndexEqual>;
    std::vector<std::byte> elements;     // Contiguous element storage
    size_t element_count{0};
    std::unique_ptr<IndexSet> index_set;
    const TypeMeta* element_type{nullptr};
};
```

**MapStorage:**
```cpp
struct MapStorage {
    SetStorage keys;                   // Embedded SetStorage for key management
    std::vector<std::byte> values;     // Parallel value storage
    const TypeMeta* value_type{nullptr};
};
```

**Observations:**
- **PARTIAL MATCH**: Uses `ankerl::unordered_dense` as specified
- **DIVERGENCE**: Uses index-based hash set, not the specified KeySet with generation tracking
- **MISSING**: SlotObserver protocol for lifecycle events
- **MISSING**: Generation-based liveness tracking for stale reference detection
- **MISSING**: SlotHandle for stable references
- **MATCH**: Map HAS-A Set composition pattern (MapStorage embeds SetStorage)

### 2.7 TypeRegistry

**Current Implementation (type_registry.h):**
- Singleton pattern with `instance()` method
- Template-based scalar registration: `register_scalar<T>()`
- Builder classes: TupleTypeBuilder, BundleTypeBuilder, ListTypeBuilder, SetTypeBuilder, MapTypeBuilder, CyclicBufferTypeBuilder, QueueTypeBuilder
- Named bundle lookup via `get_bundle_by_name()`

**Observations:**
- **PARTIAL MATCH**: Provides registration and lookup
- **GAP**: No name-based lookup for scalar types (only type_index based)
- **GAP**: No `from_python_type()` method for Python type to schema lookup
- **ADDITION**: Additional builders for CyclicBuffer and Queue types

---

## Section 3: Gap Analysis

### 3.1 Matches

| Feature | Design | Implementation | Notes |
|---------|--------|----------------|-------|
| TypeKind enum | Atomic, Bundle, Tuple, List, Set, Map | All present + CyclicBuffer, Queue | Extended |
| TypeMeta fields | size, alignment, kind | All present | Match |
| type_ops common operations | construct, destroy, copy, move, equals, hash, to_string, to_python, from_python | All present | Match |
| SBO optimization | Yes | Yes (24-byte buffer) | Larger than spec |
| Value template with policies | Not specified | Yes | Enhancement |
| View class hierarchy | View with kind-specific wrappers | Const/Mutable pairs | Enhanced |
| Set/Map use ankerl | Yes | Yes | Match |
| Map HAS-A Set | Yes | Yes (SetStorage embedded) | Match |
| TypeRegistry singleton | Yes | Yes | Match |

### 3.2 Gaps (Missing from Implementation)

| Feature | Design Reference | Priority | Notes |
|---------|------------------|----------|-------|
| TypeMeta name field | design/01_SCHEMA.md line 19 | Medium | Human-readable type names |
| type_ops inline (by value) | design/01_SCHEMA.md line 26 | Low | Performance optimization |
| type_ops tagged union | design/01_SCHEMA.md lines 99-204 | Medium | Cleaner kind-specific ops |
| View owner pointer | design/02_VALUE.md lines 158, 189-207 | High | For notification chains |
| View path tracking | design/02_VALUE.md lines 159, 189-207 | High | For navigation and debugging |
| Nullable Value (pointer tagging) | design/02_VALUE.md lines 271-413 | Medium | std::optional-style semantics |
| KeySet with generations | design/01_SCHEMA.md lines 239-288 | Medium | Stale reference detection |
| SlotObserver protocol | design/01_SCHEMA.md lines 247-253 | Low | Extension point |
| SlotHandle | design/01_SCHEMA.md lines 413-423 | Low | Stable references |
| TypeMeta::get(name) | user_guide/01_SCHEMA.md line 109 | High | Name-based schema lookup |
| from_python_type() | user_guide/01_SCHEMA.md line 291 | Medium | Python type to schema |
| ViewRange/ViewPairRange iterators | design/01_SCHEMA.md lines 73-97 | Medium | Unified iteration |
| Delta support (DeltaValue, DeltaView) | user_guide/02_VALUE.md lines 417-475 | High | Time-series delta tracking |

### 3.3 Divergences (Different from Design)

| Feature | Design | Implementation | Impact |
|---------|--------|----------------|--------|
| TypeOps storage | Inline (by value) | Pointer | Extra indirection |
| TypeOps structure | Tagged union | Flat struct with nullptrs | More memory, simpler code |
| SBO size | 8 bytes (pointer-sized) | 24 bytes | Larger inline storage |
| Null representation | Pointer tagging on meta | Separate has_value() check | Different API |
| Set/Map generation tracking | KeySet with generations | Index-based hash set | No stale ref detection |

### 3.4 Additions (Not in Design)

| Feature | Implementation Location | Purpose |
|---------|------------------------|---------|
| TypeFlags enum | type_meta.h lines 57-67 | Capability flags |
| CyclicBuffer type | type_meta.h line 43 | Fixed-size circular buffer |
| Queue type | type_meta.h line 44 | FIFO queue |
| move_construct op | type_meta.h line 138 | Placement new with move |
| Policy template | value.h | Caching, validation, tracking |
| ConstKeySetView | indexed_view.h lines 1214-1321 | Map key iteration |
| Variadic tuple flag | type_meta.h line 66 | tuple[T, ...] support |

---

## Section 4: Migration Tasks

### 4.1 Critical (Blocking for Feature Parity)

1. **Add View Path Tracking**
   - Files: `value_view.h`, `indexed_view.h`
   - Add `path.h` integration to views
   - Track path elements during navigation (at(index), at(name))
   - Required for: Error messages, debugging, time-series notification

2. **Add View Owner Pointer**
   - Files: `value_view.h`
   - Add `Value* _owner` field to ConstValueView
   - Propagate owner through view navigation
   - Required for: Notification chains, lifetime tracking

3. **Implement Delta Support**
   - New files: `delta_value.h`, `delta_view.h`
   - DeltaValue for explicit delta storage
   - DeltaView interface for computed deltas
   - Required for: Time-series delta tracking

4. **Add Name-Based Type Lookup**
   - Files: `type_registry.h`, `type_registry.cpp`
   - Add `name_cache_` map for string-based lookup
   - Add `TypeMeta::name_` field
   - Implement `registry.get("int")` style API

### 4.2 Important (Significant Feature Gaps)

5. **Implement Nullable Values**
   - Files: `value.h`, `value_storage.h`
   - Option A: Pointer tagging (as designed)
   - Option B: Separate `_has_value` flag (simpler)
   - Add `has_value()`, `reset()`, `emplace()` methods

6. **Add Generation-Based Liveness**
   - Files: `composite_ops.h`
   - Add `generations_` vector to SetStorage
   - Modify insert/erase to manage generations
   - Add `is_valid_handle(slot, gen)` method

7. **Add Python Type Lookup**
   - Files: `type_registry.h`
   - Add `python_type_cache_` map
   - Implement `from_python_type(nb::type_object)`
   - For Python class to schema resolution

8. **Implement ViewRange/ViewPairRange**
   - Files: `indexed_view.h` or new `view_range.h`
   - Unified iterator types for all iteration
   - Keys(), values(), items() returning ranges

### 4.3 Minor (Polish and Optimization)

9. **Add TypeMeta Name Field**
   - Files: `type_meta.h`, `type_registry.h`
   - Add `const char* name_` to TypeMeta
   - Populate during registration

10. **Consider Tagged Union for TypeOps**
    - Files: `type_meta.h`
    - Evaluate memory vs code simplicity trade-off
    - Current flat design is working

11. **Adjust SBO Buffer Size**
    - Files: `value_storage.h`
    - Evaluate if 24 bytes is appropriate
    - Design specifies 8 bytes (pointer-sized only)

12. **Add SlotObserver Protocol**
    - Files: `composite_ops.h`
    - Extension point for Set/Map slot lifecycle
    - Low priority without concrete use case

### 4.4 Optional (Enhancements Beyond Design)

13. **Add Type Name Caching**
    - Cache computed type names (e.g., "List[int]")
    - Useful for debugging/logging

14. **Implement less_than for Ordering**
    - Add proper ordering support for sorted containers
    - Currently only equality is well-supported

---

## Section 5: Risk Assessment

### 5.1 High Risk Changes

| Change | Risk | Mitigation |
|--------|------|------------|
| View path tracking | Breaks existing view usage | Phased rollout, optional at first |
| Nullable values | API change | Add new methods, keep old working |
| Delta support | Complex new subsystem | Start with simplest case (atomic) |

### 5.2 Medium Risk Changes

| Change | Risk | Mitigation |
|--------|------|------------|
| Generation tracking | Performance overhead | Benchmark before/after |
| Name-based lookup | Memory for name storage | Use string interning |
| Python type lookup | GIL safety concerns | Document thread safety |

### 5.3 Low Risk Changes

| Change | Risk | Mitigation |
|--------|------|------------|
| TypeMeta name field | Small memory increase | Minimal impact |
| ViewRange types | Additive API | Non-breaking |

### 5.4 Recommended Migration Order

1. **Phase 1 (Foundation)**: Name-based type lookup, TypeMeta name field
2. **Phase 2 (Views)**: Path tracking, owner pointer
3. **Phase 3 (Nullable)**: Nullable value support
4. **Phase 4 (Delta)**: Delta support for time-series
5. **Phase 5 (Polish)**: Generation tracking, ViewRange, SlotObserver

---

## Critical Files for Implementation

- `cpp/include/hgraph/types/value/value_view.h` - Add owner pointer and path tracking to view classes
- `cpp/include/hgraph/types/value/type_meta.h` - Add name field, potentially refactor TypeOps
- `cpp/include/hgraph/types/value/type_registry.h` - Add name-based lookup, Python type lookup
- `cpp/include/hgraph/types/value/composite_ops.h` - Add generation tracking to SetStorage/MapStorage
- `cpp/include/hgraph/types/value/path.h` - Existing path implementation to integrate with views
