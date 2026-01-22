# Value/View Implementation Conformance Review

**Date**: 2026-01-21
**Reviewer**: Claude Code
**Documents Reviewed**:
- Design: `ts_value_v2601/design/02_VALUE.md`
- User Guide: `ts_value_v2601/user_guide/02_VALUE.md`

**Implementation Files**:
- `cpp/include/hgraph/types/value/value_view.h`
- `cpp/include/hgraph/types/value/value.h`
- `cpp/include/hgraph/types/value/indexed_view.h`
- `cpp/include/hgraph/types/value/value_fwd.h`

---

## 1. View Class Conformance

### Design Specification (from docs)

The design document specifies:
```cpp
class View {
    void* data_;              // Borrowed pointer to data
    const TypeMeta* schema_;  // Type schema
    Value* owner_;            // Owning Value (for lifetime tracking)
    Path path_;               // Path from owner root to this position

public:
    const TypeMeta& schema() const;
    Value& owner();
    const Path& path() const;
    size_t size() const;
    template<typename T> T as() const;
    template<typename T> T* data();
    View at(size_t index);
    View at(std::string_view name);
    bool equals(const View& other) const;
    size_t hash() const;
    std::string to_string() const;
    nb::object to_python() const;
};
```

### Implementation Analysis

| Specified Feature | Implementation Status | Notes |
|-------------------|----------------------|-------|
| `data_` member | **Present** | Named `_data` (convention difference) |
| `schema_` member | **Present** | Named `_schema` (convention difference) |
| `owner_` member | **Partial** | Named `_root`, uses `void*` instead of `Value*` |
| `path_` member | **MISSING** | Path tracking not implemented |
| `schema()` | **Present** | Returns `const TypeMeta*` not `const TypeMeta&` |
| `owner()` | **Partial** | Named `root<Policy>()`, templated |
| `path()` | **MISSING** | No path tracking |
| `size()` | **MISSING on View** | Only on specialized views |
| `as<T>()` | **Present** | Differs: returns reference, not value |
| `data<T>()` | **MISSING** | Has `data()` returning `void*` |
| `at(size_t)` | **MISSING on View** | Only on specialized views |
| `at(string_view)` | **MISSING on View** | Only on specialized views |
| `equals()` | **Present** | Matches spec |
| `hash()` | **Present** | Matches spec |
| `to_string()` | **Present** | Matches spec |
| `to_python()` | **Present** | Matches spec |

### Additions in Implementation (Not in Design)

| Feature | Location | Purpose |
|---------|----------|---------|
| `valid()` | View | Validity checking (not in design) |
| `operator bool()` | View | Boolean conversion for validity |
| `is_scalar()` | View | Type kind query |
| `is_tuple()` | View | Type kind query |
| `is_bundle()` | View | Type kind query |
| `is_list()` | View | Type kind query |
| `is_set()` | View | Type kind query |
| `is_map()` | View | Type kind query |
| `is_cyclic_buffer()` | View | Type kind query |
| `is_queue()` | View | Type kind query |
| `is_type()` | View | Schema comparison |
| `is_scalar_type<T>()` | View | Typed check |
| `try_as<T>()` | View | Safe access returning pointer |
| `checked_as<T>()` | View | Throwing access |
| `try_as_*()` methods | View | Safe specialized view conversion |
| `as_*()` methods | View | Throwing specialized view conversion |
| `copy_from()` | View | Data copying |
| `from_python()` | View | Python-to-native conversion |
| `set_root<Policy>()` | View | Root tracking setter |

### Critical Issues

1. **Path tracking not implemented**: The design specifies `Path path_` member and `path()` accessor for tracking navigation through nested structures. This is completely absent in the implementation.

2. **Owner tracking is weakened**: Design specifies `Value* owner_` for type-safe owner tracking. Implementation uses `void* _root` with templated accessors, losing type safety.

3. **Base View missing navigation**: The design shows `at(index)` and `at(name)` on the base View class. Implementation only provides these on specialized views (BundleView, ListView, etc.).

4. **Size not on base View**: Design shows `size()` on View; implementation only has it on specialized views.

---

## 2. Value Class Conformance

### Design Specification (from docs)

The design document specifies:
```cpp
class Value {
    TypeMeta* meta_;          // or tagged pointer for null support
    union {
        void* heap_data_;
        uint64_t inline_data_;
    };

public:
    explicit Value(TypeMeta* meta);
    Value(TypeMeta* meta, const void* src);

    void* data();
    const void* data() const;
    TypeMeta* meta() const;

    View view();
    View view() const;

    // Null support
    bool has_value() const;
    explicit operator bool() const;
    void reset();
    void emplace();
};
```

### Implementation Analysis

| Specified Feature | Implementation Status | Notes |
|-------------------|----------------------|-------|
| `meta_` member | **Present** | Named `_schema` |
| SBO union storage | **Delegated** | Uses `ValueStorage` class |
| `Value(TypeMeta*)` | **Present** | Takes `const TypeMeta*` |
| `Value(meta, src)` | **MISSING** | No direct copy-from-void* constructor |
| `data()` | **Present** | Matches spec |
| `meta()` | **Present** | Named `schema()` |
| `view()` | **Present** | Matches spec |
| `has_value()` | **MISSING** | Uses `valid()` instead |
| `operator bool()` | **Present** | Based on `valid()`, not `has_value()` |
| `reset()` | **MISSING** | Null semantics not implemented |
| `emplace()` | **MISSING** | Null semantics not implemented |

### User Guide Requirements

| Feature | Implementation Status | Notes |
|---------|----------------------|-------|
| `Value(schema)` | **Present** | Matches |
| `Value(schema, py_obj)` | **MISSING** | No Python constructor |
| `as_bundle()` | **Present** | Matches |
| `as_list()` | **Present** | Matches |
| `as_set()` | **Present** | Matches |
| `as_map()` | **Present** | Matches |
| `copy()` | **Present** | Static method, not instance method |
| `copy_from(View)` | **MISSING on Value** | Only on View |
| `set(Value&)` | **MISSING** | Not implemented |
| `apply_delta(DeltaValue&)` | **MISSING** | Delta system not implemented |
| `from_python()` | **Present** | Matches |
| `to_python()` | **Present** | Matches |

### Additions in Implementation (Not in Design)

| Feature | Purpose |
|---------|---------|
| `template<typename Policy>` | Policy-based extensions |
| `Value(const T& val)` | Direct scalar construction |
| `Value(const View&)` | Copy from view constructor |
| `static copy()` | Static copy factory |
| `const_view()` | Explicit const view access |
| `as<T>()`, `try_as<T>()`, `checked_as<T>()` | Direct scalar access on Value |
| `is_scalar_type<T>()` | Type checking |
| `on_modified()` | Modification callbacks (policy-based) |
| Policy aliases | `PlainValue`, `CachedValue`, `TSValue`, `ValidatedValue` |

### Critical Issues

1. **Null Value semantics not implemented**: The design specifies typed-null support with `has_value()`, `reset()`, and `emplace()` methods. Implementation uses `valid()` which checks for non-null schema AND data, but doesn't support the "typed null" concept (schema present, value absent).

2. **Pointer tagging not implemented**: Design specifies using pointer tagging on `meta_` for null flag. Not evident in the implementation.

3. **Missing Python construction**: User guide shows `Value(schema, py_obj)` constructor. Implementation only has scalar and View constructors.

4. **Delta system not implemented**: User guide extensively documents `DeltaValue` and `apply_delta()`. Not present in implementation.

---

## 3. Specialized Views Conformance

### TupleView

| Specified (User Guide) | Implementation | Status |
|------------------------|----------------|--------|
| Index access `at(index)` | Present | **OK** |
| Iteration | Present via `ConstIndexedView` | **OK** |
| `element_type(index)` | Present | **OK** |

### BundleView

| Specified (User Guide) | Implementation | Status |
|------------------------|----------------|--------|
| `at(name)` | Present | **OK** |
| `at(index)` | Present | **OK** |
| `field_name(index)` | **MISSING** | Not implemented (has `field_info(index)->name`) |
| `items()` | **MISSING** | No ViewPairRange iteration |
| `field_count()` | Present | **OK** |
| `set(name, value)` | Present | **OK** |
| `set(index, value)` | Present | **OK** |

**Naming**: `field_name(index)` should return `string_view` per design. Implementation requires accessing `field_info(index)->name`.

### ListView

| Specified (User Guide) | Implementation | Status |
|------------------------|----------------|--------|
| `at(index)` | Present | **OK** |
| `append(T)` | **DIFFERENT** | Named `push_back()` |
| `clear()` | Present | **OK** |
| `values()` | **MISSING** | No dedicated values range |
| `items()` | **MISSING** | No index-value pair iteration |
| `size()` | Present | **OK** |
| `front()` | Present | **OK** |
| `back()` | Present | **OK** |
| `resize()` | Present | **ADDITION** |
| `reset()` | Present | **ADDITION** |
| `pop_back()` | Present | **ADDITION** |

### SetView

| Specified (User Guide) | Implementation | Status |
|------------------------|----------------|--------|
| `contains(elem)` | Present | **OK** |
| `add(elem)` | **DIFFERENT** | Named `insert()` |
| `remove(elem)` | **DIFFERENT** | Named `erase()` |
| `values()` | **MISSING** | No dedicated values range |
| `clear()` | Present | **OK** |
| `size()` | Present | **OK** |
| Iteration | Present | **OK** |

### MapView

| Specified (User Guide) | Implementation | Status |
|------------------------|----------------|--------|
| `at(key)` | Present | **OK** |
| `contains(key)` | Present | **OK** |
| `set_item(key, val)` | **DIFFERENT** | Named `set()` |
| `keys()` | Present | **OK** (returns `ConstKeySetView`) |
| `items()` | **MISSING** | No key-value pair iteration |
| `clear()` | Present | **OK** |
| `erase(key)` | Present | **OK** |
| `insert(key, value)` | Present | **ADDITION** |

### CyclicBufferView and QueueView

These are **ADDITIONS** not in the design document but present in implementation:

| Type | Status | Notes |
|------|--------|-------|
| `ConstCyclicBufferView` | Present | Fixed-size circular buffer |
| `CyclicBufferView` | Present | Mutable circular buffer |
| `ConstQueueView` | Present | FIFO queue |
| `QueueView` | Present | Mutable FIFO queue |

### Const* Versions - CRITICAL ISSUE

The design document states:
> "Const Correctness: The View system provides constness (or mutability) throughout the system. This is done using standard C++ const markers on methods to indicate which are considered const and which are not."

The user requirement states there should be **NO "Const" prefix** per design.

**Implementation has BOTH versions**:
- `ConstTupleView` / `TupleView`
- `ConstBundleView` / `BundleView`
- `ConstListView` / `ListView`
- `ConstSetView` / `SetView`
- `ConstMapView` / `MapView`
- `ConstCyclicBufferView` / `CyclicBufferView`
- `ConstQueueView` / `QueueView`
- `ConstIndexedView` / `IndexedView`
- `ConstKeySetView` (no mutable version)

**This is a DESIGN VIOLATION.** The specification calls for a single class per view type using const/non-const method overloads, not separate Const-prefixed classes.

---

## 4. Memory Layout Conformance

### Design Specification

```cpp
class Value {
    TypeMeta* meta_;          // or uintptr_t tagged_meta_ for null
    union {
        void* heap_data_;
        uint64_t inline_data_;
    };
};
```

### Implementation

The implementation uses a separate `ValueStorage` class:

```cpp
template<typename Policy>
class Value : private PolicyStorage<Policy> {
    ValueStorage _storage;
    const TypeMeta* _schema{nullptr};
};
```

| Aspect | Design | Implementation | Status |
|--------|--------|----------------|--------|
| Schema storage | Direct `meta_` pointer | `_schema` pointer | **OK** |
| SBO union | Direct in class | Delegated to `ValueStorage` | **DIFFERENT** |
| Null flag | Pointer tagging | Not implemented | **MISSING** |
| Policy storage | Not specified | EBO via `PolicyStorage` | **ADDITION** |

The delegation to `ValueStorage` adds indirection but enables better encapsulation. However, the null-flag pointer tagging is not implemented.

---

## 5. Critical Issues Summary

### Design Principle Violations

1. **Const-prefixed classes exist**: The implementation uses `Const*` prefixed classes (ConstBundleView, ConstListView, etc.) which violates the stated design principle of using const methods, not separate classes.

2. **Path tracking absent**: The design explicitly shows path tracking (`Path path_`) for navigation context. This is completely unimplemented.

3. **Null Value semantics absent**: The design specifies typed-null support with pointer tagging. Not implemented.

### Missing Core Functionality

1. **No `at()` methods on base View**: Navigation is only available on specialized views.
2. **No `size()` on base View**: Only on specialized views.
3. **No Delta system**: `DeltaValue`, `DeltaView`, and `apply_delta()` are documented but not implemented.
4. **No `items()` iteration**: Missing on BundleView, ListView, MapView.
5. **No Python constructor**: `Value(schema, py_obj)` not available.

### Naming Convention Differences

| Design Name | Implementation Name | Impact |
|-------------|---------------------|--------|
| `meta()` | `schema()` | Medium - API difference |
| `owner()` | `root<Policy>()` | Medium - API and signature difference |
| `append()` | `push_back()` | Low - STL convention used |
| `add()` | `insert()` | Low - STL convention used |
| `remove()` | `erase()` | Low - STL convention used |
| `set_item()` | `set()` | Low - simpler naming |
| Member `_data` | `data_` | Low - style only |

---

## 6. Summary Table

| Component | Status | Notes |
|-----------|--------|-------|
| **View** (base class) | **PARTIAL** | Missing path tracking, at(), size() |
| **Value** (base class) | **PARTIAL** | Missing null semantics, Python constructor |
| **TupleView** | **OK** | Functional but has Const variant |
| **BundleView** | **PARTIAL** | Missing field_name(), items() |
| **ListView** | **PARTIAL** | Different naming (push_back vs append), missing items() |
| **SetView** | **PARTIAL** | Different naming (insert/erase vs add/remove) |
| **MapView** | **PARTIAL** | Different naming, missing items() |
| **CyclicBufferView** | **ADDITION** | Not in design docs |
| **QueueView** | **ADDITION** | Not in design docs |
| **Const* classes** | **VIOLATION** | Should not exist per design |
| **Memory Layout** | **PARTIAL** | SBO delegated, null tagging missing |
| **Path Tracking** | **MISSING** | Complete absence |
| **Delta System** | **MISSING** | Not implemented |
| **Python Interop** | **PARTIAL** | Missing constructor, has to_python/from_python |

### Overall Assessment

The implementation provides a functional Value/View system with good coverage of container operations. However, it diverges from the design in several significant ways:

1. **Architecture**: Uses Const-prefixed classes instead of const methods
2. **Navigation**: Missing path tracking for nested access
3. **Null semantics**: Typed-null support not implemented
4. **API naming**: Several method names differ from specification

The implementation appears to follow STL conventions (push_back, insert, erase) rather than the designed API names (append, add, remove). This is a reasonable choice but represents a documentation/implementation gap.

**Recommendation**: Update either the design docs to match the implementation OR update the implementation to match the design docs. The Const-prefixed classes issue is the most significant architectural divergence that should be resolved.
