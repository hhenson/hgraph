# Value/View Compliance Implementation Plan

**Date**: 2026-01-21
**Purpose**: Bring C++ implementation into compliance with user_guide and design documents
**Exceptions Accepted**: CyclicBufferView and QueueView (additions not in docs, but accepted)

---

## Executive Summary

The implementation plan addresses the following compliance gaps:

| Issue | Priority | Complexity | Phase |
|-------|----------|------------|-------|
| Merge Const*View classes | **CRITICAL** | High | 1 |
| API naming alignment | **HIGH** | Medium | 2 |
| Missing base View methods | **HIGH** | Low | 2 |
| Missing iteration methods | **MEDIUM** | Medium | 3 |
| Path tracking | **MEDIUM** | Medium | 3 |
| Null Value semantics | **MEDIUM** | High | 4 |
| Delta system | **LOW** | High | 5 |
| Python constructor | **LOW** | Low | 5 |

---

## Phase 1: Merge Const*View Classes (CRITICAL)

### Objective
Eliminate all `Const*View` classes by merging into single classes with const/non-const method overloads, following the pattern established for `View`/`ValueView` merger.

### Classes to Merge

| Remove | Merge Into | Files Affected |
|--------|------------|----------------|
| `ConstIndexedView` | `IndexedView` | indexed_view.h |
| `ConstTupleView` | `TupleView` | indexed_view.h |
| `ConstBundleView` | `BundleView` | indexed_view.h |
| `ConstListView` | `ListView` | indexed_view.h |
| `ConstSetView` | `SetView` | indexed_view.h |
| `ConstMapView` | `MapView` | indexed_view.h |
| `ConstCyclicBufferView` | `CyclicBufferView` | indexed_view.h |
| `ConstQueueView` | `QueueView` | indexed_view.h |
| `ConstKeySetView` | `KeySetView` | indexed_view.h |
| `ConstTrackedSetView` | `TrackedSetView` | tracked_set_view.h |

### Implementation Strategy

For each pair (using `ListView` as example):

**Step 1: Update class declaration**
```cpp
// BEFORE
class ConstListView : public ConstIndexedView { ... };
class ListView : public ConstListView { ... };

// AFTER
class ListView : public IndexedView {
public:
    // Const constructor (from const data)
    ListView(const void* data, const TypeMeta* schema) noexcept;

    // Mutable constructor
    ListView(void* data, const TypeMeta* schema) noexcept;

    // Const methods (read-only)
    [[nodiscard]] View at(size_t index) const;
    [[nodiscard]] View front() const;
    [[nodiscard]] View back() const;

    // Mutable methods
    [[nodiscard]] View at(size_t index);
    [[nodiscard]] View front();
    [[nodiscard]] View back();
    void push_back(const View& elem);
    void pop_back();
    void clear();
    void resize(size_t n);
    void set(size_t index, const View& elem);
};
```

**Step 2: Update all usages**
- Search for `ConstListView` references
- Replace with `ListView`
- Update function signatures that return `ConstListView` to return `ListView`

**Step 3: Update Python bindings**
- Remove separate `ConstListView` binding
- Update `ListView` binding with all methods
- Use `static_cast` for overloaded methods as needed

### Files to Modify

```
cpp/include/hgraph/types/value/indexed_view.h      # Main changes
cpp/include/hgraph/types/value/value_view.h        # Return types
cpp/include/hgraph/types/value/value.h             # Return types
cpp/include/hgraph/types/value/tracked_set_view.h  # TrackedSetView
cpp/include/hgraph/types/value/tracked_set_storage.h
cpp/include/hgraph/types/value/set_delta_value.h
cpp/include/hgraph/types/value/path.h
cpp/include/hgraph/types/value/traversal.h
cpp/include/hgraph/types/value/visitor.h
cpp/include/hgraph/types/value/value_fwd.h         # Forward declarations
cpp/include/hgraph/types/tss.h
cpp/src/cpp/types/tss.cpp
cpp/src/cpp/api/python/py_value.cpp                # Python bindings
```

### Estimated Changes
- ~2000 lines of code changes in indexed_view.h
- ~500 lines in py_value.cpp
- ~200 lines across other files

### Verification
- All existing tests must pass
- No `Const*View` class names remain (except in comments)

---

## Phase 2: API Naming Alignment

### Objective
Align method names with user_guide specification.

### Naming Changes Required

| Current | Required | Classes | Action |
|---------|----------|---------|--------|
| `schema()` | `meta()` | View, Value | **DECISION NEEDED** |
| `push_back()` | `append()` | ListView | Rename |
| `insert()` | `add()` | SetView | Rename |
| `erase()` | `remove()` | SetView, MapView | Rename |
| `set()` | `set_item()` | MapView | Rename |
| `root<Policy>()` | `owner()` | View | Rename + simplify |

### Decision: `schema()` vs `meta()`

**DECIDED**: Keep `schema()` in code, update docs to match.
- `schema` is more descriptive than `meta`
- Docs will be updated to use `schema()` terminology

### Implementation

**DECISION**: No deprecation or fallback - direct rename/removal.

**2.1 ListView: `push_back()` → `append()`**
```cpp
void append(const View& elem);  // Renamed from push_back
```

**2.2 SetView: `insert()` → `add()`, `erase()` → `remove()`**
```cpp
bool add(const View& elem);     // Renamed from insert
bool remove(const View& elem);  // Renamed from erase
```

**2.3 MapView: `set()` → `set_item()`, `erase()` → `remove()`**
```cpp
void set_item(const View& key, const View& value);  // Renamed from set
bool remove(const View& key);                        // Renamed from erase
```

**2.4 View: `root()` → `owner()`**
```cpp
// Simplify from template to direct pointer
Value<>* owner() const { return static_cast<Value<>*>(_root); }
void set_owner(Value<>* owner) { _root = owner; }
```

### Files to Modify
```
cpp/include/hgraph/types/value/indexed_view.h
cpp/include/hgraph/types/value/value_view.h
cpp/src/cpp/api/python/py_value.cpp
```

### Add Missing Base View Methods

**2.5 Add `at()` and `size()` to View**
```cpp
class View {
    // ... existing ...

    // Navigation (delegates to specialized views)
    [[nodiscard]] size_t size() const;
    [[nodiscard]] View at(size_t index) const;
    [[nodiscard]] View at(size_t index);
    [[nodiscard]] View at(std::string_view name) const;
    [[nodiscard]] View at(std::string_view name);
};
```

Implementation dispatches based on `_schema->kind`:
```cpp
size_t View::size() const {
    if (!valid()) return 0;
    switch (_schema->kind) {
        case TypeKind::Tuple:
        case TypeKind::Bundle:
        case TypeKind::List:
            return _schema->element_count;  // or dynamic for list
        case TypeKind::Set:
        case TypeKind::Map:
            return /* get from storage */;
        default:
            return 1;  // Scalar has size 1
    }
}
```

---

## Phase 3: Missing Iteration Methods

### Objective
Add `items()` iteration to BundleView, ListView, MapView per user guide.

### Implementation

**3.1 BundleView::items()**
Returns range of (name, View) pairs:
```cpp
class BundleView : public IndexedView {
    // Iterator over (string_view, View) pairs
    class ItemIterator {
        const BundleView* _view;
        size_t _index;
    public:
        std::pair<std::string_view, View> operator*() const {
            return {_view->field_name(_index), (*_view)[_index]};
        }
    };

    [[nodiscard]] auto items() const -> ViewPairRange<ItemIterator>;
    [[nodiscard]] auto items() -> ViewPairRange<ItemIterator>;
};
```

**3.2 BundleView::field_name()**
```cpp
[[nodiscard]] std::string_view field_name(size_t index) const {
    return field_info(index)->name;
}
```

**3.3 ListView::items()**
Returns range of (index, View) pairs:
```cpp
class ListView : public IndexedView {
    [[nodiscard]] auto items() const -> IndexedViewRange;
    [[nodiscard]] auto items() -> IndexedViewRange;
};
```

**3.4 MapView::items()**
Returns range of (View, View) key-value pairs:
```cpp
class MapView : public View {
    [[nodiscard]] auto items() const -> MapItemRange;
    [[nodiscard]] auto items() -> MapItemRange;
};
```

### Files to Modify
```
cpp/include/hgraph/types/value/indexed_view.h
cpp/include/hgraph/types/value/view_range.h  # Add new range types
cpp/src/cpp/api/python/py_value.cpp
```

---

## Phase 4: Path Tracking

### Objective
Implement path tracking for nested value navigation per design docs.

### Design
```cpp
class View {
    void* _data{nullptr};
    const TypeMeta* _schema{nullptr};
    void* _root{nullptr};
    ValuePath _path;  // NEW: Track navigation path

public:
    [[nodiscard]] const ValuePath& path() const { return _path; }
};
```

### Path Propagation
When creating sub-views via `at()` or `as_bundle()`, propagate and extend path:
```cpp
View BundleView::at(std::string_view name) {
    // ... get element data ...
    View result(elem_data, elem_schema);
    result._root = _root;
    result._path = _path;
    result._path.push(PathElement::field(name));
    return result;
}
```

### Memory Consideration
`ValuePath` should use small-buffer optimization for common cases (depth ≤ 3).

### Files to Modify
```
cpp/include/hgraph/types/value/value_view.h
cpp/include/hgraph/types/value/indexed_view.h
cpp/include/hgraph/types/value/path.h
```

---

## Phase 5: Null Value Semantics

### Objective
Implement typed-null support with pointer tagging per design docs.

### Design
A Value can be in three states:
1. **Invalid**: No schema, no data (default constructed)
2. **Null**: Has schema, but no value (typed null)
3. **Valid**: Has schema and value

### Implementation

**5.1 Pointer Tagging**
Use low bit of `_schema` pointer for null flag (pointers are aligned):
```cpp
class Value {
    uintptr_t _tagged_schema{0};  // Low bit = null flag
    ValueStorage _storage;

    const TypeMeta* schema() const {
        return reinterpret_cast<const TypeMeta*>(_tagged_schema & ~uintptr_t(1));
    }

    bool is_null() const {
        return (_tagged_schema & 1) != 0;
    }

    void set_null_flag(bool is_null) {
        if (is_null) _tagged_schema |= 1;
        else _tagged_schema &= ~uintptr_t(1);
    }
};
```

**5.2 New Methods**
```cpp
class Value {
    // Check if value is present (not null, not invalid)
    [[nodiscard]] bool has_value() const {
        return schema() != nullptr && !is_null();
    }

    // Reset to null state (keeps schema)
    void reset() {
        if (schema()) {
            _storage.destroy(schema());
            set_null_flag(true);
        }
    }

    // Construct value in place (from null to valid)
    void emplace() {
        if (schema() && is_null()) {
            _storage.construct(schema());
            set_null_flag(false);
        }
    }

    // Backwards compatibility
    [[nodiscard]] bool valid() const { return has_value(); }
};
```

### Migration Strategy
1. Add new methods alongside existing `valid()`
2. Update internal code to use `has_value()` where appropriate
3. Deprecate `valid()` in favor of `has_value()`

### Files to Modify
```
cpp/include/hgraph/types/value/value.h
cpp/include/hgraph/types/value/value_storage.h
cpp/src/cpp/api/python/py_value.cpp
```

---

## Phase 6: Delta System

### Objective
Implement DeltaValue and DeltaView per user guide.

### Design Overview
```cpp
// Represents changes to a Value
class DeltaValue {
    PlainValue _added;    // New/changed elements
    PlainValue _removed;  // Removed elements (for sets/maps)
    const TypeMeta* _schema;

public:
    [[nodiscard]] DeltaView view() const;
    [[nodiscard]] bool empty() const;
};

class DeltaView {
    const void* _added_data;
    const void* _removed_data;
    const TypeMeta* _schema;

public:
    [[nodiscard]] View added() const;
    [[nodiscard]] View removed() const;
};

// On Value
class Value {
    void apply_delta(const DeltaView& delta);
};
```

### Scope
The delta system is documented but complex. Defer detailed design until Phases 1-5 are complete.

### Files to Create/Modify
```
cpp/include/hgraph/types/value/delta_value.h  # Already exists partially
cpp/include/hgraph/types/value/delta_view.h   # Already exists partially
cpp/include/hgraph/types/value/value.h
cpp/src/cpp/api/python/py_value.cpp
```

---

## Phase 7: Python Constructor

### Objective
Add `Value(schema, py_obj)` constructor per user guide.

### Implementation
```cpp
template<typename Policy>
class Value {
    // Construct from Python object
    Value(const TypeMeta* schema, const nb::object& py_obj)
        : _schema(schema) {
        if (_schema) {
            _storage.allocate(_schema);
            _schema->ops->from_python(_storage.data(), py_obj, _schema);
        }
    }
};
```

### Python Binding
```cpp
.def(nb::init<const TypeMeta*, nb::object>(),
     "schema"_a, "py_obj"_a,
     "Construct Value from schema and Python object")
```

---

## Implementation Order

```
Phase 1: Merge Const*View classes     [~3 days]  CRITICAL
    └── Phase 2: API naming           [~1 day]   HIGH
        ├── Phase 3: Iteration        [~1 day]   MEDIUM
        └── Phase 4: Path tracking    [~1 day]   MEDIUM
            └── Phase 5: Null semantics [~2 days] MEDIUM
                └── Phase 6: Delta    [~3 days]  LOW
                    └── Phase 7: Python [~0.5 day] LOW
```

---

## Testing Strategy

### Phase 1 Tests
- All existing tests must pass after Const*View merger
- No `Const*View` types referenced in test code (except for backwards compat aliases during transition)

### Phase 2 Tests
- Test new method names (`append`, `add`, `remove`, `set_item`)
- Test deprecated method warnings
- Test `at()` and `size()` on base View

### Phase 3 Tests
- Test `items()` iteration on BundleView, ListView, MapView
- Test `field_name()` on BundleView

### Phase 4 Tests
- Test path tracking through nested navigation
- Test path string conversion

### Phase 5 Tests
- Test null state transitions (invalid → null → valid)
- Test `has_value()`, `reset()`, `emplace()`
- Test pointer tagging doesn't corrupt schema pointer

### Phase 6 Tests
- Test DeltaValue construction and access
- Test `apply_delta()` for each container type

---

## Risk Assessment

| Phase | Risk | Mitigation |
|-------|------|------------|
| 1 | Breaking changes to existing code | Provide type aliases during transition |
| 2 | API breakage | Use deprecation warnings, not immediate removal |
| 4 | Memory overhead from Path | Use SBO for paths ≤ 3 levels |
| 5 | Pointer tagging assumptions | Verify alignment guarantees |
| 6 | Complex delta semantics | Defer until core is stable |

---

## Acceptance Criteria

1. **No `Const*View` classes** (except CyclicBuffer/Queue exceptions)
2. **All user_guide method names available** (deprecated aliases acceptable)
3. **Base View has `at()` and `size()`**
4. **Container views have `items()` iteration**
5. **Path tracking functional** for nested navigation
6. **Null semantics implemented** with `has_value()`, `reset()`, `emplace()`
7. **All existing tests pass**
8. **New tests cover added functionality**

---

## Document Updates Required

After implementation, update:
1. `ts_value_v2601/design/02_VALUE.md` - Note `schema()` vs `meta()` decision
2. `ts_value_v2601/user_guide/02_VALUE.md` - Ensure examples match implementation
3. `04_CONFORMANCE_REVIEW.md` - Mark issues as resolved
