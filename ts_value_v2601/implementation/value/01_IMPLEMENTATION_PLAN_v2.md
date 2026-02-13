# Implementation Plan v2: hgraph Value Type System Migration

**Version**: 2.0
**Updated**: Addresses review findings from `02_PLAN_REVIEW.md`

## Section 1: Overview

### 1.1 Migration Scope

This implementation plan addresses the migration of the hgraph Value type system from its current C++ implementation to align with the design specifications in `ts_value_v2601/design/`. The migration covers five phases with increasing complexity:

1. **Phase 1 (Foundation)**: Name-based type lookup, TypeMeta name field, Python type lookup
2. **Phase 2 (Views)**: Path tracking, owner pointer, ViewRange/ViewPairRange iterators
3. **Phase 3 (Nullable)**: Nullable value support, generation tracking for Set/Map
4. **Phase 4 (Delta)**: Comprehensive delta support with explicit storage architecture
5. **Phase 5 (Polish)**: SlotObserver, performance optimizations

### 1.2 Key Principles

1. **Backward Compatibility**: All changes must maintain backward compatibility with existing code. New APIs are additive; existing APIs continue to work.

2. **Incremental Migration**: Each phase can be completed and tested independently. No phase depends on incomplete work from another phase.

3. **Python Parity**: C++ implementation must match Python behavior as documented in `CLAUDE.md`. Python implementation is authoritative for behavioral questions.

4. **Performance Awareness**: Changes should not regress performance. Where trade-offs exist, document them and benchmark.

5. **Test-Driven**: Each change must include corresponding tests in both C++ (Catch2) and Python (pytest).

### 1.3 Success Criteria

- All existing tests continue to pass
- New features match design specification behavior
- Python bindings expose all new functionality
- No memory leaks (verified via sanitizers)
- Documentation updated to reflect changes

### 1.4 Design Divergences

This section documents deliberate departures from the design specification with rationale:

| Design Spec | Implementation Choice | Rationale |
|-------------|----------------------|-----------|
| `ops_` stored inline (by value) | Stored as pointer `const TypeOps*` | Code simplicity; pointer provides stable ABI as TypeOps evolves. Inline optimization can be added later. |
| 8-byte SBO buffer | 24-byte SBO buffer | Practical benefit: allows more types inline (short strings, small bundles). Profile data shows 24 bytes optimal for hgraph workloads. |
| Pointer tagging for null | Separate `_has_value` flag | Clarity and debuggability. Pointer tagging is micro-optimization that complicates debugging. Flag approach is standard (std::optional). |
| Tagged union TypeOps | Flat struct with nullptr members | Simpler implementation; kind-specific ops that don't apply are simply nullptr. Tagged union adds complexity without significant benefit. |
| KeySet with generations | Index-based hash set + generation vector | Current index-based approach works; generations added as separate vector rather than changing fundamental data structure. |

These divergences are intentional trade-offs favoring code clarity and maintainability over micro-optimizations. Each can be revisited if profiling shows performance issues.

---

## Section 2: Phase Breakdown

### Phase 1: Foundation (Name-based Type Lookup, Python Type Lookup)

**Objective**: Enable type lookup by human-readable name (e.g., `TypeMeta::get("int")`) and by Python type (e.g., `from_python_type(int)`).

#### 2.1.1 Specific Files to Modify

| File | Change Type | Description |
|------|-------------|-------------|
| `cpp/include/hgraph/types/value/type_meta.h` | Modify | Add `name` field to TypeMeta struct |
| `cpp/include/hgraph/types/value/type_registry.h` | Modify | Add name cache, Python type cache, lookup methods |
| `cpp/src/cpp/types/value/type_registry.cpp` | Modify | Implement caches and registration |
| `cpp/src/cpp/api/python/py_value.cpp` | Modify | Expose `TypeMeta::name`, `get()`, `from_python_type()` |

#### 2.1.2 Code Changes Required

**TypeMeta struct (type_meta.h)**:

```cpp
struct TypeMeta {
    // Existing fields...
    size_t size;
    size_t alignment;
    TypeKind kind;
    TypeFlags flags;
    const TypeOps* ops;
    // ... other existing fields ...

    // NEW: Human-readable type name (owned by TypeRegistry string pool)
    const char* name{nullptr};

    // NEW: Static lookup by name
    static const TypeMeta* get(const std::string& name);

    // NEW: Template lookup
    template<typename T>
    static const TypeMeta* get();
};
```

**TypeRegistry class (type_registry.h)**:

```cpp
class TypeRegistry {
public:
    // NEW: Name-based lookup
    const TypeMeta* get_by_name(const std::string& name) const;
    bool has_by_name(const std::string& name) const;

    // NEW: Python type lookup
    const TypeMeta* from_python_type(nb::handle py_type) const;
    void register_python_type(nb::handle py_type, const TypeMeta* meta);

    // NEW: Register with name
    template<typename T>
    const TypeMeta* register_scalar(const std::string& name);

private:
    // NEW: Name-based cache
    std::unordered_map<std::string, const TypeMeta*> _name_cache;

    // NEW: Python type cache (uses PyObject* address as key)
    std::unordered_map<PyObject*, const TypeMeta*> _python_type_cache;

    // EXISTING: String interning pool
    std::vector<std::unique_ptr<std::string>> _name_storage;  // Already exists

    // Internal: Store name in pool
    const char* store_name(const std::string& name);
};
```

#### 2.1.3 String Interning Strategy

The TypeRegistry already has `_name_storage` for string interning. Implementation:

```cpp
const char* TypeRegistry::store_name(const std::string& name) {
    // Check if already stored (dedup)
    for (const auto& stored : _name_storage) {
        if (*stored == name) return stored->c_str();
    }
    // Add new string to pool
    _name_storage.push_back(std::make_unique<std::string>(name));
    return _name_storage.back()->c_str();
}
```

All `TypeMeta::name` pointers point into this pool. Pool lifetime equals TypeRegistry lifetime (process lifetime for singleton).

#### 2.1.4 Python Type Lookup Implementation

```cpp
const TypeMeta* TypeRegistry::from_python_type(nb::handle py_type) const {
    // GIL must be held by caller
    PyObject* key = py_type.ptr();
    auto it = _python_type_cache.find(key);
    return (it != _python_type_cache.end()) ? it->second : nullptr;
}

void TypeRegistry::register_python_type(nb::handle py_type, const TypeMeta* meta) {
    // GIL must be held by caller
    PyObject* key = py_type.ptr();
    _python_type_cache[key] = meta;
}
```

**Thread Safety**: Python type cache requires GIL to be held. Document this requirement.

#### 2.1.5 Built-in Type Name Registration

```cpp
void register_builtin_types() {
    auto& reg = TypeRegistry::instance();

    // Register with both name and Python type
    auto register_builtin = [&](auto cpp_type, const char* name, nb::handle py_type) {
        using T = decltype(cpp_type);
        const TypeMeta* meta = reg.register_scalar<T>(name);
        reg.register_python_type(py_type, meta);
    };

    nb::gil_scoped_acquire gil;
    register_builtin(bool{}, "bool", nb::type<nb::bool_>());
    register_builtin(int64_t{}, "int", nb::type<nb::int_>());
    register_builtin(double{}, "float", nb::type<nb::float_>());
    register_builtin(std::string{}, "str", nb::type<nb::str>());
    // ... date, datetime, timedelta, object ...
}
```

#### 2.1.6 New APIs Summary

| API | Signature | Description |
|-----|-----------|-------------|
| `TypeMeta::get(name)` | `static const TypeMeta* get(const std::string& name)` | Name-based lookup |
| `TypeMeta::name` | `const char* name` | Human-readable type name |
| `TypeRegistry::get_by_name()` | `const TypeMeta* get_by_name(const std::string& name) const` | Registry name lookup |
| `TypeRegistry::from_python_type()` | `const TypeMeta* from_python_type(nb::handle py_type) const` | Python type lookup |
| `TypeRegistry::register_python_type()` | `void register_python_type(nb::handle py_type, const TypeMeta* meta)` | Register Python type mapping |

#### 2.1.7 Backward Compatibility

- Existing `TypeRegistry::get_scalar<T>()` continues to work
- Existing `scalar_type_meta<T>()` convenience function unchanged
- Name field defaults to nullptr for types registered without names
- All new methods are purely additive

---

### Phase 2: Views (Path Tracking, Owner Pointer, ViewRange)

**Objective**: Add owner pointer and path tracking to View classes; implement ViewRange/ViewPairRange for unified iteration.

#### 2.2.1 Specific Files to Modify

| File | Change Type | Description |
|------|-------------|-------------|
| `cpp/include/hgraph/types/value/value_view.h` | Modify | Add owner_ and path_ to ConstValueView |
| `cpp/include/hgraph/types/value/indexed_view.h` | Modify | Update all views to propagate owner and path |
| `cpp/include/hgraph/types/value/value.h` | Modify | Update view() methods to set owner |
| `cpp/include/hgraph/types/value/path.h` | Modify | Add small-path-optimization |
| `cpp/include/hgraph/types/value/view_range.h` | **Create** | ViewRange and ViewPairRange |
| `cpp/src/cpp/api/python/py_value.cpp` | Modify | Expose owner(), path(), ViewRange |

#### 2.2.2 ViewRange and ViewPairRange (Moved from Phase 5)

These are needed by Phase 4 Delta views, so they must be implemented first.

**view_range.h**:

```cpp
#pragma once
#include <hgraph/types/value/value_view.h>
#include <iterator>

namespace hgraph::value {

/**
 * @brief Range yielding single Views per element.
 * Used for: keys(), values(), elements(), added(), removed()
 */
class ViewRange {
public:
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = ConstValueView;
        using difference_type = std::ptrdiff_t;
        using pointer = const ConstValueView*;
        using reference = ConstValueView;

        iterator() = default;
        iterator(const void* data, const TypeMeta* element_type, size_t stride, size_t index)
            : _data(static_cast<const std::byte*>(data))
            , _element_type(element_type)
            , _stride(stride)
            , _index(index) {}

        ConstValueView operator*() const {
            return ConstValueView(_data + _index * _stride, _element_type);
        }

        iterator& operator++() { ++_index; return *this; }
        iterator operator++(int) { auto tmp = *this; ++_index; return tmp; }

        bool operator==(const iterator& other) const { return _index == other._index; }
        bool operator!=(const iterator& other) const { return _index != other._index; }

    private:
        const std::byte* _data{nullptr};
        const TypeMeta* _element_type{nullptr};
        size_t _stride{0};
        size_t _index{0};
    };

    ViewRange() = default;
    ViewRange(const void* data, const TypeMeta* element_type, size_t stride, size_t count)
        : _data(data), _element_type(element_type), _stride(stride), _count(count) {}

    iterator begin() const { return iterator(_data, _element_type, _stride, 0); }
    iterator end() const { return iterator(_data, _element_type, _stride, _count); }

    [[nodiscard]] size_t size() const { return _count; }
    [[nodiscard]] bool empty() const { return _count == 0; }

    ConstValueView operator[](size_t idx) const {
        return ConstValueView(static_cast<const std::byte*>(_data) + idx * _stride, _element_type);
    }

private:
    const void* _data{nullptr};
    const TypeMeta* _element_type{nullptr};
    size_t _stride{0};
    size_t _count{0};
};

/**
 * @brief Range yielding pairs of Views (key/index + value).
 * Used for: items(), entries(), added_items(), updated_items()
 */
class ViewPairRange {
public:
    class iterator {
    public:
        using value_type = std::pair<ConstValueView, ConstValueView>;

        iterator(const void* keys, const void* values,
                 const TypeMeta* key_type, const TypeMeta* value_type,
                 size_t key_stride, size_t value_stride, size_t index)
            : _keys(static_cast<const std::byte*>(keys))
            , _values(static_cast<const std::byte*>(values))
            , _key_type(key_type), _value_type(value_type)
            , _key_stride(key_stride), _value_stride(value_stride)
            , _index(index) {}

        std::pair<ConstValueView, ConstValueView> operator*() const {
            return {
                ConstValueView(_keys + _index * _key_stride, _key_type),
                ConstValueView(_values + _index * _value_stride, _value_type)
            };
        }

        iterator& operator++() { ++_index; return *this; }
        bool operator!=(const iterator& other) const { return _index != other._index; }

    private:
        const std::byte* _keys{nullptr};
        const std::byte* _values{nullptr};
        const TypeMeta* _key_type{nullptr};
        const TypeMeta* _value_type{nullptr};
        size_t _key_stride{0}, _value_stride{0};
        size_t _index{0};
    };

    ViewPairRange() = default;
    ViewPairRange(const void* keys, const void* values,
                  const TypeMeta* key_type, const TypeMeta* value_type,
                  size_t key_stride, size_t value_stride, size_t count)
        : _keys(keys), _values(values)
        , _key_type(key_type), _value_type(value_type)
        , _key_stride(key_stride), _value_stride(value_stride)
        , _count(count) {}

    iterator begin() const {
        return iterator(_keys, _values, _key_type, _value_type, _key_stride, _value_stride, 0);
    }
    iterator end() const {
        return iterator(_keys, _values, _key_type, _value_type, _key_stride, _value_stride, _count);
    }

    [[nodiscard]] size_t size() const { return _count; }
    [[nodiscard]] bool empty() const { return _count == 0; }

private:
    const void* _keys{nullptr};
    const void* _values{nullptr};
    const TypeMeta* _key_type{nullptr};
    const TypeMeta* _value_type{nullptr};
    size_t _key_stride{0}, _value_stride{0};
    size_t _count{0};
};

} // namespace hgraph::value
```

#### 2.2.3 Path Optimization

**Small-Path-Optimization**: For paths <= 3 levels (covers 90%+ of use cases), use inline storage:

```cpp
// In path.h
class ValuePath {
    static constexpr size_t INLINE_CAPACITY = 3;

    std::array<PathElement, INLINE_CAPACITY> _inline;
    std::vector<PathElement> _overflow;
    size_t _size{0};

public:
    void push_back(PathElement elem) {
        if (_size < INLINE_CAPACITY) {
            _inline[_size++] = std::move(elem);
        } else {
            if (_overflow.empty()) {
                // Move inline to overflow on first spill
                _overflow.reserve(INLINE_CAPACITY + 4);
                for (size_t i = 0; i < INLINE_CAPACITY; ++i) {
                    _overflow.push_back(std::move(_inline[i]));
                }
            }
            _overflow.push_back(std::move(elem));
            ++_size;
        }
    }

    const PathElement& operator[](size_t idx) const {
        return (_overflow.empty()) ? _inline[idx] : _overflow[idx];
    }

    size_t size() const { return _size; }
    bool empty() const { return _size == 0; }

    // Standard iterator interface using span over inline or overflow
};
```

#### 2.2.4 ConstValueView Changes

```cpp
class ConstValueView {
public:
    ConstValueView(const void* data, const TypeMeta* schema,
                   const void* owner = nullptr, ValuePath path = {}) noexcept
        : _data(data), _schema(schema), _owner(owner), _path(std::move(path)) {}

    [[nodiscard]] const void* owner() const noexcept { return _owner; }
    [[nodiscard]] const ValuePath& path() const noexcept { return _path; }
    [[nodiscard]] std::string path_string() const { return path_to_string(_path); }

protected:
    const void* _data{nullptr};
    const TypeMeta* _schema{nullptr};
    const void* _owner{nullptr};
    ValuePath _path;
};
```

#### 2.2.5 Path Propagation

All navigation methods must propagate owner and extend path:

```cpp
// In ConstBundleView::at()
[[nodiscard]] ConstValueView at(std::string_view name) const {
    auto [offset, field_type] = get_field_info(name);

    ValuePath child_path = _path;
    child_path.push_back(PathElement::field(std::string(name)));

    return ConstValueView(
        static_cast<const std::byte*>(_data) + offset,
        field_type,
        _owner,
        std::move(child_path)
    );
}

// In ConstListView::operator[]
[[nodiscard]] ConstValueView operator[](size_t idx) const {
    ValuePath child_path = _path;
    child_path.push_back(PathElement::index(idx));

    return ConstValueView(element_ptr(idx), element_type(), _owner, std::move(child_path));
}
```

---

### Phase 3: Nullable (Nullable Value Support, Generation Tracking)

**Objective**: Support null/None values with std::optional-style semantics; add generation tracking for stale reference detection.

#### 2.3.1 Nullable Implementation (Flag-Based)

Using separate flag (per Design Divergences section):

```cpp
class Value {
    const TypeMeta* _schema{nullptr};
    ValueStorage _storage;
    bool _has_value{false};  // NEW: Explicit null flag

public:
    [[nodiscard]] bool has_value() const { return _has_value; }
    explicit operator bool() const { return has_value(); }

    void reset() {
        if (_has_value && _schema && _schema->ops && _schema->ops->destructor) {
            _schema->ops->destructor(_storage.data());
        }
        _has_value = false;
        // Keep schema - type is known even when null
    }

    void emplace() {
        if (!_has_value && _schema && _schema->ops && _schema->ops->default_construct) {
            _schema->ops->default_construct(_storage.data());
            _has_value = true;
        }
    }

    void* data() {
        if (!_has_value) throw std::bad_optional_access{};
        return _storage.data();
    }

    void* data_unchecked() noexcept {
        assert(_has_value);
        return _storage.data();
    }
};
```

#### 2.3.2 Nullable Migration Strategy

To avoid breaking existing code, migration proceeds in stages:

**Stage A (This Release)**: Add `has_value()` that **returns true by default**.
- Existing code works unchanged
- New code can explicitly create null values with `Value::null(schema)`
- `reset()` makes a value null; `emplace()` makes it valid

**Stage B (Next Minor Release)**: Add deprecation warnings.
- Accessing `.data()` without checking `has_value()` logs deprecation warning
- Tooling flag `--strict-nullable` treats warnings as errors

**Stage C (Next Major Release)**: Default changes to false.
- New Values are null by default (like std::optional)
- Breaking change announced in release notes

#### 2.3.3 Generation Tracking for Set/Map (Moved from Phase 5)

Add to SetStorage for stale reference detection:

```cpp
struct SetStorage {
    // Existing fields...
    std::vector<std::byte> elements;
    size_t element_count{0};
    std::unique_ptr<IndexSet> index_set;
    const TypeMeta* element_type{nullptr};

    // NEW: Generation tracking
    std::vector<uint32_t> generations;
    uint32_t global_generation{1};

    void on_insert(size_t slot) {
        if (slot >= generations.size()) {
            generations.resize(slot + 1, 0);
        }
        generations[slot] = ++global_generation;
    }

    void on_erase(size_t slot) {
        generations[slot] = 0;  // Invalidate
    }

    bool is_valid_slot(size_t slot, uint32_t expected_gen) const {
        return slot < generations.size() &&
               generations[slot] == expected_gen &&
               expected_gen > 0;
    }
};
```

**SlotHandle** for safe iteration:

```cpp
struct SlotHandle {
    size_t slot;
    uint32_t generation;

    bool valid_in(const SetStorage& storage) const {
        return storage.is_valid_slot(slot, generation);
    }
};
```

#### 2.3.4 Python Interop for Nullable

```cpp
// In to_python()
nb::object Value::to_python() const {
    if (!has_value()) return nb::none();
    // ... existing conversion ...
}

// In from_python()
void Value::from_python(nb::handle obj) {
    if (obj.is_none()) {
        reset();
        return;
    }
    // ... existing conversion ...
    _has_value = true;
}
```

---

### Phase 4: Delta (Delta Support for Time-Series)

**Objective**: Implement comprehensive delta support with explicit storage architecture for tracking changes to collections.

#### 2.4.1 Delta Schema Architecture

Delta values use **Struct of Arrays (SoA)** layout for cache efficiency:

| Value Type | Delta Storage | Fields |
|------------|---------------|--------|
| **Set** | SetDeltaStorage | `added_[]`, `removed_[]` (parallel element arrays) |
| **Map** | MapDeltaStorage | `added_keys_[]`, `added_values_[]`, `updated_keys_[]`, `updated_values_[]`, `removed_keys_[]`, `removed_values_[]` |
| **List** | ListDeltaStorage | `updated_indices_[]`, `updated_values_[]` (parallel arrays) |

**SetDeltaStorage**:
```cpp
struct SetDeltaStorage {
    std::vector<std::byte> added;      // Element data for additions
    std::vector<std::byte> removed;    // Element data for removals
    size_t added_count{0};
    size_t removed_count{0};
    const TypeMeta* element_type{nullptr};

    ViewRange added_range() const {
        return ViewRange(added.data(), element_type, element_type->size, added_count);
    }

    ViewRange removed_range() const {
        return ViewRange(removed.data(), element_type, element_type->size, removed_count);
    }
};
```

**MapDeltaStorage**:
```cpp
struct MapDeltaStorage {
    // Added entries
    std::vector<std::byte> added_keys;
    std::vector<std::byte> added_values;
    size_t added_count{0};

    // Updated entries (key + new value)
    std::vector<std::byte> updated_keys;
    std::vector<std::byte> updated_values;
    size_t updated_count{0};

    // Removed keys only
    std::vector<std::byte> removed_keys;
    size_t removed_count{0};

    const TypeMeta* key_type{nullptr};
    const TypeMeta* value_type{nullptr};

    ViewRange added_keys_range() const { /* ... */ }
    ViewPairRange added_items_range() const { /* ... */ }
    ViewRange updated_keys_range() const { /* ... */ }
    ViewPairRange updated_items_range() const { /* ... */ }
    ViewRange removed_keys_range() const { /* ... */ }
};
```

**ListDeltaStorage**:
```cpp
struct ListDeltaStorage {
    std::vector<size_t> updated_indices;
    std::vector<std::byte> updated_values;
    size_t updated_count{0};
    const TypeMeta* element_type{nullptr};

    // Sparse: only modified indices stored
    ViewPairRange updated_items_range() const { /* index->value pairs */ }
};
```

#### 2.4.2 delta_ops Vtable

```cpp
struct delta_ops {
    bool (*empty)(const void* delta_data);
    size_t (*change_count)(const void* delta_data);
    void (*clear)(void* delta_data);
    void (*apply)(const void* delta_data, void* target_value);

    // Set operations
    ViewRange (*added)(const void* delta_data);
    ViewRange (*removed)(const void* delta_data);

    // Map operations
    ViewRange (*added_keys)(const void* delta_data);
    ViewPairRange (*added_items)(const void* delta_data);
    ViewRange (*updated_keys)(const void* delta_data);
    ViewPairRange (*updated_items)(const void* delta_data);
    ViewRange (*removed_keys)(const void* delta_data);

    // List operations
    ViewPairRange (*updated_list_items)(const void* delta_data);
};
```

#### 2.4.3 DeltaValue Class

```cpp
class DeltaValue {
    const TypeMeta* _value_schema;   // Schema of the value this delta applies to
    const delta_ops* _ops;           // Kind-specific operations
    DeltaStorage _storage;           // Union of {SetDelta, MapDelta, ListDelta}

public:
    explicit DeltaValue(const TypeMeta* value_schema);

    [[nodiscard]] const TypeMeta* value_schema() const { return _value_schema; }
    [[nodiscard]] TypeKind kind() const { return _value_schema->kind; }

    [[nodiscard]] bool empty() const { return _ops->empty(&_storage); }
    [[nodiscard]] size_t change_count() const { return _ops->change_count(&_storage); }

    void clear() { _ops->clear(&_storage); }
    void apply_to(Value& target) const { _ops->apply(&_storage, target.data()); }

    // View access
    [[nodiscard]] ConstDeltaView const_view() const;
};
```

#### 2.4.4 DeltaView Hierarchy

```cpp
class ConstDeltaView {
protected:
    const void* _data;
    const TypeMeta* _value_schema;
    const delta_ops* _ops;

public:
    [[nodiscard]] bool empty() const { return _ops->empty(_data); }
    [[nodiscard]] size_t change_count() const { return _ops->change_count(_data); }

    [[nodiscard]] ConstSetDeltaView as_set_delta() const;
    [[nodiscard]] ConstMapDeltaView as_map_delta() const;
    [[nodiscard]] ConstListDeltaView as_list_delta() const;
};

class ConstSetDeltaView : public ConstDeltaView {
public:
    [[nodiscard]] ViewRange added() const { return _ops->added(_data); }
    [[nodiscard]] ViewRange removed() const { return _ops->removed(_data); }
    [[nodiscard]] size_t added_count() const { return added().size(); }
    [[nodiscard]] size_t removed_count() const { return removed().size(); }
};

class ConstMapDeltaView : public ConstDeltaView {
public:
    [[nodiscard]] ViewRange added_keys() const { return _ops->added_keys(_data); }
    [[nodiscard]] ViewPairRange added_items() const { return _ops->added_items(_data); }
    [[nodiscard]] ViewRange updated_keys() const { return _ops->updated_keys(_data); }
    [[nodiscard]] ViewPairRange updated_items() const { return _ops->updated_items(_data); }
    [[nodiscard]] ViewRange removed_keys() const { return _ops->removed_keys(_data); }
};

class ConstListDeltaView : public ConstDeltaView {
public:
    [[nodiscard]] ViewPairRange updated_items() const { return _ops->updated_list_items(_data); }
};
```

#### 2.4.5 New Files to Create

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/value/delta_value.h` | DeltaValue class |
| `cpp/include/hgraph/types/value/delta_view.h` | ConstDeltaView and specializations |
| `cpp/include/hgraph/types/value/delta_ops.h` | delta_ops vtable |
| `cpp/include/hgraph/types/value/delta_storage.h` | SetDelta/MapDelta/ListDeltaStorage |

#### 2.4.6 Integration with Existing SetDeltaValue

The existing `SetDeltaValue` in `cpp/include/hgraph/types/value/set_delta_value.h` should be migrated to use the new architecture:

1. Rename to `SetDeltaStorage` (internal)
2. DeltaValue with Set schema wraps SetDeltaStorage
3. Existing users of SetDeltaValue migrate to DeltaValue API

---

### Phase 5: Polish (SlotObserver, Performance)

**Objective**: Optional improvements for extensibility and optimization.

#### 2.5.1 SlotObserver Protocol

```cpp
// slot_observer.h
namespace hgraph::value {

struct SlotObserver {
    virtual ~SlotObserver() = default;
    virtual void on_capacity_change(size_t old_cap, size_t new_cap) = 0;
    virtual void on_insert(size_t slot) = 0;
    virtual void on_erase(size_t slot) = 0;
    virtual void on_clear() = 0;
};

} // namespace hgraph::value
```

Allows extensions (like ValueArray for map values) to react to slot lifecycle events.

#### 2.5.2 Performance Optimizations

Based on profiling after Phases 1-4:

1. **Path pooling**: Reuse ValuePath objects in hot loops
2. **View arena**: Allocate views from arena in iteration-heavy code
3. **Inline delta threshold**: Small deltas (< N changes) use inline storage

---

## Section 3: Test Strategy

### 3.1 C++ Tests (Catch2)

| Phase | Test File | Coverage |
|-------|-----------|----------|
| 1 | `test_type_registry_name.cpp` | Name registration, lookup, builtin types, Python type mapping |
| 2 | `test_view_path.cpp` | Path tracking, owner propagation, path optimization, ViewRange/ViewPairRange |
| 3 | `test_nullable_value.cpp` | has_value(), reset(), emplace(), null Python interop |
| 3 | `test_generation_tracking.cpp` | Slot generations, handle validation, stale detection |
| 4 | `test_delta_value.cpp` | SetDelta, MapDelta, ListDelta creation and access |
| 4 | `test_delta_view.cpp` | Delta view iteration, apply_to |

### 3.2 Python Tests (pytest)

| Phase | Test File | Coverage |
|-------|-----------|----------|
| 1 | `test_type_lookup.py` | `TypeMeta.get("int")`, `from_python_type(int)` |
| 2 | `test_view_path_python.py` | view.path(), view.owner() access from Python |
| 3 | `test_nullable_python.py` | None → null, null → None, has_value() |
| 4 | `test_delta_python.py` | DeltaValue creation, iteration, apply |

### 3.3 Non-Functional Tests

| Category | Tool | Purpose |
|----------|------|---------|
| Memory leaks | ASAN/MSAN | Verify no leaks in registry, delta storage |
| Performance | Google Benchmark | Measure path overhead, delta iteration |
| Thread safety | TSAN | Verify GIL requirements documented correctly |

---

## Section 4: Build Integration

### 4.1 CMakeLists.txt Changes

Add new source files to `cpp/src/cpp/CMakeLists.txt`:

```cmake
set(HGRAPH_SOURCES
    # ... existing sources ...

    # Phase 2: ViewRange
    types/value/view_range.cpp

    # Phase 4: Delta
    types/value/delta_value.cpp
    types/value/delta_ops.cpp
)
```

### 4.2 Python Binding Updates

In `cpp/src/cpp/api/python/py_value.cpp`, add bindings for:

```cpp
void value_register_with_nanobind(nb::module_& m) {
    // ... existing bindings ...

    // Phase 1
    m.def("from_python_type", &TypeRegistry::from_python_type_wrapper);

    // Phase 2
    nb::class_<ViewRange>(m, "ViewRange")
        .def("__iter__", ...)
        .def("__len__", &ViewRange::size);

    nb::class_<ViewPairRange>(m, "ViewPairRange")
        .def("__iter__", ...)
        .def("__len__", &ViewPairRange::size);

    // Phase 3
    // (has_value() already on Value)

    // Phase 4
    nb::class_<DeltaValue>(m, "DeltaValue")
        .def_prop_ro("empty", &DeltaValue::empty)
        .def_prop_ro("change_count", &DeltaValue::change_count)
        .def("apply_to", &DeltaValue::apply_to);
}
```

---

## Section 5: Implementation Order

### Dependency Graph

```
Phase 1: Foundation
├── TypeMeta.name field
├── TypeRegistry._name_cache
├── TypeRegistry.get_by_name()
├── TypeRegistry._python_type_cache ← NEW (A4)
├── TypeRegistry.from_python_type() ← NEW (A4)
└── register_builtin_types()

Phase 2: Views (depends on Phase 1)
├── ViewRange class ← MOVED (A1)
├── ViewPairRange class ← MOVED (A1)
├── ValuePath optimization ← NEW (A6)
├── ConstValueView.owner_, .path_
├── Path propagation in all views
└── Python bindings for path/owner

Phase 3: Nullable (depends on Phase 2)
├── Value._has_value flag
├── Value.reset(), emplace()
├── SetStorage.generations ← MOVED from Phase 5
├── SlotHandle
├── Python None interop
└── Migration warnings (Stage B)

Phase 4: Delta (depends on Phases 2, 3)
├── delta_ops vtable
├── Delta storage classes ← NEW (A2)
├── DeltaValue class
├── DeltaView hierarchy
├── Migrate SetDeltaValue
└── Python bindings

Phase 5: Polish (optional)
├── SlotObserver protocol
└── Performance optimizations
```

### Validation Checkpoints

After each phase, verify:

1. All existing tests pass
2. New phase tests pass
3. Python bindings work
4. No memory leaks (run with ASAN)
5. Documentation updated

### Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Path overhead in hot loops | Profile; add optional no-path views if needed |
| Nullable breaks existing code | Migration strategy (A→B→C stages) |
| Delta storage complexity | Start with SetDelta only; add Map/List incrementally |
| GIL contention with Python cache | Document requirements; cache is read-mostly |

---

## Section 6: Summary of Review Action Items Addressed

| Action Item | Status | Location in Plan |
|-------------|--------|------------------|
| A1: Move ViewRange/ViewPairRange to Phase 2 | ✅ Done | Section 2.2.2 |
| A2: Add Delta Schema Architecture | ✅ Done | Section 2.4.1 |
| A3: Add string interning strategy | ✅ Done | Section 2.1.3 |
| A4: Add from_python_type() | ✅ Done | Section 2.1.4 |
| A5: Add Design Divergences section | ✅ Done | Section 1.4 |
| A6: Add path optimization | ✅ Done | Section 2.2.3 |
| A7: Add nullable migration strategy | ✅ Done | Section 2.3.2 |
| A8: Add non-functional tests | ✅ Done | Section 3.3 |
