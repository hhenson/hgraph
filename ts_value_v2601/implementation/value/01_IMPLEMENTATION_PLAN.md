# Implementation Plan: hgraph Value Type System Migration

## Section 1: Overview

### 1.1 Migration Scope

This implementation plan addresses the migration of the hgraph Value type system from its current C++ implementation to align with the design specifications in `ts_value_v2601/design/`. The migration covers five phases with increasing complexity:

1. **Phase 1 (Foundation)**: Name-based type lookup and TypeMeta name field
2. **Phase 2 (Views)**: Path tracking and owner pointer for View classes
3. **Phase 3 (Nullable)**: Nullable value support with pointer tagging or flag-based approach
4. **Phase 4 (Delta)**: Comprehensive delta support for time-series integration
5. **Phase 5 (Polish)**: Generation tracking, ViewRange, SlotObserver (optional improvements)

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

---

## Section 2: Phase Breakdown

### Phase 1: Foundation (Name-based Type Lookup, TypeMeta Name Field)

**Objective**: Enable type lookup by human-readable name (e.g., `TypeMeta::get("int")`) and add name field to TypeMeta.

#### 2.1.1 Specific Files to Modify

| File | Change Type | Description |
|------|-------------|-------------|
| `cpp/include/hgraph/types/value/type_meta.h` | Modify | Add `name` field to TypeMeta struct |
| `cpp/include/hgraph/types/value/type_registry.h` | Modify | Add name-based cache and lookup methods |
| `cpp/src/cpp/types/value/type_registry.cpp` | Modify | Implement name-based registration and lookup |
| `cpp/src/cpp/api/python/py_value.cpp` | Modify | Expose `TypeMeta::name` property and `get()` method |

#### 2.1.2 Code Changes Required

**TypeMeta struct (type_meta.h)**:

Add `name` field after existing fields:

```cpp
struct TypeMeta {
    // Existing fields...
    size_t size;
    size_t alignment;
    TypeKind kind;
    TypeFlags flags;
    const TypeOps* ops;
    // ... other existing fields ...

    // NEW: Human-readable type name
    const char* name{nullptr};  // Owned by TypeRegistry name storage

    // NEW: Static lookup by name
    static const TypeMeta* get(const std::string& name);

    // NEW: Template lookup (existing pattern, add static method)
    template<typename T>
    static const TypeMeta* get();
};
```

**TypeRegistry class (type_registry.h)**:

Add name-based cache:

```cpp
class TypeRegistry {
public:
    // NEW: Name-based lookup
    const TypeMeta* get_by_name(const std::string& name) const;
    bool has_by_name(const std::string& name) const;

    // NEW: Register with name
    template<typename T>
    const TypeMeta* register_scalar(const std::string& name);

private:
    // NEW: Name-based cache
    std::unordered_map<std::string, const TypeMeta*> _name_cache;
};
```

**type_registry.cpp implementation**:

```cpp
const TypeMeta* TypeRegistry::get_by_name(const std::string& name) const {
    auto it = _name_cache.find(name);
    return (it != _name_cache.end()) ? it->second : nullptr;
}

template<typename T>
const TypeMeta* TypeRegistry::register_scalar(const std::string& name) {
    // Existing registration logic...
    const TypeMeta* meta = register_scalar<T>();

    // Store name and add to cache
    const char* stored_name = store_name(name);
    const_cast<TypeMeta*>(meta)->name = stored_name;
    _name_cache[name] = meta;

    return meta;
}
```

#### 2.1.3 New APIs to Add

| API | Signature | Description |
|-----|-----------|-------------|
| `TypeMeta::get(name)` | `static const TypeMeta* get(const std::string& name)` | Primary name-based lookup |
| `TypeRegistry::get_by_name()` | `const TypeMeta* get_by_name(const std::string& name) const` | Registry method for name lookup |
| `TypeRegistry::register_scalar<T>(name)` | `template<typename T> const TypeMeta* register_scalar(const std::string& name)` | Register scalar with name |
| `TypeMeta::name` property | `const char* name` | Human-readable type name |

#### 2.1.4 Backward Compatibility

- Existing `TypeRegistry::get_scalar<T>()` continues to work (type_index based lookup)
- Existing `scalar_type_meta<T>()` convenience function unchanged
- Name field defaults to nullptr for types registered without names
- New methods are purely additive

#### 2.1.5 Built-in Type Name Registration

Register standard scalar types with canonical names at startup:

```cpp
// In type_registry.cpp or init code
void register_builtin_types() {
    auto& reg = TypeRegistry::instance();
    reg.register_scalar<bool>("bool");
    reg.register_scalar<int64_t>("int");
    reg.register_scalar<double>("float");
    reg.register_scalar<std::string>("str");
    reg.register_scalar<engine_date_t>("date");
    reg.register_scalar<engine_time_t>("datetime");
    reg.register_scalar<engine_time_delta_t>("timedelta");
    // nb::object is special - register as "object"
}
```

---

### Phase 2: Views (Path Tracking, Owner Pointer)

**Objective**: Add owner pointer and path tracking to View classes for navigation context and notification chains.

#### 2.2.1 Specific Files to Modify

| File | Change Type | Description |
|------|-------------|-------------|
| `cpp/include/hgraph/types/value/value_view.h` | Modify | Add owner_ and path_ fields to ConstValueView |
| `cpp/include/hgraph/types/value/indexed_view.h` | Modify | Update all specialized views to propagate owner and path |
| `cpp/include/hgraph/types/value/value.h` | Modify | Update view() methods to set owner |
| `cpp/include/hgraph/types/value/path.h` | Already exists | Existing Path/PathElement implementation |
| `cpp/src/cpp/api/python/py_value.cpp` | Modify | Expose owner() and path() to Python |

#### 2.2.2 Code Changes Required

**ConstValueView class (value_view.h)**:

```cpp
class ConstValueView {
public:
    // Existing constructors need optional owner/path parameters
    ConstValueView(const void* data, const TypeMeta* schema,
                   const void* owner = nullptr, ValuePath path = {}) noexcept
        : _data(data), _schema(schema), _owner(owner), _path(std::move(path)) {}

    // NEW: Owner access
    [[nodiscard]] const void* owner() const noexcept { return _owner; }

    // Template version for typed access
    template<typename Policy = NoCache>
    [[nodiscard]] const Value<Policy>* typed_owner() const {
        return static_cast<const Value<Policy>*>(_owner);
    }

    // NEW: Path access
    [[nodiscard]] const ValuePath& path() const noexcept { return _path; }
    [[nodiscard]] std::string path_string() const { return path_to_string(_path); }

protected:
    const void* _data{nullptr};
    const TypeMeta* _schema{nullptr};
    const void* _owner{nullptr};      // NEW: Pointer to owning Value
    ValuePath _path;                   // NEW: Path from owner root
};
```

**Path propagation in indexed views**:

For each `at()` method that returns a child view, append to path:

```cpp
// In ConstBundleView
[[nodiscard]] ConstValueView at(std::string_view name) const {
    // Existing field lookup...
    size_t offset = /* get field offset */;
    const TypeMeta* field_type = /* get field type */;

    // Create child path
    ValuePath child_path = _path;
    child_path.push_back(PathElement::field(std::string(name)));

    return ConstValueView(
        static_cast<const std::byte*>(_data) + offset,
        field_type,
        _owner,           // Propagate owner
        std::move(child_path)
    );
}

// Similar for index access in lists/tuples
[[nodiscard]] ConstValueView operator[](size_t idx) const {
    // Existing element access...
    ValuePath child_path = _path;
    child_path.push_back(PathElement::index(idx));

    return ConstValueView(element_ptr, element_type, _owner, std::move(child_path));
}
```

#### 2.2.3 Integration with Existing View Hierarchy

The current view hierarchy is:
```
ConstValueView
  ├── ValueView (adds mutable access)
  ├── ConstIndexedView
  │   ├── ConstTupleView
  │   ├── ConstBundleView
  │   ├── ConstListView
  │   └── ... (other specialized views)
  └── ... (mutable variants)
```

All views inherit from ConstValueView, so adding owner_ and path_ to the base class propagates to all derived classes. Each derived class must update its navigation methods (at(), operator[]) to build the path correctly.

#### 2.2.4 Performance Considerations

- **Memory**: ValuePath uses `std::vector<PathElement>` which requires heap allocation. For shallow paths (1-2 levels), consider small vector optimization or pre-allocated capacity.

- **Copy cost**: Views are now larger (additional pointer + vector). Consider:
  - Using `std::string_view` for field names in PathElement when possible
  - Move semantics for path when creating child views
  - Optional path tracking (disable via compile flag for performance-critical code)

- **Optimization**: For hot paths, provide view constructors without path tracking:
  ```cpp
  // Fast path without path tracking
  ConstValueView(const void* data, const TypeMeta* schema, const void* owner) noexcept
      : _data(data), _schema(schema), _owner(owner), _path() {}
  ```

---

### Phase 3: Nullable (Nullable Value Support)

**Objective**: Support null/None values with `std::optional`-style semantics while preserving type information.

#### 2.3.1 Implementation Approach

**Option A: Pointer Tagging (as designed)**

Use the low bit of the TypeMeta pointer to encode null state. TypeMeta is 8-byte aligned, so the low 3 bits are always zero for valid pointers.

```cpp
class Value {
    // Tagged pointer: bit 0 = is_null flag
    uintptr_t _tagged_schema;
    ValueStorage _storage;

    static constexpr uintptr_t NULL_FLAG = 1;

    const TypeMeta* schema_ptr() const {
        return reinterpret_cast<const TypeMeta*>(_tagged_schema & ~NULL_FLAG);
    }

    bool has_value() const { return (_tagged_schema & NULL_FLAG) == 0; }
};
```

**Option B: Separate Flag (simpler)**

Add a boolean flag for null state. Slightly more memory but clearer code.

```cpp
class Value {
    const TypeMeta* _schema;
    ValueStorage _storage;
    bool _has_value{false};

    bool has_value() const { return _has_value; }
};
```

**Recommendation**: Use Option B (separate flag) initially for clarity. Pointer tagging can be added as an optimization later if memory is a concern.

#### 2.3.2 API Additions

```cpp
// Value class additions
class Value {
public:
    // Check if value exists
    [[nodiscard]] bool has_value() const;
    explicit operator bool() const { return has_value(); }

    // Make value null (preserves schema)
    void reset();

    // Construct default value (makes non-null)
    void emplace();

    // Access (throws std::bad_optional_access if null)
    void* data();
    const void* data() const;

    // Unchecked access (assert in debug, UB if null in release)
    void* data_unchecked();
    const void* data_unchecked() const;

    // View access (throws if null)
    ValueView view();
    ConstValueView const_view() const;
};
```

#### 2.3.3 Migration of Existing Code

1. **ValueStorage**: Add `has_value()` method that checks if schema is set AND storage is initialized.

2. **View creation**: `value.view()` should throw `std::bad_optional_access` if `!has_value()`.

3. **Python interop**:
   - `to_python()` on null Value returns `nb::none()`
   - `from_python(nb::none())` calls `reset()` on existing Value

4. **Existing code**: Audit all Value usage sites. Most code assumes values are valid; add explicit checks where null is possible.

---

### Phase 4: Delta (Delta Support for Time-Series)

**Objective**: Implement comprehensive delta support for tracking changes to collections.

#### 2.4.1 New Files to Create

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/value/delta_value.h` | DeltaValue class for explicit delta storage |
| `cpp/include/hgraph/types/value/delta_view.h` | DeltaView interface and specialized views |
| `cpp/include/hgraph/types/value/delta_ops.h` | delta_ops vtable structure |

#### 2.4.2 DeltaValue Design

```cpp
// delta_value.h
namespace hgraph::value {

/**
 * @brief Type-erased owning container for delta changes.
 *
 * Unlike Value which stores complete state, DeltaValue stores
 * the changes (additions, removals, updates) to apply.
 */
class DeltaValue {
    const TypeMeta* _delta_schema;  // Schema for delta representation
    ValueStorage _storage;          // Stores delta data

public:
    // Construct empty delta for a value schema
    explicit DeltaValue(const TypeMeta* value_schema);

    // Schema access
    [[nodiscard]] const TypeMeta* schema() const { return _delta_schema; }
    [[nodiscard]] const TypeMeta* value_schema() const;  // Original value schema

    // View access
    [[nodiscard]] DeltaView view();
    [[nodiscard]] ConstDeltaView const_view() const;

    // State
    [[nodiscard]] bool empty() const;
    void clear();

    // Apply to target
    void apply_to(Value& target) const;

    // Python interop
    [[nodiscard]] nb::object to_python() const;
};

} // namespace hgraph::value
```

#### 2.4.3 DeltaView Interface

```cpp
// delta_view.h
namespace hgraph::value {

/**
 * @brief Type-erased view of delta changes.
 *
 * Base class provides common interface; kind-specific views
 * add specialized accessors.
 */
class ConstDeltaView {
protected:
    const void* _data;
    const TypeMeta* _delta_schema;

public:
    [[nodiscard]] const TypeMeta* schema() const { return _delta_schema; }
    [[nodiscard]] bool empty() const;
    [[nodiscard]] std::string to_string() const;
    [[nodiscard]] nb::object to_python() const;

    // Kind-specific view conversion
    [[nodiscard]] ConstSetDeltaView as_set_delta() const;
    [[nodiscard]] ConstMapDeltaView as_map_delta() const;
    [[nodiscard]] ConstListDeltaView as_list_delta() const;
};

/**
 * @brief Set delta view - access to added/removed elements.
 */
class ConstSetDeltaView : public ConstDeltaView {
public:
    [[nodiscard]] ViewRange added() const;
    [[nodiscard]] ViewRange removed() const;
    [[nodiscard]] size_t added_count() const;
    [[nodiscard]] size_t removed_count() const;
};

/**
 * @brief Map delta view - access to added/updated/removed entries.
 */
class ConstMapDeltaView : public ConstDeltaView {
public:
    [[nodiscard]] ViewRange added_keys() const;
    [[nodiscard]] ViewRange updated_keys() const;
    [[nodiscard]] ViewRange removed_keys() const;
    [[nodiscard]] ViewPairRange added_items() const;
    [[nodiscard]] ViewPairRange updated_items() const;
};

/**
 * @brief List delta view - access to updated indices.
 */
class ConstListDeltaView : public ConstDeltaView {
public:
    [[nodiscard]] ViewRange updated_indices() const;
    [[nodiscard]] ViewPairRange updated_items() const;  // index -> value pairs
};

} // namespace hgraph::value
```

#### 2.4.4 Integration with Time-Series Layer

The existing `SetDeltaValue` in `cpp/include/hgraph/types/value/set_delta_value.h` provides a starting point. The new delta system should:

1. Generalize SetDeltaValue to the DeltaValue class hierarchy
2. Support Map and List deltas in addition to Set
3. Integrate with TSValue for computed delta views (not stored)
4. Provide apply_delta() method on Value for applying changes

---

### Phase 5: Polish (Generation Tracking, ViewRange, SlotObserver)

**Objective**: Optional improvements for robustness and API completeness.

#### 2.5.1 Generation Tracking for Set/Map

Add generation counters to SetStorage for stale reference detection:

```cpp
struct SetStorage {
    // Existing fields...
    std::vector<std::byte> elements;
    size_t element_count{0};
    std::unique_ptr<IndexSet> index_set;
    const TypeMeta* element_type{nullptr};

    // NEW: Generation tracking
    std::vector<uint32_t> generations;  // Per-slot generation counters
    uint32_t current_generation{1};     // Increments on modification

    // NEW: Handle validation
    bool is_valid_handle(size_t slot, uint32_t gen) const {
        return slot < generations.size() && generations[slot] == gen && gen > 0;
    }
};
```

#### 2.5.2 ViewRange and ViewPairRange

Unified iterator types for all iteration patterns:

```cpp
// view_range.h
namespace hgraph::value {

/**
 * @brief Range yielding single Views per element.
 * Used for: keys(), values(), elements()
 */
class ViewRange {
public:
    class iterator {
        // Implementation depends on source type
    public:
        using value_type = ConstValueView;
        ConstValueView operator*() const;
        iterator& operator++();
        bool operator!=(const iterator& other) const;
    };

    iterator begin() const;
    iterator end() const;

    // Range interface
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const;
};

/**
 * @brief Range yielding pairs of Views (key/index + value).
 * Used for: items(), entries()
 */
class ViewPairRange {
public:
    class iterator {
    public:
        using value_type = std::pair<ConstValueView, ConstValueView>;
        std::pair<ConstValueView, ConstValueView> operator*() const;
        iterator& operator++();
        bool operator!=(const iterator& other) const;
    };

    iterator begin() const;
    iterator end() const;
};

} // namespace hgraph::value
```

#### 2.5.3 SlotObserver Protocol

Extension point for Set/Map slot lifecycle events:

```cpp
// slot_observer.h
namespace hgraph::value {

/**
 * @brief Observer interface for slot lifecycle events.
 *
 * Allows extensions (like ValueArray for maps) to react to
 * slot insertions, removals, and resizes.
 */
struct SlotObserver {
    virtual ~SlotObserver() = default;
    virtual void on_capacity(size_t old_cap, size_t new_cap) = 0;
    virtual void on_insert(size_t slot) = 0;
    virtual void on_erase(size_t slot) = 0;
    virtual void on_clear() = 0;
};

} // namespace hgraph::value
```

---

## Section 3: File-by-File Changes

### 3.1 `cpp/include/hgraph/types/value/type_meta.h`

**Current State**: Defines TypeKind, TypeFlags, TypeOps, TypeMeta, ScalarOps, and compute_scalar_flags template. TypeMeta has no `name` field.

**Required Changes**:

1. Add `const char* name{nullptr}` field to TypeMeta struct (line ~240)
2. Add `static const TypeMeta* get(const std::string& name)` method declaration
3. Add `template<typename T> static const TypeMeta* get()` method declaration
4. Optionally add `name()` accessor method returning std::string_view

**New Code**:
```cpp
// In TypeMeta struct, after fixed_size field
const char* name{nullptr};  ///< Human-readable type name (owned by TypeRegistry)

// Static lookup methods
static const TypeMeta* get(const std::string& name);

template<typename T>
static const TypeMeta* get() {
    return TypeRegistry::instance().get_scalar<T>();
}

// Accessor
[[nodiscard]] std::string_view type_name() const noexcept {
    return name ? std::string_view(name) : std::string_view("<unnamed>");
}
```

**Dependencies**: None for struct changes; static methods depend on TypeRegistry being fully defined.

### 3.2 `cpp/include/hgraph/types/value/type_registry.h`

**Current State**: Singleton TypeRegistry with type_index-based scalar lookup, builders for composite types, named bundle lookup.

**Required Changes**:

1. Add `_name_cache` member variable
2. Add `get_by_name()` method
3. Add `has_by_name()` method
4. Update `register_scalar<T>()` to optionally accept name parameter
5. Add overloaded `register_scalar<T>(name)` method

**New Code**:
```cpp
// In private section
std::unordered_map<std::string, const TypeMeta*> _name_cache;

// Public methods
[[nodiscard]] const TypeMeta* get_by_name(const std::string& name) const {
    auto it = _name_cache.find(name);
    return (it != _name_cache.end()) ? it->second : nullptr;
}

[[nodiscard]] bool has_by_name(const std::string& name) const {
    return _name_cache.count(name) > 0;
}

template<typename T>
const TypeMeta* register_scalar(const std::string& name);
```

**Dependencies**: TypeMeta must have `name` field.

### 3.3 `cpp/include/hgraph/types/value/value_view.h`

**Current State**: ConstValueView with `_data` and `_schema` pointers. ValueView extends with `_mutable_data` and `_root`. No path tracking.

**Required Changes**:

1. Add `_owner` pointer to ConstValueView (currently only in ValueView as `_root`)
2. Add `_path` member (ValuePath type from path.h)
3. Update constructors to accept optional owner and path
4. Add `owner()` and `path()` accessor methods
5. Update all derived view classes to propagate owner/path

**New Code**:
```cpp
// In ConstValueView protected section
const void* _owner{nullptr};
ValuePath _path;

// Constructors
ConstValueView(const void* data, const TypeMeta* schema,
               const void* owner = nullptr, ValuePath path = {}) noexcept
    : _data(data), _schema(schema), _owner(owner), _path(std::move(path)) {}

// Public accessors
[[nodiscard]] const void* owner() const noexcept { return _owner; }
[[nodiscard]] const ValuePath& path() const noexcept { return _path; }
[[nodiscard]] std::string path_string() const;
```

**Dependencies**: Requires path.h include.

### 3.4 `cpp/include/hgraph/types/value/value.h`

**Current State**: Template Value class with Policy pattern, owns ValueStorage, provides typed access.

**Required Changes**:

1. Add nullable support (Phase 3)
2. Update `view()` methods to pass `this` as owner
3. Add `has_value()`, `reset()`, `emplace()` methods (Phase 3)

**New Code** (Phase 2 - owner propagation):
```cpp
// In view() method
[[nodiscard]] ValueView view() {
    return ValueView(_storage.data(), _storage.schema(), this);
}

[[nodiscard]] ConstValueView const_view() const {
    return ConstValueView(_storage.data(), _storage.schema(), this);
}
```

**New Code** (Phase 3 - nullable):
```cpp
// Add member
bool _has_value{false};

// Methods
[[nodiscard]] bool has_value() const noexcept { return _has_value; }
explicit operator bool() const noexcept { return has_value(); }

void reset() {
    if (_has_value) {
        _storage.destruct();
        _has_value = false;
    }
}

void emplace() {
    if (!_has_value) {
        _storage.construct(/* schema */);
        _has_value = true;
    }
}
```

**Dependencies**: ValueStorage, TypeMeta.

### 3.5 `cpp/include/hgraph/types/value/value_storage.h`

**Current State**: SBO storage with 24-byte inline buffer, manages construction/destruction.

**Required Changes** (minimal for Phase 1-2):

1. Ensure `schema()` accessor exists
2. No changes needed for Phase 1-2

**Phase 3 Changes**:
1. Add `has_value()` method that returns true if schema is set

**Dependencies**: TypeMeta, TypeOps.

### 3.6 `cpp/include/hgraph/types/value/composite_ops.h`

**Current State**: SetStorage and MapStorage implementations with ankerl hash set, no generation tracking.

**Required Changes** (Phase 5 only):

1. Add `generations` vector to SetStorage
2. Add `current_generation` counter
3. Update insert/erase to manage generations
4. Add `is_valid_handle()` method
5. Optionally add SlotObserver support

**Dependencies**: TypeMeta, TypeOps.

### 3.7 `cpp/src/cpp/types/value/type_registry.cpp`

**Current State**: Implements TypeRegistry singleton, builder build() methods.

**Required Changes**:

1. Implement `register_scalar<T>(name)` specialization
2. Initialize `_name_cache` in singleton
3. Register builtin types with names on first use
4. Implement `TypeMeta::get(name)` static method

**New Code**:
```cpp
template<typename T>
const TypeMeta* TypeRegistry::register_scalar(const std::string& name) {
    // First, do standard registration
    const TypeMeta* meta = register_scalar<T>();

    // Then store name and add to cache
    if (!name.empty() && meta->name == nullptr) {
        const char* stored = store_name(name);
        const_cast<TypeMeta*>(meta)->name = stored;
        _name_cache[name] = meta;
    }

    return meta;
}

// Static method implementation
const TypeMeta* TypeMeta::get(const std::string& name) {
    return TypeRegistry::instance().get_by_name(name);
}

// Builtin registration (call during module init)
void register_builtin_types() {
    auto& reg = TypeRegistry::instance();
    reg.register_scalar<bool>("bool");
    reg.register_scalar<int64_t>("int");
    reg.register_scalar<double>("float");
    reg.register_scalar<std::string>("str");
    reg.register_scalar<engine_date_t>("date");
    reg.register_scalar<engine_time_t>("datetime");
    reg.register_scalar<engine_time_delta_t>("timedelta");
}
```

**Dependencies**: TypeMeta, TypeRegistry header.

### 3.8 `cpp/src/cpp/api/python/py_value.cpp`

**Current State**: Python bindings for TypeMeta, TypeRegistry, Value, View classes.

**Required Changes**:

1. Expose `TypeMeta::name` property
2. Expose `TypeMeta::get(name)` static method
3. Expose `ConstValueView::owner()` and `path()` methods
4. Expose `Value::has_value()`, `reset()`, `emplace()` methods (Phase 3)
5. Expose DeltaValue and DeltaView classes (Phase 4)

**New Code**:
```cpp
// In register_type_meta function
.def_prop_ro("name", [](const TypeMeta& self) {
    return self.name ? std::string(self.name) : std::string();
}, "Get the human-readable type name")
.def_static("get", &TypeMeta::get, "name"_a,
    nb::rv_policy::reference,
    "Get TypeMeta by name (returns None if not found)")

// In register_const_value_view function
.def("owner", [](const ConstValueView& self) -> uintptr_t {
    return reinterpret_cast<uintptr_t>(self.owner());
}, "Get the owning Value pointer (as integer for debugging)")
.def("path", &ConstValueView::path,
    "Get the path from owner root to this view")
.def("path_string", &ConstValueView::path_string,
    "Get the path as a string")
```

**Dependencies**: Updated TypeMeta, ConstValueView headers.

---

## Section 4: New Files to Create

### 4.1 `cpp/include/hgraph/types/value/delta_value.h`

**Purpose**: Owning container for delta (change) information.

**Key Classes/Functions**:
- `DeltaValue` - Type-erased owning delta container
- Factory methods for creating deltas for different collection types

**Dependencies**: `type_meta.h`, `value_storage.h`, `delta_view.h`

### 4.2 `cpp/include/hgraph/types/value/delta_view.h`

**Purpose**: Non-owning views into delta data.

**Key Classes/Functions**:
- `ConstDeltaView` - Base class for delta views
- `SetDeltaView`, `MapDeltaView`, `ListDeltaView` - Kind-specific views
- Mutable variants for building deltas

**Dependencies**: `type_meta.h`, `value_view.h`, `view_range.h`

### 4.3 `cpp/include/hgraph/types/value/delta_ops.h`

**Purpose**: Operations vtable for delta types.

**Key Classes/Functions**:
- `delta_ops` struct with function pointers for delta operations
- `set_delta_ops`, `map_delta_ops`, `list_delta_ops` kind-specific ops

**Dependencies**: `type_meta.h`, `view_range.h`

### 4.4 `cpp/include/hgraph/types/value/view_range.h`

**Purpose**: Unified iterator types for all iteration patterns.

**Key Classes/Functions**:
- `ViewRange` - Single-value iterator (keys, values, elements)
- `ViewPairRange` - Key-value pair iterator (items, entries)
- Iterator implementations for each storage type

**Dependencies**: `value_view.h`

### 4.5 `cpp/include/hgraph/types/value/slot_observer.h` (Phase 5)

**Purpose**: Extension point for Set/Map slot lifecycle events.

**Key Classes/Functions**:
- `SlotObserver` interface

**Dependencies**: None (pure interface)

---

## Section 5: Test Strategy

### 5.1 Phase 1 Tests (Foundation)

**C++ Tests** (`cpp/tests/types/value/test_type_registry.cpp`):
```cpp
TEST_CASE("TypeMeta name-based lookup", "[type_registry]") {
    SECTION("builtin types have names") {
        auto* int_meta = TypeMeta::get("int");
        REQUIRE(int_meta != nullptr);
        REQUIRE(int_meta->name == "int");
    }

    SECTION("lookup by name returns same as template") {
        auto* by_name = TypeMeta::get("int");
        auto* by_type = scalar_type_meta<int64_t>();
        REQUIRE(by_name == by_type);
    }

    SECTION("unknown name returns nullptr") {
        REQUIRE(TypeMeta::get("nonexistent") == nullptr);
    }
}
```

**Python Tests** (`hgraph_unit_tests/_types/_value/test_type_registry.py`):
```python
def test_type_meta_get_by_name():
    """TypeMeta.get() retrieves type by name."""
    from hgraph._hgraph.value import TypeMeta

    int_meta = TypeMeta.get("int")
    assert int_meta is not None
    assert int_meta.name == "int"

def test_type_meta_unknown_name():
    """TypeMeta.get() returns None for unknown names."""
    from hgraph._hgraph.value import TypeMeta

    assert TypeMeta.get("nonexistent") is None
```

### 5.2 Phase 2 Tests (Views)

**C++ Tests** (`cpp/tests/types/value/test_value_view_path.cpp`):
```cpp
TEST_CASE("View path tracking", "[value_view]") {
    auto& reg = TypeRegistry::instance();
    auto* bundle_type = reg.bundle("Point")
        .field("x", scalar_type_meta<double>())
        .field("y", scalar_type_meta<double>())
        .build();

    Value<NoCache> point(bundle_type);

    SECTION("root view has empty path") {
        auto view = point.view();
        REQUIRE(view.path().empty());
        REQUIRE(view.owner() == &point);
    }

    SECTION("field access builds path") {
        auto x_view = point.view().as_bundle().at("x");
        REQUIRE(x_view.path().size() == 1);
        REQUIRE(x_view.path()[0].is_field());
        REQUIRE(x_view.path()[0].name() == "x");
        REQUIRE(x_view.owner() == &point);
    }
}
```

**Python Tests** (`hgraph_unit_tests/_types/_value/test_value_path.py`):
```python
def test_view_path_tracking():
    """Views track their path from root."""
    from hgraph._hgraph.value import TypeRegistry, PlainValue

    reg = TypeRegistry.instance()
    point_type = reg.bundle("TestPoint").field("x", reg.get_double_type()).build()

    point = PlainValue(point_type)
    x_view = point.view().as_bundle().at("x")

    assert x_view.path_string() == "x"
```

### 5.3 Phase 3 Tests (Nullable)

**C++ Tests** (`cpp/tests/types/value/test_value_nullable.cpp`):
```cpp
TEST_CASE("Nullable Value", "[value]") {
    SECTION("new Value starts null") {
        Value<NoCache> v(scalar_type_meta<int64_t>());
        REQUIRE_FALSE(v.has_value());
    }

    SECTION("emplace makes non-null") {
        Value<NoCache> v(scalar_type_meta<int64_t>());
        v.emplace();
        REQUIRE(v.has_value());
    }

    SECTION("reset makes null") {
        Value<NoCache> v(scalar_type_meta<int64_t>());
        v.emplace();
        v.reset();
        REQUIRE_FALSE(v.has_value());
    }

    SECTION("to_python returns None when null") {
        Value<NoCache> v(scalar_type_meta<int64_t>());
        nb::object py = v.const_view().to_python();
        REQUIRE(py.is_none());
    }
}
```

**Python Tests** (`hgraph_unit_tests/_types/_value/test_value_nullable.py`):
```python
def test_value_nullable():
    """Value supports null state."""
    from hgraph._hgraph.value import PlainValue, scalar_type_meta_int64

    v = PlainValue(scalar_type_meta_int64())
    assert not v.has_value()

    v.emplace()
    assert v.has_value()

    v.reset()
    assert not v.has_value()
```

### 5.4 Phase 4 Tests (Delta)

**C++ Tests** (`cpp/tests/types/value/test_delta_value.cpp`):
```cpp
TEST_CASE("SetDeltaValue", "[delta]") {
    auto* set_type = TypeRegistry::instance()
        .set(scalar_type_meta<int64_t>())
        .build();

    SECTION("empty delta") {
        DeltaValue delta(set_type);
        REQUIRE(delta.empty());
    }

    SECTION("mark added/removed") {
        DeltaValue delta(set_type);
        auto view = delta.view().as_set_delta();
        // ... test marking and iteration
    }
}
```

### 5.5 Phase 5 Tests (Polish)

**C++ Tests** (`cpp/tests/types/value/test_generation_tracking.cpp`):
```cpp
TEST_CASE("SetStorage generation tracking", "[set_storage]") {
    auto* set_type = TypeRegistry::instance()
        .set(scalar_type_meta<int64_t>())
        .build();

    PlainValue set_val(set_type);
    auto set_view = set_val.view().as_set();

    SECTION("handle becomes invalid after removal") {
        int64_t key = 42;
        auto [slot, gen] = set_view.insert_with_handle(key);
        REQUIRE(set_view.is_valid_handle(slot, gen));

        set_view.remove(key);
        REQUIRE_FALSE(set_view.is_valid_handle(slot, gen));
    }
}
```

---

## Section 6: Build and Integration

### 6.1 CMakeLists.txt Changes

No changes required for Phase 1-3 (modifying existing files only).

For Phase 4 (Delta), add new source files:
```cmake
# In cpp/src/cpp/CMakeLists.txt, add to HGRAPH_SOURCES:
types/value/delta_value.cpp
types/value/delta_view.cpp
```

### 6.2 Python Binding Updates

All Python binding changes are in `cpp/src/cpp/api/python/py_value.cpp`:

1. **Phase 1**: Expose `TypeMeta.name` property and `TypeMeta.get()` static method
2. **Phase 2**: Expose `ConstValueView.owner()`, `path()`, `path_string()`
3. **Phase 3**: Expose `Value.has_value()`, `reset()`, `emplace()`
4. **Phase 4**: Register new DeltaValue and DeltaView classes

### 6.3 Backward Compatibility Notes

1. **TypeMeta.name**: Defaults to nullptr for types registered without names. Existing code continues to work.

2. **View path/owner**: Defaults to nullptr/empty. Existing code that doesn't use these fields is unaffected.

3. **Value nullable**: This is a behavioral change. Audit existing code that assumes Values are always valid. Consider migration period where `has_value()` returns true by default.

4. **Python API**: All new methods are additive. Existing Python code continues to work.

---

## Section 7: Implementation Order and Dependencies

### 7.1 Dependency Graph

```
Phase 1: Foundation
├── TypeMeta.name field
├── TypeRegistry.name_cache
└── TypeMeta::get(name) static method

Phase 2: Views (depends on path.h already existing)
├── ConstValueView.owner_
├── ConstValueView.path_
├── Path propagation in indexed views
└── Python bindings

Phase 3: Nullable (independent)
├── Value.has_value_ flag
├── Value.reset(), emplace()
├── Null handling in to_python/from_python
└── Python bindings

Phase 4: Delta (independent, but benefits from Phase 2 for paths)
├── delta_ops vtable
├── DeltaValue class
├── DeltaView hierarchy
├── Apply delta to Value
└── Python bindings

Phase 5: Polish (depends on Phase 4 for some features)
├── Generation tracking (independent)
├── ViewRange/ViewPairRange (benefits Phase 4)
└── SlotObserver (benefits Phase 4)
```

### 7.2 Recommended Implementation Order

1. **Week 1**: Phase 1 (Foundation)
   - Add TypeMeta.name field
   - Implement name-based registry cache
   - Register builtin types
   - Python bindings
   - Tests

2. **Week 2**: Phase 2 (Views)
   - Add owner/path to ConstValueView
   - Update indexed views for path propagation
   - Python bindings
   - Tests

3. **Week 3**: Phase 3 (Nullable)
   - Add has_value flag to Value
   - Implement reset/emplace
   - Update Python interop
   - Tests

4. **Week 4-5**: Phase 4 (Delta)
   - Design delta storage format per type
   - Implement DeltaValue and views
   - Integrate with existing SetDeltaValue
   - Python bindings
   - Tests

5. **Week 6** (Optional): Phase 5 (Polish)
   - Generation tracking
   - ViewRange/ViewPairRange
   - SlotObserver

### 7.3 Validation Checkpoints

After each phase:

1. **Compile check**: `cmake --build cmake-build-debug`
2. **Unit tests**: `uv run pytest hgraph_unit_tests/_types/_value/ -v`
3. **Full test suite**: `uv run pytest hgraph_unit_tests -v`
4. **C++ tests** (if using Catch2): `./cmake-build-debug/cpp/tests/hgraph_tests`

### 7.4 Risk Mitigation

1. **Phase 2 (Views)**: Path tracking adds memory/copy overhead. Profile before/after. Consider compile-time flag to disable.

2. **Phase 3 (Nullable)**: Breaking change if existing code assumes non-null. Use deprecation warnings.

3. **Phase 4 (Delta)**: Complex new subsystem. Start with Set delta (existing implementation), then Map, then List.

---

## Critical Files for Implementation

List of the most critical files for implementing this plan:

1. **`cpp/include/hgraph/types/value/type_meta.h`** - Add name field, static lookup method. Foundation for Phase 1.

2. **`cpp/include/hgraph/types/value/type_registry.h`** - Add name-based cache and lookup. Core of Phase 1.

3. **`cpp/include/hgraph/types/value/value_view.h`** - Add owner pointer and path tracking. Core of Phase 2.

4. **`cpp/include/hgraph/types/value/value.h`** - Add nullable support. Core of Phase 3.

5. **`cpp/src/cpp/api/python/py_value.cpp`** - Python bindings for all phases. Required for Python API parity.
