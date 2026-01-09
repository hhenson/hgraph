# Phase 6-7: Python Wrapper Migration Design Document

**Date**: 2026-01-09
**Status**: Draft
**Phase**: 6-7 of the Value/TSValue Migration Plan
**Related**:
- `Value_TSValue_MIGRATION_PLAN.md`
- `TSValue_DESIGN.md`
- `Phase5_Hierarchical_Subscriptions_DESIGN.md`

---

## 1. Summary

This document describes the migration strategy for integrating the new TSValue/TSInput infrastructure into the Python API wrappers (`cpp/src/cpp/api/python/`). The migration replaces the legacy `TimeSeriesInput`/`TimeSeriesOutput` types with `TSValue`, `TSView`, `TSInputRoot`, and `TSLink` while maintaining full API compatibility with the existing Python-facing interface.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **API Stability**: The public Python-facing API MUST NOT change. All existing method signatures, properties, and behaviors must be preserved exactly.

2. **Full Replacement**: No fallback or backwards compatibility mode. The new TSValue-based implementation fully replaces the legacy types.

3. **Behavioral Parity**: All existing behaviors must be preserved:
   - `value`, `delta_value` properties
   - `modified()`, `valid()`, `all_valid()` semantics
   - `last_modified_time` tracking
   - Input/output binding and subscription semantics

4. **Performance Preservation**: The new implementation should not degrade performance compared to the legacy system.

5. **Unified Storage Model**: Nodes should own their storage as `TSValue` objects, with Python wrappers providing views into this storage.

### 2.2 Non-Goals

1. **New Python API Features**: This migration focuses on internal implementation changes, not new user-facing features.

2. **Python Implementation Changes**: Changes to `hgraph/_impl/` Python code are out of scope unless required for compatibility.

3. **Builder Redesign**: Builders will be modified minimally to create TSValue-based types.

4. **TypeMeta Redesign**: The `TSMeta` and `value::TypeMeta` systems remain unchanged.

---

## 3. Background

### 3.1 Current Architecture

The current system uses a dual-storage architecture:

1. **Legacy Types** (`TimeSeriesInput`, `TimeSeriesOutput` hierarchy):
   - Owned via `shared_ptr` through the builder system
   - Implement the full `TimeSeriesType` interface
   - Each concrete type (e.g., `TimeSeriesValueOutput`, `TimeSeriesBundleOutput`) manages its own storage

2. **Python Wrappers** (`PyTimeSeriesType` hierarchy):
   - Hold an `ApiPtr<TimeSeriesType>` wrapping the shared_ptr
   - Delegate all operations to the underlying legacy type
   - Created via `wrap_output()` / `wrap_input()` factory functions

3. **View-based Mode** (partial implementation):
   - `PyTimeSeriesOutput` and `PyTimeSeriesInput` can optionally hold a `TSView` / `TSMutableView`
   - Used when creating wrappers from Node's internal TSValue storage
   - Currently only supported for scalar TS types

### 3.2 New TSValue Architecture

The new system consolidates storage in `TSValue`:

1. **TSValue** (`cpp/include/hgraph/types/time_series/ts_value.h`):
   - Owns data storage via `value::Value<CombinedPolicy<...>>`
   - Owns overlay for hierarchical modification tracking
   - Provides view creation methods (`view()`, `mutable_view()`, `bundle_view()`)

2. **TSView / TSMutableView** (`cpp/include/hgraph/types/time_series/ts_view.h`):
   - Non-owning views into TSValue storage
   - Provide type-safe access with navigation methods
   - Support cast methods: `as_bundle()`, `as_list()`, `as_dict()`, `as_set()`

3. **TSInputRoot** (`cpp/include/hgraph/types/time_series/ts_input_root.h`):
   - Wraps a TSValue (always TSB) with link support
   - Manages field bindings via `TSLink`
   - Provides transparent navigation through links

4. **TSLink** (`cpp/include/hgraph/types/time_series/ts_link.h`):
   - Symbolic link to an external output's TSValue
   - Manages subscription (active/passive) state
   - Implements `Notifiable` for delegation to owning node

### 3.3 Node Integration (Already Implemented)

The `Node` class already has:

```cpp
// In node.h
std::optional<TSValue> _ts_input;            // Always TSB type
std::optional<TSValue> _ts_output;           // Any TS type
std::optional<TSValue> _ts_error_output;     // Error output path
std::optional<TSValue> _ts_recordable_state; // Recordable state path

// View access methods
[[nodiscard]] TSBView input_view();
[[nodiscard]] TSMutableView output_view();
[[nodiscard]] TSMutableView error_output_view();
[[nodiscard]] TSMutableView state_view();
```

---

## 4. Design Overview

### 4.1 Migration Approach

The migration follows a "view-first" approach:

1. **Node owns storage**: Nodes create and own `TSValue` objects for inputs/outputs
2. **Wrappers hold views**: Python wrappers hold `TSView` / `TSMutableView` into the node's storage
3. **Legacy fallback removed**: The `ApiPtr<TimeSeriesType>` path is deprecated and removed
4. **Factory creates views**: `wrap_output_view()` / `wrap_input_view()` become the primary wrapping mechanism

### 4.2 Storage Ownership Model

```
Before (Legacy):
  Node
   |
   +-- shared_ptr<TimeSeriesOutput> _output  (owns storage)
   |
   +-- shared_ptr<TimeSeriesInput> _input    (owns storage)

After (TSValue/TSInputRoot):
  Node
   |
   +-- optional<TSValue> _ts_output  (owns storage)
   |       |
   |       +-- overlay (modification tracking)
   |       +-- value (data storage)
   |
   +-- optional<TSInputRoot> _ts_input   (input wrapper with link support)
           |
           +-- _value: TSValue  (always TSB type)
           |       +-- LinkSupport (for peered children)
           |       |       +-- child_links[]  (TSLink to external outputs)
           |       |       +-- child_values[] (nested TSValue for non-peered composites)
           |       +-- overlay (modification tracking)
           |       +-- value (data storage)
           +-- _node: Node*  (back-pointer for notifications)
           +-- _active: bool (subscription state)
```

**Key Design Point**: Node holds `TSInputRoot` for input (not raw TSValue). TSInputRoot:
- Wraps a TSValue internally (always TSB type)
- Provides link support for transparent navigation
- Manages active/passive subscription state
- Provides field binding methods

### 4.3 Wrapper Lifecycle

```
1. Node construction with TSMeta schemas
   |
   +-> Node creates TSValue for input (TSB type)
   +-> Node creates TSValue for output

2. Builder wires inputs/outputs
   |
   +-> For peered inputs: create TSLink to output's TSValue
   +-> For non-peered inputs: data stored in input's TSValue

3. Python wrapper creation
   |
   +-> wrap_output_view(node.output_view()) -> PyTimeSeriesXxxOutput
   +-> wrap_input_view(input_root.field("name").view()) -> PyTimeSeriesXxxInput

4. Node evaluation
   |
   +-> Node accesses via view methods
   +-> Python accesses via wrapper (holds view)
   +-> View delegates to underlying TSValue storage
```

---

## 5. Detailed Design

### 5.1 Type Mapping

| Legacy Type | New Type (Storage) | View Type | Wrapper |
|-------------|-------------------|-----------|---------|
| `TimeSeriesValueOutput` | `TSValue` | `TSMutableView` | `PyTimeSeriesValueOutput` |
| `TimeSeriesValueInput` | TSLink to TSValue | `TSView` | `PyTimeSeriesValueInput` |
| `TimeSeriesBundleOutput` | `TSValue` (TSB schema) | `TSBView` (mutable) | `PyTimeSeriesBundleOutput` |
| `TimeSeriesBundleInput` | `TSInputRoot` | `TSBView` | `PyTimeSeriesBundleInput` |
| `TimeSeriesListOutput` | `TSValue` (TSL schema) | `TSLView` (mutable) | `PyTimeSeriesListOutput` |
| `TimeSeriesListInput` | TSLink/TSValue | `TSLView` | `PyTimeSeriesListInput` |
| `TimeSeriesDictOutput` | `TSValue` (TSD schema) | `TSDView` (mutable) | `PyTimeSeriesDictOutput` |
| `TimeSeriesDictInput` | TSLink/TSValue | `TSDView` | `PyTimeSeriesDictInput` |
| `TimeSeriesSetOutput` | `TSValue` (TSS schema) | `TSSView` (mutable) | `PyTimeSeriesSetOutput` |
| `TimeSeriesSetInput` | TSLink/TSValue | `TSSView` | `PyTimeSeriesSetInput` |

### 5.2 Python Wrapper Changes

#### 5.2.1 PyTimeSeriesType (Base Class)

Current state: Has optional `_impl` (ApiPtr) and optional `_view` (TSView).

Target state: Remove `_impl`, always use view-based access.

```cpp
struct PyTimeSeriesType {
protected:
    // REMOVE: api_ptr _impl;

    // KEEP: View-based storage only
    // (Derived classes hold the appropriate view type)

public:
    // These methods delegate to derived class view methods:
    [[nodiscard]] virtual nb::object value() const = 0;
    [[nodiscard]] virtual nb::object delta_value() const = 0;
    [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;
    [[nodiscard]] virtual nb::bool_ modified() const = 0;
    [[nodiscard]] virtual nb::bool_ valid() const = 0;
    [[nodiscard]] virtual nb::bool_ all_valid() const = 0;

    // Graph navigation (needs Node* back-reference):
    [[nodiscard]] virtual nb::object owning_node() const = 0;
    [[nodiscard]] virtual nb::object owning_graph() const = 0;
};
```

#### 5.2.2 PyTimeSeriesOutput (Base Output)

```cpp
struct PyTimeSeriesOutput : PyTimeSeriesType {
protected:
    TSMutableView _view;  // Held by VALUE - lightweight view into node's TSValue
    // NOTE: No separate Node* - view provides owning_node() via container/root

public:
    // Construct with view by value - wrapper is a lightweight facade
    explicit PyTimeSeriesOutput(TSMutableView view) : _view(view) {}

    // Value access
    [[nodiscard]] nb::object value() const override {
        return _view.to_python();
    }

    [[nodiscard]] nb::object delta_value() const override {
        // Implementation depends on type - see specialized views
        return _view.to_python();  // Default: same as value
    }

    // State queries
    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _view.last_modified_time();
    }

    [[nodiscard]] nb::bool_ modified() const override {
        Node* node = _view.owning_node();
        if (!node) return nb::bool_(false);
        return nb::bool_(_view.modified_at(*node->cached_evaluation_time_ptr()));
    }

    [[nodiscard]] nb::bool_ valid() const override {
        return nb::bool_(_view.ts_valid());
    }

    [[nodiscard]] nb::bool_ all_valid() const override {
        // For scalar: same as valid
        // For composites: override in derived class
        return valid();
    }

    // Mutation operations
    void set_value(nb::object value) {
        _view.from_python(value);
        // Note: from_python should update modification time via overlay
    }

    void apply_result(nb::object value) {
        if (!value.is_none()) {
            set_value(std::move(value));
        }
    }

    void clear() {
        _view.invalidate_ts();
    }

    void invalidate() {
        _view.invalidate_ts();
    }

    // Copy operations
    void copy_from_output(const PyTimeSeriesOutput& output) {
        _view.copy_from(output._view);
    }

    void copy_from_input(const PyTimeSeriesInput& input);

    // Graph navigation - delegate to view
    [[nodiscard]] nb::object owning_node() const override {
        Node* node = _view.owning_node();
        return node ? wrap_node(node) : nb::none();
    }

    [[nodiscard]] nb::object owning_graph() const override {
        Node* node = _view.owning_node();
        return node ? node->owning_graph_py() : nb::none();
    }
};
```

#### 5.2.3 PyTimeSeriesInput (Base Input)

```cpp
struct PyTimeSeriesInput : PyTimeSeriesType {
protected:
    TSView _view;  // Held by VALUE - lightweight view (may follow links)
    // NOTE: No separate Node* - view provides owning_node() via container/root
    // For peered inputs, the view points into the linked output's TSValue
    // For non-peered inputs, the view points into the local TSValue

public:
    // Construct with view by value - wrapper is a lightweight facade
    explicit PyTimeSeriesInput(TSView view) : _view(view) {}

    // Value access
    [[nodiscard]] nb::object value() const override {
        return _view.to_python();
    }

    // State queries (delegate to view which may follow links)
    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _view.last_modified_time();
    }

    [[nodiscard]] nb::bool_ modified() const override {
        Node* node = _view.owning_node();
        if (!node) return nb::bool_(false);
        return nb::bool_(_view.modified_at(*node->cached_evaluation_time_ptr()));
    }

    [[nodiscard]] nb::bool_ valid() const override {
        return nb::bool_(_view.ts_valid());
    }

    // Graph navigation - delegate to view
    [[nodiscard]] nb::object owning_node() const override {
        Node* node = _view.owning_node();
        return node ? wrap_node(node) : nb::none();
    }

    [[nodiscard]] nb::object owning_graph() const override {
        Node* node = _view.owning_node();
        return node ? node->owning_graph_py() : nb::none();
    }

    // Active/passive control
    // Note: Managed at the TSInputRoot/TSLink level, not here
    [[nodiscard]] nb::bool_ active() const { return nb::bool_(true); }
    void make_active() { /* Delegate to TSInputRoot */ }
    void make_passive() { /* Delegate to TSInputRoot */ }

    // Binding - handled by builder, not wrapper
    [[nodiscard]] nb::bool_ bound() const { return nb::bool_(true); }
    [[nodiscard]] nb::bool_ has_peer() const {
        // Check if view came from a TSLink
        return nb::bool_(_view.has_link_source());
    }
};
```

#### 5.2.4 PyTimeSeriesBundleOutput

```cpp
struct PyTimeSeriesBundleOutput : PyTimeSeriesOutput {
private:
    // Store TSBView for bundle-specific navigation
    TSBView _bundle_view;

public:
    explicit PyTimeSeriesBundleOutput(TSBView view)
        : PyTimeSeriesOutput(view), _bundle_view(view) {}

    // Bundle navigation
    nb::object get_item(const nb::handle& key) const {
        if (nb::isinstance<nb::str>(key)) {
            std::string name = nb::cast<std::string>(key);
            TSView field_view = _bundle_view.field(name);
            return wrap_output_view_dispatch(field_view);
        }
        if (nb::isinstance<nb::int_>(key)) {
            size_t index = nb::cast<size_t>(key);
            TSView field_view = _bundle_view.field(index);
            return wrap_output_view_dispatch(field_view);
        }
        throw std::runtime_error("Invalid key type for TimeSeriesBundle");
    }

    nb::object keys() const {
        nb::list result;
        const auto* meta = _bundle_view.bundle_meta();
        for (size_t i = 0; i < meta->field_count(); ++i) {
            result.append(nb::str(meta->field_name(i).c_str()));
        }
        return result;
    }

    nb::object values() const {
        nb::list result;
        for (size_t i = 0; i < _bundle_view.field_count(); ++i) {
            result.append(wrap_output_view_dispatch(_bundle_view.field(i)));
        }
        return result;
    }

    // modified_keys, valid_keys, etc. - use delta_view from overlay
    nb::object modified_keys() const {
        nb::list result;
        if (auto delta = _bundle_view.delta_view(current_time())) {
            for (size_t idx : delta.modified_indices()) {
                result.append(nb::str(_bundle_view.bundle_meta()->field_name(idx).c_str()));
            }
        }
        return result;
    }

    nb::bool_ all_valid() const override {
        for (size_t i = 0; i < _bundle_view.field_count(); ++i) {
            if (!_bundle_view.field(i).ts_valid()) {
                return nb::bool_(false);
            }
        }
        return nb::bool_(true);
    }
};
```

#### 5.2.5 PyTimeSeriesBundleInput

```cpp
struct PyTimeSeriesBundleInput : PyTimeSeriesInput {
private:
    TSBView _bundle_view;
    // Note: Link navigation is transparent - TSBView already follows links

public:
    explicit PyTimeSeriesBundleInput(TSBView view)
        : PyTimeSeriesInput(view), _bundle_view(view) {}

    // Same navigation interface as output
    nb::object get_item(const nb::handle& key) const;
    nb::object keys() const;
    nb::object values() const;
    nb::object modified_keys() const;
    nb::object valid_keys() const;
    // etc.
};
```

#### 5.2.6 PyTimeSeriesDictOutput

```cpp
struct PyTimeSeriesDictOutput : PyTimeSeriesOutput {
private:
    TSDView _dict_view;
    const TSDTypeMeta* _meta;

public:
    explicit PyTimeSeriesDictOutput(TSDView view)
        : PyTimeSeriesOutput(view), _dict_view(view), _meta(view.dict_meta()) {}

    // Key handling helper
    value::Value<> key_from_python(const nb::object& key) const {
        const auto* key_schema = _meta->key_type();
        value::Value<> key_val(key_schema);
        key_schema->ops->from_python(key_val.data(), key, key_schema);
        return key_val;
    }

    nb::object get_item(const nb::object& key) const {
        // Handle KEY_SET special case
        if (is_key_set_id(key)) { return key_set(); }

        auto key_val = key_from_python(key);
        TSView value_view = _dict_view.at(key_val);  // Template instantiation handled internally
        return wrap_output_view_dispatch(value_view);
    }

    bool contains(const nb::object& key) const {
        auto key_val = key_from_python(key);
        return _dict_view.contains(key_val);
    }

    // Delta access via MapDeltaView
    nb::object added_keys() const {
        nb::list result;
        if (auto delta = _dict_view.delta_view(current_time())) {
            for (const auto& key : delta.added_keys()) {
                result.append(key_to_python(key));
            }
        }
        return result;
    }

    nb::object removed_keys() const {
        nb::list result;
        if (auto delta = _dict_view.delta_view(current_time())) {
            for (const auto& key : delta.removed_keys()) {
                result.append(key_to_python(key));
            }
        }
        return result;
    }

    // Key set access
    nb::object key_set() const {
        // Return a TSSView wrapper for the key set
        KeySetOverlayView ks = _dict_view.key_set_view();
        return wrap_key_set_view(ks);
    }
};
```

### 5.3 Wrapper Factory Changes

The `wrapper_factory.cpp` needs to support view-based wrapping for all types.

**Key principle**: Wrappers are created on-demand as value objects. The `nb::cast()` creates a Python object that owns the wrapper by value - no smart pointers involved.

```cpp
// Returns Python object owning wrapper BY VALUE
// NOTE: No Node* parameter - view provides owning_node() via container/root
nb::object wrap_output_view(TSMutableView view) {
    if (!view.valid()) { return nb::none(); }

    const auto* meta = view.ts_meta();
    if (!meta) { return nb::none(); }

    // Each case creates wrapper by value, nb::cast transfers ownership to Python
    switch (meta->kind()) {
        case TSTypeKind::TS:
            return nb::cast(PyTimeSeriesValueOutput(view));
        case TSTypeKind::TSB: {
            TSBView bundle = view.as_bundle();
            return nb::cast(PyTimeSeriesBundleOutput(bundle));
        }
        case TSTypeKind::TSL: {
            TSLView list = view.as_list();
            return nb::cast(PyTimeSeriesListOutput(list));
        }
        case TSTypeKind::TSD: {
            TSDView dict = view.as_dict();
            return nb::cast(PyTimeSeriesDictOutput(dict));
        }
        case TSTypeKind::TSS: {
            TSSView set = view.as_set();
            return nb::cast(PyTimeSeriesSetOutput(set));
        }
        case TSTypeKind::TSW: {
            TSWView window = view.as_window();
            return nb::cast(PyTimeSeriesWindowOutput(window));
        }
        case TSTypeKind::REF:
            // Deferred to Phase 6.75
            throw std::runtime_error("REF wrapping not yet implemented");
        case TSTypeKind::SIGNAL:
            return nb::cast(PyTimeSeriesSignalOutput(view));
        default:
            throw std::runtime_error("wrap_output_view: Unknown TSTypeKind");
    }
}

nb::object wrap_input_view(TSView view) {
    // Similar dispatch based on meta->kind()
    // ...
}

// Helper for dispatching child views during navigation
// NOTE: No Node* parameter - view carries all needed context
nb::object wrap_output_view_dispatch(TSView view) {
    // Cast to TSMutableView when we know it's from a mutable context
    // View carries owning_node via container/root
    // ...
}
```

### 5.4 Node Changes

The Node class needs to hold `TSInputRoot` for input (not raw TSValue):

```cpp
struct Node {
    // ... existing fields ...

private:
    // ========== New TSValue/TSInputRoot Storage ==========
    std::optional<TSInputRoot> _ts_input;        // Input with link support (always TSB)
    std::optional<TSValue> _ts_output;           // Output storage (any TS type)
    std::optional<TSValue> _ts_error_output;     // Error output path
    std::optional<TSValue> _ts_recordable_state; // Recordable state path

public:
    // Constructor creates storage from TSMeta schemas
    Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
         node_signature_s_ptr signature, nb::dict scalars,
         const TSMeta* input_meta, const TSMeta* output_meta,
         const TSMeta* error_output_meta = nullptr,
         const TSMeta* recordable_state_meta = nullptr) {

        // Create output TSValue
        if (output_meta) {
            _ts_output.emplace(output_meta, this, OUTPUT_MAIN);
        }

        // Create input using TSInputRoot (wraps TSValue with link support)
        if (input_meta && input_meta->kind() == TSTypeKind::TSB) {
            _ts_input.emplace(static_cast<const TSBTypeMeta*>(input_meta), this);
        }

        // Error output
        if (error_output_meta) {
            _ts_error_output.emplace(error_output_meta, this, ERROR_PATH);
        }

        // Recordable state
        if (recordable_state_meta) {
            _ts_recordable_state.emplace(recordable_state_meta, this, STATE_PATH);
        }
    }

    // Access TSInputRoot for input binding and navigation
    [[nodiscard]] bool has_ts_input() const { return _ts_input.has_value(); }
    [[nodiscard]] TSInputRoot& ts_input() { return *_ts_input; }
    [[nodiscard]] const TSInputRoot& ts_input() const { return *_ts_input; }

    // View access delegating to TSInputRoot
    [[nodiscard]] TSBView input_view() {
        return _ts_input ? _ts_input->bundle_view() : TSBView();
    }
    [[nodiscard]] TSBView input_view() const {
        return _ts_input ? _ts_input->bundle_view() : TSBView();
    }

    // Output view access
    [[nodiscard]] bool has_ts_output() const { return _ts_output.has_value(); }
    [[nodiscard]] TSMutableView output_view() {
        return _ts_output ? _ts_output->mutable_view() : TSMutableView();
    }
    [[nodiscard]] TSView output_view() const {
        return _ts_output ? _ts_output->view() : TSView();
    }
};
```

**Note**: TSInputRoot is used for input (not raw TSValue) because it:
1. Already wraps a TSValue internally
2. Provides link support for binding fields to external outputs
3. Manages active/passive subscription state
4. Has `bind_field()` methods for builder integration

### 5.5 Builder Changes

Builders need to be modified to:

1. Use `TSMeta` schemas instead of creating legacy types
2. Bind inputs to outputs via `TSLink`

```cpp
// OutputBuilder changes
class OutputBuilder {
public:
    // NEW: Provide TSMeta for node to create TSValue internally
    [[nodiscard]] virtual const TSMeta* ts_meta() const = 0;

    // DEPRECATED: Remove make_instance once migration complete
    // [[nodiscard]] virtual time_series_output_s_ptr make_instance(node_ptr owner) = 0;
};

// InputBuilder changes
class InputBuilder {
public:
    // NEW: Provide TSMeta for input schema
    [[nodiscard]] virtual const TSMeta* ts_meta() const = 0;

    // NEW: Bind input field to output
    virtual void bind_input(TSInputRoot& input, size_t field_index, const TSValue* output);
};

// NodeBuilder changes
class NodeBuilder {
public:
    void wire_inputs(Node& node, const InputWiring& wiring) {
        TSInputRoot& input = node.ts_input();

        for (const auto& [field_name, output_source] : wiring) {
            size_t field_idx = input.bundle_meta()->field_index(field_name);

            if (auto* output_node = output_source.node()) {
                // Peered input - bind via TSLink
                const TSValue& output_ts = output_node->_ts_output.value();
                input.bind_field(field_idx, &output_ts);
            }
            // Non-peered input uses local TSValue storage within TSInputRoot
        }

        // Make active to subscribe
        input.make_active();
    }
};
```

### 5.6 Delta Value Implementation

Each specialized view provides delta access via overlay:

```cpp
// In PyTimeSeriesDictOutput
nb::object delta_value() const override {
    nb::dict result;

    auto delta = _dict_view.delta_view(current_time());
    if (!delta.valid()) return nb::none();

    // Added entries
    if (delta.has_added()) {
        nb::dict added;
        for (const auto& key : delta.added_keys()) {
            added[key_to_python(key)] = value_to_python(delta.added_value(key));
        }
        result["added"] = added;
    }

    // Removed keys
    if (delta.has_removed()) {
        nb::list removed;
        for (const auto& key : delta.removed_keys()) {
            removed.append(key_to_python(key));
        }
        result["removed"] = removed;
    }

    return result;
}
```

### 5.7 All Valid Implementation

For composite types, `all_valid()` needs to check all children:

```cpp
// In PyTimeSeriesBundleOutput/Input
nb::bool_ all_valid() const override {
    for (size_t i = 0; i < _bundle_view.field_count(); ++i) {
        TSView field = _bundle_view.field(i);
        if (!field.ts_valid()) {
            return nb::bool_(false);
        }
        // Recursively check nested composites
        if (field.ts_meta()->is_composite()) {
            // Cast and check all_valid recursively
        }
    }
    return nb::bool_(true);
}
```

---

## 6. Migration Sequence

### Phase 6.1: View-based Wrappers (Foundation)

1. Add `Node*` member to all PyXxx wrapper classes for graph navigation
2. Update `PyTimeSeriesOutput` to fully support view-based mode
3. Update `PyTimeSeriesInput` to fully support view-based mode
4. Test with existing scalar TS types

### Phase 6.2: Bundle Wrappers

1. Create view-based `PyTimeSeriesBundleOutput` using `TSBView`
2. Create view-based `PyTimeSeriesBundleInput` using `TSBView` + `TSInputRoot`
3. Implement `keys()`, `values()`, `items()` using view navigation
4. Implement `modified_keys()`, `valid_keys()` using `BundleDeltaView`
5. Test bundle navigation and delta tracking

### Phase 6.3: Collection Wrappers

1. Create view-based `PyTimeSeriesListOutput` using `TSLView`
2. Create view-based `PyTimeSeriesListInput` using `TSLView`
3. Create view-based `PyTimeSeriesDictOutput` using `TSDView`
4. Create view-based `PyTimeSeriesDictInput` using `TSDView`
5. Create view-based `PyTimeSeriesSetOutput` using `TSSView`
6. Create view-based `PyTimeSeriesSetInput` using `TSSView`
7. Implement delta tracking for collections via overlay

### Phase 6.4: Wrapper Factory Update

1. Update `wrap_output_view()` to handle all types
2. Update `wrap_input_view()` to handle all types
3. Add child view wrapping helpers
4. Test that navigation returns correct wrapper types

### Phase 6.5: Builder Integration

1. Add `ts_meta()` to builder base classes
2. Modify `OutputBuilder::make_instance()` to provide TSMeta to node
3. Modify `InputBuilder` to use `TSInputRoot::bind_field()`
4. Update `NodeBuilder::wire_inputs()` for TSLink binding

### Phase 6.6: Node Constructor Update

1. Update Node constructor to accept TSMeta schemas
2. Node creates TSValue internally using schemas
3. Node creates TSInputRoot for input management
4. Remove legacy `_input` / `_output` shared_ptr fields

### Phase 6.75: REF Type Handling (Deferred)

REF types are deferred to a separate phase due to their unique complexity:

1. REF wraps another output - requires indirection handling
2. REF rebinding semantics (when reference target changes)
3. REF navigation must follow to the referenced output
4. Requires Phase 6.1-6.6 to be stable before tackling

**Note**: REF infrastructure exists but integration is complex enough to warrant separate attention after the "mundane" types are complete.

### Phase 7.1: Legacy Type Removal (Preparation)

1. Identify all usages of legacy types outside wrappers
2. Create migration checklist for each legacy type
3. Update any remaining code to use TSValue/views

### Phase 7.2: Legacy Type Removal (Execution)

1. Remove `TimeSeriesValueOutput`, `TimeSeriesValueInput`
2. Remove `TimeSeriesBundleOutput`, `TimeSeriesBundleInput`
3. Remove `TimeSeriesListOutput`, `TimeSeriesListInput`
4. Remove `TimeSeriesDictOutputImpl`, `TimeSeriesDictInputImpl`
5. Remove `TimeSeriesSetOutput`, `TimeSeriesSetInput`
6. Remove `BaseTimeSeriesOutput`, `BaseTimeSeriesInput`
7. Remove `TimeSeriesOutput`, `TimeSeriesInput` interfaces

### Phase 7.3: Cleanup

1. Remove `ApiPtr<TimeSeriesType>` from wrapper classes
2. Remove legacy builder code
3. Clean up wrapper_factory.cpp
4. Update documentation

---

## 7. Testing Strategy

### 7.1 Unit Tests

Each phase should have corresponding tests:

| Phase | Test Focus |
|-------|------------|
| 6.1 | View-based scalar value/modified/valid |
| 6.2 | Bundle field access, delta tracking |
| 6.3 | Collection navigation, delta tracking |
| 6.4 | Wrapper factory type dispatch |
| 6.5 | Builder TSMeta integration |
| 6.6 | Node TSValue lifecycle |
| 7.1-7.3 | Regression tests for all existing functionality |

### 7.2 Behavioral Parity Tests

Create tests that verify identical behavior between legacy and new:

```python
def test_behavioral_parity():
    # Run same graph with legacy types
    legacy_result = run_with_legacy()

    # Run same graph with TSValue types
    tsvalue_result = run_with_tsvalue()

    # Compare all outputs
    assert legacy_result == tsvalue_result
```

### 7.3 Performance Tests

Benchmark key operations:

- Value get/set throughput
- Bundle field navigation
- Dict key lookup
- Delta tracking overhead
- Subscription notification latency

---

## 8. Risk Assessment

### 8.1 High Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Behavioral difference from legacy | Critical - user code breaks | Extensive behavioral parity tests |
| Graph navigation broken | High - debugging impacted | Preserve Node* back-references |
| Delta tracking incorrect | High - event processing fails | Validate against Python implementation |

### 8.2 Medium Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance regression | Medium - slower execution | Benchmark before/after each phase |
| Memory usage increase | Medium - larger footprint | Profile memory, optimize overlay allocation |
| Builder compatibility | Medium - wiring breaks | Incremental migration with fallback |

### 8.3 Low Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| API signature change | Low - compilation error | Static assert API stability |
| View lifetime issues | Low - crash on access | Document view transience, test edge cases |

---

## 9. Design Decisions (Resolved)

1. **Wrappers hold views by value (not pointers)**
   - Wrappers are lightweight facades over views
   - TSView/TSMutableView held as native value members, not pointers
   - All logic delegates to the view
   - **No separate Node* parameter** - view provides `owning_node()` via container/root
   - Node wrapper remains as-is with ApiPtr for now (separate concern)

2. **REF types deferred to Phase 6.75**
   - REF has its own complexities requiring separate attention
   - Infrastructure is in place but tackle after mundane tasks complete
   - Will be addressed as "Phase 6 and 3/4's" between 6 and 7

3. **Wrappers created on demand as value objects**
   - Created lazily via `__getitem__` / field access
   - MUST NOT be shared_ptr or any variation
   - Value semantics from Python's perspective
   - Extremely lightweight - just hold the view

4. **TSW (window) uses same view pattern**
   - No different from any other TS type
   - Just a view with window-specific navigation methods

5. **Delta value translation in PyXXX wrappers**
   - PyXXX wrappers MUST return delta values in existing format (API contract)
   - Internal implementation uses delta views from our design
   - Translation from delta view → legacy format occurs in wrapper layer

---

## 10. Exit Criteria

Phase 6 is complete when:
- [ ] All PyXxx wrappers work with view-based mode only
- [ ] No `ApiPtr<TimeSeriesType>` usage in wrappers
- [ ] All existing Python tests pass
- [ ] Performance is within 10% of legacy

Phase 7 is complete when:
- [ ] All legacy TimeSeriesType classes removed
- [ ] All legacy builder code removed
- [ ] Codebase compiles without legacy types
- [ ] All tests pass
- [ ] Documentation updated

---

## Appendix A: API Stability Reference

The following Python API must be preserved exactly:

### TimeSeriesOutput

```python
class TimeSeriesOutput:
    @property
    def value(self) -> Any: ...
    @value.setter
    def value(self, v: Any) -> None: ...
    @property
    def delta_value(self) -> Any: ...
    @property
    def modified(self) -> bool: ...
    @property
    def valid(self) -> bool: ...
    @property
    def all_valid(self) -> bool: ...
    @property
    def last_modified_time(self) -> datetime: ...
    @property
    def owning_node(self) -> Node: ...
    @property
    def owning_graph(self) -> Graph: ...
    @property
    def parent_output(self) -> TimeSeriesOutput: ...
    @property
    def has_parent_output(self) -> bool: ...

    def apply_result(self, value: Any) -> None: ...
    def clear(self) -> None: ...
    def invalidate(self) -> None: ...
    def can_apply_result(self, value: Any) -> bool: ...
    def copy_from_output(self, output: TimeSeriesOutput) -> None: ...
    def copy_from_input(self, input: TimeSeriesInput) -> None: ...
```

### TimeSeriesInput

```python
class TimeSeriesInput:
    @property
    def value(self) -> Any: ...
    @property
    def delta_value(self) -> Any: ...
    @property
    def modified(self) -> bool: ...
    @property
    def valid(self) -> bool: ...
    @property
    def all_valid(self) -> bool: ...
    @property
    def last_modified_time(self) -> datetime: ...
    @property
    def owning_node(self) -> Node: ...
    @property
    def owning_graph(self) -> Graph: ...
    @property
    def parent_input(self) -> TimeSeriesInput: ...
    @property
    def has_parent_input(self) -> bool: ...
    @property
    def active(self) -> bool: ...
    @property
    def bound(self) -> bool: ...
    @property
    def has_peer(self) -> bool: ...
    @property
    def output(self) -> TimeSeriesOutput: ...

    def make_active(self) -> None: ...
    def make_passive(self) -> None: ...
    def bind_output(self, output: TimeSeriesOutput) -> bool: ...
    def un_bind_output(self, unbind_refs: bool = False) -> None: ...
```

### TimeSeriesBundleOutput/Input

```python
class TimeSeriesBundleOutput(TimeSeriesOutput):
    def __getitem__(self, key: str | int) -> TimeSeriesOutput: ...
    def __getattr__(self, name: str) -> TimeSeriesOutput: ...
    def __iter__(self) -> Iterator[TimeSeriesOutput]: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: str) -> bool: ...

    def keys(self) -> List[str]: ...
    def values(self) -> List[TimeSeriesOutput]: ...
    def items(self) -> List[Tuple[str, TimeSeriesOutput]]: ...
    def valid_keys(self) -> List[str]: ...
    def valid_values(self) -> List[TimeSeriesOutput]: ...
    def valid_items(self) -> List[Tuple[str, TimeSeriesOutput]]: ...
    def modified_keys(self) -> List[str]: ...
    def modified_values(self) -> List[TimeSeriesOutput]: ...
    def modified_items(self) -> List[Tuple[str, TimeSeriesOutput]]: ...
    def key_from_value(self, value: TimeSeriesOutput) -> Optional[str]: ...

    @property
    def empty(self) -> bool: ...
```

### TimeSeriesDictOutput/Input

```python
class TimeSeriesDictOutput(TimeSeriesOutput):
    def __getitem__(self, key: K) -> TimeSeriesOutput: ...
    def __setitem__(self, key: K, value: Any) -> None: ...
    def __delitem__(self, key: K) -> None: ...
    def __iter__(self) -> Iterator[K]: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: K) -> bool: ...

    def get(self, key: K, default: Any = None) -> TimeSeriesOutput: ...
    def get_or_create(self, key: K) -> TimeSeriesOutput: ...
    def create(self, key: K) -> None: ...
    def pop(self, key: K, default: Any = None) -> Any: ...
    def clear(self) -> None: ...

    def keys(self) -> List[K]: ...
    def values(self) -> List[TimeSeriesOutput]: ...
    def items(self) -> List[Tuple[K, TimeSeriesOutput]]: ...
    def valid_keys(self) -> List[K]: ...
    def valid_values(self) -> List[TimeSeriesOutput]: ...
    def valid_items(self) -> List[Tuple[K, TimeSeriesOutput]]: ...
    def modified_keys(self) -> List[K]: ...
    def modified_values(self) -> List[TimeSeriesOutput]: ...
    def modified_items(self) -> List[Tuple[K, TimeSeriesOutput]]: ...
    def added_keys(self) -> List[K]: ...
    def added_values(self) -> List[TimeSeriesOutput]: ...
    def added_items(self) -> List[Tuple[K, TimeSeriesOutput]]: ...
    def removed_keys(self) -> List[K]: ...
    def removed_values(self) -> List[TimeSeriesOutput]: ...
    def removed_items(self) -> List[Tuple[K, TimeSeriesOutput]]: ...
    def was_added(self, key: K) -> bool: ...
    def was_removed(self, key: K) -> bool: ...
    def was_modified(self, key: K) -> bool: ...

    @property
    def has_added(self) -> bool: ...
    @property
    def has_removed(self) -> bool: ...
    @property
    def key_set(self) -> TimeSeriesSetOutput: ...
```

(Similar detailed API for TSL, TSS, TSW, REF types)

---

## Appendix B: Detailed Behavioral Specifications

This appendix provides the exact expected behavior for each method, derived from the Python reference implementation in `hgraph/_impl/_types/`. These specifications are the **source of truth** for behavioral parity.

### B.1 Output Base Class Behavior

From `_output.py`:

| Property/Method | Behavior |
|-----------------|----------|
| `modified` | Returns `evaluation_time == _last_modified_time` |
| `valid` | Returns `_last_modified_time > MIN_DT` |
| `all_valid` | Base: same as `valid`. Override for composites. |
| `last_modified_time` | Returns `_last_modified_time` |
| `mark_modified(time)` | If `time > _last_modified_time`: update `_last_modified_time`, call parent's `mark_child_modified()`, notify subscribers |
| `mark_invalid()` | Set `_last_modified_time = MIN_DT`, notify subscribers with current eval time |
| `mark_child_modified(child, time)` | Calls `mark_modified(time)` (propagation) |
| `subscribe(subscriber)` | Add to `_subscribers` list |
| `unsubscribe(subscriber)` | Remove from `_subscribers` list |
| `_notify(time)` | For each subscriber: call `subscriber.notify(time)` |
| `owning_node` | Traverse `_parent_or_node` chain until Node found |
| `owning_graph` | `owning_node.graph` |
| `parent_output` | Return `_parent_or_node` if not a Node |
| `has_parent_output` | `_parent_or_node is not None and not isinstance(Node)` |

### B.2 Input Base Class Behavior

From `_input.py` (PythonBoundTimeSeriesInput):

| Property/Method | Behavior |
|-----------------|----------|
| `_output` | The bound output (or None) |
| `_active` | Whether subscribed to output |
| `_sample_time` | Time when binding state changed |
| `_notify_time` | Last notification time (for deduplication) |
| `active` | Returns `_active` |
| `bound` | Returns `_output is not None` |
| `has_peer` | Base: same as `bound`. Collections may override. |
| `value` | `_output.value if _output else None` |
| `delta_value` | `_output.delta_value if _output else None` |
| `modified` | `_output.modified or _sampled` (where `_sampled = _sample_time == eval_time`) |
| `valid` | `bound and _output.valid` |
| `all_valid` | `bound and _output.all_valid` |
| `last_modified_time` | `max(_output.last_modified_time, _sample_time) if bound else MIN_DT` |
| `make_active()` | If not active: set `_active=True`, subscribe to output, if output is modified OR sampled -> notify |
| `make_passive()` | If active: set `_active=False`, unsubscribe from output |
| `notify(time)` | If `time != _notify_time`: update `_notify_time`, propagate to parent or node |
| `bind_output(output)` | Handle REF specially (defer to Phase 6.75), otherwise call `do_bind_output` |
| `do_bind_output(output)` | Make passive, set `_output`, if was active make active again |
| `un_bind_output()` | Call `do_un_bind_output`, notify if was valid and node started |
| `do_un_bind_output()` | Unsubscribe, set `_output = None` |

**Critical: `_sampled` state**

The `_sampled` property returns `_sample_time == evaluation_time`. This captures binding changes:
- When input binds to new output: `_sample_time` is set to current eval time
- When input unbinds: `_sample_time` is set to current eval time
- Effect: `modified` returns True even if bound output wasn't modified this tick

### B.3 TS[SCALAR] (Scalar Time-Series)

**Output** (`_ts.py`):

| Property/Method | Behavior |
|-----------------|----------|
| `value` (get) | Return `_value` |
| `value` (set) | If None: invalidate. Else: set `_value`, call `mark_modified()` |
| `delta_value` | Return `_value` (same as value for scalars) |
| `clear()` | No-op for scalars |
| `invalidate()` | Call `mark_invalid()` |
| `mark_invalid()` | Set `_value = None`, call `super().mark_invalid()` |
| `can_apply_result(v)` | `not self.modified` |
| `apply_result(v)` | If `v is not None`: `self.value = v` |
| `copy_from_output(out)` | `self.value = out._value` |
| `copy_from_input(inp)` | `self.value = inp.value` |

**Input**: Uses base class behavior directly.

### B.4 TSB (Bundle)

**Output** (`_tsb.py`):

| Property/Method | Behavior |
|-----------------|----------|
| `value` | If scalar_type exists: construct scalar from fields. Else: `{k: ts.value for k, ts in items() if ts.valid}` |
| `value` (set) | If None: invalidate. If scalar_type: set matching fields. Else: iterate dict setting non-None values. |
| `delta_value` | `{k: ts.delta_value for k, ts in items() if ts.modified and ts.valid}` |
| `all_valid` | `all(ts.valid for ts in _ts_values.values())` |
| `clear()` | For each child: `child.clear()` |
| `invalidate()` | If valid: invalidate all children, then `mark_invalid()` |
| `mark_invalid()` | If valid: call super, then `child.mark_invalid()` for each child |
| `copy_from_output(out)` | For each k,v in output: `self[k].copy_from_output(v)` |
| `copy_from_input(inp)` | For each k,v in input: `self[k].copy_from_input(v)` |
| `keys()`, `values()`, `items()` | Standard dict-like iteration over `_ts_values` |
| `modified_keys()` | Keys where `ts.modified` |
| `modified_values()` | Values where `ts.modified` |
| `valid_keys()` | Keys where `ts.valid` |

**Input** (`_tsb.py`):

| Property/Method | Peered Behavior | Non-peered Behavior |
|-----------------|-----------------|---------------------|
| `value` | `super().value` (delegate to output) | `{k: ts.value for k, ts in items() if ts.valid}` |
| `delta_value` | `super().delta_value` | `{k: ts.delta_value for k, ts in items() if ts.modified and ts.valid}` |
| `modified` | `super().modified` | `any(ts.modified for ts in values())` |
| `valid` | `super().valid` | `any(ts.valid for ts in values())` |
| `all_valid` | `all(ts.valid for ts in values())` | Same |
| `last_modified_time` | `super().last_modified_time` | `max(ts.last_modified_time for ts in values())` |
| `active` | `super().active` | `any(ts.active for ts in values())` |
| `make_active()` | `super().make_active()` | For each child: `child.make_active()` |
| `make_passive()` | `super().make_passive()` | For each child: `child.make_passive()` |
| `bound` | `super().bound or any(ts.bound for ts in values())` | Same |
| `do_bind_output(out)` | Bind each child: `ts.bind_output(out[k])`. peer if ALL return True. | Same |

### B.5 TSL (List)

**Output** (`_tsl.py`):

| Property/Method | Behavior |
|-----------------|----------|
| `value` | `tuple(ts.value if ts.valid else None for ts in _ts_values)` |
| `value` (set) | If dict: set matching indices. If tuple/list: zip-set all. |
| `delta_value` | `{i: ts.delta_value for i, ts in enumerate(_ts_values) if ts.modified}` |
| `all_valid` | `all(ts.valid for ts in values())` |
| `clear()` | For each child: `child.clear()` |
| `invalidate()` | For each child: `child.invalidate()` |
| `mark_invalid()` | If valid: for each child `child.mark_invalid()`, then `super().mark_invalid()` |

**Input** (`_tsl.py`):

| Property/Method | Peered Behavior | Non-peered Behavior |
|-----------------|-----------------|---------------------|
| `value` | `super().value` | `tuple(ts.value if ts.valid else None for ts in values())` |
| `delta_value` | If not sampled: `super().delta_value` | `{k: ts.delta_value for k, ts in modified_items()}` |
| `modified` | `super().modified or _sampled` | `any(ts.modified for ts in values())` |
| `valid` | `super().valid` | `any(ts.valid for ts in values())` |
| `all_valid` | `all(ts.valid for ts in values())` | Same |
| `last_modified_time` | `super().last_modified_time` | `max(ts.last_modified_time for ts in values())` |

### B.6 TSD (Dict)

**Output** (`_tsd.py`):

| Property/Method | Behavior |
|-----------------|----------|
| `value` | `frozendict({k: v.value for k, v in items() if v.valid})` |
| `delta_value` | `frozendict(chain(((k, v.delta_value) for k,v in items() if v.modified and v.valid), ((k, REMOVE) for k in removed_keys())))` |
| `key_set` | Returns the embedded TSS for keys |
| `__delitem__(k)` | Remove from key_set, notify observers, move to `_removed_items`, schedule cleanup |
| `create(k)` | Add to key_set, create value via builder, notify observers |
| `get_or_create(k)` | Return existing or create new |
| `added_keys()` | Return `_added_keys` set |
| `removed_keys()` | Return `_removed_items.keys()` |
| `modified_keys()` | Return `_modified_items.keys()` if modified |
| `mark_child_modified(child, time)` | Track in `_modified_items`, call super |

**Input** (`_tsd.py`):

| Property/Method | Peered Behavior | Non-peered Behavior |
|-----------------|-----------------|---------------------|
| `value` | `frozendict((k, v.value) for k, v in items())` | Same |
| `delta_value` | `frozendict(chain(((k, v.delta_value) for k,v in modified_items() if v.valid), ((k, REMOVE) for k,v in removed_items() if was_valid)))` | Same |
| `modified` | `super().modified` | `_last_notified_time == eval_time or key_set.modified or _sample_time == eval_time` |
| `added_keys()` | `key_set.added()` | Same |
| `removed_keys()` | `key_set.removed()` | Same |
| `modified_keys()` | If sampled: all valid keys. Else if peered: `_output.modified_keys()`. Else: `_modified_items.keys()` | Complex logic |
| `on_key_added(k)` | Create child input, bind to output[k] | Same |
| `on_key_removed(k)` | Move to `_removed_items`, unbind | Same |

### B.7 TSS (Set)

**Output** (`_tss.py`):

| Property/Method | Behavior |
|-----------------|----------|
| `value` | Return `_value` (the set) |
| `value` (set) | Handle SetDelta, frozenset, or iterable with Removed markers |
| `delta_value` | `SetDelta(added=_added, removed=_removed)` |
| `add(element)` | Add to `_value` and `_added`, call `mark_modified()` |
| `remove(element)` | Remove from `_value`, add to `_removed`, call `mark_modified()` |
| `clear()` | Set `_removed = _value - _added`, clear `_value`, call `mark_modified()` |
| `added()` | Return `_added or set()` |
| `removed()` | Return `_removed or set()` |
| `was_added(item)` | `item in _added if _added else False` |
| `was_removed(item)` | `item in _removed` |
| `mark_modified(time)` | Call super, schedule `_reset()` for after evaluation |
| `_reset()` | Clear `_added` and `_removed` |

**Input** (`_tss.py`):

| Property/Method | Behavior |
|-----------------|----------|
| `modified` | `(_output is not None and _output.modified) or _sampled` |
| `delta_value` | `SetDelta(added(), removed())` |
| `values()` | `frozenset(output.values()) if bound else frozenset()` |
| `added()` | If `_prev_output`: compute diff. Else if sampled: all values. Else: `output.added()` |
| `removed()` | If `_prev_output`: compute diff. Else if sampled: empty. Else: `output.removed()` |

**Note on `_prev_output`**: When rebinding, the previous output is saved to correctly compute deltas across the rebind.

---

## Appendix C: Testing Strategy Without REF Dependency

### C.1 Testing Philosophy

REF types add complexity due to:
1. Indirect binding (reference to another output)
2. Dynamic rebinding when reference changes
3. Observer patterns for reference lifecycle

To validate Phase 6 without REF:
- Test all non-REF types thoroughly first
- Create test fixtures that don't use REF binding patterns
- Use direct output bindings in tests

### C.2 Behavioral Parity Test Pattern

```python
import pytest
from hgraph._impl._types import *  # Python reference implementation

# Pattern: Compare C++ wrapper behavior against Python reference

class TestBehavioralParity:
    """
    Base class for behavioral parity tests.
    Tests run the same operations against both implementations
    and verify identical results.
    """

    @pytest.fixture
    def python_output(self):
        """Override to create Python reference output."""
        raise NotImplementedError

    @pytest.fixture
    def cpp_output(self):
        """Override to create C++ wrapper output."""
        raise NotImplementedError

    def assert_output_equal(self, py_out, cpp_out):
        """Compare all observable properties."""
        assert py_out.valid == cpp_out.valid
        assert py_out.modified == cpp_out.modified
        assert py_out.last_modified_time == cpp_out.last_modified_time
        if py_out.valid:
            assert py_out.value == cpp_out.value
            assert py_out.delta_value == cpp_out.delta_value
        assert py_out.all_valid == cpp_out.all_valid
```

### C.3 Direct Binding Test Fixtures (No REF)

```python
@pytest.fixture
def mock_node():
    """Create a minimal mock node for testing."""
    class MockNode:
        def __init__(self):
            self._graph = MockGraph()
            self._notify_time = None

        @property
        def graph(self):
            return self._graph

        def notify(self, time):
            self._notify_time = time

        @property
        def is_started(self):
            return True

        @property
        def is_starting(self):
            return False

    return MockNode()

@pytest.fixture
def mock_graph():
    """Create a minimal mock graph with evaluation clock."""
    class MockEvaluationClock:
        def __init__(self):
            self.evaluation_time = datetime(2024, 1, 1)

    class MockGraph:
        def __init__(self):
            self.evaluation_clock = MockEvaluationClock()
            self.evaluation_engine_api = MockEngineApi()

    return MockGraph()

def test_ts_output_basic(mock_node):
    """Test TS[int] output without any REF involvement."""
    # Create output
    output = create_ts_output(int, node=mock_node)

    # Initially invalid
    assert not output.valid
    assert not output.modified

    # Set value
    output.value = 42

    # Now valid and modified
    assert output.valid
    assert output.modified
    assert output.value == 42
    assert output.delta_value == 42

def test_tsb_output_field_modification(mock_node):
    """Test TSB field modification propagates correctly."""
    # Schema: TSB[price: TS[float], volume: TS[int]]
    output = create_tsb_output(schema, node=mock_node)

    # Set one field
    output['price'].value = 100.5

    # Bundle should be modified
    assert output.modified
    assert output['price'].modified
    assert not output['volume'].modified

    # delta_value should only contain modified field
    assert output.delta_value == {'price': 100.5}

    # all_valid should be False (volume not set)
    assert not output.all_valid

def test_tsd_add_remove_keys(mock_node):
    """Test TSD key addition and removal."""
    output = create_tsd_output(str, int, node=mock_node)

    # Add key
    output.get_or_create('a').value = 1

    assert 'a' in output
    assert list(output.added_keys()) == ['a']
    assert list(output.removed_keys()) == []

    # Simulate tick boundary (clear deltas)
    advance_time(mock_node.graph)

    # Remove key
    del output['a']

    assert 'a' not in output
    assert list(output.added_keys()) == []
    assert list(output.removed_keys()) == ['a']
```

### C.4 Input Binding Tests (Direct, No REF)

```python
def test_input_bind_output_directly(mock_node):
    """Test input binding to output without REF."""
    output = create_ts_output(int, node=mock_node)
    input = create_ts_input(int, node=mock_node)

    # Initially not bound
    assert not input.bound
    assert not input.has_peer

    # Bind directly (not via REF)
    input.bind_output(output)

    assert input.bound
    assert input.has_peer

    # Value propagates
    output.value = 42
    assert input.value == 42
    assert input.modified

def test_input_sampled_state(mock_node):
    """Test _sampled state on rebind."""
    output1 = create_ts_output(int, node=mock_node)
    output2 = create_ts_output(int, node=mock_node)
    input = create_ts_input(int, node=mock_node)

    output1.value = 1
    input.bind_output(output1)
    advance_time(mock_node.graph)

    # Rebind to different output
    output2.value = 2
    input.bind_output(output2)

    # Input should be modified due to _sampled (binding changed)
    assert input.modified
    assert input._sampled  # Internal state exposed for testing

def test_tsb_input_non_peered(mock_node):
    """Test TSB input with mixed peered/non-peered fields."""
    # Create outputs for individual fields
    price_out = create_ts_output(float, node=mock_node)
    volume_out = create_ts_output(int, node=mock_node)

    # Create input (bundle)
    input = create_tsb_input(schema, node=mock_node)

    # Bind fields individually (not a peered bundle)
    input['price'].bind_output(price_out)
    input['volume'].bind_output(volume_out)

    # Input is bound but not has_peer at bundle level
    assert input.bound
    # has_peer depends on whether ALL fields are peered to matching bundle

    # Test navigation
    price_out.value = 100.5
    assert input['price'].value == 100.5
    assert input['price'].modified
```

### C.5 Test Categories (Ordered by Complexity)

| Phase | Test Focus | REF Required? |
|-------|------------|---------------|
| 1 | TS[SCALAR] output: value, modified, valid, delta_value | No |
| 2 | TS[SCALAR] input: binding, active/passive, sampled state | No |
| 3 | TSB output: field access, aggregation, delta tracking | No |
| 4 | TSB input: peered vs non-peered, field binding | No |
| 5 | TSL output/input: element access, fixed size | No |
| 6 | TSD output: key_set, add/remove, delta tracking | No |
| 7 | TSD input: key observer pattern, non-peered mode | No |
| 8 | TSS output/input: SetDelta, add/remove tracking | No |
| 9 | Cross-type: TSB containing TSD, nested structures | No |
| 10 | REF types (Phase 6.75) | Yes |

### C.6 Exclusion Markers for REF-dependent Tests

```python
# Mark tests that require REF functionality
requires_ref = pytest.mark.skipif(
    not REF_IMPLEMENTED,
    reason="REF types not yet implemented (Phase 6.75)"
)

@requires_ref
def test_input_bind_reference_output():
    """Test binding input to TimeSeriesReferenceOutput."""
    # This test is skipped until Phase 6.75
    pass

@requires_ref
def test_tsd_get_ref():
    """Test TSD.get_ref() for reference outputs."""
    pass
```

---

## Appendix D: Identified Gaps and Required Additions

### D.1 Missing from Current Design

1. **`_sample_time` handling**: Input wrappers need to track when binding changes to implement `_sampled` correctly.

2. **`_notify_time` deduplication**: Inputs need per-input notification deduplication.

3. **`_prev_output` for TSS/TSD**: When rebinding, the previous output must be saved to compute correct deltas.

4. **`mark_child_modified` propagation**: Parent outputs must track which children were modified.

5. **`_modified_items` tracking for TSD**: Dict outputs need per-key modified tracking.

6. **After-evaluation cleanup callbacks**: TSS/TSD clear `_added`/`_removed` after each tick via callback.

7. **`TSDKeyObserver` pattern**: TSD outputs notify observers when keys are added/removed.

### D.2 View Methods Needed

The following view methods may need to be added to support wrapper implementation:

| View Method | Purpose |
|-------------|---------|
| `TSView::_sample_time()` | Access to input's sample time for `_sampled` calculation |
| `TSBView::modified_field_indices(time)` | Get indices of fields modified at given time |
| `TSDView::_modified_keys()` | Access to modified key tracking |
| `TSDView::_added_keys()` | Access to added key tracking |
| `TSDView::_removed_keys()` | Access to removed key tracking |
| `TSSView::_added()` | Access to added element tracking |
| `TSSView::_removed()` | Access to removed element tracking |

### D.3 Overlay Extensions Needed

| Overlay Extension | Purpose |
|-------------------|---------|
| `MapTSOverlay::added_keys()` | Track keys added this tick |
| `MapTSOverlay::removed_keys()` | Track keys removed this tick |
| `MapTSOverlay::clear_tick_deltas()` | Reset add/remove tracking |
| `SetTSOverlay::added()` | Track elements added this tick |
| `SetTSOverlay::removed()` | Track elements removed this tick |
| `SetTSOverlay::clear_tick_deltas()` | Reset add/remove tracking |

### D.4 Input-Specific State

Each input wrapper needs:

```cpp
struct InputState {
    engine_time_t _sample_time{MIN_DT};     // When binding changed
    engine_time_t _notify_time{MIN_DT};     // Last notification (dedup)
    // For TSS/TSD:
    const TSValue* _prev_output{nullptr};   // Previous output for delta computation
};
```

This state must be tracked **per input instance**, not per view.
