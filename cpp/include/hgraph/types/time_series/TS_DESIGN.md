 # C++ Time-Series Architecture Analysis

**Purpose:** Document the existing C++ time-series type implementation and Python wrapper architecture to plan integration with the new `hgraph::value` type system.

**Date:** 2025-12-15

---

## 1. Current Architecture Overview

The existing C++ time-series implementation consists of two layers:

1. **Core C++ Types** (`cpp/include/hgraph/types/`) - Abstract interfaces and concrete implementations
2. **Python Wrappers** (`cpp/include/hgraph/api/python/`) - Nanobind-based wrappers exposing C++ to Python

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Python Layer                                 │
│  PyTimeSeriesInput/Output, PyTimeSeriesList, PyTimeSeriesBundle...  │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │ ApiPtr<T> delegation
┌─────────────────────────────────▼───────────────────────────────────┐
│                         C++ Layer                                    │
│  TimeSeriesInput/Output, TimeSeriesList, TimeSeriesBundle...         │
│  BaseTimeSeriesInput/Output (concrete implementations)               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Value vs Time-Series Value Distinction

A fundamental concept in the value system is the distinction between **scalar values** (pure data) and **time-series values** (data + tracking).

### 2.1 The Two Layers

| Layer | Class | Purpose | Tracking |
|-------|-------|---------|----------|
| **Data Storage** | `Value` | Pure data storage, all TypeKinds | None |
| **Data Views** | `ValueView` / `ConstValueView` | Navigation + type-safe access | None |
| **Time-Series Storage** | `TimeSeriesValue` | Reactive values with change tracking | Modification + Observation |
| **Time-Series Views** | `TimeSeriesValueView` | Auto-tracking mutations | Automatic |

### 2.2 Composition Model

```
TimeSeriesValue = Value + ModificationTrackerStorage + ObserverStorage
```

Both scalar values and time-series values use the **same underlying type system** (Scalar, Bundle, List, Set, Dict, Ref, Window). The distinction is whether tracking/observation is layered on top.

### 2.3 When to Use Each

| Use Case | Class | Example |
|----------|-------|---------|
| Function parameters | `Value` | Passing data to pure functions |
| Intermediate results | `Value` | Temporary computations |
| Configuration data | `Value` | Static schema definitions |
| Node outputs | `TimeSeriesValue` | Graph node output with change propagation |
| Node inputs | `TimeSeriesValue` | Graph node input observing output changes |
| Reactive collections | `TimeSeriesValue` | TSD, TSS with delta tracking |

### 2.4 Code Structure

```cpp
// value.h - Pure data storage (any TypeKind, no tracking)
class Value {
    void* _storage;
    const TypeMeta* _schema;
};

// time_series_value.h - Data + tracking (reactive)
class TimeSeriesValue {
    Value _value;                              // Same underlying storage
    ModificationTrackerStorage _tracker;       // Modification times
    std::unique_ptr<ObserverStorage> _observers;  // Notification subscribers
};
```

The naming is intentional:
- `Value` → generic, can hold any type kind
- `TimeSeriesValue` → explicitly adds time-series semantics (tracking + notification)

---

## 3. Core Type Hierarchy

### 3.1 Abstract Interfaces (`time_series_type.h`)

```
TimeSeriesType (Abstract)
├── py_value() / py_delta_value()     # Python object representations
├── modified() / valid() / all_valid() # State queries
├── last_modified_time()               # Engine timing
├── owning_node() / owning_graph()     # Graph navigation
└── is_reference() / has_reference()   # Reference type checks

TimeSeriesOutput : TimeSeriesType
├── subscribe() / un_subscribe()       # Notifiable management
├── apply_result() / py_set_value()    # Mutation operations
├── copy_from_output/input()           # Value copying
├── mark_modified() / mark_invalid()   # State management
└── parent_output()                    # Output hierarchy

TimeSeriesInput : TimeSeriesType, Notifiable
├── parent_input()                     # Input hierarchy
├── active() / make_active/passive()   # Activity control
├── bound() / has_peer()               # Binding state
├── output() / bind_output()           # Output binding
└── get_input()                        # REF time-series binding hack
```

### 3.2 Base Implementations (`base_time_series.h`)

```
BaseTimeSeriesOutput : TimeSeriesOutput
├── _parent_ts_or_node              # Variant ownership
├── _subscribers                     # Set<Notifiable*>
└── _last_modified_time             # engine_time_t

BaseTimeSeriesInput : TimeSeriesInput
├── _parent_ts_or_node              # Variant ownership
├── _output                          # Bound output (shared_ptr)
├── _reference_output               # For backtrace support
├── _active                          # Activity flag
└── _sample_time / _notify_time     # Timing
```

---

## 4. Specialized Types

### 4.1 TimeSeriesValue (TS) - Scalar Types

**File:** `ts.h`

```cpp
TimeSeriesValueOutput<T> : TimeSeriesValueOutputBase : BaseTimeSeriesOutput
├── T _value{}                      # Direct value storage
├── value() -> const T&
├── set_value(const T&)
├── py_value() / py_delta_value()   # Python conversion
└── py_set_value(nb::object)

TimeSeriesValueInput<T> : TimeSeriesValueInputBase : BaseTimeSeriesInput
├── value() -> const T&             # Delegates to output
└── value_output() -> TimeSeriesValueOutput<T>*
```

**Key Points:**
- Template-based for any type T
- Direct value storage (not reference)
- `py_value()` converts to Python objects via nanobind

### 4.2 TimeSeriesList (TSL) - Indexed Collections

**File:** `tsl.h`

```cpp
IndexedTimeSeries<T_TS> : T_TS
├── std::vector<ts_type::s_ptr> _ts_values
├── operator[](size_t) -> ts_type*
├── values() / valid_values() / modified_values()
└── size() / empty()

TimeSeriesListOutput : TimeSeriesList<IndexedTimeSeriesOutput>
TimeSeriesListInput : TimeSeriesList<IndexedTimeSeriesInput>
├── keys() / valid_keys() / modified_keys()
├── items() / valid_items() / modified_items()
└── py_value() / py_delta_value()
```

**Key Points:**
- Element-wise validity and modification tracking
- Generic Python iteration support
- Fixed-size at construction time

### 4.3 TimeSeriesBundle (TSB) - Struct-like with Schema

**File:** `tsb.h`

```cpp
TimeSeriesSchema
├── _keys: std::vector<std::string>
└── _scalar_type: nb::object

TimeSeriesBundle<T_TS> : IndexedTimeSeries<T_TS>
├── schema() -> TimeSeriesSchema&
├── operator[](const std::string& key)
├── operator[](size_t index)        # From IndexedTimeSeries
├── contains(key) -> bool
└── keys() / items() / values()

TimeSeriesBundleOutput : TimeSeriesBundle<IndexedTimeSeriesOutput>
TimeSeriesBundleInput : TimeSeriesBundle<IndexedTimeSeriesInput>
```

**Key Points:**
- String-based property access (like Python dict)
- Schema tracks field names and types
- Both index and string access supported

### 4.4 TimeSeriesSet (TSS) - Dynamic Collections

**File:** `tss.h`

```cpp
SetDelta_T<T>
├── _added: std::unordered_set<T>
├── _removed: std::unordered_set<T>
└── _tp: nb::object (for nb::object keys)

TimeSeriesSetOutput_T<K> : TimeSeriesSetOutput : TimeSeriesSet<BaseTimeSeriesOutput>
├── std::unordered_map<K, TimeSeriesValueOutput<bool>::s_ptr> _contains_outputs
├── _is_empty_output
├── added() / removed() -> SetDelta_T<K>
├── get_contains_output(key) / release_contains_output(key)
└── py_set_item() / py_del_item()

TimeSeriesSetInput_T<K> : TimeSeriesSetInput : TimeSeriesSet<BaseTimeSeriesInput>
```

**Key Points:**
- Delta-based tracking (added/removed per cycle)
- Dynamic membership queries via contains outputs
- Integration with TSD for key tracking

### 4.5 TimeSeriesDict (TSD) - Key-Value Pairs

**File:** `tsd.h`

```cpp
TSDKeyObserver<K>
├── on_key_added(K) / on_key_removed(K)

TimeSeriesDictOutput_T<K> : TimeSeriesDictOutput : TimeSeriesDict<BaseTimeSeriesOutput>
├── std::unordered_map<K, value_type> _ts_values
├── std::unordered_map<value_type, K> _ts_values_to_keys  # Reverse lookup
├── _modified_items / _removed_items tracking
├── TimeSeriesSetOutput_T<K> _key_set
├── std::vector<TSDKeyObserver<K>*> _key_observers
├── operator[](key) -> get or create
├── py_set_item() / py_del_item()
├── py_get_ref() / py_release_ref()  # Reference counting
├── modified_items() / added_items() / removed_items()
├── was_modified(key) / was_added(key) / was_removed(key)
├── key_set() -> TimeSeriesSetOutput*
└── mark_child_modified()

TimeSeriesDictInput_T<K> : TimeSeriesDictInput : TimeSeriesDict<BaseTimeSeriesInput>
├── Implements TSDKeyObserver<K>
├── _added_items_cache / _removed_item_cache
├── _has_peer flag
└── notify_parent() override
```

**Key Points:**
- Dynamic key creation
- Change tracking per key (added/removed/modified)
- Reference counting for `get_ref()`/`release_ref()`
- Integrated TimeSeriesSet for key tracking

### 4.6 TimeSeriesWindow (TSW) - Time/Tick Windows

**File:** `tsw.h`

```cpp
TimeSeriesFixedWindowOutput<T> : BaseTimeSeriesOutput
├── std::vector<T> _buffer
├── std::vector<engine_time_t> _times
├── size_t _size, _min_size
├── size_t _start, _length           # Circular buffer state
├── value() -> std::vector<T>
├── delta_value() -> std::optional<T>
├── set_value(T)
├── value_times() -> engine_time_t*
├── first_modified_time()
└── all_valid() -> _length >= _min_size

TimeSeriesTimeWindowOutput<T> : BaseTimeSeriesOutput
├── Time-delta based instead of count-based
└── Similar interface

TimeSeriesWindowInput<T>
├── Abstraction over both variants
└── as_fixed_output() / as_time_output()
```

**Key Points:**
- Circular buffer implementation
- Minimum size validation for validity
- Time tracking per value
- Delta returns most recent value only

### 4.7 TimeSeriesReference (REF) - Reference Types

**File:** `ref.h`

```cpp
TimeSeriesReference
├── Immutable reference to another time-series
├── bind_input() / output() / items()
├── is_empty() / is_bound() / is_unbound() / is_valid()
└── operator[]

TimeSeriesReferenceOutput : BaseTimeSeriesOutput
├── TimeSeriesReference _value
└── observe_reference() / stop_observing_reference()

TimeSeriesReferenceInput : BaseTimeSeriesInput
├── value() -> TimeSeriesReference
└── has_value() / raw_value()

Specialized variants for each TS type:
├── TimeSeriesValueReferenceInput/Output
├── TimeSeriesListReferenceInput/Output
├── TimeSeriesBundleReferenceInput/Output
├── TimeSeriesDictReferenceInput/Output
├── TimeSeriesSetReferenceInput/Output
└── TimeSeriesWindowReferenceInput/Output
```

---

## 5. Python Wrapper Architecture

### 5.1 Base Wrappers (`py_time_series.h`)

```cpp
class PyTimeSeriesType {
protected:
    ApiPtr<TimeSeriesType> _impl;      // Smart pointer with control block

    template<typename U>
    U* static_cast_impl() const;       // Access typed implementation

public:
    nb::object value();                 // Delegates to py_value()
    nb::object delta_value();
    nb::bool_ modified() / valid() / all_valid();
    // ... other delegated methods
};

class PyTimeSeriesOutput : public PyTimeSeriesType {
    TimeSeriesOutput* impl() const;
    // Output-specific methods: set_value(), apply_result(), etc.
};

class PyTimeSeriesInput : public PyTimeSeriesType {
    TimeSeriesInput* impl() const;
    // Input-specific methods: bind_output(), make_active(), etc.
};
```

### 5.2 Template-Based Specialization

```cpp
// Pattern used for all collection types
template <typename T_TS, typename T_U>
    requires(/* constraints for Input/Output pairing */)
struct PyTimeSeriesList : T_TS {
    T_U* impl() const { return this->template static_cast_impl<T_U>(); }

    // Python-facing methods
    nb::object get_item(size_t);
    nb::object keys();
    nb::object values();
    // ...
};

// Explicit instantiations
using PyTimeSeriesListOutput = PyTimeSeriesList<PyTimeSeriesOutput, TimeSeriesListOutput>;
using PyTimeSeriesListInput = PyTimeSeriesList<PyTimeSeriesInput, TimeSeriesListInput>;
```

### 5.3 Wrapper Factory (`wrapper_factory.h`)

```cpp
// Functions to wrap C++ objects in Python wrappers
PyTimeSeriesOutput wrap_output(TimeSeriesOutput::s_ptr);
PyTimeSeriesInput wrap_input(TimeSeriesInput::s_ptr);

// Unwrap Python wrappers to C++ pointers
TimeSeriesOutput::s_ptr unwrap_output(PyTimeSeriesOutput&);
TimeSeriesInput::s_ptr unwrap_input(PyTimeSeriesInput&);
```

---

## 6. New Value System (`hgraph::value`)

The new value system provides a type-erased, schema-driven approach:

### 6.1 Core Components

| Component | Purpose |
|-----------|---------|
| `TypeMeta` | Type descriptor (size, alignment, flags, ops) |
| `TypeOps` | Function pointer vtable for type operations |
| `Value` | Owning storage with schema reference |
| `ValueView` / `ConstValueView` | Non-owning mutable/const views |
| `TimeSeriesValue` | Value + ModificationTracker |
| `TimeSeriesValueView` | Auto-tracking view |

### 6.1.1 TypeOps Function Pointers

The `TypeOps` struct provides type-erased operations via function pointers:

| Function | Signature | Purpose |
|----------|-----------|---------|
| `construct` | `void(void*, const TypeMeta*)` | Default construct |
| `destruct` | `void(void*, const TypeMeta*)` | Destroy value |
| `copy_construct` | `void(void*, const void*, const TypeMeta*)` | Copy construct |
| `move_construct` | `void(void*, void*, const TypeMeta*)` | Move construct |
| `copy_assign` | `void(void*, const void*, const TypeMeta*)` | Copy assign |
| `move_assign` | `void(void*, void*, const TypeMeta*)` | Move assign |
| `equals` | `bool(const void*, const void*, const TypeMeta*)` | Equality test |
| `less_than` | `bool(const void*, const void*, const TypeMeta*)` | Ordering |
| `hash` | `size_t(const void*, const TypeMeta*)` | Hash value |
| `to_string` | `std::string(const void*, const TypeMeta*)` | String representation |
| `type_name` | `std::string(const TypeMeta*)` | Python-style type description |
| `to_python` | `nb::object(const void*, const TypeMeta*)` | Python conversion |
| `from_python` | `bool(void*, const TypeMeta*, nb::handle)` | Python import |

### 6.1.2 String Representation

All value types support string conversion for logging and debugging:

**Basic `to_string()`** - Returns value-only representation:
```cpp
// Value/ValueView
value.to_string()  // e.g., "42", "{x=1, y=2}", "[1, 2, 3]"

// TimeSeriesValue/TimeSeriesValueView
ts_value.to_string()  // Same as underlying value
```

**Debug `to_debug_string()`** - Returns detailed representation with modification status:
```cpp
// TimeSeriesValueView (uses stored current_time)
ts_view.to_debug_string()
// Format: TS[type]@addr(value="...", modified=true/false)
// Example: TS[int64_t]@0x7fff5fbff8c0(value="42", modified=true)

// TimeSeriesValue (requires current_time parameter)
ts_value.to_debug_string(current_time)
// Format: TS[type]@addr(value="...", modified=true/false, last_modified=...)
// Example: TS[int64_t]@0x7fff5fbff8c0(value="42", modified=true, last_modified=2025-01-01 12:00:00.000000)
```

**Type-specific formats:**

| TypeKind | Format | Example |
|----------|--------|---------|
| Scalar (bool) | `true`/`false` | `true` |
| Scalar (int) | `<number>` | `42` |
| Scalar (float) | `<number>` | `3.14159` |
| Scalar (string) | `"<value>"` | `"hello"` |
| Bundle | `{field=val, ...}` | `{x=1, y=2.5}` |
| List | `[elem, ...]` | `[1, 2, 3]` |
| Set | `{elem, ...}` | `{1, 2, 3}` |
| Dict | `{key: val, ...}` | `{a: 1, b: 2}` |
| Window | `Window[size=N, newest=val]` | `Window[size=3, newest=42]` |
| Ref (empty) | `REF[empty]` | `REF[empty]` |
| Ref (bound) | `REF[bound: val]` | `REF[bound: 42]` |
| Ref (unbound) | `REF[unbound: N items]` | `REF[unbound: 3 items]` |

### 6.1.3 Type Names (Schema Description)

Get a Python-style description of a type's schema:

```cpp
std::string type_desc = meta->type_name_str();
```

Type names use Python naming conventions (C++ `int64_t` → Python `int`):

| TypeKind | Format | Example |
|----------|--------|---------|
| Scalar (bool) | `bool` | `bool` |
| Scalar (int*) | `int` | `int` |
| Scalar (float/double) | `float` | `float` |
| Scalar (string) | `str` | `str` |
| Bundle (named) | Uses `name` field | `Point` |
| Bundle (anon) | `{field: type, ...}` | `{x: int, y: float}` |
| List | `Tuple[elem, Size[n]]` | `Tuple[int, Size[5]]` |
| Set | `Set[elem]` | `Set[int]` |
| Dict | `Dict[key, value]` | `Dict[str, float]` |
| Window (fixed) | `Window[elem, Size[n]]` | `Window[float, Size[10]]` |
| Window (time) | `Window[elem, timedelta[...]]` | `Window[float, timedelta[seconds=60]]` |
| Ref | `REF[target]` | `REF[int]` |

### 6.2 Type Hierarchy

```
TypeMeta (base)
├── ScalarTypeMeta<T>      # Scalar types
├── BundleTypeMeta         # Named fields (like TSB)
├── ListTypeMeta           # Fixed arrays (like TSL)
├── SetTypeMeta            # Dynamic sets (like TSS)
├── DictTypeMeta           # Key-value maps (like TSD)
├── WindowTypeMeta         # Cyclic/queue windows (like TSW)
└── RefTypeMeta            # References (like REF)
```

### 6.3 Storage Classes

| Class | Purpose |
|-------|---------|
| `SetStorage` | Hash set with contiguous element storage |
| `DictStorage` | SetStorage for keys + parallel value storage |
| `CyclicWindowStorage` | Fixed-size circular buffer |
| `QueueWindowStorage` | Time-based queue with eviction |
| `RefStorage` | EMPTY/BOUND/UNBOUND reference states |

### 6.4 Binding System

| Class | Purpose |
|-------|---------|
| `BoundValue` | Result of schema matching (Peer/Deref/Composite) |
| `DerefTimeSeriesValue` | REF dereferencing wrapper |
| `bind()` | Schema-driven binding function |
| `match_schemas()` | Determines binding strategy |

---

## 7. Integration Analysis

### 7.1 Architectural Comparison

| Aspect | Current System | New Value System |
|--------|---------------|------------------|
| Type representation | C++ templates | Type-erased with TypeMeta |
| Value storage | Per-class members | Unified Value class |
| Modification tracking | Per-class _last_modified_time | ModificationTrackerStorage |
| Python conversion | Per-class py_value() | TypeOps::to_python |
| Collection iteration | Per-class iterators | TypeMeta-driven |
| Schema information | TimeSeriesSchema (TSB only) | TypeMeta for all types |

### 7.2 Key Integration Points

#### 7.2.1 Value Storage Replacement

**Current:** Each specialized type stores values directly
```cpp
// TimeSeriesValueOutput<T>
T _value{};

// TimeSeriesDictOutput_T<K>
std::unordered_map<K, value_type> _ts_values;
```

**New:** Use TimeSeriesValue with appropriate TypeMeta
```cpp
// Could become:
TimeSeriesValue _value;  // Schema-driven storage
```

#### 7.2.2 Modification Tracking Unification

**Current:** Each class tracks _last_modified_time
```cpp
engine_time_t _last_modified_time{MIN_DT};
```

**New:** ModificationTrackerStorage handles all cases
```cpp
TimeSeriesValue _ts;  // Contains both value and tracker
// Access: _ts.modified_at(time), _ts.mark_modified(time)
```

#### 7.2.3 Python Conversion Path

**Current:** Virtual py_value() method per class
```cpp
virtual nb::object py_value() const {
    return nb::cast(_value);
}
```

**New:** TypeOps-driven conversion
```cpp
nb::object py_value() const {
    return value_to_python(_ts.value().data(), _ts.schema());
}
```

### 7.3 Potential Integration Strategies

#### Strategy A: Wrapper Adaptation (Incremental)

Keep existing py_xxx wrappers, adapt C++ implementations to use new value system internally.

**Pros:**
- Minimal changes to Python-facing API
- Can migrate incrementally
- Backward compatible

**Cons:**
- Dual maintenance during transition
- May not leverage full benefits

```
PyTimeSeriesOutput (unchanged)
       │
       ▼
TimeSeriesValueOutput (adapted)
       │
       ▼
TimeSeriesValue (new value system)
```

#### Strategy B: New Wrapper Layer (Clean Break)

Create new wrappers that expose TimeSeriesValue directly.

**Pros:**
- Clean architecture
- Full benefit of new system
- Simpler long-term maintenance

**Cons:**
- Breaking change
- Significant initial effort
- Requires Python-side updates

```
PyTimeSeriesValue (new)
       │
       ▼
TimeSeriesValue (new value system)
```

#### Strategy C: Hybrid (Pragmatic)

Use new value system for storage, keep wrapper interfaces, add adapter layer.

```
PyTimeSeriesOutput (unchanged interface)
       │
       ▼
TimeSeriesOutputAdapter (new)
       │
       ▼
TimeSeriesValue (new value system)
```

### 7.4 Specific Integration Tasks

| Task | Complexity | Dependencies |
|------|------------|--------------|
| Replace TS value storage | Low | None |
| Replace TSL element storage | Medium | List TypeMeta |
| Replace TSB field storage | Medium | Bundle TypeMeta + Schema |
| Replace TSS storage | Medium | Set TypeMeta + Delta |
| Replace TSD storage | High | Dict TypeMeta + Delta + Key observers |
| Replace TSW storage | Medium | Window TypeMeta |
| Replace REF storage | Medium | Ref TypeMeta + Binding |
| Unify modification tracking | Medium | All types |
| Unify Python conversion | Medium | All types |

---

## 8. Node Architecture Constraint

**Critical Finding:** The `Node` class requires `TimeSeriesBundleInput` as the top-level input container.

```cpp
// From node.h lines 224-230, 289
time_series_bundle_input_s_ptr &input();
void set_input(const time_series_bundle_input_s_ptr &value);

// Private member
time_series_bundle_input_s_ptr  _input;  // owned
```

This means:
- **Every node's inputs are wrapped in a TSB**, even single scalar inputs
- TSB uses `TimeSeriesSchema` with field names from `NodeSignature::args`
- Child inputs are created via `TimeSeriesBundleInputBuilder::input_builders`

### Builder Hierarchy

```
NodeBuilder
├── input_builder: TimeSeriesBundleInputBuilder (always TSB)
│   ├── schema: TimeSeriesSchema
│   └── input_builders: vector<InputBuilder>
│       ├── [0] TimeSeriesValueInputBuilder (TS)
│       ├── [1] TimeSeriesListInputBuilder (TSL)
│       └── ...
└── output_builder: OutputBuilder (any type)
```

### Minimum Viable Integration

Due to this constraint, **TS and TSB must be implemented together** for minimal functionality:

1. **TS (TimeSeriesValue)** - For scalar inputs/outputs
2. **TSB (TimeSeriesBundle)** - For node input container
3. **TimeSeriesSchema** - For field name mapping

Without TSB, no nodes can have inputs. Without TS, bundles have no scalar children.

---

## 9. Revised Integration Plan

### Phase 1: Foundation (Required for ANY node)

| Component | Description | New Value Type |
|-----------|-------------|----------------|
| `TimeSeriesValueOutput<T>` | Scalar output | `TimeSeriesValue` with scalar TypeMeta |
| `TimeSeriesValueInput<T>` | Scalar input | Delegates to output's value |
| `TimeSeriesBundleOutput` | Bundle output | `TimeSeriesValue` with BundleTypeMeta |
| `TimeSeriesBundleInput` | Bundle input (node top-level) | Same, uses schema |
| `TimeSeriesSchema` | Field names | Maps to BundleTypeMeta field names |

**Integration Approach:**

```cpp
// Current: Template-based, direct storage
class TimeSeriesValueOutput<T> : BaseTimeSeriesOutput {
    T _value{};
};

// New: Type-erased, schema-driven
class TimeSeriesValueOutput : BaseTimeSeriesOutput {
    TimeSeriesValue _ts;  // Holds value + tracker
    // TypeMeta comes from builder, passed at construction
};
```

### Phase 2: Collections

| Component | Description | Dependency |
|-----------|-------------|------------|
| `TimeSeriesListOutput/Input` | Fixed-size indexed | Phase 1 + ListTypeMeta |
| `TimeSeriesSetOutput/Input` | Dynamic set | Phase 1 + SetTypeMeta + SetStorage |
| `TimeSeriesDictOutput/Input` | Dynamic dict | Phase 2 (TSS) + DictTypeMeta + DictStorage |

### Phase 3: Windows & References

| Component | Description | Dependency |
|-----------|-------------|------------|
| `TimeSeriesWindowOutput/Input` | Cyclic/time windows | Phase 1 + WindowTypeMeta |
| `TimeSeriesReferenceOutput/Input` | REF types | Phase 1 + RefTypeMeta + Binding |

### Phase 4: Python Wrappers

Once C++ types are integrated, update wrappers:
- Keep `PyTimeSeriesOutput/Input` interfaces unchanged
- Update `impl()` to work with new storage
- Verify `py_value()` / `py_delta_value()` work via TypeOps

---

## 10. Detailed Phase 1 Tasks

### 10.1 TimeSeriesValue (TS) Integration

1. **Modify `TimeSeriesValueOutput`** to use `TimeSeriesValue`:
   - Replace `T _value{}` with `TimeSeriesValue _ts`
   - Constructor takes `const TypeMeta*` instead of being templated
   - `value()` returns `ConstValueView` or typed reference
   - `set_value()` uses `TimeSeriesValueView`
   - `py_value()` uses `value_to_python()`

2. **Modify `TimeSeriesValueInput`**:
   - Delegates to output's new storage
   - No storage of its own (current design)

3. **Update `TimeSeriesValueInputBuilder` / `TimeSeriesValueOutputBuilder`**:
   - Accept `const TypeMeta*` instead of type template
   - Pass TypeMeta to constructed instances

### 10.2 TimeSeriesBundle (TSB) Integration

1. **Modify `TimeSeriesBundleOutput`**:
   - Replace indexed storage with `TimeSeriesValue` using BundleTypeMeta
   - Child outputs stored via BundleTypeMeta field layout
   - Schema maps field names to indices

2. **Modify `TimeSeriesBundleInput`**:
   - Same pattern as output
   - Keep string-based access via schema

3. **Update builders**:
   - `TimeSeriesBundleInputBuilder` creates BundleTypeMeta from schema + child builders
   - `TimeSeriesBundleOutputBuilder` same pattern

### 10.3 Schema Mapping

```cpp
// Current TimeSeriesSchema
struct TimeSeriesSchema {
    std::vector<std::string> _keys;
    nb::object _scalar_type;
};

// Maps to BundleTypeMeta
struct BundleTypeMeta : TypeMeta {
    std::vector<FieldMeta> fields;                    // name (std::string), offset, type
    std::unordered_map<std::string, size_t> name_to_index;
};
```

Both use `std::string` for field names, so the mapping is straightforward. The schema's `_keys` directly populate `fields[i].name`.

---

## 11. Builder Infrastructure Analysis

### 11.1 Current Builder Pattern

The builder infrastructure creates time-series instances with template-based type selection:

```
Python wiring layer
       │
       ▼
NodeBuilder
├── input_builder: InputBuilder (abstract)
│   └── TimeSeriesBundleInputBuilder
│       └── input_builders: vector<InputBuilder>
│           ├── TimeSeriesValueInputBuilder<bool>
│           ├── TimeSeriesValueInputBuilder<int64_t>
│           └── ... (explicit template instantiations)
│
└── output_builder: OutputBuilder (abstract)
    └── TimeSeriesValueOutputBuilder<T>
        └── make_instance() → TimeSeriesValueOutput<T>
```

### 11.2 Current Template Instantiations

From `time_series_value_output_builder.cpp`:

```cpp
// Explicit template instantiations - these are the only supported types
template struct TimeSeriesValueOutputBuilder<bool>;
template struct TimeSeriesValueOutputBuilder<int64_t>;
template struct TimeSeriesValueOutputBuilder<double>;
template struct TimeSeriesValueOutputBuilder<engine_date_t>;
template struct TimeSeriesValueOutputBuilder<engine_time_t>;
template struct TimeSeriesValueOutputBuilder<engine_time_delta_t>;
template struct TimeSeriesValueOutputBuilder<nb::object>;  // Catch-all for Python objects
```

Python bindings expose these as separate classes:
- `OutputBuilder_TS_Bool`
- `OutputBuilder_TS_Int`
- `OutputBuilder_TS_Float`
- `OutputBuilder_TS_Date`
- `OutputBuilder_TS_DateTime`
- `OutputBuilder_TS_TimeDelta`
- `OutputBuilder_TS_Object`

### 11.3 Builder make_instance Flow

```cpp
// OutputBuilder interface
virtual time_series_output_s_ptr make_instance(node_ptr owning_node) const = 0;
virtual time_series_output_s_ptr make_instance(time_series_output_ptr owning_output) const = 0;

// Current implementation (templated)
template<typename T>
time_series_output_s_ptr TimeSeriesValueOutputBuilder<T>::make_instance(node_ptr owning_node) const {
    return arena_make_shared_as<TimeSeriesValueOutput<T>, TimeSeriesOutput>(owning_node);
}
```

### 11.4 Unified Builder Design

Since `TypeMeta::kind` identifies the type category, a **single builder class** can handle all time-series types:

```cpp
// TypeKind → Time-Series Type mapping
enum class TypeKind : uint8_t {
    Scalar,  // → TS (TimeSeriesValue)
    List,    // → TSL (TimeSeriesList)
    Set,     // → TSS (TimeSeriesSet)
    Dict,    // → TSD (TimeSeriesDict)
    Bundle,  // → TSB (TimeSeriesBundle)
    Ref,     // → REF (TimeSeriesReference)
    Window,  // → TSW (TimeSeriesWindow)
};
```

### 11.5 Single TimeSeriesBuilder

```cpp
// types/value/ts_builder.h
namespace hgraph::value {

struct TimeSeriesOutputBuilder : OutputBuilder {
    const TypeMeta* schema;  // Complete type information

    explicit TimeSeriesOutputBuilder(const TypeMeta* schema) : schema(schema) {}

    time_series_output_s_ptr make_instance(node_ptr owning_node) const override {
        switch (schema->kind) {
            case TypeKind::Scalar:
                return make_scalar_output(owning_node);
            case TypeKind::Bundle:
                return make_bundle_output(owning_node);
            case TypeKind::List:
                return make_list_output(owning_node);
            case TypeKind::Set:
                return make_set_output(owning_node);
            case TypeKind::Dict:
                return make_dict_output(owning_node);
            case TypeKind::Window:
                return make_window_output(owning_node);
            case TypeKind::Ref:
                return make_ref_output(owning_node);
        }
    }

private:
    time_series_output_s_ptr make_scalar_output(node_ptr node) const {
        return arena_make_shared_as<ValueTimeSeriesOutput, TimeSeriesOutput>(node, schema);
    }

    time_series_output_s_ptr make_bundle_output(node_ptr node) const {
        auto* bundle_meta = static_cast<const BundleTypeMeta*>(schema);
        return arena_make_shared_as<ValueTimeSeriesBundleOutput, TimeSeriesOutput>(node, bundle_meta);
    }

    // ... similar for other kinds
};

struct TimeSeriesInputBuilder : InputBuilder {
    const TypeMeta* schema;

    explicit TimeSeriesInputBuilder(const TypeMeta* schema) : schema(schema) {}

    time_series_input_s_ptr make_instance(node_ptr owning_node) const override {
        // Same pattern - dispatch on schema->kind
    }
};

} // namespace hgraph::value
```

### 11.6 Builder Hierarchy Simplification

**Current:** One builder class per time-series type × per value type
```
TimeSeriesValueOutputBuilder<bool>
TimeSeriesValueOutputBuilder<int64_t>
TimeSeriesValueOutputBuilder<double>
TimeSeriesValueOutputBuilder<nb::object>
TimeSeriesBundleOutputBuilder
TimeSeriesListOutputBuilder
TimeSeriesSetOutputBuilder
TimeSeriesDictOutputBuilder
TimeSeriesWindowOutputBuilder
TimeSeriesReferenceOutputBuilder
... (×2 for Input variants)
```

**New:** Two builder classes total
```
TimeSeriesOutputBuilder(schema)   // Handles all output types
TimeSeriesInputBuilder(schema)    // Handles all input types
```

### 11.7 Schema Construction

The TypeMeta schema is built from Python type metadata during wiring:

```python
# Python side - during node wiring
def create_builder(ts_type: HgTimeSeriesTypeMetaData) -> OutputBuilder:
    schema = ts_type.to_type_meta()  # Convert Python type info to TypeMeta
    return TimeSeriesOutputBuilder(schema)
```

For composite types, the schema recursively contains child schemas:

```cpp
// BundleTypeMeta contains field types
struct BundleTypeMeta : TypeMeta {
    std::vector<FieldMeta> fields;  // Each field has name + TypeMeta*
};

// ListTypeMeta contains element type
struct ListTypeMeta : TypeMeta {
    const TypeMeta* element_type;
    size_t count;  // Fixed size
};

// DictTypeMeta contains key and value types
struct DictTypeMeta : TypeMeta {
    const TypeMeta* key_type;
    const TypeMeta* value_type;
};
```

### 11.8 Recursive Bundle Construction

For a node with inputs `(ts: TS[int], data: TSB[x: TS[float], y: TS[str]])`:

```cpp
// Top-level: BundleTypeMeta for node inputs
BundleTypeMeta* node_input_schema = BundleTypeBuilder()
    .add_field("ts", int_type())           // Scalar field
    .add_field("data", BundleTypeBuilder() // Nested bundle field
        .add_field("x", float_type())
        .add_field("y", string_type())
        .build())
    .build();

// Single builder creates entire input tree
auto input_builder = TimeSeriesInputBuilder(node_input_schema);
auto node_input = input_builder.make_instance(owning_node);
// node_input is a ValueTimeSeriesBundleInput containing:
//   - "ts": ValueTimeSeriesInput (scalar)
//   - "data": ValueTimeSeriesBundleInput containing:
//       - "x": ValueTimeSeriesInput (scalar)
//       - "y": ValueTimeSeriesInput (scalar)
```

### 11.9 Benefits

1. **Single point of creation** - All time-series construction goes through one builder
2. **Schema-driven** - Type information completely describes the structure
3. **Recursive** - Composite types naturally handle nesting
4. **No templates** - Type-erased throughout
5. **Simpler Python binding** - Just expose `TimeSeriesOutputBuilder` and `TimeSeriesInputBuilder`

---

## 12. Migration Strategy

### 12.1 Key Principles

1. **Retain PyXXX wrappers** - Keep all `py_*.h` / `py_*.cpp` files in `api/python/`
2. **Don't modify existing types** - Use existing `ts.h`, `tsl.h`, etc. only for reference
3. **Create new implementations** - New classes in `types/value/` folder
4. **Integration in `api/python/`** - Wrapper adaptations only

### 12.2 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Python Layer                              │
│  (Unchanged Python code using existing API)                  │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│              PyXXX Wrappers (api/python/)                    │
│  PyTimeSeriesOutput, PyTimeSeriesInput, PyTimeSeriesList...  │
│  (Keep these - update to wrap new implementations)           │
└─────────────────────────────┬───────────────────────────────┘
                              │ Wraps
┌─────────────────────────────▼───────────────────────────────┐
│           NEW Implementation Classes (types/value/)          │
│  ValueTimeSeriesOutput, ValueTimeSeriesInput,                │
│  ValueTimeSeriesList, ValueTimeSeriesBundle...               │
│  (New type-erased implementations using TimeSeriesValue)     │
└─────────────────────────────┬───────────────────────────────┘
                              │ Uses
┌─────────────────────────────▼───────────────────────────────┐
│              Value System (types/value/)                     │
│  TimeSeriesValue, TypeMeta, TypeRegistry,                    │
│  SetStorage, DictStorage, etc.                               │
└─────────────────────────────────────────────────────────────┘
```

### 12.3 File Organization

**New files to create in `types/value/`:**
```
types/value/
├── (existing type system files)
├── ts_output.h          # ValueTimeSeriesOutput - new TS output impl
├── ts_input.h           # ValueTimeSeriesInput - new TS input impl
├── ts_bundle_output.h   # ValueTimeSeriesBundleOutput
├── ts_bundle_input.h    # ValueTimeSeriesBundleInput
├── ts_list_output.h     # ValueTimeSeriesListOutput
├── ts_list_input.h      # ValueTimeSeriesListInput
└── ... (other TS types)
```

**Modifications in `api/python/`:**
```
api/python/
├── py_ts.h              # Update to wrap new Value* classes
├── py_tsl.h             # Update to wrap new Value* classes
├── py_tsb.h             # Update to wrap new Value* classes
└── ...
```

### 12.4 Wrapper Adaptation Pattern

The PyXXX wrappers currently hold `ApiPtr<TimeSeriesType>`. They will be updated to wrap the new implementations:

```cpp
// Current: wraps template-based TimeSeriesValueOutput<T>
class PyTimeSeriesValueOutput : public PyTimeSeriesOutput {
    // impl() returns TimeSeriesValueOutput<T>*
};

// New: wraps type-erased ValueTimeSeriesOutput
class PyTimeSeriesValueOutput : public PyTimeSeriesOutput {
    // impl() returns ValueTimeSeriesOutput*
    // ValueTimeSeriesOutput uses TimeSeriesValue internally
};
```

### 12.5 New Implementation Classes

Each new implementation class in `types/value/` will:
1. Implement the same interface as the existing type (for wrapper compatibility)
2. Use `TimeSeriesValue` for storage instead of direct member storage
3. Use `TypeMeta` for type information
4. Use `TypeOps` for Python conversion

Example structure:

```cpp
// types/value/ts_output.h
namespace hgraph::value {

class ValueTimeSeriesOutput {
public:
    ValueTimeSeriesOutput(node_ptr owning_node, const TypeMeta* value_type);

    // Interface matching existing TimeSeriesValueOutput
    ConstValueView value() const;
    void set_value(const nb::object& py_value);  // Uses TypeOps::from_python
    nb::object py_value() const;                  // Uses TypeOps::to_python
    nb::object py_delta_value() const;
    bool modified() const;
    bool valid() const;
    engine_time_t last_modified_time() const;
    void mark_modified(engine_time_t time);
    // ... other methods

private:
    TimeSeriesValue _ts;           // Type-erased storage + tracker
    node_ptr _owning_node;
    // ... other members for subscription, etc.
};

} // namespace hgraph::value
```

### 12.6 Python HgTypeMetaData Integration

The Python `HgTypeMetaData` system already contains all information needed to construct C++ `TypeMeta`:

#### Current Hierarchy

```
HgTypeMetaData (base)
├── HgScalarTypeMetaData      # int, float, str, datetime, etc.
└── HgTimeSeriesTypeMetaData  # Time-series types
    ├── HgTSTypeMetaData      # TS[scalar] - has value_scalar_tp
    ├── HgTSBTypeMetaData     # TSB[schema] - has bundle_schema_tp
    ├── HgTSLTypeMetaData     # TSL[value, size] - has value_tp, size_tp
    ├── HgTSSTypeMetaData     # TSS[key] - has value_scalar_tp
    ├── HgTSDTypeMetaData     # TSD[key, value] - has key_tp, value_tp
    ├── HgTSWTypeMetaData     # TSW[value] - has value_scalar_tp, size_tp
    └── HgREFTypeMetaData     # REF[ts] - has value_tp
```

#### Mapping to C++ TypeMeta

| Python Class | C++ TypeMeta | Key Properties |
|--------------|--------------|----------------|
| `HgScalarTypeMetaData` | `ScalarTypeMeta<T>` | `py_type` |
| `HgTSTypeMetaData` | `ScalarTypeMeta<T>` | `value_scalar_tp.py_type` |
| `HgTSBTypeMetaData` | `BundleTypeMeta` | `bundle_schema_tp.meta_data_schema` |
| `HgTSLTypeMetaData` | `ListTypeMeta` | `value_tp`, `size_tp.py_type.SIZE` |
| `HgTSSTypeMetaData` | `SetTypeMeta` | `value_scalar_tp` |
| `HgTSDTypeMetaData` | `DictTypeMeta` | `key_tp`, `value_tp` |
| `HgTSWTypeMetaData` | `WindowTypeMeta` | `value_scalar_tp`, `size_tp` |
| `HgREFTypeMetaData` | `RefTypeMeta` | `value_tp` |

#### Proposed Extension

Add a method/property to each `HgTypeMetaData` class to get or create the corresponding C++ `TypeMeta`:

```python
# In HgTypeMetaData base class
class HgTypeMetaData:
    _cpp_type_meta: "TypeMeta" = None  # Cached C++ type meta

    def cpp_type_meta(self) -> "TypeMeta":
        """Get or create the C++ TypeMeta for this type."""
        if self._cpp_type_meta is None:
            self._cpp_type_meta = self._create_cpp_type_meta()
        return self._cpp_type_meta

    def _create_cpp_type_meta(self) -> "TypeMeta":
        """Override in subclasses to create appropriate TypeMeta."""
        raise NotImplementedError()
```

```python
# In HgTSTypeMetaData
class HgTSTypeMetaData(HgTimeSeriesTypeMetaData):
    def _create_cpp_type_meta(self) -> "TypeMeta":
        from hgraph._hgraph import TypeRegistry
        # Get scalar type from registry based on py_type
        return TypeRegistry.instance().get_scalar_type(self.value_scalar_tp.py_type)
```

```python
# In HgTSBTypeMetaData
class HgTSBTypeMetaData(HgTimeSeriesTypeMetaData):
    def _create_cpp_type_meta(self) -> "TypeMeta":
        from hgraph._hgraph import BundleTypeBuilder
        builder = BundleTypeBuilder()
        for key, field_tp in self.bundle_schema_tp.meta_data_schema.items():
            builder.add_field(key, field_tp.cpp_type_meta())  # Recursive
        return builder.build()
```

#### Simplified Builder Factory

With `cpp_type_meta()` available, the builder factory becomes trivial:

```python
# Current: dispatch on type(value_tp), create specific builder
class PythonTimeSeriesBuilderFactory:
    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        return {
            HgTSTypeMetaData: lambda: PythonTSInputBuilder(...),
            HgTSBTypeMetaData: lambda: PythonTSBInputBuilder(...),
            # ... many more cases
        }.get(type(value_tp), ...)()

# New: single unified builder with TypeMeta
class CppTimeSeriesBuilderFactory:
    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        return TimeSeriesInputBuilder(value_tp.cpp_type_meta())

    def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
        return TimeSeriesOutputBuilder(value_tp.cpp_type_meta())
```

#### TypeRegistry Caching

The `TypeRegistry` handles caching to avoid recreating TypeMeta:

```cpp
// C++ side
class TypeRegistry {
public:
    // Get or create scalar type for Python type
    const TypeMeta* get_or_create_scalar(nb::type_object py_type);

    // Get or create bundle type from schema
    const BundleTypeMeta* get_or_create_bundle(const std::vector<FieldSpec>& fields);

    // ... similar for other types
private:
    std::unordered_map<size_t, std::unique_ptr<TypeMeta>> _type_cache;
};
```

### 12.7 Decisions

1. **TypeMeta Ownership**: All TypeMeta instances owned by TypeRegistry
2. **Code Location**: New implementations in `types/value/`, integration in `api/python/`
3. **Existing Code**: Reference only, do not modify
4. **Type Conversion**: Use `python_conversion.h` functions at value boundaries
5. **Python Integration**: Add `cpp_type_meta()` method to HgTypeMetaData classes

---

## 13. File Reference

### Current Implementation

| File | Contents |
|------|----------|
| `cpp/include/hgraph/types/time_series_type.h` | Abstract interfaces |
| `cpp/include/hgraph/types/base_time_series.h` | Base implementations |
| `cpp/include/hgraph/types/ts.h` | Scalar types |
| `cpp/include/hgraph/types/tsl.h` | Lists |
| `cpp/include/hgraph/types/tsb.h` | Bundles |
| `cpp/include/hgraph/types/tss.h` | Sets |
| `cpp/include/hgraph/types/tsd.h` | Dicts |
| `cpp/include/hgraph/types/tsw.h` | Windows |
| `cpp/include/hgraph/types/ref.h` | References |
| `cpp/include/hgraph/types/node.h` | Node (requires TSB input) |
| `cpp/include/hgraph/builders/*.h` | Builder classes |
| `cpp/include/hgraph/api/python/py_*.h` | Python wrappers |

### New Value System

| File | Contents |
|------|----------|
| `cpp/include/hgraph/types/value/type_meta.h` | Core type descriptors |
| `cpp/include/hgraph/types/value/scalar_type.h` | Scalar types |
| `cpp/include/hgraph/types/value/bundle_type.h` | Bundle types |
| `cpp/include/hgraph/types/value/list_type.h` | List types |
| `cpp/include/hgraph/types/value/set_type.h` | Set types + SetStorage |
| `cpp/include/hgraph/types/value/dict_type.h` | Dict types + DictStorage |
| `cpp/include/hgraph/types/value/window_type.h` | Window types + storage |
| `cpp/include/hgraph/types/value/ref_type.h` | Ref types + RefStorage |
| `cpp/include/hgraph/types/value/value.h` | Value, ValueView classes |
| `cpp/include/hgraph/types/value/time_series_value.h` | TimeSeriesValue |
| `cpp/include/hgraph/types/value/modification_tracker.h` | Tracking |
| `cpp/include/hgraph/types/value/bind.h` | Schema binding + delta |
| `cpp/include/hgraph/types/value/python_conversion.h` | Python ops |

---

## 14. V2 Time-Series Implementation (Current)

**Status:** Implemented and tested as of 2025-12-17

The V2 implementation provides a new time-series input/output system using the type-erased value system. This section documents what has been built.

### 14.1 File Locations

```
cpp/include/hgraph/types/time_series/
├── ts_output.h          # TSOutput, TSOutputView, NavigationPath
├── ts_input.h           # TSInput, TSInputView
├── access_strategy.h    # AccessStrategy hierarchy
├── TS_DESIGN.md         # This design document
└── TS_USER_GUIDE.md     # User guide

cpp/src/cpp/types/time_series/
├── ts_output.cpp        # TSOutput implementation
├── ts_input.cpp         # TSInput implementation
└── access_strategy.cpp  # Strategy implementations

cpp/tests/
└── test_ts_input.cpp    # Unit tests (34 tests, 115 assertions)
```

### 14.2 Core Classes

#### TSOutput - Time-series output with type-erased storage

```cpp
namespace hgraph::ts {

class TSOutput {
public:
    TSOutput(const TimeSeriesTypeMeta* meta, node_ptr owning_node);

    // Owns a TimeSeriesValue (type-erased storage + modification tracking)
    value::TimeSeriesValue _value;

    // View creation with path tracking
    TSOutputView view();

    // Direct value access
    template<typename T> void set_value(const T& val, engine_time_t time);
    template<typename T> const T& as() const;

    // Subscription support
    void subscribe(Notifiable* notifiable);
    void unsubscribe(Notifiable* notifiable);
};

} // namespace hgraph::ts
```

#### TSInput - Time-series input with hierarchical binding

```cpp
class TSInput : public Notifiable {
public:
    TSInput(const TimeSeriesTypeMeta* meta, node_ptr owning_node);

    // Binding uses AccessStrategy tree
    void bind_output(TSOutput* output);
    void unbind_output();

    // Activation controls subscription
    void make_active();
    void make_passive();

    // Value access (delegates to strategy)
    value::ConstValueView value() const;
    TSInputView view() const;
};
```

### 14.3 View Classes with Path Tracking

Views provide navigation into nested structures while tracking the navigation path for debugging.

```cpp
// Navigation path tracking
struct PathSegment {
    enum class Kind { Field, Element };
    Kind kind;
    std::variant<size_t, std::string> index_value;
    const TimeSeriesTypeMeta* meta;
};

class NavigationPath {
    const TSOutput* _root;
    std::vector<PathSegment> _segments;

    std::string to_string() const;  // e.g., "root.field(\"price\").element(0)"
};

// Output view (mutable)
class TSOutputView {
    value::TimeSeriesValueView _value_view;
    NavigationPath _path;

    // Chainable navigation
    TSOutputView field(size_t index) &;
    TSOutputView field(size_t index) &&;  // Move-efficient
    TSOutputView element(size_t index) &;
    TSOutputView element(size_t index) &&;

    // Mutation with explicit time
    template<typename T> void set(const T& val, engine_time_t time);
};

// Input view (read-only)
class TSInputView {
    value::ConstValueView _value_view;
    value::ModificationTracker _tracker;
    NavigationPath _path;

    // Chainable navigation (read-only)
    TSInputView field(size_t index) &;
    TSInputView element(size_t index) &;
};
```

### 14.4 AccessStrategy Hierarchy

The key innovation in V2 is the **hierarchical access strategy** pattern for binding inputs to outputs. This handles complex type transformations like REF redistribution.

```
AccessStrategy (abstract base)
├── bind() / rebind() / unbind()
├── make_active() / make_passive()
├── value() / tracker()
└── modified_at() / has_value()

DirectAccessStrategy : AccessStrategy
├── Simple delegation to bound output
├── No transformation needed
└── Most common case (matching types)

CollectionAccessStrategy : AccessStrategy
├── Has child strategies for elements
├── Used for TSL, TSB when children need different strategies
├── May have storage if children transform values
└── child_count(), child(index), set_child()

RefObserverAccessStrategy : AccessStrategy
├── Non-REF input bound to REF output
├── Observes reference changes, rebinds child
├── ALWAYS subscribed to REF output
├── Has child strategy for dereferenced value
└── on_reference_changed() callback

RefWrapperAccessStrategy : AccessStrategy
├── REF input bound to non-REF output
├── Creates REF value wrapping the output
├── Has storage for the REF value
└── Does NOT subscribe to wrapped output
```

### 14.5 Strategy Selection Algorithm

The `build_access_strategy()` function creates the appropriate strategy tree by comparing input and output schemas:

```cpp
std::unique_ptr<AccessStrategy> build_access_strategy(
    const TimeSeriesTypeMeta* input_meta,
    const TimeSeriesTypeMeta* output_meta,
    TSInput* owner);
```

**Decision tree:**

1. **Types match exactly** → `DirectAccessStrategy`
2. **Input is non-REF, output is REF** → `RefObserverAccessStrategy` with child strategy
3. **Input is REF, output is non-REF** → `RefWrapperAccessStrategy`
4. **Both are collections (TSL/TSB)** → `CollectionAccessStrategy` with child strategies
5. **Nested differences** → Recursive strategy building

### 14.6 Stacked Strategy Pattern

The V2 system supports complex type transformations through strategy stacking. For example:

**Scenario:** `TSD[str, REF[TSL[TS[int], Size[2]]]]` output bound to `TSD[str, TSL[REF[TS[int]], Size[2]]]` input

The REF is at different positions:
- Output: REF wraps the entire TSL
- Input: REF wraps each element inside the TSL

**Strategy tree:**

```
CollectionAccessStrategy (TSD level)
└── for each dict value:
    RefObserverAccessStrategy (dereference outer REF)
    └── CollectionAccessStrategy (TSL level)
        └── for each list element:
            RefWrapperAccessStrategy (wrap TS[int] as REF)
```

This enables "REF redistribution" - the strategy tree handles the mismatch by:
1. Dereferencing the outer REF (RefObserver)
2. Navigating into the dereferenced TSL (CollectionAccess)
3. Wrapping each element as a new REF (RefWrapper)

### 14.7 Subscription Rules

Different strategies have different subscription behaviors:

| Strategy | When Subscribed | What It Subscribes To |
|----------|-----------------|----------------------|
| `DirectAccess` | When active | Bound output |
| `CollectionAccess` | Propagates to children | Children handle subscriptions |
| `RefObserver` | **Always** (at bind time) | REF output (for reference changes) |
| `RefWrapper` | Never | Nothing (only tracks binding) |

The key insight: `RefObserverAccessStrategy` must always be subscribed to the REF output to detect when the reference changes, regardless of whether the input is active.

### 14.8 Test Coverage

**File:** `cpp/tests/test_ts_input.cpp`

**Statistics:** 34 tests, 115 assertions

**Test categories:**

| Category | Tests |
|----------|-------|
| Basic TSOutput creation and properties | 6 |
| Basic TSInput creation and properties | 4 |
| DirectAccessStrategy | 3 |
| CollectionAccessStrategy (TSL) | 3 |
| CollectionAccessStrategy (TSB) | 1 |
| Stacked: REF inside collection | 2 |
| Stacked: Collection inside REF | 2 |
| Complex multi-level strategies | 3 |
| Strategy tree verification | 2 |
| Edge cases (null metadata) | 3 |
| Activation/subscription | 2 |
| String representation | 3 |

**Test metadata types defined:**

```cpp
TestTSIntMeta          // TS[int]
TestTSStringMeta       // TS[string]
TestREFTSIntMeta       // REF[TS[int]]
TestTSLMeta            // TSL[TS[int], Size[2]]
TestTSBMeta            // TSB[x: TS[int], y: TS[string]]
TestTSLOfRefMeta       // TSL[REF[TS[int]], Size[2]]
TestREFTSLMeta         // REF[TSL[TS[int], Size[2]]]
TestTSDMeta            // TSD[str, TS[int]]
TestTSDOfRefTSLMeta    // TSD[str, REF[TSL[TS[int], Size[2]]]]
TestTSDOfTSLMeta       // TSD[str, TSL[TS[int], Size[2]]]
TestTSDOfTSLOfRefMeta  // TSD[str, TSL[REF[TS[int]], Size[2]]]
```

### 14.9 Building and Running Tests

```bash
# Configure with tests enabled
cmake -B cmake-build-test-v2 -S cpp -DHGRAPH_BUILD_TESTS=ON

# Build the test executable
cmake --build cmake-build-test-v2 --target hgraph_ts_input_tests

# Run tests
./cmake-build-test-v2/tests/hgraph_ts_input_tests
```

### 14.10 Design Principles

1. **Output owns data, input reads it** - TSOutput has TimeSeriesValue storage; TSInput delegates to its bound output via AccessStrategy

2. **Time as explicit parameter** - All mutation methods take `engine_time_t` as a parameter, avoiding stale time issues

3. **Path tracking for debugging** - Views know where they came from (NavigationPath) for better error messages and debugging

4. **Strategy tree mirrors type tree** - The AccessStrategy hierarchy mirrors the type hierarchy, handling transformations at each level

5. **Lazy storage allocation** - CollectionAccessStrategy only creates storage when children actually need transformation (has_storage())

6. **Move-efficient navigation** - Views have rvalue overloads that move the path instead of copying for chains like `output.view().field("x").element(0)`

### 14.11 Future Work

- [ ] Python wrapper integration (wrap TSOutput/TSInput in PyXXX classes)
- [ ] TSD (dict) dynamic element handling
- [ ] TSS (set) support
- [ ] TSW (window) support
- [ ] Full REF semantics (bound/unbound states)
- [ ] Integration with Node class
