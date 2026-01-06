# Time-Series Value (TSValue) - Design Document

**Version**: 0.1 (Draft)
**Date**: 2025-01-05
**Status**: Design
**Related**: [TSValue_USER_GUIDE.md](TSValue_USER_GUIDE.md) - User Guide

---

## Table of Contents

1. [Overview](#1-overview)
2. [Design Principles](#2-design-principles)
3. [Time-Series Schema System](#3-time-series-schema-system)
4. [Architecture](#4-architecture)
5. [TSValue Structure](#5-tsvalue-structure)
6. [View System](#6-view-system)
7. [State Machines](#7-state-machines)
8. [Notification Protocol](#8-notification-protocol)
9. [Value vs View in Composites](#9-value-vs-view-in-composites)
10. [Memory Layout](#10-memory-layout)
11. [Integration with Existing Infrastructure](#11-integration-with-existing-infrastructure)
12. [Python Integration](#12-python-integration)
13. [Testing Strategy](#13-testing-strategy)
14. [Migration Path](#14-migration-path)

**Appendices:**
- [Appendix A: File Structure](#appendix-a-file-structure)
- [Appendix B: Invariants](#appendix-b-invariants)
- [Appendix C: Behavioral Parity](#appendix-c-behavioral-parity-with-current-implementation)
- [Appendix D: Integration Strategy](#appendix-d-integration-strategy---combining-current-design-with-type-improvements)
- [Appendix E: Path-Based Identification System](#appendix-e-path-based-identification-system)

---

## 1. Overview

TSValue is the unified type-erased time-series value type for hgraph. It replaces the current templated time-series types with a single type-erased implementation.

### 1.1 Goals

1. **Type erasure** - Single TSValue type handles all time-series kinds
2. **Build on existing infrastructure** - Reuse Value, Policy, Path systems
3. **Minimal duplication** - Delegate to existing mechanisms
4. **Clear separation** - TSValue owns data, Views provide access patterns

### 1.2 Non-Goals

- Replacing the Value type system (TSValue uses it)
- Changing Python API semantics (just internal implementation)
- Supporting new time-series kinds (focus on TS, TSB first)

---

## 2. Design Principles

| Principle | Application |
|-----------|-------------|
| **Leverage Existing Infrastructure** | Use Value, Policy, Path, Notifiable systems |
| **Minimal State** | TSValue has only what it needs, views are lightweight |
| **Policy-Based Extension** | Optional features via policies, not inheritance |
| **Navigation over Storage** | View parent chain via paths, not stored pointers |

---

## 3. Time-Series Schema System

The time-series system uses a **dual-schema architecture** to separate data storage from time-series semantics.

### 3.1 The Dual-Schema Concept

| Schema Type | Purpose | Examples |
|-------------|---------|----------|
| **Scalar Schema** (`TypeMeta`) | Describes atomic data storage | `scalar<int>`, `scalar<str>` |
| **TS Schema** (`TSTypeMeta`) | Describes time-series structure | `ts<int>`, `tsb{...}` |

**Key Distinction:**
- **Atomic values** (int, float, str, etc.) are described by `TypeMeta` (scalar schema)
- **Time-series values** wrap atomic values and add modification tracking

**Time-Series Hierarchy:**

| Type | Description | Element Type |
|------|-------------|--------------|
| `TS<T>` | **Base case** - time-series of atomic type T | Scalar (atomic) |
| `TSB[...]` | Bundle with named time-series fields | Each field is a TS |
| `TSL<TS<T>>` | List of time-series values | Elements are TS |
| `TSD<K, TS<V>>` | Dict mapping scalar keys to time-series values | Values are TS |
| `TSS<T>` | Set time-series (set of scalar values) | Scalars (atomic) |
| `TSW<TS<T>>` | Window over a time-series (fixed or time-based) | Underlying TS |
| `REF<ts>` | Reference to another time-series | Any TS schema |

Each time-series element tracks its own modification state independently.

### 3.2 TSMeta Structure

**Key Insight from `type-improvements` Branch:** The type meta acts as both a **type descriptor** and a **builder**. Each TSMeta knows how to construct instances of the time-series type it represents.

```cpp
// Time-series kind classification
enum class TSKind : uint8_t {
    TS,      // TS<T> - single scalar time-series
    TSS,     // TSS<T> - time-series set
    TSD,     // TSD<K, V> - time-series dict
    TSL,     // TSL<V, Size> - time-series list
    TSB,     // TSB[Schema] - time-series bundle
    TSW,     // TSW<T, Size> - time-series window
    REF,     // REF<TS_TYPE> - time-series reference
    SIGNAL,  // SIGNAL - input-only, ticked indicator
};

/**
 * TSMeta - Base structure for all time-series type metadata
 *
 * Acts as both a type descriptor AND a builder. Each concrete type
 * implements make_output/make_input to efficiently construct instances.
 */
struct TSMeta {
    TSKind ts_kind;
    const char* name{nullptr};

    virtual ~TSMeta() = default;

    // Type name generation (e.g., "TS[int]", "TSB[price: TS[float]]")
    [[nodiscard]] virtual std::string type_name_str() const = 0;

    // Builder methods - TSMeta knows how to create its own instances
    [[nodiscard]] virtual time_series_output_s_ptr make_output(node_ptr owning_node) const = 0;
    [[nodiscard]] virtual time_series_input_s_ptr make_input(node_ptr owning_node) const = 0;

    // Schema access
    [[nodiscard]] virtual const value::TypeMeta* value_schema() const { return nullptr; }

    // Navigation helpers for composite types
    [[nodiscard]] virtual const TSMeta* field_meta(size_t index) const { return nullptr; }
    [[nodiscard]] virtual const TSMeta* field_meta(const std::string& name) const { return nullptr; }
    [[nodiscard]] virtual const TSMeta* element_meta() const { return nullptr; }
    [[nodiscard]] virtual const TSMeta* value_meta() const { return nullptr; }

    // Memory sizing for arena allocation
    [[nodiscard]] virtual size_t output_memory_size() const = 0;
    [[nodiscard]] virtual size_t input_memory_size() const = 0;

    // Reference type check
    [[nodiscard]] bool is_reference() const { return ts_kind == TSKind::REF; }
};
```

### 3.3 Specialized TSMeta Types

Each time-series kind has its own specialized metadata struct:

```cpp
// TS<T> - Single scalar time-series
struct TSValueMeta : TSMeta {
    const value::TypeMeta* scalar_type;

    [[nodiscard]] const value::TypeMeta* value_schema() const override { return scalar_type; }
};

// TSB[Schema] - Time-series bundle
struct TSBTypeMeta : TSMeta {
    struct Field {
        std::string name;
        const TSMeta* type;
    };
    std::vector<Field> fields;
    const value::TypeMeta* bundle_value_type{nullptr};

    [[nodiscard]] const value::TypeMeta* value_schema() const override { return bundle_value_type; }
    [[nodiscard]] const TSMeta* field_meta(size_t index) const override;
    [[nodiscard]] const TSMeta* field_meta(const std::string& name) const override;
};

// TSL<V, Size> - Time-series list
struct TSLTypeMeta : TSMeta {
    const TSMeta* element_ts_type;
    int64_t size;  // -1 = dynamic/unresolved
    const value::TypeMeta* list_value_type{nullptr};

    [[nodiscard]] const TSMeta* element_meta() const override { return element_ts_type; }
};

// TSD<K, V> - Time-series dict
struct TSDTypeMeta : TSMeta {
    const value::TypeMeta* key_type;
    const TSMeta* value_ts_type;
    const value::TypeMeta* dict_value_type{nullptr};

    [[nodiscard]] const TSMeta* value_meta() const override { return value_ts_type; }
};

// TSS<T> - Time-series set
struct TSSTypeMeta : TSMeta {
    const value::TypeMeta* element_type;
    const value::TypeMeta* set_value_type{nullptr};
};

// TSW<T, Size> - Time-series window
struct TSWTypeMeta : TSMeta {
    const value::TypeMeta* scalar_type;
    int64_t size;       // count for fixed, negative for time-based
    int64_t min_size;   // min count, -1 for unspecified
    const value::TypeMeta* window_value_type{nullptr};
};

// REF<TS_TYPE> - Time-series reference
struct REFTypeMeta : TSMeta {
    const TSMeta* value_ts_type;
    const value::TypeMeta* ref_value_type{nullptr};
};

// SIGNAL - Input-only, no value
struct SignalTypeMeta : TSMeta {
    [[nodiscard]] const value::TypeMeta* value_schema() const override { return nullptr; }
};
```

### 3.4 TSTypeRegistry - Interning and Caching

**Key Pattern from `type-improvements`:** Types are interned via a central registry with hash-based caching. Same type = same pointer.

```cpp
/**
 * TSTypeRegistry - Central registry for time-series type metadata
 *
 * Provides:
 * - Registration of types by hash key
 * - Lookup of types by hash key
 * - Ownership of dynamically created type metadata
 * - Thread-safe registration
 */
class TSTypeRegistry {
public:
    static TSTypeRegistry& global();

    // Register a type by hash key (returns existing if key already registered)
    const TSMeta* register_by_key(size_t key, std::unique_ptr<TSMeta> meta);

    // Lookup by hash key (returns nullptr if not found)
    [[nodiscard]] const TSMeta* lookup_by_key(size_t key) const;

    [[nodiscard]] bool contains_key(size_t key) const;
    [[nodiscard]] size_t cache_size() const;

private:
    TSTypeRegistry() = default;
    mutable std::mutex _mutex;
    std::unordered_map<size_t, std::unique_ptr<TSMeta>> _types;
};

// Hash combining utility
inline size_t ts_hash_combine(size_t h1, size_t h2) {
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
}
```

### 3.5 Dual API - Compile-Time and Runtime

**Pattern from `type-improvements`:** Two APIs for type construction - compile-time templates for static types, runtime functions for dynamic construction.

#### 3.5.1 Compile-Time API (C++20 Templates)

```cpp
namespace hgraph::types {

// Get type meta from type descriptor - automatic interning
template<typename T>
const TSMeta* ts_type() {
    return T::get();  // Each descriptor has static get() method
}

// Usage:
auto* ts_int = ts_type<TS<int>>();
auto* tss_str = ts_type<TSS<std::string>>();
auto* tsl = ts_type<TSL<TS<int>, 3>>();
auto* tsd = ts_type<TSD<std::string, TS<int>>>();

// Bundles with C++20 string literal template parameters
auto* point = ts_type<TSB<
    Field<"x", TS<int>>,
    Field<"y", TS<float>>,
    Name<"Point">
>>();

// Windows - count-based and time-based
auto* w1 = ts_type<TSW<float, 10>>();           // Last 10 values
auto* w2 = ts_type<TSW_Time<float, Seconds<60>>>();  // 60-second window

// References
auto* ref = ts_type<REF<TS<int>>>();
}
```

#### 3.5.2 Runtime API (Dynamic Construction)

```cpp
namespace hgraph::types::runtime {

// TS<T> - from scalar TypeMeta
const TSMeta* ts(const value::TypeMeta* scalar_type);

// TSS<T> - time-series set
const TSMeta* tss(const value::TypeMeta* element_type);

// TSD<K, V> - time-series dict
const TSMeta* tsd(const value::TypeMeta* key_type, const TSMeta* value_ts_type);

// TSL<V, Size> - time-series list
const TSMeta* tsl(const TSMeta* element_ts_type, int64_t size);

// TSB[...] - time-series bundle
const TSMeta* tsb(std::vector<std::pair<std::string, const TSMeta*>> fields,
                  const char* name = nullptr);

// TSW - count-based window
const TSMeta* tsw(const value::TypeMeta* scalar_type, int64_t size, int64_t min_size = 0);

// TSW - time-based window (duration in microseconds)
const TSMeta* tsw_time(const value::TypeMeta* scalar_type, int64_t duration_us, int64_t min_size = 0);

// REF<TS> - time-series reference
const TSMeta* ref(const TSMeta* value_ts_type);

// SIGNAL - input-only type
const TSMeta* signal();
}

// Usage:
auto* ts_int = runtime::ts(value::int_type());
auto* point = runtime::tsb({{"x", ts_int}, {"y", runtime::ts(value::float_type())}}, "Point");
```

#### 3.5.3 Python Bindings

Factory functions exposed to Python:

```cpp
// Python API (via nanobind)
m.def("get_ts_type_meta", [](const value::TypeMeta* scalar_type) {
    return runtime_python::ts(scalar_type);
}, "Get or create a TS[T] TypeMeta");

m.def("get_tsb_type_meta", [](nb::list fields, nb::object type_name) {
    // Convert Python list to C++ vector and call runtime::tsb
    return runtime_python::tsb(field_vec, name_cstr);
}, "Get or create a TSB TypeMeta");

// Similar for get_tss_type_meta, get_tsd_type_meta, etc.
```

### 3.6 Value Schema Composition

Each TSMeta computes its corresponding scalar `value::TypeMeta` for storage:

| TSMeta | value_schema() Returns |
|--------|------------------------|
| `TSValueMeta` | `scalar_type` directly |
| `TSBTypeMeta` | `BundleTypeMeta` built from field value schemas |
| `TSLTypeMeta` | `ListTypeMeta` with element's value schema |
| `TSDTypeMeta` | `DictTypeMeta` with key and value schemas |
| `TSSTypeMeta` | `SetTypeMeta` with element type |
| `TSWTypeMeta` | `WindowTypeMeta` for storage |
| `REFTypeMeta` | `RefTypeMeta` for ref storage |
| `SignalTypeMeta` | `nullptr` (no value) |

```cpp
// Example: Building TSB's value schema
const value::TypeMeta* TSBTypeMeta::value_schema() const {
    if (bundle_value_type) return bundle_value_type;

    // Build from fields
    value::BundleTypeBuilder builder;
    for (const auto& field : fields) {
        builder.add_field(field.name, field.type->value_schema());
    }
    bundle_value_type = builder.build();
    return bundle_value_type;
}
```

### 3.7 Schema Relationship Diagram

```
TS<int64> (Base case: time-series of atomic int64)
├── TSTypeMeta (kind=Scalar)
│   └── value_schema() -> TypeMeta(scalar<int64>)
└── Each TS<int64> instance tracks its own modification time

TSB[price: TS<float>, volume: TS<int>]
├── TSTypeMeta (kind=Bundle)
│   ├── field("price") -> TSTypeMeta(kind=Scalar, float)  // TS<float>
│   ├── field("volume") -> TSTypeMeta(kind=Scalar, int)   // TS<int>
│   └── value_schema() -> TypeMeta(bundle{price: float, volume: int})
├── The bundle tracks its own modification time
└── Each field (price, volume) ALSO tracks its own modification time
```

**Critical Point:** A TSB field like `price: TS<float>` is a **time-series** that can tick independently. When `price` is modified:
1. The `price` field's modification time is updated
2. The parent bundle's modification time is also updated
3. Observers of either can be notified

### 3.8 Why Two Schemas?

| Concern | Scalar Schema (TypeMeta) | TS Schema (TSTypeMeta) |
|---------|--------------------------|------------------------|
| **Data storage** | ✓ Size, alignment, ops | |
| **Python conversion** | ✓ to_python, from_python | |
| **Modification tracking** | | ✓ Per-element timestamps |
| **Notification observers** | | ✓ Per-element callbacks |
| **View navigation** | Path through data | Path through TS hierarchy |

The scalar schema describes the **atomic data** layout. The TS schema describes where **time-series semantics** apply - each TS schema node is a point where modification can be independently tracked and observers can subscribe.

---

## 4. Architecture

### 4.1 Component Overview

```
┌─────────────────────────────────────────────────────────┐
│                      TSValue                             │
│  ┌───────────────────────────────────────────────────┐  │
│  │ Value _value                 (type-erased data)   │  │
│  ├───────────────────────────────────────────────────┤  │
│  │ ModificationTrackerStorage   (hierarchical times) │  │
│  ├───────────────────────────────────────────────────┤  │
│  │ unique_ptr<ObserverStorage>  (hierarchical subs)  │  │
│  ├───────────────────────────────────────────────────┤  │
│  │ const TSMeta* _ts_meta       (TS schema)          │  │
│  │ Node* _owning_node           (owner)              │  │
│  │ int _output_id               (0, -1, or -2)       │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
           │
           │ view()
           ▼
┌─────────────────────────────────────────────────────────┐
│                      TSView                              │
│  ┌───────────────┐  ┌─────────────────────────────┐     │
│  │ ValueView     │  │ ModificationTracker         │     │
│  └───────────────┘  │  (view into tracker storage)│     │
│  ┌───────────────┐  └─────────────────────────────┘     │
│  │ ObserverStorage* │  (hierarchical subscriptions)    │
│  └───────────────┘                                      │
│  ┌───────────────┐  ┌───────────────┐                   │
│  │ TSMeta*       │  │ ValuePath     │ (for navigation) │
│  └───────────────┘  └───────────────┘                   │
└─────────────────────────────────────────────────────────┘
```

### 4.2 Key Relationships

- **TSValue** holds: `Value` (data) + `ModificationTrackerStorage` (hierarchical timestamps) + `ObserverStorage` (hierarchical observers)
- **TSView** carries: value view + tracker view + observer pointer + path together
- **Hierarchical tracking** - modifications propagate upward to parent
- **Hierarchical observers** - subscriptions at any level of the TS schema
- **Time as parameter** - `engine_time_t` passed to mutations, not stored in views
- **Parent chain** - handled by navigation path, not stored pointers

---

## 5. TSValue Structure

The TSValue design uses **hierarchical storage** for both modification tracking and observers (from `type-improvements` branch). This mirrors the TS schema structure level-by-level, enabling per-field/element timestamps and subscriptions.

### 5.1 Core Structure

```cpp
class TSValue {
public:
    // Construction
    explicit TSValue(const TSMeta* ts_schema);
    TSValue(const TSMeta* ts_schema, Node* owner, int output_id = OUTPUT_MAIN);

    // TS Schema access
    const TSMeta* ts_schema() const { return _ts_meta; }
    const value::TypeMeta* value_schema() const { return _ts_meta->value_schema(); }

    // === Value access ===
    value::ConstValueView value() const { return _value.const_view(); }
    TSView view();  // Returns mutable view with path + tracker + observer access

    // === Modification queries (time as parameter, NOT stored!) ===
    bool modified_at(engine_time_t time) const {
        return _tracker.tracker().modified_at(time);
    }
    engine_time_t last_modified_time() const {
        return _tracker.tracker().last_modified_time();
    }
    bool has_value() const {
        return _tracker.tracker().valid_value();
    }
    void mark_invalid() { _tracker.tracker().mark_invalid(); }

    // === Direct value access (convenience for simple TS values) ===
    template<typename T>
    void set_value(const T& val, engine_time_t time) {
        _value.view().as<T>() = val;
        _tracker.tracker().mark_modified(time);
        if (_observers) _observers->notify(time);
    }

    template<typename T>
    const T& as() const { return _value.const_view().as<T>(); }

    // === Observer/subscription API (lazy allocation) ===
    void subscribe(Notifiable* notifiable);
    void unsubscribe(Notifiable* notifiable);
    bool has_observers() const;

    // === Path identification (see Appendix E) ===
    Node* owning_node() const { return _owning_node; }
    int output_id() const { return _output_id; }
    StoredPath stored_path() const;

    // === Underlying storage access (for advanced use) ===
    Value& underlying_value() { return _value; }
    ModificationTrackerStorage& underlying_tracker() { return _tracker; }
    ObserverStorage* underlying_observers() { return _observers.get(); }

private:
    Value _value;                                    // Type-erased data storage
    ModificationTrackerStorage _tracker;             // Hierarchical modification timestamps
    std::unique_ptr<ObserverStorage> _observers;     // Hierarchical observers (lazy)
    const TSMeta* _ts_meta{nullptr};                 // Time-series schema
    Node* _owning_node{nullptr};
    int _output_id{OUTPUT_MAIN};
};
```

### 5.2 Hierarchical Modification Tracking

The modification tracker **mirrors the TS schema structure** to track timestamps at every level:

```cpp
/**
 * ModificationTrackerStorage - Hierarchical storage for modification timestamps
 *
 * Storage layout by type:
 * - Scalar: single engine_time_t
 * - Bundle: own timestamp + child storage for each field
 * - List: own timestamp + child storage for each element
 * - Set: SetModificationStorage (structural + per-element tracking)
 * - Dict: DictModificationStorage (structural + per-entry timestamps)
 *
 * Modifications propagate UPWARD: change at leaf updates all ancestors.
 */
class ModificationTrackerStorage {
public:
    explicit ModificationTrackerStorage(const TypeMeta* value_meta);

    // Own timestamp access
    engine_time_t* timestamp_ptr();

    // Child storage for nested types (bundle fields, list elements)
    ModificationTrackerStorage* child(size_t index);

    // Propagate modification time to parent
    void propagate_to_parent(engine_time_t time);

    // Create tracker view
    ModificationTracker tracker();

private:
    const TypeMeta* _value_meta{nullptr};
    void* _storage{nullptr};  // Own timestamp storage
    ModificationTrackerStorage* _parent{nullptr};
    std::vector<std::unique_ptr<ModificationTrackerStorage>> _children;
};

/**
 * ModificationTracker - Non-owning view into modification storage
 */
class ModificationTracker {
public:
    explicit ModificationTracker(ModificationTrackerStorage* storage);

    // Query modification state
    bool modified_at(engine_time_t time) const;
    engine_time_t last_modified_time() const;

    // Mark as modified (propagates to parent!)
    void mark_modified(engine_time_t time);
    void mark_invalid();

    // Navigate to child trackers
    ModificationTracker field(size_t index);
    ModificationTracker field(const std::string& name);
    ModificationTracker element(size_t index);

    // Query child modification
    bool field_modified_at(size_t index, engine_time_t time) const;
    bool element_modified_at(size_t index, engine_time_t time) const;
};
```

### 5.3 Specialized Modification Storage for Collections

```cpp
/**
 * SetModificationStorage - For TSS (Time-Series Set)
 */
struct SetModificationStorage {
    engine_time_t structural_modified{MIN_DT};
    ankerl::unordered_dense::map<size_t, engine_time_t> element_modified_at;
    std::vector<std::byte> removed_elements_data;  // For delta access
    size_t removed_element_count{0};

    // Delta queries
    size_t added_count(engine_time_t time) const;
    size_t removed_count() const;
    std::vector<size_t> added_indices(engine_time_t time) const;
};

/**
 * DictModificationStorage - For TSD (Time-Series Dict)
 */
struct DictModificationStorage {
    engine_time_t structural_modified{MIN_DT};
    ankerl::unordered_dense::map<size_t, engine_time_t> key_added_at_map;
    ankerl::unordered_dense::map<size_t, engine_time_t> value_modified_at;
    std::vector<std::byte> removed_keys_data;  // For delta access
    std::vector<char> old_values;              // For delta_value
};
```

### 5.4 Hierarchical Observer Storage

Observers also mirror the TS schema for subscriptions at any level:

```cpp
/**
 * ObserverStorage - Hierarchical observer storage for TSValue
 *
 * - Root level: notified for any change
 * - Field/element level: notified for changes at that location only
 * - Notifications propagate UPWARD
 * - Lazy allocation (no memory until first subscribe)
 */
class ObserverStorage {
public:
    explicit ObserverStorage(const TypeMeta* meta);

    // Subscription at this level
    void subscribe(Notifiable* notifiable);
    void unsubscribe(Notifiable* notifiable);
    bool has_subscribers() const;

    // Notification (propagates to parent!)
    void notify(engine_time_t time);

    // Child observer storage (lazy allocation)
    ObserverStorage* child(size_t index);
    ObserverStorage* ensure_child(size_t index, const TypeMeta* child_meta = nullptr);

private:
    const TypeMeta* _meta{nullptr};
    ObserverStorage* _parent{nullptr};
    std::unordered_set<Notifiable*> _subscribers;
    std::vector<std::unique_ptr<ObserverStorage>> _children;  // Lazy
};
```

**Key behavior:** A subscription at `bundle.observer()->child(0)` (the "price" field) only receives notifications when price changes, not when other fields change.

### 5.5 What TSValue Does NOT Have

| Removed | Reason |
|---------|--------|
| Stored `_last_modified_time` | Time passed as parameter; tracked hierarchically |
| Simple callback list | Replaced by hierarchical `ObserverStorage` |
| Variant parent | Only `Node*` - view parent chain is in path |
| Type-specific subclasses | Type erasure via Value + hierarchical tracking |

---

## 6. View System

The TSView from `type-improvements` carries **value + tracker + observer** together, and time is passed as a parameter (not stored).

### 6.1 TSView (Unified Mutable View)

```cpp
/**
 * TSView - Mutable view with explicit time parameters
 *
 * Carries:
 * - ValueView (data access)
 * - ModificationTracker (hierarchical timestamps)
 * - ObserverStorage* (hierarchical subscriptions)
 * - TSMeta* (time-series type info)
 * - ValuePath (for REF creation, owning_node access)
 *
 * Time is passed to mutation methods (not stored) to avoid stale time issues.
 * Navigation returns sub-views with proper path/tracker/observer hierarchy.
 */
class TSView {
public:
    TSView() = default;

    // Full construction (from TSOutput::view())
    TSView(ValueView value_view, ModificationTracker tracker,
           ObserverStorage* observer, const TSMeta* ts_meta,
           ValuePath path = {});

    // === Validity and type queries ===
    bool valid() const { return _value_view.valid() && _tracker.valid(); }
    const TypeMeta* schema() const { return _value_view.schema(); }
    TypeKind kind() const { return _value_view.kind(); }
    const TSMeta* ts_meta() const { return _ts_meta; }
    TSKind ts_kind() const;

    // === Path tracking (for REF creation) ===
    const ValuePath& path() const { return _path; }
    std::string path_string() const { return _path.to_string(); }
    const ts::TSOutput* root_output() const { return _path.root_output(); }
    node_ptr owning_node() const { return _path.owning_node(); }

    // === Raw access ===
    ValueView value_view() { return _value_view; }
    ConstValueView value_view() const { return _value_view; }
    ModificationTracker tracker() { return _tracker; }
    ObserverStorage* observer() { return _observer; }

    // === Scalar access with time parameter ===
    template<typename T>
    void set(const T& val, engine_time_t time) {
        _value_view.as<T>() = val;
        _tracker.mark_modified(time);  // Propagates to parent!
        if (_observer) _observer->notify(time);  // Propagates to parent!
    }

    template<typename T>
    const T& as() const { return _value_view.as<T>(); }

    // === Navigation (returns sub-views with extended paths) ===
    TSView field(size_t index);
    TSView field(const std::string& name);
    TSView element(size_t index);

    // Bundle convenience
    TSView operator[](const std::string& name) { return field(name); }
    TSView operator[](size_t index);  // List element or bundle by index

    // === Subscription at this level ===
    void subscribe(Notifiable* notifiable) {
        if (_observer) _observer->subscribe(notifiable);
    }
    void unsubscribe(Notifiable* notifiable) {
        if (_observer) _observer->unsubscribe(notifiable);
    }

    // === Modification queries ===
    bool modified_at(engine_time_t time) const {
        return _tracker.modified_at(time);
    }
    engine_time_t last_modified_time() const {
        return _tracker.last_modified_time();
    }

private:
    ValueView _value_view;
    ModificationTracker _tracker;
    ObserverStorage* _observer{nullptr};
    const TSMeta* _ts_meta{nullptr};
    ValuePath _path;
};
```

### 6.2 Navigation Returns Sub-Views

```cpp
// Navigate into a bundle field
TSView TSView::field(size_t index) {
    // Get child value view
    auto child_value = _value_view.field(index);
    // Get child tracker (hierarchical)
    auto child_tracker = _tracker.field(index);
    // Get child observer (hierarchical, lazy)
    auto* child_observer = _observer ? _observer->child(index) : nullptr;
    // Get child ts_meta
    auto* child_meta = _ts_meta ? _ts_meta->field_meta(index) : nullptr;
    // Extend path
    return TSView(child_value, child_tracker, child_observer, child_meta,
                  _path.with(index));
}
```

### 6.3 Input Views (Bound to External TSOutput)

For inputs that observe an external output:

```cpp
/**
 * TSInput - Input view bound to an external TSOutput
 *
 * Similar to TSView but:
 * - Created by binding to TSOutput (not by TSValue::view())
 * - Supports make_active()/make_passive() for subscription
 * - Has its own observer (Node*) separate from the output's observers
 */
class TSInput {
public:
    TSInput() = default;
    explicit TSInput(Notifiable* my_observer);  // Observer = owning node

    // Binding to external output
    bool bind(ts::TSOutput* output);
    bool bind(ts::TSOutput* output, const ValuePath& path);
    void unbind();

    // Activation (subscribes my_observer to output's observers)
    void make_active();
    void make_passive();
    bool active() const { return _active; }

    // Delegate to bound view
    bool valid() const { return _view.valid(); }
    bool modified_at(engine_time_t time) const { return _view.modified_at(time); }

    template<typename T>
    const T& as() const { return _view.as<T>(); }

    TSInput field(size_t index);
    TSInput field(const std::string& name);

private:
    TSView _view;                    // View into external output
    Notifiable* _my_observer{nullptr};  // My node (for notifications)
    bool _active{false};
};
```

### 6.3 Specialized Views

| View Type | Purpose |
|-----------|---------|
| `TSBView` | Bundle view - access by field name |
| `TSBMutableView` | Mutable bundle view |
| `TSLView` | List view - indexed access |
| `TSDView` | Dict view - key-based access |
| `TSSView` | Set view - membership operations |

### 6.4 Output/Input Slots (Tuple Approach)

Rather than inheritance-based combined types, we use a **tuple approach** where the value and view are stored together in a slot, with the view returned as the external interface.

#### 6.4.1 Slot Structures

```cpp
/**
 * @brief Storage slot for an output - holds value and its mutable view together.
 *
 * The view is bound to the value on construction. Node stores slots,
 * returns view reference as the external interface.
 */
struct OutputSlot {
    TSValue value;
    TSMutableView view;

    OutputSlot(const TSMeta* schema, Node* owner, int output_id = OUTPUT_MAIN)
        : value(schema, owner, output_id), view(owner) {
        view.bind(&value);
    }

    // Non-copyable (view holds pointer to value)
    OutputSlot(const OutputSlot&) = delete;
    OutputSlot& operator=(const OutputSlot&) = delete;

    // Moveable with rebind
    OutputSlot(OutputSlot&& other) noexcept
        : value(std::move(other.value)), view(std::move(other.view)) {
        view.bind(&value);  // Rebind after move
    }
    OutputSlot& operator=(OutputSlot&&) = delete;  // Simplify - no move assign
};

/**
 * @brief Storage slot for a non-peered input - holds value and its read-only view.
 */
struct InputSlot {
    TSValue value;
    TSView view;

    InputSlot(const TSMeta* schema, Node* owner)
        : value(schema, owner), view(owner) {
        view.bind(&value);
    }

    // Non-copyable, moveable with rebind (same pattern as OutputSlot)
    InputSlot(const InputSlot&) = delete;
    InputSlot& operator=(const InputSlot&) = delete;
    InputSlot(InputSlot&& other) noexcept
        : value(std::move(other.value)), view(std::move(other.view)) {
        view.bind(&value);
    }
    InputSlot& operator=(InputSlot&&) = delete;
};
```

#### 6.4.2 Factory Functions

```cpp
/**
 * @brief Create an output slot.
 *
 * Returns the slot which should be stored in stable storage.
 * The view member is the external interface.
 */
inline OutputSlot make_output(const TSMeta* schema, Node* owner,
                              int output_id = OUTPUT_MAIN) {
    return OutputSlot(schema, owner, output_id);
}

/**
 * @brief Create a non-peered input slot.
 */
inline InputSlot make_input(const TSMeta* schema, Node* owner) {
    return InputSlot(schema, owner);
}
```

#### 6.4.3 Node Storage Pattern

```cpp
class Node {
    // Use deque for pointer stability (no reallocation on push_back)
    std::deque<OutputSlot> _output_slots;
    std::deque<InputSlot> _input_slots;

public:
    /**
     * @brief Add an output and return its view as the interface.
     */
    TSMutableView& add_output(const TSMeta* schema, int output_id = OUTPUT_MAIN) {
        _output_slots.emplace_back(schema, this, output_id);
        return _output_slots.back().view;
    }

    /**
     * @brief Add a non-peered input and return its view as the interface.
     */
    TSView& add_input(const TSMeta* schema) {
        _input_slots.emplace_back(schema, this);
        return _input_slots.back().view;
    }

    /**
     * @brief Access output view by index.
     */
    TSMutableView& output(size_t index) {
        return _output_slots[index].view;
    }

    /**
     * @brief Access input view by index.
     */
    TSView& input(size_t index) {
        return _input_slots[index].view;
    }

    /**
     * @brief Access underlying value (for copy operations, etc.)
     */
    TSValue& output_value(size_t index) {
        return _output_slots[index].value;
    }
};
```

#### 6.4.4 Usage Examples

```cpp
// Node construction
class MyNode : public Node {
public:
    MyNode() {
        // Add outputs - get view references as the interface
        TSMutableView& out = add_output(ts_type<TS<int>>());
        TSMutableView& err = add_output(ts_type<TS<std::string>>(), ERROR_PATH);

        // Add non-peered inputs (typically TSB)
        TSView& in = add_input(ts_type<TSB<
            Field<"price", TS<float>>,
            Field<"volume", TS<int>>
        >>());
    }

    void do_evaluate(engine_time_t time) {
        // Use views directly - they're the interface
        float price = input(0).field("price").as<float>();
        output(0).set(static_cast<int>(price * 2), time);
    }
};
```

#### 6.4.5 Why Tuple/Slot Instead of Inheritance

| Aspect | Tuple/Slot Approach | Inheritance Approach |
|--------|---------------------|---------------------|
| **Clarity** | Explicit - value and view are separate | Implicit - view "is-a" value owner |
| **Initialization** | No ordering issues | Base constructs before derived members |
| **Storage** | Node controls where slot lives | Object is self-contained |
| **Interface** | View reference is the handle | Object itself is the interface |
| **Flexibility** | Can store in various containers | Fixed object layout |
| **Pointer stability** | Guaranteed with deque | Requires care with copies/moves |

### 6.5 When to Use Each Type

| Type | Use Case |
|------|----------|
| `OutputSlot` | Node stores this (value + view together) |
| `InputSlot` | Node stores this for non-peered inputs |
| `TSMutableView&` | Returned as output interface |
| `TSView&` | Returned as input interface, or peered input bound to external |

**Node pattern:**
- **Store**: `OutputSlot` and `InputSlot` in deques
- **Return**: View references as the external interface
- **Peered inputs**: Just `TSView` bound to upstream output (no slot needed)

---

## 7. State Machines

### 7.1 TSValue State

TSValue tracks validity hierarchically via `ModificationTrackerStorage`. Each level in the TS schema has its own timestamp.

```
                    create
                       │
                       ▼
                  ┌─────────┐
                  │ INVALID │  (last_modified_time == MIN_DT)
                  └────┬────┘
                       │ set_value(val, time) / mark_modified(time)
                       ▼
                  ┌─────────┐
                  │  VALID  │  (last_modified_time > MIN_DT)
                  └────┬────┘
                       │ mark_invalid()
                       ▼
                  ┌─────────┐
                  │ INVALID │
                  └─────────┘
```

**Key Point:** Time is passed as a parameter to `set_value()` and `mark_modified()`, not stored separately in the view. This avoids stale time issues.

### 7.2 Hierarchical Modification Propagation

When a nested element is modified, the change propagates upward:

```
Bundle                    List Element               Scalar Value
  │                           │                          │
  ▼                           ▼                          ▼
┌──────────┐              ┌──────────┐              ┌──────────┐
│ modified │ ◄─────────── │ modified │ ◄─────────── │ modified │
│ at T1    │  propagate   │ at T1    │  propagate   │ at T1    │
└──────────┘              └──────────┘              └──────────┘
```

```cpp
void ModificationTracker::mark_modified(engine_time_t time) {
    *_storage->timestamp_ptr() = time;
    if (_storage->parent()) {
        _storage->parent()->propagate_to_parent(time);  // Upward!
    }
}
```

### 7.3 TSView State

```
                    create
                       │
                       ▼
                  ┌──────────┐
                  │ UNBOUND  │  (no value_view, no tracker)
                  └────┬─────┘
                       │ bind(output) or construct from TSOutput::view()
                       ▼
           ┌───────────────────────┐
           │       BOUND           │
           │  ┌──────┐ ┌────────┐  │
           │  │ACTIVE│ │PASSIVE │  │
           │  └──┬───┘ └───┬────┘  │
           │     │ make_   │       │
           │     │ passive │       │
           │     ◀─────────▶       │
           │       make_active     │
           └───────────┬───────────┘
                       │ unbind()
                       ▼
                  ┌──────────┐
                  │ UNBOUND  │
                  └──────────┘
```

### 7.4 View Validity

A view is valid when:
1. It has a valid value_view and tracker
2. The underlying data has been set at least once
3. The path is valid (navigates to existing element)

```cpp
bool TSView::valid() const {
    return _value_view.valid() &&
           _tracker.valid() &&
           _tracker.last_modified_time() > MIN_DT;
}
```

### 7.5 Hierarchical Observer Notification

Observers at any level receive notifications when that level (or any descendant) is modified:

```cpp
void ObserverStorage::notify(engine_time_t time) {
    // Notify subscribers at this level
    for (auto* subscriber : _subscribers) {
        subscriber->notify(time);
    }
    // Propagate to parent (upward notification)
    if (_parent) {
        _parent->notify(time);
    }
}
```

---

## 8. Notification Protocol

### 8.1 Hierarchical Observer Architecture

TSValue uses hierarchical `ObserverStorage` that mirrors the TS schema structure. This allows subscriptions at any level of the hierarchy.

```
TSValue (root)
├── ObserverStorage (root subscribers)
│   └── on notify → propagates to Notifiable subscribers
│
├── bundle_field[0]  (e.g., "price")
│   └── ObserverStorage (price subscribers - only notified on price changes)
│
└── bundle_field[1]  (e.g., "volume")
    └── ObserverStorage (volume subscribers - only notified on volume changes)
```

### 8.2 Notification Flow

When a value is modified, notifications propagate upward through the hierarchy:

```
TSView::set(value, time)
       │
       ▼
_value_view.as<T>() = value
       │
       ▼
_tracker.mark_modified(time)
       │
       ├──► Update own timestamp
       └──► propagate_to_parent(time)  [upward!]
       │
       ▼
_observer->notify(time)   [if active]
       │
       ├──► Notify this level's subscribers
       └──► parent->notify(time)  [upward!]
```

### 8.3 Subscription at Different Levels

```cpp
// Subscribe to all changes (root level)
TSOutput output(schema, node);
output.subscribe(my_observer);  // Notified on ANY change

// Subscribe to specific field (field level)
TSView price_view = output.view().field("price");
price_view.subscribe(price_observer);  // Only notified on price changes

// Subscribe to list element
TSView elem_view = output.view().element(0);
elem_view.subscribe(elem_observer);  // Only notified on element 0 changes
```

### 8.4 Input Activation

When an input becomes active, it subscribes its owning node to the output's observers:

```cpp
void TSInput::make_active() {
    if (!_active && _view.valid() && _my_observer) {
        _active = true;

        // Subscribe my observer (owning node) to output's observers
        _view.subscribe(_my_observer);

        // If output is already valid and modified, notify immediately
        if (_view.valid() && _view.modified_at(current_time())) {
            _my_observer->notify(_view.last_modified_time());
        }
    }
}
```

### 8.5 Passive Views

Passive views unsubscribe from notifications:

```cpp
void TSInput::make_passive() {
    if (_active) {
        _active = false;
        _view.unsubscribe(_my_observer);
    }
}
```

### 8.6 Lazy Observer Allocation

Observers are allocated lazily - no memory cost until someone subscribes:

```cpp
void TSView::subscribe(Notifiable* notifiable) {
    if (!_observer) {
        // First subscription at this level - allocate storage
        _observer = _root_observers->ensure_child(_path);
    }
    _observer->subscribe(notifiable);
}
```

### 8.7 Graph Stopping Protection

Notifications are skipped during graph teardown:

```cpp
void TSView::set(const T& val, engine_time_t time) {
    // Check if graph is stopping
    if (auto* node = owning_node()) {
        if (auto* g = node->graph(); g && g->is_stopping()) {
            return;  // Skip during teardown
        }
    }
    // ... normal set logic ...
}
```

---

## 9. Value vs View in Composites

For composite types (bundles, lists), children can be:

| Child Type | Description | Use Case |
|------------|-------------|----------|
| **Value** | TSValue instance owned by parent | Non-peered inputs (top-level) |
| **View** | Reference to TSValue held elsewhere | Normal peered case |

### 9.1 Secondary Schema (Input Side)

A policy tracks which children are values vs views:

```cpp
struct WithElementKindTracking {
    enum class ElementKind : uint8_t { Value, View };

    // Per-element tracking
    std::vector<ElementKind> _element_kinds;

    // Storage for owned values (when element is Value)
    std::vector<TSValue> _owned_values;
};
```

### 9.2 Bundle View with Mixed Children

```cpp
class TSBView : public TSView {
    // For each field, either:
    // 1. Navigate to parent TSValue's field (View case)
    // 2. Reference local TSValue (Value case - non-peered)

    TSView at(const std::string& key) const {
        if (is_owned_value(key)) {
            // Return view to locally owned TSValue
            return TSView(&_owned_values[index]);
        } else {
            // Return view with extended path
            TSView child;
            child.bind(_value, extend_path(_path, key));
            return child;
        }
    }
};
```

---

## 10. Memory Layout

### 10.1 TSValue Memory

```
TSValue (estimated ~80-120 bytes for scalar, more for composites)
├─ Value _value                        (~32-48 bytes)
│   └─ ValueStorage (SBO 24 bytes + schema pointer)
├─ ModificationTrackerStorage _tracker (~32-48 bytes)
│   ├─ const TypeMeta* _value_meta     (8 bytes)
│   ├─ void* _storage                  (8 bytes - points to timestamp)
│   ├─ parent*                         (8 bytes)
│   └─ vector<unique_ptr<...>>         (24 bytes - children, lazy)
├─ unique_ptr<ObserverStorage>         (8 bytes - lazy allocation!)
├─ const TSMeta* _ts_meta              (8 bytes)
├─ Node* _owning_node                  (8 bytes)
└─ int _output_id                      (4 bytes + padding)
```

**Memory efficiency notes:**
- `ObserverStorage` is `unique_ptr` - no allocation until first subscription
- Tracker children allocated lazily for composite types
- For simple TS<int>, overhead is ~40 bytes beyond the value

### 10.2 Hierarchical Tracker Memory (for composites)

```
TSB[price: TS<float>, volume: TS<int>] Tracker:
├─ ModificationTrackerStorage (root)
│   ├─ engine_time_t* _storage        (8 bytes for own timestamp)
│   └─ vector<children>               (2 entries)
│       ├─ [0] ModificationTrackerStorage (price)
│       │   └─ engine_time_t*         (8 bytes)
│       └─ [1] ModificationTrackerStorage (volume)
│           └─ engine_time_t*         (8 bytes)
```

### 10.3 TSView Memory

```
TSView (estimated ~64-88 bytes)
├─ ValueView _value_view       (16 bytes - ptr + schema)
├─ ModificationTracker _tracker (~8-16 bytes - view into storage)
├─ ObserverStorage* _observer  (8 bytes - raw pointer, no ownership)
├─ const TSMeta* _ts_meta      (8 bytes)
└─ ValuePath _path             (24 bytes - vector with SBO for 4 elements)
```

### 10.4 Specialized Collection Tracker Memory

```
SetModificationStorage (for TSS):
├─ engine_time_t structural_modified        (8 bytes)
├─ ankerl::unordered_dense::map<...>        (32+ bytes - per-element)
├─ vector<byte> removed_elements_data       (24 bytes + data)
└─ size_t removed_element_count             (8 bytes)

DictModificationStorage (for TSD):
├─ engine_time_t structural_modified        (8 bytes)
├─ map<size_t, engine_time_t> key_added_at  (32+ bytes)
├─ map<size_t, engine_time_t> value_modified (32+ bytes)
├─ vector<byte> removed_keys_data           (24 bytes + data)
└─ vector<char> old_values                  (24 bytes + data)
```

---

## 11. Integration with Existing Infrastructure

### 11.1 Reusing Value System

TSValue uses the existing `Value` class for type-erased data storage:

```cpp
// TSValue uses Value for data, adds hierarchical tracking on top
class TSValue {
    Value _value;                      // Type-erased data (existing)
    ModificationTrackerStorage _tracker;  // Hierarchical timestamps (new)
    unique_ptr<ObserverStorage> _observers;  // Hierarchical observers (new)
    // ...
};
```

### 11.2 Reusing Path Navigation

Views use the existing path system with small vector optimization:

```cpp
#include <hgraph/types/value/path.h>

// ValuePath has inline capacity of 4 elements (SBO)
value::ConstValueView TSView::value_view() const {
    return value::navigate(_root_value->const_view(), _path);
}

// Navigation returns extended paths
TSView TSView::field(size_t index) {
    return TSView(
        _value_view.field(index),
        _tracker.field(index),
        _observer ? _observer->child(index) : nullptr,
        _ts_meta ? _ts_meta->field_meta(index) : nullptr,
        _path.with(index)  // Extend path
    );
}
```

### 11.3 Reusing Notifiable Interface

Observers implement the existing `Notifiable` interface:

```cpp
#include <hgraph/types/notifiable.h>

// Nodes implement Notifiable
struct Node : Notifiable {
    void notify(engine_time_t et) override {
        schedule_for_evaluation(et);
    }
};

// ObserverStorage holds Notifiable* subscribers
class ObserverStorage {
    std::unordered_set<Notifiable*> _subscribers;

    void notify(engine_time_t time) {
        for (auto* subscriber : _subscribers) {
            subscriber->notify(time);  // Call Notifiable::notify()
        }
        if (_parent) _parent->notify(time);
    }
};
```

### 11.4 Reusing TypeMeta

The hierarchical tracker uses `TypeMeta` to know the structure:

```cpp
// Tracker construction mirrors TypeMeta structure
ModificationTrackerStorage::ModificationTrackerStorage(const TypeMeta* meta)
    : _value_meta(meta)
{
    // Allocate timestamp
    _storage = allocate_timestamp();

    // For composites, create child trackers
    if (meta->kind() == TypeKind::Bundle) {
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = make_unique<ModificationTrackerStorage>(
                meta->field_type(i));
            child->_parent = this;
            _children.push_back(std::move(child));
        }
    }
    // Similar for List, Dict, etc.
}
```

### 11.5 Reusing TSMeta as Builder

TSMeta creates complete TSValue instances with hierarchical tracking:

```cpp
time_series_output_s_ptr TSValueMeta::make_output(node_ptr node) const {
    // TSOutput combines TSValue (with hierarchical tracking) and view
    return make_shared<TSOutput>(this, node.get());
}
```

---

## 12. Python Integration

### 12.1 Python Wrappers

The PyXXX wrappers will use TSValue/TSView internally:

```cpp
class PyTimeSeriesOutput {
    TSMutableView _view;  // Instead of current type-specific impl

public:
    nb::object value() const { return _view.py_value(); }
    void set_value(const nb::object& v) { _view.py_set_value(v); }
};

class PyTimeSeriesInput {
    TSView _view;  // Instead of current type-specific impl

public:
    nb::object value() const { return _view.py_value(); }
    bool valid() const { return _view.valid(); }
};
```

### 12.2 Python API Preservation

All existing Python methods are preserved:

| Python Method | Implementation |
|---------------|----------------|
| `value` | `_view.py_value()` |
| `delta_value` | `_view.py_delta_value()` |
| `modified` | `_view.modified()` |
| `valid` | `_view.valid()` |
| `apply_result(v)` | `_view.apply_result(v)` |
| `set_value(v)` | `_view.py_set_value(v)` |

---

## 13. Testing Strategy

### 13.1 Phase 1: Unit Tests (No References)

Focus on basic TS and TSB without reference types:

```
hgraph_unit_tests/_operators/test_const.py
hgraph_unit_tests/ts_tests/test_ts_behavior.py
hgraph_unit_tests/ts_tests/test_tsb_behavior.py
```

### 13.2 Phase 2: Container Types

Add TSL, TSD, TSS, TSW tests.

### 13.3 Phase 3: Reference Types

Add reference tests last (most complex).

---

## 14. Migration Path

### 14.1 Incremental Migration

1. **Create TSValue alongside existing types**
2. **Update builders to create TSValue**
3. **Update PyXXX wrappers to use TSView**
4. **Remove old types once tests pass**

### 14.2 Compatibility Layer

During migration, provide compatibility:

```cpp
// Temporary compatibility
using TimeSeriesValueOutput = TSMutableView;
using TimeSeriesValueInput = TSView;
```

---

## Appendix A: File Structure

### New Files

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/time_series/ts_type_meta.h` | TSMeta base and specialized types |
| `cpp/include/hgraph/types/time_series/ts_type_registry.h` | TSTypeRegistry for interning |
| `cpp/include/hgraph/types/time_series/ts_type_meta_bindings.h` | Python bindings header |
| `cpp/include/hgraph/types/type_api.h` | Dual API (compile-time and runtime) |
| `cpp/include/hgraph/types/tsvalue.h` | TSValue declaration |
| `cpp/include/hgraph/types/tsview.h` | View declarations |
| `cpp/include/hgraph/types/value/modification_tracker.h` | Hierarchical modification tracking |
| `cpp/include/hgraph/types/value/observer_storage.h` | Hierarchical observer storage |
| `cpp/include/hgraph/builders/time_series_types/cpp_time_series_builder.h` | Unified builder |
| `cpp/src/cpp/types/ts_type_meta.cpp` | TSMeta implementations |
| `cpp/src/cpp/types/ts_type_meta_bindings.cpp` | Python factory functions |
| `cpp/src/cpp/types/tsvalue.cpp` | TSValue implementation |
| `cpp/src/cpp/types/tsview.cpp` | View implementations |
| `cpp/src/cpp/types/value/modification_tracker.cpp` | Modification tracking implementation |
| `cpp/src/cpp/types/value/observer_storage.cpp` | Observer storage implementation |

### Modified Files

| File | Changes |
|------|---------|
| `cpp/include/hgraph/api/python/py_ts.h` | Use TSView internally |
| `cpp/include/hgraph/api/python/py_tsb.h` | Use TSBView internally |
| `cpp/include/hgraph/builders/*.h` | Use unified CppTimeSeriesBuilder |

### Unified Builder Pattern

**Key Simplification from `type-improvements`:** Replace per-type builders with a single unified builder that delegates to TSMeta:

```cpp
// Before: One builder per time-series type
TSValueOutputBuilder<bool>
TSValueOutputBuilder<int64_t>
TSValueOutputBuilder<double>
TimeSeriesListOutputBuilder
TimeSeriesBundleOutputBuilder
// ... etc

// After: Single unified builder
struct CppTimeSeriesOutputBuilder : OutputBuilder {
    const TSMeta* ts_type_meta;

    explicit CppTimeSeriesOutputBuilder(const TSMeta* meta) : ts_type_meta(meta) {}

    time_series_output_s_ptr make_instance(node_ptr owning_node) const override {
        // TSMeta knows how to create its own instances
        return ts_type_meta->make_output(owning_node);
    }

    [[nodiscard]] size_t memory_size() const override {
        return ts_type_meta->output_memory_size();
    }
};

struct CppTimeSeriesInputBuilder : InputBuilder {
    const TSMeta* ts_type_meta;

    time_series_input_s_ptr make_instance(node_ptr owning_node) const override {
        return ts_type_meta->make_input(owning_node);
    }
};
```

This drastically simplifies the builder hierarchy - one class handles all time-series types by delegating construction to the TSMeta.

---

## Appendix B: Invariants

### B.1 Schema Invariants

1. **TSValue always has a TS schema** - TSMeta is never nullptr
2. **TSMeta always has a value schema** - value_schema() never returns nullptr (except SIGNAL)
3. **TSB fields are TS types** - Bundle fields contain time-series, not scalars

### B.2 Hierarchical Tracking Invariants

4. **Tracker mirrors TS schema** - ModificationTrackerStorage structure matches TSMeta hierarchy
5. **Modification propagates upward** - Child modification always updates parent timestamp
6. **Timestamps are monotonic** - Parent timestamp >= max(child timestamps)
7. **Time as parameter** - engine_time_t passed to mutations, never stored in views

### B.3 Hierarchical Observer Invariants

8. **Observer mirrors TS schema** - ObserverStorage children match TSMeta hierarchy
9. **Observer notification propagates upward** - Child notify() always calls parent notify()
10. **Lazy observer allocation** - No memory until first subscribe at that level
11. **Active views require observer** - Notifiable* must be set for make_active()

### B.4 Path and Identification Invariants

12. **View path is always valid** - Points to existing element
13. **Only Node* parent in TSValue** - View parent chain is in path
14. **TSValue has output_id** - Identifies which output (0=main, -1=error, -2=state)
15. **Path uniquely identifies TSValue** - graph_id + node_ndx + output_id + ValuePath is unique

### B.5 Binding Invariants

16. **Active inputs are subscribed** - make_active() subscribes to output's observers
17. **Passive inputs are unsubscribed** - make_passive() removes subscription

---

## Appendix C: Behavioral Parity with Current Implementation

This appendix documents critical behavioral details from the current `BaseTimeSeriesOutput` and `BaseTimeSeriesInput` implementations that must be preserved.

### C.1 Modification Semantics

**Output (TSValue) `modified()` check:**
```cpp
// Current implementation (base_time_series.cpp:124-129)
bool BaseTimeSeriesOutput::modified() const {
    auto n = owning_node();
    if (n == nullptr) { return false; }
    // Uses cached evaluation time pointer for performance
    return *n->cached_evaluation_time_ptr() == _last_modified_time;
}
```

The `modified()` check compares `_last_modified_time` against the current evaluation time. This must be preserved in the policy's modification tracking.

**Input (TSView) `modified()` check:**
```cpp
// Current implementation (base_time_series.cpp:480)
bool BaseTimeSeriesInput::modified() const {
    return _output != nullptr && (_output->modified() || sampled());
}
```

Input modification includes the `sampled()` check - if the input was rebound during the current evaluation cycle, it reports as modified even if the output value wasn't changed. This handles rebinding notification correctly.

### C.2 Sample Time Tracking

Views must track when they were bound/rebound:

```cpp
// Current implementation (base_time_series.cpp:463-468)
bool BaseTimeSeriesInput::sampled() const {
    auto n = owning_node();
    if (n == nullptr) { return false; }
    return _sample_time != MIN_DT && _sample_time == *n->cached_evaluation_time_ptr();
}
```

This is stored in `_sample_time` on the view. When `bind_output()` is called during a started graph, `_sample_time` is set to the current evaluation time.

### C.3 Graph Stopping Protection

Notifications must be skipped during graph teardown:

```cpp
// Current implementation (base_time_series.cpp:157-161)
void BaseTimeSeriesOutput::mark_modified() {
    // ...
    auto g = n->graph();
    if (g != nullptr && g->is_stopping()) {
        return;  // Skip modifications during graph teardown
    }
    // ...
}
```

This prevents notification cascades that could access partially stopped nodes.

### C.4 `valid()` vs `all_valid()`

For scalar values, these are equivalent. For composite types (bundles, lists, etc.), they differ:

| Method | Scalar | Composite |
|--------|--------|-----------|
| `valid()` | Value set at least once | Root is valid |
| `all_valid()` | Same as valid() | All children are valid |

Current implementation (base_time_series.cpp:131-135):
```cpp
bool BaseTimeSeriesOutput::valid() const {
    return _last_modified_time > MIN_DT;
}

bool BaseTimeSeriesOutput::all_valid() const {
    return valid();  // Overridden in composite types
}
```

### C.5 Parent Output Notification

When a child is modified, the parent must be notified:

```cpp
// Current implementation (base_time_series.cpp:174-184)
void BaseTimeSeriesOutput::mark_modified(engine_time_t modified_time) {
    if (_last_modified_time < modified_time) {
        _last_modified_time = modified_time;
        if (has_parent_output()) {
            parent_output()->mark_child_modified(*this, modified_time);
        }
        _notify(modified_time);
    }
}
```

For TSValue with nested structures, modifications to nested elements (via path navigation) must trigger `notify_modified()` on the root TSValue.

### C.6 Binding Notification Behavior

When binding during a started graph:

```cpp
// Current implementation (base_time_series.cpp:322-333)
auto n = owning_node();
if ((n->is_started() || n->is_starting()) && _output && (was_bound || _output->valid())) {
    _sample_time = *n->cached_evaluation_time_ptr();
    if (active()) {
        notify(_sample_time);
    }
}
```

The view notifies if:
1. Node is started/starting AND
2. Output exists AND
3. Either was previously bound OR new output is valid

### C.7 Make Active Behavior

When making a view active:

```cpp
// Current implementation (base_time_series.cpp:363-376)
void BaseTimeSeriesInput::make_active() {
    if (!_active) {
        _active = true;
        if (_output != nullptr) {
            output()->subscribe(this);
            if (output()->valid() && output()->modified()) {
                notify(output()->last_modified_time());
                return;
            }
        }
        if (sampled()) { notify(_sample_time); }
    }
}
```

On activation:
1. Subscribe to output
2. If output is valid AND modified, notify immediately
3. Else if sampled this cycle, notify with sample time

### C.8 Notify Time Deduplication

Views track `_notify_time` to avoid duplicate notifications:

```cpp
// Current implementation (base_time_series.cpp:414-434)
void BaseTimeSeriesInput::notify(engine_time_t modified_time) {
    if (_notify_time != modified_time) {
        _notify_time = modified_time;
        // Propagate to parent input or owning node
    }
}
```

This prevents redundant notification propagation when multiple paths lead to the same view.

### C.9 Last Modified Time

For inputs, `last_modified_time()` considers both output and sample time:

```cpp
// Current implementation (base_time_series.cpp:486-488)
engine_time_t BaseTimeSeriesInput::last_modified_time() const {
    return bound() ? std::max(_output->last_modified_time(), _sample_time) : MIN_DT;
}
```

### C.10 Implementation Requirements Summary

| Requirement | TSValue | TSView |
|-------------|---------|--------|
| Modification time tracking | Via policy | Delegates to TSValue |
| Sample time tracking | N/A | `_sample_time` field |
| Notify time deduplication | N/A | `_notify_time` field |
| Graph stopping check | On `notify_modified()` | On `notify()` |
| `valid()` | `_last_modified_time > MIN_DT` | `bound() && _value->valid()` |
| `modified()` | Time comparison | Output modified OR sampled |
| `all_valid()` | Override in composites | Override in composite views |

---

## Appendix D: Integration Strategy - Combining Current Design with type-improvements

This appendix documents how to combine the best elements from the current TSValue design with patterns from the `type-improvements` branch.

### D.1 What to Keep from Current TSValue Design

| Element | Reason to Keep |
|---------|----------------|
| **Type/View separation** | Clean separation of storage (TSValue) from access patterns (TSView) |
| **Policy-based extension** | Compose behaviors via CombinedPolicy without inheritance |
| **Path-based navigation** | Views traverse via ValuePath, avoiding stored parent pointers |
| **Value system integration** | Build on existing Value<Policy> infrastructure |

### D.2 What to Adopt from type-improvements

| Element | Reason to Adopt |
|---------|-----------------|
| **TSMeta hierarchy** | Comprehensive type metadata with all TS kinds |
| **TSMeta as builder** | Type meta knows how to construct its instances |
| **TSTypeRegistry** | Interning with hash-based caching for type reuse |
| **Dual API** | Compile-time (templates) + Runtime (factory functions) |
| **Value schema composition** | Each TSMeta builds its corresponding TypeMeta |
| **Unified builder** | Single CppTimeSeriesBuilder dispatches via TSMeta |

### D.3 Synthesis Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Type Construction Layer                         │
│                                                                          │
│  Compile-Time API                     Runtime API                        │
│  ─────────────────                    ───────────                        │
│  ts_type<TS<int>>()                   runtime::ts(int_type())           │
│  ts_type<TSB<Field<"x", TS<int>>>>()  runtime::tsb({{"x", ts_int}})     │
│                        │                          │                      │
│                        └──────────┬───────────────┘                      │
│                                   ▼                                      │
│                          TSTypeRegistry (interning)                      │
│                                   │                                      │
│                                   ▼                                      │
│                              TSMeta hierarchy                            │
│                    (TSValueMeta, TSBTypeMeta, TSDTypeMeta, ...)         │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     │ make_output() / make_input()
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Builder Layer                                   │
│                                                                          │
│  CppTimeSeriesOutputBuilder          CppTimeSeriesInputBuilder           │
│  ┌─────────────────────────┐        ┌─────────────────────────┐         │
│  │ TSMeta* ts_type_meta    │        │ TSMeta* ts_type_meta    │         │
│  │ make_instance() →       │        │ make_instance() →       │         │
│  │   ts_type_meta->        │        │   ts_type_meta->        │         │
│  │   make_output(node)     │        │   make_input(node)      │         │
│  └─────────────────────────┘        └─────────────────────────┘         │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Instance Layer                                  │
│                                                                          │
│  TSValue                              TSView / TSMutableView             │
│  ┌─────────────────────────┐         ┌─────────────────────────┐        │
│  │ Value<CombinedPolicy<   │         │ TSValue* _value         │        │
│  │   WithPythonCache,      │         │ ValuePath _path         │        │
│  │   WithModificationTrack │         │ bool _active            │        │
│  │ >> _value               │◄────────│ engine_time_t times...  │        │
│  │ Node* _owning_node      │ bind()  │                         │        │
│  │ const TSMeta* _ts_meta  │         │ Specialized views:      │        │
│  └─────────────────────────┘         │ TSBView, TSLView, etc.  │        │
│                                      └─────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘
```

### D.4 Implementation Phases

**Phase 1: Schema Infrastructure**
1. Adopt TSMeta hierarchy from type-improvements
2. Adopt TSTypeRegistry for interning
3. Port type_api.h (dual API)
4. Create Python bindings for factory functions

**Phase 2: TSValue Core**
1. Create TSValue using Value<CombinedPolicy<...>>
2. Add `const TSMeta* _ts_meta` for schema reference
3. Implement make_output() in each TSMeta subclass
4. Create unified CppTimeSeriesBuilder

**Phase 3: View System**
1. Create TSView with path-based navigation
2. Create TSMutableView extending TSView
3. Create specialized views (TSBView, TSLView, etc.)
4. Implement make_input() in TSMeta subclasses

**Phase 4: Integration**
1. Update Python wrappers to use TSView internally
2. Migrate node builders to use CppTimeSeriesBuilder
3. Run behavioral parity tests
4. Remove old templated types

### D.5 Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| TSMeta storage in TSValue | `const TSMeta*` pointer | Schemas are interned, pointer comparison is sufficient |
| View parent tracking | ValuePath, not pointer | Avoids ownership complexity, enables rebinding |
| Modification tracking | Policy-based | Composable with Python caching |
| Builder pattern | TSMeta as builder | Type knows best how to construct itself |
| Type interning | Hash-based registry | Same type = same pointer, efficient equality |

### D.6 Migration Compatibility

During migration, maintain compatibility via type aliases:

```cpp
// Temporary aliases for gradual migration
using TimeSeriesValueOutput = TSMutableView;  // Old API → New impl
using TimeSeriesValueInput = TSView;          // Old API → New impl

// Old builder → New builder delegation
struct TSValueOutputBuilder : OutputBuilder {
    const TSMeta* meta;
    TSValueOutputBuilder(const value::TypeMeta* scalar_type)
        : meta(runtime::ts(scalar_type)) {}

    time_series_output_s_ptr make_instance(node_ptr node) const override {
        return meta->make_output(node);
    }
};
```

This allows incremental migration without breaking existing code.

---

## Appendix E: Path-Based Identification System

This appendix documents the path-based identification system for uniquely identifying TSValues within a graph.

### E.1 Output Identification Constants

Nodes in hgraph can have multiple outputs. Each output type is identified by a constant:

```cpp
// From hgraph/_runtime/_node.py
constexpr int OUTPUT_MAIN = 0;    // Main output (out)
constexpr int ERROR_PATH = -1;    // Error output (error_output)
constexpr int STATE_PATH = -2;    // Recordable state output (recordable_state)
```

| Constant | Value | Python Field | Description |
|----------|-------|--------------|-------------|
| `OUTPUT_MAIN` | `0` | `_output` | Primary output of the node |
| `ERROR_PATH` | `-1` | `_error_output` | Error output for exception propagation |
| `STATE_PATH` | `-2` | `_recordable_state` | Recordable state for checkpointing |

These constants are used in wiring edges to identify which output of a source node feeds into a destination node's input.

### E.2 Two Path Types

The system supports two distinct path representations optimized for different use cases:

| Path Type | Lifetime | Storage | Use Case |
|-----------|----------|---------|----------|
| **Lightweight Path** | One engine cycle | Ordinal integers only | Internal navigation, views |
| **Stored Path** | Persistent | Graph + node_id + typed elements | Global state, serialization |

### E.3 Lightweight Paths (Internal/One-Cycle)

Lightweight paths use **ordinal positions only** - no field names, just indices. These are efficient for internal navigation within a single engine cycle.

```cpp
// Lightweight path - integer indices only
struct LightweightPath {
    std::vector<size_t> elements;  // All ordinal positions
};

// Path element interpretation by container type:
// - TSB: ordinal position of field (0, 1, 2, ...) NOT field name
// - TSL: index in list (0, 1, 2, ...)
// - TSD: index in map implementation (internal order)

// Example: Navigate to 2nd field, then 3rd element
LightweightPath path{{1, 2}};  // All integers

// Views use lightweight paths for efficiency
class TSView {
    TSValue* _root{nullptr};
    LightweightPath _path;  // Ordinal navigation only
};
```

**Characteristics:**
- Zero allocation for small paths (SBO)
- No string comparisons
- Valid only for current engine cycle (map order may change)
- Cannot be serialized meaningfully

### E.4 Stored Paths (Global State/Persistent)

Stored paths are used for references that persist across engine cycles or need serialization. They are **completely pointer-free** using IDs instead.

```cpp
// Stored path - fully serializable, no pointers
struct StoredPath {
    tuple<int, ...> graph_id;    // Graph identification (not pointer!)
    size_t node_ndx;             // Node index within the graph (not full node_id!)
    int output_id;               // 0=main, -1=error, -2=state
    std::vector<PathElement> elements;
};

// Path element types
struct PathElement {
    enum class Kind : uint8_t {
        Index,    // size_t - for TSL
        Field,    // string - for TSB
        Key,      // Value  - for TSD (raw key value)
    };
    Kind kind;

    // Storage (union or variant)
    size_t index;           // When kind == Index
    std::string field;      // When kind == Field
    value::Value key;       // When kind == Key
};
```

**Key Design Choices:**

1. **`graph_id`** (tuple of ints) instead of `graph_s_ptr`:
   - Completely serializable (no pointers)
   - Valid across process restarts (with graph reconstruction)
   - Suitable for checkpointing and replay

2. **`node_ndx`** (size_t) instead of full `node_id` (tuple):
   - Since we have `graph_id`, we only need the index within that graph
   - More compact than the full path tuple
   - Direct array lookup in graph's node list

**Path Element by Container Type:**

| Container | Element Type | Storage | Example |
|-----------|--------------|---------|---------|
| TSL | `Index` | `size_t` | `PathElement{Index, 3}` |
| TSB | `Field` | `std::string` | `PathElement{Field, "price"}` |
| TSD | `Key` | `Value` | `PathElement{Key, Value("USD")}` |

### E.5 String Path Representation (Limited)

String-based path representation (e.g., `"orders[0].price"`) requires to/from string support in `Value`, which may not be suitable for all key types.

```cpp
// String path support - ONLY when:
// 1. No TSD in path, OR
// 2. TSD key type has string bijection (to_string/from_string)

std::optional<std::string> path_to_string(const StoredPath& path) {
    // Returns nullopt if path contains TSD with non-stringifiable key
}

std::optional<StoredPath> path_from_string(const std::string& str) {
    // Returns nullopt if parsing fails or TSD key cannot be parsed
}
```

**Recommendation:** Defer string path support until `Value` has comprehensive to/from string capabilities. For now, use structured paths.

### E.6 TSValue Association with Node

TSValue stores identification for path construction:

```cpp
class TSValue {
public:
    // Path identification
    Node* owning_node() const { return _owning_node; }
    int output_id() const { return _output_id; }

    // Lightweight path (for views within same cycle)
    LightweightPath lightweight_path() const {
        return LightweightPath{};  // Root - empty path
    }

    // Stored path (for persistent references) - pointer-free!
    StoredPath stored_path() const {
        return StoredPath{
            _owning_node->graph()->graph_id(),  // Graph ID, not pointer
            _owning_node->node_ndx(),           // Node index within graph
            _output_id,
            {}  // Root - empty elements
        };
    }

private:
    value::TSValueStorage _value;
    const TSMeta* _ts_meta{nullptr};
    Node* _owning_node{nullptr};
    int _output_id{OUTPUT_MAIN};
};
```

### E.7 TSView Association with Notifiable

Views use lightweight paths and require an observer for notifications:

```cpp
class TSView {
public:
    // Observer association for notifications
    Notifiable* observer() const { return _observer; }

    void make_active() {
        if (!_active && _value && _observer) {
            _active = true;
            _value->on_modified([this]() {
                if (_observer) {
                    _observer->notify(current_engine_time());
                }
            });
        }
    }

    void make_passive() {
        if (_active) {
            _active = false;
            // Callback cleanup handled by policy
        }
    }

protected:
    TSValue* _value{nullptr};
    LightweightPath _path;           // Ordinal navigation (efficient)
    Notifiable* _observer{nullptr};  // For make_active/passive
    bool _active{false};
    // ... time tracking fields ...
};
```

### E.8 REF Type Path Storage

REF types store **StoredPaths** for persistence. Resolution requires a graph registry to look up graphs by ID.

```cpp
// TimeSeriesReference stores a fully serializable path
class TimeSeriesReference {
public:
    // Resolve stored path to actual TSValue
    // Requires graph registry to resolve graph_id to Graph*
    TSValue* resolve(const GraphRegistry& registry) const {
        // Look up graph by ID (not pointer)
        Graph* graph = registry.find_graph(_path.graph_id);
        if (!graph) return nullptr;

        // Get node by index (direct array lookup)
        Node* node = graph->node_at(_path.node_ndx);
        if (!node) return nullptr;

        // Get appropriate output
        TimeSeriesOutput* output = nullptr;
        switch (_path.output_id) {
            case OUTPUT_MAIN: output = node->output(); break;
            case ERROR_PATH:  output = node->error_output(); break;
            case STATE_PATH:  output = node->recordable_state(); break;
        }
        if (!output) return nullptr;

        // Navigate using path elements
        return output->navigate(_path.elements);
    }

    void bind_input(TimeSeriesInput* input, const GraphRegistry& registry) {
        if (auto* value = resolve(registry)) {
            input->bind(value);
        }
    }

    // Serialization - path is fully serializable (no pointers)
    void serialize(Serializer& s) const { s << _path; }
    void deserialize(Deserializer& d) { d >> _path; }

private:
    StoredPath _path;  // Persistent, serializable, pointer-free
};

// Graph registry for ID-based lookup
class GraphRegistry {
public:
    void register_graph(Graph* graph);
    void unregister_graph(Graph* graph);
    Graph* find_graph(const tuple<int, ...>& graph_id) const;
};
```

### E.9 Converting Between Path Types

```cpp
// Lightweight to Stored (requires schema for element types)
StoredPath to_stored(const TSValue* root, const LightweightPath& light) {
    StoredPath stored = root->stored_path();
    const TSMeta* meta = root->ts_schema();

    for (size_t ordinal : light.elements) {
        switch (meta->ts_kind) {
            case TSKind::TSB:
                // Convert ordinal to field name
                stored.elements.push_back({PathElement::Field,
                    meta->field_name(ordinal)});
                meta = meta->field_meta(ordinal);
                break;
            case TSKind::TSL:
                stored.elements.push_back({PathElement::Index, ordinal});
                meta = meta->element_meta();
                break;
            case TSKind::TSD:
                // Need actual key value - not available from ordinal alone!
                throw std::runtime_error("Cannot convert TSD ordinal to stored path");
                break;
        }
    }
    return stored;
}

// Stored to Lightweight (for current cycle only)
LightweightPath to_lightweight(const TSValue* root, const StoredPath& stored) {
    LightweightPath light;
    const TSMeta* meta = root->ts_schema();

    for (const auto& elem : stored.elements) {
        switch (elem.kind) {
            case PathElement::Field:
                light.elements.push_back(meta->field_ordinal(elem.field));
                meta = meta->field_meta(elem.field);
                break;
            case PathElement::Index:
                light.elements.push_back(elem.index);
                meta = meta->element_meta();
                break;
            case PathElement::Key:
                // Look up current ordinal for key
                light.elements.push_back(
                    dynamic_cast<TSDValue*>(root)->key_ordinal(elem.key));
                meta = meta->value_meta();
                break;
        }
    }
    return light;
}
```

### E.10 Edge Path Usage in Wiring

The wiring system uses lightweight paths (all integers):

```cpp
// From hgraph/_builder/_graph_builder.py
@dataclass(frozen=True)
class Edge:
    src_node: int                    # Source node index (in graph)
    output_path: tuple[int, ...]     # Lightweight path (ordinals)
    dst_node: int                    # Destination node index
    input_path: tuple[int, ...]      # Lightweight path (ordinals)

// Special output_path first element:
// ERROR_PATH (-1)  → Error output
// STATE_PATH (-2)  → Recordable state
// Otherwise       → Main output + navigation
```

### E.11 Summary: Path Type Selection

| Use Case | Path Type | Reason |
|----------|-----------|--------|
| View navigation | Lightweight | Performance, same cycle |
| REF storage | Stored | Persistence, serialization |
| Wiring edges | Lightweight | Static structure, efficiency |
| Global state | Stored | Cross-cycle validity |
| Checkpointing | Stored | Serialization required |
| Debug/logging | Stored → String | Human readability |

**Key Constraint:** TSD paths with non-stringifiable keys cannot be represented as strings. Use structured StoredPath for such cases.
