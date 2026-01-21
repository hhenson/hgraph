# Schema Design

## Overview

The schema system provides runtime type information through a two-layer architecture:
- **TypeMeta**: Describes value/data types
- **TSMeta**: Describes time-series types (wraps TypeMeta with temporal semantics)

## TypeMeta

### Purpose
Describes the structure and operations for value types.

### Structure

```cpp
struct TypeMeta {
    // Identity
    std::string name_;            // Human-readable name (owned)

    // Layout (for memory allocation)
    size_t size_;                 // Static size in bytes
    size_t alignment_;            // Required alignment

    // Operations vtable (stored by value to reduce pointer chasing)
    type_ops ops_;                // Polymorphic operations - INLINE, not pointer

    // Children (for compounds)
    // - Compound/Bundle: vector of (name, TypeMeta*) pairs
    // - List: element TypeMeta*
    // - Dict: key TypeMeta*, value TypeMeta*
    // - Set: element TypeMeta*

    // Kind (for dispatch)
    TypeKind kind_;               // Atomic, Bundle, Tuple, List, Set, Map

    // Accessors
    size_t size() const { return size_; }
    size_t alignment() const { return alignment_; }
    const type_ops& ops() const { return ops_; }  // Return by const ref (stored by value)
    TypeKind kind() const { return kind_; }

    // SBO decision helper
    bool is_primitive() const {
        // Primitives: bool, integers, floats, nb::object
        // These can be stored inline in Value's data_ pointer
        return kind_ == TypeKind::Atomic && size_ <= sizeof(void*);
    }
};
```

**Performance Note**: `type_ops` is stored by value (not pointer) to reduce pointer chasing. When calling `meta->ops().default_construct(...)`, we avoid an extra indirection compared to `meta->ops_->default_construct(...)`. Since TypeMeta instances are long-lived and shared, the slightly larger TypeMeta size is a worthwhile trade-off for faster operation dispatch.

### Size Calculation

The `size_` field represents the **static** memory required:

| Kind | Size Contains |
|------|---------------|
| Atomic | sizeof(T) for the scalar type |
| Bundle | Sum of child sizes (with padding for alignment) |
| Tuple | Sum of element sizes (with padding for alignment) |
| List | sizeof(container_header) - elements allocated separately |
| Map | sizeof(container_header) - entries allocated separately |
| Set | sizeof(container_header) - elements allocated separately |

For containers, the static size covers the container bookkeeping (e.g., size, capacity, pointer to elements). The actual elements are managed dynamically by the container's vtable operations.

### Iterator Types

Two iterator concepts cover all iteration needs (shared with ts_ops layer):

```cpp
// Single-value iterator - yields View per element
// Used for: keys, values, indices, elements
struct ViewRange {
    struct iterator {
        View operator*() const;
        iterator& operator++();
        bool operator!=(const iterator& other) const;
    };
    iterator begin() const;
    iterator end() const;
};

// Key-value pair iterator - yields pair of Views per element
// Used for: items, field name+value pairs
struct ViewPairRange {
    struct iterator {
        std::pair<View, View> operator*() const;  // (key/name, value)
        iterator& operator++();
        bool operator!=(const iterator& other) const;
    };
    iterator begin() const;
    iterator end() const;
};
```

### type_ops Vtable

```cpp
// Kind-specific extension ops
struct atomic_ops {
    // Atomic types have no additional ops beyond common
};

struct bundle_ops {
    // Field access
    View (*field_at_index)(void* ptr, size_t idx);
    View (*field_at_name)(void* ptr, std::string_view name);
    size_t (*field_count)(const void* ptr);
    std::string_view (*field_name)(const void* ptr, size_t idx);
    // Iteration (ViewPairRange: field_name -> value)
    ViewPairRange (*items)(const void* ptr);
};

struct tuple_ops {
    // Element access (positional only, no names)
    View (*at)(void* ptr, size_t idx);
    size_t (*size)(const void* ptr);
    // Iteration (ViewPairRange: index -> value)
    ViewPairRange (*items)(const void* ptr);
};

struct list_ops {
    // Element access
    View (*at)(void* ptr, size_t idx);
    size_t (*size)(const void* ptr);

    // Mutation
    void (*append)(void* ptr, View elem);
    void (*clear)(void* ptr);

    // Iteration
    ViewRange (*values)(const void* ptr);
    ViewPairRange (*items)(const void* ptr);  // index -> value
};

struct map_ops {
    // Entry access
    View (*at)(void* ptr, View key);
    bool (*contains)(const void* ptr, View key);
    size_t (*size)(const void* ptr);

    // Mutation
    void (*set_item)(void* ptr, View key, View val);
    bool (*remove)(void* ptr, View key);
    void (*clear)(void* ptr);

    // Iteration
    ViewRange (*keys)(const void* ptr);
    ViewPairRange (*items)(const void* ptr);  // key -> value
};

struct set_ops {
    // Membership
    bool (*contains)(const void* ptr, View elem);
    size_t (*size)(const void* ptr);

    // Mutation
    void (*add)(void* ptr, View elem);
    bool (*remove)(void* ptr, View elem);
    void (*clear)(void* ptr);

    // Iteration
    ViewRange (*values)(const void* ptr);
};

struct type_ops {
    // === Common operations (all kinds) ===

    // Lifecycle
    void (*construct)(void* dst);
    void (*destroy)(void* ptr);
    void (*copy)(void* dst, const void* src);
    void (*move)(void* dst, void* src);

    // Comparison
    bool (*equals)(const void* a, const void* b);

    // Hashing
    size_t (*hash)(const void* ptr);

    // String representation
    std::string (*to_string)(const void* ptr);

    // Python conversion (all types)
    nb::object (*to_python)(const void* ptr);
    void (*from_python)(void* ptr, nb::object obj);

    // === Kind-specific extension ops (tagged by TypeMeta::kind_) ===
    TypeKind kind;
    union {
        atomic_ops atomic;
        bundle_ops bundle;
        tuple_ops tuple;
        list_ops list;
        set_ops set;
        map_ops map;
    } specific;
};
```

**Note**: The `specific` union is tagged by the `kind` field in type_ops. Only access the union member corresponding to the type's kind. This keeps all operations inline in a single struct, avoiding additional pointer chasing for kind-specific operations.

### TypeKind Enumeration

| Kind | Examples | Notes |
|------|----------|-------|
| Atomic | int, float, string, date, datetime | Leaf types (scalars) |
| Bundle | struct, compound scalar | Named fields |
| Tuple | (int, float) | Positional fields (unnamed) |
| List | List[T] | Homogeneous sequence |
| Map | Map[K, V] | Key-value mapping |
| Set | Set[T] | Unique elements |

## TSMeta

### Purpose
Describes time-series types with temporal tracking semantics.

### Structure

```cpp
struct TSMeta {
    // Identity
    TSKind kind_;                     // TS, TSB, TSL, TSD, TSS, REF, SIGNAL

    // The underlying value schema
    const TypeMeta* value_schema_;    // Schema for user-visible data

    // Timestamp tracking schema (mirrors data structure)
    const TypeMeta* time_meta_;       // Schema for modification times

    // Observer list schema (mirrors data structure)
    const TypeMeta* observer_meta_;   // Schema for observer lists

    // Time-series operations vtable (stored by value to reduce pointer chasing)
    ts_ops ops_;                      // Polymorphic operations - INLINE, not pointer

    // Accessors
    TSKind kind() const { return kind_; }
    const TypeMeta& value_schema() const { return *value_schema_; }
    const ts_ops& ops() const { return ops_; }  // Return by const ref (stored by value)
};
```

### TSOutput/TSInput Construction

TSOutput and TSInput are constructed directly from TSMeta - the schema provides the type information needed to initialize all three value structures.

```cpp
// Construction - TSMeta passed to constructor
TSOutput output{ts_meta, node_ptr};
TSInput input{ts_meta, node_ptr};

// TSOutput constructor uses TSMeta to initialize:
// - native_value_ (using value_schema_)
// - time tracking (using time_meta_)
// - observer lists (using observer_meta_)
class TSOutput {
public:
    TSOutput(const TSMeta* meta, Node* node);
};

class TSInput {
public:
    TSInput(const TSMeta* meta, Node* node);
};
```

### Python API Wrappers

TSMeta provides factory methods for creating Python wrapper objects (since wrappers are kind-specific):

```cpp
class TSMeta {
public:
    // Python API wrapper factory - creates kind-specific wrapper
    virtual nb::object as_output_api(TSView view, std::shared_ptr<Node> node) const = 0;
    virtual nb::object as_input_api(TSInputView view, std::shared_ptr<Node> node) const = 0;
};
```

### TSMeta Hierarchy

Each time-series kind has a specialized TSMeta subclass:

| TSMeta Subclass | TS Kind | Additional Info |
|-----------------|---------|-----------------|
| TSValueMeta | TS[T] | Scalar time-series |
| TSBTypeMeta | TSB[...] | Field schemas, peered flag |
| TSLTypeMeta | TSL[T] | Element schema, size bounds |
| TSDTypeMeta | TSD[K,V] | Key schema, value schema |
| TSSTypeMeta | TSS[T] | Element schema |
| TSWTypeMeta | TSW[T] | Window configuration |
| TSRefMeta | REF[T] | Referenced TS schema |
| SIGNALMeta | SIGNAL | No value, pure tick notification |

```cpp
class TSValueMeta : public TSMeta {
    // TS[T] - scalar time-series
    // value_schema_: T
    // time_meta_: engine_time_t
    // observer_meta_: ObserverList
};

class TSBTypeMeta : public TSMeta {
    std::vector<std::pair<std::string, const TSMeta*>> fields_;
    bool peered_;  // All fields tick together?

public:
    const TSMeta* field_meta(std::string_view name) const;
    const TSMeta* field_meta(size_t index) const;
    size_t field_count() const;
    bool is_peered() const { return peered_; }

    // Schema generation:
    // value_schema_: Bundle of field value types
    // time_meta_: engine_time_t (peered) or Bundle[engine_time_t, ...] (un-peered)
    // observer_meta_: ObserverList (peered) or Bundle[ObserverList, ...] (un-peered)
};

class TSLTypeMeta : public TSMeta {
    const TSMeta* element_meta_;
    size_t size_;  // Fixed size (0 for dynamic)

public:
    const TSMeta* element_meta() const { return element_meta_; }
    size_t size() const { return size_; }
    bool is_fixed_size() const { return size_ > 0; }

    // Schema generation:
    // value_schema_: List[element_value_type]
    // time_meta_: List[engine_time_t]
    // observer_meta_: List[ObserverList]
};

class TSDTypeMeta : public TSMeta {
    const TypeMeta* key_meta_;
    const TSMeta* value_meta_;

public:
    const TypeMeta* key_meta() const { return key_meta_; }
    const TSMeta* value_meta() const { return value_meta_; }

    // Schema generation:
    // value_schema_: Map[K, V] (MapStorage with key set)
    // time_meta_: List[engine_time_t] (parallel by slot index)
    // observer_meta_: List[ObserverList] (parallel by slot index)
};

class TSSTypeMeta : public TSMeta {
    const TypeMeta* element_meta_;

public:
    const TypeMeta* element_meta() const { return element_meta_; }

    // Schema generation:
    // value_schema_: Set[T] (SetStorage with delta tracking)
    // time_meta_: List[engine_time_t] (parallel by slot index)
    // observer_meta_: ObserverList (container-level only)
};

class TSWTypeMeta : public TSMeta {
    const TypeMeta* element_meta_;
    size_t period_;           // Cyclic buffer capacity
    engine_time_delta_t min_window_period_;  // Minimum time span

public:
    const TypeMeta* element_meta() const { return element_meta_; }
    size_t period() const { return period_; }
    engine_time_delta_t min_window_period() const { return min_window_period_; }

    // Schema generation:
    // value_schema_: WindowStorage[T] (custom type with cyclic + queue buffers)
    // time_meta_: (embedded in WindowStorage - times stored with values)
    // observer_meta_: ObserverList (container-level only)
};

class TSRefMeta : public TSMeta {
    const TSMeta* referenced_meta_;

public:
    const TSMeta* referenced_meta() const { return referenced_meta_; }

    // Schema generation:
    // value_schema_: TimeSeriesReference (pointer/handle to referenced TS)
    // time_meta_: engine_time_t
    // observer_meta_: ObserverList
};

class SIGNALMeta : public TSMeta {
    // Schema generation:
    // value_schema_: void (no data)
    // time_meta_: engine_time_t
    // observer_meta_: ObserverList
};
```

### ts_ops Vtable

The TS layer uses the same `ViewRange` and `ViewPairRange` iterator types as the value layer.

```cpp
// TS kind-specific extension ops
struct ts_scalar_ops {
    // Scalar TS has no additional ops beyond common
};

struct tsb_ops {
    // Field access
    TSView (*field_at_index)(void* ptr, size_t idx);
    TSView (*field_at_name)(void* ptr, std::string_view name);
    size_t (*field_count)(const void* ptr);
    // Iteration (all return ViewPairRange: field_name -> TSView)
    ViewPairRange (*items)(const void* ptr);
    ViewPairRange (*valid_items)(const void* ptr);
    ViewPairRange (*modified_items)(const void* ptr);
};

struct tsl_ops {
    // Element access
    TSView (*at)(void* ptr, size_t idx);
    size_t (*size)(const void* ptr);
    // Values iteration (ViewRange of TSView)
    ViewRange (*values)(const void* ptr);
    ViewRange (*valid_values)(const void* ptr);
    ViewRange (*modified_values)(const void* ptr);
    // Items iteration (ViewPairRange: index -> TSView)
    ViewPairRange (*items)(const void* ptr);
    ViewPairRange (*valid_items)(const void* ptr);
    ViewPairRange (*modified_items)(const void* ptr);
};

struct tsd_ops {
    // Entry access
    TSView (*at)(void* ptr, View key);
    bool (*contains)(const void* ptr, View key);
    void (*set_item)(void* ptr, View key, TSView value);
    bool (*remove)(void* ptr, View key);
    size_t (*size)(const void* ptr);
    TSSView (*key_set)(const void* ptr);  // Keys as TSS-like view
    // Keys iteration (ViewRange)
    ViewRange (*keys)(const void* ptr);
    ViewRange (*valid_keys)(const void* ptr);
    ViewRange (*added_keys)(const void* ptr);
    ViewRange (*removed_keys)(const void* ptr);
    ViewRange (*modified_keys)(const void* ptr);
    // Items iteration (ViewPairRange: key -> TSView)
    ViewPairRange (*items)(const void* ptr);
    ViewPairRange (*valid_items)(const void* ptr);
    ViewPairRange (*added_items)(const void* ptr);
    ViewPairRange (*modified_items)(const void* ptr);
};

struct tss_ops {
    // Membership
    bool (*contains)(const void* ptr, View elem);
    size_t (*size)(const void* ptr);
    // Mutation
    void (*add)(void* ptr, View elem);
    bool (*remove)(void* ptr, View elem);
    // Values iteration (ViewRange)
    ViewRange (*values)(const void* ptr);
    ViewRange (*added)(const void* ptr);
    ViewRange (*removed)(const void* ptr);
    bool (*was_added)(const void* ptr, View elem);
    bool (*was_removed)(const void* ptr, View elem);
};

struct tsw_ops {
    // Window properties
    size_t (*size)(const void* ptr);
    size_t (*capacity)(const void* ptr);
    engine_time_t (*oldest_time)(const void* ptr);
    engine_time_t (*newest_time)(const void* ptr);

    // Value access
    View (*at_time)(const void* ptr, engine_time_t t);

    // Iteration (ViewRange of values, oldest first)
    ViewRange (*values)(const void* ptr);
    ViewRange (*times)(const void* ptr);
    ViewRange (*range)(const void* ptr, engine_time_t start, engine_time_t end);

    // Items iteration (ViewPairRange: time -> value)
    ViewPairRange (*items)(const void* ptr);
};

struct ref_ops {
    // REF uses common value()/set_value() with TimeSeriesReference
    // Reserved for future operations
};

struct signal_ops {
    void (*tick)(void* ptr);
};

struct ts_ops {
    // === Common operations (all TS kinds) ===

    // Lifecycle
    void (*construct)(void* dst, engine_time_t time);
    void (*destroy)(void* ptr);
    void (*copy)(void* dst, const void* src);

    // Tick detection
    bool (*modified)(const void* ptr, engine_time_t time);
    bool (*valid)(const void* ptr);
    bool (*all_valid)(const void* ptr);  // For composites: all children valid

    // Time access
    engine_time_t (*last_modified_time)(const void* ptr);
    void (*set_modified_time)(void* ptr, engine_time_t time);

    // Value access (all TS types)
    View (*value)(const void* ptr);
    void (*set_value)(void* ptr, View value);
    void (*apply_delta)(void* ptr, DeltaView delta);

    // Python conversion (all TS types)
    nb::object (*to_python)(const void* ptr);
    nb::object (*delta_to_python)(const void* ptr);

    // Delta access
    DeltaView (*delta)(const void* ptr);

    // === Kind-specific extension ops (tagged by TSMeta::ts_kind_) ===
    TSKind kind;
    union {
        ts_scalar_ops scalar;
        tsb_ops bundle;
        tsl_ops list;
        tsd_ops dict;
        tss_ops set;
        tsw_ops window;
        ref_ops ref;
        signal_ops signal;
    } specific;
};
```

**Note**: The `specific` union is tagged by the `kind` field in ts_ops. Only access the union member corresponding to the TS type's kind.

### Three-Value Structure

TSValue contains three parallel values (internal implementation):
1. **value_**: User-visible data (described by `value_schema_`)
2. **time_**: Modification timestamps (mirrors data structure, described by `time_meta_`)
3. **observer_**: Observer lists (mirrors data structure, described by `observer_meta_`)

## Type Registration

### TypeRegistry

Manages value type schemas. Two caches provide lookup by name or C++ type.

```cpp
class TypeRegistry {
    ankerl::unordered_dense::map<std::string, TypeMeta*> name_cache_;
    ankerl::unordered_dense::map<std::type_index, TypeMeta*> type_cache_;

public:
    static TypeRegistry& instance();

    // Registration - template form (populates both caches)
    template<typename T>
    void register_type(const std::string& name);

    // Registration - template with custom ops
    template<typename T>
    void register_type(const std::string& name, type_ops ops);

    // Registration - name only (populates name cache only)
    void register_type(const std::string& name, type_ops ops);

    // Lookup by name
    const TypeMeta& get(const std::string& name) const;

    // Lookup by C++ type (template shortcut)
    template<typename T>
    const TypeMeta& get() const;

    // Lookup from Python type
    const TypeMeta& from_python_type(nb::type_object py_type) const;
};

// Usage
TypeRegistry& registry = TypeRegistry::instance();
registry.register_type<int64_t>("int");
registry.register_type<double>("float");
registry.register_type<nb::object>("object");

const TypeMeta& int_schema = TypeMeta::get("int");       // By name
const TypeMeta& int_schema2 = TypeMeta::get<int64_t>();  // Template shortcut
```

### TSRegistry

Manages time-series type schemas. Same dual-cache pattern.

```cpp
class TSRegistry {
    ankerl::unordered_dense::map<std::string, TSMeta*> name_cache_;
    ankerl::unordered_dense::map<std::type_index, TSMeta*> type_cache_;

public:
    static TSRegistry& instance();

    // Registration
    template<typename T>
    void register_type(const std::string& name);

    template<typename T>
    void register_type(const std::string& name, ts_ops ops);

    void register_type(const std::string& name, ts_ops ops);

    // Lookup
    const TSMeta& get(const std::string& name) const;

    template<typename T>
    const TSMeta& get() const;
};

// Usage
const TSMeta& price_ts = TSMeta::get("TS[float]");
```

### Built-in Atomic Types

| Name | Python | C++ | Size |
|------|--------|-----|------|
| `bool` | `bool` | `bool` | 1 byte |
| `int` | `int` | `int64_t` | 8 bytes |
| `float` | `float` | `double` | 8 bytes |
| `date` | `datetime.date` | `engine_date_t` | 4 bytes |
| `datetime` | `datetime.datetime` | `engine_time_t` | 8 bytes |
| `timedelta` | `datetime.timedelta` | `engine_time_delta_t` | 8 bytes |
| `object` | `object` | `nb::object` | 8 bytes |

## Schema Builders

Composite types are constructed using fluent builders that validate structure and register with the appropriate registry.

### Value Type Builders

| Builder | Creates | Key Methods |
|---------|---------|-------------|
| BundleBuilder | BundleMeta | `set_name()`, `add_field(name, type)` |
| TupleBuilder | TupleMeta | `add_element(type)` |
| ListBuilder | ListMeta | `set_element_type()`, `set_size()` |
| SetBuilder | SetMeta | `set_element_type()` |
| MapBuilder | MapMeta | `set_key_type()`, `set_value_type()` |

```cpp
// Bundle with named fields
const TypeMeta& point = BundleBuilder()
    .set_name("Point")
    .add_field("x", TypeMeta::get("float"))
    .add_field("y", TypeMeta::get("float"))
    .build();

// Tuple (positional only)
const TypeMeta& pair = TupleBuilder()
    .add_element(TypeMeta::get("int"))
    .add_element(TypeMeta::get("float"))
    .build();

// List (dynamic or fixed-size)
const TypeMeta& prices = ListBuilder()
    .set_element_type(TypeMeta::get("float"))
    .set_size(10)  // Optional: 0 for dynamic
    .build();

// Set
const TypeMeta& ids = SetBuilder()
    .set_element_type(TypeMeta::get("int"))
    .build();

// Map
const TypeMeta& scores = MapBuilder()
    .set_key_type(TypeMeta::get("int"))
    .set_value_type(TypeMeta::get("float"))
    .build();
```

### Time-Series Builders

| Builder | Creates | Key Methods |
|---------|---------|-------------|
| TSBuilder | TSMeta | `set_value_type()` |
| TSBBuilder | TSBMeta | `set_name()`, `add_field(name, ts_meta)`, `set_peered()` |
| TSLBuilder | TSLMeta | `set_element_ts()`, `set_size()` |
| TSDBuilder | TSDMeta | `set_key_type()`, `set_value_ts()` |
| TSSBuilder | TSSMeta | `set_element_type()` |
| TSWBuilder | TSWMeta | `set_element_type()`, `set_period()`, `set_min_window_period()` |
| REFBuilder | REFMeta | `set_target_ts()` |

```cpp
// Scalar TS
const TSMeta& price_ts = TSBuilder()
    .set_value_type(TypeMeta::get("float"))
    .build();

// Bundle TS
const TSMeta& quote_ts = TSBBuilder()
    .set_name("Quote")
    .add_field("bid", TSBuilder().set_value_type(TypeMeta::get("float")).build())
    .add_field("ask", TSBuilder().set_value_type(TypeMeta::get("float")).build())
    .set_peered(false)
    .build();

// List TS
const TSMeta& prices_ts = TSLBuilder()
    .set_element_ts(price_ts)
    .set_size(10)
    .build();

// Dict TS
const TSMeta& price_dict_ts = TSDBuilder()
    .set_key_type(TypeMeta::get("int"))
    .set_value_ts(price_ts)
    .build();

// Set TS
const TSMeta& active_ids_ts = TSSBuilder()
    .set_element_type(TypeMeta::get("int"))
    .build();

// Window TS
const TSMeta& price_window_ts = TSWBuilder()
    .set_element_type(TypeMeta::get("float"))
    .set_period(100)
    .set_min_window_period(std::chrono::hours(1))
    .build();

// Reference TS
const TSMeta& price_ref_ts = REFBuilder()
    .set_target_ts(price_ts)
    .build();
```

### Builder Design

All builders follow the same pattern:

1. **Fluent interface**: Methods return `Builder&` for chaining
2. **Validation on build()**: Throws if required fields missing or invalid
3. **Registry integration**: `build()` registers the schema and returns a reference
4. **Deduplication**: Structurally identical schemas share the same TypeMeta/TSMeta instance

## References

- User Guide: `01_SCHEMA.md`
- Research: `11_VALUE_VIEW_REIMPLEMENTATION.md`
