# Schema Design

## Overview

The schema system provides runtime type information through a two-layer architecture:
- **TypeMeta**: Describes value/data types
- **TSMeta**: Describes time-series types (wraps TypeMeta with temporal semantics)

### Status Update (2026-02-13)

The current implementation has locked in the following schema/value decisions relevant to layout:

1. Null is a state, not a type.
2. `Value(schema)` is typed-null by default.
3. Nested nullability is tracked with validity bitmaps for bundle/tuple/list/map-values.
4. Map keys and set elements are non-null; map values are nullable.
5. Internal storage is optimized for compactness and algorithm efficiency; Arrow compatibility is maintained at buffer/bitmap semantics level (zero-copy where layout permits).

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
    // - Bundle: vector of (name, TypeMeta*) pairs
    // - List/Set/CyclicBuffer/Queue: element TypeMeta*
    // - Map: key TypeMeta*, value TypeMeta*
    // - Tuple: element TypeMeta* array

    // Kind (for dispatch)
    TypeKind kind_;               // Atomic, Bundle, Tuple, List, Set, Map, CyclicBuffer, Queue

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
| CyclicBuffer | sizeof(container_header) - elements in fixed-size ring buffer |
| Queue | sizeof(container_header) - elements allocated separately |

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
    // Ordering for sorted containers and comparison operators
    bool (*less_than)(const void* a, const void* b);
};

struct bundle_ops {
    // Field access by index
    size_t (*size)(const void* ptr);
    View (*at)(void* ptr, size_t idx);
    void (*set_at)(void* ptr, size_t idx, View value);
    // Field access by name
    View (*get_field)(void* ptr, std::string_view name);
    void (*set_field)(void* ptr, std::string_view name, View value);
    // Iteration (ViewPairRange: field_name -> value)
    ViewPairRange (*items)(const void* ptr);
};

struct tuple_ops {
    // Element access (positional only, no names)
    size_t (*size)(const void* ptr);
    View (*at)(void* ptr, size_t idx);
    void (*set_at)(void* ptr, size_t idx, View value);
    // Iteration (ViewPairRange: index -> value)
    ViewPairRange (*items)(const void* ptr);
};

struct list_ops {
    // Element access
    size_t (*size)(const void* ptr);
    View (*at)(void* ptr, size_t idx);
    void (*set_at)(void* ptr, size_t idx, View value);

    // Mutation
    void (*append)(void* ptr, View elem);
    void (*clear)(void* ptr);

    // Iteration
    ViewRange (*values)(const void* ptr);
    ViewPairRange (*items)(const void* ptr);  // index -> value
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

struct cyclic_buffer_ops {
    // Element access
    size_t (*size)(const void* ptr);
    View (*at)(void* ptr, size_t idx);
    void (*set_at)(void* ptr, size_t idx, View value);

    // Mutation
    void (*push)(void* ptr, View elem);
    void (*pop)(void* ptr);
    void (*clear)(void* ptr);

    // Capacity
    size_t (*capacity)(const void* ptr);

    // Iteration
    ViewRange (*values)(const void* ptr);
};

struct queue_ops {
    // Element access
    size_t (*size)(const void* ptr);
    View (*at)(void* ptr, size_t idx);

    // Mutation
    void (*push)(void* ptr, View elem);
    void (*pop)(void* ptr);
    void (*clear)(void* ptr);

    // Capacity
    size_t (*max_capacity)(const void* ptr);

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
    void (*move_construct)(void* dst, void* src);

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
        cyclic_buffer_ops cyclic_buffer;
        queue_ops queue;
    } specific;
};
```

**Note**: The `specific` union is tagged by the `kind` field in type_ops. Only access the union member corresponding to the type's kind. This keeps all operations inline in a single struct, avoiding additional pointer chasing for kind-specific operations.

### TypeKind Enumeration

| Kind | Examples | Notes |
|------|----------|-------|
| Atomic | int, float, string, date, datetime | Leaf types (scalars) |
| Bundle | struct, compound scalar | Named fields (index + name access) |
| Tuple | (int, float) | Positional fields (unnamed, index access only) |
| List | List[T] | Homogeneous dynamic sequence |
| Set | Set[T] | Unique unordered elements |
| Map | Map[K, V] | Key-value mapping |
| CyclicBuffer | CyclicBuffer[T, N] | Fixed-size circular buffer (TSW storage) |
| Queue | Queue[T] | FIFO with optional max capacity |

## Set and Map Storage Architecture

Set and Map use a layered, protocol-based architecture that enables:
- **Composition**: Map HAS-A Set (Map is Set + parallel value array)
- **Memory stability**: Slot-based storage with stable addresses
- **Toll-free casting**: `MapStorage.as_set()` returns reference to contained `SetStorage`
- **Arrow/NumPy conversion**: contiguous buffer export where layout permits

### Layer Structure

```
┌─────────────────────────────────────────────────────────────┐
│                     type_ops layer                          │
│  SetStorage, MapStorage - value semantics via set_ops/map_ops│
├─────────────────────────────────────────────────────────────┤
│                    KeySet (core)                            │
│  Slot management, hash index, membership, liveness bits     │
└─────────────────────────────────────────────────────────────┘
```

### KeySet (Core Membership Storage)

KeySet manages **membership only** - no values, no timestamps. It provides:
- Slot-based storage with stable addresses (keys never move)
- Alive bitset tracking for liveness (`alive_[slot] == 1` means live)
- Hash indexing via `ankerl::unordered_dense::set` with transparent hash/equality
- Observer protocol for extensions to track slot lifecycle

```cpp
// Observer interface for slot lifecycle events
struct SlotObserver {
    virtual ~SlotObserver() = default;
    virtual void on_capacity(size_t old_cap, size_t new_cap) = 0;  // Resize parallel arrays
    virtual void on_insert(size_t slot) = 0;                       // Initialize slot data
    virtual void on_erase(size_t slot) = 0;                        // Cleanup slot data
    virtual void on_clear() = 0;                                   // Reset all slots
};

// Core membership storage
class KeySet {
public:
    const TypeMeta* key_meta_;

    // Key storage
    std::vector<std::byte> keys_;        // [key_size * capacity] - stable addresses
    sul::dynamic_bitset<> alive_;        // [capacity] - 1 = live, 0 = dead

    // Hash index (standard library implementation)
    ankerl::unordered_dense::set<size_t> index_set_;

    // Slot management
    std::vector<size_t> free_list_;      // Available slots for reuse
    size_t live_count_;
    size_t capacity_;

    // Extension protocol
    std::vector<SlotObserver*> observers_;

    // Core operations
    std::pair<size_t, bool> insert(const void* key_data);  // Returns (slot, was_new)
    bool erase(const void* key_data);
    size_t find(const void* key_data) const;  // npos-style on miss

    bool is_alive(size_t slot) const { return slot < alive_.size() && alive_[slot]; }
    void* key_at(size_t slot) { return keys_.data() + slot * key_meta_->size(); }
};
```

### SetStorage (type_ops Layer)

SetStorage wraps KeySet and implements `set_ops`:

```cpp
class SetStorage {
    KeySet keys_;

public:
    explicit SetStorage(const TypeMeta* element_meta) : keys_{element_meta} {}

    // set_ops implementation
    bool contains(const void* elem) const { return keys_.find(elem).has_value(); }
    bool add(const void* elem) { return keys_.insert(elem).second; }
    bool remove(const void* elem) { return keys_.erase(elem); }
    size_t size() const { return keys_.live_count_; }
    void clear();

    ViewRange values() const;  // Iterate alive slots as Views

    // Access to underlying KeySet
    KeySet& key_set() { return keys_; }
    const KeySet& key_set() const { return keys_; }

    // Toll-free Arrow access
    const std::byte* data() const { return keys_.keys_.data(); }
};
```

### ValueArray (Parallel Value Extension)

ValueArray observes KeySet and maintains a parallel array of values:

```cpp
class ValueArray : public SlotObserver {
    const TypeMeta* value_meta_;
    std::vector<std::byte> values_;    // [value_size * capacity]
    std::vector<std::byte> validity_;  // [ceil(capacity/8)] value-null bitmap

public:
    explicit ValueArray(const TypeMeta* value_meta) : value_meta_(value_meta) {}

    void on_capacity(size_t, size_t new_cap) override {
        values_.resize(new_cap * value_meta_->size());
    }
    void on_insert(size_t slot) override {
        value_meta_->ops().construct(value_at(slot));
    }
    void on_erase(size_t slot) override {
        value_meta_->ops().destroy(value_at(slot));
    }
    void on_clear() override;

    void* value_at(size_t slot) {
        return values_.data() + slot * value_meta_->size();
    }

    // Null-aware access
    const void* value_or_null_at(size_t slot) const;
    bool is_valid_slot(size_t slot) const;
    void set_valid_slot(size_t slot, bool valid);

    // Contiguous buffer access
    std::byte* data() { return values_.data(); }
};
```

### MapStorage (Composes SetStorage + ValueArray)

Map HAS-A Set plus a parallel value array:

```cpp
class MapStorage {
    SetStorage set_;
    ValueArray values_;

public:
    MapStorage(const TypeMeta* key_meta, const TypeMeta* value_meta)
        : set_(key_meta), values_(value_meta)
    {
        set_.key_set().observers_.push_back(&values_);
    }

    // map_ops implementation
    bool contains(const void* key) const { return set_.contains(key); }
    size_t size() const { return set_.size(); }

    void* at(const void* key) {
        auto slot = set_.key_set().find(key);
        if (!slot) throw std::out_of_range("key not found");
        return values_.value_at(*slot);
    }

    void* set_item(const void* key, const void* value) {
        auto [slot, was_new] = set_.key_set().insert(key);
        if (value) {
            values_.value_meta_->ops().copy(values_.value_at(slot), value);
            values_.set_valid_slot(slot, true);
        } else {
            values_.set_valid_slot(slot, false);  // present key, null value
        }
        return values_.value_at(slot);
    }

    bool remove(const void* key) { return set_.remove(key); }

    // Toll-free casting: Map → Set
    const SetStorage& as_set() const { return set_; }

    ViewRange keys() const { return set_.values(); }
    ViewPairRange items() const;

    // Toll-free Arrow access
    const std::byte* key_data() const { return set_.data(); }
    std::byte* value_data() { return values_.data(); }
};
```

### Composition Diagram

```
MapStorage
├── SetStorage (as_set() returns reference)
│   └── KeySet
│       ├── keys_[]        ──► Arrow key column
│       ├── alive_ bits    ──► live slot mask
│       └── index_set_     (ankerl::unordered_dense)
└── ValueArray (observes KeySet)
    ├── values_[]          ──► value payload column
    └── validity_ bits     ──► value-null mask
```

### Slot Handle for Stable References

External references use slot identity with liveness checks:

```cpp
struct SlotHandle {
    size_t slot;

    bool is_valid(const KeySet& ks) const {
        return ks.is_alive(slot);
    }
};
```

### Design Rationale

| Decision | Rationale |
|----------|-----------|
| Composition (Map HAS-A Set) | Enables toll-free casting; shared key management |
| Alive-bitset liveness | Compact membership tracking with cheap slot checks |
| SlotObserver protocol | Decouples extensions; each owns its memory |
| `ankerl::unordered_dense` | Proven implementation; no hand-coded hash table |
| Parallel arrays + validity bits | Compact internal processing with Arrow-compatible export semantics |

## TSMeta

### Purpose
Describes time-series types with temporal tracking semantics.

### Structure

TSMeta uses a **flat struct with tagged union** approach - the `kind` field determines which other fields are valid. This is more memory-efficient than an inheritance hierarchy and enables simple serialization.

```cpp
/**
 * Categories of time-series types.
 */
enum class TSKind : uint8_t {
    TSValue,     // TS[T] - scalar time-series
    TSS,         // TSS[T] - time-series set
    TSD,         // TSD[K, V] - time-series dict
    TSL,         // TSL[TS, Size] - time-series list
    TSW,         // TSW[T, size, min_size] - time-series window
    TSB,         // TSB[Schema] - time-series bundle
    REF,         // REF[TS] - reference to time-series
    SIGNAL       // SIGNAL - presence/absence marker
};

/**
 * Metadata for a single field in a TSB (time-series bundle).
 */
struct TSBFieldInfo {
    const char* name;        // Field name (owned by registry)
    size_t index;            // 0-based field index
    const TSMeta* ts_type;   // Field's time-series schema
};

/**
 * Complete metadata describing a time-series type.
 *
 * TSMeta is the schema for a time-series type. It uses a tagged union approach
 * where the `kind` field determines which members are valid:
 *
 * - TSValue: value_type is valid
 * - TSS: value_type is valid (set element type)
 * - TSD: key_type, element_ts are valid
 * - TSL: element_ts, fixed_size are valid
 * - TSW: value_type, is_duration_based, window union are valid
 * - TSB: fields, field_count, bundle_name, python_type are valid
 * - REF: element_ts is valid (referenced time-series)
 * - SIGNAL: no additional fields
 */
struct TSMeta {
    TSKind kind;

    // ========== Value/Key Types ==========
    // Valid for: TSValue (value), TSS (element), TSW (value), TSD (key)

    /// Value type - valid for: TSValue, TSS, TSW
    const TypeMeta* value_type = nullptr;

    /// Key type - valid for: TSD
    const TypeMeta* key_type = nullptr;

    // ========== Nested Time-Series ==========
    // Valid for: TSD (value TS), TSL (element TS), REF (referenced TS)

    /// Element time-series - valid for: TSD (value), TSL (element), REF (referenced)
    const TSMeta* element_ts = nullptr;

    // ========== Size Information ==========

    /// Fixed size - valid for: TSL (0 = dynamic SIZE)
    size_t fixed_size = 0;

    // ========== Window Parameters ==========
    // Valid for: TSW

    /// True if duration-based window, false if tick-based
    bool is_duration_based = false;

    /// Window parameters union - saves space since only one is used
    union WindowParams {
        struct {
            size_t period;
            size_t min_period;
        } tick;
        struct {
            engine_time_delta_t time_range;
            engine_time_delta_t min_time_range;
        } duration;

        // Default constructor - initialize tick params
        WindowParams() : tick{0, 0} {}
    } window;

    // ========== Bundle Fields ==========
    // Valid for: TSB

    /// Field metadata array - valid for: TSB
    const TSBFieldInfo* fields = nullptr;

    /// Number of fields - valid for: TSB
    size_t field_count = 0;

    /// Bundle schema name - valid for: TSB
    const char* bundle_name = nullptr;

    /// Python type for reconstruction - valid for: TSB (optional)
    /// When set, to_python conversion returns an instance of this class.
    /// When not set (None), returns a dict.
    nb::object python_type;

    // ========== Helper Methods ==========

    /**
     * Check if this is a collection time-series.
     * @return true if TSS, TSD, TSL, or TSB
     */
    bool is_collection() const noexcept {
        return kind == TSKind::TSS || kind == TSKind::TSD ||
               kind == TSKind::TSL || kind == TSKind::TSB;
    }

    /**
     * Check if this is a scalar-like time-series.
     * @return true if TS, TSW, or SIGNAL
     */
    bool is_scalar_ts() const noexcept {
        return kind == TSKind::TSValue || kind == TSKind::TSW ||
               kind == TSKind::SIGNAL;
    }
};
```

### Field Validity by TSKind

| TSKind | value_type | key_type | element_ts | fixed_size | window | fields/field_count | bundle_name |
|--------|------------|----------|------------|------------|--------|-------------------|-------------|
| TSValue | Y (value type) | - | - | - | - | - | - |
| TSS | Y (element type) | - | - | - | - | - | - |
| TSD | - | Y | Y (value TS) | - | - | - | - |
| TSL | - | - | Y (element TS) | Y (0=dynamic) | - | - | - |
| TSW | Y (value type) | - | - | - | Y | - | - |
| TSB | - | - | - | - | - | Y | Y |
| REF | - | - | Y (referenced TS) | - | - | - | - |
| SIGNAL | - | - | - | - | - | - | - |

### Operations Retrieval

Operations for time-series types are retrieved via the `get_ts_ops()` function rather than being stored inline in TSMeta. This separation keeps TSMeta lightweight and allows different ops implementations for TSW variants.

```cpp
// Get ops by TSKind (for TSW, returns scalar_ops)
const ts_ops* get_ts_ops(TSKind kind);

// Get ops by TSMeta (for TSW, selects based on is_duration_based)
const ts_ops* get_ts_ops(const TSMeta* meta);

// Usage
const ts_ops* ops = get_ts_ops(ts_meta);
bool is_modified = ops->modified(view_data, current_time);
```

### Schema Generation via TSMetaSchemaCache

Parallel Value structures (time tracking, observer lists, delta tracking, etc.) are generated dynamically based on the TSMeta structure. The `TSMetaSchemaCache` singleton provides caching for these generated schemas:

```cpp
class TSMetaSchemaCache {
public:
    static TSMetaSchemaCache& instance();

    // Generated schemas - cached per TSMeta
    const TypeMeta* get_time_schema(const TSMeta* ts_meta);
    const TypeMeta* get_observer_schema(const TSMeta* ts_meta);
    const TypeMeta* get_delta_value_schema(const TSMeta* ts_meta);
    const TypeMeta* get_link_schema(const TSMeta* ts_meta);
    const TypeMeta* get_active_schema(const TSMeta* ts_meta);  // For TSInput
};
```

**Schema generation rules:**

| Schema | Scalar Types | TSD | TSB | TSL |
|--------|--------------|-----|-----|-----|
| time | engine_time_t | tuple[t, var_list[...]] | tuple[t, fixed_list[...]] | tuple[t, fixed_list[...]] |
| observer | ObserverList | tuple[OL, var_list[...]] | tuple[OL, fixed_list[...]] | tuple[OL, fixed_list[...]] |
| delta_value | nullptr | MapDelta | BundleDeltaNav* | ListDeltaNav* |
| link | nullptr | bool | fixed_list[bool] | bool |
| active | bool | tuple[bool, var_list[...]] | tuple[bool, fixed_list[...]] | tuple[bool, fixed_list[...]] |

*For TSB/TSL delta_value, returns nullptr if no nested TSS/TSD exists.

### TSOutput/TSInput Construction

TSOutput and TSInput are constructed directly from TSMeta. The required parallel schemas are obtained via TSMetaSchemaCache:

```cpp
// Construction - TSMeta passed to constructor
TSOutput output{ts_meta, node_ptr};
TSInput input{ts_meta, node_ptr};

// Internally uses TSMetaSchemaCache for parallel structures:
// - value storage: directly from TSMeta (value_type, element_ts, etc.)
// - time tracking: TSMetaSchemaCache::get_time_schema(ts_meta)
// - observer lists: TSMetaSchemaCache::get_observer_schema(ts_meta)
// - delta tracking: TSMetaSchemaCache::get_delta_value_schema(ts_meta)
// - active state (TSInput only): TSMetaSchemaCache::get_active_schema(ts_meta)
```

### ts_ops Vtable

The `ts_ops` structure provides the operations vtable for time-series types. Unlike `type_ops` which may be stored inline, `ts_ops` is retrieved via the `get_ts_ops()` function based on the TSMeta.

```cpp
/**
 * Operations vtable for time-series types.
 *
 * ts_ops enables polymorphic dispatch for TSView operations based on
 * the time-series kind (TS, TSB, TSL, TSD, TSS, TSW, REF, SIGNAL).
 *
 * Each TS kind has its own ts_ops instance with appropriate implementations.
 * The ops pointer is stored in ViewData and used by TSView for dispatch.
 */
struct ts_ops {
    // ========== Schema Access ==========
    const TSMeta* (*ts_meta)(const ViewData& vd);

    // ========== Time-Series Semantics ==========
    engine_time_t (*last_modified_time)(const ViewData& vd);
    bool (*modified)(const ViewData& vd, engine_time_t current_time);
    bool (*valid)(const ViewData& vd);
    bool (*all_valid)(const ViewData& vd);
    bool (*sampled)(const ViewData& vd);

    // ========== Value Access ==========
    value::View (*value)(const ViewData& vd);
    value::View (*delta_value)(const ViewData& vd);
    bool (*has_delta)(const ViewData& vd);

    // ========== Mutation (for outputs) ==========
    void (*set_value)(ViewData& vd, const value::View& src, engine_time_t current_time);
    void (*apply_delta)(ViewData& vd, const value::View& delta, engine_time_t current_time);
    void (*invalidate)(ViewData& vd);

    // ========== Python Interop ==========
    nb::object (*to_python)(const ViewData& vd);
    nb::object (*delta_to_python)(const ViewData& vd);
    void (*from_python)(ViewData& vd, const nb::object& src, engine_time_t current_time);

    // ========== Navigation ==========
    TSView (*child_at)(const ViewData& vd, size_t index, engine_time_t current_time);
    TSView (*child_by_name)(const ViewData& vd, const std::string& name, engine_time_t current_time);
    TSView (*child_by_key)(const ViewData& vd, const value::View& key, engine_time_t current_time);
    size_t (*child_count)(const ViewData& vd);

    // ========== Observer Management ==========
    value::View (*observer)(const ViewData& vd);
    void (*notify_observers)(ViewData& vd, engine_time_t current_time);

    // ========== Link Management ==========
    void (*bind)(ViewData& vd, const ViewData& target);
    void (*unbind)(ViewData& vd);
    bool (*is_bound)(const ViewData& vd);

    // ========== Input Active State Management ==========
    void (*set_active)(ViewData& vd, value::View active_view, bool active, TSInput* input);

    // ========== Kind-Specific Operations ==========
    // Window operations (nullptr for non-TSW)
    const engine_time_t* (*window_value_times)(const ViewData& vd);
    size_t (*window_value_times_count)(const ViewData& vd);
    engine_time_t (*window_first_modified_time)(const ViewData& vd);
    bool (*window_has_removed_value)(const ViewData& vd);
    value::View (*window_removed_value)(const ViewData& vd);
    size_t (*window_removed_value_count)(const ViewData& vd);
    size_t (*window_size)(const ViewData& vd);
    size_t (*window_min_size)(const ViewData& vd);
    size_t (*window_length)(const ViewData& vd);

    // Set operations (nullptr for non-TSS)
    bool (*set_add)(ViewData& vd, const value::View& elem, engine_time_t current_time);
    bool (*set_remove)(ViewData& vd, const value::View& elem, engine_time_t current_time);
    void (*set_clear)(ViewData& vd, engine_time_t current_time);

    // Dict operations (nullptr for non-TSD)
    bool (*dict_remove)(ViewData& vd, const value::View& key, engine_time_t current_time);
    TSView (*dict_create)(ViewData& vd, const value::View& key, engine_time_t current_time);
    TSView (*dict_set)(ViewData& vd, const value::View& key, const value::View& value, engine_time_t current_time);
};

// Get ops by TSKind (for TSW, returns scalar_ops)
const ts_ops* get_ts_ops(TSKind kind);

// Get ops by TSMeta (for TSW, selects based on is_duration_based)
const ts_ops* get_ts_ops(const TSMeta* meta);
```

**Note**: The ops pointer is obtained via `get_ts_ops(meta)` and stored in `ViewData` during view construction. This allows different implementations for TSW variants (tick-based vs duration-based windows).

### Parallel Value Structures

Time-series values internally contain parallel structures for tracking:
1. **value_**: User-visible data (schema from TSMeta fields)
2. **time_**: Modification timestamps (schema from `TSMetaSchemaCache::get_time_schema()`)
3. **observer_**: Observer lists (schema from `TSMetaSchemaCache::get_observer_schema()`)
4. **delta_value_**: Delta tracking (schema from `TSMetaSchemaCache::get_delta_value_schema()`)
5. **link_**: Link targets (schema from `TSMetaSchemaCache::get_link_schema()`)
6. **active_** (TSInput only): Active state (schema from `TSMetaSchemaCache::get_active_schema()`)

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
