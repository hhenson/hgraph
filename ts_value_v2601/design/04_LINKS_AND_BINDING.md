# Links and Binding Design

## Overview

Links provide the mechanism for connecting TSInputs to TSOutputs:
- **Link**: ViewData pointing to source data
- **Binding**: Process of connecting inputs to outputs
- **REF**: Dynamic reference with runtime binding

## Link

### Purpose
A ViewData stored at the leaf of a TSInput's value structure, pointing to TSOutput data.

### Structure

```cpp
struct Link {
    // Link = ViewData (no current_time)
    ShortPath path;     // Source location
    void* data;         // Pointer to TSOutput's data
    ts_ops* ops;        // Operations for the linked data
};
```

### Link Resolution

```
TSInput                         TSOutput
┌─────────────┐                ┌─────────────┐
│ value_      │                │ native_value│
│  └─ [Link]──┼───────────────►│  └─ data    │
│             │                │             │
│ active_     │                │             │
│  └─ bool    │                │             │
└─────────────┘                └─────────────┘
```

## Binding

### Binding Process

1. **Wiring Phase**: Graph construction determines connections
2. **Bind Phase**: Links are established from inputs to outputs
3. **Runtime**: Links are followed for value access

### Bind Operation

```cpp
void TSInput::bind(TSOutput& source) {
    // TODO: Define binding logic

    // 1. Store link to source's data
    // 2. Set up observer registration
    // 3. Initialize active state
}
```

### Un-Peered Binding

For un-peered TSB:
- Each field can bind to different sources
- Links stored at each field's leaf

```
TSInput[TSB[a, b]]          TSOutput1    TSOutput2
     value_                      │            │
       ├─ a: [Link] ─────────────┘            │
       └─ b: [Link] ──────────────────────────┘
```

## AccessStrategy Pattern

### Purpose

Abstraction for flexible binding that handles different access scenarios uniformly. TSInput delegates value access to an AccessStrategy, enabling clean handling of direct binding, collection navigation, and REF wrapping.

### Strategy Types

```cpp
class AccessStrategy {
public:
    virtual ~AccessStrategy() = default;

    // Core access - always queries source freshly (never materializes)
    virtual TSView view(engine_time_t current_time) const = 0;
    virtual bool modified(engine_time_t current_time) const = 0;
    virtual bool valid() const = 0;

    // Delta access
    virtual DeltaView delta(engine_time_t current_time) const = 0;
};
```

### DirectAccess

Most common case - TSInput directly bound to a TSOutput.

```cpp
class DirectAccess : public AccessStrategy {
    TSOutput* source_;

public:
    explicit DirectAccess(TSOutput* source) : source_(source) {}

    TSView view(engine_time_t t) const override {
        return source_->view(t);
    }

    bool modified(engine_time_t t) const override {
        return source_->modified(t);
    }

    bool valid() const override {
        return source_->valid();
    }
};
```

### CollectionAccess

For TSInput bound to an element within a collection (TSL, TSD, TSB field).

```cpp
class CollectionAccess : public AccessStrategy {
    AccessStrategy* parent_;      // Strategy for parent collection
    size_t index_;                // For TSL element or TSB field
    // OR
    Key key_;                     // For TSD element

public:
    TSView view(engine_time_t t) const override {
        TSView parent_view = parent_->view(t);
        return parent_view.at(index_);  // or .get(key_)
    }
};
```

### RefObserverAccess

For non-REF TSInput bound to a REF TSOutput. Follows the reference to get actual value.

```cpp
class RefObserverAccess : public AccessStrategy {
    TSOutput* ref_output_;        // The REF[T] output

public:
    TSView view(engine_time_t t) const override {
        // Follow the reference to get target TSView
        TSView ref_view = ref_output_->view(t);
        return ref_view.dereference();  // Follow REF to target
    }

    bool modified(engine_time_t t) const override {
        // Modified if reference changed OR target ticked
        // (unless sampled mode)
        return ref_output_->modified(t) ||
               (!ref_output_->is_sampled() && target_modified(t));
    }
};
```

### RefWrapperAccess

For REF TSInput bound to a non-REF TSOutput. Wraps the output to provide REF semantics.

```cpp
class RefWrapperAccess : public AccessStrategy {
    TSOutput* source_;

public:
    TSView view(engine_time_t t) const override {
        // Return a REF-wrapped view of the source
        return TSView::make_ref_wrapper(source_->view(t));
    }
};
```

### Strategy Selection

During binding, the appropriate strategy is selected based on input/output types:

```cpp
AccessStrategy* select_strategy(TSInput* input, TSOutput* output) {
    bool input_is_ref = input->meta()->ts_kind() == TSKind::REF;
    bool output_is_ref = output->meta()->ts_kind() == TSKind::REF;

    if (!input_is_ref && !output_is_ref) {
        return new DirectAccess(output);
    } else if (!input_is_ref && output_is_ref) {
        return new RefObserverAccess(output);
    } else if (input_is_ref && !output_is_ref) {
        return new RefWrapperAccess(output);
    } else {
        return new DirectAccess(output);  // REF to REF
    }
}
```

### Benefits

1. **Uniform interface**: TSInput always calls `strategy_->view(t)` regardless of binding type
2. **Never materializes**: Views are computed fresh on each access (critical for REF correctness)
3. **Composable**: CollectionAccess can wrap any other strategy for nested access
4. **Extensible**: New binding patterns can be added without modifying TSInput

## Memory Stability

### Requirement
Linked data must have stable addresses throughout graph execution.

### Implications

1. **TSD elements**: Must use stable storage (no vector reallocation)
2. **TSL elements**: Similar stability requirements
3. **Value moves**: Links must be updated or invalidated

### Stable Storage Patterns

The preferred pattern uses **index-based indirection** with dense element storage:

```cpp
// SetStorage - index-based hash set with contiguous element storage
struct SetStorage {
    using IndexSet = ankerl::unordered_dense::set<size_t, SetIndexHash, SetIndexEqual>;

    std::vector<std::byte> elements;     // Contiguous element storage
    size_t element_count{0};             // Number of valid elements
    std::unique_ptr<IndexSet> index_set; // Index-based hash set
    const TypeMeta* element_type{nullptr};

    // Helper to get element pointer by index
    [[nodiscard]] const void* get_element_ptr(size_t idx) const {
        return elements.data() + idx * element_type->size;
    }
};

// MapStorage - reuses SetStorage for keys with parallel value storage
struct MapStorage {
    SetStorage keys;                   // Key storage with index_set
    std::vector<std::byte> values;     // Parallel value storage
    const TypeMeta* value_type{nullptr};

    [[nodiscard]] const void* get_key_ptr(size_t idx) const {
        return keys.get_element_ptr(idx);
    }

    [[nodiscard]] const void* get_value_ptr(size_t idx) const {
        return values.data() + idx * value_type->size;
    }
};
```

**Key Features**:
- Elements stored contiguously for cache efficiency
- Hash set stores indices (not pointers or elements)
- Index indirection enables stable lookups
- Keys and values share the same index space for O(1) parallel access

### Transparent Hash/Equality Functors

Enable heterogeneous lookup without creating temporary objects:

```cpp
struct SetIndexHash {
    using is_transparent = void;        // Enables heterogeneous lookup
    using is_avalanching = void;        // Quality hint for hash performance

    const SetStorage* storage{nullptr};

    // Index-to-hash: dereference index to get element, then hash element
    [[nodiscard]] uint64_t operator()(size_t idx) const;

    // Pointer-to-hash: hash element directly (for find/contains)
    [[nodiscard]] uint64_t operator()(const void* ptr) const;
};

struct SetIndexEqual {
    using is_transparent = void;

    const SetStorage* storage{nullptr};

    [[nodiscard]] bool operator()(size_t a, size_t b) const;           // Index vs Index
    [[nodiscard]] bool operator()(size_t idx, const void* ptr) const;  // Index vs Pointer
    [[nodiscard]] bool operator()(const void* ptr, size_t idx) const;  // Pointer vs Index
};
```

**Pattern**: `is_transparent` allows `ankerl::unordered_dense::set` to lookup with pointers directly: `set->find(element_ptr)` without needing the index first.

### Free List with Death Time Markers

Elements are never moved after insertion. Deleted slots are marked with a death time and tracked in a free list for reuse:

```cpp
struct SetStorage {
    using IndexSet = ankerl::unordered_dense::set<size_t, SetIndexHash, SetIndexEqual>;

    // Keys stored as (key, death_time) tuples
    // death_time == MIN_DT → alive
    // death_time != MIN_DT → dead at that time
    std::vector<std::byte> keys;
    std::vector<size_t> free_list;        // Indices of dead slots for reuse
    std::unique_ptr<IndexSet> index_set;  // For live key lookup
    const TypeMeta* key_type{nullptr};

    [[nodiscard]] size_t size() const {
        return key_count() - free_list.size();
    }

    [[nodiscard]] size_t key_count() const {
        return keys.size() / entry_size();
    }

    [[nodiscard]] size_t entry_size() const {
        return key_type->size + sizeof(engine_time_t);  // key + death_time
    }

    [[nodiscard]] const void* get_key_ptr(size_t idx) const {
        return keys.data() + idx * entry_size();
    }

    [[nodiscard]] engine_time_t get_death_time(size_t idx) const {
        auto* entry = keys.data() + idx * entry_size();
        return *reinterpret_cast<const engine_time_t*>(entry + key_type->size);
    }

    void set_death_time(size_t idx, engine_time_t time) {
        auto* entry = keys.data() + idx * entry_size();
        *reinterpret_cast<engine_time_t*>(entry + key_type->size) = time;
    }

    [[nodiscard]] bool is_alive(size_t idx) const {
        return get_death_time(idx) == MIN_DT;
    }
};

struct MapStorage : SetStorage {
    std::vector<std::byte> values;  // Parallel to keys
    const TypeMeta* value_type{nullptr};

    [[nodiscard]] const void* get_value_ptr(size_t idx) const {
        return values.data() + idx * value_type->size;
    }
};
```

#### Insertion with Free List Reuse

```cpp
size_t insert_element(SetStorage* storage, const void* key, engine_time_t current_time) {
    size_t idx;

    if (!storage->free_list.empty()) {
        // Reuse a dead slot
        idx = storage->free_list.back();
        storage->free_list.pop_back();

        void* slot = storage->get_key_ptr(idx);
        key_type->ops.copy_construct(slot, key, key_type);
        storage->set_death_time(idx, MIN_DT);  // Mark alive
    } else {
        // Append to end
        idx = storage->key_count();
        storage->keys.resize(storage->keys.size() + storage->entry_size());

        void* slot = storage->get_key_ptr(idx);
        key_type->ops.copy_construct(slot, key, key_type);
        storage->set_death_time(idx, MIN_DT);  // Mark alive
    }

    storage->index_set->insert(idx);
    return idx;
}
```

#### Erasure with Death Time

```cpp
bool erase_element(SetStorage* storage, const void* key, engine_time_t current_time) {
    auto it = storage->index_set->find(key);
    if (it == storage->index_set->end()) return false;

    size_t idx = *it;

    // Remove from index_set (no longer findable)
    storage->index_set->erase(it);

    // Mark death time (key data preserved for delta queries)
    storage->set_death_time(idx, current_time);

    // Add to free list
    storage->free_list.push_back(idx);

    return true;
}
```

#### Safe Iteration (Skip Dead Entries)

```cpp
// Iterator skips dead entries
class SetIterator {
    const SetStorage* storage_;
    size_t current_;

    void skip_dead() {
        size_t count = storage_->key_count();
        while (current_ < count && !storage_->is_alive(current_)) {
            ++current_;
        }
    }

public:
    SetIterator& operator++() {
        ++current_;
        skip_dead();
        return *this;
    }
};
```

#### Delta Computation

Erased items are added to the end of the free list, enabling efficient delta queries:

```cpp
// Find keys removed this tick by walking free_list in reverse
std::vector<size_t> get_removed_this_tick(const SetStorage* storage, engine_time_t current_time) {
    std::vector<size_t> removed;
    // Walk free_list from end until we find entries not removed this tick
    for (auto it = storage->free_list.rbegin(); it != storage->free_list.rend(); ++it) {
        if (storage->get_death_time(*it) == current_time) {
            removed.push_back(*it);
        } else {
            break;  // Earlier removals - stop
        }
    }
    return removed;
}
```

**Benefits**:
- **Reference stability**: Element addresses never change after insertion
- **Safe iteration**: Can iterate while other code holds references
- **Slot reuse**: Free list prevents unbounded memory growth
- **Delta support**: Death time enables efficient removed-key queries
- **Key preservation**: Dead keys preserved until slot reused (for delta access)

## REF (Dynamic Reference)

### Purpose
Runtime-determined reference to another time-series.

### Structure

```cpp
struct REF {
    // TODO: Define REF structure

    // Reference target (set at runtime)
    // ShortPath target_path;

    // Sampled flag
    // bool sampled;
};
```

### REF Modes

| Mode | Sampled Flag | Behavior |
|------|--------------|----------|
| Normal | false | Ticks when reference or target ticks |
| Sampled | true | Only ticks when reference changes |

### REF Resolution

```cpp
TSView REF::resolve(engine_time_t current_time) const {
    // TODO: Define resolution logic

    // 1. Follow reference path to target
    // 2. Return TSView of target
    // 3. Handle sampled flag for modification
}
```

## Cast Logic

### Cast Storage

Casts are stored in TSOutput::alternatives_:

```cpp
class TSOutput {
    TSValue native_value_;
    std::unordered_map<TypeMeta*, TSValue> alternatives_;
};
```

### Cast Creation

```cpp
TSView TSOutput::cast_to(TypeMeta* target_type) {
    // Check if cast exists
    auto it = alternatives_.find(target_type);
    if (it != alternatives_.end()) {
        return it->second.ts_view(current_time_);
    }

    // Create new cast
    TSValue cast_value = create_cast(native_value_, target_type);
    alternatives_[target_type] = std::move(cast_value);
    return alternatives_[target_type].ts_view(current_time_);
}
```

### Cast Invalidation

- Casts are invalidated when source is modified
- Lazy recomputation on next access

## Open Questions

- TODO: How to handle circular references?
- TODO: Link validation and error handling?
- TODO: Performance of link resolution?

## References

- User Guide: `04_LINKS_AND_BINDING.md`
- Research: `08_REF.md`
