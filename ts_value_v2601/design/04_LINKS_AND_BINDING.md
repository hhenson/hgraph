# Links and Binding Design

## Overview

Links provide the mechanism for connecting TSInputs to TSOutputs:
- **Link**: ViewData stored at a position, pointing to source data
- **Binding**: Process of creating links via TSView operations
- **REF**: Dynamic reference with runtime binding

## Link

### Purpose

A Link is an **internal storage mechanism** (not an exposed type). Conceptually like a filesystem symlink, it creates a branch from one position in a TSValue to a position in another TSValue.

A Link is **ViewData stored at a position** in the value structure:

```cpp
struct ViewData {
    ShortPath path;     // Graph-aware path to source
    void* data;         // Pointer to source data
    ts_ops* ops;        // Operations vtable for source
};
```

### Link Storage and Identification

Links exist only within collections. The storage strategy is optimized per collection type:

**TSL/TSD: Collection-Level Flag**

For TSL and TSD, if one element is a link, all elements are links (uniform). A single flag at the collection level:

```cpp
struct CollectionStorage {
    bool is_linked = false;  // If true, elements store ViewData (links)

    // When is_linked == false: elements contain local TS data
    // When is_linked == true:  elements contain ViewData array
    std::vector<std::byte> elements;
};
```

**TSB: Per-Field Bitset**

TSB fields can independently be local or linked:

```cpp
#include <sul/dynamic_bitset.hpp>

struct BundleStorage {
    sul::dynamic_bitset<> link_flags;  // Empty if no links; bit[i] indicates field i is a link
    // Each field contains local data or ViewData based on its flag
};
```

The bitset is empty (zero overhead) when no fields are linked. Uses `sul::dynamic_bitset` (already integrated via FetchContent).

### Link Resolution

```
TSInput.value_ (TSB)            TSOutput1.native_value_
┌─────────────────────┐         ┌─────────────────┐
│ link_flags: [1, 1]  │         │ data: 42        │
│ field_a: ViewData ──┼────────►└─────────────────┘
│ field_b: ViewData ──┼─────┐
└─────────────────────┘     │   TSOutput2.native_value_
                            │   ┌─────────────────┐
                            └──►│ data: 3.14      │
                                └─────────────────┘
```

### Navigation with Links

```cpp
// TSL/TSD navigation
TSView TSLView::element(size_t index) {
    if (storage_.is_linked) {
        ViewData& vd = get_view_data_at(index);
        return TSView{vd, current_time_};
    } else {
        return TSView{make_local_view_data(index), current_time_};
    }
}

// TSB navigation
TSView TSBView::field(size_t index) {
    if (!link_flags_.empty() && link_flags_[index]) {
        ViewData& vd = get_view_data_at(index);
        return TSView{vd, current_time_};
    } else {
        return TSView{make_local_view_data(index), current_time_};
    }
}
```

The caller sees a TSView either way - Links are transparent.

## Binding

### TSView Bind Operations

Links are created via `bind()` / `unbind()` on a mutable TSView:

```cpp
class TSView {
public:
    // Create a Link at this position pointing to source
    void bind(const TSView& source);

    // Remove Link at this position (revert to local data)
    void unbind();

    // Check if this position is a Link
    bool is_bound() const;
};
```

Implementation depends on the parent collection type:

```cpp
// For TSL/TSD element binding (sets collection-level flag)
void TSLView::bind_all(const TSLView& source) {
    storage_.is_linked = true;
    for (size_t i = 0; i < size(); ++i) {
        store_view_data_at(i, source.element(i).view_data());
    }
}

// For TSB field binding (sets per-field flag)
void TSBView::bind_field(size_t index, const TSView& source) {
    ensure_link_flags_allocated();
    link_flags_.set(index, true);
    store_view_data_at(index, source.view_data());
}
```

### Binding Process

1. **Wiring Phase**: Graph construction determines connections
2. **Bind Phase**: TSView::bind() creates Links, sets appropriate flags
3. **Runtime**: Navigation transparently follows Links

### TSInputView Binding

TSInputView wraps TSView binding and adds subscription management:

```cpp
void TSInputView::bind(TSOutputView& output) {
    // 1. Create Link at TSValue level
    ts_view_.bind(output.ts_view());

    // 2. Subscribe for notifications if active
    if (active()) {
        output.subscribe(owning_input_);
    }
}
```

### Un-Peered Binding (TSB)

For un-peered TSB, each field can bind to different sources:

```cpp
// Each field binds independently
input_view.field("a").bind(output1_view);
input_view.field("b").bind(output2_view);
```

```
TSInput.value_[TSB]
  link_flags: [1, 1]
  ├─ a: ViewData ───────► TSOutput1
  └─ b: ViewData ───────► TSOutput2
```

### Peered Binding (TSL/TSD)

For TSL/TSD, binding is all-or-nothing:

```cpp
// Entire list binds to source list
input_list_view.bind(output_list_view);
// Sets is_linked = true, all elements become ViewData
```

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
Runtime-determined reference to another time-series. REF enables dynamic routing where the target isn't known until runtime.

### TSReference Value Type

`TSReference` is the value stored in a `REF[TS[T]]` time-series. It represents a pointer to another time-series location.

```cpp
struct TSReference {
    enum class Kind { EMPTY, PEERED, NON_PEERED };

    Kind kind;

    // For PEERED: direct reference to a single output
    ShortPath target_path;

    // For NON_PEERED: collection of child references (for TSL/TSD/TSB)
    std::vector<TSReference> items;

    bool is_empty() const { return kind == Kind::EMPTY; }
    bool is_peered() const { return kind == Kind::PEERED; }
    bool is_non_peered() const { return kind == Kind::NON_PEERED; }

    // Resolve to ViewData for binding
    ViewData resolve() const;
};
```

### FQReference (Serialization)

For Python interop and serialization, `FQReference` uses stable node IDs instead of pointers:

```cpp
struct FQReference {
    enum class Kind { EMPTY, PEERED, NON_PEERED };

    Kind kind;
    uint64_t node_id;              // Stable node identifier
    PortType port_type;
    std::vector<size_t> indices;   // Path through compound types
    std::vector<FQReference> items; // For NON_PEERED

    // Convert to/from TSReference given graph context
    TSReference to_ts_reference(Graph& graph) const;
    static FQReference from_ts_reference(const TSReference& ref);
};
```

### REFLink

`REFLink` is used when an alternative needs to dereference a REF (REF → TS conversion). It manages two subscriptions:
1. To the REF source (for rebind notifications)
2. To the current dereferenced target (for value notifications)

```cpp
struct REFLink {
    Link target;             // Current dereferenced target
    Link ref_source;         // Link to the REF source

    // Called when ref_source's TSReference value changes
    void on_ref_changed(engine_time_t current_time) {
        // 1. Unbind from old target (unsubscribes)
        target.unbind();

        // 2. Get new TSReference from ref_source
        TSView ref_view{ref_source.view_data, current_time};
        TSReference new_ref = ref_view.value().as<TSReference>();

        // 3. Resolve and bind to new target
        if (!new_ref.is_empty()) {
            target.bind(new_ref.resolve());
        }
    }
};
```

### REF Binding Modes

REF participates in three binding modes:

| Mode | Native | Alternative | Mechanism |
|------|--------|-------------|-----------|
| TS → REF | TS[T] | REF[TS[T]] | Create TSReference pointing to native |
| REF → REF | REF[TS[T]] | REF[TS[T]] | Normal LINK (TSReference is the value) |
| REF → TS | REF[TS[T]] | TS[T] | REFLink that follows the reference |

### REF → TS (Dereferencing)

When an input needs `TS[T]` but output provides `REF[TS[T]]`, the alternative uses REFLink:

```
Native: TSB[a: REF[TS[int]], b: TS[float]]

Alternative: TSB[a: TS[int], b: TS[float]]
┌────────────────────────────────────────────────────┐
│ a: REFLink                                         │
│    ├── ref_source: Link → Native.a (the REF)       │
│    └── target: Link → dereferenced target          │
│ b: Link → Native.b                                 │
└────────────────────────────────────────────────────┘
```

The REFLink subscribes to the REF source. When the TSReference changes, it unbinds from the old target and binds to the new one.

### TS → REF (Wrapping)

When an input needs `REF[TS[T]]` but output provides `TS[T]`, the alternative stores TSReference values:

```
Native: TSB[a: TS[int], b: TS[float]]

Alternative: TSB[a: REF[TS[int]], b: TS[float]]
┌────────────────────────────────────────────────────┐
│ a: TSReference(path=Native.a)  ← constructed value │
│ b: Link → Native.b                                 │
└────────────────────────────────────────────────────┘
```

The TSReference is constructed from the native's ShortPath at that position. Per Mode 1 behavior, the REF position does NOT tick when the underlying TS changes - only when the reference itself would change (which in this case is only at creation/rebind time).

For collections (TSD/TSL), the alternative mirrors the native's structure:

```
Native: TSD[str, TS[int]]              Alternative: TSD[str, REF[TS[int]]]
┌──────────────────────────┐           ┌───────────────────────────────────┐
│ "a" → TS[int] (value=1)  │           │ "a" → TSReference(path=Native.a)  │
│ "b" → TS[int] (value=2)  │           │ "b" → TSReference(path=Native.b)  │
└──────────────────────────┘           └───────────────────────────────────┘
```

The alternative subscribes to native's structural changes:
- **Key/element added**: Create new TSReference pointing to new native element
- **Key/element removed**: Remove corresponding TSReference from alternative

### Sampled Flag

When a REF → TS link is traversed and the REF was modified (reference changed), the resulting view is marked as **sampled**:

```cpp
bool REFLink::modified(engine_time_t current_time) const {
    // Modified if REF source changed OR target changed
    TSView ref_view{ref_source.view_data, current_time};
    if (ref_view.modified()) {
        return true;  // Reference changed - always report modified (sampled)
    }
    TSView target_view{target.view_data, current_time};
    return target_view.modified();
}
```

This ensures consumers are notified when their data source changes, even if the new target wasn't modified at that tick.

## Cast Logic

### Cast Storage

Alternatives (casts) are stored in TSOutput and indexed by target schema:

```cpp
class TSOutput {
    TSValue native_value_;
    std::map<const TSMeta*, TSValue> alternatives_;

    TSValue& get_or_create_alternative(const TSMeta& schema);
};
```

### Alternative Structure

Alternatives are TSValues that contain a mixture of:
- **Link**: Direct link to native position (no schema conversion needed)
- **REFLink**: For REF → TS positions (dereferencing)
- **TSReference values**: For TS → REF positions (wrapping)

### Alternative Creation

When creating an alternative, walk the native and target schemas in parallel:

```cpp
TSValue& TSOutput::get_or_create_alternative(const TSMeta& target_schema) {
    auto it = alternatives_.find(&target_schema);
    if (it != alternatives_.end()) {
        return it->second;
    }

    // Create new alternative with appropriate link structure
    TSValue alt(target_schema);
    establish_alternative_links(alt, native_value_, target_schema);

    return alternatives_.emplace(&target_schema, std::move(alt)).first->second;
}

void establish_alternative_links(TSValue& alt, TSValue& native, const TSMeta& target_schema) {
    // Walk schemas in parallel, at each position:
    // - Native REF, Target TS: Create REFLink
    // - Native TS, Target REF: Store TSReference value
    // - Same type: Create direct Link
    // - Collection: Recurse into elements, subscribe to structural changes
}
```

### Position-by-Position Logic

| Native Type | Target Type | Alternative Contains | Subscription |
|-------------|-------------|---------------------|--------------|
| TS[T] | TS[T] | Link → native | Via Link |
| REF[TS[T]] | REF[TS[T]] | Link → native | Via Link |
| REF[TS[T]] | TS[T] | REFLink | REF source + current target |
| REF[X] | Y (X≠Y) | REFLink + nested conversion | REF source + recursive |
| TS[T] | REF[TS[T]] | TSReference value | None (value is static) |
| TSD/TSL | TSD/TSL | Per-element links | Native structure changes |
| TSB | TSB | Per-field links | Per-field as above |

**Nested Conversion Example**: `REF[TSD[str, TS[int]]] → TSD[str, REF[TS[int]]]`

1. REFLink dereferences outer REF to get link to the actual `TSD[str, TS[int]]` (on a different output)
2. Element-level conversion (`TS[int] → REF[TS[int]]`) happens on that **linked TSD output**, creating TSReferences that point to its elements
3. When REF changes target, REFLink rebinds to new TSD and regenerates element TSReferences for new target's elements

Note: The alternative does NOT copy the TSD - it links to the dereferenced TSD and wraps its elements.

### No Explicit Sync

Alternatives do not require explicit synchronization:
- **Link positions**: Directly access native data
- **REFLink positions**: Subscribe to REF source and current target
- **TSReference positions**: Value is constructed once from native path
- **Structural changes**: Alternative subscribes to native for key/element changes

## Open Questions

- TODO: How to handle circular references?
- TODO: Link validation and error handling?
- TODO: Performance of link resolution?

## References

- User Guide: `04_LINKS_AND_BINDING.md`
- Research: `08_REF.md`
