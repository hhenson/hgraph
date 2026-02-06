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

### Link Storage

Links are stored using **LinkTarget** structures for TSInput binding. The storage is part of TSValue's five-value structure in the `link_` storage.

**Important**: LinkTarget is for TSInput simple binding only. REFLink (which contains a LinkTarget plus REF tracking) is used exclusively in TSOutput alternatives for REF→TS dereferencing. TSInput should NEVER dereference a REF - that logic belongs in TSOutput alternatives.

**Link Schema Generation (TSInput)**

| Type | Link Schema |
|------|-------------|
| `TS[T]` | `nullptr` (scalars don't support binding) |
| `TSB[...]` | `fixed_list[LinkTarget, field_count]` (per-field LinkTarget) |
| `TSL[T]` | `LinkTarget` (collection-level) |
| `TSD[K,V]` | `LinkTarget` (collection-level) |

**TSL/TSD: Collection-Level LinkTarget**

For TSL and TSD, a single LinkTarget at the collection level handles the entire binding:

```cpp
// link_data points to a single LinkTarget
LinkTarget* link = static_cast<LinkTarget*>(view_data.link_data);
if (link && link->is_linked) {
    // Entire collection is linked to source
}
```

**TSB: Per-Field LinkTarget Array**

TSB fields each have an independent LinkTarget stored in a fixed-size array:

```cpp
// link_data points to fixed_list[LinkTarget, field_count]
LinkTarget* links = static_cast<LinkTarget*>(view_data.link_data);
LinkTarget& field_link = links[field_index];
if (field_link.is_linked) {
    // This field is linked to source
}
```

This design provides:
- **Clear separation**: LinkTarget for simple TSInput binding, REFLink only for TSOutput REF→TS alternatives
- **Stable addresses**: Inline storage ensures LinkTarget addresses don't change
- **Automatic lifecycle**: LinkTarget cleanup happens when parent element is destroyed

### Link Resolution

```
TSInput.value_ (TSB)               TSOutput1.native_value_
┌────────────────────────┐         ┌─────────────────┐
│ link_[0]: LinkTarget ──┼────────►│ data: 42        │
│ link_[1]: LinkTarget ──┼─────┐   └─────────────────┘
└────────────────────────┘     │   TSOutput2.native_value_
                               │   ┌─────────────────┐
                               └──►│ data: 3.14      │
                                   └─────────────────┘
```

### Navigation with Links

```cpp
// TSL/TSD navigation - check collection-level LinkTarget
TSView TSLView::element(size_t index) {
    LinkTarget* link = get_collection_link();
    if (link && link->is_linked) {
        // Navigate through the linked target
        TSView target = make_view_from_link(*link, current_time_);
        return target[index];
    }
    return TSView{make_local_view_data(index), current_time_};
}

// TSB navigation - check per-field LinkTarget
TSView TSBView::field(size_t index) {
    LinkTarget* field_link = get_field_link(index);
    if (field_link && field_link->is_linked) {
        // Return target view for this field
        return make_view_from_link(*field_link, current_time_);
    }
    return TSView{make_local_view_data(index), current_time_};
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

Implementation uses LinkTarget at the appropriate position:

```cpp
// For TSL/TSD binding (collection-level LinkTarget)
void TSView::bind(const TSView& source) {
    LinkTarget* link = get_link_target();
    if (link) {
        // Store source's ViewData in the LinkTarget
        store_view_data_in_link(*link, source.view_data());
    }
}

// For TSB field binding (per-field LinkTarget)
void TSBView::bind_field(size_t index, const TSView& source) {
    LinkTarget* field_link = get_field_link(index);
    if (field_link) {
        store_view_data_in_link(*field_link, source.view_data());
    }
}
```

### Binding Process

1. **Wiring Phase**: Graph construction determines connections
2. **Bind Phase**: TSView::bind() configures REFLink to point to source
3. **Runtime**: Navigation transparently follows REFLink targets

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
  link_[0]: LinkTarget ───────► TSOutput1
  link_[1]: LinkTarget ───────► TSOutput2
```

### Peered Binding (TSL/TSD)

For TSL/TSD, binding is all-or-nothing:

```cpp
// Entire list binds to source list
input_list_view.bind(output_list_view);
// Collection-level REFLink binds to source, navigation follows link
```

## Notification Model

### Overview

When a linked output changes, the notification system must accomplish two things:
1. **Time-accounting**: Stamp modification times up through the input's schema hierarchy so that composite `modified()` and `valid()` checks work without scanning children.
2. **Node-scheduling**: Schedule the owning node for evaluation.

These are two **independent** notification chains, each with its own subscription and lifecycle.

### Two Notification Chains

```
Target output changes → ObserverList::notify_modified() calls:
├── LinkTarget::notify(et) → stamp owner_time → parent_link->notify(et) → ... → root stamp
└── ActiveNotifier::notify(et) → TSInput::notify(et) → node scheduled
```

**Time-accounting chain** (link-owned):
- Stamps `last_modified_time` at each level of the input's schema hierarchy
- Always active — subscribed at **bind time**, regardless of active/passive state
- Implemented by `LinkTarget` inheriting from `Notifiable`
- Each LinkTarget stores `owner_time_ptr` (pointer to its level's time slot in the input's TSValue) and `parent_link` (pointer to parent level's LinkTarget)
- Propagation: `notify(et)` stamps `*owner_time_ptr = et`, then calls `parent_link->notify(et)` up to the root
- Deduplication guard: `last_notify_time == et` prevents re-stamping on the same tick

**Node-scheduling chain** (TSInput subscription):
- Schedules the owning node for evaluation
- Only active when the input is active — subscribed at **set_active time**
- Implemented by `ActiveNotifier` (embedded in each LinkTarget)
- `ActiveNotifier::notify(et)` calls `TSInput::notify(et)` which schedules the node
- Guard: `active_notifier.owning_input == nullptr` prevents double-subscription when `set_active` is called at multiple composite levels

### LinkTarget as Notifiable

LinkTarget inherits from `Notifiable` and carries structural fields for the time-accounting chain:

```cpp
struct LinkTarget : public Notifiable {
    // --- Target-data fields (copied by store_to_link_target) ---
    bool is_linked{false};
    void* value_data{nullptr};
    void* time_data{nullptr};
    void* observer_data{nullptr};
    void* delta_data{nullptr};
    void* link_data{nullptr};
    const ts_ops* ops{nullptr};
    const TSMeta* meta{nullptr};

    // --- Structural fields (NOT copied by store_to_link_target) ---
    engine_time_t* owner_time_ptr{nullptr};   // This level's time slot in INPUT's TSValue
    LinkTarget* parent_link{nullptr};          // Parent level's LinkTarget (nullptr at root)
    engine_time_t last_notify_time{MIN_DT};   // Dedup guard

    // --- Embedded node-scheduling wrapper ---
    struct ActiveNotifier : public Notifiable {
        TSInput* owning_input{nullptr};
        void notify(engine_time_t et) override;
    };
    ActiveNotifier active_notifier;

    // Notifiable interface — time-accounting only
    void notify(engine_time_t et) override;
};
```

**Critical**: `copy_assign` / `move_assign` on LinkTarget must only copy the target-data fields. The structural fields (`owner_time_ptr`, `parent_link`, `last_notify_time`, `active_notifier`) belong to the input's schema position and must not be overwritten when rebinding to a new target.

### Subscription Points

| Operation | Time-accounting (LinkTarget) | Node-scheduling (ActiveNotifier) |
|-----------|-------|-------|
| `bind()` | Subscribe LinkTarget to target's ObserverList | — |
| `set_active(true)` | — | Subscribe ActiveNotifier to target's ObserverList |
| `set_active(false)` | — | Unsubscribe ActiveNotifier |
| `unbind()` | Unsubscribe LinkTarget | Unsubscribe ActiveNotifier (if active) |

Each bound+active position has **two** entries in the target's ObserverList:
- `LinkTarget*` (time-accounting, always present when bound)
- `ActiveNotifier*` (node-scheduling, present only when active)

### Composite Structure (TSB Example)

For a TSB with fields `a: TS[int]`, `b: TS[float]`:

```
Input TSValue link storage: [container_LT, field_a_LT, field_b_LT]

field_a_LT:
  owner_time_ptr → &time_tuple[1]  (field a's time slot)
  parent_link → container_LT
  Subscribed to Output.a's ObserverList (time-accounting)
  active_notifier subscribed to Output.a's ObserverList (if active)

field_b_LT:
  owner_time_ptr → &time_tuple[2]  (field b's time slot)
  parent_link → container_LT
  Subscribed to Output.b's ObserverList (time-accounting)
  active_notifier subscribed to Output.b's ObserverList (if active)

container_LT:
  owner_time_ptr → &time_tuple[0]  (container's time slot)
  parent_link → nullptr (root)
  NOT subscribed to any observer (receives from children only)
```

When Output.a changes at time T:
1. `field_a_LT.notify(T)` → stamps `time_tuple[1] = T`
2. `field_a_LT.parent_link->notify(T)` → `container_LT.notify(T)` → stamps `time_tuple[0] = T`
3. `field_a_LT.active_notifier.notify(T)` → `TSInput::notify(T)` → node scheduled

### REFLink Integration

REFLink follows the same dual-chain pattern with its own `owner_time_ptr`, `parent_link`, `last_notify_time`, and embedded `ActiveNotifier`. When the REF source changes and REFLink rebinds to a new target, both subscriptions (time-accounting and node-scheduling) are transferred to the new target's ObserverList.

### Benefits Over Lazy Scanning

Without proactive time-stamping, composite `modified()` and `valid()` must lazily scan all linked children — O(N) per query. With the dual-chain model:
- `modified()` reduces to `last_modified_time >= current_time` — O(1)
- `valid()` reduces to `last_modified_time != MIN_DT` — O(1)
- Notification cost is O(depth) per change, amortized across all queries

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

### REFLink (TSOutput Alternatives Only)

`REFLink` is used **exclusively in TSOutput alternatives** when a REF needs to be dereferenced (REF → TS conversion). It manages two subscriptions:
1. To the REF source (for rebind notifications)
2. To the current dereferenced target (for value notifications)

**Important**: REFLink is NEVER used in TSInput. TSInput uses LinkTarget for simple binding. All REF dereferencing logic belongs in TSOutput alternatives.

```cpp
class REFLink : public Notifiable {
    LinkTarget target_;              // Current dereferenced target
    ViewData ref_source_view_data_;  // ViewData pointing to the REF source
    bool ref_source_bound_{false};   // Whether bound to a REF source

    // Called when ref_source's TSReference value changes (via notify())
    void rebind_target(engine_time_t current_time) {
        // 1. Unbind from old target (unsubscribes)
        if (target_.is_linked && target_.observer_data) {
            // Unsubscribe from old target
        }
        target_.clear();

        // 2. Get new TSReference from ref_source
        TSView ref_view{ref_source_view_data_, current_time};
        TSReference new_ref = ref_view.value().as<TSReference>();

        // 3. Resolve and bind to new target
        if (!new_ref.is_empty() && new_ref.is_peered()) {
            TSView resolved = new_ref.resolve(current_time);
            // Store resolved ViewData in target_
            // Subscribe to new target
        }
    }
};
```

### REFLink Inline Storage (TSOutput Alternatives)

In TSOutput alternatives that require REF→TS dereferencing, REFLink is stored **inline** as part of the alternative's link schema at each position that needs dereferencing. This is the same pattern used for LinkTarget in TSInput.

**Context**: This applies to TSOutput alternatives only, not TSInput. TSInput uses LinkTarget directly.

**Why inline storage works for alternatives:**

1. **Memory stability**: Inline data in TSValue storage has stable addresses by definition. Elements are never moved after insertion - only marked dead and later erased.

2. **Automatic lifecycle**: When an alternative element is removed (e.g., TSD key deleted), the inline REFLink is destroyed along with its containing storage, automatically cleaning up subscriptions.

3. **No external tracking needed**: Unlike a separate `std::vector<REFLink>`, inline storage doesn't require searching to find which REFLink corresponds to which position.

**Incorrect approach** (do not use):
```cpp
// WRONG: Separate vector makes removal complex
class TSOutput {
    std::vector<std::unique_ptr<REFLink>> ref_links_;  // Hard to track ownership
};
```

**Correct approach** (inline storage in alternatives):
```cpp
// RIGHT: REFLink stored inline in alternative's link schema
// Only for TSOutput alternatives where REF→TS dereferencing is needed
// TSInput uses LinkTarget, not REFLink
```

### REFLink Removal Lifecycle (TSOutput Alternatives)

For REFLink in TSOutput alternatives (where REF→TS dereferencing is used), element removal follows a **two-phase lifecycle**:

1. **Mark as dead** → Unsubscribe from notifications
   - Called when the element is logically removed
   - Stops all notification callbacks immediately
   - The element is no longer "live" but storage persists

2. **Later destroy** → Actually free the storage
   - Called when the slot is reused or container is destroyed
   - REFLink destructor runs, cleaning up any remaining resources
   - Safe because subscriptions were already removed in phase 1

```cpp
// Phase 1: Mark dead and unsubscribe (TSOutput alternative element)
void mark_element_dead(size_t index, engine_time_t death_time) {
    // Get the REFLink at this position in the alternative
    REFLink* ref_link = get_ref_link_at(index);
    if (ref_link) {
        ref_link->unbind();  // Stop receiving notifications, clear target
    }
    set_death_time(index, death_time);
    // Storage persists - can still read "last value" this cycle
}

// Phase 2: Actual destruction (later, when slot reused)
void destroy_element(size_t index) {
    // REFLink destructor runs - safe because already unbound
    destruct_at(index);
}
```

This separation ensures:
- No notification callbacks arrive for "dead" elements
- Other parts of the graph can still read the "last value" during the current engine cycle
- No dangling pointer issues in notification handlers

**Note**: For TSInput, which uses LinkTarget, there's no notification subscription to manage, so the lifecycle is simpler - LinkTarget is just cleared/destructed directly.
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

### Sampled Flag (TSOutput Alternatives)

When a REF → TS link is traversed in a TSOutput alternative and the REF was modified (reference changed), the resulting view is marked as **sampled**:

```cpp
bool REFLink::modified(engine_time_t current_time) const {
    // Modified if REF source changed OR target changed
    TSView ref_view{ref_source_view_data_, current_time};
    if (ref_view.modified()) {
        return true;  // Reference changed - always report modified (sampled)
    }
    TSView target_view = target_view(current_time);
    return target_view.modified();
}
```

This ensures consumers of the alternative are notified when their data source changes, even if the new target wasn't modified at that tick. This logic is only relevant for REFLink in TSOutput alternatives, not for TSInput which uses simple LinkTarget binding.

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
- **LinkTarget**: Direct link to native position (no schema conversion needed)
- **REFLink**: For REF → TS positions (dereferencing) - TSOutput alternatives only
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
| TS[T] | TS[T] | LinkTarget → native | Via LinkTarget |
| REF[TS[T]] | REF[TS[T]] | LinkTarget → native | Via LinkTarget |
| REF[TS[T]] | TS[T] | REFLink | REF source + current target |
| REF[X] | Y (X≠Y) | REFLink + nested conversion | REF source + recursive |
| TS[T] | REF[TS[T]] | TSReference value | None (value is static) |
| TSD/TSL | TSD/TSL | Per-element LinkTargets | Native structure changes |
| TSB | TSB | Per-field LinkTargets | Per-field as above |

**Nested Conversion Example**: `REF[TSD[str, TS[int]]] → TSD[str, REF[TS[int]]]`

1. REFLink dereferences outer REF to get link to the actual `TSD[str, TS[int]]` (on a different output)
2. Element-level conversion (`TS[int] → REF[TS[int]]`) happens on that **linked TSD output**, creating TSReferences that point to its elements
3. When REF changes target, REFLink rebinds to new TSD and regenerates element TSReferences for new target's elements

Note: The alternative does NOT copy the TSD - it links to the dereferenced TSD and wraps its elements.

### No Explicit Sync

Alternatives do not require explicit synchronization:
- **LinkTarget positions**: Directly access native data (for same-type bindings)
- **REFLink positions**: Subscribe to REF source and current target (for REF→TS dereferencing)
- **TSReference positions**: Value is constructed once from native path
- **Structural changes**: Alternative subscribes to native for key/element changes

## Open Questions

- TODO: How to handle circular references?
- TODO: Link validation and error handling?
- TODO: Performance of link resolution?

## References

- User Guide: `04_LINKS_AND_BINDING.md`
- Research: `08_REF.md`
