# Delta Design

## Overview

Delta tracking provides efficient access to changes. The design supports **two forms of delta**:

| Form | Backing | Lifetime | Use Case |
|------|---------|----------|----------|
| **Computed** | DeltaTracker, timestamps, storage state | Current tick only | In-process evaluation |
| **Persistent** | DeltaValue (owned storage) | Beyond tick | Serialization, transmission, replay |

Both forms are accessed through a unified **DeltaView** interface, which provides kind-specific delta operations regardless of backing.

```
┌─────────────────────────────────────────────────────────────────┐
│                        DeltaView                                │
│         Unified interface for delta operations                  │
├─────────────────────────────────────────────────────────────────┤
│  Backing::COMPUTED          │  Backing::STORED                  │
│  ─────────────────          │  ───────────────                  │
│  • DeltaTracker slots       │  • DeltaValue storage             │
│  • TimeArray timestamps     │  • Copied/owned data              │
│  • Live storage references  │  • Self-contained                 │
│  • Ephemeral (tick scope)   │  • Persistent                     │
└─────────────────────────────┴───────────────────────────────────┘
```

## Delta Concepts

### What is a Delta?

A delta represents the change to a time-series value during the current tick:
- For scalars: the new value (full replacement)
- For TSB: updated fields
- For TSL: updated elements (by index)
- For TSD: added/removed/updated key-value pairs
- For TSS: added/removed elements

### Delta vs Value

```
TSS (Set):
  Value: {a, b, c, d}       Delta: added={c, d}, removed={x}

TSD (Dict):
  Value: {k1→v1, k2→v2}     Delta: added={k2→v2}, updated={k1→v1'}, removed={k3→v3}

TSL (List):
  Value: [v0, v1, v2]       Delta: updated={0→v0', 2→v2'}  (indices that changed)

TSB (Bundle):
  Value: {.a=1, .b=2}       Delta: updated={.b}  (fields that changed)
```

### Two Delta Forms

**Computed Delta** (ephemeral):
- Derived from live storage state (DeltaTracker, timestamps)
- References storage directly via slot indices
- Valid only during current tick
- Zero-copy - no data duplication
- Used for in-process graph evaluation

**Persistent Delta** (DeltaValue):
- Self-contained storage with copied data
- Owns its added/removed element data
- Valid beyond the tick
- Used for serialization, network transmission, replay, logging

## DeltaView

### Purpose
Type-erased **unified interface** for delta operations. Abstracts over both computed (ephemeral) and stored (persistent) delta forms, allowing consumers to work with deltas without knowing the backing.

### Structure

```cpp
class DeltaView {
    enum class Backing { STORED, COMPUTED };

    Backing backing_;
    union {
        struct {
            const void* delta_data_;        // DeltaValue storage
            const delta_ops* ops_;          // Type-specific ops
        } stored;
        struct {
            const void* ts_storage_;        // TSSStorage/TSDStorage/etc.
            const ts_delta_ops* ops_;       // Computed delta ops
            engine_time_t tick_time_;       // Reference time for "modified"
        } computed;
    };
    const TSMeta* meta_;

public:
    // Common interface - works for both backings
    bool empty() const;
    size_t change_count() const;
    Backing backing() const { return backing_; }

    // Kind-specific wrappers (delegate to appropriate ops)
    DeltaScalarView as_scalar() const;
    DeltaTSLView as_list() const;
    DeltaTSDView as_dict() const;
    DeltaTSSView as_set() const;

    // Factory methods
    static DeltaView from_stored(const DeltaValue& value, const TSMeta* meta);
    static DeltaView from_computed(const void* ts_storage, const ts_delta_ops* ops,
                                   engine_time_t tick_time, const TSMeta* meta);

    // Convert computed → stored (materializes the delta)
    DeltaValue materialize() const;
};
```

### Unified Operations via delta_ops

The delta_ops vtable provides the same interface regardless of backing:

```cpp
struct delta_ops {
    // Common delta interface
    bool (*empty)(const void* delta_source);
    size_t (*change_count)(const void* delta_source);

    // For TSS/TSD: iteration over added/removed
    ViewRange (*added)(const void* delta_source);
    ViewRange (*removed)(const void* delta_source);

    // For TSD: also updated (existing keys with new values)
    ViewRange (*updated)(const void* delta_source, engine_time_t tick_time);
};

// Computed delta ops - reads from live storage
struct tss_computed_delta_ops : delta_ops {
    bool empty(const void* storage) override {
        auto* tss = static_cast<const TSSStorage*>(storage);
        return tss->delta().added().empty() && tss->delta().removed().empty();
    }
    ViewRange added(const void* storage) override {
        // Return range that iterates slots via DeltaTracker
        auto* tss = static_cast<const TSSStorage*>(storage);
        return make_slot_view_range(tss, tss->delta().added());
    }
    // ...
};

// Stored delta ops - reads from DeltaValue
struct tss_stored_delta_ops : delta_ops {
    bool empty(const void* data) override {
        auto* delta = static_cast<const TSSDeltaValue*>(data);
        return delta->added().empty() && delta->removed().empty();
    }
    ViewRange added(const void* data) override {
        auto* delta = static_cast<const TSSDeltaValue*>(data);
        return delta->added_range();  // Iterate owned storage
    }
    // ...
};
```

### Kind-Specific Wrappers

| Kind | Wrapper | Interface |
|------|---------|-----------|
| Scalar | DeltaScalarView | `new_value()` |
| TSL | DeltaTSLView | `updated_indices()` |
| TSB | DeltaTSBView | `updated_fields()` |
| TSD | DeltaTSDView | `added()`, `removed()`, `updated()`, `removed_keys()` |
| TSS | DeltaTSSView | `added()`, `removed()` |

**Note**: TSL and TSB only support **updated** - elements/fields are modified in place, not added or removed. TSS has add/remove. TSD has add/remove/updated.

All wrappers work identically regardless of COMPUTED or STORED backing.

## DeltaValue (Persistent Delta)

### Purpose
Self-contained, owning storage for delta information. Used when delta must persist beyond the current tick (serialization, transmission, logging, replay).

### Structure

```cpp
// Base class for all delta values
class DeltaValue {
protected:
    const TSMeta* meta_;

public:
    virtual ~DeltaValue() = default;
    const TSMeta* meta() const { return meta_; }

    // Type-specific delta values override these
    virtual bool empty() const = 0;
    virtual size_t change_count() const = 0;
};

// TSS delta value - SoA storage for added/removed elements
class TSSDeltaValue : public DeltaValue {
    // Parallel vectors (element-aligned, SoA layout)
    std::vector<std::byte> added_;      // Added element values
    std::vector<std::byte> removed_;    // Removed element values

    size_t added_count_{0};
    size_t removed_count_{0};

public:
    TSSDeltaValue(const TSMeta* meta);

    // Construct from computed delta (copies data from live storage)
    static TSSDeltaValue from_computed(const TSSStorage& storage);

    bool empty() const override {
        return added_count_ == 0 && removed_count_ == 0;
    }
    size_t change_count() const override {
        return added_count_ + removed_count_;
    }

    // Element access
    size_t added_count() const { return added_count_; }
    size_t removed_count() const { return removed_count_; }

    View added_at(size_t i) const;      // Element value
    View removed_at(size_t i) const;    // Element value (for user queries)

    // Iteration
    ViewRange added() const;
    ViewRange removed() const;

    // Raw data for Arrow conversion
    const std::byte* added_data() const { return added_.data(); }
    const std::byte* removed_data() const { return removed_.data(); }
};

// TSD delta value - SoA storage for added/updated/removed key-value pairs
class TSDDeltaValue : public DeltaValue {
    // Parallel vectors per category (keys and values element-aligned)
    std::vector<std::byte> added_keys_;
    std::vector<std::byte> added_values_;

    std::vector<std::byte> updated_keys_;
    std::vector<std::byte> updated_values_;

    std::vector<std::byte> removed_keys_;
    std::vector<std::byte> removed_values_;   // Values captured for user queries

    size_t added_count_{0};
    size_t updated_count_{0};
    size_t removed_count_{0};

public:
    TSDDeltaValue(const TSMeta* meta);

    // Construct from computed delta (copies data from live storage)
    static TSDDeltaValue from_computed(const TSDStorage& storage, engine_time_t tick_time);

    bool empty() const override {
        return added_count_ == 0 && updated_count_ == 0 && removed_count_ == 0;
    }
    size_t change_count() const override {
        return added_count_ + updated_count_ + removed_count_;
    }

    // Counts
    size_t added_count() const { return added_count_; }
    size_t updated_count() const { return updated_count_; }
    size_t removed_count() const { return removed_count_; }

    // Element access (key, value pairs)
    View added_key_at(size_t i) const;
    View added_value_at(size_t i) const;

    View updated_key_at(size_t i) const;
    View updated_value_at(size_t i) const;

    View removed_key_at(size_t i) const;
    View removed_value_at(size_t i) const;  // For user queries

    // Iteration
    ViewPairRange added() const;      // Yields (key, value) pairs
    ViewPairRange updated() const;    // Yields (key, value) pairs
    ViewPairRange removed() const;    // Yields (key, value) pairs
    ViewRange removed_keys() const;   // Keys only (for apply operations)

    // Raw data for Arrow conversion (columnar access)
    const std::byte* added_keys_data() const { return added_keys_.data(); }
    const std::byte* added_values_data() const { return added_values_.data(); }
    const std::byte* updated_keys_data() const { return updated_keys_.data(); }
    const std::byte* updated_values_data() const { return updated_values_.data(); }
    const std::byte* removed_keys_data() const { return removed_keys_.data(); }
    const std::byte* removed_values_data() const { return removed_values_.data(); }
};
```

### Materializing Computed → Stored

```cpp
DeltaValue DeltaView::materialize() const {
    if (backing_ == Backing::STORED) {
        // Already stored - clone the DeltaValue
        return clone_delta_value(stored.delta_data_, meta_);
    }

    // Computed - copy data from live storage into DeltaValue
    switch (meta_->kind()) {
        case TSKind::TSS: {
            auto* tss = static_cast<const TSSStorage*>(computed.ts_storage_);
            return TSSDeltaValue::from_computed(*tss);
        }
        case TSKind::TSD: {
            auto* tsd = static_cast<const TSDStorage*>(computed.ts_storage_);
            return TSDDeltaValue::from_computed(*tsd, computed.tick_time_);
        }
        // ... other kinds
    }
}
```

### SoA Storage Layout

DeltaValue uses **Struct of Arrays (SoA)** layout with keys and values in separate parallel vectors:

```
TSDDeltaValue:
├── added_keys_[]      ─┐
├── added_values_[]    ─┴─ Element-aligned (added_keys_[i] pairs with added_values_[i])
├── updated_keys_[]    ─┐
├── updated_values_[]  ─┴─ Element-aligned
├── removed_keys_[]    ─┐
└── removed_values_[]  ─┴─ Element-aligned
```

**Benefits**:
- Toll-free Arrow column conversion (`added_keys_.data()` → Arrow key column)
- Cache-efficient iteration over keys only or values only
- Natural fit for columnar serialization

### Removed Values: Query vs Apply

DeltaValue captures **both keys and values** for removed entries. This supports two use cases:

| Use Case | Needs Values? | Example |
|----------|---------------|---------|
| **User query** | Yes | Logging: "key X with value Y was removed" |
| **Apply operation** | No | Replay: "remove key X" (value not needed) |

```cpp
// User query - needs removed values
for (size_t i = 0; i < delta.removed_count(); ++i) {
    View key = delta.removed_key_at(i);
    View value = delta.removed_value_at(i);
    log("Removed: {} -> {}", key, value);
}

// Or via pair iteration
for (auto [key, value] : delta.removed()) {
    log("Removed: {} -> {}", key, value);
}

// Apply operation - only needs keys
for (auto key : delta.removed_keys()) {
    target.remove(key);  // Value not needed
}
```

**Computed delta** can provide removed values during the tick (slot data still accessible), but this is **required** for user queries. When materializing to DeltaValue, values are always captured.

### When to Use DeltaValue

| Scenario | Use Computed | Use DeltaValue |
|----------|--------------|----------------|
| In-process graph evaluation | ✓ | |
| Serialize delta to disk/network | | ✓ |
| Delta needed after tick ends | | ✓ |
| Replay/audit logging | | ✓ |
| Cross-process delta transmission | | ✓ |
| Memory-constrained (avoid copies) | ✓ | |

## Computed Delta (Ephemeral)

### Purpose
Ephemeral delta derived from live storage state - either from **DeltaTracker** (for add/remove tracking) or **timestamp comparison** (for modified detection). No data copying; valid only during current tick.

### Computation Sources

| Source | What it provides | Used by |
|--------|------------------|---------|
| **DeltaTracker** | Slot indices of added/removed elements | TSS, TSD |
| **TimeArray** | Per-slot timestamps for updated detection | TSD |
| **Timestamp comparison** | Elements/fields where time >= tick_time | TSL, TSB |
| **Value itself** | The new value is the delta | Scalar |

**Note**: TSL and TSB only track **updated** elements/fields (no add/remove semantics). TSS tracks add/remove via DeltaTracker. TSD tracks add/remove via DeltaTracker, plus updated via TimeArray.

### Creating Computed DeltaView

```cpp
// TSS: DeltaTracker provides add/remove
DeltaView tss_delta(const TSSStorage& storage, engine_time_t tick_time) {
    return DeltaView::from_computed(&storage, &tss_computed_delta_ops, tick_time, storage.meta());
}

// TSD: DeltaTracker + TimeArray
DeltaView tsd_delta(const TSDStorage& storage, engine_time_t tick_time) {
    return DeltaView::from_computed(&storage, &tsd_computed_delta_ops, tick_time, storage.meta());
}

// TSL: Timestamp comparison
DeltaView tsl_delta(const TSLStorage& storage, engine_time_t tick_time) {
    return DeltaView::from_computed(&storage, &tsl_computed_delta_ops, tick_time, storage.meta());
}
```

### Characteristics

- **Zero-copy**: References live storage via slot indices
- **Ephemeral**: Invalid after tick ends (storage may change)
- **Lazy**: Iteration happens on access, not creation
- **Efficient**: No memory allocation for delta

## Delta Backing Strategies

### In-Process Evaluation (Computed)

For in-process graph evaluation, **all** TS kinds use computed delta:

| TS Kind | Computation Source | Delta Operations |
|---------|-------------------|------------------|
| TS[T] (scalar) | Value itself | updated only (delta = new value) |
| TSB | Timestamp comparison | updated fields only |
| TSL | Timestamp comparison | updated indices only |
| TSD | DeltaTracker + TimeArray | added/removed/updated |
| TSS | DeltaTracker | added/removed |
| REF | Reference change | updated only |

**Key Insight**: TSL/TSB only support "updated" - elements/fields change in place. TSS has add/remove. TSD has add/remove/updated via DeltaTracker + TimeArray.

### Persistence/Transmission (DeltaValue)

When delta must survive beyond the tick, **materialize** to DeltaValue:

| TS Kind | DeltaValue Type | Contents |
|---------|-----------------|----------|
| TS[T] (scalar) | ScalarDeltaValue | Copy of new value |
| TSB | TSBDeltaValue | Updated field values |
| TSL | TSLDeltaValue | Updated elements (index + value) |
| TSD | TSDDeltaValue | Added/updated/removed (keys + values) |
| TSS | TSSDeltaValue | Added/removed elements |

**Note**: Removed entries in DeltaValue include values (not just keys) for user query purposes.

### When to Materialize

```cpp
// In-process: use computed (no copy)
DeltaView delta = tss.delta();
for (auto elem : delta.as_set().added()) { ... }

// Need to persist: materialize (copies data)
DeltaValue stored = delta.materialize();
serialize(stored);  // Delta now owns its data
```

### DeltaTracker (Computed Delta for TSS/TSD)

For TSS and TSD, **computed** delta tracking is handled by a **DeltaTracker** that observes the KeySet via the SlotObserver protocol (see `01_SCHEMA.md` and `03_TIME_SERIES.md`). This provides zero-copy delta during the tick.

#### DeltaTracker Design

DeltaTracker is a SlotObserver that records slot indices of added/removed elements per tick:

```cpp
class DeltaTracker : public SlotObserver {
    std::vector<size_t> added_;      // Slots added this tick
    std::vector<size_t> removed_;    // Slots removed this tick
    const TypeMeta* key_meta_;       // For optional key copying

public:
    explicit DeltaTracker(const TypeMeta* key_meta) : key_meta_(key_meta) {}

    void on_capacity(size_t, size_t) override { /* No-op */ }

    void on_insert(size_t slot) override {
        // If was removed this tick, cancel out; else add to added_
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            removed_.erase(it);  // Cancel removal
        } else {
            added_.push_back(slot);
        }
    }

    void on_erase(size_t slot) override {
        // If was added this tick, cancel out; else add to removed_
        auto it = std::find(added_.begin(), added_.end(), slot);
        if (it != added_.end()) {
            added_.erase(it);  // Cancel addition
        } else {
            removed_.push_back(slot);
        }
    }

    void on_clear() override {
        // All existing slots become removed (unless added this tick)
        added_.clear();
        // removed_ = all previously alive slots (handled by caller)
    }

    // Tick lifecycle
    void begin_tick() {
        added_.clear();
        removed_.clear();
    }

    // Delta access
    const std::vector<size_t>& added() const { return added_; }
    const std::vector<size_t>& removed() const { return removed_; }

    bool was_added(size_t slot) const {
        return std::find(added_.begin(), added_.end(), slot) != added_.end();
    }

    bool was_removed(size_t slot) const {
        return std::find(removed_.begin(), removed_.end(), slot) != removed_.end();
    }
};
```

#### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Observer pattern** | DeltaTracker observes KeySet; decoupled from core storage |
| **Slot indices, not copies** | Slots are stable; can read key data via KeySet until slot reused |
| **Add/remove cancellation** | Efficient handling of add-then-remove or remove-then-add in same tick |
| **No tombstoning in KeySet** | Generation handles liveness; DeltaTracker handles delta |
| **ts_ops layer concern** | Delta tracking is time-series semantics, not value-layer storage |

#### Lifecycle

1. **begin_tick()**: Clear added_/removed_ vectors
2. **During tick**: on_insert/on_erase callbacks populate vectors
3. **During evaluation**: Consumers query added()/removed() for delta
4. **End of tick**: Vectors remain valid until next begin_tick()

#### Accessing Removed Key Data

Since KeySet doesn't tombstone (removed slots are marked dead via generation=0), the key data remains accessible until the slot is reused:

```cpp
// Safe to read key data during same tick as removal
for (size_t slot : delta_tracker.removed()) {
    // Key data still valid at slot (generation=0 but data not overwritten yet)
    const void* key = key_set.key_at(slot);
    // Use key data...
}
```

**Note**: If key data must survive beyond the tick, DeltaTracker can optionally copy removed keys into a separate buffer.

### Computed (from timestamps)

Used for TS, TSB, TSL where timestamps are sufficient:

```cpp
// TSL: Find elements modified since tick_time
DeltaTSLView compute_tsl_delta(const TSLValue& list, engine_time_t time) {
    // No storage needed - iterate and filter by timestamp
    return DeltaTSLView(&list, time);  // Lazy evaluation
}

// TSB: Find fields modified since tick_time
DeltaTSBView compute_tsb_delta(const TSBValue& bundle, engine_time_t time) {
    return DeltaTSBView(&bundle, time);  // Lazy evaluation
}
```

**Benefits**:
- Zero storage overhead
- Always consistent with source
- Lazy evaluation - only compute what's accessed

### Hybrid Approach for TSL

TSL can track additions at the end efficiently:

```cpp
class TSLValue {
    std::vector<TSValue> elements_;
    size_t size_at_tick_start_;  // Track size at tick start

public:
    void begin_tick() {
        size_at_tick_start_ = elements_.size();
    }

    // Added elements = elements after size_at_tick_start_
    auto added_elements() const {
        return std::span(elements_).subspan(size_at_tick_start_);
    }

    // Modified elements = elements before size_at_tick_start_ with recent timestamp
    auto modified_indices(engine_time_t tick_time) const {
        // ... filter by timestamp
    }
};
```

## TSS Delta Tracking

TSS uses DeltaTracker to observe its underlying KeySet. See `03_TIME_SERIES.md` for the full TSSStorage composition.

### Storage Structure

```cpp
class TSSStorage {
    SetStorage set_;           // Underlying Set (contains KeySet)
    TimeArray times_;          // Per-element timestamps
    ObserverArray observers_;  // Per-element observer lists
    DeltaTracker delta_;       // Tracks added/removed slots
    // ...
};
```

### Access Pattern

```cpp
TSSView set = input.value().as_set();

// Iterate added elements (via slot indices)
for (size_t slot : set.added_slots()) {
    View elem = set.element_at_slot(slot);
    // Handle new element
}

// Iterate removed elements
for (size_t slot : set.removed_slots()) {
    View elem = set.element_at_slot(slot);  // Still readable this tick
    // Handle removed element
}

// Query specific element
bool was_new = set.was_added(elem);
bool was_gone = set.was_removed(elem);
```

### ts_ops::tss_ops Integration

```cpp
struct tss_ops {
    // ...existing membership ops...

    // Delta iteration (slot-based for efficiency)
    const std::vector<size_t>& (*added_slots)(const void* storage);
    const std::vector<size_t>& (*removed_slots)(const void* storage);

    // ViewRange wrappers for convenience
    ViewRange (*added)(const void* storage);    // Yields Views over added slots
    ViewRange (*removed)(const void* storage);  // Yields Views over removed slots

    bool (*was_added)(const void* storage, View elem);
    bool (*was_removed)(const void* storage, View elem);
};
```

## TSD Delta Tracking

TSD uses DeltaTracker for add/remove and TimeArray for modified detection. See `03_TIME_SERIES.md` for the full TSDStorage composition.

### Storage Structure

```cpp
class TSDStorage {
    MapStorage map_;           // Underlying Map (contains SetStorage → KeySet)
    TimeArray times_;          // Per-entry timestamps
    ObserverArray observers_;  // Per-entry observer lists
    DeltaTracker delta_;       // Tracks added/removed key slots
    // ...
};
```

### Delta Categories

| Category | Source | Detection |
|----------|--------|-----------|
| **added** | DeltaTracker.added() | Slot inserted this tick |
| **removed** | DeltaTracker.removed() | Slot erased this tick |
| **updated** | TimeArray | Existing slot with time >= current_tick (not in added) |

**Note**: "updated" = existing key with new value (not newly added this tick).

### Access Pattern

```cpp
TSDView dict = input.value().as_dict();

// Iterate added keys
for (size_t slot : dict.added_slots()) {
    View key = dict.key_at_slot(slot);
    TSView val = dict.value_at_slot(slot);
    // Handle new entry
}

// Iterate removed keys
for (size_t slot : dict.removed_slots()) {
    View key = dict.key_at_slot(slot);  // Still readable this tick
    View val = dict.value_at_slot(slot);  // Value also readable
    // Handle removed entry
}

// Iterate updated keys (existing entries with new values, excluding added)
for (size_t slot : dict.updated_slots(current_time)) {
    View key = dict.key_at_slot(slot);
    TSView val = dict.value_at_slot(slot);
    // Handle updated entry
}
```

### ts_ops::tsd_ops Integration

```cpp
struct tsd_ops {
    // ...existing map ops...

    // Delta iteration (slot-based for efficiency)
    const std::vector<size_t>& (*added_slots)(const void* storage);
    const std::vector<size_t>& (*removed_slots)(const void* storage);
    std::vector<size_t> (*updated_slots)(const void* storage, engine_time_t current);

    // ViewRange wrappers for convenience
    ViewRange (*added_keys)(const void* storage);
    ViewRange (*removed_keys)(const void* storage);
    ViewRange (*updated_keys)(const void* storage, engine_time_t current);

    // Items iteration with delta awareness
    ViewPairRange (*added_items)(const void* storage);
    ViewPairRange (*removed_items)(const void* storage);  // Includes values
    ViewPairRange (*updated_items)(const void* storage, engine_time_t current);
};
```

## Delta Lifecycle

### Computed Delta Lifecycle

1. **begin_tick()**: DeltaTracker clears added_/removed_ vectors
2. **During mutations**: on_insert/on_erase callbacks populate DeltaTracker
3. **On access**: DeltaView::from_computed() wraps live storage
4. **Iteration**: Reads slot indices from DeltaTracker, data from storage
5. **End of tick**: Computed delta becomes invalid (storage may change)

### DeltaValue Lifecycle

1. **Creation**: `delta.materialize()` copies data from live storage
2. **Ownership**: DeltaValue owns its added/removed element data
3. **Access**: DeltaView::from_stored() wraps DeltaValue
4. **Persistence**: Can be serialized, transmitted, stored
5. **Cleanup**: Normal RAII destruction when no longer needed

### Propagation

```cpp
// Output modification triggers delta tracking
output.as_set().add(elem);  // DeltaTracker records slot

// Input receives computed delta view
DeltaView delta = input.delta();  // from_computed wraps storage

// If needed beyond tick, materialize
if (needs_persistence) {
    DeltaValue stored = delta.materialize();
    // stored survives tick boundary
}
```

## Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| **Two delta forms** | Computed (ephemeral, zero-copy) for evaluation; DeltaValue (persistent, owned) for serialization |
| **DeltaView unifies both** | Consumers use same interface regardless of backing |
| **DeltaTracker as SlotObserver** | Decouples delta tracking from core storage; ts_ops layer concern |
| **No tombstoning in KeySet** | Generation handles liveness; DeltaTracker handles delta |
| **Slot indices in computed delta** | Stable references; can read key/value data until slot reused |
| **Add/remove cancellation** | Efficient; no redundant entries for add-then-remove in same tick |
| **SoA storage for DeltaValue** | Keys/values in parallel vectors; toll-free Arrow conversion |
| **Removed includes values** | Required for user queries; optional for apply operations |
| **Materialize on demand** | Only copy data when persistence is needed |
| **delta_ops vtable** | Same operations work for computed and stored via polymorphism |

## Open Questions

- **DeltaValue serialization format**: Binary vs JSON for DeltaValue persistence
- **Delta compression**: For large DeltaValue sets, consider bloom filter or sorted+compressed representation
- **Lazy materialization**: Could defer copying until actually serialized
- **Incremental DeltaValue**: Build DeltaValue incrementally during tick vs materialize at end

## References

- User Guide: `07_DELTA.md`
- Research: `05_TSD.md`, `06_TSS.md`
