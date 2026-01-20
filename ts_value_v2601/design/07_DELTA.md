# Delta Design

## Overview

Delta tracking provides efficient access to changes:
- **DeltaView**: Type-erased view of changes
- **DeltaValue**: Stored delta representation
- **Computed Delta**: Delta derived from timestamps

## Delta Concepts

### What is a Delta?

A delta represents the change to a time-series value during the current tick:
- For scalars: the new value (full replacement)
- For TSL: added/removed elements
- For TSD: added/removed/modified keys
- For TSS: added/removed elements

### Delta vs Value

```
TSValue (cumulative)     Delta (change)
─────────────────────    ─────────────────
[1, 2, 3, 4, 5]         added: [4, 5]
                        removed: []
```

## DeltaView

### Purpose
Type-erased access to delta information. Can be backed by either stored data (DeltaValue) or computed on-the-fly from timestamps.

### Structure

```cpp
class DeltaView {
    // Backing can be stored or computed
    enum class Backing { STORED, COMPUTED };

    Backing backing_;
    union {
        struct {
            void* delta_data_;          // For STORED: pointer to DeltaValue data
            const delta_ops* ops_;
        } stored;
        struct {
            const TSValue* ts_value_;   // For COMPUTED: source TSValue
            engine_time_t tick_time_;   // Time to compare against
        } computed;
    };
    const TSMeta* meta_;

public:
    // Common interface
    bool empty() const;
    size_t change_count() const;

    // Kind-specific access (via wrappers)
    DeltaScalarView as_scalar() const;
    DeltaTSLView as_list() const;
    DeltaTSDView as_dict() const;
    DeltaTSSView as_set() const;

    // Factory methods
    static DeltaView from_stored(void* data, const delta_ops* ops, const TSMeta* meta);
    static DeltaView from_computed(const TSValue* ts_value, engine_time_t tick_time, const TSMeta* meta);
};
```

### Computed DeltaView

For types where delta can be derived from timestamps:

```cpp
DeltaView DeltaView::from_computed(const TSValue* ts_value, engine_time_t tick_time, const TSMeta* meta) {
    DeltaView dv;
    dv.backing_ = Backing::COMPUTED;
    dv.computed.ts_value_ = ts_value;
    dv.computed.tick_time_ = tick_time;
    dv.meta_ = meta;
    return dv;
}

// TSL computed delta - find elements modified since tick_time
class DeltaTSLView {
    const TSValue* ts_value_;
    engine_time_t tick_time_;

public:
    // Iterator yields indices of modified elements
    auto modified_indices() const {
        return ts_value_->elements()
            | std::views::filter([this](const auto& elem) {
                return elem.last_modified_time() >= tick_time_;
            })
            | std::views::transform([](const auto& elem) { return elem.index(); });
    }
};
```

### Kind-Specific Wrappers

| Kind | Wrapper | Interface |
|------|---------|-----------|
| Scalar | DeltaScalarView | new_value() |
| TSL | DeltaTSLView | added(), removed(), modified_indices() |
| TSD | DeltaTSDView | added_keys(), removed_keys(), modified_keys() |
| TSS | DeltaTSSView | added(), removed() |

## DeltaValue

### Purpose
Explicit storage for delta information when deltas cannot be computed from timestamps.

### Structure

```cpp
class DeltaValue {
    // TODO: Define fields

    // TypeMeta* meta_;
    // void* delta_data_;
    // engine_time_t tick_time_;
};
```

### When DeltaValue is Needed

1. **TSD with removals**: Need to track which keys were removed
2. **TSS**: Need to track added/removed elements explicitly
3. **Complex compound deltas**: When nested changes need explicit tracking

## Computed Delta

### Purpose
Delta derived from timestamp comparisons (no explicit storage).

### Mechanism

```cpp
DeltaView compute_delta(const TSValue& ts_value, engine_time_t current_time) {
    // For TSL: compare each element's timestamp
    // For TSB: compare each field's timestamp
    // Return view of elements where time >= current_time
}
```

### When Computation Works

- TSL: Can compute modified elements by timestamp
- TSB: Can compute modified fields by timestamp
- Scalar: Delta is just the value itself

### When Computation Fails

- TSD: Cannot compute removed keys (they're gone)
- TSS: Cannot compute removed elements (they're gone)

## Delta Backing Strategies

### Decision Matrix

| TS Kind | Backing | Reason |
|---------|---------|--------|
| TS[T] (scalar) | Computed | Delta is just the value; check `modified()` |
| TSB | Computed | Per-field timestamps enable computation |
| TSL | Computed | Per-element timestamps; can find modified indices |
| TSD | **Stored** | Cannot compute removed keys (they're gone) |
| TSS | **Stored** | Cannot compute removed elements (they're gone) |
| REF | Computed | Delta is reference change or target delta |

**Key Insight**: Stored deltas are only needed when **removals** must be tracked. Once an element is removed, there's no timestamp to query - the data is gone.

### Stored (DeltaValue)

Used for TSD and TSS where removals must be tracked.

#### TrackedSetStorage

Combines the current value with delta tracking in a single structure:

```cpp
struct TrackedSetStorage {
    PlainValue _value;            // Current set contents
    PlainValue _added;            // Elements added this cycle
    PlainValue _removed;          // Elements removed this cycle
    const TypeMeta* _element_type{nullptr};
    const TypeMeta* _set_schema{nullptr};

    explicit TrackedSetStorage(const TypeMeta* element_type)
        : _element_type(element_type) {
        if (_element_type) {
            _set_schema = TypeRegistry::instance().set(_element_type).build();
            _value = PlainValue(_set_schema);
            _added = PlainValue(_set_schema);
            _removed = PlainValue(_set_schema);
        }
    }
};
```

#### Delta-Aware Mutation

Mutations track deltas efficiently by canceling out add/remove within the same cycle:

```cpp
bool add(const ConstValueView& elem) {
    if (contains(elem)) return false;

    value().insert(elem);

    // Track delta: if it was removed this cycle, just un-remove it
    auto removed_view = _removed.view().as_set();
    if (removed_view.contains(elem)) {
        removed_view.erase(elem);  // Un-remove
    } else {
        _added.view().as_set().insert(elem);  // New addition
    }
    return true;
}

bool remove(const ConstValueView& elem) {
    if (!contains(elem)) return false;

    value().erase(elem);

    // Track delta: if it was added this cycle, just un-add it
    auto added_view = _added.view().as_set();
    if (added_view.contains(elem)) {
        added_view.erase(elem);  // Un-add
    } else {
        _removed.view().as_set().insert(elem);  // New removal
    }
    return true;
}
```

**Key Insight**: Un-doing operations within the same cycle is efficient - just remove from delta sets rather than storing redundant add+remove pairs.

#### SetDeltaValue

Standalone delta value for serialization or transmission:

```cpp
struct SetDeltaValue {
    PlainValue _added;
    PlainValue _removed;
    const TypeMeta* _element_type{nullptr};
    const TypeMeta* _set_schema{nullptr};

    // Construct from existing set views (copies data)
    SetDeltaValue(ConstSetView added_view, ConstSetView removed_view,
                  const TypeMeta* element_type);
};
```

**Lifecycle**:
- Cleared at start of tick
- Populated during modifications
- Queried by consumers via DeltaView

### Death Time Markers for Delta

Instead of separate delta storage, SetStorage/MapStorage embed a death time in each key entry:

```cpp
// Key entry: (key_data, death_time)
// death_time == MIN_DT → alive
// death_time != MIN_DT → dead at that time (preserved for delta queries)
```

#### Delta Query for Removed Keys

Erased items are added to the end of the free list, enabling efficient delta queries:

```cpp
// Find keys removed this tick by walking free_list in reverse
auto get_removed_this_tick(const SetStorage* storage, engine_time_t current_time) {
    std::vector<size_t> removed;
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
- No separate delta storage needed for removals
- Dead keys preserved until slot reused (can still access key data for delta)
- Efficient reverse traversal of free list finds this tick's removals
- Death time tells exactly when each key was removed

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

### Added/Removed Sets

```cpp
struct TSSDelta {
    std::set<Element> added_;
    std::set<Element> removed_;
};
```

### Access Pattern

```cpp
TSSView set = input.value().as_set();

// Get delta
DeltaTSSView delta = set.delta();

// Iterate added
for (const auto& elem : delta.added()) {
    // Handle new element
}

// Iterate removed
for (const auto& elem : delta.removed()) {
    // Handle removed element
}
```

## TSD Delta Tracking

### Key Categories

```cpp
struct TSDDelta {
    std::vector<Key> added_;      // New keys this tick
    std::vector<Key> removed_;    // Removed keys this tick
    std::vector<Key> modified_;   // Existing keys with new values
};
```

### Access Pattern

```cpp
TSDView dict = input.value().as_dict();
DeltaTSDView delta = dict.delta();

for (const auto& key : delta.added_keys()) {
    TSView new_val = dict.get(key);
}

for (const auto& key : delta.removed_keys()) {
    // Key no longer exists in dict
}

for (const auto& key : delta.modified_keys()) {
    TSView updated_val = dict.get(key);
}
```

## Delta Lifecycle

### Creation
- Delta created when TSOutput is modified
- For stored: explicit construction
- For computed: lazy on first access

### Clearing
- Delta cleared at start of next tick
- Stored deltas reset
- Computed deltas automatically invalid (time moved)

### Propagation
- Delta flows through links to TSInputs
- DeltaView constructed with current time context

## Open Questions

- TODO: Hybrid stored/computed strategy?
- TODO: Delta compression for large changes?
- TODO: Delta serialization format?

## References

- User Guide: `07_DELTA.md`
- Research: `05_TSD.md`, `06_TSS.md`
