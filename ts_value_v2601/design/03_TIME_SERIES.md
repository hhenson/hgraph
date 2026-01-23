# Time Series Design

## Overview

Time series extend values with temporal semantics:
- **TSValue**: Owns time-series data with modification tracking
- **TSView**: Provides time-aware access to time-series data

## TSMeta Schema Generation

When a TSMeta is created, it generates four parallel schemas from the core TS type:

1. **value_schema_**: Schema for user-visible data
2. **time_meta_**: Schema for modification timestamps (mirrors data structure)
3. **observer_meta_**: Schema for observer lists (mirrors data structure)
4. **delta_meta_**: Schema for delta tracking (kind-specific change tracking)

### TS Type to Value Schema Mapping

Each TS type maps to a corresponding value storage type:

| TS Type | Value Schema | Notes |
|---------|--------------|-------|
| TS[T] | T (atomic) | Single scalar value |
| TSB[...] | Bundle | Named fields, each field is a TS type |
| TSL[T] | List[T] | Fixed-size list of TS elements |
| TSS[T] | Set[T] | Set with delta tracking |
| TSD[K,V] | Map[K,V] | Map with delta tracking |
| TSW[T] | WindowStorage[T] | Custom cyclic + queue buffer |
| REF[T] | TimeSeriesReference | Reference to another TS |
| SIGNAL | void | No data, pure tick |

### Time Schema Generation

The time schema mirrors the data schema structure but stores `engine_time_t` timestamps:

| TS Type | Time Schema | Notes |
|---------|-------------|-------|
| TS[T] | engine_time_t | Single timestamp |
| TSB[...] (peered) | engine_time_t | Single timestamp for all fields |
| TSB[...] (un-peered) | Bundle[engine_time_t, ...] | Per-field timestamps |
| TSL[T] | List[engine_time_t] | Per-element timestamps |
| TSS[T] | TimeArray (SlotObserver) | Per-element timestamps (parallel by slot) |
| TSD[K,V] | TimeArray (SlotObserver) | Per-entry timestamps (parallel by slot) |
| TSW[T] | (embedded in WindowStorage) | Timestamps stored with values |

### Observer Schema Generation

The observer schema mirrors the data schema structure but stores observer lists:

| TS Type | Observer Schema | Notes |
|---------|-----------------|-------|
| TS[T] | ObserverList | Single observer list |
| TSB[...] (peered) | ObserverList | Single list for all fields |
| TSB[...] (un-peered) | Bundle[ObserverList, ...] | Per-field observer lists |
| TSL[T] | List[ObserverList] | Per-element observer lists |
| TSS[T] | ObserverArray (SlotObserver) | Per-element observer lists (parallel by slot) |
| TSD[K,V] | ObserverArray (SlotObserver) | Per-entry observer lists (parallel by slot) |

### Delta Schema Generation

The delta schema tracks changes per tick with a leading `delta_time` timestamp for lazy clearing:

| TS Type | Delta Schema | Notes |
|---------|--------------|-------|
| TS[T] | `ScalarDelta` | `{delta_time}` - no data needed, delta = value itself |
| TSB[...] (peered) | `BundleDelta` | `{delta_time, modified_bits}` - bitset of modified field indices |
| TSB[...] (un-peered) | `Bundle[Delta, ...]` | Per-field recursive delta schemas |
| TSL[T] | `ListDelta` | `{delta_time, modified_indices[]}` - list of changed indices |
| TSS[T] | `SetDelta` | `{delta_time, added[], removed[]}` - slot indices |
| TSD[K,V] | `MapDelta` | `{delta_time, added[], removed[]}` - slot indices |
| TSW[T] | `WindowDelta` | `{delta_time}` - additions tracked via buffer position |
| REF[T] | `RefDelta` | `{delta_time}` - reference change tracked elsewhere |
| SIGNAL | `SignalDelta` | `{delta_time}` - pure tick, no data |

**Lazy Clearing**: The `delta_time` field tracks when the delta was last computed. When accessing delta with `current_time > delta_time`, the delta is automatically cleared before returning. This avoids explicit `begin_tick()` calls for delta management.

**Slot-Based Tracking**: For TSS/TSD, delta stores slot indices (not element copies). Element data is accessed via KeySet when iterating. This enables zero-copy delta during the tick.

### TSW WindowStorage

TSW requires a custom storage type that maintains a time-ordered window of values:

```cpp
struct WindowStorage {
    // Cyclic buffer for the main window (fixed capacity)
    struct CyclicBuffer {
        std::vector<engine_time_t> times;   // Timestamps
        std::vector<std::byte> values;       // Type-erased values (element_size * capacity)
        size_t head;                         // Write position
        size_t count;                        // Current element count
        size_t capacity;                     // Maximum elements
    } cyclic;

    // Queue buffer for overflow (unbounded, oldest items)
    struct QueueBuffer {
        std::vector<engine_time_t> times;
        std::vector<std::byte> values;
    } queue;

    // Schema for element type
    const TypeMeta* element_meta;
};
```

**Window Semantics:**
- New values are added to the cyclic buffer
- When cyclic buffer is full, oldest values overflow to queue
- Query methods provide time-range access
- Both buffers maintain parallel time/value arrays

## TSValue

### Purpose
Owning container for time-series data with four parallel value structures.

### Structure

```cpp
class TSValue {
    Value value_;             // User-visible data (schema from ts_meta->value_schema_)
    Value time_;              // Modification timestamps (schema from ts_meta->time_meta_)
    Value observer_;          // Observer lists (schema from ts_meta->observer_meta_)
    Value delta_;             // Delta tracking (schema from ts_meta->delta_meta_)
    const TSMeta* meta_;      // Time-series schema

public:
    // Construction from TSMeta
    explicit TSValue(const TSMeta* meta)
        : value_(meta->value_schema())
        , time_(meta->time_meta())
        , observer_(meta->observer_meta())
        , delta_(meta->delta_meta())
        , meta_(meta)
    {}

    // Schema access
    const TSMeta* meta() const { return meta_; }

    // Data access
    View value_view() { return value_.view(); }
    const View value_view() const { return value_.view(); }

    // Time access
    View time_view() { return time_.view(); }
    engine_time_t last_modified_time() const;
    bool modified(engine_time_t current_time) const;
    bool valid() const;

    // Observer access
    View observer_view() { return observer_.view(); }

    // Delta access (with lazy clearing)
    DeltaView delta(engine_time_t current_time);

    // TSView creation
    TSView ts_view(engine_time_t current_time);
};
```

### Delta Access and Lazy Clearing

The `delta_` value includes a `delta_time` field as its first property. When accessing delta:

```cpp
View TSValue::delta_view(engine_time_t current_time) {
    engine_time_t delta_time = delta_.view().field(0).as<engine_time_t>();
    if (current_time > delta_time) {
        // Lazy clear: reset delta contents, update delta_time
        clear_delta_contents();
        delta_.view().field(0).set(current_time);
    }
    return delta_.view();
}
```

This eliminates the need for explicit `begin_tick()` calls - delta state is managed automatically on access.

### Four-Value Parallel Structure

Each TSValue has four separate Value instances. Examples:

**TSB[a: TS[int], b: TS[float]] (un-peered):**

```
value_:     Bundle{a: int(42),              b: float(3.14)}
time_:      Bundle{a: engine_time_t(100),   b: engine_time_t(50)}
observer_:  Bundle{a: ObserverList[...],    b: ObserverList[...]}
delta_:     Bundle{a: ScalarDelta{time:100}, b: ScalarDelta{time:50}}
```

**TSD[str, TS[int]]:**

```
value_:     MapStorage{KeySet: ["foo","bar"], values: [42, 99]}
time_:      TimeArray[100, 75]          // Parallel by slot index
observer_:  ObserverArray[obs0, obs1]   // Parallel by slot index
delta_:     MapDelta{
                delta_time: 100,
                added: [1],             // Slot indices added this tick
                removed: []             // Slot indices removed this tick
            }
```

**TSS[int]:**

```
value_:     SetStorage{KeySet: [1, 2, 3, 4]}
time_:      TimeArray[100, 75, 100, 100]    // Parallel by slot index
observer_:  ObserverArray[obs0, obs1, ...]  // Parallel by slot index
delta_:     SetDelta{
                delta_time: 100,
                added: [2, 3],          // Slot indices added this tick
                removed: []             // Slot indices removed this tick
            }
```

**TSW[float] with period=3:**

```
value_:     WindowStorage{
                cyclic: {times: [t1, t2, t3], values: [1.0, 2.0, 3.0]},
                queue: {times: [], values: []}
            }
time_:      (embedded in WindowStorage - uses cyclic.times)
observer_:  ObserverList[...]           // Container-level only
delta_:     WindowDelta{delta_time: t3} // Additions tracked via buffer position
```

## TSView

### Purpose
Type-erased non-owning reference to time-series data with current time context. TSView provides coordinated access to the four parallel structures (value, time, observer, delta) in TSValue.

### Structure

```cpp
class TSView {
    View value_view_;                // View into TSValue::value_
    View time_view_;                 // View into TSValue::time_
    View observer_view_;             // View into TSValue::observer_
    View delta_view_;                // View into TSValue::delta_ (lazily cleared)
    const TSMeta* meta_;             // Time-series schema
    engine_time_t current_time_;     // Engine's current time

public:
    // Construction (handles lazy delta clearing)
    TSView(TSValue& ts_value, engine_time_t current_time)
        : value_view_(ts_value.value_view())
        , time_view_(ts_value.time_view())
        , observer_view_(ts_value.observer_view())
        , delta_view_(ts_value.delta_view(current_time))  // Lazy clear on access
        , meta_(ts_value.schema())
        , current_time_(current_time)
    {}

    // Schema access
    const TSMeta* meta() const { return meta_; }

    // State queries
    bool modified() const { return time_view_.as<engine_time_t>() >= current_time_; }
    bool valid() const { return time_view_.as<engine_time_t>() != MIN_ENGINE_TIME; }

    // Data access
    View value() const { return value_view_; }

    // Delta access (already lazily cleared during construction)
    View delta() const { return delta_view_; }
    DeltaView delta_value() const;  // Typed delta view for kind-specific access
    engine_time_t delta_time() const { return delta_view_.field(0).as<engine_time_t>(); }

    // Time access
    engine_time_t last_modified_time() const { return time_view_.as<engine_time_t>(); }
    engine_time_t current_time() const { return current_time_; }

    // Observer access (for internal use)
    View observer() const { return observer_view_; }
};
```

### Kind-Specific Wrappers

| Kind | Wrapper | Additional Interface |
|------|---------|---------------------|
| Scalar | TSScalarView | value<T>() |
| Bundle | TSBView | field(name), field(index), __getattr__ |
| List | TSLView | size(), at(index), __iter__ |
| Dict | TSDView | at(key), keys(), added_keys(), removed_keys() |
| Set | TSSView | added(), removed() |
| Window | TSWView | values(), times(), at_time(t), range(t1, t2) |

### Example: TSBView

```cpp
class TSBView : public TSView {
public:
    // Field access
    TSView field(std::string_view name);
    TSView field(size_t index);

    // Per-field modification
    bool modified(std::string_view name) const;
    bool valid(std::string_view name) const;

    // Python-style access
    TSView __getattr__(std::string_view name);
};
```

### Example: TSWView

```cpp
class TSWView : public TSView {
public:
    // Access all values in the window (time-ordered, oldest first)
    ViewRange values() const;

    // Access all timestamps in the window
    ViewRange times() const;

    // Access value at specific time (or nearest)
    View at_time(engine_time_t t) const;

    // Access values in time range [start, end)
    ViewRange range(engine_time_t start, engine_time_t end) const;

    // Window properties
    size_t size() const;              // Current number of values
    size_t capacity() const;          // Maximum values in cyclic buffer
    engine_time_t oldest_time() const;
    engine_time_t newest_time() const;

    // Iteration with timestamps
    ViewPairRange items() const;      // (time, value) pairs
};
```

## Modification Tracking

Modification tracking uses the **three-value parallel structure** in TSValue:
- `value_`: User-visible data
- `time_`: Modification timestamps (mirrors data structure)
- `observer_`: Observer lists (mirrors data structure)

For collections (TSL, TSD, TSS), `time_` contains per-element timestamps that track when each element was last modified. This is tracked independently within TSValue - no separate overlay needed.

### Tick Detection

```cpp
bool TSView::modified() const {
    return time_value() >= current_time_;
}
```

### Validity

```cpp
bool TSView::valid() const {
    return time_value() != MIN_ENGINE_TIME;
}
```

### Per-Element Tracking (Collections)

For TSL, TSD, TSS - each element has its own timestamp in `time_`, indexed by the same slot index as the data:

```cpp
// TSL example - time_ is a parallel array of timestamps
bool TSLView::element_modified(size_t idx) const {
    return time_.at(idx) >= current_time_;
}

// TSD example - time_ is parallel array, shares slot index with MapStorage
bool TSDView::key_modified(const Key& key) const {
    // 1. Look up slot index from value_'s index_set
    size_t idx = value_.index_of(key);
    // 2. Access timestamp at same index
    return time_.at(idx) >= current_time_;
}
```

**TSD Key Sharing**: For TSD, the key set lives only in `value_` (MapStorage). The `time_` and `observer_` are parallel arrays indexed by the same slot index - no duplicate key storage.

### Nested Modification

For compounds, modification cascades up:
- If `tsb.a` is modified, `tsb` is also marked modified
- Parent timestamps are max of child timestamps

## Peered vs Un-Peered (TSB)

### Peered TSB
- All fields tick together
- Single timestamp for entire bundle
- Atomic update semantics

### Un-Peered TSB
- Fields tick independently
- Per-field timestamps
- Fine-grained update tracking

## TSS and TSD Storage Architecture

TSS and TSD extend the Set/Map storage architecture (see `01_SCHEMA.md`) with time-series extensions via the SlotObserver protocol.

### Layer Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                      ts_ops layer                               │
│  TSSStorage, TSDStorage - time-series semantics, delta tracking │
├─────────────────────────────────────────────────────────────────┤
│                     type_ops layer                              │
│  SetStorage, MapStorage - value semantics via set_ops/map_ops   │
├─────────────────────────────────────────────────────────────────┤
│                    KeySet (core)                                │
│  Slot management, hash index, membership, generation            │
└─────────────────────────────────────────────────────────────────┘
```

### TS Extension: TimeArray

Parallel timestamp array that observes KeySet:

```cpp
class TimeArray : public SlotObserver {
    std::vector<engine_time_t> times_;

public:
    void on_capacity(size_t, size_t new_cap) override {
        times_.resize(new_cap, MIN_ENGINE_TIME);
    }
    void on_insert(size_t slot) override {
        times_[slot] = MIN_ENGINE_TIME;  // Invalid until set
    }
    void on_erase(size_t slot) override {
        // Keep time (may be queried for delta)
    }
    void on_clear() override {
        std::fill(times_.begin(), times_.end(), MIN_ENGINE_TIME);
    }

    engine_time_t at(size_t slot) const { return times_[slot]; }
    void set(size_t slot, engine_time_t t) { times_[slot] = t; }
    bool modified(size_t slot, engine_time_t current) const {
        return times_[slot] >= current;
    }
    bool valid(size_t slot) const { return times_[slot] != MIN_ENGINE_TIME; }

    // Toll-free numpy/Arrow access
    engine_time_t* data() { return times_.data(); }
};
```

### TS Extension: ObserverArray

Parallel observer lists that observe KeySet:

```cpp
class ObserverArray : public SlotObserver {
    std::vector<ObserverList> observers_;

public:
    void on_capacity(size_t, size_t new_cap) override {
        observers_.resize(new_cap);
    }
    void on_insert(size_t slot) override {
        observers_[slot].clear();
    }
    void on_erase(size_t slot) override {
        observers_[slot].notify_removed();
        observers_[slot].clear();
    }
    void on_clear() override {
        for (auto& obs : observers_) {
            obs.notify_removed();
            obs.clear();
        }
    }

    ObserverList& at(size_t slot) { return observers_[slot]; }
};
```

### TS Extension: Delta Storage (in delta_ Value)

Delta tracking for TSS/TSD is stored in the `delta_` Value of TSValue. The delta schema includes a `delta_time` timestamp as its first field for lazy clearing. Delta uses **slot-based tracking** - storing slot indices, not element copies. See `07_DELTA.md` for full delta design.

**SetDelta Schema** (for TSS):
```cpp
struct SetDelta {
    engine_time_t delta_time;     // When delta was last computed
    std::vector<size_t> added;    // Slot indices added since delta_time
    std::vector<size_t> removed;  // Slot indices removed since delta_time
};
```

**MapDelta Schema** (for TSD):
```cpp
struct MapDelta {
    engine_time_t delta_time;     // When delta was last computed
    std::vector<size_t> added;    // Slot indices added since delta_time
    std::vector<size_t> removed;  // Slot indices removed since delta_time
};
```

**Lazy Clearing**: When accessing delta with `current_time > delta_time`:
1. Clear added/removed slot vectors
2. Update `delta_time = current_time`
3. Return cleared delta view

This replaces explicit `begin_tick()` calls with automatic management on access.

**Delta Population via SlotObserver**: The delta storage implements SlotObserver to receive insert/erase notifications from KeySet:

```cpp
// SetDelta/MapDelta as SlotObserver (embedded in delta_ Value)
void on_insert(size_t slot) {
    // If was removed this tick, cancel out; else add to added
    auto it = std::find(removed.begin(), removed.end(), slot);
    if (it != removed.end()) {
        removed.erase(it);
    } else {
        added.push_back(slot);
    }
}

void on_erase(size_t slot) {
    // If was added this tick, cancel out; else add to removed
    auto it = std::find(added.begin(), added.end(), slot);
    if (it != added.end()) {
        added.erase(it);
    } else {
        removed.push_back(slot);
    }
}
```

**Slot-Based Benefits**:
- Zero-copy during tick - slots reference live data in KeySet
- Stable slot indices until reused
- Element/key/value data accessed via KeySet when iterating delta

### TSS: Four Parallel Values

For TSS, each of the four Values stores its respective data:

| Value | Schema | Contents |
|-------|--------|----------|
| `value_` | SetStorage | KeySet (elements) |
| `time_` | TimeArray | Per-slot timestamps |
| `observer_` | ObserverArray | Per-slot observer lists |
| `delta_` | SetDelta | delta_time + added/removed slot indices |

All three extensions (time_, observer_, delta_) observe the KeySet in value_:

```cpp
// TSValue construction for TSS wires up observers
TSValue::TSValue(const TSMeta* meta) {
    // value_ contains SetStorage with KeySet
    auto& ks = value_.as<SetStorage>().key_set();

    // time_, observer_, delta_ observe the KeySet
    ks.observers_.push_back(&time_.as<TimeArray>());
    ks.observers_.push_back(&observer_.as<ObserverArray>());
    ks.observers_.push_back(&delta_.as<SetDelta>());
}

// SetDelta in delta_ (implements SlotObserver)
struct SetDelta : public SlotObserver {
    engine_time_t delta_time{MIN_ENGINE_TIME};
    std::vector<size_t> added;    // Slot indices
    std::vector<size_t> removed;  // Slot indices

    // SlotObserver interface - tracks slot indices
    void on_insert(size_t slot) override {
        auto it = std::find(removed.begin(), removed.end(), slot);
        if (it != removed.end()) { removed.erase(it); }
        else { added.push_back(slot); }
    }
    void on_erase(size_t slot) override {
        auto it = std::find(added.begin(), added.end(), slot);
        if (it != added.end()) { added.erase(it); }
        else { removed.push_back(slot); }
    }

    // Lazy clear (called when current_time > delta_time)
    void clear_if_stale(engine_time_t current_time) {
        if (current_time > delta_time) {
            added.clear();
            removed.clear();
            delta_time = current_time;
        }
    }
};
```

### TSD: Four Parallel Values

For TSD, each of the four Values stores its respective data:

| Value | Schema | Contents |
|-------|--------|----------|
| `value_` | MapStorage | KeySet (keys) + ValueArray (values) |
| `time_` | TimeArray | Per-slot timestamps |
| `observer_` | ObserverArray | Per-slot observer lists |
| `delta_` | MapDelta | delta_time + added/removed slot indices |

All three extensions (time_, observer_, delta_) observe the KeySet in value_:

```cpp
// TSValue construction for TSD wires up observers
TSValue::TSValue(const TSMeta* meta) {
    // value_ contains MapStorage with KeySet + ValueArray
    auto& ks = value_.as<MapStorage>().as_set().key_set();

    // time_, observer_, delta_ observe the KeySet
    ks.observers_.push_back(&time_.as<TimeArray>());
    ks.observers_.push_back(&observer_.as<ObserverArray>());
    ks.observers_.push_back(&delta_.as<MapDelta>());
}

// MapDelta in delta_ (implements SlotObserver)
struct MapDelta : public SlotObserver {
    engine_time_t delta_time{MIN_ENGINE_TIME};
    std::vector<size_t> added;    // Slot indices
    std::vector<size_t> removed;  // Slot indices

    // SlotObserver interface - tracks slot indices
    void on_insert(size_t slot) override {
        auto it = std::find(removed.begin(), removed.end(), slot);
        if (it != removed.end()) { removed.erase(it); }
        else { added.push_back(slot); }
    }
    void on_erase(size_t slot) override {
        auto it = std::find(added.begin(), added.end(), slot);
        if (it != added.end()) { added.erase(it); }
        else { removed.push_back(slot); }
    }

    // Lazy clear (called when current_time > delta_time)
    void clear_if_stale(engine_time_t current_time) {
        if (current_time > delta_time) {
            added.clear();
            removed.clear();
            delta_time = current_time;
        }
    }
};
```

### Composition Diagram

TSValue has four parallel Values. For TSD:

```
TSValue (4 parallel Values)
│
├── value_: MapStorage
│   ├── SetStorage (as_set() returns reference)
│   │   └── KeySet
│   │       ├── keys_[]        ──► Arrow key column
│   │       ├── generations_[] ──► Validity bitmap
│   │       └── observers_ ────────┬──────┬──────┐
│   └── ValueArray                 │      │      │
│       └── values_[]      ──► Arrow value column│
│                                  │      │      │
├── time_: TimeArray ◄─────────────┘      │      │
│   └── times_[]           ──► Arrow time column │
│                                         │      │
├── observer_: ObserverArray ◄────────────┘      │
│   └── observers_[]                             │
│                                                │
└── delta_: MapDelta ◄───────────────────────────┘
    ├── delta_time        // For lazy clearing
    ├── added[]           // Slot indices
    └── removed[]         // Slot indices

value_    = user data (MapStorage for TSD, SetStorage for TSS)
time_     = TimeArray (observes KeySet in value_)
observer_ = ObserverArray (observes KeySet in value_)
delta_    = SetDelta/MapDelta (observes KeySet in value_)
```

### Toll-Free Casting Chain

All casts return references - no copying:

```cpp
// TSD → Map → Set → KeySet
const KeySet& ks = tsd.as_map().as_set().key_set();

// TSS → Set → KeySet
const KeySet& ks = tss.as_set().key_set();

// Access underlying arrays for Arrow conversion
const std::byte* keys = ks.keys_.data();
const std::byte* values = tsd.as_map().value_data();
engine_time_t* times = tsd.times_.data();
```

### Design Rationale

| Decision | Rationale |
|----------|-----------|
| Four parallel Values | Clean separation: value_, time_, observer_, delta_ each have distinct schema |
| delta_ as 4th Value | All delta tracking in one place; separates delta semantics from time semantics |
| delta_time for lazy clearing | No explicit begin_tick(); delta auto-clears when accessed with newer time |
| SlotObserver protocol | Decouples TS extensions from core storage; each owns its memory |
| Slot-based delta tracking | Zero-copy during tick; slots reference live KeySet data |
| No tombstoning in KeySet | Generation handles liveness; delta tracks slots |
| Composition over inheritance | Enables toll-free casting; clear ownership |
| Parallel arrays by slot index | All extensions use same slot index; zero translation |

## Open Questions

- TODO: How to handle time wrap-around?
- TODO: Observer notification mechanism?

## References

- User Guide: `03_TIME_SERIES.md`
- Research: `01_BASE_TIME_SERIES.md`, `03_TSB.md`
