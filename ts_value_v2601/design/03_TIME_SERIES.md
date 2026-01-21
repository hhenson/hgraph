# Time Series Design

## Overview

Time series extend values with temporal semantics:
- **TSValue**: Owns time-series data with modification tracking
- **TSView**: Provides time-aware access to time-series data

## TSMeta Schema Generation

When a TSMeta is created, it generates three parallel schemas from the core TS type:

1. **value_schema_**: Schema for user-visible data
2. **time_meta_**: Schema for modification timestamps (mirrors data structure)
3. **observer_meta_**: Schema for observer lists (mirrors data structure)

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
| TSS[T] | Set[engine_time_t] | Per-element timestamps (parallel to data) |
| TSD[K,V] | List[engine_time_t] | Per-entry timestamps (indexed by slot) |
| TSW[T] | (embedded in WindowStorage) | Timestamps stored with values |

### Observer Schema Generation

The observer schema mirrors the data schema structure but stores observer lists:

| TS Type | Observer Schema | Notes |
|---------|-----------------|-------|
| TS[T] | ObserverList | Single observer list |
| TSB[...] (peered) | ObserverList | Single list for all fields |
| TSB[...] (un-peered) | Bundle[ObserverList, ...] | Per-field observer lists |
| TSL[T] | List[ObserverList] | Per-element observer lists |
| TSS[T] | ObserverList | Container-level only |
| TSD[K,V] | List[ObserverList] | Per-entry observer lists (indexed by slot) |

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
Owning container for time-series data with three parallel value structures.

### Structure

```cpp
class TSValue {
    Value value_;             // User-visible data (schema from ts_meta->value_schema_)
    Value time_;              // Modification timestamps (schema from ts_meta->time_meta_)
    Value observer_;          // Observer lists (schema from ts_meta->observer_meta_)
    const TSMeta* meta_;      // Time-series schema

public:
    // Construction from TSMeta
    explicit TSValue(const TSMeta* meta)
        : value_(meta->value_schema())
        , time_(meta->time_meta())
        , observer_(meta->observer_meta())
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

    // TSView creation
    TSView ts_view(engine_time_t current_time);
};
```

### Three-Value Parallel Structure

For a TSB[a: TS[int], b: TS[float]] (un-peered):

```
value_:     Bundle{a: int(42),           b: float(3.14)}
time_:      Bundle{a: engine_time_t(100), b: engine_time_t(50)}
observer_:  Bundle{a: ObserverList[...],  b: ObserverList[...]}
```

For a TSD[str, TS[int]]:

```
value_:     Map{key_set: {"foo", "bar"}, values: [42, 99]}
time_:      List[engine_time_t(100), engine_time_t(75)]  // Parallel by slot index
observer_:  List[ObserverList[...], ObserverList[...]]   // Parallel by slot index
```

For a TSW[float] with period=3:

```
value_:     WindowStorage{
                cyclic: {times: [t1, t2, t3], values: [1.0, 2.0, 3.0]},
                queue: {times: [], values: []}
            }
time_:      (embedded in WindowStorage - uses cyclic.times)
observer_:  ObserverList[...]  // Container-level only
```

## TSView

### Purpose
Type-erased non-owning reference to time-series data with current time context. TSView provides coordinated access to the three parallel structures (value, time, observer) in TSValue.

### Structure

```cpp
class TSView {
    View value_view_;                // View into TSValue::value_
    View time_view_;                 // View into TSValue::time_
    View observer_view_;             // View into TSValue::observer_
    const TSMeta* meta_;             // Time-series schema
    engine_time_t current_time_;     // Engine's current time

public:
    // Construction
    TSView(TSValue& ts_value, engine_time_t current_time)
        : value_view_(ts_value.value_view())
        , time_view_(ts_value.time_view())
        , observer_view_(ts_value.observer_view())
        , meta_(ts_value.meta())
        , current_time_(current_time)
    {}

    // Schema access
    const TSMeta* meta() const { return meta_; }

    // State queries
    bool modified() const { return time_view_.as<engine_time_t>() >= current_time_; }
    bool valid() const { return time_view_.as<engine_time_t>() != MIN_ENGINE_TIME; }

    // Data access
    View value() const { return value_view_; }
    DeltaView delta_value() const;  // For delta-capable types

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

## Open Questions

- TODO: How to handle time wrap-around?
- TODO: Observer notification mechanism?
- TODO: Lazy vs eager timestamp propagation?

## References

- User Guide: `03_TIME_SERIES.md`
- Research: `01_BASE_TIME_SERIES.md`, `03_TSB.md`
