# Time Series Design

## Overview

Time series extend values with temporal semantics:
- **TSValue**: Owns time-series data with modification tracking
- **TSView**: Provides time-aware access to time-series data

## TSValue

### Purpose
Owning container for time-series data with three parallel value structures.

### Structure

```cpp
class TSValue {
    // TODO: Define fields

    // Value value_;           // User-visible data
    // Value time_;            // Modification timestamps
    // Value observer_;        // Observer lists
    // TSMeta* meta_;          // Time-series schema

public:
    // Construction
    // TSValue(TSMeta* meta);

    // Data access
    // View data_view();
    // const View data_view() const;

    // Time access
    // engine_time_t last_modified_time() const;
    // bool modified(engine_time_t current_time) const;
    // bool valid() const;

    // TSView creation
    // TSView ts_view(engine_time_t current_time);
};
```

### Three-Value Parallel Structure

For a TSB[a: TS[int], b: TS[float]]:

```
value_:     {a: 42,    b: 3.14}
time_:      {a: t=100, b: t=50}
observer_:  {a: [...], b: [...]}
```

## TSView

### Purpose
Type-erased non-owning reference to time-series data with current time context.

### Structure

```cpp
class TSView {
    // TODO: Define fields

    // ViewData view_data_;           // path + data* + ops*
    // engine_time_t current_time_;   // Engine's current time

public:
    // State queries
    // bool modified() const;         // time_value >= current_time
    // bool valid() const;            // time_value != MIN_TIME

    // Data access
    // View value() const;            // Access data_value
    // View delta_value() const;      // For delta-capable types

    // Time access
    // engine_time_t last_modified_time() const;
};
```

### Kind-Specific Wrappers

| Kind | Wrapper | Additional Interface |
|------|---------|---------------------|
| Scalar | TSScalarView | value<T>() |
| Bundle | TSBView | field(name), field(index), __getattr__ |
| List | TSLView | size(), at(index), __iter__ |
| Dict | TSDView | get(key), keys(), added_keys(), removed_keys() |
| Set | TSSView | added(), removed() |

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
