# Time Series Design

## Overview

Time series extend values with temporal semantics:
- **TSValue**: Owns time-series data with modification tracking
- **TSView**: Provides time-aware access to time-series data

## TSMeta Schema Generation

When a TSMeta is created, it generates five parallel schemas from the core TS type:

1. **value_schema_**: Schema for user-visible data
2. **time_meta_**: Schema for modification timestamps (mirrors data structure)
3. **observer_meta_**: Schema for observer lists (mirrors data structure)
4. **delta_value_meta_**: Schema for delta tracking data (only where TSS/TSD exist)
5. **link_schema_**: Schema for link tracking data (REFLink storage for binding support)

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

The time schema is **recursive**: every container has its own `engine_time_t` plus a collection of child time schemas (if it has children). This design supports peered/un-peered modes as a **runtime access pattern** rather than a structural difference.

#### Time Schema Rules

```
time_schema(TS[T])     = engine_time_t                                    // Atomic
time_schema(TSS[T])    = engine_time_t                                    // Delta tracks membership
time_schema(TSW[T])    = engine_time_t                                    // Entry times in buffer
time_schema(REF[T])    = engine_time_t                                    // Reference time
time_schema(SIGNAL)    = engine_time_t                                    // Tick time
time_schema(TSB[...])  = tuple[engine_time_t, fixed_list[time_schema(field_i), ...]]
time_schema(TSL[T])    = tuple[engine_time_t, fixed_list[time_schema(T), N]]
time_schema(TSD[K,V])  = tuple[engine_time_t, var_list[time_schema(V)]]
```

| TS Type | Time Schema | Notes |
|---------|-------------|-------|
| TS[T] | `engine_time_t` | Atomic - no children |
| TSB[...] | `tuple[engine_time_t, fixed_list[...]]` | Container time + per-field child times |
| TSL[T] | `tuple[engine_time_t, fixed_list[...]]` | Container time + per-element child times |
| TSD[K,V] | `tuple[engine_time_t, var_list[...]]` | Container time + per-slot child times |
| TSS[T] | `engine_time_t` | Container time only (delta tracks membership) |
| TSW[T] | `engine_time_t` | Container time (entry times embedded in buffer) |
| REF[T] | `engine_time_t` | Reference modification time |
| SIGNAL | `engine_time_t` | Tick time |

#### Worked Examples

**TSL[TS[int]]:**
```
tuple[engine_time_t, fixed_list[engine_time_t, N]]
       ↑                        ↑
       container time           per-element times (fixed size N)
```

**TSL[TSL[TS[int]]]:**
```
tuple[engine_time_t, fixed_list[tuple[engine_time_t, fixed_list[engine_time_t, M]], N]]
       ↑                              ↑                        ↑
       outer time                     inner time               leaf times
```

**TSB[a: TS[int], b: TSL[TS[float]]]:**
```
tuple[engine_time_t, fixed_list[engine_time_t, tuple[engine_time_t, fixed_list[engine_time_t, M]]]]
       ↑                        ↑                    ↑                        ↑
       bundle time              field 'a' time       field 'b' time           field 'b' elements
```

**TSD[str, TS[int]]:**
```
tuple[engine_time_t, var_list[engine_time_t]]
       ↑                      ↑
       container time         per-slot times (variable size, grows with map)
```

**TSD[str, TSL[TS[int]]]:**
```
tuple[engine_time_t, var_list[tuple[engine_time_t, fixed_list[engine_time_t, M]]]]
       ↑                            ↑                        ↑
       container time               per-slot TSL time        per-slot TSL element times
```

**TSS[int]:**
```
engine_time_t    // Just container time - delta_ handles add/remove tracking
```

#### Peered vs Un-Peered (Runtime Behavior)

The time structure is **identical** for peered and un-peered modes. The difference is in **access pattern**:

- **Peered**: Check container's `engine_time_t` for all children (ignore child times)
- **Un-peered**: Check each child's individual `engine_time_t`

This allows peered/un-peered status to change at runtime without restructuring data.

### Observer Schema Generation

The observer schema is **recursive**: every container has its own `ObserverList` plus a collection of child observer schemas (if it has children). This mirrors the time schema structure exactly.

#### Observer Schema Rules

```
observer_schema(TS[T])     = ObserverList                                    // Atomic
observer_schema(TSS[T])    = ObserverList                                    // Container-level only
observer_schema(TSW[T])    = ObserverList                                    // Container-level only
observer_schema(REF[T])    = ObserverList                                    // Reference-level only
observer_schema(SIGNAL)    = ObserverList                                    // Container-level only
observer_schema(TSB[...])  = tuple[ObserverList, fixed_list[observer_schema(field_i), ...]]
observer_schema(TSL[T])    = tuple[ObserverList, fixed_list[observer_schema(T), N]]
observer_schema(TSD[K,V])  = tuple[ObserverList, var_list[observer_schema(V)]]
```

| TS Type | Observer Schema | Notes |
|---------|-----------------|-------|
| TS[T] | `ObserverList` | Atomic - no children |
| TSB[...] | `tuple[ObserverList, fixed_list[...]]` | Container observers + per-field child observers |
| TSL[T] | `tuple[ObserverList, fixed_list[...]]` | Container observers + per-element child observers |
| TSD[K,V] | `tuple[ObserverList, var_list[...]]` | Container observers + per-slot child observers |
| TSS[T] | `ObserverList` | Container-level only (no per-element observers) |
| TSW[T] | `ObserverList` | Container-level only |
| REF[T] | `ObserverList` | Reference-level only |
| SIGNAL | `ObserverList` | Container-level only |

#### Worked Examples

**TSL[TS[int]]:**
```
tuple[ObserverList, fixed_list[ObserverList, N]]
       ↑                        ↑
       container observers      per-element observers (fixed size N)
```

**TSL[TSL[TS[int]]]:**
```
tuple[ObserverList, fixed_list[tuple[ObserverList, fixed_list[ObserverList, M]], N]]
       ↑                              ↑                        ↑
       outer observers                inner observers          leaf observers
```

**TSB[a: TS[int], b: TSL[TS[float]]]:**
```
tuple[ObserverList, fixed_list[ObserverList, tuple[ObserverList, fixed_list[ObserverList, M]]]]
       ↑                        ↑                    ↑                        ↑
       bundle observers         field 'a' obs        field 'b' obs            field 'b' elements
```

**TSD[str, TS[int]]:**
```
tuple[ObserverList, var_list[ObserverList]]
       ↑                      ↑
       container observers    per-slot observers (variable size, grows with map)
```

**TSD[str, TSL[TS[int]]]:**
```
tuple[ObserverList, var_list[tuple[ObserverList, fixed_list[ObserverList, M]]]]
       ↑                            ↑                        ↑
       container observers          per-slot TSL obs         per-slot TSL element obs
```

**TSS[int]:**
```
ObserverList    // Just container observers - no per-element observer tracking
```

#### Observer Purpose

Observers enable fine-grained subscription to changes:
- **Container-level**: Notified when any part of the container changes
- **Child-level**: Notified when specific field/element/slot changes

This supports efficient partial subscriptions (e.g., subscribe only to `tsb.field_a` changes).

### Delta Schema Generation

Delta tracking uses a single `delta_value_` component that contains actual delta data - only exists where needed (TSS/TSD and their containers).

**Key insight**: Delta validity can be derived from `time_` data since modifications bubble up. When `current_time > time_[container]`, the delta for that subtree needs clearing. This eliminates the need for a separate `delta_time_` parallel structure.

**Modified status**: Can be derived from `time_` data, so delta tracking is only needed for TSS/TSD which have dynamic add/remove operations. TSB/TSL only appear in delta_value_ as navigation scaffolding when they contain TSS/TSD.

#### has_delta() - Determines if delta tracking is needed

```
has_delta(TS[T])     = false
has_delta(TSS[T])    = true                    // TSS needs add/remove tracking
has_delta(TSD[K,V])  = true                    // TSD needs add/remove/update tracking
has_delta(TSW[T])    = false
has_delta(REF[T])    = false
has_delta(SIGNAL)    = false
has_delta(TSB[...])  = any(has_delta(field_i) for field_i in fields)
has_delta(TSL[T])    = has_delta(T)
```

#### delta_value_ Schema

```
delta_value_schema(X)        = void                     // if not has_delta(X)

delta_value_schema(TSS[T])   = SetDelta {
                                   added[],             // Slot indices
                                   removed[]            // Slot indices
                               }

delta_value_schema(TSD[K,V]) = MapDelta {
                                   added[],             // Slot indices
                                   removed[],           // Slot indices
                                   updated[],           // Slot indices (existing, modified)
                                   children[]           // var_list[delta_value_schema(V)]
                               }                        // children parallels key vector

delta_value_schema(TSB[...]) = BundleDeltaNav {
                                   children[]           // fixed_list[delta_value_schema(field_i)]
                               }                        // only if has_delta(TSB)

delta_value_schema(TSL[T])   = ListDeltaNav {
                                   children[]           // fixed_list[delta_value_schema(T), N]
                               }                        // only if has_delta(TSL)
```

#### Clear Optimization (Optional)

Navigation structures can optionally embed `last_cleared_time` to short-circuit recursive clearing:

```
BundleDeltaNav {
    last_cleared_time,       // Optional: skip clear if time_ unchanged
    children[]
}

ListDeltaNav {
    last_cleared_time,       // Optional: skip clear if time_ unchanged
    children[]
}
```

When clearing, compare `time_[container]` against `last_cleared_time`. If unchanged, skip clearing that subtree. This avoids recursively visiting unchanged branches.

#### Delta Value Structures

| Type | Structure | Purpose |
|------|-----------|---------|
| `SetDelta` | `{added[], removed[]}` | Terminal - tracks TSS membership changes |
| `MapDelta` | `{added[], removed[], updated[], children[]}` | Terminal + navigation - tracks TSD changes + child deltas |
| `BundleDeltaNav` | `{children[]}` | Navigation only - routes to fields with delta |
| `ListDeltaNav` | `{children[]}` | Navigation only - routes to elements with delta |
| `void` | - | No delta tracking needed |

#### Worked Examples

**TS[int]:**
```
has_delta = false
delta_value_: void
```

**TSS[int]:**
```
has_delta = true
delta_value_: SetDelta { added: [2, 5], removed: [0] }
```

**TSD[str, TS[int]]:**
```
has_delta = true
delta_value_: MapDelta {
    added: [3],
    removed: [1],
    updated: [0, 4],
    children: []          // void children - TS[int] has no delta
}
```

**TSL[TS[int]] size 4:**
```
has_delta = false         // TS[int] has no delta
delta_value_: void
```

**TSL[TSS[int]] size 3:**
```
has_delta = true          // TSS[int] has delta
delta_value_: ListDeltaNav {
    children: [
        SetDelta { added: [...], removed: [...] },   // element 0
        SetDelta { added: [...], removed: [...] },   // element 1
        SetDelta { added: [...], removed: [...] }    // element 2
    ]
}
```

**TSB[a: TS[int], b: TSD[str, TS[float]]]:**
```
has_delta = true          // field 'b' has delta
delta_value_: BundleDeltaNav {
    children: [
        void,             // field 'a': TS[int] has no delta
        MapDelta {        // field 'b': TSD
            added: [...],
            removed: [...],
            updated: [...],
            children: []  // TS[float] has void delta
        }
    ]
}
```

**TSD[str, TSL[TSS[int]]] with TSL size 2:**
```
has_delta = true
delta_value_: MapDelta {
    added: [...],
    removed: [...],
    updated: [...],
    children: [           // Parallels slot storage
        ListDeltaNav {    // Slot 0's TSL
            children: [SetDelta{...}, SetDelta{...}]
        },
        ListDeltaNav {    // Slot 1's TSL
            children: [SetDelta{...}, SetDelta{...}]
        },
        ...
    ]
}
```

**TSD[str, TSD[int, TS[float]]]:**
```
has_delta = true
delta_value_: MapDelta {
    added: [...],
    removed: [...],
    updated: [...],
    children: [           // Parallels outer slot storage
        MapDelta {        // Slot 0's inner TSD
            added: [...], removed: [...], updated: [...],
            children: []  // TS[float] has void delta
        },
        MapDelta {        // Slot 1's inner TSD
            added: [...], removed: [...], updated: [...],
            children: []
        },
        ...
    ]
}
```

#### Lazy Clearing

Delta validity is derived from `time_`. When accessing delta with `current_time > time_[container]`, the delta is automatically cleared. Since modifications bubble up through `time_`, the container-level timestamp indicates whether any subtree has been modified.

```cpp
void clear_delta(SetDelta& d) {
    d.added.clear();
    d.removed.clear();
}

void clear_delta(MapDelta& d) {
    d.added.clear();
    d.removed.clear();
    d.updated.clear();
    for (auto& child : d.children) {
        clear_delta(child);
    }
}

void clear_delta(BundleDeltaNav& d) {
    for (auto& child : d.children) {
        if (child) clear_delta(*child);
    }
}

void clear_delta(ListDeltaNav& d) {
    for (auto& child : d.children) {
        clear_delta(child);
    }
}
```

**Optimization**: Navigation structures can optionally track `last_cleared_time` to short-circuit clearing unchanged subtrees. Compare child's `time_` against `last_cleared_time` - if unchanged, skip that branch.

#### Slot-Based Tracking

For TSS/TSD, delta stores slot indices (not element copies). Element data is accessed via KeySet when iterating. This enables zero-copy delta during the tick.

### Link Schema Generation

The link schema provides storage for binding support. It uses **REFLink** at each position that can be bound, enabling both simple linking and REF→TS dereferencing.

#### Link Schema Rules

```
link_schema(TS[T])     = nullptr                  // Scalars don't support binding
link_schema(TSS[T])    = nullptr                  // Sets don't support binding
link_schema(TSW[T])    = nullptr                  // Windows don't support binding
link_schema(REF[T])    = nullptr                  // REFs don't support binding
link_schema(SIGNAL)    = nullptr                  // Signals don't support binding
link_schema(TSB[...])  = fixed_list[REFLink, field_count]  // Per-field REFLink
link_schema(TSL[T])    = REFLink                  // Collection-level REFLink
link_schema(TSD[K,V])  = REFLink                  // Collection-level REFLink
```

| TS Type | Link Schema | Notes |
|---------|-------------|-------|
| TS[T] | `nullptr` | Scalars are bound at parent level |
| TSB[...] | `fixed_list[REFLink, N]` | One REFLink per field |
| TSL[T] | `REFLink` | Single collection-level link |
| TSD[K,V] | `REFLink` | Single collection-level link |
| TSS[T] | `nullptr` | Sets don't support binding |
| TSW[T] | `nullptr` | Windows don't support binding |
| REF[T] | `nullptr` | References don't support binding |
| SIGNAL | `nullptr` | Signals don't support binding |

#### REFLink Purpose

REFLink serves dual purposes:
1. **Simple linking**: When not bound to a REF source, stores target ViewData (like a simple pointer)
2. **REF dereferencing**: When bound to a REF source, handles dynamic rebinding when the REF value changes

See [Links and Binding](04_LINKS_AND_BINDING.md) for detailed REFLink documentation.

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
Owning container for time-series data with five parallel value structures.

### Structure

```cpp
class TSValue {
    Value value_;             // User-visible data (schema from ts_meta->value_schema_)
    Value time_;              // Modification timestamps (schema from ts_meta->time_meta_)
    Value observer_;          // Observer lists (schema from ts_meta->observer_meta_)
    Value delta_value_;       // Delta tracking data (schema from ts_meta->delta_value_meta_)
    Value link_;              // Link tracking data (schema from link_schema, see 04_LINKS_AND_BINDING)
    const TSMeta* meta_;      // Time-series schema
    engine_time_t last_delta_clear_time_{MIN_ENGINE_TIME};  // For lazy clearing

public:
    // Construction from TSMeta
    explicit TSValue(const TSMeta* meta)
        : value_(meta->value_schema())
        , time_(meta->time_meta())
        , observer_(meta->observer_meta())
        , delta_value_(meta->delta_value_meta())  // May be void if no TSS/TSD
        , link_(generate_link_schema(meta))       // REFLink storage for binding support
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

    // Delta access (with lazy clearing based on time_)
    View delta_value_view(engine_time_t current_time);  // Triggers lazy clear
    DeltaView delta(engine_time_t current_time);

    // TSView creation
    TSView ts_view(engine_time_t current_time);
};
```

### Delta Access and Lazy Clearing

Delta validity is derived from `time_` - since modifications bubble up, the container-level timestamp in `time_` tells us if the structure has been modified. When accessing delta_value_:

```cpp
View TSValue::delta_value_view(engine_time_t current_time) {
    if (current_time > last_delta_clear_time_) {
        // Lazy clear: reset delta_value_ contents
        clear_delta_value();
        last_delta_clear_time_ = current_time;
    }
    return delta_value_.view();
}
```

This eliminates the need for explicit `begin_tick()` calls - delta state is managed automatically on access.

**Note**: `delta_value_` may be void if the TS type contains no TSS/TSD. In that case, delta access is a no-op.

### Five-Value Parallel Structure

Each TSValue has five separate Value instances:

1. **value_**: User-visible data (schema from ts_meta->value_schema_)
2. **time_**: Modification timestamps (recursive, mirrors data structure)
3. **observer_**: Observer lists (recursive, mirrors data structure)
4. **delta_value_**: Delta tracking data (only where TSS/TSD exist)
5. **link_**: Link tracking data (REFLink storage for binding support)

Examples:

**TS[int]** (atomic, no delta tracking):

```
value_:       int(42)
time_:        engine_time_t(100)
observer_:    ObserverList[...]
delta_value_: void                      // No TSS/TSD
```

**TSB[a: TS[int], b: TS[float]]** (no delta tracking):

```
value_:       Bundle{a: int(42), b: float(3.14)}
time_:        tuple[engine_time_t(100), [engine_time_t(100), engine_time_t(50)]]
observer_:    tuple[ObserverList, [ObserverList, ObserverList]]
delta_value_: void                      // No TSS/TSD in fields
```

**TSL[TS[int]] with size 3** (no delta tracking):

```
value_:       List[10, 20, 30]
time_:        tuple[engine_time_t(100), [engine_time_t(100), engine_time_t(75), engine_time_t(100)]]
observer_:    tuple[ObserverList, [ObserverList, ObserverList, ObserverList]]
delta_value_: void                      // No TSS/TSD
```

**TSD[str, TS[int]]** (has delta tracking):

```
value_:       MapStorage{KeySet: ["foo","bar"], values: [42, 99]}
time_:        tuple[engine_time_t(100), var_list[engine_time_t(100), engine_time_t(75)]]
observer_:    tuple[ObserverList, var_list[ObserverList, ObserverList]]
delta_value_: MapDelta{added: [1], removed: [], updated: [0], children: []}
```

**TSD[str, TSL[TS[int]]] with TSL size 2** (has delta tracking):

```
value_:       MapStorage{KeySet: ["foo"], values: [List[10, 20]]}
time_:        tuple[engine_time_t(100), var_list[tuple[engine_time_t(90), [engine_time_t(90), engine_time_t(85)]]]]
observer_:    tuple[ObserverList, var_list[tuple[ObserverList, [ObserverList, ObserverList]]]]
delta_value_: MapDelta{added: [], removed: [], updated: [], children: []}  // TSL has void delta
```

**TSS[int]** (has delta tracking):

```
value_:       SetStorage{KeySet: [1, 2, 3, 4]}
time_:        engine_time_t(100)
observer_:    ObserverList[...]
delta_value_: SetDelta{added: [2, 3], removed: []}  // Slot indices
```

**TSL[TSS[int]] with size 2** (has delta tracking via children):

```
value_:       List[SetStorage{...}, SetStorage{...}]
time_:        tuple[engine_time_t(100), [engine_time_t(90), engine_time_t(85)]]
observer_:    tuple[ObserverList, [ObserverList, ObserverList]]
delta_value_: ListDeltaNav{
                  children: [
                      SetDelta{added: [...], removed: [...]},   // element 0
                      SetDelta{added: [...], removed: [...]}    // element 1
                  ]
              }
```

**TSB[a: TS[int], b: TSD[str, TS[float]]]** (has delta tracking via field b):

```
value_:       Bundle{a: int(42), b: MapStorage{...}}
time_:        tuple[engine_time_t(100), [engine_time_t(100), tuple[engine_time_t(90), var_list[...]]]]
observer_:    tuple[ObserverList, [ObserverList, tuple[ObserverList, var_list[...]]]]
delta_value_: BundleDeltaNav{
                  children: [
                      void,             // field 'a': no delta
                      MapDelta{...}     // field 'b': TSD has delta
                  ]
              }
```

**TSW[float] with period=3** (no delta tracking):

```
value_:       WindowStorage{
                  cyclic: {times: [t1, t2, t3], values: [1.0, 2.0, 3.0]},
                  queue: {times: [], values: []}
              }
time_:        engine_time_t(t3)
observer_:    ObserverList[...]
delta_value_: void                      // No TSS/TSD
```

## TSView

### Purpose
Type-erased non-owning reference to time-series data with current time context. TSView provides coordinated access to the four parallel structures (value, time, observer, delta_value) in TSValue.

### Structure

```cpp
class TSView {
    View value_view_;                // View into TSValue::value_
    View time_view_;                 // View into TSValue::time_
    View observer_view_;             // View into TSValue::observer_
    View delta_value_view_;          // View into TSValue::delta_value_ (lazily cleared)
    const TSMeta* meta_;             // Time-series schema
    engine_time_t current_time_;     // Engine's current time

public:
    // Construction (handles lazy delta clearing)
    TSView(TSValue& ts_value, engine_time_t current_time)
        : value_view_(ts_value.value_view())
        , time_view_(ts_value.time_view())
        , observer_view_(ts_value.observer_view())
        , delta_value_view_(ts_value.delta_value_view(current_time))  // Lazy clear on access
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

    // Delta access (delta_value_ already lazily cleared during construction)
    View delta_value() const { return delta_value_view_; }  // May be void if no TSS/TSD
    DeltaView delta() const;  // Typed delta view for kind-specific access

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

Modification tracking uses the **four-value parallel structure** in TSValue:
- `value_`: User-visible data
- `time_`: Modification timestamps (mirrors data structure)
- `observer_`: Observer lists (mirrors data structure)
- `delta_value_`: Delta tracking data (only where TSS/TSD exist)

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

For TSS, the schemas are simple (no per-element time/observer tracking):

| Value | Schema | Contents |
|-------|--------|----------|
| `value_` | SetStorage | KeySet (elements) |
| `time_` | `engine_time_t` | Container modification time |
| `observer_` | `ObserverList` | Container-level observers |
| `delta_value_` | SetDelta | added/removed slot indices |

Delta observes the KeySet in value_ for add/remove tracking:

```cpp
// TSValue construction for TSS
TSValue::TSValue(const TSMeta* meta) {
    // value_ contains SetStorage with KeySet
    auto& ks = value_.as<SetStorage>().key_set();

    // delta_value_ observes KeySet (for add/remove tracking)
    ks.observers_.push_back(&delta_value_.as<SetDelta>());
}

// SetDelta in delta_value_ (implements SlotObserver)
struct SetDelta : public SlotObserver {
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

    void clear() {
        added.clear();
        removed.clear();
    }
};

// Lazy clearing is handled by checking delta_time_ before accessing delta_value_
```

### TSD: Four Parallel Values

For TSD, the schemas follow the recursive pattern with variable-size lists for slots:

| Value | Schema | Contents |
|-------|--------|----------|
| `value_` | MapStorage | KeySet (keys) + ValueArray (values) |
| `time_` | `tuple[engine_time_t, var_list[time_schema(V)]]` | Container time + per-slot child times |
| `observer_` | `tuple[ObserverList, var_list[observer_schema(V)]]` | Container + per-slot observers |
| `delta_value_` | MapDelta | added/removed/updated slot indices + child deltas |

Time and observer var_lists grow with the map. Delta observes KeySet for add/remove:

```cpp
// TSValue construction for TSD wires up observers
TSValue::TSValue(const TSMeta* meta) {
    // value_ contains MapStorage with KeySet + ValueArray
    auto& ks = value_.as<MapStorage>().as_set().key_set();

    // time_, observer_ var_lists track slot additions/removals
    ks.observers_.push_back(&time_.as<TimeTuple>().var_list());
    ks.observers_.push_back(&observer_.as<ObserverTuple>().var_list());

    // delta_value_ tracks add/remove for delta queries
    ks.observers_.push_back(&delta_value_.as<MapDelta>());
}

// MapDelta in delta_value_ (implements SlotObserver)
struct MapDelta : public SlotObserver {
    std::vector<size_t> added;    // Slot indices added this tick
    std::vector<size_t> removed;  // Slot indices removed this tick
    std::vector<size_t> updated;  // Slot indices updated (existing, modified)
    std::vector<ChildDelta> children;  // Parallels slot storage (for nested TSS/TSD)

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

    // Called when an existing slot's value is modified (not added/removed)
    void on_update(size_t slot) {
        if (std::find(added.begin(), added.end(), slot) == added.end()) {
            // Only track as updated if not already in added
            if (std::find(updated.begin(), updated.end(), slot) == updated.end()) {
                updated.push_back(slot);
            }
        }
    }

    void clear() {
        added.clear();
        removed.clear();
        updated.clear();
        for (auto& child : children) {
            child.clear();
        }
    }
};

// Lazy clearing is handled by checking delta_time_ before accessing delta_value_
```

### Composition Diagram

TSValue has four parallel Values with recursive schemas. For TSD[K, TS[V]]:

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
├── time_: tuple[engine_time_t, var_list] ◄──────┤
│   ├── container_time                           │
│   └── child_times[]     ──► per-slot times     │
│                         (var_list observes KS) │
│                                                │
├── observer_: tuple[ObserverList, var_list] ◄───┤
│   ├── container_observers                      │
│   └── child_observers[] ──► per-slot observers │
│                         (var_list observes KS) │
│                                                │
└── delta_value_: MapDelta ◄─────────────────────┘
    ├── added[]           // Slot indices
    ├── removed[]         // Slot indices
    ├── updated[]         // Slot indices
    └── children[]        // Per-slot child deltas
```

For TSS[T] (simpler - no per-element tracking):

```
TSValue (4 parallel Values)
│
├── value_: SetStorage
│   └── KeySet
│       └── observers_ ──────────────────────────┐
│                                                │
├── time_: engine_time_t      // Container time only
│                                                │
├── observer_: ObserverList   // Container-level only
│                                                │
└── delta_value_: SetDelta ◄─────────────────────┘
    ├── added[]
    └── removed[]
```

**Key Points:**
- `value_` = user data (MapStorage, SetStorage, Bundle, List, scalar)
- `time_` = recursive: `engine_time_t` or `tuple[engine_time_t, list[...]]` (also used for delta validity)
- `observer_` = recursive: `ObserverList` or `tuple[ObserverList, list[...]]`
- `delta_value_` = kind-specific delta tracking (only where TSS/TSD exist)

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
| Four parallel Values | Clean separation: value_, time_, observer_, delta_value_ each have distinct schema |
| Delta validity from time_ | Modifications bubble up through time_; no separate delta_time_ structure needed |
| Lazy clearing via time_ | No explicit begin_tick(); delta auto-clears when current_time > last_delta_clear_time_ |
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
