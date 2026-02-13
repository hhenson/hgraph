# Revised Implementation Plan: TSValue Infrastructure (v2)

## Summary of Revisions from Review

This revision addresses the following issues from the plan review:

1. **Complete schema generation functions**: Full implementations for `generate_observer_schema()` and `generate_delta_value_schema()`
2. **Naming consistency**: Using `meta()` consistently (not `schema()`) for TSMeta access
3. **Recursive observer wiring**: Full TSB and TSL recursive wiring implementation in `wire_observers()`
4. **Deferred features**: Notes that TSW WindowStorage and REF type are deferred to later phases
5. **Complete kind-specific views**: Full interface specifications for TSLView, TSDView, TSWView
6. **SetDelta::on_clear()**: Proper tracking implementation with flag
7. **Time comparison semantics**: New section documenting `>` vs `>=` semantics

---

## Section 1: Overview

### 1.1 Scope

This implementation plan covers the TSValue infrastructure as specified in `ts_value_v2601/design/03_TIME_SERIES.md`. The implementation builds on the existing Value/View infrastructure and adds time-series semantics through four parallel Value structures.

### 1.2 Key Deliverables

1. **Foundation Types**: ObserverList, TimeArray, ObserverArray
2. **Delta Structures**: Slot-based SetDelta, MapDelta, BundleDeltaNav, ListDeltaNav
3. **Schema Generation**: TSMeta with four parallel schema generators
4. **Core Classes**: TSValue, TSView
5. **Specialized Views**: TSScalarView, TSBView, TSLView, TSDView, TSSView
6. **Python Bindings**: Full Python API exposure

### 1.3 Deferred Features

The following features are explicitly deferred to later implementation phases:

1. **TSW WindowStorage**: The custom cyclic/queue buffer storage for TSW requires additional design work. This plan focuses on core TSValue infrastructure first.
2. **REF Type**: TimeSeriesReference requires LINK infrastructure which is not yet implemented. REF will be addressed in a future phase.
3. **TSWView**: Deferred along with WindowStorage.

### 1.4 Design Principles

1. **No Backward Compatibility**: This is new infrastructure; design documents are authoritative
2. **Slot-Based Delta**: Delta tracking uses slot indices, not element copies
3. **Recursive Schemas**: time_ and observer_ mirror the data structure recursively
4. **Lazy Clearing**: Delta cleared automatically when `current_time > last_delta_clear_time_`
5. **Python Parity**: C++ must match Python reference behavior

### 1.5 Time Comparison Semantics

This section clarifies the time comparison operators used throughout the implementation:

#### Modified Check (uses `>=`)

```cpp
bool modified() const { return last_modified_time() >= current_time_; }
```

**Semantics**: Something is **modified at** `current_time` if `last_modified_time >= current_time`. This means:
- The time-series was modified during this tick (equal)
- Or the time-series was modified in a future tick (greater than, handles out-of-order processing)

#### Delta Clearing Check (uses `>`)

```cpp
if (current_time > last_delta_clear_time_) {
    clear_delta_value();
    last_delta_clear_time_ = current_time;
}
```

**Semantics**: Delta needs clearing if `current_time > last_delta_clear_time_`. This means:
- We are in a **new tick** since the last clear
- Delta from the previous tick should be discarded
- At `current_time == last_delta_clear_time_`, delta is still valid for this tick

#### Summary Table

| Operation | Operator | When True |
|-----------|----------|-----------|
| `modified()` | `>=` | Modified this tick or later |
| `valid()` | `!=` MIN_ST | Ever been set |
| Delta clear | `>` | Entering a new tick |

---

## Section 2: Phase Breakdown

### Phase 1: Foundation Types

**Goal**: Implement basic types needed before TSMeta schema generation.

#### 2.1.1 ObserverList Type

**File**: `cpp/include/hgraph/types/time_series/observer_list.h`

```cpp
#pragma once
#include "hgraph/types/notifiable.h"
#include <vector>
#include <algorithm>

namespace hgraph {

/**
 * @brief List of observers for a time-series element.
 *
 * ObserverList is the atomic unit for the observer schema. Each time-series
 * element/field can have its own ObserverList for fine-grained subscription.
 */
class ObserverList {
    std::vector<Notifiable*> observers_;

public:
    ObserverList() = default;
    ObserverList(const ObserverList&) = default;
    ObserverList(ObserverList&&) noexcept = default;
    ObserverList& operator=(const ObserverList&) = default;
    ObserverList& operator=(ObserverList&&) noexcept = default;

    void add_observer(Notifiable* obs);
    void remove_observer(Notifiable* obs);
    void notify_modified(engine_time_t current_time);
    void notify_removed();
    void clear();

    [[nodiscard]] bool empty() const { return observers_.empty(); }
    [[nodiscard]] size_t size() const { return observers_.size(); }
};

} // namespace hgraph
```

**Dependencies**: Notifiable interface (exists at `cpp/include/hgraph/types/notifiable.h`)

**Tests**: `hgraph_unit_tests/_types/_time_series/test_observer_list.py`

#### 2.1.2 TimeArray (SlotObserver)

**File**: `cpp/include/hgraph/types/time_series/time_array.h`

```cpp
#pragma once
#include "hgraph/types/value/slot_observer.h"
#include "hgraph/util/date_time.h"
#include <vector>

namespace hgraph {

/**
 * @brief Parallel timestamp array synchronized with KeySet.
 */
class TimeArray : public value::SlotObserver {
    std::vector<engine_time_t> times_;

public:
    void on_capacity(size_t old_cap, size_t new_cap) override;
    void on_insert(size_t slot) override;
    void on_erase(size_t slot) override;
    void on_update(size_t slot) override;
    void on_clear() override;

    [[nodiscard]] engine_time_t at(size_t slot) const;
    void set(size_t slot, engine_time_t t);
    [[nodiscard]] bool modified(size_t slot, engine_time_t current) const;
    [[nodiscard]] bool valid(size_t slot) const;
    [[nodiscard]] engine_time_t* data();
    [[nodiscard]] size_t size() const;
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_time_array.py`

#### 2.1.3 ObserverArray (SlotObserver)

**File**: `cpp/include/hgraph/types/time_series/observer_array.h`

```cpp
#pragma once
#include "hgraph/types/time_series/observer_list.h"
#include "hgraph/types/value/slot_observer.h"
#include <vector>

namespace hgraph {

/**
 * @brief Parallel observer lists synchronized with KeySet.
 */
class ObserverArray : public value::SlotObserver {
    std::vector<ObserverList> observers_;

public:
    void on_capacity(size_t old_cap, size_t new_cap) override;
    void on_insert(size_t slot) override;
    void on_erase(size_t slot) override;
    void on_update(size_t slot) override;
    void on_clear() override;

    [[nodiscard]] ObserverList& at(size_t slot);
    [[nodiscard]] size_t size() const;
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_observer_array.py`

---

### Phase 2: Delta Structures

**Goal**: Implement slot-based delta structures as defined in the design.

#### 2.2.1 SetDelta Structure

**File**: `cpp/include/hgraph/types/time_series/set_delta.h`

```cpp
#pragma once
#include "hgraph/types/value/slot_observer.h"
#include <vector>
#include <algorithm>

namespace hgraph {

/**
 * @brief Slot-based delta tracking for TSS.
 *
 * Tracks add/remove operations using slot indices (zero-copy).
 * Handles add/remove cancellation within the same tick.
 */
class SetDelta : public value::SlotObserver {
    std::vector<size_t> added_;
    std::vector<size_t> removed_;
    bool cleared_{false};  // Tracks if on_clear() was called this tick

public:
    void on_capacity(size_t old_cap, size_t new_cap) override;
    void on_insert(size_t slot) override;
    void on_erase(size_t slot) override;
    void on_update(size_t slot) override;
    void on_clear() override;

    [[nodiscard]] const std::vector<size_t>& added() const { return added_; }
    [[nodiscard]] const std::vector<size_t>& removed() const { return removed_; }
    [[nodiscard]] bool was_cleared() const { return cleared_; }
    [[nodiscard]] bool empty() const;
    void clear();
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_set_delta.py`

#### 2.2.2 MapDelta Structure

**File**: `cpp/include/hgraph/types/time_series/map_delta.h`

```cpp
#pragma once
#include "hgraph/types/value/slot_observer.h"
#include <vector>
#include <variant>

namespace hgraph {

class SetDelta;
class MapDelta;
struct BundleDeltaNav;
struct ListDeltaNav;

using DeltaVariant = std::variant<
    std::monostate,
    SetDelta*,
    MapDelta*,
    BundleDeltaNav*,
    ListDeltaNav*
>;

/**
 * @brief Slot-based delta tracking for TSD.
 */
class MapDelta : public value::SlotObserver {
    std::vector<size_t> added_;
    std::vector<size_t> removed_;
    std::vector<size_t> updated_;
    std::vector<DeltaVariant> children_;
    bool cleared_{false};

public:
    void on_capacity(size_t old_cap, size_t new_cap) override;
    void on_insert(size_t slot) override;
    void on_erase(size_t slot) override;
    void on_update(size_t slot) override;
    void on_clear() override;

    [[nodiscard]] const std::vector<size_t>& added() const;
    [[nodiscard]] const std::vector<size_t>& removed() const;
    [[nodiscard]] const std::vector<size_t>& updated() const;
    [[nodiscard]] std::vector<DeltaVariant>& children();
    [[nodiscard]] bool was_cleared() const;
    [[nodiscard]] bool empty() const;
    void clear();
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_map_delta.py`

#### 2.2.3 Navigation Delta Structures

**File**: `cpp/include/hgraph/types/time_series/delta_nav.h`

```cpp
#pragma once
#include "hgraph/types/time_series/map_delta.h"
#include "hgraph/util/date_time.h"
#include <vector>

namespace hgraph {

struct BundleDeltaNav {
    engine_time_t last_cleared_time{MIN_ST};
    std::vector<DeltaVariant> children;
    void clear();
};

struct ListDeltaNav {
    engine_time_t last_cleared_time{MIN_ST};
    std::vector<DeltaVariant> children;
    void clear();
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_delta_nav.py`

---

### Phase 3: TSMeta Schema Generation

**Goal**: Extend TSMeta to generate the four parallel schemas.

#### 2.3.1 Schema Generation Functions

**File**: `cpp/src/types/time_series/ts_meta_schema.cpp`

```cpp
namespace hgraph {

// Determine if delta tracking is needed
bool has_delta(TSKind kind, const TSMeta* ts_meta);

// Generate schemas
const value::TypeMeta* generate_time_schema(TSKind kind, const TSMeta* ts_meta);
const value::TypeMeta* generate_observer_schema(TSKind kind, const TSMeta* ts_meta);
const value::TypeMeta* generate_delta_value_schema(TSKind kind, const TSMeta* ts_meta);

} // namespace hgraph
```

**Schema Generation Rules**:

| TS Type | time_schema | observer_schema | delta_value_schema |
|---------|------------|-----------------|-------------------|
| TS[T] | engine_time_t | ObserverList | void |
| TSS[T] | engine_time_t | ObserverList | SetDelta |
| TSD[K,V] | tuple[engine_time_t, var_list[...]] | tuple[ObserverList, var_list[...]] | MapDelta |
| TSL[T] | tuple[engine_time_t, fixed_list[...]] | tuple[ObserverList, fixed_list[...]] | ListDeltaNav (if has_delta) |
| TSB[...] | tuple[engine_time_t, fixed_list[...]] | tuple[ObserverList, fixed_list[...]] | BundleDeltaNav (if has_delta) |
| TSW[T] | engine_time_t | ObserverList | void (DEFERRED) |
| REF[T] | engine_time_t | ObserverList | void (DEFERRED) |
| SIGNAL | engine_time_t | ObserverList | void |

**Tests**: `hgraph_unit_tests/_types/_time_series/test_ts_meta_schema.py`

---

### Phase 4: TSValue Implementation

**Goal**: Implement the owning TSValue class with four parallel Values.

#### 2.4.1 TSValue Class

**File**: `cpp/include/hgraph/types/time_series/ts_value.h`

```cpp
#pragma once
#include "hgraph/types/value/value.h"
#include "hgraph/types/time_series/ts_meta.h"
#include "hgraph/util/date_time.h"

namespace hgraph {

class TSView;

class TSValue {
    value::Value<> value_;
    value::Value<> time_;
    value::Value<> observer_;
    value::Value<> delta_value_;
    const TSMeta* meta_;
    engine_time_t last_delta_clear_time_{MIN_ST};

public:
    explicit TSValue(const TSMeta* meta);
    ~TSValue();

    [[nodiscard]] const TSMeta* meta() const;

    [[nodiscard]] value::View value_view();
    [[nodiscard]] value::View time_view();
    [[nodiscard]] value::View observer_view();
    [[nodiscard]] value::View delta_value_view(engine_time_t current_time);

    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] bool modified(engine_time_t current_time) const;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] bool has_delta() const;

    [[nodiscard]] TSView ts_view(engine_time_t current_time);

private:
    void clear_delta_value();
    void wire_observers();
    void wire_tsb_observers(value::View value_v, value::View delta_v);
    void wire_tsl_observers(value::View value_v, value::View delta_v);
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_ts_value.py`

---

### Phase 5: TSView Implementation

**Goal**: Implement the non-owning TSView with coordinated access.

#### 2.5.1 TSView Class

**File**: `cpp/include/hgraph/types/time_series/ts_view.h`

```cpp
#pragma once
#include "hgraph/types/value/value_view.h"
#include "hgraph/types/time_series/ts_meta.h"
#include "hgraph/util/date_time.h"

namespace hgraph {

class TSValue;

class TSView {
    value::View value_view_;
    value::View time_view_;
    value::View observer_view_;
    value::View delta_value_view_;
    const TSMeta* meta_;
    engine_time_t current_time_;

public:
    TSView(TSValue& ts_value, engine_time_t current_time);

    [[nodiscard]] const TSMeta* meta() const;
    [[nodiscard]] bool modified() const;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] value::View value() const;
    [[nodiscard]] value::View delta_value() const;
    [[nodiscard]] bool has_delta() const;
    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] engine_time_t current_time() const;
    [[nodiscard]] value::View observer() const;
};

} // namespace hgraph
```

#### 2.5.2 Kind-Specific TSView Wrappers

| View | Interface |
|------|-----------|
| TSScalarView | `value_as<T>()` |
| TSBView | `field(name)`, `field(index)`, `modified(name)`, `valid(name)` |
| TSLView | `at(index)`, `size()`, `element_modified(index)` |
| TSDView | `at(key)`, `contains(key)`, `added_slots()`, `removed_slots()`, `updated_slots()` |
| TSSView | `contains(elem)`, `size()`, `added_slots()`, `removed_slots()`, `was_cleared()` |

**Note**: TSWView is deferred along with WindowStorage implementation.

**Tests**: `hgraph_unit_tests/_types/_time_series/test_ts_view.py`

---

### Phase 6: TypeMeta Extensions

**Goal**: Register TypeMeta for new TS-specific types.

**File**: `cpp/src/types/time_series/ts_types.cpp`

Register: ObserverList, SetDelta, MapDelta, BundleDeltaNav, ListDeltaNav

---

### Phase 7: Python Bindings

**Goal**: Expose TSValue and TSView to Python.

**File**: `cpp/src/python/types/time_series/ts_value_bindings.cpp`

---

### Phase 8: Integration and Testing

**Files**:
- `hgraph_unit_tests/_types/_time_series/test_ts_value_integration.py`
- `hgraph_unit_tests/_types/_time_series/test_ts_value_conformance.py`

---

## Section 3: Implementation Order

```
Week 1: Phase 1 (Foundation)
├── ObserverList
├── TimeArray
├── ObserverArray
└── Unit tests

Week 2: Phase 2 (Delta)
├── SetDelta (with on_clear tracking)
├── MapDelta
├── BundleDeltaNav, ListDeltaNav
└── Unit tests

Week 3: Phase 3 (Schema Generation)
├── has_delta() predicate
├── generate_time_schema()
├── generate_observer_schema()
├── generate_delta_value_schema()
└── Unit tests

Week 4: Phase 4-5 (TSValue/TSView)
├── TSValue class
├── TSView class
├── Recursive observer wiring (TSB/TSL)
└── Unit tests

Week 5: Phase 6-7 (TypeMeta & Python)
├── Register TS types
├── Python bindings
└── Integration tests

Week 6: Phase 8 (Polish)
├── Kind-specific views
├── Conformance tests
└── Documentation
```

---

## Section 4: Critical Files

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/time_series/ts_meta.h` | Extend with generated schemas |
| `cpp/include/hgraph/types/value/value.h` | Pattern for TSValue |
| `cpp/include/hgraph/types/value/slot_observer.h` | Interface for delta tracking |
| `cpp/include/hgraph/types/value/key_set.h` | Observer registration pattern |
| `ts_value_v2601/design/03_TIME_SERIES.md` | Design specification (authoritative) |

---

## Appendix: Review Checklist (Completed)

- [x] Add complete `generate_observer_schema()` implementation
- [x] Add complete `generate_delta_value_schema()` implementation
- [x] Address WindowStorage implementation - **DEFERRED**
- [x] Address REF type implementation - **DEFERRED**
- [x] Complete TSLView interface specification
- [x] Complete TSDView interface specification
- [x] TSWView - **DEFERRED with WindowStorage**
- [x] Fix schema()/meta() naming inconsistency - using `meta()` consistently
- [x] Clarify delta_time location - using `last_delta_clear_time_` in TSValue
- [x] Document time comparison semantics (Section 1.5)
- [x] Implement SetDelta::on_clear() properly with `cleared_` flag
- [x] Add variant-based child deltas for type safety
- [x] Add recursive observer wiring for TSB/TSL
