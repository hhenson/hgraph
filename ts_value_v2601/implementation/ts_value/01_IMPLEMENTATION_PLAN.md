# Implementation Plan: TSValue Infrastructure

## Section 1: Overview

### 1.1 Scope

This implementation plan covers the TSValue infrastructure as specified in `ts_value_v2601/design/03_TIME_SERIES.md`. The implementation builds on the existing Value/View infrastructure and adds time-series semantics through four parallel Value structures.

### 1.2 Key Deliverables

1. **Foundation Types**: ObserverList, TimeArray, ObserverArray
2. **Delta Structures**: Slot-based SetDelta, MapDelta, BundleDeltaNav, ListDeltaNav
3. **Schema Generation**: TSMeta with four parallel schema generators
4. **Core Classes**: TSValue, TSView
5. **Specialized Views**: TSScalarView, TSBView, TSLView, TSDView, TSSView, TSWView
6. **Python Bindings**: Full Python API exposure

### 1.3 Design Principles

1. **No Backward Compatibility**: This is new infrastructure; design documents are authoritative
2. **Slot-Based Delta**: Delta tracking uses slot indices, not element copies
3. **Recursive Schemas**: time_ and observer_ mirror the data structure recursively
4. **Lazy Clearing**: Delta cleared automatically when `current_time > last_delta_clear_time_`
5. **Python Parity**: C++ must match Python reference behavior

---

## Section 2: Phase Breakdown

### Phase 1: Foundation Types

**Goal**: Implement basic types needed before TSMeta schema generation.

#### 2.1.1 ObserverList Type

**File**: `cpp/include/hgraph/types/time_series/observer_list.h`

```cpp
#pragma once
#include <vector>
#include <algorithm>

namespace hgraph {

class Notifiable;  // Forward declaration

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

    void add_observer(Notifiable* obs) {
        if (obs && std::find(observers_.begin(), observers_.end(), obs) == observers_.end()) {
            observers_.push_back(obs);
        }
    }

    void remove_observer(Notifiable* obs) {
        observers_.erase(std::remove(observers_.begin(), observers_.end(), obs), observers_.end());
    }

    void notify_modified();  // Implemented in .cpp
    void notify_removed();   // Implemented in .cpp

    void clear() { observers_.clear(); }

    [[nodiscard]] bool empty() const { return observers_.empty(); }
    [[nodiscard]] size_t size() const { return observers_.size(); }
};

} // namespace hgraph
```

**Dependencies**: Notifiable interface (exists in time-series framework)

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
 *
 * TimeArray maintains per-slot modification timestamps for TSD.
 * Implements SlotObserver to stay synchronized with KeySet operations.
 */
class TimeArray : public value::SlotObserver {
    std::vector<engine_time_t> times_;

public:
    // SlotObserver interface
    void on_capacity(size_t /*old_cap*/, size_t new_cap) override {
        times_.resize(new_cap, MIN_ST);
    }

    void on_insert(size_t slot) override {
        if (slot < times_.size()) {
            times_[slot] = MIN_ST;  // Invalid until set
        }
    }

    void on_erase(size_t slot) override {
        // Keep time (may be queried for delta)
        // Optionally: times_[slot] = MIN_ST;
    }

    void on_update(size_t /*slot*/) override {
        // Time update handled externally
    }

    void on_clear() override {
        std::fill(times_.begin(), times_.end(), MIN_ST);
    }

    // Time access
    [[nodiscard]] engine_time_t at(size_t slot) const {
        return slot < times_.size() ? times_[slot] : MIN_ST;
    }

    void set(size_t slot, engine_time_t t) {
        if (slot < times_.size()) {
            times_[slot] = t;
        }
    }

    [[nodiscard]] bool modified(size_t slot, engine_time_t current) const {
        return at(slot) >= current;
    }

    [[nodiscard]] bool valid(size_t slot) const {
        return at(slot) != MIN_ST;
    }

    // Toll-free numpy/Arrow access
    [[nodiscard]] engine_time_t* data() { return times_.data(); }
    [[nodiscard]] const engine_time_t* data() const { return times_.data(); }
    [[nodiscard]] size_t size() const { return times_.size(); }
};

} // namespace hgraph
```

**Dependencies**: SlotObserver, engine_time_t (both exist)

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
 *
 * ObserverArray maintains per-slot observer lists for TSD.
 * Enables fine-grained subscription to individual map entries.
 */
class ObserverArray : public value::SlotObserver {
    std::vector<ObserverList> observers_;

public:
    void on_capacity(size_t /*old_cap*/, size_t new_cap) override {
        observers_.resize(new_cap);
    }

    void on_insert(size_t slot) override {
        if (slot < observers_.size()) {
            observers_[slot].clear();
        }
    }

    void on_erase(size_t slot) override {
        if (slot < observers_.size()) {
            observers_[slot].notify_removed();
            observers_[slot].clear();
        }
    }

    void on_update(size_t /*slot*/) override {
        // Observer notification handled externally
    }

    void on_clear() override {
        for (auto& obs : observers_) {
            obs.notify_removed();
            obs.clear();
        }
    }

    [[nodiscard]] ObserverList& at(size_t slot) { return observers_[slot]; }
    [[nodiscard]] const ObserverList& at(size_t slot) const { return observers_[slot]; }
    [[nodiscard]] size_t size() const { return observers_.size(); }
};

} // namespace hgraph
```

**Dependencies**: ObserverList, SlotObserver

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
 * SetDelta tracks add/remove operations using slot indices (zero-copy).
 * Implements SlotObserver to receive notifications from KeySet.
 * Handles add/remove cancellation within the same tick.
 */
class SetDelta : public value::SlotObserver {
    std::vector<size_t> added_;
    std::vector<size_t> removed_;

public:
    // SlotObserver interface with add/remove cancellation
    void on_capacity(size_t /*old_cap*/, size_t /*new_cap*/) override {}

    void on_insert(size_t slot) override {
        // If was removed this tick, cancel out; else add to added
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            removed_.erase(it);
        } else {
            added_.push_back(slot);
        }
    }

    void on_erase(size_t slot) override {
        // If was added this tick, cancel out; else add to removed
        auto it = std::find(added_.begin(), added_.end(), slot);
        if (it != added_.end()) {
            added_.erase(it);
        } else {
            removed_.push_back(slot);
        }
    }

    void on_update(size_t /*slot*/) override {
        // TSS doesn't track updates (elements are immutable)
    }

    void on_clear() override {
        // All existing elements removed - but we don't track all slots
        // This is called when the underlying KeySet is cleared
        // For delta purposes, this should be handled at a higher level
    }

    // Access
    [[nodiscard]] const std::vector<size_t>& added() const { return added_; }
    [[nodiscard]] const std::vector<size_t>& removed() const { return removed_; }
    [[nodiscard]] bool empty() const { return added_.empty() && removed_.empty(); }

    // Lifecycle
    void clear() {
        added_.clear();
        removed_.clear();
    }
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
#include <algorithm>
#include <memory>
#include <functional>

namespace hgraph {

// Forward declarations for child delta types
class SetDelta;
class MapDelta;
struct BundleDeltaNav;
struct ListDeltaNav;

// Type-erased delta with custom deleter
using DeltaPtr = std::unique_ptr<void, std::function<void(void*)>>;

/**
 * @brief Slot-based delta tracking for TSD.
 *
 * MapDelta tracks add/remove/update operations using slot indices.
 * Also maintains child deltas for nested TSS/TSD values.
 */
class MapDelta : public value::SlotObserver {
    std::vector<size_t> added_;
    std::vector<size_t> removed_;
    std::vector<size_t> updated_;
    std::vector<DeltaPtr> children_;  // Per-slot child deltas (may be null)

public:
    void on_capacity(size_t /*old_cap*/, size_t new_cap) override {
        children_.resize(new_cap);
    }

    void on_insert(size_t slot) override {
        auto it = std::find(removed_.begin(), removed_.end(), slot);
        if (it != removed_.end()) {
            removed_.erase(it);
        } else {
            added_.push_back(slot);
        }
    }

    void on_erase(size_t slot) override {
        auto it = std::find(added_.begin(), added_.end(), slot);
        if (it != added_.end()) {
            added_.erase(it);
        } else {
            removed_.push_back(slot);
        }
        // Also remove from updated if present
        updated_.erase(std::remove(updated_.begin(), updated_.end(), slot), updated_.end());
    }

    void on_update(size_t slot) override {
        // Only track as updated if not already in added
        if (std::find(added_.begin(), added_.end(), slot) == added_.end()) {
            if (std::find(updated_.begin(), updated_.end(), slot) == updated_.end()) {
                updated_.push_back(slot);
            }
        }
    }

    void on_clear() override {
        // Higher-level handling needed
    }

    // Access
    [[nodiscard]] const std::vector<size_t>& added() const { return added_; }
    [[nodiscard]] const std::vector<size_t>& removed() const { return removed_; }
    [[nodiscard]] const std::vector<size_t>& updated() const { return updated_; }
    [[nodiscard]] const std::vector<DeltaPtr>& children() const { return children_; }
    [[nodiscard]] std::vector<DeltaPtr>& children() { return children_; }
    [[nodiscard]] bool empty() const {
        return added_.empty() && removed_.empty() && updated_.empty();
    }

    // Child delta access
    void set_child(size_t slot, DeltaPtr delta) {
        if (slot < children_.size()) {
            children_[slot] = std::move(delta);
        }
    }

    // Clear
    void clear();  // Implemented in .cpp - clears all vectors and child deltas
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_map_delta.py`

#### 2.2.3 Navigation Delta Structures

**File**: `cpp/include/hgraph/types/time_series/delta_nav.h`

```cpp
#pragma once
#include "hgraph/util/date_time.h"
#include <vector>
#include <memory>
#include <functional>

namespace hgraph {

using DeltaPtr = std::unique_ptr<void, std::function<void(void*)>>;

/**
 * @brief Navigation structure for TSB containing TSS/TSD.
 *
 * BundleDeltaNav routes to child deltas in bundle fields.
 * Only exists when has_delta(TSB) is true.
 */
struct BundleDeltaNav {
    engine_time_t last_cleared_time{MIN_ST};  // Optional optimization
    std::vector<DeltaPtr> children;            // Per-field child deltas

    void clear() {
        for (auto& child : children) {
            if (child) {
                // Type-specific clearing handled by deleter/clear function
            }
        }
    }
};

/**
 * @brief Navigation structure for TSL containing TSS/TSD.
 *
 * ListDeltaNav routes to child deltas in list elements.
 * Only exists when has_delta(TSL) is true.
 */
struct ListDeltaNav {
    engine_time_t last_cleared_time{MIN_ST};  // Optional optimization
    std::vector<DeltaPtr> children;            // Per-element child deltas

    void clear() {
        for (auto& child : children) {
            if (child) {
                // Type-specific clearing handled by deleter/clear function
            }
        }
    }
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_delta_nav.py`

---

### Phase 3: TSMeta Schema Generation

**Goal**: Extend TSMeta to generate the four parallel schemas.

#### 2.3.1 TSMeta Extension

**File**: Modify `cpp/include/hgraph/types/time_series/ts_meta.h`

Add new members to TSMeta:

```cpp
struct TSMeta {
    // ... existing members (TSKind, etc.) ...

    // Generated parallel schemas
    const value::TypeMeta* value_schema_{nullptr};
    const value::TypeMeta* time_meta_{nullptr};
    const value::TypeMeta* observer_meta_{nullptr};
    const value::TypeMeta* delta_value_meta_{nullptr};  // nullptr if !has_delta()

    // Accessors
    [[nodiscard]] const value::TypeMeta* value_schema() const { return value_schema_; }
    [[nodiscard]] const value::TypeMeta* time_meta() const { return time_meta_; }
    [[nodiscard]] const value::TypeMeta* observer_meta() const { return observer_meta_; }
    [[nodiscard]] const value::TypeMeta* delta_value_meta() const { return delta_value_meta_; }

    // Query
    [[nodiscard]] bool has_delta() const { return delta_value_meta_ != nullptr; }
};
```

#### 2.3.2 Schema Generation Functions

**File**: `cpp/src/types/time_series/ts_meta_schema.cpp`

```cpp
namespace hgraph {

// Determine if delta tracking is needed
bool has_delta(TSKind kind, const TSMeta* ts_meta) {
    switch (kind) {
        case TSKind::TS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return false;
        case TSKind::TSS:
        case TSKind::TSD:
            return true;
        case TSKind::TSB:
            // True if any field has delta
            for (const auto& field : ts_meta->fields()) {
                if (has_delta(field.ts_meta->kind(), field.ts_meta)) {
                    return true;
                }
            }
            return false;
        case TSKind::TSL:
            // True if element type has delta
            return has_delta(ts_meta->element_ts_meta()->kind(), ts_meta->element_ts_meta());
    }
    return false;
}

// Generate time schema (recursive)
const value::TypeMeta* generate_time_schema(TSKind kind, const TSMeta* ts_meta) {
    auto& reg = value::TypeRegistry::instance();

    switch (kind) {
        case TSKind::TS:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return value::scalar_type_meta<engine_time_t>();

        case TSKind::TSB: {
            // tuple[engine_time_t, fixed_list[time_schema(field_i), ...]]
            auto builder = reg.tuple();
            builder.element(value::scalar_type_meta<engine_time_t>());

            auto list_builder = reg.tuple();  // Fixed list as tuple
            for (const auto& field : ts_meta->fields()) {
                list_builder.element(generate_time_schema(field.ts_meta->kind(), field.ts_meta));
            }
            builder.element(list_builder.build());
            return builder.build();
        }

        case TSKind::TSL: {
            // tuple[engine_time_t, fixed_list[time_schema(T), N]]
            auto builder = reg.tuple();
            builder.element(value::scalar_type_meta<engine_time_t>());

            auto element_time = generate_time_schema(
                ts_meta->element_ts_meta()->kind(),
                ts_meta->element_ts_meta()
            );
            // Fixed-size list of element times
            builder.element(reg.list(element_time, ts_meta->size()).build());
            return builder.build();
        }

        case TSKind::TSD: {
            // tuple[engine_time_t, var_list[time_schema(V)]]
            auto builder = reg.tuple();
            builder.element(value::scalar_type_meta<engine_time_t>());

            auto value_time = generate_time_schema(
                ts_meta->value_ts_meta()->kind(),
                ts_meta->value_ts_meta()
            );
            // Variable-size list (grows with map)
            builder.element(reg.list(value_time).build());  // Unbounded list
            return builder.build();
        }
    }
    return nullptr;
}

// Similar implementations for:
// const value::TypeMeta* generate_observer_schema(TSKind kind, const TSMeta* ts_meta);
// const value::TypeMeta* generate_delta_value_schema(TSKind kind, const TSMeta* ts_meta);

} // namespace hgraph
```

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

class TSView;  // Forward declaration

/**
 * @brief Owning container for time-series data with four parallel Value structures.
 *
 * TSValue maintains:
 * - value_: User-visible data
 * - time_: Modification timestamps (recursive)
 * - observer_: Observer lists (recursive)
 * - delta_value_: Delta tracking data (only where TSS/TSD exist)
 */
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

    // Non-copyable, movable
    TSValue(const TSValue&) = delete;
    TSValue& operator=(const TSValue&) = delete;
    TSValue(TSValue&&) noexcept = default;
    TSValue& operator=(TSValue&&) noexcept = default;

    // Schema access
    [[nodiscard]] const TSMeta* meta() const { return meta_; }

    // Data access
    [[nodiscard]] value::View value_view();
    [[nodiscard]] value::ConstView value_view() const;

    // Time access
    [[nodiscard]] value::View time_view();
    [[nodiscard]] value::ConstView time_view() const;
    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] bool modified(engine_time_t current_time) const;
    [[nodiscard]] bool valid() const;

    // Observer access
    [[nodiscard]] value::View observer_view();
    [[nodiscard]] value::ConstView observer_view() const;

    // Delta access (with lazy clearing)
    [[nodiscard]] value::View delta_value_view(engine_time_t current_time);
    [[nodiscard]] bool has_delta() const;

    // TSView creation
    [[nodiscard]] TSView ts_view(engine_time_t current_time);

private:
    void clear_delta_value();
    void wire_observers();  // Connect delta observers to KeySets
};

} // namespace hgraph
```

**Implementation** (`cpp/src/types/time_series/ts_value.cpp`):

```cpp
#include "hgraph/types/time_series/ts_value.h"
#include "hgraph/types/time_series/ts_view.h"
#include "hgraph/types/time_series/set_delta.h"
#include "hgraph/types/time_series/map_delta.h"

namespace hgraph {

TSValue::TSValue(const TSMeta* meta)
    : value_(meta->value_schema())
    , time_(meta->time_meta())
    , observer_(meta->observer_meta())
    , delta_value_(meta->delta_value_meta())  // May be void schema
    , meta_(meta)
{
    wire_observers();
}

TSValue::~TSValue() = default;

value::View TSValue::value_view() { return value_.view(); }
value::ConstView TSValue::value_view() const { return value_.view(); }

value::View TSValue::time_view() { return time_.view(); }
value::ConstView TSValue::time_view() const { return time_.view(); }

engine_time_t TSValue::last_modified_time() const {
    // Container-level time is first element of tuple, or direct engine_time_t
    auto tv = time_.view();
    if (tv.schema()->kind() == value::TypeKind::Tuple) {
        return tv.as_tuple()[0].as<engine_time_t>();
    }
    return tv.as<engine_time_t>();
}

bool TSValue::modified(engine_time_t current_time) const {
    return last_modified_time() >= current_time;
}

bool TSValue::valid() const {
    return last_modified_time() != MIN_ST;
}

value::View TSValue::observer_view() { return observer_.view(); }
value::ConstView TSValue::observer_view() const { return observer_.view(); }

value::View TSValue::delta_value_view(engine_time_t current_time) {
    if (current_time > last_delta_clear_time_) {
        clear_delta_value();
        last_delta_clear_time_ = current_time;
    }
    return delta_value_.view();
}

bool TSValue::has_delta() const {
    return meta_->has_delta();
}

TSView TSValue::ts_view(engine_time_t current_time) {
    return TSView(*this, current_time);
}

void TSValue::clear_delta_value() {
    if (!has_delta()) return;

    // Type-specific clearing based on TS kind
    switch (meta_->kind()) {
        case TSKind::TSS:
            delta_value_.view().as<SetDelta>().clear();
            break;
        case TSKind::TSD:
            delta_value_.view().as<MapDelta>().clear();
            break;
        case TSKind::TSB:
        case TSKind::TSL:
            // Navigation structures - clear recursively
            // Implementation depends on delta_value_ schema structure
            break;
        default:
            break;
    }
}

void TSValue::wire_observers() {
    if (!has_delta()) return;

    switch (meta_->kind()) {
        case TSKind::TSS: {
            auto& ks = value_.view().as_set().storage().key_set();
            ks.add_observer(&delta_value_.view().as<SetDelta>());
            break;
        }
        case TSKind::TSD: {
            auto& ks = value_.view().as_map().storage().key_set();
            ks.add_observer(&delta_value_.view().as<MapDelta>());
            // Also wire time_ and observer_ var_lists if needed
            break;
        }
        default:
            // TSB and TSL need recursive wiring
            break;
    }
}

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

class TSValue;  // Forward declaration

/**
 * @brief Non-owning coordinated access to TSValue's four parallel structures.
 *
 * TSView provides a unified interface for accessing time-series data
 * with current time context for modification and delta queries.
 */
class TSView {
    value::View value_view_;
    value::View time_view_;
    value::View observer_view_;
    value::View delta_value_view_;
    const TSMeta* meta_;
    engine_time_t current_time_;

public:
    // Construction (handles lazy delta clearing)
    TSView(TSValue& ts_value, engine_time_t current_time);

    // Schema access
    [[nodiscard]] const TSMeta* meta() const { return meta_; }

    // State queries
    [[nodiscard]] bool modified() const;
    [[nodiscard]] bool valid() const;

    // Data access
    [[nodiscard]] value::View value() const { return value_view_; }

    // Delta access (already lazily cleared during construction)
    [[nodiscard]] value::View delta_value() const { return delta_value_view_; }
    [[nodiscard]] bool has_delta() const { return meta_->has_delta(); }

    // Time access
    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] engine_time_t current_time() const { return current_time_; }

    // Observer access
    [[nodiscard]] value::View observer() const { return observer_view_; }

    // Kind-specific view conversion
    // TSScalarView as_scalar() const;
    // TSBView as_bundle() const;
    // TSLView as_list() const;
    // TSDView as_dict() const;
    // TSSView as_set() const;
    // TSWView as_window() const;
};

} // namespace hgraph
```

**Tests**: `hgraph_unit_tests/_types/_time_series/test_ts_view.py`

#### 2.5.2 Kind-Specific TSView Wrappers

**Files**:
- `cpp/include/hgraph/types/time_series/ts_scalar_view.h`
- `cpp/include/hgraph/types/time_series/tsb_view.h`
- `cpp/include/hgraph/types/time_series/tsl_view.h`
- `cpp/include/hgraph/types/time_series/tsd_view.h`
- `cpp/include/hgraph/types/time_series/tss_view.h`
- `cpp/include/hgraph/types/time_series/tsw_view.h`

Example TSBView:

```cpp
class TSBView : public TSView {
public:
    using TSView::TSView;

    // Field access returns TSView, not View
    [[nodiscard]] TSView field(std::string_view name) const;
    [[nodiscard]] TSView field(size_t index) const;

    // Per-field modification
    [[nodiscard]] bool modified(std::string_view name) const;
    [[nodiscard]] bool valid(std::string_view name) const;
};
```

Example TSSView:

```cpp
class TSSView : public TSView {
public:
    using TSView::TSView;

    // Set operations
    [[nodiscard]] bool contains(value::ConstView elem) const;
    [[nodiscard]] size_t size() const;

    // Delta access (slot indices)
    [[nodiscard]] const std::vector<size_t>& added_slots() const;
    [[nodiscard]] const std::vector<size_t>& removed_slots() const;
};
```

---

### Phase 6: TypeMeta Extensions

**Goal**: Register TypeMeta for new TS-specific types.

**File**: `cpp/src/types/time_series/ts_types.cpp`

```cpp
#include "hgraph/types/value/type_registry.h"
#include "hgraph/types/time_series/observer_list.h"
#include "hgraph/types/time_series/set_delta.h"
#include "hgraph/types/time_series/map_delta.h"
#include "hgraph/types/time_series/delta_nav.h"

namespace hgraph {

void register_ts_types() {
    auto& reg = value::TypeRegistry::instance();

    // ObserverList
    reg.register_scalar<ObserverList>("ObserverList");

    // Delta types
    reg.register_scalar<SetDelta>("SetDelta");
    reg.register_scalar<MapDelta>("MapDelta");
    reg.register_scalar<BundleDeltaNav>("BundleDeltaNav");
    reg.register_scalar<ListDeltaNav>("ListDeltaNav");
}

} // namespace hgraph
```

---

### Phase 7: Python Bindings

**Goal**: Expose TSValue and TSView to Python.

**File**: `cpp/src/python/types/time_series/ts_value_bindings.cpp`

```cpp
#include <nanobind/nanobind.h>
#include "hgraph/types/time_series/ts_value.h"
#include "hgraph/types/time_series/ts_view.h"

namespace nb = nanobind;

void bind_ts_value(nb::module_& m) {
    nb::class_<hgraph::TSValue>(m, "TSValue")
        .def(nb::init<const hgraph::TSMeta*>())
        .def("meta", &hgraph::TSValue::meta, nb::rv_policy::reference)
        .def("value_view", nb::overload_cast<>(&hgraph::TSValue::value_view))
        .def("time_view", nb::overload_cast<>(&hgraph::TSValue::time_view))
        .def("modified", &hgraph::TSValue::modified)
        .def("valid", &hgraph::TSValue::valid)
        .def("has_delta", &hgraph::TSValue::has_delta)
        .def("ts_view", &hgraph::TSValue::ts_view);
}

void bind_ts_view(nb::module_& m) {
    nb::class_<hgraph::TSView>(m, "TSView")
        .def("meta", &hgraph::TSView::meta, nb::rv_policy::reference)
        .def("modified", &hgraph::TSView::modified)
        .def("valid", &hgraph::TSView::valid)
        .def("value", &hgraph::TSView::value)
        .def("delta_value", &hgraph::TSView::delta_value)
        .def("has_delta", &hgraph::TSView::has_delta)
        .def("last_modified_time", &hgraph::TSView::last_modified_time)
        .def("current_time", &hgraph::TSView::current_time);

    // Kind-specific views...
}
```

---

### Phase 8: Integration and Testing

**Goal**: Integration tests ensuring end-to-end functionality.

**Files**:
- `hgraph_unit_tests/_types/_time_series/test_ts_value_integration.py`
- `hgraph_unit_tests/_types/_time_series/test_ts_value_conformance.py`

---

## Section 3: Test Strategy

### 3.1 Unit Tests by Phase

| Phase | Test File | Coverage |
|-------|-----------|----------|
| 1 | `test_observer_list.py` | ObserverList add/remove/notify |
| 1 | `test_time_array.py` | TimeArray slot synchronization |
| 1 | `test_observer_array.py` | ObserverArray slot synchronization |
| 2 | `test_set_delta.py` | SetDelta add/remove cancellation |
| 2 | `test_map_delta.py` | MapDelta add/remove/update tracking |
| 2 | `test_delta_nav.py` | BundleDeltaNav/ListDeltaNav navigation |
| 3 | `test_ts_meta_schema.py` | Schema generation for all TS types |
| 4 | `test_ts_value.py` | TSValue construction, lazy clearing |
| 5 | `test_ts_view.py` | TSView state queries, access |

### 3.2 Integration Tests

| Test File | Coverage |
|-----------|----------|
| `test_ts_value_integration.py` | End-to-end TSValue lifecycle |
| `test_ts_value_conformance.py` | Match Python reference behavior |

---

## Section 4: Implementation Order

```
Week 1: Phase 1 (Foundation)
├── ObserverList
├── TimeArray
├── ObserverArray
└── Unit tests

Week 2: Phase 2 (Delta)
├── SetDelta
├── MapDelta
├── BundleDeltaNav, ListDeltaNav
└── Unit tests

Week 3: Phase 3 (Schema Generation)
├── TSMeta schema generators
├── has_delta() predicate
└── Unit tests

Week 4: Phase 4-5 (TSValue/TSView)
├── TSValue class
├── TSView class
├── Observer wiring
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

## Section 5: Critical Files

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/time_series/ts_meta.h` | Extend with generated schemas |
| `cpp/include/hgraph/types/value/value.h` | Pattern for TSValue |
| `cpp/include/hgraph/types/value/slot_observer.h` | Interface for delta tracking |
| `cpp/include/hgraph/types/value/key_set.h` | Observer registration pattern |
| `ts_value_v2601/design/03_TIME_SERIES.md` | Design specification (authoritative) |
