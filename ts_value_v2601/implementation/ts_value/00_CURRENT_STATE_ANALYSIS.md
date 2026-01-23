# Current State Analysis: TSValue Implementation

## Section 1: Overview

### 1.1 Purpose

This document analyzes the current state of TSValue-related infrastructure in the C++ codebase compared to the design specifications in `ts_value_v2601/design/03_TIME_SERIES.md`.

### 1.2 Summary

The C++ codebase has **no implementation** of the TSValue/TSView architecture described in the design document. However, substantial **foundational infrastructure** exists that can be leveraged.

---

## Section 2: Design Requirements

### 2.1 TSValue Structure (from design)

```cpp
class TSValue {
    Value value_;             // User-visible data
    Value time_;              // Modification timestamps (recursive)
    Value observer_;          // Observer lists (recursive)
    Value delta_value_;       // Delta tracking data (only where TSS/TSD exist)
    const TSMeta* meta_;
    engine_time_t last_delta_clear_time_{MIN_ENGINE_TIME};
};
```

### 2.2 TSMeta Schema Generation (from design)

TSMeta must generate **four parallel schemas** from a single TS type:

1. `value_schema_` - User-visible data schema
2. `time_meta_` - Recursive time schema (mirrors structure)
3. `observer_meta_` - Recursive observer schema (mirrors structure)
4. `delta_value_meta_` - Delta tracking schema (only where has_delta)

### 2.3 Schema Generation Rules

| TS Type | time_schema | observer_schema | delta_value_schema |
|---------|------------|-----------------|-------------------|
| TS[T] | engine_time_t | ObserverList | void |
| TSS[T] | engine_time_t | ObserverList | SetDelta |
| TSD[K,V] | tuple[engine_time_t, var_list[...]] | tuple[ObserverList, var_list[...]] | MapDelta |
| TSL[T] | tuple[engine_time_t, fixed_list[...]] | tuple[ObserverList, fixed_list[...]] | ListDeltaNav (if has_delta(T)) |
| TSB[...] | tuple[engine_time_t, fixed_list[...]] | tuple[ObserverList, fixed_list[...]] | BundleDeltaNav (if any field has_delta) |
| TSW[T] | engine_time_t | ObserverList | void |
| REF[T] | engine_time_t | ObserverList | void |
| SIGNAL | engine_time_t | ObserverList | void |

### 2.4 Delta Structures (from design)

- **SetDelta**: `{added[], removed[]}` - slot indices
- **MapDelta**: `{added[], removed[], updated[], children[]}` - slot indices + child deltas
- **BundleDeltaNav**: `{children[]}` - navigation to child deltas
- **ListDeltaNav**: `{children[]}` - navigation to child deltas

---

## Section 3: Existing Infrastructure

### 3.1 Complete and Ready to Use

| Component | File | Status | Notes |
|-----------|------|--------|-------|
| Value<Policy> | `types/value/value.h` | Ready | Can be used for value_, time_, observer_, delta_value_ |
| View | `types/value/value_view.h` | Ready | Non-owning type-erased view |
| TypeMeta | `types/value/type_meta.h` | Ready | Need to extend with TS-specific schema generation |
| KeySet | `types/value/key_set.h` | Ready | Slot-based key storage for TSD/TSS |
| SlotObserver | `types/value/slot_observer.h` | Ready | Observer protocol for parallel arrays |
| SetStorage/MapStorage | `types/value/set_storage.h`, `map_storage.h` | Ready | Value storage for TSS/TSD |
| ValueArray | `types/value/value_array.h` | Ready | Parallel value storage implementing SlotObserver |
| engine_time_t | `util/date_time.h` | Ready | Time type definitions |

### 3.2 Partially Exists (Needs Modification)

| Component | File | Issue |
|-----------|------|-------|
| Delta Storage | `types/value/delta_storage.h` | Stores element copies, not slot indices |
| Delta Views | `types/value/delta_view.h` | Based on element copies |
| SetDeltaValue | `types/value/set_delta_value.h` | Pattern exists but needs slot-based rewrite |

### 3.3 Missing Components

| Component | Purpose | Priority |
|-----------|---------|----------|
| TSMeta schema generation | Generate 4 parallel schemas from TS type | HIGH |
| TSValue class | Owning container for 4 parallel Values | HIGH |
| TSView class | Non-owning coordinated access | HIGH |
| ObserverList type | Atomic unit for observer schema | HIGH |
| Slot-based SetDelta | Track add/remove via slot indices | HIGH |
| Slot-based MapDelta | Track add/remove/update via slot indices | HIGH |
| BundleDeltaNav | Navigation structure for TSB with TSS/TSD | MEDIUM |
| ListDeltaNav | Navigation structure for TSL with TSS/TSD | MEDIUM |
| TimeArray | Parallel timestamps for TSD per-slot tracking | MEDIUM |
| ObserverArray | Parallel observer lists for TSD | MEDIUM |
| Kind-specific TSViews | TSBView, TSLView, TSDView, TSSView, TSWView | MEDIUM |

---

## Section 4: Key Files for Understanding

### 4.1 Core Value System

| File | Lines | Purpose |
|------|-------|---------|
| `cpp/include/hgraph/types/value/value.h` | ~775 | Owning Value class with policy extensions |
| `cpp/include/hgraph/types/value/value_view.h` | ~841 | Non-owning View class |
| `cpp/include/hgraph/types/value/type_meta.h` | ~521 | Type metadata system |
| `cpp/include/hgraph/types/value/policy.h` | ~315 | Policy-based extensions |

### 4.2 Storage Infrastructure

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/value/key_set.h` | Slot-based key storage |
| `cpp/include/hgraph/types/value/slot_observer.h` | Observer protocol |
| `cpp/include/hgraph/types/value/map_storage.h` | Map composition |
| `cpp/include/hgraph/types/value/value_array.h` | Parallel value storage |

### 4.3 Delta Infrastructure

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/types/value/delta_storage.h` | Delta storage structures |
| `cpp/include/hgraph/types/value/delta_view.h` | Delta view classes |
| `cpp/include/hgraph/types/value/set_delta_value.h` | Set delta wrapper |

### 4.4 Time Types

| File | Purpose |
|------|---------|
| `cpp/include/hgraph/util/date_time.h` | engine_time_t, MIN_DT, MAX_DT |

---

## Section 5: Architecture Considerations

### 5.1 Delta Tracking Mismatch

**Design Requirement**: Slot-based delta tracking (indices, not copies)

**Current Implementation**: Element-based delta tracking (copies values)

**Impact**: SetDelta and MapDelta need to be rewritten to use slot indices. The SlotObserver infrastructure supports this - delta structures can implement SlotObserver to receive insert/erase notifications from KeySet.

### 5.2 ObserverList Type

**Design Requirement**: ObserverList as atomic unit for observer schema

**Current State**: No ObserverList type exists. ObserverDispatcher exists but serves a different purpose.

**Required**: Define ObserverList that can be stored in Value and used as schema element.

### 5.3 TSMeta Schema Generation

**Design Requirement**: TSMeta generates 4 parallel schemas

**Current State**: No TSMeta class found. TypeMeta exists but has no time-series-specific schema generation.

**Required**: Implement TSMeta with recursive schema generation for time_, observer_, and conditional delta_value_.

### 5.4 Lazy Delta Clearing

**Design Requirement**: Delta cleared when `current_time > last_delta_clear_time_`

**Current State**: No lazy clearing mechanism exists.

**Required**: TSValue must track `last_delta_clear_time_` and clear delta_value_ on access when needed.

---

## Section 6: Implementation Dependencies

```
Level 0 (Exists):
├── Value/View system
├── TypeMeta/TypeRegistry
├── KeySet/SlotObserver
├── SetStorage/MapStorage
├── engine_time_t

Level 1 (Foundation - Must implement first):
├── ObserverList type
│   └── Requires: TypeMeta, TypeOps
├── Slot-based SetDelta
│   └── Requires: SlotObserver, KeySet
├── Slot-based MapDelta
│   └── Requires: SlotObserver, KeySet

Level 2 (Schema Generation):
├── TSMeta class
│   ├── Requires: TypeMeta, ObserverList
│   └── Generates: value_schema_, time_meta_, observer_meta_, delta_value_meta_

Level 3 (Core Time-Series):
├── TSValue class
│   └── Requires: TSMeta, Value, ObserverList
├── TSView class
│   └── Requires: TSValue, View

Level 4 (Navigation & Specialized):
├── BundleDeltaNav
├── ListDeltaNav
├── TSScalarView, TSBView, TSLView, TSDView, TSSView, TSWView
│   └── Requires: TSView

Level 5 (Advanced):
├── TimeArray (for TSD per-slot times)
├── ObserverArray (for TSD per-slot observers)
```

---

## Section 7: Recommendations

### 7.1 Implementation Strategy

1. **Start with Foundation Types**: ObserverList, slot-based SetDelta/MapDelta
2. **Build Schema Generation**: TSMeta with recursive schema generators
3. **Implement Core Classes**: TSValue, TSView
4. **Add Specialized Views**: Kind-specific TSView wrappers
5. **Integrate**: Wire up with existing time-series framework

### 7.2 Risk Areas

1. **Delta Rewrite**: Changing from element copies to slot indices is significant
2. **Recursive Schema**: Complex generation logic for nested types
3. **Observer Wiring**: TSValue must connect delta observers to KeySets
4. **Python Alignment**: Must match Python reference implementation behavior

### 7.3 Reuse Opportunities

1. Value<Policy> can be used directly for all four parallel values
2. SlotObserver protocol is ready for delta tracking
3. KeySet/SetStorage/MapStorage provide TSD/TSS foundation
4. TypeMeta system can be extended for TS-specific types
