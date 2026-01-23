# Implementation Plan Review: TSValue Infrastructure

**Reviewer:** Claude Code
**Date:** 2026-01-23
**Documents Reviewed:**
- Implementation Plan: `ts_value_v2601/implementation/ts_value/01_IMPLEMENTATION_PLAN.md`
- Design Specification: `ts_value_v2601/design/03_TIME_SERIES.md`

---

## Summary Assessment

**Status: NEEDS_REVISION**

The implementation plan is well-structured and covers most of the required components, but has several gaps and inconsistencies with the design document that need to be addressed before implementation can proceed.

---

## Section 1: Completeness Issues

### 1.1 Missing TS Types in Schema Generation

**Issue:** The `generate_time_schema()` function in Phase 3 does not handle all TS types consistently.

The design document specifies these mappings:
```
time_schema(REF[T])    = engine_time_t
time_schema(SIGNAL)    = engine_time_t
```

The implementation plan's `generate_time_schema()` function at line 534 treats SIGNAL and REF correctly, but groups them with TSW which is also correct. However, the observer and delta schema generation functions are marked as "Similar implementations" without being fully specified.

**Recommendation:** Add complete implementations for `generate_observer_schema()` and `generate_delta_value_schema()` in the plan.

### 1.2 TSW WindowStorage Not Addressed

**Issue:** The design document specifies a detailed `WindowStorage` structure (lines 406-426) but the implementation plan does not include this as a deliverable or specify its implementation phase.

The design specifies:
```cpp
struct WindowStorage {
    struct CyclicBuffer {
        std::vector<engine_time_t> times;
        std::vector<std::byte> values;
        size_t head, count, capacity;
    } cyclic;

    struct QueueBuffer {
        std::vector<engine_time_t> times;
        std::vector<std::byte> values;
    } queue;

    const TypeMeta* element_meta;
};
```

**Recommendation:** Add a phase or section covering WindowStorage implementation, or explicitly note it as out-of-scope for this plan.

### 1.3 REF Type Not Detailed

**Issue:** The REF type (TimeSeriesReference) is listed in the value schema mapping but no implementation details are provided for how references work.

**Recommendation:** Either add REF handling to the plan or document it as out-of-scope.

### 1.4 Missing Kind-Specific View Implementations

**Issue:** Phase 5 lists kind-specific views (TSBView, TSLView, TSDView, TSSView, TSWView) but provides only skeleton examples for TSBView and TSSView. The following are missing:

- TSLView - no implementation shown
- TSDView - no implementation shown
- TSWView - no implementation shown (and the design has detailed interface for this)

The design document (lines 686-706) specifies TSWView interface in detail:
```cpp
class TSWView : public TSView {
    ViewRange values() const;
    ViewRange times() const;
    View at_time(engine_time_t t) const;
    ViewRange range(engine_time_t start, engine_time_t end) const;
    size_t size() const;
    size_t capacity() const;
    engine_time_t oldest_time() const;
    engine_time_t newest_time() const;
    ViewPairRange items() const;
};
```

**Recommendation:** Add complete interface specifications for all kind-specific views.

---

## Section 2: Correctness Issues

### 2.1 has_delta() Predicate - Incorrect Return for Base Case

**Issue:** The implementation plan's `has_delta()` function (lines 507-530) does not handle the recursive case correctly for TSB.

Plan code:
```cpp
case TSKind::TSB:
    for (const auto& field : ts_meta->fields()) {
        if (has_delta(field.ts_meta->kind(), field.ts_meta)) {
            return true;
        }
    }
    return false;
```

Design document (lines 199-209):
```
has_delta(TSB[...])  = any(has_delta(field_i) for field_i in fields)
```

The implementation looks correct but the function signature takes both `TSKind kind` and `const TSMeta* ts_meta` as parameters. For the recursive call, it accesses `field.ts_meta->kind()` and `field.ts_meta`, but `fields()` method is not defined in the TSMeta struct shown in the plan.

**Recommendation:** Ensure TSMeta has a `fields()` method defined, or adjust the implementation to match actual TSMeta structure.

### 2.2 Lazy Clearing Time Comparison

**Issue:** The implementation plan uses inconsistent time comparisons.

In TSValue (line 727):
```cpp
if (current_time > last_delta_clear_time_) {
    clear_delta_value();
    last_delta_clear_time_ = current_time;
}
```

This uses `>` (greater than).

The design document (line 366) states:
> "When accessing delta with `current_time > time_[container]`, the delta is automatically cleared."

The comparison should be consistent. However, note the design document also states (line 491):
```cpp
if (current_time > last_delta_clear_time_) {
```

This is consistent with the plan. The issue is that `modified()` checks use `>=`:
```cpp
bool modified() const { return time_value() >= current_time_; }
```

The semantics need to be clear:
- Something is **modified** at `current_time` if `last_modified_time >= current_time` (it was modified during this tick or later)
- Delta needs clearing if `current_time > last_delta_clear_time_` (we're in a new tick since last clear)

**Recommendation:** Add explicit documentation in the plan clarifying the time comparison semantics to avoid confusion during implementation.

### 2.3 SetDelta on_clear() Implementation

**Issue:** The `SetDelta::on_clear()` implementation (lines 286-290) is incomplete:

```cpp
void on_clear() override {
    // All existing elements removed - but we don't track all slots
    // This is called when the underlying KeySet is cleared
    // For delta purposes, this should be handled at a higher level
}
```

When a KeySet is cleared, all elements are removed. The delta should track these removals.

**Recommendation:** Either implement proper tracking of all removed slots when clear happens, or document why this is handled at a higher level and how.

### 2.4 TSView Constructor - Schema Access

**Issue:** In the TSView implementation (line 625 in design, referenced in plan), the constructor accesses `ts_value.schema()`:

```cpp
TSView(TSValue& ts_value, engine_time_t current_time)
    : ...
    , meta_(ts_value.schema())
```

But in the TSValue class definition in the plan (line 645), the accessor is named `meta()`:
```cpp
[[nodiscard]] const TSMeta* meta() const { return meta_; }
```

**Recommendation:** Use consistent naming: either `meta()` or `schema()` throughout.

### 2.5 MapDelta Children Not Properly Typed

**Issue:** The `MapDelta` class uses `DeltaPtr = std::unique_ptr<void, std::function<void(void*)>>` for children, which loses type information.

While type erasure is necessary for heterogeneous child types, the plan should specify:
1. How child deltas are created with the correct type
2. How they are accessed with type safety
3. How clearing works through the type-erased interface

**Recommendation:** Add a factory or creation pattern for typed child deltas in MapDelta.

---

## Section 3: Gaps Between Plan and Design

### 3.1 Design Specifies delta_time in Delta Structures

**Issue:** The design document (lines 862-877) specifies that delta structures should include a `delta_time` timestamp:

```cpp
struct SetDelta {
    engine_time_t delta_time;     // When delta was last computed
    std::vector<size_t> added;
    std::vector<size_t> removed;
};

struct MapDelta {
    engine_time_t delta_time;
    std::vector<size_t> added;
    std::vector<size_t> removed;
};
```

The implementation plan's delta structures (SetDelta at line 254, MapDelta at line 338) do NOT include `delta_time`.

However, the implementation plan puts `last_delta_clear_time_` in TSValue instead. This is a design decision difference.

**Analysis:**
- Design: delta_time in each delta structure
- Plan: single last_delta_clear_time_ in TSValue

The plan's approach is simpler for non-nested cases but may not handle nested TSS/TSD correctly where different parts of the structure may need independent clearing.

**Recommendation:** Clarify which approach is authoritative. If the TSValue-level approach is preferred, document how nested deltas are cleared correctly.

### 3.2 ObserverList notify Methods Not Specified

**Issue:** `ObserverList` declares `notify_modified()` and `notify_removed()` (lines 73-74) but marks them as "Implemented in .cpp" without specifying behavior.

The design document mentions observer notification (lines 186-188):
> "Observers enable fine-grained subscription to changes:
> - Container-level: Notified when any part of the container changes
> - Child-level: Notified when specific field/element/slot changes"

**Recommendation:** Add specification for notify methods including:
- What `Notifiable` interface looks like
- When observers are notified
- Notification order guarantees (if any)

### 3.3 Observer Wiring for TSB and TSL

**Issue:** The `wire_observers()` method in TSValue (lines 762-780) only handles TSS and TSD cases:

```cpp
void TSValue::wire_observers() {
    if (!has_delta()) return;

    switch (meta_->kind()) {
        case TSKind::TSS: { ... }
        case TSKind::TSD: { ... }
        default:
            // TSB and TSL need recursive wiring
            break;
    }
}
```

The recursive wiring for TSB and TSL is not implemented.

**Recommendation:** Add implementation for TSB and TSL recursive wiring, which needs to:
1. Iterate through fields/elements
2. For each child with has_delta(), recursively wire observers
3. Handle the BundleDeltaNav/ListDeltaNav navigation structures

### 3.4 TimeArray/ObserverArray for TSD Not Wired

**Issue:** The design document (lines 979-990) specifies that for TSD, the time_ and observer_ var_lists need to observe the KeySet:

```cpp
// time_, observer_ var_lists track slot additions/removals
ks.observers_.push_back(&time_.as<TimeTuple>().var_list());
ks.observers_.push_back(&observer_.as<ObserverTuple>().var_list());
```

The implementation plan mentions TimeArray and ObserverArray as SlotObservers in Phase 1, but does not show how they are wired to TSD's KeySet.

**Recommendation:** Add explicit wiring for TimeArray and ObserverArray as part of TSD construction.

---

## Section 4: Architectural Concerns

### 4.1 Type Erasure Complexity

The use of `DeltaPtr = std::unique_ptr<void, std::function<void(void*)>>` for child deltas in MapDelta and navigation structures adds complexity:

1. No compile-time type safety
2. Custom deleters needed for each delta type
3. Clearing requires type-specific logic through the erased interface

**Recommendation:** Consider a variant-based approach or a DeltaBase interface with virtual clear() method.

### 4.2 Schema Generation Timing

The plan does not specify when schema generation happens:
- Is it at TSMeta creation time?
- Is it lazy?
- Are schemas cached/shared?

**Recommendation:** Add section on schema lifecycle and caching strategy.

### 4.3 Memory Ownership Model

For nested structures like TSD[K, TSL[TSS[T]]], the ownership model becomes complex:
- Who owns child delta structures?
- When are they created/destroyed?
- How is capacity managed for variable-size children?

**Recommendation:** Add explicit ownership and lifecycle documentation.

---

## Section 5: Recommended Changes

### High Priority (Must Fix)

1. **Add complete observer and delta schema generation functions** in Phase 3
2. **Fix inconsistent accessor naming** (schema() vs meta())
3. **Implement recursive observer wiring** for TSB and TSL
4. **Document time comparison semantics** clearly
5. **Resolve delta_time location** (in delta structure vs TSValue)

### Medium Priority (Should Fix)

6. **Add WindowStorage implementation** or mark as out-of-scope
7. **Complete all kind-specific view interfaces** (TSLView, TSDView, TSWView)
8. **Specify Notifiable interface** and notification semantics
9. **Add TimeArray/ObserverArray wiring** for TSD

### Low Priority (Nice to Have)

10. **Consider variant-based delta children** instead of void pointer
11. **Add schema lifecycle documentation**
12. **Add memory ownership model documentation**

---

## Section 6: Conclusion

The implementation plan provides a solid foundation but requires revision in the following areas:

1. **Completeness:** Missing implementations for TSW, REF, and full kind-specific views
2. **Correctness:** Inconsistent naming, incomplete recursive wiring
3. **Design Alignment:** delta_time location differs from design document

Once these issues are addressed, the plan will be ready for implementation. The phased approach and test strategy are sound, and the dependency order is correct for the components that are specified.

---

## Appendix: Checklist for Revision

- [ ] Add complete `generate_observer_schema()` implementation
- [ ] Add complete `generate_delta_value_schema()` implementation
- [ ] Address WindowStorage implementation or scope
- [ ] Address REF type implementation or scope
- [ ] Complete TSLView interface specification
- [ ] Complete TSDView interface specification
- [ ] Complete TSWView interface specification
- [ ] Fix schema()/meta() naming inconsistency
- [ ] Clarify delta_time location decision
- [ ] Document time comparison semantics
- [ ] Implement SetDelta::on_clear() properly
- [ ] Add factory pattern for typed child deltas
- [ ] Add recursive observer wiring for TSB/TSL
- [ ] Add TimeArray/ObserverArray wiring for TSD
- [ ] Specify Notifiable interface
- [ ] Add schema lifecycle documentation
