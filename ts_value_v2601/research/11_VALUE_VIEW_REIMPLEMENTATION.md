# Value/View Re-Implementation Plan (C++), Referencing User Guide

This document connects the **intended design** in `ts_design_docs` to the **new user guide**
(`ts_value_v2601/user_guide`) and explains how the **Value/View system** can replace the current C++ type system.

Key design sources:
- `ts_design_docs/Value_DESIGN.md` (Value + View, TypeMeta, ops, traits)
- `ts_design_docs/TSValue_DESIGN.md` (TS overlay, dual schema, deltas)
- `ts_design_docs/TSInput_DESIGN.md` (symbolic links, partial peering)

User guide anchors:
- `ts_value_v2601/user_guide/03_TIME_SERIES.md`
- `ts_value_v2601/user_guide/04_LINKS_AND_BINDING.md`
- `ts_value_v2601/user_guide/06_DELTA.md`

---

## Core Replacement Architecture (Shared Across Types)

| Layer | Design intent (ts_design_docs) | User guide concept | Re-implementation approach |
| --- | --- | --- | --- |
| Data storage | `Value` + `TypeMeta` | "value()" / container data | Store raw data in `Value`; expose through `ValueView` |
| Time-series overlay | TS overlay (timestamps + observers) | `modified`, `valid`, `last_modified_time` | Parallel overlay arrays keyed by schema path |
| Linking | `TSLink` symbolic links | binding, active/passive | `TSLink` bound to output overlay; view resolves to linked output |
| Delta | delta applies at Value or TS layer | `delta_value` + modified_* | Produce deltas from overlay state + container diffs |

The user guide types map to **one TSValue + overlay** rather than distinct C++ classes per type.

---

## Per-Type Re-Implementation Tables

### TS (Scalar)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `value()` | `ConstValueView.as<T>()` | Read scalar from Value storage |
| `set_value(x)` | `ValueView.as<T>() = x` + overlay mark | Set data and mark modified/valid |
| `delta_value()` | overlay tracks last write | Same as value for scalars |
| `modified()/valid()` | overlay flags | True if modified/ever-set |
| binding | `TSLink` for peered input | Input view reads linked output |

### TSB (Bundle)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `field("a")` | `ConstBundleView.field("a")` | Returns child view for field |
| `modified()` | overlay on bundle node | True if any child modified |
| `modified_items()` | overlay child mask | Iterate modified children |
| `delta_value()` | ValueView + overlay | Dict of modified fields only |
| binding | `TSLink` per peered field | Partial peering via links; non-peered fields local |

### TSL (List)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `operator[](i)` | `ConstListView.at(i)` | Child view at index |
| `modified()` | overlay list node | True if any element modified |
| `modified_items()` | overlay index mask | Dict of modified indices |
| `delta_value()` | list diff + overlay | Dict of modified indices to deltas |
| binding | `TSLink` per element | Mixed local + peered elements supported |

### TSD (Dict)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `contains(k)` | map view lookup | Key presence from Value storage |
| `key_set` | overlay + map hooks | Track added/removed keys per tick |
| `modified_items()` | overlay keyed by slot index | Emit per-key modified items |
| `delta_value()` | map diff + overlay | Modified values + REMOVE markers |
| non-peered case | type mismatch triggers local storage | `TSLink` disabled; local Value + overlay used |
| container hooks | map insert/erase/swap callbacks | Keep overlay arrays aligned with map storage |

### TSS (Set)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `values()` | set view iterator | Iterate current elements |
| `added()/removed()` | overlay diff vs prior | SetDelta computed from overlay |
| `delta_value()` | SetDelta object | `added` / `removed` for tick |
| binding | set overlay + link at set node | Set itself is a leaf for peering |

### TSW (Window)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `value()` | cyclic buffer view | Window slice when ready |
| `value_times()` | parallel time buffer | Return aligned timestamps |
| `delta_value()` | overlay + last tick check | Last element if appended at eval time |
| removed element | overlay hook on roll | Track removed_value + has_removed_value |

### REF (Reference)

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `REF -> REF` | `TS[TimeSeriesReference]` | Plain scalar TS behavior |
| `TS -> REF` | construct `TimeSeriesReference` token | Wrap output as reference; non-peered |
| `REF -> TS` | `TimeSeriesReference.bind_input` | Rebind link targets on ref updates |
| `value()/delta_value()` | value is token | Delta equals value; rebind is sample tick |
| rebind deltas | overlay sample_time | Full delta for TSL/TSD/TSS on target switch |

### SIGNAL

| User guide API | Value/View replacement | Behavior re-created |
| --- | --- | --- |
| `modified()` | overlay only | True when ticked |
| `value()` | constant true | Always `True` (no payload) |
| indexing | child signal overlay | Child signals created for bundle/TSL bindings |

---

## Method-Level Re-Implementation Notes

These connect user guide methods to specific Value/View + overlay behaviors.

| User guide method | C++ Value/View + overlay steps | Notes |
| --- | --- | --- |
| `value()` | `ConstValueView` read | Read-only; does not change overlay |
| `set_value(x)` | write Value + `overlay.mark_modified(node)` | Update data and timestamps |
| `modified()` | `overlay.modified_at(node, eval_time)` | Uses timestamp equality |
| `valid()` | `overlay.valid(node)` | Set on first write or explicit invalidate |
| `delta_value()` | `overlay.delta(node)` | Type-specific: scalar == value, composite == diff |
| `bind_output(out)` | `TSLink.bind(out)` | Link tracks active/subscription |
| `un_bind_output()` | `TSLink.unbind()` | Sample-time triggers modified |

---

## How This Replaces the Current C++ Type System

1) **Replace per-type storage classes** with `Value` + `TypeMeta`.
2) **Replace per-type TS state** with TS overlay arrays keyed by schema path.
3) **Expose behavior through views** (`TSView`, `TSMutableView`) built on `ValueView`.
4) **Implement binding via TSLink** (symbolic link) instead of direct pointer binding.
5) **Rebuild deltas from overlay and container diffs**, matching Python semantics.

This preserves the user guide API while consolidating implementation into a single Value/View + overlay system.

