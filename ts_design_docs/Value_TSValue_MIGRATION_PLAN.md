# Value ↔ TSValue Migration Plan

**Date**: 2026-01-07
**Status**: Draft
**Related**:
- `Value_DESIGN.md`
- `TSValue_DESIGN.md`

---

## 1. Goals

This migration exists to deliver a reworked `Value`/`TSValue` system that:

1. Keeps **type-erased, partitioned storage** and the `Value` / `ValueView` / `ConstValueView` model.
2. Treats **`Value` and `TSValue` as distinct concepts**, while providing a **zero-cost bridge** between them at the
   storage/view level.
3. Provides **hierarchical modification tracking** and **hierarchical observability** using the *same structural model*
   (no repeat of the “flat observer / hierarchical tracking” mismatch).
4. Enables **set/map TS extensions** (per-element timestamps + per-element observers) by reusing the existing
   robin-hood/index-based backing store and swap-with-last erase strategy.
5. Ensures the new `TSValue` + views can satisfy the **API demands** of the legacy time-series types (not necessarily
   identical signatures, but equivalent capabilities).

---

## 2. Non-goals (for this migration)

- A full redesign of `TypeMeta` or the core Value storage strategy.
- Changing python-facing semantics beyond what is required for correctness/performance.
- Optimizing every container path immediately (we prioritize correctness + stable hook surfaces).
- The interfaces in api/python represent a formal contract with the user of the code and cannot be modified outside of constructors and perhaps some utility methods for internal use.

---

## 3. Phased Migration Plan

### Phase 0 — Baseline and constraints

1. Freeze the current behavior expectations:
   - Identify the minimal behavior set needed from the legacy `TimeSeriesType` interface:
     - `py_value`, `py_delta_value`
     - `last_modified_time`, `modified`, `valid`, `all_valid`
     - `subscribe`, `un_subscribe` (see `cpp/include/hgraph/types/time_series_type.h` and `notifiable.h`)
2. Document (in code comments or small doc notes) where the current TSValue implementation diverges from the intended
   semantics (e.g., view-based `from_python` not updating modification time).

#### Phase 0 checklist (required behaviors + invariants)

The following is the *baseline capability checklist* that the new `TSValue` + views must be able to satisfy
(directly or via a thin adapter) in order to replace the legacy time-series types.

**A. Python-facing `TimeSeriesType` surface (capability parity)**

- [ ] `TimeSeriesType.value` returns the current value as a Python object.
- [ ] `TimeSeriesType.delta_value` returns a delta (patch-like) Python object when applicable.
  - [ ] When no delta is available/meaningful for a type, the behavior is explicitly defined (e.g. `None`).
- [ ] `TimeSeriesType.valid` and `TimeSeriesType.all_valid` are defined and consistent with the legacy semantics.
- [ ] `TimeSeriesType.modified` is defined and matches legacy semantics (time-based equality against evaluation time).
- [ ] `TimeSeriesType.last_modified_time` is defined and stable.
- [ ] Graph navigation properties are either supported or explicitly out-of-scope for view-only wrappers:
  - [ ] `owning_node`, `owning_graph`
  - [ ] `has_parent_or_node`, `has_owning_node`

**B. Output mutation semantics (legacy behavior expectations)**

- [ ] `apply_result(v)`:
  - [ ] If `v is None`, it invalidates (legacy convention).
  - [ ] Otherwise, it applies the value.
- [ ] `set_value(v)`:
  - [ ] If `v is None`, it invalidates.
  - [ ] Otherwise, it sets the value.
- [ ] `invalidate()` / `clear()` behavior is defined and consistent (even if the internal mechanism differs).
- [ ] Copy helpers (`copy_from_input`, `copy_from_output`) do not silently lose required semantics.

**C. Modification time invariants (legacy + TS overlay direction)**

- [ ] Root validity is equivalent to `last_modified_time > MIN_DT` (as in `BaseTimeSeriesOutput::valid()`).
- [ ] `mark_modified(et)` is monotonic (`_last_modified_time` only increases).
- [ ] Parent propagation: modifying a child updates the parent modification time (legacy `mark_child_modified`).
- [ ] Invalidation sets `last_modified_time = MIN_DT` and notifies subscribers (legacy `mark_invalid`).

**D. Observability invariants (legacy + hierarchical direction)**

- [ ] Root-level `subscribe(Notifiable*)` / `un_subscribe(Notifiable*)` works.
- [ ] Notifications deliver the correct `engine_time_t` and are safe under teardown/stop conditions.
- [ ] TS-level observability must be hierarchical and share the same structural model as hierarchical timestamping.

**E. Delta invariants (Value vs TS layer)**

- [ ] A delta is applicable at either layer:
  - [ ] applying to `Value` updates data only
  - [ ] applying to `TSValue` updates data **and** TS overlay (timestamps + observers) hierarchically
- [ ] For set/dict-like types, deltas can express structural changes (e.g. add/remove events) where required.

**Exit criteria:** this checklist is present and referenced from the key divergence points in code (Phase 0 notes).

---

### Phase 1 — Define the “container hook surface” in Value (composition)

The core requirement is to let TS overlays attach **parallel per-element data** to set/map containers without
inheritance.

1. Introduce (internal) hook interfaces/types that container ops can call:
   - `on_insert(index)`
   - `on_swap(index_a, index_b)` (for swap-with-last)
   - `on_erase(index)` (or `on_erase_swap(erased, last)`)

2. Extend set/map operations so they can:
   - return the slot/index on insert (or expose it through an API), and
   - report swap-with-last movements.

3. Ensure hooks are **zero cost when unused**:
   - compiled out via templates, or
   - a nullable pointer checked only in the rare paths that already branch.

**Exit criteria:** Value containers can optionally drive extension hooks without changing existing semantics.

---

### Phase 2 — Introduce TS overlay storage (timestamps + observers)

1. Define TS overlay storage as a schema-shaped structure parallel to the data view tree.
2. For set/map overlays:
   - store per-element metadata in vectors indexed by the backing store’s slot/index
   - update metadata on insert and swap-with-last via the Phase 1 hooks

3. Define a minimal observer model that can scale hierarchically:
   - root observers
   - per-child observers (bundle field, list element, set/map slot)
   - propagation rules (notify parents on child updates, or notify by subscription scope)

**Exit criteria:** overlays can be attached to container storages and remain aligned under insert/erase/swap.

---

### Phase 3 — Implement the zero-cost `Value` ↔ `TSValue` bridge

1. Define bridging APIs:
   - `TSView`/`TSMutableView` wrapping a `ValueView` + `TSMeta` + overlay pointer/handle
   - `ValueView` extraction from `TSView`

2. Ensure bridging is *storage-level* and does not allocate/copy:
   - all views remain non-owning
   - TS overlay is referenced, not duplicated

**Exit criteria:** bridging works for scalar + bundle + list + set/map paths.

---

### Phase 4 — Replace `tracking_value` with overlay-driven tracking

1. Deprecate the separate `tracking_schema_from(...)` / `tracking_value_type` approach for runtime behavior.
2. Implement tracking and `ts_valid` semantics from the overlay storage directly.
3. Ensure view-based mutation paths update timestamps consistently:
   - `set<T>(..., time)` and `from_python(...)` must have a defined rule for timestamp updates.

**Exit criteria:** hierarchical timestamps are correct and consistent across all mutation routes.

---

### Phase 5 — Observability: hierarchical subscriptions compatible with legacy `Notifiable`

1. Provide subscription APIs that can model legacy behavior:
   - subscribe/unsubscribe at the root

2. Add hierarchical options needed by TS containers:
   - subscribe to bundle fields
   - subscribe to specific keys/indices
   - subscribe to “any element changed” for a container

3. Define notification ordering and deduplication rules.

**Exit criteria:** observers are correct under container swaps, key removals, and nested updates.

---

### Phase 6 — Python and API parity validation

1. Ensure python wrappers can be implemented against TS views without loss of required semantics:
   - correct `value`/`delta_value`
   - correct `modified`/`valid`/`all_valid`
   - correct notification behavior for graph evaluation

2. Decide and document caching rules:
   - whether TS view conversions should reuse `Value`’s python cache or remain uncached

**Exit criteria:** python-facing behavior matches required expectations for the replaced API.

---

### Phase 7 — Decommission legacy TS types

1. Provide temporary compatibility adapters (only if needed to reduce churn).
2. Move builders and internal runtime code to create TSValue-based objects.
3. Remove old types once parity tests are green.

**Exit criteria:** legacy TS types are no longer required for core functionality.

---

## 4. Risk register / watch-outs

- **Index stability assumptions**: overlays for sets/maps rely on the container’s slot/index being stable unless a
  swap-with-last occurs. Any future change to the backing store must preserve this contract or update the hook surface.
- **Mutation paths**: direct `TypeMeta::ops->from_python` bypasses Value policies; migration must explicitly define how
  timestamps and observers are updated in view-based paths.
- **Observer lifetimes**: legacy `Notifiable*` subscriptions require clear ownership and teardown rules.

---

## 5. Verification strategy (high level)

- Unit tests for set/map overlay alignment:
  - insert → hook `on_insert`
  - erase-last (no swap)
  - erase-middle (swap-with-last)
  - repeated insert/erase cycles
- Unit tests for hierarchical timestamps:
  - bundle field update updates field + parent timestamps
  - set/map element update updates element + container timestamp
- Unit tests for observer propagation:
  - subscription to root notified on nested update
  - subscription to specific element notified only on that element
