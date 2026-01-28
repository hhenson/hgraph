# Value (Notes)

This file is kept as a short, high-level note.

The canonical documents are:

- `Value_DESIGN.md` (comprehensive design)
- `Value_USER_GUIDE.md` (usage)
- `Value_EXAMPLES.md` (examples)

---

## Current direction (2026-01)

### Value remains “data only”

`Value` is responsible for:

- type-erased storage
- schema-driven operations (`TypeMeta` / `TypeOps`)
- `Value` / `ValueView` / `ConstValueView` access patterns
- policy-based extensions where they make sense (e.g., Python caching)

### TSValue remains a distinct concept

`TSValue` is not “just a Value with extra fields”. Instead:

- `TSValue` and `Value` are distinct type-erased concepts
- they must share a **zero-cost bridge** at the storage/view level
  - wrap a `ValueView` as a `TSView` without copying
  - extract a `ValueView` from a `TSView` without copying

### Deltas can be applied at either layer

The system must support **delta (patch-like) updates**:

- apply a delta to a `Value` to update the underlying data, and
- apply the same delta to a `TSValue` to update data **and** TS semantics (hierarchical tracking + observability).

### Hierarchical tracking + observability

Both:

- hierarchical modification time tracking, and
- hierarchical observer/subscription tracking

must follow the same structural model (especially for containers).

### Set/Map container integration (composition)

Sets and maps use an index-based backing store with swap-with-last erase. TS-level extensions (timestamps and observers)
must reuse those indices via a composition-based hook surface (index acquisition + swap notification).

See:

- `TSValue_DESIGN.md` (design realignment section)
- `Value_TSValue_MIGRATION_PLAN.md` (phased migration plan)


