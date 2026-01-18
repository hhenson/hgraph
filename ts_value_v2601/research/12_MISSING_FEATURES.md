# Missing Features / Concepts Not Documented in User Guide

This lists design concepts from `ts_design_docs` that are **not** covered in the new user guide
(`ts_value_v2601/user_guide`). These gaps need documentation or explicit decisions.

---

## Value system (from Value_DESIGN.md)

| Concept | Design source | Not in user guide | Why it matters |
| --- | --- | --- | --- |
| `TypeMeta` / `TypeRegistry` | `Value_DESIGN.md` | Yes | Required for type-erased storage and schema identity |
| `TypeOps` (ops vtable) | `Value_DESIGN.md` | Yes | Defines copy, equality, hash, to/from Python |
| Trait system + extension hooks | `Value_DESIGN.md` | Yes | Needed for composability, Python cache, hooks |
| Memory layout + SBO | `Value_DESIGN.md` | Yes | Critical for performance + pre-allocation |
| Named vs unnamed bundles | `Value_DESIGN.md` | Yes | Affects schema reuse + interop |
| ValueView hierarchy | `Value_DESIGN.md` | Yes | Defines how views are navigated and cast |

---

## TSValue / overlay system (from TSValue_DESIGN.md)

| Concept | Design source | Not in user guide | Why it matters |
| --- | --- | --- | --- |
| Dual-schema (TypeMeta vs TSMeta) | `TSValue_DESIGN.md` | Yes | Separates data schema from TS structure |
| TS overlay (timestamps + observers) | `TSValue_DESIGN.md` | Yes | Core of modified/valid/notify behavior |
| Zero-copy Value <-> TSValue bridge | `TSValue_DESIGN.md` | Yes | Needed to reuse Value system without copies |
| Container hooks for set/map | `TSValue_DESIGN.md` | Yes | Required to keep overlays aligned with swap/erase |
| Delta apply at Value or TS layer | `TSValue_DESIGN.md` | Yes | Needed for parity with Python delta semantics |
| Path-based identification | `TSValue_DESIGN.md` | Yes | Used for navigation + overlay alignment |

---

## TSInput / linking (from TSInput_DESIGN.md)

| Concept | Design source | Not in user guide | Why it matters |
| --- | --- | --- | --- |
| `TSLink` symbolic links | `TSInput_DESIGN.md` | Yes | Required for partial peering and dynamic links |
| Mixed local + linked hierarchy | `TSInput_DESIGN.md` | Yes | Non-peered containers with peered leaves |
| Sample-time on rebind | `TSInput_DESIGN.md` | Partially | Needed for REF rebind modified semantics |
| Direct notification to node | `TSInput_DESIGN.md` | Yes | Affects graph scheduling correctness |

---

## Migration / integration

| Concept | Design source | Not in user guide | Why it matters |
| --- | --- | --- | --- |
| Value <-> TSValue migration plan | `Value_TSValue_MIGRATION_PLAN.md` | Yes | Guides staged implementation |
| Schema integration plan | `value_schema_integration.md` | Yes | Links TypeMeta to TS schemas |
| Phase6/7 Python wrapper migration | `Phase6_7_PyWrapper_Migration_DESIGN.md` | Yes | Defines how Python API maps to C++ |

