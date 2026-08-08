# CLAUDE.md

Working guide for AI sessions on **hgraph**. This file is the *operational* layer; it
does not restate project direction.

- **Canonical direction:** [`AGENTS.md`](AGENTS.md) — project goals, build philosophy,
  source layout, dependency policy, git hygiene. Read it first; it wins on any conflict
  about *direction*.
- **This file** adds: the doc-vs-code discipline, guardrails, an architecture map, the
  honest current state, the working commands, and pointers to the design corpus.

---

## 1. What this project is (one paragraph)

A clean-slate, **C++-first** reimplementation of the `hgraph` runtime. The C++ runtime
is the source of truth; Python is a wiring/compat bridge, never the foundation
(`AGENTS.md`). It is a deliberate re-do of an earlier working-but-messy attempt:
keep the proven runtime ideas, drop the Python-first build assumptions and accumulated
implicit state. Main target: `hgraph_core` (alias `hgraph::core`), C++23, CMake.

---

## 2. The non-negotiable workflow: design-first, *enforced*

The recurring failure on this project is **code outrunning the design docs**. The rule
going forward:

> **The developer-guide docs in `docs/source/developer_guide/` are authoritative.
> Change the doc in the *same* change as the code. A doc/code divergence is a bug.**

Concretely, for any non-trivial change:

1. **Doc first.** If you are adding/altering a structure, layer, or invariant, update
   the relevant `.rst` (and `AGENTS.md`/memory if direction-level) *before or with*
   the implementation — never "later".
2. **No silent structural change.** If the code needs a shape the docs don't describe
   (a new file, type, link kind, ops table), either (a) update the doc to make it the
   intended design, or (b) stop and flag it. Do not just write the code.
3. **Cite the doc** the change implements in your summary/commit, so drift is visible.
4. **If you find existing drift**, treat the *current intended* structure as the target:
   update the doc up to what the code now does (when the code is the better design), or
   fix the code to match the doc (when the doc is right). Record which way and why.
   Do **not** revert good code to stale docs.
5. **Keep the tree green** (see §6). It is green now; every change lands green.

---

## 3. Guardrails (the failure modes to actively prevent)

- **(i) Ahead-of-design code.** Do not build machinery the current milestone (§5) does
  not need. The simple-TS path does **not** need REF alternatives, TSD proxies, window
  views, or container kinds. If a task tempts you toward them, confirm scope first.
- **(ii) Structural drift.** Names/layout must match the docs *and the established
  vocabulary* (§4). New name → goes in the doc with a rationale, same change. Note the
  historical gap: docs/memory predating the runtime may say
  `ts_value`/`TSValue`/`ts_state`/`TSState`; the code uses `ts_data`/`TSData` +
  `ts_input`/`ts_output`. The code names are current — reconcile *toward the code*.
- **(iii) Parallel abstractions.** v2 principle: **one runtime model, no generic
  fallback**. Subscription/notification, delta cleanup, and modified-time tracking
  already exist (§4). Two ways to do one thing is the smell to kill, not add to.

---

## 4. Architecture map (matches the current tree)

**Universal vocabulary** (every layer reuses it — memory `core_data_structure_model`,
`docs/.../data_structures/core_concepts.rst`): `Plan` (memory layout + lifecycle ops) ·
`Schema` (layout-free type identity) · `Ops` (struct of fn-ptrs; **first param is always
the memory pointer**) · `Builder` (only place Schema binds to a concrete Plan+Ops) ·
`Value` (owns memory) · `View` (borrows memory + Ops). Plans/Schemas/Ops/Builders are
**interned** (`InternTable<Key,Value>`); Values are not. Registries are thin wrappers
over an `InternTable`.

| Path (`include/hgraph/`, mirrored in `src/`) | Layer |
|---|---|
| `util/` | `date_time.h` (`DateTime`, `MIN_ST`, `MAX_ET`), `scope.h`, `tagged_ptr.h` |
| `types/utils/` | foundation: `intern_table`, `memory_utils` (StoragePlan/StorageHandle/LifecycleOps), slot stores, `slot_observer` |
| `types/value/` | **Value layer**: `value`, `value_view`, `value_ops`, `value_builder`, compact/container storage, `specialized_views` |
| `types/metadata/` | **Schemas + bindings + registries**: `*_type_meta_data`, `type_binding`, `type_registry`, `value_plan_factory`, `ts_data_plan_factory` |
| `types/time_series/ts_data/` | **TS data structures** — payload+delta substrate used by both output and input |
| `types/time_series/ts_output/` | **TS output impls** — owning output endpoints |
| `types/time_series/ts_input/` | **TS input proxies** — non-owning endpoints; `target_link` is the peered binding |
| `types/time_series/` | umbrellas `ts_{data,input,output}.h`, `endpoint_schema.h` (`TSEndpointSchema`), `time_series_reference.h` |
| `runtime/` | **Execution**: `node.h`, `graph.h`, `executor.h`, `runtime.h` |

**The TS three-way split (intended meaning — keep it this way):**
- `ts_data` = basic data types holding/accessing value + delta; the substrate both
  `TSOutput` (output value) and `TSInput` (input value) use to implement behaviour.
- `ts_output` = the **output implementations** (owning endpoints that mutate/tick data).
- `ts_input` = the **input proxies** (non-owning endpoints that bind to an output and
  read it).

**Reference tree (read-only, never edit):** `ext/main` tracks the maintained
Python-first `release/0.5` line for compatibility comparisons. It is not the
implementation source of truth. Memory: `reference_branches`.

---

## 5. Current state (honest) & current milestone

**The runtime is feature-complete against the maintained Python-first reference
and replaces it in hgraph 0.8.0.** It ships as the `hgraph` wheel (cp312-abi3; Linux
manylinux_2_28 / macOS arm64 / Windows). The release contract lives in
`docs/source/developer_guide/release_readiness.rst`; accepted upstream
deviations in `parity_matrix.rst`; direction in `roadmap.rst`.

**Capability areas — all DONE, each with an authoritative design record:**

- **Execution**: simulation + real-time executors (`runtime/executor.h`; state
  folds into executor ops — no separate engine/clock, recorded decision), push
  sources, wall-clock scheduler alarms (due alarms deliver next cycle, never
  drop), end-of-run drain bounded by logical progress, opt-in recursion guard
  (`max_consecutive_immediate_cycles` → `RecursiveEvaluationError`). Records:
  `data_structures/overview/execution_layer.rst`, `services.rst` (alarms).
- **Nested graphs & higher-order ops**: `map_`/`switch_`/`reduce`/`mesh_`,
  variadic + named args/defaults/kwargs, Python call-shape parity. Records:
  `nested_graphs.rst`, `mesh.rst`.
- **Services, adaptors, contexts**: all three service flavours, service
  adaptors, shared outputs, `context::scope`/`Context<>`. Record:
  `services.rst`.
- **Error handling**: per-node capture, `exception_time_series`, `try_except_`.
  Record: `error_handling.rst`.
- **Temporal types (RFC 0002)**: Instant/Duration/CivilDateTime/Period/ZoneId/
  ZonedDateTime/ranges, checked arithmetic, dual TZ backends (std chrono vs
  date/tz by conformance probe), JSON/Arrow v2 codecs with v1 ingest.
- **Python bridge & DSL**: `python/hgraph` IS the future hgraph package —
  types/operators/@graph/eval_node/map_/switch_/services/components all work
  from Python; 162-name curated `__all__` + PEP 562 operator registry; ported
  upstream suites green (operator tests 48/48 files; ts/wiring tiers besides).
  Records: `python_bridge.rst`, `parity_matrix.rst`.
- **Frames & serialization**: typed frame metadata (RFC 0001), TABLE protocol +
  data-frame operators, record/replay. Record: `record_replay_table.rst`.
- **Extension SDK**: downstream native extensions (shared-lib SDK,
  `hgraphConfig.cmake`, `hgraph_add_python_module`), Python scalar registration
  (RFC 0003), python-owned structured scalars (RFC 0004). Record:
  `extension_policy.rst`.
- **Adaptor families** (`python/hgraph/adaptors/`): sql (+snowflake), delta,
  kafka, perspective, tornado/web, data_frame, json, data_catalogue — each with
  a pyproject extra; heavy deps lazy-imported.

**RFC catalogue** (`docs/source/rfc/`, process in `rfc_0000.rst`): 0001 frame
metadata, 0002 temporal, 0003 scalar registration, 0004 python-owned structured
scalars — all Accepted and implemented. New structural/API proposals go through
an RFC first.

**Current milestone: hgraph 0.8 hardening.** The C++-first runtime now owns the
main `hgraph` package and repository while the Python-first 0.5 line remains on
`release/0.5`. Harden the port and its extension packages before the 1.0 API
freeze. RFC 0005
(`rfc_0005_hgraph_1_0_api.rst`) proposes the package structure, API surface,
stability tiers, and release process — read it before any 1.0-directed work.

## 6. Build & test

```sh
cmake -S . -B build                 # configure (fmt + Catch2 fetched if absent)
cmake --build build -j              # build hgraph_core + tests
ctest --test-dir build --output-on-failure
./build/tests/cpp/hgraph_unit_tests # or run the Catch2 suite directly
```

- Tests on by default (`BUILD_TESTING`, CTest). Catch2 suite target `hgraph_unit_tests`
  — **add new test files to `tests/cpp/CMakeLists.txt`**. Also `hgraph_smoke_test`,
  `hgraph_header_compile_check`.
- Options: `-DHGRAPH_WARNINGS_AS_ERRORS=ON`, `-DHGRAPH_ENABLE_ASAN=ON
  -DHGRAPH_ENABLE_UBSAN=ON` (Clang/GCC; ASAN/UBSAN exclusive with TSAN).
- **Never** make the default build need Python/nanobind. Opt-in only:
  `-DHGRAPH_BUILD_PYTHON_BINDINGS=ON` / `-DHGRAPH_ENABLE_PYTHON_USER_NODES=ON`.

---

## 7. Conventions

- **C++23**; prefer std features that simplify.
- **Single-threaded evaluation** (ruling 2026-07-02): the **per-tick runtime path**
  (value/TS/runtime ops invoked during evaluation) must be lock-free and
  `shared_ptr`-free. **Build-time machinery** (interning, plan/ops-synthesis caches,
  registries) MAY use mutexes to guard shared resources — that is sanctioned, not
  drift. Push-source senders + the real-time executor CV remain the only
  cross-thread runtime boundary.
- **Ops tables**: structs of fn-ptrs, first param = the structure's memory; metadata
  (header) separate from operations (`*_ops`).
- **Lifetime**: builders are build-time only; no `shared_ptr` for builder lifetime in
  graph code; long-lived immutable artifacts live in registries (stable addresses).
- **Containers**: `ankerl::unordered_dense`; index-based indirection for stable
  addresses; default SBO `<sizeof(void*),alignof(void*)>`.
- **Tests close to behaviour**; write the failing test that captures the intended
  semantic before the fix.

---

## 8. Design corpus & memory (read before designing)

- **Developer guide** (authoritative): `docs/source/developer_guide/`, esp.
  `data_structures/` (`core_concepts`, `overview/`, `schemas/`, `plans_and_ops/`,
  `linking_strategies`, `refinements`).
- **Auto-memory** (`MEMORY.md` index): `v2_design_principles`,
  `core_data_structure_model`, `developer_guide_doc_decisions`, branch maps. Memory is
  point-in-time — verify file:line claims against current code.
