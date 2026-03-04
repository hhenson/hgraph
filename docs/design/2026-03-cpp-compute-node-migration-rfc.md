# RFC: Python Compute Node Migration to C++ (Name-Stable API, Dual Runtime)

Date: 2026-03-03  
Status: Draft for implementation  
Scope: `@compute_node` migration path from Python eval functions to C++ kernels while preserving Python API shape.

## 1. Goals

1. Preserve Python API shape:
   - Users keep calling the same Python function names.
   - Operator overload selection remains unchanged.
2. Support migration in place:
   - Non-migrated nodes continue to run in Python.
   - Migrated nodes can run in C++ when `HGRAPH_USE_CPP=1`.
3. Avoid silent masking:
   - If a node is mapped for C++ but no compatible mapped builder exists, fail at wiring/build.
4. Keep C++ node authoring practical:
   - Clear registration and lookup model.
   - No per-tick dynamic Python dispatch in hot paths.

## 2. Non-Goals

1. Changing user-facing operator semantics.
2. Replacing overload resolution logic.
3. Removing Python runtime support during migration.

## 3. Existing Seams (to reuse)

1. Decorator metadata path (`compute_node`, `node_impl`, `overloads`).
2. `PythonWiringNodeClass.create_node_builder_instance(...)` builder selection.
3. `_use_cpp_runtime.py` runtime-mode registration/swap.
4. Existing operator overload resolution (`OperatorWiringNodeClass`).

## 4. Proposed API Changes (Python)

## 4.1 `compute_node` signature

No new decorator argument is required for current migration routing.

Rules:
1. C++ lookup id is always derived from Python declaration path + function name.
2. Migration selection is controlled by the mapping table, not by per-node decorator flags.
3. `cpp_impl`-style decorator routing is out of scope for the current phase.

## 4.2 Derived C++ Node Id (Primary)

Allocate the node id from Python declaration path plus function name:
1. Start from `<module_path>.<function_name>`.
2. Remove all "private" path segments (segments starting with `_`).
3. Use remaining path segments as namespace components.
4. Use function name as the terminal key.
5. Join with `::`.

Example:
1. `hgraph._impl._operators._getattr.getattr_cs` -> `hgraph::getattr_cs`

This gives a stable id that is:
1. Deterministic from Python source location.
2. Human-readable.
3. Compatible with C++ namespace naming.

## 4.3 Python-to-C++ Mapping Table Shape

Use a nested mapping by namespace path:

```python
mapping = {
    "hgraph": {
        "getattr_cs": _hgraph.op_getattr_cs
    }
}
```

Resolution:
1. Derive id (for example `hgraph::getattr_cs`).
2. Split by `::`.
3. Walk nested dictionaries to find the callable/factory.

This keeps registration explicit while retaining source-derived identity.

## 5. Wiring-Managed Mapping (Python Only)

There is no C++ registry in this design.

All C++ mapping is owned by Python wiring logic:
1. Derive node id from Python declaration path.
2. Check runtime mode (`HGRAPH_USE_CPP` / `features.use_cpp`).
3. If C++ mode is off, always build Python node.
4. If C++ mode is on, resolve derived id in the Python mapping table.
5. If mapping exists, create C++ builder via mapped callable/factory.
6. If mapping does not exist, use existing Python builder.

Notes:
1. Mapping data lives on Python side.
2. `_hgraph` only exposes builder factories/callables; it does not own lookup state.
3. Signature compatibility validation is performed in wiring/build path before runtime execution.

## 6. Builder Selection Decision Table

Inputs:
1. `HGRAPH_USE_CPP` / `features.use_cpp`
2. Derived node id
3. Mapping entry for derived node id
4. Mapped callable supports resolved signature

Decision table:

| C++ Mode | Mapping entry for derived id | Signature supported | Builder used | Behavior |
|---|---|---|---|---|
| Off (`HGRAPH_USE_CPP=0`) | No/Yes | N/A | Python | Normal Python execution |
| On (`HGRAPH_USE_CPP=1`) | No | N/A | Python | Non-migrated node stays Python |
| On (`HGRAPH_USE_CPP=1`) | Yes | Yes | C++ compute builder | Mapped node runs in C++ |
| On (`HGRAPH_USE_CPP=1`) | Yes | No | Error | Wiring/build error (mapped but incompatible C++ builder) |

This preserves migration flexibility while avoiding silent fallback for explicitly mapped nodes.

## 7. Operator Integration

No operator API change required.

Pattern:
1. Keep operator declaration with `@operator`.
2. Keep overload implementation declared as `@compute_node(overloads=op)`.
3. Overload resolution picks the same Python overload identity; builder selection decides Python vs C++ backend.

Result:
1. Same call site.
2. Same overload semantics.
3. Backend switch is transparent to graph authors.

## 8. Runtime/Implementation Plan

## Phase 1: Metadata + Wiring
1. Add derived id generation utility (`module + function`, strip private segments, join `::`).
2. Carry derived id through wiring node objects.
3. Add mapping-table lookup by derived id during builder selection.

Exit criteria:
1. Derived id visible in wiring object inspection.
2. No behavior change for non-migrated nodes (no mapping entry).

## Phase 2: Mapping Dispatch + C++ Builder
1. Add `CppComputeNodeBuilder` (or equivalent factory path) in `_hgraph`.
2. Implement Python mapping lookup + builder dispatch in `PythonWiringNodeClass.create_node_builder_instance(...)`.
3. Validate mapped callable compatibility against resolved node signature during wiring.

Exit criteria:
1. Migrated node can instantiate C++ compute node in `HGRAPH_USE_CPP=1`.
2. Mapped-but-incompatible entry fails with clear error message.

## Phase 3: First Migrated Operator Set
1. Choose 3-5 high-frequency compute nodes/operators.
2. Implement kernels and add mapping entries.
3. Add parity tests and microbench baselines.

Exit criteria:
1. Parity pass on selected nodes in both modes.
2. Measured performance gain on representative benchmarks.

## Phase 4: Scale-out
1. Migrate additional compute nodes by ROI.
2. Keep Python implementation for semantic reference.
3. Add tooling to report `% of compute ticks handled by C++ kernels`.

Exit criteria:
1. Stable CI parity.
2. No regression in key end-to-end benchmarks.

## 9. Error Model

Wiring/build errors (not runtime silent fallback):
1. Mapping entry exists for derived id but callable cannot build resolved signature.
2. Invalid mapping shape or missing callable for a mapped id.
3. Signature mismatch between resolved node and mapped C++ builder contract.

Error message must include:
1. Python function identity (`module.qualname`).
2. Derived id (`namespace::name`).
3. Resolved signature.
4. Missing/duplicate key details.

## 10. Test and Validation Matrix

## 10.1 Parity matrix

For each migrated node:
1. Run targeted tests with `HGRAPH_USE_CPP=0`.
2. Run the same with `HGRAPH_USE_CPP=1`.
3. Assert identical outputs/events/exceptions.

Recommended command shape:

```bash
UV_CACHE_DIR=/tmp/uv-cache HGRAPH_USE_CPP=0 uv run pytest <targeted-tests> -q
UV_CACHE_DIR=/tmp/uv-cache HGRAPH_USE_CPP=1 uv run pytest <targeted-tests> -q
```

## 10.2 Regression matrix

1. `HGRAPH_USE_CPP=0` full `hgraph_unit_tests`.
2. `HGRAPH_USE_CPP=1` full `hgraph_unit_tests`.

```bash
UV_CACHE_DIR=/tmp/uv-cache HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests -q
UV_CACHE_DIR=/tmp/uv-cache HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests -q
```

## 10.3 Performance matrix

1. Microbench: per-node tick latency, p50/p95.
2. Macrobench: representative graph throughput/latency.
3. Report:
   - Python mapping hit rate for migrated nodes.
   - Total compute ticks on C++ vs Python path.

## 11. Open Implementation Choices (to finalize before coding)

1. Where mapping table should live (for example `_use_cpp_runtime.py` vs dedicated migration module).
2. Whether mapped callables are imported eagerly at startup or lazily on first use.
3. Whether to expose introspection API:
   - `list_cpp_compute_bindings()`
   - `explain_cpp_binding(node, derived_id, resolved_signature)`

## 12. Recommended Initial Scope

1. Implement Phase 1 + Phase 2 only.
2. Migrate one narrow operator family first (arithmetic TS scalar path).
3. Gate expansion on measured parity + benchmark wins.

## 13. Future Options (Not In Scope Now)

Potential extensions for later phases:
1. Module-assignment style binding:
   - `from hgraph._impl._operators._getattr import getattr_cs`
   - `getattr_cs.cpp_impl = _hgraph.op_getattr_cs`
2. Decorator-side callable binding:
   - `@compute_node(cpp_impl=_hgraph.op_getattr_cs, ...)`
3. C++-only node declarations with no Python eval implementation.

Current RFC scope remains a single routing method:
1. Derived id from Python declaration path.
2. Lookup through the mapping table.

## 14. Operator Conversion Plan

This section defines the concrete plan to migrate operators to C++ in waves.

## 14.1 Migration Unit

One migration unit is one Python implementation function that wires to an operator overload:
1. `@compute_node(overloads=...)`
2. `@generator(overloads=...)`
3. `@sink_node(overloads=...)`

`@graph(overloads=...)` wrappers are not runtime nodes and are not migration units.
They remain composition/wiring functions that call migrated leaf nodes.

## 14.2 Current Baseline

Already migrated and mapped:
1. `hgraph::const_default`
2. `hgraph::nothing_impl`
3. `hgraph::null_sink_impl`

Current mapping source:
1. `hgraph/_cpp_nodes.py`

## 14.3 Wave Order

| Wave | Scope | Target modules | Why this order |
|---|---|---|---|
| 0 | Infrastructure hardening | Mapping + builder exposure + parity harness | Keep routing stable before large rollout |
| 1 | Core foundational node leaves | `_graph_operators.py` compute/sink/generator implementations only (exclude `@graph` wrappers) | Highest fan-out, low algorithm complexity |
| 2 | Scalar arithmetic/comparison/logical | `_scalar_operators.py`, `_number_operators.py`, `_bool_operators.py`, `_enum_operators.py` | High tick frequency and measurable hot-path gains |
| 3 | Access/projection/indexing | `_getattr.py`, `_tuple_operators.py`, `_series_operators.py`, key `getitem_/getattr_` paths in `_tsd_operators.py` and `_tsb_operators.py` | Removes Python dispatch/conversion inside indexing-heavy paths |
| 4 | Collection algebra and keyed ops | `_set_operators.py`, `_tss_operators.py`, `_tsd_operators.py`, `_tsl_operators.py` | Larger performance win after view APIs are in place |
| 5 | Flow and stream control | `_flow_control.py`, `_stream_operators.py`, `_stream_analytical_operators.py`, `_time_series_properties.py`, `_tsw_operators.py` | More stateful semantics; do after primitives are stable |
| 6 | Conversion and serialization | `_conversion_operators/*`, `_json.py`, `_to_json.py`, `_to_table_impl.py` | Broad surface, lower immediate compute ROI |

## 14.4 Wave Exit Criteria

Each wave must satisfy all gates:
1. All selected operator impls mapped in `hgraph/_cpp_nodes.py`.
2. Targeted parity tests pass in both modes:
   - `HGRAPH_USE_CPP=0`
   - `HGRAPH_USE_CPP=1`
3. No mapped-node silent fallback:
   - mapped + incompatible builder must fail wiring with a clear error.
4. No hot-path Python conversion in C++ kernels unless required by semantics.
5. Full `hgraph_unit_tests` pass in C++ mode before wave merge (allow existing xfail/skip only).
6. Performance check shows neutral-or-better behavior on wave benchmark graphs.

## 14.5 Implementation Pattern Per Operator

For each operator impl:
1. Add C++ spec in `cpp/include/hgraph/nodes/ops/<family>_nodes.h`.
2. Add factory exposure symbol in `_hgraph` (for example `op_add_ts` style).
3. Register in `node_bindings.h`.
4. Add Python mapping entry in `hgraph/_cpp_nodes.py`.
5. Add/extend tests:
   - mapping registration test
   - targeted operator parity tests
   - failure-path test for mapped-incompatible signature

## 14.6 Prioritization Rules Inside a Wave

Within each wave, migrate in this priority order:
1. Operators with highest usage in existing tests/graphs.
2. Operators where Python wrapper iteration/conversion dominates runtime.
3. Operators with straightforward scalar/container semantics and low ambiguity.
4. Operators with minimal external adaptor/runtime dependencies.

## 14.7 Progress Metrics

Track progress with objective counters:
1. `migrated_impl_count / total_impl_count` (compute + generator + sink overload impls).
2. `mapped_impl_count / migrated_impl_count`.
3. C++ dispatch coverage in test runs:
   - number of mapped nodes instantiated
   - number of ticks executed by mapped nodes
4. Performance snapshots per wave:
   - p50/p95 tick latency on benchmark graphs
   - end-to-end graph runtime vs Python baseline

## 14.8 First Execution Slice (recommended now)

Start with Wave 2 subset:
1. Arithmetic: `add_`, `sub_`, `mul_`, `div_`, `floordiv_`, `mod_`.
2. Comparison: `eq_`, `ne_`, `lt_`, `le_`, `gt_`, `ge_`.
3. Logical/bitwise: `and_`, `or_`, `not_`, `bit_and`, `bit_or`, `bit_xor`.
4. Aggregates on scalar tuples/sets where already covered by tests: `min_`, `max_`, `sum_`, `mean`.

Reason:
1. Highest hot-path payoff.
2. Existing tests are strong and localized.
3. Minimal adaptor/runtime coupling.

## 14.9 Templating Refinement Policy (Arithmetic and Similar Families)

Some operator families require finer-grained specialization than one generic compute kernel.
Arithmetic is the primary case.

Policy:
1. Keep one stable Python-derived id per operator implementation function.
2. Map that id to one C++ factory symbol.
3. Inside that factory, select a specialized builder/kernel at wiring time using resolved signature/type metadata.

Specialization axes for arithmetic operators:
1. Time-series shape:
   - scalar TS vs collection/container projections.
2. Scalar category:
   - bool/integral/floating/date-time-duration paths as required by semantics.
3. Strictness/behavior flags:
   - for example divide-by-zero mode, strict validity behavior.

Implementation rules:
1. Prefer compile-time specialized kernels for hot combinations.
2. Use a bounded instantiation matrix to avoid template explosion.
3. Use explicit promotion rules (Python-parity) to route mixed numeric types to canonical kernels.
4. If mapped and unsupported specialization is encountered, fail wiring/build with a clear error (no hidden fallback).

Wave planning impact:
1. Wave 2 should be delivered as sub-waves:
   - 2a: core numeric hot-path specializations.
   - 2b: mixed-type/promotion and less-common numeric combinations.
2. Exit criteria apply per sub-wave, not only at the full Wave 2 boundary.

## 14.10 Fallback Policy (explicit)

Migration fallback policy for mapped operators:
1. If `HGRAPH_USE_CPP=0`: use Python builder.
2. If `HGRAPH_USE_CPP=1` and mapping missing: use Python builder.
3. If `HGRAPH_USE_CPP=1` and mapping exists but incompatible: wiring/build error.
4. No hidden runtime fallback for mapped nodes.
