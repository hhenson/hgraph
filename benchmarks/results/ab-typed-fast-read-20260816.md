# A/B: typed fast read for bound native scalar inputs (2026-08-16)

Commit-level A/B of the `try_native_value_memory` fast read
(`perf/typed-scalar-read` @ 4b8602e92) against its parent
(`docs/type-ops-catalogue` @ 837acbd92), same machine, same session,
back-to-back, nothing else running. 15 fresh-process samples each,
cycle scale 30, `--mode hg-cpp`.

| workload | cycles | parent (no fast read) | fast read |
|---|---|---|---|
| `type_int_std` | 600000 | 0.351s +/- 0.009s | 0.350s +/- 0.007s |
| `tick_std` | 3000000 | 0.801s +/- 0.004s | 0.802s +/- 0.010s |

Raw matrices: `matrix-20260816-090810.md` (fast read),
`matrix-20260816-091121.md` (parent).

**Verdict: parity.** The engagement probe
(`test_audit_behavior.cpp`, "the typed fast read engages") proves the
path fires through real wiring, so this is a true null result, not a
disengaged gate: the erased read it replaces measures ~6ns/op
(`hgraph_type_erasure_perf` `scalar_ts_read`) inside a ~580ns/cycle
graph, so removing most of it sits below the sampling floor. The
remaining read-side headroom identified by the RFC 0008 trace is in
the per-eval machinery around the read — the duplicated slot
resolution in `ready_to_evaluate`, the per-read route trust check,
and the passive-input full-resolution cliff — not in the payload
access itself.

Earlier same-day runs against the pinned 0.8.1 wheel (default and 10x
scale) also show x1.0 on `tick_std` / `type_int_std` /
`type_float_std`; one 10x cell that read 4% slower was traced to a
concurrently running perf harness and disappeared on a clean rerun
with `--refresh-baseline`.
