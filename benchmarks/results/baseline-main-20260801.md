# hg_cpp main performance and memory baseline — 2026-08-01

## Scope and reproducibility

This snapshot rebases the maintained performance and memory suites after the executor and lifecycle cleanup merged to `main`.

- Revision: `251cd09cbe17a640cde67fb9a76196ef1f55d4a7`
- Source fingerprint: `057ad8fb69455f1b67fbc0a6c010b66e7fd83e55fd8957d85e3b5f65902325ea`
- Reference hgraph: `0.5.34`
- macOS: Apple M4 Max, Python 3.14.6, Apple Clang 21
- Linux: physical `hg-linux` host (`hhenson-Darter-Pro`), Intel Core Ultra 7 155H, Python 3.14.4, GCC 14.3; timing and native microbenchmarks pinned to CPU 2
- Timing: all 68 maintained core and diagnostic scenarios, five fresh-process samples; 56 scenarios have both Python and legacy-C++ comparisons and 12 are hg_cpp-only
- Memory: all 59 maintained profiles plus the process floor, three fresh-process RSS samples at 5 ms intervals, followed by native Inspector accounting
- Native microbenchmarks: type erasure/runtime, JSON, and stable-slot representation targets from a fresh Release configure

The generated matrices report `251cd09cbe17+dirty` because result artifacts were created in the worktrees while the suites ran. The source fingerprint matches the exact commit above; no source changes were present. The first macOS reference run found a stale hgraph 0.5.31 baseline environment. It was upgraded and the complete timing and memory matrices were rerun; only the 0.5.34 artifacts below are used in this report.

Artifacts:

- [macOS timing matrix](matrix-20260801-120940.md) and [raw timing data](raw-20260801-120940.json)
- [physical-Linux timing matrix](matrix-20260801-114632-linux-main.md) and [raw timing data](raw-20260801-114632-linux-main.json)
- [macOS memory matrix](memory-matrix-20260801-121856.md) and [raw memory data](memory-raw-20260801-121856.json)
- [physical-Linux memory matrix](memory-matrix-20260801-120408-linux-main.md) and [raw memory data](memory-raw-20260801-120408-linux-main.json)

All timing and memory validation cells passed. No upstream baseline cell was reused.

## Executive summary

The current runtime is materially faster than released Python hgraph and, in almost every workload, faster than the equivalent legacy C++ path. Across the 56 comparable timing scenarios, hg_cpp is 25.70x faster than Python and 2.24x faster than legacy C++ on the M4 Max; on physical Linux it is 19.22x and 1.99x faster respectively.

The cleanup produced its clearest memory gains in graph construction and repeated graph lifetime. On macOS, the large construction delta fell from 19.0 to 10.5 MiB and 100 repeated identical graph executions fell from 12.3 to 1.7 MiB. The repeated graph's first-to-last growth fell from 10.7 to 0.25 MiB. Bounded key churn, clear/repopulate, key reactivation, switches, meshes, and dynamic TSL storage all remain duration- or cardinality-bounded.

The hot-path timing aggregate is essentially unchanged from the July 31 `main` baseline: 0.994x on macOS and 1.010x on Linux. The initial five-sample matrices identified three small keyed/value movements worth checking. A subsequent 15-sample, same-affinity comparison narrowed that to one Linux-specific signal: sparse reduce is 7.7% slower, while the other movements are below 5%. Linux also retains three Python-heavy scenarios where legacy C++ is still faster.

The main unresolved lifetime issue is structural rather than a live-graph leak: repeated identical service/adaptor wiring continues to add runtime type and graph-program registry records linearly. RSS growth is much smaller than before, but registry identity/deduplication still needs attention.

## Timing results

### Overall comparison

Geometric means are over the 56 scenarios supported by all three implementations. A ratio above 1.0 means hg_cpp is faster.

| host | Python / hg_cpp | legacy C++ / hg_cpp | >5% faster than legacy | within 5% | >5% slower |
|---|---:|---:|---:|---:|---:|
| macOS / M4 Max | 25.70x | 2.24x | 55 | 1 | 0 |
| physical Linux / Core Ultra 7 | 19.22x | 1.99x | 51 | 2 | 3 |

### Speed-up over legacy C++ by workload group

| group | macOS | Linux |
|---|---:|---:|
| Graph construction | 5.39x | 5.56x |
| Scheduler | 1.99x | 1.63x |
| Python boundary | 1.60x | 1.57x |
| Value types | 1.96x | 1.76x |
| TSD — dense | 1.80x | 1.39x |
| TSD — sparse | 2.49x | 1.93x |
| TSD — key lifecycle | 3.07x | 2.94x |
| Reduce | 1.52x | 1.24x |
| Nested graphs | 2.16x | 2.46x |
| Services | 2.38x | 2.10x |
| Adaptors | 1.68x | 1.45x |

Construction, keyed lifecycle, nested graphs, and services are now strong relative to both references. The Python boundary remains faster than legacy C++ as a group, but has less headroom than native runtime work.

### Remaining legacy-C++ gaps on Linux

| scenario | legacy C++ | hg_cpp | legacy / hg_cpp | interpretation |
|---|---:|---:|---:|---|
| `reduce_tsd_python_combiner` | 0.104 s | 0.134 s | 0.774x | hg_cpp is 29% slower |
| `tsd_dense_py` | 0.174 s | 0.199 s | 0.878x | hg_cpp is 14% slower |
| `service_adaptor_py` | 0.059 s | 0.064 s | 0.915x | hg_cpp is 9% slower |

On macOS, `reduce_tsd_python_combiner` is within 4% of legacy C++ and the other two are faster. This makes the Linux result a useful target for profiling Python/native transition cost, GIL phase scope, and platform-specific call overhead rather than evidence of a general executor regression.

### Change from the July 31 hg_cpp baseline

The 68-scenario hg_cpp geometric mean is effectively flat: macOS is 0.6% slower and Linux is 1.0% faster. The previous Linux run used a wider CPU affinity set, while the new baseline is pinned to CPU 2, so isolated Linux-only changes should not be treated as regressions without a same-affinity rerun.

The initial five-sample matrices showed three scenarios moving backwards by more than 5% on both hosts:

| scenario | macOS change | Linux change |
|---|---:|---:|
| `tss_add_remove_std` | -5.6% | -8.5% |
| `tsd_sparse_reduce_std` | -5.5% | -8.1% |
| `tsd_explicit_key_set_std` | -5.6% | -5.5% |

A follow-up used 15 fresh processes for both `9db88d02` and the current revision, with Linux pinned to CPU 2 for both sides:

| scenario | macOS change | Linux change |
|---|---:|---:|
| `tss_add_remove_std` | -3.1% | -4.1% |
| `tsd_sparse_reduce_std` | +2.5% | -7.7% |
| `tsd_explicit_key_set_std` | -3.2% | -2.7% |

Here a minus sign means slower and a plus sign means faster. The higher-sample comparison does not reproduce a general cross-host regression. Only Linux sparse reduce remains above 5%; native decomposition attributes roughly 4.8% to sparse source/update work and 2.5% to the reducer, with zero steady-state allocations in both paths.

## Memory results

### Process floor

| host | interpreter + psutil | hg_cpp ready RSS | hg_cpp runtime-load delta | change from July 31 ready RSS |
|---|---:|---:|---:|---:|
| macOS | 22.6 MiB | 64.2 MiB | 41.6 MiB | +0.7 MiB |
| physical Linux | 17.5 MiB | 72.5 MiB | 55.0 MiB | +7.2 MiB |

The macOS load floor is stable. The Linux floor increase is large enough to investigate, but should first be isolated as a loader/dependency/environment change: it is visible before graph execution and the current run came from a fresh environment. Because the ready floor moved, historical Linux peak *deltas* cannot be read as pure core-memory savings. Current same-run cross-implementation comparisons remain valid.

### Representative current peak RSS deltas

Values are MiB above each mode's own post-import/pre-run state.

| profile | mac Python | mac legacy | mac hg_cpp | Linux Python | Linux legacy | Linux hg_cpp |
|---|---:|---:|---:|---:|---:|---:|
| Large static construction | 18.9 | 20.5 | 10.5 | 19.6 | 20.8 | 9.4 |
| Identical graph repeated 100 times | 8.9 | 10.8 | 1.7 | 9.3 | 11.8 | 0.9 |
| Sparse retained capacity — large | 616.9 | 446.4 | 102.1 | 615.4 | 450.4 | 105.0 |
| Bounded key churn — long | 36.1 | 10.1 | 2.9 | 31.0 | 9.6 | 1.8 |
| Clear/repopulate — long | 101.0 | 74.3 | 7.2 | 106.1 | 65.4 | 6.2 |
| Keyed switch — large | 38.0 | 8.7 | 4.2 | 33.0 | 8.7 | 2.9 |
| Mesh — large | 10.0 | 5.2 | 3.5 | 10.2 | 5.4 | 1.8 |
| Python service adaptor — large | 1.1 | 1.4 | 2.4 | 1.3 | 1.4 | 1.0 |

The large sparse-capacity case is now about 17% of Python and 23% of legacy C++ on both hosts. Clear/repopulate and bounded churn remain flat as duration grows, which is the key lifetime invariant. Repeated identical normal graphs also plateau: first-to-last growth is 0.25 MiB on macOS and 0.11 MiB on Linux over 100 executions.

The remaining small-workload floor is visible on macOS: scalar loops and small service/adaptor graphs generally consume 1.4–2.1 MiB above ready state, more than the references. Larger graphs amortize that cost and usually become smaller than both references. This looks like fixed runtime/allocator state rather than duration-proportional retention.

### Improvements since July 31

The most comparable historical evidence is macOS because its ready floor stayed stable:

| profile | July 31 hg_cpp | current hg_cpp | change |
|---|---:|---:|---:|
| Large static construction peak delta | 19.0 MiB | 10.5 MiB | -44.8% |
| Identical graph repeated 100 times | 12.3 MiB | 1.7 MiB | -86.4% |
| Repeated graph first-to-last growth | 10.7 MiB | 0.25 MiB | -97.7% |
| Service/adaptor repeated 50 times | 5.0 MiB | 3.6 MiB | -29.2% |
| Service/adaptor first-to-last growth | 3.1 MiB | 1.5 MiB | -51.0% |

The keyed and nested hot-loop profiles are otherwise broadly flat on macOS, indicating that the cleanup primarily improved construction and process-lifetime behavior rather than moving memory into the steady-state hot path.

### Retained runtime registry growth

Normal identical graph construction deduplicates correctly: one, ten, and 100 executions all retain one graph program and two graph runtime types. Ten intentionally novel graph programs retain ten programs and twenty graph types, as expected.

Repeated identical service/adaptor wiring does not yet deduplicate:

| executions | node runtime types | graph programs | graph runtime types | all type records | first-to-last RSS growth |
|---:|---:|---:|---:|---:|---:|
| 1 | 15 | 2 | 4 | 113 | 0.0 MiB |
| 10 | 87 | 11 | 22 | 203 | 0.3 MiB |
| 50 | 407 | 51 | 102 | 603 | 1.5 MiB |

This is process-lifetime registry retention, not live graph instances. The RSS cost has improved, but the one-program-per-execution shape remains the clearest ownership/deduplication issue in the new baseline.

## Native microbenchmark signals

The native executables completed successfully on both hosts. Absolute nanoseconds are not compared across architectures; allocations and relative shapes are consistent.

| benchmark signal | macOS | physical Linux | observation |
|---|---:|---:|---|
| Small graph construct/destroy | 990 ns; 32 alloc/op; 3.28 KiB/op | 973 ns; 32 alloc/op; 3.25 KiB/op | Stable and compact |
| Native churn TSD map cycle | 27.8 us; 330 alloc/op; 29.96 KiB/op | 37.7 us; 350 alloc/op; 31.13 KiB/op | Largest clear native allocation hotspot |
| Evaluation profiler, disabled | 35.8 ns/cycle | 46.9 ns/cycle | Low disabled overhead |
| Evaluation profiler, enabled | 161.4 ns/cycle | 195.6 ns/cycle | 4.5x / 4.2x the disabled cycle |
| Alternating switch lifecycle | 77.1 us; 1,269 alloc/op | 113.3 us; 1,307 alloc/op | Lifecycle remains allocation-heavy |
| JSON decode + extract | 18.7 us; 335 KiB/tick | 19.2 us; 335 KiB/tick | Payload bytes dominate transient accounting |
| JSON decode + semantic equality | 91.8 us; 737 KiB/tick | 119.9 us; 737 KiB/tick | Highest JSON temporary-allocation path |

The production tracked packed-slot representation remains memory-efficient at 33.253 bytes/slot. Alternative state/tag encodings are faster in some isolated slot operations but consume more storage or change representation constraints. Revisit this only with an end-to-end keyed workload and lifetime proof, not from the isolated microbenchmark alone.

## Recommended focus

1. Profile the Linux sparse-reduce regression. The source/update and reducer microbenchmarks are both allocation-free, so focus on slot lookup/dispatch, changed-key iteration, and generated code rather than allocator traffic. Keep set add/remove and explicit-key-set map as watch-list cells; their 15-sample changes are below 5%.
2. Close the Linux Python-heavy gaps. Measure Python/native transitions and time spent inside each complete executor phase for the Python combiner, dense Python map, and Python service adaptor. Preserve the coarse phase-level GIL-guard design unless evidence identifies a specific phase that should be split.
3. Deduplicate or release service/adaptor wiring registry records. The normal graph path proves that repeated programs can stabilize; service/adaptor identities should be made equally canonical without introducing process globals or bypassing normal graph teardown.
4. Explain the Linux ready-RSS increase before optimizing graph deltas. Compare loaded libraries and dependency versions in an otherwise identical environment, then rerun the process-floor probe. Also quantify the fixed 1.4–2.1 MiB small-workload floor on macOS.
5. Reduce churn-map reconstruction allocations. The native benchmark's 330–350 allocations and roughly 30 KiB per cycle make slot/child-graph reuse a higher-value target than nanosecond-level visitor dispatch.
6. Reduce JSON temporary storage, particularly semantic equality and re-encoding. These paths allocate hundreds of KiB per 18 KiB payload even though their allocation counts are modest.
7. Optimize enabled profiling only if production workloads keep it on. The disabled path is cheap; enabled collection adds about 4.2–4.5x per-cycle overhead in the isolated benchmark.
8. Continue expanding Inspector coverage and thresholds. Native accounting now follows planned and dynamic graph storage well, but RSS still includes Python bridge, loader, allocator, and untracked link/wiring state. Baselines should retain both views rather than treating either one as total memory.

## Bottom line

`main` is in a substantially better construction and lifecycle-memory position without a broad throughput regression. The runtime is roughly 2x faster than legacy C++ across the maintained matrix and dramatically faster than released Python hgraph. The next work should be targeted: the Linux sparse-reduce path, Linux Python-call overhead, service/adaptor registry canonicalization, and the fixed/import memory floors. The bounded-memory invariants for churn, reactivation, clear/repopulate, switches, meshes, and repeated ordinary graph execution are holding.
