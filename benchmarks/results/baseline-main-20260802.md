# hg_cpp main performance and memory baseline — 2026-08-02

## Scope and reproducibility

This snapshot rebases the maintained performance and memory suites after the keyed-runtime, service/adaptor registry, and wiring-allocation work completed since the August 1 baseline.

- Measured revision: `a4fccd9b5898a739ed0f968ac71ef84ab0c765ae` (PR #265 head)
- Published `main` revision: `05bcef558d024a195da63fc9fbbede393fd357ae` (merge of PR #265 with the measured source tree unchanged)
- Source fingerprint: `b982b71ce0c817bbfd6acf1e1f05879364eaf9b9ee1e7d2b716a1653e75702b6`
- Comparison baseline: `251cd09cbe17a640cde67fb9a76196ef1f55d4a7` from 2026-08-01
- Reference hgraph: `0.5.34`
- macOS: Apple M4 Max, Python 3.14.6, Apple Clang 21
- Linux: physical `hg-linux` host (`hhenson-Darter-Pro`), Intel Core Ultra 7 155H, Python 3.14.4, GCC 14.3; timing and native microbenchmarks pinned to CPU 2
- Timing: all 68 maintained core and diagnostic scenarios, five fresh-process samples; 56 scenarios have both Python and legacy-C++ comparisons and 12 are hg_cpp-only
- Memory: all 59 maintained profiles plus the process floor, three fresh-process RSS samples at 5 ms intervals, followed by native Inspector accounting
- Native microbenchmarks: type-erasure/runtime, JSON, and stable-slot representation targets from fresh Release configures

The generated matrices report `a4fccd9b5898+dirty` because result artifacts existed in the isolated worktrees while the suites ran. The source fingerprint matches the exact commit above; no source file was changed. An initial macOS timing run overlapped unrelated compiler activity, so it was discarded and the complete matrix was rerun after the machine became quiet. Only that quiet-host matrix is used here.

The first full Linux memory run had two transient reference-process failures: one Python startup/encoding failure and one released-Python segfault. Both exact cells then passed three fresh samples. The final complete matrix reuses the 112 reference aggregates generated in that same campaign and reruns every hg_cpp and Inspector cell. It has no missing or failed cell. A separate five-process hg_cpp rerun also confirms the large sparse-capacity RSS result after one low outlier in the three-sample matrix.

Artifacts:

- [macOS timing matrix](matrix-20260802-191315.md) and [raw timing data](raw-20260802-191315.json)
- [physical-Linux timing matrix](matrix-20260802-185720-linux-main.md) and [raw timing data](raw-20260802-185720-linux-main.json)
- [macOS memory matrix](memory-matrix-20260802-185507.md) and [raw memory data](memory-raw-20260802-185507.json)
- [physical-Linux memory matrix](memory-matrix-20260802-192519-linux-main.md) and [raw memory data](memory-raw-20260802-192519-linux-main.json)
- [physical-Linux five-sample sparse-capacity check](memory-matrix-20260802-193051-linux-sparse-focused.md) and [raw focused data](memory-raw-20260802-193051-linux-sparse-focused.json)

## Executive summary

The current main revision is faster and substantially smaller than the August 1 baseline in the areas targeted since then. Across all 68 hg_cpp timing scenarios, the geometric mean improved by 10.6% on macOS and 4.5% on physical Linux. Nested-graph workloads improved by 38.9% and 28.0% respectively; keyed lifecycle improved by 27.6% and 18.5%. No broad native-path regression is visible.

Across the 56 scenarios comparable with both released implementations, hg_cpp is now 28.41x faster than Python and 2.44x faster than legacy C++ on macOS; on Linux it is 20.51x and 2.12x faster. The remaining legacy-C++ gaps are narrowly concentrated in three Linux Python-heavy workloads. macOS has one marginal 5.4% gap in the Python reducer.

The large sparse-capacity RSS delta fell from roughly 102–105 MiB to 69 MiB on both hosts. Capacity growth, clear/repopulate, bounded churn, key reactivation, keyed switches, and graph construction also became materially smaller. Most importantly, repeated identical service/adaptor wiring now reaches stable registry cardinalities after the first execution: the previous one-program-per-execution retention is gone. Its 50-execution RSS growth fell from 1.50 to 0.31 MiB on macOS and from 1.48 to 0.08 MiB on Linux.

Native measurements corroborate the structural improvements. Small graph construction fell from 32 to 17 allocations per operation. Keyed churn-map cycles fell from 330/350 allocations on macOS/Linux to 70/80, with about one quarter of the previous allocated bytes. The remaining work is therefore more focused: Linux Python/native transitions, a small cross-host Python-chain timing movement, fixed bridge/allocator memory floors, nested lifecycle allocations, and JSON temporary storage.

## Timing results

### Overall comparison

Geometric means are over the 56 scenarios supported by all three implementations. A speed-up above 1.0 means hg_cpp is faster.

| host | Python / hg_cpp | legacy C++ / hg_cpp | >5% faster than legacy | within 5% | >5% slower |
|---|---:|---:|---:|---:|---:|
| macOS / M4 Max | 28.41x | 2.44x | 55 | 0 | 1 |
| physical Linux / Core Ultra 7 | 20.51x | 2.12x | 53 | 0 | 3 |

### Speed-up over legacy C++ by workload group

| group | macOS | Linux |
|---|---:|---:|
| Graph construction | 6.15x | 6.23x |
| Scheduler | 1.94x | 1.59x |
| Python boundary | 1.44x | 1.53x |
| Value types | 1.86x | 1.73x |
| TSD — dense | 1.79x | 1.41x |
| TSD — sparse | 2.67x | 2.11x |
| TSD — key lifecycle | 4.13x | 3.63x |
| Reduce | 1.56x | 1.24x |
| Nested graphs | 3.47x | 3.43x |
| Services | 2.43x | 2.08x |
| Adaptors | 1.61x | 1.40x |

### Change from the August 1 hg_cpp baseline

| group | macOS change | Linux change |
|---|---:|---:|
| All 68 scenarios | 10.6% faster | 4.5% faster |
| Nested graphs | 38.9% faster | 28.0% faster |
| TSD — key lifecycle | 27.6% faster | 18.5% faster |
| Graph construction | 16.2% faster | 9.1% faster |
| TSD — sparse | 13.6% faster | 8.0% faster |

At the five-percent threshold, macOS has 30 improvements, 37 unchanged scenarios, and one regression. Linux has 18 improvements, 43 unchanged scenarios, and seven regressions; its largest is 7.9%, and the seven are small Python/value/service paths rather than keyed or nested hot paths. Representative gains include:

| scenario | macOS | Linux |
|---|---:|---:|
| `switch_alternating_branch_sizes_std` | 44.4% faster | 31.5% faster |
| `switch_keyed_collection_std` | 38.2% faster | 29.1% faster |
| `tsd_clear_repopulate_std` | 41.1% faster | 31.4% faster |
| `tsd_key_reactivation_std` | 43.1% faster | 30.3% faster |
| `tsd_churn_map_std` | 39.7% faster | 27.2% faster |

The only greater-than-five-percent movement reproduced on both hosts is the five-node Python chain: `tick_py` is 6.1% slower than August 1 on macOS and 7.9% slower on Linux. It remains faster than legacy C++ on both hosts (0.0237 versus 0.0253 seconds on macOS; 0.0355 versus 0.0412 seconds on Linux), so this is a focused watch item rather than an executor-wide regression.

### Remaining legacy-C++ gaps

| host | scenario | legacy C++ | hg_cpp | hg_cpp slower by |
|---|---|---:|---:|---:|
| macOS | `reduce_tsd_python_combiner` | 0.0720 s | 0.0759 s | 5.4% |
| Linux | `reduce_tsd_python_combiner` | 0.1034 s | 0.1325 s | 28.2% |
| Linux | `tsd_dense_py` | 0.1757 s | 0.2022 s | 15.1% |
| Linux | `service_adaptor_py` | 0.0587 s | 0.0671 s | 14.3% |

The platform split remains strong evidence that the next timing work belongs around Python/native transition cost and Linux call overhead, not in the native scheduler or keyed lifecycle machinery.

## Memory results

### Process floor

| host | interpreter + psutil | hg_cpp ready RSS | hg_cpp runtime-load delta | August 1 ready RSS |
|---|---:|---:|---:|---:|
| macOS | 22.6 MiB | 64.4 MiB | 41.8 MiB | 64.2 MiB |
| physical Linux | 17.5 MiB | 71.3 MiB | 53.8 MiB | 72.5 MiB |

The macOS process floor is stable. The Linux ready floor moved down by 1.2 MiB and is no longer worsening, though it remains environment- and loader-sensitive. Current peak deltas are measured from each mode's own ready state, so cross-implementation comparisons below do not include those import/load differences.

### Representative current peak RSS deltas

Values are MiB above each mode's own post-import/pre-run state.

| profile | mac Python | mac legacy | mac hg_cpp | Linux Python | Linux legacy | Linux hg_cpp |
|---|---:|---:|---:|---:|---:|---:|
| Large static construction | 19.0 | 21.6 | 10.2 | 19.5 | 20.8 | 7.5 |
| Identical graph repeated 100 times | 9.0 | 11.0 | 2.0 | 9.3 | 11.8 | 0.8 |
| Service/adaptor graph repeated 50 times | 4.6 | 5.8 | 2.5 | 4.9 | 6.3 | 0.9 |
| Sparse retained capacity — large | 616.8 | 445.1 | 69.2 | 614.7 | 450.4 | 68.9 |
| Bounded key churn — long | 36.0 | 10.0 | 2.5 | 31.0 | 9.6 | 1.2 |
| Clear/repopulate — long | 100.2 | 75.7 | 5.6 | 116.0 | 65.4 | 4.4 |
| Keyed switch — large | 38.0 | 8.7 | 3.5 | 33.0 | 8.8 | 2.0 |
| Mesh — large | 10.0 | 5.2 | 3.3 | 10.1 | 5.4 | 1.6 |
| Python service adaptor — large | 1.1 | 1.3 | 2.3 | 1.2 | 1.4 | 1.0 |

The focused five-process Linux sparse-capacity rerun measured 68.914, 68.941, 68.938, 68.941, and 68.918 MiB. This confirms the 68.9 MiB median and identifies the earlier 38.3 MiB sample as a one-off RSS/allocator observation rather than a lower steady result.

The large sparse case is now about 11% of Python and 15% of legacy C++ on both hosts. Duration-sensitive profiles remain flat across short, medium, and long cases. The macOS Python service-adaptor profile still exposes a fixed small-graph floor not reproduced on Linux; it does not grow with execution duration or registry cardinality.

### Change from August 1

| profile | macOS | Linux |
|---|---:|---:|
| Large static construction | 10.5 -> 10.2 MiB (-2.7%) | 9.4 -> 7.5 MiB (-20.9%) |
| Sparse retained capacity — large | 102.1 -> 69.2 MiB (-32.2%) | 105.0 -> 68.9 MiB (-34.4%) |
| Monotonic capacity growth — long | 22.5 -> 15.9 MiB (-29.2%) | 21.9 -> 8.8 MiB (-59.7%) |
| Clear/repopulate — long | 7.2 -> 5.6 MiB (-23.1%) | 6.2 -> 4.4 MiB (-29.6%) |
| Bounded churn — long | 2.9 -> 2.5 MiB (-15.0%) | 1.8 -> 1.2 MiB (-33.6%) |
| Keyed switch — large | 4.2 -> 3.5 MiB (-16.3%) | 2.9 -> 2.0 MiB (-31.1%) |
| Service/adaptor repeated 50 times | 3.6 -> 2.5 MiB (-29.4%) | 2.3 -> 0.9 MiB (-62.9%) |
| Service/adaptor first-to-last growth | 1.50 -> 0.31 MiB (-79.1%) | 1.48 -> 0.08 MiB (-94.7%) |

The only greater-than-20% Linux increases are the small and medium dynamic-TSL profiles, whose absolute deltas rose to 3.0 and 3.3 MiB while the large profile fell from 4.5 to 3.9 MiB. That inverse scaling points to fixed process/allocator noise rather than capacity-proportional storage growth. No macOS profile regressed by more than 20%.

### Service/adaptor registry growth is resolved

For the 50-execution identical service/adaptor profile, both hosts have the same registry snapshot after execution 1 and execution 50:

| host | node runtime types | graph programs | graph runtime types | executor runtime types | all type records | first-to-last RSS growth |
|---|---:|---:|---:|---:|---:|---:|
| macOS | 12 -> 12 | 2 -> 2 | 4 -> 4 | 1 -> 1 | 155 -> 155 | 0.31 MiB |
| physical Linux | 12 -> 12 | 2 -> 2 | 4 -> 4 | 1 -> 1 | 155 -> 155 | 0.08 MiB |

This closes the structural retention identified in the August 1 report. The remaining RSS movement is a small allocator/process plateau, not one retained graph program per execution.

Repeated ordinary graph execution is also bounded. Over 100 executions, first-to-last RSS growth is 0.45 MiB on macOS and 0.12 MiB on Linux, while Inspector dynamic storage is unchanged at about 4.5 KiB. The macOS value is above August 1's 0.25 MiB but is not accompanied by native-accounted or registry growth; retain it as an allocator watch cell.

## Native microbenchmark signals

All three Release executables completed on both hosts. Linux measurements were pinned to CPU 2. Absolute nanoseconds are not compared across architectures; allocation changes are directly useful.

| benchmark signal | macOS | physical Linux | change from August 1 |
|---|---:|---:|---|
| Small graph construct/destroy | 574 ns; 17 alloc/op; 1.63 KiB/op | 617 ns; 17 alloc/op; 1.61 KiB/op | 32 -> 17 allocations; 3.28/3.25 -> 1.63/1.61 KiB |
| Native churn TSD map cycle | 16.85 us; 70 alloc/op; 7.23 KiB/op | 26.59 us; 80 alloc/op; 7.50 KiB/op | 330/350 -> 70/80 allocations; 27.8/37.7 -> 16.9/26.6 us |
| Evaluation profiler, disabled | 36.0 ns/cycle | 47.1 ns/cycle | Essentially unchanged |
| Evaluation profiler, enabled | 161.6 ns/cycle | 196.3 ns/cycle | Essentially unchanged |
| Alternating switch lifecycle | 66.2 us; 1,112 alloc/op | 102.3 us; 1,144 alloc/op | Down from 77.1/113.3 us and 1,269/1,307 allocations |
| JSON decode + extract | 17.9 us; 335 kB/tick | 18.9 us; 335 kB/tick | Stable |
| JSON decode + semantic equality | 90.0 us; 737 kB/tick | 121.5 us; 737 kB/tick | Stable within normal host variance |

The production tracked packed-slot representation remains 33.253 bytes per slot. The alternative encodings still trade representation constraints and storage for isolated-operation speed; nothing in the new end-to-end data justifies changing that production choice.

## Recommended focus

1. Profile the three Linux Python-heavy legacy gaps. Measure Python/native transition count and phase time for the Python TSD combiner, dense Python map, and Python service adaptor. Preserve the coarse one-guard-per-executor-phase design unless measurements identify a specific phase boundary that should change.
2. Recheck `tick_py` with a 15-process same-revision comparison before changing code. Its 6–8% movement reproduces across hosts, but hg_cpp remains faster than legacy C++ and the native scheduler groups improved or stayed flat.
3. Quantify the fixed Python-bridge/allocator floor. The macOS service-adaptor large case is 2.3 MiB versus 1.1/1.3 MiB for the references, while Linux is smaller than both; repeated ordinary graphs also show a small macOS-only RSS step with flat Inspector and registry state.
4. Continue reducing nested lifecycle allocations. Alternating switch lifecycle improved, but roughly 1,100 allocations and 128–136 KiB per operation remain the largest obvious native lifecycle cost after churn-map allocation reuse was fixed.
5. Reduce JSON temporary bytes, especially semantic equality and re-encoding. Runtime is stable, but approximately 737 kB of allocation per 18 kB semantic-equality payload remains disproportionate.
6. Reconcile RSS with Inspector for large retained capacity. Inspector accounts for about 42–43 MiB while the process peak is about 69 MiB; separating allocator slack, Python bridge state, and currently untracked native structures would make future ownership regressions easier to localize.
7. Optimize enabled profiling only if production workloads keep it on. Disabled overhead remains low; enabled collection is still roughly 4.2–4.5 times the isolated disabled cycle.

The August 1 priorities for service/adaptor registry canonicalization and churn-map reconstruction allocations are now complete. The earlier Linux sparse-reduce watch item is also no longer a current regression: the maintained sparse group improved by 8.0% on Linux and 13.6% on macOS.

## Bottom line

Main is in a clearly better position than the August 1 baseline. The recent work produced large, cross-host gains in keyed lifecycle, nested graphs, retained-capacity memory, construction allocation, and service/adaptor process lifetime without a broad executor or native-runtime regression. The next useful work is narrow and measurable: Linux Python call boundaries, the small Python-chain movement, fixed bridge/allocator memory, remaining nested lifecycle allocation, and JSON temporary storage.
