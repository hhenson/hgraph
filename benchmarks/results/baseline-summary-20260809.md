# Published hgraph 0.8.1 vs release/0.5 baseline — 2026-08-09

This is the fixed forward performance and memory baseline for the published
C++-first hgraph 0.8.1 release. It supersedes the earlier record measured from
an `hg_cpp`/current-source candidate rather than the distributed 0.8.1
artifacts. Ordinary future reports compare current source with this 0.8.1
record; they do not rerun the 0.5 line.

## Scope and identity

- Fixed baseline: published hgraph 0.8.1, tag `bc6c0a7ae1ab`.
- Historical reference: published hgraph 0.5.41, tag `v_0.5.41` at
  `27819fa6612e`, also the recorded head of `release/0.5`.
- Python reference (`upstream-py`): hgraph 0.5.41's Python runtime.
- Legacy C++ reference (`upstream-cpp`): the same 0.5.41 wheel with
  `HGRAPH_USE_CPP=true`.
- Fixed release (`release`): the exact platform hgraph 0.8.1 wheel below.
- Performance: 69 core and diagnostic scenarios, five fresh-process samples
  per supported mode, reported as median elapsed time.
- Memory: 59 profiles, three fresh-process samples per supported mode, 5 ms
  RSS sampling, plus a separate 0.8.1 `GraphDiagnostics` inspector run.
- Released-runtime caches were refreshed. No timing or memory cells were
  reused from the former `hg_cpp` candidate baseline.

| Host | Python | 0.5.41 wheel / SHA-256 | 0.8.1 wheel / SHA-256 |
|---|---|---|---|
| macOS 26.5.2 arm64, Apple M4 Max | 3.14.6 | `hgraph-0.5.41-cp312-abi3-macosx_15_0_arm64.whl`<br>`872bd8f07fcec148317786be517ba7c73bc8c6023de50a4fe88cc68c0ae1eef1` | `hgraph-0.8.1-cp312-abi3-macosx_15_0_arm64.whl`<br>`daab5629e766d26bcfa2dee0d06e27de5567485825d512edaf75e74787ad708c` |
| Linux 7.0 x86_64, Intel Core Ultra 7 155H | 3.14.4 | `hgraph-0.5.41-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl`<br>`c24da699910c3eb44019a38a0fb293557ec707b48a8e8ab5b3e5fd8b0be2db7d` | `hgraph-0.8.1-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl`<br>`c584116405c6b454220758764d3f3ad39055d2a3b43c4bd4b044fd70365025f0` |
| Windows 10 x86_64, Intel Core i9-9980HK | 3.14.7 | `hgraph-0.5.41-cp312-abi3-win_amd64.whl`<br>`74deabc55a4e5a93f3d5234ff828d499c51344924fdac303303abe8b80b224f8` | `hgraph-0.8.1-cp312-abi3-win_amd64.whl`<br>`7d30ce7b27e3add5869eeda795cbd8ce21830218258533cd8a1a963b711adcd8` |

## Performance summary

The win/near/loss counts use a 5% band. A win means 0.8.1 was more than 5%
faster; near means within 5%. Geometric means use only comparable successful
cells and compare ratios within a host.

| Host | Comparable vs Python | Geometric mean vs Python | Win / near / loss | Comparable vs legacy C++ | Geometric mean vs legacy C++ | Win / near / loss |
|---|---:|---:|---:|---:|---:|---:|
| macOS | 56 | **28.68x** | 56 / 0 / 0 | 56 | **2.45x** | 55 / 1 / 0 |
| Linux | 55 | **21.77x** | 55 / 0 / 0 | 56 | **2.23x** | 53 / 1 / 2 |
| Windows | 56 | **19.57x** | 56 / 0 / 0 | 56 | **1.87x** | 43 / 4 / 9 |

Published 0.8.1 beat the Python runtime in every comparable successful cell.
The smallest gains were in set add/remove and Python-authored graph
construction; keyed reduction, churn, switch, and mesh workloads produced the
largest gains.

The legacy-C++ losses remain concentrated at Python boundaries. Linux losses
were the Python TSD combiner (0.90x) and Python service adaptor (0.91x), with
the Python dense-TSD path inside the near band (0.99x). Windows had nine
losses; the largest were the Python sink boundary (0.54x), Python service
adaptor (0.61x), Python dense-TSD path (0.81x), set add/remove (0.82x), and
Python tick path (0.82x). macOS had no loss; its Python TSD combiner was inside
the near band (0.97x).

## Memory summary

The ready-process floor includes the interpreter, imported runtime, and
bridge. The 0.8.1 floor is lower on every host even though its first
small graph touches roughly 1.3--1.6 MiB of pages. Tiny incremental profiles
therefore often favour 0.5.41 while the fully loaded 0.8.1 process remains
smaller; page-sized deltas should not drive node-level design.

| Host | Python ready RSS | Legacy C++ ready RSS | 0.8.1 ready RSS |
|---|---:|---:|---:|
| macOS | 83.9 MiB | 86.3 MiB | **64.3 MiB** |
| Linux | 79.9 MiB | 82.8 MiB | **66.2 MiB** |
| Windows | 67.3 MiB | 69.4 MiB | **41.3 MiB** |

Across seven representative high-scale graph, sparse-capacity, churn, growth,
clear/repopulate, switch, and mesh profiles, peak-RSS reductions were:

| Host | Geometric mean reduction vs Python | Geometric mean reduction vs legacy C++ |
|---|---:|---:|
| macOS | **7.22x** | **3.93x** |
| Linux | **7.27x** | **3.94x** |
| Windows | **7.07x** | **4.08x** |

The largest sparse retained-capacity profile is the clearest comparison:

| Host | Python peak delta | Legacy C++ peak delta | 0.8.1 peak delta |
|---|---:|---:|---:|
| macOS | 618.8 MiB | 445.1 MiB | **69.0 MiB** |
| Linux | 614.8 MiB | 450.3 MiB | **70.4 MiB** |
| Windows | 609.2 MiB | 514.6 MiB | **74.5 MiB** |

Repeated wiring of the same graph is bounded in 0.8.1. For 100 small graph
executions in one process, first-to-last post-GC growth was:

| Host | Python growth | Legacy C++ growth | 0.8.1 growth |
|---|---:|---:|---:|
| macOS | 8.75 MiB | 10.22 MiB | **0.30 MiB** |
| Linux | 8.73 MiB | 10.99 MiB | **0.11 MiB** |
| Windows | 8.70 MiB | 13.88 MiB | **0.10 MiB** |

The native inspector attributed 44,188,776 reserved bytes on macOS and
45,090,920 bytes on both x86_64 hosts for the largest sparse-capacity profile. The
remaining process peak belongs to key/value/index payloads, wiring/Python
state, or allocator overhead rather than the accounted slot block.

## Diagnostics and limitations

- The Linux full performance run recorded one Python 3.14 interpreter-startup
  failure in `scheduler_conflated_fixed_tsl_std / upstream-py` sample 4
  (`Failed to import encodings module`). A focused exact-reference rerun
  completed 10/10 fresh processes at 1.550 s +/- 0.017 s. The original failure
  and focused rerun are both preserved; the failed full-run cell is excluded
  from the aggregate rather than rewritten.
- Thirteen scenarios are C++-first-only diagnostics and therefore do not enter
  the 0.5.41 comparative aggregates.
- The 5 ms RSS sampler can miss a short transient peak. Occasional low samples
  are preserved in the raw files; the three-sample median, not the minimum, is
  the reported profile value.
- Absolute timings and RSS values are not compared between hosts. Hardware,
  loaders, allocators, page sizes, and Python builds differ.

## Forward comparison policy

The benchmark orchestrators now default to `release` versus `hg-cpp`: the
exact published 0.8.1 wheel versus a wheel built from current source. Their
baseline-cache identities include the pinned wheel SHA-256 digests, so a
normal optimisation run reuses only matching 0.8.1 cells.

Do not rerun the 0.5.41 modes for an ordinary source comparison. Select
`upstream-py`, `upstream-cpp`, and `release` together only when deliberately
reconstructing this historical comparison because the profile pack, host, or
measurement policy changed. New comparisons should cite this summary and the
matching raw per-host record.

## Artifacts

Performance:

- [macOS matrix](performance-baseline-20260809-macos.md) and
  [raw samples](performance-baseline-20260809-macos.json)
- [Linux matrix](performance-baseline-20260809-linux.md) and
  [raw samples](performance-baseline-20260809-linux.json)
- [Windows matrix](performance-baseline-20260809-windows.md) and
  [raw samples](performance-baseline-20260809-windows.json)
- [Linux focused rerun](performance-rerun-scheduler-conflated-20260809-linux.md)
  and
  [raw samples](performance-rerun-scheduler-conflated-20260809-linux.json)

Memory:

- [macOS matrix](memory-baseline-20260809-macos.md) and
  [raw samples](memory-baseline-20260809-macos.json)
- [Linux matrix](memory-baseline-20260809-linux.md) and
  [raw samples](memory-baseline-20260809-linux.json)
- [Windows matrix](memory-baseline-20260809-windows.md) and
  [raw samples](memory-baseline-20260809-windows.json)

The stable filenames replace the former candidate reports. Generated
baseline-cache files remain local measurement artifacts and are intentionally
not committed.
