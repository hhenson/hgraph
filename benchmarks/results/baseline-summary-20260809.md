# Final hg_cpp comparative baseline — 2026-08-09

This is the final historical performance and memory cut for the C++-first
implementation while it was known as `hg_cpp`. The implementation had already
been ported into `hhenson/hgraph` when these measurements were taken, so the
candidate revision below is in that repository. The `hg-cpp` label is retained
in the benchmark schema to distinguish the ported implementation from the
released Python-first hgraph baselines.

## Scope and method

- Candidate: hgraph revision `17a5b81bca91`, Release build.
- Python reference (`upstream-py`): released hgraph 0.5.41.
- Legacy C++ reference (`upstream-cpp`): hgraph 0.5.41 with its C++ path.
- Performance: 69 core and diagnostic scenarios, five fresh-process samples
  per supported mode, median elapsed time.
- Memory: 59 profiles, three fresh-process samples per supported mode, 5 ms
  RSS sampling, plus a separate native `GraphDiagnostics` inspector run.
- Baseline caches were refreshed. No upstream measurements were reused.
- Speed-up aggregates are geometric means of within-workload ratios. Absolute
  timings are not compared across hosts with different hardware.

| Host | CPU | Python | Compiler |
|---|---|---|---|
| macOS 26.5.2 arm64 | Apple M4 Max | 3.14.6 | Apple clang 21.0.0 |
| Linux 7.0 x86_64 | Intel Core Ultra 7 155H | 3.14.4 | GCC 14.3.0 |
| Windows 10 x86_64 | Intel Core i9-9980HK | 3.14.7 | MSVC 19.51.36252 |

## Performance summary

The win/near/loss counts use a 5% band. A win means the `hg-cpp` candidate was
more than 5% faster; near means within 5%.

| Host | Comparable vs Python | Geometric mean vs Python | Win / near / loss | Comparable vs legacy C++ | Geometric mean vs legacy C++ | Win / near / loss |
|---|---:|---:|---:|---:|---:|---:|
| macOS | 56 | **28.88x** | 56 / 0 / 0 | 56 | **2.45x** | 55 / 1 / 0 |
| Linux | 55 | **21.26x** | 55 / 0 / 0 | 56 | **2.18x** | 53 / 0 / 3 |
| Windows | 56 | **19.59x** | 56 / 0 / 0 | 56 | **1.84x** | 44 / 4 / 8 |

The candidate beat the Python implementation in every comparable successful
cell. The smallest gains were the set add/remove workload (1.25x–1.64x) and
Python-authored construction (1.47x–2.30x). Heavy keyed operations produced the
largest gains: explicit-key-set, churn/reduce, sparse reduce, and keyed switch
were commonly tens of times faster.

The remaining legacy-C++ losses are concentrated at Python boundaries, not in
the native graph/runtime paths. Linux losses were the Python TSD combiner
(0.85x), Python service adaptor (0.91x), and Python dense-TSD path (0.94x).
Windows had eight losses; the largest were the Python sink boundary (0.56x),
Python service adaptor (0.61x), Python dense-TSD path (0.82x), Python tick path
(0.83x), and Python TSD combiner (0.84x). macOS had no loss outside the 5%
near band.

## Memory summary

The ready-process floor includes the interpreter, imported runtime, and bridge.
The candidate floor was lower on every host even though its first small graph
touches roughly 1.3–1.6 MiB of pages. Consequently, tiny scalar profiles often
show a larger *increment* for the candidate while the fully loaded candidate
process remains smaller; page-sized deltas should not drive node-level design.

| Host | Python ready RSS | Legacy C++ ready RSS | hg_cpp ready RSS |
|---|---:|---:|---:|
| macOS | 83.9 MiB | 86.5 MiB | **64.2 MiB** |
| Linux | 79.1 MiB | 82.1 MiB | **71.8 MiB** |
| Windows | 67.3 MiB | 69.5 MiB | **41.4 MiB** |

Across seven representative high-scale graph, keyed-collection, churn, growth,
switch, and mesh profiles, peak-RSS improvements were consistent:

| Host | Geometric mean reduction vs Python | Geometric mean reduction vs legacy C++ |
|---|---:|---:|
| macOS | **7.14x** | **3.88x** |
| Linux | **10.87x** | **5.93x** |
| Windows | **7.06x** | **4.04x** |

The largest sparse retained-capacity profile is the clearest cross-platform
comparison:

| Host | Python peak delta | Legacy C++ peak delta | hg_cpp peak delta |
|---|---:|---:|---:|
| macOS | 618.2 MiB | 454.8 MiB | **69.2 MiB** |
| Linux | 614.9 MiB | 450.4 MiB | **68.9 MiB** |
| Windows | 609.7 MiB | 513.4 MiB | **74.2 MiB** |

Repeated wiring of the same graph is now bounded in the candidate. For 100
small graph executions in one process, first-to-last post-GC growth was:

| Host | Python growth | Legacy C++ growth | hg_cpp growth |
|---|---:|---:|---:|
| macOS | 8.55 MiB | 10.23 MiB | **0.44 MiB** |
| Linux | 8.76 MiB | 11.04 MiB | **0.12 MiB** |
| Windows | 8.70 MiB | 13.58 MiB | **0.08 MiB** |

The native inspector attributed 44,188,776 reserved bytes on macOS and
45,090,920 bytes on both x86_64 hosts for the largest sparse-capacity profile.
That explains roughly 58%–62% of the candidate RSS peak; the remainder is
payload, Python/wiring state, and allocator overhead.

## Diagnostics and limitations

- The Linux full run recorded two Python 3.14 interpreter-startup failures in
  `tsd_dense_strkeys_std / upstream-py` (`Failed to import encodings module`).
  The other three samples succeeded, and an immediate isolated rerun completed
  10/10 fresh processes at 3.951 s +/- 0.026 s. The original failure and the
  focused rerun are both preserved.
- The Windows full memory run reached the default 600-second limit while
  inspecting the largest sparse-capacity profile. A focused rerun with an
  1,800-second limit completed successfully and reported 45,090,920 bytes. The
  original timeout remains in the full report.
- Windows report metadata could not identify the compiler automatically. The
  candidate was built with MSVC 19.51.36252; the exact Git revision is recorded
  in every result.
- Thirteen scenarios are candidate-only diagnostics and therefore do not enter
  the comparative aggregates.

## Artifacts

Performance:

- [macOS matrix](performance-baseline-20260809-macos.md) and
  [raw samples](performance-baseline-20260809-macos.json)
- [Linux matrix](performance-baseline-20260809-linux.md) and
  [raw samples](performance-baseline-20260809-linux.json)
- [Windows matrix](performance-baseline-20260809-windows.md) and
  [raw samples](performance-baseline-20260809-windows.json)
- [Linux focused rerun](performance-rerun-tsd-dense-strkeys-20260809-linux.md)
  and [raw samples](performance-rerun-tsd-dense-strkeys-20260809-linux.json)

Memory:

- [macOS matrix](memory-baseline-20260809-macos.md) and
  [raw samples](memory-baseline-20260809-macos.json)
- [Linux matrix](memory-baseline-20260809-linux.md) and
  [raw samples](memory-baseline-20260809-linux.json)
- [Windows matrix](memory-baseline-20260809-windows.md) and
  [raw samples](memory-baseline-20260809-windows.json)
- [Windows extended-timeout rerun](memory-rerun-tsd-sparse-large-capacity-20260809-windows.md)
  and [raw samples](memory-rerun-tsd-sparse-large-capacity-20260809-windows.json)

These stable filenames replace the historical timestamped reports previously
stored in `benchmarks/results/`. Generated baseline-cache files are intentionally
not committed.
