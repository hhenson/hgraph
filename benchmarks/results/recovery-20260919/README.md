# Checkpoint save and restore — scaling, 2026-09-19

> **Read with `../recovery-phases-20260919/`.** The benchmark harness made deep copies of the image
> inside the clock. The RESTORE figures below are overstated by 18-28% on macOS and 37-48% on Linux, and
> the Linux SAVE figures by 12-26%; the reading that restore is what rises on Linux does not survive the
> correction. The comparisons between variants and every flatness verdict stand.

`hgraph_unit_tests '[recovery-benchmark]'` (`tests/cpp/test_recovery_benchmark.cpp`), Release builds,
commit `c58a1a7aa`. Each row differences a completed day with and without recovery configured, so
what is timed is the capture at the end of day one and the restore at the start of day two, not the
day itself. Four workers where `dmap_` is involved. The test REQUIRES the cost per key to stay flat
from 5k to 40k keys (within 3x + 1 µs), and passed on every pass below.

Raw rows, one JSON object per size, for every pass: the `*.jsonl` files beside this one.

## macOS arm64 (Apple silicon, clang), pass 1

| Variant | Save µs/key (5k, 10k, 20k, 40k) | Restore µs/key | Bytes/key |
|---|---|---|---|
| map_ component (the bar) | 1.50, 1.57, 1.62, 1.72 | 2.70, 2.89, 2.99, 3.12 | 2.3, 2.2, 2.1, 2.0 |
| dmap_ member, in process | 1.52, 1.62, 1.69, 1.87 | 3.21, 3.23, 3.47, 3.73 | 6.9, 6.8, 5.6, 4.1 |
| dmap_ hosting a component, in process | 1.57, 1.64, 1.66, 1.88 | 3.38, 3.37, 3.57, 3.74 | 6.1, 5.3, 4.5, 4.7 |
| dmap_ member, processes | 0.94, 0.85, 0.84, 0.94 | 2.12, 1.97, 1.99, 1.93 | 6.9, 6.8, 5.6, 4.1 |
| dmap_ hosting a component, processes | 0.75, 0.84, 0.95, 0.98 | 2.61, 2.16, 2.04, 2.04 | 6.1, 5.3, 4.5, 4.7 |

## Linux x86_64 (Threadripper 9980X, GCC 14), pass 1

| Variant | Save µs/key (5k, 10k, 20k, 40k) | Restore µs/key | Bytes/key |
|---|---|---|---|
| map_ component (the bar) | 2.21, 3.11, 3.03, 2.97 | 4.04, 5.22, 6.61, 7.05 | 2.3, 2.2, 2.1, 2.0 |
| dmap_ member, in process | 2.18, 2.22, 2.43, 2.70 | 4.30, 4.86, 5.80, 5.92 | 6.9, 6.8, 5.6, 4.1 |
| dmap_ hosting a component, in process | 2.19, 2.29, 2.45, 2.58 | 4.19, 4.60, 5.81, 6.49 | 6.1, 5.3, 4.5, 4.7 |
| dmap_ member, processes | 2.45, 2.17, 1.87, 1.70 | 3.79, 3.02, 2.83, 2.83 | 6.9, 6.8, 5.6, 4.1 |
| dmap_ hosting a component, processes | 2.02, 2.17, 1.71, 1.96 | 4.33, 3.41, 3.65, 2.71 | 6.1, 5.3, 4.5, 4.7 |

## Reading it

Per-key cost at 40k keys over per-key cost at 5k (1.00 is perfectly flat, 8.00 would be
quadratic), both passes:

| Variant | macOS save | macOS restore | Linux save | Linux restore |
|---|---|---|---|---|
| `map_` component (the bar) | 1.15, 1.20 | 1.16, 1.17 | 1.34, 1.09 | **1.75, 1.90** |
| `dmap_` member, in process | 1.23, 1.21 | 1.16, 1.12 | 1.24, 1.19 | 1.38, 1.51 |
| `dmap_` hosting, in process | 1.20, 1.19 | 1.11, 1.13 | 1.18, 1.20 | 1.55, 1.56 |
| `dmap_` member, processes | 1.00, 1.29 | 0.91, 0.88 | 0.69, 1.18 | 0.75, 0.64 |
| `dmap_` hosting, processes | 1.31, 1.08 | 0.78, 0.91 | 0.97, 0.79 | 0.63, 0.74 |

* **Nothing is quadratic** (guardrail iv): the worst ratio anywhere is 1.90 against 8.00, and the
  test's bound of 3x holds on every pass.
* **Restore in one process is not flat on Linux.** The `map_` bar's restore rises 1.75-1.90x per key
  over an 8x growth in keys there, against 1.16x on macOS, and `dmap_` in process rises 1.4-1.6x.
  It is the bar that rises most, so this is `map_`'s restore, not something `dmap_` added. The
  shape fits a working set outgrowing cache -- four workers holding a quarter of the keys each
  rise less than one graph holding them all, and the process-hosted variants, with a quarter of
  the keys per address space, do not rise at all -- but that is an inference from these rows, not
  a profile. **Open:** profile `map_` restore at 40k keys on Linux before treating it as settled.
* **`dmap_` in process costs about what `map_` costs**: within ~10% on save and ~20% on restore on
  macOS, and cheaper than the bar at 20k and 40k on Linux.
* **Across processes it beats the single-graph bar** at 10k keys and above on both machines: four
  workers capture and restore in parallel, because the owner sends every checkpoint (and every
  restore) frame before it reads a reply. At 5k on Linux the process round trip still shows.
* **Hosting a component costs the same as saving whole workers.** The selection is a filter over
  the same walk.
* **Bytes per key** are 2 for `map_` and 4-7 for `dmap_`. The difference is the keyed input
  baseline: each worker image holds its boundary's copy of it as well as the child state. It does
  not grow with keys, and is the cost of a worker being restartable by itself.
* Single-threaded, the Mac is roughly 1.5-2x faster per key than the Threadripper, as expected.
