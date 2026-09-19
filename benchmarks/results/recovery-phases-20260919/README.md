# Save and restore — where a recovered day spends its time, 2026-09-19

`hgraph_unit_tests '[recovery-benchmark]'` (`tests/cpp/test_recovery_benchmark.cpp`), Release builds,
two passes per machine. It follows `../recovery-20260919/`, whose open question was why restore in
one process is not flat on Linux, and answers part of it.

Raw rows, one JSON object per size, for every pass: the `*.jsonl` files beside this one. The
`*-copies-inside-clock-*` files are the same benchmark BEFORE the harness correction below, with the
phase split already in place, so the two can be compared row for row. Linux rows are `main`
(`928b2681e`) plus the benchmark change, GCC 14, on a private validation host (Threadripper 9980X);
macOS rows are the same benchmark file built on the `fix/slot-growth-commutes-with-flush` checkout
(`22f79a970`, which does not touch the measured path), Apple silicon, clang.

## What changed in the benchmark

1. **Each recovered day is split at the two moments recovery calls out.** `load` fires when the graph
   is wired and built; `commit` fires after the executor has captured AND stopped the graph. So a day
   is `build` (to `load`), `run` (restore + start + evaluation + capture + stop) and `destroy`. The
   published `restore` figure is a difference of three medians and could not say which part grew:
   the quiet restored day also starts, stops and destroys N children the plain quiet day never has.
2. **The harness no longer copies the image inside the clock.** It took the image as a by-value
   parameter, copied it into the `load` lambda and copied it again on return — three deep copies of
   an N-child image per repetition, plus one more of every captured image in `commit` — and freed
   them all inside the timed region. A store hands a decoded image over by move, so that was the
   benchmark's cost, not recovery's. One copy per repetition is now staged before the clock starts.
   The tell was in the phase split: `build` on the quiet day scaled with the key count, though it
   wires the same empty graph every time.

## The `map_` bar, µs per key at 5k, 10k, 20k, 40k keys

| Machine, pass | Harness | Restore (published figure) | Save | Quiet day: build | run | destroy | Busy day: run | destroy |
|---|---|---|---|---|---|---|---|---|
| macOS, 1 | copies inside | 2.55, 2.60, 2.84, 3.02 | 1.36, 1.51, 1.54, 1.63 | 0.31, 0.34, 0.31, 0.34 | 3.15, 3.29, 3.57, 3.70 | 0.41, 0.42, 0.45, 0.44 | 3.10, 3.22, 3.29, 3.47 | 0.34, 0.36, 0.37, 0.38 |
| macOS, 2 | copies inside | 2.64, 2.67, 2.81, 2.86 | 1.42, 1.50, 1.58, 1.84 | 0.33, 0.33, 0.31, 0.32 | 3.25, 3.32, 3.57, 3.77 | 0.45, 0.43, 0.44, 0.46 | 3.13, 3.24, 3.37, 3.65 | 0.34, 0.37, 0.38, 0.39 |
| macOS, 1 | **staged** | 2.00, 2.04, 2.46, 2.44 | 1.36, 1.53, 1.49, 1.59 | 0.02, 0.02, 0.01, 0.00 | 2.94, 3.06, 3.35, 3.45 | 0.41, 0.50, 0.56, 0.56 | 2.98, 3.14, 3.16, 3.26 | 0.49, 0.54, 0.55, 0.58 |
| macOS, 2 | **staged** | 1.99, 2.06, 2.25, 2.27 | 1.20, 1.41, 1.52, 1.64 | 0.02, 0.02, 0.01, 0.00 | 2.80, 2.98, 3.20, 3.34 | 0.38, 0.48, 0.56, 0.59 | 2.90, 3.07, 3.10, 3.22 | 0.42, 0.51, 0.60, 0.61 |
| Linux, 1 | copies inside | 4.43, 5.12, 6.10, 6.67 | 1.94, 2.59, 3.08, 3.09 | 0.43, 0.53, 0.72, 0.65 | 4.88, 5.88, 7.04, 7.59 | 0.98, 1.24, 1.33, 1.41 | 4.02, 4.44, 4.92, 5.03 | 0.55, 0.75, 0.96, 0.98 |
| Linux, 2 | copies inside | 4.99, 5.58, 6.54, 6.90 | 1.84, 2.79, 3.13, 3.19 | 0.45, 0.54, 0.71, 0.65 | 5.24, 6.38, 7.47, 7.84 | 1.07, 1.33, 1.34, 1.47 | 4.22, 4.74, 5.05, 5.25 | 0.59, 0.83, 0.95, 0.99 |
| Linux, 1 | **staged** | 2.70, 3.38, 3.67, 4.67 | 2.18, 2.62, 2.93, 3.09 | 0.05, 0.03, 0.02, 0.01 | 4.03, 4.95, 5.47, 6.50 | 0.81, 1.04, 1.12, 1.24 | 3.90, 4.26, 4.59, 4.85 | 0.90, 1.06, 1.19, 1.30 |
| Linux, 2 | **staged** | 2.97, 3.00, 3.50, 3.76 | 1.76, 2.33, 2.60, 2.89 | 0.05, 0.03, 0.02, 0.01 | 4.02, 4.27, 5.03, 5.55 | 0.70, 0.97, 1.05, 1.09 | 4.01, 4.22, 4.28, 4.63 | 0.70, 1.01, 1.06, 1.22 |

## Reading it

* **The published restore figure was overstated by the harness**: by 19-25% on macOS
  (2.6-3.0 → 2.0-2.4 µs per key) and by 30-45% on Linux (4.4-6.9 → 2.7-4.7). The restore numbers in
  `../recovery-20260919/` carry that overhead; its save numbers and every flatness verdict stand.
  Save barely moves: the one copy it carried is dropped by the median either way on macOS and is
  within the noise on Linux.
* **`build` is now flat at zero**, as wiring an empty `map_` should be. That is the check that the
  correction removed what it was meant to.
* **A rise remains on Linux, and it is not specific to restore.** Per key, from 5k to 40k keys, with
  the copies staged:

  | | quiet day `run` | busy day `run` (restores nothing) | save | `destroy` |
  |---|---|---|---|---|
  | Linux | 1.38-1.61x | 1.15-1.24x | 1.42-1.64x | 1.44-1.74x |
  | macOS | 1.17-1.19x | 1.09-1.11x | 1.17-1.37x | 1.37-1.55x |

  Everything that walks N child graphs gets dearer per key as N grows, on both machines and more so
  on Linux; destroying them rises about as much on either. Restore is the phase that touches the
  most memory per key (the owned image is about 2 KB per child-graph node, in many small
  allocations, as well as the graphs themselves), so it is where the difference between the two
  machines shows most.
* **That still fits a working set outgrowing cache, and it is still an inference.** Splitting the
  day cannot separate cache misses from allocator behaviour. Hardware counters can, and
  `perf_event_paranoid` is 4 on the validation host, so unprivileged `perf` is refused there.
  **Open, and now narrower:** with counters available, compare last-level cache misses per key at 5k
  and 40k for the quiet day's `run`. If the image's footprint is the cause, the recorded alternative
  is the streaming checkpoint ops (RFC 0039), which never materialise the whole image.
* **Nothing is quadratic** (guardrail iv): the worst per-key ratio over 8x keys anywhere in these rows
  is 1.74 against 8.00, and the test's bound of 3x holds on every pass of both harnesses.
