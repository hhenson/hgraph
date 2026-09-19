# Save and restore — where a recovered day spends its time, 2026-09-19

`hgraph_unit_tests '[recovery-benchmark]'` (`tests/cpp/test_recovery_benchmark.cpp`), Release builds,
two passes per machine. It follows `../recovery-20260919/`, whose open question was why restore in
one process is not flat on Linux, and changes the answer.

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
   an N-child image per repetition — and copied every captured image in `commit`, freeing all of them
   inside the timed region. A store hands a decoded image over by move, so that was the benchmark's
   cost, not recovery's. One copy per repetition is now staged before the clock starts, and the image
   itself comes from one separate UNTIMED run, so no timed repetition copies anything (a copy in one
   of five repetitions is not reliably dropped by their median — review finding on the PR).
   The tell was in the phase split: `build` on the quiet day scaled with the key count, though it
   wires the same empty graph every time.

## The `map_` bar, µs per key at 5k, 10k, 20k, 40k keys

| Machine, pass | Harness | Restore (published figure) | Save | Quiet day: build | run | destroy | Busy day: run | destroy |
|---|---|---|---|---|---|---|---|---|
| macOS, 1 | copies inside | 2.55, 2.60, 2.84, 3.02 | 1.36, 1.51, 1.54, 1.63 | 0.31, 0.34, 0.31, 0.34 | 3.15, 3.29, 3.57, 3.70 | 0.41, 0.42, 0.45, 0.44 | 3.10, 3.22, 3.29, 3.47 | 0.34, 0.36, 0.37, 0.38 |
| macOS, 2 | copies inside | 2.64, 2.67, 2.81, 2.86 | 1.42, 1.50, 1.58, 1.84 | 0.33, 0.33, 0.31, 0.32 | 3.25, 3.32, 3.57, 3.77 | 0.45, 0.43, 0.44, 0.46 | 3.13, 3.24, 3.37, 3.65 | 0.34, 0.37, 0.38, 0.39 |
| macOS, 1 | **untimed image** | 1.95, 2.06, 2.10, 2.16 | 1.29, 1.42, 1.57, 1.65 | 0.02, 0.01, 0.01, 0.00 | 2.82, 2.97, 3.10, 3.24 | 0.39, 0.51, 0.56, 0.57 | 2.90, 2.98, 3.04, 3.16 | 0.48, 0.53, 0.59, 0.61 |
| macOS, 2 | **untimed image** | 2.04, 2.08, 2.21, 2.35 | 1.22, 1.42, 1.60, 1.60 | 0.02, 0.01, 0.01, 0.00 | 2.84, 2.97, 3.29, 3.38 | 0.39, 0.51, 0.57, 0.57 | 2.86, 2.96, 3.12, 3.16 | 0.43, 0.54, 0.57, 0.57 |
| Linux, 1 | copies inside | 4.43, 5.12, 6.10, 6.67 | 1.94, 2.59, 3.08, 3.09 | 0.43, 0.53, 0.72, 0.65 | 4.88, 5.88, 7.04, 7.59 | 0.98, 1.24, 1.33, 1.41 | 4.02, 4.44, 4.92, 5.03 | 0.55, 0.75, 0.96, 0.98 |
| Linux, 2 | copies inside | 4.99, 5.58, 6.54, 6.90 | 1.84, 2.79, 3.13, 3.19 | 0.45, 0.54, 0.71, 0.65 | 5.24, 6.38, 7.47, 7.84 | 1.07, 1.33, 1.34, 1.47 | 4.22, 4.74, 5.05, 5.25 | 0.59, 0.83, 0.95, 0.99 |
| Linux, 1 | **untimed image** | 2.81, 2.86, 3.36, 3.69 | 1.45, 2.21, 2.70, 2.73 | 0.05, 0.03, 0.02, 0.01 | 3.60, 4.15, 5.04, 5.35 | 0.63, 0.91, 1.01, 1.05 | 3.51, 3.87, 4.37, 4.46 | 0.63, 0.94, 1.04, 1.13 |
| Linux, 2 | **untimed image** | 2.85, 2.98, 3.43, 3.62 | 1.36, 2.15, 2.67, 2.79 | 0.05, 0.03, 0.02, 0.01 | 3.54, 4.20, 5.03, 5.35 | 0.62, 0.90, 1.04, 1.05 | 3.49, 3.82, 4.35, 4.67 | 0.63, 0.91, 1.03, 1.14 |

## Reading it

* **The published restore figure was overstated by the harness**: by 18-28% on macOS
  (2.6-3.0 → 2.0-2.4 µs per key) and by 37-48% on Linux (4.4-6.9 → 2.8-3.7). **Save was overstated
  too on Linux**, by 12-26% (the copy in `commit`); on macOS that copy was within the noise. The
  figures in `../recovery-20260919/` carry those overheads. Its comparisons between variants and its
  flatness verdicts stand — every variant ran through the same harness, and the 3x bound held then
  and holds now.
* **`build` is now flat at zero**, as wiring an empty `map_` should be. That is the check that the
  correction removed what it was meant to.
* **The earlier attribution does not survive: on Linux it is not restore that rises most.** Per key,
  from 5k to 40k keys, with a clean harness:

  | | restore (published figure) | save | quiet day `run` | busy day `run` | `destroy` |
  |---|---|---|---|---|---|
  | Linux | 1.27-1.31x | **1.88-2.05x** | 1.49-1.51x | 1.27-1.34x | 1.67-1.82x |
  | macOS | 1.11-1.15x | 1.28-1.31x | 1.15-1.19x | 1.09-1.11x | 1.26-1.46x |

  The earlier results showed restore rising 1.75-1.90x and save 1.09-1.34x on Linux. Both were
  artefacts of the same copies: they inflated save most where runs are shortest, and restore is
  computed by subtracting save. With them gone, everything that walks N child graphs gets dearer
  per key as N grows, on both machines and more on Linux, and save and destroy lead.
* **The shape is a step, not a slope.** Linux save goes 1.4, 2.2, 2.7, 2.75 µs per key: it climbs to
  20k keys and then stops. A quadratic, or anything algorithmic, keeps climbing; a working set that
  has finished leaving cache does not. The two passes now agree to within a few percent, which the
  earlier harness never managed.
* **That is still an inference.** Timers cannot separate cache misses from allocator behaviour.
  Hardware counters can, and `perf_event_paranoid` is 4 on the validation host, so unprivileged
  `perf` is refused there. **Open, and now narrower:** with counters available, compare last-level
  cache misses per key at 5k and 40k for the busy saved day. If the owned image's footprint (about
  2 KB per child-graph node, in many small allocations) is the cause, the recorded alternative is
  the streaming checkpoint ops (RFC 0039), which never materialise the whole image.
* **Nothing is quadratic** (guardrail iv): the worst per-key ratio over 8x keys anywhere in these rows
  is 2.05 against 8.00, and the test's bound of 3x holds on every pass of both harnesses.
