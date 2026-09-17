# dmap_ vs map_: where distribution starts paying

2026-09-17, branch `feat/externally-driven-executor` at `627349626`
(RFC 0037 acceptance criterion 9).

Produced by `tests/cpp/hgraph_distributed_perf 200`. Raw JSON beside this file:
`dmap-crossover-20260917-linux.json`, `dmap-crossover-20260917-macos.json`.

## What was measured

A `map_` and a `dmap_` over the same kernel, the same key set, and the same
per-key state, differing only in where the children run. Per-cycle cost is
steady state: the graph start and the first cycle are timed separately, because
`posix_spawn` and `CreateProcess` return when the child *exists*, not when it is
*ready* — averaged in, that made a 100-cycle run look four times worse than a
1000-cycle one.

Cycles are driven through the `ExternallyDriven` executor rather than `run()`,
so the split is measured rather than inferred. Inferring it from two run lengths
was tried and abandoned: the fixed cost varies between runs by more than the
per-cycle cost being measured, which produced negative slopes.

Every configuration's output is checksummed against `map_`'s. **All 80 agreed.**

## The answer

`dmap_` breaks even at about **0.6 microseconds of child work per key per tick**.
Below that it is a loss, and no key count rescues it; above it the win grows to
roughly 5x.

The figure is within 25% on a 128-core Threadripper and on an 8-core laptop,
which is what one would expect of a ratio between two costs that both scale with
the processor. It is a property of the design, not of the hardware.

## Linux — AMD Ryzen Threadripper 9980X, 128 threads, GCC 14.3 `-O3`

`dmap_` over processes as a ratio of `map_`, at the best of 1/2/4/8 workers:

| child work per key | 64 keys | 256 keys |
|---|---|---|
| 0 (empty child) | 2.52x | 2.09x |
| 0.15 us | 2.01x | 1.69x |
| 0.6 us | 1.39x | **1.00x** |
| 2.4 us | **0.66** | **0.52** |
| 18 us | **0.21** | **0.20** |

## macOS — AppleClang, `-O3`

| child work per key | 64 keys | 256 keys |
|---|---|---|
| 0 (empty child) | 3.84x | 2.50x |
| 0.18 us | 2.67x | 1.74x |
| 0.75 us | 1.37x | 1.02x |
| 2.9 us | **0.66** | **0.48** |
| 22 us | **0.22** | **0.19** |

## Reading it

- **One worker is never worth it** — 1.05x to 4.2x. That column is the cleanest
  reading of what distribution *costs* before it buys anything.
- **In-process workers are always slower than `map_`** — 1.03x to 5.2x. That is
  what the mode is for: the model's overhead with the parallelism removed, so
  the machinery is priced separately from the win.
- **Eight workers beats four at 256 keys and loses to it at 64.** The barrier
  waits for the slowest worker, and a worker holding eight keys is mostly
  overhead. Worker count should follow key count, not core count.
- **Worker startup is 7-14 ms on Linux and 100-113 ms on macOS**, once.

## Caveat

A synthetic kernel with one `TS[int]` in and one out. A child with a wide
boundary, or one whose output is a large composite, pays more per cycle than
this measures — the per-cycle cost is per changed *value*, and this has one.

The parent still captures, encodes and applies every delta itself, serially.
That is what flattens the curve at high worker counts, and it is the next thing
to attack — ahead of the transport, which these numbers do not show as the
bottleneck.
