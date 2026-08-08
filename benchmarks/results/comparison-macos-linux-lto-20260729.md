# macOS vs Linux normalized hgraph performance after Linux IPO

- Ratios are throughput ratios; higher is better.
- Python is retained here only as the normalization reference for the fixed baseline.
- Baseline: hgraph 0.5.31, reused from the same scenario pack.
- Candidate: macOS shared-library optimization from PR #181; Linux additionally uses target-scoped IPO/LTO.
- macOS: Apple M4 Max / Apple Clang 21 / Python 3.14.6.
- Linux: Intel Core Ultra 7 155H / GCC 15.2 / Python 3.14.4, pinned P-core.

## Overall

| metric | macOS | Linux |
|---|---:|---:|
| comparable workloads | 56 | 56 |
| legacy C++ / Python | 10.79x | 8.33x |
| hg_cpp / Python | 19.39x | 16.57x |
| hg_cpp / legacy C++ | 1.80x | 1.99x |
| hg_cpp vs legacy (>5% faster / parity / >5% slower) | 49 / 5 / 2 | 51 / 2 / 3 |

## By workload group

Geometric mean of `hg_cpp / legacy C++`. `Linux / macOS` compares the relative
uplift over legacy rather than absolute runtimes on dissimilar machines.

| group | workloads | macOS | Linux | Linux / macOS |
|---|---:|---:|---:|---:|
| Graph construction | 2 | 4.65x | 5.38x | 1.16x |
| Scheduler | 5 | 1.26x | 1.26x | 1.00x |
| Python boundary | 2 | 1.23x | 1.25x | 1.02x |
| Value types | 8 | 1.37x | 1.45x | 1.06x |
| TSD - dense | 6 | 1.44x | 1.50x | 1.04x |
| TSD - sparse | 5 | 2.12x | 2.55x | 1.20x |
| TSD - key lifecycle | 11 | 2.86x | 3.62x | 1.26x |
| Reduce | 3 | 1.14x | 1.15x | 1.01x |
| Nested graphs | 3 | 2.00x | 2.89x | 1.44x |
| Services | 7 | 1.98x | 2.05x | 1.04x |
| Adaptors | 4 | 1.26x | 1.19x | 0.94x |

## IPO-only impact on Linux

This comparison uses the same hg_cpp scenario pack before and after IPO.

| metric | result |
|---|---:|
| scenarios | 68 |
| geometric-mean speed-up | 1.089x |
| >5% faster / parity / >5% slower | 42 / 26 / 0 |
| Scheduler | 1.176x |
| Reduce | 1.125x |
| Nested graphs | 1.128x |
| Services | 1.123x |
| TSD - sparse | 1.115x |
| TSD - key lifecycle | 1.097x |

The largest individual improvements are scheduler fan-in (1.30x), ordered
fixed-list reduction (1.25x), Python subscription service (1.23x), scheduler
fan-out (1.23x), and the native feedback loop (1.21x).

## Rejected follow-up experiments

Both experiments used the same 13-workload focused pack with nine
fresh-process samples on the same pinned Linux P-core.

| experiment | geometric-mean throughput | decision |
|---|---:|---|
| `-march=x86-64-v3 -mtune=generic` | 1.003x | Do not narrow wheel compatibility for a parity result |
| nanobind `LTO` on `_hgraph` | 1.002x | Do not add bridge link cost for a parity result |

The v3 experiment also proved a clean unsupported-CPU failure mechanism:
linking the ELF DSOs with `-Wl,-z,x86-64-v3` records
`GNU_PROPERTY_X86_ISA_1_NEEDED`. Loading the probe on the AVX2-less OrbStack
VM failed before executing code with `CPU ISA level is lower than required`,
rather than `SIGILL`.
