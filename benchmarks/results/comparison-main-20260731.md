# Main performance baseline — `9db88d02` (2026-07-31)

Clean two-mode benchmark of the latest merged main after the value visitor,
Python-aware output storage, prepared-route, and structural endpoint work. The
benchmark pack is unchanged from the 2026-07-30 release comparison, so the
latest-main/release comparison isolates runtime changes.

The pure-Python runtime was intentionally omitted. Both hosts ran the released
hgraph 0.5.33 C++ engine and the optimized hg_cpp wheel built from
`9db88d027108`, using five fresh-process samples per cell:

```sh
python benchmarks/orchestrate.py \
    --suite core --suite diagnostic \
    --mode upstream-cpp --mode hg-cpp \
    --samples 5 --refresh-baseline
```

All workload guards passed. All 68 scenarios completed on both hosts with no
failed cells and no reused baseline cells. There are 56 cross-runtime scenarios
and 12 hg_cpp-only scenarios.

Artifacts:

- [macOS matrix](matrix-20260731-031345-mac-main.md)
- [macOS raw samples](raw-20260731-031345-mac-main.json)
- [Linux matrix](matrix-20260731-031650-linux-main.md)
- [Linux raw samples](raw-20260731-031650-linux-main.json)

## Headline

| host | hg_cpp vs legacy C++ | July 30 ratio | latest/release hg_cpp speed, all 68 | refreshed legacy speed |
|---|---:|---:|---:|---:|
| macOS, Apple M4 Max, Python 3.14.6 | **x2.15** | x2.12 | x0.998 | x0.986 |
| Linux, Intel Core Ultra 7 155H, Python 3.14.4, `taskset -c 4-9` | **x2.01** | x2.01 | x0.999 | x0.997 |

The overall hg_cpp runtime is effectively flat versus the July 30 release
baseline: -0.2% on macOS and -0.1% on Linux across all 68 scenarios. The
slightly higher macOS old-C++/hg_cpp ratio is mostly explained by the refreshed
legacy baseline running 1.4% slower; Linux reproduced both runtimes within
0.3%.

The intended Python value-path improvement is clear and repeatable:
`type_cs_py` is **x1.206 faster on macOS and x1.213 faster on Linux** than the
release runtime. It now runs x1.49/x1.48 faster than legacy C++ on
macOS/Linux.

The Python combiner route also improves by x1.071 on macOS and x1.030 on
Linux. `reduce_tsd_python_combiner` remains below legacy C++ parity, but the
macOS gap narrowed from x0.85 to x0.92.

## Latest main versus the release runtime

`latest/release` is the previous hg_cpp median divided by the new hg_cpp
median; values above one are improvements.

| scenario | mac old/hg | Linux old/hg | mac latest/release | Linux latest/release |
|---|---:|---:|---:|---:|
| CompoundScalar crossing Python nodes (`type_cs_py`) | x1.49 | x1.48 | **x1.206** | **x1.213** |
| Python TSD combiner (`reduce_tsd_python_combiner`) | x0.92 | x0.77 | **x1.071** | x1.030 |
| Dense TSD with Python child (`tsd_dense_py`) | x1.13 | x0.86 | x0.967 | x1.000 |
| Python service adaptor (`service_adaptor_py`) | x1.13 | x0.87 | x1.004 | x1.021 |
| TSD capacity growth (`tsd_capacity_growth_std`) | x2.13 | x2.27 | x0.940 | x0.971 |

There is no regression of at least 5% reproduced on both platforms.
`tsd_capacity_growth_std` is 6.0% slower on macOS but only 2.9% slower on
Linux. The macOS `tsd_sparse_source_std` median moved from 4.4 ms to 5.8 ms,
but its 6.6% median absolute deviation and the flat Linux result make that a
noisy cell rather than a cross-platform signal.

## Remaining legacy C++ gaps

| scenario | mac old/hg | Linux old/hg |
|---|---:|---:|
| Dense TSD with Python child (`tsd_dense_py`) | x1.13 | **x0.86** |
| Python TSD combiner (`reduce_tsd_python_combiner`) | **x0.92** | **x0.77** |
| Python service adaptor (`service_adaptor_py`) | x1.13 | **x0.87** |

The outstanding sub-parity work remains confined to Python-heavy TSD and
adaptor paths, especially on Linux. The latest-main changes materially help
compound scalar crossings without causing a broad runtime regression.

## Normalized macOS versus Linux view

The geomean same-machine old-C++/hg_cpp ratio is x2.15 on macOS and x2.01 on
Linux. Dividing those ratios gives **x1.073**: relative to the common legacy C++
baseline, hg_cpp's advantage is 7.3% stronger on macOS. This was x1.059 in the
July 30 baseline.

In absolute time, Linux hg_cpp is x1.697 slower than macOS while legacy C++ is
x1.582 slower. These are different CPU architectures, so the normalized ratio
is the more useful indicator.

| relative strength | scenario | mac old/hg | Linux old/hg | mac/Linux normalized |
|---|---|---:|---:|---:|
| macOS | Dense TSD reduce | x2.06 | x1.49 | x1.38 |
| macOS | Scheduler fan-out | x2.62 | x1.90 | x1.38 |
| macOS | Dense TSD with Python child | x1.13 | x0.86 | x1.31 |
| Linux | TSD clear/repopulate | x2.34 | x3.66 | x0.64 |
| Linux | Mesh | x3.66 | x4.62 | x0.79 |
| Linux | Alternating switch branches | x1.17 | x1.37 | x0.86 |
