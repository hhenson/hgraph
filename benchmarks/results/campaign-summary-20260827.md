# hgraph 0.8.19 performance and memory campaign — 2026-08-27

Three hosts, both campaigns, 79 timing scenarios (core + diagnostic, five
fresh-process samples) and 59 memory profiles (three samples, 5 ms RSS
sampling). The fixed release pin moved from 0.8.1 to 0.8.19 in `c9935bd35`.

## What this campaign can and cannot say

Tag `0.8.19` resolves to `844a086ea`, which is `origin/main`. The campaign
branch differs from that tag only in benchmark pins, developer-guide prose,
and harness tests — no runtime, binding, or packaged Python source. The
`current source` and `hgraph 0.8.19` columns of every matrix here are
therefore **the same code compiled twice**, and their ratio measures the
build, not the source.

Two questions are answerable from the data, and they are reported separately:

1. **Build parity** — does a locally built wheel match the published one?
2. **Release over release** — what did 0.8.1 → 0.8.19 actually change?
   Both sides are published wheels, so no build difference is in play.

## Measurement noise floor

Two runs of the *identical* macOS binary, same idle host, five samples each:

| statistic | value |
|---|---:|
| geometric-mean shift between runs | 1.35% |
| median per-scenario deviation | 1.7% |
| 90th percentile | 4.4% |
| maximum | 17.2% |
| scenarios moving more than 5% | 6 of 79 |

The harness's 5% win/loss band is therefore close to the 90th percentile of
pure noise. **No single-scenario claim inside that band is meaningful**, and
the aggregate geometric mean is only trustworthy beyond roughly 1.5%. Every
per-scenario finding below was required to reproduce on at least two hosts.

## 1. Build parity: published wheel vs local build

| host | toolchain (local build) | timing geomean | memory geomean | ready RSS, published → local |
|---|---|---:|---:|---|
| macOS 26.6, M4 Max | Apple clang 21 | 1.004x, 1.018x | 0.996x | 63.1 → 63.2 MiB |
| Windows 11, 32-core AMD | MSVC 14.51 | 1.001x | 1.018x | 44.3 → 44.2 MiB |
| Linux, Core Ultra 7 155H | **GCC 15.2.0** | **0.958x** | **0.526x** | 63.5 → 70.1 MiB |
| Linux, same host rebuilt | **GCC 14.3.0** | **0.979x** | **1.183x** | 63.5 → 67.7 MiB |

macOS and Windows are at parity: the local build reproduces the published
wheel within noise, and on macOS the two binaries differ by 176 bytes
(1,985,312 vs 1,985,488), i.e. the same toolchain on both sides.

Linux is not, and the cause is the compiler. `readelf -p .comment` gives:

- published `manylinux` wheel — `GCC: (GNU) 14.2.1 20250110 (Red Hat 14.2.1-11)`
- local wheel as first built — `GCC: (Ubuntu 15.2.0-16ubuntu1) 15.2.0`

The host's unversioned `cc`/`c++`/`gcc`/`g++` all resolve to 14.3.0, but
`g++-15` is installed and CMake selected it. GCC 15 is not a toolchain this
project builds or tests on anywhere; CI uses GCC 14. Rebuilding the same
source with `CXX=g++-14` moved timing 0.958x → 0.979x (losses 30 → 14, wins
0 → 2), leaving a residual ~2% against a 1.35% noise floor — consistent with
Red Hat 14.2.1 vs Ubuntu 14.3.0 and the manylinux build environment.

**The memory column is the stronger warning.** The same source, same host,
same published wheel to compare against, produced geomeans of 0.526x under
GCC 15 and 1.183x under GCC 14 — a 2.2x swing driven entirely by the
compiler. Incremental-RSS ratios must not be compared across toolchains.

## 2. Release over release: published 0.8.1 → published 0.8.19

| host | timing geomean | faster / near / slower | memory geomean |
|---|---:|---|---:|
| macOS | **1.111x** | 38 / 36 / 5 | 1.030x |
| Linux | **1.136x** | 34 / 39 / 5 | 1.116x |

0.8.19 is roughly 11–14% faster than 0.8.1 across the pack, for about 3–12%
more incremental resident memory. Of 78 scenarios comparable on both hosts,
exactly one disagreed in direction, so the timing signal is solid.

### Improvements that reproduce on both hosts

| workload | macOS | Linux |
|---|---:|---:|
| Set add/remove deltas (`tss_add_remove_std`) | 6.33x | 5.68x |
| Buffered stream lag/gate (`audit_stream_buffered_std`) | 4.84x | 4.67x |
| Regex match_/replace (`audit_string_match_std`) | 1.44x | 9.80x |
| Scalar-collection convert and collect (`audit_convert_collect_std`) | 1.63x | 1.67x |
| Bundle partial field updates (`type_tsb_partial_fields_std`) | 1.49x | 1.36x |
| race over if_-routed references (`audit_ref_race_std`) | 1.44x | 1.34x |
| Map and reduce, Python map child (`tsd_dense_py`) | 1.34x | 1.36x |
| Request/reply service, Python (`service_request_reply_py`) | 1.30x | 1.26x |

### Regressions that reproduce on both hosts

| workload | macOS | Linux |
|---|---:|---:|
| Wide/deep graph — Python nodes (`construct_py`) | **0.88x** | **0.88x** |
| Wide/deep graph — native operators (`construct_std`) | **0.93x** | **0.92x** |
| Equality/dedup — Python-owned (`python_owned_dedup_python`) | **0.90x** | **0.92x** |

Graph **construction** is the one coherent regression in the 0.8.x line: both
construction scenarios are 7–12% slower on both hosts, while steady-state
execution improved sharply. The trade is favourable for long-running graphs
and unfavourable for workloads that build many short-lived graphs. This is
the finding most worth acting on.

### Memory, and what not to conclude from it

Per-profile memory deltas mostly do **not** reproduce across hosts. The
largest apparent 0.8.19 wins on Linux — sparse retained capacity medium at
0.63x, monotonic key growth long at 0.72x — are 1.01x and 1.02x on macOS.
Treat them as allocator and page-granularity behaviour, exactly as
`memory_utilisation.rst` warns, not as code improvements.

## Failures and stability

| host | timing cells | memory profiles | notes |
|---|---|---|---|
| macOS | 79/79 | 59/59 | clean, both runs |
| Windows | 79/79 | 59/59 | clean |
| Linux | 76–77 of 79 | 54–55 of 59 | see below |

Linux lost two to four cells per run. Two distinct causes:

- **Interpreter startup flake** — `Fatal Python error: Failed to import
  encodings module` / `ValueError: unsupported error handler`, referencing the
  system `/usr/lib/python3.14` stdlib rather than the uv venv. The subprocess
  dies before hgraph is imported. The same scenario needed an independent
  rerun in the 2026-08-09 record, so this is recurring host behaviour.
- **Two SIGSEGVs** (`exit -11`) in the locally built extension:
  `python_owned_project_several_native` and `audit_ref_race_std`, both at
  sample 2, neither in the published wheel. **Not reproduced**: 20 direct
  runs of each scenario were clean (60/60), so the rate is about 0.25% of
  subprocess launches. Unresolved; see below.

For context on that host: `/var/crash` holds kdump directories from
**19:33 and 19:44 local**, and the box rebooted at ~19:45. The campaigns ran
22:51–23:15 local, so the kernel crashes precede this work and did not affect
it — but that machine panicked twice on the day of the run.

## Harness issues found and fixed (`5a9cbc774`)

- **Compiler misattribution.** `benchmark_metadata()` reported
  `$CXX`/`c++ --version`, so the Linux matrix claimed `c++ 14.3.0` for a wheel
  GCC 15.2.0 had built — the exact confound above, invisible in the artifact.
  It now prefers the producer string recorded in the built extension.
- **Self-inflicted dirty stamp.** The same function ran `git status
  --untracked-files=normal`, so a campaign's own matrices marked the *next*
  run `+dirty`. `benchmarks/results/` is now excluded.

Two further provenance gaps are recorded but not fixed:

- On Windows the compiler line reads `unknown` — there is no `c++` to probe,
  and PE carries no producer string.
- The Windows source fingerprint (`5bf642b9…`) differs from macOS and Linux
  (`5ceb81b0…`) for the same commit, because `core.autocrlf=true` rewrites
  line endings on checkout. The fingerprint remains a valid *local* staleness
  guard but cannot prove two hosts measured the same source.

## Artifacts

| file | contents |
|---|---|
| `performance-20260827-{macos,linux,windows}.{md,json}` | build parity, primary run per host (Linux = GCC 14) |
| `performance-20260827-macos-repeat.md` | second macOS run; the noise floor above |
| `performance-20260827-linux-gcc15.md` | the GCC 15 build, kept as toolchain evidence |
| `performance-20260827-linux-repeat.md` | second GCC 14 run |
| `performance-081-20260827-{macos,linux}.{md,json}` | published 0.8.1, for release over release |
| `memory-20260827-{macos,linux,windows}.{md,json}` | build parity, memory |
| `memory-20260827-linux-gcc15.md` | GCC 15 memory, the 0.526x column |
| `memory-081-20260827-{macos,linux}.{md,json}` | published 0.8.1, memory |

## Open items

1. The `construct_std` / `construct_py` construction regression, 7–12% on
   both hosts, is unexplained and is real code, not build noise.
2. The two Linux SIGSEGVs are unreproduced and remain outstanding; they
   occurred in release code (current source is the 0.8.19 tag).
3. No Windows release-over-release pass was taken; the box was rebuilding the
   `web` extension. macOS and Linux agree closely, so a third host is
   confirmation rather than a gap.
