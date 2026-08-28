# hgraph benchmark baseline moved to 0.8.19 — 2026-08-27

The fixed release pin moved from the published hgraph 0.8.1 wheel to the
published **0.8.19** wheel (`c9935bd35`). `memory_orchestrate.py` imports the
same constants, so one pin covers both campaigns. This file is the record of
that move: the new point of testing, and what the 0.8.1 → 0.8.19 line
delivered.

Ordinary future reports compare current source against 0.8.19 and do not rerun
the 0.5.41 modes. The committed 0.8.1-versus-0.5.41 record in
`baseline-summary-20260809.md` becomes historical; the current pin cannot
reproduce it.

## The new point of testing

Both sides of every comparison below were measured in one session on current
hardware — no cell is carried over from an earlier record.

| Host | CPU | Python | 0.8.19 wheel / SHA-256 |
|---|---|---|---|
| macOS 26.6 arm64 | Apple M4 Max | 3.14.7 | `hgraph-0.8.19-cp312-abi3-macosx_15_0_arm64.whl`<br>`e7c4f19920a45ce9da0d4e4c479af2fd258e4f55b0f2de0215e5b105548629d1` |
| Linux 7.0 x86_64 | Intel Core Ultra 7 155H | 3.14.4 | `hgraph-0.8.19-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl`<br>`3c58610039211b0a9965727c4a940da64664188d20ebad6711a02bc700670637` |
| Windows 11 x86_64 | AMD, 32 core | 3.14.7 | `hgraph-0.8.19-cp312-abi3-win_amd64.whl`<br>`b072b49300aa2bb744372da81e070be13846b8a5ccb03cbbb0be2c2108ffb377` |

**The Windows host is new hardware.** The 20260809 record measured Windows 10
on an Intel i9-9980HK; this one is Windows 11 on a 32-core AMD part. Its 0.8.1
figures were therefore re-measured here rather than carried across, and
**Windows numbers must not be compared between the two dates**. macOS (M4 Max)
and Linux (Core Ultra 7 155H) are the same machines as in the 20260809 record.

Coverage per host: 79 timing scenarios (core + diagnostic) at five
fresh-process samples, 59 memory profiles at three samples with 5 ms RSS
sampling plus the native structural pass.

## What 0.8.1 → 0.8.19 delivered

Published wheel to published wheel, same host, same session, so no build
difference is in play.

| Host | Timing geomean | Faster / near / slower | Memory geomean |
|---|---:|---|---:|
| macOS | **1.111x** | 38 / 36 / 5 | 1.030x |
| Linux | **1.136x** | 34 / 39 / 5 | 1.116x |
| Windows (new box) | **1.463x** | 76 / 2 / 1 | 1.035x |
| Windows (old box, constant hardware) | **1.183x** | — | not measured |

0.8.19 is faster on every host for roughly 3–12% more incremental resident
memory.

### Most of the Windows gain is the new hardware, not the release

The 1.463x measured on the new Windows box is a same-host ratio — both wheels
ran on it 21 minutes apart with the platform constant — so it is real for that
machine. But it is *not* the release effect. The old Windows box
(`hg-windows`, Windows 10, Intel i9-9980HK, the 20260809 hardware) was
re-instated on 2026-08-28 and both published wheels were run on it too, same
scenario pack, same Python 3.14.7:

| Windows host | 0.8.1 geomean | 0.8.19 geomean | Gain |
|---|---:|---:|---:|
| new — Windows 11, 32-core AMD | 0.0801s | 0.0547s | **1.463x** |
| old — Windows 10, i9-9980HK | 0.1162s | 0.0983s | **1.183x** |

**On constant old hardware the release is worth 1.183x**, in line with macOS
(1.111x) and Linux (1.136x). The extra gain on the new box comes from the
platform change interacting with the release: the old box is 1.45x slower than
the new on 0.8.1 but 1.80x slower on 0.8.19, so 0.8.19 exploits the newer
machine better than 0.8.1 did.

Quote **1.18x** as what 0.8.1 → 0.8.19 delivered on Windows. The 1.463x
belongs to the machine upgrade plus the release together, and must not be
reported as a property of the release.

This also retires an earlier reading in this file, which took the new box's
1.463x as evidence that 0.8.1 carried a large Windows-specific cost. The
correct statement is narrower: Windows improved about as much as the POSIX
hosts, and the new hardware is disproportionately good at running 0.8.19.

One measurement asymmetry, noted for honesty: the new box's 0.8.19 figures come
from a two-mode run and its 0.8.1 figures from a single-mode run, so the former
did more work per session. If that host throttles under sustained load the
effect understates the 0.8.19 column. The old-box pair has no such asymmetry —
both are single-mode runs — which is another reason to prefer its 1.183x.

**No scenario is slower on all three hosts.** 32 of 78 are faster everywhere.
Direction has to be read per platform rather than pooled.

### Improvements on all three hosts

| Workload | macOS | Linux | Windows |
|---|---:|---:|---:|
| Buffered stream lag/gate (`audit_stream_buffered_std`) | 4.84x | 4.67x | 17.10x |
| Set add/remove deltas (`tss_add_remove_std`) | 6.33x | 5.68x | 4.30x |
| Regex match_/replace (`audit_string_match_std`) | 1.44x | 9.80x | 2.16x |
| Bundle partial field updates (`type_tsb_partial_fields_std`) | 1.49x | 1.36x | 2.11x |
| Scalar-collection convert and collect (`audit_convert_collect_std`) | 1.63x | 1.67x | 1.91x |
| race over if_-routed references (`audit_ref_race_std`) | 1.44x | 1.34x | 1.84x |
| Map and reduce, Python map child (`tsd_dense_py`) | 1.34x | 1.36x | 1.76x |
| Request/reply service, Python (`service_request_reply_py`) | 1.30x | 1.26x | 1.67x |

Magnitudes vary widely by host — regex is 1.44x on macOS and 9.80x on Linux,
the buffered stream 4.84x and 17.10x — so treat the direction as the finding
and the size as host-specific.

### Graph construction regressed on POSIX only

| Workload | macOS | Linux | Win new | Win old (i9) |
|---|---:|---:|---:|---:|
| Wide/deep graph — Python nodes (`construct_py`) | **0.88x** | **0.88x** | 1.26x | 0.98x |
| Wide/deep graph — native operators (`construct_std`) | **0.93x** | **0.92x** | 1.30x | 1.01x |
| Equality/dedup — Python-owned (`python_owned_dedup_python`) | **0.90x** | **0.92x** | 1.41x | 1.03x |

Two independent POSIX hosts agree closely that graph construction became 7–12%
slower, and the three affected workloads move together — a coherent group, not
scattered cells.

The old Windows box settles what the new one could not. It is the slowest
machine here, so if a construction cost were being masked by platform-wide
gains it should surface there; instead all three workloads are flat
(0.98x/1.01x/1.03x). **The masking hypothesis is falsified: the regression is
genuinely POSIX-specific**, and the new Windows box's 26–41% gain on these
three is that machine's own affinity for 0.8.19, not the absence of a cost.

Settling the POSIX side needs a profile of graph construction on macOS or
Linux, not more benchmark runs.

### Nothing regresses broadly

Across the four host configurations — macOS, Linux, and both Windows boxes —
**no scenario regresses by more than 5% on three or more of them.** The old
Windows box has three of its own (`tsd_capacity_growth_std` 0.80x,
`tsd_sparse_source_std` 0.83x, `tsd_sparse_large_capacity_std` 0.84x), but only
the sparse-source one overlaps with any other host, and the same workloads
improve 1.16–1.22x on the new Windows box. Treat them as host-specific.

### Memory

Incremental RSS rose slightly on every host (1.030x / 1.116x / 1.035x), and
per-profile deltas mostly do not reproduce across hosts. The largest apparent
0.8.19 memory wins on Linux — sparse retained capacity at 0.63x, monotonic key
growth at 0.72x — are 1.01x and 1.02x on macOS. Treat them as allocator and
page granularity, as `memory_utilisation.rst` warns, not as code improvements.

## Reading future runs against this baseline

### Noise floor

Two runs of the identical macOS binary on an idle host:

| Statistic | Value |
|---|---:|
| Geometric-mean shift between runs | 1.35% |
| Median per-scenario deviation | 1.7% |
| 90th percentile | 4.4% |
| Maximum | 17.2% |
| Scenarios moving more than 5% | 6 of 79 |

The harness's 5% win/loss band is close to the 90th percentile of pure noise.
No single-scenario claim inside that band is meaningful, and an aggregate
geometric mean is only trustworthy past roughly 1.5%. Every per-scenario
finding above reproduces on at least two hosts.

### Pin the compiler before trusting a local-build comparison

Running the default modes on this commit compares 0.8.19 with itself, because
tag `0.8.19` is `origin/main`; that accident measured the *build* rather than
the source, and it exposed something worth keeping. On macOS and Windows the
local build reproduces the published wheel (1.004x and 1.001x). On Linux it did
not — 0.958x — because CMake selected `g++-15` while the published wheel is
built by Red Hat GCC 14.2.1:

- published manylinux wheel — `GCC: (GNU) 14.2.1 20250110 (Red Hat 14.2.1-11)`
- local wheel as first built — `GCC: (Ubuntu 15.2.0-16ubuntu1) 15.2.0`

Every unversioned alias on that host resolves to 14.3.0, but `g++-15` is
installed and CMake preferred it; GCC 15 is a toolchain this project builds
nowhere. Rebuilding with `CXX=g++-14` moved timing to 0.979x. The memory column
is the stronger warning: the same source against the same wheel gave 0.526x
under GCC 15 and 1.183x under GCC 14, a 2.2x swing from the compiler alone.

**Set `CXX` explicitly on hg-linux for any candidate-versus-baseline run**, and
do not compare incremental-RSS ratios across toolchains.

### Harness provenance fixed (`5a9cbc774`)

- The recorded compiler came from `$CXX`/`c++ --version`, so the Linux matrix
  claimed `c++ 14.3.0` for a wheel GCC 15.2.0 had built — the confound above,
  invisible in the artifact. It now reads the producer string out of the built
  extension.
- `+dirty` was stamped from `git status --untracked-files`, which counts the
  matrices a previous campaign wrote, so every second run in a checkout
  recorded an inexact revision. `benchmarks/results/` is now excluded.

Two gaps remain: Windows records `compiler: unknown` (no `c++` to probe, and PE
carries no producer string), and the Windows source fingerprint differs from
the POSIX hosts for the same commit because `core.autocrlf=true` rewrites line
endings — still a valid local staleness guard, not a cross-host identity.

## Failures observed

| Host | Timing cells | Memory profiles |
|---|---|---|
| macOS | 79/79 | 59/59 |
| Windows | 79/79 | 59/59 |
| Linux | 76–77 of 79 | 54–55 of 59 |

Linux lost cells to two distinct causes:

- **Interpreter startup flake** — `Fatal Python error: Failed to import
  encodings module`, referencing the system `/usr/lib/python3.14` rather than
  the uv venv; the subprocess dies before hgraph is imported. The same
  scenario needed an independent rerun in the 20260809 record.
- **Two SIGSEGVs** in the locally built extension
  (`python_owned_project_several_native`, `audit_ref_race_std`), neither in the
  published wheel. Not reproduced: 60 of 60 direct runs clean, so roughly 0.25%
  of subprocess launches. Open.

That host also holds kdump directories from 19:33 and 19:44 local and rebooted
at ~19:45; the campaigns ran 22:51–23:15, so the kernel crashes precede this
work.

## Artifacts

| File | Contents |
|---|---|
| `performance-baseline-20260827-{macos,linux,windows}.{md,json}` | the new baseline: published 0.8.19 per host, with a local build alongside |
| `memory-baseline-20260827-{macos,linux,windows}.{md,json}` | the same for memory |
| `performance-081-20260827-{macos,linux,windows}.{md,json}` | published 0.8.1, re-measured on current hardware for the delta |
| `memory-081-20260827-{macos,linux,windows}.{md,json}` | the same for memory |
| `performance-{081,0819}-20260828-windows-i9.{md,json}` | both wheels on the old Windows box, isolating the release from the hardware change |
| `performance-baseline-20260827-{macos,linux}-repeat.md` | second runs; the noise floor above |
| `performance-20260827-linux-gcc15-localbuild.md`, `memory-20260827-linux-gcc15-localbuild.md` | the GCC 15 local build, kept as toolchain evidence |
