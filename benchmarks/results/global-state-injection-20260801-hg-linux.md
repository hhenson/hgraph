# Injected GlobalState snapshot — hg-linux

This snapshot measures replacing the Python bridge's run-scoped C++
thread-local runtime state with direct `GlobalStateView` injection. A general
Python node owns one guarded Python projection for its lifetime and advances
only the projection's call generation at each callback. Nodes whose layouts do
not request `GlobalState` do not construct a projection. `GlobalState.instance()`
remains a wiring/configuration convenience and is rejected during execution.

The C++ execution and callback paths contain no thread-local lookup. The
existing Python wiring-context `threading.local` still holds the wiring seed and
the once-per-run misuse marker used by `GlobalState.instance()`; node callbacks
do not read it.

## Environment

- Date: 2026-08-01
- Host: `hg-linux` (physical host, not OrbStack), Linux 7.0.0-28-generic x86_64
- CPU: Intel Core Ultra 7 155H, both variants pinned to CPU 2 with `taskset`
- Python: 3.12.13
- Compiler: GCC 14.3.0
- Build: optimized Release stable-ABI wheel with LTO
- Baseline: merged `main` at `84a52af0765ce6305318e7fe5ff56b868a073896`
- Baseline source fingerprint: `3b62ceaa6404e4d10ec9899724ae0f334e7b26c3129d38e314a93dbeedef7262`
- Candidate source fingerprint: `7f2046ee5b6d77eb4589afd8a569a3d9bba6e739aded3282fcf397cd2205c20a`
- Samples: nine fresh processes per variant and workload, alternating
  baseline/candidate order for every sample
- Scale: `--cycle-scale 10 --size-scale 1`

## Results

Wall-clock values are median seconds; `+/-` is median absolute deviation.
Lower is better. RSS is the median process-wide maximum reported by the same
fresh processes, so it includes the interpreter and imported dependencies.

| workload | cycles | baseline seconds | injected-state seconds | change | baseline RSS | injected-state RSS | RSS change |
|---|---:|---:|---:|---:|---:|---:|---:|
| Native feedback loop (`tick_std`) | 1,000,000 | 0.483228 +/- 0.001173 | 0.482044 +/- 0.001205 | -0.25% | 67.1 MiB | 67.2 MiB | +0.1 MiB |
| Generator to native sink (`python_generator_boundary`) | 200,000 | 0.115201 +/- 0.000448 | 0.115190 +/- 0.000763 | -0.01% | 67.2 MiB | 67.2 MiB | 0.0 MiB |
| Generator to Python sink (`python_sink_boundary`) | 200,000 | 0.175498 +/- 0.001969 | 0.177597 +/- 0.001935 | +1.20% | 66.2 MiB | 66.1 MiB | -0.1 MiB |
| Five-node Python compute chain (`tick_py`) | 200,000 | 0.312475 +/- 0.001415 | 0.314079 +/- 0.001859 | +0.51% | 67.3 MiB | 67.2 MiB | -0.1 MiB |
| Python compute with injected `GlobalState` (`python_global_state_boundary`) | 200,000 | 0.272155 +/- 0.001991 | 0.278843 +/- 0.000545 | +2.46% | 67.1 MiB | 67.2 MiB | +0.1 MiB |

The native control and ordinary Python boundaries are neutral at this sample
size: their median changes are between -0.25% and +1.20%. The directly affected
injected `GlobalState` path has a small measurable +2.46% cost, approximately
33 ns per callback at this scale. Maximum RSS is neutral to the 0.1 MiB
resolution of these process-level samples.

An initial implementation allocated a guard and Python projection per callback
and regressed the injected path by 5.27%. It was rejected. The final design
caches both objects on the owning node, advances only the generation used for
retained-view safety, and reduces the measured difference by more than half.
The targeted `python_global_state_boundary` scenario remains in
`benchmarks/scenarios.py` for future optimization work.

## Lifetime and compatibility gates

On the same physical Linux host, the Release stable-ABI wheel built with Python
3.12 and installed into a fresh Python 3.14 environment passed 1,793 tests with
10 skipped. The fresh native suite passed all 1,372 tests. The same stable-ABI
wheel gate on macOS passed 1,793 tests with 10 skipped.

The complete Python 3.14 non-WIP suite also passed under a Debug
AddressSanitizer build on `hg-linux`: 1,793 passed and 10 skipped, with no
sanitizer report.

## Reproduction shape

Each sample invokes `benchmarks/runner.py` in the already-built baseline or
candidate environment, for example:

```sh
HGRAPH_BENCHMARK_SOURCE_FINGERPRINT="$fingerprint" \
  taskset -c 2 "$python" benchmarks/runner.py \
  --scenario python_global_state_boundary \
  --cycle-scale 10 --size-scale 1
```

The variant order is reversed on every other sample. The same procedure is
used for all five workloads.
