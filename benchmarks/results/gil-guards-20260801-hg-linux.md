# Coarse executor-phase GIL guard snapshot — hg-linux

This snapshot measures the change from the merged cycle-observer mechanism to
one ordinary `nanobind::gil_scoped_acquire` spanning each complete executor
start phase, root evaluation cycle, and stop phase. Python node trampolines do
not acquire separately.

## Environment

- Date: 2026-08-01
- Host: `hg-linux` (not OrbStack), Linux 7.0.0-28-generic x86_64
- CPU: Intel Core Ultra 7 155H, both variants pinned to CPU 2 with `taskset`
- Python: 3.12.13
- Compiler: GCC 14.3.0
- Build: optimized Release wheel with LTO
- Baseline: merged `main` at `ac12029758eeab76c3b492eaa31593c365ffee52`
- Baseline source fingerprint: `87fafa37926655f96bf10d013170a89eeecbdc6b2014bf0577f3c101e8590590`
- Candidate source fingerprint: `f9824c5631b061394ff449c61dff45570fb5ba22c7c0198c20d9627a0bba84c9`
- Samples: nine fresh processes per variant and workload, alternating
  baseline/candidate order for every sample
- Scale: `--cycle-scale 10 --size-scale 1`

## Results

Median wall-clock seconds; `+/-` is median absolute deviation. Lower is
better.

| workload | cycles | Python nodes per cycle | baseline | coarse phase guard | change |
|---|---:|---:|---:|---:|---:|
| Native feedback loop (`tick_std`) | 1,000,000 | 0 | 0.466569 +/- 0.002398 | 0.468608 +/- 0.001403 | +0.44% |
| Generator to native sink (`python_generator_boundary`) | 200,000 | 1 | 0.117021 +/- 0.001056 | 0.115487 +/- 0.000806 | -1.31% |
| Generator to Python sink (`python_sink_boundary`) | 200,000 | 2 | 0.182408 +/- 0.002141 | 0.175379 +/- 0.000791 | -3.85% |
| Five-node Python compute chain (`tick_py`) | 200,000 | 5 | 0.344208 +/- 0.003957 | 0.332636 +/- 0.001310 | -3.36% |

The native-only result is neutral at the observed variance. Python workloads
are neutral-to-faster, and the five-node chain no longer accumulates GIL
acquisition cost with node count because all five nodes share the evaluation
phase's single guard.

An initial unconditional phase-runner prototype was rejected before this
snapshot: it wrapped even pure-native Python-authored graphs and regressed
`tick_std` by 10.99%. The final implementation propagates a generic
`requires_phase_runner` bit through nested graph plans and installs the Python
runner only for graphs that can enter Python (or runs with Python lifecycle
observers). That restores the native fast path without adding per-node guard
fallbacks.

## Reproduction shape

Each sample invokes `benchmarks/runner.py` in the already-built baseline or
candidate environment, for example:

```sh
HGRAPH_BENCHMARK_SOURCE_FINGERPRINT="$fingerprint" \
  taskset -c 2 "$python" benchmarks/runner.py \
  --scenario tick_py --cycle-scale 10 --size-scale 1
```

The variant order is reversed on every other sample. The same procedure is
used for all four workloads.
