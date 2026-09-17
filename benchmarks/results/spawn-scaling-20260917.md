# Process-hosted spawn pipeline scaling

Measured on macOS arm64 with Python 3.14 and a Python 3.12 stable-ABI wheel.
Reproduce with `python benchmarks/spawn_pipeline.py`. Importable worker stages
are in `benchmarks/spawn_workload.py`; raw observations are in
`spawn-scaling-20260917.csv`. Each case uses a fresh parent and reports the median
of three runs, including wiring, process startup, evaluation, drain and reaping.
No local build or test suite was running during these measurements. Channel
capacity is eight frames and 1 MiB. The sink verifies the count and its process
identity after each run.

For one process stage, increasing scalar input from 1,000 to 4,000 ticks changed
elapsed time from 117.85 to 156.37 ms. Four stages changed from 133.78 to 194.09 ms.
Startup dominates these small workloads; adding process boundaries has a real
cost and trivial graphs do not benefit from distribution.

With 1,000 ticks and two stages, raising dictionary membership from 16 to 1,024
keys changed time from 126.60 to 129.60 ms. Only the first frame contains the whole
dictionary; subsequent frames change one key. Raising count-window capacity
from 16 to 1,024 changed time from 124.63 to 123.95 ms. These measurements are
consistent with incremental transfer, without per-tick copying of all history.

The CPU case runs two Python compute stages, each doing 500,000 integer-loop
iterations per tick, followed by a sink, for 32 ticks. Synchronous composition
took 1.110 seconds; three process stages took 0.716 seconds, a 1.55x end-to-end
speedup including startup and drain. This is a workload-specific observation,
not a general throughput guarantee. Separate interpreters allow the two compute
stages to run Python bytecode concurrently.

Parent peak RSS was 68.8–72.3 MiB; the largest worker peak was 66.9–67.7 MiB.
These are separate process peaks, not aggregate pipeline memory or queue bytes.
They include interpreters, libraries and decoded graph state. A process per stage
therefore carries substantial baseline memory cost. Admission counts queued and
evaluating frames and releases capacity after forwarding; each producer also
has one separately bounded frame under construction per destination.
