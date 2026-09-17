# Spawn pipeline scaling

Measured on macOS arm64 with Python 3.14 and a Python 3.12 stable-ABI wheel.
Reproduce with `python benchmarks/spawn_pipeline.py`. Raw observations are in
`spawn-scaling-20260917.csv`. Each case runs in a fresh process and reports the
median of three runs, including wiring, evaluation, drain and thread join.
Channel capacity is eight frames and 1 MiB; the sink checks every input arrived.
Python callbacks include GIL acquisition costs. The machine was also compiling
validation builds, so these timings are scaling evidence rather than a stable
throughput baseline or a speedup claim.

Increasing scalar input length fourfold (1,000 to 4,000 ticks) changed elapsed
time from 6.60 to 22.56 ms for one stage and 23.98 to 93.43 ms for four stages.
There is no observed quadratic growth in these cases. More execution boundaries
add serialization, coordination and callback overhead, as expected.

With 1,000 ticks and two stages, raising dictionary membership from 16 to 1,024
keys changed time from 16.57 to 20.89 ms. Only the initial frame contains the
whole dictionary; subsequent frames change one key. Increasing the count-window
capacity from 16 to 1,024 changed time from 12.59 to 12.04 ms. These observations
support incremental boundary work rather than full history copying per tick.

Peak process RSS was 68.5–71.9 MiB. This includes the interpreter, libraries,
input fixture and decoded graph state; it is not a measurement of queue bytes.
The admission implementation counts queued and evaluating frames and releases
capacity after forwarding. Each producer also has one separately bounded frame
under construction per destination, as documented in RFC 0038.
