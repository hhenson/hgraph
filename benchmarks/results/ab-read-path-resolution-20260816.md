# A/B: read-path resolution (2026-08-16)

Commit-level A/B of the read-path changes (`perf/read-path-resolution`
— static-node validity gated inside the invocation frame; cursor-cached
link-storage pointer) against main @ 011f927d4. Back-to-back, 15
fresh-process samples, cycle scale 30, `--mode hg-cpp`.

| workload | main | read-path branch | delta |
|---|---|---|---|
| `tick_std` | 0.799s +/- 0.003s | 0.780s +/- 0.004s | **-2.4%** |
| `type_int_std` | 0.337s +/- 0.006s | 0.339s +/- 0.006s | parity |
| `audit_take_drop_std` | 0.428s +/- 0.004s | 0.420s +/- 0.005s | **-1.9%** |
| `audit_running_mean_std` | 0.721s +/- 0.014s | 0.719s +/- 0.016s | parity |
| `audit_stream_buffered_std` | 0.155s +/- 0.002s | 0.152s +/- 0.002s | **-1.9%** |

Raw matrices: `matrix-20260816-122033.md` (branch),
`matrix-20260816-122411.md` (main).

The shape matches the audit's prediction: the win lands on STATIC-node
paths (the old gate built and resolved every slot view a second time
per tick), while lifted-kernel-dominated scenarios (`type_int_std`)
already skipped the generic gate and sit at parity. Independent of and
roughly additive with the write-path branch's -5.0%/-1.8%
(`ab-write-path-trust-20260816.md`).
