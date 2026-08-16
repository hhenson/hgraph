# A/B: write-path trust (2026-08-16)

Commit-level A/B of the write-path changes (`perf/write-path-trust` —
trusted apply-path ops access, one validation per mutation scope,
trusted mutation re-tags, parallel root-mutation API removed) against
main @ 011f927d4. Same machine, back-to-back, nothing else running.
15 fresh-process samples, cycle scale 30, `--mode hg-cpp`.

| workload | cycles | main | write-path trust | delta |
|---|---|---|---|---|
| `type_int_std` | 600000 | 0.338s +/- 0.006s | 0.321s +/- 0.002s | **-5.0%** |
| `tick_std` | 3000000 | 0.793s +/- 0.001s | 0.779s +/- 0.002s | **-1.8%** |

Raw matrices: `matrix-20260816-120210.md` (branch),
`matrix-20260816-120502.md` (main).

Both deltas sit outside the runs' MADs — the first measurable
end-to-end movement from this optimisation series, consistent with the
audit's prediction that the write path's repeated re-validation (4x
MIN_DT, 4x liveness, 5-7x Mutable derivations, two TypeRecord::valid
walks per set) was in the measurable 5-20%-of-cycle range, unlike the
payload-load micro-costs the earlier typed fast read removed
(`ab-typed-fast-read-20260816.md`, parity).
