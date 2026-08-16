# A/B: container access (2026-08-16)

Commit-level A/B of the container Out mutation hoist
(`perf/container-access`) against main @ 011f927d4. Back-to-back, 15
fresh-process samples, cycle scale 30, `--mode hg-cpp`.

| workload | main | branch | delta |
|---|---|---|---|
| `audit_convert_collect_std` | 22.195s +/- 0.082s | 22.182s +/- 0.171s | parity |
| `tick_std` | 0.792s +/- 0.003s | 0.802s +/- 0.006s | parity (run drift; tick has no container writes) |

Raw matrices: `matrix-20260816-124941.md` (branch),
`matrix-20260816-125754.md` (main).

**Verdict: parity.** The set-output scenario is dominated by
conversion machinery (~370us/cycle), so the hoisted per-call scope
validation is below the floor. The change's value is structural (one
validation per selector instead of per call, consistent with the
scalar write path); the two FALSIFIED audit items (O10 parent
re-stamp, O5 input-view caching) are recorded in the commit and at the
code sites.
