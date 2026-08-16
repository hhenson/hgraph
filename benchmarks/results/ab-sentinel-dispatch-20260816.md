# A/B: sentinel dispatch (2026-08-16)

Commit-level A/B of the completed no-value sentinel family + the 19
unconditional target-link read dispatches (`perf/sentinel-dispatch`)
against main @ d020bc009. Back-to-back, 15 fresh-process samples, cycle
scale 30, `--mode hg-cpp`.

| workload | main | branch | delta |
|---|---|---|---|
| `audit_tsd_getitem_std` | 0.985s +/- 0.008s | 0.952s +/- 0.008s | **-3.4%** |
| `audit_ref_race_std` | 0.555s +/- 0.007s | 0.549s +/- 0.006s | -1.1% (borderline) |
| `tick_std` (control) | 0.754s +/- 0.003s | 0.756s +/- 0.004s | parity |

Raw matrices: `matrix-20260816-141338.md` (branch),
`matrix-20260816-141722.md` (main).

The win lands on the dict link-read path — where the per-call
`target.valid()` branches and four try/catch fences were removed — and
the untouched scalar control stays at parity.
