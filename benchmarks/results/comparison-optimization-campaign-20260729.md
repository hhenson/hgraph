# Optimization campaign capstone — hg-linux, 2026-07-29

One day of merged optimization work, measured before and after on the same box
(Core Ultra 7 155H, pinned P-core, quiet machine), against released hgraph
0.5.33 (`upstream-cpp`, itself freshly optimized: -DNDEBUG hgraph#364, -O3 +
LTO hgraph#366).

Merged between the morning and evening runs: hg_cpp #185 (raw tp_getset
TimeSeries getters), #186 (macOS IPO), #187 (static-node invocation frames,
RFC 0008), #191 (single-projection lifted kernels + header-inline TypeRef
accessors), on top of #182 (Linux interposition + IPO) from the prior day.

Sources: `matrix-20260729-085844.md` (morning: pre-campaign hg_cpp vs 0.5.33),
`matrix-20260729-155242.md` (evening: merged main; its legacy column is
DISTORTED — the run pinned both modes to one core, which serializes the
legacy engine's internal threading; use it for the hg_cpp column only),
`matrix-20260729-163133.md` (legacy re-measured fairly on a six-P-core
affinity set; matches the morning legacy numbers, confirming the correction).

| scenario | legacy 0.5.33 (fair) | hg_cpp (evening) | ratio | morning ratio |
|---|---|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1.313s | 0.079s | x16.62 | x16.78 |
| Wide/deep graph - Python nodes (`construct_py`) | 0.225s | 0.125s | x1.80 | x1.69 |
| Feedback hot loop - native add (`tick_std`) | 0.093s | 0.050s | x1.86 | x1.48 |
| Five-node Python compute chain (`tick_py`) | 0.038s | 0.035s | x1.09 | x0.87 |
| Integer arithmetic (`type_int_std`) | 0.039s | 0.021s | x1.86 | x1.50 |
| Floating-point arithmetic (`type_float_std`) | 0.038s | 0.021s | x1.81 | x1.41 |
| String concatenation (`type_str_std`) | 0.041s | 0.023s | x1.78 | x1.47 |
| CompoundScalar field access - native operators (`type_cs_std`) | 0.038s | 0.025s | x1.52 | x1.21 |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 0.031s | 0.030s | x1.03 | x1.06 |
| Map and reduce - native child graph (`tsd_dense_std`) | 0.263s | 0.172s | x1.53 | x1.24 |
| Map and reduce - Python map child (`tsd_dense_py`) | 0.184s | 0.194s | x0.95 | x0.82 |
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 0.144s | 0.058s | x2.48 | x2.09 |
| Map and reduce with key replacement (`tsd_churn_std`) | 0.267s | 0.085s | x3.14 | x2.94 |
| Python map with key replacement (`tsd_churn_py`) | 0.190s | 0.069s | x2.75 | x2.55 |
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 0.437s | 0.293s | x1.49 | x1.37 |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 3.875s | 1.512s | x2.56 | x2.38 |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 0.153s | 0.033s | x4.64 | x4.49 |
| Reference service - native implementation (`service_reference_std`) | 0.028s | 0.018s | x1.56 | x1.35 |
| Reference service - Python implementation (`service_reference_py`) | 0.033s | 0.023s | x1.43 | x1.16 |
| Request/reply service - native implementation (`service_request_reply_std`) | 0.140s | 0.069s | x2.03 | x1.65 |
| Request/reply service - Python implementation (`service_request_reply_py`) | 0.124s | 0.078s | x1.59 | x1.35 |
| Subscription service - native implementation (`service_subscription_std`) | 0.210s | 0.056s | x3.75 | x3.35 |
| Subscription service - Python implementation (`service_subscription_py`) | 0.250s | 0.096s | x2.60 | x2.43 |
| Duplex adaptor - native implementation (`adaptor_std`) | 0.030s | 0.016s | x1.88 | x1.50 |
| Duplex adaptor - Python implementation (`adaptor_py`) | 0.027s | 0.019s | x1.42 | x1.25 |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 0.070s | 0.057s | x1.23 | x1.03 |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 0.055s | 0.064s | x0.86 | x0.73 |
| **aggregate** | **8.336s** | **3.321s** | **x2.51** | - |

Caveat — interpreter versions differ between the endpoints: the morning
matrix ran Python 3.14.4 while both evening matrices ran 3.12.13 (the
capstone worktree's environments resolved 3.12). Morning-vs-evening ratio
movements therefore carry an interpreter confound on the python-node
scenarios and are indicative rather than attributive. The per-change
attributions below rest on same-interpreter controlled measurements taken
during the campaign, not on this table.

Observations:

- hg_cpp leads 25 of 27 scenarios; 25 of 27 ratios improved over the
  morning run. The two declines (`construct_std` x16.78 -> x16.62,
  `type_cs_py` x1.06 -> x1.03) are legacy-side run-to-run movement at
  noise scale, not hg_cpp regressions (hg_cpp absolute times were equal
  or better in both).
- `tick_py` moved from x0.87 to x1.09 in this table (interpreter confound
  applies). The controlled same-interpreter measurement of the #185
  boundary work on this box (Python 3.14 pack path, main vs #185 branch,
  same morning) showed tick_py 0.046s -> 0.042s (-8.7%); the mac
  like-for-like micro-bench attributed the mechanism (.value read
  40ns -> 26ns).
- The two remaining sub-parity scenarios narrowed and have owned
  follow-ups: `tsd_dense_py` x0.82 -> x0.95 and `service_adaptor_py`
  x0.73 -> x0.86 (keyed-route resolution: RFC 0008; observer notify:
  issue #192).
- Absolute cross-machine comparisons remain invalid; same-machine ratios are
  the convention (the mac/155H hardware factor is ~1.59x on identical
  CPython work).
- Raw sample records: `raw-20260729-085844.json`, `raw-20260729-155242.json`,
  `raw-20260729-163133.json`.
