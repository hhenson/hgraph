# Sparse shared-map performance comparison

The shared element lookup fix removes the observed linear notification and map-selection cost. It is **not performance-neutral**: longer confirmation runs show macOS regressions of 13–27% in several existing collection workloads. These are measured candidate/baseline differences; their specific code-generation or runtime cause has not been isolated.

Baseline: `7e45748c966e8cf6c3cb2fbf57adfbc259302040` (current main when tested). Candidate code: `9b0a668e5fc6ec62c3f18518a6ec530866c6f4b3`, [PR #1796](https://github.com/hhenson/hgraph/pull/1796). The linked runtime contract is [specification PR #130](https://github.com/hhenson/hgraph_spec/pull/130).

## Method

After opening the PR, ran every registered core and diagnostic scenario supported by the current runtime: **84 scenarios × 2 versions × 3 fresh-process samples on each platform**, all successful. Used the same updated scenario source against separately installed baseline and candidate wheels, alternating version order. Both cycle and size scales were 1. Apparent regressions were repeated seven times with cycle scale 10 and size scale 1. The runner times graph construction, execution and teardown. Measurements are wall time without CPU affinity; small changes can be noise. This matrix does not claim exhaustive coverage of every graph or native microbenchmark.

Full samples, medians, reproduction results and Python versions are in [measurements.json](measurements.json). Ratios below are candidate/baseline elapsed time; lower is better.

## Main reproduction

The user’s exact 1,000-cycle shape on macOS, including setup and teardown, improved from 6.56 to 3.37 µs/cycle at 250 mapped keys and from 82.94 to 33.28 µs/cycle at 4,000. Initializing and destroying N children still costs O(N), so this end-to-end measurement cannot establish steady-state complexity. Both versions execute exactly N initial consumer calls plus 1,000 update calls.

`benchmarks/repro_map_pass_through.py --source-size 8192` holds dictionary cardinality fixed and measures 20,000 updates after warmup, with five samples. Each update affects one reader.

| Platform | Mapped keys | Shared lookup before → after (µs/update) | Direct per-key control before → after |
|---|---:|---:|---:|
| macos | 250 | 5.18 → 1.35 | 7.71 → 9.65 |
| macos | 500 | 8.70 → 1.32 | 7.70 → 9.63 |
| macos | 1000 | 15.78 → 1.38 | 7.67 → 9.80 |
| macos | 2000 | 29.85 → 1.40 | 7.74 → 9.86 |
| macos | 4000 | 57.49 → 1.45 | 7.83 → 9.86 |
| linux | 250 | 9.59 → 2.02 | 18.78 → 18.14 |
| linux | 500 | 18.22 → 2.05 | 19.15 → 18.56 |
| linux | 1000 | 31.98 → 2.14 | 19.28 → 18.96 |
| linux | 2000 | 66.10 → 3.22 | 18.65 → 19.03 |
| linux | 4000 | 120.94 → 3.37 | 19.20 → 17.76 |

The direct-map control has its own collection costs; its slower macOS result is a negative impact, not evidence that the shared-lookup result is free of tradeoffs. The new selected/unselected scenarios also assert consumer counts in the compatibility suite. Native tests verify that adding structural readers adds zero observers to the dictionary value-tick surface.

## Longer confirmation runs

| Scenario | macOS ratio | Linux ratio |
|---|---:|---:|
| `python_generator_boundary` | — | 1.068× |
| `switch_alternating_branch_sizes_std` | — | 1.030× |
| `tsd_dense_strkeys_std` | 1.013× | 1.037× |
| `tsd_sparse_large_capacity_std` | 1.269× | 0.997× |
| `tsd_sparse_reduce_std` | 1.171× | 0.997× |
| `tsd_sparse_std` | 1.160× | 0.991× |
| `tsd_two_input_intersection_std` | 1.128× | 0.999× |
| `tsd_two_input_union_std` | 1.134× | 0.999× |

The macOS sparse reduction workload contains no map, so attributing all these differences to map scheduling alone would be unsupported. Linux initially showed 10–16% slower dense/multi-input cases; longer repeats reduced those differences to approximately 0–4%. The generator-boundary confirmation remains about 7% slower. No unrelated runtime tuning is included in this change.

## Complete matrix

| Scenario | macOS ratio | Linux ratio |
|---|---:|---:|
| `construct_std` | 0.989× | 1.013× |
| `construct_py` | 0.997× | 0.984× |
| `construct_higher_order_py` | 0.972× | 1.012× |
| `construct_dispatch_cases` | 0.988× | 1.036× |
| `construct_overloads` | 1.012× | 1.012× |
| `tick_std` | 0.999× | 0.875× |
| `tick_py` | 0.969× | 0.970× |
| `scheduler_fan_out_std` | 0.996× | 1.001× |
| `scheduler_fan_in_std` | 1.017× | 0.997× |
| `scheduler_conflated_fixed_tsl_std` | 0.996× | 1.047× |
| `python_generator_boundary` | 1.015× | 1.093× |
| `python_sink_boundary` | 0.970× | 1.077× |
| `python_global_state_boundary` | 0.977× | 1.019× |
| `type_int_std` | 1.024× | 1.004× |
| `type_float_std` | 0.986× | 1.042× |
| `type_str_std` | 1.005× | 0.994× |
| `type_cs_std` | 1.003× | 1.014× |
| `type_cs_py` | 1.008× | 0.991× |
| `python_owned_pass_through_native` | 0.980× | 1.075× |
| `python_owned_pass_through_python` | 0.983× | 1.030× |
| `python_owned_project_one_native` | 0.994× | 0.976× |
| `python_owned_project_one_python` | 1.003× | 1.004× |
| `python_owned_project_several_native` | 1.028× | 1.032× |
| `python_owned_project_several_python` | 0.977× | 1.004× |
| `python_owned_construct_native` | 0.970× | 1.058× |
| `python_owned_construct_python` | 0.987× | 1.008× |
| `python_owned_dedup_native` | 0.976× | 1.043× |
| `python_owned_dedup_python` | 0.993× | 1.008× |
| `type_tsb_partial_fields_std` | 0.997× | 1.049× |
| `type_tsw_append_evict_std` | 1.014× | 0.916× |
| `tss_add_remove_std` | 0.979× | 0.960× |
| `tsd_dense_std` | 1.023× | 1.003× |
| `tsd_dense_py` | 1.022× | 0.982× |
| `tsd_dense_source_std` | 0.989× | 1.025× |
| `tsd_dense_map_std` | 1.024× | 1.059× |
| `tsd_dense_reduce_std` | 1.027× | 1.010× |
| `tsd_dense_strkeys_std` | 1.004× | 1.162× |
| `tsd_sparse_std` | 1.112× | 0.988× |
| `tsd_sparse_source_std` | 1.003× | 1.018× |
| `tsd_sparse_map_std` | 1.052× | 1.008× |
| `tsd_sparse_reduce_std` | 1.124× | 1.064× |
| `tsd_sparse_large_capacity_std` | 1.202× | 0.992× |
| `tsd_shared_lookup_py` | 0.056× | 0.065× |
| `tsd_shared_unselected_py` | 0.044× | 0.044× |
| `tsd_churn_std` | 0.998× | 1.020× |
| `tsd_churn_py` | 1.007× | 1.057× |
| `tsd_churn_source_std` | 0.998× | 1.011× |
| `tsd_churn_map_std` | 1.006× | 1.016× |
| `tsd_churn_reduce_std` | 0.988× | 1.053× |
| `tsd_capacity_growth_std` | 1.077× | 0.991× |
| `tsd_clear_repopulate_std` | 0.999× | 1.002× |
| `tsd_key_reactivation_std` | 0.999× | 1.011× |
| `tsd_two_input_union_std` | 1.071× | 1.105× |
| `tsd_two_input_intersection_std` | 1.070× | 1.116× |
| `tsd_explicit_key_set_std` | 0.996× | 1.002× |
| `reduce_tsd_nested_graph_std` | 1.001× | 0.999× |
| `reduce_tsd_python_combiner` | 0.982× | 0.981× |
| `reduce_fixed_tsl_ordered_std` | 1.017× | 0.990× |
| `reduce_tsd_without_zero_std` | 0.997× | 1.025× |
| `reduce_dynamic_tsl_std` | 0.985× | 1.009× |
| `switch_alternating_branch_sizes_std` | 1.016× | 1.094× |
| `switch_keyed_collection_std` | 1.012× | 1.030× |
| `mesh_std` | 0.998× | 1.033× |
| `service_reference_std` | 0.951× | 1.053× |
| `service_reference_py` | 0.975× | 0.982× |
| `service_request_reply_std` | 0.994× | 1.036× |
| `service_request_reply_py` | 1.031× | 1.005× |
| `service_request_reply_multiple_paths_std` | 0.999× | 0.975× |
| `service_subscription_std` | 1.043× | 1.002× |
| `service_subscription_py` | 0.974× | 1.018× |
| `adaptor_std` | 0.982× | 0.953× |
| `adaptor_py` | 0.983× | 0.943× |
| `service_adaptor_std` | 0.992× | 1.025× |
| `service_adaptor_py` | 0.992× | 1.004× |
| `audit_convert_collect_std` | 1.007× | 1.004× |
| `audit_map_transform_std` | 0.972× | 1.002× |
| `audit_tsd_getitem_std` | 1.017× | 1.004× |
| `audit_eq_tuple_std` | 0.984× | 1.004× |
| `audit_string_match_std` | 0.956× | 1.026× |
| `audit_json_roundtrip_std` | 0.956× | 1.025× |
| `audit_ref_race_std` | 1.000× | 0.997× |
| `audit_stream_buffered_std` | 0.979× | 0.984× |
| `audit_take_drop_std` | 0.989× | 0.981× |
| `audit_running_mean_std` | 1.003× | 0.968× |

## Correctness and lifetime validation

- macOS and Linux: fresh native acceptance builds with all enabled extensions; 2,469 tests passed on each.
- macOS and Linux: stable-ABI wheels built using Python 3.12, installed into fresh Python 3.14 environments; 4,221 passed and 11 skipped on each full non-WIP suite.
- macOS installed-SDK consumer: 2 tests passed.
- Windows, MSVC 19.51: 2,451 native core tests passed; optional extensions disabled for this supplementary run.
- Linux Debug ASan/LSan: 2,448 of 2,452 tests clean. All functional assertions passed; four tests report leaks (16, 24, 352 and 16 bytes) in registry/cache/test allocations. The same four reports and sizes reproduce on the unmodified baseline. No new sanitizer finding was observed.
- Tests cover selection changes, multiple consumers of one element, unselected changes, removal/recreation, active/structural transitions, source teardown, and preservation of assembled-parent modification tracking.

The optimization applies to stable dictionary endpoints. Whole-value consumers still receive every value tick; membership changes, source replacement, assembled collection parents and movable forwarding targets retain the wider work required by their existing semantics.
