# RFC 0036 stack: before/after performance and memory comparison

Before = main at 7fff96182 (just before the RFC 0036 stack merged); after = main at 59ecd06d6 (#758, #759, #760, #762 merged).
Both trees were built as optimized `current` wheels by `benchmarks/orchestrate.py` and run back to back on an otherwise idle machine.
Every number below is computed by `benchmarks/results/rfc0036-before-after-20260906/report.py` from the raw orchestrator outputs listed at the end (every sample, source fingerprint, native module and compiler is in them).

- before: source fingerprint `78803557f58a9447…`, revision `7fff96182cb5`, Apple clang version 21.0.0 (clang-2100.1.1.101), Python 3.14.7, Apple M4 Max
- after: source fingerprint `bc45582f37c393f5…`, revision `59ecd06d6176`, Apple clang version 21.0.0 (clang-2100.1.1.101), Python 3.14.7, Apple M4 Max

## Performance, pass 1 (core + diagnostic suites, 5 samples, before tree first)

82 scenarios; median after/before x1.011; geometric mean x1.016; 10 slower and 0 faster beyond the noise band (twice the summed MADs, floor 3%).

| scenario | group | suite | before s | after s | after/before | delta | noise band | verdict | RSS before MB | RSS after MB |
|---|---|---|---|---|---|---|---|---|---|---|
| `adaptor_py` | Adaptors | core | 0.011523 | 0.011791 | x1.023 | +2.3% | ±6.7% | noise | 66.9 | 67.0 |
| `adaptor_std` | Adaptors | core | 0.010451 | 0.010823 | x1.036 | +3.6% | ±6.4% | noise | 66.9 | 66.9 |
| `audit_convert_collect_std` | Audited operators | diagnostic | 0.027141 | 0.026863 | x0.990 | -1.0% | ±4.6% | noise | 67.2 | 67.1 |
| `audit_eq_tuple_std` | Audited operators | diagnostic | 0.015219 | 0.015282 | x1.004 | +0.4% | ±4.9% | noise | 66.8 | 66.9 |
| `audit_json_roundtrip_std` | Audited operators | diagnostic | 0.015496 | 0.016011 | x1.033 | +3.3% | ±4.1% | noise | 67.0 | 67.2 |
| `audit_map_transform_std` | Audited operators | diagnostic | 0.045051 | 0.045461 | x1.009 | +0.9% | ±3.2% | noise | 66.8 | 67.1 |
| `audit_ref_race_std` | Audited operators | diagnostic | 0.018629 | 0.019248 | x1.033 | +3.3% | ±8.1% | noise | 67.3 | 67.1 |
| `audit_running_mean_std` | Audited operators | diagnostic | 0.023260 | 0.023678 | x1.018 | +1.8% | ±10.2% | noise | 66.6 | 66.7 |
| `audit_stream_buffered_std` | Audited operators | diagnostic | 0.005807 | 0.005881 | x1.013 | +1.3% | ±2.5% | noise | 66.8 | 67.0 |
| `audit_string_match_std` | Audited operators | diagnostic | 0.019908 | 0.019904 | x1.000 | -0.0% | ±3.4% | noise | 67.0 | 67.2 |
| `audit_take_drop_std` | Audited operators | diagnostic | 0.014055 | 0.014004 | x0.996 | -0.4% | ±3.0% | noise | 66.8 | 66.8 |
| `audit_tsd_getitem_std` | Audited operators | diagnostic | 0.033046 | 0.033061 | x1.000 | +0.0% | ±2.6% | noise | 67.0 | 67.1 |
| `construct_dispatch_cases` | Graph construction | core | 0.101021 | 0.103979 | x1.029 | +2.9% | ±2.0% | noise | 77.9 | 78.0 |
| `construct_higher_order_py` | Graph construction | core | 0.015901 | 0.016369 | x1.029 | +2.9% | ±0.4% | noise | 75.0 | 75.2 |
| `construct_overloads` | Graph construction | core | 0.012385 | 0.012590 | x1.017 | +1.7% | ±2.7% | noise | 70.2 | 70.1 |
| `construct_py` | Graph construction | core | 0.073176 | 0.072814 | x0.995 | -0.5% | ±4.2% | noise | 83.7 | 83.7 |
| `construct_std` | Graph construction | core | 0.026183 | 0.027176 | x1.038 | +3.8% | ±1.5% | SLOWER | 75.9 | 75.9 |
| `mesh_std` | Nested graphs | core | 0.015554 | 0.015655 | x1.006 | +0.6% | ±2.9% | noise | 67.9 | 68.0 |
| `python_generator_boundary` | Python boundary | diagnostic | 0.007747 | 0.008372 | x1.081 | +8.1% | ±7.6% | SLOWER | 66.6 | 66.7 |
| `python_global_state_boundary` | Python boundary | diagnostic | 0.015801 | 0.016049 | x1.016 | +1.6% | ±6.8% | noise | 66.8 | 66.8 |
| `python_owned_construct_native` | Python-owned structured scalars | diagnostic | 0.016267 | 0.016845 | x1.036 | +3.6% | ±8.4% | noise | 66.9 | 66.9 |
| `python_owned_construct_python` | Python-owned structured scalars | diagnostic | 0.027501 | 0.029040 | x1.056 | +5.6% | ±6.7% | noise | 66.9 | 67.0 |
| `python_owned_dedup_native` | Python-owned structured scalars | diagnostic | 0.011249 | 0.011750 | x1.045 | +4.5% | ±1.1% | SLOWER | 66.6 | 66.7 |
| `python_owned_dedup_python` | Python-owned structured scalars | diagnostic | 0.013405 | 0.013124 | x0.979 | -2.1% | ±4.1% | noise | 66.7 | 66.7 |
| `python_owned_pass_through_native` | Python-owned structured scalars | diagnostic | 0.009226 | 0.009665 | x1.048 | +4.8% | ±4.0% | SLOWER | 66.6 | 66.7 |
| `python_owned_pass_through_python` | Python-owned structured scalars | diagnostic | 0.008104 | 0.008457 | x1.044 | +4.4% | ±6.7% | noise | 66.6 | 66.7 |
| `python_owned_project_one_native` | Python-owned structured scalars | diagnostic | 0.010866 | 0.011508 | x1.059 | +5.9% | ±3.7% | SLOWER | 67.2 | 66.7 |
| `python_owned_project_one_python` | Python-owned structured scalars | diagnostic | 0.010745 | 0.011171 | x1.040 | +4.0% | ±3.4% | SLOWER | 66.7 | 66.8 |
| `python_owned_project_several_native` | Python-owned structured scalars | diagnostic | 0.015285 | 0.015710 | x1.028 | +2.8% | ±5.2% | noise | 66.7 | 66.9 |
| `python_owned_project_several_python` | Python-owned structured scalars | diagnostic | 0.017006 | 0.017101 | x1.006 | +0.6% | ±3.1% | noise | 66.7 | 66.8 |
| `python_sink_boundary` | Python boundary | diagnostic | 0.012257 | 0.012721 | x1.038 | +3.8% | ±6.0% | noise | 66.7 | 66.8 |
| `reduce_dynamic_tsl_std` | C++-first - dynamic TSL | diagnostic | 0.014174 | 0.014173 | x1.000 | -0.0% | ±0.9% | noise | 67.4 | 67.6 |
| `reduce_fixed_tsl_ordered_std` | Reduce | diagnostic | 0.015283 | 0.015405 | x1.008 | +0.8% | ±2.8% | noise | 67.0 | 67.0 |
| `reduce_tsd_nested_graph_std` | Reduce | diagnostic | 0.049557 | 0.052871 | x1.067 | +6.7% | ±6.1% | SLOWER | 67.2 | 67.4 |
| `reduce_tsd_python_combiner` | Reduce | diagnostic | 0.071092 | 0.071342 | x1.004 | +0.4% | ±5.6% | noise | 67.7 | 67.7 |
| `reduce_tsd_without_zero_std` | Reduce | diagnostic | 0.022693 | 0.022156 | x0.976 | -2.4% | ±4.6% | noise | 67.1 | 67.1 |
| `scheduler_conflated_fixed_tsl_std` | Scheduler | diagnostic | 0.040038 | 0.040834 | x1.020 | +2.0% | ±1.3% | noise | 67.1 | 67.2 |
| `scheduler_fan_in_std` | Scheduler | diagnostic | 0.079208 | 0.077301 | x0.976 | -2.4% | ±2.1% | noise | 66.9 | 67.0 |
| `scheduler_fan_out_std` | Scheduler | diagnostic | 0.055985 | 0.055440 | x0.990 | -1.0% | ±2.9% | noise | 66.8 | 67.4 |
| `service_adaptor_py` | Adaptors | core | 0.027982 | 0.027354 | x0.978 | -2.2% | ±5.1% | noise | 67.4 | 67.6 |
| `service_adaptor_std` | Adaptors | core | 0.029143 | 0.028303 | x0.971 | -2.9% | ±2.6% | noise | 67.4 | 67.5 |
| `service_reference_py` | Services | core | 0.013262 | 0.013320 | x1.004 | +0.4% | ±7.0% | noise | 66.9 | 67.0 |
| `service_reference_std` | Services | core | 0.010997 | 0.010706 | x0.974 | -2.6% | ±9.1% | noise | 66.9 | 66.9 |
| `service_request_reply_multiple_paths_std` | Services | diagnostic | 0.017466 | 0.017767 | x1.017 | +1.7% | ±6.6% | noise | 67.5 | 67.6 |
| `service_request_reply_py` | Services | core | 0.025756 | 0.026359 | x1.023 | +2.3% | ±2.8% | noise | 67.4 | 67.5 |
| `service_request_reply_std` | Services | core | 0.026305 | 0.026376 | x1.003 | +0.3% | ±4.6% | noise | 67.5 | 67.4 |
| `service_subscription_py` | Services | core | 0.043803 | 0.043296 | x0.988 | -1.2% | ±1.7% | noise | 68.1 | 68.0 |
| `service_subscription_std` | Services | core | 0.030574 | 0.032390 | x1.059 | +5.9% | ±4.0% | SLOWER | 67.5 | 67.5 |
| `switch_alternating_branch_sizes_std` | Nested graphs | core | 0.125545 | 0.125992 | x1.004 | +0.4% | ±2.5% | noise | 67.0 | 67.0 |
| `switch_keyed_collection_std` | Nested graphs | core | 0.626138 | 0.619992 | x0.990 | -1.0% | ±0.9% | noise | 68.2 | 68.3 |
| `tick_py` | Scheduler | core | 0.019781 | 0.020979 | x1.061 | +6.1% | ±5.7% | SLOWER | 66.8 | 66.8 |
| `tick_std` | Scheduler | core | 0.024545 | 0.025024 | x1.020 | +2.0% | ±1.8% | noise | 66.6 | 66.6 |
| `tsd_capacity_growth_std` | TSD - key lifecycle | diagnostic | 0.034778 | 0.034678 | x0.997 | -0.3% | ±2.4% | noise | 81.7 | 81.8 |
| `tsd_churn_map_std` | TSD - key lifecycle | diagnostic | 0.032189 | 0.032364 | x1.005 | +0.5% | ±2.5% | noise | 67.4 | 67.5 |
| `tsd_churn_py` | TSD - key lifecycle | core | 0.030724 | 0.031300 | x1.019 | +1.9% | ±2.1% | noise | 67.9 | 68.1 |
| `tsd_churn_reduce_std` | TSD - key lifecycle | diagnostic | 0.008204 | 0.008173 | x0.996 | -0.4% | ±0.8% | noise | 67.2 | 67.3 |
| `tsd_churn_source_std` | TSD - key lifecycle | diagnostic | 0.003111 | 0.003143 | x1.010 | +1.0% | ±1.9% | noise | 66.8 | 66.9 |
| `tsd_churn_std` | TSD - key lifecycle | core | 0.037739 | 0.037698 | x0.999 | -0.1% | ±4.1% | noise | 67.8 | 68.4 |
| `tsd_clear_repopulate_std` | TSD - key lifecycle | diagnostic | 0.427516 | 0.433203 | x1.013 | +1.3% | ±1.1% | noise | 70.8 | 71.0 |
| `tsd_dense_map_std` | TSD - dense | diagnostic | 0.064920 | 0.072043 | x1.110 | +11.0% | ±3.0% | SLOWER | 67.4 | 67.5 |
| `tsd_dense_py` | TSD - dense | core | 0.083205 | 0.082528 | x0.992 | -0.8% | ±5.5% | noise | 67.9 | 68.0 |
| `tsd_dense_reduce_std` | TSD - dense | diagnostic | 0.034292 | 0.033711 | x0.983 | -1.7% | ±2.4% | noise | 67.2 | 67.4 |
| `tsd_dense_source_std` | TSD - dense | diagnostic | 0.013798 | 0.013785 | x0.999 | -0.1% | ±3.7% | noise | 66.8 | 66.9 |
| `tsd_dense_std` | TSD - dense | core | 0.093290 | 0.089716 | x0.962 | -3.8% | ±22.3% | noise | 67.7 | 67.6 |
| `tsd_dense_strkeys_std` | TSD - dense | diagnostic | 0.106278 | 0.104828 | x0.986 | -1.4% | ±9.8% | noise | 67.7 | 67.8 |
| `tsd_explicit_key_set_std` | TSD - key lifecycle | diagnostic | 0.013177 | 0.013261 | x1.006 | +0.6% | ±4.3% | noise | 68.0 | 67.5 |
| `tsd_key_reactivation_std` | TSD - key lifecycle | diagnostic | 0.022393 | 0.022768 | x1.017 | +1.7% | ±2.3% | noise | 67.7 | 67.7 |
| `tsd_sparse_large_capacity_std` | TSD - sparse | diagnostic | 0.902358 | 0.926156 | x1.026 | +2.6% | ±2.2% | noise | 136.2 | 136.6 |
| `tsd_sparse_map_std` | TSD - sparse | diagnostic | 0.024793 | 0.025075 | x1.011 | +1.1% | ±6.7% | noise | 71.7 | 72.4 |
| `tsd_sparse_reduce_std` | TSD - sparse | diagnostic | 0.010677 | 0.010746 | x1.006 | +0.6% | ±6.4% | noise | 69.5 | 69.0 |
| `tsd_sparse_source_std` | TSD - sparse | diagnostic | 0.003922 | 0.003958 | x1.009 | +0.9% | ±8.8% | noise | 67.0 | 67.1 |
| `tsd_sparse_std` | TSD - sparse | core | 0.027659 | 0.031492 | x1.139 | +13.9% | ±15.8% | noise | 73.9 | 73.9 |
| `tsd_two_input_intersection_std` | TSD - key lifecycle | diagnostic | 0.018321 | 0.018488 | x1.009 | +0.9% | ±8.4% | noise | 70.4 | 70.6 |
| `tsd_two_input_union_std` | TSD - key lifecycle | diagnostic | 0.033274 | 0.033679 | x1.012 | +1.2% | ±2.5% | noise | 76.2 | 76.8 |
| `tss_add_remove_std` | Value types | diagnostic | 0.009527 | 0.009558 | x1.003 | +0.3% | ±3.6% | noise | 67.0 | 67.0 |
| `type_cs_py` | Value types | core | 0.014672 | 0.014782 | x1.007 | +0.7% | ±4.5% | noise | 66.8 | 66.9 |
| `type_cs_std` | Value types | core | 0.013348 | 0.013798 | x1.034 | +3.4% | ±3.8% | noise | 66.7 | 66.8 |
| `type_float_std` | Value types | core | 0.011585 | 0.011739 | x1.013 | +1.3% | ±9.1% | noise | 67.2 | 66.8 |
| `type_int_std` | Value types | core | 0.011601 | 0.012097 | x1.043 | +4.3% | ±5.8% | noise | 66.8 | 66.8 |
| `type_str_std` | Value types | core | 0.012621 | 0.013387 | x1.061 | +6.1% | ±8.4% | noise | 66.7 | 66.8 |
| `type_tsb_partial_fields_std` | Value types | diagnostic | 0.020754 | 0.020579 | x0.992 | -0.8% | ±7.5% | noise | 66.9 | 66.9 |
| `type_tsw_append_evict_std` | Value types | diagnostic | 0.009840 | 0.010185 | x1.035 | +3.5% | ±7.1% | noise | 66.7 | 67.4 |

## Performance, pass 2 (reversed order, 7 samples, after tree first: the cells the first measurement session had flagged, plus controls)

| scenario | pass 1 after/before (5 samples, before first) | pass 2 after/before (7 samples, after first) | pooled after/before | verdict |
|---|---|---|---|---|
| `construct_higher_order_py` | x1.029 | x1.000 | x1.005 | run-order drift (not reproduced) |
| `construct_py` | x0.995 | x0.991 | x0.992 | run-order drift (not reproduced) |
| `python_owned_dedup_native` | x1.045 | x1.035 | x1.037 | run-order drift (not reproduced) |
| `python_owned_dedup_python` | x0.979 | x1.021 | x1.002 | run-order drift (not reproduced) |
| `tick_std` | x1.020 | x0.988 | x0.997 | run-order drift (not reproduced) |
| `tsd_dense_std` | x0.962 | x0.968 | x0.965 | run-order drift (not reproduced) |
| `tsd_dense_strkeys_std` | x0.986 | x0.949 | x0.957 | run-order drift (not reproduced) |
| `tsd_two_input_union_std` | x1.012 | x1.001 | x1.007 | run-order drift (not reproduced) |
| `type_tsb_partial_fields_std` | x0.992 | x0.997 | x0.996 | run-order drift (not reproduced) |
| `type_tsw_append_evict_std` | x1.035 | x1.020 | x1.022 | run-order drift (not reproduced) |

## Performance, pass 3 (reversed order, 7 samples, after tree first: the cells pass 1 above flagged, plus controls)

| scenario | pass 1 after/before (5 samples, before first) | pass 3 after/before (7 samples, after first) | pooled after/before | verdict |
|---|---|---|---|---|
| `construct_std` | x1.038 | x1.006 | x1.011 | run-order drift (not reproduced) |
| `python_generator_boundary` | x1.081 | x1.060 | x1.064 | run-order drift (not reproduced) |
| `python_owned_dedup_native` | x1.045 | x0.997 | x1.018 | run-order drift (not reproduced) |
| `python_owned_pass_through_native` | x1.048 | x1.014 | x1.025 | run-order drift (not reproduced) |
| `python_owned_project_one_native` | x1.059 | x1.020 | x1.036 | run-order drift (not reproduced) |
| `python_owned_project_one_python` | x1.040 | x1.015 | x1.013 | run-order drift (not reproduced) |
| `reduce_tsd_nested_graph_std` | x1.067 | x1.004 | x1.037 | run-order drift (not reproduced) |
| `service_subscription_std` | x1.059 | x1.011 | x1.033 | run-order drift (not reproduced) |
| `tick_py` | x1.061 | x1.020 | x1.024 | run-order drift (not reproduced) |
| `tick_std` | x1.020 | x0.999 | x1.008 | run-order drift (not reproduced) |
| `tsd_dense_map_std` | x1.110 | x0.917 | x1.019 | run-order drift (not reproduced) |
| `type_tsb_partial_fields_std` | x0.992 | x1.008 | x1.006 | run-order drift (not reproduced) |

## Performance, alternation A on the dense TSD cells (after, before, after, before; 9 samples)

| run | `tsd_dense_source_std` s (MAD) | `tsd_dense_std` s (MAD) | `tsd_dense_strkeys_std` s (MAD) |
|---|---|---|---|
| after | 0.01381 (0.00004) | 0.08591 (0.00079) | 0.09933 (0.00099) |
| before | 0.01374 (0.00007) | 0.09314 (0.00215) | 0.10530 (0.00259) |
| after | 0.01369 (0.00009) | 0.08804 (0.00291) | 0.09932 (0.00059) |
| before | 0.01384 (0.00018) | 0.09442 (0.00046) | 0.10541 (0.00214) |

`tsd_dense_source_std` pooled after/before x1.001; `tsd_dense_std` pooled after/before x0.919; `tsd_dense_strkeys_std` pooled after/before x0.943.

## Performance, alternation B on the Python-boundary cells (after, before, after, before; 9 samples)

| run | `python_generator_boundary` s (MAD) | `reduce_tsd_nested_graph_std` s (MAD) | `tick_py` s (MAD) | `tsd_dense_map_std` s (MAD) |
|---|---|---|---|---|
| after | 0.00775 (0.00012) | 0.05104 (0.00130) | 0.02004 (0.00061) | 0.06780 (0.00402) |
| before | 0.00772 (0.00012) | 0.04825 (0.00034) | 0.02002 (0.00018) | 0.07128 (0.00321) |
| after | 0.00784 (0.00022) | 0.05206 (0.00096) | 0.02058 (0.00030) | 0.07092 (0.00195) |
| before | 0.00780 (0.00005) | 0.04849 (0.00034) | 0.01985 (0.00007) | 0.06463 (0.00115) |

`python_generator_boundary` pooled after/before x1.005; `reduce_tsd_nested_graph_std` pooled after/before x1.059; `tick_py` pooled after/before x1.019; `tsd_dense_map_std` pooled after/before x1.016.

## Performance, alternation C on the nested-graph reduce cells (after, before, after, before; 15 samples)

| run | `reduce_tsd_nested_graph_std` s (MAD) | `reduce_tsd_without_zero_std` s (MAD) | `tsd_churn_std` s (MAD) |
|---|---|---|---|
| after | 0.04643 (0.00044) | 0.02173 (0.00026) | 0.03719 (0.00025) |
| before | 0.04943 (0.00168) | 0.02213 (0.00040) | 0.03729 (0.00029) |
| after | 0.04961 (0.00246) | 0.02200 (0.00014) | 0.03736 (0.00015) |
| before | 0.04812 (0.00047) | 0.02205 (0.00027) | 0.03771 (0.00025) |

`reduce_tsd_nested_graph_std` pooled after/before x0.973; `reduce_tsd_without_zero_std` pooled after/before x0.990; `tsd_churn_std` pooled after/before x0.995.

## Performance, bisect across the four PR points (two rounds, before → PR 1 → PR 2 → PR 3 → after)

`reduce_tsd_nested_graph_std` was the one cell still 6% slower after alternation B; a wheel was built at each merged PR point and the cell run at every point twice in order.

| tree | commit | round 1 `reduce_tsd_nested_graph_std` s (MAD, 11 samples) | round 2 s (MAD) |
|---|---|---|---|
| before | 7fff96182 (before) | 0.04749 (0.00059) | 0.04886 (0.00092) |
| pr1 | d20af5cd8 (after PR 1) | 0.04948 (0.00112) | 0.04850 (0.00069) |
| pr2 | 84c79139b (after PR 2) | 0.05004 (0.00177) | 0.04921 (0.00160) |
| pr3 | ecb1832a1 (after PR 3) | 0.05046 (0.00212) | 0.05117 (0.00263) |
| after | 59ecd06d6 (after PR 4) | 0.04966 (0.00229) | 0.04868 (0.00153) |

## Memory (`memory_orchestrate.py`, process pass, 3 samples)

Peak RSS, peak increment over the ready process, memory retained after teardown and two GC passes, runtime load increment, retained type-record growth, and wall time per profile. Profiles beyond noise: none.

| profile | group | peak RSS MB before → after | peak increment MB before → after | retained MB before → after | runtime load MB before → after | type records before → after | seconds before → after | verdict |
|---|---|---|---|---|---|---|---|---|
| `construct_std__large` | Static graph | 77.359 → 77.438 | 10.875 → 10.797 | 10.875 → 10.797 | 44.125 → 44.328 | 28 → 28 | 0.030 → 0.031 | noise |
| `construct_std__medium` | Static graph | 70.656 → 70.562 | 3.984 → 3.921 | 3.984 → 3.921 | 44.204 → 44.313 | 28 → 28 | 0.010 → 0.010 | noise |
| `construct_std__novel_ten` | Process lifetime | 69.531 → 69.438 | 2.890 → 2.672 | 2.890 → 2.672 | 44.234 → 44.344 | 46 → 46 | 0.037 → 0.037 | noise |
| `construct_std__repeat_hundred` | Process lifetime | 68.531 → 68.672 | 1.937 → 1.906 | 1.937 → 1.906 | 44.156 → 44.391 | 28 → 28 | 0.288 → 0.289 | noise |
| `construct_std__repeat_once` | Process lifetime | 68.375 → 68.438 | 1.782 → 1.719 | 1.782 → 1.719 | 44.171 → 44.281 | 28 → 28 | 0.004 → 0.004 | noise |
| `construct_std__repeat_ten` | Process lifetime | 68.344 → 68.391 | 1.844 → 1.766 | 1.844 → 1.766 | 44.219 → 44.359 | 28 → 28 | 0.031 → 0.030 | noise |
| `construct_std__small` | Static graph | 68.922 → 68.938 | 2.219 → 2.250 | 2.219 → 2.250 | 44.141 → 44.360 | 28 → 28 | 0.005 → 0.005 | noise |
| `mesh_std__large` | Nested graphs | 70.156 → 70.312 | 3.594 → 3.609 | 3.594 → 3.609 | 44.250 → 44.328 | 118 → 118 | 0.007 → 0.007 | noise |
| `mesh_std__medium` | Nested graphs | 69.438 → 69.562 | 2.969 → 2.954 | 2.969 → 2.954 | 44.156 → 44.265 | 118 → 118 | 0.006 → 0.006 | noise |
| `mesh_std__small` | Nested graphs | 69.234 → 69.344 | 2.562 → 2.625 | 2.562 → 2.625 | 44.266 → 44.266 | 118 → 118 | 0.006 → 0.006 | noise |
| `reduce_dynamic_tsl_std__large` | C++-first dynamic storage | 70.375 → 70.375 | 3.797 → 3.781 | 3.797 → 3.781 | 44.219 → 44.359 | 64 → 64 | 0.008 → 0.008 | noise |
| `reduce_dynamic_tsl_std__medium` | C++-first dynamic storage | 69.109 → 69.078 | 2.469 → 2.453 | 2.469 → 2.453 | 44.203 → 44.344 | 64 → 64 | 0.006 → 0.006 | noise |
| `reduce_dynamic_tsl_std__small` | C++-first dynamic storage | 68.781 → 68.906 | 2.297 → 2.359 | 2.297 → 2.359 | 44.266 → 44.250 | 64 → 64 | 0.005 → 0.005 | noise |
| `reduce_tsd_nested_graph_std__large` | Nested graphs | 68.781 → 68.828 | 2.219 → 2.188 | 2.219 → 2.188 | 44.250 → 44.344 | 65 → 65 | 0.006 → 0.006 | noise |
| `reduce_tsd_nested_graph_std__medium` | Nested graphs | 68.750 → 68.875 | 2.078 → 2.172 | 2.078 → 2.172 | 44.235 → 44.235 | 65 → 65 | 0.005 → 0.005 | noise |
| `reduce_tsd_nested_graph_std__small` | Nested graphs | 68.672 → 68.719 | 2.093 → 2.156 | 2.093 → 2.156 | 44.250 → 44.234 | 65 → 65 | 0.004 → 0.004 | noise |
| `service_adaptor_py__large` | Services | 69.125 → 69.188 | 2.609 → 2.500 | 2.609 → 2.500 | 44.204 → 44.266 | 124 → 124 | 0.009 → 0.009 | noise |
| `service_adaptor_py__medium` | Services | 69.047 → 69.016 | 2.453 → 2.328 | 2.453 → 2.328 | 44.172 → 44.391 | 124 → 124 | 0.005 → 0.005 | noise |
| `service_adaptor_py__repeat_fifty` | Process lifetime | 69.125 → 69.078 | 2.516 → 2.484 | 2.516 → 2.484 | 44.407 → 44.265 | 124 → 124 | 0.158 → 0.157 | noise |
| `service_adaptor_py__repeat_once` | Process lifetime | 68.984 → 68.984 | 2.468 → 2.391 | 2.468 → 2.391 | 44.266 → 44.140 | 124 → 124 | 0.004 → 0.004 | noise |
| `service_adaptor_py__repeat_ten` | Process lifetime | 69.156 → 69.031 | 2.438 → 2.453 | 2.438 → 2.453 | 44.281 → 44.281 | 124 → 124 | 0.033 → 0.033 | noise |
| `service_adaptor_py__small` | Services | 68.984 → 68.875 | 2.437 → 2.312 | 2.437 → 2.312 | 44.079 → 44.125 | 124 → 124 | 0.005 → 0.004 | noise |
| `switch_keyed_collection_std__large` | Nested graphs | 70.438 → 70.547 | 3.922 → 3.891 | 3.922 → 3.891 | 44.219 → 44.203 | 108 → 108 | 0.131 → 0.129 | noise |
| `switch_keyed_collection_std__medium` | Nested graphs | 69.891 → 69.750 | 3.313 → 3.188 | 3.313 → 3.188 | 44.156 → 44.296 | 108 → 108 | 0.069 → 0.068 | noise |
| `switch_keyed_collection_std__small` | Nested graphs | 69.172 → 69.234 | 2.688 → 2.485 | 2.688 → 2.485 | 44.125 → 44.234 | 108 → 108 | 0.022 → 0.022 | noise |
| `tick_py__long` | Bounded execution | 68.234 → 68.438 | 1.734 → 1.766 | 1.734 → 1.766 | 44.188 → 44.281 | 34 → 34 | 0.023 → 0.024 | noise |
| `tick_py__medium` | Bounded execution | 68.328 → 68.484 | 1.703 → 1.765 | 1.703 → 1.765 | 44.156 → 44.266 | 34 → 34 | 0.014 → 0.014 | noise |
| `tick_py__short` | Bounded execution | 68.266 → 68.375 | 1.735 → 1.719 | 1.735 → 1.719 | 44.266 → 44.266 | 34 → 34 | 0.006 → 0.006 | noise |
| `tick_std__long` | Bounded execution | 68.172 → 68.188 | 1.532 → 1.547 | 1.532 → 1.547 | 44.344 → 44.437 | 30 → 30 | 0.103 → 0.104 | noise |
| `tick_std__medium` | Bounded execution | 68.203 → 68.094 | 1.547 → 1.546 | 1.547 → 1.546 | 44.187 → 44.297 | 30 → 30 | 0.029 → 0.028 | noise |
| `tick_std__short` | Bounded execution | 68.266 → 68.156 | 1.578 → 1.578 | 1.578 → 1.578 | 44.204 → 44.297 | 30 → 30 | 0.006 → 0.006 | noise |
| `tsd_capacity_growth_std__long` | Keyed collections | 83.266 → 83.344 | 16.719 → 16.641 | 16.719 → 16.641 | 44.187 → 44.281 | 84 → 84 | 0.041 → 0.041 | noise |
| `tsd_capacity_growth_std__medium` | Keyed collections | 75.969 → 76.047 | 9.266 → 9.344 | 9.266 → 9.344 | 44.219 → 44.297 | 84 → 84 | 0.019 → 0.019 (+0.00) | noise |
| `tsd_capacity_growth_std__short` | Keyed collections | 70.156 → 70.188 | 3.500 → 3.563 | 3.500 → 3.563 | 44.172 → 44.453 | 84 → 84 | 0.006 → 0.006 | noise |
| `tsd_churn_std__long` | Keyed collections | 69.312 → 69.344 | 2.594 → 2.750 | 2.594 → 2.750 | 44.235 → 44.282 | 84 → 84 | 0.049 → 0.049 | noise |
| `tsd_churn_std__medium` | Keyed collections | 69.344 → 69.312 | 2.672 → 2.734 | 2.672 → 2.734 | 44.172 → 44.125 | 84 → 84 | 0.026 → 0.026 | noise |
| `tsd_churn_std__short` | Keyed collections | 69.328 → 69.453 | 2.703 → 2.734 | 2.703 → 2.734 | 44.188 → 44.204 | 84 → 84 | 0.009 → 0.008 | noise |
| `tsd_clear_repopulate_std__long` | Keyed collections | 72.438 → 72.625 | 5.750 → 5.813 | 5.750 → 5.813 | 44.265 → 44.297 | 84 → 84 | 0.432 → 0.433 | noise |
| `tsd_clear_repopulate_std__medium` | Keyed collections | 72.344 → 72.609 | 5.844 → 6.031 | 5.844 → 6.031 | 44.188 → 44.281 | 84 → 84 | 0.113 → 0.114 | noise |
| `tsd_clear_repopulate_std__short` | Keyed collections | 72.344 → 72.391 | 5.719 → 5.782 | 5.719 → 5.782 | 44.234 → 44.359 | 84 → 84 | 0.027 → 0.027 | noise |
| `tsd_dense_std__large` | Keyed collections | 69.203 → 69.172 | 2.657 → 2.594 | 2.657 → 2.594 | 44.156 → 44.281 | 84 → 84 | 0.014 → 0.014 | noise |
| `tsd_dense_std__medium` | Keyed collections | 69.125 → 69.250 | 2.484 → 2.469 | 2.484 → 2.469 | 44.266 → 44.343 | 84 → 84 | 0.009 → 0.009 | noise |
| `tsd_dense_std__small` | Keyed collections | 68.891 → 68.984 | 2.297 → 2.281 | 2.297 → 2.281 | 44.141 → 44.297 | 84 → 84 | 0.005 → 0.005 | noise |
| `tsd_key_reactivation_std__long` | Keyed collections | 69.281 → 69.391 | 2.688 → 2.672 | 2.688 → 2.672 | 44.126 → 44.297 | 84 → 84 | 0.026 → 0.027 | noise |
| `tsd_key_reactivation_std__medium` | Keyed collections | 69.281 → 69.422 | 2.656 → 2.641 | 2.656 → 2.641 | 44.234 → 44.219 | 84 → 84 | 0.016 → 0.016 | noise |
| `tsd_key_reactivation_std__short` | Keyed collections | 69.266 → 69.281 | 2.641 → 2.672 | 2.641 → 2.672 | 44.204 → 44.297 | 84 → 84 | 0.007 → 0.007 | noise |
| `tsd_sparse_large_capacity_std__large` | Keyed collections | 138.031 → 138.406 | 71.437 → 71.734 | 62.765 → 63.031 | 44.062 → 44.343 | 84 → 84 | 0.701 → 0.715 | noise |
| `tsd_sparse_large_capacity_std__medium` | Keyed collections | 86.297 → 86.203 | 19.563 → 19.687 | 19.563 → 19.687 | 44.234 → 44.328 | 84 → 84 | 0.063 → 0.066 (+0.00) | noise |
| `tsd_sparse_large_capacity_std__small` | Keyed collections | 72.219 → 72.203 | 5.594 → 5.641 | 5.594 → 5.641 | 44.094 → 44.281 | 84 → 84 | 0.011 → 0.011 | noise |
| `tss_add_remove_std__large` | Value storage | 68.625 → 68.562 | 1.969 → 1.876 | 1.969 → 1.876 | 44.266 → 44.312 | 37 → 37 | 0.005 → 0.005 | noise |
| `tss_add_remove_std__medium` | Value storage | 68.453 → 68.547 | 1.921 → 1.828 | 1.921 → 1.828 | 44.297 → 44.281 | 37 → 37 | 0.005 → 0.005 | noise |
| `tss_add_remove_std__small` | Value storage | 68.578 → 68.406 | 1.906 → 1.765 | 1.906 → 1.765 | 44.125 → 44.265 | 37 → 37 | 0.005 → 0.005 | noise |
| `type_cs_py__long` | Value storage | 68.391 → 68.547 | 1.797 → 1.828 | 1.797 → 1.828 | 44.187 → 44.312 | 47 → 47 | 0.018 → 0.019 (+0.00) | noise |
| `type_cs_py__short` | Value storage | 68.344 → 68.391 | 1.844 → 1.860 | 1.844 → 1.860 | 44.063 → 44.265 | 47 → 47 | 0.005 → 0.005 | noise |
| `type_str_std__long` | Value storage | 68.203 → 68.453 | 1.719 → 1.703 | 1.719 → 1.703 | 44.266 → 44.250 | 28 → 28 | 0.016 → 0.016 | noise |
| `type_str_std__short` | Value storage | 68.109 → 68.328 | 1.687 → 1.672 | 1.687 → 1.672 | 44.109 → 44.344 | 28 → 28 | 0.005 → 0.005 | noise |
| `type_tsw_append_evict_std__long` | Value storage | 68.281 → 68.312 | 1.703 → 1.703 | 1.703 → 1.703 | 44.250 → 44.328 | 30 → 30 | 0.042 → 0.041 | noise |
| `type_tsw_append_evict_std__medium` | Value storage | 68.344 → 68.344 | 1.703 → 1.735 | 1.703 → 1.735 | 44.188 → 44.125 | 30 → 30 | 0.014 → 0.012 (-0.00) | noise |
| `type_tsw_append_evict_std__short` | Value storage | 68.266 → 68.406 | 1.625 → 1.687 | 1.625 → 1.687 | 44.281 → 44.297 | 30 → 30 | 0.006 → 0.006 | noise |

## Conclusion

Pass 1 puts the stack at median x1.011 over 82 scenarios. Of the cells beyond their noise band, 0 kept their sign in a reversed-order pass; the alternations give `tsd_dense_source_std` pooled after/before x1.001; `tsd_dense_std` pooled after/before x0.919; `tsd_dense_strkeys_std` pooled after/before x0.943; `python_generator_boundary` pooled after/before x1.005; `reduce_tsd_nested_graph_std` pooled after/before x1.059; `tick_py` pooled after/before x1.019; `tsd_dense_map_std` pooled after/before x1.016; `reduce_tsd_nested_graph_std` pooled after/before x0.973; `reduce_tsd_without_zero_std` pooled after/before x0.990; `tsd_churn_std` pooled after/before x0.995; the bisect shows no step at any PR point (every tree within the cell's ±5% run-to-run spread). Memory: 0 of 59 profiles beyond noise.

## Raw outputs

- `benchmarks/results/rfc0036-before-after-20260906/after/abab1-raw-20260906-192105.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/abab2-raw-20260906-192114.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/ababB1-raw-20260906-192417.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/ababB2-raw-20260906-192426.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/ababC1-raw-20260906-192604.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/ababC2-raw-20260906-192615.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/memory-raw-20260906-191943.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/pass1-raw-20260906-191439.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/pass2-raw-20260906-192052.json`
- `benchmarks/results/rfc0036-before-after-20260906/after/pass3-raw-20260906-192244.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/abab1-raw-20260906-192110.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/abab2-raw-20260906-192118.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/ababB1-raw-20260906-192422.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/ababB2-raw-20260906-192431.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/ababC1-raw-20260906-192609.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/ababC2-raw-20260906-192620.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/memory-raw-20260906-191712.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/pass1-raw-20260906-191337.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/pass2-raw-20260906-192101.json`
- `benchmarks/results/rfc0036-before-after-20260906/before/pass3-raw-20260906-192253.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/after-raw-20260906-193109.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/after-raw-20260906-193117.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/before-raw-20260906-193046.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/before-raw-20260906-193110.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/pr1-raw-20260906-193053.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/pr1-raw-20260906-193112.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/pr2-raw-20260906-193100.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/pr2-raw-20260906-193114.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/pr3-raw-20260906-193107.json`
- `benchmarks/results/rfc0036-before-after-20260906/bisect/pr3-raw-20260906-193115.json`
