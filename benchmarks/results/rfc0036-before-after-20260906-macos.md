# RFC 0036 stack: before/after performance and memory comparison

Before = main at 7fff96182 (just before the RFC 0036 stack merged); after = main at 59ecd06d6 (#758, #759, #760, #762 merged).
Both trees built as optimized `current` wheels by `benchmarks/orchestrate.py` (Python 3.14.7), run back to back on an otherwise idle Apple M4 Max, 5 samples per perf cell (median, MAD), 3 per memory profile.

## Performance (core + diagnostic suites)

82 scenarios; median after/before x1.005; geometric mean x1.005; 4 slower and 3 faster beyond the noise band (2x the summed MADs, floor 3%)

| scenario | group | suite | before s | after s | after/before | delta | noise band | verdict | RSS before MB | RSS after MB |
|---|---|---|---|---|---|---|---|---|---|---|
| `adaptor_py` | Adaptors | core | 0.011570 | 0.011655 | x1.007 | +0.7% | ±10.2% | noise | 67.0 | 66.9 |
| `adaptor_std` | Adaptors | core | 0.010349 | 0.010643 | x1.028 | +2.8% | ±2.9% | noise | 66.9 | 66.9 |
| `audit_convert_collect_std` | Audited operators | diagnostic | 0.026955 | 0.026794 | x0.994 | -0.6% | ±7.3% | noise | 67.0 | 67.1 |
| `audit_eq_tuple_std` | Audited operators | diagnostic | 0.015268 | 0.015535 | x1.017 | +1.7% | ±9.4% | noise | 66.8 | 66.8 |
| `audit_json_roundtrip_std` | Audited operators | diagnostic | 0.015825 | 0.016064 | x1.015 | +1.5% | ±2.0% | noise | 67.0 | 67.5 |
| `audit_map_transform_std` | Audited operators | diagnostic | 0.045672 | 0.045973 | x1.007 | +0.7% | ±1.7% | noise | 67.0 | 67.0 |
| `audit_ref_race_std` | Audited operators | diagnostic | 0.019016 | 0.019387 | x1.020 | +2.0% | ±1.7% | noise | 67.3 | 67.1 |
| `audit_running_mean_std` | Audited operators | diagnostic | 0.022638 | 0.024014 | x1.061 | +6.1% | ±13.6% | noise | 66.7 | 66.8 |
| `audit_stream_buffered_std` | Audited operators | diagnostic | 0.006036 | 0.005948 | x0.985 | -1.5% | ±2.1% | noise | 66.8 | 66.9 |
| `audit_string_match_std` | Audited operators | diagnostic | 0.019817 | 0.020106 | x1.015 | +1.5% | ±3.8% | noise | 67.1 | 67.2 |
| `audit_take_drop_std` | Audited operators | diagnostic | 0.013987 | 0.014137 | x1.011 | +1.1% | ±5.1% | noise | 66.7 | 66.8 |
| `audit_tsd_getitem_std` | Audited operators | diagnostic | 0.032611 | 0.033318 | x1.022 | +2.2% | ±3.5% | noise | 67.0 | 67.0 |
| `construct_dispatch_cases` | Graph construction | core | 0.108473 | 0.107030 | x0.987 | -1.3% | ±1.7% | noise | 77.9 | 78.1 |
| `construct_higher_order_py` | Graph construction | core | 0.017439 | 0.016718 | x0.959 | -4.1% | ±1.4% | faster | 75.0 | 75.1 |
| `construct_overloads` | Graph construction | core | 0.013289 | 0.012877 | x0.969 | -3.1% | ±3.3% | noise | 70.2 | 70.1 |
| `construct_py` | Graph construction | core | 0.077447 | 0.074801 | x0.966 | -3.4% | ±1.2% | faster | 83.6 | 83.8 |
| `construct_std` | Graph construction | core | 0.028152 | 0.027657 | x0.982 | -1.8% | ±2.3% | noise | 75.8 | 76.0 |
| `mesh_std` | Nested graphs | core | 0.015789 | 0.015763 | x0.998 | -0.2% | ±3.3% | noise | 67.9 | 68.0 |
| `python_generator_boundary` | Python boundary | diagnostic | 0.008014 | 0.007836 | x0.978 | -2.2% | ±8.6% | noise | 66.6 | 66.8 |
| `python_global_state_boundary` | Python boundary | diagnostic | 0.016379 | 0.016649 | x1.016 | +1.6% | ±3.5% | noise | 66.8 | 66.9 |
| `python_owned_construct_native` | Python-owned structured scalars | diagnostic | 0.016057 | 0.017140 | x1.067 | +6.7% | ±6.9% | noise | 66.9 | 66.9 |
| `python_owned_construct_python` | Python-owned structured scalars | diagnostic | 0.028719 | 0.029008 | x1.010 | +1.0% | ±2.8% | noise | 66.9 | 66.9 |
| `python_owned_dedup_native` | Python-owned structured scalars | diagnostic | 0.010986 | 0.011529 | x1.049 | +4.9% | ±2.9% | SLOWER | 66.9 | 66.8 |
| `python_owned_dedup_python` | Python-owned structured scalars | diagnostic | 0.013220 | 0.013855 | x1.048 | +4.8% | ±2.7% | SLOWER | 66.6 | 66.6 |
| `python_owned_pass_through_native` | Python-owned structured scalars | diagnostic | 0.009405 | 0.009599 | x1.021 | +2.1% | ±8.9% | noise | 66.5 | 66.7 |
| `python_owned_pass_through_python` | Python-owned structured scalars | diagnostic | 0.008225 | 0.008201 | x0.997 | -0.3% | ±2.6% | noise | 66.6 | 66.6 |
| `python_owned_project_one_native` | Python-owned structured scalars | diagnostic | 0.011305 | 0.011310 | x1.000 | +0.0% | ±4.1% | noise | 66.7 | 66.7 |
| `python_owned_project_one_python` | Python-owned structured scalars | diagnostic | 0.010997 | 0.011189 | x1.017 | +1.7% | ±8.2% | noise | 66.8 | 66.8 |
| `python_owned_project_several_native` | Python-owned structured scalars | diagnostic | 0.015432 | 0.015704 | x1.018 | +1.8% | ±7.0% | noise | 66.7 | 66.8 |
| `python_owned_project_several_python` | Python-owned structured scalars | diagnostic | 0.017084 | 0.017231 | x1.009 | +0.9% | ±9.9% | noise | 66.7 | 67.3 |
| `python_sink_boundary` | Python boundary | diagnostic | 0.012188 | 0.012425 | x1.019 | +1.9% | ±8.1% | noise | 66.8 | 66.8 |
| `reduce_dynamic_tsl_std` | C++-first - dynamic TSL | diagnostic | 0.014260 | 0.014257 | x1.000 | -0.0% | ±3.8% | noise | 67.5 | 67.6 |
| `reduce_fixed_tsl_ordered_std` | Reduce | diagnostic | 0.015373 | 0.015599 | x1.015 | +1.5% | ±2.1% | noise | 66.8 | 67.0 |
| `reduce_tsd_nested_graph_std` | Reduce | diagnostic | 0.049528 | 0.049041 | x0.990 | -1.0% | ±13.3% | noise | 67.2 | 67.8 |
| `reduce_tsd_python_combiner` | Reduce | diagnostic | 0.070171 | 0.071797 | x1.023 | +2.3% | ±1.8% | noise | 67.7 | 67.7 |
| `reduce_tsd_without_zero_std` | Reduce | diagnostic | 0.022845 | 0.022310 | x0.977 | -2.3% | ±3.6% | noise | 67.1 | 67.0 |
| `scheduler_conflated_fixed_tsl_std` | Scheduler | diagnostic | 0.040477 | 0.040358 | x0.997 | -0.3% | ±2.8% | noise | 67.0 | 67.2 |
| `scheduler_fan_in_std` | Scheduler | diagnostic | 0.079562 | 0.078420 | x0.986 | -1.4% | ±2.4% | noise | 66.8 | 67.0 |
| `scheduler_fan_out_std` | Scheduler | diagnostic | 0.056764 | 0.056124 | x0.989 | -1.1% | ±2.6% | noise | 66.9 | 66.9 |
| `service_adaptor_py` | Adaptors | core | 0.028239 | 0.027803 | x0.985 | -1.5% | ±2.7% | noise | 67.5 | 67.6 |
| `service_adaptor_std` | Adaptors | core | 0.028735 | 0.029185 | x1.016 | +1.6% | ±4.1% | noise | 67.4 | 67.4 |
| `service_reference_py` | Services | core | 0.013755 | 0.013456 | x0.978 | -2.2% | ±2.5% | noise | 67.0 | 67.1 |
| `service_reference_std` | Services | core | 0.011114 | 0.010820 | x0.974 | -2.6% | ±0.7% | noise | 67.2 | 66.9 |
| `service_request_reply_multiple_paths_std` | Services | diagnostic | 0.017907 | 0.017430 | x0.973 | -2.7% | ±9.0% | noise | 67.4 | 67.6 |
| `service_request_reply_py` | Services | core | 0.026364 | 0.026469 | x1.004 | +0.4% | ±6.0% | noise | 67.5 | 67.5 |
| `service_request_reply_std` | Services | core | 0.026302 | 0.027027 | x1.028 | +2.8% | ±3.7% | noise | 67.4 | 67.5 |
| `service_subscription_py` | Services | core | 0.042893 | 0.043483 | x1.014 | +1.4% | ±1.6% | noise | 67.6 | 68.0 |
| `service_subscription_std` | Services | core | 0.030604 | 0.031494 | x1.029 | +2.9% | ±3.5% | noise | 67.4 | 67.4 |
| `switch_alternating_branch_sizes_std` | Nested graphs | core | 0.126533 | 0.126476 | x1.000 | -0.0% | ±4.7% | noise | 66.9 | 67.0 |
| `switch_keyed_collection_std` | Nested graphs | core | 0.624624 | 0.629380 | x1.008 | +0.8% | ±2.5% | noise | 68.4 | 68.5 |
| `tick_py` | Scheduler | core | 0.020957 | 0.020956 | x1.000 | -0.0% | ±4.4% | noise | 66.8 | 66.9 |
| `tick_std` | Scheduler | core | 0.025067 | 0.024871 | x0.992 | -0.8% | ±3.9% | noise | 67.0 | 66.7 |
| `tsd_capacity_growth_std` | TSD - key lifecycle | diagnostic | 0.035572 | 0.035055 | x0.985 | -1.5% | ±4.2% | noise | 81.7 | 81.8 |
| `tsd_churn_map_std` | TSD - key lifecycle | diagnostic | 0.032127 | 0.032637 | x1.016 | +1.6% | ±1.4% | noise | 67.4 | 67.5 |
| `tsd_churn_py` | TSD - key lifecycle | core | 0.031075 | 0.031410 | x1.011 | +1.1% | ±2.0% | noise | 67.9 | 68.0 |
| `tsd_churn_reduce_std` | TSD - key lifecycle | diagnostic | 0.008299 | 0.008267 | x0.996 | -0.4% | ±1.0% | noise | 67.2 | 68.0 |
| `tsd_churn_source_std` | TSD - key lifecycle | diagnostic | 0.003147 | 0.003170 | x1.007 | +0.7% | ±5.7% | noise | 66.9 | 66.9 |
| `tsd_churn_std` | TSD - key lifecycle | core | 0.038024 | 0.037796 | x0.994 | -0.6% | ±1.0% | noise | 67.7 | 67.8 |
| `tsd_clear_repopulate_std` | TSD - key lifecycle | diagnostic | 0.430562 | 0.433652 | x1.007 | +0.7% | ±1.9% | noise | 71.1 | 71.0 |
| `tsd_dense_map_std` | TSD - dense | diagnostic | 0.075807 | 0.074716 | x0.986 | -1.4% | ±3.4% | noise | 67.5 | 67.5 |
| `tsd_dense_py` | TSD - dense | core | 0.082323 | 0.082076 | x0.997 | -0.3% | ±12.4% | noise | 67.9 | 67.9 |
| `tsd_dense_reduce_std` | TSD - dense | diagnostic | 0.034151 | 0.034162 | x1.000 | +0.0% | ±2.5% | noise | 67.3 | 67.3 |
| `tsd_dense_source_std` | TSD - dense | diagnostic | 0.013749 | 0.013768 | x1.001 | +0.1% | ±5.4% | noise | 66.8 | 67.4 |
| `tsd_dense_std` | TSD - dense | core | 0.086153 | 0.091563 | x1.063 | +6.3% | ±7.5% | noise | 67.7 | 67.8 |
| `tsd_dense_strkeys_std` | TSD - dense | diagnostic | 0.108054 | 0.100065 | x0.926 | -7.4% | ±5.0% | faster | 68.1 | 67.7 |
| `tsd_explicit_key_set_std` | TSD - key lifecycle | diagnostic | 0.013267 | 0.013297 | x1.002 | +0.2% | ±2.4% | noise | 67.5 | 67.6 |
| `tsd_key_reactivation_std` | TSD - key lifecycle | diagnostic | 0.022843 | 0.023040 | x1.009 | +0.9% | ±2.7% | noise | 67.9 | 67.7 |
| `tsd_sparse_large_capacity_std` | TSD - sparse | diagnostic | 0.916635 | 0.923455 | x1.007 | +0.7% | ±2.3% | noise | 136.7 | 137.1 |
| `tsd_sparse_map_std` | TSD - sparse | diagnostic | 0.023377 | 0.024233 | x1.037 | +3.7% | ±5.6% | noise | 71.8 | 71.8 |
| `tsd_sparse_reduce_std` | TSD - sparse | diagnostic | 0.010794 | 0.010858 | x1.006 | +0.6% | ±4.5% | noise | 68.9 | 69.0 |
| `tsd_sparse_source_std` | TSD - sparse | diagnostic | 0.004617 | 0.004490 | x0.972 | -2.8% | ±12.0% | noise | 67.0 | 67.1 |
| `tsd_sparse_std` | TSD - sparse | core | 0.029598 | 0.031311 | x1.058 | +5.8% | ±15.8% | noise | 73.8 | 73.9 |
| `tsd_two_input_intersection_std` | TSD - key lifecycle | diagnostic | 0.018040 | 0.018815 | x1.043 | +4.3% | ±11.0% | noise | 70.4 | 70.6 |
| `tsd_two_input_union_std` | TSD - key lifecycle | diagnostic | 0.033656 | 0.034809 | x1.034 | +3.4% | ±2.4% | SLOWER | 76.3 | 76.6 |
| `tss_add_remove_std` | Value types | diagnostic | 0.009794 | 0.009736 | x0.994 | -0.6% | ±5.2% | noise | 66.9 | 66.9 |
| `type_cs_py` | Value types | core | 0.014738 | 0.014658 | x0.995 | -0.5% | ±4.2% | noise | 66.8 | 66.8 |
| `type_cs_std` | Value types | core | 0.014152 | 0.013513 | x0.955 | -4.5% | ±5.6% | noise | 66.8 | 66.8 |
| `type_float_std` | Value types | core | 0.012084 | 0.012068 | x0.999 | -0.1% | ±8.5% | noise | 66.8 | 66.8 |
| `type_int_std` | Value types | core | 0.011858 | 0.011581 | x0.977 | -2.3% | ±8.0% | noise | 66.7 | 66.7 |
| `type_str_std` | Value types | core | 0.013242 | 0.013182 | x0.995 | -0.5% | ±6.3% | noise | 66.6 | 66.8 |
| `type_tsb_partial_fields_std` | Value types | diagnostic | 0.020677 | 0.020907 | x1.011 | +1.1% | ±9.1% | noise | 66.8 | 66.8 |
| `type_tsw_append_evict_std` | Value types | diagnostic | 0.009664 | 0.010172 | x1.053 | +5.3% | ±3.2% | SLOWER | 66.7 | 66.8 |

## Memory (`memory_orchestrate.py`, process pass)

Peak RSS, peak increment over the ready process, memory retained after teardown and two GC passes, runtime load increment, retained type-record growth, and wall time per profile.

| profile | group | peak RSS MB before → after | peak increment MB before → after | retained MB before → after | runtime load MB before → after | type records before → after | seconds before → after | verdict |
|---|---|---|---|---|---|---|---|---|
| `construct_std__large` | Static graph | 77.344 → 77.391 | 10.641 → 10.750 | 10.641 → 10.750 | 44.266 → 44.188 | 28 → 28 | 0.031 → 0.031 | noise |
| `construct_std__medium` | Static graph | 70.594 → 70.625 | 3.907 → 3.922 | 3.907 → 3.922 | 44.187 → 44.250 | 28 → 28 | 0.010 → 0.010 | noise |
| `construct_std__novel_ten` | Process lifetime | 69.547 → 69.516 | 2.906 → 2.813 | 2.906 → 2.813 | 44.156 → 44.375 | 46 → 46 | 0.038 → 0.038 | noise |
| `construct_std__repeat_hundred` | Process lifetime | 68.562 → 68.672 | 1.953 → 1.844 | 1.953 → 1.844 | 44.250 → 44.281 | 28 → 28 | 0.297 → 0.295 | noise |
| `construct_std__repeat_once` | Process lifetime | 68.312 → 68.406 | 1.734 → 1.797 | 1.734 → 1.797 | 44.219 → 44.141 | 28 → 28 | 0.004 → 0.004 | noise |
| `construct_std__repeat_ten` | Process lifetime | 68.359 → 68.453 | 1.765 → 1.796 | 1.765 → 1.796 | 44.172 → 44.109 | 28 → 28 | 0.032 → 0.031 (-0.00) | noise |
| `construct_std__small` | Static graph | 68.859 → 68.938 | 2.297 → 2.281 | 2.297 → 2.281 | 44.234 → 44.235 | 28 → 28 | 0.005 → 0.005 | noise |
| `mesh_std__large` | Nested graphs | 70.172 → 70.188 | 3.594 → 3.640 | 3.594 → 3.640 | 44.250 → 44.204 | 118 → 118 | 0.007 → 0.007 | noise |
| `mesh_std__medium` | Nested graphs | 69.469 → 69.484 | 2.875 → 2.906 | 2.875 → 2.906 | 44.172 → 44.313 | 118 → 118 | 0.006 → 0.006 | noise |
| `mesh_std__small` | Nested graphs | 69.156 → 69.281 | 2.640 → 2.609 | 2.640 → 2.609 | 44.188 → 44.250 | 118 → 118 | 0.006 → 0.006 | noise |
| `reduce_dynamic_tsl_std__large` | C++-first dynamic storage | 70.469 → 70.391 | 3.781 → 3.797 | 3.781 → 3.797 | 44.156 → 44.328 | 64 → 64 | 0.008 → 0.008 | noise |
| `reduce_dynamic_tsl_std__medium` | C++-first dynamic storage | 69.109 → 69.047 | 2.500 → 2.485 | 2.500 → 2.485 | 44.204 → 44.250 | 64 → 64 | 0.006 → 0.006 | noise |
| `reduce_dynamic_tsl_std__small` | C++-first dynamic storage | 68.844 → 68.906 | 2.313 → 2.282 | 2.313 → 2.282 | 44.218 → 44.343 | 64 → 64 | 0.005 → 0.005 | noise |
| `reduce_tsd_nested_graph_std__large` | Nested graphs | 68.703 → 68.922 | 2.203 → 2.250 | 2.203 → 2.250 | 44.219 → 44.265 | 65 → 65 | 0.006 → 0.006 | noise |
| `reduce_tsd_nested_graph_std__medium` | Nested graphs | 68.688 → 68.922 | 2.157 → 2.187 | 2.157 → 2.187 | 44.203 → 44.250 | 65 → 65 | 0.005 → 0.005 | noise |
| `reduce_tsd_nested_graph_std__small` | Nested graphs | 68.594 → 68.688 | 2.063 → 2.063 | 2.063 → 2.063 | 44.250 → 44.250 | 65 → 65 | 0.004 → 0.004 | noise |
| `service_adaptor_py__large` | Services | 69.250 → 69.203 | 2.609 → 2.625 | 2.609 → 2.625 | 44.203 → 44.344 | 124 → 124 | 0.009 → 0.009 | noise |
| `service_adaptor_py__medium` | Services | 69.109 → 68.969 | 2.468 → 2.407 | 2.468 → 2.407 | 44.141 → 44.296 | 124 → 124 | 0.006 → 0.005 | noise |
| `service_adaptor_py__repeat_fifty` | Process lifetime | 69.156 → 69.109 | 2.625 → 2.453 | 2.625 → 2.453 | 44.265 → 44.359 | 124 → 124 | 0.164 → 0.160 | noise |
| `service_adaptor_py__repeat_once` | Process lifetime | 68.938 → 69.078 | 2.469 → 2.313 | 2.469 → 2.313 | 44.250 → 44.266 | 124 → 124 | 0.004 → 0.004 | noise |
| `service_adaptor_py__repeat_ten` | Process lifetime | 69.156 → 69.062 | 2.531 → 2.421 | 2.531 → 2.421 | 44.141 → 44.407 | 124 → 124 | 0.034 → 0.033 | noise |
| `service_adaptor_py__small` | Services | 68.891 → 68.875 | 2.297 → 2.375 | 2.297 → 2.375 | 44.204 → 44.266 | 124 → 124 | 0.004 → 0.005 (+0.00) | noise |
| `switch_keyed_collection_std__large` | Nested graphs | 70.500 → 70.406 | 3.844 → 3.828 | 3.844 → 3.828 | 44.188 → 44.266 | 108 → 108 | 0.131 → 0.129 | noise |
| `switch_keyed_collection_std__medium` | Nested graphs | 69.812 → 69.766 | 3.281 → 3.125 | 3.281 → 3.125 | 44.234 → 44.360 | 108 → 108 | 0.069 → 0.068 | noise |
| `switch_keyed_collection_std__small` | Nested graphs | 69.266 → 69.250 | 2.594 → 2.547 | 2.594 → 2.547 | 44.234 → 44.172 | 108 → 108 | 0.023 → 0.022 | noise |
| `tick_py__long` | Bounded execution | 68.328 → 68.359 | 1.797 → 1.797 | 1.797 → 1.797 | 44.172 → 44.312 | 34 → 34 | 0.025 → 0.024 | noise |
| `tick_py__medium` | Bounded execution | 68.250 → 68.344 | 1.735 → 1.718 | 1.735 → 1.718 | 44.250 → 44.250 | 34 → 34 | 0.014 → 0.014 | noise |
| `tick_py__short` | Bounded execution | 68.328 → 68.328 | 1.719 → 1.734 | 1.719 → 1.734 | 44.141 → 44.266 | 34 → 34 | 0.006 → 0.006 | noise |
| `tick_std__long` | Bounded execution | 68.172 → 68.188 | 1.562 → 1.594 | 1.562 → 1.594 | 44.172 → 44.313 | 30 → 30 | 0.103 → 0.104 | noise |
| `tick_std__medium` | Bounded execution | 68.172 → 68.234 | 1.546 → 1.531 | 1.546 → 1.531 | 44.219 → 44.313 | 30 → 30 | 0.028 → 0.028 | noise |
| `tick_std__short` | Bounded execution | 68.234 → 68.266 | 1.609 → 1.578 | 1.609 → 1.578 | 44.172 → 44.313 | 30 → 30 | 0.006 → 0.006 | noise |
| `tsd_capacity_growth_std__long` | Keyed collections | 83.375 → 83.266 | 16.625 → 16.640 | 16.625 → 16.640 | 44.093 → 44.219 | 84 → 84 | 0.042 → 0.041 | noise |
| `tsd_capacity_growth_std__medium` | Keyed collections | 75.984 → 76.078 | 9.312 → 9.391 | 9.312 → 9.391 | 44.204 → 44.265 | 84 → 84 | 0.018 → 0.020 (+0.00) | noise |
| `tsd_capacity_growth_std__short` | Keyed collections | 70.188 → 70.141 | 3.531 → 3.562 | 3.531 → 3.562 | 44.250 → 44.219 | 84 → 84 | 0.006 → 0.006 | noise |
| `tsd_churn_std__long` | Keyed collections | 69.391 → 69.375 | 2.750 → 2.797 | 2.750 → 2.797 | 44.157 → 44.250 | 84 → 84 | 0.050 → 0.050 | noise |
| `tsd_churn_std__medium` | Keyed collections | 69.391 → 69.359 | 2.671 → 2.719 | 2.671 → 2.719 | 44.157 → 44.266 | 84 → 84 | 0.027 → 0.027 | noise |
| `tsd_churn_std__short` | Keyed collections | 69.312 → 69.234 | 2.703 → 2.703 | 2.703 → 2.703 | 44.265 → 44.219 | 84 → 84 | 0.009 → 0.009 | noise |
| `tsd_clear_repopulate_std__long` | Keyed collections | 72.453 → 72.578 | 5.781 → 5.828 | 5.781 → 5.828 | 44.188 → 44.312 | 84 → 84 | 0.434 → 0.439 | noise |
| `tsd_clear_repopulate_std__medium` | Keyed collections | 72.266 → 72.422 | 5.828 → 5.875 | 5.828 → 5.875 | 44.250 → 44.250 | 84 → 84 | 0.114 → 0.115 | noise |
| `tsd_clear_repopulate_std__short` | Keyed collections | 72.375 → 72.438 | 5.844 → 5.860 | 5.844 → 5.860 | 44.297 → 44.328 | 84 → 84 | 0.028 → 0.028 | noise |
| `tsd_dense_std__large` | Keyed collections | 69.312 → 69.219 | 2.656 → 2.610 | 2.656 → 2.610 | 44.157 → 44.344 | 84 → 84 | 0.013 → 0.013 | noise |
| `tsd_dense_std__medium` | Keyed collections | 68.969 → 69.062 | 2.390 → 2.453 | 2.390 → 2.453 | 44.188 → 44.343 | 84 → 84 | 0.009 → 0.009 | noise |
| `tsd_dense_std__small` | Keyed collections | 68.938 → 69.000 | 2.282 → 2.234 | 2.282 → 2.234 | 44.140 → 44.328 | 84 → 84 | 0.005 → 0.005 (-0.00) | noise |
| `tsd_key_reactivation_std__long` | Keyed collections | 69.406 → 69.312 | 2.750 → 2.610 | 2.750 → 2.610 | 44.234 → 44.328 | 84 → 84 | 0.026 → 0.026 | noise |
| `tsd_key_reactivation_std__medium` | Keyed collections | 69.281 → 69.375 | 2.656 → 2.672 | 2.656 → 2.672 | 44.218 → 44.328 | 84 → 84 | 0.016 → 0.016 | noise |
| `tsd_key_reactivation_std__short` | Keyed collections | 69.266 → 69.281 | 2.704 → 2.640 | 2.704 → 2.640 | 44.140 → 44.250 | 84 → 84 | 0.007 → 0.007 (-0.00) | noise |
| `tsd_sparse_large_capacity_std__large` | Keyed collections | 137.984 → 138.438 | 71.484 → 71.735 | 62.812 → 63.156 | 44.156 → 44.328 | 84 → 84 | 0.683 → 0.727 (+0.04) | noise |
| `tsd_sparse_large_capacity_std__medium` | Keyed collections | 86.234 → 86.281 | 19.625 → 19.672 | 19.625 → 19.672 | 44.093 → 44.312 | 84 → 84 | 0.068 → 0.066 | noise |
| `tsd_sparse_large_capacity_std__small` | Keyed collections | 72.203 → 72.156 | 5.531 → 5.640 | 5.531 → 5.640 | 44.203 → 44.266 | 84 → 84 | 0.011 → 0.011 | noise |
| `tss_add_remove_std__large` | Value storage | 68.703 → 68.594 | 2.031 → 1.938 | 2.031 → 1.938 | 44.172 → 44.266 | 37 → 37 | 0.006 → 0.006 | noise |
| `tss_add_remove_std__medium` | Value storage | 68.406 → 68.328 | 1.985 → 1.735 | 1.985 → 1.735 | 44.203 → 44.125 | 37 → 37 | 0.005 → 0.005 | noise |
| `tss_add_remove_std__small` | Value storage | 68.422 → 68.406 | 1.843 → 1.844 | 1.843 → 1.844 | 44.218 → 44.328 | 37 → 37 | 0.005 → 0.005 | noise |
| `type_cs_py__long` | Value storage | 68.375 → 68.516 | 1.797 → 1.813 | 1.797 → 1.813 | 44.266 → 44.360 | 47 → 47 | 0.018 → 0.018 | noise |
| `type_cs_py__short` | Value storage | 68.359 → 68.594 | 1.781 → 1.750 | 1.781 → 1.750 | 44.359 → 44.516 | 47 → 47 | 0.005 → 0.005 | noise |
| `type_str_std__long` | Value storage | 68.297 → 68.359 | 1.719 → 1.719 | 1.719 → 1.719 | 44.187 → 44.375 | 28 → 28 | 0.016 → 0.017 | noise |
| `type_str_std__short` | Value storage | 68.328 → 68.281 | 1.672 → 1.688 | 1.672 → 1.688 | 44.171 → 44.297 | 28 → 28 | 0.005 → 0.005 | noise |
| `type_tsw_append_evict_std__long` | Value storage | 68.234 → 68.406 | 1.797 → 1.703 | 1.797 → 1.703 | 44.187 → 44.141 | 30 → 30 | 0.041 → 0.041 | noise |
| `type_tsw_append_evict_std__medium` | Value storage | 68.344 → 68.297 | 1.750 → 1.750 | 1.750 → 1.750 | 44.219 → 44.125 | 30 → 30 | 0.014 → 0.012 (-0.00) | noise |
| `type_tsw_append_evict_std__short` | Value storage | 68.391 → 68.391 | 1.734 → 1.735 | 1.734 → 1.735 | 44.281 → 44.328 | 30 → 30 | 0.006 → 0.006 | noise |

## Recheck of the cells beyond their noise band

A second pass over the seven flagged cells plus three controls (`tsd_dense_std`, `type_tsb_partial_fields_std`, `tick_std`), run in the opposite order (after-tree first) with 7 samples on the idle machine; the pooled column is the median over both passes' samples.

| scenario | pass 1 after/before (5 samples, before first) | pass 2 after/before (7 samples, after first) | pooled after/before | verdict |
|---|---|---|---|---|
| `construct_higher_order_py` | x0.959 | x0.998 | x0.984 | run-order drift (not reproduced) |
| `construct_py` | x0.966 | x0.997 | x1.008 | run-order drift (not reproduced) |
| `python_owned_dedup_native` | x1.049 | x1.021 | x1.041 | run-order drift (not reproduced) |
| `python_owned_dedup_python` | x1.048 | x1.022 | x1.015 | run-order drift (not reproduced) |
| `tick_std` | x0.992 | x1.000 | x0.995 | run-order drift (not reproduced) |
| `tsd_dense_std` | x1.063 | x1.053 | x1.058 | consistent slowdown |
| `tsd_dense_strkeys_std` | x0.926 | x0.976 | x0.944 | run-order drift (not reproduced) |
| `tsd_two_input_union_std` | x1.034 | x1.014 | x1.027 | run-order drift (not reproduced) |
| `type_tsb_partial_fields_std` | x1.011 | x1.000 | x1.001 | run-order drift (not reproduced) |
| `type_tsw_append_evict_std` | x1.053 | x0.968 | x1.030 | run-order drift (not reproduced) |

## Alternation on the dense TSD cells

`tsd_dense_std` was the one cell slower in both earlier passes (x1.063, x1.053). A third pass alternated the trees (after, before, after, before) with 9 samples per cell on the idle machine:

| run | `tsd_dense_std` s (MAD) | `tsd_dense_strkeys_std` s (MAD) | `tsd_dense_source_std` s (MAD) |
|---|---|---|---|
| after | 0.08494 (0.00057) | 0.09885 (0.00117) | 0.01375 (0.00008) |
| before | 0.09134 (0.00182) | 0.09954 (0.00214) | 0.01368 (0.00017) |
| after | 0.08537 (0.00049) | 0.10105 (0.00364) | 0.01375 (0.00008) |
| before | 0.09372 (0.00304) | 0.10784 (0.00210) | 0.01359 (0.00013) |

Here the after tree is 7–9% *faster* on `tsd_dense_std`, the opposite sign of the earlier passes: the cell's median moves by about 8% between sessions of the same build (0.085–0.094 s), far more than its within-session MAD. The earlier "consistent slowdown" was session-to-session variance, not an effect of the change.

## Conclusion

Over 82 performance scenarios the stack is neutral (median x1.005, geometric mean x1.005); every cell that left its noise band in one pass returned to it or reversed sign in a later pass. Over 59 memory profiles peak and retained RSS move by less than 0.5 MB and retained type-record growth is identical. The RFC 0036 stack costs no runtime performance or memory, as its "Performance and memory" section predicted.
