# hgraph performance matrix

- date: 2026-08-27T19:05:29+00:00
- host: macOS-26.6.2-arm64-arm-64bit-Mach-O / arm
- CPU: Apple M4 Max
- Python: 3.14.7
- fixed release baseline: hgraph 0.8.19 (published wheel)
- fixed release wheel: hgraph-0.8.19-cp312-abi3-macosx_15_0_arm64.whl
- fixed release SHA-256: e7c4f19920a45ce9da0d4e4c479af2fd258e4f55b0f2de0215e5b105548629d1
- current-source compiler: Apple clang version 21.0.0 (clang-2100.1.1.101)
- current-source revision: c9935bd35fd1+dirty
- current-source fingerprint: 5ceb81b004f4532ba81e59799b7aeac034a5f4bb0b01015f089c83b4567fbb6b
- current-source build type: Release
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: hgraph 0.8.19 (`release`), current source (`hg-cpp`)
- reused fixed baseline cells: 79

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs hgraph 0.8.19.
C++-first-only sections are tracked without a 0.5 comparison.

## Graph construction

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 0.050s +/- 0.001s | 0.049s +/- 0.000s (x1.0) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.088s +/- 0.000s | 0.086s +/- 0.001s (x1.0) |

## Scheduler

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 0.025s +/- 0.000s | 0.025s +/- 0.000s (x1.0) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.021s +/- 0.001s | 0.020s +/- 0.000s (x1.0) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 0.057s +/- 0.000s | 0.055s +/- 0.000s (x1.0) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 0.081s +/- 0.000s | 0.079s +/- 0.000s (x1.0) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 0.040s +/- 0.000s | 0.040s +/- 0.000s (x1.0) |

## Python boundary

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.008s +/- 0.000s | 0.008s +/- 0.000s (x1.1) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.013s +/- 0.000s | 0.012s +/- 0.000s (x1.0) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | 0.017s +/- 0.000s | 0.016s +/- 0.000s (x1.0) |

## Value types

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.012s +/- 0.000s | 0.011s +/- 0.000s (x1.0) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.013s +/- 0.000s | 0.012s +/- 0.000s (x1.1) |
| String concatenation (`type_str_std`) | 20000 | 0.014s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.014s +/- 0.000s | 0.014s +/- 0.000s (x1.0) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.015s +/- 0.000s | 0.014s +/- 0.000s (x1.0) |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.010s +/- 0.000s | 0.010s +/- 0.000s (x1.0) |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.009s +/- 0.000s | 0.008s +/- 0.000s (x1.1) |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.011s +/- 0.000s | 0.011s +/- 0.000s (x1.1) |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.011s +/- 0.000s | 0.011s +/- 0.000s (x1.0) |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.017s +/- 0.000s | 0.016s +/- 0.000s (x1.0) |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.018s +/- 0.000s | 0.018s +/- 0.000s (x1.0) |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.017s +/- 0.000s | 0.016s +/- 0.000s (x1.0) |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.028s +/- 0.000s | 0.028s +/- 0.001s (x1.0) |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.012s +/- 0.000s | 0.011s +/- 0.000s (x1.1) |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.013s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |

## Value types

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.021s +/- 0.000s | 0.020s +/- 0.000s (x1.0) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.010s +/- 0.000s | 0.009s +/- 0.000s (x1.1) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.010s +/- 0.000s | 0.009s +/- 0.000s (x1.0) |

## TSD - dense

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 0.085s +/- 0.001s | 0.091s +/- 0.001s (x0.9) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 0.080s +/- 0.002s | 0.080s +/- 0.002s (x1.0) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.013s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |
| Map only (`tsd_dense_map_std`) | 1000 | 0.068s +/- 0.003s | 0.071s +/- 0.003s (x1.0) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 0.033s +/- 0.000s | 0.033s +/- 0.000s (x1.0) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 0.099s +/- 0.001s | 0.105s +/- 0.002s (x0.9) |

## TSD - sparse

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 0.030s +/- 0.001s | 0.028s +/- 0.000s (x1.1) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.005s +/- 0.000s | 0.005s +/- 0.000s (x1.0) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.024s +/- 0.001s | 0.021s +/- 0.000s (x1.1) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.011s +/- 0.000s | 0.010s +/- 0.000s (x1.0) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 0.910s +/- 0.001s | 0.888s +/- 0.013s (x1.0) |

## TSD - key lifecycle

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 0.038s +/- 0.000s | 0.037s +/- 0.000s (x1.0) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 0.031s +/- 0.000s | 0.031s +/- 0.000s (x1.0) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.003s +/- 0.000s | 0.003s +/- 0.000s (x1.0) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 0.032s +/- 0.000s | 0.032s +/- 0.000s (x1.0) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.008s +/- 0.000s | 0.008s +/- 0.000s (x1.0) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 0.036s +/- 0.001s | 0.034s +/- 0.000s (x1.0) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 0.426s +/- 0.003s | 0.427s +/- 0.002s (x1.0) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 0.023s +/- 0.000s | 0.022s +/- 0.000s (x1.0) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 0.034s +/- 0.001s | 0.033s +/- 0.001s (x1.0) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.019s +/- 0.000s | 0.018s +/- 0.000s (x1.1) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 0.013s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |

## Reduce

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 0.048s +/- 0.001s | 0.052s +/- 0.001s (x0.9) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 0.066s +/- 0.001s | 0.070s +/- 0.001s (x0.9) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.016s +/- 0.000s | 0.015s +/- 0.000s (x1.0) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | 0.023s +/- 0.000s | 0.022s +/- 0.000s (x1.0) |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.013s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |

## Nested graphs

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 0.125s +/- 0.000s | 0.124s +/- 0.000s (x1.0) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 0.627s +/- 0.000s | 0.622s +/- 0.001s (x1.0) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.016s +/- 0.000s | 0.016s +/- 0.000s (x1.0) |

## Services

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.010s +/- 0.000s | 0.010s +/- 0.000s (x1.0) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.013s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 0.026s +/- 0.000s | 0.026s +/- 0.001s (x1.0) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.026s +/- 0.000s | 0.026s +/- 0.000s (x1.0) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.018s +/- 0.000s | 0.018s +/- 0.000s (x1.0) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 0.031s +/- 0.000s | 0.031s +/- 0.001s (x1.0) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 0.043s +/- 0.000s | 0.042s +/- 0.000s (x1.0) |

## Adaptors

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.010s +/- 0.000s | 0.010s +/- 0.000s (x1.0) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.012s +/- 0.000s | 0.011s +/- 0.000s (x1.0) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.029s +/- 0.000s | 0.029s +/- 0.001s (x1.0) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.028s +/- 0.001s | 0.029s +/- 0.001s (x1.0) |

## Audited operators

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 | current source |
|---|---|---|---|
| Scalar-collection convert and collect (`audit_convert_collect_std`) | 2000 | 0.024s +/- 0.000s | 0.025s +/- 0.000s (x1.0) |
| Scalar-map flip/keys/values (`audit_map_transform_std`) | 10000 | 0.045s +/- 0.000s | 0.045s +/- 0.000s (x1.0) |
| TSD getitem with a ticking key (`audit_tsd_getitem_std`) | 20000 | 0.033s +/- 0.001s | 0.032s +/- 0.000s (x1.0) |
| Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | 20000 | 0.015s +/- 0.000s | 0.015s +/- 0.000s (x1.0) |
| Regex match_ and replace (`audit_string_match_std`) | 10000 | 0.020s +/- 0.000s | 0.019s +/- 0.000s (x1.0) |
| CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | 10000 | 0.013s +/- 0.000s | 0.013s +/- 0.000s (x1.0) |
| race over if_-routed references (`audit_ref_race_std`) | 20000 | 0.019s +/- 0.000s | 0.019s +/- 0.000s (x1.0) |
| Buffered stream lag/gate (`audit_stream_buffered_std`) | 5000 | 0.006s +/- 0.000s | 0.006s +/- 0.000s (x1.1) |
| Stream take/drop/step counters (`audit_take_drop_std`) | 20000 | 0.014s +/- 0.001s | 0.014s +/- 0.000s (x1.1) |
| Running mean accumulator (`audit_running_mean_std`) | 50000 | 0.024s +/- 0.000s | 0.023s +/- 0.000s (x1.0) |
