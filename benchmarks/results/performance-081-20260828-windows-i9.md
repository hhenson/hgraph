# hgraph performance matrix

- date: 2026-08-28T03:59:43+00:00
- host: Windows-10-10.0.19045-SP0 / Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- CPU: Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- Python: 3.14.7
- fixed release baseline: hgraph 0.8.1 (published wheel)
- fixed release wheel: hgraph-0.8.1-cp312-abi3-win_amd64.whl
- fixed release SHA-256: 7d30ce7b27e3add5869eeda795cbd8ce21830218258533cd8a1a963b711adcd8
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: hgraph 0.8.1 (`release`)
- reused fixed baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs hgraph 0.8.1.
C++-first-only sections are tracked without a 0.5 comparison.

## Graph construction

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 0.278s +/- 0.002s |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.391s +/- 0.002s |

## Scheduler

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 0.095s +/- 0.000s |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.086s +/- 0.001s |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 0.249s +/- 0.000s |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 0.314s +/- 0.004s |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 0.201s +/- 0.002s |

## Python boundary

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.035s +/- 0.000s |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.057s +/- 0.000s |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | 0.098s +/- 0.000s |

## Value types

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.052s +/- 0.000s |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.053s +/- 0.000s |
| String concatenation (`type_str_std`) | 20000 | 0.063s +/- 0.001s |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.067s +/- 0.000s |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.062s +/- 0.001s |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.042s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.036s +/- 0.001s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.053s +/- 0.001s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.052s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.074s +/- 0.001s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.082s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.077s +/- 0.001s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.136s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.053s +/- 0.001s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.054s +/- 0.000s |

## Value types

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.131s +/- 0.000s |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.045s +/- 0.000s |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.210s +/- 0.001s |

## TSD - dense

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 0.331s +/- 0.001s |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 0.477s +/- 0.002s |
| Source only (`tsd_dense_source_std`) | 1000 | 0.056s +/- 0.000s |
| Map only (`tsd_dense_map_std`) | 1000 | 0.245s +/- 0.002s |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 0.141s +/- 0.000s |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 0.343s +/- 0.000s |

## TSD - sparse

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 0.107s +/- 0.000s |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.016s +/- 0.000s |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.081s +/- 0.000s |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.042s +/- 0.000s |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 3.195s +/- 0.002s |

## TSD - key lifecycle

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 0.173s +/- 0.000s |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 0.176s +/- 0.000s |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.015s +/- 0.000s |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 0.147s +/- 0.001s |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.036s +/- 0.000s |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 0.139s +/- 0.001s |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 1.722s +/- 0.002s |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 0.103s +/- 0.000s |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 0.135s +/- 0.002s |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.072s +/- 0.000s |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 0.068s +/- 0.001s |

## Reduce

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 0.170s +/- 0.000s |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 0.269s +/- 0.001s |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.061s +/- 0.001s |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | 0.143s +/- 0.001s |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.094s +/- 0.001s |

## Nested graphs

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 0.531s +/- 0.002s |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 2.680s +/- 0.002s |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.074s +/- 0.000s |

## Services

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.045s +/- 0.000s |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.061s +/- 0.001s |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 0.129s +/- 0.001s |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.172s +/- 0.000s |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.086s +/- 0.000s |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 0.227s +/- 0.001s |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 0.404s +/- 0.001s |

## Adaptors

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.044s +/- 0.000s |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.053s +/- 0.000s |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.137s +/- 0.001s |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.181s +/- 0.000s |

## Audited operators

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Scalar-collection convert and collect (`audit_convert_collect_std`) | 2000 | 0.110s +/- 0.001s |
| Scalar-map flip/keys/values (`audit_map_transform_std`) | 10000 | 0.218s +/- 0.000s |
| TSD getitem with a ticking key (`audit_tsd_getitem_std`) | 20000 | 0.175s +/- 0.000s |
| Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | 20000 | 0.074s +/- 0.000s |
| Regex match_ and replace (`audit_string_match_std`) | 10000 | 0.096s +/- 0.000s |
| CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | 10000 | 0.077s +/- 0.001s |
| race over if_-routed references (`audit_ref_race_std`) | 20000 | 0.125s +/- 0.000s |
| Buffered stream lag/gate (`audit_stream_buffered_std`) | 5000 | 0.510s +/- 0.006s |
| Stream take/drop/step counters (`audit_take_drop_std`) | 20000 | 0.061s +/- 0.001s |
| Running mean accumulator (`audit_running_mean_std`) | 50000 | 0.105s +/- 0.000s |
