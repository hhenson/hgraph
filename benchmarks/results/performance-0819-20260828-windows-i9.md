# hgraph performance matrix

- date: 2026-08-28T03:56:36+00:00
- host: Windows-10-10.0.19045-SP0 / Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- CPU: Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- Python: 3.14.7
- fixed release baseline: hgraph 0.8.19 (published wheel)
- fixed release wheel: hgraph-0.8.19-cp312-abi3-win_amd64.whl
- fixed release SHA-256: b072b49300aa2bb744372da81e070be13846b8a5ccb03cbbb0be2c2108ffb377
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: hgraph 0.8.19 (`release`)
- reused fixed baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs hgraph 0.8.19.
C++-first-only sections are tracked without a 0.5 comparison.

## Graph construction

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 0.275s +/- 0.003s |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.398s +/- 0.002s |

## Scheduler

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 0.085s +/- 0.000s |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.078s +/- 0.001s |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 0.203s +/- 0.001s |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 0.285s +/- 0.003s |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 0.158s +/- 0.001s |

## Python boundary

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.030s +/- 0.000s |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.053s +/- 0.001s |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | 0.084s +/- 0.000s |

## Value types

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.044s +/- 0.000s |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.049s +/- 0.001s |
| String concatenation (`type_str_std`) | 20000 | 0.057s +/- 0.000s |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.057s +/- 0.000s |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.060s +/- 0.001s |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.037s +/- 0.001s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.032s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.045s +/- 0.001s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.045s +/- 0.001s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.062s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.073s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.067s +/- 0.001s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.127s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.047s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.052s +/- 0.000s |

## Value types

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.086s +/- 0.001s |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.039s +/- 0.001s |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.037s +/- 0.000s |

## TSD - dense

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 0.305s +/- 0.000s |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 0.323s +/- 0.002s |
| Source only (`tsd_dense_source_std`) | 1000 | 0.047s +/- 0.000s |
| Map only (`tsd_dense_map_std`) | 1000 | 0.226s +/- 0.002s |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 0.129s +/- 0.001s |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 0.321s +/- 0.001s |

## TSD - sparse

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 0.111s +/- 0.003s |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.020s +/- 0.000s |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.085s +/- 0.001s |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.042s +/- 0.000s |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 3.816s +/- 0.008s |

## TSD - key lifecycle

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 0.169s +/- 0.000s |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 0.164s +/- 0.001s |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.013s +/- 0.000s |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 0.144s +/- 0.000s |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.034s +/- 0.000s |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 0.173s +/- 0.000s |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 1.666s +/- 0.007s |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 0.100s +/- 0.000s |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 0.127s +/- 0.001s |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.070s +/- 0.000s |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 0.064s +/- 0.000s |

## Reduce

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 0.162s +/- 0.000s |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 0.247s +/- 0.001s |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.057s +/- 0.000s |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | 0.132s +/- 0.000s |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.060s +/- 0.000s |

## Nested graphs

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 0.530s +/- 0.000s |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 2.610s +/- 0.011s |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.072s +/- 0.000s |

## Services

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.040s +/- 0.001s |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.055s +/- 0.001s |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 0.120s +/- 0.002s |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.132s +/- 0.000s |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.081s +/- 0.001s |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 0.212s +/- 0.001s |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 0.330s +/- 0.000s |

## Adaptors

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.041s +/- 0.001s |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.050s +/- 0.001s |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.125s +/- 0.001s |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.138s +/- 0.001s |

## Audited operators

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 |
|---|---|---|
| Scalar-collection convert and collect (`audit_convert_collect_std`) | 2000 | 0.070s +/- 0.000s |
| Scalar-map flip/keys/values (`audit_map_transform_std`) | 10000 | 0.192s +/- 0.001s |
| TSD getitem with a ticking key (`audit_tsd_getitem_std`) | 20000 | 0.149s +/- 0.001s |
| Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | 20000 | 0.063s +/- 0.001s |
| Regex match_ and replace (`audit_string_match_std`) | 10000 | 0.053s +/- 0.001s |
| CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | 10000 | 0.072s +/- 0.000s |
| race over if_-routed references (`audit_ref_race_std`) | 20000 | 0.085s +/- 0.000s |
| Buffered stream lag/gate (`audit_stream_buffered_std`) | 5000 | 0.023s +/- 0.000s |
| Stream take/drop/step counters (`audit_take_drop_std`) | 20000 | 0.056s +/- 0.001s |
| Running mean accumulator (`audit_running_mean_std`) | 50000 | 0.098s +/- 0.000s |
