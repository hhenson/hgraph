# hgraph performance matrix

- date: 2026-08-09T17:37:25+00:00
- host: Windows-10-10.0.19045-SP0 / Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- CPU: Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- Python: 3.14.7
- reference baseline: hgraph 0.5.41 (published wheel)
- reference wheel: hgraph-0.5.41-cp312-abi3-win_amd64.whl
- reference SHA-256: 74deabc55a4e5a93f3d5234ff828d499c51344924fdac303303abe8b80b224f8
- fixed release baseline: hgraph 0.8.1 (published wheel)
- fixed release wheel: hgraph-0.8.1-cp312-abi3-win_amd64.whl
- fixed release SHA-256: 7d30ce7b27e3add5869eeda795cbd8ce21830218258533cd8a1a963b711adcd8
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: Python (`upstream-py`), legacy C++ (`upstream-cpp`), hgraph 0.8.1 (`release`)
- reused fixed baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs Python.
C++-first-only sections are tracked without a 0.5 comparison.

## Graph construction

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 2.872s +/- 0.033s | 2.778s +/- 0.027s (x1.0) | 0.264s +/- 0.011s (x10.9) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.526s +/- 0.001s | 0.480s +/- 0.001s (x1.1) | 0.363s +/- 0.002s (x1.4) |

## Scheduler

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 3.250s +/- 0.021s | 0.178s +/- 0.002s (x18.2) | 0.091s +/- 0.000s (x35.5) |
| Five-node Python compute chain (`tick_py`) | 20000 | 1.244s +/- 0.005s | 0.069s +/- 0.001s (x18.0) | 0.084s +/- 0.001s (x14.9) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 9.905s +/- 0.096s | 0.489s +/- 0.002s (x20.3) | 0.242s +/- 0.000s (x41.0) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 12.385s +/- 0.084s | 0.683s +/- 0.003s (x18.1) | 0.307s +/- 0.003s (x40.3) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 3.311s +/- 0.011s | 0.198s +/- 0.001s (x16.8) | 0.196s +/- 0.001s (x16.9) |

## Python boundary

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.326s +/- 0.002s | 0.031s +/- 0.000s (x10.6) | 0.033s +/- 0.000s (x9.9) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.333s +/- 0.002s | 0.031s +/- 0.000s (x10.9) | 0.057s +/- 0.000s (x5.9) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.093s +/- 0.001s |

## Value types

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.937s +/- 0.008s | 0.069s +/- 0.001s (x13.5) | 0.050s +/- 0.001s (x18.9) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.937s +/- 0.003s | 0.068s +/- 0.000s (x13.8) | 0.052s +/- 0.001s (x18.0) |
| String concatenation (`type_str_std`) | 20000 | 0.944s +/- 0.005s | 0.077s +/- 0.000s (x12.2) | 0.058s +/- 0.001s (x16.4) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.938s +/- 0.007s | 0.070s +/- 0.000s (x13.4) | 0.064s +/- 0.001s (x14.8) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.487s +/- 0.002s | 0.054s +/- 0.000s (x9.0) | 0.060s +/- 0.001s (x8.1) |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | N/A | N/A | 0.039s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | N/A | N/A | 0.034s +/- 0.001s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | N/A | N/A | 0.049s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | N/A | N/A | 0.048s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | N/A | N/A | 0.070s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | N/A | N/A | 0.080s +/- 0.001s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | N/A | N/A | 0.075s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | N/A | N/A | 0.132s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | N/A | N/A | 0.050s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | N/A | N/A | 0.050s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 2.275s +/- 0.001s | 0.167s +/- 0.001s (x13.6) | 0.119s +/- 0.001s (x19.2) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.635s +/- 0.004s | 0.044s +/- 0.001s (x14.5) | 0.043s +/- 0.001s (x14.8) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.244s +/- 0.003s | 0.164s +/- 0.001s (x1.5) | 0.200s +/- 0.003s (x1.2) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 8.087s +/- 0.049s | 0.501s +/- 0.001s (x16.1) | 0.307s +/- 0.003s (x26.3) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 5.965s +/- 0.101s | 0.359s +/- 0.003s (x16.6) | 0.442s +/- 0.003s (x13.5) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.693s +/- 0.009s | 0.080s +/- 0.000s (x8.7) | 0.052s +/- 0.000s (x13.2) |
| Map only (`tsd_dense_map_std`) | 1000 | 6.055s +/- 0.033s | 0.400s +/- 0.001s (x15.1) | 0.228s +/- 0.002s (x26.6) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 3.122s +/- 0.018s | 0.215s +/- 0.000s (x14.5) | 0.131s +/- 0.001s (x23.8) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 8.125s +/- 0.063s | 0.565s +/- 0.001s (x14.4) | 0.321s +/- 0.002s (x25.3) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 3.453s +/- 0.001s | 0.293s +/- 0.001s (x11.8) | 0.100s +/- 0.001s (x34.5) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.125s +/- 0.001s | 0.029s +/- 0.000s (x4.3) | 0.015s +/- 0.000s (x8.2) |
| Map only (`tsd_sparse_map_std`) | 2000 | 1.262s +/- 0.006s | 0.152s +/- 0.005s (x8.3) | 0.076s +/- 0.000s (x16.7) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 2.322s +/- 0.002s | 0.172s +/- 0.001s (x13.5) | 0.039s +/- 0.000s (x59.4) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 83.142s +/- 0.211s | 15.742s +/- 0.117s (x5.3) | 3.037s +/- 0.003s (x27.4) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 7.610s +/- 0.016s | 0.602s +/- 0.002s (x12.6) | 0.161s +/- 0.001s (x47.2) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 6.002s +/- 0.004s | 0.443s +/- 0.001s (x13.5) | 0.165s +/- 0.000s (x36.5) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.316s +/- 0.001s | 0.063s +/- 0.000s (x5.0) | 0.014s +/- 0.000s (x22.7) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 5.368s +/- 0.004s | 0.459s +/- 0.002s (x11.7) | 0.137s +/- 0.001s (x39.2) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 2.599s +/- 0.002s | 0.189s +/- 0.001s (x13.8) | 0.034s +/- 0.000s (x77.5) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 4.219s +/- 0.007s | 0.522s +/- 0.005s (x8.1) | 0.128s +/- 0.000s (x32.9) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 74.429s +/- 0.242s | 6.437s +/- 0.020s (x11.6) | 1.603s +/- 0.008s (x46.4) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 3.734s +/- 0.002s | 0.312s +/- 0.001s (x12.0) | 0.095s +/- 0.000s (x39.3) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 3.659s +/- 0.004s | 0.300s +/- 0.003s (x12.2) | 0.124s +/- 0.000s (x29.4) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 1.308s +/- 0.006s | 0.116s +/- 0.000s (x11.3) | 0.068s +/- 0.000s (x19.3) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 4.581s +/- 0.007s | 0.414s +/- 0.011s (x11.1) | 0.064s +/- 0.000s (x71.3) |

## Reduce

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 3.146s +/- 0.035s | 0.215s +/- 0.000s (x14.6) | 0.159s +/- 0.000s (x19.8) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 3.172s +/- 0.017s | 0.215s +/- 0.001s (x14.7) | 0.251s +/- 0.001s (x12.6) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 1.698s +/- 0.020s | 0.116s +/- 0.001s (x14.6) | 0.059s +/- 0.001s (x28.8) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.134s +/- 0.001s |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | N/A | N/A | 0.087s +/- 0.001s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 12.105s +/- 0.069s | 1.021s +/- 0.003s (x11.9) | 0.494s +/- 0.001s (x24.5) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 101.600s +/- 0.486s | 8.086s +/- 0.046s (x12.6) | 2.476s +/- 0.003s (x41.0) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 2.152s +/- 0.005s | 0.324s +/- 0.001s (x6.6) | 0.068s +/- 0.001s (x31.5) |

## Services

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.808s +/- 0.003s | 0.046s +/- 0.000s (x17.6) | 0.044s +/- 0.001s (x18.6) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 1.018s +/- 0.002s | 0.057s +/- 0.001s (x17.9) | 0.058s +/- 0.000s (x17.4) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 3.159s +/- 0.018s | 0.308s +/- 0.001s (x10.3) | 0.124s +/- 0.001s (x25.5) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 2.655s +/- 0.035s | 0.271s +/- 0.003s (x9.8) | 0.161s +/- 0.002s (x16.5) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 2.155s +/- 0.010s | 0.258s +/- 0.001s (x8.3) | 0.082s +/- 0.001s (x26.3) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 3.963s +/- 0.049s | 0.563s +/- 0.002s (x7.0) | 0.211s +/- 0.001s (x18.7) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 4.822s +/- 0.011s | 0.685s +/- 0.003s (x7.0) | 0.336s +/- 0.001s (x14.3) |

## Adaptors

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.555s +/- 0.003s | 0.051s +/- 0.001s (x10.8) | 0.042s +/- 0.000s (x13.3) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.552s +/- 0.003s | 0.047s +/- 0.000s (x11.7) | 0.051s +/- 0.000s (x10.9) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 1.915s +/- 0.015s | 0.135s +/- 0.001s (x14.2) | 0.129s +/- 0.001s (x14.8) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 1.465s +/- 0.004s | 0.102s +/- 0.001s (x14.4) | 0.168s +/- 0.000s (x8.7) |
