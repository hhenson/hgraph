# hgraph performance matrix

- date: 2026-08-09T16:50:16+00:00
- host: macOS-26.5.2-arm64-arm-64bit-Mach-O / arm
- CPU: Apple M4 Max
- Python: 3.14.6
- reference baseline: hgraph 0.5.41 (published wheel)
- reference wheel: hgraph-0.5.41-cp312-abi3-macosx_15_0_arm64.whl
- reference SHA-256: 872bd8f07fcec148317786be517ba7c73bc8c6023de50a4fe88cc68c0ae1eef1
- fixed release baseline: hgraph 0.8.1 (published wheel)
- fixed release wheel: hgraph-0.8.1-cp312-abi3-macosx_15_0_arm64.whl
- fixed release SHA-256: daab5629e766d26bcfa2dee0d06e27de5567485825d512edaf75e74787ad708c
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
| Wide/deep graph - native operators (`construct_std`) | 1 | 0.984s +/- 0.002s | 0.938s +/- 0.005s (x1.0) | 0.049s +/- 0.000s (x20.1) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.184s +/- 0.001s | 0.149s +/- 0.001s (x1.2) | 0.081s +/- 0.000s (x2.3) |

## Scheduler

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 1.165s +/- 0.007s | 0.060s +/- 0.000s (x19.3) | 0.026s +/- 0.001s (x44.6) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.452s +/- 0.002s | 0.026s +/- 0.000s (x17.6) | 0.021s +/- 0.000s (x21.5) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 3.930s +/- 0.014s | 0.175s +/- 0.001s (x22.5) | 0.063s +/- 0.001s (x62.6) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 4.952s +/- 0.024s | 0.230s +/- 0.001s (x21.6) | 0.088s +/- 0.001s (x56.3) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 1.265s +/- 0.003s | 0.072s +/- 0.000s (x17.6) | 0.050s +/- 0.000s (x25.3) |

## Python boundary

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.110s +/- 0.001s | 0.014s +/- 0.000s (x7.8) | 0.009s +/- 0.000s (x12.9) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.111s +/- 0.001s | 0.014s +/- 0.000s (x7.9) | 0.013s +/- 0.000s (x8.8) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.018s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.342s +/- 0.001s | 0.028s +/- 0.001s (x12.2) | 0.013s +/- 0.000s (x27.0) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.343s +/- 0.000s | 0.028s +/- 0.000s (x12.5) | 0.013s +/- 0.000s (x27.0) |
| String concatenation (`type_str_std`) | 20000 | 0.344s +/- 0.002s | 0.030s +/- 0.000s (x11.6) | 0.014s +/- 0.000s (x24.3) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.340s +/- 0.001s | 0.028s +/- 0.000s (x12.2) | 0.015s +/- 0.000s (x22.2) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.173s +/- 0.001s | 0.022s +/- 0.000s (x7.7) | 0.015s +/- 0.000s (x11.4) |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | N/A | N/A | 0.010s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | N/A | N/A | 0.009s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | N/A | N/A | 0.012s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | N/A | N/A | 0.012s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | N/A | N/A | 0.017s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | N/A | N/A | 0.019s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | N/A | N/A | 0.017s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | N/A | N/A | 0.030s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | N/A | N/A | 0.012s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | N/A | N/A | 0.013s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.803s +/- 0.002s | 0.060s +/- 0.000s (x13.3) | 0.032s +/- 0.000s (x25.4) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.208s +/- 0.000s | 0.018s +/- 0.000s (x11.5) | 0.010s +/- 0.000s (x20.2) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.077s +/- 0.000s | 0.072s +/- 0.000s (x1.1) | 0.061s +/- 0.000s (x1.3) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 3.164s +/- 0.015s | 0.176s +/- 0.000s (x18.0) | 0.095s +/- 0.004s (x33.4) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 2.288s +/- 0.003s | 0.124s +/- 0.000s (x18.5) | 0.104s +/- 0.001s (x22.0) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.273s +/- 0.001s | 0.031s +/- 0.000s (x8.7) | 0.016s +/- 0.000s (x17.3) |
| Map only (`tsd_dense_map_std`) | 1000 | 2.386s +/- 0.021s | 0.150s +/- 0.001s (x16.0) | 0.070s +/- 0.001s (x34.1) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 1.229s +/- 0.003s | 0.073s +/- 0.001s (x16.9) | 0.035s +/- 0.000s (x34.8) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 3.188s +/- 0.030s | 0.195s +/- 0.001s (x16.4) | 0.105s +/- 0.001s (x30.3) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 1.284s +/- 0.008s | 0.081s +/- 0.001s (x15.8) | 0.029s +/- 0.001s (x44.7) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.048s +/- 0.001s | 0.014s +/- 0.001s (x3.4) | 0.004s +/- 0.000s (x11.3) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.467s +/- 0.004s | 0.045s +/- 0.001s (x10.4) | 0.023s +/- 0.000s (x20.4) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.868s +/- 0.004s | 0.046s +/- 0.000s (x18.9) | 0.012s +/- 0.001s (x71.5) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 30.526s +/- 0.105s | 1.610s +/- 0.006s (x19.0) | 0.920s +/- 0.011s (x33.2) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 2.780s +/- 0.011s | 0.169s +/- 0.000s (x16.5) | 0.038s +/- 0.000s (x73.7) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 2.203s +/- 0.003s | 0.122s +/- 0.001s (x18.0) | 0.033s +/- 0.000s (x67.2) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.109s +/- 0.000s | 0.019s +/- 0.000s (x5.9) | 0.003s +/- 0.000s (x31.9) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 1.975s +/- 0.005s | 0.125s +/- 0.001s (x15.8) | 0.032s +/- 0.000s (x61.1) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.924s +/- 0.002s | 0.059s +/- 0.000s (x15.6) | 0.008s +/- 0.000s (x112.9) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 1.552s +/- 0.003s | 0.116s +/- 0.000s (x13.4) | 0.035s +/- 0.000s (x44.5) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 28.429s +/- 0.166s | 1.597s +/- 0.011s (x17.8) | 0.425s +/- 0.001s (x66.9) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 1.378s +/- 0.003s | 0.091s +/- 0.000s (x15.1) | 0.023s +/- 0.000s (x60.9) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 1.366s +/- 0.003s | 0.084s +/- 0.001s (x16.2) | 0.035s +/- 0.001s (x39.4) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.486s +/- 0.002s | 0.039s +/- 0.000s (x12.6) | 0.018s +/- 0.000s (x26.6) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 1.711s +/- 0.011s | 0.111s +/- 0.001s (x15.4) | 0.014s +/- 0.000s (x121.6) |

## Reduce

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 1.227s +/- 0.004s | 0.073s +/- 0.000s (x16.9) | 0.052s +/- 0.002s (x23.6) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 1.237s +/- 0.007s | 0.072s +/- 0.001s (x17.1) | 0.074s +/- 0.001s (x16.6) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.654s +/- 0.004s | 0.045s +/- 0.000s (x14.5) | 0.016s +/- 0.000s (x39.7) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.025s +/- 0.001s |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | N/A | N/A | 0.026s +/- 0.006s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 5.005s +/- 0.278s | 0.275s +/- 0.007s (x18.2) | 0.126s +/- 0.002s (x39.6) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 41.158s +/- 1.333s | 2.255s +/- 0.011s (x18.3) | 0.631s +/- 0.005s (x65.2) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.825s +/- 0.002s | 0.088s +/- 0.001s (x9.4) | 0.016s +/- 0.000s (x50.9) |

## Services

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.297s +/- 0.002s | 0.021s +/- 0.000s (x13.8) | 0.011s +/- 0.000s (x27.8) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.374s +/- 0.003s | 0.024s +/- 0.000s (x15.8) | 0.014s +/- 0.000s (x26.5) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 1.139s +/- 0.003s | 0.097s +/- 0.001s (x11.7) | 0.029s +/- 0.000s (x39.1) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.947s +/- 0.007s | 0.085s +/- 0.001s (x11.2) | 0.033s +/- 0.000s (x28.5) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.769s +/- 0.004s | 0.082s +/- 0.000s (x9.4) | 0.019s +/- 0.000s (x39.6) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 1.492s +/- 0.005s | 0.133s +/- 0.001s (x11.2) | 0.035s +/- 0.000s (x42.9) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 1.832s +/- 0.007s | 0.156s +/- 0.002s (x11.7) | 0.049s +/- 0.001s (x37.5) |

## Adaptors

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.200s +/- 0.001s | 0.022s +/- 0.000s (x8.9) | 0.010s +/- 0.000s (x19.5) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.193s +/- 0.001s | 0.020s +/- 0.000s (x9.7) | 0.012s +/- 0.000s (x16.0) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.722s +/- 0.006s | 0.051s +/- 0.000s (x14.1) | 0.032s +/- 0.000s (x22.8) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.540s +/- 0.003s | 0.039s +/- 0.001s (x13.8) | 0.036s +/- 0.000s (x15.2) |
