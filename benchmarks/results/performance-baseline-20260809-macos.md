# hgraph performance matrix

- date: 2026-08-09T15:14:40+00:00
- host: macOS-26.5.2-arm64-arm-64bit-Mach-O / arm
- CPU: Apple M4 Max
- Python: 3.14.6
- compiler: Apple clang version 21.0.0 (clang-2100.1.1.101)
- hg_cpp revision: 17a5b81bca91
- hg_cpp source fingerprint: fb5d49c9b61a226bce0c2a1f268556e2a3ffcc13babcec3a90d692f49767da36
- hg_cpp build type: Release
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: Python (`upstream-py`), legacy C++ (`upstream-cpp`), hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs Python.
hg_cpp-only sections are tracked without an upstream comparison.

## Graph construction

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 0.955s +/- 0.003s | 0.899s +/- 0.001s (x1.1) | 0.046s +/- 0.000s (x20.8) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.179s +/- 0.001s | 0.144s +/- 0.001s (x1.2) | 0.078s +/- 0.000s (x2.3) |

## Scheduler

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 1.137s +/- 0.006s | 0.058s +/- 0.000s (x19.5) | 0.025s +/- 0.000s (x44.6) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.449s +/- 0.002s | 0.025s +/- 0.000s (x18.2) | 0.021s +/- 0.000s (x21.4) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 3.884s +/- 0.051s | 0.169s +/- 0.000s (x23.0) | 0.063s +/- 0.001s (x62.0) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 4.840s +/- 0.002s | 0.229s +/- 0.001s (x21.2) | 0.089s +/- 0.001s (x54.2) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 1.267s +/- 0.006s | 0.072s +/- 0.000s (x17.5) | 0.049s +/- 0.000s (x25.7) |

## Python boundary

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.110s +/- 0.001s | 0.014s +/- 0.000s (x7.7) | 0.008s +/- 0.000s (x13.7) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.110s +/- 0.000s | 0.014s +/- 0.000s (x7.9) | 0.012s +/- 0.000s (x9.1) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.018s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.343s +/- 0.001s | 0.027s +/- 0.000s (x12.5) | 0.012s +/- 0.000s (x28.7) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.346s +/- 0.003s | 0.028s +/- 0.000s (x12.5) | 0.012s +/- 0.000s (x28.6) |
| String concatenation (`type_str_std`) | 20000 | 0.346s +/- 0.001s | 0.029s +/- 0.001s (x11.8) | 0.014s +/- 0.001s (x24.3) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.340s +/- 0.002s | 0.028s +/- 0.000s (x12.3) | 0.015s +/- 0.000s (x22.2) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.174s +/- 0.000s | 0.022s +/- 0.000s (x7.9) | 0.015s +/- 0.000s (x11.4) |

## Python-owned structured scalars

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.010s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.009s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.012s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.011s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.017s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.019s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.017s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.030s +/- 0.000s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.012s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.012s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.805s +/- 0.004s | 0.060s +/- 0.001s (x13.4) | 0.030s +/- 0.000s (x26.4) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.209s +/- 0.001s | 0.018s +/- 0.000s (x11.6) | 0.010s +/- 0.000s (x21.3) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.077s +/- 0.000s | 0.071s +/- 0.000s (x1.1) | 0.061s +/- 0.000s (x1.3) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 3.153s +/- 0.028s | 0.175s +/- 0.001s (x18.0) | 0.090s +/- 0.002s (x35.1) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 2.278s +/- 0.011s | 0.122s +/- 0.001s (x18.7) | 0.107s +/- 0.000s (x21.3) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.271s +/- 0.001s | 0.030s +/- 0.001s (x9.0) | 0.015s +/- 0.000s (x17.9) |
| Map only (`tsd_dense_map_std`) | 1000 | 2.367s +/- 0.011s | 0.147s +/- 0.001s (x16.1) | 0.069s +/- 0.001s (x34.3) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 1.225s +/- 0.006s | 0.071s +/- 0.001s (x17.2) | 0.035s +/- 0.000s (x34.6) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 3.154s +/- 0.027s | 0.190s +/- 0.000s (x16.6) | 0.109s +/- 0.001s (x29.0) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 1.275s +/- 0.003s | 0.079s +/- 0.001s (x16.0) | 0.029s +/- 0.001s (x44.5) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.047s +/- 0.000s | 0.013s +/- 0.000s (x3.5) | 0.004s +/- 0.000s (x11.2) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.459s +/- 0.000s | 0.044s +/- 0.000s (x10.3) | 0.025s +/- 0.001s (x18.5) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.861s +/- 0.003s | 0.046s +/- 0.000s (x18.8) | 0.011s +/- 0.000s (x81.4) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 30.955s +/- 0.450s | 1.643s +/- 0.005s (x18.8) | 0.912s +/- 0.006s (x33.9) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 2.821s +/- 0.011s | 0.171s +/- 0.001s (x16.5) | 0.038s +/- 0.000s (x74.4) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 2.243s +/- 0.004s | 0.124s +/- 0.001s (x18.0) | 0.033s +/- 0.000s (x68.2) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.111s +/- 0.000s | 0.019s +/- 0.000s (x5.8) | 0.003s +/- 0.000s (x32.2) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 2.016s +/- 0.013s | 0.128s +/- 0.000s (x15.8) | 0.033s +/- 0.000s (x62.0) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.934s +/- 0.003s | 0.060s +/- 0.000s (x15.7) | 0.008s +/- 0.000s (x112.9) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 1.571s +/- 0.004s | 0.119s +/- 0.001s (x13.2) | 0.034s +/- 0.001s (x46.0) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 29.151s +/- 0.088s | 1.627s +/- 0.002s (x17.9) | 0.430s +/- 0.002s (x67.9) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 1.373s +/- 0.002s | 0.091s +/- 0.001s (x15.1) | 0.023s +/- 0.000s (x60.0) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 1.369s +/- 0.004s | 0.084s +/- 0.001s (x16.3) | 0.035s +/- 0.001s (x39.6) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.490s +/- 0.003s | 0.039s +/- 0.001s (x12.5) | 0.019s +/- 0.000s (x25.5) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 1.713s +/- 0.026s | 0.111s +/- 0.001s (x15.4) | 0.014s +/- 0.000s (x120.8) |

## Reduce

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 1.225s +/- 0.004s | 0.072s +/- 0.000s (x17.0) | 0.051s +/- 0.000s (x23.8) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 1.224s +/- 0.002s | 0.072s +/- 0.001s (x16.9) | 0.073s +/- 0.001s (x16.8) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.649s +/- 0.002s | 0.044s +/- 0.000s (x14.7) | 0.016s +/- 0.000s (x40.8) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.023s +/- 0.000s |

## hg_cpp - dynamic TSL

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.019s +/- 0.000s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 4.741s +/- 0.024s | 0.261s +/- 0.002s (x18.2) | 0.126s +/- 0.001s (x37.7) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 38.966s +/- 0.215s | 2.242s +/- 0.009s (x17.4) | 0.627s +/- 0.001s (x62.2) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.818s +/- 0.004s | 0.086s +/- 0.000s (x9.5) | 0.016s +/- 0.000s (x51.1) |

## Services

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.293s +/- 0.003s | 0.021s +/- 0.000s (x14.3) | 0.011s +/- 0.000s (x27.1) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.374s +/- 0.001s | 0.023s +/- 0.000s (x16.5) | 0.014s +/- 0.000s (x27.0) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 1.145s +/- 0.007s | 0.097s +/- 0.001s (x11.8) | 0.029s +/- 0.000s (x39.3) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.962s +/- 0.007s | 0.083s +/- 0.001s (x11.6) | 0.033s +/- 0.000s (x29.5) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.774s +/- 0.005s | 0.079s +/- 0.000s (x9.7) | 0.019s +/- 0.000s (x40.0) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 1.475s +/- 0.004s | 0.130s +/- 0.002s (x11.3) | 0.033s +/- 0.000s (x44.2) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 1.807s +/- 0.003s | 0.151s +/- 0.001s (x11.9) | 0.047s +/- 0.000s (x38.3) |

## Adaptors

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.196s +/- 0.001s | 0.022s +/- 0.000s (x9.1) | 0.011s +/- 0.000s (x18.1) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.191s +/- 0.002s | 0.019s +/- 0.000s (x9.9) | 0.012s +/- 0.000s (x16.0) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.708s +/- 0.003s | 0.050s +/- 0.001s (x14.2) | 0.031s +/- 0.000s (x22.5) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.541s +/- 0.004s | 0.038s +/- 0.000s (x14.1) | 0.035s +/- 0.000s (x15.6) |
