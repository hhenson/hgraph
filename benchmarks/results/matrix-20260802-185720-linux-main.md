# hgraph performance matrix

- date: 2026-08-02T18:57:20+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- compiler: c++ (Ubuntu 14.3.0-14ubuntu1) 14.3.0
- hg_cpp revision: a4fccd9b5898
- hg_cpp source fingerprint: b982b71ce0c817bbfd6acf1e1f05879364eaf9b9ee1e7d2b716a1653e75702b6
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
| Wide/deep graph - native operators (`construct_std`) | 1 | 1.421s +/- 0.007s | 1.370s +/- 0.006s (x1.0) | 0.062s +/- 0.001s (x22.9) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.265s +/- 0.000s | 0.239s +/- 0.000s (x1.1) | 0.136s +/- 0.001s (x2.0) |

## Scheduler

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 1.480s +/- 0.005s | 0.097s +/- 0.000s (x15.3) | 0.049s +/- 0.000s (x30.4) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.588s +/- 0.001s | 0.041s +/- 0.000s (x14.3) | 0.035s +/- 0.000s (x16.6) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 4.731s +/- 0.008s | 0.255s +/- 0.004s (x18.6) | 0.130s +/- 0.001s (x36.5) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 6.086s +/- 0.009s | 0.339s +/- 0.005s (x18.0) | 0.176s +/- 0.000s (x34.6) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 1.579s +/- 0.005s | 0.107s +/- 0.000s (x14.7) | 0.094s +/- 0.000s (x16.8) |

## Python boundary

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.147s +/- 0.001s | 0.022s +/- 0.000s (x6.6) | 0.012s +/- 0.000s (x12.6) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.147s +/- 0.001s | 0.023s +/- 0.000s (x6.5) | 0.018s +/- 0.000s (x8.0) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.029s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.439s +/- 0.001s | 0.042s +/- 0.000s (x10.4) | 0.020s +/- 0.000s (x21.5) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.436s +/- 0.002s | 0.042s +/- 0.001s (x10.4) | 0.021s +/- 0.000s (x20.8) |
| String concatenation (`type_str_std`) | 20000 | 0.441s +/- 0.002s | 0.045s +/- 0.000s (x9.8) | 0.021s +/- 0.000s (x20.6) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.427s +/- 0.002s | 0.042s +/- 0.000s (x10.1) | 0.025s +/- 0.000s (x17.2) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.226s +/- 0.001s | 0.034s +/- 0.000s (x6.6) | 0.024s +/- 0.000s (x9.5) |

## Python-owned structured scalars

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.014s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.012s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.019s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.017s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.028s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.031s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.029s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.049s +/- 0.000s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.018s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.018s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 1.087s +/- 0.000s | 0.091s +/- 0.001s (x11.9) | 0.048s +/- 0.000s (x22.5) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.269s +/- 0.002s | 0.029s +/- 0.000s (x9.4) | 0.016s +/- 0.000s (x17.0) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.116s +/- 0.001s | 0.084s +/- 0.000s (x1.4) | 0.078s +/- 0.000s (x1.5) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 3.887s +/- 0.016s | 0.256s +/- 0.001s (x15.2) | 0.172s +/- 0.000s (x22.6) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 2.844s +/- 0.005s | 0.176s +/- 0.001s (x16.2) | 0.202s +/- 0.000s (x14.1) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.355s +/- 0.003s | 0.048s +/- 0.000s (x7.4) | 0.028s +/- 0.000s (x12.6) |
| Map only (`tsd_dense_map_std`) | 1000 | 2.870s +/- 0.002s | 0.201s +/- 0.001s (x14.3) | 0.127s +/- 0.000s (x22.5) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 1.554s +/- 0.003s | 0.104s +/- 0.000s (x14.9) | 0.073s +/- 0.000s (x21.4) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 3.954s +/- 0.015s | 0.295s +/- 0.001s (x13.4) | 0.185s +/- 0.000s (x21.4) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 1.637s +/- 0.005s | 0.143s +/- 0.001s (x11.4) | 0.059s +/- 0.000s (x27.7) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.064s +/- 0.000s | 0.021s +/- 0.000s (x3.1) | 0.010s +/- 0.000s (x6.5) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.629s +/- 0.001s | 0.082s +/- 0.001s (x7.7) | 0.045s +/- 0.000s (x14.0) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 1.047s +/- 0.002s | 0.071s +/- 0.000s (x14.7) | 0.024s +/- 0.000s (x43.6) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 34.397s +/- 0.047s | 3.424s +/- 0.003s (x10.0) | 2.294s +/- 0.004s (x15.0) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 3.736s +/- 0.008s | 0.268s +/- 0.001s (x14.0) | 0.069s +/- 0.000s (x54.2) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 2.948s +/- 0.003s | 0.191s +/- 0.001s (x15.5) | 0.057s +/- 0.000s (x51.6) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.162s +/- 0.000s | 0.029s +/- 0.000s (x5.6) | 0.006s +/- 0.000s (x28.2) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 2.656s +/- 0.008s | 0.202s +/- 0.002s (x13.2) | 0.058s +/- 0.000s (x46.2) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 1.190s +/- 0.003s | 0.089s +/- 0.001s (x13.4) | 0.016s +/- 0.000s (x75.3) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 2.031s +/- 0.001s | 0.222s +/- 0.001s (x9.2) | 0.091s +/- 0.000s (x22.3) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 39.244s +/- 0.084s | 3.898s +/- 0.003s (x10.1) | 0.765s +/- 0.002s (x51.3) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 1.825s +/- 0.008s | 0.143s +/- 0.001s (x12.7) | 0.040s +/- 0.000s (x45.3) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 1.735s +/- 0.005s | 0.146s +/- 0.002s (x11.9) | 0.064s +/- 0.000s (x26.9) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.609s +/- 0.003s | 0.061s +/- 0.000s (x10.0) | 0.035s +/- 0.000s (x17.4) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 2.202s +/- 0.012s | 0.173s +/- 0.001s (x12.7) | 0.027s +/- 0.000s (x82.3) |

## Reduce

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 1.569s +/- 0.012s | 0.105s +/- 0.000s (x15.0) | 0.095s +/- 0.001s (x16.5) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 1.555s +/- 0.008s | 0.103s +/- 0.000s (x15.0) | 0.133s +/- 0.001s (x11.7) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.804s +/- 0.003s | 0.065s +/- 0.000s (x12.4) | 0.029s +/- 0.000s (x27.6) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.042s +/- 0.000s |

## hg_cpp - dynamic TSL

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.037s +/- 0.000s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 5.963s +/- 0.026s | 0.431s +/- 0.001s (x13.8) | 0.211s +/- 0.002s (x28.2) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 51.547s +/- 0.056s | 3.795s +/- 0.004s (x13.6) | 1.144s +/- 0.001s (x45.0) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 1.102s +/- 0.003s | 0.159s +/- 0.000s (x6.9) | 0.027s +/- 0.000s (x41.3) |

## Services

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.363s +/- 0.002s | 0.032s +/- 0.000s (x11.4) | 0.017s +/- 0.000s (x22.0) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.467s +/- 0.005s | 0.037s +/- 0.000s (x12.6) | 0.022s +/- 0.000s (x20.8) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 1.521s +/- 0.006s | 0.137s +/- 0.000s (x11.1) | 0.072s +/- 0.000s (x21.1) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 1.292s +/- 0.004s | 0.122s +/- 0.000s (x10.6) | 0.083s +/- 0.000s (x15.5) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 1.043s +/- 0.003s | 0.115s +/- 0.000s (x9.0) | 0.058s +/- 0.000s (x17.9) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 1.886s +/- 0.021s | 0.213s +/- 0.001s (x8.9) | 0.062s +/- 0.000s (x30.3) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 2.342s +/- 0.002s | 0.267s +/- 0.003s (x8.8) | 0.095s +/- 0.001s (x24.7) |

## Adaptors

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.252s +/- 0.001s | 0.033s +/- 0.000s (x7.5) | 0.016s +/- 0.000s (x15.8) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.247s +/- 0.002s | 0.030s +/- 0.000s (x8.1) | 0.019s +/- 0.000s (x13.3) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.897s +/- 0.005s | 0.074s +/- 0.001s (x12.1) | 0.058s +/- 0.000s (x15.6) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.685s +/- 0.002s | 0.059s +/- 0.001s (x11.7) | 0.067s +/- 0.000s (x10.2) |
