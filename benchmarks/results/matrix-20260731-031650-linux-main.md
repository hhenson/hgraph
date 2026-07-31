# hgraph performance matrix

- date: 2026-07-31T03:16:50+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- compiler: c++ (Ubuntu 15.2.0-16ubuntu1) 15.2.0
- hg_cpp revision: 9db88d027108
- hg_cpp source fingerprint: e0deeba44edf67b7856d84377041d54d9892d2b8c8ff509fa68e191f88bf0a70
- hg_cpp build type: Release
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: legacy C++ (`upstream-cpp`), hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs legacy C++.
hg_cpp-only sections are tracked without an upstream comparison.

## Graph construction

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 1.352s +/- 0.024s | 0.085s +/- 0.000s (x15.9) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.234s +/- 0.002s | 0.144s +/- 0.001s (x1.6) |

## Scheduler

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 0.095s +/- 0.001s | 0.046s +/- 0.000s (x2.0) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.041s +/- 0.000s | 0.036s +/- 0.000s (x1.1) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 0.252s +/- 0.002s | 0.132s +/- 0.001s (x1.9) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 0.336s +/- 0.005s | 0.177s +/- 0.002s (x1.9) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 0.108s +/- 0.001s | 0.093s +/- 0.001s (x1.2) |

## Python boundary

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.023s +/- 0.000s | 0.013s +/- 0.000s (x1.8) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.023s +/- 0.000s | 0.020s +/- 0.001s (x1.2) |

## Value types

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.043s +/- 0.000s | 0.021s +/- 0.000s (x2.0) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.042s +/- 0.000s | 0.022s +/- 0.001s (x1.9) |
| String concatenation (`type_str_std`) | 20000 | 0.045s +/- 0.000s | 0.023s +/- 0.000s (x2.0) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.043s +/- 0.000s | 0.025s +/- 0.000s (x1.7) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.035s +/- 0.000s | 0.023s +/- 0.000s (x1.5) |

## Python-owned structured scalars

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.015s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.013s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.019s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.018s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.029s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.030s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.029s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.046s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.019s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.019s +/- 0.000s |

## Value types

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.090s +/- 0.000s | 0.049s +/- 0.001s (x1.8) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.029s +/- 0.000s | 0.017s +/- 0.000s (x1.7) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.085s +/- 0.000s | 0.074s +/- 0.000s (x1.1) |

## TSD - dense

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 0.259s +/- 0.001s | 0.168s +/- 0.000s (x1.5) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 0.178s +/- 0.001s | 0.206s +/- 0.001s (x0.9) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.048s +/- 0.000s | 0.027s +/- 0.000s (x1.8) |
| Map only (`tsd_dense_map_std`) | 1000 | 0.204s +/- 0.001s | 0.125s +/- 0.000s (x1.6) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 0.106s +/- 0.001s | 0.071s +/- 0.000s (x1.5) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 0.295s +/- 0.003s | 0.184s +/- 0.001s (x1.6) |

## TSD - sparse

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 0.146s +/- 0.000s | 0.064s +/- 0.000s (x2.3) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.021s +/- 0.000s | 0.008s +/- 0.000s (x2.6) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.083s +/- 0.000s | 0.047s +/- 0.000s (x1.8) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.073s +/- 0.000s | 0.025s +/- 0.000s (x2.9) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 3.492s +/- 0.005s | 1.989s +/- 0.001s (x1.8) |

## TSD - key lifecycle

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 0.272s +/- 0.002s | 0.089s +/- 0.001s (x3.1) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 0.194s +/- 0.001s | 0.072s +/- 0.000s (x2.7) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.029s +/- 0.000s | 0.006s +/- 0.000s (x5.1) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 0.202s +/- 0.001s | 0.077s +/- 0.001s (x2.6) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.090s +/- 0.000s | 0.016s +/- 0.000s (x5.7) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 0.225s +/- 0.000s | 0.099s +/- 0.000s (x2.3) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 3.952s +/- 0.014s | 1.079s +/- 0.002s (x3.7) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 0.144s +/- 0.001s | 0.057s +/- 0.000s (x2.5) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 0.147s +/- 0.000s | 0.073s +/- 0.000s (x2.0) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.062s +/- 0.000s | 0.037s +/- 0.000s (x1.6) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 0.177s +/- 0.001s | 0.030s +/- 0.000s (x5.9) |

## Reduce

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 0.105s +/- 0.000s | 0.098s +/- 0.000s (x1.1) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 0.105s +/- 0.001s | 0.136s +/- 0.000s (x0.8) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.066s +/- 0.000s | 0.030s +/- 0.001s (x2.2) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | 0.052s +/- 0.001s |

## hg_cpp - dynamic TSL

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.037s +/- 0.000s |

## Nested graphs

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 0.435s +/- 0.004s | 0.318s +/- 0.002s (x1.4) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 3.786s +/- 0.003s | 1.569s +/- 0.006s (x2.4) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.163s +/- 0.001s | 0.035s +/- 0.000s (x4.6) |

## Services

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.032s +/- 0.000s | 0.017s +/- 0.000s (x1.9) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.037s +/- 0.000s | 0.022s +/- 0.000s (x1.6) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 0.141s +/- 0.002s | 0.072s +/- 0.000s (x2.0) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.124s +/- 0.001s | 0.082s +/- 0.001s (x1.5) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.118s +/- 0.001s | 0.057s +/- 0.000s (x2.0) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 0.215s +/- 0.001s | 0.059s +/- 0.001s (x3.6) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 0.270s +/- 0.000s | 0.100s +/- 0.000s (x2.7) |

## Adaptors

| workload | cycles | legacy C++ | hg_cpp |
|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.034s +/- 0.000s | 0.016s +/- 0.000s (x2.1) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.031s +/- 0.000s | 0.019s +/- 0.000s (x1.6) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.075s +/- 0.000s | 0.057s +/- 0.001s (x1.3) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.059s +/- 0.000s | 0.067s +/- 0.002s (x0.9) |
