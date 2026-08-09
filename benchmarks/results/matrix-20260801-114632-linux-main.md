# hgraph performance matrix

- date: 2026-08-01T11:46:32+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- compiler: c++ (Ubuntu 14.3.0-14ubuntu1) 14.3.0
- hg_cpp revision: 251cd09cbe17
- hg_cpp source fingerprint: 057ad8fb69455f1b67fbc0a6c010b66e7fd83e55fd8957d85e3b5f65902325ea
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
| Wide/deep graph - native operators (`construct_std`) | 1 | 1.401s +/- 0.005s | 1.352s +/- 0.001s (x1.0) | 0.073s +/- 0.000s (x19.1) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.260s +/- 0.001s | 0.233s +/- 0.000s (x1.1) | 0.139s +/- 0.002s (x1.9) |

## Scheduler

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 1.471s +/- 0.012s | 0.096s +/- 0.000s (x15.4) | 0.047s +/- 0.000s (x31.6) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.584s +/- 0.001s | 0.041s +/- 0.000s (x14.3) | 0.033s +/- 0.000s (x17.8) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 4.718s +/- 0.011s | 0.249s +/- 0.001s (x18.9) | 0.127s +/- 0.000s (x37.0) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 6.008s +/- 0.004s | 0.334s +/- 0.001s (x18.0) | 0.170s +/- 0.000s (x35.3) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 1.564s +/- 0.006s | 0.106s +/- 0.000s (x14.7) | 0.090s +/- 0.000s (x17.5) |

## Python boundary

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.145s +/- 0.000s | 0.023s +/- 0.000s (x6.4) | 0.012s +/- 0.000s (x12.4) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.145s +/- 0.000s | 0.022s +/- 0.000s (x6.5) | 0.018s +/- 0.000s (x8.2) |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.434s +/- 0.001s | 0.042s +/- 0.000s (x10.4) | 0.020s +/- 0.000s (x22.1) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.431s +/- 0.002s | 0.042s +/- 0.000s (x10.3) | 0.020s +/- 0.000s (x21.5) |
| String concatenation (`type_str_std`) | 20000 | 0.434s +/- 0.001s | 0.044s +/- 0.000s (x9.9) | 0.021s +/- 0.000s (x20.6) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.425s +/- 0.003s | 0.041s +/- 0.000s (x10.3) | 0.024s +/- 0.000s (x17.8) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.223s +/- 0.001s | 0.034s +/- 0.000s (x6.6) | 0.023s +/- 0.000s (x9.9) |

## Python-owned structured scalars

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.014s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.012s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.018s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.017s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.028s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.029s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.027s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.047s +/- 0.000s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.017s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.018s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 1.082s +/- 0.004s | 0.091s +/- 0.000s (x11.9) | 0.048s +/- 0.000s (x22.7) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.268s +/- 0.001s | 0.029s +/- 0.000s (x9.3) | 0.015s +/- 0.000s (x17.8) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.115s +/- 0.001s | 0.083s +/- 0.000s (x1.4) | 0.080s +/- 0.000s (x1.4) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 3.888s +/- 0.025s | 0.256s +/- 0.004s (x15.2) | 0.174s +/- 0.000s (x22.4) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 2.846s +/- 0.004s | 0.174s +/- 0.000s (x16.3) | 0.199s +/- 0.000s (x14.3) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.349s +/- 0.000s | 0.047s +/- 0.000s (x7.4) | 0.029s +/- 0.000s (x12.2) |
| Map only (`tsd_dense_map_std`) | 1000 | 2.860s +/- 0.017s | 0.201s +/- 0.001s (x14.3) | 0.129s +/- 0.000s (x22.1) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 1.549s +/- 0.012s | 0.104s +/- 0.000s (x14.9) | 0.074s +/- 0.000s (x20.9) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 3.922s +/- 0.031s | 0.292s +/- 0.000s (x13.4) | 0.187s +/- 0.000s (x20.9) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 1.627s +/- 0.004s | 0.142s +/- 0.000s (x11.5) | 0.068s +/- 0.000s (x23.8) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.064s +/- 0.000s | 0.021s +/- 0.000s (x3.0) | 0.010s +/- 0.000s (x6.5) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.623s +/- 0.002s | 0.081s +/- 0.000s (x7.7) | 0.050s +/- 0.000s (x12.4) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 1.026s +/- 0.002s | 0.070s +/- 0.000s (x14.6) | 0.027s +/- 0.000s (x37.9) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 34.201s +/- 0.098s | 3.394s +/- 0.003s (x10.1) | 2.381s +/- 0.006s (x14.4) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 3.726s +/- 0.026s | 0.265s +/- 0.001s (x14.1) | 0.091s +/- 0.000s (x40.9) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 2.940s +/- 0.007s | 0.190s +/- 0.000s (x15.5) | 0.073s +/- 0.000s (x40.5) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.161s +/- 0.001s | 0.029s +/- 0.000s (x5.6) | 0.006s +/- 0.000s (x27.4) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 2.625s +/- 0.004s | 0.199s +/- 0.002s (x13.2) | 0.079s +/- 0.000s (x33.2) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 1.180s +/- 0.004s | 0.088s +/- 0.000s (x13.4) | 0.016s +/- 0.000s (x72.3) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 2.023s +/- 0.002s | 0.222s +/- 0.000s (x9.1) | 0.113s +/- 0.001s (x17.9) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 38.996s +/- 0.033s | 3.855s +/- 0.006s (x10.1) | 1.115s +/- 0.005s (x35.0) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 1.819s +/- 0.006s | 0.142s +/- 0.000s (x12.9) | 0.058s +/- 0.000s (x31.5) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 1.726s +/- 0.002s | 0.143s +/- 0.001s (x12.1) | 0.074s +/- 0.000s (x23.2) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.607s +/- 0.001s | 0.061s +/- 0.000s (x10.0) | 0.038s +/- 0.000s (x15.8) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 2.180s +/- 0.008s | 0.176s +/- 0.000s (x12.4) | 0.032s +/- 0.000s (x68.4) |

## Reduce

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 1.537s +/- 0.007s | 0.104s +/- 0.001s (x14.8) | 0.099s +/- 0.000s (x15.5) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 1.579s +/- 0.009s | 0.104s +/- 0.000s (x15.2) | 0.134s +/- 0.000s (x11.8) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.798s +/- 0.004s | 0.065s +/- 0.000s (x12.3) | 0.028s +/- 0.000s (x28.7) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.051s +/- 0.000s |

## hg_cpp - dynamic TSL

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.038s +/- 0.000s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 5.916s +/- 0.011s | 0.431s +/- 0.001s (x13.7) | 0.308s +/- 0.000s (x19.2) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 51.009s +/- 0.246s | 3.778s +/- 0.008s (x13.5) | 1.614s +/- 0.001s (x31.6) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 1.090s +/- 0.006s | 0.158s +/- 0.000s (x6.9) | 0.035s +/- 0.000s (x31.4) |

## Services

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.362s +/- 0.002s | 0.031s +/- 0.000s (x11.5) | 0.016s +/- 0.000s (x22.5) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.465s +/- 0.001s | 0.037s +/- 0.000s (x12.6) | 0.022s +/- 0.000s (x21.5) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 1.522s +/- 0.002s | 0.137s +/- 0.000s (x11.1) | 0.073s +/- 0.000s (x20.9) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 1.299s +/- 0.003s | 0.122s +/- 0.001s (x10.7) | 0.082s +/- 0.000s (x15.9) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 1.038s +/- 0.004s | 0.116s +/- 0.001s (x8.9) | 0.058s +/- 0.000s (x17.9) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 1.870s +/- 0.002s | 0.217s +/- 0.002s (x8.6) | 0.059s +/- 0.000s (x31.7) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 2.319s +/- 0.008s | 0.268s +/- 0.003s (x8.7) | 0.102s +/- 0.001s (x22.7) |

## Adaptors

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.250s +/- 0.001s | 0.033s +/- 0.000s (x7.5) | 0.015s +/- 0.000s (x16.4) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.250s +/- 0.003s | 0.030s +/- 0.000s (x8.3) | 0.018s +/- 0.000s (x14.0) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.891s +/- 0.002s | 0.074s +/- 0.000s (x12.1) | 0.057s +/- 0.000s (x15.6) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.684s +/- 0.009s | 0.059s +/- 0.001s (x11.6) | 0.064s +/- 0.000s (x10.6) |
