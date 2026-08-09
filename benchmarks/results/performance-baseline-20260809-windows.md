# hgraph performance matrix

- date: 2026-08-09T15:00:43+00:00
- host: Windows-10-10.0.19045-SP0 / Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- CPU: Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- Python: 3.14.7
- compiler: unknown
- hg_cpp revision: 17a5b81bca91
- hg_cpp source fingerprint: e99eea3f4792ca4f3b105f14d98a98a6ae3fbb257207a04a1cd138fea5a0aa26
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
| Wide/deep graph - native operators (`construct_std`) | 1 | 2.862s +/- 0.002s | 2.808s +/- 0.017s (x1.0) | 0.255s +/- 0.002s (x11.2) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.531s +/- 0.002s | 0.482s +/- 0.002s (x1.1) | 0.361s +/- 0.001s (x1.5) |

## Scheduler

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 3.318s +/- 0.024s | 0.172s +/- 0.001s (x19.3) | 0.091s +/- 0.001s (x36.4) |
| Five-node Python compute chain (`tick_py`) | 20000 | 1.261s +/- 0.012s | 0.066s +/- 0.000s (x19.0) | 0.080s +/- 0.001s (x15.8) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 10.190s +/- 0.038s | 0.477s +/- 0.002s (x21.4) | 0.237s +/- 0.000s (x43.0) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 13.296s +/- 0.105s | 0.668s +/- 0.002s (x19.9) | 0.311s +/- 0.001s (x42.7) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 3.327s +/- 0.009s | 0.193s +/- 0.001s (x17.2) | 0.194s +/- 0.001s (x17.1) |

## Python boundary

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.326s +/- 0.001s | 0.031s +/- 0.000s (x10.4) | 0.032s +/- 0.000s (x10.3) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.332s +/- 0.003s | 0.031s +/- 0.000s (x10.8) | 0.055s +/- 0.001s (x6.0) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.094s +/- 0.002s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.943s +/- 0.007s | 0.070s +/- 0.000s (x13.5) | 0.049s +/- 0.000s (x19.3) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.951s +/- 0.009s | 0.068s +/- 0.001s (x14.0) | 0.050s +/- 0.000s (x19.0) |
| String concatenation (`type_str_std`) | 20000 | 0.952s +/- 0.004s | 0.079s +/- 0.000s (x12.1) | 0.057s +/- 0.001s (x16.6) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.919s +/- 0.012s | 0.069s +/- 0.000s (x13.3) | 0.060s +/- 0.001s (x15.3) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.487s +/- 0.002s | 0.054s +/- 0.000s (x9.0) | 0.062s +/- 0.002s (x7.9) |

## Python-owned structured scalars

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.038s +/- 0.001s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.033s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.045s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.044s +/- 0.001s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.067s +/- 0.001s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.074s +/- 0.001s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.071s +/- 0.001s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.125s +/- 0.002s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.047s +/- 0.001s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.049s +/- 0.001s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 2.288s +/- 0.016s | 0.165s +/- 0.001s (x13.9) | 0.122s +/- 0.000s (x18.8) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.633s +/- 0.002s | 0.044s +/- 0.000s (x14.4) | 0.041s +/- 0.001s (x15.4) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.243s +/- 0.001s | 0.166s +/- 0.003s (x1.5) | 0.194s +/- 0.001s (x1.2) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 8.095s +/- 0.100s | 0.488s +/- 0.001s (x16.6) | 0.306s +/- 0.002s (x26.4) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 5.927s +/- 0.035s | 0.352s +/- 0.001s (x16.9) | 0.431s +/- 0.002s (x13.8) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.696s +/- 0.006s | 0.078s +/- 0.001s (x8.9) | 0.051s +/- 0.000s (x13.6) |
| Map only (`tsd_dense_map_std`) | 1000 | 6.017s +/- 0.042s | 0.398s +/- 0.002s (x15.1) | 0.228s +/- 0.000s (x26.4) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 3.171s +/- 0.026s | 0.209s +/- 0.002s (x15.2) | 0.131s +/- 0.001s (x24.3) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 8.150s +/- 0.064s | 0.554s +/- 0.001s (x14.7) | 0.323s +/- 0.001s (x25.2) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 3.463s +/- 0.007s | 0.298s +/- 0.004s (x11.6) | 0.105s +/- 0.001s (x33.0) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.123s +/- 0.000s | 0.029s +/- 0.000s (x4.3) | 0.020s +/- 0.000s (x6.3) |
| Map only (`tsd_sparse_map_std`) | 2000 | 1.261s +/- 0.007s | 0.153s +/- 0.004s (x8.2) | 0.081s +/- 0.000s (x15.5) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 2.331s +/- 0.012s | 0.173s +/- 0.001s (x13.5) | 0.042s +/- 0.000s (x56.0) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 83.160s +/- 2.081s | 15.876s +/- 0.066s (x5.2) | 3.672s +/- 0.014s (x22.6) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 7.578s +/- 0.035s | 0.616s +/- 0.013s (x12.3) | 0.160s +/- 0.001s (x47.3) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 6.050s +/- 0.038s | 0.440s +/- 0.002s (x13.8) | 0.162s +/- 0.001s (x37.3) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.316s +/- 0.001s | 0.051s +/- 0.001s (x6.1) | 0.013s +/- 0.000s (x24.0) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 5.411s +/- 0.041s | 0.457s +/- 0.005s (x11.8) | 0.135s +/- 0.001s (x40.0) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 2.565s +/- 0.002s | 0.186s +/- 0.002s (x13.8) | 0.034s +/- 0.000s (x76.4) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 4.228s +/- 0.000s | 0.522s +/- 0.002s (x8.1) | 0.164s +/- 0.000s (x25.8) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 74.861s +/- 0.325s | 6.383s +/- 0.011s (x11.7) | 1.547s +/- 0.001s (x48.4) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 3.725s +/- 0.010s | 0.308s +/- 0.003s (x12.1) | 0.094s +/- 0.000s (x39.6) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 3.653s +/- 0.006s | 0.298s +/- 0.005s (x12.3) | 0.123s +/- 0.000s (x29.8) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 1.301s +/- 0.002s | 0.114s +/- 0.000s (x11.4) | 0.067s +/- 0.000s (x19.5) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 4.576s +/- 0.006s | 0.400s +/- 0.001s (x11.4) | 0.063s +/- 0.000s (x72.1) |

## Reduce

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 3.146s +/- 0.017s | 0.209s +/- 0.002s (x15.1) | 0.157s +/- 0.001s (x20.1) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 3.169s +/- 0.024s | 0.207s +/- 0.002s (x15.3) | 0.247s +/- 0.001s (x12.8) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 1.685s +/- 0.004s | 0.115s +/- 0.000s (x14.6) | 0.058s +/- 0.000s (x28.9) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.133s +/- 0.002s |

## hg_cpp - dynamic TSL

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.086s +/- 0.000s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 12.244s +/- 0.079s | 1.010s +/- 0.004s (x12.1) | 0.492s +/- 0.001s (x24.9) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 100.937s +/- 0.806s | 8.000s +/- 0.057s (x12.6) | 2.438s +/- 0.023s (x41.4) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 2.151s +/- 0.008s | 0.317s +/- 0.002s (x6.8) | 0.067s +/- 0.000s (x32.0) |

## Services

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.809s +/- 0.003s | 0.046s +/- 0.000s (x17.4) | 0.042s +/- 0.001s (x19.3) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 1.032s +/- 0.011s | 0.056s +/- 0.000s (x18.5) | 0.056s +/- 0.001s (x18.4) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 3.180s +/- 0.013s | 0.297s +/- 0.002s (x10.7) | 0.122s +/- 0.001s (x26.0) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 2.653s +/- 0.018s | 0.259s +/- 0.000s (x10.2) | 0.161s +/- 0.003s (x16.5) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 2.154s +/- 0.003s | 0.249s +/- 0.001s (x8.6) | 0.080s +/- 0.000s (x26.8) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 3.972s +/- 0.024s | 0.550s +/- 0.001s (x7.2) | 0.206s +/- 0.001s (x19.3) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 4.869s +/- 0.027s | 0.670s +/- 0.002s (x7.3) | 0.330s +/- 0.001s (x14.8) |

## Adaptors

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.558s +/- 0.001s | 0.052s +/- 0.001s (x10.8) | 0.042s +/- 0.001s (x13.3) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.545s +/- 0.006s | 0.046s +/- 0.000s (x11.8) | 0.050s +/- 0.000s (x10.9) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 1.907s +/- 0.007s | 0.132s +/- 0.001s (x14.4) | 0.130s +/- 0.002s (x14.7) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 1.458s +/- 0.004s | 0.101s +/- 0.001s (x14.4) | 0.167s +/- 0.001s (x8.7) |
