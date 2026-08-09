# hgraph performance matrix

- date: 2026-08-09T14:33:22+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- compiler: c++ (Ubuntu 14.3.0-14ubuntu1) 14.3.0
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
| Wide/deep graph - native operators (`construct_std`) | 1 | 1.369s +/- 0.002s | 1.311s +/- 0.007s (x1.0) | 0.062s +/- 0.001s (x22.2) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.255s +/- 0.002s | 0.228s +/- 0.000s (x1.1) | 0.144s +/- 0.002s (x1.8) |

## Scheduler

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 1.464s +/- 0.005s | 0.094s +/- 0.001s (x15.5) | 0.047s +/- 0.000s (x31.1) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.582s +/- 0.006s | 0.041s +/- 0.000s (x14.2) | 0.032s +/- 0.001s (x18.4) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 4.832s +/- 0.052s | 0.251s +/- 0.001s (x19.2) | 0.127s +/- 0.000s (x38.1) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 5.989s +/- 0.032s | 0.335s +/- 0.000s (x17.9) | 0.169s +/- 0.001s (x35.4) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 1.596s +/- 0.010s | 0.109s +/- 0.001s (x14.7) | 0.094s +/- 0.000s (x17.0) |

## Python boundary

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.148s +/- 0.005s | 0.022s +/- 0.000s (x6.6) | 0.012s +/- 0.000s (x12.4) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.147s +/- 0.001s | 0.023s +/- 0.000s (x6.3) | 0.018s +/- 0.000s (x8.0) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.029s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.440s +/- 0.006s | 0.042s +/- 0.000s (x10.6) | 0.021s +/- 0.000s (x21.3) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.438s +/- 0.010s | 0.042s +/- 0.001s (x10.6) | 0.021s +/- 0.000s (x20.8) |
| String concatenation (`type_str_std`) | 20000 | 0.448s +/- 0.007s | 0.044s +/- 0.001s (x10.1) | 0.022s +/- 0.001s (x20.3) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.441s +/- 0.001s | 0.042s +/- 0.001s (x10.6) | 0.024s +/- 0.000s (x18.2) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.225s +/- 0.002s | 0.034s +/- 0.000s (x6.7) | 0.022s +/- 0.001s (x10.0) |

## Python-owned structured scalars

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.014s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.012s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.018s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.018s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.028s +/- 0.000s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.031s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.030s +/- 0.001s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.051s +/- 0.000s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.019s +/- 0.001s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.019s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 1.111s +/- 0.016s | 0.091s +/- 0.001s (x12.2) | 0.050s +/- 0.001s (x22.1) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.267s +/- 0.004s | 0.030s +/- 0.000s (x9.0) | 0.015s +/- 0.000s (x17.6) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.118s +/- 0.001s | 0.082s +/- 0.001s (x1.4) | 0.072s +/- 0.000s (x1.6) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 3.924s +/- 0.023s | 0.258s +/- 0.000s (x15.2) | 0.171s +/- 0.001s (x22.9) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 2.844s +/- 0.034s | 0.178s +/- 0.001s (x16.0) | 0.189s +/- 0.004s (x15.0) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.356s +/- 0.003s | 0.048s +/- 0.001s (x7.4) | 0.028s +/- 0.000s (x12.9) |
| Map only (`tsd_dense_map_std`) | 1000 | 2.936s +/- 0.029s | 0.197s +/- 0.002s (x14.9) | 0.125s +/- 0.001s (x23.4) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 1.562s +/- 0.023s | 0.105s +/- 0.001s (x14.9) | 0.071s +/- 0.001s (x21.9) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | FAIL | 0.291s +/- 0.004s | 0.179s +/- 0.002s |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 1.655s +/- 0.011s | 0.145s +/- 0.001s (x11.4) | 0.058s +/- 0.000s (x28.4) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.065s +/- 0.000s | 0.021s +/- 0.000s (x3.1) | 0.010s +/- 0.000s (x6.7) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.637s +/- 0.009s | 0.081s +/- 0.001s (x7.8) | 0.044s +/- 0.000s (x14.6) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 1.009s +/- 0.003s | 0.071s +/- 0.001s (x14.2) | 0.023s +/- 0.000s (x43.8) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 34.213s +/- 0.220s | 3.395s +/- 0.030s (x10.1) | 2.330s +/- 0.002s (x14.7) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 3.785s +/- 0.010s | 0.266s +/- 0.002s (x14.2) | 0.067s +/- 0.000s (x56.6) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 2.997s +/- 0.062s | 0.193s +/- 0.003s (x15.5) | 0.057s +/- 0.001s (x52.6) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.160s +/- 0.001s | 0.029s +/- 0.000s (x5.5) | 0.006s +/- 0.000s (x28.1) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 2.705s +/- 0.021s | 0.204s +/- 0.001s (x13.3) | 0.058s +/- 0.000s (x46.5) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 1.202s +/- 0.008s | 0.088s +/- 0.002s (x13.6) | 0.016s +/- 0.000s (x75.1) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 2.049s +/- 0.002s | 0.225s +/- 0.001s (x9.1) | 0.091s +/- 0.003s (x22.6) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 39.579s +/- 0.292s | 3.902s +/- 0.027s (x10.1) | 0.754s +/- 0.005s (x52.5) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 1.856s +/- 0.001s | 0.143s +/- 0.002s (x13.0) | 0.040s +/- 0.000s (x46.9) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 1.711s +/- 0.011s | 0.144s +/- 0.001s (x11.9) | 0.064s +/- 0.001s (x26.9) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.600s +/- 0.008s | 0.060s +/- 0.000s (x9.9) | 0.034s +/- 0.000s (x17.4) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 2.217s +/- 0.028s | 0.176s +/- 0.002s (x12.6) | 0.026s +/- 0.000s (x84.4) |

## Reduce

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 1.545s +/- 0.011s | 0.103s +/- 0.002s (x15.0) | 0.096s +/- 0.000s (x16.2) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 1.554s +/- 0.021s | 0.102s +/- 0.001s (x15.3) | 0.120s +/- 0.004s (x13.0) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.835s +/- 0.021s | 0.066s +/- 0.000s (x12.6) | 0.030s +/- 0.000s (x28.2) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.045s +/- 0.001s |

## hg_cpp - dynamic TSL

This section is tracked within hg_cpp and is not a cross-implementation comparison.

| workload | cycles | hg-cpp |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.038s +/- 0.000s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 6.036s +/- 0.002s | 0.440s +/- 0.002s (x13.7) | 0.213s +/- 0.003s (x28.4) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 51.762s +/- 0.390s | 3.847s +/- 0.024s (x13.5) | 1.125s +/- 0.005s (x46.0) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 1.122s +/- 0.005s | 0.159s +/- 0.001s (x7.1) | 0.026s +/- 0.000s (x42.6) |

## Services

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.357s +/- 0.005s | 0.031s +/- 0.000s (x11.4) | 0.017s +/- 0.000s (x21.3) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.478s +/- 0.007s | 0.037s +/- 0.000s (x13.1) | 0.022s +/- 0.001s (x22.0) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 1.494s +/- 0.008s | 0.143s +/- 0.003s (x10.4) | 0.054s +/- 0.001s (x27.6) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 1.269s +/- 0.011s | 0.123s +/- 0.002s (x10.3) | 0.060s +/- 0.001s (x21.3) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 1.055s +/- 0.010s | 0.117s +/- 0.002s (x9.0) | 0.035s +/- 0.000s (x30.3) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 1.906s +/- 0.021s | 0.216s +/- 0.002s (x8.8) | 0.066s +/- 0.001s (x29.1) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 2.373s +/- 0.018s | 0.262s +/- 0.003s (x9.1) | 0.098s +/- 0.001s (x24.1) |

## Adaptors

| workload | cycles | Python | legacy C++ | hg_cpp |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.258s +/- 0.001s | 0.034s +/- 0.000s (x7.7) | 0.017s +/- 0.000s (x15.5) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.252s +/- 0.002s | 0.031s +/- 0.001s (x8.2) | 0.018s +/- 0.000s (x14.1) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.918s +/- 0.010s | 0.076s +/- 0.000s (x12.1) | 0.057s +/- 0.001s (x16.0) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.696s +/- 0.006s | 0.058s +/- 0.000s (x11.9) | 0.064s +/- 0.000s (x10.9) |

## Failures

### tsd_dense_strkeys_std / upstream-py

```
sample 3: no result line (exit 1)
stdout:
stderr: Fatal Python error: Failed to import encodings module
Python runtime state: core initialized
Traceback (most recent call last):
  File "/usr/lib/python3.14/encodings/__init__.py", line 33, in <module>
  File "<frozen importlib._bootstrap>", line 1371, in _find_and_load
  File "<frozen importlib._bootstrap>", line 1333, in _find_and_load_unlocked
  File "<frozen importlib._bootstrap>", line 1267, in _find_spec
  File "<frozen importlib._bootstrap_external>", line 1292, in find_spec
  File "<frozen importlib._bootstrap_external>", line 1266, in _get_spec
  File "<frozen importlib._bootstrap_external>", line 1369, in find_spec
  File "<frozen importlib._bootstrap_external>", line 1412, in _fill_cache
ValueError: unsupported error handler

sample 4: no result line (exit 1)
stdout:
stderr: Fatal Python error: Failed to import encodings module
Python runtime state: core initialized
Traceback (most recent call last):
  File "/usr/lib/python3.14/encodings/__init__.py", line 33, in <module>
  File "<frozen importlib._bootstrap>", line 1371, in _find_and_load
  File "<frozen importlib._bootstrap>", line 1333, in _find_and_load_unlocked
  File "<frozen importlib._bootstrap>", line 1267, in _find_spec
  File "<frozen importlib._bootstrap_external>", line 1292, in find_spec
  File "<frozen importlib._bootstrap_external>", line 1266, in _get_spec
  File "<frozen importlib._bootstrap_external>", line 1369, in find_spec
  File "<frozen importlib._bootstrap_external>", line 1412, in _fill_cache
ValueError: unsupported error handler
```
