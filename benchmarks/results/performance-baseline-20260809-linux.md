# hgraph performance matrix

- date: 2026-08-09T17:09:32+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- reference baseline: hgraph 0.5.41 (published wheel)
- reference wheel: hgraph-0.5.41-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl
- reference SHA-256: c24da699910c3eb44019a38a0fb293557ec707b48a8e8ab5b3e5fd8b0be2db7d
- fixed release baseline: hgraph 0.8.1 (published wheel)
- fixed release wheel: hgraph-0.8.1-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl
- fixed release SHA-256: c584116405c6b454220758764d3f3ad39055d2a3b43c4bd4b044fd70365025f0
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
| Wide/deep graph - native operators (`construct_std`) | 1 | 1.357s +/- 0.002s | 1.318s +/- 0.009s (x1.0) | 0.063s +/- 0.000s (x21.7) |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.260s +/- 0.003s | 0.227s +/- 0.000s (x1.1) | 0.136s +/- 0.001s (x1.9) |

## Scheduler

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 1.490s +/- 0.004s | 0.094s +/- 0.001s (x15.9) | 0.047s +/- 0.001s (x31.6) |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.575s +/- 0.005s | 0.040s +/- 0.000s (x14.3) | 0.031s +/- 0.001s (x18.3) |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 4.711s +/- 0.021s | 0.245s +/- 0.003s (x19.2) | 0.129s +/- 0.000s (x36.5) |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 6.067s +/- 0.054s | 0.327s +/- 0.002s (x18.6) | 0.175s +/- 0.001s (x34.7) |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | FAIL | 0.105s +/- 0.001s | 0.093s +/- 0.000s |

## Python boundary

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.146s +/- 0.002s | 0.023s +/- 0.000s (x6.4) | 0.012s +/- 0.000s (x12.0) |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.145s +/- 0.002s | 0.023s +/- 0.000s (x6.3) | 0.018s +/- 0.000s (x8.1) |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | N/A | N/A | 0.029s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.451s +/- 0.006s | 0.042s +/- 0.001s (x10.8) | 0.021s +/- 0.000s (x21.8) |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.448s +/- 0.005s | 0.041s +/- 0.000s (x10.8) | 0.020s +/- 0.000s (x22.0) |
| String concatenation (`type_str_std`) | 20000 | 0.433s +/- 0.008s | 0.044s +/- 0.001s (x9.8) | 0.021s +/- 0.000s (x20.4) |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.421s +/- 0.009s | 0.041s +/- 0.000s (x10.2) | 0.024s +/- 0.001s (x17.2) |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.226s +/- 0.001s | 0.034s +/- 0.000s (x6.7) | 0.022s +/- 0.000s (x10.1) |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | N/A | N/A | 0.014s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | N/A | N/A | 0.012s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | N/A | N/A | 0.019s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | N/A | N/A | 0.018s +/- 0.001s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | N/A | N/A | 0.028s +/- 0.001s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | N/A | N/A | 0.030s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | N/A | N/A | 0.029s +/- 0.001s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | N/A | N/A | 0.049s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | N/A | N/A | 0.019s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | N/A | N/A | 0.018s +/- 0.000s |

## Value types

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 1.089s +/- 0.015s | 0.092s +/- 0.002s (x11.9) | 0.047s +/- 0.000s (x23.3) |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.265s +/- 0.003s | 0.030s +/- 0.000s (x9.0) | 0.016s +/- 0.000s (x17.1) |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.118s +/- 0.001s | 0.083s +/- 0.001s (x1.4) | 0.074s +/- 0.000s (x1.6) |

## TSD - dense

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 3.917s +/- 0.041s | 0.259s +/- 0.003s (x15.1) | 0.164s +/- 0.001s (x23.9) |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 2.837s +/- 0.036s | 0.181s +/- 0.003s (x15.6) | 0.184s +/- 0.006s (x15.4) |
| Source only (`tsd_dense_source_std`) | 1000 | 0.357s +/- 0.004s | 0.047s +/- 0.001s (x7.5) | 0.027s +/- 0.000s (x13.3) |
| Map only (`tsd_dense_map_std`) | 1000 | 2.869s +/- 0.020s | 0.197s +/- 0.001s (x14.5) | 0.121s +/- 0.001s (x23.8) |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 1.539s +/- 0.014s | 0.102s +/- 0.001s (x15.1) | 0.070s +/- 0.001s (x22.1) |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 3.949s +/- 0.024s | 0.299s +/- 0.001s (x13.2) | 0.177s +/- 0.002s (x22.3) |

## TSD - sparse

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 1.646s +/- 0.014s | 0.142s +/- 0.001s (x11.6) | 0.055s +/- 0.001s (x29.9) |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.064s +/- 0.001s | 0.021s +/- 0.000s (x3.1) | 0.008s +/- 0.000s (x8.0) |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.635s +/- 0.003s | 0.082s +/- 0.001s (x7.8) | 0.041s +/- 0.000s (x15.5) |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 1.049s +/- 0.010s | 0.072s +/- 0.000s (x14.6) | 0.022s +/- 0.000s (x47.4) |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 5000 | 34.141s +/- 0.365s | 3.404s +/- 0.027s (x10.0) | 1.875s +/- 0.007s (x18.2) |

## TSD - key lifecycle

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 3.770s +/- 0.012s | 0.266s +/- 0.001s (x14.2) | 0.066s +/- 0.000s (x57.3) |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 2.986s +/- 0.026s | 0.192s +/- 0.006s (x15.5) | 0.058s +/- 0.000s (x51.1) |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.160s +/- 0.003s | 0.028s +/- 0.000s (x5.6) | 0.006s +/- 0.000s (x27.7) |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 2.682s +/- 0.022s | 0.200s +/- 0.001s (x13.4) | 0.055s +/- 0.000s (x48.7) |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 1.190s +/- 0.005s | 0.089s +/- 0.001s (x13.3) | 0.016s +/- 0.000s (x75.1) |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 2.055s +/- 0.004s | 0.225s +/- 0.003s (x9.1) | 0.077s +/- 0.001s (x26.8) |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 39.239s +/- 0.481s | 3.898s +/- 0.019s (x10.1) | 0.723s +/- 0.004s (x54.3) |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 1.790s +/- 0.016s | 0.142s +/- 0.001s (x12.6) | 0.039s +/- 0.001s (x45.4) |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 1.751s +/- 0.020s | 0.146s +/- 0.002s (x12.0) | 0.062s +/- 0.001s (x28.0) |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.613s +/- 0.004s | 0.061s +/- 0.001s (x10.0) | 0.035s +/- 0.000s (x17.7) |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 2.161s +/- 0.013s | 0.172s +/- 0.001s (x12.6) | 0.027s +/- 0.000s (x81.4) |

## Reduce

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 1.521s +/- 0.008s | 0.105s +/- 0.002s (x14.5) | 0.088s +/- 0.001s (x17.3) |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 1.554s +/- 0.030s | 0.104s +/- 0.001s (x14.9) | 0.117s +/- 0.002s (x13.3) |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.790s +/- 0.009s | 0.068s +/- 0.002s (x11.7) | 0.029s +/- 0.000s (x27.0) |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | N/A | N/A | 0.043s +/- 0.001s |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | N/A | N/A | 0.034s +/- 0.000s |

## Nested graphs

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 6.023s +/- 0.008s | 0.442s +/- 0.000s (x13.6) | 0.213s +/- 0.001s (x28.3) |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 51.675s +/- 0.078s | 3.814s +/- 0.012s (x13.5) | 1.144s +/- 0.005s (x45.2) |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 1.109s +/- 0.003s | 0.160s +/- 0.001s (x6.9) | 0.027s +/- 0.000s (x41.7) |

## Services

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.366s +/- 0.004s | 0.032s +/- 0.000s (x11.5) | 0.017s +/- 0.000s (x21.9) |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.476s +/- 0.005s | 0.036s +/- 0.000s (x13.4) | 0.022s +/- 0.000s (x21.6) |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 1.510s +/- 0.005s | 0.136s +/- 0.002s (x11.1) | 0.051s +/- 0.001s (x29.7) |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 1.281s +/- 0.014s | 0.123s +/- 0.004s (x10.4) | 0.058s +/- 0.001s (x22.3) |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 1.021s +/- 0.014s | 0.117s +/- 0.001s (x8.7) | 0.034s +/- 0.001s (x30.2) |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 1.840s +/- 0.019s | 0.215s +/- 0.004s (x8.6) | 0.064s +/- 0.001s (x28.7) |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 2.333s +/- 0.034s | 0.267s +/- 0.004s (x8.7) | 0.099s +/- 0.000s (x23.5) |

## Adaptors

| workload | cycles | Python | legacy C++ | hgraph 0.8.1 |
|---|---|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.258s +/- 0.004s | 0.033s +/- 0.000s (x7.9) | 0.016s +/- 0.000s (x16.2) |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.247s +/- 0.001s | 0.030s +/- 0.000s (x8.2) | 0.018s +/- 0.000s (x13.8) |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.900s +/- 0.024s | 0.073s +/- 0.001s (x12.4) | 0.055s +/- 0.000s (x16.4) |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.694s +/- 0.001s | 0.057s +/- 0.001s (x12.1) | 0.063s +/- 0.001s (x11.1) |

## Failures

### scheduler_conflated_fixed_tsl_std / upstream-py

```
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
