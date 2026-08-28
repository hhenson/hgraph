# hgraph performance matrix

- date: 2026-08-27T19:17:51+00:00
- host: Linux-7.0.0-30-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- fixed release baseline: hgraph 0.8.1 (published wheel)
- fixed release wheel: hgraph-0.8.1-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl
- fixed release SHA-256: c584116405c6b454220758764d3f3ad39055d2a3b43c4bd4b044fd70365025f0
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: hgraph 0.8.1 (`release`)
- reused fixed baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs hgraph 0.8.1.
C++-first-only sections are tracked without a 0.5 comparison.

## Graph construction

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Wide/deep graph - native operators (`construct_std`) | 1 | 0.063s +/- 0.000s |
| Wide/deep graph - Python nodes (`construct_py`) | 1 | 0.141s +/- 0.000s |

## Scheduler

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Feedback hot loop - native add (`tick_std`) | 100000 | 0.048s +/- 0.001s |
| Five-node Python compute chain (`tick_py`) | 20000 | 0.032s +/- 0.000s |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 20000 | 0.129s +/- 0.001s |
| Many branches joining one output (`scheduler_fan_in_std`) | 20000 | 0.178s +/- 0.001s |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 0.092s +/- 0.002s |

## Python boundary

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Python scalar generator to native sink (`python_generator_boundary`) | 20000 | 0.012s +/- 0.000s |
| Python scalar generator to Python sink (`python_sink_boundary`) | 20000 | 0.018s +/- 0.000s |
| Python compute with injected GlobalState (`python_global_state_boundary`) | 20000 | 0.029s +/- 0.000s |

## Value types

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Integer arithmetic (`type_int_std`) | 20000 | 0.020s +/- 0.000s |
| Floating-point arithmetic (`type_float_std`) | 20000 | 0.021s +/- 0.000s |
| String concatenation (`type_str_std`) | 20000 | 0.022s +/- 0.000s |
| CompoundScalar field access - native operators (`type_cs_std`) | 20000 | 0.025s +/- 0.001s |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 10000 | 0.022s +/- 0.000s |

## Python-owned structured scalars

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Whole-object pass-through - native layout (`python_owned_pass_through_native`) | 20000 | 0.015s +/- 0.000s |
| Whole-object pass-through - Python-owned (`python_owned_pass_through_python`) | 20000 | 0.012s +/- 0.000s |
| One field projection - native layout (`python_owned_project_one_native`) | 20000 | 0.018s +/- 0.000s |
| One field projection - Python-owned (`python_owned_project_one_python`) | 20000 | 0.018s +/- 0.000s |
| Three field projections - native layout (`python_owned_project_several_native`) | 20000 | 0.028s +/- 0.001s |
| Three field projections - Python-owned (`python_owned_project_several_python`) | 20000 | 0.030s +/- 0.000s |
| Construction from fields - native layout (`python_owned_construct_native`) | 20000 | 0.029s +/- 0.000s |
| Construction from fields - Python-owned (`python_owned_construct_python`) | 20000 | 0.049s +/- 0.001s |
| Equality and deduplication - native layout (`python_owned_dedup_native`) | 20000 | 0.019s +/- 0.000s |
| Equality and deduplication - Python-owned (`python_owned_dedup_python`) | 20000 | 0.018s +/- 0.000s |

## Value types

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 20000 | 0.048s +/- 0.000s |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 20000 | 0.016s +/- 0.000s |
| Set add/remove deltas (`tss_add_remove_std`) | 5000 | 0.077s +/- 0.001s |

## TSD - dense

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Map and reduce - native child graph (`tsd_dense_std`) | 1000 | 0.170s +/- 0.000s |
| Map and reduce - Python map child (`tsd_dense_py`) | 1000 | 0.192s +/- 0.002s |
| Source only (`tsd_dense_source_std`) | 1000 | 0.027s +/- 0.000s |
| Map only (`tsd_dense_map_std`) | 1000 | 0.125s +/- 0.002s |
| Reduce only (`tsd_dense_reduce_std`) | 1000 | 0.070s +/- 0.001s |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 0.184s +/- 0.000s |

## TSD - sparse

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 2000 | 0.056s +/- 0.000s |
| Source only (`tsd_sparse_source_std`) | 2000 | 0.008s +/- 0.000s |
| Map only (`tsd_sparse_map_std`) | 2000 | 0.041s +/- 0.001s |
| Reduce only (`tsd_sparse_reduce_std`) | 2000 | 0.022s +/- 0.000s |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | - | FAIL |

## TSD - key lifecycle

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Map and reduce with key replacement (`tsd_churn_std`) | 2000 | 0.068s +/- 0.001s |
| Python map with key replacement (`tsd_churn_py`) | 2000 | 0.058s +/- 0.000s |
| Key replacement - source only (`tsd_churn_source_std`) | 2000 | 0.006s +/- 0.000s |
| Key replacement - map only (`tsd_churn_map_std`) | 2000 | 0.056s +/- 0.000s |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 2000 | 0.016s +/- 0.000s |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1000 | 0.077s +/- 0.001s |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 200 | 0.743s +/- 0.006s |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 2000 | 0.039s +/- 0.001s |
| Two-input map with union membership (`tsd_two_input_union_std`) | 2000 | 0.062s +/- 0.001s |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 2000 | 0.034s +/- 0.000s |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 2000 | 0.027s +/- 0.000s |

## Reduce

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1000 | 0.091s +/- 0.002s |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1000 | 0.117s +/- 0.000s |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 10000 | 0.029s +/- 0.000s |
| Empty/singleton/two-value TSD without zero (`reduce_tsd_without_zero_std`) | 20000 | 0.043s +/- 0.000s |

## C++-first - dynamic TSL

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Sparse map and reduce over an unbounded list (`reduce_dynamic_tsl_std`) | 5000 | 0.034s +/- 0.000s |

## Nested graphs

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 20000 | 0.211s +/- 0.001s |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 2000 | 1.137s +/- 0.006s |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 500 | 0.026s +/- 0.000s |

## Services

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Reference service - native implementation (`service_reference_std`) | 20000 | 0.017s +/- 0.000s |
| Reference service - Python implementation (`service_reference_py`) | 20000 | 0.022s +/- 0.000s |
| Request/reply service - native implementation (`service_request_reply_std`) | 20000 | 0.051s +/- 0.001s |
| Request/reply service - Python implementation (`service_request_reply_py`) | 20000 | 0.058s +/- 0.001s |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 10000 | 0.033s +/- 0.001s |
| Subscription service - native implementation (`service_subscription_std`) | 20000 | 0.065s +/- 0.001s |
| Subscription service - Python implementation (`service_subscription_py`) | 20000 | 0.100s +/- 0.001s |

## Adaptors

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Duplex adaptor - native implementation (`adaptor_std`) | 20000 | 0.016s +/- 0.000s |
| Duplex adaptor - Python implementation (`adaptor_py`) | 20000 | 0.018s +/- 0.000s |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 20000 | 0.056s +/- 0.000s |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 20000 | 0.062s +/- 0.001s |

## Audited operators

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.1 |
|---|---|---|
| Scalar-collection convert and collect (`audit_convert_collect_std`) | 2000 | 0.065s +/- 0.001s |
| Scalar-map flip/keys/values (`audit_map_transform_std`) | 10000 | 0.068s +/- 0.001s |
| TSD getitem with a ticking key (`audit_tsd_getitem_std`) | 20000 | 0.064s +/- 0.000s |
| Tuple-scalar eq_/cmp_ fallback (`audit_eq_tuple_std`) | 20000 | 0.028s +/- 0.001s |
| Regex match_ and replace (`audit_string_match_std`) | 10000 | 0.205s +/- 0.001s |
| CompoundScalar to_json/from_json round trip (`audit_json_roundtrip_std`) | 10000 | 0.024s +/- 0.000s |
| race over if_-routed references (`audit_ref_race_std`) | 20000 | 0.043s +/- 0.000s |
| Buffered stream lag/gate (`audit_stream_buffered_std`) | 5000 | 0.041s +/- 0.000s |
| Stream take/drop/step counters (`audit_take_drop_std`) | 20000 | 0.023s +/- 0.000s |
| Running mean accumulator (`audit_running_mean_std`) | 50000 | 0.040s +/- 0.000s |

## Failures

### tsd_sparse_large_capacity_std / release

```
sample 3: no result line (exit 1)
stdout: 
stderr: Fatal Python error: Failed to import encodings module
Python runtime state: core initialized
Traceback (most recent call last):
  File "<frozen importlib._bootstrap>", line 1371, in _find_and_load
  File "<frozen importlib._bootstrap>", line 1333, in _find_and_load_unlocked
  File "<frozen importlib._bootstrap>", line 1267, in _find_spec
  File "<frozen importlib._bootstrap_external>", line 1292, in find_spec
  File "<frozen importlib._bootstrap_external>", line 1266, in _get_spec
  File "<frozen importlib._bootstrap_external>", line 1369, in find_spec
  File "<frozen importlib._bootstrap_external>", line 1412, in _fill_cache
ValueError: unsupported error handler
```

