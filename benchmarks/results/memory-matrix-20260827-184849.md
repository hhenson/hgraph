# hgraph memory-utilisation matrix

- date: 2026-08-27T18:48:49+00:00
- host: macOS-26.6.2-arm64-arm-64bit-Mach-O / arm
- CPU: Apple M4 Max
- Python: 3.14.7
- fixed release baseline: hgraph 0.8.19 (published wheel)
- fixed release wheel: hgraph-0.8.19-cp312-abi3-macosx_15_0_arm64.whl
- fixed release SHA-256: e7c4f19920a45ce9da0d4e4c479af2fd258e4f55b0f2de0215e5b105548629d1
- current-source revision: c9935bd35fd1+dirty
- current-source fingerprint: 5ceb81b004f4532ba81e59799b7aeac034a5f4bb0b01015f089c83b4567fbb6b
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: hgraph 0.8.19 (`release`), current source (`hg-cpp`)
- reused fixed baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
GraphDiagnostics columns are a separate C++-first run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| hgraph 0.8.19 (`release`) | 22.4 | 63.1 | 40.7 |
| current source (`hg-cpp`) | 22.3 | 63.2 | 40.8 |

## Static graph

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 2.0 +/- 0.1 | 2.1 +/- 0.1 | 1.07x | 2.0 +/- 0.1 | 2.1 +/- 0.1 | 42.5 | 23.0 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 3.7 +/- 0.0 | 3.7 +/- 0.0 | 1.00x | 3.7 +/- 0.0 | 3.7 +/- 0.0 | 172.8 | 93.7 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | 10.7 +/- 0.1 | 10.5 +/- 0.1 | 0.98x | 10.7 +/- 0.1 | 10.5 +/- 0.1 | 673.4 | 367.2 |

## Bounded execution

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 1.4 +/- 0.0 | 1.4 +/- 0.0 | 1.01x | 1.4 +/- 0.0 | 1.4 +/- 0.0 | 0.9 | 0.3 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 1.4 +/- 0.0 | 1.4 +/- 0.0 | 1.01x | 1.4 +/- 0.0 | 1.4 +/- 0.0 | 0.9 | 0.3 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 1.4 +/- 0.0 | 1.4 +/- 0.0 | 0.98x | 1.4 +/- 0.0 | 1.4 +/- 0.0 | 0.9 | 0.3 |
| Python compute chain - short (`tick_py__short`) | duration | 1.5 +/- 0.0 | 1.5 +/- 0.0 | 0.98x | 1.5 +/- 0.0 | 1.5 +/- 0.0 | 1.9 | 1.5 |
| Python compute chain - medium (`tick_py__medium`) | duration | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 0.99x | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 1.9 | 1.5 |
| Python compute chain - long (`tick_py__long`) | duration | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 1.01x | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 1.9 | 1.5 |

## Process lifetime

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | hgraph 0.8.19 first-to-last growth | current source first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 1.7 +/- 0.0 | 1.6 +/- 0.0 | 0.98x | 1.7 +/- 0.0 | 1.6 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.5 | 4.5 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 1.8 +/- 0.1 | 1.8 +/- 0.0 | 0.98x | 1.8 +/- 0.1 | 1.8 +/- 0.0 | 0.1 +/- 0.0 | 0.0 +/- 0.0 | 8.5 | 4.5 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 1.9 +/- 0.0 | 1.8 +/- 0.0 | 0.97x | 1.9 +/- 0.0 | 1.8 +/- 0.0 | 0.2 +/- 0.0 | 0.2 +/- 0.0 | 8.5 | 4.5 |
| Repeated novel graph programs - ten (`construct_std__novel_ten`) | distinct graph programs | 2.7 +/- 0.1 | 2.7 +/- 0.0 | 1.02x | 2.7 +/- 0.1 | 2.7 +/- 0.0 | 1.0 +/- 0.1 | 1.1 +/- 0.0 | 8.5 | 4.5 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 2.3 +/- 0.0 | 2.3 +/- 0.0 | 1.01x | 2.3 +/- 0.0 | 2.3 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 5.7 | 12.7 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 2.3 +/- 0.0 | 2.4 +/- 0.0 | 1.01x | 2.3 +/- 0.0 | 2.4 +/- 0.0 | 0.1 +/- 0.0 | 0.0 +/- 0.0 | 5.7 | 12.7 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 2.4 +/- 0.0 | 2.5 +/- 0.0 | 1.01x | 2.4 +/- 0.0 | 2.5 +/- 0.0 | 0.1 +/- 0.0 | 0.1 +/- 0.0 | 5.7 | 12.7 |

## Value storage

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 1.6 +/- 0.0 | 1.5 +/- 0.0 | 0.97x | 1.6 +/- 0.0 | 1.5 +/- 0.0 | 1.4 | 0.7 |
| String arithmetic - long (`type_str_std__long`) | duration | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 1.00x | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 1.4 | 0.7 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 1.7 +/- 0.0 | 1.5 +/- 0.0 | 0.93x | 1.7 +/- 0.0 | 1.5 +/- 0.0 | 1.3 | 0.8 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 1.7 +/- 0.0 | 1.6 +/- 0.0 | 0.97x | 1.7 +/- 0.0 | 1.6 +/- 0.0 | 1.3 | 0.8 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 0.99x | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 1.3 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 1.00x | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 1.3 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 0.98x | 1.6 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 1.3 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 1.7 +/- 0.0 | 1.7 +/- 0.0 | 1.02x | 1.7 +/- 0.0 | 1.7 +/- 0.0 | 0.7 | 6.4 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 1.7 +/- 0.0 | 1.7 +/- 0.0 | 0.99x | 1.7 +/- 0.0 | 1.7 +/- 0.0 | 0.7 | 23.1 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 1.8 +/- 0.0 | 1.8 +/- 0.0 | 1.01x | 1.8 +/- 0.0 | 1.8 +/- 0.0 | 0.7 | 89.6 |

## Keyed collections

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 2.2 +/- 0.0 | 2.1 +/- 0.0 | 0.97x | 2.2 +/- 0.0 | 2.1 +/- 0.0 | 1.8 | 45.2 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 2.4 +/- 0.1 | 2.3 +/- 0.0 | 0.97x | 2.4 +/- 0.1 | 2.3 +/- 0.0 | 1.8 | 201.1 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 2.6 +/- 0.1 | 2.5 +/- 0.0 | 0.97x | 2.6 +/- 0.1 | 2.5 +/- 0.0 | 1.8 | 400.1 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 5.4 +/- 0.1 | 5.5 +/- 0.0 | 1.01x | 5.4 +/- 0.1 | 5.5 +/- 0.0 | 1.8 | 1864.3 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 19.0 +/- 0.0 | 19.0 +/- 0.1 | 1.00x | 19.0 +/- 0.0 | 19.0 +/- 0.1 | 1.8 | 10893.5 |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 69.5 +/- 0.1 | 69.5 +/- 0.1 | 1.00x | 61.3 +/- 0.1 | 61.3 +/- 0.1 | 1.8 | 43565.4 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 2.6 +/- 0.1 | 2.6 +/- 0.0 | 0.99x | 2.6 +/- 0.1 | 2.6 +/- 0.0 | 1.8 | 619.7 |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 2.6 +/- 0.0 | 2.7 +/- 0.0 | 1.03x | 2.6 +/- 0.0 | 2.7 +/- 0.0 | 1.8 | 619.7 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 2.7 +/- 0.0 | 2.6 +/- 0.0 | 0.97x | 2.7 +/- 0.0 | 2.6 +/- 0.0 | 1.8 | 619.7 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 3.2 +/- 0.0 | 3.3 +/- 0.0 | 1.03x | 3.2 +/- 0.0 | 3.3 +/- 0.0 | 1.8 | 1031.2 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 8.9 +/- 0.0 | 9.0 +/- 0.0 | 1.00x | 8.9 +/- 0.0 | 9.0 +/- 0.0 | 1.8 | 4203.7 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 16.1 +/- 0.0 | 16.2 +/- 0.0 | 1.01x | 16.1 +/- 0.0 | 16.2 +/- 0.0 | 1.8 | 8404.0 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | 5.6 +/- 0.0 | 5.6 +/- 0.1 | 1.01x | 5.6 +/- 0.0 | 5.6 +/- 0.1 | 1.8 | 1884.3 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 5.6 +/- 0.1 | 5.6 +/- 0.1 | 1.01x | 5.6 +/- 0.1 | 5.6 +/- 0.1 | 1.8 | 1884.3 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 5.7 +/- 0.0 | 5.5 +/- 0.0 | 0.97x | 5.7 +/- 0.0 | 5.5 +/- 0.0 | 1.8 | 1884.3 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 2.5 +/- 0.0 | 2.5 +/- 0.0 | 1.01x | 2.5 +/- 0.0 | 2.5 +/- 0.0 | 1.8 | 400.2 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 2.5 +/- 0.0 | 2.6 +/- 0.2 | 1.05x | 2.5 +/- 0.0 | 2.6 +/- 0.2 | 1.8 | 400.2 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 2.6 +/- 0.0 | 2.5 +/- 0.0 | 0.97x | 2.6 +/- 0.0 | 2.5 +/- 0.0 | 1.8 | 400.2 |

## Nested graphs

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 2.0 +/- 0.0 | 1.9 +/- 0.1 | 0.98x | 2.0 +/- 0.0 | 1.9 +/- 0.1 | 1.4 | 18.6 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 2.0 +/- 0.0 | 2.0 +/- 0.1 | 0.97x | 2.0 +/- 0.0 | 2.0 +/- 0.1 | 1.4 | 75.6 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 2.0 +/- 0.0 | 2.1 +/- 0.0 | 1.03x | 2.0 +/- 0.0 | 2.1 +/- 0.0 | 1.4 | 150.2 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 2.5 +/- 0.0 | 2.5 +/- 0.0 | 0.99x | 2.5 +/- 0.0 | 2.5 +/- 0.0 | 3.1 | 157.7 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 3.1 +/- 0.0 | 3.1 +/- 0.0 | 1.01x | 3.1 +/- 0.0 | 3.1 +/- 0.0 | 3.1 | 622.2 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 3.8 +/- 0.0 | 3.8 +/- 0.0 | 0.99x | 3.8 +/- 0.0 | 3.8 +/- 0.0 | 3.1 | 1241.4 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | 2.7 +/- 0.0 | 2.8 +/- 0.1 | 1.04x | 2.7 +/- 0.0 | 2.8 +/- 0.1 | 2.0 | 147.6 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | 2.9 +/- 0.0 | 2.8 +/- 0.0 | 0.98x | 2.9 +/- 0.0 | 2.8 +/- 0.0 | 2.0 | 517.9 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 3.5 +/- 0.1 | 3.6 +/- 0.0 | 1.02x | 3.5 +/- 0.1 | 3.6 +/- 0.0 | 2.0 | 1022.7 |

## Services

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 2.2 +/- 0.0 | 2.3 +/- 0.0 | 1.01x | 2.2 +/- 0.0 | 2.3 +/- 0.0 | 2.4 | 9.4 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 2.4 +/- 0.0 | 2.3 +/- 0.0 | 0.95x | 2.4 +/- 0.0 | 2.3 +/- 0.0 | 5.7 | 12.7 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 2.5 +/- 0.0 | 2.5 +/- 0.0 | 1.03x | 2.5 +/- 0.0 | 2.5 +/- 0.0 | 18.7 | 32.1 |

## C++-first dynamic storage

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | 2.1 +/- 0.0 | 2.1 +/- 0.0 | 0.99x | 2.1 +/- 0.0 | 2.1 +/- 0.0 | 1.6 | 62.3 |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | 2.4 +/- 0.0 | 2.2 +/- 0.0 | 0.94x | 2.4 +/- 0.0 | 2.2 +/- 0.0 | 1.6 | 246.2 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | 3.6 +/- 0.0 | 3.6 +/- 0.0 | 1.00x | 3.6 +/- 0.0 | 3.6 +/- 0.0 | 1.6 | 982.0 |

## current source retained runtime registry growth

Counts are final-minus-pre-run cold-path cardinalities. They are process-lifetime structural records, not live graph instances.

| profile | node types | graph programs | graph types | executor types | all type records |
|---|---:|---:|---:|---:|---:|
| `construct_std__small` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `construct_std__medium` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `construct_std__large` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `tick_std__short` | 5.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 26.0 +/- 0.0 |
| `tick_std__medium` | 5.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 26.0 +/- 0.0 |
| `tick_std__long` | 5.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 26.0 +/- 0.0 |
| `tick_py__short` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 31.0 +/- 0.0 |
| `tick_py__medium` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 31.0 +/- 0.0 |
| `tick_py__long` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 31.0 +/- 0.0 |
| `construct_std__repeat_once` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `construct_std__repeat_ten` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `construct_std__repeat_hundred` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `construct_std__novel_ten` | 4.0 +/- 0.0 | 10.0 +/- 0.0 | 20.0 +/- 0.0 | 1.0 +/- 0.0 | 43.0 +/- 0.0 |
| `service_adaptor_py__repeat_once` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 114.0 +/- 0.0 |
| `service_adaptor_py__repeat_ten` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 114.0 +/- 0.0 |
| `service_adaptor_py__repeat_fifty` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 114.0 +/- 0.0 |
| `type_str_std__short` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `type_str_std__long` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `type_cs_py__short` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 42.0 +/- 0.0 |
| `type_cs_py__long` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 42.0 +/- 0.0 |
| `type_tsw_append_evict_std__short` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `type_tsw_append_evict_std__medium` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `type_tsw_append_evict_std__long` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `tss_add_remove_std__small` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 33.0 +/- 0.0 |
| `tss_add_remove_std__medium` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 33.0 +/- 0.0 |
| `tss_add_remove_std__large` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 33.0 +/- 0.0 |
| `tsd_dense_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_dense_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_dense_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_sparse_large_capacity_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_sparse_large_capacity_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_sparse_large_capacity_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_churn_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_churn_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_churn_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_capacity_growth_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_capacity_growth_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_capacity_growth_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_clear_repopulate_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_clear_repopulate_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_clear_repopulate_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_key_reactivation_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_key_reactivation_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `tsd_key_reactivation_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__small` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 58.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__medium` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 58.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__large` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 58.0 +/- 0.0 |
| `switch_keyed_collection_std__small` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 100.0 +/- 0.0 |
| `switch_keyed_collection_std__medium` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 100.0 +/- 0.0 |
| `switch_keyed_collection_std__large` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 100.0 +/- 0.0 |
| `mesh_std__small` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 109.0 +/- 0.0 |
| `mesh_std__medium` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 109.0 +/- 0.0 |
| `mesh_std__large` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 109.0 +/- 0.0 |
| `service_adaptor_py__small` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 114.0 +/- 0.0 |
| `service_adaptor_py__medium` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 114.0 +/- 0.0 |
| `service_adaptor_py__large` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 114.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 52.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 52.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 52.0 +/- 0.0 |

## Interpretation contract

- `construct_std__small`: peak should scale with wired graph size; post-GC growth should remain bounded.
- `construct_std__medium`: peak should scale with wired graph size; post-GC growth should remain bounded.
- `construct_std__large`: peak should scale with wired graph size; post-GC growth should remain bounded.
- `tick_std__short`: peak and retained memory should be approximately duration independent.
- `tick_std__medium`: peak and retained memory should be approximately duration independent.
- `tick_std__long`: peak and retained memory should be approximately duration independent.
- `tick_py__short`: peak and retained memory should be approximately duration independent.
- `tick_py__medium`: peak and retained memory should be approximately duration independent.
- `tick_py__long`: peak and retained memory should be approximately duration independent.
- `construct_std__repeat_once`: post-GC memory should reach a warm plateau rather than grow proportionally with repeated graph lifecycles.
- `construct_std__repeat_ten`: post-GC memory should reach a warm plateau rather than grow proportionally with repeated graph lifecycles.
- `construct_std__repeat_hundred`: post-GC memory should reach a warm plateau rather than grow proportionally with repeated graph lifecycles.
- `construct_std__novel_ten`: registry cardinality should grow for intentionally different graph programs, distinguishing legitimate retention from identical rewiring.
- `service_adaptor_py__repeat_once`: service and adaptor registration should deduplicate or be released rather than retain one graph's state per execution.
- `service_adaptor_py__repeat_ten`: service and adaptor registration should deduplicate or be released rather than retain one graph's state per execution.
- `service_adaptor_py__repeat_fifty`: service and adaptor registration should deduplicate or be released rather than retain one graph's state per execution.
- `type_str_std__short`: temporary value memory should remain bounded across cycles.
- `type_str_std__long`: temporary value memory should remain bounded across cycles.
- `type_cs_py__short`: bridge storage should remain bounded across cycles.
- `type_cs_py__long`: bridge storage should remain bounded across cycles.
- `type_tsw_append_evict_std__short`: the 64-element window should remain bounded as evictions continue.
- `type_tsw_append_evict_std__medium`: the 64-element window should remain bounded as evictions continue.
- `type_tsw_append_evict_std__long`: the 64-element window should remain bounded as evictions continue.
- `tss_add_remove_std__small`: peak should scale with live set cardinality; duration is fixed.
- `tss_add_remove_std__medium`: peak should scale with live set cardinality; duration is fixed.
- `tss_add_remove_std__large`: peak should scale with live set cardinality; duration is fixed.
- `tsd_dense_std__small`: peak native storage should scale with simultaneously live keys.
- `tsd_dense_std__medium`: peak native storage should scale with simultaneously live keys.
- `tsd_dense_std__large`: peak native storage should scale with simultaneously live keys.
- `tsd_sparse_large_capacity_std__small`: peak should scale with retained key and child-slot capacity.
- `tsd_sparse_large_capacity_std__medium`: peak should scale with retained key and child-slot capacity.
- `tsd_sparse_large_capacity_std__large`: peak should scale with retained key and child-slot capacity.
- `tsd_churn_std__short`: live keys are bounded; slot reuse should prevent cycle-proportional growth.
- `tsd_churn_std__medium`: live keys are bounded; slot reuse should prevent cycle-proportional growth.
- `tsd_churn_std__long`: live keys are bounded; slot reuse should prevent cycle-proportional growth.
- `tsd_capacity_growth_std__short`: memory should grow with the intentionally increasing key population.
- `tsd_capacity_growth_std__medium`: memory should grow with the intentionally increasing key population.
- `tsd_capacity_growth_std__long`: memory should grow with the intentionally increasing key population.
- `tsd_clear_repopulate_std__short`: capacity may be retained, but repeated clear/repopulate must remain bounded.
- `tsd_clear_repopulate_std__medium`: capacity may be retained, but repeated clear/repopulate must remain bounded.
- `tsd_clear_repopulate_std__long`: capacity may be retained, but repeated clear/repopulate must remain bounded.
- `tsd_key_reactivation_std__short`: reused identities should not create cycle-proportional storage growth.
- `tsd_key_reactivation_std__medium`: reused identities should not create cycle-proportional storage growth.
- `tsd_key_reactivation_std__long`: reused identities should not create cycle-proportional storage growth.
- `reduce_tsd_nested_graph_std__small`: reducer banks should scale with input cardinality.
- `reduce_tsd_nested_graph_std__medium`: reducer banks should scale with input cardinality.
- `reduce_tsd_nested_graph_std__large`: reducer banks should scale with input cardinality.
- `switch_keyed_collection_std__small`: active and retained branch storage should scale with live keys.
- `switch_keyed_collection_std__medium`: active and retained branch storage should scale with live keys.
- `switch_keyed_collection_std__large`: active and retained branch storage should scale with live keys.
- `mesh_std__small`: mesh instance and dependency storage should scale with live keys.
- `mesh_std__medium`: mesh instance and dependency storage should scale with live keys.
- `mesh_std__large`: mesh instance and dependency storage should scale with live keys.
- `service_adaptor_py__small`: graph memory should scale with independently wired clients.
- `service_adaptor_py__medium`: graph memory should scale with independently wired clients.
- `service_adaptor_py__large`: graph memory should scale with independently wired clients.
- `reduce_dynamic_tsl_std__small`: native slot storage should scale with list capacity.
- `reduce_dynamic_tsl_std__medium`: native slot storage should scale with list capacity.
- `reduce_dynamic_tsl_std__large`: native slot storage should scale with list capacity.
