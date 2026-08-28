# hgraph memory-utilisation matrix

- date: 2026-08-27T19:00:38+00:00
- host: Linux-7.0.0-30-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- fixed release baseline: hgraph 0.8.19 (published wheel)
- fixed release wheel: hgraph-0.8.19-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl
- fixed release SHA-256: 3c58610039211b0a9965727c4a940da64664188d20ebad6711a02bc700670637
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
| hgraph 0.8.19 (`release`) | 17.7 | 63.5 | 45.8 |
| current source (`hg-cpp`) | 17.7 | 70.1 | 52.4 |

## Static graph

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 2.5 +/- 0.0 | 1.4 +/- 0.0 | 0.54x | 2.4 +/- 0.0 | 1.2 +/- 0.0 | 44.1 | 23.0 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 4.1 +/- 0.0 | 3.0 +/- 0.0 | 0.72x | 4.0 +/- 0.0 | 2.8 +/- 0.0 | 179.0 | 93.7 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | N/A | 8.8 +/- 0.0 | N/A | N/A | 7.6 +/- 0.0 | 697.6 | 367.2 |

## Bounded execution

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 2.1 +/- 0.0 | 1.0 +/- 0.0 | 0.46x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 0.9 | 0.3 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.45x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 0.9 | 0.3 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.45x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 0.9 | 0.3 |
| Python compute chain - short (`tick_py__short`) | duration | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.45x | 1.9 +/- 0.0 | 0.8 +/- 0.0 | 2.2 | 1.5 |
| Python compute chain - medium (`tick_py__medium`) | duration | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.45x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 2.2 | 1.5 |
| Python compute chain - long (`tick_py__long`) | duration | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.45x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 2.2 | 1.5 |

## Process lifetime

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | hgraph 0.8.19 first-to-last growth | current source first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.46x | 1.9 +/- 0.0 | 0.8 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.9 | 4.5 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 2.1 +/- 0.0 | 1.0 +/- 0.0 | 0.45x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.9 | 4.5 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 2.2 +/- 0.0 | 1.0 +/- 0.0 | 0.47x | 2.0 +/- 0.0 | 0.9 +/- 0.0 | 0.1 +/- 0.0 | 0.1 +/- 0.0 | 8.9 | 4.5 |
| Repeated novel graph programs - ten (`construct_std__novel_ten`) | distinct graph programs | 3.0 +/- 0.0 | 1.9 +/- 0.0 | 0.62x | 2.9 +/- 0.0 | 1.8 +/- 0.0 | 0.9 +/- 0.0 | 0.9 +/- 0.0 | 8.9 | 4.5 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 2.7 +/- 0.0 | 1.0 +/- 0.0 | 0.37x | 2.6 +/- 0.0 | 0.9 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 6.0 | 13.1 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 2.7 +/- 0.0 | 1.1 +/- 0.0 | 0.39x | 2.6 +/- 0.0 | 1.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 6.0 | 13.1 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 2.8 +/- 0.0 | 1.1 +/- 0.0 | 0.39x | 2.7 +/- 0.0 | 1.0 +/- 0.0 | 0.1 +/- 0.0 | 0.1 +/- 0.0 | 6.0 | 13.1 |

## Value storage

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 2.1 +/- 0.0 | 1.0 +/- 0.0 | 0.46x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 1.5 | 0.7 |
| String arithmetic - long (`type_str_std__long`) | duration | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.45x | 2.0 +/- 0.0 | 0.8 +/- 0.0 | 1.5 | 0.7 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 2.2 +/- 0.0 | 0.9 +/- 0.0 | 0.43x | 2.1 +/- 0.0 | 0.8 +/- 0.0 | 1.5 | 0.8 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 2.2 +/- 0.0 | 0.9 +/- 0.0 | 0.43x | 2.1 +/- 0.0 | 0.8 +/- 0.0 | 1.5 | 0.8 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 2.3 +/- 0.0 | 0.9 +/- 0.0 | 0.40x | 2.2 +/- 0.0 | 0.8 +/- 0.0 | 0.8 | 1.3 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 2.3 +/- 0.0 | 0.9 +/- 0.0 | 0.40x | 2.2 +/- 0.0 | 0.8 +/- 0.0 | 0.8 | 1.3 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 2.3 +/- 0.0 | 0.9 +/- 0.0 | 0.41x | 2.2 +/- 0.0 | 0.8 +/- 0.0 | 0.8 | 1.3 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 2.2 +/- 0.0 | 1.0 +/- 0.0 | 0.43x | 2.1 +/- 0.0 | 0.8 +/- 0.0 | 0.8 | 6.4 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 2.2 +/- 0.0 | 1.0 +/- 0.0 | 0.43x | 2.1 +/- 0.0 | 0.9 +/- 0.0 | 0.8 | 23.1 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 2.4 +/- 0.0 | 1.2 +/- 0.0 | 0.48x | 2.3 +/- 0.0 | 1.0 +/- 0.0 | 0.8 | 89.6 |

## Keyed collections

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 2.4 +/- 0.0 | 1.0 +/- 0.0 | 0.43x | 2.3 +/- 0.0 | 0.9 +/- 0.0 | 1.9 | 46.0 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 2.7 +/- 0.0 | 1.4 +/- 0.0 | 0.49x | 2.6 +/- 0.0 | 1.2 +/- 0.0 | 1.9 | 205.2 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 3.0 +/- 0.0 | 1.5 +/- 0.0 | 0.51x | 2.8 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 408.4 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 4.3 +/- 0.0 | 3.0 +/- 0.1 | 0.69x | 4.2 +/- 0.0 | 2.9 +/- 0.0 | 1.9 | 1903.6 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 12.2 +/- 0.0 | 10.8 +/- 0.0 | 0.89x | 12.1 +/- 0.0 | 10.7 +/- 0.0 | N/A | N/A |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 70.9 +/- 0.0 | 69.6 +/- 0.0 | 0.98x | 39.9 +/- 0.0 | 38.5 +/- 0.0 | 1.9 | 44446.4 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 2.9 +/- 0.0 | 1.5 +/- 0.0 | 0.52x | 2.8 +/- 0.0 | 1.4 +/- 0.0 | N/A | N/A |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 2.9 +/- 0.0 | 1.5 +/- 0.0 | 0.52x | 2.8 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 634.2 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 2.9 +/- 0.0 | 1.5 +/- 0.0 | 0.51x | 2.8 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 634.2 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 3.7 +/- 0.0 | 2.3 +/- 0.0 | 0.62x | 3.6 +/- 0.0 | 2.2 +/- 0.0 | 1.9 | 1053.1 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 6.8 +/- 0.0 | 5.4 +/- 0.0 | 0.79x | 6.6 +/- 0.0 | 5.2 +/- 0.0 | 1.9 | 4291.7 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 10.5 +/- 0.0 | 9.1 +/- 0.0 | 0.87x | 10.4 +/- 0.0 | 9.0 +/- 0.0 | 1.9 | 8580.0 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | N/A | 4.6 +/- 0.0 | N/A | N/A | 3.2 +/- 0.0 | 1.9 | 1923.5 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 6.1 +/- 0.0 | 4.7 +/- 0.0 | 0.77x | 4.6 +/- 0.0 | 3.2 +/- 0.0 | 1.9 | 1923.5 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 6.1 +/- 0.0 | 4.7 +/- 0.0 | 0.77x | 4.6 +/- 0.0 | 3.2 +/- 0.0 | 1.9 | 1923.5 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 3.0 +/- 0.1 | 1.5 +/- 0.0 | 0.49x | 2.8 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 408.5 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 2.8 +/- 0.0 | 1.5 +/- 0.0 | 0.53x | 2.7 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 408.5 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 2.9 +/- 0.0 | 1.6 +/- 0.0 | 0.54x | 2.8 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 408.5 |

## Nested graphs

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 2.3 +/- 0.0 | 1.0 +/- 0.0 | 0.41x | 2.2 +/- 0.0 | 0.8 +/- 0.0 | 1.4 | 18.9 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 2.4 +/- 0.0 | 1.0 +/- 0.0 | 0.42x | 2.3 +/- 0.0 | 0.9 +/- 0.0 | 1.4 | 76.6 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 2.5 +/- 0.0 | 1.1 +/- 0.0 | 0.45x | 2.4 +/- 0.0 | 1.0 +/- 0.0 | 1.4 | 152.2 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 2.7 +/- 0.0 | 1.3 +/- 0.0 | 0.46x | 2.6 +/- 0.0 | 1.1 +/- 0.0 | 3.3 | 161.3 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 3.3 +/- 0.0 | 1.8 +/- 0.0 | 0.54x | 3.2 +/- 0.0 | 1.7 +/- 0.0 | 3.3 | 636.8 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 3.9 +/- 0.0 | 2.3 +/- 0.0 | 0.61x | 3.7 +/- 0.0 | 2.2 +/- 0.0 | 3.3 | 1270.4 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | N/A | 1.3 +/- 0.0 | N/A | N/A | 1.1 +/- 0.0 | 2.1 | 151.4 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | N/A | 1.7 +/- 0.0 | N/A | N/A | 1.5 +/- 0.0 | 2.1 | 532.5 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 3.5 +/- 0.0 | 1.8 +/- 0.0 | 0.51x | 3.4 +/- 0.0 | 1.7 +/- 0.0 | 2.1 | 1051.8 |

## Services

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 2.6 +/- 0.0 | 1.0 +/- 0.0 | 0.38x | 2.5 +/- 0.0 | 0.9 +/- 0.0 | 2.6 | 9.8 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 2.7 +/- 0.0 | 1.0 +/- 0.0 | 0.38x | 2.6 +/- 0.0 | 0.9 +/- 0.0 | 6.0 | 13.1 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 2.9 +/- 0.0 | N/A | N/A | 2.8 +/- 0.0 | N/A | 19.5 | 33.0 |

## C++-first dynamic storage

| profile | axis | hgraph 0.8.19 peak delta | current source peak delta | current source/hgraph 0.8.19 | hgraph 0.8.19 retained | current source retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | 3.1 +/- 0.0 | 3.4 +/- 0.0 | 1.10x | 2.9 +/- 0.0 | 3.2 +/- 0.0 | N/A | N/A |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | 3.4 +/- 0.0 | 3.7 +/- 0.0 | 1.09x | 3.2 +/- 0.0 | 3.6 +/- 0.0 | 1.7 | 251.2 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | 4.0 +/- 0.0 | 4.2 +/- 0.0 | 1.06x | 3.9 +/- 0.0 | 4.1 +/- 0.0 | 1.7 | 1002.0 |

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
| `service_adaptor_py__large` | 12.0 | 2.0 | 4.0 | 1.0 | 114.0 |
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

## Failures

### construct_std__large / release

```
sample 3: no result line (exit -11)
stdout: 
stderr: 
```

### tsd_sparse_large_capacity_std__medium / inspector

```
no result line (exit 1)
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
SystemError: error return without exception set

```

### tsd_churn_std__short / inspector

```
no result line (exit 1)
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

### tsd_clear_repopulate_std__short / release

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
SystemError: error return without exception set

```

### mesh_std__small / release

```
sample 1: no result line (exit -4)
stdout: 
stderr: 
```

### mesh_std__medium / release

```
sample 2: no result line (exit -11)
stdout: 
stderr: 
```

### service_adaptor_py__large / hg-cpp

```
sample 2: no result line (exit 1)
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

### reduce_dynamic_tsl_std__small / inspector

```
no result line (exit -11)
stdout: 
stderr: Fatal Python error: Failed to import encodings module
Python runtime state: core initialized

```

