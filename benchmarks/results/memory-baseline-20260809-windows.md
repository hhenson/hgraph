# hgraph memory-utilisation matrix

- date: 2026-08-09T18:04:46+00:00
- host: Windows-10-10.0.19045-SP0 / Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- CPU: Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- Python: 3.14.7
- reference baseline: hgraph 0.5.41 (published wheel)
- reference wheel: hgraph-0.5.41-cp312-abi3-win_amd64.whl
- reference SHA-256: 74deabc55a4e5a93f3d5234ff828d499c51344924fdac303303abe8b80b224f8
- fixed release baseline: hgraph 0.8.1 (published wheel)
- fixed release wheel: hgraph-0.8.1-cp312-abi3-win_amd64.whl
- fixed release SHA-256: 7d30ce7b27e3add5869eeda795cbd8ce21830218258533cd8a1a963b711adcd8
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: Python (`upstream-py`), hgraph C++ (`upstream-cpp`), hgraph 0.8.1 (`release`)
- reused fixed baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
GraphDiagnostics columns are a separate C++-first run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| Python (`upstream-py`) | 19.5 | 67.3 | 47.8 |
| hgraph C++ (`upstream-cpp`) | 19.5 | 69.4 | 50.0 |
| hgraph 0.8.1 (`release`) | 19.5 | 41.3 | 21.8 |

## Static graph

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 1.3 +/- 0.0 | 2.0 +/- 0.1 | 2.0 +/- 0.0 | 1.48x | 0.99x | 1.3 +/- 0.0 | 2.0 +/- 0.1 | 2.0 +/- 0.0 | 44.0 | 22.6 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 5.2 +/- 0.0 | 6.3 +/- 0.1 | 3.0 +/- 0.0 | 0.57x | 0.47x | 5.2 +/- 0.0 | 6.3 +/- 0.1 | 3.0 +/- 0.0 | 179.0 | 94.0 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | 19.9 +/- 0.1 | 19.5 +/- 0.0 | 9.8 +/- 0.2 | 0.50x | 0.51x | 17.7 +/- 0.2 | 19.5 +/- 0.0 | 4.3 +/- 0.0 | 697.5 | 363.4 |

## Bounded execution

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.2 +/- 0.0 | 6.24x | 2.43x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.2 +/- 0.0 | 0.9 | 0.3 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 0.2 +/- 0.1 | 0.5 +/- 0.0 | 1.3 +/- 0.0 | 5.14x | 2.44x | 0.2 +/- 0.1 | 0.5 +/- 0.0 | 1.3 +/- 0.0 | 0.9 | 0.3 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 0.3 +/- 0.1 | 0.5 +/- 0.0 | 1.2 +/- 0.0 | 4.63x | 2.46x | 0.3 +/- 0.1 | 0.5 +/- 0.0 | 1.2 +/- 0.0 | 0.9 | 0.3 |
| Python compute chain - short (`tick_py__short`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 7.77x | 2.90x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 1.5 |
| Python compute chain - medium (`tick_py__medium`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 7.74x | 2.81x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 1.9 | 1.5 |
| Python compute chain - long (`tick_py__long`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 7.51x | 2.70x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.3 +/- 0.0 | 1.9 | 1.5 |

## Process lifetime

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | Python first-to-last growth | hgraph C++ first-to-last growth | hgraph 0.8.1 first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.1 | 3.64x | 2.18x | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.1 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.8 | 4.5 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 1.4 +/- 0.0 | 2.1 +/- 0.0 | 1.5 +/- 0.0 | 1.06x | 0.71x | 1.4 +/- 0.0 | 2.1 +/- 0.0 | 1.5 +/- 0.0 | 0.9 +/- 0.1 | 1.4 +/- 0.0 | 0.0 +/- 0.0 | 8.8 | 4.5 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 9.2 +/- 0.0 | 14.6 +/- 0.0 | 1.6 +/- 0.0 | 0.17x | 0.11x | 9.2 +/- 0.0 | 14.6 +/- 0.0 | 1.6 +/- 0.0 | 8.7 +/- 0.0 | 13.9 +/- 0.1 | 0.1 +/- 0.0 | 8.8 | 4.5 |
| Repeated novel graph programs - ten (`construct_std__novel_ten`) | distinct graph programs | 4.2 +/- 0.1 | 5.8 +/- 0.0 | 2.5 +/- 0.0 | 0.59x | 0.43x | 4.2 +/- 0.1 | 5.8 +/- 0.0 | 2.5 +/- 0.0 | 3.9 +/- 0.1 | 5.1 +/- 0.0 | 1.1 +/- 0.1 | 8.8 | 4.5 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 0.5 +/- 0.0 | 1.0 +/- 0.0 | 2.2 +/- 0.0 | 4.33x | 2.08x | 0.5 +/- 0.0 | 1.0 +/- 0.0 | 2.2 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 5.8 | 12.4 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 1.3 +/- 0.1 | 2.2 +/- 0.0 | 2.3 +/- 0.0 | 1.76x | 1.04x | 1.3 +/- 0.1 | 2.2 +/- 0.0 | 2.3 +/- 0.0 | 0.7 +/- 0.0 | 1.2 +/- 0.0 | 0.1 +/- 0.0 | 5.8 | 12.4 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 4.8 +/- 0.0 | 7.9 +/- 0.0 | 2.3 +/- 0.0 | 0.48x | 0.29x | 4.8 +/- 0.0 | 7.9 +/- 0.0 | 2.3 +/- 0.0 | 4.2 +/- 0.0 | 6.9 +/- 0.0 | 0.1 +/- 0.0 | 5.8 | 12.4 |

## Value storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 7.15x | 2.94x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 1.4 | 0.7 |
| String arithmetic - long (`type_str_std__long`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 7.08x | 2.74x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 1.4 | 0.7 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 7.69x | 3.02x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 1.3 | 0.8 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 0.3 +/- 0.1 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 5.89x | 2.92x | 0.3 +/- 0.1 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 1.3 | 0.8 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 0.2 +/- 0.0 | 0.6 +/- 0.1 | 1.5 +/- 0.0 | 7.22x | 2.48x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 1.3 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 6.59x | 2.65x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 0.7 | 1.3 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 6.69x | 2.77x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 1.4 +/- 0.0 | 0.7 | 1.3 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.6 +/- 0.0 | 7.26x | 2.59x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 6.4 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 0.4 +/- 0.0 | 0.9 +/- 0.0 | 1.6 +/- 0.0 | 3.49x | 1.83x | 0.4 +/- 0.0 | 0.9 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 23.1 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 0.2 +/- 0.0 | 0.8 +/- 0.0 | 1.8 +/- 0.0 | 7.59x | 2.31x | 0.2 +/- 0.0 | 0.8 +/- 0.0 | 1.8 +/- 0.0 | 0.7 | 89.6 |

## Keyed collections

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 0.9 +/- 0.0 | 1.3 +/- 0.0 | 1.9 +/- 0.0 | 2.22x | 1.52x | 0.9 +/- 0.0 | 1.3 +/- 0.0 | 1.9 +/- 0.0 | 1.9 | 45.6 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 3.0 +/- 0.0 | 2.8 +/- 0.1 | 2.3 +/- 0.0 | 0.77x | 0.84x | 3.0 +/- 0.0 | 1.8 +/- 0.0 | 2.3 +/- 0.0 | 1.9 | 203.4 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 5.8 +/- 0.0 | 5.5 +/- 0.0 | 2.6 +/- 0.0 | 0.44x | 0.47x | 5.8 +/- 0.0 | 3.0 +/- 0.1 | 2.5 +/- 0.0 | 1.9 | 404.8 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 25.5 +/- 0.0 | 21.3 +/- 0.8 | 2.6 +/- 0.0 | 0.10x | 0.12x | 18.3 +/- 1.0 | 5.8 +/- 0.1 | 2.6 +/- 0.0 | 1.9 | 1887.8 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 153.6 +/- 0.3 | 131.2 +/- 0.1 | 20.2 +/- 0.0 | 0.13x | 0.15x | 32.5 +/- 0.7 | 13.3 +/- 1.0 | 3.7 +/- 0.1 | 1.9 | 11010.7 |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 609.2 +/- 0.9 | 514.6 +/- 0.2 | 74.5 +/- 0.1 | 0.12x | 0.14x | 32.9 +/- 0.7 | 32.7 +/- 0.1 | 3.9 +/- 0.1 | 1.9 | 44034.1 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 16.9 +/- 0.0 | 5.3 +/- 0.1 | 2.1 +/- 0.1 | 0.12x | 0.40x | 12.9 +/- 0.0 | 3.1 +/- 0.3 | 2.0 +/- 0.0 | 1.9 | 629.0 |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 34.2 +/- 0.1 | 7.3 +/- 0.0 | 2.2 +/- 0.0 | 0.06x | 0.30x | 17.2 +/- 0.1 | 2.8 +/- 0.2 | 2.2 +/- 0.0 | 1.9 | 629.0 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 35.8 +/- 0.1 | 9.3 +/- 0.0 | 2.1 +/- 0.0 | 0.06x | 0.23x | 18.1 +/- 0.0 | 2.8 +/- 0.0 | 2.1 +/- 0.0 | 1.9 | 629.0 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 11.4 +/- 0.1 | 3.8 +/- 0.2 | 2.4 +/- 0.3 | 0.21x | 0.64x | 8.4 +/- 1.1 | 3.8 +/- 0.1 | 2.4 +/- 0.3 | 1.9 | 1043.2 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 50.1 +/- 0.1 | 37.6 +/- 0.9 | 3.3 +/- 0.0 | 0.06x | 0.09x | 29.7 +/- 0.1 | 10.1 +/- 0.2 | 3.3 +/- 0.0 | 1.9 | 4251.7 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 99.5 +/- 0.1 | 72.9 +/- 1.0 | 17.1 +/- 0.1 | 0.17x | 0.23x | 44.5 +/- 0.8 | 18.5 +/- 0.1 | 3.6 +/- 0.0 | 1.9 | 8500.0 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | 45.9 +/- 0.1 | 23.1 +/- 1.1 | 5.9 +/- 0.0 | 0.13x | 0.25x | 29.2 +/- 2.5 | 6.4 +/- 0.4 | 2.7 +/- 0.0 | 1.9 | 1909.4 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 50.1 +/- 0.2 | 32.0 +/- 0.0 | 5.9 +/- 0.0 | 0.12x | 0.18x | 26.3 +/- 2.0 | 7.7 +/- 0.2 | 3.0 +/- 0.0 | 1.9 | 1909.4 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 82.9 +/- 1.4 | 61.4 +/- 0.2 | 6.1 +/- 0.0 | 0.07x | 0.10x | 43.6 +/- 2.8 | 7.0 +/- 0.1 | 3.0 +/- 0.1 | 1.9 | 1909.4 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 6.5 +/- 0.1 | 5.7 +/- 0.2 | 2.6 +/- 0.0 | 0.40x | 0.45x | 6.5 +/- 0.1 | 3.1 +/- 0.5 | 2.6 +/- 0.0 | 1.9 | 404.9 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 6.7 +/- 0.0 | 5.6 +/- 0.1 | 2.6 +/- 0.0 | 0.39x | 0.46x | 6.7 +/- 0.0 | 3.2 +/- 0.2 | 2.6 +/- 0.0 | 1.9 | 404.9 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 6.9 +/- 0.0 | 5.4 +/- 0.0 | 2.5 +/- 0.0 | 0.36x | 0.46x | 6.9 +/- 0.0 | 3.0 +/- 0.1 | 2.5 +/- 0.0 | 1.9 | 404.9 |

## Nested graphs

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 0.6 +/- 0.0 | 1.0 +/- 0.0 | 1.7 +/- 0.1 | 3.01x | 1.74x | 0.6 +/- 0.0 | 1.0 +/- 0.0 | 1.7 +/- 0.1 | 1.4 | 18.6 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 1.5 +/- 0.0 | 1.7 +/- 0.0 | 1.8 +/- 0.0 | 1.21x | 1.06x | 1.5 +/- 0.0 | 1.7 +/- 0.0 | 1.8 +/- 0.0 | 1.4 | 75.6 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 2.8 +/- 0.0 | 3.0 +/- 0.1 | 1.9 +/- 0.0 | 0.67x | 0.62x | 2.8 +/- 0.0 | 2.0 +/- 0.0 | 1.9 +/- 0.0 | 1.4 | 150.2 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 8.8 +/- 0.0 | 1.8 +/- 0.0 | 2.3 +/- 0.0 | 0.26x | 1.24x | 8.8 +/- 0.0 | 1.8 +/- 0.0 | 2.3 +/- 0.0 | 3.1 | 160.0 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 27.2 +/- 0.0 | 5.8 +/- 0.2 | 2.7 +/- 0.1 | 0.10x | 0.47x | 25.7 +/- 0.5 | 3.3 +/- 0.0 | 2.7 +/- 0.1 | 3.1 | 631.2 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 37.5 +/- 0.2 | 10.7 +/- 0.0 | 3.5 +/- 0.1 | 0.09x | 0.33x | 34.1 +/- 0.1 | 4.6 +/- 0.3 | 2.8 +/- 0.1 | 3.1 | 1259.8 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | 4.9 +/- 0.0 | 2.3 +/- 0.3 | 2.4 +/- 0.0 | 0.50x | 1.08x | 4.9 +/- 0.0 | 2.3 +/- 0.3 | 2.4 +/- 0.0 | 2.0 | 148.2 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | 7.5 +/- 0.0 | 4.0 +/- 0.2 | 2.8 +/- 0.1 | 0.37x | 0.70x | 7.5 +/- 0.0 | 3.1 +/- 0.1 | 2.8 +/- 0.1 | 2.0 | 522.4 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 10.2 +/- 0.3 | 6.6 +/- 0.0 | 2.7 +/- 0.0 | 0.27x | 0.41x | 10.2 +/- 0.3 | 3.0 +/- 0.0 | 2.7 +/- 0.0 | 2.0 | 1032.7 |

## Services

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 0.3 +/- 0.0 | 0.9 +/- 0.0 | 2.1 +/- 0.0 | 6.24x | 2.39x | 0.3 +/- 0.0 | 0.9 +/- 0.0 | 2.1 +/- 0.0 | 2.5 | 9.2 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 0.6 +/- 0.0 | 1.1 +/- 0.1 | 2.2 +/- 0.0 | 3.92x | 2.01x | 0.6 +/- 0.0 | 1.1 +/- 0.1 | 2.2 +/- 0.0 | 5.8 | 12.4 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 1.1 +/- 0.0 | 1.8 +/- 0.1 | 2.5 +/- 0.0 | 2.31x | 1.40x | 1.1 +/- 0.0 | 1.8 +/- 0.1 | 2.5 +/- 0.0 | 19.3 | 31.3 |

## C++-first dynamic storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hgraph 0.8.1 peak delta | hgraph 0.8.1/Python | hgraph 0.8.1/hgraph C++ | Python retained | hgraph C++ retained | hgraph 0.8.1 retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | N/A | N/A | 2.1 +/- 0.0 | N/A | N/A | N/A | N/A | 2.1 +/- 0.0 | 1.6 | 62.9 |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | N/A | N/A | 2.3 +/- 0.1 | N/A | N/A | N/A | N/A | 2.3 +/- 0.1 | 1.6 | 246.2 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | N/A | N/A | 2.8 +/- 0.3 | N/A | N/A | N/A | N/A | 2.7 +/- 0.3 | 1.6 | 995.6 |

## hgraph 0.8.1 retained runtime registry growth

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
| `service_adaptor_py__repeat_once` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `service_adaptor_py__repeat_ten` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `service_adaptor_py__repeat_fifty` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `type_str_std__short` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `type_str_std__long` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `type_cs_py__short` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 42.0 +/- 0.0 |
| `type_cs_py__long` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 42.0 +/- 0.0 |
| `type_tsw_append_evict_std__short` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `type_tsw_append_evict_std__medium` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `type_tsw_append_evict_std__long` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `tss_add_remove_std__small` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 34.0 +/- 0.0 |
| `tss_add_remove_std__medium` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 34.0 +/- 0.0 |
| `tss_add_remove_std__large` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 34.0 +/- 0.0 |
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
| `reduce_tsd_nested_graph_std__small` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 59.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__medium` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 59.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__large` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 59.0 +/- 0.0 |
| `switch_keyed_collection_std__small` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 100.0 +/- 0.0 |
| `switch_keyed_collection_std__medium` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 100.0 +/- 0.0 |
| `switch_keyed_collection_std__large` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 100.0 +/- 0.0 |
| `mesh_std__small` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 109.0 +/- 0.0 |
| `mesh_std__medium` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 109.0 +/- 0.0 |
| `mesh_std__large` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 109.0 +/- 0.0 |
| `service_adaptor_py__small` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `service_adaptor_py__medium` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `service_adaptor_py__large` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 53.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 53.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 53.0 +/- 0.0 |

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
