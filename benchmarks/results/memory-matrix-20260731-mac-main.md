# hgraph memory-utilisation matrix

- date: 2026-07-31T16:20:56+00:00
- host: macOS-26.5.2-arm64-arm-64bit-Mach-O / arm
- CPU: Apple M4 Max
- Python: 3.14.6
- hg_cpp revision: 1fca20809d06
- hg_cpp source fingerprint: e0deeba44edf67b7856d84377041d54d9892d2b8c8ff509fa68e191f88bf0a70
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: Python (`upstream-py`), hgraph C++ (`upstream-cpp`), hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
Inspector columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| Python (`upstream-py`) | 22.3 | 83.8 | 61.5 |
| hgraph C++ (`upstream-cpp`) | 22.3 | 86.2 | 63.9 |
| hg_cpp (`hg-cpp`) | 22.3 | 63.5 | 41.1 |

## Static graph

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 1.3 +/- 0.0 | 1.4 +/- 0.0 | 2.4 +/- 0.0 | 1.83x | 1.67x | 1.3 +/- 0.0 | 1.4 +/- 0.0 | 2.4 +/- 0.0 | 42.5 | 0.0 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 5.0 +/- 0.1 | 4.6 +/- 0.0 | 5.7 +/- 0.0 | 1.15x | 1.24x | 5.0 +/- 0.1 | 4.6 +/- 0.0 | 5.7 +/- 0.0 | 172.8 | 0.0 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | 19.0 +/- 0.1 | 21.6 +/- 0.0 | 19.0 +/- 0.0 | 1.00x | 0.88x | 18.0 +/- 0.6 | 20.6 +/- 0.0 | 19.0 +/- 0.0 | 673.4 | 0.0 |

## Bounded execution

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 7.10x | 4.26x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 0.9 | 0.0 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 0.1 +/- 0.0 | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 9.42x | 4.26x | 0.1 +/- 0.0 | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 0.9 | 0.0 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 7.90x | 4.36x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 0.9 | 0.0 |
| Python compute chain - short (`tick_py__short`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 8.17x | 4.73x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 1.7 | 0.0 |
| Python compute chain - medium (`tick_py__medium`) | duration | 0.1 +/- 0.0 | 0.4 +/- 0.1 | 1.4 +/- 0.0 | 10.09x | 3.96x | 0.1 +/- 0.0 | 0.4 +/- 0.1 | 1.4 +/- 0.0 | 1.7 | 0.0 |
| Python compute chain - long (`tick_py__long`) | duration | 0.1 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 9.87x | 4.93x | 0.1 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 1.7 | 0.0 |

## Process lifetime

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | Python first-to-last growth | hgraph C++ first-to-last growth | hg_cpp first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 0.3 +/- 0.0 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 4.94x | 3.20x | 0.3 +/- 0.0 | 0.5 +/- 0.0 | 1.5 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.5 | 0.0 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 1.2 +/- 0.0 | 1.5 +/- 0.0 | 2.5 +/- 0.1 | 2.11x | 1.63x | 1.2 +/- 0.0 | 1.5 +/- 0.0 | 2.5 +/- 0.1 | 0.9 +/- 0.0 | 1.0 +/- 0.0 | 1.0 +/- 0.0 | 8.5 | 0.0 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 8.9 +/- 0.0 | 11.0 +/- 0.4 | 12.3 +/- 0.0 | 1.37x | 1.12x | 8.9 +/- 0.0 | 11.0 +/- 0.4 | 12.3 +/- 0.0 | 8.6 +/- 0.0 | 10.5 +/- 0.5 | 10.7 +/- 0.0 | 8.5 | 0.0 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 0.5 +/- 0.0 | 0.7 +/- 0.0 | 2.0 +/- 0.0 | 3.79x | 2.78x | 0.5 +/- 0.0 | 0.7 +/- 0.0 | 2.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 5.8 | 4.4 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 1.2 +/- 0.0 | 1.7 +/- 0.0 | 2.6 +/- 0.1 | 2.09x | 1.52x | 1.2 +/- 0.0 | 1.7 +/- 0.0 | 2.6 +/- 0.1 | 0.8 +/- 0.0 | 1.0 +/- 0.0 | 0.6 +/- 0.0 | 5.8 | 4.4 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 4.6 +/- 0.0 | 5.7 +/- 0.0 | 5.0 +/- 0.0 | 1.10x | 0.88x | 4.6 +/- 0.0 | 5.7 +/- 0.0 | 5.0 +/- 0.0 | 4.1 +/- 0.0 | 4.9 +/- 0.0 | 3.1 +/- 0.0 | 5.8 | 4.4 |

## Value storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 7.52x | 4.73x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 1.4 | 0.0 |
| String arithmetic - long (`type_str_std__long`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 8.96x | 4.29x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 1.4 | 0.0 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.4 +/- 0.1 | 9.21x | 4.00x | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.4 +/- 0.1 | 1.2 | 0.0 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 9.62x | 4.57x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 1.2 | 0.0 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 0.2 +/- 0.0 | 0.4 +/- 0.1 | 1.4 +/- 0.0 | 7.65x | 3.54x | 0.2 +/- 0.0 | 0.4 +/- 0.1 | 1.4 +/- 0.0 | 0.7 | 0.0 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 8.63x | 3.96x | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 7.32x | 4.32x | 0.2 +/- 0.0 | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.6 +/- 0.0 | 10.12x | 4.04x | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.6 +/- 0.0 | 6.56x | 3.89x | 0.2 +/- 0.0 | 0.4 +/- 0.0 | 1.6 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.8 +/- 0.1 | 8.21x | 2.56x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.8 +/- 0.1 | 0.7 | 0.0 |

## Keyed collections

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 0.9 +/- 0.0 | 1.0 +/- 0.0 | 1.8 +/- 0.0 | 1.98x | 1.83x | 0.9 +/- 0.0 | 1.0 +/- 0.0 | 1.8 +/- 0.0 | 2.1 | 32.9 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 3.0 +/- 0.0 | 2.6 +/- 0.1 | 2.1 +/- 0.0 | 0.71x | 0.83x | 3.0 +/- 0.0 | 2.6 +/- 0.1 | 2.1 +/- 0.0 | 2.1 | 151.3 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 5.8 +/- 0.0 | 4.6 +/- 0.0 | 2.5 +/- 0.1 | 0.43x | 0.55x | 5.8 +/- 0.0 | 4.6 +/- 0.0 | 2.5 +/- 0.1 | 2.1 | 302.5 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 25.2 +/- 0.0 | 17.9 +/- 0.1 | 6.9 +/- 0.1 | 0.27x | 0.39x | 19.9 +/- 0.0 | 17.9 +/- 0.1 | 6.9 +/- 0.1 | 2.1 | 1408.4 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 154.2 +/- 0.4 | 109.0 +/- 0.1 | 28.0 +/- 0.0 | 0.18x | 0.26x | 43.5 +/- 1.4 | 92.6 +/- 0.1 | 28.0 +/- 0.0 | 2.1 | 8290.6 |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 616.5 +/- 1.2 | 448.2 +/- 3.3 | 106.1 +/- 0.1 | 0.17x | 0.24x | 63.5 +/- 2.2 | 354.9 +/- 3.0 | 98.3 +/- 0.1 | 2.1 | 33162.2 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 17.1 +/- 0.0 | 4.9 +/- 0.1 | 2.5 +/- 0.0 | 0.14x | 0.50x | 13.1 +/- 1.0 | 4.9 +/- 0.1 | 2.5 +/- 0.0 | 2.1 | 501.0 |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 33.6 +/- 0.0 | 7.2 +/- 0.0 | 2.7 +/- 0.1 | 0.08x | 0.38x | 13.6 +/- 0.0 | 7.2 +/- 0.0 | 2.7 +/- 0.1 | 2.1 | 501.0 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 36.4 +/- 0.1 | 10.0 +/- 0.0 | 2.8 +/- 0.0 | 0.08x | 0.28x | 19.4 +/- 0.8 | 10.0 +/- 0.0 | 2.8 +/- 0.0 | 2.1 | 501.0 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 11.5 +/- 0.1 | 8.1 +/- 0.0 | 3.7 +/- 0.0 | 0.32x | 0.45x | 9.5 +/- 0.1 | 8.1 +/- 0.0 | 3.7 +/- 0.0 | 2.1 | 819.3 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 49.3 +/- 0.0 | 34.5 +/- 0.0 | 12.3 +/- 0.0 | 0.25x | 0.36x | 30.9 +/- 0.8 | 34.5 +/- 0.0 | 12.3 +/- 0.0 | 2.1 | 3279.8 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 98.3 +/- 0.0 | 68.7 +/- 0.1 | 23.1 +/- 0.0 | 0.24x | 0.34x | 43.3 +/- 1.0 | 68.7 +/- 0.1 | 23.1 +/- 0.0 | 2.1 | 6560.4 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | 44.4 +/- 0.0 | 21.1 +/- 0.1 | 7.2 +/- 0.0 | 0.16x | 0.34x | 26.8 +/- 0.0 | 21.1 +/- 0.1 | 7.2 +/- 0.0 | 2.1 | 1408.4 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 55.8 +/- 0.2 | 31.7 +/- 0.0 | 7.3 +/- 0.1 | 0.13x | 0.23x | 34.8 +/- 1.6 | 31.7 +/- 0.0 | 7.3 +/- 0.1 | 2.1 | 1408.4 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 100.6 +/- 0.1 | 75.8 +/- 0.0 | 7.3 +/- 0.1 | 0.07x | 0.10x | 77.2 +/- 5.1 | 75.8 +/- 0.0 | 7.3 +/- 0.1 | 2.1 | 1408.4 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 6.6 +/- 0.0 | 4.3 +/- 0.0 | 2.5 +/- 0.0 | 0.38x | 0.58x | 6.6 +/- 0.0 | 4.3 +/- 0.0 | 2.5 +/- 0.0 | 2.1 | 302.5 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 6.9 +/- 0.0 | 4.6 +/- 0.0 | 2.5 +/- 0.0 | 0.36x | 0.55x | 6.9 +/- 0.0 | 4.6 +/- 0.0 | 2.5 +/- 0.0 | 2.1 | 302.5 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 7.0 +/- 0.0 | 4.5 +/- 0.0 | 2.7 +/- 0.0 | 0.38x | 0.60x | 7.0 +/- 0.0 | 4.5 +/- 0.0 | 2.7 +/- 0.0 | 2.1 | 302.5 |

## Nested graphs

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 1.7 +/- 0.0 | 3.38x | 2.20x | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 1.7 +/- 0.0 | 1.5 | 13.0 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 1.6 +/- 0.0 | 1.5 +/- 0.1 | 1.8 +/- 0.0 | 1.18x | 1.26x | 1.6 +/- 0.0 | 1.5 +/- 0.1 | 1.8 +/- 0.0 | 1.5 | 52.0 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 2.9 +/- 0.0 | 2.5 +/- 0.0 | 1.9 +/- 0.0 | 0.67x | 0.79x | 2.9 +/- 0.0 | 2.5 +/- 0.0 | 1.9 +/- 0.0 | 1.5 | 104.0 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 8.6 +/- 0.0 | 1.9 +/- 0.1 | 2.2 +/- 0.1 | 0.26x | 1.18x | 8.6 +/- 0.0 | 1.9 +/- 0.1 | 2.2 +/- 0.1 | 3.5 | 125.2 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 27.5 +/- 0.0 | 4.8 +/- 0.1 | 3.2 +/- 0.0 | 0.11x | 0.65x | 25.5 +/- 1.0 | 4.8 +/- 0.1 | 3.2 +/- 0.0 | 3.5 | 501.0 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 37.8 +/- 0.0 | 8.6 +/- 0.0 | 4.2 +/- 0.1 | 0.11x | 0.48x | 33.8 +/- 2.0 | 8.6 +/- 0.0 | 4.2 +/- 0.1 | 3.5 | 1001.9 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | 4.6 +/- 0.0 | 1.7 +/- 0.0 | 2.2 +/- 0.0 | 0.47x | 1.27x | 4.6 +/- 0.0 | 1.7 +/- 0.0 | 2.2 +/- 0.0 | 2.4 | 110.8 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | 7.1 +/- 0.0 | 3.2 +/- 0.0 | 2.6 +/- 0.0 | 0.37x | 0.80x | 7.1 +/- 0.0 | 3.2 +/- 0.0 | 2.6 +/- 0.0 | 2.4 | 409.6 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 10.0 +/- 0.0 | 5.2 +/- 0.0 | 3.5 +/- 0.1 | 0.35x | 0.66x | 10.0 +/- 0.0 | 5.2 +/- 0.0 | 3.5 +/- 0.1 | 2.4 | 819.2 |

## Services

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 2.0 +/- 0.0 | 6.00x | 3.23x | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 2.0 +/- 0.0 | 2.5 | 4.4 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 2.0 +/- 0.0 | 4.36x | 2.62x | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 2.0 +/- 0.0 | 5.8 | 4.4 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 1.0 +/- 0.0 | 1.2 +/- 0.0 | 2.3 +/- 0.0 | 2.24x | 1.88x | 1.0 +/- 0.0 | 1.2 +/- 0.0 | 2.3 +/- 0.0 | 18.7 | 8.9 |

## hg_cpp dynamic storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | N/A | N/A | 1.9 +/- 0.0 | N/A | N/A | N/A | N/A | 1.9 +/- 0.0 | 1.9 | 44.0 |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | N/A | N/A | 2.3 +/- 0.0 | N/A | N/A | N/A | N/A | 2.3 +/- 0.0 | 1.9 | 176.0 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | N/A | N/A | 4.3 +/- 0.0 | N/A | N/A | N/A | N/A | 4.3 +/- 0.0 | 1.9 | 704.1 |

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
