# hgraph memory-utilisation matrix

- date: 2026-07-31T16:35:45+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- hg_cpp revision: a4b979bf2861
- hg_cpp source fingerprint: e0deeba44edf67b7856d84377041d54d9892d2b8c8ff509fa68e191f88bf0a70
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: Python (`upstream-py`), hgraph C++ (`upstream-cpp`), hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 110

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
Inspector columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| Python (`upstream-py`) | 17.5 | 76.8 | 59.3 |
| hgraph C++ (`upstream-cpp`) | 17.5 | 79.8 | 62.3 |
| hg_cpp (`hg-cpp`) | 17.5 | 65.4 | 47.8 |

## Static graph

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 1.5 +/- 0.0 | 1.8 +/- 0.0 | 2.6 +/- 0.0 | 1.77x | 1.47x | 1.4 +/- 0.0 | 1.7 +/- 0.0 | 2.5 +/- 0.0 | 44.1 | 0.0 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 5.2 +/- 0.0 | 5.7 +/- 0.0 | 6.1 +/- 0.1 | 1.17x | 1.07x | 5.1 +/- 0.0 | 5.6 +/- 0.0 | 6.0 +/- 0.1 | 179.0 | 0.0 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | 19.6 +/- 0.0 | 20.9 +/- 0.1 | 19.2 +/- 0.0 | 0.98x | 0.92x | 17.3 +/- 0.0 | 19.9 +/- 0.1 | 17.7 +/- 0.0 | 697.6 | 0.0 |

## Bounded execution

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 1.6 +/- 0.0 | 4.14x | 2.16x | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 1.5 +/- 0.0 | 0.9 | 0.0 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 0.3 +/- 0.0 | 0.7 +/- 0.0 | 1.6 +/- 0.0 | 4.46x | 2.11x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.4 +/- 0.0 | 0.9 | 0.0 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.0 | 4.25x | 2.04x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.4 +/- 0.0 | 0.9 | 0.0 |
| Python compute chain - short (`tick_py__short`) | duration | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 1.4 +/- 0.0 | 3.72x | 1.88x | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 1.3 +/- 0.0 | 1.8 | 0.0 |
| Python compute chain - medium (`tick_py__medium`) | duration | 0.3 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.0 | 4.43x | 2.09x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.4 +/- 0.0 | 1.8 | 0.0 |
| Python compute chain - long (`tick_py__long`) | duration | 0.3 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.1 | 4.49x | 2.10x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.4 +/- 0.1 | 1.8 | 0.0 |

## Process lifetime

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | Python first-to-last growth | hgraph C++ first-to-last growth | hg_cpp first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 0.6 +/- 0.0 | 0.9 +/- 0.0 | 1.7 +/- 0.0 | 2.95x | 1.90x | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 1.6 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.9 | 0.0 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 1.4 +/- 0.0 | 1.7 +/- 0.0 | 2.7 +/- 0.0 | 1.99x | 1.61x | 1.3 +/- 0.0 | 1.6 +/- 0.0 | 2.6 +/- 0.0 | 0.8 +/- 0.0 | 0.9 +/- 0.0 | 1.0 +/- 0.0 | 8.9 | 0.0 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 9.3 +/- 0.0 | 11.8 +/- 0.0 | 12.9 +/- 0.0 | 1.39x | 1.09x | 9.1 +/- 0.0 | 11.7 +/- 0.0 | 12.8 +/- 0.0 | 8.7 +/- 0.0 | 10.9 +/- 0.0 | 11.2 +/- 0.0 | 8.9 | 0.0 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 0.7 +/- 0.0 | 0.9 +/- 0.0 | 2.3 +/- 0.0 | 3.50x | 2.45x | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 2.2 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 6.0 | 4.5 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 1.4 +/- 0.0 | 1.8 +/- 0.0 | 3.0 +/- 0.1 | 2.11x | 1.65x | 1.3 +/- 0.0 | 1.7 +/- 0.0 | 2.8 +/- 0.1 | 0.7 +/- 0.0 | 0.9 +/- 0.0 | 0.6 +/- 0.0 | 6.0 | 4.5 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 4.8 +/- 0.0 | 6.2 +/- 0.0 | 5.5 +/- 0.1 | 1.16x | 0.89x | 4.6 +/- 0.0 | 6.1 +/- 0.0 | 5.4 +/- 0.1 | 4.1 +/- 0.0 | 5.3 +/- 0.0 | 3.2 +/- 0.0 | 6.0 | 4.5 |

## Value storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 1.6 +/- 0.0 | 4.58x | 2.24x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.5 +/- 0.0 | 1.5 | 0.0 |
| String arithmetic - long (`type_str_std__long`) | duration | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 1.6 +/- 0.0 | 4.55x | 2.10x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.0 | 1.5 | 0.0 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 0.3 +/- 0.0 | 0.7 +/- 0.0 | 1.6 +/- 0.0 | 4.51x | 2.13x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 1.4 +/- 0.0 | 1.3 | 0.0 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 1.6 +/- 0.0 | 4.41x | 2.02x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.4 +/- 0.0 | 1.3 | 0.0 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 0.3 +/- 0.0 | 0.8 +/- 0.0 | 1.7 +/- 0.0 | 5.15x | 2.18x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.6 +/- 0.0 | 0.8 | 0.0 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 0.3 +/- 0.0 | 0.8 +/- 0.0 | 1.5 +/- 0.0 | 4.55x | 1.99x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.4 +/- 0.0 | 0.8 | 0.0 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 1.8 +/- 0.0 | 4.75x | 2.21x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.6 +/- 0.0 | 0.8 | 0.0 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 0.3 +/- 0.0 | 0.8 +/- 0.0 | 1.6 +/- 0.0 | 4.81x | 2.04x | 0.2 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 1.7 +/- 0.0 | 4.21x | 2.00x | 0.3 +/- 0.0 | 0.7 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 0.7 +/- 0.0 | 1.1 +/- 0.0 | 2.0 +/- 0.0 | 2.94x | 1.73x | 0.6 +/- 0.0 | 1.0 +/- 0.0 | 1.9 +/- 0.0 | 0.7 | 0.0 |

## Keyed collections

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 1.0 +/- 0.0 | 1.1 +/- 0.0 | 2.2 +/- 0.1 | 2.18x | 2.07x | 0.9 +/- 0.0 | 1.0 +/- 0.0 | 2.1 +/- 0.1 | 2.2 | 33.7 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 3.1 +/- 0.0 | 2.6 +/- 0.0 | 2.7 +/- 0.0 | 0.86x | 1.06x | 3.0 +/- 0.0 | 2.4 +/- 0.0 | 2.6 +/- 0.0 | 2.2 | 155.4 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 6.0 +/- 0.0 | 4.6 +/- 0.0 | 3.2 +/- 0.1 | 0.53x | 0.69x | 5.8 +/- 0.0 | 4.5 +/- 0.0 | 3.0 +/- 0.1 | 2.2 | 310.8 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 25.2 +/- 0.0 | 19.1 +/- 0.0 | 7.4 +/- 0.0 | 0.29x | 0.38x | 19.1 +/- 0.2 | 19.0 +/- 0.0 | 6.0 +/- 0.0 | 2.2 | 1447.7 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 154.0 +/- 0.0 | 112.9 +/- 0.0 | 29.4 +/- 0.0 | 0.19x | 0.26x | 33.4 +/- 0.1 | 86.7 +/- 0.8 | 22.1 +/- 0.0 | 2.2 | 8510.8 |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 615.3 +/- 0.4 | 450.4 +/- 0.0 | 111.2 +/- 0.0 | 0.18x | 0.25x | 42.2 +/- 0.5 | 324.6 +/- 0.0 | 80.8 +/- 0.0 | 2.2 | 34043.2 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 15.8 +/- 0.1 | 5.1 +/- 0.0 | 3.3 +/- 0.0 | 0.21x | 0.64x | 14.8 +/- 0.1 | 5.0 +/- 0.0 | 3.1 +/- 0.0 | 2.2 | 515.5 |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 28.5 +/- 0.0 | 7.3 +/- 0.0 | 3.2 +/- 0.0 | 0.11x | 0.44x | 24.8 +/- 1.0 | 7.2 +/- 0.0 | 3.1 +/- 0.0 | 2.2 | 515.5 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 31.0 +/- 0.0 | 9.6 +/- 0.0 | 3.3 +/- 0.0 | 0.11x | 0.34x | 28.6 +/- 0.0 | 9.4 +/- 0.0 | 3.1 +/- 0.0 | 2.2 | 515.5 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 11.4 +/- 0.0 | 8.5 +/- 0.1 | 4.2 +/- 0.0 | 0.37x | 0.50x | 8.5 +/- 0.9 | 8.3 +/- 0.1 | 4.1 +/- 0.0 | 2.2 | 841.3 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 50.6 +/- 0.0 | 36.4 +/- 0.0 | 13.1 +/- 0.0 | 0.26x | 0.36x | 31.4 +/- 0.4 | 36.3 +/- 0.0 | 10.4 +/- 0.0 | 2.2 | 3367.8 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 101.0 +/- 0.1 | 72.2 +/- 0.0 | 24.2 +/- 0.0 | 0.24x | 0.34x | 42.0 +/- 0.8 | 72.1 +/- 0.0 | 18.4 +/- 0.0 | 2.2 | 6736.4 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | 49.3 +/- 0.0 | 21.1 +/- 0.2 | 7.8 +/- 0.0 | 0.16x | 0.37x | 37.0 +/- 1.2 | 20.9 +/- 0.2 | 6.4 +/- 0.0 | 2.2 | 1447.7 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 67.3 +/- 0.5 | 32.0 +/- 0.0 | 7.8 +/- 0.0 | 0.12x | 0.24x | 52.1 +/- 2.2 | 31.8 +/- 0.0 | 6.4 +/- 0.0 | 2.2 | 1447.7 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 116.2 +/- 2.5 | 64.7 +/- 0.0 | 7.8 +/- 0.0 | 0.07x | 0.12x | 69.4 +/- 4.0 | 64.6 +/- 0.0 | 6.4 +/- 0.0 | 2.2 | 1447.7 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 8.4 +/- 0.0 | 4.6 +/- 0.0 | 3.2 +/- 0.0 | 0.38x | 0.69x | 8.3 +/- 0.0 | 4.5 +/- 0.0 | 3.1 +/- 0.0 | 2.2 | 310.8 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 11.7 +/- 0.1 | 4.7 +/- 0.0 | 3.2 +/- 0.0 | 0.27x | 0.68x | 9.7 +/- 0.2 | 4.5 +/- 0.0 | 3.0 +/- 0.0 | 2.2 | 310.8 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 11.8 +/- 0.1 | 4.7 +/- 0.1 | 3.1 +/- 0.0 | 0.26x | 0.66x | 10.8 +/- 0.4 | 4.6 +/- 0.1 | 3.0 +/- 0.0 | 2.2 | 310.8 |

## Nested graphs

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 2.1 +/- 0.0 | 2.87x | 2.75x | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 2.0 +/- 0.0 | 1.6 | 13.3 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 1.7 +/- 0.0 | 1.4 +/- 0.0 | 2.2 +/- 0.0 | 1.26x | 1.56x | 1.6 +/- 0.0 | 1.3 +/- 0.0 | 2.1 +/- 0.0 | 1.6 | 53.0 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 3.0 +/- 0.0 | 2.4 +/- 0.0 | 2.2 +/- 0.0 | 0.73x | 0.91x | 2.9 +/- 0.0 | 2.3 +/- 0.0 | 2.1 +/- 0.0 | 1.6 | 106.0 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 14.7 +/- 0.1 | 1.7 +/- 0.0 | 2.5 +/- 0.0 | 0.17x | 1.46x | 13.9 +/- 0.6 | 1.6 +/- 0.0 | 2.4 +/- 0.0 | 3.7 | 128.9 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 24.1 +/- 0.0 | 4.7 +/- 0.0 | 3.4 +/- 0.0 | 0.14x | 0.72x | 24.0 +/- 0.0 | 4.6 +/- 0.0 | 3.3 +/- 0.0 | 3.7 | 515.5 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 33.1 +/- 0.0 | 8.8 +/- 0.0 | 4.5 +/- 0.0 | 0.14x | 0.51x | 32.9 +/- 0.0 | 8.7 +/- 0.0 | 4.3 +/- 0.0 | 3.7 | 1030.9 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | 5.3 +/- 0.0 | 1.7 +/- 0.0 | 2.7 +/- 0.0 | 0.52x | 1.64x | 5.2 +/- 0.0 | 1.5 +/- 0.0 | 2.6 +/- 0.0 | 2.5 | 114.5 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | 7.2 +/- 0.0 | 3.4 +/- 0.1 | 3.4 +/- 0.1 | 0.47x | 0.99x | 7.1 +/- 0.0 | 3.3 +/- 0.1 | 3.2 +/- 0.1 | 2.5 | 424.2 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 10.1 +/- 0.0 | 5.4 +/- 0.0 | 4.0 +/- 0.0 | 0.40x | 0.75x | 10.0 +/- 0.0 | 5.3 +/- 0.0 | 3.6 +/- 0.0 | 2.5 | 848.4 |

## Services

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 2.3 +/- 0.0 | 4.27x | 2.70x | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 2.1 +/- 0.0 | 2.7 | 4.5 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 0.7 +/- 0.0 | 0.9 +/- 0.0 | 2.4 +/- 0.0 | 3.40x | 2.53x | 0.6 +/- 0.0 | 0.8 +/- 0.0 | 2.2 +/- 0.0 | 6.0 | 4.5 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 1.2 +/- 0.0 | 1.3 +/- 0.0 | 2.7 +/- 0.0 | 2.17x | 2.02x | 1.1 +/- 0.0 | 1.2 +/- 0.0 | 2.6 +/- 0.0 | 19.5 | 9.0 |

## hg_cpp dynamic storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | N/A | N/A | 2.9 +/- 0.0 | N/A | N/A | N/A | N/A | 2.8 +/- 0.0 | 2.0 | 45.3 |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | N/A | N/A | 3.4 +/- 0.1 | N/A | N/A | N/A | N/A | 3.3 +/- 0.1 | 2.0 | 181.0 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | N/A | N/A | 5.4 +/- 0.0 | N/A | N/A | N/A | N/A | 4.7 +/- 0.0 | 2.0 | 724.1 |

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
