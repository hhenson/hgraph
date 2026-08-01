# hgraph memory-utilisation matrix

- date: 2026-08-01T12:04:08+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- hg_cpp revision: 251cd09cbe17+dirty
- hg_cpp source fingerprint: 057ad8fb69455f1b67fbc0a6c010b66e7fd83e55fd8957d85e3b5f65902325ea
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: Python (`upstream-py`), hgraph C++ (`upstream-cpp`), hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
Inspector columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| Python (`upstream-py`) | 17.5 | 78.3 | 60.8 |
| hgraph C++ (`upstream-cpp`) | 17.5 | 81.2 | 63.7 |
| hg_cpp (`hg-cpp`) | 17.5 | 72.6 | 55.1 |

## Static graph

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 1.5 +/- 0.0 | 1.7 +/- 0.0 | 1.2 +/- 0.0 | 0.78x | 0.68x | 1.4 +/- 0.0 | 1.6 +/- 0.0 | 1.1 +/- 0.0 | 44.1 | 35.6 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 5.3 +/- 0.0 | 5.6 +/- 0.0 | 2.9 +/- 0.0 | 0.56x | 0.52x | 5.1 +/- 0.0 | 5.5 +/- 0.0 | 2.8 +/- 0.0 | 179.0 | 145.2 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | 19.6 +/- 0.0 | 20.8 +/- 0.0 | 9.4 +/- 0.0 | 0.48x | 0.45x | 17.6 +/- 0.9 | 19.8 +/- 0.0 | 7.9 +/- 0.0 | 697.6 | 568.3 |

## Bounded execution

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 2.00x | 1.10x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 0.6 +/- 0.0 | 0.9 | 0.4 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 1.86x | 0.96x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.9 | 0.4 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 1.83x | 1.05x | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.9 | 0.4 |
| Python compute chain - short (`tick_py__short`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 2.11x | 1.09x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.7 +/- 0.0 | 1.8 | 1.8 |
| Python compute chain - medium (`tick_py__medium`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 1.99x | 1.03x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 1.8 | 1.8 |
| Python compute chain - long (`tick_py__long`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 1.96x | 1.07x | 0.2 +/- 0.0 | 0.5 +/- 0.0 | 0.6 +/- 0.0 | 1.8 | 1.8 |

## Process lifetime

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | Python first-to-last growth | hgraph C++ first-to-last growth | hg_cpp first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 0.6 +/- 0.0 | 0.8 +/- 0.0 | 0.8 +/- 0.0 | 1.35x | 0.95x | 0.5 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.9 | 6.8 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 1.4 +/- 0.0 | 1.8 +/- 0.0 | 0.8 +/- 0.0 | 0.58x | 0.45x | 1.3 +/- 0.0 | 1.7 +/- 0.0 | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 1.0 +/- 0.0 | 0.0 +/- 0.0 | 8.9 | 6.8 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 9.3 +/- 0.0 | 11.8 +/- 0.0 | 0.9 +/- 0.0 | 0.09x | 0.07x | 9.2 +/- 0.0 | 11.7 +/- 0.0 | 0.8 +/- 0.0 | 8.8 +/- 0.0 | 11.0 +/- 0.0 | 0.1 +/- 0.0 | 8.9 | 6.8 |
| Repeated novel graph programs - ten (`construct_std__novel_ten`) | distinct graph programs | 4.1 +/- 0.0 | 4.8 +/- 0.0 | 1.8 +/- 0.0 | 0.43x | 0.37x | 4.0 +/- 0.0 | 4.7 +/- 0.0 | 1.7 +/- 0.0 | 3.5 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 8.9 | 6.8 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 0.7 +/- 0.0 | 0.9 +/- 0.0 | 0.8 +/- 0.0 | 1.28x | 0.95x | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 0.7 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 5.9 | 13.2 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 1.4 +/- 0.0 | 1.9 +/- 0.0 | 1.2 +/- 0.0 | 0.81x | 0.61x | 1.3 +/- 0.0 | 1.8 +/- 0.0 | 1.1 +/- 0.0 | 0.8 +/- 0.0 | 1.0 +/- 0.0 | 0.3 +/- 0.0 | 5.9 | 13.2 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 4.9 +/- 0.0 | 6.3 +/- 0.0 | 2.3 +/- 0.0 | 0.48x | 0.37x | 4.8 +/- 0.0 | 6.2 +/- 0.0 | 2.2 +/- 0.0 | 4.2 +/- 0.0 | 5.4 +/- 0.0 | 1.5 +/- 0.0 | 5.9 | 13.2 |

## Value storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 2.14x | 1.16x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.7 +/- 0.0 | 1.5 | 1.0 |
| String arithmetic - long (`type_str_std__long`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 2.00x | 1.09x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 1.5 | 1.0 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 2.04x | 1.10x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 1.3 | 1.0 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 2.11x | 1.14x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.7 +/- 0.0 | 1.3 | 1.0 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 0.7 +/- 0.0 | 1.94x | 0.98x | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.8 | 1.4 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 1.90x | 0.93x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.8 | 1.4 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 1.70x | 0.93x | 0.3 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.8 | 1.4 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 0.7 +/- 0.0 | 2.09x | 0.97x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.7 | 6.5 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 0.4 +/- 0.0 | 0.8 +/- 0.0 | 0.8 +/- 0.0 | 2.15x | 1.03x | 0.2 +/- 0.0 | 0.6 +/- 0.0 | 0.7 +/- 0.0 | 0.7 | 23.2 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 0.7 +/- 0.0 | 1.1 +/- 0.0 | 1.0 +/- 0.0 | 1.49x | 0.92x | 0.6 +/- 0.0 | 1.0 +/- 0.0 | 0.9 +/- 0.0 | 0.7 | 89.7 |

## Keyed collections

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 1.0 +/- 0.0 | 1.0 +/- 0.0 | 0.8 +/- 0.0 | 0.81x | 0.82x | 0.9 +/- 0.0 | 0.9 +/- 0.0 | 0.7 +/- 0.0 | 1.9 | 49.8 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 3.1 +/- 0.0 | 2.5 +/- 0.0 | 1.2 +/- 0.0 | 0.38x | 0.46x | 3.0 +/- 0.0 | 2.4 +/- 0.0 | 1.1 +/- 0.0 | 1.9 | 223.7 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 6.0 +/- 0.0 | 4.6 +/- 0.0 | 1.7 +/- 0.0 | 0.29x | 0.37x | 5.9 +/- 0.0 | 4.5 +/- 0.0 | 1.6 +/- 0.0 | 1.9 | 445.2 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 25.1 +/- 0.0 | 19.2 +/- 0.0 | 5.8 +/- 0.0 | 0.23x | 0.30x | 19.8 +/- 0.9 | 19.0 +/- 0.0 | 4.4 +/- 0.0 | 1.9 | 2090.9 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 153.6 +/- 0.1 | 113.0 +/- 0.0 | 26.7 +/- 0.0 | 0.17x | 0.24x | 41.0 +/- 0.1 | 85.6 +/- 0.1 | 19.5 +/- 0.0 | 1.9 | 12001.6 |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 615.4 +/- 0.2 | 450.4 +/- 0.0 | 105.0 +/- 0.0 | 0.17x | 0.23x | 38.3 +/- 2.8 | 293.6 +/- 0.5 | 74.6 +/- 0.0 | 1.9 | 47997.1 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 15.8 +/- 0.1 | 5.1 +/- 0.0 | 1.8 +/- 0.0 | 0.11x | 0.35x | 13.9 +/- 0.1 | 5.0 +/- 0.0 | 1.7 +/- 0.0 | 1.9 | 668.0 |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 28.6 +/- 0.0 | 7.2 +/- 0.0 | 1.8 +/- 0.0 | 0.06x | 0.25x | 25.8 +/- 1.0 | 7.1 +/- 0.0 | 1.7 +/- 0.0 | 1.9 | 668.0 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 31.0 +/- 0.0 | 9.6 +/- 0.0 | 1.8 +/- 0.0 | 0.06x | 0.19x | 28.7 +/- 0.9 | 9.5 +/- 0.0 | 1.7 +/- 0.0 | 1.9 | 668.0 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 11.4 +/- 0.0 | 8.4 +/- 0.0 | 2.8 +/- 0.0 | 0.24x | 0.33x | 8.4 +/- 0.0 | 8.3 +/- 0.0 | 2.6 +/- 0.0 | 1.9 | 1120.9 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 50.6 +/- 0.0 | 36.4 +/- 0.0 | 11.2 +/- 0.1 | 0.22x | 0.31x | 33.4 +/- 0.2 | 36.3 +/- 0.0 | 8.6 +/- 0.1 | 1.9 | 4649.5 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 101.0 +/- 0.0 | 72.2 +/- 0.0 | 21.9 +/- 0.0 | 0.22x | 0.30x | 43.0 +/- 1.9 | 72.1 +/- 0.0 | 16.0 +/- 0.0 | 1.9 | 9295.3 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | 49.4 +/- 0.0 | 20.9 +/- 0.0 | 6.2 +/- 0.0 | 0.13x | 0.30x | 37.9 +/- 0.3 | 20.8 +/- 0.0 | 4.8 +/- 0.0 | 1.9 | 2110.8 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 55.8 +/- 0.1 | 31.9 +/- 0.0 | 6.2 +/- 0.0 | 0.11x | 0.19x | 44.9 +/- 0.6 | 31.8 +/- 0.0 | 4.8 +/- 0.0 | 1.9 | 2110.8 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 106.1 +/- 0.0 | 65.4 +/- 0.0 | 6.2 +/- 0.1 | 0.06x | 0.09x | 64.3 +/- 0.2 | 65.2 +/- 0.0 | 4.8 +/- 0.1 | 1.9 | 2110.8 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 8.5 +/- 0.0 | 4.6 +/- 0.0 | 1.8 +/- 0.0 | 0.21x | 0.38x | 8.3 +/- 0.0 | 4.5 +/- 0.0 | 1.7 +/- 0.0 | 1.9 | 445.3 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 11.8 +/- 0.0 | 4.6 +/- 0.0 | 1.8 +/- 0.0 | 0.15x | 0.38x | 11.7 +/- 0.0 | 4.5 +/- 0.0 | 1.7 +/- 0.0 | 1.9 | 445.3 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 11.8 +/- 0.0 | 4.6 +/- 0.0 | 1.8 +/- 0.0 | 0.15x | 0.38x | 9.8 +/- 0.0 | 4.5 +/- 0.0 | 1.7 +/- 0.0 | 1.9 | 445.3 |

## Nested graphs

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 0.7 +/- 0.0 | 0.8 +/- 0.0 | 0.7 +/- 0.0 | 1.00x | 0.87x | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 0.6 +/- 0.0 | 1.4 | 20.6 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 1.7 +/- 0.0 | 1.4 +/- 0.0 | 0.8 +/- 0.0 | 0.49x | 0.58x | 1.6 +/- 0.0 | 1.3 +/- 0.0 | 0.7 +/- 0.0 | 1.4 | 85.5 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 3.0 +/- 0.0 | 2.4 +/- 0.0 | 1.0 +/- 0.0 | 0.34x | 0.43x | 2.9 +/- 0.0 | 2.3 +/- 0.0 | 0.9 +/- 0.0 | 1.4 | 170.1 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 14.7 +/- 0.0 | 1.7 +/- 0.0 | 1.1 +/- 0.0 | 0.07x | 0.63x | 14.6 +/- 0.0 | 1.6 +/- 0.0 | 1.0 +/- 0.0 | 3.2 | 169.9 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 24.1 +/- 0.0 | 4.7 +/- 0.0 | 1.9 +/- 0.0 | 0.08x | 0.40x | 24.0 +/- 0.0 | 4.6 +/- 0.0 | 1.8 +/- 0.0 | 3.2 | 670.5 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 33.0 +/- 0.0 | 8.7 +/- 0.0 | 2.9 +/- 0.0 | 0.09x | 0.33x | 32.9 +/- 0.0 | 8.6 +/- 0.0 | 2.7 +/- 0.0 | 3.2 | 1337.7 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | 5.3 +/- 0.0 | 1.7 +/- 0.0 | 1.1 +/- 0.0 | 0.20x | 0.64x | 5.2 +/- 0.0 | 1.6 +/- 0.0 | 1.0 +/- 0.0 | 2.1 | 157.1 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | 7.2 +/- 0.0 | 3.4 +/- 0.0 | 1.6 +/- 0.0 | 0.22x | 0.47x | 7.1 +/- 0.0 | 3.3 +/- 0.0 | 1.5 +/- 0.0 | 2.1 | 549.9 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 10.2 +/- 0.0 | 5.4 +/- 0.0 | 1.8 +/- 0.0 | 0.18x | 0.34x | 10.0 +/- 0.0 | 5.3 +/- 0.0 | 1.7 +/- 0.0 | 2.1 | 1083.8 |

## Services

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 0.8 +/- 0.0 | 1.56x | 0.99x | 0.4 +/- 0.0 | 0.7 +/- 0.0 | 0.7 +/- 0.0 | 2.5 | 9.3 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 0.7 +/- 0.0 | 0.9 +/- 0.0 | 0.8 +/- 0.0 | 1.21x | 0.89x | 0.5 +/- 0.0 | 0.8 +/- 0.0 | 0.7 +/- 0.0 | 5.9 | 13.2 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 1.3 +/- 0.0 | 1.4 +/- 0.0 | 1.0 +/- 0.0 | 0.79x | 0.73x | 1.1 +/- 0.0 | 1.2 +/- 0.0 | 0.9 +/- 0.0 | 19.4 | 34.8 |

## hg_cpp dynamic storage

| profile | axis | Python peak delta | hgraph C++ peak delta | hg_cpp peak delta | hg/Python | hg/hgraph C++ | Python retained | hgraph C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | N/A | N/A | 2.2 +/- 0.0 | N/A | N/A | N/A | N/A | 2.1 +/- 0.0 | 1.7 | 69.7 |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | N/A | N/A | 2.7 +/- 0.1 | N/A | N/A | N/A | N/A | 2.6 +/- 0.1 | 1.7 | 275.4 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | N/A | N/A | 4.5 +/- 0.0 | N/A | N/A | N/A | N/A | 3.8 +/- 0.0 | 1.7 | 1098.1 |

## hg_cpp retained runtime registry growth

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
| `service_adaptor_py__repeat_once` | 15.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `service_adaptor_py__repeat_ten` | 87.0 +/- 0.0 | 11.0 +/- 0.0 | 22.0 +/- 0.0 | 1.0 +/- 0.0 | 203.0 +/- 0.0 |
| `service_adaptor_py__repeat_fifty` | 407.0 +/- 0.0 | 51.0 +/- 0.0 | 102.0 +/- 0.0 | 1.0 +/- 0.0 | 603.0 +/- 0.0 |
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
| `tsd_dense_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_dense_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_dense_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_sparse_large_capacity_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_sparse_large_capacity_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_sparse_large_capacity_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_churn_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_churn_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_churn_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_capacity_growth_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_capacity_growth_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_capacity_growth_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_clear_repopulate_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_clear_repopulate_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_clear_repopulate_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_key_reactivation_std__short` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_key_reactivation_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `tsd_key_reactivation_std__long` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__small` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 59.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__medium` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 59.0 +/- 0.0 |
| `reduce_tsd_nested_graph_std__large` | 5.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 59.0 +/- 0.0 |
| `switch_keyed_collection_std__small` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 97.0 +/- 0.0 |
| `switch_keyed_collection_std__medium` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 97.0 +/- 0.0 |
| `switch_keyed_collection_std__large` | 11.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 97.0 +/- 0.0 |
| `mesh_std__small` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 107.0 +/- 0.0 |
| `mesh_std__medium` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 107.0 +/- 0.0 |
| `mesh_std__large` | 14.0 +/- 0.0 | 5.0 +/- 0.0 | 10.0 +/- 0.0 | 1.0 +/- 0.0 | 107.0 +/- 0.0 |
| `service_adaptor_py__small` | 12.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 110.0 +/- 0.0 |
| `service_adaptor_py__medium` | 15.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 113.0 +/- 0.0 |
| `service_adaptor_py__large` | 27.0 +/- 0.0 | 2.0 +/- 0.0 | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 125.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__small` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 54.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__medium` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 54.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 54.0 +/- 0.0 |

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
