# hgraph memory-utilisation matrix

- date: 2026-07-31T16:02:45+00:00
- host: macOS-26.5.2-arm64-arm-64bit-Mach-O / arm
- CPU: Apple M4 Max
- Python: 3.14.6
- hg_cpp revision: b934704dddd5
- hg_cpp source fingerprint: e0deeba44edf67b7856d84377041d54d9892d2b8c8ff509fa68e191f88bf0a70
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: legacy C++ (`upstream-cpp`), hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
Inspector columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from legacy modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| legacy C++ (`upstream-cpp`) | 22.3 | 86.2 | 63.9 |
| hg_cpp (`hg-cpp`) | 22.3 | 63.5 | 41.1 |

## Static graph

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Wide/deep native graph - small (`construct_std__small`) | graph size | 1.3 +/- 0.0 | 2.5 +/- 0.0 | 1.85x | 1.3 +/- 0.0 | 2.5 +/- 0.0 | 42.5 | 0.0 |
| Wide/deep native graph - medium (`construct_std__medium`) | graph size | 4.7 +/- 0.0 | 5.8 +/- 0.0 | 1.23x | 4.7 +/- 0.0 | 5.8 +/- 0.0 | 172.8 | 0.0 |
| Wide/deep native graph - large (`construct_std__large`) | graph size | 21.6 +/- 1.1 | 19.0 +/- 0.1 | 0.88x | 20.6 +/- 0.1 | 19.0 +/- 0.1 | 673.4 | 0.0 |

## Bounded execution

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native scalar hot loop - short (`tick_std__short`) | duration | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 4.31x | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 0.9 | 0.0 |
| Native scalar hot loop - medium (`tick_std__medium`) | duration | 0.3 +/- 0.0 | 1.3 +/- 0.1 | 4.31x | 0.3 +/- 0.0 | 1.3 +/- 0.1 | 0.9 | 0.0 |
| Native scalar hot loop - long (`tick_std__long`) | duration | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 4.47x | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 0.9 | 0.0 |
| Python compute chain - short (`tick_py__short`) | duration | 0.3 +/- 0.0 | 1.4 +/- 0.1 | 4.74x | 0.3 +/- 0.0 | 1.4 +/- 0.1 | 1.7 | 0.0 |
| Python compute chain - medium (`tick_py__medium`) | duration | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 4.43x | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 1.7 | 0.0 |
| Python compute chain - long (`tick_py__long`) | duration | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 4.79x | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 1.7 | 0.0 |

## Process lifetime

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | legacy C++ first-to-last growth | hg_cpp first-to-last growth | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Repeated small graph - once (`construct_std__repeat_once`) | graph executions | 0.5 +/- 0.0 | 1.6 +/- 0.0 | 3.29x | 0.5 +/- 0.0 | 1.6 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 8.5 | 0.0 |
| Repeated small graph - ten (`construct_std__repeat_ten`) | graph executions | 1.5 +/- 0.0 | 2.6 +/- 0.0 | 1.74x | 1.5 +/- 0.0 | 2.6 +/- 0.0 | 1.0 +/- 0.0 | 1.0 +/- 0.0 | 8.5 | 0.0 |
| Repeated small graph - hundred (`construct_std__repeat_hundred`) | graph executions | 10.6 +/- 0.0 | 12.2 +/- 0.0 | 1.15x | 10.6 +/- 0.0 | 12.2 +/- 0.0 | 10.1 +/- 0.0 | 10.7 +/- 0.0 | 8.5 | 0.0 |
| Repeated service/adaptor graph - once (`service_adaptor_py__repeat_once`) | graph executions | 0.7 +/- 0.0 | 2.0 +/- 0.0 | 2.75x | 0.7 +/- 0.0 | 2.0 +/- 0.0 | 0.0 +/- 0.0 | 0.0 +/- 0.0 | 5.8 | 4.4 |
| Repeated service/adaptor graph - ten (`service_adaptor_py__repeat_ten`) | graph executions | 1.7 +/- 0.0 | 2.6 +/- 0.0 | 1.51x | 1.7 +/- 0.0 | 2.6 +/- 0.0 | 1.0 +/- 0.0 | 0.6 +/- 0.0 | 5.8 | 4.4 |
| Repeated service/adaptor graph - fifty (`service_adaptor_py__repeat_fifty`) | graph executions | 5.7 +/- 0.0 | 5.1 +/- 0.0 | 0.90x | 5.7 +/- 0.0 | 5.1 +/- 0.0 | 4.9 +/- 0.0 | 3.1 +/- 0.0 | 5.8 | 4.4 |

## Value storage

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| String arithmetic - short (`type_str_std__short`) | duration | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 3.86x | 0.3 +/- 0.0 | 1.3 +/- 0.0 | 1.4 | 0.0 |
| String arithmetic - long (`type_str_std__long`) | duration | 0.4 +/- 0.0 | 1.4 +/- 0.0 | 3.46x | 0.4 +/- 0.0 | 1.4 +/- 0.0 | 1.4 | 0.0 |
| CompoundScalar through Python - short (`type_cs_py__short`) | duration | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 4.52x | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 1.2 | 0.0 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 4.38x | 0.3 +/- 0.0 | 1.4 +/- 0.0 | 1.2 | 0.0 |
| Fixed tick window - short (`type_tsw_append_evict_std__short`) | duration | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 4.31x | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Fixed tick window - medium (`type_tsw_append_evict_std__medium`) | duration | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 4.04x | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 4.22x | 0.3 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - small (`tss_add_remove_std__small`) | live cardinality | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 3.52x | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - medium (`tss_add_remove_std__medium`) | live cardinality | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 3.89x | 0.4 +/- 0.0 | 1.5 +/- 0.0 | 0.7 | 0.0 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 0.8 +/- 0.0 | 1.9 +/- 0.0 | 2.43x | 0.8 +/- 0.0 | 1.9 +/- 0.0 | 0.7 | 0.0 |

## Keyed collections

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dense TSD map/reduce - small (`tsd_dense_std__small`) | cardinality | 1.0 +/- 0.0 | 1.8 +/- 0.0 | 1.87x | 1.0 +/- 0.0 | 1.8 +/- 0.0 | 2.1 | 32.9 |
| Dense TSD map/reduce - medium (`tsd_dense_std__medium`) | cardinality | 2.6 +/- 0.0 | 2.1 +/- 0.0 | 0.80x | 2.6 +/- 0.0 | 2.1 +/- 0.0 | 2.1 | 151.3 |
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 4.5 +/- 0.0 | 2.6 +/- 0.1 | 0.58x | 4.5 +/- 0.0 | 2.6 +/- 0.1 | 2.1 | 302.5 |
| Sparse retained capacity - small (`tsd_sparse_large_capacity_std__small`) | key capacity | 17.8 +/- 0.1 | 6.9 +/- 0.0 | 0.39x | 17.8 +/- 0.1 | 6.9 +/- 0.0 | 2.1 | 1408.4 |
| Sparse retained capacity - medium (`tsd_sparse_large_capacity_std__medium`) | key capacity | 108.7 +/- 0.1 | 28.0 +/- 0.0 | 0.26x | 92.4 +/- 0.1 | 28.0 +/- 0.0 | 2.1 | 8290.6 |
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 444.9 +/- 0.3 | 106.0 +/- 0.0 | 0.24x | 351.2 +/- 0.0 | 98.2 +/- 0.0 | 2.1 | 33162.2 |
| Bounded key churn - short (`tsd_churn_std__short`) | duration | 5.0 +/- 0.0 | 2.6 +/- 0.0 | 0.52x | 5.0 +/- 0.0 | 2.6 +/- 0.0 | 2.1 | 501.0 |
| Bounded key churn - medium (`tsd_churn_std__medium`) | duration | 7.1 +/- 0.0 | 2.8 +/- 0.0 | 0.39x | 7.1 +/- 0.0 | 2.8 +/- 0.0 | 2.1 | 501.0 |
| Bounded key churn - long (`tsd_churn_std__long`) | duration | 10.2 +/- 0.2 | 2.8 +/- 0.0 | 0.27x | 10.2 +/- 0.2 | 2.8 +/- 0.0 | 2.1 | 501.0 |
| Monotonic key growth - short (`tsd_capacity_growth_std__short`) | duration | 8.0 +/- 0.0 | 3.7 +/- 0.0 | 0.46x | 8.0 +/- 0.0 | 3.7 +/- 0.0 | 2.1 | 819.3 |
| Monotonic key growth - medium (`tsd_capacity_growth_std__medium`) | duration | 34.6 +/- 0.1 | 12.1 +/- 0.0 | 0.35x | 34.6 +/- 0.1 | 12.1 +/- 0.0 | 2.1 | 3279.8 |
| Monotonic key growth - long (`tsd_capacity_growth_std__long`) | duration | 68.8 +/- 0.0 | 23.2 +/- 0.0 | 0.34x | 68.8 +/- 0.0 | 23.2 +/- 0.0 | 2.1 | 6560.4 |
| Clear and repopulate - short (`tsd_clear_repopulate_std__short`) | duration | 21.2 +/- 0.1 | 7.3 +/- 0.0 | 0.34x | 21.2 +/- 0.1 | 7.3 +/- 0.0 | 2.1 | 1408.4 |
| Clear and repopulate - medium (`tsd_clear_repopulate_std__medium`) | duration | 31.8 +/- 0.0 | 7.2 +/- 0.0 | 0.23x | 31.8 +/- 0.0 | 7.2 +/- 0.0 | 2.1 | 1408.4 |
| Clear and repopulate - long (`tsd_clear_repopulate_std__long`) | duration | 75.5 +/- 0.0 | 7.3 +/- 0.1 | 0.10x | 75.5 +/- 0.0 | 7.3 +/- 0.1 | 2.1 | 1408.4 |
| Key reactivation - short (`tsd_key_reactivation_std__short`) | duration | 4.5 +/- 0.0 | 2.5 +/- 0.0 | 0.56x | 4.5 +/- 0.0 | 2.5 +/- 0.0 | 2.1 | 302.5 |
| Key reactivation - medium (`tsd_key_reactivation_std__medium`) | duration | 4.6 +/- 0.0 | 2.5 +/- 0.0 | 0.55x | 4.6 +/- 0.0 | 2.5 +/- 0.0 | 2.1 | 302.5 |
| Key reactivation - long (`tsd_key_reactivation_std__long`) | duration | 4.5 +/- 0.0 | 2.7 +/- 0.0 | 0.59x | 4.5 +/- 0.0 | 2.7 +/- 0.0 | 2.1 | 302.5 |

## Nested graphs

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TSD nested-graph reduce - small (`reduce_tsd_nested_graph_std__small`) | cardinality | 0.7 +/- 0.0 | 1.7 +/- 0.0 | 2.30x | 0.7 +/- 0.0 | 1.7 +/- 0.0 | 1.5 | 13.0 |
| TSD nested-graph reduce - medium (`reduce_tsd_nested_graph_std__medium`) | cardinality | 1.5 +/- 0.1 | 1.8 +/- 0.0 | 1.18x | 1.5 +/- 0.1 | 1.8 +/- 0.0 | 1.5 | 52.0 |
| TSD nested-graph reduce - large (`reduce_tsd_nested_graph_std__large`) | cardinality | 2.5 +/- 0.0 | 2.0 +/- 0.0 | 0.79x | 2.5 +/- 0.0 | 2.0 +/- 0.0 | 1.5 | 104.0 |
| Keyed collection switch - small (`switch_keyed_collection_std__small`) | live cardinality | 1.8 +/- 0.0 | 2.2 +/- 0.1 | 1.23x | 1.8 +/- 0.0 | 2.2 +/- 0.1 | 3.5 | 125.2 |
| Keyed collection switch - medium (`switch_keyed_collection_std__medium`) | live cardinality | 4.9 +/- 0.1 | 3.2 +/- 0.0 | 0.65x | 4.9 +/- 0.1 | 3.2 +/- 0.0 | 3.5 | 501.0 |
| Keyed collection switch - large (`switch_keyed_collection_std__large`) | live cardinality | 8.6 +/- 0.0 | 4.1 +/- 0.0 | 0.48x | 8.6 +/- 0.0 | 4.1 +/- 0.0 | 3.5 | 1001.9 |
| Dependency mesh - small (`mesh_std__small`) | live cardinality | 1.8 +/- 0.0 | 2.1 +/- 0.0 | 1.17x | 1.8 +/- 0.0 | 2.1 +/- 0.0 | 2.4 | 110.8 |
| Dependency mesh - medium (`mesh_std__medium`) | live cardinality | 3.2 +/- 0.1 | 2.7 +/- 0.0 | 0.83x | 3.2 +/- 0.1 | 2.7 +/- 0.0 | 2.4 | 409.6 |
| Dependency mesh - large (`mesh_std__large`) | live cardinality | 5.1 +/- 0.0 | 3.6 +/- 0.0 | 0.70x | 5.1 +/- 0.0 | 3.6 +/- 0.0 | 2.4 | 819.2 |

## Services

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Multiplexed Python service adaptor - small (`service_adaptor_py__small`) | client count | 0.6 +/- 0.0 | 2.0 +/- 0.0 | 3.15x | 0.6 +/- 0.0 | 2.0 +/- 0.0 | 2.5 | 4.4 |
| Multiplexed Python service adaptor - medium (`service_adaptor_py__medium`) | client count | 0.8 +/- 0.0 | 2.1 +/- 0.0 | 2.77x | 0.8 +/- 0.0 | 2.1 +/- 0.0 | 5.8 | 4.4 |
| Multiplexed Python service adaptor - large (`service_adaptor_py__large`) | client count | 1.3 +/- 0.0 | 2.3 +/- 0.0 | 1.82x | 1.3 +/- 0.0 | 2.3 +/- 0.0 | 18.7 | 8.9 |

## hg_cpp dynamic storage

| profile | axis | legacy C++ peak delta | hg_cpp peak delta | hg/baseline | legacy C++ retained | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dynamic TSL map/reduce - small (`reduce_dynamic_tsl_std__small`) | initial capacity | N/A | 2.0 +/- 0.0 | N/A | N/A | 2.0 +/- 0.0 | 1.9 | 44.0 |
| Dynamic TSL map/reduce - medium (`reduce_dynamic_tsl_std__medium`) | initial capacity | N/A | 2.2 +/- 0.0 | N/A | N/A | 2.2 +/- 0.0 | 1.9 | 176.0 |
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | N/A | 4.3 +/- 0.0 | N/A | N/A | 4.3 +/- 0.0 | 1.9 | 704.1 |

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
