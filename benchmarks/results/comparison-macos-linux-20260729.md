# macOS vs Linux normalized hgraph performance

- lower wall-clock time is better; normalized speed-up ratios are higher-is-better
- tuple format: `Python : legacy C++ : hg_cpp`, with Python fixed at `1.00`
- `hg_cpp / legacy` is the direct relative throughput factor (`legacy seconds / hg_cpp seconds`)
- macOS: Apple M4 Max / Apple Clang 21 / Python 3.14.6
- Linux: Intel Core Ultra 7 155H / GCC 15.2 / Python 3.14.4, pinned P-core
- upstream baseline: hgraph 0.5.31; reused until the upstream version or scenario pack changes
- five fresh-process samples for hg_cpp and all core baselines; macOS diagnostics also use five; Linux diagnostics reuse the complete earlier three-sample baseline

## Overall

| metric | macOS | Linux |
|---|---:|---:|
| comparable workloads | 56 | 56 |
| geometric mean legacy speed-up vs Python | 10.79x | 8.33x |
| geometric mean hg_cpp speed-up vs Python | 19.39x | 15.08x |
| geometric mean hg_cpp / legacy | 1.80x | 1.81x |
| hg_cpp vs legacy (>5% faster / parity / >5% slower) | 49 / 5 / 2 | 48 / 4 / 4 |

## By workload group

Geometric mean of `hg_cpp / legacy`; values above 1.0 mean hg_cpp is faster.

| group | workloads | macOS | Linux | Linux / macOS uplift |
|---|---:|---:|---:|---:|
| Graph construction | 2 | 4.65x | 5.35x | 1.15x |
| Scheduler | 5 | 1.26x | 1.07x | 0.85x |
| Python boundary | 2 | 1.23x | 1.22x | 1.00x |
| Value types | 8 | 1.37x | 1.35x | 0.99x |
| TSD - dense | 6 | 1.44x | 1.38x | 0.96x |
| TSD - sparse | 5 | 2.12x | 2.29x | 1.08x |
| TSD - key lifecycle | 11 | 2.86x | 3.30x | 1.15x |
| Reduce | 3 | 1.14x | 1.02x | 0.90x |
| Nested graphs | 3 | 2.00x | 2.56x | 1.28x |
| Services | 7 | 1.98x | 1.83x | 0.92x |
| Adaptors | 4 | 1.26x | 1.10x | 0.87x |

## Per workload

| workload | macOS Python : legacy : hg_cpp | macOS hg/legacy | Linux Python : legacy : hg_cpp | Linux hg/legacy | Linux / macOS uplift |
|---|---:|---:|---:|---:|---:|
| **Graph construction** |  |  |  |  |  |
| Wide/deep graph - native operators (`construct_std`) | 1.00 : 1.05 : 13.67 | 13.02x | 1.00 : 1.03 : 17.12 | 16.66x | 1.28x |
| Wide/deep graph - Python nodes (`construct_py`) | 1.00 : 1.23 : 2.03 | 1.66x | 1.00 : 1.08 : 1.86 | 1.72x | 1.04x |
| **Scheduler** |  |  |  |  |  |
| Feedback hot loop - native add (`tick_std`) | 1.00 : 17.55 : 27.76 | 1.58x | 1.00 : 14.03 : 18.42 | 1.31x | 0.83x |
| Five-node Python compute chain (`tick_py`) | 1.00 : 15.89 : 16.62 | 1.05x | 1.00 : 12.72 : 12.73 | 1.00x | 0.96x |
| One source fanning out to many sinks (`scheduler_fan_out_std`) | 1.00 : 20.82 : 28.55 | 1.37x | 1.00 : 16.76 : 18.50 | 1.10x | 0.80x |
| Many branches joining one output (`scheduler_fan_in_std`) | 1.00 : 19.42 : 25.99 | 1.34x | 1.00 : 16.11 : 17.06 | 1.06x | 0.79x |
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 1.00 : 16.18 : 16.87 | 1.04x | 1.00 : 13.44 : 12.14 | 0.90x | 0.87x |
| **Python boundary** |  |  |  |  |  |
| Python scalar generator to native sink (`python_generator_boundary`) | 1.00 : 7.44 : 10.92 | 1.47x | 1.00 : 6.12 : 9.16 | 1.50x | 1.02x |
| Python scalar generator to Python sink (`python_sink_boundary`) | 1.00 : 7.46 : 7.64 | 1.02x | 1.00 : 6.10 : 6.10 | 1.00x | 0.98x |
| **Value types** |  |  |  |  |  |
| Integer arithmetic (`type_int_std`) | 1.00 : 11.68 : 17.91 | 1.53x | 1.00 : 9.38 : 12.99 | 1.38x | 0.90x |
| Floating-point arithmetic (`type_float_std`) | 1.00 : 11.96 : 18.05 | 1.51x | 1.00 : 9.50 : 12.87 | 1.35x | 0.90x |
| String concatenation (`type_str_std`) | 1.00 : 11.13 : 16.23 | 1.46x | 1.00 : 8.90 : 12.44 | 1.40x | 0.96x |
| CompoundScalar field access - native operators (`type_cs_std`) | 1.00 : 11.67 : 15.28 | 1.31x | 1.00 : 9.23 : 11.94 | 1.29x | 0.99x |
| CompoundScalar crossing Python nodes (`type_cs_py`) | 1.00 : 7.51 : 7.83 | 1.04x | 1.00 : 6.09 : 6.79 | 1.11x | 1.07x |
| Bundle with partial field updates (`type_tsb_partial_fields_std`) | 1.00 : 12.30 : 17.83 | 1.45x | 1.00 : 10.74 : 14.67 | 1.37x | 0.94x |
| Tick window append and eviction (`type_tsw_append_evict_std`) | 1.00 : 10.83 : 15.01 | 1.39x | 1.00 : 8.35 : 12.35 | 1.48x | 1.07x |
| Set add/remove deltas (`tss_add_remove_std`) | 1.00 : 0.90 : 1.20 | 1.33x | 1.00 : 1.03 : 1.52 | 1.48x | 1.11x |
| **TSD - dense** |  |  |  |  |  |
| Map and reduce - native child graph (`tsd_dense_std`) | 1.00 : 16.69 : 22.75 | 1.36x | 1.00 : 13.12 : 16.26 | 1.24x | 0.91x |
| Map and reduce - Python map child (`tsd_dense_py`) | 1.00 : 17.39 : 17.24 | 0.99x | 1.00 : 13.61 : 12.68 | 0.93x | 0.94x |
| Source only (`tsd_dense_source_std`) | 1.00 : 7.72 : 15.15 | 1.96x | 1.00 : 5.77 : 12.73 | 2.21x | 1.12x |
| Map only (`tsd_dense_map_std`) | 1.00 : 14.61 : 21.55 | 1.47x | 1.00 : 12.16 : 15.72 | 1.29x | 0.88x |
| Reduce only (`tsd_dense_reduce_std`) | 1.00 : 15.32 : 25.93 | 1.69x | 1.00 : 12.04 : 19.49 | 1.62x | 0.96x |
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1.00 : 15.59 : 20.58 | 1.32x | 1.00 : 11.83 : 15.59 | 1.32x | 1.00x |
| **TSD - sparse** |  |  |  |  |  |
| Map and reduce with five updates per cycle (`tsd_sparse_std`) | 1.00 : 14.29 : 28.84 | 2.02x | 1.00 : 9.72 : 20.90 | 2.15x | 1.07x |
| Source only (`tsd_sparse_source_std`) | 1.00 : 3.36 : 9.34 | 2.78x | 1.00 : 2.94 : 7.94 | 2.70x | 0.97x |
| Map only (`tsd_sparse_map_std`) | 1.00 : 9.61 : 13.97 | 1.45x | 1.00 : 7.09 : 11.11 | 1.57x | 1.08x |
| Reduce only (`tsd_sparse_reduce_std`) | 1.00 : 15.93 : 55.72 | 3.50x | 1.00 : 11.53 : 39.06 | 3.39x | 0.97x |
| Large retained capacity with two updates per cycle (`tsd_sparse_large_capacity_std`) | 1.00 : 16.54 : 24.83 | 1.50x | 1.00 : 6.49 : 13.12 | 2.02x | 1.35x |
| **TSD - key lifecycle** |  |  |  |  |  |
| Map and reduce with key replacement (`tsd_churn_std`) | 1.00 : 14.64 : 40.37 | 2.76x | 1.00 : 11.04 : 35.98 | 3.26x | 1.18x |
| Python map with key replacement (`tsd_churn_py`) | 1.00 : 15.86 : 41.42 | 2.61x | 1.00 : 12.04 : 34.43 | 2.86x | 1.10x |
| Key replacement - source only (`tsd_churn_source_std`) | 1.00 : 5.17 : 28.81 | 5.57x | 1.00 : 4.49 : 27.00 | 6.01x | 1.08x |
| Key replacement - map only (`tsd_churn_map_std`) | 1.00 : 13.87 : 32.52 | 2.34x | 1.00 : 10.47 : 30.41 | 2.91x | 1.24x |
| Key replacement - reduce only (`tsd_churn_reduce_std`) | 1.00 : 14.11 : 85.36 | 6.05x | 1.00 : 10.62 : 63.04 | 5.94x | 0.98x |
| Monotonic key growth across capacity boundaries (`tsd_capacity_growth_std`) | 1.00 : 12.04 : 25.19 | 2.09x | 1.00 : 7.34 : 20.21 | 2.75x | 1.32x |
| Full clear followed by repopulation (`tsd_clear_repopulate_std`) | 1.00 : 15.21 : 34.43 | 2.26x | 1.00 : 8.90 : 29.72 | 3.34x | 1.48x |
| Remove and later recreate the same keys (`tsd_key_reactivation_std`) | 1.00 : 13.45 : 30.46 | 2.26x | 1.00 : 10.14 : 28.47 | 2.81x | 1.24x |
| Two-input map with union membership (`tsd_two_input_union_std`) | 1.00 : 14.52 : 26.96 | 1.86x | 1.00 : 10.23 : 20.56 | 2.01x | 1.08x |
| Two-input map with intersection membership (`tsd_two_input_intersection_std`) | 1.00 : 11.61 : 19.42 | 1.67x | 1.00 : 8.92 : 14.73 | 1.65x | 0.99x |
| Map driven by an explicit key set (`tsd_explicit_key_set_std`) | 1.00 : 13.58 : 76.19 | 5.61x | 1.00 : 10.15 : 61.54 | 6.06x | 1.08x |
| **Reduce** |  |  |  |  |  |
| Dense TSD with nested graph combiner (`reduce_tsd_nested_graph_std`) | 1.00 : 15.33 : 17.10 | 1.12x | 1.00 : 12.32 : 12.18 | 0.99x | 0.89x |
| Dense TSD with Python node combiner (`reduce_tsd_python_combiner`) | 1.00 : 15.62 : 12.54 | 0.80x | 1.00 : 11.75 : 9.23 | 0.79x | 0.98x |
| Ordered non-associative fixed-list reduction (`reduce_fixed_tsl_ordered_std`) | 1.00 : 13.66 : 22.53 | 1.65x | 1.00 : 11.23 : 15.37 | 1.37x | 0.83x |
| **Nested graphs** |  |  |  |  |  |
| Switch alternating small and large branches (`switch_alternating_branch_sizes_std`) | 1.00 : 16.22 : 18.03 | 1.11x | 1.00 : 11.15 : 16.39 | 1.47x | 1.32x |
| Switch returning a churning keyed collection (`switch_keyed_collection_std`) | 1.00 : 15.55 : 33.42 | 2.15x | 1.00 : 11.33 : 28.31 | 2.50x | 1.16x |
| Mesh with predecessor dependencies and key churn (`mesh_std`) | 1.00 : 8.71 : 29.22 | 3.36x | 1.00 : 5.92 : 27.01 | 4.56x | 1.36x |
| **Services** |  |  |  |  |  |
| Reference service - native implementation (`service_reference_std`) | 1.00 : 13.81 : 20.32 | 1.47x | 1.00 : 10.21 : 13.96 | 1.37x | 0.93x |
| Reference service - Python implementation (`service_reference_py`) | 1.00 : 15.16 : 20.41 | 1.35x | 1.00 : 11.66 : 14.54 | 1.25x | 0.93x |
| Request/reply service - native implementation (`service_request_reply_std`) | 1.00 : 10.89 : 21.40 | 1.97x | 1.00 : 9.46 : 16.14 | 1.71x | 0.87x |
| Request/reply service - Python implementation (`service_request_reply_py`) | 1.00 : 10.49 : 17.76 | 1.69x | 1.00 : 9.00 : 13.21 | 1.47x | 0.87x |
| Request/reply service across multiple paths (`service_request_reply_multiple_paths_std`) | 1.00 : 9.12 : 17.13 | 1.88x | 1.00 : 8.27 : 14.73 | 1.78x | 0.95x |
| Subscription service - native implementation (`service_subscription_std`) | 1.00 : 10.29 : 37.92 | 3.68x | 1.00 : 6.96 : 24.81 | 3.57x | 0.97x |
| Subscription service - Python implementation (`service_subscription_py`) | 1.00 : 10.82 : 27.96 | 2.58x | 1.00 : 6.99 : 17.48 | 2.50x | 0.97x |
| **Adaptors** |  |  |  |  |  |
| Duplex adaptor - native implementation (`adaptor_std`) | 1.00 : 8.85 : 14.08 | 1.59x | 1.00 : 6.99 : 10.49 | 1.50x | 0.94x |
| Duplex adaptor - Python implementation (`adaptor_py`) | 1.00 : 9.41 : 13.60 | 1.44x | 1.00 : 7.40 : 10.02 | 1.35x | 0.94x |
| Multiplexed service adaptor - native implementation (`service_adaptor_std`) | 1.00 : 13.55 : 16.29 | 1.20x | 1.00 : 11.07 : 10.60 | 0.96x | 0.80x |
| Multiplexed service adaptor - Python implementation (`service_adaptor_py`) | 1.00 : 13.49 : 12.28 | 0.91x | 1.00 : 10.77 : 8.14 | 0.76x | 0.83x |

## Interpretation

- `Linux / macOS uplift` near 1.0 means hg_cpp gains about the same amount over legacy C++ on both platforms.
- Values below 1.0 identify Linux-specific headroom; values above 1.0 mean Linux gains more than macOS.
- Absolute Mac and Linux seconds should not be compared directly because the CPU, ISA, compiler, allocator, and operating system differ.
