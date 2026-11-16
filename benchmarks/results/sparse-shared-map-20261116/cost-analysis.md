# Where the collection regression came from

The measured penalty grows with retained collection size. It is negligible at 16–64 keys, then grows to approximately 20–27% at 16,384 keys. The notification change is not responsible for this penalty. The map-only diagnostic binary reproduces it even in the reduction-only graph.

## Controlled size sweep

Used the same sparse source with five changed elements per cycle, 20,000 cycles and five fresh-process samples at each size. Measured source-only, map-only, reduce-only and map-plus-reduce graphs. Version order was seeded and randomized within each repetition. These are whole-run timings, so setup and teardown remain included. Existing `benchmarks/runner.py` reproduces each cell with `--cycle-scale 10 --size-scale KEYS/2000` and the corresponding scenario.

The following reduction-only results isolate the penalty from map execution. “Map change only” restores both target-link files from the original baseline; “notification change only” restores `map_node.cpp`. Both variants are diagnostic builds, not proposed changes. All samples for all four graph shapes are in [cost-analysis.json](cost-analysis.json).

| Keys | Baseline µs/cycle | Initial PR ratio | Map change only | Notification change only |
|---:|---:|---:|---:|---:|
| 16 | 1.067 | 1.013× | 1.007× | 0.994× |
| 64 | 1.124 | 1.023× | 1.002× | 1.015× |
| 256 | 1.302 | 1.058× | 1.064× | 1.016× |
| 1,024 | 1.953 | 1.114× | 1.123× | 0.997× |
| 4,096 | 4.500 | 1.198× | 1.202× | 0.996× |
| 16,384 | 14.267 | 1.270× | 1.271× | 1.000× |

At 1,024–16,384 keys, the initial candidate adds roughly **0.22–0.24 ns per retained slot per cycle** in this graph. That tiny per-slot penalty becomes visible because the empty-removal path visits every slot; it is not a fixed charge confined to small collections.

A separate clean rebuild of the original baseline reproduced its speed (0.287 versus 0.283 seconds for the large reduction case). The source-only pipeline remains near baseline across all sizes.

## Profile and source explanation

Native sampling of the 16,384-key reduction graph identified the filtered removed-key iterator in `reconcile_dict_collection`, together with `TSSContextBase<TSDSlotStorage, false>::set_range_predicate<Removed>`. This loop/predicate accounts for about 88% of baseline main-thread samples and 91% of initial-candidate samples. The wider reconciliation call accounts for about 91% and 92%, respectively.

Both map and reduce already ask `next_removed_slot()` for the first removal. If it returned the no-child sentinel, the old `else` still entered `removed_keys()`. An empty filtered range does not necessarily have a zero storage limit: `Range::iterator::advance_to_match()` walks every retained slot, invoking the removed-membership predicate, before discovering that there are no matches. That gives a value-only tick a full-capacity scan. Map-plus-reduce can pay it at both stages.

This scan existed before the PR. The map-only diagnostic changes no reduction source code and has the same hot-loop/predicate instruction sequence, with different linked addresses. The slowdown is therefore consistent with a code-placement effect in the unchanged scan, rather than more map evaluations or more notifications. The precise CPU fetch/cache/branch mechanism was not measured with hardware counters; code placement is the supported explanation, not a proven microarchitectural diagnosis. The avoidable scan itself is established by source, sampling and its size dependence.

## Fix and confirmation

Commit `5deb2bc88` changes the fallback guard in map and reduce to `else if (first_removed_slot != TS_DATA_NO_CHILD_ID)`. A real sampled forwarding removal can still name a previous-source slot that is not marked removed in the current source, so it still takes the filtered-range fallback. No-removal ticks skip the range entirely. Existing removal, invalidation, recreation and source-retarget tests continue to pass.

| Keys | Original baseline µs/cycle | Fixed µs/cycle | Fixed/baseline |
|---:|---:|---:|---:|
| 16 | 1.039 | 1.024 | 0.986× |
| 64 | 1.126 | 1.045 | 0.928× |
| 256 | 1.327 | 1.124 | 0.847× |
| 1,024 | 1.960 | 1.141 | 0.582× |
| 4,096 | 4.542 | 1.321 | 0.291× |
| 16,384 | 14.274 | 1.870 | 0.131× |

The steady-state direct-map control with 8,192 source keys now measures 1.34 µs/update at 250 mapped keys and 1.40 µs at 4,000, compared with the original baseline’s 7.71 and 7.83 µs. The shared-lookup case remains fast at 1.23 and 1.29 µs/update. These measurements exclude initialization and teardown.

The complete 84-scenario matrix was rerun on macOS and native Linux with three alternating fresh-process samples per version. The previously regressing collection cases are now faster than the original baseline. The two initially slower Linux construction cases returned to 0.997× and 1.004× in seven repeats. Raw timings and remaining small variations are retained below; these measurements do not prove performance neutrality for every possible graph.

| Scenario | macOS fixed/baseline | Linux fixed/baseline |
|---|---:|---:|
| `construct_std` | 0.987× | 1.009× |
| `construct_py` | 1.002× | 1.082× |
| `construct_higher_order_py` | 0.983× | 1.028× |
| `construct_dispatch_cases` | 1.031× | 1.082× |
| `construct_overloads` | 1.009× | 1.074× |
| `tick_std` | 0.988× | 1.073× |
| `tick_py` | 0.975× | 0.838× |
| `scheduler_fan_out_std` | 1.020× | 0.994× |
| `scheduler_fan_in_std` | 1.030× | 0.958× |
| `scheduler_conflated_fixed_tsl_std` | 0.990× | 0.970× |
| `python_generator_boundary` | 0.990× | 1.057× |
| `python_sink_boundary` | 0.967× | 1.013× |
| `python_global_state_boundary` | 1.051× | 1.033× |
| `type_int_std` | 1.001× | 1.065× |
| `type_float_std` | 0.988× | 1.057× |
| `type_str_std` | 1.015× | 0.951× |
| `type_cs_std` | 0.980× | 0.955× |
| `type_cs_py` | 0.993× | 1.077× |
| `python_owned_pass_through_native` | 0.999× | 1.036× |
| `python_owned_pass_through_python` | 1.012× | 1.045× |
| `python_owned_project_one_native` | 1.003× | 1.006× |
| `python_owned_project_one_python` | 0.997× | 1.042× |
| `python_owned_project_several_native` | 1.007× | 0.908× |
| `python_owned_project_several_python` | 0.979× | 1.058× |
| `python_owned_construct_native` | 0.984× | 1.077× |
| `python_owned_construct_python` | 1.017× | 1.008× |
| `python_owned_dedup_native` | 1.025× | 1.073× |
| `python_owned_dedup_python` | 1.014× | 1.062× |
| `type_tsb_partial_fields_std` | 0.994× | 1.057× |
| `type_tsw_append_evict_std` | 0.999× | 1.019× |
| `tss_add_remove_std` | 0.982× | 1.051× |
| `tsd_dense_std` | 1.011× | 0.990× |
| `tsd_dense_py` | 1.009× | 0.988× |
| `tsd_dense_source_std` | 1.013× | 0.947× |
| `tsd_dense_map_std` | 1.010× | 1.020× |
| `tsd_dense_reduce_std` | 0.999× | 1.047× |
| `tsd_dense_strkeys_std` | 1.011× | 0.957× |
| `tsd_sparse_std` | 0.670× | 0.472× |
| `tsd_sparse_source_std` | 1.011× | 1.007× |
| `tsd_sparse_map_std` | 0.767× | 0.683× |
| `tsd_sparse_reduce_std` | 0.538× | 0.432× |
| `tsd_sparse_large_capacity_std` | 0.280× | 0.266× |
| `tsd_shared_lookup_py` | 0.055× | 0.054× |
| `tsd_shared_unselected_py` | 0.044× | 0.043× |
| `tsd_churn_std` | 1.005× | 1.011× |
| `tsd_churn_py` | 0.992× | 1.010× |
| `tsd_churn_source_std` | 0.999× | 1.056× |
| `tsd_churn_map_std` | 0.973× | 1.003× |
| `tsd_churn_reduce_std` | 1.023× | 1.005× |
| `tsd_capacity_growth_std` | 0.789× | 0.777× |
| `tsd_clear_repopulate_std` | 0.999× | 1.006× |
| `tsd_key_reactivation_std` | 0.977× | 0.938× |
| `tsd_two_input_union_std` | 0.772× | 0.737× |
| `tsd_two_input_intersection_std` | 0.747× | 0.651× |
| `tsd_explicit_key_set_std` | 1.005× | 1.016× |
| `reduce_tsd_nested_graph_std` | 0.946× | 0.998× |
| `reduce_tsd_python_combiner` | 1.009× | 0.957× |
| `reduce_fixed_tsl_ordered_std` | 1.018× | 1.027× |
| `reduce_tsd_without_zero_std` | 0.962× | 1.008× |
| `reduce_dynamic_tsl_std` | 1.002× | 1.054× |
| `switch_alternating_branch_sizes_std` | 1.006× | 1.020× |
| `switch_keyed_collection_std` | 1.012× | 1.021× |
| `mesh_std` | 1.023× | 0.961× |
| `service_reference_std` | 0.980× | 1.056× |
| `service_reference_py` | 1.003× | 1.038× |
| `service_request_reply_std` | 0.985× | 1.001× |
| `service_request_reply_py` | 1.051× | 0.955× |
| `service_request_reply_multiple_paths_std` | 0.982× | 0.981× |
| `service_subscription_std` | 1.006× | 1.004× |
| `service_subscription_py` | 1.019× | 1.013× |
| `adaptor_std` | 0.978× | 1.002× |
| `adaptor_py` | 0.987× | 0.970× |
| `service_adaptor_std` | 1.021× | 0.947× |
| `service_adaptor_py` | 1.003× | 1.000× |
| `audit_convert_collect_std` | 1.004× | 0.994× |
| `audit_map_transform_std` | 0.977× | 0.999× |
| `audit_tsd_getitem_std` | 1.001× | 0.992× |
| `audit_eq_tuple_std` | 0.990× | 1.052× |
| `audit_string_match_std` | 1.015× | 0.953× |
| `audit_json_roundtrip_std` | 1.005× | 0.999× |
| `audit_ref_race_std` | 0.990× | 0.948× |
| `audit_stream_buffered_std` | 0.996× | 0.984× |
| `audit_take_drop_std` | 0.994× | 0.997× |
| `audit_running_mean_std` | 0.989× | 0.976× |

## Repeats of small residual differences

Reran all remaining initial slowdowns above 5% with seven samples and ten times the cycle count. Linux repeats inherit affinity to one allowed CPU to reduce placement noise; the macOS repeats use normal scheduling. These runs change only cycle count, not collection size.

| Scenario | Platform | Fixed/baseline |
|---|---|---:|
| `construct_overloads` | linux | 0.992× |
| `tick_std` | linux | 0.973× |
| `python_generator_boundary` | linux | 1.063× |
| `type_int_std` | linux | 0.985× |
| `type_float_std` | linux | 0.995× |
| `type_cs_py` | linux | 0.994× |
| `python_owned_project_several_python` | linux | 1.020× |
| `python_owned_construct_native` | linux | 1.017× |
| `python_owned_dedup_native` | linux | 1.011× |
| `python_owned_dedup_python` | linux | 1.010× |
| `type_tsb_partial_fields_std` | linux | 0.995× |
| `tss_add_remove_std` | linux | 0.990× |
| `tsd_churn_source_std` | linux | 1.008× |
| `reduce_dynamic_tsl_std` | linux | 1.010× |
| `service_reference_std` | linux | 1.017× |
| `audit_eq_tuple_std` | linux | 0.997× |
| `python_global_state_boundary` | macos | 0.985× |
| `service_request_reply_py` | macos | 0.999× |

The collection regressions are resolved. A separate Linux `python_generator_boundary` difference remains approximately **6% slower** (about 0.012 µs per cycle); it contains no collection. The subsequent [generator investigation](generator-analysis.md) isolates runtime code placement as the predominant contributor: a diagnostic relink of the same compiled inputs reduces the gap to about 0–1%, confirmed on another CPU and with process ASLR disabled. The shipping build retains its normal layout and the measured difference. The other repeated differences are approximately neutral or faster. This remaining result is retained explicitly rather than calling the entire change performance-neutral.

## Validation

- macOS and Linux: complete native acceptance suites, 2,469 passed each; full non-WIP Python 3.14 suites from Python 3.12 stable-ABI wheels, 4,221 passed and 11 skipped each.
- Windows/MSVC 19.51: all 2,451 native core tests passed.
- Linux ASan/LSan after the guard change: all 237 map/reduce/input tests passed, with leak detection enabled. The earlier complete sanitizer run and its four baseline-reproduced unrelated leak findings remain documented in the initial report.
- The subsequent main-branch integration changes only `uv.lock` (Tornado 6.6.1). Native and Python acceptance gates were rerun on the merged tree; the runtime text section remains byte-identical to the measured fixed binary.

The original report is retained as historical evidence. Its 13–27% collection regression applies to the first PR implementation, not the updated guard fix.
