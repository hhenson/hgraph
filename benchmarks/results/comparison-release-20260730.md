# Release comparison — merged main `1e792614` (2026-07-30)

Clean three-mode benchmark of the runtime after the 2026-07 performance
campaign (through PR #199 cycle-scoped GIL and PR #200 RFC 0008 stage 5),
run from fresh worktrees of main with 5 fresh process samples per cell:

```
uv run python benchmarks/orchestrate.py \
    --suite core --suite diagnostic \
    --mode upstream-py --mode upstream-cpp --mode hg-cpp --samples 5
```

Modes: `upstream-py` = pure-python hgraph; `upstream-cpp` = the legacy C++
engine (hgraph 0.5.33, its first properly optimized release); `hg-cpp` =
this runtime. Ratios are same-machine medians (the cross-machine
normalisation convention). Scenarios shown with `-` are hg_cpp-only
features with no upstream counterpart.

**Headline**: hg_cpp is **x25.2 (mac) / x19.4 (linux)** the pure-python
engine and **x2.12 (mac) / x2.01 (linux)** the legacy C++ engine, geomean
over 56 comparable scenarios. Remaining sub-parity vs the legacy engine is
confined to the python-combiner/adaptor boundary family
(`reduce_tsd_python_combiner` x0.85/x0.75, and on linux `service_adaptor_py`
x0.85, `tsd_dense_py` x0.86) — the recorded RFC 0008 follow-up (prepared
routes for the erased node front-ends) owns these.

### macOS (Apple M4 Max, Python 3.14.6, unpinned)

Geomean speedup of hg_cpp over pure-python hgraph: **x25.22**; over the legacy C++ engine (0.5.33): **x2.12** (56 comparable scenarios).

| scenario | python (s) | old C++ (s) | hg_cpp (s) | py/hg | old/hg |
|---|---|---|---|---|---|
| construct_std | 1.0191 | 0.9662 | 0.0652 | x15.62 | x14.81 |
| construct_py | 0.1858 | 0.1509 | 0.0814 | x2.28 | x1.85 |
| tick_std | 1.1750 | 0.0619 | 0.0255 | x46.00 | x2.42 |
| tick_py | 0.4575 | 0.0265 | 0.0228 | x20.05 | x1.16 |
| scheduler_fan_out_std | 3.9642 | 0.1745 | 0.0667 | x59.42 | x2.62 |
| scheduler_fan_in_std | 4.9502 | 0.2333 | 0.0937 | x52.85 | x2.49 |
| scheduler_conflated_fixed_tsl_std | 1.2743 | 0.0722 | 0.0485 | x26.29 | x1.49 |
| python_generator_boundary | 0.1107 | 0.0134 | 0.0080 | x13.90 | x1.68 |
| python_sink_boundary | 0.1099 | 0.0138 | 0.0118 | x9.28 | x1.16 |
| type_int_std | 0.3463 | 0.0273 | 0.0121 | x28.51 | x2.25 |
| type_float_std | 0.3450 | 0.0271 | 0.0126 | x27.40 | x2.15 |
| type_str_std | 0.3473 | 0.0295 | 0.0137 | x25.37 | x2.15 |
| type_cs_std | 0.3386 | 0.0269 | 0.0149 | x22.76 | x1.81 |
| type_cs_py | 0.1727 | 0.0221 | 0.0183 | x9.46 | x1.21 |
| python_owned_pass_through_native | - | - | 0.0097 | - | - |
| python_owned_pass_through_python | - | - | 0.0084 | - | - |
| python_owned_project_one_native | - | - | 0.0115 | - | - |
| python_owned_project_one_python | - | - | 0.0108 | - | - |
| python_owned_project_several_native | - | - | 0.0165 | - | - |
| python_owned_project_several_python | - | - | 0.0182 | - | - |
| python_owned_construct_native | - | - | 0.0165 | - | - |
| python_owned_construct_python | - | - | 0.0295 | - | - |
| python_owned_dedup_native | - | - | 0.0114 | - | - |
| python_owned_dedup_python | - | - | 0.0121 | - | - |
| type_tsb_partial_fields_std | 0.8217 | 0.0606 | 0.0308 | x26.67 | x1.97 |
| type_tsw_append_evict_std | 0.2065 | 0.0175 | 0.0098 | x21.13 | x1.80 |
| tss_add_remove_std | 0.0771 | 0.0706 | 0.0592 | x1.30 | x1.19 |
| tsd_dense_std | 3.1408 | 0.1735 | 0.0936 | x33.56 | x1.85 |
| tsd_dense_py | 2.2944 | 0.1213 | 0.1049 | x21.88 | x1.16 |
| tsd_dense_source_std | 0.2664 | 0.0302 | 0.0153 | x17.39 | x1.97 |
| tsd_dense_map_std | 2.3322 | 0.1474 | 0.0753 | x30.98 | x1.96 |
| tsd_dense_reduce_std | 1.2363 | 0.0704 | 0.0347 | x35.63 | x2.03 |
| tsd_dense_strkeys_std | 3.1714 | 0.1888 | 0.1069 | x29.66 | x1.77 |
| tsd_sparse_std | 1.2673 | 0.0779 | 0.0378 | x33.54 | x2.06 |
| tsd_sparse_source_std | 0.0460 | 0.0126 | 0.0044 | x10.57 | x2.90 |
| tsd_sparse_map_std | 0.4523 | 0.0444 | 0.0281 | x16.11 | x1.58 |
| tsd_sparse_reduce_std | 0.8515 | 0.0455 | 0.0138 | x61.49 | x3.28 |
| tsd_sparse_large_capacity_std | 30.4799 | 1.6267 | 1.0857 | x28.07 | x1.50 |
| tsd_churn_std | 2.7675 | 0.1678 | 0.0570 | x48.52 | x2.94 |
| tsd_churn_py | 2.2068 | 0.1227 | 0.0442 | x49.96 | x2.78 |
| tsd_churn_source_std | 0.1089 | 0.0181 | 0.0033 | x32.75 | x5.46 |
| tsd_churn_map_std | 1.9938 | 0.1266 | 0.0511 | x39.02 | x2.48 |
| tsd_churn_reduce_std | 0.9359 | 0.0582 | 0.0083 | x113.31 | x7.04 |
| tsd_capacity_growth_std | 1.5622 | 0.1150 | 0.0525 | x29.75 | x2.19 |
| tsd_clear_repopulate_std | 28.7964 | 1.6268 | 0.6944 | x41.47 | x2.34 |
| tsd_key_reactivation_std | 1.3859 | 0.0908 | 0.0376 | x36.88 | x2.42 |
| tsd_two_input_union_std | 1.3718 | 0.0832 | 0.0427 | x32.16 | x1.95 |
| tsd_two_input_intersection_std | 0.4842 | 0.0380 | 0.0215 | x22.57 | x1.77 |
| tsd_explicit_key_set_std | 1.6990 | 0.1100 | 0.0180 | x94.60 | x6.13 |
| reduce_tsd_nested_graph_std | 1.2296 | 0.0726 | 0.0567 | x21.70 | x1.28 |
| reduce_tsd_python_combiner | 1.2298 | 0.0710 | 0.0834 | x14.75 | x0.85 |
| reduce_fixed_tsl_ordered_std | 0.6497 | 0.0442 | 0.0162 | x40.11 | x2.73 |
| reduce_tsd_without_zero_std | - | - | 0.0289 | - | - |
| reduce_dynamic_tsl_std | - | - | 0.0192 | - | - |
| switch_alternating_branch_sizes_std | 4.8124 | 0.2606 | 0.2199 | x21.89 | x1.19 |
| switch_keyed_collection_std | 38.7214 | 2.2239 | 0.9558 | x40.51 | x2.33 |
| mesh_std | 0.8014 | 0.0843 | 0.0232 | x34.48 | x3.63 |
| service_reference_std | 0.2921 | 0.0199 | 0.0108 | x26.97 | x1.84 |
| service_reference_py | 0.3740 | 0.0220 | 0.0136 | x27.43 | x1.62 |
| service_request_reply_std | 1.1350 | 0.0937 | 0.0401 | x28.30 | x2.34 |
| service_request_reply_py | 0.9280 | 0.0821 | 0.0444 | x20.89 | x1.85 |
| service_request_reply_multiple_paths_std | 0.7612 | 0.0788 | 0.0358 | x21.24 | x2.20 |
| service_subscription_std | 1.4602 | 0.1305 | 0.0316 | x46.27 | x4.13 |
| service_subscription_py | 1.7953 | 0.1500 | 0.0537 | x33.42 | x2.79 |
| adaptor_std | 0.1948 | 0.0204 | 0.0101 | x19.36 | x2.03 |
| adaptor_py | 0.1877 | 0.0185 | 0.0117 | x16.02 | x1.58 |
| service_adaptor_std | 0.7109 | 0.0489 | 0.0309 | x23.02 | x1.58 |
| service_adaptor_py | 0.5388 | 0.0372 | 0.0341 | x15.80 | x1.09 |

### Linux (Intel Core Ultra 7 155H, Python 3.14.4, taskset -c 4-9, quiet box)

Geomean speedup of hg_cpp over pure-python hgraph: **x19.37**; over the legacy C++ engine (0.5.33): **x2.01** (56 comparable scenarios).

| scenario | python (s) | old C++ (s) | hg_cpp (s) | py/hg | old/hg |
|---|---|---|---|---|---|
| construct_std | 1.3779 | 1.3594 | 0.0835 | x16.49 | x16.27 |
| construct_py | 0.2600 | 0.2299 | 0.1409 | x1.85 | x1.63 |
| tick_std | 1.4403 | 0.0955 | 0.0466 | x30.90 | x2.05 |
| tick_py | 0.5760 | 0.0414 | 0.0349 | x16.51 | x1.19 |
| scheduler_fan_out_std | 4.7455 | 0.2462 | 0.1341 | x35.39 | x1.84 |
| scheduler_fan_in_std | 5.9049 | 0.3270 | 0.1837 | x32.15 | x1.78 |
| scheduler_conflated_fixed_tsl_std | 1.5620 | 0.1079 | 0.0911 | x17.15 | x1.18 |
| python_generator_boundary | 0.1445 | 0.0223 | 0.0126 | x11.42 | x1.77 |
| python_sink_boundary | 0.1475 | 0.0224 | 0.0195 | x7.58 | x1.15 |
| type_int_std | 0.4396 | 0.0414 | 0.0209 | x21.04 | x1.98 |
| type_float_std | 0.4381 | 0.0416 | 0.0214 | x20.47 | x1.94 |
| type_str_std | 0.4450 | 0.0448 | 0.0222 | x20.05 | x2.02 |
| type_cs_std | 0.4324 | 0.0423 | 0.0261 | x16.54 | x1.62 |
| type_cs_py | 0.2244 | 0.0352 | 0.0284 | x7.90 | x1.24 |
| python_owned_pass_through_native | - | - | 0.0148 | - | - |
| python_owned_pass_through_python | - | - | 0.0122 | - | - |
| python_owned_project_one_native | - | - | 0.0189 | - | - |
| python_owned_project_one_python | - | - | 0.0178 | - | - |
| python_owned_project_several_native | - | - | 0.0288 | - | - |
| python_owned_project_several_python | - | - | 0.0308 | - | - |
| python_owned_construct_native | - | - | 0.0295 | - | - |
| python_owned_construct_python | - | - | 0.0475 | - | - |
| python_owned_dedup_native | - | - | 0.0193 | - | - |
| python_owned_dedup_python | - | - | 0.0190 | - | - |
| type_tsb_partial_fields_std | 1.0998 | 0.0906 | 0.0497 | x22.12 | x1.82 |
| type_tsw_append_evict_std | 0.2650 | 0.0295 | 0.0167 | x15.89 | x1.77 |
| tss_add_remove_std | 0.1184 | 0.0842 | 0.0753 | x1.57 | x1.12 |
| tsd_dense_std | 3.9152 | 0.2636 | 0.1676 | x23.35 | x1.57 |
| tsd_dense_py | 2.8654 | 0.1776 | 0.2060 | x13.91 | x0.86 |
| tsd_dense_source_std | 0.3489 | 0.0478 | 0.0268 | x13.02 | x1.78 |
| tsd_dense_map_std | 2.9017 | 0.2049 | 0.1261 | x23.01 | x1.63 |
| tsd_dense_reduce_std | 1.5555 | 0.1046 | 0.0694 | x22.42 | x1.51 |
| tsd_dense_strkeys_std | 3.9393 | 0.2941 | 0.1836 | x21.45 | x1.60 |
| tsd_sparse_std | 1.6069 | 0.1458 | 0.0622 | x25.82 | x2.34 |
| tsd_sparse_source_std | 0.0644 | 0.0210 | 0.0078 | x8.23 | x2.68 |
| tsd_sparse_map_std | 0.6372 | 0.0832 | 0.0463 | x13.76 | x1.80 |
| tsd_sparse_reduce_std | 1.0266 | 0.0726 | 0.0239 | x43.03 | x3.04 |
| tsd_sparse_large_capacity_std | 34.0070 | 3.4949 | 1.9021 | x17.88 | x1.84 |
| tsd_churn_std | 3.8260 | 0.2712 | 0.0888 | x43.08 | x3.05 |
| tsd_churn_py | 2.9779 | 0.1934 | 0.0705 | x42.21 | x2.74 |
| tsd_churn_source_std | 0.1625 | 0.0296 | 0.0057 | x28.75 | x5.24 |
| tsd_churn_map_std | 2.6872 | 0.2036 | 0.0776 | x34.65 | x2.63 |
| tsd_churn_reduce_std | 1.1986 | 0.0902 | 0.0159 | x75.58 | x5.69 |
| tsd_capacity_growth_std | 2.0529 | 0.2253 | 0.0960 | x21.38 | x2.35 |
| tsd_clear_repopulate_std | 39.4746 | 3.9639 | 1.0529 | x37.49 | x3.76 |
| tsd_key_reactivation_std | 1.8635 | 0.1444 | 0.0559 | x33.34 | x2.58 |
| tsd_two_input_union_std | 1.7331 | 0.1474 | 0.0702 | x24.70 | x2.10 |
| tsd_two_input_intersection_std | 0.6164 | 0.0619 | 0.0360 | x17.11 | x1.72 |
| tsd_explicit_key_set_std | 2.2386 | 0.1785 | 0.0304 | x73.66 | x5.87 |
| reduce_tsd_nested_graph_std | 1.5798 | 0.1050 | 0.0966 | x16.35 | x1.09 |
| reduce_tsd_python_combiner | 1.5485 | 0.1044 | 0.1399 | x11.07 | x0.75 |
| reduce_fixed_tsl_ordered_std | 0.8018 | 0.0656 | 0.0299 | x26.78 | x2.19 |
| reduce_tsd_without_zero_std | - | - | 0.0520 | - | - |
| reduce_dynamic_tsl_std | - | - | 0.0358 | - | - |
| switch_alternating_branch_sizes_std | 6.0685 | 0.4372 | 0.3055 | x19.86 | x1.43 |
| switch_keyed_collection_std | 51.9211 | 3.8053 | 1.5471 | x33.56 | x2.46 |
| mesh_std | 1.1255 | 0.1628 | 0.0346 | x32.52 | x4.70 |
| service_reference_std | 0.3655 | 0.0321 | 0.0173 | x21.17 | x1.86 |
| service_reference_py | 0.4725 | 0.0366 | 0.0229 | x20.59 | x1.60 |
| service_request_reply_std | 1.5347 | 0.1396 | 0.0720 | x21.31 | x1.94 |
| service_request_reply_py | 1.3000 | 0.1229 | 0.0847 | x15.36 | x1.45 |
| service_request_reply_multiple_paths_std | 1.0457 | 0.1184 | 0.0580 | x18.04 | x2.04 |
| service_subscription_std | 1.9205 | 0.2104 | 0.0598 | x32.12 | x3.52 |
| service_subscription_py | 2.3917 | 0.2662 | 0.1044 | x22.91 | x2.55 |
| adaptor_std | 0.2542 | 0.0340 | 0.0164 | x15.48 | x2.07 |
| adaptor_py | 0.2496 | 0.0309 | 0.0191 | x13.09 | x1.62 |
| service_adaptor_std | 0.8988 | 0.0740 | 0.0585 | x15.38 | x1.27 |
| service_adaptor_py | 0.6820 | 0.0586 | 0.0688 | x9.91 | x0.85 |
