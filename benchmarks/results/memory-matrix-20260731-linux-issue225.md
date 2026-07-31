# hgraph memory-utilisation matrix

- date: 2026-07-31T21:02:01+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- hg_cpp revision: unknown+dirty
- hg_cpp source fingerprint: 98e7f53b493affa5bc89b662c7d52a401606f68755605d0aad625c61da4dd5b9
- fresh-process samples: 1
- RSS sampling interval: 5 ms
- modes: hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
Inspector columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| hg_cpp (`hg-cpp`) | 17.5 | 72.9 | 55.4 |

## Value storage

| profile | axis | hg_cpp peak delta | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|
| String arithmetic - long (`type_str_std__long`) | duration | 0.7 +/- 0.0 | 0.6 +/- 0.0 | 1.5 | 0.2 |
| CompoundScalar through Python - long (`type_cs_py__long`) | duration | 0.7 +/- 0.0 | 0.5 +/- 0.0 | 1.3 | 0.1 |
| Fixed tick window - long (`type_tsw_append_evict_std__long`) | duration | 0.6 +/- 0.0 | 0.4 +/- 0.0 | 0.8 | 1.1 |
| Set add/remove - large (`tss_add_remove_std__large`) | live cardinality | 0.8 +/- 0.0 | 0.7 +/- 0.0 | 0.7 | 89.8 |

## Keyed collections

| profile | axis | hg_cpp peak delta | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|
| Dense TSD map/reduce - large (`tsd_dense_std__large`) | cardinality | 1.7 +/- 0.0 | 1.6 +/- 0.0 | 2.2 | 356.6 |

## hg_cpp dynamic storage

| profile | axis | hg_cpp peak delta | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|
| Dynamic TSL map/reduce - large (`reduce_dynamic_tsl_std__large`) | initial capacity | 5.2 +/- 0.0 | 4.5 +/- 0.0 | 2.0 | 828.3 |

## hg_cpp retained runtime registry growth

Counts are final-minus-pre-run cold-path cardinalities. They are process-lifetime structural records, not live graph instances.

| profile | node types | graph programs | graph types | executor types | all type records |
|---|---:|---:|---:|---:|---:|
| `type_str_std__long` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 25.0 +/- 0.0 |
| `type_cs_py__long` | 4.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 42.0 +/- 0.0 |
| `type_tsw_append_evict_std__long` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 28.0 +/- 0.0 |
| `tss_add_remove_std__large` | 3.0 +/- 0.0 | 1.0 +/- 0.0 | 2.0 +/- 0.0 | 1.0 +/- 0.0 | 34.0 +/- 0.0 |
| `tsd_dense_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 74.0 +/- 0.0 |
| `reduce_dynamic_tsl_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 54.0 +/- 0.0 |

## Interpretation contract

- `type_str_std__long`: temporary value memory should remain bounded across cycles.
- `type_cs_py__long`: bridge storage should remain bounded across cycles.
- `type_tsw_append_evict_std__long`: the 64-element window should remain bounded as evictions continue.
- `tss_add_remove_std__large`: peak should scale with live set cardinality; duration is fixed.
- `tsd_dense_std__large`: peak native storage should scale with simultaneously live keys.
- `reduce_dynamic_tsl_std__large`: native slot storage should scale with list capacity.
