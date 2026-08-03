# hgraph memory-utilisation matrix

- date: 2026-08-02T19:30:51+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- hg_cpp revision: a4fccd9b5898+dirty
- hg_cpp source fingerprint: b982b71ce0c817bbfd6acf1e1f05879364eaf9b9ee1e7d2b716a1653e75702b6
- fresh-process samples: 5
- RSS sampling interval: 5 ms
- modes: hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
Inspector columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| hg_cpp (`hg-cpp`) | 17.5 | 71.3 | 53.8 |

## Keyed collections

| profile | axis | hg_cpp peak delta | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 68.9 +/- 0.0 | 38.2 +/- 0.0 | N/A | N/A |

## hg_cpp retained runtime registry growth

Counts are final-minus-pre-run cold-path cardinalities. They are process-lifetime structural records, not live graph instances.

| profile | node types | graph programs | graph types | executor types | all type records |
|---|---:|---:|---:|---:|---:|
| `tsd_sparse_large_capacity_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |

## Interpretation contract

- `tsd_sparse_large_capacity_std__large`: peak should scale with retained key and child-slot capacity.
