# hgraph memory-utilisation matrix

- date: 2026-08-09T15:45:04+00:00
- host: Windows-10-10.0.19045-SP0 / Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- CPU: Intel64 Family 6 Model 158 Stepping 13, GenuineIntel
- Python: 3.14.7
- hg_cpp revision: 17a5b81bca91
- hg_cpp source fingerprint: e99eea3f4792ca4f3b105f14d98a98a6ae3fbb257207a04a1cd138fea5a0aa26
- fresh-process samples: 3
- RSS sampling interval: 5 ms
- modes: hg_cpp (`hg-cpp`)
- reused upstream baseline cells: 0

RSS values are medians in MiB; +/- is median absolute deviation. Peak delta is measured from the post-import/pre-run process state. Retained delta is measured after graph teardown and two Python GC passes.
GraphDiagnostics columns are a separate hg_cpp run and are native-accounted bytes, not RSS; they are intentionally absent from reference modes.

## Process floor

| mode | interpreter + psutil RSS | ready RSS | runtime load delta |
|---|---:|---:|---:|
| hg_cpp (`hg-cpp`) | 19.5 | 41.4 | 21.9 |

## Keyed collections

| profile | axis | hg_cpp peak delta | hg_cpp retained | planned KiB | peak dynamic KiB |
|---|---|---:|---:|---:|---:|
| Sparse retained capacity - large (`tsd_sparse_large_capacity_std__large`) | key capacity | 74.1 +/- 0.0 | 4.4 +/- 0.0 | 1.9 | 44034.1 |

## hg_cpp retained runtime registry growth

Counts are final-minus-pre-run cold-path cardinalities. They are process-lifetime structural records, not live graph instances.

| profile | node types | graph programs | graph types | executor types | all type records |
|---|---:|---:|---:|---:|---:|
| `tsd_sparse_large_capacity_std__large` | 8.0 +/- 0.0 | 3.0 +/- 0.0 | 6.0 +/- 0.0 | 1.0 +/- 0.0 | 77.0 +/- 0.0 |

## Interpretation contract

- `tsd_sparse_large_capacity_std__large`: peak should scale with retained key and child-slot capacity.
