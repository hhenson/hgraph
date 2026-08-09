# hgraph performance matrix

- date: 2026-08-09T14:35:47+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- compiler: c++ (Ubuntu 14.3.0-14ubuntu1) 14.3.0
- hg_cpp revision: 17a5b81bca91
- hg_cpp source fingerprint: fb5d49c9b61a226bce0c2a1f268556e2a3ffcc13babcec3a90d692f49767da36
- hg_cpp build type: Release
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 10
- modes: Python (`upstream-py`)
- reused upstream baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs Python.
hg_cpp-only sections are tracked without an upstream comparison.

## TSD - dense

| workload | cycles | Python |
|---|---|---|
| String-key map and reduce (`tsd_dense_strkeys_std`) | 1000 | 3.951s +/- 0.026s |
