# hgraph performance matrix

- date: 2026-08-09T17:10:12+00:00
- host: Linux-7.0.0-28-generic-x86_64-with-glibc2.43 / x86_64
- CPU: Intel(R) Core(TM) Ultra 7 155H
- Python: 3.14.4
- reference baseline: hgraph 0.5.41 (published wheel)
- reference wheel: hgraph-0.5.41-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl
- reference SHA-256: c24da699910c3eb44019a38a0fb293557ec707b48a8e8ab5b3e5fd8b0be2db7d
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 10
- modes: Python (`upstream-py`)
- reused fixed baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs Python.
C++-first-only sections are tracked without a 0.5 comparison.

## Scheduler

| workload | cycles | Python |
|---|---|---|
| Eight notifications conflated into one reducer (`scheduler_conflated_fixed_tsl_std`) | 20000 | 1.550s +/- 0.017s |
