# Lazy Python owner scheduler — 2026-08-03

The Linux Python-heavy profiles showed that the fast compute path constructed
and copied a complete `NodeScheduler` for every time-series argument on every
callback.  The common case only reads the time-series value; it does not ask
for `owning_node`.

This change keeps scheduler state in the fast node's planned storage, but
passes an empty scheduler-support marker through each evaluation.  A
`PyTimeSeries` now constructs the `NodeScheduler` from its live consumer node
only when Python requests `owning_node`.  The executor's coarse, one-GIL-guard
per phase design is unchanged.  The implementation uses neither thread-local
nor process-global state.

## Controlled Linux result

Both revisions were clean Release/LTO builds of `05bcef558d02` on the physical
`hg-linux` host, pinned to CPU 2, using Python 3.12.13 and 15 fresh processes
per cell.  Values are median seconds +/- median absolute deviation.

| scenario | main | lazy scheduler | change | legacy C++ | remaining vs legacy |
|---|---:|---:|---:|---:|---:|
| `tsd_dense_py` | 0.206531 +/- 0.001112 | 0.192219 +/- 0.000602 | -6.93% | 0.180323 +/- 0.000656 | 6.60% slower |
| `reduce_tsd_python_combiner` | 0.134381 +/- 0.001082 | 0.120743 +/- 0.000509 | -10.15% | 0.105747 +/- 0.001756 | 14.18% slower |
| `service_adaptor_py` | 0.065769 +/- 0.000619 | 0.064055 +/- 0.000615 | -2.61% | 0.055808 +/- 0.000270 | 14.78% slower |

Source fingerprints were `718d35d320b65e953c50f2e68f347721282e7476534ffc36c940bd2e6684123c`
for main and `2440c5700406101837273eafc03119c944f8e345dd3982023616730ef58f934b`
for the candidate.  `gprofng` no longer reported the eager `NodeView::graph`,
`graph_value`, and `evaluation_clock` calls in the hot loop.

## macOS parity snapshot

The candidate and released legacy C++ engine were also measured on an Apple
M4 Max with Python 3.12.10 and 15 fresh processes.  This is a parity snapshot,
not a controlled candidate-versus-main attribution.

| scenario | candidate | legacy C++ | candidate vs legacy |
|---|---:|---:|---:|
| `tsd_dense_py` | 0.105782 +/- 0.001341 | 0.119307 +/- 0.000985 | 11.34% faster |
| `reduce_tsd_python_combiner` | 0.071853 +/- 0.001204 | 0.068460 +/- 0.000297 | 4.96% slower |
| `service_adaptor_py` | 0.033676 +/- 0.000602 | 0.034455 +/- 0.000563 | 2.26% faster |

## Validation

- macOS: all 1,431 native tests passed; the stable-ABI wheel built with
  Python 3.12 and all 1,884 non-WIP compatibility tests passed with Python
  3.14 (11 skipped).
- Physical `hg-linux`: all 1,431 native tests passed; the stable-ABI wheel
  built with Python 3.12 and all 1,884 non-WIP compatibility tests passed with
  Python 3.14 (11 skipped).
- Existing ownership coverage verifies that a fast-compute input's
  `owning_node.notify_next_cycle()` retains the callback scheduler semantics.

## Next profiling targets

The scheduler construction was material for dense-map and reducer workloads,
but explains little of the remaining service-adaptor gap.  The next campaign
should separately measure:

1. result publication and Python-to-native value conversion in
   `apply_py_result`;
2. keyed notification/scheduling work in the dense map path;
3. service-adaptor routing independently from the generic Python callback
   boundary.

An experiment replacing the unwind cleanup guard with a normal scope-exit
guard was flat to 1.4% slower and was discarded.
