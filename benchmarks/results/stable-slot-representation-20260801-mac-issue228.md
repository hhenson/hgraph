# Stable-slot lifecycle representation trial

- Issue: [#228](https://github.com/hhenson/hg_cpp/issues/228)
- Date: 2026-08-01T02:14:21Z
- Host: macOS 26.5.2, Apple M4 Max (arm64)
- Compiler: AppleClang 21.0.0, `-O3 -DNDEBUG`
- Prototype base revision: `a08db52b9541a87c78b85c28c52cc9960e128112`
- Capacity: 65,536 slots
- Samples: 15 in one process; reported timings are medians

This is a test-only prototype. It reuses `StableSlotBlock` for chained stable
payload allocations and compares:

1. the current-shape pointer table plus constructed/live bitmaps;
2. a two-bit tagged pointer (`00` live, `01` pending erase, `10` staged,
   `11` free);
3. parent-tracked trivial storage, which removes the constructed bitmap; and
4. one lifecycle byte stored immediately after a weakly aligned non-trivial
   payload.

All variants implement stable growth, staged-publication rollback,
remove/resurrect, deferred erase, and slot reuse.

## Memory and timing matrix

`Representation B/slot` includes payload stride, the slot pointer/lifecycle
index, and block descriptors. `Total B/slot` additionally includes the common
prototype free-slot and pending-erase vectors, both pre-reserved to capacity.
Allocation counts include the payload block, slot index/bitmap allocations,
the two common management vectors, and the block-descriptor vector.

| Payload | Representation | Stride | Representation B/slot | Total B/slot | Allocations | Sequential live read ns | Random live read ns | Remove/resurrect ns per transition | Erase/reinsert ns per transition |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| aligned non-trivial, 8 B / align 8 | bitmap | 8 | 16.251 | 32.251 | 7 | 0.468 | 0.993 | 1.082 | 2.739 |
| aligned non-trivial, 8 B / align 8 | tagged-8 | 8 | 16.001 | 32.001 | 5 | 0.310 | 0.889 | 1.203 | 2.609 |
| packed trivial, 9 B / align 1 | bitmap | 9 | 17.251 | 33.251 | 7 | 0.378 | 0.926 | 1.133 | 2.814 |
| packed trivial, 9 B / align 1 | parent-tracked | 9 | 17.126 | 33.126 | 6 | 0.373 | 0.928 | 1.453 | 3.257 |
| packed trivial, 9 B / align 1 | tagged-8 | 16 | 24.001 | 40.001 | 5 | 0.325 | 0.918 | 1.225 | 2.684 |
| packed non-trivial, 9 B / align 1 | bitmap | 9 | 17.251 | 33.251 | 7 | 0.375 | 0.938 | 1.163 | 3.594 |
| packed non-trivial, 9 B / align 1 | state byte | 10 | 18.001 | 34.001 | 5 | 0.355 | 1.007 | 2.314 | 2.754 |
| packed non-trivial, 9 B / align 1 | tagged-4 | 12 | 20.001 | 36.001 | 5 | 0.355 | 0.927 | 1.176 | 4.997 |
| packed non-trivial, 9 B / align 1 | tagged-8 | 16 | 24.001 | 40.001 | 5 | 0.352 | 0.933 | 1.241 | 2.757 |

## Initial conclusions

- The tagged-pointer representation is the leading candidate when the payload
  is already pointer-aligned. It removes 0.25 B/slot and two allocations in
  this model. Here it improved sequential live reads by 34% and random live
  reads by 10%, while remove/resurrect was 11% slower.
- Parent-tracked trivial storage is the byte-minimising weak-alignment choice.
  It saves 0.125 B/slot and one allocation without padding the payload. The
  lifecycle mutation paths were slower in this run, so this should remain a
  compile-time specialization rather than the universal representation.
- The state byte does what the proposal intended relative to forced
  pointer-alignment: it uses 18.001 B/slot versus 24.001 B/slot for tagged-8
  and removes the standalone lifecycle allocations. It does **not** beat the
  compact two-bitmap baseline on total bytes: one byte per slot costs more than
  two bits per slot, producing 18.001 versus 17.251 B/slot here.
- State-byte remove/resurrect was roughly twice the bitmap cost. Its erase path
  was faster, and its single index allocation may still be useful when
  allocation count/locality matters more than the extra 0.75 B/slot.
- The tagged-4 erase/reinsert result remained materially slower on repeat. It
  is an exploratory negative control; the proposed production boundary should
  remain natural `size_t`/pointer alignment unless a separate investigation
  explains this result.

The prototype's compile-time footprint selector therefore chooses tagged
pointers for naturally pointer-aligned values, parent-tracked state for weakly
aligned trivial values, and retains bitmaps for weakly aligned non-trivial
values. The explicit state byte remains available for further locality and
allocation-count experiments rather than being selected on byte footprint.

## Production façade verification

The implemented `StableSlotStore` deliberately uses the simpler two-way
policy selected for #228: naturally pointer-aligned layouts use tagged
pointers, while every weaker alignment retains the existing bitmap
representation without padding. Parent-tracked trivial values and the inline
state byte remain benchmark-only alternatives.

The production façade was rerun at 31 samples after migrating the real slot
owners. The direct rows isolate each representation; the production rows add
the alignment-selected, type-erased façade used by TSS, TSD, nested graph
storage, and mutable collection storage.

| Payload | Measurement | Representation B/slot | Allocations | Sequential live read ns | Random live read ns | Remove/resurrect ns per transition | Erase/reinsert ns per transition |
|---|---|---:|---:|---:|---:|---:|---:|
| aligned non-trivial, 8 B / align 8 | direct bitmap baseline | 16.251 | 7 | 0.382 | 1.019 | 1.058 | 3.463 |
| aligned non-trivial, 8 B / align 8 | direct tagged pointer | 16.001 | 5 | 0.336 | 1.016 | 1.320 | 2.711 |
| aligned non-trivial, 8 B / align 8 | production tagged façade | 16.001 | 5 | 0.377 | 1.023 | 1.129 | 5.049 |
| packed non-trivial, 9 B / align 1 | direct bitmap baseline | 17.251 | 7 | 0.388 | 1.013 | 1.346 | 3.179 |
| packed non-trivial, 9 B / align 1 | production bitmap façade | 17.251 | 7 | 0.382 | 1.012 | 1.934 | 5.225 |

For aligned storage, the production representation removes the two lifecycle
bitmap allocations and 0.25 B/slot without changing payload stride. Live-read
cost is effectively unchanged from the bitmap baseline in this run. The raw
transition microbenchmarks expose façade dispatch overhead: remove/resurrect
was 6.7% slower and erase/reinsert was 45.8% slower. The weak-alignment façade
preserves the bitmap footprint and read cost, with the same dispatch overhead
visible when lifecycle transitions are measured in isolation.

These mutation figures are conservative for the actual owners because the
microbenchmark excludes key hashing, observer publication, destructor work,
and free/pending queue maintenance. They establish the cost of the reusable
type-erased boundary itself; an owner-level benchmark is the appropriate
follow-up if lifecycle transitions become measurable in an application hot
path.

## Reproduction

```sh
cmake --preset cpp --fresh
cmake --build --preset cpp --target hgraph_stable_slot_representation_perf --parallel
./cmake-build-cpp/tests/cpp/hgraph_stable_slot_representation_perf
```

Useful controls:

```sh
HGRAPH_STABLE_SLOT_PERF_CAPACITY=65536 \
HGRAPH_STABLE_SLOT_PERF_SAMPLES=31 \
HGRAPH_STABLE_SLOT_PERF_FILTER=aligned8 \
./cmake-build-cpp/tests/cpp/hgraph_stable_slot_representation_perf
```

The benchmark intentionally excludes the key-to-slot hash index and complete
TSS/TSD/TSL wiring. The production rows do exercise the reusable store façade
now used by those owners.

## Validation

Historical prototype validation:

- Prototype lifecycle coverage: 108 assertions in 8 test cases.
- Full native Release suite: 1,339/1,339 passed.
- Stable-ABI wheel built with Python 3.12 and tested with Python 3.14.6:
  1,754 passed, 10 skipped (`not wip`).
- Focused AppleClang ASan + UBSan run: all prototype tests passed.

Production implementation validation after migrating the real owners:

- Fresh macOS native Release suite: 1,343/1,343 passed.
- Fresh Linux native Release suite under GCC 15: 1,343/1,343 passed.
- Stable-ABI wheel built with Python 3.12 and tested from fresh Python 3.14
  environments on macOS and Linux: 1,760 passed, 10 skipped (`not wip`) on
  each platform.
- The macOS installed-SDK consumer compiled and passed using the public
  `StableSlotStore` header and both alignment strategies.
- Focused AppleClang ASan + UBSan lifecycle suite: 200/200 passed. Leak
  detection is not supported by the macOS ASan runtime and was disabled.
- The complete Linux Python compatibility suite passed under GCC 15 ASan +
  UBSan: 1,760 passed, 10 skipped. Leak detection was disabled for the
  process-wide Python run, following the documented sanitizer workflow.
- Debugger common-layer tests: 12/12 passed; the native debugger fixture also
  passed as part of the full Release suite.
- Sphinx documentation build with warnings treated as errors: passed.
