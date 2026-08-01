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
the two common management vectors, and the block-descriptor vector. Production
rows additionally include the fixed erased strategy object.

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

The production façade was rerun at 31 samples after replacing its closed
`std::variant` with the project's passive ops-table and explicit
`ErasedOwner` pattern. The direct rows isolate each representation; the
production rows add the alignment-selected, type-erased façade used by TSS,
TSD, nested graph storage, and mutable collection storage.

| Payload | Measurement | Representation B/slot | Allocations | Sequential live read ns | Random live read ns | Remove/resurrect ns per transition | Erase/reinsert ns per transition |
|---|---|---:|---:|---:|---:|---:|---:|
| aligned non-trivial, 8 B / align 8 | direct bitmap baseline | 16.251 | 7 | 0.396 | 1.017 | 1.183 | 3.422 |
| aligned non-trivial, 8 B / align 8 | direct tagged pointer | 16.001 | 5 | 0.332 | 0.995 | 1.312 | 2.696 |
| aligned non-trivial, 8 B / align 8 | production tagged façade | 16.002 | 6 | 1.508 | 1.909 | 2.548 | 4.129 |
| packed non-trivial, 9 B / align 1 | direct bitmap baseline | 17.251 | 7 | 0.382 | 1.005 | 1.190 | 3.331 |
| packed non-trivial, 9 B / align 1 | production bitmap façade | 17.253 | 8 | 1.520 | 1.984 | 2.587 | 4.602 |

For aligned storage, the production representation removes the two lifecycle
bitmap allocations but adds one fixed 64-byte strategy allocation. The net
result is one fewer allocation and a 0.249 B/slot saving at this capacity,
without changing payload stride. The weak-alignment strategy object is 112
bytes and one allocation; its per-slot footprint remains effectively the
bitmap footprint.

The indirect passive-ops call is measurable in these deliberately tiny loops.
Against the direct bitmap baseline, aligned production sequential/random reads
were 281%/88% slower, remove/resurrect was 115% slower, and erase/reinsert was
21% slower. The packed bitmap façade showed the same shape. This is the cost of
keeping semantic owners independent of the closed set of concrete strategies;
the type-erased boundary is not a zero-cost abstraction in an isolated slot
operation benchmark.

These figures isolate the boundary and therefore make it a larger fraction of
the measured work than it is for owners that also hash keys, publish observers,
run destructors, and maintain free/pending queues. They establish both the
fixed memory cost and the per-call dispatch cost of the reusable type-erased
boundary; an owner-level benchmark remains the appropriate follow-up before
further hot-path changes.

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

- Fresh macOS native Release suite: 1,344/1,344 passed.
- Fresh Linux native Release suite under GCC 15: 1,344/1,344 passed.
- Stable-ABI wheel built with Python 3.12 and tested from fresh Python 3.14
  environments on macOS and Linux: 1,760 passed, 10 skipped (`not wip`) on
  each platform.
- The macOS installed-SDK consumer compiled and passed using the public
  `StableSlotStore` header and both alignment strategies.
- Focused AppleClang ASan + UBSan lifecycle suite: 19/19 passed. Leak
  detection is not supported by the macOS ASan runtime and was disabled.
- The complete Linux Python compatibility suite passed under GCC 15 ASan +
  UBSan: 1,760 passed, 10 skipped. Leak detection was disabled for the
  process-wide Python run, following the documented sanitizer workflow.
- Debugger common-layer tests: 12/12 passed; the native debugger fixture also
  passed as part of the full Release suites, and the LLDB smoke test passed.
  The GDB smoke navigated the erased stable-slot collections before reaching
  an unrelated optimized-GCC fixture limitation: complete `TSInput` and
  `TSOutput` owning types were not emitted in its debug information.
- Sphinx documentation build with warnings treated as errors: passed.
