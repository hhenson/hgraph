Memory utilisation and accounting
=================================

Purpose
-------

This document is the initial static ownership audit and measurement contract
for graph memory. It is intended to direct optimisation work from reproducible
evidence, not to establish a platform-independent byte limit. The associated
campaign in ``benchmarks/memory_orchestrate.py`` produces:

* comparative, fresh-process RSS/USS/PSS samples for the fixed published 0.8.1
  release and current source, with 0.5.41 Python and legacy-C++ reconstruction
  modes;
* growth series for graph size, duration, cardinality, retained capacity, and
  client count;
* C++-first process-lifetime runtime-registry cardinalities for identical and
  intentionally novel graph wiring;
* an independent native ``GraphDiagnostics`` snapshot for planned and dynamic graph
  storage; and
* raw JSON containing every sample and the largest native dynamic owners.

Measurement model
-----------------

Process memory and native structural accounting answer different questions.
They must not be combined into one apparent total.

``peak RSS delta``
  Maximum resident set sampled during ``run_graph``, minus RSS immediately
  before the run. This includes graph wiring/execution/teardown allocations,
  Python objects, extension libraries, allocator metadata, and dirty pages.

``retained RSS delta``
  RSS after graph teardown and two Python garbage collections, minus pre-run
  RSS. A positive value may be live process state, an intentional intern/cache,
  Python or native retained ownership, or allocator pages that are free but
  have not been returned to the operating system. It is not by itself a leak.

``USS`` / ``PSS``
  Unique and proportionally shared set size, recorded where psutil and the
  platform expose them. USS is helpful when shared-library pages dominate RSS;
  neither metric replaces an ownership profile.

``GraphDiagnostics planned bytes``
  The root graph's checked runtime storage-plan size. Graph, node, input,
  output, and fixed structured time-series storage placed within that plan are
  deterministic native bytes.

``GraphDiagnostics dynamic bytes``
  Live/reserved nested-graph slot-store bytes reported by map, mesh, switch,
  TSL map, and reducer implementations, plus built-in value and TSData storage
  attributed through their common erased ops surfaces. This includes node
  state/scalars, atomic strings and bytes, compound/container payloads, and
  TS/TSB/TSL/TSS/TSD/TSW output ownership. The peak survives graph teardown in
  the owned snapshot. Dynamic reporting remains a lower bound, not a native
  heap total.

``runtime registry growth``
  Final-minus-pre-run counts for retained node runtime types, graph programs,
  graph runtime types, executor runtime types, and all common type records.
  These cold-path counts are captured without GraphDiagnostics through
  ``runtime_registry_snapshot()``. They describe process-lifetime structural
  ownership, not live graph instances or allocated bytes.

The process pass never attaches GraphDiagnostics. GraphDiagnostics retains an owned record
and strings for every graph/node it observes, so using it during RSS sampling
would change the quantity being measured. Each profile and each sample runs in
a fresh process so process-global state from one graph cannot contaminate the
next graph's delta. For repeated-lifecycle profiles, the GraphDiagnostics pass runs
one execution and reports the per-graph structural footprint; only the
uninstrumented process pass is used to infer cross-execution retention because
GraphDiagnostics would retain its own records across those executions.
The identical repeated-wiring profiles reuse one graph callable. The novel
control rebuilds ``construct_std`` with a different graph shape on each run,
so legitimate growth for new programs is visible separately from avoidable
growth when an unchanged program is wired again.

Static ownership audit
----------------------

Audit scope and method
~~~~~~~~~~~~~~~~~~~~~~

The initial audit traced every graph/node owner from its public erased handle
through its ``StoragePlan`` construction/destruction hooks, then followed all
dynamic allocation and capacity-reporting sites in the runtime and time-series
value layers. It separately inventoried function-static registries, intern
tables, explicit ``new``/``make_unique``/``make_shared`` calls, standard
containers, observer spill paths, and Python-owned bridge state. The primary
source areas were:

.. list-table::
   :header-rows: 1

   * - Ownership area
     - Primary implementation
     - Accounting status
   * - Erased owners and storage plans
     - ``types/utils/memory_utils.h`` and value/node/graph/executor owners
     - Plan layout is reported for the root graph
   * - Fixed time-series layout
     - ``metadata/ts_data_fixed_structured_*`` and ``time_series/ts_data``
     - Included when embedded in the graph plan
   * - Key/value slots and indices
     - ``key_slot_store.h``, ``value_slot_store.h``, and
       ``stable_slot_storage.h``
     - TSS/TSD key/value blocks, exact dense-index allocations, bitmaps, and
       slot bookkeeping are reported
   * - Nested graph policies
     - ``map_node.cpp``, ``reduce_node.cpp``, ``ordered_reduce_node.cpp``,
       ``switch_node.cpp``, ``mesh_node.cpp``, and ``tsl_map_node.cpp``
     - Implemented policies report slot live/reserved peaks
   * - Value containers
     - ``compact_storage.h``, ``mutable_container_ops.h``, and value builders
     - Raw buffers, validity storage, exact dense indices, retained capacity,
       and recursive element payloads are reported
   * - Target/observer state
     - ``ts_input/target_link*``, TS data observer sets, and slot observers
     - Inline state is planned; spill/transitions are not fully attributed
   * - Canonical registries
     - type/plan/ops registries, ``InternTable``, and policy context stores
     - Process-lifetime ownership; excluded from per-graph GraphDiagnostics totals
   * - Python bridge
     - ``py_nodes.cpp``, ``py_ports.cpp``, ``py_wiring.cpp``, and
       ``value_conversion.cpp``
     - Included in process metrics only

The audit did not infer heap size from container ``sizeof`` or add allocator
estimates to GraphDiagnostics totals. Such estimates look precise but miss capacity,
load factor, alignment, small-string optimisation, shared ownership, and
allocator metadata. The growth profiles provide the empirical bound until
each owner exposes a cold-path metric.

Planned graph memory
~~~~~~~~~~~~~~~~~~~~

The primary runtime follows the desired model: storage shape is planned at
wiring/type-realisation time and constructed in place for execution.
``MemoryUtils::StoragePlan`` carries layout, lifecycle operations, and
allocator operations; ``GraphValue``, ``NodeValue``, ``ExecutorValue``, and
``Value`` own erased storage through ``ErasedOwner``. The default owner has a
one-pointer inline budget and allocates larger plans once. The graph plan
therefore removes a large class of per-node heap allocations and gives
GraphDiagnostics a deterministic static byte count.

Fixed structured time-series data is synthesised as a composite plan rather
than assembled from independently owned children. The public handle sizes are
also explicitly constrained on 64-bit builds:

* all type references are one word;
* ``ValueView`` and typed pointers are two words;
* ``TSDataStorageRef`` is two words;
* ``TSData`` and ``TSDataOwnedStorage`` are three words (24 bytes);
* ``TSParentLink`` is at most three words;
* the common ``TSDataObserverSet`` is at most one word; and
* ``TSSDataLayout`` is five words while ``TSDDataLayout`` is ten words before
  their indirectly owned payloads.

These static assertions are valuable regression guards. They do not account
for pointees, container capacities, allocator headers, alignment padding, or
Python mirrors.

Stable dynamic slots
~~~~~~~~~~~~~~~~~~~~

Keyed nested graphs use ``StableSlotStore`` and ``InPlaceGraphSlotStore``.
Capacity growth appends a payload block so already published graph addresses
never move. A separate pointer table is replaced as capacity grows. The slot
store selects its lifecycle representation once from the payload plan:
pointer-aligned payloads carry state in two low pointer bits, while weaker
alignment retains one or two compact bitmaps according to the required state
model. The tagged live state is zero, so dereferencing a known-live pointer
does not require masking. A one-word semantic facade owns the selected heap
strategy through a tagged implementation pointer. Its private tag selects the
canonical nop, aligned, or bitmap path through an inline switch; the common
aligned path also uses tag zero. Reported reserved bytes are approximately:

.. code-block:: text

   capacity * (aligned entry-plus-graph stride + pointer size)
   + block descriptors
   + weak-alignment lifecycle-bitmap word capacity
   + one fixed erased strategy object

Map and reducers may maintain multiple banks/generations for safe structural
transition. Ordered reduce deliberately retains a previous chain for one
engine cycle. Mesh additionally owns dependency/ranking structures. Destroying
an entry stops and unsubscribes the graph before destruction; capacity remains
available for reuse until the owning node is destroyed. This is intentional
address-stability and churn behaviour, not automatically a leak. The bounded
churn, clear/repopulate, and reactivation profiles distinguish reuse from
monotonic growth.

TSS and TSD use the same allocation-level distinction. Their cold-path
``TSDataOps`` hook reports the occupied portion and retained capacity of stable
key/value blocks, pointer tables, block descriptors, slot and delta bitmaps,
free/pending vectors, observer-pointer vectors, and the dense hash index. The
index uses a contained counting allocator, so its values and rebound bucket
allocations are measured rather than inferred from load factor. TSD recursively
adds dynamic ownership reported by constructed child TSData, and both shapes
include allocations owned by constructed key values. The child's fixed storage
is already part of the parent value-slot stride and is therefore not counted
again.

The hook is sampled by GraphDiagnostics only. It adds no work to graph evaluation and
does not include allocator headers or fragmentation. A projected TSD key-set
reports its key-set allocation surface; the owning TSD root reports the full
dictionary and descendants. Node attribution samples only owned output roots,
not input aliases or projections.

TSW uses the same hook for its two aligned contiguous allocations: one for
values and one for evaluation timestamps. Live bytes are the occupied slot
count multiplied by the two physical strides; reserved bytes use the actual
retained capacity. A fixed window therefore exposes its configured capacity
from construction, while a duration window exposes geometric growth retained
after span eviction or clear. Dynamically allocated element payloads are left
to the value-storage attribution layer and are added recursively, including the
separately owned most-recently-evicted value.

Atomic TSData delegates payload attribution to its value binding. Fixed TSB and
fixed TSL recurse into their embedded children without recounting the fixed
child bytes already present in the graph plan. Dynamic TSL reports its handle
and ordinal vectors, each heap-only child allocation, and every child's
recursive ownership. This makes the same rule hold at every TSData shape:
static planned bytes are counted once and only separately allocated ownership
is added dynamically.

The value ops ABI exposes the corresponding cold-path hook. Built-in strings
and bytes distinguish small-string inline storage from their owned character
allocation. Fixed tuple, bundle, and list representations recurse through
their constructed fields; ``Any`` and ``Owned`` recurse through the active
owner. Compact and mutable list, set, map, queue, and cyclic-buffer storage
reports raw element/slot buffers, validity metadata, retained capacity, and
recursive payloads. Hash indices use counting allocators. A map key-set
projection reports only its keys and index, while the owning map adds the value
side. Node aggregation also includes scalar configuration and state values and
deduplicates aliased output roots.

``live_bytes`` describes occupied allocation content and required metadata;
``reserved_bytes`` describes the complete retained allocation capacity. An
invalid field may therefore still contribute live payload bytes if its
constructed storage retains a value. Neither number includes inline object
bytes, allocator headers, or fragmentation.

Unaccounted dynamic ownership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GraphDiagnostics currently reports native dynamic storage only where a node's
``storage_metrics`` implementation exposes it. The following material memory
categories are visible to process metrics but are not fully attributed in an
inspection snapshot:

* TSData observer spill storage;
* input target-link active trees and transient structural-transition objects;
* wiring builders, signatures, labels, service/adaptor registries, and result
  capture;
* Python callables, wrappers, generators, values, traceback state, and
  selectively materialised ``PyObject`` mirrors; and
* opaque or externally shared scalar payloads (for example Arrow-owned buffers)
  and extension value ops that do not provide the optional metric hook;
* allocator bookkeeping, fragmentation, thread stacks, shared libraries, and
  pages retained by the system allocator.

Consequently, ``planned + peak dynamic`` must never be labelled as total graph
memory. It is a precise lower-level decomposition of the categories currently
instrumented.

Process-lifetime registries
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Canonical metadata and operation tables require stable addresses and are
interned for process lifetime. The static scan identified intentional
long-lived registries for value/time-series metadata, storage plans, compact
and mutable container plans, debug descriptors, operator dispatch, node/graph/
executor type records, projection operations, service descriptors, empty
deltas, and node policy contexts. Many use ``InternTable`` or a heap-allocated
singleton specifically to avoid destruction-order hazards.

This design avoids repeated schema/ops construction and makes pointer identity
valid, but a process that continually introduces novel schemas, names,
operator signatures, or service descriptors can grow even after every graph
has stopped. The per-profile fresh-process baseline intentionally excludes
cross-graph accumulation. A future registry-growth campaign should repeatedly
wire unique and repeated schemas in one process, then use the existing test
registry reset hooks to separate deduplication defects from intentional
process-lifetime ownership.

Allocation and lifecycle risk review
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The audited graph/nested-graph paths use owning RAII types and explicit
placement lifecycle operations. Owners destroy constructed ``StableSlotStore``
payloads before releasing its blocks; the allocator-aware block deleter pairs
layout/alignment with the matching deallocation. Graph deletion follows stop,
unsubscribe, then erase.
No retired-object side container was found in these paths.

The highest-risk future changes are therefore not simple missing ``delete``
calls. They are semantic ownership errors: failing to unsubscribe before slot
reuse, preserving an erased reference to temporary storage, compacting stable
slots while pointers are published, mismatching Python/native ownership, or
adding a cache without a bounded key space. Changes in those areas require the
full native/Python suites plus Linux validation and ASan as described in
``debugging``.

Final measured baseline
-----------------------

The final controlled baseline compares the exact published hgraph 0.8.1 wheel
with hgraph 0.5.41, the head of ``release/0.5``. The macOS run used Python
3.14.6, three fresh-process samples, and an Apple M4 Max. The complete
cross-platform summary is committed as
``benchmarks/results/baseline-summary-20260809.md``; the macOS matrix and raw
samples are
``benchmarks/results/memory-baseline-20260809-macos.md`` and
``benchmarks/results/memory-baseline-20260809-macos.json``. Raw metadata records
both platform wheel filenames and SHA-256 digests.

The loaded 0.8.1 process floor was 64.3 MiB RSS, compared with 83.9 MiB for
Python hgraph and 86.3 MiB for legacy hgraph C++. Small run deltas favour both
reference runtimes because 0.8.1 touches an approximately 1.4--1.6 MiB
one-time set of pages on first graph use. Ratios at this scale are
allocator/page effects and should not drive node-level optimisation.

Larger graph and collection profiles favour 0.8.1, increasingly with scale:

.. list-table:: macOS peak RSS delta (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - Legacy C++
     - 0.8.1
     - 0.8.1/Python
     - 0.8.1/legacy C++
   * - Large wide/deep graph
     - 19.1
     - 21.5
     - 10.2
     - 0.53
     - 0.47
   * - Large dense TSD map/reduce
     - 5.8
     - 4.6
     - 2.4
     - 0.42
     - 0.53
   * - Large sparse retained capacity
     - 618.8
     - 445.1
     - 69.0
     - 0.11
     - 0.16
   * - Long monotonic key growth
     - 98.4
     - 68.8
     - 15.6
     - 0.16
     - 0.23
   * - Long clear/repopulate
     - 100.8
     - 75.7
     - 5.4
     - 0.05
     - 0.07
   * - Large keyed switch
     - 38.2
     - 8.7
     - 3.5
     - 0.09
     - 0.40
   * - Large dependency mesh
     - 10.0
     - 5.2
     - 3.1
     - 0.31
     - 0.60

The 0.8.1 duration series are bounded where the data structure is bounded.
Scalar loops, Python compute chains, strings, the fixed 64-item tick window,
key reactivation, and clear/repopulate have no material cycle-proportional
slope. Native structural storage is constant across the bounded churn,
reactivation, and clear/repopulate duration points. Monotonic key growth scales
intentionally; the large point reserves 8.1 MiB of native-accounted dynamic
storage while its process peak is 15.6 MiB.

The sparse-capacity profile also quantifies the attribution gap. At the large
point, GraphDiagnostics attributes 42.1 MiB of reserved dynamic storage while
process peak is 69.0 MiB. Approximately 26.9 MiB remains in key/value/index
payloads, wiring/Python state, or allocator overhead. This remaining gap is a
reason to extend structural accounting rather than tune the already-accounted
slot block in isolation.

Repeated graph lifecycle finding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same graph callable was wired, executed, stopped, and collected repeatedly
inside one process. The 0.5.41 post-GC series remain nearly linear, while 0.8.1
approaches a warm plateau:

.. list-table:: macOS first-to-last post-GC growth (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - Legacy C++
     - 0.8.1
   * - Small graph, 10 executions
     - 0.75
     - 1.02
     - 0.06
   * - Small graph, 100 executions
     - 8.75
     - 10.22
     - 0.30
   * - Service/adaptor graph, 10 executions
     - 0.73
     - 1.00
     - 0.05
   * - Service/adaptor graph, 50 executions
     - 4.09
     - 4.86
     - 0.14

RSS and USS growth are effectively identical for these series, so this is live
or allocator-retained private memory rather than shared-library accounting.
The published 0.8.1 release does not reproduce the earlier approximately
linear growth for repeated wiring of the same schema: 100 executions grow by
0.30 MiB on macOS, and the service/adaptor series grows by 0.14 MiB across 50
executions. Novel-schema wiring remains intentionally higher (0.87 MiB across
10 schemas) and is tracked separately. Keep these profiles as regression
guards for runtime-registry and policy-context deduplication; stable context
addresses referenced by published ops tables must still be preserved.

Native Linux cross-check
~~~~~~~~~~~~~~~~~~~~~~~~

The corresponding native x86_64 Linux baseline used the same published
releases, Python 3.14.4, and an Intel Core Ultra 7 155H. Its report and raw
samples are committed as ``benchmarks/results/memory-baseline-20260809-linux.md`` and
``benchmarks/results/memory-baseline-20260809-linux.json``. The loaded process
floors are 79.9 MiB for Python, 82.8 MiB for legacy C++, and 66.2 MiB for
0.8.1.

The absolute deltas differ from macOS, as expected from the loader and
allocator, but the material ratios agree:

.. list-table:: Linux peak RSS delta (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - Legacy C++
     - 0.8.1
     - 0.8.1/Python
     - 0.8.1/legacy C++
   * - Large wide/deep graph
     - 19.6
     - 20.8
     - 8.3
     - 0.42
     - 0.40
   * - Large dense TSD map/reduce
     - 5.9
     - 4.6
     - 2.9
     - 0.49
     - 0.64
   * - Large sparse retained capacity
     - 614.8
     - 450.3
     - 70.4
     - 0.12
     - 0.16
   * - Long monotonic key growth
     - 101.0
     - 72.2
     - 10.4
     - 0.10
     - 0.14
   * - Long clear/repopulate
     - 115.6
     - 65.4
     - 5.9
     - 0.05
     - 0.09
   * - Large keyed switch
     - 33.1
     - 8.7
     - 3.7
     - 0.11
     - 0.42
   * - Large dependency mesh
     - 10.1
     - 5.4
     - 3.6
     - 0.35
     - 0.66

Bounded repeated-lifecycle behaviour also reproduces: 100 small executions
grow by 8.73 MiB for Python, 10.99 MiB for legacy C++, and only 0.11 MiB for
0.8.1.

Native Windows cross-check
~~~~~~~~~~~~~~~~~~~~~~~~~~

The Windows 10 x86_64 baseline used the same published releases, Python
3.14.7, and an Intel Core i9-9980HK. Its report and raw samples are committed
as ``benchmarks/results/memory-baseline-20260809-windows.md`` and
``benchmarks/results/memory-baseline-20260809-windows.json``. The loaded
process floors are 67.3 MiB for Python, 69.4 MiB for legacy C++, and 41.3 MiB
for 0.8.1.

The scale-series direction also reproduces on Windows:

.. list-table:: Windows peak RSS delta (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - Legacy C++
     - 0.8.1
     - 0.8.1/Python
     - 0.8.1/legacy C++
   * - Large wide/deep graph
     - 19.9
     - 19.5
     - 9.8
     - 0.50
     - 0.51
   * - Large dense TSD map/reduce
     - 5.8
     - 5.5
     - 2.6
     - 0.44
     - 0.47
   * - Large sparse retained capacity
     - 609.2
     - 514.6
     - 74.5
     - 0.12
     - 0.14
   * - Long monotonic key growth
     - 99.5
     - 72.9
     - 17.1
     - 0.17
     - 0.23
   * - Long clear/repopulate
     - 82.9
     - 61.4
     - 6.1
     - 0.07
     - 0.10
   * - Large keyed switch
     - 37.5
     - 10.7
     - 3.5
     - 0.09
     - 0.33
   * - Large dependency mesh
     - 10.2
     - 6.6
     - 2.7
     - 0.27
     - 0.41

Across 100 small executions, first-to-last post-GC growth was 8.70 MiB for
Python, 13.88 MiB for legacy C++, and 0.10 MiB for 0.8.1. The service/adaptor
series likewise grew by 4.25 MiB, 6.95 MiB, and 0.08 MiB across 50 executions.
The large sparse inspector completed within the extended 1,800-second limit
and attributed 45,090,920 reserved bytes. Bounded repeated wiring is therefore
a cross-platform property of the published release rather than a macOS/Linux
allocator artifact.

Initial optimisation priorities
-------------------------------

Priorities should be re-ranked after each controlled baseline. The static
audit suggests this order:

1. Extend structural attribution before optimising opaque RSS. Add cold-path
   metrics for TSData observer spill storage, input target-link trees, and the
   selectively retained Python bridge state. Keep evaluation fast paths
   unchanged when no GraphDiagnostics is attached.
2. Use the cardinality and monotonic-growth profiles to calculate bytes per
   live/reserved key for map, reduce, mesh, and dynamic TSL. Investigate the
   pointer-table plus block overhead and bank/generation multiplicity where it
   materially exceeds payload size.
3. Use duration profiles as leak/boundedness guards. A statistically material
   slope for scalar, fixed-window, churn, reactivation, or clear/repopulate
   workloads is higher priority than a one-time import or allocator step.
4. Preserve bounded repeated wiring. Keep process-lifetime registry and
   policy-context cardinality counters in the regression profiles, deduplicate
   before allocating published backing records, and preserve stable addresses
   for records referenced by live graphs. Keep novel-schema wiring as a
   separate intentional-growth axis.
5. Continue demand-driven Python materialisation. A Python mirror is useful
   only when a Python consumer/observer will read the output; Python-only
   storage is useful only when no native consumer requires native expansion.
6. Consider internal arenas only for ownership domains proven by profiles to
   have many same-lifetime allocations. Do not install a global allocator
   override inside ``libhgraph`` because allocation/deallocation crosses Python
   and extension DSO boundaries.

Baseline and comparison procedure
---------------------------------

Run the complete profile pack on an otherwise idle host from a clean main
revision. Use the same build type, Python version, sample count, sampling
interval, and CPU for comparisons. The default report compares current source
with the exact published hgraph 0.8.1 wheel and adds current-source
GraphDiagnostics data. To reconstruct the historical release baseline, select
``upstream-py``, ``upstream-cpp``, and ``release`` explicitly; that report
includes both ``0.8.1/Python`` and ``0.8.1/legacy-C++`` peak-memory ratios.
Values below one mean 0.8.1 used less incremental resident memory. Preserve the
raw JSON; the markdown matrix is a presentation view and intentionally rounds
values.

For optimisation work, first select the affected group and increase to five or
more samples. Compare medians, median absolute deviation, and the complete
scale series. Treat a change smaller than page/allocator granularity or within
run-to-run spread as inconclusive. The committed 0.8.1-versus-0.5.41 record is
the forward comparison baseline. Do not rerun the 0.5.41 modes for ordinary
optimisation work. Reconstruct that historical comparison only when its
profile pack, host, or sampling policy changes; the orchestrator pins both
published wheel artifacts and enforces the identity in its cache.

The first committed macOS and Linux reports are baseline artifacts rather than
normative thresholds. Cross-platform absolute RSS is not directly comparable:
different loaders, allocators, page sizes, Python builds, and shared-memory
accounting dominate small graphs. Within each host, the useful signals are
current-source/0.8.1 ratios and growth slopes across the profile series.
