Memory utilisation and accounting
=================================

Purpose
-------

This document is the initial static ownership audit and measurement contract
for graph memory. It is intended to direct optimisation work from reproducible
evidence, not to establish a platform-independent byte limit. The associated
campaign in ``benchmarks/memory_orchestrate.py`` produces:

* comparative, fresh-process RSS/USS/PSS samples for current Python hgraph,
  hgraph C++, and hg_cpp;
* growth series for graph size, duration, cardinality, retained capacity, and
  client count;
* hg_cpp process-lifetime runtime-registry cardinalities for identical and
  intentionally novel graph wiring;
* an independent native ``Inspector`` snapshot for planned and dynamic graph
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

``Inspector planned bytes``
  The root graph's checked runtime storage-plan size. Graph, node, input,
  output, and fixed structured time-series storage placed within that plan are
  deterministic native bytes.

``Inspector dynamic bytes``
  Live/reserved nested-graph slot-store bytes reported by map, mesh, switch,
  TSL map, and reducer implementations, plus keyed TSS/TSD output storage
  attributed through the common TSData ops surface. The peak survives graph
  teardown in the owned snapshot. Current dynamic reporting is a lower bound,
  not a native heap total.

``runtime registry growth``
  Final-minus-pre-run counts for retained node runtime types, graph programs,
  graph runtime types, executor runtime types, and all common type records.
  These cold-path counts are captured without Inspector through
  ``runtime_registry_snapshot()``. They describe process-lifetime structural
  ownership, not live graph instances or allocated bytes.

The process pass never attaches Inspector. Inspector retains an owned record
and strings for every graph/node it observes, so using it during RSS sampling
would change the quantity being measured. Each profile and each sample runs in
a fresh process so process-global state from one graph cannot contaminate the
next graph's delta. For repeated-lifecycle profiles, the Inspector pass runs
one execution and reports the per-graph structural footprint; only the
uninstrumented process pass is used to infer cross-execution retention because
Inspector would retain its own records across those executions.
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
     - Payload allocation is not currently attributed by Inspector
   * - Target/observer state
     - ``ts_input/target_link*``, TS data observer sets, and slot observers
     - Inline state is planned; spill/transitions are not fully attributed
   * - Canonical registries
     - type/plan/ops registries, ``InternTable``, and policy context stores
     - Process-lifetime ownership; excluded from per-graph Inspector totals
   * - Python bridge
     - ``py_nodes.cpp``, ``py_ports.cpp``, ``py_wiring.cpp``, and
       ``value_conversion.cpp``
     - Included in process metrics only

The audit did not infer heap size from container ``sizeof`` or add allocator
estimates to Inspector totals. Such estimates look precise but miss capacity,
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
Inspector a deterministic static byte count.

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

Keyed nested graphs use ``StableSlotStorage`` and
``InPlaceGraphSlotStore``. Capacity growth appends a payload block so already
published graph addresses never move. A separate pointer table is replaced as
capacity grows; a bitmap tracks constructed slots. Reported reserved bytes are
approximately:

.. code-block:: text

   capacity * (aligned entry-plus-graph stride + pointer size)
   + constructed-bitmap word capacity

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
adds dynamic ownership reported by constructed child TSData. The child's fixed
storage is already part of the parent value-slot stride and is therefore not
counted again.

The hook is sampled by Inspector only. It adds no work to graph evaluation and
does not include allocator headers or fragmentation. A projected TSD key-set
reports its key-set allocation surface; the owning TSD root reports the full
dictionary and descendants. Node attribution samples only owned output roots,
not input aliases or projections.

Unaccounted dynamic ownership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspector currently reports native dynamic storage only where a node's
``storage_metrics`` implementation exposes it. The following material memory
categories are visible to process metrics but are not fully attributed in an
inspection snapshot:

* compact and mutable value-container allocations for list, set, map, queue,
  cyclic buffer, string, and compound values;
* TSW data-level dynamic capacity and TSData observer spill storage;
* input target-link active trees and transient structural-transition objects;
* wiring builders, signatures, labels, service/adaptor registries, and result
  capture;
* Python callables, wrappers, generators, values, traceback state, and
  selectively materialised ``PyObject`` mirrors; and
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
placement lifecycle operations. ``StableSlotStorage`` destroys payloads before
releasing blocks; its allocator-aware deleter pairs layout/alignment with the
matching deallocation. Graph deletion follows stop, unsubscribe, then erase.
No retired-object side container was found in these paths.

The highest-risk future changes are therefore not simple missing ``delete``
calls. They are semantic ownership errors: failing to unsubscribe before slot
reuse, preserving an erased reference to temporary storage, compacting stable
slots while pointers are published, mismatching Python/native ownership, or
adding a cache without a bounded key space. Changes in those areas require the
full native/Python suites plus Linux validation and ASan as described in
``debugging``.

Initial measured findings
-------------------------

The first controlled macOS baseline used hgraph 0.5.33, Python 3.14.6, three
fresh-process samples, and an Apple M4 Max. The complete report and raw samples
are committed as ``benchmarks/results/memory-matrix-20260731-mac-main.md`` and
``benchmarks/results/memory-raw-20260731-mac-main.json``. The raw metadata
records the exact source revision and environment fingerprint used for the
run.

The loaded hg_cpp process floor was 63.5 MiB RSS, compared with 83.8 MiB for
Python hgraph and 86.2 MiB for hgraph C++. Small run deltas favour both reference
runtimes because hg_cpp touches an approximately 1.3--1.5 MiB one-time set of
pages on first graph use; USS shows that only about 0.1 MiB remains uniquely
resident for the scalar duration series. Ratios at this scale are allocator/page
effects and should not drive node-level optimisation.

Larger graph and collection profiles favour hg_cpp, increasingly with scale:

.. list-table:: macOS peak RSS delta (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - hgraph C++
     - hg_cpp
     - hg/Python
     - hg/hgraph C++
   * - Large wide/deep graph
     - 19.0
     - 21.6
     - 19.0
     - 1.00
     - 0.88
   * - Large dense TSD map/reduce
     - 5.8
     - 4.6
     - 2.5
     - 0.43
     - 0.55
   * - Large sparse retained capacity
     - 616.5
     - 448.3
     - 106.1
     - 0.17
     - 0.24
   * - Long monotonic key growth
     - 98.3
     - 68.7
     - 23.1
     - 0.24
     - 0.34
   * - Long clear/repopulate
     - 100.6
     - 75.8
     - 7.3
     - 0.07
     - 0.10
   * - Large keyed switch
     - 37.8
     - 8.6
     - 4.2
     - 0.11
     - 0.48
   * - Large dependency mesh
     - 10.0
     - 5.2
     - 3.5
     - 0.35
     - 0.66

The hg_cpp duration series are bounded where the data structure is bounded.
Scalar loops, Python compute chains, strings, the fixed 64-item tick window,
key reactivation, and clear/repopulate have no material cycle-proportional
slope. Native peak reserved storage is constant at 501 KiB for bounded TSD
churn, 302.5 KiB for key reactivation, and 1,408 KiB for clear/repopulate.
Monotonic key growth scales intentionally: native reported storage rises from
819 KiB to 6,560 KiB, while process peak rises from 3.6 MiB to 23.2 MiB.

The sparse-capacity profile also quantifies the attribution gap. At the large
point, Inspector attributes 33.2 MiB of nested graph slots while process peak
is 106.1 MiB. Approximately 72.9 MiB remains in key/value/index payloads,
wiring/Python state, or allocator overhead. TSS cardinality produces a visible
RSS slope while Inspector reports zero dynamic bytes. These are stronger
reasons to extend structural accounting than to tune the already-accounted
slot block in isolation.

Repeated graph lifecycle finding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same graph callable was wired, executed, stopped, and collected repeatedly
inside one process. The post-GC series is nearly linear rather than reaching a
warm plateau:

.. list-table:: macOS first-to-last post-GC growth (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - hgraph C++
     - hg_cpp
   * - Small graph, 10 executions
     - 0.9
     - 1.0
     - 1.0
   * - Small graph, 100 executions
     - 8.6
     - 10.5
     - 10.7
   * - Service/adaptor graph, 10 executions
     - 0.8
     - 1.0
     - 0.6
   * - Service/adaptor graph, 50 executions
     - 4.1
     - 4.9
     - 3.1

RSS and USS growth are effectively identical for these series, so this is live
or allocator-retained private memory rather than shared-library accounting.
The comparable Python and hgraph-C++ slopes suggest a wider authoring/runtime
lifecycle pattern, but hg_cpp's static ownership makes its candidate sources
concrete: ``NodeRuntimeRegistry::make_type`` and the graph runtime registry
append schemas, ops, contexts, and names before type interning, while several
node policies append unique contexts to intentionally program-lifetime
vectors. Rewiring an already-known graph can therefore retain new backing
records even if the canonical type record is deduplicated.

This is an evidence-backed root-cause hypothesis, not yet an allocation-level
proof. The leading follow-up is a focused repeated-wiring profile with registry
cardinality counters, followed by allocation tracing. Any fix must preserve
the stable context addresses referenced by published ops tables; moving or
freeing registry entries after publication is not valid.

Native Linux cross-check
~~~~~~~~~~~~~~~~~~~~~~~~

The corresponding native x86_64 Linux baseline used hgraph 0.5.33, Python
3.14.4, and an Intel Core Ultra 7 155H. Its report and raw samples are committed
as ``benchmarks/results/memory-matrix-20260731-linux-main.md`` and
``benchmarks/results/memory-raw-20260731-linux-main.json``. The loaded process
floors are 76.8 MiB for Python, 79.8 MiB for hgraph C++, and 65.4 MiB for
hg_cpp.

The absolute deltas differ from macOS, as expected from the loader and
allocator, but the material ratios agree:

.. list-table:: Linux peak RSS delta (median MiB)
   :header-rows: 1

   * - Profile
     - Python
     - hgraph C++
     - hg_cpp
     - hg/Python
     - hg/hgraph C++
   * - Large wide/deep graph
     - 19.6
     - 20.9
     - 19.2
     - 0.98
     - 0.92
   * - Large dense TSD map/reduce
     - 6.0
     - 4.6
     - 3.2
     - 0.53
     - 0.69
   * - Large sparse retained capacity
     - 615.3
     - 450.4
     - 111.2
     - 0.18
     - 0.25
   * - Long monotonic key growth
     - 101.0
     - 72.2
     - 24.2
     - 0.24
     - 0.34
   * - Long clear/repopulate
     - 116.2
     - 64.7
     - 7.8
     - 0.07
     - 0.12
   * - Large keyed switch
     - 33.1
     - 8.8
     - 4.5
     - 0.14
     - 0.51
   * - Large dependency mesh
     - 10.1
     - 5.4
     - 4.0
     - 0.40
     - 0.75

Repeated-lifecycle growth also reproduces: 100 small executions retain 8.7 MiB
for Python, 10.9 MiB for hgraph C++, and 11.2 MiB for hg_cpp. This makes the
lifecycle finding a cross-platform signal rather than a macOS allocator
artifact.

Initial optimisation priorities
-------------------------------

Priorities should be re-ranked after each controlled baseline. The static
audit suggests this order:

1. Extend structural attribution before optimising opaque RSS. Add cold-path
   metrics for TSD/TSS/TSW key/value capacities, hash/index bytes, observer
   spill storage, and value-container payloads. Keep collection fast paths
   unchanged when no Inspector is attached.
2. Use the cardinality and monotonic-growth profiles to calculate bytes per
   live/reserved key for map, reduce, mesh, and dynamic TSL. Investigate the
   pointer-table plus block overhead and bank/generation multiplicity where it
   materially exceeds payload size.
3. Use duration profiles as leak/boundedness guards. A statistically material
   slope for scalar, fixed-window, churn, reactivation, or clear/repopulate
   workloads is higher priority than a one-time import or allocator step.
4. Address repeated-wiring growth. Instrument process-lifetime registry and
   policy-context cardinalities, deduplicate before allocating published
   backing records, and preserve stable addresses for records referenced by
   live graphs. Add novel-schema wiring as a separate intentional-growth axis.
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
interval, and CPU for comparisons. The default report compares current Python
hgraph, hgraph C++, and hg_cpp and adds hg_cpp Inspector data. It reports both
``hg_cpp/Python`` and ``hg_cpp/hgraph-C++`` peak-memory ratios; values below one
mean hg_cpp used less incremental resident memory. Preserve the raw JSON; the
markdown matrix is a presentation view and intentionally rounds values.

For optimisation work, first select the affected group and increase to five or
more samples. Compare medians, median absolute deviation, and the complete
scale series. Treat a change smaller than page/allocator granularity or within
run-to-run spread as inconclusive. Re-run the released baseline only when the
hgraph version, profile pack, host, or sampling policy changes; the
orchestrator enforces this identity in its cache.

The first committed macOS and Linux reports are baseline artifacts rather than
normative thresholds. Cross-platform absolute RSS is not directly comparable:
different loaders, allocators, page sizes, Python builds, and shared-memory
accounting dominate small graphs. Within each host, the useful signals are
hg_cpp/released-C++ ratios and growth slopes across the profile series.
