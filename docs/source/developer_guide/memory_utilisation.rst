Memory utilisation and accounting
=================================

Purpose
-------

This document is the initial static ownership audit and measurement contract
for graph memory. It is intended to direct optimisation work from reproducible
evidence, not to establish a platform-independent byte limit. The associated
campaign in ``benchmarks/memory_orchestrate.py`` produces:

* comparative, fresh-process RSS/USS/PSS samples for released C++ hgraph and
  hg_cpp;
* growth series for graph size, duration, cardinality, retained capacity, and
  client count;
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
  TSL map, and reducer implementations. The peak survives graph teardown in
  the owned snapshot. Current dynamic reporting is a lower bound, not a native
  heap total.

The process pass never attaches Inspector. Inspector retains an owned record
and strings for every graph/node it observes, so using it during RSS sampling
would change the quantity being measured. Each profile and each sample runs in
a fresh process so process-global state from one graph cannot contaminate the
next graph's delta.

Static ownership audit
----------------------

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

Unaccounted dynamic ownership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspector currently reports native dynamic storage only where a node's
``storage_metrics`` implementation exposes it. The following material memory
categories are visible to process metrics but are not fully attributed in an
inspection snapshot:

* key/value slot-store indices, hash-table buckets, key payloads, and value
  payloads outside nested graph slots;
* compact and mutable value-container allocations for list, set, map, queue,
  cyclic buffer, string, and compound values;
* TSD/TSS/TSW data-level dynamic capacity and observer spill storage;
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
4. Measure process-lifetime registries separately with repeated identical and
   novel schemas. Preserve canonical stable addresses; optimise duplicate
   keys, context breadth, or reset/test policy rather than freeing records that
   live graphs can reference.
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
interval, and CPU for comparisons. The default report compares released C++
hgraph with hg_cpp and adds hg_cpp Inspector data. Preserve the raw JSON; the
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
