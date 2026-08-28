RFC 0013: Per-Leaf Pools for Polymorphic Compound Scalars
=========================================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-08-03
:Target: C++ value storage, graph runtime, and Python compatibility

.. note::

   The **storage-selection mechanism** described here — a thread-local
   compound-storage scope established around graph lifecycle and evaluation —
   is superseded by :doc:`RFC 0029 <rfc_0029_value_pool_ownership_and_binding>`,
   which binds a pool through the realized ops context and the builder instead.
   Everything else in this RFC — per-leaf pools, stable slots, the
   ``LeafSlotHeader``, non-atomic counts, and the accounting rule — stands
   unchanged and is depended upon there.

Summary
-------

Graph-local polymorphic compound scalars may opt into replacing their current
``type-tag + largest-alternative`` representation with one stable pointer to
the active concrete leaf.  The graph-level policy is captured once at wiring
time.  Under that policy, a closed union selects the indirect representation
when the difference between its largest and smallest realised leaf plans is
strictly greater than 32 bytes.

Each root graph owns a registry of pools keyed by exact leaf ``TypeRecord``.
Every leaf pool uses stable slots bound to that leaf's exact storage plan;
growth appends backing blocks and never relocates an existing payload.  Values,
keys, and nested compound fields can therefore share the direct payload pointer
without pointer fix-up when a pool grows.

The public authoring and value contract is unchanged.  Values outside graph
storage retain the current self-contained representation, and ordinary
``Value`` construction materialises a graph-local view before it can escape.

Motivation
----------

Closed polymorphic unions currently reserve enough inline storage for their
largest registered alternative in every holder.  This is especially costly
for keyed time series: the logical key can appear in the key store, mapped
child state, child key output, and downstream keyed output.  It also penalises
the common small leaf whenever one uncommon leaf is large.

Grouping allocations only by byte size would reduce the holder but would mix
unrelated lifecycle plans and introduce internal fragmentation.  Per-leaf
pools instead preserve exact type and lifecycle identity, allocate only the
active leaf plan, and reuse the runtime's existing stable-slot representation.

Ownership boundary
------------------

The C++ runtime owns the complete facility.  Python conversion, Python push
queues, JSON codecs, captured deltas, ``GlobalState``, graph traits, and caller-
owned ``Value`` objects continue to use independently owning storage.  They do
not retain graph pool references.

An opted-in root graph's erased storage plan contains one compound-scalar
storage owner; its nested-graph plan contains a borrowed view of that owner.
These are conditional named plan fields, not fixed runtime-header members.
The concrete pool registry is allocated lazily on the first pooled value.  A
graph which does not opt in has neither field and constructs no pool facade.
(The compound-storage scope this originally described is gone; see
:doc:`RFC 0029 <rfc_0029_value_pool_ownership_and_binding>`.)  A graph-local pooled handle never crosses to another root graph
without materialisation.  Consequently intrusive reference counts are
non-atomic: allocation, retain, release, and mutation occur on the graph's own
evaluation thread.

Selection contract
------------------

Inline storage is the default.  C++ selects pooling on the wiring-time
``GlobalState`` with ``set_pooled_compound_scalar_storage(state.view())``;
Python uses ``set_pooled_compound_scalar_storage()`` inside the active
``GlobalContext``.  The immutable option is part of type-realisation and graph
runtime cache identity, and nested graphs inherit the root selection.

For each polymorphic Bundle declaration in an opted-in graph, realisation
computes the minimum and maximum ``StoragePlan::layout.size`` of its recursively
realised concrete leaves.  It selects pooled storage exactly when::

   maximum_size - minimum_size > 32

The threshold is an internal engine constant, not graph or per-value
configuration.  Exact Bundles and closed unions at or below the threshold keep
their flat representation.  External realisation always keeps the existing
flat closed union regardless of the threshold.

Leaf-pool representation
------------------------

A pooled union plan is one pointer wide.  The pointer addresses the concrete
leaf payload directly.  A fixed control header immediately precedes the
payload and contains:

* the owning leaf-pool pointer;
* a 32-bit stable slot id; and
* a 32-bit reference-state word: 31 reference-count bits and one
  permanently-unshareable bit.

Alignment padding, when required, precedes the header so the header is always
recoverable by subtracting its fixed size from the payload pointer.  The slot
layout is the control header, the leaf's exact aligned plan, and only the tail
padding required to align adjacent slots.  No slot contains space for a
different leaf.

Each leaf pool owns a ``StableSlotStore<ConstructedAndLive>`` whose envelope is
at least pointer-aligned, selecting the tagged-pointer liveness strategy.  A
small free-slot stack makes allocation O(1); the stable-slot state remains the
authoritative free/staged/live record.  Capacity grows geometrically from one
slot.  The pointer index may be replaced during growth, but the appended block
model keeps every published payload address stable.  Capacity is retained for
reuse until root graph destruction.

A pool is created lazily for an active graph-realised leaf ``TypeRecord``.
Different leaf records always use different pools even when their layouts are
identical.  Default construction obtains an ordinary live slot; it does not
create a canonical shared default.  There is no value interning and no cached
content hash.

Lifecycle and mutation
----------------------

Allocation stages a free slot, constructs the exact leaf using its plan, then
marks the slot live.  Failure destroys any constructed payload and returns the
slot to the free state.  Final release marks the slot pending erase, destroys
the leaf, marks the slot free, and returns its id to the free stack.

Copies within one root graph retain the slot.  Cross-graph copies and copies to
external ownership materialise the concrete value.  Whole-value assignment
acquires or constructs the replacement before releasing the old pointer.

A mutable field or writable concrete projection first makes the slot unique.
It then permanently marks the slot unshareable.  Later copies of that value
deep-copy instead of sharing.  This preserves existing retained writable-view
behaviour without tracking view lifetimes.

The value ops ABI gains a throwing writable-concrete projection hook and moves
to version 5.  Graph-local bindings explicitly advertise an external owning
binding.  Ordinary ``Value`` constructors honour that binding recursively;
only private graph-lifetime owners may preserve pooled representation.

Push boundary
-------------

Push senders and their mutex-protected queues retain the current full-copy
representation.  Once a queued delta reaches the evaluation thread, a
consuming application path moves its concrete values into the appropriate
leaf pools.  Pool access never occurs while an off-thread queue lock is held,
and the change introduces no new GIL retention.

Inspection and memory accounting
--------------------------------

The compound-scalar storage facade publishes a data-only aggregate inspection
view with pool count, live-slot count, and retained capacity.  Type-realisation
inspection separately reports the selected representation and leaf-size
range, without requiring a debugger to infer concrete strategy or
standard-library layouts.

Pool live and reserved bytes are attributed once at the root graph.  Metrics
include slot indexes, control storage, free-slot capacity, leaf payloads, and
nested dynamic storage found by scanning live slots.  Individual pooled
handles report no owned bytes, preventing reference-counted values from being
counted once per holder.

Compatibility
-------------

Existing C++ and Python authoring APIs, JSON and table formats, discriminators,
hashing, equality, comparison, and tick behaviour do not change.  Pooling is a
new opt-in graph configuration API; omitting it preserves the previous inline
layout with zero pool bytes in the graph plan.  Exact and below-threshold
layouts do not change.  The value-ops ABI change requires extensions to
rebuild; the installed-SDK consumer fixture validates the new contract.

Alternatives considered
-----------------------

* **Pools by size class:** rejected because they mix leaf lifecycles and can
  retain incorrectly sized slots.
* **Pool every compound scalar:** deferred because small exact Bundles benefit
  from flat cache-local storage and would pay indirection without max-variant
  waste.
* **Enable eligible pooling for every graph:** rejected because an unused
  representation strategy must not add planned storage or per-tick scope work
  to ordinary type-erased graphs.
* **General interning:** rejected for the first implementation because content
  lookup, collision handling, and mutation invalidation add cost unrelated to
  eliminating max-alternative padding.
* **Atomic reference counts:** rejected while a graph and its pooled values are
  confined to one execution thread.  Off-thread producers materialise values.

Acceptance criteria
-------------------

* Disabled graph plans contain no pool owner/view field and execute the
  ordinary lifecycle/evaluation ops.  (Originally: "without a compound-storage
  scope" — under :doc:`RFC 0029 <rfc_0029_value_pool_ownership_and_binding>` no
  graph establishes one.)
* Eligible opted-in holders are one pointer and existing payload pointers
  remain stable across leaf-pool growth.
* Separate leaf records never share a pool, and each slot uses only its exact
  leaf plan plus fixed control/alignment overhead.
* Public C++ wiring tests and matching Python tests cover polymorphic ``TS``,
  ``TSS``, ``TSD``, map, mesh, reduce, mutation, push ingress, and teardown.
* Differential parity recipes match released Python hgraph tick for tick.
* Multiple pure-C++ engines execute concurrently on separate threads without
  sharing pool state.
* Replicated-key and small-active-leaf profiles reduce total planned plus
  dynamic memory.  A median hot-path regression greater than 5%, when outside
  measurement noise, blocks the pooled strategy.
* The complete native and Python 3.14 suites, installed-SDK consumer, and
  independent Linux-host ASan validation pass.

Implementation status
---------------------

The C++ runtime, public contracts, Python bridge adaptation, inspection,
documentation, and regression coverage are implemented.  The final acceptance
evidence is:

* all 1,438 native tests pass from fresh builds on macOS and an independent
  Linux host with GCC 14 warnings treated as errors;
* the stable-ABI wheel, built with Python 3.12 and installed into fresh Python
  3.14 environments, passes 1,886 tests with 11 skips on macOS and Linux;
* all native tests and the complete non-WIP Python suite pass under AddressSanitizer
  on the independent Linux validation host;
* the installed-SDK consumer builds and executes against the new public storage
  and representation contracts;
* all 56 differential recipes validate, and the keyed map/reduce and mesh
  lifecycle recipes match released hgraph exactly; and
* the ordinary small-graph construction profile retains the pre-change 17
  allocations and 1,664 allocated bytes per graph; its plan contains no pool
  owner or nested view.  The representative pooled polymorphic copy/hash
  profile records 10.608 ns/op against 14.652 ns/op for external flat storage,
  with no timed allocations in either case.

References
----------

* :doc:`rfc_0000` — RFC process and ABI/performance acceptance gate.
* :doc:`../developer_guide/memory_utilisation` — controlled memory baseline
  and comparison procedure.
* :doc:`../developer_guide/parity_testing` — isolated differential correctness
  campaign.
