RFC 0028: Shared Value Representation
=====================================

:Status: Accepted (core implementation complete; adaptor migration pending)
:Author: Howard Henson
:Created: 2026-08-24
:Updated: 2026-08-26
:Target: C++ value storage, cross-thread retention, adaptor ingress paths

Summary
-------

Add ``Shared<T>``, an immutable one-pointer value representation backed by a
process-wide stable-slot arena.  Copying a handle atomically retains the slot,
moving steals the pointer, and destruction atomically releases it.  The final
release destroys the payload and returns its slot to an ABA-safe lock-free free
list.

The design has two deliberately separate reference systems:

* a **strong value count**, which represents live ``Shared<T>`` handles; and
* an **allocator reference count**, used only by the free-list algorithm while
  a thread may still inspect a free node's next pointer.

Strong handles carry no generation.  A handle is either a live owning
reference or null, and the runtime controls every handle copy, move, and
destruction.  Consequently a slot cannot be returned while a valid handle
still names it.  ABA protection remains necessary inside the allocator because
a free-list reader is not a value owner; it is supplied by the independent
allocator reference count.

The arena is global to the hgraph runtime rather than to a graph.  A shared
value can therefore cross graph, executor, and producer-thread boundaries
without rebinding, and a stable allocation can be reused by any compatible
structured value.  This is intentionally different from the graph-bound,
mutable polymorphic pool in :doc:`RFC 0029
<rfc_0029_value_pool_ownership_and_binding>`.

Motivation
----------

Large immutable records are commonly retained at several points between an
adaptor and a graph output.  The plain ``Value`` contract makes retention a
deep copy.  A Kafka record, for example, may be copied into a schedule, copied
again while its envelope is rebuilt, and copied once more into a dictionary
child although no consumer mutates it.

``Shared<T>`` changes the cost of retaining the already-materialised record
from ``O(payload)`` to ``O(1)``.  One materialisation is still required when a
plain value first becomes shared.  Subsequent holders name the same immutable
payload.

The original proposal put these allocations in a graph-owned pool with a
non-atomic count.  That made a handle unusable in a push sender and coupled its
lifetime to one realization binding.  It also prevented independently running
graphs from exchanging or retaining the same value.  The process-wide arena
removes that coupling.

Public type contract
--------------------

``Shared<T>`` is a type-level representation marker, parallel to ``Owned<T>``:

.. code-block:: c++

   using TransportEvent =
       Bundle<"example::TransportEvent",
              Field<"record", Shared<Record>>,
              Field<"received", DateTime>>;

The wrapper is explicit in the schema so storage cost is visible at the field
that chooses it.  It has:

* a distinct ``ValueTypeFlags::Shared`` bit;
* ``TypeRegistry::shared(target)`` and ``scalar_descriptor<Shared<T>>``;
* a one-pointer ``StoragePlan``;
* the target's fields and value semantics; and
* transparent JSON and Python value conversion.

The initial contract accepts a direct ``Bundle`` target.  ``Shared<Shared<T>>``
and atomic/container targets are rejected.  Broadening the target set requires
a demonstrated use; it is not implied by the arena's ability to store an
arbitrary concrete plan.

``Shared<T>`` is not a structural Bundle.  Predicates which previously used
``!is_owned()`` to identify a structural Bundle use ``!is_indirect()`` so both
one-pointer wrappers remain distinct from their target.

Lifecycle and immutability
--------------------------

The handle lifecycle is:

.. list-table::
   :header-rows: 1
   :widths: 32 68

   * - Operation
     - Behaviour
   * - default construct
     - Construct a null handle; allocate nothing.
   * - materialise from ``T``
     - Reserve a slot, default-construct and assign the complete payload while
       private, then publish one strong reference.
   * - copy construct / assign
     - Atomically retain the source before releasing the destination.
   * - move construct / assign
     - Steal the slot pointer and null the source.
   * - destroy
     - Atomically release; the final release destroys the concrete payload and
       returns the slot to its size-class free list.
   * - ``from_python``
     - Build and publish a replacement, then release the old handle.  Never
       mutate a published payload.

Immutability is enforced by the ops table, not by convention:

* ``allows_mutation`` is false;
* no ``mutable_concrete_memory`` or ``writable_concrete_memory`` operation is
  published;
* no mutable indexed range or element operation is published; and
* ``ValueView::concrete()`` returns a read-only target view when the declaring
  ops do not permit mutation, even if the inline handle itself sits in writable
  owner storage.

Assignment replaces a handle and is still legal.  It is a lifecycle operation,
not a mutation of the published payload.

Global stable-slot arena
------------------------

The arena contains power-of-two payload size classes, also separated by
alignment class.  Struct types with compatible size and alignment share the
same class.  A slot contains:

.. code-block:: c++

   struct SharedValueAllocation {
       atomic<uint32_t> free_list_refs;  // allocator only
       atomic<SharedValueAllocation *> free_list_next;
       atomic<uint32_t> strong_refs;     // Shared<T> owners only
       SharedSizeClassPool *owner;
       const TypeRecord *record;         // active concrete payload
       // aligned payload follows
   };

The inline ``Shared<T>`` value points directly at this stable header.  The
``owner`` is the pool identity and owns the class-specific payload offset, so a
separate ``(pool id, offset)`` pair in every handle would duplicate information
and widen the common handle from one word to two.  Retain, release, concrete
type lookup, and payload projection require no global registry lookup.

Slots are allocated in non-moving slabs.  Normal acquire and return use the
free list and are lock-free.  First creation of a size class and slab growth are
cold paths protected by a mutex because the operating-system allocation itself
is not lock-free.  A request which requires capacity can therefore block;
``try_acquire`` with pre-reservation can be added if a future real-time caller
requires a no-growth guarantee.

ABA-free free list
------------------

The free list is adapted from the intrusive ``FreeList`` used by Cameron
Desrochers' ``moodycamel::ConcurrentQueue`` under its Boost Software License
1.0 option.  The complete attribution and license text are retained in the
private implementation header.

The algorithm is appropriate here because its central precondition is exactly
the arena invariant: nodes are not freed until the free list itself is
destroyed.  Before reading a head's next pointer, a pop operation acquires an
allocator reference on that node.  A return which races with such a reader
marks the node as needing insertion; whichever thread removes the last
allocator reference performs the insertion.  The head may revisit the same
address without fooling a compare-exchange into using a changed next pointer.

Allocator references must not be merged with ``strong_refs``.  A successful
pop removes the list's reference and the popper's temporary reference while
the slot still has no published value owner.  Conversely the final strong
release destroys the payload before offering the node to the free list.  The
two state machines meet only at that hand-off.

Why strong handles have no generation
-------------------------------------

A generation is required for a weak, borrowed, or externally fabricated handle
which may survive slot reuse.  ``Shared<T>`` has none of those operations:

* every non-null handle is a strong reference;
* the count is incremented before a copied handle becomes observable;
* the count reaches zero before the slot is recycled; and
* raw slot handles are not a public construction surface.

There is therefore no valid stale handle to detect.  Adding a generation would
either widen the handle or require a second load on every projection without
closing an allowed lifetime race.  If weak handles or serialised slot tokens
are introduced later, they require a separate generational contract.

Threading and memory ordering
-----------------------------

``strong_refs`` is atomic.  A retain is a relaxed increment because ownership,
not payload publication, is being transferred.  Publishing a newly constructed
payload is a release operation.  The final decrement is acquire-release so
payload destruction happens after preceding owners have released it.

The payload is immutable once published.  Concurrent const access is permitted
when the target's own const operations are thread-safe.  As with an ordinary
``Value`` crossing a producer boundary, a custom target remains responsible
for the thread-safety of resources it embeds.  Python-owned payloads still
obey the Python bridge's GIL and interpreter-lifetime rules; the arena does not
turn an interpreter-owned object into an unrestricted native object.

Memory bound and accounting
---------------------------

This design removes case-pair storage.  Its retained memory is:

.. math::

   O(H + K B)

where ``H`` is the sum of high-water slot capacity across used size classes,
``K`` is the number of used classes, and ``B`` is the bounded slab tail (at
most 64 slots in the baseline implementation).  It is independent of dispatch
case-pair count and is never ``O(C^2)``.

Power-of-two class rounding consumes less than twice the requested payload
size, excluding the fixed slot header.  During a switch, the old and new value
sets coexist exactly while strong references to both exist; no retired-case
matrix is retained.  This gives the desired two-live-layout pattern naturally.

The baseline retains empty slabs until registry reset/process exit.  Thus a
large historical spike remains in the arena's high-water capacity; strict
``O(current live bytes)`` reclamation would require epochs or hazard pointers
for slab retirement in addition to the free-list algorithm.  It is a possible
future extension, not silently claimed here.

Per-handle ``DynamicStorageMetrics`` reports zero for the shared payload so the
same allocation is not counted once per holder.  ``shared_value_pool_metrics``
is the single process-wide accounting owner and reports classes, slabs,
capacity, live values, and reserved bytes.

Type and registry lifetime
--------------------------

Each occupied slot borrows the concrete ``TypeRecord`` whose plan constructs
and destroys its payload.  Production registries are process-lived.  The
test-only canonical reset therefore clears the shared arena before
``TypeRecordRegistry``, then clears value plans and schemas in the existing
borrowers-before-lenders order.  A live shared handle at registry reset is a
contract violation and terminates rather than becoming a dangling record.

Relationship to RFC 0029
------------------------

RFC 0029 continues to govern mutable, graph-confined polymorphic compound
storage.  That representation needs an explicit realization binding because a
mutable slot belongs to one graph and its accounting owner.

``Shared<T>`` does not use that pool or binding cell.  Its payload is immutable,
its reference count is atomic, and its lifetime may span graphs, so binding it
to a graph would be an artificial restriction.  The two facilities share the
general stable-slot principle but not ownership, concurrency policy, or ops.

Compatibility and ABI
---------------------

Schemas not using ``Shared<T>`` retain their existing plans and lifecycle.
The new metadata bit is additive.  ``Shared<T>`` values serialize as ``T``;
the wrapper is a local representation choice and does not add a wire envelope.

The free-list implementation is private.  The installed public surface is the
schema marker, registry entry point, normal ``Value`` materialisation
constructor, and aggregate arena metrics.  A separately built extension does
not depend on the slot header layout.

Implementation status
---------------------

The core implementation includes:

* ``Shared<T>``, metadata, registry, static descriptor, and Python metadata
  exposure;
* immutable indexed ops with atomic retain/release and replacement assignment;
* the process-wide size-class arena and attributed ABA-safe free list;
* transparent JSON/Python conversion and read-only concrete projection;
* canonical registry-reset ordering and process-wide metrics; and
* native tests for immutability, stable reuse, cross-schema class reuse,
  concurrent retain/release, allocator contention, JSON, and Python exposure.

Migration of Kafka/web transport fields and before/after adaptor benchmarks is
deliberately a subsequent, separately measurable change.  The representation
contract does not require every struct to become shared; sites opt in where
retention data justifies it.

Acceptance criteria
-------------------

* A ``Shared<T>`` handle is one pointer and mutation is unavailable through
  the wrapper, concrete projection, and indexed children.
* Copies share one stable address; the final release destroys once and makes
  the slot reusable.
* Concurrent retain/release and concurrent allocator churn pass ordinary,
  ASan, and TSan testing on supported hosts.
* The steady-state allocator path performs no allocation, lock, type-registry
  lookup, or generation check.
* Compatible struct types reuse one size class; memory accounting is reported
  once by the arena and is independent of dispatch case pairs.
* Registry reset withdraws all shared slots before their TypeRecords.
* Native, Python, JSON, and installed-SDK construction paths preserve target
  value semantics.
* Adaptor migrations report allocation count, retained bytes, and p50/p99
  latency before and after, and remain separate from the core facility change.

References
----------

* ``include/hgraph/types/value/shared_value_pool.h`` — public metrics and the
  opaque internal allocation contract.
* ``src/hgraph/types/value/shared_value_pool.cpp`` — global size-class arena,
  stable slabs, atomic strong references, and reset integration.
* ``src/hgraph/types/value/impl/lock_free_freelist.h`` — attributed adaptation
  of the moodycamel free list.
* `moodycamel ConcurrentQueue FreeList
  <https://github.com/cameron314/concurrentqueue/blob/master/concurrentqueue.h>`_.
* `Solving the ABA Problem for Lock-Free Free Lists
  <https://moodycamel.com/blog/2014/solving-the-aba-problem-for-lock-free-free-lists>`_.
* :doc:`RFC 0029 <rfc_0029_value_pool_ownership_and_binding>` — the separate
  graph-bound mutable pool contract.
* :doc:`RFC 0027 <rfc_0027_bounded_push_source_queues>` — the adaptor ingress
  path which motivated cheap immutable retention.
