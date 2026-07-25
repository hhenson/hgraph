RFC 0007: Scheduler-Driven Duration TSW Eviction
================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-25
:Target: Duration TSW scheduling, deltas, recording, and recovery
:Related: RFC 0006

Summary
-------

Define the implementation plan for duration-based time-series windows to evict
expired observations at their actual expiry time, even when the source remains
silent.  A root graph containing one or more owned duration ``TSW`` outputs,
including outputs inside nested graph instances, receives one root-owned pull
source.  A runtime-only forward reference to that source is published in the
root ``GlobalState`` shared by every nested graph.  Duration windows subscribe
to the source, which schedules the earliest expiry and mutates every due window
when evaluated.  This covers ``to_window`` and any other node whose output is,
or contains, a duration TSW without giving each producer or nested graph its
own scheduling implementation.  The work also replaces the singular removal
surface and scalar-only TSW delta with representations capable of describing
multiple evictions, reset-only ticks, and removal-only scheduled ticks.

This RFC is a design plan.  No scheduler-driven eviction or persistence-format
change is supplied with RFC 0006.

Problem statement
-----------------

The present duration TSW prunes old values only while pushing a new source
value.  Given a ten-second range:

.. code-block:: text

   t=0s   1
   t=1s   2
   t=2s   3
   t=20s  4

the values ``1``, ``2``, and ``3`` remain visible until ``t=20s`` even though
they expired earlier.  At ``t=20s`` several values leave together, while the
current C++ surface preserves only the last removed element.  Incremental
consumers cannot update correctly from that delta.

Root-graph expiry source
------------------------

TSW value storage remains independent of graph execution and scheduling.  It
must be usable in values, tests, record/replay materialisation, and extension
code without requiring a graph.  A standalone duration window continues to
prune on explicit mutation; scheduled eviction is an execution-layer service.

During root graph assembly, core recursively inspects resolved, owned output
schemas, including the output requirements of nested graph definitions.  If at
least one output is, or contains, a duration TSW, it adds exactly one special
pull source to the root graph.  This must apply to both normal ``Wiring``
construction and the supported direct C++ graph-builder path.

The root source publishes a typed, runtime-only forward reference under a
reserved internal ``GlobalState`` key.  Nested ``GraphView::global_state()``
already resolves to the root store, so a TSW at any nesting depth finds the
same source.  The concrete representation should follow the existing
type-erasure pattern: a reference to a private control output resolves its
owning node, and that node exposes a typed expiry-source view.  It must not
expose or persist a raw ``NodePtr``.  Root construction binds the forward
reference after node storage is attached and before any node start hook; root
stop removes it.

Layout-only dependencies place the source before every normal root node that
can own or host a duration TSW.  It is the first normal-rank source, after the
root graph's mandatory push-source phase.  A nested graph does not create
another expiry source.

When an owning node starts, the framework attaches each duration TSW in its
output tree to the root expiry source resolved from ``GlobalState``.  The
relationship is subscription based:

* the TSW output retains an opaque subscription/callback to the source through
  the existing execution-time notification machinery;
* the source retains a stable handle to the TSW and its next expiry;
* a TSW mutation notifies the source, which inserts, updates, or removes that
  window's expiry entry; and
* source invalidation or graph stop removes the subscription before either
  endpoint is destroyed.

The root source is shared by ``to_window``, custom adaptors, replay sources,
aggregation nodes, extension nodes, and dynamically created nested graph
instances.  Producers neither inject ``NodeScheduler`` for this purpose nor
implement expiry callbacks themselves.

The forward reference is bound before start, so even a root push-source output
can attach although push-source start hooks precede normal nodes.  Nested
producers start later through their root parent and resolve the same reference.
Stop and dynamic graph teardown detach subscriptions while both endpoints are
alive.  The root source must release every remaining subscription and erase the
reserved ``GlobalState`` entry before root graph storage destruction.

Output-tree coverage
--------------------

The subscription pass must cover owned duration TSWs at any fixed path,
including fields beneath ``TSB`` and fixed ``TSL`` outputs.  A node with several
duration windows creates several subscriptions to the same pull source.

Dynamic output trees require incremental attachment.  When a duration TSW is
created beneath a dynamic ``TSL`` or ``TSD``, the output-tree lifecycle attaches
it before or as part of publishing that structural change.  Erasure detaches it
before its storage is destroyed.  This should reuse the existing slot-observer
and source-invalidation patterns rather than rescan the complete output tree on
every tick.

Forwarding outputs and ``REF`` values do not create a second subscription to an
upstream window: only the output that owns the duration TSW storage registers
it.  Alternative views over the same storage likewise resolve to one
subscription identity.

Proposed execution algorithm
----------------------------

The storage/view layer will provide operations equivalent to:

.. code-block:: cpp

   DateTime next_expiry_time() const;
   TSWRemovalView evict_expired(DateTime now);

The exact names and return representation are subject to implementation review.
The root-level algorithm is:

1. Root graph assembly creates one expiry pull source when the complete graph
   can own at least one duration TSW and ranks it before corresponding normal
   root producers and nested-graph host nodes.
2. Root construction binds the source's typed forward reference into the
   reserved ``GlobalState`` entry before start.
3. Start-time attachment at any graph depth resolves that reference and
   registers each live window.  Registration reads its current oldest
   timestamp, so a restored or pre-seeded window is armed without persisting an
   alarm handle.
4. After a push, clear, restore, or removal, the TSW subscription reports its
   new ``next_expiry_time``.  An empty window removes its entry.
5. The root source maintains all entries and schedules itself for the earliest
   expiry across the complete graph.  Updating one window does not create
   another source or poll the other windows.
6. When evaluated, the source visits every entry due at or before the current
   evaluation time, calls ``evict_expired(now)``, and marks each changed TSW
   modified.
7. For a nested TSW, normal output notification schedules local consumers; the
   existing nested schedule propagation wakes each parent until the
   later-ranked root host node is scheduled in the same cycle.
8. The resulting mutation notification supplies the window's next expiry, and
   the source arms itself for the new global minimum.

The source's queue should use a stable subscription identifier plus a
generation to reject stale entries after reschedule, reset, dynamic erasure, or
address reuse.

Eviction marks the TSW modified and therefore notifies its expiry subscription
while the source is itself evaluating.  Queue updates must be re-entrant safe:
the entry being processed is identified by subscription and generation, and
the callback records its replacement deadline without invalidating the due-set
iteration.

When a normal producer tick and expiry coincide, root or nested rank evaluates
the expiry effect before the producer.  The producer can then append or
clear-and-append in the same cycle, and downstream consumers observe one final
window value and one structured delta containing all removals and additions.
Stage 1 must therefore allow scheduler removal followed by an owner mutation
through separate mutation scopes at the same evaluation time; the present
singular-delta guard is not sufficient.  A direct root push-source TSW producer
runs in the mandatory push phase before the expiry source; its structured delta
must normalize push/removal into the same final per-cycle result.

Window closure
--------------

The current implementation removes values whose timestamp is strictly less
than ``now - range``.  It therefore represents the closed interval
``[now-range, now]``.  With microsecond-resolution ``DateTime``, a value at
``t`` first becomes invalid at ``t + range + MIN_TD``.

The implementation should initially preserve this boundary for compatibility
and schedule that first invalid instant.  Tests must cover the exact boundary,
one microsecond after it, duplicate timestamps, and several values sharing one
expiry.  A different closure policy would require an explicit operator policy
and separate RFC review.

Removal surface
---------------

The C++ TSW surface will gain a per-evaluation range:

.. code-block:: cpp

   Range<ValueView> removed_values() const;

For a fixed-count TSW the range contains zero or one value.  For a duration TSW
it may contain any number.  The existing singular ``removed_value`` may remain
as a fixed-window convenience, but duration consumers must not silently receive
only the final removal.

The storage representation should own removed values only until the end of the
evaluation cycle.  The plan must establish stable borrowed-view lifetime,
destruction, and after-evaluation cleanup without allocating on the
single-removal fixed-window hot path.

Structured TSW delta
--------------------

The existing scalar TSW delta cannot represent clear or removal-only events.
A versioned structured delta is required before scheduled eviction can ship.
The candidate logical shape is:

.. code-block:: text

   TSWDelta<T>:
       cleared: bool
       added:   zero-or-one T
       removed: zero-or-more T

The implementation review must choose concrete value schemas that preserve
type erasure and avoid optional-value ambiguity.  A zero-or-one list for
``added`` is preferable to assigning sentinel meaning to a default ``T``.

``removed`` includes every value removed by expiry or fixed-window overflow.
On clear it includes the formerly retained values and ``cleared`` remains true,
including for an already-empty window.  This lets incremental consumers remove
their derived state while preserving the semantically important empty clear.

The review must decide whether removal timestamps are part of the public delta
or remain accessible only through a richer transient view.  This decision
requires examples using time-weighted and event-time algorithms before the
schema is accepted.

Recording and migration
-----------------------

The structured delta is a wire-format change and requires its own version
marker.  Readers must continue accepting legacy scalar TSW deltas, interpreting
each as ``cleared=false, added=[value], removed=[]``.  Writers emit only the new
format after migration.

A checkpoint for a duration TSW must preserve retained values and their
original evaluation timestamps.  Replaying values without timestamps changes
their future expiry and is not recovery.

The expiry-source queue, subscription generations, and graph alarm are derived
execution state and are not persisted.  Start-time subscription reads the
restored TSW and derives its next alarm from the oldest value.  If recovery
seeding occurs during the owning node's start hook, attachment must happen
first so the seed mutation updates the source.  A start-completion invariant
check must prove that every non-empty owned duration TSW has one live source
entry.

Recovery conformance is defined by comparing uninterrupted and recovered runs:
they must emit the same append, reset, and scheduled-removal cycles at the same
evaluation times.

Runtime and performance goals
-----------------------------

* Create at most one expiry pull source per root graph execution and one
  subscription per live, owned duration TSW across all nested graphs.
* Keep one root graph alarm for the earliest window expiry; the source owns the
  remaining per-window deadlines.
* Perform no polling ticks while the window is empty.
* For ``w`` live windows, update a deadline in ``O(log w)`` without scanning
  unrelated windows.  Evicting ``r`` observations from ``d`` due windows should
  cost ``O(r + d log w)``.
* Keep ordinary fixed-count TSW push and singular eviction allocation-free.
* Add no scheduler state to producer nodes and no process-global scheduler
  registry.  Standalone TSW value storage remains usable without a source
  subscription.
* Bound transient removal storage by the number of values evicted in the
  current evaluation cycle and release it after observers have run.

Implementation stages
---------------------

Stage 1: delta and mutation semantics
   Specify and implement the structured delta, multi-value removal storage,
   clear capture/apply, legacy scalar reads, and same-cycle removal followed by
   owner mutation.  Add direct native tests.  Do not schedule expiry yet.

Stage 2: root-graph expiry source
   Add automatic root source creation, the reserved ``GlobalState`` forward
   reference, rank dependencies, static output-tree subscriptions, deadline
   management, and due eviction.  Exercise direct TSW output, a TSW nested in a
   bundle, root and nested producers, and coincident producer/expiry cycles.
   Retain existing fixed-window behaviour.

Stage 3: recording and recovery
   Version the Arrow/JSON recording representation, preserve timestamps in
   checkpoints, restore TSW state, rebuild source subscriptions and deadlines,
   and compare uninterrupted with recovered execution.

Stage 4: dynamic, real-time, and nested lifecycle
   Add dynamic-child attachment and detachment.  Exercise wall-clock lateness,
   graph stop/start boundaries, nested graph teardown, reset cancellation, and
   no callbacks after node destruction.

Test plan
---------

Native and Python tests must cover:

* source silence beyond one or several expiry boundaries;
* exact closed-left boundary behaviour;
* multiple and duplicate-timestamp evictions;
* input, reset, and expiry coinciding in one cycle;
* one root expiry source shared by multiple producer nodes, nested graph
  instances, and multiple TSW fields;
* a direct TSW output, a bundle-contained TSW, and dynamically created TSW
  children;
* root-source rank, nested schedule propagation, and downstream same-cycle
  notification order;
* incremental sum/mean consumers using ``removed_values``;
* old scalar-delta recordings read by the new implementation;
* clear-only and removal-only delta record/replay;
* checkpoint recovery during warm-up and immediately before expiry;
* recovery with an already-overdue oldest value;
* simulation, real-time, nested graph stop, and teardown;
* no scheduler or transient-removal allocation on fixed-count push; and
* an installed C++ extension node whose output contains a duration TSW, without
  depending on ``to_window``.

Unresolved questions
--------------------

* Whether public removal deltas require original timestamps as well as values.
* The exact structured value schema used for zero-or-one ``added``.
* Whether the delta migration belongs in the next major wire version or can be
  introduced under a TSW-specific metadata version.
* Whether the expiry source should use an indexed heap or a lazy heap with
  subscription generations.
* The exact output-tree hook used to attach dynamic duration TSW children
  without a full-tree scan.
* Whether scheduler lateness should expose the scheduled expiry time in
  addition to the actual evaluation time.

Acceptance criteria
-------------------

This RFC may move to ``Accepted`` only when all four implementation stages,
versioned migration tests, native/Python parity, installed-SDK coverage, and the
complete acceptance suites pass.

References
----------

* :doc:`rfc_0000`
* :doc:`rfc_0006_tsw_reset_and_clear`
