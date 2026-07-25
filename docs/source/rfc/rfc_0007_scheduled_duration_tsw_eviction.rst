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
silent.  The ``to_window`` node, not the output or storage object, owns the
scheduler interaction.  The work also replaces the singular removal surface
and scalar-only TSW delta with representations capable of describing multiple
evictions, reset-only ticks, and removal-only scheduled ticks.

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

Scheduler ownership
-------------------

TSW value storage remains independent of graph execution and scheduling.  It
must be usable in values, tests, record/replay materialisation, and extension
code without owning a node or graph.

``to_window`` is the node that knows all required parties:

* the duration and closure rule;
* the output TSW;
* the current evaluation time; and
* its injected ``NodeScheduler``.

The duration overload will therefore inject ``NodeScheduler`` and manage one
tagged ``"expiry"`` alarm.  The output view will not retain a scheduler pointer
or call back into its owning node.

Proposed execution algorithm
----------------------------

The storage/view layer will provide operations equivalent to:

.. code-block:: cpp

   DateTime next_expiry_time() const;
   TSWRemovalView evict_expired(DateTime now);

The exact names and return representation are subject to implementation review.
The node algorithm is:

1. On reset, cancel ``"expiry"`` and clear the window.
2. On a due ``"expiry"`` alarm, evict every value outside the time range.
3. On a valid source tick, append it after reset/expiry processing.
4. If the window is non-empty, schedule ``"expiry"`` for the oldest retained
   value's expiry time; otherwise leave the tag unscheduled.
5. Mark the TSW modified once when any clear, removal, or append occurred.

When an input and alarm coincide, expiry is processed before append.  This
gives one deterministic TSW delta for the evaluation cycle and avoids
transiently exceeding the duration contract.

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

Scheduled alarm handles are not persisted.  Recovery restores the TSW current
state and then derives the next alarm from the oldest value.  The implementation
must verify graph-start ordering so re-arming occurs after output seeding.  If
the existing lifecycle cannot guarantee that ordering, add an explicit
post-recovery/re-arm hook rather than persisting scheduler internals.

Recovery conformance is defined by comparing uninterrupted and recovered runs:
they must emit the same append, reset, and scheduled-removal cycles at the same
evaluation times.

Runtime and performance goals
-----------------------------

* Maintain at most one pending expiry alarm per duration ``to_window`` node.
* Perform no polling ticks while the window is empty.
* Make each alarm ``O(k)`` for the ``k`` values actually evicted, plus ``O(1)``
  rescheduling.
* Keep ordinary fixed-count TSW push and singular eviction allocation-free.
* Avoid an output-to-node pointer, scheduler pointer in TSW storage, or
  process-global scheduler registry.
* Bound transient removal storage by the number of values evicted in the
  current evaluation cycle and release it after observers have run.

Implementation stages
---------------------

Stage 1: delta and mutation semantics
   Specify and implement the structured delta, multi-value removal storage,
   clear capture/apply, legacy scalar reads, and direct native tests.  Do not
   schedule expiry yet.

Stage 2: node scheduling
   Add the tagged scheduler to duration ``to_window``; implement expiry-only and
   coincident input/expiry cycles in simulation; retain existing fixed-window
   behaviour.

Stage 3: recording and recovery
   Version the Arrow/JSON recording representation, preserve timestamps in
   checkpoints, restore TSW state, re-arm the derived alarm, and compare
   uninterrupted with recovered execution.

Stage 4: real-time and nested lifecycle
   Exercise wall-clock lateness, graph stop/start boundaries, nested graph
   teardown, reset cancellation, and no callbacks after node destruction.

Test plan
---------

Native and Python tests must cover:

* source silence beyond one or several expiry boundaries;
* exact closed-left boundary behaviour;
* multiple and duplicate-timestamp evictions;
* input, reset, and expiry coinciding in one cycle;
* incremental sum/mean consumers using ``removed_values``;
* old scalar-delta recordings read by the new implementation;
* clear-only and removal-only delta record/replay;
* checkpoint recovery during warm-up and immediately before expiry;
* recovery with an already-overdue oldest value;
* simulation, real-time, nested graph stop, and teardown;
* no scheduler or transient-removal allocation on fixed-count push; and
* installed C++ extension use of the new removal surface.

Unresolved questions
--------------------

* Whether public removal deltas require original timestamps as well as values.
* The exact structured value schema used for zero-or-one ``added``.
* Whether the delta migration belongs in the next major wire version or can be
  introduced under a TSW-specific metadata version.
* Which lifecycle hook should re-arm derived alarms after recovery seeding.
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
