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
including outputs inside nested graph instances, shares one expiry service.
The root owns it in a native execution slot alongside ``GlobalState``.
Duration windows subscribe to the service, which schedules the earliest expiry
and evicts every due sample when the root runs.  This covers ``to_window`` and any other node whose output is,
or contains, a duration TSW without giving each producer or nested graph its
own scheduling implementation.  The work also replaces the singular removal
surface and scalar-only TSW delta with representations capable of describing
multiple evictions, reset-only ticks, and removal-only scheduled ticks.

Stage 1 introduced the window delta and its in-memory and distributed
representations. Stage 2 adds a native root expiry service for duration windows
at fixed owned output paths, including paths inside nested graph instances.
Stage 3 adds versioned durable window events and restores live deadlines from
checkpointed sample times. Stage 4 covers dynamic collection attachment.

Problem statement
-----------------

Before Stage 2 a live duration TSW pruned old values only while pushing a new
source value.  Given a ten-second range:

.. code-block:: text

   t=0s   1
   t=1s   2
   t=2s   3
   t=20s  4

the values ``1``, ``2``, and ``3`` used to remain visible until ``t=20s``
even though they expired earlier. At ``t=20s`` several values left together.
The pre-RFC C++ surface reported only the last removal, so incremental
consumers could lose track of the others.

Root-graph expiry source
------------------------

TSW value storage remains independent of graph execution and scheduling.  It
must be usable in values, tests, record/replay materialisation, and extension
code without requiring a graph.  A standalone duration window continues to
prune on explicit mutation; scheduled eviction is an execution-layer service.

Each graph definition plans its duration-window paths once. Both public wiring
and direct C++ graph builders use that plan. A fixed-list step represents all
positions without expanding the schema into one path per element.

The first eligible owner creates a single pull service in the root's native
global state. This typed execution slot sits alongside the run configuration.
Nested owners borrow a weak forward reference to that root service. Copying
the configuration copies no deadlines or endpoint references. Roots without
live duration owners allocate no service.

Start subscribes each fixed-path owner after the node start hooks and
checkpoint callbacks have restored their outputs. The service contributes its earliest deadline to the root's next scheduled time.
When the root runs, it pulls due windows in the phase immediately after push
sources and before ordinary nodes. This gives each consumer the final state for
the cycle while preserving public node indices, edges and observer identities.
Graph stop and failed start detach registrations before endpoint destruction.

Recording readers, transport endpoints and operators that deliberately replay,
sample or delay window publications preserve their own event timing. These
outputs reproduce the source's departures when the operator publishes them.
Forwarding endpoints borrow the real owner's publication. A custom native
node that replays such a stream declares ``replays_output_events = true`` on
its implementation. A C++ graph builder can set the same policy on a node
instance with ``builder.replays_output_events(true)``. An ordinary window
producer uses the default live policy.

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

The storage view exposes ``next_expiry_time()`` and the existing
``begin_mutation(now).expire()``. The root algorithm is:

1. An owned window registers one stable alarm. Registration reads the oldest
   timestamp so pre-seeded windows have an initial deadline.
2. The source stores armed owners in an indexed minimum heap. Each registration
   records its position; cancellation erases it immediately. A stable insertion
   sequence orders equal deadlines. Empty windows have no heap entry.
3. At evaluation the root source removes each due alarm before invoking it.
   Expiry publishes all samples at or beyond their first invalid instant as one
   removal delta. A callback can cancel or reschedule an alarm without leaving
   a stale heap entry.
4. Ordinary output notification wakes downstream nodes. When the owner is
   nested, the existing child scheduling machinery wakes its parent in this
   cycle. The source observes each modified owner and refreshes its deadline.
5. Notifications coalesce at most once per endpoint per evaluation. The service
   keeps a deduplicated list of touched registrations and rereads their final
   deadlines after the root finishes, covering expire-then-append and
   clear-then-push. Cancelling a registration also unlinks it from this list.
6. A newly installed snapshot can already be overdue. A pre-start snapshot is
   processed on the first evaluation. A snapshot published after the root's
   expiry phase can first wake it on the next representable instant.

When a normal producer tick and expiry coincide, the root processes the
deadline before the producer evaluates in its own graph.  The producer can then append or
clear-and-append in the same cycle, and downstream consumers observe one final
window value and one structured delta containing all removals and additions.
Stage 1 allows a scheduled removal followed by an owner push or clear
through separate mutation scopes at the same evaluation time. A direct root
push source runs before the expiry service. Its window delta combines the
push and removal in the same cycle.

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

C++ window inputs and outputs expose parallel ranges of removed values and
original sample times, oldest first. At the data-view level the caller supplies
the evaluation time. Python window views expose the values through
``removed_values()`` and the timestamped samples through ``window_delta``.
The legacy singular ``removed_value`` reports the last overflow or expiry
on an ordinary tick. Reset clears this singular observation. The plural range
includes everything discarded by a reset, even for a fixed-count window.

Structured TSW delta
--------------------

A canonical incremental change has the following fields:

.. code-block:: text

   cleared: bool
   added:   list<T>                  # zero or one
   removed: list<{value: T, time: DateTime}>

``cleared`` distinguishes a reset on an already-empty window from silence.
If expiry removes a prefix and the producer clears and pushes in the same
cycle, the delta contains the prefix, the other cleared samples, and the
single addition. Equal values and equal times retain their multiplicity.
The receiving evaluation time stamps an incremental addition. Each removal
carries its original sample timestamp, even when a receiver applies it later.

Assigning a whole window or sampling a newly bound window uses the optional
``replacement`` field. Its ordered list of timestamped samples supplies the
complete current state. A present empty list installs an empty window. This
form has ``cleared=false`` and empty incremental lists.

The native scalar observation and Python ``delta_value`` retain the arriving
value on incremental ticks. A removal-only tick has no arriving value.
For a duration window, an explicit zero minimum makes the first sample
fully ready. Omitting the minimum uses the full duration.

Recording and migration
-----------------------

Stage 1 uses the stored value schema to distinguish legacy scalar arrivals
from captured structured deltas. A legacy arrival is applied at its recorded
time under the original prune-on-push behaviour. Gaps in its historical trace
stay silent. A recorded structured removal supplies its own timestamped samples.
The distributed boundary marks its new window payload explicitly and still
reads the earlier clear/push encoding. Deploy compatible readers before new
writers.

Stage 3 writes window-bearing JSON deltas as an object containing a numeric
format version and the canonical event. Arrow frames carry the version in
schema metadata and store the window delta as a typed struct. The storage
formats, legacy reads and deployment order are described in
:ref:`durable-window-records`.

A live checkpoint already stores sample values, their original timestamps
and window validity. A restored owner attaches to the root expiry service.
Each graph attaches its fixed-path owners after its node start hooks and
checkpoint callbacks have run, before the graph-started notification.
Attachment reads each owner's final oldest sample once. This also arms an
independently started nested graph. Empty windows contribute no deadline. The checkpoint contains no scheduler
handles or heap entries.

Resuming before the next deadline preserves the uninterrupted departure
times. Resuming exactly on the deadline publishes the departure in the first
cycle. Resuming later removes every overdue sample in one publication at the
resumed time. Its removal list still carries each original arrival time.

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
   Add automatic root source creation, the typed root global-state execution
   slot, pre-consumer dispatch, static output-tree subscriptions, deadline
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
* root pull-phase ordering, nested schedule propagation, and downstream
  same-cycle notification order;
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
