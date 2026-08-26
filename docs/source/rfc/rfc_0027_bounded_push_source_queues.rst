RFC 0027: Push-Source Queue Models and the Sender Contract
===========================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-21
:Target: Core runtime push-source policies, extension SDK, Python ``push_queue``

Summary
-------

Make the push source a policy-selected cross-thread delivery boundary.  It
supports:

* a FIFO queue with optional capacity and event-by-event delivery;
* the same FIFO queue with optional capacity and burst delivery, accepting
  individual scalar values and emitting the values currently pending as one
  homogeneous tuple; and
* a conflating current-state accumulator.

Replace the sender's ``void send(Value)`` with an explicit pair:

.. code-block:: cpp

   [[nodiscard]] bool try_send(Value value) const;
   bool               send_blocking(Value value) const;

``try_send`` returns ``false`` when the selected policy cannot accept the
value.  ``send_blocking`` waits until it can and returns ``true``, or returns
``false`` if the receiver stops before admission.  Its result may be discarded
when a producer has no stop-specific action.  Capacity is a count of admitted
elements; there is no byte accounting in the core contract.

The push-source node then *is* the queue.  A service implementation uses one
push source to carry every value its adaptor returns to the graph, in one
order, and demultiplexes into separate outputs with ordinary graph nodes.
Downstream transport extensions migrate from their private queue ownership to
this shared core contract.

The ``Conflating`` policy remains the public
``@push_queue(..., conflate=True)`` behaviour.  The new ``Burst`` selection is
a delivery mode over the FIFO queue, not a different enqueue model.  Its
output must be ``TS[tuple[SCALAR, ...]]``, while its sender accepts one
``SCALAR`` per call.  It has the same bounded or unbounded admission behaviour
as the ordinary queue and only changes how values already pending are
delivered when the graph evaluates.  It does not introduce a timer or target
batch size.

Motivation
----------

The existing ``Queue`` policy is an unbounded ``std::deque<Value>``
(``src/hgraph/runtime/push_source_node.cpp``) and ``PushSourceSender::send``
returns ``void``, discarding the ``bool`` the policy already produces.  A
producer therefore has no way to learn that the graph is behind, and no way to
bound the memory its arrival rate consumes.

The existing FIFO policy also imposes one graph evaluation per admitted value.
That is correct for event-by-event delivery, but inefficient for consumers that
can naturally process a homogeneous collection.  Such producers currently
need another queue and drain node merely to turn several scalar arrivals into
one ``TS[tuple[SCALAR, ...]]`` tick.  Burst delivery uses the same push-source
FIFO queue and makes only the delivery choice at wiring time.

Two independently built downstream transport extensions encountered the same
requirement: bounded record admission, non-blocking refusal for producers with
a liveness obligation, blocking admission for simple producers, and safe
shutdown of cross-thread waiters.  Both had to own queueing outside the push
source because core did not provide that contract.  Their independent
production use supplies the promotion evidence; their private representations
are not part of this RFC.

Duplicating this boundary also weakens ordering: related values placed in
independent queues have no defined order across those queues.  A single push
source queue gives that ordering by construction.

RFC 0015 anticipated this and deferred it explicitly:

   A generic bounded cross-thread channel may be proposed for core only after
   a second downstream integration demonstrates the same contract and supplies
   promotion evidence.

``hgraph-web`` is that second integration.  The gate in RFC 0000 is met.

RFC 0015 also recorded the reason payloads did not go through the push source:

   The existing queue push-source policy is intentionally unbounded.  Passing
   every Kafka payload directly to it would allow broker rate to determine
   graph memory use.

That is an argument against the *current unbounded* ``Queue``, not against a
push source owning payload.  Once the queue has a capacity the argument no
longer holds, and the second queue has no reason to exist.

Ownership boundary
------------------

The core owns:

* the ``Queue`` policy's capacity, admission decision, and blocking wait;
* the queue's event or burst delivery boundary;
* the sender contract (``try_send`` / ``send_blocking``) and its stop
  semantics;
* releasing bounded capacity when the graph dequeues work; and
* the Python ``push_queue`` exposure of blocking admission and policy
  configuration.

The core does **not** own:

* byte accounting or payload sizing — a domain that needs to bound bytes
  bounds them in its own producer before calling ``try_send``;
* watermark fractions, pause/resume, and what a producer does when refused;
* protocol-level completion, acknowledgement, message identity, or offsets;
  a protocol that needs these wires an explicit sink node downstream;
* the envelope schema an adaptor multiplexes onto one push source.

This keeps the core contract domain-independent as RFC 0000's promotion gate
requires.  Transport-specific identity, validation, and flow-control decisions
remain downstream; queue storage and admission move to core.

Public C++ contract
-------------------

Sender
~~~~~~

.. code-block:: cpp

   class HGRAPH_EXPORT PushSourceSender
   {
     public:
       PushSourceSender() noexcept;

       [[nodiscard]] bool valid() const noexcept;
       [[nodiscard]] const TypeRealizationSnapshot *type_realization() const noexcept;

       /** Enqueue if the policy can accept the value now.
        *
        * Returns false when the queue is at capacity or the node is not
        * accepting (before start, after stop). Never blocks. The value is
        * destroyed when the call returns false; a producer that needs to
        * retry retains its own copy. */
       [[nodiscard]] bool try_send(Value value) const;

       /** Enqueue, waiting for capacity if necessary.
        *
        * Returns false only when the node stops before admission. Must not be
        * called from the graph evaluation thread when it would wait. */
       bool send_blocking(Value value) const;

       template <typename T> [[nodiscard]] bool try_send(T &&value) const;
       template <typename T> bool send_blocking(T &&value) const;
   };

``void send(Value)`` is removed rather than deprecated.  It has few call sites
(``python/py_carriers.h``, both extension bridges, three test files), the
bridges are being retired by this RFC, and leaving a silent-discard overload
beside an explicit pair would reintroduce the ambiguity the change exists to
remove.

Policy selection
~~~~~~~~~~~~~~~~

The policy is selected once from wiring-time metadata and remains behind the
existing type-erased ``PushSourcePolicy`` operations:

.. code-block:: cpp

   enum class PushSourcePolicyKind : std::uint8_t
   {
       Queue,
       Conflating,
       Burst,
   };

``Queue`` with ``max_pending == 0`` is unbounded.  ``Queue`` with a non-zero
``max_pending`` is bounded.  ``Conflating`` retains one accumulated current
state.  ``Burst`` selects the queue's alternative delivery operation: enqueue,
capacity checks, refusal, blocking, and producer wake-up remain those of
``Queue``, while evaluation emits a tuple containing a snapshot of all
elements pending at its start.

The public sender type does not change with the model.  Its ``sender_schema()``
describes the value accepted by each call: the output delta schema for
``Queue`` and ``Conflating``, and the tuple element schema for ``Burst``.

Capacity
~~~~~~~~

.. code-block:: cpp

   [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_queue_policy(
       const ValueTypeMetaData &sender_schema, std::size_t max_pending = 0);

   [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_queue_policy(
       const TSValueTypeMetaData &output_schema, std::size_t max_pending = 0);

``max_pending == 0`` means unbounded, matching the convention
``QueueView::has_max_capacity()`` already uses for the ``Queue`` value type.
Existing callers keep today's behaviour without change; bounding is opt-in.

``max_pending`` counts elements admitted and still queued.  Dequeuing an
element for graph processing releases its capacity immediately.

Because burst uses the FIFO queue, its factory exposes the same capacity
setting:

.. code-block:: cpp

   [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_burst_policy(
       const TSValueTypeMetaData &output_schema, std::size_t max_pending = 0);

There is intentionally no overload taking only a sender schema.  The factory
needs the complete output schema both to validate the tuple result and to
derive its scalar element schema.  A non-zero ``max_pending`` limits pending
scalar elements, not the number of tuple ticks.  The existing generic
``make_push_source_policy(kind, sender_schema)`` overload rejects ``Burst`` and
directs callers to this factory because a sender schema alone cannot establish
the output tuple contract.

Burst contract
~~~~~~~~~~~~~~

For this RFC, ``Burst`` supports only an atomic time-series output whose scalar
value is a homogeneous variadic tuple:

.. code-block:: text

   output: TS[tuple[SCALAR, ...]]
   sender: SCALAR

The equivalent native schema is ``TS<HomogeneousTuple<T>>``.  In runtime
metadata, Python ``tuple[T, ...]`` is the immutable variadic-tuple form of the
list representation: it has one element schema and the ``VariadicTuple`` flag.
Fixed heterogeneous tuples, mutable lists, ``TSL``, ``TSS``, ``TSD``, ``TSW``
and nested time-series elements are rejected by the burst-policy factory.

Each sender call enqueues one scalar ``Value`` through the ordinary FIFO queue
admission path.  ``try_send`` and ``send_blocking`` therefore respond to
``max_pending`` exactly as they do for event-by-event delivery.  The difference
begins only when the push source evaluates: event delivery dequeues one value,
whereas burst delivery takes one lock and detaches every scalar currently
queued.  It then releases the lock, constructs one tuple in FIFO order, and
applies that tuple as the ``TS`` output delta.  Values admitted after the
detach form the next burst and re-arm the executor.  An empty tuple is never
emitted; a burst containing one scalar emits a one-element tuple.

This defines a deterministic boundary for a concurrent producer without
claiming a total order across producer threads: values appear in the order in
which their admissions acquire the policy mutex.  It also keeps tuple
construction and output mutation off producer threads and outside the queue
lock.

Detaching a burst releases capacity for every element in that burst and wakes
blocked producers.  The detached values have entered graph processing and no
longer belong to the sender queue.

.. code-block:: cpp

   auto policy = make_push_source_burst_policy(
       *ts_type<TS<HomogeneousTuple<Int>>>(), 256);

   // Both calls contribute scalar elements to a later tuple-valued tick.
   sender.send_blocking(Int{1});
   sender.send_blocking(Int{2});

Protocol acknowledgements
~~~~~~~~~~~~~~~~~~~~~~~~~

Dequeuing a value means the graph has started processing it, so queue capacity
is released at that point.  The sender has no completion handle, release mode,
or outstanding-work count.

Some protocols need an explicit acknowledgement after downstream processing
has completed.  That is protocol implementation, not sender admission.  Such
an implementation carries any required identity in its data and wires a sink
node at the appropriate point in the graph to perform the acknowledgement.
The sink's lifecycle and failure semantics are defined by that protocol and do
not change capacity in the push-source queue.

Usage model
~~~~~~~~~~~

A service implementation uses one push source for each independently ordered
asynchronous result stream.  When several related result kinds require one
order, their sender schema is a discriminated envelope and the implementation
demultiplexes it with ordinary graph nodes.  A service implementation may own
more than one push source when the external streams are genuinely independent.

This is the point of the design rather than an incidental economy.  One queue
is one order, so values that are related — a record and the state change that
explains it, a delivery failure and its event — arrive in the order the adaptor
produced them.  Several push sources cannot promise that at any price.

The core does not expose the sender's queue, lock, condition variable, executor
notification, or selected policy to the service implementation.  Adaptors only
retain the sender and use its admission operations.

Python contract
---------------

``PySender.send`` maps to native ``send_blocking`` and returns its boolean
admission result:

.. code-block:: python

   @push_queue(TS[int])
   def my_source(sender: Callable[[int], bool]):
       accepted = sender(1)  # blocks if full; false only after receiver stop

``PySender::send`` already releases the GIL around the native call
(``python/py_carriers.h``), so blocking there does not stall the interpreter.
A stopped node returns ``False``.  The common producer loop may discard this
result because its stop callback requests task shutdown; a producer that needs
different shutdown behaviour can inspect it.  Internal sender lifecycle or
queue implementation types are not exposed to Python.

``@push_queue`` also supports a lifecycle stop hook.  The start hook constructs
and starts an external task or worker thread and stores it in ``STATE``; the
stop hook requests shutdown and joins it:

.. code-block:: python

   @push_queue(TS[int])
   def my_source(sender, state: STATE = None):
       state.task = start_task(sender)

   @my_source.stop
   def stop_my_source(state: STATE = None):
       state.task.request_stop()
       state.task.join()

``@push_queue`` gains optional ``max_pending`` and ``burst`` keyword arguments:

.. code-block:: python

   @push_queue(TS[int])
   def unbounded(sender): ...

   @push_queue(TS[int], max_pending=256)
   def bounded(sender): ...

   @push_queue(TS[int], conflate=True)
   def latest(sender): ...

   @push_queue(TS[tuple[int, ...]], burst=True, max_pending=256)
   def batches(sender):
       sender(1)  # sends int; graph receives tuple[int, ...]

``max_pending`` defaults to ``None`` (unbounded).  ``conflate=True`` rejects
``max_pending``, since a conflating policy retains exactly one accumulated
value.  ``conflate`` and ``burst`` are mutually exclusive.  ``burst=True``
requires a ``TS[tuple[SCALAR, ...]]`` output and permits ``max_pending`` because
its queue holds the individual scalar elements.

A non-blocking Python surface (``try_send`` returning ``bool``) is deliberately
left for a later RFC once there is a Python producer that needs it.

Runtime representation
----------------------

``PushSourcePolicyOps`` replaces ``send_impl`` with two admission operations.
The erased result separates successful admission from the need to wake the
executor; these are distinct for a conflating delta that is accepted but makes
no effective change:

.. code-block:: cpp

   struct PushSourceSendResult
   {
       bool accepted;
       bool wake_required;
   };

   PushSourceSendResult (*try_send_impl)(
       const void *context, void *storage, Value value);
   PushSourceSendResult (*send_blocking_impl)(
       const void *context, void *storage, Value value);

and ``QueuePolicyStorage`` gains ``max_pending`` and a
``std::condition_variable`` for the blocking wait.
No new mechanism is introduced: the policy is already a struct of function
pointers plus a context, following the repository's passive ops-table and
explicit erased-ownership pattern.

Threading:

* the queue mutex is a producer/consumer boundary that already exists — this
  RFC does not add a per-tick lock;
* the condition variable is notified when ``emit_next`` dequeues a value and
  by ``stop``;
* ``stop()`` sets ``accepting = false`` and notifies all waiters *before*
  the node's producer threads are joined, so a blocked ``send_blocking``
  cannot deadlock teardown; and
* several producer threads may share one sender.  Both entry points are
  safe under the queue mutex.

``send_blocking`` must never wait on the graph evaluation thread.  The queue
captures its consumer thread at start and rejects a full-queue call from that
thread before entering the condition-variable wait.  An immediately
admissible call remains legal during a start callback, preserving existing
unbounded start-hook behaviour.

The sender remains a non-templated, type-erased facade.  Its private, non-null
operations table dispatches either to the live control block or to a canonical
stopped sentinel; default and moved-from senders therefore return ``false``
without branches at call sites.  The selected policy validates each admitted
``Value`` against its sender schema.  Queue and conflating policies therefore
continue to accept the output delta schema; burst accepts the element schema
derived from its output tuple.  There is no per-send or per-evaluation registry
lookup.

The control block holds the sender's internal executor projection.  A policy
first queues or applies the value under its own synchronization and returns
whether executor wake-up is required.  Only after successful admission does
the sender mark push work pending and notify the real-time executor's condition
variable.  The executor consults that pending state to decide whether to run
the push-source prefix in the next cycle; the same notification wakes an
executor waiting for its next scheduled time.  External producer code never
receives the executor projection or invokes the graph directly.

The ``Burst`` selection uses the queue storage, admission operations, and
condition-variable machinery unchanged.  Only its ``emit_next`` operation is
different: it detaches the pending deque under the policy mutex, releases
capacity for the detached elements, and constructs the variadic tuple using
the output schema's preselected ``ValueOps`` strategy.  Building and applying
the tuple occur after releasing the policy mutex.

Sender lifetime and stop
~~~~~~~~~~~~~~~~~~~~~~~~

The current sender borrows raw node and executor storage, and ``valid()`` only
reports that those pointers were originally bound.  A blocking operation makes
that insufficient: graph teardown must not destroy a condition variable while
a sender is waiting on it, and a retained sender must not dereference graph
storage after executor destruction.

The implementation therefore adds a small shared sender-control object.  It
outlives node storage, rejects new operations once closing begins, and counts
operations that have entered policy storage.  Stop proceeds in this order:

1. mark the sender control as closing, preventing new policy calls;
2. mark the policy non-accepting and notify its capacity waiters;
3. wait for entered sender operations to leave policy storage;
4. run the node's stop callback; and
5. detach the control from node and executor storage before either is
   destroyed.

After step 1, both sender operations return ``false`` without accessing node
storage.  ``valid()`` means the control is attached to a running, accepting
policy, rather than merely holding a non-null historical pointer.  This is one
small, explicit erased-ownership boundary per push source; payload and queue
storage remain owned by the node's planned storage.

Performance and memory
----------------------

The change removes work from the per-tick path rather than adding it.  Current
downstream compositions pass a record through a private queue, a conflated
wake-up, the push source, and a drain node.  Moving admission into the push
source removes the duplicate queue, repeated evaluation-thread locking, and
per-record type reconstruction.  The linked downstream migration PRs provide
the before/after measurements without making their private implementation part
of this public contract.

Bounded admission is one comparison against ``max_pending`` inside the lock the
policy already takes.  ``max_pending == 0`` short-circuits it, so existing
unbounded users are unaffected.

For a burst containing *n* elements, enqueue is the same O(1) operation as the
ordinary FIFO queue, detaching the pending deque is O(1), and tuple
construction/application is O(n) for that graph tick.  Retained memory is
O(n), bounded by ``max_pending`` when non-zero.
The evaluation thread holds the policy mutex only for the detach and capacity
update, not for the O(n) tuple construction.  The implementation must move or
transfer admitted values into the tuple representation without an avoidable
second payload copy.

Evidence to be supplied with the implementation:

* allocation and latency comparison for a downstream subscription path before
  and after its duplicate queue and drain are removed;
* no per-record type-registry acquisition in the migrated evaluation path; and
* throughput of a saturated bounded queue against today's unbounded one; and
* burst throughput and allocation count across representative burst sizes,
  including comparison with an external queue plus drain node.

Compatibility and migration
---------------------------

Source compatibility
   ``PushSourceSender::send`` is removed.  Every call site is in this
   repository.  The Python carrier moves to ``send_blocking``; downstream
   extensions remove or delegate their private queues; native tests move to
   ``try_send`` with an assertion or to ``send_blocking``.

Behavioural compatibility
   Default ``max_pending == 0`` preserves today's queue semantics.  The one
   visible change for existing users is that Python ``sender(value)`` returns
   a boolean; after stop it returns ``False`` instead of silently discarding.
   ``burst=False`` is the default, so existing Python output schemas and sender
   payload types are unchanged.

ABI
   ``PushSourcePolicyOps`` is a private detail structure but it is reachable
   through the installed SDK's headers, so this is a rebuild-required change
   for downstream native extensions.  It is grouped with the RFC 0026 node
   metadata layout change in the same release rather than adding a second
   rebuild point.

Serialization
   None.  Policies are wiring-time construction, not manifest content;
   ``max_pending`` and ``burst`` are builder arguments and do not alter node
   schema identity.  A graph manifest records the selected concrete policy in
   its node construction metadata when push-source policy serialization is
   introduced; this RFC does not introduce manifest support.

Downstream incubation evidence and removal plan
-----------------------------------------------

The bounded sender contract has two independent downstream implementations to
draw from.  Both demonstrate bounded record admission, producer-visible
refusal, blocking producer support, and lifecycle wake-up.  Domain-specific
byte accounting, watermark policy, message identity, and producer flow control
remain downstream.  The common queue ownership and sender semantics promote to
core.

The bounded sender contract is promoted from those two downstream
implementations.  ``Burst`` is a new composition over the existing homogeneous
tuple representation rather than a promoted downstream algorithm.  Before
this RFC can become ``Accepted``, the implementation must supply the native and
Python behavioural coverage, installed-SDK use, and allocation/latency evidence
listed below.  If implementation experience shows that tuple materialisation
cannot meet the stated cost, burst is removed from this RFC rather than
introducing a second scalar collection representation.

Removal plan, as separate linked pull requests after the core change lands:

1. ``hgraph-kafka`` delegates record admission and storage to one core push
   source, removes its duplicate queue, and uses an ordinary scheduled node for
   deterministic simulation replay.
2. ``hgraph-web`` delegates record admission and storage to the same core
   contract and removes its duplicate queue.

Each downstream PR documents compatibility, dependency, and release ordering,
and links back to the core implementation PR.

Out of scope
------------

Recorded so they are not re-proposed against this RFC.

Time
   Push sources carry no time.  Scheduling a value for a future evaluation is
   ``NodeKind::PullSource``.  Timestamp-ordered replay belongs in a pull source
   rather than being generalised into this real-time admission contract.

Other batching shapes
   Burst intentionally models a batch as the scalar value of
   ``TS[tuple[SCALAR, ...]]``.  It does not change the delta contract of
   ``TS<T>``, batch ``TSW`` elements, manufacture multi-element ``TSS``/``TSD``
   deltas, accept heterogeneous fixed tuples, or recursively batch structural
   time-series values.  Those shapes require separate evidence and contracts.

Batch size and time
   Burst has no target size, maximum emission size independent of queue
   capacity, linger duration, or timer.  The first admitted value wakes the
   graph; evaluation takes whatever has accumulated by that point.  A producer
   needing a minimum-size or time-windowed batch owns that policy outside the
   push source until there is implementation evidence for a general contract.

Byte bounds
   Excluded from the core contract by decision.  A producer that must bound
   bytes tracks them itself and stops calling ``try_send``.

Alternatives considered
-----------------------

Capacity from a ``Queue<T>`` value schema
   ``ValueTypeKind::Queue`` already exists, carries ``max_capacity`` in its
   metadata via ``TypeRegistry::queue(element_type, max_capacity)``, and
   ``QueueView`` exposes ``full()`` and ``max_capacity()``.  Holding the
   pending values in such a value would put capacity in the type system, where
   it would appear in manifests and node inspection.

   Deferred.  ``static_schema.h`` has no ``Queue<T>`` authoring alias — the
   type has exactly one caller in the tree, in the Python container conversion
   path — so this RFC would have to introduce native authoring for a value kind
   as a side effect.  The policy's elements are also *delta* values of the
   output schema rather than instances of one declared element type, which is
   not the shape ``Queue<T>`` describes.  A builder parameter is the smaller
   change; the value-typed queue remains available later.

Keeping ``send`` and adding a bool overload
   Rejected.  An overload set where one member silently discards a failure and
   another reports it is the ambiguity this RFC removes.

A separate bounded-channel type in core, outside the push source
   Rejected.  It would be a third structure between the producer and the node,
   which is what both bridges built and what this RFC retires.  The push
   source node is the queue.

Blocking as the only mode
   Rejected.  A producer with a liveness obligation on its own thread cannot
   block; a broker client, for example, may need to keep polling to serve
   heartbeats and lifecycle callbacks.  ``try_send`` plus producer-side flow
   control is the correct mode for such a producer.

Non-blocking as the only mode
   Rejected.  A producer with no such obligation — a file reader, a replay
   feed — should be able to simply wait, and forcing it to spin or build its
   own condition variable recreates the problem.

Changing ``TS<T>`` to carry several deltas per tick
   Rejected.  ``TS<T>`` has one scalar delta ``T``.  Burst preserves that
   contract by selecting ``T == tuple[SCALAR, ...]`` at wiring time.  Changing
   the time-series substrate would affect every node and operator for a result
   already expressible with the existing homogeneous tuple scalar.

Passing a tuple to the sender
   Rejected for burst.  It would make the producer choose batch boundaries and
   leave the push source as an ordinary queue of tuple values.  That remains
   possible with the normal queue policy.  Burst specifically accepts one
   scalar per call so the graph's evaluation cadence defines the opportunistic
   batch boundary.

Deferred questions
------------------

1. **Control-lane headroom.**  Some transports must guarantee that a lifecycle
   record cannot be starved by a full payload queue.  With one queue this is
   reserved headroom rather than a second lane.  It remains downstream until
   the migrations establish a shared domain-independent requirement.

2. **Two-phase reserve.**  Some producers reserve capacity before performing
   work so its resulting control record always has somewhere to land.  This is
   deferred until multiple migrations establish a shared contract.

Implementation plan
-------------------

Stage 1 — sender and capacity
   ``try_send`` / ``send_blocking``; ``max_pending`` on the queue policy;
   Python ``send`` mapped to ``send_blocking`` and ``max_pending`` on
   ``@push_queue``; safe sender-control lifetime and stop quiescence.  No
   extension changes beyond the mechanical call-site move.

Stage 2 — burst delivery
   ``PushSourcePolicyKind::Burst`` and
   ``make_push_source_burst_policy``; reuse of queue storage and admission;
   atomic detach into ``TS<HomogeneousTuple<T>>``; Python ``burst=True``; and
   unbounded and bounded queue behaviour with burst delivery.

Stage 3 — extension migration
   Kafka and web each move to one push source, remove their duplicate queues,
   and Kafka moves replay to an ordinary simulation-scheduled graph node.
   Separate linked pull requests per RFC 0000.

Acceptance criteria
-------------------

Public C++ and extension boundary
   * ``PushSourceSender`` exposes ``try_send`` (``[[nodiscard]]``) and
     ``send_blocking``; ``send`` no longer exists.
   * ``make_push_source_queue_policy`` accepts ``max_pending`` defaulting to
     ``0``.
   * ``PushSourcePolicyKind::Burst`` and
     ``make_push_source_burst_policy`` are available to an installed native
     extension, and the sender schema is the output tuple's scalar element.
   * An installed-SDK fixture builds a separately compiled extension that
     bounds queue and burst push sources and observes a refused ``try_send``.

Behaviour
   * A bounded queue refuses admission at capacity and admits again after the
     graph dequeues a value for processing.
   * ``send_blocking`` returns once capacity is available and does not busy-wait.
   * ``send_blocking`` unblocks on stop and does not deadlock graph teardown,
     under TSAN.
   * Several producer threads sharing one sender neither lose nor duplicate
     values, under TSAN.
   * A sender blocked on capacity is released by stop before policy storage is
     destroyed; a retained sender is safely invalid after executor destruction,
     under TSAN and ASan.
   * ``max_pending == 0`` reproduces today's unbounded behaviour, including the
     existing real-time execution tests unchanged.
   * A stopped node's native and Python blocking send returns ``false`` rather
     than exposing a sender-internal exception.
   * Python blocking admission releases the GIL, and a ``@push_queue`` stop
     hook can request task shutdown and join its thread using shared ``STATE``.
   * Burst rejects every output except ``TS[tuple[SCALAR, ...]]`` and rejects a
     sender value whose schema is not ``SCALAR``.
   * Burst uses the same bounded and unbounded enqueue path as ordinary queue
     delivery: at capacity ``try_send`` refuses and ``send_blocking`` waits.
   * Burst preserves FIFO admission order, emits every scalar pending at detach
     as one tuple, never emits an empty tuple, and assigns concurrent arrivals
     after detach to a later tick without loss or duplication.
   * Bounded burst capacity counts scalar elements and is released for the
     whole detached batch when it is dequeued.
   * Python burst senders accept scalar values, emit Python tuples, and a
     bounded ``sender(value)`` does not hold the GIL while waiting.

Performance
   * A migrated downstream subscription path shows no per-record type-registry
     acquisition.
   * Allocation and latency evidence for the collapsed path is recorded in
     this RFC before it is Accepted.
   * Burst enqueue remains O(1); a burst of *n* scalars has O(n) per-tick work
     and O(n) retained memory, with benchmarked tuple construction and no
     per-tick schema-registry lookup.

Compatibility
   * ``@push_queue(..., conflate=True)`` behaviour is unchanged and covered by
     its existing tests.
   * ``@push_queue(..., burst=True)`` is additive and mutually exclusive with
     ``conflate=True``; existing decorators retain queue semantics.
   * ``docs/source/user_guide/cpp/authoring_nodes.rst`` and the ``push_queue``
     docstring describe all policy models, both sender entry points, the burst
     scalar/tuple distinction, and the blocking rule.

Implementation status
---------------------

Stages 1 and 2 are implemented on the RFC implementation branch.  The native
sender exposes ``try_send`` and ``send_blocking`` through a shared lifecycle
control, bounded FIFO admission releases capacity at dequeue, stop wakes and
quiesces blocked producers, and burst reuses that admission queue while
materialising all detached values into a homogeneous tuple.  Burst
materialisation moves owning values into an adopted list buffer and uses its
preselected binding; it performs no per-tick schema lookup or avoidable second
payload copy.

Python ``push_queue`` exposes ``burst`` and ``max_pending``, maps its sender to
native boolean blocking admission with the GIL released, and provides a stop
hook for task shutdown and thread joining through shared ``STATE``.  Native
and Python tests cover capacity refusal/release, stop and retained-sender
safety, concurrent producers, burst FIFO/batch boundaries, invalid schemas and
option validation, Python blocking behaviour, and start/stop task lifecycle.
The installed-SDK fixture exercises bounded queue and burst factories through
public headers.

Stage 3 is implemented for both extensions. Kafka uses one FIFO push source
for its ordered transport envelope; a stateless classifier plus standard
``collect``/mapped ``emit`` handles delivery reports, standard ``emit`` handles
scalar events, graph sinks issue commands and commits, byte-capacity
configuration is removed, and simulation uses a scheduled replay node without
a worker or push source. Web uses one bounded FIFO burst source per independently
ordered channel; stateless grouping plus standard ``collect``/mapped ``emit``
handles keyed outputs and standard ``emit`` handles scalar events. Its former
value queues, wake sources, projection schedules, and drain nodes are removed.
Web retains only data-free domain byte/reservation accounting until graph
handoff; callback-driven live and fake transports reject simulation wiring.
Downstream before/after latency and
allocation measurements remain outstanding before ``Accepted`` status can be
recorded.
Until the implementation and conformance tests merge, this RFC remains
``Proposed`` as required by RFC 0000.

References
----------

* :doc:`rfc_0000` — RFC process and promotion gate.
* :doc:`rfc_0015_kafka_extension_api` — the deferral this RFC discharges, and
  the bounded-queue requirement it states.
* :doc:`rfc_0024_web_extension_api` — the second independent implementation.
* ``docs/source/developer_guide/data_structures/overview/execution_layer.rst``
  — push sources and the real-time executor boundary.
* ``docs/source/user_guide/cpp/authoring_nodes.rst`` — push-source authoring
  and the real-time-only rule.
