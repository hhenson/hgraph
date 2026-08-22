Real-time adaptors
==================

This page defines the lifecycle and threading pattern for connecting an
external asynchronous process to an hgraph real-time graph.  The external
process may use threads, callbacks, or an event loop, but it enters the graph
only through a push-source sender.  It must not retain a graph or executor
object and must never schedule or evaluate graph nodes directly.

The sender boundary
-------------------

``PushSourceSender`` is the complete producer-facing boundary.  Its internals
own the selected queue or conflating representation, synchronization, receiver
lifetime, and the projection onto the real-time executor's wake machinery.
Those details are not exposed to adaptor code.

Admission has this ordering:

1. the selected sender policy validates and queues or applies the value under
   its own synchronization;
2. the policy reports whether the accepted value made push work pending; and
3. the sender marks push work pending and notifies the real-time executor.

The executor checks the pending state to decide whether to evaluate the root
graph's push-source prefix.  The notification also wakes an executor waiting
for its next scheduled time.  Work is always stored before notification, so a
woken executor can observe it.

The queue policy owns its producer/consumer safety.  The basic FIFO policy
uses a mutex.  A bounded queue also uses a condition variable: blocking sends
wait without holding the queue lock, dequeue notifies available capacity, and
stop makes the queue non-accepting and wakes every waiter before producer
threads are joined.

The sender exposes two admission choices:

``try_send(value) -> bool``
   Never waits.  ``False`` means capacity was unavailable or the receiver had
   stopped.  Use it only when the producer has an explicit refusal plan, such
   as pausing upstream reads while continuing to serve protocol heartbeats.
   Ignoring ``False`` loses the value.

``send_blocking(value) -> bool``
   Waits for bounded capacity.  ``False`` means the receiver stopped before
   admission.  A producer may discard this result when its stop lifecycle
   already requests task shutdown.  Python releases the GIL for the native
   wait.

Neither result means the graph processed the value.  Admission is buffering,
and dequeue only means processing has begun.

Simple source adaptors
----------------------

A source-only adaptor consists of one push source whose lifecycle owns the
external task:

* node ``State`` holds the task, its stop mechanism, and its thread handles;
* ``start`` receives the framework-created sender, constructs the task with
  that sender, and starts its threads; and
* ``stop`` requests task shutdown, wakes any task-owned waits, and joins every
  thread started by ``start``.

The stop path must tolerate partial start.  If construction or thread start
can fail after acquiring resources, use the existing scope guards from
``hgraph/util/scope.h`` to roll back the completed steps.

Python ``@push_queue`` uses the same model:

.. code-block:: python

   @push_queue(TS[Event])
   def events(sender, config: Config, state: STATE = None):
       state.task = EventTask(config, sender)
       state.task.start()

   @events.stop
   def stop_events(config: Config, state: STATE = None):
       state.task.request_stop()
       state.task.join()

Do not capture a call-scoped Python ``STATE`` or time-series wrapper in the
worker.  Copy the task-owned values needed by the worker during ``start`` and
store the task itself in state for ``stop``.

Bidirectional and multi-interface adaptors
------------------------------------------

An adaptor that both receives asynchronous results and consumes graph inputs
is a service implementation, not a push source plus an unrelated external
object.  Register the ``service_impl`` once for its graph path so it is the
graph-scoped singleton responsible for the external resource.

The implementation composes:

* one or more push sources for asynchronous responses entering the graph;
* sink nodes for requests or commands leaving the graph;
* the ordinary service or adaptor boundary nodes for its public interfaces;
  and
* one central task/resource instance shared by those nodes.

Use one push source for each independently ordered response stream.  Combine
related result kinds into a discriminated envelope on one sender when they
must preserve a single external order; demultiplex the envelope with ordinary
graph nodes.

Prefer a graph-scoped resource held through ``GlobalState`` when several
source and sink nodes need the task.  When exactly one push source is the
natural resource owner, it may lazily construct the task in its state and
expose passive access to the sinks.  That connection is a real graph edge and
can delay inputs; choose it only when the resulting ordering is intended.
Never introduce a process global for a resource whose configuration and
lifetime belong to one graph execution.

One node, preferably the resource-owning push source, is responsible for
creating and starting the shared task.  One corresponding stop path requests
shutdown and joins it.  Sink-node stop hooks must not independently destroy a
resource still used by response callbacks.

Simulation specializations
--------------------------

Push sources belong only to real-time root graphs.  There is no simulated push
source and no external thread that is allowed to wake a simulation executor.

An adaptor that supports simulation supplies a separate native wiring-time
implementation selected from the wiring context's run mode.  Historical data
is produced by ordinary scheduled or pull-source nodes so graph time alone
controls replay.  For example, a Kafka real-time implementation consumes the
broker on an external task and sends records through a push source, while its
simulation implementation replays a stored log through a pull source.

Keep both implementations behind the same service interfaces.  Client graphs
should not need to know whether the service is live or replayed.

Protocol confirmation and commits
---------------------------------

Never acknowledge, commit, or release protocol work merely because
``try_send`` or ``send_blocking`` accepted it.  Enqueueing proves only that the
graph can receive the buffered value.

When a protocol requires confirmation after successful graph processing,
carry a correlation key, cursor, or offset with the data.  Emit that identity
at the point in the graph that defines successful processing and connect it to
a service sink that performs the acknowledgement or commit.  Kafka
transactional processing therefore commits from a confirmation flowing
through the graph, not from the consumer callback that enqueued the record.

Validation
----------

Test the adaptor through its public wiring interfaces in both C++ and Python
where Python exposure exists.  At minimum cover:

* start constructs one task and stop requests shutdown and joins it;
* stop while a producer is blocked returns ``false`` without deadlock;
* retained senders are inert after graph teardown;
* ``try_send`` refusal follows the documented producer flow-control path;
* several producer threads preserve the selected sender policy's semantics;
* the graph wakes and evaluates only after accepted work is available;
* protocol acknowledgement occurs only after the explicit confirmation sink;
  and
* simulation selects the scheduled or pull-source implementation and creates
  no producer thread.

See :doc:`services` for service materialization and graph-boundary wiring, and
:doc:`../user_guide/cpp/authoring_nodes` for the public push-source APIs.
