RFC 0015: C++-First Kafka Extension API
=======================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-07
:Target: Kafka extension, public C++ SDK, and Python compatibility surface

Summary
-------

Replace the current Python-only Kafka implementation with a separately
installed, C++-first ``hgraph-kafka`` extension.  Kafka is exposed as four
service interfaces implemented by one multi-interface ``service_impl`` at a
path.  Registration supplies one immutable compound-scalar configuration;
the implementation materializes once for that binding point and creates its
broker resources only when the owning graph starts.  There is no user-held
Kafka runtime object outside the graph.

The extension uses hgraph's existing boundary vocabulary rather than adding a
Kafka-specific runtime to the core:

.. list-table::
   :header-rows: 1
   :widths: 26 27 47

   * - Operation
     - hgraph contract
     - Reason
   * - Subscribe
     - subscription service
     - The immutable subscription request is the key and the record stream is
       keyed by that same request.
   * - Publish
     - request/reply service
     - Each publisher has a stable client id and receives asynchronous delivery
       reports for its own record stream.
   * - Commit
     - reply-less request/reply service
     - Processed offsets are a keyed sink with no graph response dependency.
   * - Service events
     - reference service
     - Connection, rebalance, retry, and fatal events are one shared source per
       service path.

The ``service_impl`` obtains subscription keys, publish requests, and commit
requests through ``service::impl_input`` and feeds them to graph sink nodes.
For real-time service graphs, its consumer and producer threads return
subscription values, delivery reports, and events only through root
push-source nodes bound with ``service::impl_output``.  These are decoupled
external sinks and sources, so
RFC 0014's automatic transport planner gives them direct request and response
paths.  No feedback delay, adaptor-specific cycle, or user transport flag is
required.

The current ``message_publisher`` and ``message_subscriber`` decorators remain
as compatibility wrappers for at least one transition release.  They lower to
the same native extension and preserve the released ``KafkaMessage``, replay,
``recovered``, flush, and legacy cross-partition ordering behavior.  New code
does not depend on magic ``msg``/``recovered`` parameter names.

Live Kafka ingress is real-time-only.  One standard unbounded burst push source
carries discriminated, fully owned transport envelopes for subscription
values, delivery reports, and service events.  Each sender call admits one
envelope; one source evaluation transfers all currently pending envelopes as
an ordered tuple.  One Kafka-specific graph emit classifies that burst into a
structural service bundle: keyed subscription and delivery lanes plus a scalar
service-event lane.  The emit walks one ordered pending queue and delegates its
collision-free prefix into that bundle.  Independent keys can therefore share
a keyed delta, but the first repeated key, second scalar event, or
timestamp/recovery constraint stops the walk; the front event resumes on the
next applicable engine cycle.  No later event may overtake that boundary.
Standard field projection and ``map_`` then give each service lane its public
shape and keep graph-stop and commit-on-delivery policy on graph.  There is no
extension-owned cross-thread ingress queue, conflated wake token, or opaque
service projection or drain object in front of the graph.

Subscription lifecycle envelopes share that same ordered transport.  A
``Starting`` envelope is admitted before its consumer owner starts, and a
removal envelope is admitted only after that owner stops and joins.  The graph
emitter therefore has one transport input: records already accepted from a
session drain before its removal, and a later session starts after that removal
without a second-stream priority rule or subscription-generation filter.

No push source is permitted in simulation.  The simulation specialization
performs a finite bounded read without a worker thread, retains the resulting
history in graph-owned replay state, and schedules it at the recorded Kafka
timestamps.  Replay feeds the same graph emit and mapped service projections as
live ingress.  Independent subscription records sharing one timestamp are
applied in one keyed mutation so exact simulated time is preserved; a collision
on one subscription key is released on the following engine cycle.

Motivation
----------

The existing adaptor reached useful compatibility quickly, but its public
shape and runtime ownership are not a suitable basis for a native extension:

* authoring is expressed by decorators that inspect parameter names and graph
  signatures rather than ordinary source and sink calls;
* one untyped configuration dictionary combines Kafka client options, test
  factories, and injected object ownership;
* a ``GlobalState`` singleton, rather than one service implementation at a
  graph path, owns consumers, producer reference counts, topic registries, and
  wiring registration;
* at most one publisher is permitted for a topic even though Kafka and hgraph
  do not impose that restriction;
* ``KafkaMessage`` omits topic, partition, offset, timestamp type, timestamp,
  and delivery metadata;
* headers are reduced to a dictionary, losing duplicate names, ordering, and
  nullable header values, while ``content-type`` is given transport-level
  special treatment;
* recovery is selected indirectly by whether the wrapped function happens to
  declare ``msg`` and ``recovered``;
* producer flush intervals and message counts are fixed in the authoring
  wrapper;
* offset commits, rebalances, delivery acknowledgements, queue overflow,
  backpressure, and fatal-error policy are not part of the user contract; and
* the released implementation can reduce a worker failure to a log message.

Some current implementation details are worth preserving.  Recovery takes an
end-offset snapshot, consumes only records before that per-partition boundary,
then seeks back to the snapshot before live polling.  That is a sounder
history/live hand-off than treating an empty poll or a wall-clock timestamp as
the boundary.  The same consumer continues into live mode, preventing a gap
between two independently assigned clients.

The updated Python hgraph implementation also established two compatibility
requirements which remain valid:

* a replaying publisher sees history but not the live records it may itself
  publish; and
* a subscriber which did not request recovery does not receive history merely
  because another client on the topic requested it.

The C++ implementation should retain those observable behaviors without
retaining the decorator-driven architecture.

The upstream implementation's use of ``adaptor_impl`` for the combined
history/live source is a Python-runtime workaround: upstream service
implementations are nested graphs and reject push sources.  hg_cpp inlines
service implementations into the owning graph and already supports root push
sources there.  The workaround is therefore not a reason to expose another
adaptor API or to give up the service contracts above.

Prior art and evidence
----------------------

hgraph integrations
~~~~~~~~~~~~~~~~~~~

The SQL, Delta, data-catalogue, HTTP, WebSocket, and threaded-graph adaptors
already point toward the appropriate hgraph model:

* correlated operations use service adaptors and keyed push responses;
* network or worker completion crosses into the graph through a push source;
* status and error information is data rather than only logging;
* graph-time declarations are separated from start/stop resource ownership;
* request generations prevent late worker completion from reviving a removed
  client; and
* a shared implementation can own one external resource while serving several
  graph clients.

Those integrations are evidence for the boundary shape, not implementations
to copy wholesale.  Several still retain Python sender dictionaries,
unbounded queues, or executor-specific cleanup.  Kafka needs an explicit
native ownership and flow-control contract.

Point72 CSP
~~~~~~~~~~~

Point72 CSP's Kafka integration uses a graph-time ``KafkaAdapterManager`` to
create a native runtime manager.  The native manager shares producers and
consumers, fans a topic/key stream out to multiple adapters, offers a status
stream, batches related pushed values, and uses a push/pull adapter to hand
historical data over to real-time delivery.  CSP's adaptor guide explicitly
separates graph-build descriptors from runtime resource creation and requires
worker threads to be stopped and joined.

Those are useful patterns.  The CSP public Kafka surface is nevertheless a
Python manager backed by property dictionaries; its concrete C++ classes are
runtime implementation types rather than a first-class C++ graph-authoring
API.  Its replay and commit choices are also coupled to manager-wide settings.
hg_cpp should take the lifecycle and sharing model, not copy that user API.

Apache Flink
~~~~~~~~~~~~

Flink separates Kafka source and sink builders, makes topic/partition
selection and deserialization explicit, and models starting and stopping
positions independently.  Its supported initializers include earliest,
latest, committed, timestamp, and explicit per-partition offsets.  Boundedness
is a stopping-offset decision rather than an inferred consequence of whether
one poll happened to be empty.

Flink also demonstrates why a connector must not advertise a delivery label
without the matching engine contract.  Its at-least-once sink waits for
outstanding broker acknowledgements at a checkpoint, while exactly-once uses
Kafka transactions coordinated with checkpoint state.  hgraph does not yet
have that connector checkpoint/transaction protocol, so this RFC does not
claim end-to-end exactly-once processing.

Kafka and librdkafka
~~~~~~~~~~~~~~~~~~~~

Kafka guarantees ordering within a partition, not a canonical total order
across partitions.  Consumer delivery semantics depend on when offsets are
stored relative to user processing.  Exactly-once consume/process/produce
requires the consumed offsets and produced records to participate in the same
Kafka transaction, or equivalent cooperation from the destination.

librdkafka is asynchronous.  Produce enqueues a record, delivery success or
failure arrives later through callbacks served by ``poll``, and a bounded
producer queue can return ``QUEUE_FULL``.  Consumer prefetch has its own byte
and message limits.  These are observable connector concerns and must not be
hidden behind a synchronous ``send`` facade.

The extension will use librdkafka's C API.  The upstream project guarantees C
ABI stability and notes that its C++ wrapper may lag the C API.  A small
extension-owned RAII layer gives hg_cpp a typed C++ surface without making the
public hgraph contract depend on librdkafka C++ classes.

Ownership boundary
------------------

``hgraph-kafka`` owns:

* all Kafka record, cursor, configuration, state, delivery-report, and event
  types;
* librdkafka discovery, linking, configuration, callbacks, and RAII wrappers;
* producer and consumer threads, queues, pause/resume, rebalances, offset
  operations, recovery hand-off, and shutdown;
* Kafka-specific source, publish, commit, and event service descriptors and
  implementations;
* byte codecs supplied by the extension and its C++ and Python user APIs; and
* integration tests and performance evidence involving a broker.

hg_cpp owns only the already-public facilities the extension consumes:

* native graph and node authoring;
* subscription, request/reply, reference-service, and transport planning;
* real-time root push sources and graph-thread scheduling;
* native extension scalar and Python-class registration; and
* installed-SDK extension boundaries.

The core must not link to librdkafka, include Kafka headers, import
``hgraph_kafka`` during a normal ``hgraph`` import, or declare a package
dependency on the extension. A normal core CMake build remains independent of
Python and Kafka. The core wheel owns a guarded compatibility shim at
``hgraph.adaptors.kafka`` which imports ``hgraph_kafka`` only when that legacy
path is explicitly requested. The extension wheel installs only the
``hgraph_kafka`` package and never contributes files to ``hgraph``. The
coordinated core change removes the former Kafka implementation and
``kafka-python`` extra while retaining this protected migration path.

This RFC lives in hg_cpp because it changes that existing public compatibility
surface and fixes the public SDK boundary on which the new extension depends.
The first-party extension lives under ``extensions/kafka`` in the hg_cpp
monorepo, with its own CMake package, ``pyproject.toml``, version, and release
artifacts.  Repository co-location keeps cross-cutting core and extension
changes atomic without changing the one-way package dependency.

No generic messaging layer is introduced by this RFC.  One Kafka
implementation is not sufficient promotion evidence for generic connector
types, acknowledgement protocols, bounded ingress, or stream-status APIs.

Public value contract
---------------------

The semantic shapes below are normative for the proposal; exact field spelling
remains reviewable while the RFC is Proposed.  Record, cursor, configuration,
event, and delivery-report values are named ``Bundle`` schemas: they are
hgraph compound scalars, not ordinary C++ structs transported outside the
type system.  Collections inside them use scalar collection schemas such as
``HomogeneousTuple<T>``.  A collection of time-series fields is a named
``TSB`` schema.  The Python classes are registrations of those same schemas,
not independent dataclasses with separate semantics.

For example, the public declarations have this shape (the complete field list
is specified below):

.. code-block:: cpp

   using KafkaHeader = Bundle<"hgraph.kafka::KafkaHeader",
       Field<"name", Str>,
       Field<"value", Bytes>>;

   using KafkaRecord = Bundle<"hgraph.kafka::KafkaRecord",
       Field<"topic", Str>,
       Field<"partition", Int>,
       Field<"offset", Int>,
       Field<"timestamp", DateTime>,
       Field<"timestamp_type", KafkaTimestampType>,
       Field<"key", Bytes>,
       Field<"value", Bytes>,
       Field<"headers", HomogeneousTuple<KafkaHeader>>>;

   using KafkaSubscriptionOutput = TSB<"hgraph.kafka::KafkaSubscriptionOutput",
       Field<"record", TS<Shared<KafkaRecord>>>,
       Field<"cursor", TS<KafkaCursor>>,
       Field<"state", TS<KafkaSubscriptionState>>>;

   using KafkaPublishRequest = TSB<"hgraph.kafka::KafkaPublishRequest",
       Field<"topic", TS<Str>>,
       Field<"record", TS<KafkaProduceRecord>>>;

``KafkaServiceConfig``, ``KafkaSubscriptionKey``, ``KafkaProduceRecord``,
``KafkaCursor``, ``KafkaDeliveryReport``, and ``KafkaEvent`` follow the same
named ``Bundle<..., Field<...>>`` form.  Builders mentioned later construct
values of those schemas; they do not define alternate C++ storage types.

An unset nullable ``Bundle`` field represents Kafka ``null``; a present empty
``Bytes`` field represents an empty byte string.  This applies independently
to keys, values, header values, and optional timestamps.  The exact C++
integer aliases will be selected to match Kafka's partition and offset widths.

``KafkaHeader``
   An ordered pair of UTF-8 header name and nullable bytes value.  Headers are
   represented as a sequence, never a map, so duplicate names and order are
   preserved.

``KafkaRecord``
   One consumed record with ``topic``, ``partition``, ``offset``, nullable
   ``timestamp``, ``timestamp_type``, nullable ``key``, ``value``, and ordered
   ``headers``.  ``value`` is nullable because a compacted-topic tombstone is
   a valid Kafka record; it is not converted to empty bytes.  Offset is the
   record's offset, not the next commit position.

``KafkaProduceRecord``
   One outbound record with nullable ``value``, nullable ``key``, ordered
   ``headers``, nullable timestamp, optional partition, and a user token.  It
   is a compound scalar.  Topic is a separate field of the publish request
   ``TSB`` so a wiring-time topic can tick once and remain valid rather than
   being copied into every record value.

``KafkaCursor``
   A subscription identity, assignment generation, topic, partition, and
   ``next_offset`` suitable for commit.  Conversion from a record uses
   ``record.offset + 1``.  Making the next position explicit prevents the
   common off-by-one commit error, while the generation prevents a delayed
   graph acknowledgement from committing a partition after it was revoked and
   reassigned.

``KafkaDeliveryReport``
   The user token, service-assigned sequence, topic, partition, resulting
   offset when known, status, Kafka error code, retriable/fatal flags, and
   diagnostic message.  Enqueue acceptance is not delivery success.

``KafkaEvent``
   Severity, component, category, Kafka error code, retriable/fatal flags,
   service path, optional subscription/publisher identity, and message.
   Credentials and raw configuration values are never included.

``KafkaSubscriptionState``
   ``Starting``, ``Recovering``, ``Live``, ``BoundedComplete``, ``Retrying``,
   ``Stopped``, or ``Failed``.  This is an enum scalar; state is independent
   of record ticks.

``KafkaSubscriptionOutput``
   A named time-series bundle containing
   ``record: TS[Shared[KafkaRecord]]`` in C++,
   ``cursor: TS[KafkaCursor]``, and
   ``state: TS[KafkaSubscriptionState]``.  Record and cursor tick together.
   ``Shared`` is an immutable storage strategy.  Python declares the exact
   edge as ``TS[Shared[KafkaRecord]]``, while conversion remains transparent
   and node callables receive ``KafkaRecord`` values.
   The explicit cursor field is required because assignment generation cannot
   be reconstructed safely from record metadata after processing.  Service-
   wide details remain on the event service instead of being copied into
   every subscription.

``KafkaPublishRequest``
   A named time-series bundle containing ``topic: TS[Str]`` and
   ``record: TS[KafkaProduceRecord]``.  Static-topic wiring supplies a constant
   topic time series; dynamic routing supplies a changing one.  This is one
   service request schema, not a C++ struct containing time-series handles.

The transport is byte-oriented.  Typed serialization is graph composition:
an encoder maps ``TS<T>`` to ``TS[KafkaProduceRecord]`` and a native decoder
maps ``TS[Shared[KafkaRecord]]`` to ``TS<T>`` by borrowing the concrete record
view.  Python decoders continue to receive the transparent ``KafkaRecord``
value.  A codec is not hidden in connection state.
C++ codecs may provide native nodes; a Python callable codec executes on the
graph thread under the GIL and is never invoked by a Kafka worker thread.

Configuration contract
----------------------

Configuration is immutable compound-scalar data.  Public C++ builders may
make those values convenient to construct, but the built result is a ``Value``
with a named ``Bundle`` schema, not a transport-owning configuration object.
The principal schemas are divided by responsibility:

``KafkaServiceConfig``
   The complete configuration registered at one service path.  It contains
   connection, producer, default consumer, flow-control, retry, failure, and
   observability configuration.  All clients of the path use this one value.
   A materially different service configuration requires a different path.

``KafkaConnectionConfig``
   Bootstrap servers, client identity, security material/provider, common
   librdkafka options, and observability labels.

``KafkaSubscriptionKey``
   Exactly one topic selector: an explicit topic list, topic pattern, or
   explicit partition set; plus group/assignment mode, start/stop position,
   isolation, commit and recovery policy, optional exact record-key filter,
   and named sharing identity.  It is one immutable, hashable ``Bundle`` and
   is the key of the subscription service.  Key filtering is local fan-out
   and does not pretend that Kafka assigns partitions by record key.

``KafkaConsumerDefaults``
   Path-wide defaults for partition discovery, ingress limits, overflow,
   recovery clock, merge, retry, and consumer option overrides.  Every
   semantic value which may vary independently between subscriptions is
   resolved into ``KafkaSubscriptionKey`` so sharing equality is exact.

``KafkaProducerOptions``
   Acknowledgement/idempotence settings, batching and linger, outbound queue
   limits, overflow action, shutdown drain timeout, failure policy, and
   producer option overrides.

Common, consumer, and producer passthrough collections remain available
because the Kafka configuration surface evolves independently of hgraph.
They are represented as immutable tuples of compound-scalar option pairs,
not mutable C++ or Python dictionaries.  The collections are separate and
validated.  Keys whose semantics the extension owns, such as
group identity, auto offset storage, event callbacks, opaque pointers, and
queue limits, cannot be contradicted by passthrough values.

Security credentials may be supplied by an extension-owned credential
provider referenced by the service configuration.  Diagnostic rendering
redacts secret values.  The configuration is copied into the graph's planned
service-implementation state and is not stored in a process-global singleton.

Starting position is a tagged value with these initial forms:

* ``Earliest``;
* ``Latest``;
* ``Committed`` with an explicit missing-offset fallback;
* ``Timestamp``;
* explicit offsets by topic/partition; and
* ``GraphStartTime`` for compatibility.

Stopping position is independently one of ``Unbounded``, a snapshot of latest
offsets taken at start, a timestamp, or explicit offsets.  Consumer group and
start position are not artificially mutually exclusive: an explicit start
may intentionally override a committed position.  The selected behavior is
visible in configuration rather than encoded through ``auto.offset.reset``.

Public C++ wiring API
---------------------

The intended graph-authoring shape uses one explicit service implementation
registration and ordinary service calls.  The extension may offer the shown
free functions as typed wiring sugar, but there is deliberately no
``KafkaConnector`` instance:

.. code-block:: cpp

   static void compose(Wiring &w)
   {
       const auto primary = service::path("primary");

       kafka::register_service(
           w, primary,
           kafka::service_config()
               .bootstrap_servers({"kafka:9092"})
               .idempotent_producer(true)
               .build());

       auto subscription_key = wire<stdlib::const_>(
           w,
           kafka::subscription_key()
               .topics({"orders"})
               .group_id("risk")
               .start(KafkaStartPosition::committed(OffsetFallback::Earliest))
               .commit_mode(KafkaCommitMode::Explicit)
               .build())
           .as<TS<KafkaSubscriptionKey>>();

       auto orders = wire<KafkaSubscriptionService>(
           w, primary, subscription_key)
           .as<KafkaSubscriptionOutput>();

       auto decoded = wire<DecodeOrder>(w, orders.field<"record">());
       auto processed = wire<ProcessOrder>(w, decoded);
       auto processed_cursor = wire<ProcessedCursor>(
           w, processed, orders.field<"cursor">());
       wire<KafkaCommitService>(w, primary, processed_cursor);

       auto outbound = wire<EncodeResult>(w, processed);
       auto publish_request = kafka::publish_request(
           w, "risk-results", outbound);
       auto delivery = wire<KafkaPublishService>(
           w, primary, publish_request);

       wire<ObserveDelivery>(w, delivery);
       wire<ObserveKafkaEvent>(
           w, wire<KafkaEventService>(w, primary));
   }

``kafka::register_service`` is graph-time only.  It delegates to
``service::register_services<KafkaServiceImpl, ...>`` with the four public
interfaces, the path, and ``KafkaServiceConfig`` scalar.  It records one lazy
multi-interface implementation candidate; it does not construct a librdkafka
client, start a thread, resolve metadata, or perform network I/O.  Like other
native services, duplicate implementation registration at the same path is a
wiring error.  Clients carry only the service path and time-series ports.

The primary operations are:

``wire<KafkaSubscriptionService>(w, path, TS[KafkaSubscriptionKey])``
   Returns ``KafkaSubscriptionOutput``.  The service mechanism converts live
   client keys to the implementation's ``TSS[KafkaSubscriptionKey]``.  A
   constant key is the normal static-subscription case; changing the key uses
   the normal subscription lifecycle rather than a separate object API.

``wire<KafkaPublishService>(w, path, KafkaPublishRequest)``
   Returns ``TS[KafkaDeliveryReport]``.  ``KafkaPublishRequest`` is a ``TSB``
   with topic and record fields.  ``kafka::publish_request`` is wiring sugar:
   its static-topic overload wires a constant topic; its dynamic overload
   accepts ``TS[Str]``.  Any number of clients may publish through the same
   service path.

``wire<KafkaCommitService>(w, path, TS[KafkaCursor])``
   Sends explicit processed positions to the consumer owner.  Commits for one
   partition are monotonic; a stale cursor is ignored with a diagnostic event
   rather than moving the committed position backwards.

``wire<KafkaEventService>(w, path)``
   Returns the shared service event stream.

Callers may discard delivery reports and events with normal graph sinks, but
the implementation still services librdkafka callbacks and accounts for
failures.  There is no synchronous publish operation on the graph evaluation
thread.

Service representation
----------------------

The extension-owned service descriptors are public C++ graph interfaces.  The
descriptor declarations are ordinary C++ structs because they describe a
service flavour; the data passing through them remains ``Bundle``/``TSB``
schema data:

.. code-block:: cpp

   struct KafkaSubscriptionService {
       using key_type = KafkaSubscriptionKey;
       using value_schema = KafkaSubscriptionOutput;
   };

   struct KafkaPublishService {
       using request_schema = KafkaPublishRequest;
       using response_schema = TS<KafkaDeliveryReport>;
   };

   struct KafkaCommitService {
       using request_schema = TS<KafkaCursor>;
   };

   struct KafkaEventService {
       using output_schema = TS<KafkaEvent>;
   };

They use only public hgraph C++ APIs, proving that the installed SDK is
sufficient for a separately built connector.  ``KafkaSubscriptionKey`` is the
immutable combination of subscription and consumer semantics plus named
sharing identity; no semantic option is omitted from the service key.

Identical subscriptions on one service path share a graph stream by default.
Session sharing occurs only when connection, group/assignment, topic selector,
start/stop, isolation, commit, replay, merge, and buffer policies are
compatible.  A named independent-subscription option creates a distinct
session.  The implementation never generates an invisible random group id to
change sharing semantics.

A shared subscription is one logical acknowledgement domain.  An explicit
commit from any downstream path advances that session's position.  Code which
needs several processing branches to complete first wires one coordinated
cursor after their join.  Code which needs independent acknowledgement domains
uses distinct subscription identities; Kafka group assignment then follows
the group ids it explicitly supplied.

Multi-interface service implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All four interfaces are registered atomically against one implementation and
one path.  In schematic C++ the implementation is:

.. code-block:: cpp

   struct KafkaServiceImpl
   {
       static void compose(
           Wiring &w,
           Scalar<"path", Str> path,
           Scalar<"config", KafkaServiceConfig> config)
       {
           const auto binding = service::path(path.value());

           auto subscription_keys =
               service::impl_input<KafkaSubscriptionService>(w, binding);
           auto publish_requests =
               service::impl_input<KafkaPublishService>(w, binding);
           auto commit_requests =
               service::impl_input<KafkaCommitService>(w, binding);

           auto outputs = wire<KafkaRuntimeGraph>(
               w, subscription_keys, publish_requests, commit_requests,
               config.value());

           service::impl_output<KafkaSubscriptionService>(
               w, binding, outputs.field<"subscriptions">());
           service::impl_output<KafkaPublishService>(
               w, binding, outputs.field<"deliveries">());
           service::impl_output<KafkaEventService>(
               w, binding, outputs.field<"events">());
       }
   };

``KafkaRuntimeGraph`` is an extension implementation graph, not a user object
and not one opaque node which bypasses graph inputs.  It wires:

* a subscription-command sink consuming
  ``TSS<KafkaSubscriptionKey>``;
* a publish-command sink consuming
  ``TSD<Int, KafkaPublishRequest>``;
* a commit-command sink consuming ``TSD<Int, TS<KafkaCursor>>``;
* one real-time root push source plus graph-side projection nodes, or a
  deterministic simulation replay node, producing
  ``TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>``,
  ``TSD<Int, TS<KafkaDeliveryReport>>``, and ``TS<KafkaEvent>``; and
* one graph-local lifecycle/resource state shared only by those nodes.

The sinks are the only graph-to-Kafka route.  In real time, the push source is
the only Kafka-to-graph wake route.  In simulation, the bounded preload
is performed without an external worker and an ordinary graph edge releases
the scheduled replay; no push source is wired.  The implementation is inlined into the root graph, as all
native service implementations are, so the selected nodes are legal and the
keyed transport planner can inspect the real decoupled dependency graph.

``kafka::register_service`` expands to one
``service::register_services<KafkaServiceImpl, ...>`` call.  Lazy service
materialization composes this implementation at most once for a demanded
path; the graph-owned runtime resource is then constructed once during graph
start and stopped with that graph.  The service registry, not a C++ connector
instance or ``GlobalState`` singleton, is the authority for instance identity.

Runtime architecture
--------------------

Each materialized ``KafkaServiceImpl`` owns one graph-local Kafka runtime
resource for its path and ``KafkaServiceConfig``.  It contains no Python
objects on the pure C++ path and is not process-global.  Multiple graph
engines may therefore run concurrently on different threads without sharing
runtime state, callbacks, queues, or librdkafka handles.

The graph-local runtime resource owns:

* one shared producer and poll thread for the path's producer configuration;
* one consumer owner thread per consumer session;
* a session registry keyed by the complete semantic subscription identity;
* the standard burst push-source sender used for real-time ingress and ordered
  subscription lifecycle envelopes;
* the bounded, record-counted simulation recovery and producer staging state;
* explicit start, accepting, stopping, and stopped states.

One thread owns all subscribe, assign, seek, poll, pause/resume, commit, and
close operations for a consumer handle.  This remains the rule even though
librdkafka's general API is thread-safe; it makes rebalance and shutdown
ordering auditable and also works with Kafka clients whose consumer object is
not thread-safe.

The producer is asynchronous.  Only the publish sink node moves a graph-owned
record into the task's record-counted staging queue or librdkafka queue.  The
producer poll thread serves delivery and error callbacks and sends fully owned
reports/events through the standard transport sender.

No worker thread calls ``EvaluationEngineApi``, requests graph stop, mutates a
time-series value, or retains a borrowed graph value.  Fatal events cross the
push boundary, and an ordinary graph-thread node applies the configured
``Report`` or ``StopGraph`` policy.

Queue ownership and capacity
----------------------------

RFC 0027 makes the push-source node the cross-thread queue.  Kafka therefore
does not duplicate its storage, locking, wake generation, or receiver-lifetime
logic.  A consumer callback constructs one owned ``KafkaTransportEvent`` and
calls ``send_blocking`` on the unbounded burst policy selected by the service
graph.  The scalar call moves one envelope into the queue; dequeue produces a
tuple containing all work pending at that point.  The send cannot wait for
capacity; ``false`` means only that graph teardown closed the receiver.
Storage is updated before the sender wakes the real-time executor.

The subscription command sink starts and stops session owners, but it does not
publish a second graph-side lifecycle stream.  It admits ``Starting`` before a
new owner can publish and admits removal after the old owner has joined, so the
standard sender FIFO is the causal order consumed by the graph emitter.  The
scheduled simulation queue mirrors deletion by discarding that subscription's
not-yet-replayed envelopes before it schedules the removal.

The public Kafka configuration has record counts, not byte limits.  The
consumer record limit bounds finite recovery retained before replay; the
producer record limit bounds its protocol staging queue.  Real-time ingress
has no second Kafka-specific capacity setting.  If production evidence later
requires a bounded push source, that policy can be selected directly and must
include an explicit ``try_send`` refusal plan that keeps broker polling and
group membership live.

Outbound overflow is an explicit wiring-time policy:

``Fail``
   Emit a failure report/event and apply the configured graph failure policy.

``Drop``
   Drop the record and emit a delivery report identifying the drop.  This is
   never silent.

``Stage``
   Move the record to a bounded extension queue for later production.  When
   that queue is full, one of the two policies above applies.

Blocking the graph evaluation thread for broker capacity is not offered as a
default or hidden fallback.  Shutdown drain has a separate explicit timeout.

Ordering and time
-----------------

The native default promises:

* record offset order within each Kafka partition;
* no invented total order across partitions;
* worker/poll arrival order for records already classified as live on the same
  subscription key; and
* one keyed modification per record, with no conflation: distinct subscription
  keys pending in one burst tick together, while repeated values for one key
  are emitted in arrival order on consecutive ``MIN_TD`` cycles.

Delivery reports for distinct request ids distribute together as one keyed
mutation.  Repeated reports for one id retain FIFO order over later cycles via
one standard ``emit`` node per mapped request id, just like repeated
subscription values for one key.  Service events are scalar and use standard
``emit`` to unroll in FIFO order, one per graph cycle.  The discriminated tuple
preserves admission order for projection, but no public total order is promised
between independent subscription keys, delivery reports, and service events.

Record timestamp is metadata.  A live record's evaluation time is the graph
cycle in which the transport projection emits it; its Kafka timestamp does
not rewrite live engine time.

Historical replay has an explicit clock policy.  ``ArrivalClock`` catches up
history through normal graph cycles while retaining record timestamps only as
metadata.  ``RecordTimestampClock`` schedules history by the selected event
timestamp and requires an explicit out-of-order policy.  The compatibility
profile uses record timestamp, clamps non-increasing values by ``MIN_TD``, and
uses the released deterministic merge key ``(timestamp, topic, partition,
offset)``.

New native code should not depend on the compatibility merge as a Kafka
guarantee.  Equal or skewed timestamps do not create causal order across
partitions.

Simulation selects a separate service graph at wiring time.  It rejects
publishing, commits, unbounded subscriptions, arrival-clock recovery, and
non-deterministic partition merging.  A valid subscription is finite, uses
record timestamps with the deterministic
``(timestamp, topic, partition, offset)`` merge, and is fully preloaded before
the graph receives a release tick.  An ordinary replay node then schedules each
retained value in simulated graph time; broker-thread timing never determines
an evaluation timestamp.

Recovery hand-off
-----------------

For a subscription that catches up and then continues live:

1. assign the resolved partitions;
2. snapshot each partition's end offset;
3. resolve and seek each starting position;
4. emit only records with offsets below that partition's snapshot;
5. mark ``Live`` only after every partition reaches its snapshot;
6. seek any over-fetched partition back to its snapshot; and
7. continue polling with the same consumer/session.

The snapshot is by offset, never by an empty poll or wall-clock end time.  A
poll batch which crosses the boundary cannot lose or duplicate its live tail.
Partition discovery during recovery is policy-controlled; newly discovered
partitions cannot silently acquire a different start rule.

``BoundedComplete`` is emitted only after every selected stopping offset is
reached.  ``Live`` is a phase transition, not an assertion that broker and
graph wall clocks are aligned.

Commit and delivery semantics
-----------------------------

Producer configuration exposes factual mechanisms rather than unsupported
end-to-end labels:

* broker acknowledgement level;
* retries;
* idempotent producer mode;
* shutdown drain behavior.

``KafkaDeliveryReport::Delivered`` means librdkafka reported broker delivery.
It does not mean a downstream consumer processed the record.

Consumer commit mode is one of:

``None``
   Do not store or commit offsets.

``OnGraphDelivery``
   Wire the projected cursor into the same graph-side commit sink after the
   record enters its hgraph time series.  This is explicitly not
   acknowledgement of arbitrary downstream work and is not a sender callback.

``Explicit``
   Commit only cursors supplied through ``KafkaCommitService``.  User code can
   wire that sink after the operation whose completion it considers processed.

Automatic offset storage before graph delivery is disabled whenever the
extension manages commits.  Commit callbacks become events.  Rebalances flush
only positions allowed by the selected mode and never infer that queued but
undelivered graph work has completed.

This RFC does not expose ``ExactlyOnce``.  Exactly-once consume/process/produce
requires an hgraph checkpoint contract, transactional producer lifecycle,
state restoration, and atomic offset publication.  A later RFC may add that
capability; a configuration string alone cannot.  The initial extension also
rejects ``transactional.id`` in passthrough configuration so transactions
cannot be enabled without the lifecycle API which makes them correct.

Lifecycle and teardown
----------------------

Start order is:

1. initialize graph-side projection and command-sink storage;
2. in real time, start the one transport push source, construct the graph-local
   runtime resource with its framework sender, and then start the Kafka task;
3. in simulation, construct the replay resource without a sender or worker;
4. admit each subscription's ``Starting`` lifecycle envelope, then start its
   non-daemon owner thread; and
5. have each consumer owner establish assignment and the recovery snapshot
   before it reports the session ready.

Stop order is:

1. have the service implementation's lifecycle node atomically stop accepting
   new sink commands and worker publications;
2. signal consumer and producer loops to wake;
3. apply the configured producer drain/abort timeout;
4. join every extension-owned thread;
5. close consumer and producer handles in their required order;
6. release protocol handles and task-owned staging; and only then
7. release the graph-scoped runtime resource.

Push-source stop closes its sender before invoking the adaptor stop callback,
so blocked sends return ``false`` and the callback can join every task thread
without deadlock.  A retained sender is an inert lifetime-safe handle after
teardown.  Destructors repeat a noexcept emergency stop if normal graph stop
was skipped, but normal cleanup failures are reported before destruction.

Removing one subscription stops only its session/client reference.  Shared
sessions remain alive while another graph client uses them.  Late delivery or
consumer callbacks carry a generation and cannot publish into a replacement
client with the same logical key.

Python API
----------

Python mirrors the C++ service model.  Registration creates a service
implementation binding; the remaining functions are graph wiring calls, not
methods on a Python-owned Kafka object:

.. code-block:: python

   register_kafka_service(
       KafkaServiceConfig(bootstrap_servers=("kafka:9092",)),
       path="primary",
   )

   subscription = kafka_subscribe(
       KafkaSubscriptionKey(
           topics=("orders",),
           group_id="risk",
           start=KafkaStartPosition.committed(OffsetFallback.EARLIEST),
           commit_mode=KafkaCommitMode.EXPLICIT,
       ),
       path="primary",
   )

   orders = decode_order(subscription.record)
   processed = process_order(orders)
   kafka_commit(
       processed_cursor(processed, subscription.cursor),
       path="primary",
   )
   deliveries = kafka_publish(
       "risk-results", encode_result(processed), path="primary")
   observe(deliveries, kafka_events(path="primary"))

These calls lower to the same native descriptors, values, queues, and runtime
as C++.  Python does not own a second Kafka session registry or consumer
thread.  Native byte records do not acquire the GIL.  Python codecs and Python
user nodes acquire it only when their graph nodes execute.

The Python declarations use ``@subscription_service`` for subscribe,
``@request_reply_service`` for publish and commit (commit is reply-less), and
``@reference_service`` for events.  One ``@service_impl``-equivalent native
registration supplies all four interfaces at the path.  The public
``register_kafka_service`` helper performs that registration with the
compound-scalar configuration; it does not place a Kafka instance in
``GlobalState``.

``KafkaServiceConfig``, ``KafkaSubscriptionKey``, ``KafkaRecord``, and the
other structured values are registered ``CompoundScalar`` classes.
``KafkaPublishRequest`` and ``KafkaSubscriptionOutput`` are
``TimeSeriesSchema`` bundles.  The helpers use normal explicit inputs and
outputs; they do not inspect a wrapped function for reserved parameter names,
change its signature, or infer recovery policy from whether an argument
happens to exist.

Compatibility and migration
---------------------------

The initial extension release keeps these imports and signatures operational:

* ``KafkaMessage``;
* ``MessageState``;
* ``register_kafka_adaptor``;
* ``message_publisher``; and
* ``message_subscriber``.

They become Python graph wrappers over the four Kafka service interfaces and
the registered ``KafkaServiceImpl``.  The compatibility profile preserves:

* ``TS[bytes]`` and ``TS[KafkaMessage]`` inputs/outputs;
* decorator-time or call-time topic selection;
* the public wrapped graph signature;
* the one-publisher-per-topic error, even though the new API permits several;
* ``content-type`` lifting and the legacy dictionary header projection;
* the 100 ms / 1,000-record flush behavior;
* recovery from graph start time;
* ``recovered=False`` before history and ``True`` after the final history
  tick;
* history-only input to a recovering publisher;
* live-only input to a subscriber which omitted ``recovered``;
* the same consumer across recovery and live hand-off; and
* deterministic legacy cross-partition history ordering.

The compatibility shim may lose duplicate headers when converting the richer
native record to ``KafkaMessage`` because the released type cannot represent
them.  The native record and new Python API do not lose them.

The current Python test factories move to an extension testing module.  Native
object injection is not accepted through production configuration.  Tests use
an internal transport seam or librdkafka mock cluster without making factory
objects part of the user API.

No decorator deprecation warning is emitted in the first release.  Once the
new Python surface has equivalent recovery and operational evidence, a later
compatibility decision may deprecate the decorators and ``MessageState``.

Packaging and ABI
-----------------

The extension is built and installed separately and exports a native CMake
target such as ``hgraph::kafka``.  It consumes only installed hg_cpp headers
and ``hgraph::core``.  Its optional Python module uses the stable Python bridge
and registers extension-owned scalar classes through the public native scalar
registration API.

The extension links librdkafka; core does not.  Packaging must choose and
document either a bundled or system librdkafka strategy per platform and must
not expose librdkafka handles in public hgraph value types.  Public headers
contain compound-scalar/TSB schemas, service descriptors, and wiring helpers;
librdkafka handles and callback types remain behind extension-owned runtime
node and RAII implementation boundaries, so upgrades do not change consumer
headers.

The migration lands atomically in one monorepo implementation pull request:

1. create the extension with native C++ API, runtime, tests, and wheel;
2. retain a guarded, core-owned ``hgraph.adaptors.kafka`` compatibility shim
   and test it both with and without the extension installed; and
3. remove the Kafka implementation, ``kafka-python`` extra, and broker tests
   from core without making normal core imports depend on the extension.

Performance and memory
----------------------

The native hot path must avoid Python and minimise copies:

* librdkafka payloads are copied or retained exactly once into an owned
  cross-thread record according to the chosen safe lifetime strategy;
* the standard burst push-source queue retains transport envelopes by move;
* the transport envelope and public subscription edge retain
  ``Shared<KafkaRecord>`` handles across graph-thread hand-offs, without a
  concrete record copy at publication;
* codecs run after transport unless a native codec explicitly opts into safe
  off-thread decoding; and
* delivery callbacks use their opaque token to avoid record lookup by content.

Benchmarks report throughput, p50/p99 ingress latency, producer enqueue and
delivery latency, copies/allocations per record, pending ingress count,
recovery throughput, and shutdown drain time.  Measurements cover
small records, large records, headers, several partitions, several graph
clients, and two concurrent pure-C++ engines.

The real-time ingress policy is explicitly the standard unbounded burst queue
rather than a hidden extension queue.  The Kafka graph emit has one ordered
pending queue for work that cannot enter the current output tick; it does not
create a queue per projected service lane.  Capacity may be
introduced later only by selecting the core bounded policy with a documented
refusal path.  The extension should expose data-only inspection views rather
than requiring debuggers to decode librdkafka or STL layouts.

Alternatives considered
-----------------------

Port the existing Python classes directly to C++
   Rejected.  It would preserve magic decorator signatures, topic-global
   state, incomplete records, and hidden policies while merely moving their
   implementation language.

Copy Point72 CSP's ``KafkaAdapterManager`` API
   Rejected as the primary API.  Its graph-time/runtime manager split,
   sharing, status stream, and push/pull hand-off are useful, but its public
   Kafka surface remains Python/property-dictionary oriented and does not meet
   hg_cpp's first-class C++ authoring requirement.

Add a generic core messaging connector first
   Rejected.  The current evidence supports Kafka-specific behavior and
   existing service boundaries.  It does not yet establish a stable
   domain-independent acknowledgement, checkpoint, or backpressure contract.

Send every record through the core queue policy
   Selected after RFC 0027 supplied the shared sender admission, shutdown, and
   queue-lifetime contract.  The former extension queue and conflated wake-up
   duplicated that core boundary and were removed.

Expose only a sink and hide delivery reports
   Rejected.  Asynchronous enqueue and broker delivery are different events;
   queue-full and permanent delivery failure must be observable and
   attributable.

Treat Kafka timestamp as engine time for every record
   Rejected.  It changes live graph time and invents a total order across
   partitions.  Historical record-time replay remains explicit and the legacy
   wrapper selects it for compatibility.

Advertise at-least-once or exactly-once as a producer option
   Rejected.  Those are processing/checkpoint claims, not aliases for Kafka
   ``acks`` or idempotence.

Use librdkafka's C++ wrapper in the public API
   Rejected.  The C API has the stronger ABI commitment and the C++ wrapper may
   lag it.  All librdkafka types stay behind an extension-owned RAII boundary.

Acceptance criteria
-------------------

Public C++ and extension boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A separately built, Python-free installed-SDK consumer wires subscribe,
  publish, commit, delivery reports, and service events.
* Core builds and tests without Kafka, librdkafka, Python, or the extension.
* The extension exports no librdkafka type through its public hgraph values or
  service interfaces.
* One ``KafkaServiceImpl`` materializes for one demanded path and configuration;
  duplicate registration at that path is rejected.
* Its graph-to-Kafka edges are distinct sink nodes over ``impl_input``.
  Real-time Kafka-to-graph values use one burst root push source, one structural
  service emit, and mapped projection graphs; bounded simulation recovery uses
  a scheduled replay node feeding the same emit and projections.  Both are
  published through ``impl_output``.
* The real-time/simulation implementation is selected at wiring time.  A
  simulation graph contains no push source and accepts only deterministic,
  bounded record-time subscriptions.
* Structured scalar values use named ``Bundle`` schemas and collections of
  time-series fields use named ``TSB`` schemas in C++ and Python.
* Two pure-C++ graph engines run concurrently on different threads with
  independent service implementations and no shared mutable runtime state.

Behavior
~~~~~~~~

* Multiple publishers per topic work in the new API; the legacy wrapper keeps
  its compatibility error.
* Multiple identical subscriptions share one session, while explicitly
  independent subscriptions do not.
* Per-partition record order is preserved through replay, live ingress, and
  rebalance.
* End-offset snapshot hand-off has no lost or duplicated boundary record,
  including a poll batch which crosses several partition snapshots.
* Starting and stopping positions cover earliest, latest, committed with
  fallback, timestamp, explicit offsets, graph-start compatibility, snapshot,
  and bounded completion.
* Duplicate, ordered, and nullable Kafka headers round-trip in C++ and the new
  Python API.
* Null keys, empty keys, null tombstone values, and empty values remain
  distinct through consume and publish.
* Static and dynamic topic publishing use distinct wiring helpers which
  produce the same ``KafkaPublishRequest`` TSB; the static topic ticks once
  rather than being copied into each record compound scalar.
* Delivery reports distinguish enqueue rejection, drop, broker success,
  retriable failure, and permanent failure.
* Explicit commits occur only after the graph supplies a cursor and never move
  a partition backwards; cursors from a revoked assignment generation are
  rejected and reported.

Lifetime, failure, and flow control
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Consumer failure, producer delivery failure, rebalance failure, queue
  overflow, and shutdown timeout are observable as typed events.
* Only a graph-thread node applies graph-stop policy.
* Producer staging and finite simulation recovery are bounded by record count;
  no byte-capacity contract is exposed.
* Stop prevents new publication, wakes polling, joins all threads, closes
  handles, and only then releases sender storage.
* Repeated start/stop, partial start failure, subscription removal/re-add, and
  graph failure pass under AddressSanitizer; race-sensitive tests also run
  under ThreadSanitizer where platform dependencies permit.

Compatibility
~~~~~~~~~~~~~

* Existing public Kafka tests run against the native compatibility shim.
* Equivalent C++ tests cover every behavior which is not solely Python
  signature reflection.
* Differential tests against the released Python hgraph adaptor cover bytes,
  ``KafkaMessage``, publisher bundles, recovery phases, live-only gating,
  history-only publisher input, flush timing/count, failures, and teardown.
* Timing tests construct their broker and graph scheduling so only promised
  order is compared; uncontrolled worker wall-clock alignment is not treated
  as parity.

Performance and release
~~~~~~~~~~~~~~~~~~~~~~~

* Native byte transport has no GIL acquisition.
* Benchmarks and memory profiles cover the scenarios listed above and record a
  baseline before compatibility-shim removal.
* The extension's native suite, Python 3.14 suite, real/mock broker integration
  tests, installed-SDK consumer, and Linux sanitizer gates pass.
* The monorepo migration pull request tests core and the extension at the same
  commit and documents their shared 0.8 version tag and release ordering.

Implementation plan
-------------------

1. Create ``hgraph-kafka`` with value/configuration types, a fake transport
   seam, CMake package, and installed pure-C++ smoke consumer.
2. Implement one multi-interface ``KafkaServiceImpl`` and the four service
   contracts using a fake transport; prove one materialization per path,
   sink/push boundaries, lifecycle, ordered transport delivery, cursor
   assignment generations, and multi-engine behavior before introducing
   librdkafka.
3. Add the librdkafka C RAII layer, consumer recovery/live state machine,
   producer callbacks, commits, rebalances, and typed events.
4. Add native byte codecs and the Python bridge/new service wiring API.
5. Move the existing decorators and ``KafkaMessage`` implementation into
   ``hgraph_kafka``, retain a guarded, core-owned ``hgraph.adaptors.kafka``
   forwarding shim, and run differential behavior tests against released
   hgraph without adding a core dependency on the extension.
6. After RFC 0027, replace the private ingress queues, byte accounting,
   generation wake tokens, and drain nodes with the standard push-source FIFO,
   graph-side envelope projections, command sinks, and scheduled simulation
   replay described above.
7. Add broker integration, failure injection, memory/performance evidence,
   ASan/TSan validation, and packaging on supported platforms.
8. Land the extension addition and removal of the core Kafka implementation
   atomically, and update this RFC to Accepted only when implementation and
   transition tests have merged.

Implementation status
---------------------

The native extension, service interfaces, fake transport, librdkafka runtime,
and Python authoring bridge are implemented.  The RFC 0027 migration described
in step 6 is included in the current implementation: real-time ingress uses the
standard burst push source, delivery/scalar unrolling uses standard graph
composition, and simulation uses graph-owned scheduled replay.
This RFC remains ``Proposed`` until the remaining performance, sanitizer,
installed-package, and compatibility-transition evidence satisfies the
acceptance criteria above.

References
----------

* :doc:`rfc_0000` — RFC and downstream-promotion process.
* :doc:`rfc_0003_extension_scalar_registration` — installed Python/native
  scalar association for extensions.
* :doc:`rfc_0005_hgraph_1_0_api` — compact core and separately installed
  integration packages.
* :doc:`rfc_0011_source_only_adaptor_collapse` — common service/adaptor
  boundary substrate.
* :doc:`rfc_0014_request_reply_transport_planning` — direct transport for
  decoupled external sinks and sources.
* :doc:`../developer_guide/services` — authoritative service and adaptor
  boundary behavior.
* `Point72 CSP Kafka graph-time API
  <https://github.com/Point72/csp/blob/main/csp/adapters/kafka.py>`_.
* `Point72 CSP native Kafka manager
  <https://github.com/Point72/csp/blob/main/cpp/csp/adapters/kafka/KafkaAdapterManager.h>`_.
* `Point72 CSP realtime adaptor guidance
  <https://github.com/Point72/csp/blob/main/docs/wiki/how-tos/Write-Realtime-Input-Adapters.md>`_.
* `Apache Flink Kafka connector
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/>`_.
* `Apache Flink Kafka sink delivery contract
  <https://nightlies.apache.org/flink/flink-docs-release-1.16/api/java/org/apache/flink/connector/kafka/sink/KafkaSink.html>`_.
* `Apache Kafka consumer API and threading model
  <https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html>`_.
* `Apache Kafka delivery semantics
  <https://kafka.apache.org/41/design/design/>`_.
* `librdkafka introduction
  <https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md>`_.
* `librdkafka FAQ
  <https://github.com/confluentinc/librdkafka/wiki/FAQ>`_.
