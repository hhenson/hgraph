Kafka
=====

``hgraph-kafka`` is a first-party extension that connects an hgraph graph to
Apache Kafka.  It is a separate distribution because it contains a native
Kafka client (librdkafka); installing the core ``hgraph`` package does not add
Kafka or its dependencies.  Install it with:

.. code-block:: console

   pip install hgraph-kafka

The extension is C++-first.  Its Python API is an authoring surface over the
same native service, queues, worker threads, value types, and lifecycle as the
C++ API.  The older ``hgraph.adaptors.kafka`` decorators remain available as a
compatibility layer, but new applications should use ``hgraph_kafka`` and the
explicit calls described here.  The design background is :doc:`RFC 0015
<../../rfc/rfc_0015_kafka_extension_api>`; this page is the user guide.

What the extension provides
---------------------------

Registering a Kafka service at a *path* gives graph code four typed
interfaces.  A path is an application-local name such as ``"orders"`` or
``"audit"``.  It selects one immutable connection and queue configuration;
use another path when those settings must differ.

.. list-table::
   :header-rows: 1
   :widths: 27 27 46

   * - Operation
     - Python API
     - Result
   * - Register
     - ``register_kafka_service(config, path=)``
     - Declares the lazy, graph-owned Kafka service.  It opens no broker
       connection while the graph is being wired.
   * - Consume
     - ``kafka_subscribe(key, path=)``
     - A bundle with a record stream, the matching commit cursor, and the
       subscription state.
   * - Produce
     - ``kafka_publish(topic, record, path=)``
     - One asynchronous delivery-report stream for the publisher's records.
   * - Commit
     - ``kafka_commit(cursor, path=)``
     - A sink for cursors that the graph has decided are processed.
   * - Observe
     - ``kafka_events(path=)``
     - One shared stream of connection, rebalance, queue, retry, and failure
       events for that service path.

The actual Kafka clients and their worker threads are created when the graph
starts and joined when it stops.  There is no Python or C++ Kafka manager to
construct, poll, or close outside the graph.  The worker threads exchange data
with graph evaluation through bounded extension-owned queues, so broker input
does not create an unbounded graph queue.

The essential data flow is:

.. mermaid::

   flowchart LR
      K[Kafka broker] --> C[subscription service]
      C --> R[record]
      C --> U[cursor]
      R --> P[application processing]
      P --> A[acknowledged cursor]
      A --> M[commit service]
      P --> E[encode]
      E --> S[publish service]
      S --> K
      S --> D[delivery report]
      K --> V[service events]

The record and cursor of a subscription tick together.  Keep that pairing:
the cursor carries the live subscription identity and assignment generation
as well as the record's next offset, so a late acknowledgement cannot commit a
revoked partition assignment.

Quick start: Python
-------------------

The following graph subscribes to ``orders``, processes each record on the
graph thread, explicitly commits the cursor only after that processing step,
and observes service events.  ``persist_order`` stands for the application's
durable processing operation.

.. code-block:: python

   import hgraph as hg
   import hgraph_kafka as kafka


   @hg.compute_node
   def persist_then_ack(
       record: hg.TS[kafka.KafkaRecord],
       cursor: hg.TS[kafka.KafkaCursor],
   ) -> hg.TS[kafka.KafkaCursor]:
       persist_order(record.value)  # application-defined, synchronous work
       return cursor.value


   @hg.sink_node
   def log_kafka_event(event: hg.TS[kafka.KafkaEvent]):
       print(event.value.severity, event.value.category, event.value.message)


   @hg.graph
   def orders_worker():
       kafka.register_kafka_service(
           kafka.KafkaServiceConfig.from_bootstrap_servers(
               ["localhost:9092"], client_id="orders-worker"
           ),
           path="orders",
       )

       key = kafka.KafkaSubscriptionKey(
           topics=("orders",),
           group_id="orders-worker",
           start_position=kafka.KafkaStartPosition.committed(),
           commit_mode=kafka.KafkaCommitMode.EXPLICIT,
       )
       subscription = kafka.kafka_subscribe(
           hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]), path="orders"
       )

       acknowledged = persist_then_ack(
           subscription["record"], subscription["cursor"]
       )
       kafka.kafka_commit(acknowledged, path="orders")
       log_kafka_event(kafka.kafka_events(path="orders"))

``KafkaSubscriptionKey`` is scalar configuration, so wrap a fixed key with
``hg.const(..., tp=hg.TS[KafkaSubscriptionKey])``.  A dynamic application can
also supply a changing key time series, adding and removing subscriptions as
the key changes.

``persist_then_ack`` is intentionally one node: it makes the cursor depend on
the operation considered complete.  Do not commit the cursor directly if
durable processing happens elsewhere or later.  Conversely, when graph
delivery itself is sufficient, set ``commit_mode=KafkaCommitMode.ON_GRAPH_DELIVERY``
and do not wire ``kafka_commit``.  ``ON_GRAPH_DELIVERY`` means the record
entered hgraph, *not* that arbitrary downstream work completed.

Records, cursors, and reports
-----------------------------

Kafka values retain facts that are easy to lose in a generic message wrapper:

``KafkaRecord``
   A consumed record: ``topic``, ``partition``, ``offset``, optional
   ``timestamp`` and ``key``, optional ``value``, timestamp type, and ordered
   ``headers``.  A ``None`` value is a valid compacted-topic tombstone; it is
   distinct from ``b""``.  Header values may likewise be ``None``, and headers
   remain a tuple so duplicate names and their order are preserved.

``KafkaCursor``
   The cursor paired with a record.  Its ``next_offset`` is already
   ``record.offset + 1``—pass it unchanged to ``kafka_commit`` rather than
   calculating an offset yourself.

``KafkaProduceRecord``
   The outbound payload: optional ``value`` and ``key``, ordered headers,
   optional timestamp and explicit partition, plus an application
   ``user_token``.  The topic is supplied separately to ``kafka_publish`` so
   a static topic is not copied into every record.

``KafkaDeliveryReport``
   The outcome of asynchronous production.  It includes the user token,
   service sequence, topic, partition and offset where known, status, and
   diagnostic/error fields.  Broker enqueue and broker delivery are different
   events: only ``KafkaDeliveryStatus.DELIVERED`` is broker acknowledgement.

``KafkaEvent``
   Path-wide operational diagnostics.  Check ``severity``, ``category``,
   ``retriable``, and ``fatal`` rather than relying on worker-thread logs.
   Events intentionally never include credentials or raw configuration.

Producing records
-----------------

Publishing accepts either a fixed string topic or ``TS[str]`` for dynamic
routing.  In both cases the result is a delivery-report time series.  Use a
``user_token`` to correlate that report with an order or request in your
application.

.. code-block:: python

   @hg.compute_node
   def encode_result(value: hg.TS[str]) -> hg.TS[kafka.KafkaProduceRecord]:
       return kafka.KafkaProduceRecord(
           value=value.value.encode(),
           key=b"customer-42",
           headers=(
               kafka.KafkaHeader("content-type", b"text/plain"),
               kafka.KafkaHeader("trace-id", b"c4f1"),
           ),
           user_token="result-42",
       )


   @hg.sink_node
   def observe_delivery(report: hg.TS[kafka.KafkaDeliveryReport]):
       if report.value.status != kafka.KafkaDeliveryStatus.DELIVERED:
           raise RuntimeError(
               f"Kafka publish {report.value.user_token} failed: "
               f"{report.value.message}"
           )


   @hg.graph
   def publisher(result: hg.TS[str], topic: hg.TS[str]):
       # The configuration/path can be shared with consumers in this graph.
       kafka.register_kafka_service(
           kafka.KafkaServiceConfig.from_bootstrap_servers(["localhost:9092"]),
           path="orders",
       )
       record = encode_result(result)
       static_report = kafka.kafka_publish("order-results", record, path="orders")
       dynamic_report = kafka.kafka_publish(topic, record, path="orders")
       observe_delivery(static_report)
       observe_delivery(dynamic_report)

The extension supports several publishers for the same topic in the new API.
Do not treat a successful call to ``kafka_publish`` as delivery: a bounded
producer queue can reject or drop work according to configuration, and broker
delivery arrives later on the report stream.

Subscription choices
--------------------

A ``KafkaSubscriptionKey`` is the complete, immutable description of one
subscription.  It must choose exactly one selector:

* ``topics=("orders", "refunds")`` subscribes by explicit topic name;
* ``topic_pattern="events-.*"`` lets Kafka discover matching topics; or
* ``partitions=(KafkaTopicPartition("orders", 0), ...)`` pins the request to
  known partitions.

``assignment_mode=KafkaAssignmentMode.GROUP`` uses consumer-group assignment.
``INDEPENDENT`` gives the subscription its own assignment and therefore
requires explicit topics or partitions (not a pattern).  ``sharing_identity``
is an optional application label that participates in identity; use it to
make intentionally distinct subscriptions clear.  ``key_filter=b"..."`` is an
optional exact record-key filter applied after Kafka delivery; it is useful for
local fan-out but does not change Kafka's partition assignment.  Set
``isolation_level="read_committed"`` when a consumer must not read records
from aborted Kafka transactions (the default is ``"read_uncommitted"``).

Starting and stopping positions are deliberately independent.  These are the
most useful forms:

.. code-block:: python

   from datetime import UTC, datetime

   # Restart a worker from its committed offset, or earliest if no offset exists.
   restart = kafka.KafkaStartPosition.committed(
       kafka.KafkaOffsetFallback.EARLIEST
   )

   # Run a bounded replay from a timestamp through the end offsets observed
   # when the subscription starts.
   replay = kafka.KafkaSubscriptionKey(
       topics=("orders",),
       group_id="orders-backfill",
       assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
       start_position=kafka.KafkaStartPosition.at_timestamp(
           datetime(2026, 8, 1, tzinfo=UTC)
       ),
       stop_position=kafka.KafkaStopPosition.snapshot(),
       commit_mode=kafka.KafkaCommitMode.NONE,
       recovery_clock=kafka.KafkaRecoveryClock.RECORD_TIMESTAMP,
       merge_policy=kafka.KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET,
   )

Other start forms are ``earliest()``, ``latest()``, ``at_offsets(...)``, and
``graph_start_time()``.  Other stop forms are ``unbounded()``,
``at_timestamp(...)``, ``at_offsets(...)``, and ``graph_lifetime()``.
``snapshot()`` captures each selected partition's end offset at startup, so it
finishes after the history visible at that point.  Watch the state stream for
``KafkaSubscriptionState.BOUNDED_COMPLETE`` rather than inferring completion
from an empty poll.

The commit mode makes the acknowledgement point explicit:

``KafkaCommitMode.NONE``
   The extension neither stores nor commits offsets.

``KafkaCommitMode.ON_GRAPH_DELIVERY``
   It stores the next offset once the record has entered its hgraph time
   series.  This is appropriate only when graph delivery is the intended
   acknowledgement boundary.

``KafkaCommitMode.EXPLICIT``
   It commits only live cursors supplied to ``kafka_commit``.  This is the
   usual choice when the graph performs application processing before the
   acknowledgement.

For an unbounded subscription that first replays history, the extension takes
the same end-offset snapshot, emits the history below it, then continues using
the same consumer session.  This avoids a gap between replay and live input.
The state stream progresses through values such as ``STARTING``,
``RECOVERING``, ``LIVE``, ``BOUNDED_COMPLETE``, ``RETRYING``, and ``FAILED``.
Kafka preserves order within a partition; it does not define a total order
across partitions.  ``PARTITION`` preserves the natural partition delivery;
``TIMESTAMP_TOPIC_PARTITION_OFFSET`` is the explicit deterministic merge used
for record-time recovery.

Simulation
----------

Kafka selects a separate service graph for simulation.  It contains no push
source: the service first preloads a finite recovery window, then ordinary
drain nodes schedule records by their Kafka timestamps.  This path requires a
bounded stop position, ``RECORD_TIMESTAMP`` recovery, and
``TIMESTAMP_TOPIC_PARTITION_OFFSET`` merge ordering.  ``GRAPH_LIFETIME`` is
treated as a snapshot boundary in simulation.  Publishing, committing,
unbounded subscriptions, and arrival-clock recovery are rejected.

Python ``run_graph`` selects the service graph from its wiring-time evaluation
mode.  C++ authors select it explicitly when registering the service::

   kafka::register_service(wiring, path, config,
                           kafka::KafkaServiceMode::Simulation);

The preload barrier keeps broker-thread timing out of simulated graph time;
only the retained record timestamps drive later evaluations.

Configuration, flow control, and failures
------------------------------------------

``KafkaServiceConfig`` has separate connection, consumer-default, and
producer sections.  The defaults are suitable for a local broker, but
production applications should make their queue limits, failure policy, and
librdkafka options explicit.  Options are immutable ``KafkaOption`` pairs;
keep secrets out of source control and supply the appropriate Kafka security
options through deployment configuration.

.. code-block:: python

   config = kafka.KafkaServiceConfig(
       connection=kafka.KafkaConnectionConfig(
           bootstrap_servers=("broker-1:9093", "broker-2:9093"),
           client_id="billing-worker",
           options=(
               kafka.KafkaOption("security.protocol", "SASL_SSL"),
               kafka.KafkaOption("sasl.mechanism", "SCRAM-SHA-512"),
           ),
       ),
       consumer_defaults=kafka.KafkaConsumerDefaults(
           ingress_record_limit=20_000,
           ingress_byte_limit=128 * 1024 * 1024,
           inbound_overflow=kafka.KafkaOverflowAction.FAIL,
           failure_policy=kafka.KafkaFailurePolicy.STOP_GRAPH,
       ),
       producer=kafka.KafkaProducerOptions(
           idempotent=True,
           acknowledgements="all",
           linger_ms=10,
           outbound_record_limit=20_000,
           outbound_byte_limit=128 * 1024 * 1024,
           overflow=kafka.KafkaOverflowAction.STAGE,
           stage_overflow=kafka.KafkaOverflowAction.FAIL,
           failure_policy=kafka.KafkaFailurePolicy.STOP_GRAPH,
       ),
   )

Ingress overflow may ``FAIL`` or ``DROP``; it cannot stage.  Outbound overflow
may ``FAIL``, ``DROP``, or ``STAGE`` in a second bounded queue, whose own
overflow action must be ``FAIL`` or ``DROP``.  A failure policy of ``REPORT``
keeps the graph running and reports the problem as ``KafkaEvent`` or a delivery
report; ``STOP_GRAPH`` requests an orderly graph stop.  Choose it according to
whether your application can safely continue after that failure.

Idempotent production requires ``acknowledgements="all"`` (``"-1"`` is also
accepted).  These are Kafka producer mechanisms, not an end-to-end processing
guarantee.  In particular, the extension does not claim exactly-once
consume/process/produce semantics: that requires a coordinated checkpoint and
transaction protocol.

Native C++ wiring
-----------------

The C++ surface is first-class and follows the same model.  Builders create
registered hgraph scalar values; they do not construct a Kafka runtime object.
This example wires a subscription, an explicit commit, publication, delivery
reports, and events at one path.  ``ProcessOrder``, ``EncodeResult``, and the
observer nodes are application nodes.

.. code-block:: cpp

   #include <hgraph/kafka/service.h>
   #include <hgraph/kafka/value_builders.h>
   #include <hgraph/lib/std/operators/conversion.h>

   using namespace hgraph;
   using namespace hgraph::kafka;

   struct OrdersGraph {
       static constexpr auto name = "orders_graph";

       static void compose(Wiring &w) {
           const auto path = service::path("orders");
           register_service(
               w, path,
               service_config()
                   .bootstrap_servers({Str{"localhost:9092"}})
                   .client_id(Str{"orders-worker"})
                   .build());

           auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
               w, subscription_key()
                      .topics({Str{"orders"}})
                      .group_id(Str{"orders-worker"})
                      .commit_mode(KafkaCommitMode::Explicit)
                      .build());
           auto subscription = subscribe(w, path, key);

           // ProcessAndReturnCursor makes the acknowledgement depend on the
           // work considered complete; record and cursor tick together.
           auto cursor = wire<ProcessAndReturnCursor>(
               w, subscription.field<"record">(), subscription.field<"cursor">());
           commit(w, path, cursor);

           auto outbound = wire<EncodeResult>(w, subscription.field<"record">());
           auto delivery = publish(
               w, path, publish_request(w, Str{"order-results"}, outbound));
           static_cast<void>(wire<ObserveDelivery>(w, delivery));
           static_cast<void>(wire<ObserveKafkaEvent>(w, events(w, path)));
       }
   };

For a static topic, use ``publish_request(w, Str{...}, record)``.  The
``Port<TS<Str>>`` overload supports dynamic topics and creates the same publish
request bundle.  C++ callers can use ``make_produce_record`` for records and
``make_start_position`` / ``make_stop_position`` for the explicit position
forms; the fluent ``service_config`` and ``subscription_key`` builders cover
the common configuration shape.

Moving from the legacy API
--------------------------

Existing imports from ``hgraph.adaptors.kafka`` continue to work when
``hgraph-kafka`` is installed.  ``KafkaMessage``, ``message_publisher``,
``message_subscriber``, and ``register_kafka_adaptor`` are compatibility
wrappers over this native service.  They retain historical constraints such as
one publisher per topic and their reduced message/header shape.

Prefer the new API for new work.  It makes topic selection, replay boundaries,
commit timing, failure handling, and delivery reports explicit; it also keeps
duplicate or nullable headers and the complete Kafka record metadata intact.
