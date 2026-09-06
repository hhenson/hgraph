Research Note: Layered Network Services and Conflated State Distribution
=========================================================================

:Status: Research
:Author: Howard Henson
:Created: 2026-08-17
:Related: :doc:`rfc_0014_request_reply_transport_planning`,
          :doc:`rfc_0015_kafka_extension_api`, and
          :doc:`rfc_0024_web_extension_api`

Purpose
-------

This note records the protocol research that should guide the HTTP/2 streaming
API, a future gRPC extension, cached time-series distribution, and network
service discovery. It does not propose a public API or wire format. Those
decisions require separate numbered RFCs.

The central conclusion is that hgraph needs three distinct data-plane
contracts rather than one configurable messaging abstraction:

.. list-table::
   :header-rows: 1
   :widths: 22 31 24 23

   * - Contract
     - Delivery semantics
     - Normal completion
     - Recovery
   * - Unary request/reply
     - One request and one response, with no silent loss
     - Response plus terminal status
     - Retry under an explicit idempotency policy
   * - Streaming request/reply
     - Ordered, non-conflated messages in each direction, with no silent loss
     - Half-close followed by terminal status
     - Restart or an application resume protocol
   * - Cached state subscription
     - Current-state accurate; intermediate states may be conflated
     - Unsubscribe or terminal state
     - Resume from a usable cache state or receive a fresh image

Event-accurate subscriptions are a fourth use case, but not a fourth hgraph
wire protocol. Kafka or another standard event broker should provide retained
event logs, consumer offsets, replay, and broker replication. RFC 0015 defines
the corresponding hgraph integration boundary.

Layering model
--------------

The intended separation is:

.. code-block:: text

   HTTP/2 transport
   +-- unary request/reply
   +-- reliable streaming request/reply
   +-- cached state subscription
       +-- initial image
       +-- structural deltas
       +-- temporal conflation
       +-- cache fan-out
       +-- re-image and recovery

   event-accurate subscription --> Kafka or another event broker

   control plane
   +-- service and endpoint discovery
   +-- connection state
   +-- readiness and availability
   +-- draining and removal
   +-- capacity and observed load
   +-- data quality and cache freshness

HTTP/2 is the common byte-stream substrate. gRPC is one protocol layered on
that substrate. Cached state distribution is another. Neither gRPC framing nor
time-series semantics belong in the generic HTTP/2 session API.

Unary and reliable-streaming semantics
--------------------------------------

gRPC is the primary interoperability reference for request/reply. It defines
unary, server-streaming, client-streaming, and bidirectional-streaming calls,
and preserves message order within an individual call. [#grpc-core-concepts]_
The generic hgraph streaming contract should be expressive enough to implement
all four forms without depending on protobuf or gRPC status codes.

"Non-lossy" must have a bounded meaning. For an active call, an accepted
message is delivered in order or the call terminates with an explicit error.
It does not imply exactly-once processing across a broken connection. The gRPC
HTTP/2 protocol similarly closes calls when a connection fails. [#grpc-h2]_
Retry can duplicate work unless the application supplies idempotency or resume
semantics.

The common HTTP/2 contract consequently needs to preserve:

* initial metadata and terminal trailers as distinct collections;
* ordered incremental body or message delivery in both directions;
* independent half-close of each direction;
* explicit inbound flow-control release after application admission;
* bounded outbound buffering and observable backpressure;
* per-stream cancellation and reset reasons;
* deadlines and terminal status;
* connection and stream identity;
* GOAWAY and graceful drain, including the last accepted stream; and
* failure that is explicit rather than represented by an empty or partial
  successful result.

The transport should expose these primitives once. Unary HTTP, streaming HTTP,
gRPC, and cached subscriptions should adapt them rather than implement
independent socket or lifecycle machinery.

Cached state subscription
-------------------------

The cached subscription contract is **state-lossless but event-lossy**. Its
purpose is to keep a consumer's materialized state current, not to reproduce
every intermediate producer event.

The first concrete application is TSD distribution. The same model may extend
to other time-series types when their snapshot, delta-application, and
delta-conflation operations are well defined. The protocol must not assume
that retaining the last serialized frame is a valid conflation algorithm.

Three optimizations must remain distinct:

``structural delta``
   Transmit only keys, fields, or structural elements that changed.

``temporal conflation``
   Compose pending changes so that obsolete intermediate states are not sent
   to a slower consumer.

``wire compression``
   Compress the serialized bytes without changing the message sequence.

Conflation is part of the value or time-series delta algebra. For a dictionary,
the pending state normally follows these rules:

* repeated add/update operations for one key retain the latest value;
* add/update followed by removal becomes a tombstone;
* removal followed by add/update becomes the new current value; and
* nested values conflate through their registered delta operation rather than
  through a byte-level last-write rule.

Images and the live hand-off
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A subscription begins with an image representing a coherent cache state. A
large image may be chunked, but the chunks share one image identity, source
epoch, and target revision. Updates that arrive while the image is being
transmitted are accumulated behind the image barrier and may be conflated.

After the image completes, the consumer receives a delta that moves it toward
the cache's then-current state. The next target revision need not be the image
revision plus one: revision gaps are expected when intermediate updates have
been conflated. A gap is therefore not, by itself, a data-loss error.

A revision identifies ordering within a source epoch. An epoch or generation
change identifies loss of continuity, such as primary failover or cache
reconstruction. Unless the protocol can prove a valid cross-epoch transition,
the safe response is a new image.

The minimum logical message vocabulary is expected to cover:

* subscribe, amend selection, and unsubscribe;
* image begin, image part, and image complete;
* delta with source epoch and target revision;
* item or stream status, including stale/suspect and recovering states;
* re-image required; and
* terminal close or error.

Exact message shapes, acknowledgements, and resume tokens remain RFC work.

Cache fan-out and slow consumers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The producer publishes each state change once. Cache servers maintain the
current materialized value, construct initial images, and fan out subsequent
deltas. This prevents each new subscriber from imposing image-construction or
network work on the producer.

HTTP/2 flow control bounds bytes admitted by a stream but does not implement
state-aware conflation. The cache must manage queued, not-yet-emitted changes
using the delta algebra. Bytes already handed to the transport cannot be
replaced safely; an application acknowledgement is required if recovery must
distinguish transport delivery from consumer application.

Consumers can be partitioned by progress or service class:

* fast consumers receive frequent small deltas;
* slower cohorts receive increasingly conflated deltas; and
* consumers beyond the supported lag or memory bound receive a new image.

The implementation should share immutable values or delta segments between
cohort members. It must not copy the full materialized state or retain an
unbounded event queue per subscriber.

Primary and secondary caches require explicit ownership and failover rules:

* source and cache instance identity;
* monotonic epochs with fencing of obsolete primaries;
* snapshot or state transfer to a secondary;
* freshness and data-quality state; and
* a defined re-image boundary after continuity is lost.

Comparable state-distribution systems
-------------------------------------

LSEG Real-Time
~~~~~~~~~~~~~~

LSEG Real-Time is the closest direct precedent. A provider publishes a Refresh
message (the image) and subsequent Update messages. The distribution system
caches the item, serves the cached image to a new consumer, forwards updates,
and propagates Status messages. Its guidance recommends an unsolicited fresh
image after source recovery so the distribution cache and consumers return to
a known coherent state. [#lseg-cache]_

The useful lessons are the explicit separation of image, update, and status;
cache-mediated fan-out; data quality independent of connectivity; multipart
images for large structures; and re-image after recovery.

Solace message eliding
~~~~~~~~~~~~~~~~~~~~~~

Solace calls slow-consumer conflation "message eliding". Eligible direct
messages representing current state are delivered at a rate the consumer can
handle instead of queueing obsolete intermediate values. [#solace-eliding]_
This supports an explicit per-stream or per-subscription eligibility policy:
reliable event messages must never enter an eliding queue accidentally.

DDS
~~~

DDS provides a mature policy vocabulary for state distribution: keyed
instances, reliable versus best-effort delivery, ``KEEP_LAST`` versus
``KEEP_ALL`` history, durability for late joiners, resource limits,
liveliness, ownership, and content/time filtering. With ``KEEP_LAST`` the
history depth applies per keyed instance, and non-volatile durability can
supply retained samples to a late reader. [#dds-qos]_ [#dds-durability]_

hgraph should learn from the separation of these policies without recreating
the complete DDS API. In particular, reliability, history, durability, and
resource bounds must not be collapsed into one "quality" option.

NATS KV and Kafka tables
~~~~~~~~~~~~~~~~~~~~~~~~

NATS KV watchers can receive the initial values followed by live changes, and
the default history retains only the latest operation per key. [#nats-kv]_
This is a useful compact reference for image-plus-watch usability, although it
does not by itself define hgraph's structural delta algebra or cache cohorts.
:doc:`rfc_0034_nats_extension_api` applies the narrower broker-specific case:
one full current TSD child image per KV key, an atomic generation/head image
barrier, bounded per-key conflation, and explicit recovery by re-image.

Kafka log compaction retains at least the latest known value per key and uses
tombstones for deletion. [#kafka-compaction]_ Kafka Streams makes the semantic
boundary especially clear: compaction is valid for a table/changelog, but
would violate a stream contract where every record matters. [#kafka-table]_
That is the same reason cached state subscription and event subscription must
remain different public contracts.

Control plane: discovery, lifecycle, and load
---------------------------------------------

The data protocols require a control plane, but connection state, service
availability, data quality, and load are not interchangeable:

.. list-table::
   :header-rows: 1
   :widths: 22 36 42

   * - Signal
     - Example states
     - Meaning
   * - Connection
     - idle, connecting, ready, transient failure, shutdown
     - Whether a transport channel can currently carry work
   * - Service readiness
     - serving, not serving, unknown
     - Whether an application service accepts new work
   * - Endpoint lifecycle
     - discovered, active, draining, removed
     - Whether routing may select the endpoint
   * - Data quality
     - current, stale/suspect, recovering, unavailable
     - Whether cached values may be trusted
   * - Load and capacity
     - weight, utilization, queue depth, request cost
     - How suitable an otherwise available endpoint is for new work

gRPC already separates channel connectivity from the standard health service.
Its health ``Watch`` RPC allows clients to stop selecting a connected endpoint
whose service reports ``NOT_SERVING``. [#grpc-connectivity]_
[#grpc-health]_

Graceful drain is a first-class lifecycle state. A draining endpoint remains
alive for accepted unary calls and existing streams but receives no new work.
Long-lived streams are bound to the selected endpoint; moving them requires an
application resubscription or hand-off. Cached subscriptions can make that
movement cheap because the destination cache can supply a fresh image or a
valid state transition.

xDS as a control-plane reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

xDS is valuable both as deployable infrastructure and as protocol prior art.
It supports State-of-the-World and incremental resource delivery, explicit
resource removal, resource versions, ACK/NACK, reconnect state, lazy resource
subscription, and optional TTL heartbeats. [#xds-protocol]_

Endpoint Discovery Service supplies endpoint membership, locality, priority,
health, and load-balancing weights. [#envoy-eds]_ Load Reporting Service sends
proxy-observed request and endpoint statistics to the management server.
[#envoy-lrs]_ ORCA complements this with backend-reported utilization and
per-request cost metrics, including an out-of-band stream suitable for
long-lived calls. [#grpc-orca]_

A likely integration boundary is:

* Envoy handles TLS termination where desired, HTTP/2 proxying, routing,
  ordinary health checks, load balancing, observability, and connection
  management;
* an xDS-compatible or xDS-inspired control plane distributes service and
  endpoint state;
* hgraph services report application readiness, data quality, cache epoch,
  cache coverage, and application-specific load; and
* the cached subscription protocol owns image, delta, conflation, and recovery
  semantics that a generic proxy cannot infer.

Load-aware cached routing has an additional locality dimension: an endpoint
that already owns the requested partition or image may be a better choice than
a nominally less-loaded endpoint that must first acquire the state. Endpoint
metadata and weights can carry inputs to that choice, but the policy must
avoid oscillation and stale load reports. Capacity, measured utilization,
cache affinity, and data freshness should remain distinguishable inputs.

Implications for hgraph services
--------------------------------

The network API should compose with hgraph's existing keyed services rather
than introduce a process-global client or server runtime:

* unary calls naturally map to request/reply services;
* server subscriptions and bidirectional calls need a first-class streaming
  service boundary rather than a completed-response value;
* cached subscriptions need selection identity, image/delta/status output,
  and observable subscription lifecycle;
* service discovery and events should be shared reference-service outputs;
  and
* connection creation, listeners, workers, and registrations follow graph
  start/stop ownership.

The streaming service design must preserve C++ as the semantic owner. Python
adapts values and callables to the same native path; it must not implement a
second stream scheduler or cache protocol.

HTTP/2 review criteria
----------------------

Until the streaming RFC fixes the public contract, HTTP/2 changes should be
reviewed for whether they preserve the required extension points:

* body data can be delivered incrementally instead of requiring one complete
  request or response allocation;
* flow-control credit is released only after bounded application admission;
* ingress and egress pressure are isolated per stream;
* headers, trailers, half-close, reset, timeout, and GOAWAY remain observable;
* a slow stream can be reset without terminating unrelated streams;
* connection shutdown stops new streams and gives accepted streams a bounded
  drain period;
* stream identities remain stable across the transport/service boundary;
* transport code does not assume unary completion, protobuf, gRPC, TSD, or
  event-accurate replay; and
* buffering and callbacks are structured so a later streaming API can avoid
  copying complete bodies through an intermediate unary representation.

Open questions for numbered RFCs
--------------------------------

* Should streaming service calls expose one bidirectional primitive with unary
  and one-way forms as restrictions, or four explicit gRPC-like call shapes?
* What is the graph-time-series representation of half-close, cancellation,
  terminal status, and backpressure?
* Which acknowledgements are transport-only, and which prove that a consumer
  applied a cached delta?
* What snapshot and delta operations must a time-series type register to be
  distributable?
* Are cache cohorts selected automatically from lag, or may clients request a
  maximum update frequency or freshness bound?
* Which cache state is replicated to secondaries, and what continuity can be
  promised across promotion?
* Should service discovery consume xDS directly, integrate through Envoy, or
  expose an hgraph contract with xDS as its first implementation?
* Which load metrics are routing inputs, which are observability only, and how
  are stale measurements expired?
* How are authentication, authorization, schema negotiation, and protocol
  evolution represented without coupling them to one serialization format?

References
----------

.. [#grpc-core-concepts] `gRPC core concepts, architecture, and lifecycle
   <https://grpc.io/docs/what-is-grpc/core-concepts/>`__.
.. [#grpc-h2] `gRPC over HTTP/2
   <https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md>`__.
.. [#lseg-cache] `LSEG Real-Time cached image and update workflow
   <https://developers.lseg.com/en/api-catalog/real-time-opnsrc/rt-sdk-cc/tutorials/ema-ni-provider/ema-ni-provider-publishing-our-first-market-price>`__.
.. [#solace-eliding] `Solace message eliding
   <https://docs.solace.com/Messaging/Direct-Msg/Direct-Messages.htm#Message_Eliding>`__.
.. [#dds-qos] `RTI Connext DDS history QoS
   <https://community.rti.com/static/documentation/connext-dds/current/doc/manuals/connext_dds_professional/getting_started_guide/csharp/intro_qos.html>`__.
.. [#dds-durability] `RTI Connext DDS durability QoS
   <https://community.rti.com/static/documentation/connext-dds/current/doc/manuals/connext_dds_professional/users_manual/users_manual/DURABILITY_QosPolicy.htm>`__.
.. [#nats-kv] `NATS JetStream key/value store
   <https://docs.nats.io/nats-concepts/jetstream/key-value-store>`__.
.. [#kafka-compaction] `Apache Kafka log compaction design
   <https://kafka.apache.org/documentation/#compaction>`__.
.. [#kafka-table] `Kafka Streams KTable and KStream semantics
   <https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html>`__.
.. [#grpc-connectivity] `gRPC connectivity semantics
   <https://grpc.github.io/grpc/core/md_doc_connectivity-semantics-and-api.html>`__.
.. [#grpc-health] `gRPC health checking
   <https://grpc.io/docs/guides/health-checking/>`__.
.. [#xds-protocol] `Envoy xDS protocol
   <https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol>`__.
.. [#envoy-eds] `Envoy endpoint discovery configuration
   <https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/endpoint/v3/endpoint.proto.html>`__.
.. [#envoy-lrs] `Envoy Load Reporting Service
   <https://www.envoyproxy.io/docs/envoy/latest/api-v3/service/load_stats/v3/lrs.proto>`__.
.. [#grpc-orca] `gRPC custom backend metrics and ORCA
   <https://github.com/grpc/proposal/blob/master/A51-custom-backend-metrics.md>`__.
