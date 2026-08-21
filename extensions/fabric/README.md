# hgraph-fabric

`hgraph-fabric` is the C++-first versioned dataflow extension specified by
[RFC 0026](../../docs/source/rfc/rfc_0026_versioned_dataflow_fabric.rst).
It decouples recurring graph components through immutable, complete `Frame`
versions and contiguous lineage revisions.

The current implementation provides the installed C++/Python operator and
value contracts, canonical durable keys and metadata, run-scoped
configuration, and broker-free memory notification. One lazy root
`FabricServiceImpl` owns publication state, persistence access, live revision
caches, replay/snapshot sessions, bounded queues and diagnostics. Client
`subscribe_data` and `publish_data` operators only communicate with that
service through hgraph service edges.

The native publication state machine writes each Frame, wins the immutable
revision slot, repairs the derived as-of/latest indexes, and only then
advertises the accepted revision. Notifier failure leaves the accepted
revision pending delivery so retry cannot change the durable winner. The
wiring-time planner discovers subscriptions through direct and nested graph
ownership, validates explicit dependency handles, partitions independent
consistency forests, and wires hidden lineage signals to publication requests.

The shared ingress coordinator supports all three wiring-time modes:

* `Snapshot` emits one recursively bounded consistent image;
* `Replay` walks durable as-of histories over the executor's half-open
  interval using ordinary node scheduling; and
* `Live` loads a durable initial image, then advances only when a complete
  revision arrives on the ordinary notice edge.

Complete live revision messages populate dependency indexes directly. Durable
metadata is read for startup, reconnect reconciliation and explicit revision
gaps; only a selected changed root causes its Frame to load. The live cache
conflates by data id and is bounded.

The optional `hgraph::fabric_kafka` target supplies the production transport.
It validates idempotent `acks=all` publication and non-dropping queue policies,
subscribes independently to the complete configured topic, and carries the
full accepted `DataRevision` keyed by canonical data id. The Kafka service owns
the worker queue and root push sources. Its drain node emits ordinary graph
edges into Fabric; broker callbacks never access the graph or a Fabric output.
`Recovering` and `Live` lifecycle edges gate the initial durable image and
trigger durable-head reconciliation for each new live generation. Replay and
Snapshot do not compose the adapter and never create a push source.

Publication crosses a graph-native request edge only after its Frame, immutable
revision and derived indexes are durable. Correlated Kafka delivery reports
return on a separate graph edge. Retriable failures requeue the same accepted
revision through a bounded bridge; acknowledgement never selects or creates a
revision. Valid decoded subscription cursors are explicitly committed, but
offsets remain non-authoritative because durable history repairs duplicates or
missed notifications.

Durable keys use a canonical reversible data-id segment and a portable
1,024-byte whole-key limit shared with S3. The fabric prefix and encoded data
id must leave room for the key category and fixed-width ordinal.

Native hosts install `FabricConfig` in `GlobalState`, call
`hgraph::fabric::register_service()`, call
`hgraph::fabric::register_fabric_operators()`, and link `hgraph::fabric`.
Production Kafka hosts instead link `hgraph::fabric_kafka` and call
`hgraph::fabric::register_kafka_transport()` with the topic, stable identity
and `KafkaServiceConfig`; that call registers both lazy service singletons.
Python consumers import `hgraph_fabric` and call
`register_memory_fabric_service()` for the deterministic local host; importing
the package registers the same native operators and scalar enum.

## Production configuration

`FabricConfig` is run-scoped state. A host constructs the persistence handles
once, installs the config in the graph's `GlobalState`, and registers the
service at wiring time. This local-filesystem host is a useful production-like
deployment and exercises the same protocol as S3:

```cpp
namespace hgf = hgraph::fabric;
namespace hgps = hgraph::persistence::store;

auto config = hgf::make_memory_fabric_config("production/blue");
config.objects = hgps::make_object_store(
    hgps::ObjectStoreConfig{hgps::LocalLocation{"/srv/fabric/metadata"}});
config.frames = hgps::make_frame_store(hgps::FrameStoreConfig{
    .location = hgps::LocalLocation{"/srv/fabric/frames"},
    .format = hgps::Format::Parquet,
    .compression = hgps::Compression::Zstd,
});
hgf::set_fabric_config(wiring.global_state(), std::move(config));
hgf::register_service(wiring);
```

For S3, replace both `LocalLocation` values with independently prefixed
`S3Location` values. Credentials use the persistence extension's ambient,
explicit or assume-role policy. Prefer ambient workload credentials; never put
credentials into a data id, Frame metadata, revision, Kafka message, or log.
The fabric prefix must be a valid relative persistence key and should identify
one environment. Object-store and topic permissions should be scoped to that
prefix, with encryption enabled in transit and at rest.

A distributed host registers the optional Kafka transport instead of the
configured in-process notifier. The registration validates idempotent
production, `acks=all`, and non-dropping queue policies. The repository CMake
build exports `hgraph::fabric_kafka` when both optional extensions are enabled.
The standalone `hgraph-fabric` wheel deliberately exports only
`hgraph::fabric`; it therefore remains installable without Kafka. A native
distribution that wants the adapter builds with
`HGRAPH_FABRIC_BUILD_KAFKA=ON` and supplies the installed `hgraph-kafka` SDK.

Configuration errors fail at graph startup. Missing stores or notifier,
invalid prefixes, unavailable Parquet support, unreachable S3, unsafe Kafka
profiles, and conflicting service registration never fall back to memory.

## Operations

The `diagnostics()` service publishes a bundle with `metrics` and `events`.
Metrics remain string values under stable names so lifecycle values and
counters share one map. Important groups are:

* `resolution.*`: calls, forest outcomes, cache hits/misses, examined revisions
  and edges, candidate selections, backtracking depth, and notice-to-ready
  samples/microseconds;
* `publication.*`: current queue occupancy and its per-data-id bound;
* `live.*`: conflated notice occupancy and its per-session bound;
* `transport.notification.*`: pending, delivered, retried, failed, and stale
  correlated delivery reports.

Events are keyed by `<component>.<category>` and retain typed `component`,
`category`, `message`, `retriable`, `fatal`, and `occurrences` fields. Repeated
events conflate at that path without losing their count. Kafka lifecycle and
delivery events use their native component/category and severity; synchronous
store reads and publication boundaries report `store.*` failures before the
original graph error is rethrown.

The root service logs one `info` record at successful start and one at stop,
including the canonical service path so multiple Fabric services can be
distinguished without enabling per-tick logging.

Alert on corrupt/ambiguous/cyclic forest counts, a sustained non-zero pending
forest or publication queue, notice-to-ready latency, notification retries or
failures, and Kafka reconnect/rebalance events. Broker notices are hints:
durable revision history remains authoritative and reconnect performs a
durable-head reconciliation.

All queues are bounded. Publication accepts at most 1,024 waiting requests per
data id, each live session retains at most 4,096 conflated data ids, and the
graph transport retains at most 1,024 correlated deliveries with at most eight
retries. Diagnostic events retain at most 256 distinct paths; additional
paths conflate into `diagnostics.capacity` with an occurrence count. Hitting a
work queue bound is an explicit failure, never silent data loss.

V1 retention is intentionally unbounded: one complete Frame per output tick,
plus one small revision and as-of entry for each accepted input/output tuple.
A losing concurrent writer may leave an unreferenced candidate Frame. Do not
apply object-store lifecycle deletion to a live Fabric prefix; retention or
garbage collection needs a later protocol with ancestry-aware compaction.

The first accepted Frame fixes the Arrow schema for a data id. A schema change
uses a new data id (for example `prices/v2`), runs old and new producers during
the consumer migration, then retires the old id only under an explicit
retention plan. Fabric does not reinterpret or transparently migrate stored
Frames.

## Broker conformance

The deterministic suite uses librdkafka's mock cluster and the graph-native
fake service for exact failure injection. Linux CI additionally runs a pinned
single-node Redpanda broker, stops it while a live graph is running, accepts a
new durable revision during the outage, observes a retriable delivery failure,
then restarts the same broker. The test verifies startup image/notice
de-duplication, reconnect reconciliation, explicit retry, same-key partition
ordering, and operation with deliberately small non-dropping ingress/outbound
queues.

Run the same scenario locally against Docker with:

```sh
python3 extensions/fabric/tools/run_kafka_broker_conformance.py \
  --test-executable build/extensions/fabric/tests/hgraph_fabric_kafka_tests
```
