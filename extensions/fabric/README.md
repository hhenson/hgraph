# hgraph-fabric

`hgraph-fabric` is the C++-first versioned dataflow extension specified by
[RFC 0026](../../docs/source/rfc/rfc_0026_versioned_dataflow_fabric.rst).
It decouples recurring graph components through immutable, complete `Frame`
versions and contiguous lineage revisions.

The current implementation provides the installed C++/Python operator and
value contracts, canonical durable keys and metadata, run-scoped
configuration, and broker-free memory notification. One lazy root
`FabricServiceImpl` graph composes publication, live, replay, version load and
diagnostics nodes for each `GraphValue`. Each node owns only its local
algorithm state; their sequencing, candidates, completions, metrics and events
remain on ordinary graph edges. Persistence handles are copied from the
run-scoped `FabricConfig`. Client
`subscribe_data` and `publish_data` operators only communicate with that
service through hgraph service edges.

The native publication state machine writes each Frame, wins the immutable
revision slot, repairs the derived as-of/latest indexes, and only then
advertises the accepted revision. Notifier failure leaves the accepted
revision pending delivery so retry cannot change the durable winner. The
wiring-time planner discovers subscriptions through direct and nested graph
ownership, validates explicit dependency handles, partitions independent
consistency forests, and wires hidden lineage signals to publication requests.

The shared ingress coordinator derives behavior from the graph run:

* simulation walks durable as-of histories over the executor's half-open
  interval using ordinary node scheduling; and
* real-time execution loads a durable initial image, then advances only when
  a complete shared revision arrives on the ordinary notice edge.

There is no per-subscription mode. A narrow simulation interval provides the
graph-coordinated equivalent of a one-point replay. The separate synchronous
`load_data` API handles a simple single-dataset point lookup without
constructing or solving a consistency forest.

Complete live revision messages populate dependency indexes directly. Durable
metadata is read for startup, reconnect reconciliation and explicit revision
gaps; only a selected changed root causes its Frame to load. The live cache
retains only ids in the observed consistency forest, conflates by data id and
is bounded.

The optional `hgraph::fabric_kafka` target supplies the production transport.
It validates idempotent `acks=all` publication and non-dropping queue policies,
subscribes independently to the complete configured topic, and carries the
full accepted `DataRevision` keyed by canonical data id. Decoded Kafka records
and revisions cross their public C++ graph edges as immutable `Shared` values.
The Kafka service creates one execution-local broker worker resource. Its
standard burst push source emits ordinary graph
edges into Fabric; broker callbacks never access the graph or a Fabric output.
`Recovering` and `Live` lifecycle edges gate the initial durable image and
trigger durable-head reconciliation for each new live generation. Replay and
other simulation runs do not compose the adapter and never create a push source.

Publication crosses a graph-native request edge only after its Frame, immutable
revision and derived indexes are durable. The Fabric service graph retains
durable candidates in a keyed time series, serialises them onto one ordered
request edge, and correlates Kafka delivery reports returning on a separate
graph edge. Retriable failures unbind and rebind a reference to the same
``Shared<DataRevision>`` allocation. Candidate selection, retry, completion and
diagnostics remain graph-owned; acknowledgement never selects or creates a
revision. The Kafka worker owns only broker I/O, returning events through its
FIFO root push source. Valid decoded subscription cursors are explicitly
committed, but offsets remain non-authoritative because durable history repairs
duplicates or missed notifications.

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
the package registers the same native operators.

## Python examples

Install the extension and import ordinary hgraph graph-building primitives:

```sh
python -m pip install hgraph-fabric
```

Fabric registration belongs in the outer host graph. Reusable components call
only `subscribe_data()` and `publish_data()`, so the same component can run
against the local memory host or a production host configured with persistent
stores and Kafka.

### Publish one Frame locally

This complete example publishes one atomic Arrow table. The memory service is
run-scoped and intended for local development and tests; a separate graph run
gets a separate memory Fabric.

```python
from datetime import timedelta

import pyarrow as pa
import hgraph as hg
import hgraph_fabric as fabric


@hg.graph
def publish_prices() -> None:
    prices = hg.const(
        pa.table({"symbol": ["AAPL", "MSFT"], "price": [201.5, 415.0]}),
        tp=hg.TS[hg.Frame],
    )
    fabric.publish_data("prices/raw", prices)


@hg.graph
def local_app() -> None:
    fabric.register_memory_fabric_service(prefix="examples/basic")
    publish_prices()


hg.run_graph(
    local_app,
    run_mode=hg.EvaluationMode.SIMULATION,
    start_time=hg.MIN_ST,
    end_time=hg.MIN_ST + timedelta(microseconds=20),
)
```

The runnable version is
[`python/examples/publish_once.py`](python/examples/publish_once.py).

### Run one subscription graph live or as replay

Application code declares only the durable data id:

```python
prices = fabric.subscribe_data("prices/enriched")
```

The run owns the policy. `EvaluationMode.REAL_TIME` follows accepted revisions
from the configured live transport. Simulation deterministically replays the
executor's start/end interval. Running simulation over one timestamp (or the
smallest practical interval around it) gives the graph-coordinated equivalent
of a snapshot without changing application wiring. The complete alternatives
are in
[`python/examples/subscription_modes.py`](python/examples/subscription_modes.py).

### Load one dataset directly

When no graph coordination is required, use the standalone point lookup. It
loads the latest stored version of one data id by default. Pass `as_of` to
select the newest revision at or before a cutoff:

```python
config = fabric.make_memory_fabric_config(prefix="examples/history")
latest = fabric.load_data(config, "prices/enriched")
historical = fabric.load_data(config, "prices/enriched", as_of)
```

The configuration is explicit; the call does not inspect graph state and does
not solve transitive lineage. It returns `None` when no matching value exists.
The Python Frame presentation is PyArrow by default and Polars when hgraph's
Polars compatibility switch is enabled and Polars is installed. See
[`python/examples/load_data.py`](python/examples/load_data.py) for a runnable
publish-then-load example using one owning configuration.

### Build a derived dataset with automatic lineage

Application code may give incoming Frames typed row views, compose ordinary
hgraph operators, and publish the complete result. Fabric's durable boundary is
`TS[Frame]`, so the typed result is converted back to that schema-free Frame
view for publication; its Arrow schema remains part of the stored Frame.

```python
raw_prices = fabric.subscribe_data("prices/raw")
instrument_reference = fabric.subscribe_data("instruments/reference")

prices = hg.convert[hg.TS[hg.Frame[Price]]](raw_prices)
instruments = hg.convert[hg.TS[hg.Frame[Instrument]]](instrument_reference)
enriched: hg.TS[hg.Frame[EnrichedPrice]] = hg.join(
    prices, instruments, on="symbol", how="left"
)

fabric.publish_data(
    "prices/enriched", hg.convert[hg.TS[hg.Frame]](enriched)
)
```

The wiring planner discovers both subscriptions upstream of the joined result,
so every accepted `prices/enriched` revision records both immediate input
versions. Reusing one subscription in several computations is safe: each
publisher records the lineage reachable from its own value edge.

[`python/examples/derived_dataset.py`](python/examples/derived_dataset.py)
contains the complete graph, plus an explicit-lineage variant using
`dependency_handle()` and `DependencySelection.explicit()`. Prefer automatic
lineage; use explicit handles only when the semantic dependency is deliberately
not reachable through the published value's graph ancestry.

All example files keep service registration in small local host wrappers. A
production native host installs `FabricConfig` and the Kafka transport instead;
the reusable Python component graphs are unchanged.

## Production configuration

`FabricConfig` is run-scoped state. A host constructs the persistence handles
once, installs the config in the graph's `GlobalState`, and registers the
service at wiring time. This local-filesystem host is a useful production-like
deployment and exercises the same protocol as S3:

```cpp
namespace hgf = hgraph::fabric;
namespace hgps = hgraph::persistence::store;
namespace hg = hgraph;

auto config = hgf::make_memory_fabric_config("production/blue");
config.notification_request_limit = 4096;
config.objects = hgps::make_object_store(
    hgps::ObjectStoreConfig{hgps::LocalLocation{"/srv/fabric/metadata"}});
config.frames = hgps::make_frame_store(hgps::FrameStoreConfig{
    .location = hgps::LocalLocation{"/srv/fabric/frames"},
    .format = hgps::Format::Parquet,
    .compression = hgps::Compression::Zstd,
});
const auto path = hg::service::path("blue-fabric");
hgf::set_fabric_config(wiring.global_state(), path.value, std::move(config));
hgf::register_service(wiring, path);
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

Fabric-owned queues are bounded. Publication accepts at most 1,024 waiting
requests per data id, each live session retains at most 4,096 conflated
observed data ids, and the graph transport retains at most 1,024 correlated
deliveries with at most eight retries. Diagnostic events retain at most 256
distinct paths; additional paths conflate into `diagnostics.capacity` with an
occurrence count. Hitting a Fabric work-queue bound is an explicit failure,
never silent data loss. Kafka real-time ingress deliberately uses RFC 0015's
standard unbounded burst push-source queue to preserve non-dropping worker
admission; finite recovery and producer staging retain their configured record
bounds.

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
