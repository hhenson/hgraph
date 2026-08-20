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
metadata is read for startup and explicit revision gaps; only a selected
changed root causes its Frame to load. The live cache conflates by data id and
is bounded. The Kafka adapter which supplies the production real-time
push-source notice edge and reconnect reconciliation is the next RFC
checkpoint; replay and snapshot never create a push source.

Durable keys use a canonical reversible data-id segment and a portable
1,024-byte whole-key limit shared with S3. The fabric prefix and encoded data
id must leave room for the key category and fixed-width ordinal.

Native hosts install `FabricConfig` in `GlobalState`, call
`hgraph::fabric::register_service()`, call
`hgraph::fabric::register_fabric_operators()`, and link `hgraph::fabric`.
Python consumers import `hgraph_fabric` and call
`register_memory_fabric_service()` for the deterministic local host; importing
the package registers the same native operators and scalar enum.
