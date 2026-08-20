# hgraph-fabric

`hgraph-fabric` is the C++-first versioned dataflow extension specified by
[RFC 0026](../../docs/source/rfc/rfc_0026_versioned_dataflow_fabric.rst).
It decouples recurring graph components through immutable, complete `Frame`
versions and contiguous lineage revisions.

The current implementation establishes the installed C++/Python operator and
value contracts, canonical durable keys and metadata, run-scoped
configuration, and broker-free memory notification. Its native publication
state machine writes each Frame, wins the immutable revision slot, repairs the
derived as-of/latest indexes, and only then advertises the accepted revision.
Notifier failure leaves the accepted revision pending delivery so retry cannot
change the durable winner. The wiring-time planner discovers subscriptions
through direct and nested graph ownership, validates explicit dependency
handles, partitions
independent consistency forests, and wires one root coordinator with hidden
lineage cuts. The ingress consistency resolver observes and repairs accepted
heads, caches immutable revisions and Frames,
indexes revision runs by output version, and selects the unique greatest
newest-compatible cut. Its coordinator dynamically merges and splits
consistency forests, isolates failed forests, prevents exposed root regression,
and returns changed roots as one atomic graph-delivery batch together with the
updated hidden lineage. Later RFC checkpoints connect that coordinator to
subscription modes and Kafka.

Durable keys use a canonical reversible data-id segment and a portable
1,024-byte whole-key limit shared with S3. The fabric prefix and encoded data
id must leave room for the key category and fixed-width ordinal.

Native consumers call `hgraph::fabric::register_fabric_operators()` and link
`hgraph::fabric`. Python consumers import `hgraph_fabric`; importing the package
registers the same native operator overloads and scalar enum.
