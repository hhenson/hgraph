# hgraph-fabric

`hgraph-fabric` is the C++-first versioned dataflow extension specified by
[RFC 0026](../../docs/source/rfc/rfc_0026_versioned_dataflow_fabric.rst).
It decouples recurring graph components through immutable, complete `Frame`
versions and contiguous lineage revisions.

The current implementation establishes the installed C++/Python operator and
value contracts, canonical durable keys and metadata, run-scoped
configuration, and broker-free memory notification. Its native publication
state machine writes each Frame before proposing a revision, waits for an
asynchronous notifier acknowledgement before racing the immutable revision
slot, and repairs the derived as-of/latest indexes from contiguous accepted
history. Later RFC checkpoints connect that machinery to wiring-time
dependency planning, consistent-cut resolution, subscription modes, and Kafka.

Durable keys use a canonical reversible data-id segment and a portable
1,024-byte whole-key limit shared with S3. The fabric prefix and encoded data
id must leave room for the key category and fixed-width ordinal.

Native consumers call `hgraph::fabric::register_fabric_operators()` and link
`hgraph::fabric`. Python consumers import `hgraph_fabric`; importing the package
registers the same native operator overloads and scalar enum.
