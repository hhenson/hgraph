# hgraph-fabric

`hgraph-fabric` is the C++-first versioned dataflow extension specified by
[RFC 0026](../../docs/source/rfc/rfc_0026_versioned_dataflow_fabric.rst).
It decouples recurring graph components through immutable, complete `Frame`
versions and contiguous lineage revisions.

The current checkpoint establishes the installed C++/Python contract,
canonical metadata encoding, run-scoped configuration, and broker-free memory
notifier. Later RFC checkpoints add durable publication, dependency planning,
consistent-cut resolution, subscription modes, and Kafka notification.

Native consumers call `hgraph::fabric::register_fabric_operators()` and link
`hgraph::fabric`. Python consumers import `hgraph_fabric`; importing the package
registers the same native operator overloads and scalar enum.
