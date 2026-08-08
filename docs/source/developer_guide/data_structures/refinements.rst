Refinement Topics
=================

This page collects forward-looking topics that the early structural
design deliberately leaves open. Resolving them belongs to the next
refinement pass, not the initial implementation.

Memory Layout Refinement Topics
-------------------------------

The next refinement pass should decide the physical layout for each structure:

- whether optional node schedulers are colocated with nodes or held in a side table,
- how input and output arrays are laid out for fast readiness checks,
- how subscriber lists avoid invalidation during graph construction and dynamic graph changes,
- how time-series child structures maintain stable identity while supporting sparse dictionaries and dynamic lists,
- which value containers need stable slots, tombstones, or compacting storage,
- where Python bridge objects are attached without infecting C++ core structures.

Memory Management Topics
------------------------

Open questions inherited from the runtime memory-management notes:

- allocator strategy (single arena per graph, pool per shape, plain heap),
- arena or pool ownership for graph instances,
- stable handles versus raw pointers for cross-graph references,
- intrusive versus external reference counting,
- Python object ownership at bridge boundaries,
- teardown order across graph / node / time-series,
- observer and trace lifetimes.

Schema Management Topics
------------------------

Open questions inherited from the schema-management notes:

- canonical Python type metadata conversion (Python type → schema),
- C++ template integration for typed wrappers around erased schemas,
- error messages and diagnostics for failed schema resolution,
- serialisation or debug representation of schemas across runs.

Node and graph schemas are covered in *Schemas > Node Schemas* and
*Schemas > Graph Schemas*; several details on those pages are still in
flight and will be filled in alongside the implementation.

Open Questions
--------------

- Should graph storage be a single arena per graph instance, or a set of stable typed arenas?
- Should node-local scheduler storage be inline in a node allocation when present, or stored in a scheduler side table keyed by node index?
- Should graph schedule entries carry only time, or time plus debug/status flags for trace tooling?
- Which time-series containers require stable child addresses across mutation?
- Which structures must survive graph hot-swap or dynamic subgraph extension?
