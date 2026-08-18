# hgraph-persistence

Durable record/replay and frame storage for hgraph (RFC 0025): the
FrameStore (memory / local filesystem / S3, Arrow IPC / Parquet), the
frame-backed record/replay/compare overloads for the core operator
markers, and the DataFrameStorage compatibility surface.

Installing and importing `hgraph_persistence` registers the
`"hgraph.persistence.frame"` backend with the shared hgraph runtime;
selecting that backend (`set_record_replay_model`) activates it from
unchanged `hgraph` imports.
