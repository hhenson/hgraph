# hgraph-persistence

Durable record/replay and frame storage for hgraph (RFC 0025): the
FrameStore (memory / local filesystem / S3, Arrow IPC / Parquet), the
frame-backed record/replay/compare overloads for the core operator
markers, and the DataFrameStorage compatibility surface.

The public C++ `ObjectStore` is the durable metadata substrate shared by
checkpoint/recovery and the versioned dataflow fabric. It provides:

- immutable create-if-absent with distinct created, idempotent, and conflict
  outcomes;
- typed absent reads carrying an opaque version token when present, with
  backend failures reported separately as `ObjectStoreError`;
- deterministic lexicographic prefix paging;
- compare/exchange for small named references; and
- memory, local-filesystem, and S3 strategies behind one owning erased handle.

Local immutable publication uses atomic filesystem creation and reference CAS
uses cross-process file locking plus atomic replacement. S3 conditional writes
are sent with `If-None-Match` / `If-Match` and AWS Signature V4 through
libcurl; they are not emulated with a read followed by an unconditional write.
`FrameStore` immutable local and S3 writes use this same backend publication
path after serialising the Frame. This requires one additional
serialized-Frame-sized buffer for an immutable local or S3 write; mutable
FrameStore writes retain their streaming path.

Wheel builds embed a pinned private curl for conditional S3 requests. A shared
`hgraph::persistence` library carries that implementation itself; consumers of
a static installed C++ library must provide `CURL::libcurl` version 7.75 or
newer at the final link.

Installing and importing `hgraph_persistence` registers the
`"hgraph.persistence.frame"` backend with the shared hgraph runtime;
selecting that backend (`set_record_replay_config`) activates it from
unchanged `hgraph` imports.

Runnable Python examples for direct record/replay, transparent component
modes, and keyed time-series storage are in
[`python/examples`](python/examples/README.md).
