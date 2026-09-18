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

`ComponentCheckpointStore` stores a complete eligible component image under
one immutable key. `configure_component_recovery(store, component_id,
checkpoint_key, restore_key=None, revision="1", global_state=state)` selects
the previous completed day and the next key before wiring. The runtime captures
the component before stop and publishes only after a successful bounded run
and successful teardown. Recovery quietly restores outputs, declared recorded
state, and supported keyed map membership before new input is evaluated.
Inputs must come directly from pull sources used exclusively by the component.
Each run supplies future events for its interval; recovery restores the source
endpoint baseline, while the caller remains responsible for its input cursor.

Each image is one Arrow cell holding core's canonical checkpoint image
(RFC 0039) plus envelope metadata naming the format and the predecessor. Values
use the binary value codec, so `Frame`, `Series` and non-finite floats are
ordinary state. The whole image is encoded before the native store publishes it
through its immutable object operation: a value the codec cannot represent
fails before publication, every read verifies the image checksum, and
`write(..., verify=True)` adds a decode-and-re-encode comparison. Version 1
images from hgraph 0.8.25-0.8.27 remain readable. There is
no mutable latest pointer: applications select an exact predecessor and use a
new key for every completed day. Filesystem and S3 durability follow the native
object store's guarantees; this does not coordinate external sink transactions
or external input acknowledgements.

The initial contract is deterministic simulation within a component, using
declared `RECORDABLE_STATE` for semantic user-node state. Unsupported services,
ordinary user `STATE`, captured Python closure state, schedulers, endpoint
representations, and dynamic owners are refused explicitly. Error capture is
also refused so a failed evaluation cannot be committed as a completed day.
Python callback identity includes module, qualified
name, input policy and scalar arguments. The application must change `revision`
when its strategy code changes. Explicit `__recordable_id__` values identify
Python nodes inside a configured component and must be unique there.

See [`completed_days.py`](python/examples/completed_days.py) for a runnable
example that restores day one in a new process and computes day two.
