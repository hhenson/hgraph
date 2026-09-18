RFC 0039: Compact Checkpoint Images and Recovery Across Execution Boundaries
============================================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-18
:Target: Checkpoint image encoding, ``Frame`` state, and recovery of graphs hosted by ``dmap_`` and ``spawn``
:Related: RFC 0017 (binary value codec), RFC 0023 (checkpoint recovery),
          RFC 0025 (hgraph-persistence), RFC 0037 (distributed map),
          RFC 0038 (spawn pipelines)

Summary
-------

The completed-day component checkpoint of :doc:`rfc_0023_graph_checkpoint_recovery`
is correct but has three defects that block its use for real strategies:

* it cannot store a ``Frame`` (or ``Series``) value anywhere in a component;
* it refuses any component containing ``dmap_`` or ``spawn``, so state held in a
  worker-hosted graph -- including ``map_``/``reduce``/``mesh_`` nested inside
  one -- cannot be recovered; and
* its durable image is an order of magnitude larger and two orders slower to
  write than a hand-written record of the same state.

This RFC replaces the image *encoding*, not the image *model*. The owned
in-memory images, the representation-owned ``TSCheckpointOps`` /
``NodeCheckpointOps`` contracts, quiet import and the restore phases are
unchanged. A canonical binary image codec moves into core beside the RFC 0017
value codec, because the same bytes are the durable form **and** the form a
worker process returns to its caller. ``hgraph-persistence`` keeps the durable
envelope, the store, publication and selection.

Evidence
--------

``extensions/persistence/tests/test_checkpoint_benchmark.cpp``
(``hgraph_persistence_tests '[checkpoint-benchmark]'``) records one
``TSD[int, TS[float]]`` endpoint through the generic path and through a
hand-written record of the same keys, values and modification times. Release
build, Apple Clang, arm64, image format version 1:

.. list-table::
   :header-rows: 1
   :widths: 12 14 12 12 14 14 12

   * - Keys
     - Encoded bytes
     - Bytes/key
     - Capture (ms)
     - Encode+store (ms)
     - Read+decode (ms)
     - Restore (ms)
   * - 1,000
     - 257,325
     - 257
     - 0.07
     - 3.2
     - 1.4
     - 0.13
   * - 10,000
     - 2,638,205
     - 264
     - 0.76
     - 30.5
     - 14.4
     - 1.3
   * - 100,000
     - 26,315,709
     - 263
     - 7.1
     - 306
     - 144
     - 14.6

The hand-written record of the 100,000-key endpoint is 2,400,008 bytes
(24 bytes/key), written in 2.2 ms and recovered through the ordinary mutation
API in 8.3 ms. The generic path is therefore 11x larger, about 140x slower to
record and about 19x slower to recover.

The split matters. Owned capture and quiet import are within 2-3x of the
hand-written form. The durable codec is 95% of the cost, for four reasons that
are all properties of the version 1 encoding rather than of the image model:

* every endpoint image, **including every child of a collection**, writes its
  schema name, its full manifest descriptor and a reconstruction recipe, and
  every key and payload ``Value`` writes its value schema the same way;
* decode resolves each of those through the locked type registry and
  regenerates the manifest descriptor to compare it;
* values are JSON text and every integer, count and flag is eight bytes; and
* ``write`` encodes, fully decodes, and deep-compares the image before
  publishing it.

Every child graph of a ``map_`` additionally repeats the identifier and the
complete contract signature of every node in the (identical) child template.

Why ``Frame`` fails today
-------------------------

Three independent refusals, any one of which is sufficient:

1. the JSON value codec has no ``Frame`` or ``Series`` form;
2. the schema-recipe encoder accepts only named schemas and
   list/set/map/tuple/bundle recipes, so an interned ``Frame[Row]`` or
   ``Series[T]`` is "unregistered value schema cannot be reconstructed"; and
3. the write-time round-trip check compares values with ``==``, and ``Frame``
   equality is handle identity (``types/frame.h``), so a decoded frame never
   equals its source.

The RFC 0017 binary codec already encodes ``Frame`` and ``Series`` as Arrow
IPC; ``dmap_`` and ``spawn`` depend on it for exactly that. The endpoint
representation needs no change: ``TS[Frame]`` is an ordinary atomic
time series whose owned image copies the shared-immutable handle.

Ownership revision
------------------

:doc:`rfc_0025_hgraph_persistence` moved "the durable checkpoint and
input-journal formats" to the extension. That is revised as follows.

Core owns the **canonical image encoding**: the byte form of a
``GraphCheckpointImage`` and of a ``ComponentCheckpoint``. It is a sibling of
the RFC 0017 value codec and has the same justification -- it is a
cross-process transfer form used by core runtime nodes (``dmap_``, ``spawn``),
which may not depend on an extension. It has no storage, publication or
discovery semantics.

``hgraph-persistence`` continues to own the durable envelope (the object that
is published), the ``ComponentCheckpointStore`` contract, immutable
publication, predecessor selection, retention, and the Python store adapter.
Its envelope carries core's bytes opaquely.

Core's header is ``hgraph/runtime/checkpoint_codec.h``:

.. code-block:: cpp

   void encode_component_checkpoint(const ComponentCheckpoint &, std::string &out);
   ComponentCheckpoint decode_component_checkpoint(std::string_view bytes);
   void encode_graph_checkpoint(const GraphCheckpointImage &, std::string &out,
                                DateTime base_time = MIN_DT);
   GraphCheckpointImage decode_graph_checkpoint(std::string_view bytes);
   bool is_checkpoint_image(std::string_view bytes) noexcept;

These are cold-path functions. They are never reachable from evaluation.

Image encoding, version 2
-------------------------

Conventions are those of RFC 0017: little-endian, packed, LEB128 varints for
counts, indices, flags and slots; numeric and temporal *values* fixed width.
Decode is bounded by ``BinaryDecodeLimits`` with a work budget proportional to
the input length, and every count is charged before it allocates.

Layout
~~~~~~

.. code-block:: text

   text      "hgraph.checkpoint-image"
   varint    2                                  format version
   varint    kind                               0 graph image, 1 component image
   i64       base time                          times below are relative to it
   [component image: id, graph signature, completed-until; its cut is the base time]
   varint    body length
   tables:   strings, value schemas, time-series schemas
   body:     graph image
   u64       checksum of every preceding byte

The checksum is FNV-1a 64 taken over little-endian 64-bit words, with the
trailing bytes folded in singly. It is word-wise so that verifying an image is
a small fraction of decoding it, and it is specified here because the value is
durable. It is an integrity check, not an authenticator.

Tables follow the body in construction order but precede it in the stream, so
a decoder resolves every table entry exactly once before reading any image.
The encoder builds the body into a second buffer while interning table
entries, then emits tables and body; nothing is walked twice.

Schemas once
~~~~~~~~~~~~

Each distinct value schema and time-series schema is written **once**, as the
version 1 recipe (name, manifest descriptor, named flag, structural recipe
referring to earlier table indices). The descriptor comparison that detects a
changed schema is therefore performed once per distinct schema rather than
once per image. Two recipes are added: ``Frame`` (row schema and optional
metadata schema) and ``Series`` (element schema), reconstructed through
``TypeRegistry::frame`` and ``TypeRegistry::series``.

The table is ``hgraph/types/metadata/schema_table.h``. It belongs to the type
layer, not to the runtime codec that uses it: a recipe is schema-kind
knowledge, and runtime code does not probe ``TSTypeKind`` (the
``runtime-ref-kind-probes`` ratchet). For the same reason the rule that
reference metadata sits only on a ``REF`` endpoint is one of the image
validators in ``ts_data/checkpoint.h``, which the codec calls.

A root endpoint image names its schema by table index. A child image does not
name a schema at all when it is the one its parent implies -- the field type of
a ``TSB``, the element type of a ``TSL`` or ``TSD`` -- which is every child the
built-in representations produce. An explicit index remains available behind a
flag so a representation is never forced into the implied form.

The same rule applies to values: a payload whose schema is the endpoint's
value type, and a key whose schema is the collection's key type, write no
schema reference. Keys of one collection are written consecutively through one
resolved converter.

Values
~~~~~~

Values use the RFC 0017 binary codec through one ``BoundBinaryConverter`` per
table entry. This is what admits ``Frame``, ``Series``, non-finite floats,
``Bytes`` and the temporal types, none of which the JSON form could carry
losslessly.

Endpoint images
~~~~~~~~~~~~~~~

One varint flag word per image selects which parts follow. The common leaf --
a valid atomic time series with a payload of its declared type -- is one flag
byte, one time byte and the value.

* **Times** are zig-zag varint offsets from the enclosing image's
  modification time (from the base time at a root). A child last modified with
  its parent costs one byte.
* **Slots.** A keyed image whose live slots are exactly ``0..n-1`` with no free
  slots sets a *dense* flag and writes no slot, free-slot or capacity data.
  Otherwise capacity, ascending slot deltas and the free stack are written; the
  free stack is verbatim because its order is semantic
  (:doc:`rfc_0023_graph_checkpoint_recovery`, chosen-slot key import).
* **Published plane** is elided when every entry is published, else bit-packed.
* **Child count** is always written; structure remains validated by the owning
  representation on import, never trusted from the stream.

Graph images
~~~~~~~~~~~~

Node identifiers and contract signatures are string-table references, so the
``n`` children of a ``map_`` cost two small varints per child node instead of
``n`` copies of the template's signatures. One flag byte per node records which
optional endpoints and owner state follow. Sibling child keys share one value
schema reference and are written consecutively.

Compatibility
~~~~~~~~~~~~~

Version 1 images were published by hgraph 0.8.25-0.8.27 and remain **readable**.
The extension recognises the version 1 envelope and decodes it with the
retained version 1 reader; it writes only version 2. No migration is needed: a
day recovered from a version 1 predecessor publishes a version 2 successor.
An unknown version is refused, as before.

**Version 3** (RFC 0040) keeps this layout and adds, after the fixed header and
the component header, the binary profile of the image's values and that
profile's revision, and then holds everything that follows -- body length,
tables and body -- as one RFC 0040 compression block. A stored image is
``Compact`` and compressed; one exchanged with a worker-hosted graph is
``Fast`` and never compressed (``CheckpointImageOptions::stored()`` /
``transport()``). The checksum covers the compressed bytes, so damage is found
before a claimed length is trusted, and the component header stays readable
without decompressing anything. A collection's keys, which share one schema,
are written as a run, so under ``Compact`` sorted keys cost their steps rather
than their values. Version 2 images remain readable: no profile, no revision,
no block, values as ``Compact`` revision 0; real version 2 bytes are pinned in
``tests/cpp/checkpoint_v2_fixture.h``.

Publication safety
------------------

Version 1 decoded and deep-compared every image before publishing because its
value codec could refuse or lose information after a schema had been accepted.
Version 2 replaces that with:

* **fail-closed encoding** -- a value the binary codec cannot represent
  (opaque Python storage, an extended-width atomic without a portable form)
  throws during ``encode``, before anything is published, exactly as an
  unsupported endpoint already refuses capture; and
* **an integrity checksum** verified on every read, which the round trip never
  provided.

``ComponentCheckpointStore::write`` accepts ``verify = true`` to additionally
decode the encoded bytes and require that re-encoding them reproduces the same
bytes. Byte equality of a canonical encoding is the only comparison that is
meaningful for handle-identity values such as ``Frame``. It is off by default:
it doubles the cost of a write to guard against a codec defect that the codec's
own conformance suite is responsible for.

Recovery of worker-hosted graphs
--------------------------------

``dmap_`` and ``spawn`` evaluate complete graphs in other processes. Both are
refused inside a recoverable component today because their owning nodes carry
ordinary ``State``, a scheduler and (for ``spawn``) sink kind, and declare no
``NodeCheckpointOps``; and because nothing can carry a graph image across the
worker channel. The byte form above removes the second obstacle. The first is
resolved by treating each as what it is: a **dynamic-graph owner** under
RFC 0023, whose children happen to live elsewhere.

Whole-graph coordinator
~~~~~~~~~~~~~~~~~~~~~~~

``ComponentRecoverySession`` fuses two things: the graph-image mechanics
(capture, validation, prepare/fix-up/finalise restore, reference locators) and
the completed-day policy (component selection, ``GlobalState`` configuration,
simulation-only, publish-after-stop). The mechanics are extracted as
``GraphCheckpointCoordinator``; the session becomes its completed-day client.

A worker graph is a second client. Its selection predicate is "every node";
its boundary sources are its ingress baselines; and it is driven by two
executor verbs on the externally driven mode, beside ``start_external`` /
``step`` / ``stop_external``:

.. code-block:: cpp

   void start_external_restored(DateTime start_time, const GraphCheckpointImage &image);
   GraphCheckpointImage capture_external() const;   // between steps only

``capture_external`` is legal only at a completed step boundary, which is the
RFC 0023 consistency cut: the externally driven executor has no in-flight
cycle between calls by construction.

Worker graphs are wired inside a checkpoint scope so their nodes receive
identities and signatures through the same ``assign_checkpoint_identity`` path
as component members. The recipe's boundary source and sink nodes declare
checkpoint support: a source's output is an ingress baseline (a later removal
must reach a source that already holds the key); a sink is stateless.

Protocol
~~~~~~~~

Two control frames join the existing bootstrap frame and ``"stop"`` sentinel.
Neither is a ``CycleRequest``:

``@hgraph-checkpoint:1``
   Caller to worker, between cycles. The worker replies with one frame holding
   ``encode_graph_checkpoint`` of its graph, or a ``CycleReply`` error.

``@hgraph-restore:1`` followed by image bytes
   Caller to worker, after bootstrap and before the first cycle. The worker
   decodes, validates against its freshly wired graph and starts through
   ``start_external_restored``. A worker started this way reports
   ``next_scheduled_time`` from the restored schedule.

After a restore the caller's ``BoundaryTransfer`` must not send its first-cycle
full image: the worker already holds the baseline, and a full image would tick
every restored value. The transfer's "first" state therefore becomes part of
the owner's restored state.

``dmap_``
~~~~~~~~~

The owner's checkpoint state is the ordered list of per-worker image blobs
plus its output extents. Its owned output ``TSD`` is captured as an ordinary
output. Key placement is ``hash % workers`` and is not stored; the worker count
and hosting mode are part of the owner's contract signature, so a changed
worker count is an incompatible image rather than a silent re-partition.
In-process hosting uses the same images without the frames, which keeps the
RFC 0037 attribution property: if the two modes disagree after a restore the
fault is in the transport.

The per-key children need nothing new. Each worker hosts an ordinary ``map_``,
whose existing checkpoint operations already capture membership, slots and
child graphs.

``spawn``
~~~~~~~~~

RFC 0038 requires "a consistent frontier fence, graph checkpoints and channel
cursor recovery". At the completed-day boundary the first is already true and
the third is vacuous: the executor settles every stage (``next_time(true)``)
before it concludes, so at capture every stage has completed the frontier and
every channel is empty. The owner's capture therefore *asserts* quiescence
(every stage ready, idle and at the frontier; no pending or queued frame) and
refuses the image otherwise, then collects one image per stage in pipeline
order. Its restored state is the per-stage blobs plus each stage's requested
next time. An online (mid-run) spawn checkpoint needs a real fence and channel
cursors and stays out of scope, as RFC 0023's online snapshot does.

A spawned pipeline ends in a sink that acts in a worker process. RFC 0023's
external-effect rule applies unchanged: the effect is outside the recoverable
contract and is not made exactly-once by recovery.

Failure
~~~~~~~

A worker that cannot capture fails the owner's capture, so no completed day is
published. A worker that refuses an image fails start. Neither falls back to a
fresh worker: a partially restored pipeline is never started.

Result
------

The same benchmark after stages 1 and 2, 100,000 keys:

.. list-table::
   :header-rows: 1
   :widths: 30 18 18 18

   * -
     - Version 1
     - This RFC
     - Hand-written
   * - Encoded bytes
     - 26,315,709
     - 1,931,274
     - 2,400,008
   * - Bytes per key
     - 263
     - 19.3
     - 24
   * - Record: capture + encode and store (ms)
     - 7.1 + 306
     - 5.1 + 2.6
     - 2.2
   * - Recover: read and decode + restore (ms)
     - 144 + 14.6
     - 7.4 + 8.1
     - 8.6

The image is smaller than the hand-written record. Recording went from about
140x the hand-written cost to 3.4x, and recovery from 19x to 1.8x, while still
preserving what the hand-written form discards: slot identity, free-stack
order, the published plane and the key set's independent clock. A count window
of 4,096 integers went from 56,805 bytes to 37,074, which is eight bytes of
value and one of time per sample.

The owned image is now the larger half of both directions. ``TSCheckpointImage``
is 360 bytes and ``NodeCheckpointImage`` 1,640, per leaf and per child-graph
node respectively. That is the argument for the streaming alternative below,
and the numbers above are the bar it would have to clear.

Stages
------

1. **Image codec.** Core codec and version 2, ``Frame``/``Series`` recipes,
   extension envelope, version 1 reader retained, publication-safety change,
   benchmark and size/speed acceptance. *Implemented.*
2. **Owned-image hot paths.** *Implemented in part:* keyed validation claims
   slots in one bitmap pass and detects duplicate keys through a flat set of
   borrowed keys; keyed capture sizes its image once; the dictionary validates
   every child against one prototype; and the coordinator's endpoint position
   index is built only when a reference locator or adapter inventory consults
   it. Profiling this stage also found that tearing down a large keyed
   collection was quadratic -- the ownership projection's ``child_at`` searched
   for the *n*-th occupied slot and every lifecycle traversal called it for each
   ordinal. That is fixed in the projection itself
   (``developer_guide/data_structures/plans_and_ops/time_series.rst``) and is
   independent of checkpointing. Columnar leaf images remain open.
3. **Whole-graph coordinator** and the two externally driven executor verbs.
4. **``dmap_`` recovery**, in-process then process hosting.
5. **``spawn`` recovery** at the completed boundary.

Acceptance
----------

Stage 1:

* a component holding ``TS[Frame]``, typed ``TS[Frame[Row]]``, a frame inside a
  bundle and a frame in recordable state restarts from a durable store with the
  table contents, Arrow schema and hgraph frame metadata intact;
* non-finite floats recover bit-exactly;
* every existing native and Python checkpoint test passes unchanged against
  version 2, and a stored version 1 fixture still loads;
* truncated, over-long, bad-checksum, bad-table-index and count-overflow images
  are refused without unbounded allocation; and
* for the benchmark endpoint, the image is smaller than the hand-written
  record, and encode and decode are each within 3x of the hand-written record
  and recover.

Stages 4 and 5: for every supported graph an uninterrupted run and a run
restarted at each completed-day boundary emit identical output deltas, in both
hosting modes, including membership churn across the restart and a ``map_``,
``reduce`` and ``mesh_`` nested inside the worker graph.

Alternatives considered
-----------------------

Stream images straight from the representation (no owned image)
   The fastest end state: ``TSCheckpointOps`` would write to and read from a
   byte cursor. Deferred, not rejected. It rewrites seven representations, six
   dynamic owners, the coordinator and twelve test suites to remove a cost that
   measurement puts at 2-3x of hand-written, while the codec costs 100x. It
   remains available after stage 2 if the owned image is then the bottleneck.

Keep the codec in the extension and have workers link it
   Rejected. ``dmap_`` and ``spawn`` are core; a core node cannot require an
   extension to be recoverable.

Drop schemas from the image entirely
   The node signatures already pin every endpoint schema, so a decoder driven
   by the live graph needs none. Rejected for now because decoding would have
   to move inside restore, against live endpoints, which removes the owned
   decoded image that validation-before-import depends on. The schema table
   costs a few hundred bytes per image.

Columnar Arrow encoding of the whole image
   Attractive for keyed leaves, poor for the recursive, heterogeneous majority
   of an image, and it would make core's transfer form depend on Arrow layout
   choices. The binary codec already stores homogeneous keys consecutively.

Unresolved questions
--------------------

* Whether stage 2 should introduce columnar leaf images now or wait for a
  consumer whose capture time, rather than encode time, dominates.
* Whether a changed ``dmap_`` worker count should become a supported
  re-partitioning restore rather than an incompatible image.
* Whether the durable envelope should move from a one-cell Arrow table to the
  extension's ``ObjectStore``, which would change stored object layout.

References
----------

* :doc:`rfc_0017_binary_value_codec`
* :doc:`rfc_0023_graph_checkpoint_recovery`
* :doc:`rfc_0025_hgraph_persistence`
* :doc:`rfc_0037_distributed_map`
* :doc:`rfc_0038_spawn_pipelines`
* :doc:`../user_guide/component_recovery`
