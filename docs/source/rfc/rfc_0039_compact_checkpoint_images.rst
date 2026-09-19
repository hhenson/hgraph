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
cycle between calls by construction. "Nothing in flight" is not "nothing due":
a fresh start leaves its start-scheduled nodes waiting for a first step, the
image would hold them unevaluated, and a restored start discards bootstrap
schedules -- so they would never run. Capture therefore refuses while
``next_scheduled_time() <= evaluation_time()``. A completed cycle always leaves
the next scheduled time after it, and a restored start leaves no bootstrap, so
a worker that sat out a quiet day is still captured without a step. A graph
whose last cycle failed has no completed cut and is refused too.

The coordinator (``hgraph/runtime/graph_checkpoint_coordinator.h``) is one class
with one selection rule and four operations:

``GraphCheckpointSelection``
   ``owned_by(component)`` selects the nodes a component, or a component nested
   in it, owns; ``whole_graph()`` selects every node. A whole-graph selection
   refuses a node that carries no checkpoint identity, because an image whose
   nodes are all named ``""`` would validate against any graph.

``shape(graph)``
   Identities and contract signatures only: what an image must match. It is
   what a client hashes into its own signature (``signature(image, revision)``).

``capture(graph)``
   The owned image at ``graph.evaluation_time()``. A pending schedule beyond
   that time is refused, as RFC 0023 requires.

``restore(graph, image, start, cut)``
   Validates the whole static graph, imports endpoints, resolves reference
   locators and adapter clocks, then finalises owners -- all before the graph
   starts. Every restored timestamp must be at or before ``cut``. A failure
   detaches the whole preparation before it propagates. The image is borrowed
   and must outlive the start phase.

``complete_start()``
   Releases the preparation inventory once the root start has succeeded.

The coordinator is the ``LifecycleObserver`` that restores saved input activity
after each node's start hook. A client registers it for the start phase only.

``start_external_restored`` carries no cut of its own. The rule it needs is that
restored state lies strictly in the past of the first evaluation, so it
validates against ``start_time - MIN_TD``. A ``dmap_`` owner could not supply
anything stronger: ``NodeCheckpointOps::restore_impl`` is handed the start time
and nothing else. ``ComponentRecoverySession`` keeps the stronger check against
the cut its envelope records.

Configured component recovery stays refused on the externally driven mode: it
is a completed-day policy, and a stepped run has no completed-interval
publication boundary. The two verbs are the mechanism without the policy.

Worker graphs are wired inside a checkpoint scope so their nodes receive
identities and signatures through the same ``assign_checkpoint_identity`` path
as component members. The recipe's boundary source and sink nodes declare
checkpoint support: a source's output is an ingress baseline (a later removal
must reach a source that already holds the key); a sink is stateless.

*Every* worker graph is wired that way, whether or not anything will ever
capture it, because the caller and the worker process each wire the graph for
themselves and must arrive at the same identities without being told to. A
component scope refuses a node it cannot checkpoint, and that rule cannot apply
here: most ``dmap_`` children are not recoverable and must still wire. So the
worker scope (``Wiring::checkpoint_worker_graph``) *records* the refusal
instead -- ``NodeCheckpointIdentity::refusal`` holds the wiring diagnostic --
and the two places that need it read it:

* the coordinator refuses to capture or restore a node that carries one, with
  the recorded reason, so an unrecoverable worker fails the owner's capture;
* a ``dmap_`` owner wired inside a *component* scope walks its worker plans,
  nested child templates included, and refuses at wiring. A recoverable
  component therefore still learns at wiring that a child cannot be recovered,
  which is where a component learns everything else.

Protocol
~~~~~~~~

Two control frames join the existing bootstrap frame and ``"stop"`` sentinel.
Neither is a ``CycleRequest``:

``@hgraph-checkpoint:1``
   Caller to worker, between cycles. The worker replies with one frame: a
   status byte, then ``encode_graph_checkpoint`` of its graph (``0``) or the
   rendered error (``1``).

``@hgraph-restore:1`` followed by image bytes, in the same frame
   Caller to worker, after bootstrap and before the first cycle. The worker
   decodes, validates against its freshly wired graph and starts through
   ``start_external_restored``. It answers with an ordinary ``CycleReply``:
   ``next_scheduled_time`` from the restored schedule, or the error.

A worker therefore reads its first frame *before* it starts its graph, which
is the only change to the ordinary path: a first frame that is not a restore
starts the graph fresh and is then served as the cycle it is, and a channel
that closes without a frame still starts and stops the graph, so start and stop
hooks run exactly when they did.

A request opens with its evaluation time as eight little-endian bytes. Read
that way, ``@hgraph-`` is a time about a hundred thousand years after
``MAX_ET``, so a marker cannot be a request. The protocol test pins that
rather than leaving it to arithmetic in a comment.

After a restore the caller's ``BoundaryTransfer`` must not send its first-cycle
full image: the worker already holds the baseline, and a full image would tick
every restored value. The transfer's "first" state therefore becomes part of
the owner's restored state.

``dmap_``
~~~~~~~~~

**The expected nesting is a component inside the** ``dmap_`` **child** (ruling
2026-09-19), the same way round as ``spawn_`` below, and for the same reason:
the recoverable unit is a ``component``, and a ``dmap_`` is a way of running
one per key somewhere else.

.. code-block:: python

   @component
   def pricing(ticks: TS[float]) -> TS[float]: ...      # recovered, per key

   prices = dmap_(pricing, ticks_by_symbol)            # in no component itself

The mechanism is the one ``spawn_`` uses. The ``dmap_`` node finds the configured
component in its worker plans and stands in for it in the owner graph
(``worker_checkpoint::HostedComponentScope``): wired inside the component's
scope, with its inputs entering through component input boundaries. The worker
images are selected, ``GraphCheckpointSelection::hosted``: the component plus
the runtime's own nodes.

Two things are particular to ``dmap_``.

*The component is one level down.* Everything at the top of a ``dmap_`` worker is
the runtime's own -- the sources, the key partition, the ``map_``, the sink -- so
all of it is wired under ``@hgraph.worker.boundary`` and travels with the image,
including the ``map_``'s membership, slots and children, which is what leads to
the per-key component instances. A child template wired *from* a boundary node
is the user's again, so its nodes fall back to ``@hgraph.worker`` and only the component
inside it is selected. (The coordinator descends into a dynamic owner's children
only when the owner is selected. Here it is. A ``map_`` the *user* wrote, in a
``spawn_`` stage, is not, and a component under one is not reached -- nor
reported as hosted, so that case still fails loudly as "not wired".)

*A restored child can hold fresh nodes with work due.* What the child holds
outside the component starts fresh when its restored child graph starts, and a
fresh node may schedule itself at that moment -- a constant does. That is live
work, not history. But the coordinator discards a restored node's bootstrap
schedule, the ``map_`` that owns the child is a restored node, and so is the
``dmap_`` owner above it: left alone, the work would have been skipped
silently, against the engine's first invariant. ``NodeCheckpointOps`` therefore
gains ``live_schedule_impl``: after the restored start an owner reports the
earliest time it still has to run for live work, and the coordinator discards
the bootstrap and then honours that. ``map_`` answers from its children, the
list ``map_`` likewise, a ``dmap_`` owner from what its restored workers asked
for. This also applies to whole-worker recovery: transient sinks are excluded
from even a whole-worker image and may schedule fresh startup work.

Owners whose wakeups come entirely from children declare
``NodeCheckpointOps::schedules_children``. Their capture validates each child's
work rather than rejecting the aggregate deadline: a transient sink's pending
flush is allowed, while a selected compute node's pending event still refuses
the image. The owner must still reject incomplete local work.

**A** ``dmap_`` **wired inside a component keeps working** as stage 4 built it:
the owner is a member, and its workers are saved whole, with the empty
selection. Both nestings share every mechanism above; they differ in the
selection and in nothing else.

The owner's checkpoint state is the ordered list of per-worker image blobs
plus its output extents. Capture asks every worker before it waits for any, as
a cycle does. Restore happens before the owner starts, and the workers are
raised *in* its start, so the restored state is parked in the graph's
``GlobalState`` under the owner node's own address and consumed by the start
that follows. ``GlobalState`` owns it: a preparation that never reaches start
leaves nothing to free. Its owned output ``TSD`` is captured as an ordinary
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

**What recovers is a component inside a stage, not the stage** (ruling
2026-09-18). A stage is a graph the user wrote; the recoverable unit inside it is
the same one as anywhere else, a ``component``. Everything else in the stage is
*processed*, not recovered -- above all the sink the pipeline ends in, which acts
in a worker process and whose effect no checkpoint could replay. An earlier
draft of this section, and the first implementation, took the whole stage graph
as the unit. That dragged the sink into the contract, demanded that it declare
itself recoverable, and left a Python pipeline unrecoverable because a Python
sink had no way to say so. The unit was wrong, not the sink.

So the recovery configuration names a component, and that component is wired
*inside* a stage:

.. code-block:: python

   @component
   def pricing(ticks: TS[float]) -> TS[float]: ...      # recovered

   @graph
   def stage(ticks: TS[float]):
       publish(pricing(ticks))                          # publish: processed

   spawn_(stage, ticks)

Three things make that work.

*A worker graph names the nodes of every component in it, always.* It already
carries identities always, for the same reason: the owner and the stage's
process each wire the graph for themselves and have to agree without being told.
Naming is *all* a hosted component does. It adds no node and changes no binding,
because it has to be invisible in a graph nobody will ever capture; and it
needs no input boundary of its own, because the runtime's boundary nodes already
hold the baselines. (An earlier cut gave it the forwarding input boundary a
configured component has. Inside a ``map_`` child created mid-cycle that lost
the creation-cycle tick and the removals, with no recovery configured at all;
a test now pins that wrapping a child or a stage in a component changes
nothing.) Where a configured component would refuse to wire -- a reference in
an input, a reference escaping the output -- a hosted one records the refusal on
its nodes, as a worker scope does for a single node.

One thing a boundary used to guarantee is now checked instead. Producer scopes
outside a member's own component are recorded at wiring
(``NodeCheckpointIdentity::input_components``) and checked against the actual
selection. Selecting an enclosing component includes both it and its nested
components; selecting only an inner component does not include its parent's
producers. A dependency omitted from the selected image is refused at the
owner's wiring, naming the node and the remedy: include what computes its input
in the recovered component.

*The* ``spawn_`` *node stands in for the component in the owner graph.* At wiring
it looks for the configured component among its stages' nodes. If a stage hosts
it, the ``spawn_`` node is wired inside that component's scope, so the
completed-day session finds a member exactly where it looks for one, and
nothing else about the session changes.

Its inputs enter through component input boundaries, as any component's do, and
for the same reason. The owner graph is not recovered, so without a restored
source baseline its side of a keyed input would be empty after a restart, and
the removal of a key it never saw again could not be expressed: the worker would
hold that key for good. (The first cut of this wired the owner in a separate
"host scope" that took its inputs as they came; a keyed input with a removal
after a restart showed the hole, and the host scope is gone.) The usual rules
follow. An input has to be a direct source output, and that source may feed
nothing else; a computed input is refused at wiring. The usual remedy follows
too: wrap ``spawn_``, and whatever computes its inputs, in a component, which is
the other nesting below. The caller's contract is the component's: a run
supplies only future events.

*A stage's image is selected, not whole.* It covers the component and the
runtime's own boundary nodes -- the stage's sources and its output sink, wired
under ``@hgraph.worker.boundary``. Those hold the input baselines and the output's
observation state, they are not the user's, and restoring them is what lets a
restored pipeline skip the first-capture baselines. The checkpoint and restore
frames carry the component id; an empty id is the whole graph, which is what
``dmap_`` sends. A node outside the component starts fresh on every run,
bootstrap schedule and all.

What this does not reach: a component nested under a ``map_`` inside the stage.
The coordinator descends into a dynamic owner's children only when the owner is
selected, and that ``map_`` is not.

If ``spawn_`` is itself wired inside a recoverable component, the user has said
the pipeline is part of that component, and every stage is captured whole as a
member's children are. The same mechanism, with the empty selection.

*Sinks* (ruling 2026-09-19). A sink may sit inside a component, and what
recovery does with it depends on one thing:

* **With recordable state it is recovered**, through that state: it is a member,
  and its recordable state and input observation state are in the image.
* **Without, it is transient**: inside the scope, outside the image *and* the
  contract. It starts fresh on every run; it has no id, so adding, removing or
  changing one never refuses a checkpoint; and whatever it holds -- ordinary
  state, a scheduler, the clock -- is its own business.

This is safe for a sink and for nothing else, because a sink has no output:
nothing inside the recovered graph can observe what it forgot. A compute node
that lost its ``State`` would change values downstream. Recordable state is how
a sink's author marks what must survive, and no other declaration exists or is
needed -- which is also why no Python API for one was added. RFC 0023 refused a
sink outright, to make its author acknowledge that recovery does not replay an
effect; that acknowledgement is now implicit in the rule.

*A sink's schedule is its own, in both cases.* A pending alarm does not block a
capture, and a restored sink keeps the schedule its start hook set. Both are
things the coordinator does to every other restored node, and either would
break a periodic flush: it would never re-arm. A sink re-evaluating cannot
disturb the graph, so there is nothing to protect.

A sink that declares ``NodeCheckpointOps`` -- a boundary sink, or an owner of
worker graphs, which is a sink by kind -- is what its operations say.

The mechanics below are unchanged by any of this.

RFC 0038 requires "a consistent frontier fence, graph checkpoints and channel
cursor recovery". At the completed-day boundary the first is already true and
the third is vacuous: the executor settles every stage (``next_time(true)``)
before it concludes, so at capture every stage has completed the frontier and
every channel is empty. The owner's capture therefore *asserts* quiescence
(every stage ready, idle and at the frontier; no pending or queued frame) and
refuses the image otherwise, then collects one image per stage in pipeline
order. An online (mid-run) spawn checkpoint needs a real fence and channel
cursors and stays out of scope, as RFC 0023's online snapshot does.

Three details the implementation settled:

* **Who asks.** A stage's transport thread owns its channel, so the owner does
  not talk to a worker itself. It marks each stage, and the stage's thread --
  idle, because the owner asserted quiescence first -- sends
  ``@hgraph-checkpoint:1`` and hands the image back. A stage that cannot
  capture reports why and carries on; the refusal fails the owner's capture.
* **How a stage starts.** A ``spawn_`` stage answers its start before any cycle,
  so unlike a ``dmap_`` worker it cannot wait to see whether its first frame is
  a restore. The owner says which, straight after the boundary identity:
  ``@hgraph-start:1`` or a restore frame.
* **What else is restored.** Not the stages' requested next times, which an
  earlier draft listed. An image never holds a pending schedule (RFC 0023), so
  each is "nothing", and the stage reports its own in its start reply anyway.
  What *is* restored beside the images is the fact of restoration: a fresh
  pipeline sends every input in full on its first capture, and a restored one
  already holds those baselines. Re-sending them would tick inputs that did
  not tick. This is the "first" state the protocol section refers to; for
  ``spawn_`` it is real, and a test with a once-ticking side input pins it.

The contract signature is the selected nodes of every stage, in order: the
hosted component's, or all of them when ``spawn_`` is a member. Stage bootstraps
are left out on purpose -- they carry configuration, such as paths, that a
restart is free to change -- and so, in the hosted form, is everything outside
the component, which a restart is equally free to change.

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
   *Implemented.*
4. **``dmap_`` recovery**, in-process then process hosting. *Implemented*, for
   both the prepared and the typed ``dmap_`` forms. Building it found one
   defect in the coordinator that predates it: a restored node's bootstrap
   schedule was discarded from the graph's slot but not from the node's own
   ``NodeScheduler`` state, so after a quiet first cycle the alarm was re-armed
   in the past. ``dmap_`` was simply the first recoverable node to use a
   ``NodeScheduler``. The "first-cycle full image" concern above did not
   materialise. A transfer has no "first" state of its own: it sends a full
   image when its input reports a sampled rebind, and a restored owner's input
   does not. The acceptance tests would show it if it did -- every child
   accumulates, so a baseline re-ticked into a worker changes the totals of
   keys the cycle never touched.
5. **``spawn`` recovery** at the completed boundary. *Implemented.*
6. **A component inside the worker**, for both owners: hosted selection, the
   owner standing in for the component, transient sinks, live schedules.
   *Implemented.*

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

Recovery review corrections
---------------------------

Internal worker scopes use the reserved ``@hgraph.`` namespace. A user component
cannot claim that namespace, and recovery configuration cannot select it.
Ordinary names such as ``worker`` and ``worker.boundary`` remain valid user
component names, but must refer to a component that was actually wired.

Child input and output binding signatures name checkpoint identities rather
than physical node positions. Input bindings to transient sinks are excluded,
so inserting one cannot renumber the recoverable contract. The same encoding
is used by dictionary and list maps, meshes, and both reduction strategies.
These signature corrections fail closed against images with the previous
encoding; an incompatible image must be regenerated rather than imported.
