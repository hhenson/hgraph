RFC 0023: Durable Graph Checkpoints and Deterministic Input Replay
==================================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-16
:Target: Running-graph snapshots, input journals, and bounded-cost recovery

Summary
-------

Define how a checkpointable deterministic hgraph execution is captured and
restored.  A checkpoint is a logically complete image of the graph at a
successful root evaluation-cycle boundary: owned node outputs, hidden error
outputs, recordable state, semantic schedules, dynamic graph topology, stable
slots, and graph-internal reference bindings.  Between checkpoints, a durable
input journal records the canonical deltas actually admitted at every external
graph boundary.

Recovery rebuilds and validates the wired graph through
:doc:`rfc_0022_serializable_graph_manifest`, restores the latest compatible
complete checkpoint without producing ticks, replays journal entries after the
checkpoint watermark, and then hands each source over to live delivery at an
explicit no-loss/no-duplication boundary.

The logical model is a full checkpoint plus input deltas.  Physical storage may
reuse unchanged, content-addressed state chunks so checkpoint cost tracks
changed state rather than rewriting the whole graph every time.  Internal node
output deltas are not required for recovery; they may be recorded separately as
an audit oracle.

Motivation
----------

Record/replay currently operates at explicitly wrapped component boundaries.
It can record component inputs and outputs, replace inputs during replay, and
seed a recovering pass-through at graph start.  That is useful for backtests
and local recovery but does not preserve a running graph's internal state.

RFC 0017 defines value images, canonical time-series deltas, and the familiar
snapshot-plus-delta log shape.  It explicitly defers restoration of a running
graph.  RFC 0019 provides immutable segmented recordings and an atomic complete
marker for one run.  RFC 0021 sketches continued recording and version ancestry.
None defines the consistency cut, graph identity, state inventory, lifecycle,
dynamic graph reconstruction, source watermark, or external-effect semantics
needed to join those pieces.

Without a graph checkpoint, recovering a long-lived deterministic execution
requires replaying its entire input history.  That is correct but recovery time
grows with the age of the run.  Periodic checkpoints bound the replay interval:

.. code-block:: text

   input image -- deltas -- checkpoint -- deltas -- checkpoint -- deltas
                                      ^ restore here and replay only this tail

The capability is also the missing engine half of connector guarantees.  A
Kafka sink cannot claim exactly-once consume/process/produce until consumed
offsets, graph state, and produced effects share a checkpoint/transaction
boundary.

Ownership boundary
------------------

.. note::
   **Ownership revised by** :doc:`rfc_0025_hgraph_persistence`
   (2026-08-17): core keeps the capture/restore *mechanics* below
   (eligibility, consistency boundary, restore lifecycle, type-erased
   checkpoint operations, traversal and identity, replay suppression);
   the durable checkpoint and input-journal *formats*, the durable
   checkpoint-store contract, publication, recovery selection, and
   retention move to the ``hgraph-persistence`` extension.

Core owns:

* checkpoint eligibility and deterministic-run declarations;
* the graph-wide consistency boundary and restore lifecycle;
* type-erased checkpoint operations for time-series data and special nodes;
* checkpoint and input-journal formats;
* graph/runtime traversal and stable dynamic-instance identity;
* the durable checkpoint-store contract;
* replay suppression and idempotency capability declarations; and
* C++ execution, with Python as an authoring and compatibility surface.

Concrete external systems own their cursor, acknowledgement, transaction, and
live-handoff implementations.  An extension plugs those behaviours into the
core checkpoint contract; core does not acquire Kafka, database, filesystem,
or cloud-service semantics.

This RFC does not serialize executable graph code.  The application rebuilds
the graph and bindings, then the manifest contract verifies them before state
is imported.

Terminology
-----------

``checkpoint cut``
   A successful, completed root evaluation-cycle boundary after node
   evaluation, nested-graph work, observers, deferred structural cleanup, and
   executor after-evaluation notifications have completed.

``checkpoint image``
   The complete logical semantic state at one cut.  It is richer than an RFC
   0017 stream image because exact runtime recovery also requires timestamps,
   slot lifecycle, schedules, references, and dynamic topology.

``boundary input``
   A source value admitted from outside the deterministic graph contract.  A
   replay clock or deterministic in-graph source is not automatically a
   boundary input; an adaptor, service response, user push, or undeclared host
   observation is.

``input journal``
   The ordered, durable sequence of canonical boundary-input events as observed
   by the graph.

``input watermark``
   The greatest journal sequence whose effect is included in a checkpoint.

``non-binding run contract``
   The run-manifest fields other than the concrete ``BindingManifest`` values:
   graph identity, executor semantics, logical bounds, seeds, and other
   reproducibility inputs.  These fields remain exact even when a checkpoint
   permits contract-compatible resource substitution.

``semantic state``
   State which changes future graph outputs and cannot be reconstructed from
   checkpointed values and immutable configuration alone.

``derived state``
   Caches, subscriptions, heap indices, alarms, and other runtime machinery
   reconstructed deterministically from semantic state during restore.

Checkpoint eligibility
----------------------

A graph is checkpointable only when every participating graph, node, value
strategy, source, and sink supplies the required capability.  Eligibility is
computed at wiring and recorded in the graph manifest.  Refusal is conservative
and path-addressed.

A checkpointable deterministic run must satisfy all of:

* every external input is journalled or has an equivalent durable source cursor
  with a declared deterministic replay contract;
* every persistent node-local semantic state is a ``RecordableState`` or is
  handled by explicit node checkpoint operations;
* ordinary ``State<T>`` is derived/transient, or the node is rejected;
* host clock, randomness, locale, calendar, filesystem, environment, and
  network observations are absent or declared as reproducibility inputs;
* every time-series representation reachable from an owned endpoint provides
  exact checkpoint capture/import operations;
* every dynamic graph owner can capture and restore its live slot topology;
* every external sink declares replay suppression, idempotency, or transactional
  checkpoint coordination; and
* the graph, extension, and non-binding run contract are serializable and
  exact, and every binding declares whether checkpoint attachment requires
  ``Contract`` or ``Reproducible`` compatibility from RFC 0022.

Determinism is a contract, not something core can prove for arbitrary user
code.  A node or extension which reads an undeclared process resource may still
violate it; the explicit capability makes that defect attributable rather than
silently weakening recovery for every graph.

State inventory
---------------

Endpoint state
~~~~~~~~~~~~~~

For each node the generic walker captures every **owned** endpoint:

* the ordinary output;
* the hidden error output, when present; and
* the hidden recordable-state output, when present.

Input endpoints are bindings onto outputs and are not duplicated as values.
Forwarding outputs and mapped child terminals may alias parent-owned storage;
the ownership graph is captured separately so each physical/semantic owner is
imaged once.  Restoring the same aliased value through several mutation paths
would incorrectly emit notifications and can bind a forwarding endpoint to the
wrong owner.

An exact endpoint checkpoint includes:

* current validity and every partially valid structural child;
* current scalar/structural values;
* original per-node or per-child modification times required by semantics;
* TSD/TSS slot capacity, live/constructed/pending-erase planes, keys, holes, and
  free-list ordering;
* dynamic TSL shape;
* TSW retained values and their original evaluation timestamps; and
* representation-specific semantic metadata declared by its checkpoint ops.

Per-cycle delta buffers, observer lists, mutation guards, borrowed cursors, and
cached Python wrappers are not persisted.  A cut occurs only after all work for
that cycle has observed the deltas.  The next recovered evaluation is strictly
after the checkpoint cut unless the restore is deliberately followed by
journal replay at a later recorded cycle.

Recordable and local state
~~~~~~~~~~~~~~~~~~~~~~~~~~

``RecordableState<TSchema>`` is an output-backed endpoint and follows the same
exact image rules.  It is the default for feedback-like state where a previous
tick changes future behaviour.

An ordinary ``State<T>`` is not captured generically even though ``NodeView``
can inspect it.  Its value may hold caches, pointers, locks, resources, or
representation-private data.  A node using ordinary state must declare one of:

``Derived``
   The state is rebuilt by the node restore hook from immutable scalars,
   restored endpoints, and recordable state.

``Semantic``
   Explicit ``NodeCheckpointOps`` encode and restore it through a public stable
   schema.  Prefer conversion to ``RecordableState`` when the state is naturally
   a time-series loopback.

``Unsupported``
   The containing graph cannot be checkpointed, although input replay from the
   beginning remains possible if the graph is deterministic.

Schedulers and graph activation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pending ``NodeSchedulerState`` events and tags are semantic: scheduling a
future evaluation based on an earlier tick changes future outputs.  They are
captured as ordered ``(time, tag)`` entries.  The redundant tag index is
reconstructed and checked.

Future graph schedule entries not backed by ``NodeSchedulerState`` are also
captured.  This includes stateless single-shot scheduling.  Stale entries at or
before the completed cut are lazy-cleanup representation and are not persisted.
``next_scheduled_time`` and nested-parent schedule caches are derived from the
restored entries.

A specialised node may instead declare its schedule derived and rebuild it
from semantic state.  RFC 0007's duration TSW is the model: retained values and
timestamps persist; the expiry queue, subscription generations, and graph alarm
are rebuilt.

Dynamic nested graphs
~~~~~~~~~~~~~~~~~~~~~

Static ``GraphView::node_at`` traversal is not sufficient for map, mesh,
reduce, switch, and other nodes owning runtime-created child graphs.  Each such
owner supplies checkpoint operations that expose:

* stable child slot and lifecycle state;
* key/index/branch identity;
* child graph template id from RFC 0022;
* child graph view during capture;
* representation-specific semantic topology; and
* restore-by-slot construction before child endpoint import.

The checkpoint path for a dynamic instance is:

.. code-block:: text

   parent graph instance / owner node id / stable slot / child graph template

Keys travel in the slot binding where the owning structure uses keys.  The slot
is identity; a restore does not compact holes or reassign keys.  ``KeySlotStore``
therefore gains a validated chosen-slot import operation, and dynamic graph
owners gain public capture/restore operations rather than exposing their
private containers to generic graph code.

Deferred remove/erase state is either represented exactly or normalized by a
documented checkpoint stabilization phase after all observers have completed.
The implementation may not retain retired children in a side container or
bypass the existing stop, unsubscribe, and erase protocol.

Mesh dependency edges, reduce topology, child scheduling queues, and similar
owner state are classified individually as semantic or derived.  The owning
node operations make the decision once; the checkpoint walker does not switch
on node kind.

References and forwarding bindings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RFC 0017 correctly excludes ``REF`` from a cross-process value stream.  A
reference in a complete graph checkpoint has a different representation: it is
an endpoint locator within the restored graph hierarchy.

.. code-block:: text

   graph-instance path / node id / endpoint port / structural child path

The manifest validates the target schema.  Restoration resolves all owning
endpoints and dynamic instances first, then rebinds references and forwarding
aliases.  A reference targeting an endpoint outside the checkpointed graph is
an external binding and requires a manifested binding restore contract; an
unmanifested external reference makes the graph ineligible.

Checkpoint operation tables
---------------------------

Two passive type-erased contracts keep representation policy with its owner.
Their exact signatures are settled during implementation, but their semantic
operations are fixed here.

``TSCheckpointOps``
   Capture an owned exact endpoint image; validate a decoded image; quietly
   import it into unstarted storage; enumerate/reconstruct structural slots; and
   report checkpoint capability.  Concrete TSData strategies install the
   operations and recursively invoke child strategies.  Generic checkpoint
   code does not branch on ``TSTypeKind``.

``NodeCheckpointOps``
   Report state capability; append/restore node-specific semantic state;
   enumerate/reconstruct dynamic children; append/resolve internal endpoint
   bindings; and rebuild derived state during the restore lifecycle.  Ordinary
   stateless nodes use a canonical no-op table.

Both contracts live in clearly named public headers and keep concrete
strategies under an ``impl`` boundary.  Their ops pointers are non-null, with a
canonical unsupported/no-op table as appropriate.  They are cold-path
contracts and add no evaluation-time lookup.

Quiet import
------------

Existing ``apply_delta`` and ``apply_current_value`` are publication mutations:
they record modification, notify parents/subscribers, and may schedule nodes.
That is correct for replayed input and wrong for loading a checkpoint.

Checkpoint import writes exact state under a graph-wide restore guard:

* no output tick is published;
* no observer or parent is notified;
* no input schedules its consumer;
* original modification times are retained;
* slot observers see only the explicit restore lifecycle; and
* partial failure rolls back by destroying the new graph, never by continuing
  with a partially restored one.

The imported graph is not visible to live sources or sinks until validation,
derived-state rebuilding, and start complete.

Restore lifecycle
-----------------

Restoration is a distinct executor operation, not a collection of calls made by
application code against live node views:

1. Rebuild the graph through ordinary wiring and resolve external binding
   descriptors without starting delivery.
2. Validate the graph manifest and non-binding run contract exactly, then
   validate each restored binding at the ``Contract`` or ``Reproducible``
   level recorded by the checkpoint.
3. Allocate root/static storage and establish static edge ownership.
4. Enter restore mode and reconstruct dynamic child slots in parent-before-child
   order.
5. Quietly import owned endpoint images and explicit node semantic state.
6. Resolve internal ``REF`` and forwarding bindings now that every target
   exists.
7. Restore scheduler events, external source cursors, and future graph schedule
   entries.
8. Invoke node restore hooks to rebuild derived caches, subscriptions, indices,
   and alarms.
9. Start nodes/resources in ``Restore`` mode while ingress remains gated.
10. Replay input journal entries after the checkpoint watermark.
11. Complete each external source's replay/live handoff and then admit live
    input and effects.

Fresh and restored starts are distinguishable through a native restore/start
context.  A start callback which initializes ``RecordableState`` must initialize
only a fresh invalid value; it may not overwrite a restored value.  Nodes which
need more than that rule implement the restore hook.  Existing ordinary nodes
with no semantic state require no callback change.

The graph cannot be restarted after ordinary stop; restoration constructs a
new graph instance, preserving the current stop-as-step-toward-erase invariant.

Checkpoint consistency boundary
-------------------------------

A checkpoint request is consumed by the executor at the next successful root
cycle boundary.  It is not captured:

* while any node or nested graph is evaluating;
* at a paused graph cursor;
* after an exception or partially completed cycle;
* before deferred after-evaluation notifications finish; or
* while a structural mutation is open.

The evaluation thread captures an owned immutable checkpoint image.  Encoding,
compression, hashing, and durable writes may proceed asynchronously after the
capture no longer borrows graph storage.  At most the bounded capture/copy step
pauses evaluation.

Real-time senders may continue queuing input while a checkpoint image is being
written.  The checkpoint watermark refers only to journal events already
applied at its cut.  Later events remain outside the image and are replayed from
the journal.  A source's guarantee for data acknowledged before it becomes a
canonical graph input is part of that source binding: it must use a durable
upstream cursor, journal accepted input, or explicitly declare a weaker loss
model.

Input journal
-------------

The durable journal records graph-observed input, not every internal edge and
not the unprocessed contents of an arbitrary transport queue.  Each entry
contains:

.. code-block:: text

   run id
   monotonically increasing input sequence
   evaluation cycle id and engine time
   within-cycle ordinal
   stable boundary binding/port id
   schema descriptor identity
   image or canonical delta payload
   optional external source cursor

Sequence is the recovery order.  Engine time alone is insufficient because
several boundary inputs can participate in one cycle and several events can
share a timestamp.  Cycle id and ordinal reproduce grouping and diagnostics;
the sequence unambiguously identifies the checkpoint watermark.

The journal begins with an image for every boundary input or with an explicit
invalid/empty declaration.  Subsequent entries use RFC 0017 canonical deltas.
An input schema or graph manifest mismatch is an attach error.

Entries are packed into immutable, content-addressed segments.  Each segment
manifest records its inclusive sequence range, payload object id and checksum,
and the preceding segment-manifest id.  Its id therefore selects one exact
parent-linked journal lineage; recovery never discovers a journal by listing
all objects bearing the run id or by selecting every segment whose sequence
number falls in a range.  Objects written by a fenced writer may remain until
garbage collection, but they are unreachable from the selected lineage and
cannot introduce a duplicate sequence during recovery.

For recoverable mode, a canonical input event is durable before its output
mutation becomes visible to the graph.  Backends may batch/group-commit several
events, but the graph cannot advance their durable watermark until the segment
and the run-head update selecting that segment are both committed.  A
lower-durability diagnostic recording mode may acknowledge earlier, but it
cannot be advertised as recoverable.

Replay replaces external source publication.  It applies all entries sharing a
cycle under the recorded engine time and preserves the recorded source/ordinal
order.  Once the last retained event is applied, each source binding performs
an explicit handoff from its recorded cursor to live delivery.  The handoff is
tested for the record immediately before, on, and after its boundary.

Output recording
----------------

Deterministic recovery needs only the checkpoint plus boundary inputs.  Logging
every internal output delta multiplies storage without adding recovery
information.

Optional output recording remains valuable as an oracle:

* record graph outputs or selected internal endpoints;
* replay inputs and compare produced deltas or hashes;
* report the first divergent node/cycle; and
* never use the oracle to silently patch a deterministic mismatch.

This composes with existing component ``Compare`` mode and native table
recording without making them part of the checkpoint format.

Checkpoint format
-----------------

Every checkpoint has one immutable manifest and zero or more immutable state
objects:

.. code-block:: text

   CheckpointEnvelope
     checkpoint id
     manifest: CheckpointManifest
       format version
       parent checkpoint id (optional)
       run/version id
       graph manifest descriptor + id
       source run manifest descriptor + id (provenance)
       binding compatibility requirement per external binding
       checkpoint engine time and cycle id
       last applied input sequence
       input-journal lineage root + head ids
       endpoint/node/topology part index
       part schema descriptors, sizes, and checksums
       completion metadata

The checkpoint id is the digest of the canonical inner
``CheckpointManifest``.  The envelope's checkpoint-id field is omitted from the
digest input by construction.  Decoders recompute and verify it; there is no
self-referential fixed-point requirement.  Publication metadata outside the
immutable descriptor does not contribute to the id.  Each part is addressed by
digest and may be shared with an earlier checkpoint when unchanged.  A
checkpoint manifest is logically complete even when most of its parts are
inherited/reused.  Readers never have to guess whether an omitted endpoint
means unchanged, invalid, or forgotten.

An initial implementation may write a complete physical image every time.
Incremental physical checkpoints follow without changing restore semantics:

* endpoint/node mutation generations identify candidate changed parts;
* content digests confirm reuse;
* a new manifest references old unchanged objects and new changed objects; and
* periodic compaction may publish a physically self-contained checkpoint.

Input-log segment manifests identify their inclusive sequence range,
engine-time bounds, payload digest, and parent segment.  The checkpoint's
journal head transitively selects every exact segment through its watermark;
the head's ending sequence must equal ``last applied input sequence``.  That
lineage must already be durably published before the checkpoint manifest
becomes complete.

Durable checkpoint store
------------------------

RFC 0016 ``FrameStore`` remains the typed Arrow-frame store.  It is a useful
backend precedent but its public ``write/read/contains/clear`` contract lacks
the discovery, conditional publication, per-object deletion, and byte-object
surface required by checkpoint lineage and retention.

This RFC introduces a separate owning, type-erased ``CheckpointStore`` with at
least these semantic operations:

.. code-block:: cpp

   put_immutable(object_id, Bytes)
   get(object_id) -> Bytes
   contains(object_id) -> bool
   list(prefix, continuation) -> page<object_id>
   compare_exchange_ref(name, expected, desired) -> bool
   erase(object_id)

Memory, local filesystem, and S3 strategies may share RFC 0016's private
location and credential machinery.  The public contracts remain separate:
Arrow frames are interoperable analytical objects; checkpoint objects are
versioned binary state and atomic lineage references.

Store implementations guarantee whole-object visibility.  They do not provide
a multi-object transaction.  A named run reference points to an immutable
``RunHeadManifest`` containing the run id, previous published-head id, selected
journal-lineage head and last sequence, and latest complete checkpoint id.  The
run reference never points directly to an unqualified segment or checkpoint.

Journal publication is:

1. write the content-addressed payload and segment manifest, whose parent is the
   journal head in the expected published run head;
2. write a new immutable ``RunHeadManifest`` selecting that segment and
   retaining the latest complete checkpoint; and
3. conditionally advance the named run reference from the expected published
   head to the new one.

Only step 3 advances the graph's durable input watermark.  Concurrent writers
from the same expected head cannot publish different journal successors: the
loser is fenced and must stop or choose a new run/version.  Its immutable
objects are unreachable and ignored by recovery.

Checkpoint publication first writes the content-addressed state parts and then
the immutable checkpoint manifest, which references the journal head at the
captured cut.  The run publisher then writes and conditionally publishes a new
``RunHeadManifest`` that retains the current selected journal head and points to
the new checkpoint.  The captured journal head must be an ancestor of that
current head, allowing journal publication to continue while checkpoint bytes
are encoded.  A single per-run publisher serializes these metadata updates; a
CAS retry may rebase only onto a descendant in the same selected journal
lineage.

A crash before an immutable manifest write leaves unreachable objects, never a
partial checkpoint or journal.  A crash before the run-reference CAS leaves a
complete object discoverable by id but outside the selected run lineage.
Garbage collection removes unreachable objects only after a retention grace
period.  RFC 0021's version and continued-recording model is revised to use
these run ids, parent links, and published heads.

Recovery selection and retention
--------------------------------

Normal recovery starts from one named run reference and follows only its
``RunHeadManifest`` ancestry.  It selects the latest referenced complete
checkpoint whose graph, non-binding run contract, and binding requirements
validate, then follows the selected parent-linked journal lineage and applies
entries in ``(checkpoint_watermark, target_watermark]``.  Prefix listing is
never a recovery-selection mechanism.  "Latest" is published lineage order,
not wall-clock time or the maximum engine time.

Retention is manifest reachability:

* retain every published run-head manifest required by policy;
* retain checkpoint manifests reachable from those heads;
* retain every state part and journal segment referenced by those manifests;
* do not delete deltas following a checkpoint until a retained successor covers
  them; and
* retain ancestor runs while a continued version depends on their content.

A self-contained compaction checkpoint may cut ancestry and make older state
parts eligible for collection.  Deletion is administrative cold-path work and
never runs in graph evaluation.

External effects
----------------

Replaying inputs can repeat sink effects even when computation is perfectly
deterministic.  Every external sink binding therefore declares one policy:

``SuppressDuringReplay``
   Compute and optionally compare the effect, but do not emit it while catching
   up.  Begin live emission only after the replay/live barrier.

``Idempotent``
   Emit with a stable effect id derived from run id, input/cycle sequence, sink
   id, and effect ordinal.  The destination de-duplicates it.

``Transactional``
   Coordinate external commits with checkpoint publication.  The binding's
   transaction state and source cursor participate in the checkpoint protocol.

``Unsupported``
   The graph may be checkpointed for analysis, but automatic effectful recovery
   is refused.

At-least-once and exactly-once are end-to-end claims.  Core exposes the
coordination contract; a connector earns the claim through its own acceptance
tests.  Kafka exactly-once, for example, must coordinate consumed offsets and
produced records in the same transaction or an equivalent atomic protocol; a
configuration label alone remains insufficient.

Python contract
---------------

Checkpoint capture, restore, journal replay, schema validation, and state
import are native C++ operations.  Python exposes configuration and immutable
result handles:

.. code-block:: python

   checkpoint = hg.request_checkpoint("run-id")
   hg.restore_graph(graph, checkpoint="latest", replay_to=target)

The exact asynchronous handle is settled during implementation.  Python does
not walk node objects, encode TS kinds, manipulate dynamic slots, or implement
a second checkpoint store.  Python-authored nodes use native endpoint storage
and contribute checkpoint/manifest capability through their registered bridge
implementation.

Errors and failure handling
---------------------------

Failures are explicit and path-addressed:

* manifest, schema, implementation, or binding mismatch;
* unsupported node state, TSData representation, external reference, or sink;
* corrupt part checksum or missing referenced object;
* input journal gap or duplicate sequence with unequal payload;
* failure to reconstruct a chosen dynamic slot;
* restored scheduler event in the past relative to its checkpoint cut;
* concurrent writer/head conflict; and
* live-handoff cursor disagreement.

No error falls back to an older schema, compacts slots, skips a node, clears its
state, or starts a partially restored graph.  The new graph allocation is
destroyed and the currently running graph, if any, remains unaffected.

Performance and memory
----------------------

The ordinary evaluation hot path remains unchanged except at declared boundary
recorders and existing mutation-generation bookkeeping:

* no graph-wide scan per tick;
* no per-node serialization branch in ``eval``;
* endpoint and node checkpoint operations are resolved before execution;
* journal encoding is paid only by external boundary inputs;
* journal writes batch/group-commit according to durability policy; and
* snapshot encoding/storage is off-thread after an owned consistent image is
  captured.

A full logical checkpoint costs ``O(live graph semantic state)`` to validate
and restore.  A first physical full capture has the same bound.  Incremental
physical capture costs ``O(changed semantic state + manifest size)`` plus
content hashing.  Recovery costs ``O(checkpoint live state + journal events
after its watermark)``.  These costs and retained bytes are reported per graph,
endpoint kind, and checkpoint.

The design does not promise zero-copy persistence: an asynchronous writer must
own an immutable image after evaluation resumes.  It does require that graph
storage is copied once into that image rather than repeatedly materialized per
format or store layer.

Implementation stages
---------------------

Stage 1: exact static-graph checkpoint in memory
   Land RFC 0022, ``TSCheckpointOps``, quiet endpoint import, recordable state,
   scheduler state, root-cycle capture, exact manifest validation, and
   uninterrupted-versus-restored native tests for static graphs.

Stage 2: dynamic topology and references
   Add chosen-slot key import, map/mesh/reduce/switch child traversal and
   reconstruction, REF/forwarding locators, TSW timestamps, and derived-state
   restore hooks.

Stage 3: durable input journal and recovery
   Add canonical boundary event sequencing, replay source substitution,
   checkpoint watermarks, source cursors, live handoff, and output-oracle
   comparison.

Stage 4: durable store and incremental physical images
   Add ``CheckpointStore`` memory/filesystem/S3 strategies, immutable manifest
   publication, CAS run heads, checksums, content reuse, compaction, retention,
   and crash injection at every write boundary.

Stage 5: external effects and Python surface
   Add sink replay policies, connector coordination hooks, native Python
   wrappers, Python-authored node coverage, installed-SDK extensions, and Linux
   lifetime/ASan validation.

Compatibility and migration
---------------------------

The feature is additive.  Existing component record/replay, ``FrameStore``,
table recordings, and JSON serialization retain their contracts.  A component
may continue to record/compare selected values inside a checkpointed graph.

The initial restore contract requires exact graph identity and exact equality
for non-binding run semantics.  The checkpoint records whether each external
binding requires RFC 0022 ``Contract`` or ``Reproducible`` compatibility; a
replacement locator is accepted only at the former level.  Checkpoint format
version, graph migration, endpoint schema migration, and recording/run lineage
are distinct version axes.  A future migration names exact source and target
manifest ids and transforms a decoded checkpoint before quiet import.  There
is no implicit schema widening or best-effort partial restore.

RFC 0017 stream images remain appropriate for convergence of one transported
endpoint.  Graph checkpoint images add runtime metadata and binding topology;
the two share the binary value codec rather than pretending to be the same
artifact.

Alternatives considered
-----------------------

Replay every input from the beginning
   Correct for deterministic graphs and retained as the fallback.  Recovery
   time grows without bound, which periodic checkpoints solve.

Record every internal output delta
   Rejected as the recovery log.  Internal deltas are determined by boundary
   inputs and multiply storage by graph size.  Retained only as an optional
   comparison oracle.

Capture only recordable state
   Rejected.  Current node outputs are the values future ticks observe; without
   them a restore needs earlier input history to rebuild the missing state.

Serialize graph allocation bytes
   Rejected.  Storage contains pointers, subscriptions, erased strategies,
   containers, resources, and allocator state.  Restore uses semantic ops.

Apply images through ordinary output mutation
   Rejected.  It creates ticks, schedules downstream nodes, destroys original
   modification times, and can produce external effects before restore is
   complete.

Compact stable slots during restore
   Rejected.  Slot-derived identity would silently rebind TSD children and
   dynamic graph instances.

Pause external senders for the entire durable write
   Rejected as the normal model.  The consistent image is captured at a short
   evaluator boundary; the input journal carries later events while immutable
   objects are written asynchronously.

Put checkpoint operations on each concrete node through RTTI/downcasts
   Rejected.  It breaks the public type-erasure boundary and prevents installed
   extensions from participating safely.

Unresolved questions
--------------------

* Whether the first implementation captures pending-erase slot state exactly
  or adds a universal post-observer stabilization boundary.
* Whether recoverable push sources journal raw accepted payloads as well as
  canonical graph-observed emissions, or leave the former entirely to their
  binding delivery contract.
* The maximum manifest-chain depth before automatic physical compaction.
* Whether graph migration belongs in this RFC's eventual implementation or a
  separate RFC after exact restore has production evidence.
* Which core sinks can initially claim ``Idempotent`` rather than
  ``SuppressDuringReplay``.

Acceptance criteria
-------------------

* For every supported graph, an uninterrupted run and a run restored at every
  possible successful cycle cut emit identical output deltas at identical
  evaluation times after the cut.
* Static C++ and Python-authored nodes restore ordinary output, error output,
  and recordable state through public graph wiring APIs.
* A node with undeclared semantic ``State<T>`` is rejected at wiring with its
  stable manifest path; a derived-state node rebuilds through its restore hook.
* Node-scheduler tags/events and stateless future graph schedules fire at the
  same times after restore; stale schedule entries are not resurrected.
* Duration TSW recovery preserves original timestamps and therefore expires at
  the same future cycles as the uninterrupted run.
* TSD and keyed dynamic graphs restore live slots, holes, pending erase/free-list
  state, keys, and subsequent slot reuse exactly.
* Map, mesh, reduce, switch, and fixed nested graphs restore their child graphs,
  outputs, schedules, recordable state, and semantic owner topology without
  private-container access from the walker.
* Internal REF and forwarding outputs rebind to the intended restored endpoint;
  external REF targets require and validate a binding contract.
* Quiet import produces no node evaluation, downstream notification, output
  tick, or external sink effect.
* A checkpoint request during evaluation captures only after successful root
  cycle completion; requests around a pause or exception never publish a
  partial checkpoint.
* Multiple input ports at the same engine time replay in the same cycle and
  deterministic ordinal order.  A missing, duplicate-unequal, or corrupt entry
  is refused.
* Two writers attempting different journal successors from the same published
  run head produce one selected successor.  Recovery follows only that exact
  segment lineage and ignores the fenced writer's unreachable objects.
* Loading a checkpoint at watermark ``N`` and replaying ``N+1..M`` agrees with
  replaying the journal from its initial images through ``M``.
* A checkpoint id is reproducible by encoding its canonical manifest with the
  id field omitted; altering any included field changes the verified id.
* A contract-compatible replacement binding with a different resource locator
  attaches when the checkpoint requires ``Contract`` compatibility and is
  rejected when it requires ``Reproducible`` compatibility.
* Each source replay/live handoff has no lost or duplicated boundary event,
  including concurrent real-time ingress during checkpoint persistence.
* Crash injection before every part, manifest, and head write exposes either
  the previous complete checkpoint or the new complete checkpoint, never a
  torn one.
* Concurrent publication from one run head allows one successor and reports a
  fencing conflict to the other writer.
* Retention never deletes a part or journal segment reachable from a retained
  checkpoint/version; compaction makes unreachable ancestry collectible.
* Replay-suppressed sinks emit nothing during catch-up; idempotent sinks receive
  stable effect ids; transactional connectors pass their own atomic recovery
  tests before advertising exactly-once.
* Full and incremental physical checkpoints restore to identical graph state;
  measurements report capture pause, encode/write cost, restore time, journal
  throughput, and retained bytes.
* A separately built C++ extension supplies manifest, TSData or node checkpoint,
  and binding operations through installed public headers.
* Complete macOS and Linux native/Python acceptance suites pass; dynamic graph,
  REF, Python callback, and asynchronous checkpoint lifetime cases pass under
  ASan on Linux.

Implementation status
---------------------

Not started.  The RFC records the graph-level contract intentionally deferred
by RFC 0017 and the durable checkpoint/store gap named by the extension policy.

References
----------

* :doc:`rfc_0000`
* :doc:`rfc_0007_scheduled_duration_tsw_eviction`
* :doc:`rfc_0009_time_series_endpoint_visitors`
* :doc:`rfc_0015_kafka_extension_api`
* :doc:`rfc_0016_object_store_frame_persistence`
* :doc:`rfc_0017_binary_value_codec`
* :doc:`rfc_0019_native_table_recording`
* :doc:`rfc_0021_recording_versions`
* :doc:`rfc_0022_serializable_graph_manifest`
* :doc:`../developer_guide/record_replay_table`
* :doc:`../developer_guide/extension_policy`
