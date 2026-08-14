RFC 0019: Native Table Recording for Partitioned Time-Series
============================================================

:Status: Proposed; reference implementation and acceptance validation complete
:Author: Howard Henson
:Created: 2026-08-12
:Target: Table codec, record/replay data-frame backend, and the Python data-frame adaptor

Summary
-------

Make the native recorder own the data-frame recording path — ``TSD``
partitions, removals, and multi-row frames, including a frame-valued leaf below
``TSD`` — so recording is one implementation rather than two and the per-tick
path never enters Python.

The original missing piece was smaller and better-shaped than it first
appeared. C++
already flattens a partitioned time-series into table rows, and C++ already
appends rows into Arrow builders. What does not exist is a recorder driven by
the *table layout*: the Arrow recorder is built from a plain value schema and
has never known about partition levels.

This RFC adds that recorder, fuses row emission with column appending so no row
tuples are materialised on either side of the boundary, and gives recording an
explicit options surface — as-of tracking, removal tracking and column naming
— rather than the ad-hoc dictionary the adaptor carried.

Motivation
----------

Before this work, recording a ``TSD`` to a data frame failed natively:

.. code-block:: text

   RuntimeError: node[1 'g.record'] start failed:
     table codec: unsupported value kind for 'Map[str,float]'
     (atomics and depth-1 bundles in v1)

The Python adaptor therefore implemented recording itself: it took rows from
``to_table``, pulls every cell into Python, builds Arrow, and writes through
``DataFrameStorage``. That was the only route for the shapes people actually
record, and it had already drifted once from the columnar-builder design
``record_replay_table.rst`` mandates — it was building a one-row Arrow table
per tick and concatenating, which cost 12.3s and 380MB peak where the batched
form costs 0.77s and 226MB on the same 20 000-tick, five-row-per-tick sparse
``TSD``.

Fixing that drift did not remove the reason it happened. Two implementations of
one capability, one of them in Python on the per-tick path, will drift again.

**A measurement that shapes this RFC.** On a flat ``TS[int]``, the native
recorder and a batched Python recorder measure the same — 0.23s vs 0.21s,
0.48MB retained each, over 20 000 ticks. The per-tick Python boundary is
therefore *not* the dominant cost at that shape, and this RFC should not be
sold on speed alone. What it buys is a single implementation, native support
for the shapes that currently have none, and the removal of a row-materialising
step that both paths pay today.

Current state
-------------

.. list-table::
   :header-rows: 1
   :widths: 26 32 42

   * - Piece
     - Where
     - What it does / does not do
   * - ``to_table`` row flattening
     - ``table_impl.cpp``, ``TableLayout``
     - Handles nested ``TSD`` levels, per-level key columns and removed flags,
       and multi-row frame leaves both alone and below ``TSD``. The public
       ``to_table`` result necessarily materialises rows; the recorder uses the
       shared cell-emission traversal instead.
   * - ``TableConverter`` / ``FrameRecorder``
     - ``table_codec.cpp``
     - Appends **straight into Arrow builders**, no row tuples — the shape the
       design record asks for. Built from a *value schema*; atomic leaves and
       depth-1 bundles only. Knows nothing of partition levels.
   * - ``record`` (``DATA_FRAME`` model)
     - ``record_replay_frame_impl.h``
     - Uses ``TableRecorder`` over ``TableLayout`` and writes the finished
       frame to the graph-scoped RFC 0016 store. Partitioned frame values expand
       to one stored row per key/frame-row pair. Optional native segmentation
       flushes independently valid frames without changing replay semantics.
   * - Python data-frame adaptor
     - ``adaptors/data_frame``
     - A compatibility storage adapter only. It delegates complete frames
       through ``store``/``load``/``has``; the recorder and replayer are native.
       The legacy override registry is translated to explicit native wiring
       options at the Python call boundary.

The reference implementation closes that gap with an Arrow recorder driven by
``TableLayout`` rather than by a value schema. It also implements keyed frame
expansion, native segmented flushing, and the public ``TableTypeOps`` extension
contract described below. All eight stages and the acceptance validation are
complete; the RFC remains Proposed until review accepts it and the pull request
is merged.

Design
------

Record the layout, not the value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``TableLayout`` already computes what an Arrow schema needs: ``keys`` (all
column names in row order), ``col_metas`` (per-column leaf metadata),
``levels`` (per ``TSD`` level: key columns and the removed-flag column),
``value_cols``, and the ``is_multi_row`` frame case.

A ``TableRecorder`` is constructed from a layout plus options, holds one
``arrow::ArrayBuilder`` per column, and exposes the same two verbs the existing
recorder does — append a row, finish a frame. The existing ``FrameRecorder``
becomes the degenerate case of it (no levels), which keeps one code path rather
than two that look alike.

Fuse emission with appending
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``emit_partition_rows`` currently walks the levels and pushes a materialised
``Value`` row per row. Recording then reads those rows back. The design record
(*I5*) already rejects that shape: "materialising a row value per tick, then
accumulating rows into a frame in the recorder, costs two copies".

The walk is therefore parameterised by a row sink. ``to_table`` keeps the
existing sink that materialises rows, because its output *is* a row-valued
time-series. Recording supplies a sink that appends cells directly into the
builders, so no row value is ever built. This removes a copy from the native
path that exists today even for the shapes it already supports.

**Both emitters, not just the partition walk.** A frame-valued leaf does not go
through ``emit_partition_rows`` at all: ``emit_frame_rows`` is a separate
function that calls ``read_row`` to build a ``Value`` for every frame row. Since
frame expansion is one of the shapes this RFC promises, parameterising only the
partition walker would leave exactly that path materialising rows and would not
satisfy the acceptance criterion below. The sink is therefore introduced as a
shared cell-emission traversal that both emitters use.

Options
~~~~~~~

The adaptor's overrides become an explicit, validated structure rather than a
dictionary read at three call sites. Recording configuration is per recordable
key, so a graph can record two series with different policies:

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Option
     - Meaning
   * - ``as_of``
     - ``Inherit`` (default), ``Track``, or ``Omit``. Inherit uses the fixed
       ``Config::as_of`` when present and otherwise tracks evaluation time;
       Omit removes the as-of column entirely. The column name comes from
       ``Config::as_of_key`` and may be overridden per key.
   * - ``removes``
     - ``Omit`` (default, matching ``_OverrideState``) or ``Track``. Omitted
       means a removal **does nothing**: no removed-flag column on any level,
       and no row — not a row of nulls. Per-level column names may be supplied
       for ``Track``, which is what ``remove_partition_keys`` does today.
   * - ``partition_names``
     - Replacement names **per flattened key column**, not per level: a level
       whose key is a tuple or compound scalar flattens into several columns
       (``Level::key_paths`` is one path per column), and the existing
       ``partition_keys`` override supplies a name for each. A per-level name
       could not express ``TSD[tuple[int, str], ...]``.
   * - ``date_key``
     - The value-time column name; defaults to ``Config::date_key``.
   * - ``frame_prefix``
     - Prefix applied to the columns of an expanded frame-valued leaf. Empty by
       default, which is today's behaviour. Namespacing the expansion is the
       ordinary way to avoid a frame column landing on the same name as a key
       or bitemporal column.
   * - ``mode``
     - ``Tick`` (default), ``Sample``, ``Snap`` — the existing
       ``ToTableMode``, which the recorder must honour because it changes which
       rows exist.
   * - ``flush_rows`` / ``flush_interval``
     - **Off by default**: the run is accumulated in the Arrow builders and
       written once at ``stop``, which is the current and expected approach.
       Set to a row count or evaluation-time interval for a run whose output should
       not be held whole — see *Flushing writes segments*. Within a run only.

Defaults follow today's configuration — notably ``removes`` defaults to
``Omit``, because ``_OverrideState`` sets ``track_removes`` to ``False``.

One default does **not** reproduce today's output, and it is a deliberate
correction rather than an oversight. Today, omitting removals drops the
removed-flag column but the rows ``to_table`` emitted for those removals are
still stored — a row that cannot be interpreted, since nothing marks it as a
removal. Under this RFC omission emits no row at all. Recordings will therefore
contain fewer rows by default than they do today, which is the intended
semantic and worth knowing before migration.

**What ``removes: Omit`` means.** This is the ordinary shape of a recorded data
stream rather than a lossy variant of one: a removal means nothing further is
recorded for that key, and the recording is not a reversible structure. Most
consumers of a time-indexed stream read it exactly that way, which is why the
option exists at all.

The consequence to be aware of is at replay: nothing signals the removal, so a
replayed series retains the key rather than dropping it. That is correct under
this model — the stream never said it was removed — but it means a ``TSD`` whose
membership carries meaning must explicitly select ``Track``.

Frame-valued leaves expand into columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A frame-valued leaf under a ``TSD`` level is supported by **expansion**: the
frame's own columns become value columns alongside the level's key columns, and
each frame row becomes a table row carrying a copy of the key cells. A tick of
``K`` keys whose frames hold ``R`` rows therefore produces ``K x R`` rows, each
fully qualified.

``TableLayout`` already carries ``frame_converter`` for the ``is_multi_row``
case, so expansion reuses that converter's columns as the value columns rather
than inventing a second description of the frame.

Expanded columns may be prefixed, through ``frame_prefix``. That is the
intended way to keep a frame's columns clear of the key and bitemporal columns,
and to distinguish two frames recorded side by side.

Replay of a frame-valued leaf therefore consumes a **run** of rows rather than
one row: the rows sharing a value time and, below ``TSD``, a key path are the
tick's frame. Key and removal columns retain their structural role, and the
remaining columns are the frame's payload.

Payload columns are resolved by their configured name, exactly like every other
column — **replay must be given the same ``frame_prefix`` the recording used.**
An earlier revision resolved them positionally so the prefix need not be
repeated. That is withdrawn: see *Projection is explicit or it fails* below.

``frame_prefix`` is a ``record`` argument like ``as_of``, ``removes``,
``partition_names`` and ``removed_names``: the whole option surface is set at
the call site, so two recordings in one graph differ by being called
differently rather than through a registry keyed on their name.

Compound keys are rebuilt through their paths
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``TSD`` key that is a tuple or bundle flattens into one column per leaf, so
replay cannot read it back as a single cell. ``assemble_from_paths`` is the
exact inverse of that flattening: it consumes the level's ``key_paths`` and the
cells read from those columns together, in the field order ``flatten_value``
emitted them in, and rebuilds the key to any nesting depth. An atomic key is
the one-column case of the same walk, not a separate path.

Every leaf must carry a value. A key missing a component is not a key, and
building a partial one would replay ticks under a key that never existed.

A name that still collides after the configured prefix is applied is an
**error** at layout time. Silently renaming would produce a frame that replays
by name into the wrong column, which is the failure mode this whole path exists
to avoid.

Projection is explicit or it fails
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Replay reads a table it did not necessarily write — a hand-built Arrow table is
a first-class input, so the recording options can never be assumed present.
That makes it tempting to *infer* a column when the configured name is absent.
**Replay never infers.** A configured column that is not found is an error, at
start, naming the column.

The rule exists because every inference available here is unsound:

* **Position does not identify a column.** A default recording and one with
  ``as_of: Omit`` plus ``removes: Track`` are both four columns —
  ``[date, as_of, key, value]`` against ``[date, removed, key, value]``.
  Omitting the as-of removes a column and tracking removals adds one, so the
  counts cancel and position 1 is the as-of in one and a removed flag in the
  other. A positional reader silently swaps a timestamp for a boolean.
* **Type does not identify a column either.** It breaks the tie in that one
  example only because those two types differ; it says nothing about two key
  columns of the same type in the wrong order.

A guess that is right most of the time is worse than no guess at all: it moves
the failure from a clear error at start to wrong data much later, in a
recording that already looks readable. So the payload-column positional
fallback is removed, and ``frame_prefix`` must be supplied to replay like every
other projection argument. Configuration that can be ignored is configuration
that cannot be trusted.

Current native recordings make this rule enforceable for optional columns as
well. Every completed frame carries a versioned Arrow schema-metadata entry for
each layout column: its exact stored name, or an explicit absent marker. Replay
therefore distinguishes ``as_of: Omit`` from an as-of column stored as
``revision``, and ``removes: Omit`` from a removal flag stored as ``gone``. If a
caller omits the corresponding non-default replay projection, start fails
instead of silently dropping revision or removal semantics. The descriptor is
written on every segment and preserved by the native file/S3 formats and the
Python compatibility seam.

An unannotated frame remains a first-class input. This includes hand-built
Arrow tables and recordings written before the descriptor existed. Replay does
not infer their projection: required and explicitly named optional columns must
still resolve by name, while an unnamed optional column may be absent because
the legacy frame contains no authoritative information with which to
distinguish omission from a rename. Compatibility is therefore explicit and
bounded to frames that genuinely lack writer metadata.

Resolution does not rewrite the table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Replay resolves each layout column to a **column index** in the stored table,
once at start, and reads through those indices. It does not rename the stored
table's columns to the layout's canonical names.

Renaming was the earlier approach, and it has two defects. It assumes the
canonical names (``__date_time__``, ``__key_1__``) never occur as real columns
in a stored table; they were chosen to be unlikely, but "unlikely" is not a
contract, and a table that happens to carry one becomes unreadable through no
fault of its author. And it discards the configuration: after the rewrite,
nothing downstream can tell what the caller actually asked for, which is why a
projection argument could be silently ignored in the first place.

Index resolution has neither problem. The configured name is used to *find* the
column and is never overwritten, so a stored column called ``__key_1__`` is
simply a column called ``__key_1__``, and a mis-supplied projection fails at
start rather than resolving to something plausible.

A recording is one run
~~~~~~~~~~~~~~~~~~~~~~

A recording is written by exactly one run and its schema is fixed for that run.
Appending to a recording made by an earlier run is not supported. A native
write to an existing key fails: another run has no relationship to the first
and must use another key, or the caller must explicitly remove the old object.
The Python compatibility seam delegates overwrite policy to its implementation;
core does not impose native storage policy on user code.

.. important::

   **This is a behaviour change, and it is the intended one.** Recording twice
   to the same key previously replaced the first recording silently. It now
   fails at ``start``:

   .. code-block:: text

      node[1 'g.record'] start failed: recording key already exists: native.out

   **To re-record a key, delete it first.** The failure is deliberate rather
   than a limitation: a silent replace loses a completed recording with no
   signal, and cannot be distinguished from the append that this model does
   not support.

   The scope that matters is the ``GlobalState``, not the graph run — a
   ``GlobalState`` may span several runs, and a second run under the same one
   hits this. Re-running under a fresh ``GlobalState`` gets a fresh store and
   is unaffected, which is why test suites do not normally encounter it.

That removes schema evolution from scope entirely — there is no widening rule
to design and no compatibility check on append. Through adaptor migration the
storage operation is one atomic write/read of one complete frame. Flushing is a
later native-store feature within a run; it never merges with a previous run.

If cross-run append is wanted later it is a deliberate change, not an accident
of the flush policy.

Flushing writes segments
~~~~~~~~~~~~~~~~~~~~~~~~

Flushing is **opt-in**. The default is to accumulate the whole run in the
builders and write once at ``stop`` — the current approach, and the right one
while a run's output fits in memory. Columnar builders are why that is
affordable: the cost tracks the payload rather than the tick count, which is
the whole point of recording into builders rather than assembling per tick.

Where a run's output should not be held whole, flushing bounds it — and then a
flushed recording must be **readable up to its last flush**, including after
the run writing it has died. That makes a flush a commit point, and it decides
the write shape.

Each flush writes a **new object** rather than rewriting the recording. A
recording is therefore one run stored as an ordered sequence of independently
valid frames. Replay holds only the current segment: after replaying it, the
frame is released and the next segment is loaded. Rewriting a single object per
flush would re-read and re-write the whole recording every time — the quadratic
shape this work removed from the Python adaptor, restored one layer down.

Segment writes suit RFC 0016's store directly: objects are written once and
never mutated, which is the immutable-by-default case rather than an exception
to it. It also means a partially written segment does not become visible — an
object appears whole or not at all — so a reader naturally sees exactly the
completed flushes and nothing torn.

The object at the logical key is an immutable marker identifying a segmented
recording and claiming the key before any segment is published. Segments use
``<key>.0``, ``<key>.1`` and so on. A flat recording stores its frame directly
at ``<key>``. Consequently a writer refuses a flat write if either ``<key>`` or
the legacy ``<key>.0`` already exists, avoiding an ambiguous old layout. A
segmented writer also refuses a stale ``<key>.complete`` before it publishes
the marker.

Three consequences follow:

* Segments need no enumeration. A reader that sees the marker loads ``.0`` and
  probes monotonically until the next segment is absent. Recording time is
  monotonic, so every segment also has an ordered time bound that can later be
  used to skip whole objects.
* A reader cannot tell a still-running recording from one whose process died,
  and does not need to: both are "everything up to the last flush". A
  ``<key>.complete`` manifest written atomically at successful ``stop`` lets a
  reader that *does* care distinguish them. It may carry the segment count,
  row count and time bounds, but is not required to replay completed segments.
* Segmentation is native-only. The Python compatibility bridge receives one
  complete frame through ``store``/``load``/``has`` and has no segment or flush
  API. This keeps it suitable for compatibility adapters without making it a
  second storage framework.

This keeps *A recording is one run* intact: one run, one recording, written as
one flat frame by default or as however many segments its flush policy
produced.

Configuration is GlobalState-scoped, with per-record shaping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``record_replay::Config`` lives in ``GlobalState``, reached through
``GlobalContext::active_state()``: the configuration and the store selection
belong to the enclosing ``GlobalState``, not to a process-wide singleton.

**A ``GlobalState`` is not one-per-run.** It is an ambient scope that may span
several graph runs, and every run inside one sees the same configuration *and
the same store*. That is precisely why a second run recording to the same key
under one ``GlobalState`` hits the duplicate-key rejection described in
*A recording is one run*, while re-running under a fresh ``GlobalState`` does
not — the store is new. Anything reasoning about recording lifetime must reason
about the ``GlobalState``, not the graph.

The adaptor grew a second, per-key override dictionary beside this. Instead the
options that shape one recording are passed **to the recorder**, and the
``GlobalState``-level bitemporal configuration supplies their defaults.

This also answers where per-key options live: they do not live anywhere. They
are an argument at the call site, so two recordings in one graph differ by
being called differently rather than by a registry keyed on name.

Backend selection is local too, which is not obvious: overloads guard on the
model through ``requires_``, which runs before the node exists — but
``OperatorCallContext::scalar`` exposes scalar wiring arguments by name, so a
``model`` argument at the call site is readable there. ``record`` and
``replay`` therefore take ``model``, defaulting to empty, meaning "use the
graph's". The model does not have to stay graph-scoped for dispatch to work.

The constraint this creates is that the guards must remain **mutually
exclusive**. Every record/replay overload resolves through one function,
``record_replay::call_model``, which returns the call-site model if one was
supplied and the graph's otherwise. If one overload consulted the override and
another read the configuration directly, a call supplying it would match
several overloads or none, and overload resolution would report the symptom
without the cause. That is why the resolution point is shared rather than
duplicated into each guard.

Storage
~~~~~~~

Recording writes through the graph-scoped ``store::FrameStore`` contract that
RFC 0016 defines. The owning erased handle and its passive ops table keep the
recorder independent of memory, filesystem, S3 and Python representations;
none of those strategies is a public base-class implementation.

``DataFrameStorage`` is then a compatibility frame-store implementation rather
than a parallel recording stack. The Python adaptor keeps its public surface
(``MemoryDataFrameStorage``, ``FileBasedDataFrameStorage``, subclass hooks,
``set_data_frame_overrides``) and loses only the part that reimplemented the
recorder. Its native-facing protocol is deliberately just ``store(key, frame)``,
``load(key)`` and ``has(key)``. Python decides whether a repeated ``store``
overwrites; native immutability is not projected onto this compatibility seam.

Production memory, filesystem and S3 stores remain C++ implementations. They
do not delegate through ``DataFrameStorage`` and retain native key validation,
immutability, compression and segmentation. ``MemoryDataFrameStorage``
exists so release/0.5 Python code can still set and retrieve frames directly.

The flush policy matters here: a store write per flush, not per tick.

Replay
~~~~~~

Replay reads back through the same layout, which is what makes the round trip
verifiable rather than merely plausible. It needs the row grouping the Python
side does today — rows sharing a value time form one tick — plus ``start_time``
/ ``end_time`` / ``as_of`` filtering.

``replay_const`` takes the **native** semantics, not the adaptor's. The two
differ, and in a direction that matters:
``record_replay.cpp``'s ``replay_const_value`` skips rows whose value time is
after the cutoff and keeps the *last qualifying* row — the last value at or
before the start. The Python ``_df_replay_const_rows`` yields the *first* group
at or after the start, which for a graph starting between recorded ticks is a
**future** value. ``recorded_seed_resolver`` seeds RECOVER through
``replay_const_value``, so adopting the adaptor's behaviour would seed graphs
with data from their own future.

The adaptor's behaviour changes to match, which is a fix rather than a
migration loss.

The caller supplies the same local column projection when replaying a recording
with renamed partition or removal columns. The release/0.5 compatibility layer
does this from ``set_data_frame_overrides`` at wiring time. A missing projected
column is an error rather than a silent reinterpretation. This is a fixed-schema
read contract, not schema evolution.

Staging
-------

The implementation followed these independently verifiable steps:

1. **Row sink.** Parameterise ``emit_partition_rows`` by a sink; ``to_table``
   keeps materialising. No behaviour change; pure refactor with the existing
   tests as the guard.
2. **``TableRecorder`` over a layout.** Arrow schema from the layout, builder
   per column, appending sink. ``FrameRecorder`` re-expressed as the
   no-levels case.
3. **Options.** The structure above, with graph-scoped defaults and explicit
   per-record shaping arguments.
4. **Native ``record`` for partitioned types.** Remove the ``start``-time
   rejection; the ``DATA_FRAME`` model now records ``TSD`` and multi-row
   shapes.
5. **Replay parity.** Grouping, filtering, ``replay_const``.
6. **Adaptor migration.** ``DataFrameStorage`` as the narrow Python
   ``store``/``load``/``has`` bridge; the Python recorder and replayer deleted,
   their public compatibility surface unchanged. No segmentation in Python.
7. **Segmented flushing.** Optional row/time thresholds for native stores;
   the default still writes once at ``stop``.
8. **Table ops and a real builder.** The public passive extension contract
   described below, selected once into the interned layout.

Steps 1–2 are where the performance is; steps 3–5 are where the parity is;
step 7 bounds large native recordings; step 8 makes the traversal extensible.

Table ops and a real builder (step 8)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table machinery reuses the universal vocabulary —
``TableLayout`` is a **Plan** (interned by schema, describing a row's column
order and which columns are keys, flags or values), the TS schema is the
**Schema**, and ``recording_columns`` plus recorder construction is the
**Builder**. Before step 8, **Ops** was the missing piece.

Three separate recursive walks — ``build_layout``, ``emit_rows_to`` and
``apply_recorded_row`` — each switch on ``TSTypeKind`` (TSD / TSB / TS /
Frame) in their own ``if``/``else`` chain. That has two costs:

* **It is closed.** A type defined outside the core package cannot take part
  in table recording, because there is nowhere to register; participating
  means editing the chain. This is the extensibility the Python
  multi-dispatch design was reaching for.
* **Nothing enforces that the walks agree.** ``emit`` and ``apply`` must stay
  mutual inverses, and today only a test catches it when they drift — which
  is exactly how the compound-``TSD``-key gap surfaced in step 5.

The implemented shape is the public ``TableTypeOps`` passive function table
(``describe`` / ``emit`` / ``apply``), resolved by exact time-series schema,
with ``ts_table_layout`` as the builder that walks the schema and interns the
resulting Plan. The selected ops pointer is stored in that plan, so direct
``from_table`` and stored replay use the same ``TableRowSource`` contract.
Default and unsupported states use a canonical non-null no-op table.

When an exact registered schema is nested below a built-in ``TSB`` or ``TSD``,
the parent plan retains the independently interned child plan and a local-to-
parent column projection. The parent owns structural columns and row formation;
the child owns conversion of its value columns through the same ``emit`` and
``apply`` operations it uses at the root. Embedded multi-row strategies are
rejected because combining their rows with sibling structural fields would
require a separate row-product policy.

Two constraints bound the design:

* **Ops resolve at BUILD time into the Plan, never per tick.** The per-tick
  path is required to be lock-free and ``shared_ptr``-free, so a per-tick
  registry lookup is not available. This is how Plan/Ops already work
  everywhere else, so it is a constraint the pattern already satisfies.
* **Extensibility is only real if the registry is reachable from the
  extension SDK.** ``table_type_ops.h`` and
  ``register_table_type_ops(schema, ops)`` are installed public contracts.
  An extension registers a static, complete table for an exact schema before
  wiring. The installed-SDK consumer verifies that the contract is passive,
  non-polymorphic, and has canonical non-null defaults.

The Plan, ``TableRowSink``, projection, and recorder remain the semantic
owners. The ops table changes dispatch without leaking an implementation
strategy or introducing a second runtime.

Compatibility
-------------

Stored frames keep their current column layout and names, so recordings written
before this RFC replay after it. The Python adaptor's public API does not
change. ``ToTableMode`` semantics are unchanged.

Two things do change and need calling out. A recording becomes visible to a
reader at flush boundaries rather than per tick, which is already true after
the batching fix. And the native path becomes the one that runs for the
``DATA_FRAME`` model on partitioned types, where previously it refused — so a
graph that relied on the refusal to fall through to the Python model must
select the model explicitly.

Alternatives considered
-----------------------

**Extend the table codec to ``Map`` kinds.** The obvious reading of the error
message, and wrong: it would give ``TableConverter`` a second, parallel notion
of partitioning next to ``TableLayout``, which already models levels, removed
flags and multi-row leaves correctly. The codec's value-schema orientation is
right for what it does; recording simply needs a different driver.

**Bind ``FrameRecorder`` to Python and append from the adaptor.** Smaller, but
it keeps the per-tick Python boundary and leaves two recorders. It also cannot
help the shapes that matter, since the recorder still could not describe a
partitioned layout.

**Leave the Python recorder in place.** Defensible on measurement alone —
after batching it matches the native path on flat values. Rejected because the
split is the defect: the drift that cost 12.3s and 380MB happened *because*
recording was implemented twice, and nothing prevents a recurrence.

Resolved scope decisions
------------------------

* Reads and writes are atomic whole-frame operations through step 6.
* Stored schema evolution is out of scope; a key identifies one immutable run.
* Python compatibility storage is limited to ``store``/``load``/``has`` and
  owns its overwrite policy. Segmentation is native-only.
* A segmented recording is identified by its base-key marker and consists of
  independently valid ``.N`` frames; an optional ``.complete`` manifest closes
  a successful run.

Acceptance criteria
-------------------

* A ``TSD[str, TS[float]]`` records and replays natively under the
  ``DATA_FRAME`` model, round-tripping tick-for-tick, including removals.
* Nested ``TSD`` levels produce one key column per level, in layout order.
* ``as_of`` omitted, tracked, and fixed each produce the stated column layout;
  the same for ``removes``.
* Renamed partition and removed columns appear under the supplied names and
  replay by name.
* Tick, Sample and Snap produce the same rows natively as through
  ``to_table`` today.
* No ``Value`` row is materialised on the recording path, **for the frame-leaf
  path as well as the partition walk** — asserted by covering the appending
  sink directly, not inferred from timing.
* ``replay_const`` returns the last value at or before the start time, for both
  the native reader and the migrated adaptor, and a graph starting between
  recorded ticks is never seeded with a later value.
* Retained Arrow memory tracks payload rather than tick count, at both one row
  per tick and five rows per tick over 20 000 ticks.
* The Python adaptor's existing tests pass against the migrated implementation,
  with one deliberate exception: **Python integration is limited to atomic
  whole tables — batching is no longer supported.** The batching knob
  (``RECORD_BATCH_ROWS``) and the test that drove it
  (``test_recording_spanning_several_batches_is_complete_and_ordered``) are
  gone with the Python recorder, replaced by
  ``test_native_recording_is_complete_and_ordered`` asserting the same
  completeness and ordering over the native path. Known client usage meets the
  atomic-table constraint. The chunk-retention regression
  (``test_recording_does_not_retain_a_chunk_per_tick``) is retained unchanged
  in substance, since it pins the defect that motivated this work rather than
  the mechanism that fixed it.
* A release/0.5-style ``MemoryDataFrameStorage`` can set and retrieve complete
  frames while all row construction and replay remain native.
* A Python compatibility store is invoked only through ``store``/``load``/
  ``has``; no clear, immutability, compression or segmentation operation crosses
  the Python boundary.
* Native memory, filesystem and S3 stores reject a duplicate recording key;
  a Python compatibility implementation may choose to overwrite it.
* A recording written by the current Python adaptor replays through the native
  reader.
* ``removes: Omit`` emits no row and no column for a removal, and the replayed
  series is asserted to retain the key — the documented divergence, pinned by
  test rather than left implicit.
* A frame-valued leaf under a ``TSD`` level expands to ``K x R`` rows with the
  key cells repeated.
* ``frame_prefix`` renames expanded payload columns, and replay resolves them
  by the same configured name. Replaying without the prefix the recording used
  **fails**, naming the missing column, rather than recovering the columns
  positionally. A collision that survives the prefix is refused at layout time.
* No projection argument is ever inferred. Hgraph-produced frames persist the
  exact projection, including absent optional columns. For every projection
  option, a recording made with it and replayed without it fails at start, and
  the error names the column that was not found. Unannotated hand-built and
  legacy frames retain name-based compatibility without schema guessing.
* Replay resolves layout columns to stored column indices and leaves the stored
  table's column names untouched. A stored table carrying a column literally
  named ``__date_time__`` or ``__key_1__`` is readable, and its own projection
  still applies.
* A recording interrupted mid-run reads back exactly the rows up to its last
  flush, and never a torn segment.
* Segments read back in write order, and a recording of one segment is
  indistinguishable in content from the same rows flushed across several.
* Two ``record`` calls in one graph with different per-record options produce
  differently-shaped recordings, and a call with no options matches the
  graph-scoped defaults.
* Selecting the backend through a local ``model`` argument dispatches to the
  same overload the equivalent graph configuration would, and a call with no
  ``model`` follows the graph. Every record/replay overload resolves the model
  through one function, so a call supplying ``model`` matches exactly one.
* ``date_key`` and ``as_of_key`` may be renamed per recording and replayed
  through the corresponding projection without changing graph configuration.
* A statically registered exact-schema ``TableTypeOps`` controls describe,
  emit, direct apply, and persisted replay through one non-null passive table,
  both at the root and beneath built-in ``TSB``/``TSD`` structure.
* The installed SDK can include ``table_type_ops.h``, register an operation
  table, and consume the canonical no-op table without depending on private
  representation headers.

Reference implementation validation
-----------------------------------

The acceptance criteria above are pinned at the public wiring boundary rather
than by constructing recorder or replay internals directly:

* Native layout, record/replay, projection, frame expansion, segmentation,
  interruption and extension dispatch are covered by
  ``test_record_replay_partitioned.cpp``, ``test_record_replay_frame.cpp``,
  ``test_recording_columns.cpp``, ``test_table.cpp`` and
  ``test_frame_store.cpp``. The recording tests exercise the cell-appending
  ``TableRowSink`` path for both partition and frame leaves; public
  ``to_table`` remains the only path that materialises row ``Value`` objects.
* Python parity is covered by ``test_native_table_recording.py`` and
  ``test_python_frame_store.py``, together with the unchanged data-frame
  adaptor and release/0.5 compatibility tests. The Python compatibility store
  is asserted to receive one complete frame and no segmentation operations.
* The 20 000-tick measurements in *Motivation* cover the one-row and
  five-row-per-tick shapes. The regression
  ``test_recording_does_not_retain_a_chunk_per_tick`` additionally asserts
  that a complete run retains one Arrow chunk per output column rather than
  one chunk per tick.
* The installed-package consumer includes ``table_type_ops.h``, registers and
  resolves an exact-schema operation table, checks the passive layout and
  canonical no-op contract, and constructs the typed ``ToTableMode`` scalar.

On 2026-08-13 the fresh macOS and Linux native acceptance builds each passed
all 1 565 tests. Stable-ABI wheels built with Python 3.12 and installed into
fresh Python 3.14 environments passed all 2 128 non-WIP compatibility tests on
both platforms (9 skipped). The generated API inventory was current, the
installed-SDK consumer passed, and the complete Sphinx build passed with
warnings treated as errors.

References
----------

* ``docs/source/developer_guide/record_replay_table.rst`` — the columnar-builder
  ruling (*I5*) this RFC finishes applying.
* RFC 0016 — object-store frame persistence; the graph-scoped
  ``store::FrameStore`` is the storage seam used here.
* ``include/hgraph/types/table_type_ops.h`` — the public ``TableLayout`` and
  ``TableTypeOps`` contract.
* ``include/hgraph/types/value/table_codec.h`` — ``TableConverter``,
  ``FrameRecorder``.
* ``python/hgraph/adaptors/data_frame/_data_frame_record_replay.py`` — the
  adaptor whose recording half this RFC removes.
