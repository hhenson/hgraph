RFC 0019: Native Table Recording for Partitioned Time-Series
============================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-12
:Target: Table codec, record/replay data-frame backend, and the Python data-frame adaptor

Summary
-------

Make the native recorder able to record everything the Python data-frame
adaptor records — ``TSD`` partitions, removals, multi-row frames — so that
recording a data frame is one implementation rather than two, and so the
per-tick path never enters Python.

The missing piece is smaller and better-shaped than it first appears. C++
already flattens a partitioned time-series into table rows, and C++ already
appends rows into Arrow builders. What does not exist is a recorder driven by
the *table layout*: the Arrow recorder is built from a plain value schema and
has never known about partition levels.

This RFC adds that recorder, fuses row emission with column appending so no row
tuples are materialised on either side of the boundary, and gives recording an
explicit options surface — as-of tracking, removal tracking, column naming,
record mode, flush policy — rather than the ad-hoc dictionary the adaptor
carries today.

Motivation
----------

Recording a ``TSD`` to a data frame currently fails natively:

.. code-block:: text

   RuntimeError: node[1 'g.record'] start failed:
     table codec: unsupported value kind for 'Map[str,float]'
     (atomics and depth-1 bundles in v1)

So the Python adaptor implements recording itself: it takes rows from
``to_table``, pulls every cell into Python, builds Arrow, and writes through
``DataFrameStorage``. That is the only route for the shapes people actually
record, and it has already drifted once from the columnar-builder design
``record_replay_table.rst`` mandates — it was building a one-row Arrow table
per tick and concatenating, which cost 12.3s and 380MB peak where the batched
form costs 0.77s and 226MB on the same 20 000-tick sparse ``TSD``.

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
     - ``table_impl.cpp``, ``TsTableLayout``
     - **Handles everything**: nested ``TSD`` levels, per-level key columns and
       removed flags, multi-row frame leaves, Tick/Sample/Snap modes. But
       ``emit_partition_rows`` materialises a ``Value`` row per row into a
       ``std::vector<Value>``.
   * - ``TableConverter`` / ``FrameRecorder``
     - ``table_codec.cpp``
     - Appends **straight into Arrow builders**, no row tuples — the shape the
       design record asks for. Built from a *value schema*; atomic leaves and
       depth-1 bundles only. Knows nothing of partition levels.
   * - ``record`` (``DATA_FRAME`` model)
     - ``record_replay_frame_impl.h``
     - Uses ``FrameRecorder``, writes the finished frame to the RFC 0016 frame
       store. Fails at ``start`` for any partitioned type.
   * - Python data-frame adaptor
     - ``adaptors/data_frame``
     - Consumes ``to_table`` rows in Python, builds Arrow, writes through
       ``DataFrameStorage``. Carries the override surface: ``track_as_of``,
       ``track_removes``, partition-key and removed-key renames.

The gap is one component: **an Arrow recorder driven by ``TsTableLayout``
rather than by a value schema.** Everything either side of it exists.

Design
------

Record the layout, not the value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``TsTableLayout`` already computes what an Arrow schema needs: ``keys`` (all
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
     - ``Track`` (default), ``Omit`` — no as-of column at all — or ``Fixed``
       with a supplied time. The column name comes from ``Config::as_of_key``
       and may be overridden per key.
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
   * - ``flush``
     - **Off by default**: the run is accumulated in the Arrow builders and
       written once at ``stop``, which is the current and expected approach.
       Set to a row count or wall-clock interval for a run whose output should
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
membership carries meaning wants ``Track``, which stays the default.

Frame-valued leaves expand into columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A frame-valued leaf under a ``TSD`` level is supported by **expansion**: the
frame's own columns become value columns alongside the level's key columns, and
each frame row becomes a table row carrying a copy of the key cells. A tick of
``K`` keys whose frames hold ``R`` rows therefore produces ``K x R`` rows, each
fully qualified.

``TsTableLayout`` already carries ``frame_converter`` for the ``is_multi_row``
case, so expansion reuses that converter's columns as the value columns rather
than inventing a second description of the frame.

Expanded columns may be prefixed, through ``frame_prefix``. That is the
intended way to keep a frame's columns clear of the key and bitemporal columns,
and to distinguish two frames recorded side by side.

A name that still collides after the configured prefix is applied is an
**error** at layout time. Silently renaming would produce a frame that replays
by name into the wrong column, which is the failure mode this whole path exists
to avoid.

A recording is one run
~~~~~~~~~~~~~~~~~~~~~~

A recording is written by exactly one run and its schema is fixed for that run.
Appending to a recording made by an earlier run is not supported; re-recording
to the same key replaces it.

That removes schema evolution from scope entirely — there is no widening rule
to design, and no compatibility check on append — and it is what makes the
options above safe to vary per run, since no two runs ever share a frame.
Flushing still happens within a run: those writes build the current recording
incrementally, they do not merge with a previous one.

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
recording is therefore one run stored as an ordered sequence of segments, and
reading it concatenates the segments in order. Rewriting a single object per
flush would be correct but would re-read and re-write the whole recording every
time — the quadratic shape this work removed from the Python adaptor, restored
one layer down.

Segment writes suit RFC 0016's store directly: objects are written once and
never mutated, which is the immutable-by-default case rather than an exception
to it. It also means a partially written segment does not become visible — an
object appears whole or not at all — so a reader naturally sees exactly the
completed flushes and nothing torn.

Two consequences follow:

* Segments need an ordering *and a discovery rule*, because ``FrameStoreOps``
  is ``write``/``read``/``contains``/``clear`` with no enumeration. Each
  segment is a separate key carrying a monotonic index under the recording's
  key, and a reader probes ``contains`` upwards until a key is absent. Writes
  complete in order, so an absent index means end-of-recording rather than a
  hole.

  This deliberately adds no manifest and no store capability. Appending to a
  single key is not an option: ``write`` has no mode, an immutable store
  rejects the second write, and a mutable one replaces the object — a
  multi-segment recording would either fail or retain only its last segment.
* A reader cannot tell a still-running recording from one whose process died,
  and does not need to: both are "everything up to the last flush". A
  completion marker written at ``stop`` lets a reader that *does* care —
  a downstream job wanting only finished runs — distinguish them, without
  making the partial case unreadable.

This keeps *A recording is one run* intact: one run, one recording, written as
however many segments its flush policy produced — one, in the default case.

Configuration is local, with a global default
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``record_replay::Config`` is global today, which is why the adaptor grew a
second, per-key override dictionary beside it. Instead the configuration is
passed **to the recorder**, and the global one becomes the default when no
local configuration is supplied.

This also answers where per-key options live: they do not live anywhere. They
are an argument at the call site, so two recordings in one graph differ by
being called differently rather than by a registry keyed on name.

Backend selection can be local too, which is not obvious: overloads guard on
the model through ``requires_``, which runs before the node exists — but
``OperatorCallContext::scalar`` exposes scalar wiring arguments by name, so
``requires_`` reads a supplied ``config`` first and falls back to the wiring
state. The model therefore does not have to stay global for dispatch to work.

Storage
~~~~~~~

Recording writes through ``record_replay::FrameStoreOps`` — the seam RFC 0016
already defines — so the native recorder does not learn about files, object
stores, or Python.

``DataFrameStorage`` is then exposed as a frame store implementation rather
than as a parallel recording stack. The Python adaptor keeps its public surface
(``MemoryDataFrameStorage``, ``FileBasedDataFrameStorage``, subclass hooks,
``set_data_frame_overrides``) and loses only the part that was reimplementing
the recorder.

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

Reading a recording whose options differ from the reader's — a missing as-of
column, renamed partition columns — is resolved by name against the stored
schema, and a genuine mismatch is an error rather than a silent reinterpretation.

Staging
-------

Each step is independently useful and independently verifiable, and the order
is chosen so nothing is rewritten twice:

1. **Row sink.** Parameterise ``emit_partition_rows`` by a sink; ``to_table``
   keeps materialising. No behaviour change; pure refactor with the existing
   tests as the guard.
2. **``TableRecorder`` over a layout.** Arrow schema from the layout, builder
   per column, appending sink. ``FrameRecorder`` re-expressed as the
   no-levels case.
3. **Options.** The structure above, with defaults reproducing current
   behaviour, plumbed through ``record_replay::Config`` per recordable key.
4. **Native ``record`` for partitioned types.** Remove the ``start``-time
   rejection; the ``DATA_FRAME`` model now records ``TSD`` and multi-row
   shapes.
5. **Replay parity.** Grouping, filtering, ``replay_const``.
6. **Adaptor migration.** ``DataFrameStorage`` as a frame store; the Python
   recorder deleted, its public surface unchanged.
7. **Segmented flushing.** Only when a run's output stops fitting in memory.
   Nothing earlier depends on it, because the default writes once at ``stop``.
8. **Table ops and a real builder.** See below. Deliberately last: it
   restructures how the walks are *dispatched*, and doing it before the
   behaviour settled would mean designing the ops table against a moving
   target.

Steps 1–2 are where the performance is; steps 3–5 are where the parity is;
step 7 waits for a workload that needs it; step 8 is what makes it extensible.

Table ops and a real builder (step 8)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table machinery already reuses most of the universal vocabulary —
``TsTableLayout`` is a **Plan** (interned by schema, describing a row's column
order and which columns are keys, flags or values), the TS schema is the
**Schema**, and ``recording_columns`` plus recorder construction is the
**Builder**. What is missing is **Ops**.

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

The shape: a ``TableTypeOps`` (``describe`` / ``emit`` / ``apply``) interned
per schema in a registry like every other, with ``ts_table_layout`` as the
builder that walks the schema, asks each node to describe itself, and interns
the resulting Plan. "How is this type handled" then has one answer — look at
its ops — instead of three walks that have to be read together.

Two constraints bound the design:

* **Ops resolve at BUILD time into the Plan, never per tick.** The per-tick
  path is required to be lock-free and ``shared_ptr``-free, so a per-tick
  registry lookup is not available. This is how Plan/Ops already work
  everywhere else, so it is a constraint the pattern already satisfies.
* **Extensibility is only real if the registry is reachable from the
  extension SDK.** Otherwise this is a tidier core with the same closed set.
  That is the acceptance test, and it connects to the scalar-registration
  machinery of RFC 0003 / 0004 rather than being free.

Nothing landed so far is invalidated: the Plan, the ``RowSink`` seam (which
already separated *producing* cells from *consuming* them), the projection
and the recorder all survive. Step 8 replaces how the three walks dispatch.

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
of partitioning next to ``TsTableLayout``, which already models levels, removed
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

Unresolved questions
--------------------

None outstanding. The flush-durability question is resolved above under
*Flushing writes segments*.

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
* The Python adaptor's existing tests pass unchanged against the migrated
  implementation.
* A recording written by the current Python adaptor replays through the native
  reader.
* ``removes: Omit`` emits no row and no column for a removal, and the replayed
  series is asserted to retain the key — the documented divergence, pinned by
  test rather than left implicit.
* A frame-valued leaf under a ``TSD`` level expands to ``K x R`` rows with the
  key cells repeated.
* ``frame_prefix`` renames the expanded columns and they replay by the
  prefixed name; a collision that survives the prefix is refused at layout
  time.
* A recording interrupted mid-run reads back exactly the rows up to its last
  flush, and never a torn segment.
* Segments read back in write order, and a recording of one segment is
  indistinguishable in content from the same rows flushed across several.
* Two ``record`` calls in one graph with different local configurations produce
  differently-shaped recordings, and a call with no configuration matches the
  global default.
* Selecting the backend through a local configuration dispatches to the same
  overload as the global one.

References
----------

* ``docs/source/developer_guide/record_replay_table.rst`` — the columnar-builder
  ruling (*I5*) this RFC finishes applying.
* RFC 0016 — object-store frame persistence; ``FrameStoreOps`` is the storage
  seam used here.
* ``include/hgraph/lib/std/operators/impl/table_impl.h`` — ``TsTableLayout``.
* ``include/hgraph/types/value/table_codec.h`` — ``TableConverter``,
  ``FrameRecorder``.
* ``python/hgraph/adaptors/data_frame/_data_frame_record_replay.py`` — the
  adaptor whose recording half this RFC removes.
